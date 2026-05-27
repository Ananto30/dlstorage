"""
AsyncStorageNode – the core of dlstorage.

Each node:
  - Runs a TCP server (asyncio) that speaks the dlstorage wire protocol.
  - Maintains a RendezvousRing to route keys to the correct peer.
  - Stores values locally in a thread-safe LocalStore.
  - Participates in gossip to discover and propagate peer membership.
  - Uses a ConnectionPool for efficient outbound TCP reuse.

Usage (context-manager style)::

    async with AsyncStorageNode("127.0.0.1", 7001, StaticDiscovery([...])) as node:
        await node.set("key", {"any": "value"})
        val = await node.get("key")
        await node.delete("key")
"""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Any

from dlstorage.connection_pool.interface import ConnectionPool
from dlstorage.discovery import Discovery, GossipDiscovery
from dlstorage.connection_pool.async_ import (
    recv_message,
    send_message,
    AsyncConnectionPool,
)
from dlstorage.connection_pool.interface import (
    AsyncConnectionPool as AsyncConnectionPoolT,
)
from dlstorage.ring import RendezvousRing
from dlstorage.ring.interface import Ring
from dlstorage.store import LocalStore
from dlstorage.types import Message, MessageType, NodeInfo

logger = logging.getLogger(__name__)

_GOSSIP_INTERVAL = 5.0  # seconds between gossip rounds
_GOSSIP_FANOUT = 3  # peers to gossip with per round
_PURGE_INTERVAL = 30.0  # seconds between TTL-expiry sweeps


class AsyncStorageNode:
    """
    A single node in the distributed storage cluster.

    Args:
        discovery:   A Discovery backend (Static / DNS / Gossip).
        host:        Bind address for the TCP server (default "0.0.0.0").
        port:        Bind port for the TCP server (default 7001).
        ring:        Ring implementation for key routing (default Rendezvous).
        connection_pool: ConnectionPool implementation for outbound connections (default AsyncConnectionPool).
        replication: How many ring nodes each key is written to (default 3).
        backlog:     TCP listen backlog (default 256).
    """

    def __init__(
        self,
        discovery: Discovery,
        host: str = "0.0.0.0",
        port: int = 7001,
        *,
        ring: Ring = RendezvousRing(),
        connection_pool: AsyncConnectionPoolT = AsyncConnectionPool(max_per_peer=64),
        replication: int = 3,
        backlog: int = 256,
    ) -> None:
        self.info = NodeInfo(host, port)
        self.discovery = discovery
        self._store = LocalStore()
        self._ring = ring
        self._pool = connection_pool
        self._replication = replication
        self._backlog = backlog
        self._server: asyncio.Server | None = None
        self._tasks: list[asyncio.Task] = []
        self._handler_tasks: set[asyncio.Task] = set()

    # Lifecycle

    async def start(self) -> None:
        """Bootstrap peers, start TCP server, launch background tasks."""
        await self._bootstrap()
        self._ring.add(self.info)

        self._server = await asyncio.start_server(
            self._handle_client,
            self.info.host,
            self.info.port,
            backlog=self._backlog,
        )
        self._tasks = [
            asyncio.create_task(self._gossip_loop(), name="gossip"),
            asyncio.create_task(self._purge_loop(), name="purge"),
        ]
        logger.info("AsyncStorageNode started at %s", self.info.address)
        await self._announce_join()

    async def stop(self) -> None:
        """Announce departure, stop the server and background tasks."""
        await self._announce_leave()
        # Cancel background tasks
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        # Cancel active client-handler tasks so wait_closed() returns promptly
        for task in list(self._handler_tasks):
            task.cancel()
        await asyncio.gather(*self._handler_tasks, return_exceptions=True)
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        await self._pool.close_all()
        logger.info("AsyncStorageNode stopped at %s", self.info.address)

    async def __aenter__(self) -> "AsyncStorageNode":
        await self.start()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.stop()

    # Public store API

    async def get(self, key: str) -> Any:
        """
        Retrieve a value by key.

        Tries the same replica set that set() wrote to (top-*replication*
        nodes by HRW score).  Walking all replicas means the value is found
        even when the ring changed after the write (peer left / rejoined).
        Returns ``None`` if no replica holds the key.
        """
        nodes = self._ring.get_nodes(key, n=self._replication)
        if not nodes:
            return self._store.get(key)
        for node in nodes:
            if node == self.info:
                value = self._store.get(key)
                if value is not None:
                    return value
            else:
                resp = await self._pool.execute(
                    node.host,
                    node.port,
                    Message(MessageType.GET, {"key": key}),
                )
                if resp and resp.type == MessageType.OK:
                    return resp.payload.get("value")
        return None

    async def set(self, key: str, value: Any, ttl: float | None = None) -> bool:
        """
        Store a value.

        Writes to the top-*replication* nodes for *key* concurrently.
        Returns True if at least one write succeeded.

        Args:
            key:   String key.
            value: Any Python object (serialised with pickle over the wire).
            ttl:   Optional time-to-live in seconds.
        """
        nodes = self._ring.get_nodes(key, n=self._replication)
        if not nodes:
            self._store.set(key, value, ttl)
            return True
        results = await asyncio.gather(
            *[self._set_on(n, key, value, ttl) for n in nodes],
            return_exceptions=True,
        )
        return any(r is True for r in results)

    async def delete(self, key: str) -> bool:
        """
        Delete a key from the cluster.

        Removes from the top-*replication* nodes concurrently.
        Returns True if at least one node had the key.
        """
        nodes = self._ring.get_nodes(key, n=self._replication)
        if not nodes:
            return self._store.delete(key)
        results = await asyncio.gather(
            *[self._delete_on(n, key) for n in nodes],
            return_exceptions=True,
        )
        return any(r is True for r in results)

    # Internal: per-node store helpers

    async def _set_on(
        self, node: NodeInfo, key: str, value: Any, ttl: float | None
    ) -> bool:
        if node == self.info:
            self._store.set(key, value, ttl)
            return True
        payload: dict[str, Any] = {"key": key, "value": value}
        if ttl is not None:
            payload["ttl"] = ttl
        resp = await self._pool.execute(
            node.host, node.port, Message(MessageType.SET, payload)
        )
        return resp is not None and resp.type == MessageType.OK

    async def _delete_on(self, node: NodeInfo, key: str) -> bool:
        if node == self.info:
            return self._store.delete(key)
        resp = await self._pool.execute(
            node.host,
            node.port,
            Message(MessageType.DELETE, {"key": key}),
        )
        return resp is not None and resp.type == MessageType.OK

    # Bootstrap

    async def _bootstrap(self) -> None:
        if isinstance(self.discovery, GossipDiscovery):
            await self._gossip_bootstrap()
            return

        for peer in self.discovery.get_peers():
            self._ring.add(peer)

    async def _gossip_bootstrap(self) -> None:
        """Fetch the seed's peer list over TCP, then populate the ring."""
        assert isinstance(self.discovery, GossipDiscovery)
        seed = self.discovery.seed
        resp = await self._pool.execute(
            seed.host,
            seed.port,
            Message(MessageType.PEER_LIST, {"requester": self.info.to_dict()}),
        )
        if resp and resp.type == MessageType.PEER_LIST:
            for raw in resp.payload.get("peers", []):
                peer = NodeInfo.from_dict(raw)
                self.discovery.add_peer(peer)
                self._ring.add(peer)
        # Always treat the seed itself as a known peer
        self.discovery.add_peer(seed)
        self._ring.add(seed)

    # Gossip

    async def _announce_join(self) -> None:
        msg = Message(MessageType.PEER_ANNOUNCE, {"peer": self.info.to_dict()})
        await self._broadcast(msg)

    async def _announce_leave(self) -> None:
        msg = Message(MessageType.PEER_LEAVE, {"peer": self.info.to_dict()})
        await self._broadcast(msg)

    async def _broadcast(self, msg: Message) -> None:
        peers = [n for n in self._ring.nodes() if n != self.info]
        if peers:
            await asyncio.gather(
                *[self._pool.execute(p.host, p.port, msg) for p in peers],
                return_exceptions=True,
            )

    async def _gossip_loop(self) -> None:
        """Periodically exchange peer lists with a random subset of peers."""
        while True:
            try:
                await asyncio.sleep(_GOSSIP_INTERVAL)
                peers = [n for n in self._ring.nodes() if n != self.info]
                if not peers:
                    continue
                sample = random.sample(peers, min(_GOSSIP_FANOUT, len(peers)))
                await asyncio.gather(
                    *[self._gossip_with(p) for p in sample],
                    return_exceptions=True,
                )
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.debug("Gossip round error: %s", exc)

    async def _gossip_with(self, peer: NodeInfo) -> None:
        resp = await self._pool.execute(
            peer.host,
            peer.port,
            Message(MessageType.PEER_LIST, {"requester": self.info.to_dict()}),
        )
        if resp and resp.type == MessageType.PEER_LIST:
            known = set(self._ring.nodes())
            for raw in resp.payload.get("peers", []):
                new_peer = NodeInfo.from_dict(raw)
                if new_peer != self.info and new_peer not in known:
                    self._ring.add(new_peer)
                    if isinstance(self.discovery, GossipDiscovery):
                        self.discovery.add_peer(new_peer)
                    logger.debug("Discovered new peer via gossip: %s", new_peer)

    async def _purge_loop(self) -> None:
        """Periodically remove expired keys from the local store."""
        while True:
            try:
                await asyncio.sleep(_PURGE_INTERVAL)
                removed = self._store.purge_expired()
                if removed:
                    logger.debug("Purged %d expired keys", removed)
            except asyncio.CancelledError:
                break

    # TCP server

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        task = asyncio.current_task()
        if task:
            self._handler_tasks.add(task)
        peer_addr = writer.get_extra_info("peername")
        try:
            while True:
                msg = await recv_message(reader)
                assert msg
                response = await self._dispatch(msg)
                await send_message(writer, response)
        except asyncio.IncompleteReadError:
            pass  # client closed the connection
        except asyncio.CancelledError:
            pass  # node is shutting down
        except Exception as exc:
            logger.debug("Client handler error (%s): %s", peer_addr, exc)
        finally:
            writer.close()
            if task:
                self._handler_tasks.discard(task)

    async def _dispatch(self, msg: Message) -> Message:
        match msg.type:
            # Membership
            case MessageType.PING:
                return Message(MessageType.PONG, {"node": self.info.to_dict()})

            case MessageType.PEER_LIST:
                peers = [n.to_dict() for n in self._ring.nodes() if n != self.info]
                return Message(MessageType.PEER_LIST, {"peers": peers})

            case MessageType.PEER_ANNOUNCE:
                peer = NodeInfo.from_dict(msg.payload["peer"])
                if peer not in self._ring.nodes():
                    self._ring.add(peer)
                    if isinstance(self.discovery, GossipDiscovery):
                        self.discovery.add_peer(peer)
                    logger.debug("Peer joined: %s", peer)
                return Message(MessageType.OK, {})

            case MessageType.PEER_LEAVE:
                peer = NodeInfo.from_dict(msg.payload["peer"])
                self._ring.remove(peer)
                if isinstance(self.discovery, GossipDiscovery):
                    self.discovery.remove_peer(peer)
                await self._pool.close_peer(peer.host, peer.port)
                logger.debug("Peer left: %s", peer)
                return Message(MessageType.OK, {})

            # Store
            case MessageType.GET:
                key = msg.payload["key"]
                value = self._store.get(key)
                if value is None:
                    return Message(MessageType.NOT_FOUND, {"key": key})
                return Message(MessageType.OK, {"key": key, "value": value})

            case MessageType.SET:
                key = msg.payload["key"]
                value = msg.payload["value"]
                ttl = msg.payload.get("ttl")
                self._store.set(key, value, ttl)
                return Message(MessageType.OK, {"key": key})

            case MessageType.DELETE:
                key = msg.payload["key"]
                deleted = self._store.delete(key)
                if deleted:
                    return Message(MessageType.OK, {"key": key})
                return Message(MessageType.NOT_FOUND, {"key": key})

            case _:
                return Message(
                    MessageType.ERROR,
                    {"error": f"unknown message type: {msg.type}"},
                )

    def __repr__(self) -> str:
        return (
            f"StorageNode({self.info.address}, "
            f"peers={len(self._ring) - 1}, "
            f"keys={len(self._store)})"
        )
