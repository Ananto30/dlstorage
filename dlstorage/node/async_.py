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
import time
from typing import Any

from dlstorage.connection_pool.async_ import (AsyncConnectionPool,
                                              recv_message, send_message)
from dlstorage.connection_pool.interface import \
    AsyncConnectionPool as AsyncConnectionPoolT
from dlstorage.consistency.interface import AsyncMergeResolver
from dlstorage.consistency.lww import AsyncLWW
from dlstorage.discovery import Discovery
from dlstorage.node.dispatcher import dispatch
from dlstorage.node.interface import AsyncReplicaHandle
from dlstorage.peer_comm.gossip import AsyncGossip
from dlstorage.ring import RendezvousRing
from dlstorage.ring.interface import Ring
from dlstorage.store import LocalLWWStore
from dlstorage.types import Message, MessageType, NodeInfo

logger = logging.getLogger(__name__)

_PURGE_INTERVAL = 10.0  # seconds between TTL-expiry sweeps


class _AsyncLocalHandle(AsyncReplicaHandle):
    """ReplicaHandle backed by the local store (no network)."""

    __slots__ = ("_store",)

    def __init__(self, store: LocalLWWStore) -> None:
        self._store = store

    async def get(self, key: str) -> Any:
        return self._store.get(key)

    async def get_versioned(self, key: str) -> tuple[Any, int, bool] | None:
        return self._store.get_versioned(key)

    async def set(self, key: str, value: Any, ttl: float | None, ts: int) -> bool:
        return self._store.set(key, value, ttl, ts=ts)

    async def delete(self, key: str, ts: int) -> bool:
        return self._store.delete(key, ts=ts)


class _AsyncRemoteHandle(AsyncReplicaHandle):
    """ReplicaHandle backed by the async connection pool."""

    __slots__ = ("_host", "_port", "_pool")

    def __init__(self, node: NodeInfo, pool: AsyncConnectionPoolT) -> None:
        self._host = node.host
        self._port = node.port
        self._pool = pool

    async def get(self, key: str) -> Any:
        result = await self.get_versioned(key)
        if result is None:
            return None
        value, _, is_tombstone = result
        return None if is_tombstone else value

    async def get_versioned(self, key: str) -> tuple[Any, int, bool] | None:
        resp = await self._pool.execute(
            self._host, self._port, Message(MessageType.GET, {"key": key})
        )
        if resp is None:
            return None
        if resp.type == MessageType.OK:
            return (resp.payload.get("value"), resp.payload.get("ts", 0), False)
        if resp.type == MessageType.NOT_FOUND:
            ts = resp.payload.get("ts", 0)
            is_tombstone = resp.payload.get("tombstone", False)
            return (None, ts, is_tombstone) if ts > 0 else None
        return None

    async def set(self, key: str, value: Any, ttl: float | None, ts: int) -> bool:
        payload: dict[str, Any] = {"key": key, "value": value, "ts": ts}
        if ttl is not None:
            payload["ttl"] = ttl
        resp = await self._pool.execute(
            self._host, self._port, Message(MessageType.SET, payload)
        )
        return resp is not None and resp.type == MessageType.OK

    async def delete(self, key: str, ts: int) -> bool:
        resp = await self._pool.execute(
            self._host,
            self._port,
            Message(MessageType.DELETE, {"key": key, "ts": ts}),
        )
        return resp is not None and resp.type == MessageType.OK


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
        merge_resolver: AsyncMergeResolver = AsyncLWW(),
        replication: int = 3,
        backlog: int = 256,
    ) -> None:
        self.info = NodeInfo(host, port)
        self.discovery = discovery
        self._store = LocalLWWStore()
        self._ring = ring
        self._pool = connection_pool
        self._merge_resolver = merge_resolver
        self._replication = replication
        self._backlog = backlog
        self._server: asyncio.Server | None = None
        self._tasks: list[asyncio.Task] = []
        self._handler_tasks: set[asyncio.Task] = set()
        self._gossip = AsyncGossip(self.info, self._ring, self._pool, self.discovery)

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
            asyncio.create_task(self._gossip.gossip_loop(), name="gossip"),
            asyncio.create_task(self._purge_loop(), name="purge"),
        ]
        logger.info("AsyncStorageNode started at %s", self.info.address)
        await self._gossip.announce_join()

    async def stop(self) -> None:
        """Announce departure, stop the server and background tasks."""
        await self._gossip.announce_leave()
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
        self._pool.close_all()
        logger.info("AsyncStorageNode stopped at %s", self.info.address)

    async def __aenter__(self) -> "AsyncStorageNode":
        await self.start()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.stop()

    # Public store API

    async def get(self, key: str) -> Any:
        """Retrieve a value by key, resolved by the consistency policy."""
        candidates = self._ring.get_nodes(key, n=self._replication)
        if not candidates:
            return self._store.get(key)
        return await self._merge_resolver.read_resolve(
            key, self._make_handles(candidates)
        )

    async def set(self, key: str, value: Any, ttl: float | None = None) -> bool:
        """
        Store a value.

        Writes to the top-*replication* nodes concurrently via the
        consistency policy.  Returns True if at least one write succeeded.

        Args:
            key:   String key.
            value: Any Python object (serialised with pickle over the wire).
            ttl:   Optional time-to-live in seconds.
        """
        nodes = self._ring.get_nodes(key, n=self._replication)
        ts = time.time_ns()
        if not nodes:
            self._store.set(key, value, ttl, ts=ts)
            return True
        handles = self._make_handles(nodes)
        results = await asyncio.gather(
            *[h.set(key, value, ttl, ts) for h in handles],
            return_exceptions=True,
        )
        return any(r is True for r in results)

    async def delete(self, key: str) -> bool:
        """Delete a key from the cluster via the consistency policy."""
        nodes = self._ring.get_nodes(key, n=self._replication)
        ts = time.time_ns()
        if not nodes:
            return self._store.delete(key, ts=ts)
        handles = self._make_handles(nodes)
        results = await asyncio.gather(
            *[h.delete(key, ts) for h in handles],
            return_exceptions=True,
        )
        return any(r is True for r in results)

    def _make_handles(self, nodes: list[NodeInfo]) -> list[AsyncReplicaHandle]:
        """Build ReplicaHandle instances for the given ring nodes."""
        return [
            (
                _AsyncLocalHandle(self._store)
                if n == self.info
                else _AsyncRemoteHandle(n, self._pool)
            )
            for n in nodes
        ]

    # Bootstrap

    async def _bootstrap(self) -> None:
        await self._gossip.sync_peers()
        for peer in self.discovery.get_peers():
            self._ring.add(peer)

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
                response = (
                    self._gossip.dispatch(msg)
                    if msg.type.is_gossip()
                    else dispatch(msg, self._store)
                )
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

    # Background tasks

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

    def __repr__(self) -> str:
        return (
            f"StorageNode({self.info.address}, "
            f"peers={len(self._ring) - 1}, "
            f"keys={len(self._store)})"
        )
