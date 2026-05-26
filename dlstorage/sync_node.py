"""
SyncStorageNode – blocking/threaded counterpart to StorageNode.

Each node:
  - Runs a TCP server on a background thread (one handler thread per connection).
  - Routes keys via RendezvousRing (HRW), same as the async node.
  - Stores values in the shared thread-safe LocalStore.
  - Supports the same wire protocol as the async node, so sync and async nodes
    can coexist in the same cluster.

Usage (context-manager style)::

    with SyncStorageNode("127.0.0.1", 7001, StaticDiscovery([...])) as node:
        node.set("key", {"any": "value"})
        val = node.get("key")
        node.delete("key")
"""

from __future__ import annotations

import logging
import socket
import threading
import time
from typing import Any

from .discovery import Discovery, GossipDiscovery
from .ring import RendezvousRing
from .store import LocalStore
from .sync_pool import SyncConnectionPool, recv_message, send_message
from .types import Message, MessageType, NodeInfo

logger = logging.getLogger(__name__)

_PURGE_INTERVAL = 30.0  # seconds between TTL-expiry sweeps


class SyncStorageNode:
    """
    Synchronous (blocking, thread-per-connection) storage node.

    Args:
        host:        Bind address for the TCP server.
        port:        Bind port for the TCP server.
        discovery:   A Discovery backend (Static / DNS / Gossip).
        replication: How many ring nodes each key is written to (default 1).
        max_conns:   Max idle connections per peer in the pool (default 16).
        backlog:     TCP listen backlog (default 256).
    """

    def __init__(
        self,
        host: str,
        port: int,
        discovery: Discovery,
        *,
        replication: int = 1,
        max_conns: int = 16,
        backlog: int = 256,
    ) -> None:
        self.info = NodeInfo(host, port)
        self.discovery = discovery
        self._store = LocalStore()
        self._ring = RendezvousRing()
        self._pool = SyncConnectionPool(max_per_peer=max_conns)
        self._replication = replication
        self._backlog = backlog
        self._server_sock: socket.socket | None = None
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []

    # ----------------------------------------------------------------------- #
    # Lifecycle                                                                #
    # ----------------------------------------------------------------------- #

    def start(self) -> None:
        """Bootstrap peers, bind the TCP server, start background threads."""
        self._bootstrap()
        self._ring.add(self.info)

        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((self.info.host, self.info.port))
        self._server_sock.listen(self._backlog)
        self._server_sock.settimeout(0.5)  # allows the accept loop to check _stop

        accept_thread = threading.Thread(
            target=self._accept_loop,
            daemon=True,
            name=f"dlstorage-accept-{self.info.port}",
        )
        purge_thread = threading.Thread(
            target=self._purge_loop,
            daemon=True,
            name=f"dlstorage-purge-{self.info.port}",
        )
        accept_thread.start()
        purge_thread.start()
        self._threads = [accept_thread, purge_thread]

        logger.info("SyncStorageNode started at %s", self.info.address)
        self._announce_join()

    def stop(self) -> None:
        """Announce departure, stop the server and background threads."""
        self._announce_leave()
        self._stop.set()
        if self._server_sock:
            try:
                self._server_sock.close()
            except OSError:
                pass
        for t in self._threads:
            t.join(timeout=3.0)
        self._pool.close_all()
        logger.info("SyncStorageNode stopped at %s", self.info.address)

    def __enter__(self) -> "SyncStorageNode":
        self.start()
        return self

    def __exit__(self, *_: Any) -> None:
        self.stop()

    # ----------------------------------------------------------------------- #
    # Public store API                                                         #
    # ----------------------------------------------------------------------- #

    def get(self, key: str) -> Any:
        """Return value for *key*, or ``None`` if absent/expired."""
        node = self._ring.get_node(key)
        if node is None or node == self.info:
            return self._store.get(key)
        resp = self._pool.execute(
            node.host,
            node.port,
            Message(MessageType.GET, {"key": key}),
        )
        if resp and resp.type == MessageType.OK:
            return resp.payload.get("value")
        return None

    def set(self, key: str, value: Any, ttl: float | None = None) -> bool:
        """Store *value* under *key* on the top-*replication* ring nodes."""
        nodes = self._ring.get_nodes(key, n=self._replication)
        if not nodes:
            self._store.set(key, value, ttl)
            return True
        return any(self._set_on(n, key, value, ttl) for n in nodes)

    def delete(self, key: str) -> bool:
        """Delete *key* from the top-*replication* ring nodes."""
        nodes = self._ring.get_nodes(key, n=self._replication)
        if not nodes:
            return self._store.delete(key)
        return any(self._delete_on(n, key) for n in nodes)

    # ----------------------------------------------------------------------- #
    # Internal: per-node store helpers                                         #
    # ----------------------------------------------------------------------- #

    def _set_on(self, node: NodeInfo, key: str, value: Any, ttl: float | None) -> bool:
        if node == self.info:
            self._store.set(key, value, ttl)
            return True
        payload: dict[str, Any] = {"key": key, "value": value}
        if ttl is not None:
            payload["ttl"] = ttl
        resp = self._pool.execute(
            node.host, node.port, Message(MessageType.SET, payload)
        )
        return resp is not None and resp.type == MessageType.OK

    def _delete_on(self, node: NodeInfo, key: str) -> bool:
        if node == self.info:
            return self._store.delete(key)
        resp = self._pool.execute(
            node.host,
            node.port,
            Message(MessageType.DELETE, {"key": key}),
        )
        return resp is not None and resp.type == MessageType.OK

    # ----------------------------------------------------------------------- #
    # Bootstrap                                                                #
    # ----------------------------------------------------------------------- #

    def _bootstrap(self) -> None:
        if isinstance(self.discovery, GossipDiscovery):
            self._gossip_bootstrap()
            return
        for peer in self.discovery.get_peers():
            self._ring.add(peer)

    def _gossip_bootstrap(self) -> None:
        assert isinstance(self.discovery, GossipDiscovery)
        seed = self.discovery.seed
        resp = self._pool.execute(
            seed.host,
            seed.port,
            Message(MessageType.PEER_LIST, {"requester": self.info.to_dict()}),
        )
        if resp and resp.type == MessageType.PEER_LIST:
            for raw in resp.payload.get("peers", []):
                peer = NodeInfo.from_dict(raw)
                self.discovery.add_peer(peer)
                self._ring.add(peer)
        self.discovery.add_peer(seed)
        self._ring.add(seed)

    # ----------------------------------------------------------------------- #
    # Peer announcements                                                       #
    # ----------------------------------------------------------------------- #

    def _announce_join(self) -> None:
        self._broadcast(
            Message(MessageType.PEER_ANNOUNCE, {"peer": self.info.to_dict()})
        )

    def _announce_leave(self) -> None:
        self._broadcast(Message(MessageType.PEER_LEAVE, {"peer": self.info.to_dict()}))

    def _broadcast(self, msg: Message) -> None:
        for peer in self._ring.nodes():
            if peer != self.info:
                self._pool.execute(peer.host, peer.port, msg)

    # ----------------------------------------------------------------------- #
    # TCP server                                                               #
    # ----------------------------------------------------------------------- #

    def _accept_loop(self) -> None:
        while not self._stop.is_set():
            try:
                client_sock, addr = self._server_sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            t = threading.Thread(
                target=self._handle_client,
                args=(client_sock, addr),
                daemon=True,
            )
            t.start()

    def _handle_client(self, sock: socket.socket, addr: Any) -> None:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        try:
            while True:
                msg = recv_message(sock)
                response = self._dispatch(msg)
                send_message(sock, response)
        except EOFError:
            pass  # client closed connection
        except Exception as exc:
            logger.debug("Client handler error (%s): %s", addr, exc)
        finally:
            sock.close()

    def _dispatch(self, msg: Message) -> Message:
        match msg.type:
            # ---- membership -------------------------------------------- #
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
                return Message(MessageType.OK, {})

            case MessageType.PEER_LEAVE:
                peer = NodeInfo.from_dict(msg.payload["peer"])
                self._ring.remove(peer)
                if isinstance(self.discovery, GossipDiscovery):
                    self.discovery.remove_peer(peer)
                return Message(MessageType.OK, {})

            # ---- store ------------------------------------------------- #
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

    # ----------------------------------------------------------------------- #
    # Background: TTL purge                                                    #
    # ----------------------------------------------------------------------- #

    def _purge_loop(self) -> None:
        while not self._stop.wait(timeout=_PURGE_INTERVAL):
            removed = self._store.purge_expired()
            if removed:
                logger.debug("Purged %d expired keys", removed)

    # ----------------------------------------------------------------------- #
    # Repr                                                                     #
    # ----------------------------------------------------------------------- #

    def __repr__(self) -> str:
        return (
            f"SyncStorageNode({self.info.address}, "
            f"peers={len(self._ring) - 1}, "
            f"keys={len(self._store)})"
        )
