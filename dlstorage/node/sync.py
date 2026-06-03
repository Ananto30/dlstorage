"""
StorageNode – blocking/threaded counterpart to AsyncStorageNode.

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
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from dlstorage.connection_pool.interface import ConnectionPool
from dlstorage.connection_pool.sync import ConnectionPool as SimpleConnectionPool
from dlstorage.connection_pool.wire import recv_message, send_message
from dlstorage.consistency.interface import MergeResolver
from dlstorage.consistency.lww import LWW
from dlstorage.discovery import Discovery
from dlstorage.node.dispatcher import dispatch
from dlstorage.peer_comm.gossip import Gossip
from dlstorage.ring import RendezvousRing, Ring
from dlstorage.store import LocalLWWStore
from dlstorage.types import Message, MessageType, NodeInfo

from .interface import ReplicaHandle
from .interface import StorageNode as SyncStorageNodeProto

logger = logging.getLogger(__name__)

_PURGE_INTERVAL = 10.0  # seconds between TTL-expiry sweeps


class _SyncLocalHandle(ReplicaHandle):
    """ReplicaHandle backed by the local store (no network)."""

    __slots__ = ("_store",)

    def __init__(self, store: LocalLWWStore) -> None:
        self._store = store

    def get(self, key: str) -> Any:
        return self._store.get(key)

    def get_versioned(self, key: str) -> tuple[Any, int, bool] | None:
        return self._store.get_versioned(key)

    def set(self, key: str, value: Any, ttl: float | None, ts: int) -> bool:
        return self._store.set(key, value, ttl, ts=ts)

    def delete(self, key: str, ts: int) -> bool:
        return self._store.delete(key, ts=ts)


class _SyncRemoteHandle(ReplicaHandle):
    """ReplicaHandle backed by the sync connection pool."""

    __slots__ = ("_host", "_port", "_pool")

    def __init__(self, node: NodeInfo, pool: ConnectionPool) -> None:
        self._host = node.host
        self._port = node.port
        self._pool = pool

    def get(self, key: str) -> Any:
        result = self.get_versioned(key)
        if result is None:
            return None
        value, _, is_tombstone = result
        return None if is_tombstone else value

    def get_versioned(self, key: str) -> tuple[Any, int, bool] | None:
        resp = self._pool.execute(
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

    def set(self, key: str, value: Any, ttl: float | None, ts: int) -> bool:
        payload: dict[str, Any] = {"key": key, "value": value, "ts": ts}
        if ttl is not None:
            payload["ttl"] = ttl
        resp = self._pool.execute(
            self._host, self._port, Message(MessageType.SET, payload)
        )
        return resp is not None and resp.type == MessageType.OK

    def delete(self, key: str, ts: int) -> bool:
        resp = self._pool.execute(
            self._host,
            self._port,
            Message(MessageType.DELETE, {"key": key, "ts": ts}),
        )
        return resp is not None and resp.type == MessageType.OK


class StorageNode(SyncStorageNodeProto):
    """
    Synchronous (blocking, thread-per-connection) storage node.

    The outbound connection pool uses I/O multiplexing via
    ``selectors.DefaultSelector`` (kqueue / epoll) so many concurrent
    requests share a single selector thread instead of blocking per-thread.

    Args:
        discovery:   A Discovery backend (Static / DNS / Gossip).
        host:        Bind address for the TCP server (default "0.0.0.0").
        port:        Bind port for the TCP server (default 7001).
        ring:        Ring implementation for key routing (default Rendezvous).
        connection_pool: ConnectionPool implementation for peer connections
                         (default MuxConnectionPool).
        replication: How many ring nodes each key is written to (default 3).
        backlog:     TCP listen backlog (default 256).
    """

    def __init__(
        self,
        discovery: Discovery,
        host: str = "0.0.0.0",
        port: int = 7001,
        *,
        advertise_host: str | None = None,
        ring: Ring = RendezvousRing(),
        connection_pool: ConnectionPool = SimpleConnectionPool(max_per_peer=64),
        merge_resolver: MergeResolver = LWW(),
        replication: int = 2,
        backlog: int = 256,
    ) -> None:
        # bind_host is the address the TCP server listens on (e.g. 0.0.0.0).
        # self.info uses advertise_host so peers can reach this node by its
        # actual IP/hostname rather than the wildcard bind address.
        self._bind_host = host
        advertise_host = advertise_host or socket.gethostbyname(socket.gethostname())
        self.info = NodeInfo(advertise_host, port)

        self.discovery = discovery
        self._store = LocalLWWStore()
        self._ring = ring
        self._pool = connection_pool
        self._merge_resolver = merge_resolver
        self._replication = replication
        self._backlog = backlog
        self._server_sock: socket.socket | None = None
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self._gossip = Gossip(self.info, self._ring, self._pool, self.discovery)
        # Fan-out executor: sends to all replicas in parallel (like Redis pipeline)
        self._fanout = ThreadPoolExecutor(
            max_workers=64, thread_name_prefix="dlstorage-fanout"
        )

    # Lifecycle

    def start(self) -> None:
        """Bootstrap peers, bind the TCP server, start background threads."""
        self._bootstrap()
        self._ring.add(self.info)

        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((self._bind_host, self.info.port))
        self._server_sock.listen(self._backlog)
        self._server_sock.settimeout(0.5)

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
        gossip_thread = threading.Thread(
            target=self._gossip.gossip_loop,
            daemon=True,
            name=f"dlstorage-gossip-{self.info.port}",
        )
        accept_thread.start()
        purge_thread.start()
        gossip_thread.start()
        self._threads = [accept_thread, purge_thread, gossip_thread]

        logger.info("SyncStorageNode started at %s", self.info.address)
        self._gossip.announce_join()

    def stop(self) -> None:
        """Announce departure, stop the server and background threads."""
        self._gossip.announce_leave()
        self._stop.set()
        self._fanout.shutdown(wait=False)
        if self._server_sock:
            try:
                self._server_sock.close()
            except OSError:
                pass
        for t in self._threads:
            t.join(timeout=3.0)
        self._pool.close_all()
        logger.info("SyncStorageNode stopped at %s", self.info.address)

    def __enter__(self) -> "StorageNode":
        self.start()
        return self

    def __exit__(self, *_: Any) -> None:
        self.stop()

    # Public store API

    def get(self, key: str) -> Any:
        """Return value for *key*, resolved by the consistency policy."""
        candidates = self._ring.get_nodes(key, n=self._replication)
        if not candidates:
            return self._store.get(key)

        logger.debug("Getting key %r from nodes %s", key, candidates)

        return self._merge_resolver.read_resolve(key, self._make_handles(candidates))

    def set(self, key: str, value: Any, ttl: float | None = None) -> bool:
        """Store *value* under *key* on the top-*replication* ring nodes."""
        nodes = self._ring.get_nodes(key, n=self._replication)
        ts = time.time_ns()
        if not nodes:
            self._store.set(key, value, ttl, ts=ts)
            logger.debug("Set key %r locally (%s)", key, self.info.address)
            return True

        handles = self._make_handles(nodes)
        futs = [self._fanout.submit(h.set, key, value, ttl, ts) for h in handles]
        results = [f.result() for f in as_completed(futs, timeout=5.0)]
        logger.debug("Key %r set in nodes %s with results: %s", key, nodes, results)
        return any(results)

    def delete(self, key: str) -> bool:
        """Delete *key* from the top-*replication* ring nodes."""
        nodes = self._ring.get_nodes(key, n=self._replication)
        ts = time.time_ns()
        if not nodes:
            self._store.delete(key, ts=ts)
            logger.debug("Deleted key %r locally (%s)", key, self.info.address)
            return True

        handles = self._make_handles(nodes)
        futs = [self._fanout.submit(h.delete, key, ts) for h in handles]
        results = [f.result() for f in as_completed(futs, timeout=5.0)]
        logger.debug("Key %r delete in nodes %s with results: %s", key, nodes, results)
        return any(results)

    def _make_handles(self, nodes: list[NodeInfo]) -> list[ReplicaHandle]:
        """Build ReplicaHandle instances for the given ring nodes."""
        return [
            (
                _SyncLocalHandle(self._store)
                if n == self.info
                else _SyncRemoteHandle(n, self._pool)
            )
            for n in nodes
        ]

    # Bootstrap

    def _bootstrap(self) -> None:
        for peer in self.discovery.get_peers():
            self._ring.add(peer)

    # TCP server

    def _accept_loop(self) -> None:
        assert self._server_sock
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
                response = (
                    self._gossip.dispatch(msg)
                    if msg.type.is_gossip()
                    else dispatch(msg, self._store)
                )
                send_message(sock, response)
        except EOFError:
            pass
        except Exception as exc:
            logger.warning("Client handler error (%s): %s", addr, exc)
        finally:
            sock.close()

    # Background: TTL purge

    def _purge_loop(self) -> None:
        while not self._stop.wait(timeout=_PURGE_INTERVAL):
            removed = self._store.purge_expired()
            if removed:
                logger.debug("Purged %d expired keys", removed)

    def __repr__(self) -> str:
        return (
            f"SyncStorageNode({self.info.address}, "
            f"peers={len(self._ring) - 1}, "
            f"keys={len(self._store)})"
        )
