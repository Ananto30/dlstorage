"""
Synchronous TCP connection pool and wire-protocol helpers.

Same wire format as pool.py (4-byte big-endian length prefix + pickle body)
so sync and async nodes can talk to each other.

Connections are kept in a per-peer ``queue.LifoQueue`` which is already
thread-safe – no explicit lock needed for acquire/release.
"""

from __future__ import annotations

import logging
import pickle
import queue
import socket

from dlstorage.types import Message
from .interface import ConnectionPool as ConnectionPoolProtocol

logger = logging.getLogger(__name__)

HEADER_SIZE = 4  # must match pool.py


def _recvexactly(sock: socket.socket, n: int) -> bytes:
    buf = bytearray(n)
    view = memoryview(buf)
    pos = 0
    while pos < n:
        received = sock.recv_into(view[pos:], n - pos)
        if received == 0:
            raise EOFError("connection closed")
        pos += received
    return bytes(buf)


def send_message(sock: socket.socket, msg: Message) -> None:
    data = pickle.dumps(msg, protocol=pickle.HIGHEST_PROTOCOL)
    header = len(data).to_bytes(HEADER_SIZE, "big")
    sock.sendall(header + data)


def recv_message(sock: socket.socket) -> Message:
    header = _recvexactly(sock, HEADER_SIZE)
    length = int.from_bytes(header, "big")
    data = _recvexactly(sock, length)
    return pickle.loads(data)  # noqa: S301 – trusted internal network only


class ConnectionPool(ConnectionPoolProtocol):
    """
    Thread-safe TCP connection pool backed by ``queue.LifoQueue``.

    ``LifoQueue.get_nowait`` / ``put_nowait`` are O(1) and require no
    additional lock.  Overflow connections (pool full) are closed immediately.
    """

    def __init__(self, max_per_peer: int = 16, connect_timeout: float = 2.0):
        self._max = max_per_peer
        self._timeout = connect_timeout
        # address -> LifoQueue of idle sockets
        self._queues: dict[str, queue.LifoQueue[socket.socket]] = {}
        # guards first-time queue creation (rare path)
        import threading

        self._init_lock = threading.Lock()

    def _queue(self, address: str) -> queue.LifoQueue[socket.socket]:
        q = self._queues.get(address)
        if q is not None:
            return q
        with self._init_lock:
            if address not in self._queues:
                self._queues[address] = queue.LifoQueue(maxsize=self._max)
            return self._queues[address]

    def _new_connection(self, host: str, port: int) -> socket.socket:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.settimeout(self._timeout)
        s.connect((host, port))
        s.settimeout(5.0)
        return s

    def execute(self, host: str, port: int, msg: Message) -> Message | None:
        """Send *msg* and return the response, reusing idle connections."""
        address = f"{host}:{port}"
        q = self._queue(address)

        # Fast path: grab idle connection
        sock = None
        reused = False
        while True:
            try:
                sock = q.get_nowait()
                reused = True
                break
            except queue.Empty:
                break

        # Open new connection if nothing was idle
        if sock is None:
            try:
                sock = self._new_connection(host, port)
            except Exception as exc:
                logger.debug("Cannot connect to %s: %s", address, exc)
                return None

        try:
            send_message(sock, msg)
            response = recv_message(sock)
            # Return to pool; discard if pool is full
            try:
                q.put_nowait(sock)
            except queue.Full:
                sock.close()
            return response
        except Exception as exc:
            logger.debug("Connection to %s failed: %s", address, exc)
            try:
                sock.close()
            except Exception:
                pass
            if not reused:
                return None
            # Pooled connection was stale (peer restarted) — retry once fresh
            try:
                sock = self._new_connection(host, port)
            except Exception as exc2:
                logger.debug("Cannot connect to %s: %s", address, exc2)
                return None
            try:
                send_message(sock, msg)
                response = recv_message(sock)
                try:
                    q.put_nowait(sock)
                except queue.Full:
                    sock.close()
                return response
            except Exception as exc2:
                logger.debug("Retry to %s failed: %s", address, exc2)
                try:
                    sock.close()
                except Exception:
                    pass
                return None

    def close_peer(self, host: str, port: int) -> None:
        """Drain and close all pooled connections for one peer."""
        address = f"{host}:{port}"
        q = self._queues.pop(address, None)
        if q is None:
            return
        while True:
            try:
                q.get_nowait().close()
            except (queue.Empty, OSError):
                break

    def close_all(self) -> None:
        for q in self._queues.values():
            while True:
                try:
                    q.get_nowait().close()
                except (queue.Empty, OSError):
                    break
        self._queues.clear()
