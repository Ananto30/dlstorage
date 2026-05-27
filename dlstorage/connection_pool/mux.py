"""
MuxConnectionPool – I/O-multiplexed connection pool using selectors.

Architecture
------------
One background thread owns a ``selectors.DefaultSelector`` (kqueue on macOS,
epoll on Linux) and handles ALL socket reads.  Calling threads:

  1. Grab an idle socket from the per-peer pool (lock-free via deque pop).
  2. Send the serialised request synchronously with ``sendall()`` — on loopback
     with messages < 64 KB this returns in microseconds.
  3. Drop the socket into the selector via a thread-safe register queue and
     write one byte to a wakeup socketpair to interrupt the blocked selector.
  4. Block cheaply on a ``threading.Event`` until the selector thread delivers
     the response.

This means N caller threads can have N requests in-flight simultaneously while
only ONE OS thread does I/O.  No asyncio, no coroutines, no event loop.

Same wire format as pool.py (4-byte big-endian length prefix + pickle body) so
sync and async nodes remain wire-compatible.
"""

from __future__ import annotations

import logging
import pickle
import queue as _queue
import selectors
import socket
import threading
from collections import deque
from dataclasses import dataclass, field

from dlstorage.connection_pool.wire import HEADER_SIZE
from dlstorage.types import Message

from .interface import ConnectionPool as ConnectionPoolProtocol

logger = logging.getLogger(__name__)


@dataclass
class _InFlight:
    address: str
    event: threading.Event
    result: list  # [Message | None]  – written by selector thread
    buf: bytearray = field(default_factory=bytearray)
    expected: int = HEADER_SIZE  # bytes we still need to read
    hdr_done: bool = False  # False = reading header, True = body


class MuxConnectionPool(ConnectionPoolProtocol):
    """
    I/O-multiplexed connection pool.

    Args:
        max_per_peer:    Max idle sockets kept per peer address (default 64).
        connect_timeout: Timeout for opening new connections in seconds (default 2.0).
    """

    def __init__(self, max_per_peer: int = 64, connect_timeout: float = 2.0) -> None:
        self._max = max_per_peer
        self._timeout = connect_timeout

        # Per-peer idle socket pool (LIFO deque, protected by _idle_lock)
        self._idle: dict[str, deque[socket.socket]] = {}
        self._idle_lock = threading.Lock()

        # Selector – owned exclusively by _io_loop thread
        self._sel = selectors.DefaultSelector()

        # New-socket registrations queued from caller threads
        self._reg_queue: _queue.SimpleQueue[tuple[socket.socket, _InFlight]] = (
            _queue.SimpleQueue()
        )

        # Socketpair used to wake the selector when _reg_queue gets an item
        self._wake_r, self._wake_w = socket.socketpair()
        self._wake_r.setblocking(False)
        self._sel.register(self._wake_r, selectors.EVENT_READ, data=None)

        self._running = True
        self._thread = threading.Thread(
            target=self._io_loop, daemon=True, name="dlstorage-mux"
        )
        self._thread.start()

    def execute(self, host: str, port: int, msg: Message) -> Message | None:
        """
        Send *msg* to ``host:port`` and return the response.

        Thread-safe.  Blocks the calling thread only on ``threading.Event``
        (no blocking socket recv in the calling thread).
        """
        address = f"{host}:{port}"
        sock = self._acquire(host, port, address)
        if sock is None:
            return None

        data = pickle.dumps(msg, protocol=pickle.HIGHEST_PROTOCOL)
        hdr = len(data).to_bytes(HEADER_SIZE, "big")
        try:
            sock.sendall(hdr + data)  # synchronous; fast on loopback
        except Exception as exc:
            logger.debug("send to %s failed: %s", address, exc)
            sock.close()
            return None

        # Hand off to the selector thread for async read
        sock.setblocking(False)
        inflight = _InFlight(address=address, event=threading.Event(), result=[None])
        self._reg_queue.put((sock, inflight))
        self._wakeup()

        # Block until response arrives (or timeout)
        inflight.event.wait(timeout=5.0)
        return inflight.result[0]

    def close_peer(self, host: str, port: int) -> None:
        """Drain and close all pooled connections for one peer."""
        address = f"{host}:{port}"
        with self._idle_lock:
            socks = self._idle.pop(address, [])
        for s in socks:
            try:
                s.close()
            except Exception:
                pass

    def close_all(self) -> None:
        self._running = False
        self._wakeup()
        self._thread.join(timeout=2.0)
        try:
            self._sel.close()
        except Exception:
            pass
        self._wake_r.close()
        self._wake_w.close()
        with self._idle_lock:
            for socks in self._idle.values():
                for s in socks:
                    try:
                        s.close()
                    except Exception:
                        pass
            self._idle.clear()

    def _acquire(self, host: str, port: int, address: str) -> socket.socket | None:
        with self._idle_lock:
            pool = self._idle.get(address)
            if pool:
                while pool:
                    s = pool.pop()  # LIFO – most recently used stays warm
                    return s  # assume alive; errors caught in execute()
        # No idle socket → open new connection (blocking, done in caller thread)
        s = None
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            s.settimeout(self._timeout)
            s.connect((host, port))
            s.settimeout(None)  # will be set non-blocking before selector
            return s
        except Exception as exc:
            logger.debug("connect to %s failed: %s", address, exc)
            if s is not None:
                try:
                    s.close()
                except Exception:
                    pass
            return None

    def _return(self, address: str, sock: socket.socket) -> None:
        """Return *sock* to the idle pool (called from selector thread)."""
        sock.setblocking(True)
        with self._idle_lock:
            pool = self._idle.setdefault(address, deque())
            if len(pool) < self._max:
                pool.appendleft(sock)
            else:
                sock.close()

    def _wakeup(self) -> None:
        try:
            self._wake_w.send(b"\x00")
        except Exception:
            pass

    # Selector / I/O loop  (runs on dedicated thread)

    def _io_loop(self) -> None:
        sel = self._sel
        while self._running:
            try:
                events = sel.select(timeout=0.2)
            except Exception:
                continue

            for key, _ in events:
                if key.fileobj is self._wake_r:
                    # Drain the wakeup bytes
                    try:
                        self._wake_r.recv(4096)
                    except BlockingIOError:
                        pass
                    # Register any newly submitted sockets
                    while True:
                        try:
                            sock, inflight = self._reg_queue.get_nowait()
                            sel.register(sock, selectors.EVENT_READ, data=inflight)
                        except _queue.Empty:
                            break
                else:
                    self._handle_read(key.fileobj, key.data)  # type: ignore[arg-type]

        # Drain remaining in-flight on shutdown
        for key in list(sel.get_map().values()):
            if key.fileobj is not self._wake_r:
                inflight: _InFlight = key.data
                inflight.event.set()  # unblock waiting callers with result=None

    def _handle_read(self, sock: socket.socket, inflight: _InFlight) -> None:
        try:
            need = inflight.expected - len(inflight.buf)
            chunk = sock.recv(need)
        except (BlockingIOError, InterruptedError):
            return
        except Exception as exc:
            logger.debug("recv error on %s: %s", inflight.address, exc)
            self._fail(sock, inflight)
            return

        if not chunk:
            self._fail(sock, inflight)
            return

        inflight.buf.extend(chunk)

        if len(inflight.buf) < inflight.expected:
            return  # need more bytes

        if not inflight.hdr_done:
            # Header complete → read body
            inflight.expected = int.from_bytes(inflight.buf[:HEADER_SIZE], "big")
            inflight.buf = bytearray()
            inflight.hdr_done = True
        else:
            # Body complete → deserialise and deliver
            try:
                msg = pickle.loads(bytes(inflight.buf))  # noqa: S301
            except Exception as exc:
                logger.debug("deserialise error: %s", exc)
                self._fail(sock, inflight)
                return

            self._sel.unregister(sock)
            self._return(inflight.address, sock)
            inflight.result[0] = msg
            inflight.event.set()

    def _fail(self, sock: socket.socket, inflight: _InFlight) -> None:
        try:
            self._sel.unregister(sock)
        except Exception:
            pass
        try:
            sock.close()
        except Exception:
            pass
        inflight.event.set()  # caller unblocks with result=None
