import asyncio
import logging

from dlstorage.connection_pool.wire import recv_message_async, send_message_async
from dlstorage.types import Message

from .interface import AsyncConnectionPool as AsyncConnectionPoolProtocol

logger = logging.getLogger(__name__)


class AsyncConnectionPool(AsyncConnectionPoolProtocol):
    """
    Async TCP connection pool.

    Uses one ``asyncio.LifoQueue`` per peer address.  ``get_nowait`` and
    ``put_nowait`` are O(1) with no lock, eliminating the contention that a
    shared ``asyncio.Lock`` would create under high concurrency.
    """

    def __init__(self, max_per_peer: int = 5, connect_timeout: float = 2.0):
        self._max = max_per_peer
        self._timeout = connect_timeout
        # address -> LifoQueue of idle (reader, writer) pairs
        self._queues: dict[str, asyncio.LifoQueue] = {}
        # guards first-time queue creation only (rare path)
        self._init_lock = asyncio.Lock()

    def _queue(self, address: str) -> asyncio.LifoQueue:
        q = self._queues.get(address)
        if q is not None:
            return q
        # created lazily; the init_lock is only held during first creation
        q = asyncio.LifoQueue(maxsize=self._max)
        self._queues[address] = q
        return q

    async def _new_connection(self, host: str, port: int):
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=self._timeout,
        )
        return reader, writer

    async def execute(self, host: str, port: int, msg: Message) -> Message | None:
        """Send a message and return the response, reusing connections."""
        address = f"{host}:{port}"
        # Fast path: grab an idle connection without any lock
        q = self._queue(address)
        reader, writer = None, None
        reused = False
        while not q.empty():
            try:
                r, w = q.get_nowait()
            except asyncio.QueueEmpty:
                break
            if not w.is_closing():
                reader, writer = r, w
                reused = True
                break
            # stale connection – discard and try next
        if writer is None:
            try:
                reader, writer = await self._new_connection(host, port)
            except Exception as e:
                logger.debug("Cannot connect to %s: %s", address, e)
                return None
        try:
            await send_message_async(writer, msg)
            assert reader is not None
            response = await asyncio.wait_for(recv_message_async(reader), timeout=5.0)
            # Return to pool (no lock needed – put_nowait is thread-safe in asyncio)
            if not writer.is_closing():
                try:
                    q.put_nowait((reader, writer))
                except asyncio.QueueFull:
                    writer.close()
            return response
        except Exception as e:
            logger.debug("Connection to %s failed: %s", address, e)
            writer.close()
            if not reused:
                return None
            # Pooled connection was stale (peer restarted) — retry once fresh
            try:
                reader, writer = await self._new_connection(host, port)
            except Exception as e2:
                logger.debug("Cannot connect to %s: %s", address, e2)
                return None
            try:
                await send_message_async(writer, msg)
                response = await asyncio.wait_for(
                    recv_message_async(reader), timeout=5.0
                )
                if not writer.is_closing():
                    try:
                        q.put_nowait((reader, writer))
                    except asyncio.QueueFull:
                        writer.close()
                return response
            except Exception as e2:
                logger.debug("Retry to %s failed: %s", address, e2)
                writer.close()
                return None

    async def execute_raw(self, host: str, port: int, encoded: bytes) -> Message | None:
        """Send pre-encoded msgpack bytes and return the response.

        Identical flow to ``execute`` but skips re-serialising the message,
        allowing callers to encode once and fan out to N peers.
        """
        address = f"{host}:{port}"
        q = self._queue(address)
        reader, writer = None, None
        reused = False
        while not q.empty():
            try:
                r, w = q.get_nowait()
            except asyncio.QueueEmpty:
                break
            if not w.is_closing():
                reader, writer = r, w
                reused = True
                break
        if writer is None:
            try:
                reader, writer = await self._new_connection(host, port)
            except Exception as e:
                logger.debug("Cannot connect to %s: %s", address, e)
                return None
        header = len(encoded).to_bytes(4, "big")
        try:
            writer.write(header + encoded)
            assert reader is not None
            response = await asyncio.wait_for(recv_message_async(reader), timeout=5.0)
            if not writer.is_closing():
                try:
                    q.put_nowait((reader, writer))
                except asyncio.QueueFull:
                    writer.close()
            return response
        except Exception as e:
            logger.debug("Connection to %s failed (raw): %s", address, e)
            writer.close()
            if not reused:
                return None
            try:
                reader, writer = await self._new_connection(host, port)
            except Exception as e2:
                logger.debug("Cannot connect to %s: %s", address, e2)
                return None
            try:
                writer.write(header + encoded)
                response = await asyncio.wait_for(
                    recv_message_async(reader), timeout=5.0
                )
                if not writer.is_closing():
                    try:
                        q.put_nowait((reader, writer))
                    except asyncio.QueueFull:
                        writer.close()
                return response
            except Exception as e3:
                logger.debug("Retry to %s failed (raw): %s", address, e3)
                writer.close()
                return None

    def close_peer(self, host: str, port: int) -> None:
        """Drain and close all pooled connections for one peer."""
        address = f"{host}:{port}"
        q = self._queues.pop(address, None)
        if q is None:
            return
        while not q.empty():
            try:
                _, writer = q.get_nowait()
                writer.close()
            except asyncio.QueueEmpty:
                break

    def close_all(self) -> None:
        for q in self._queues.values():
            while not q.empty():
                try:
                    _, writer = q.get_nowait()
                    writer.close()
                except asyncio.QueueEmpty:
                    break
        self._queues.clear()
