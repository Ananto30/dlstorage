import asyncio
import logging
import pickle

from .types import Message

logger = logging.getLogger(__name__)

HEADER_SIZE = 4  # 4-byte big-endian length prefix

# Wire format: pickle is used so that Message payloads can carry arbitrary
# Python objects (e.g. any value stored in the LocalStore).
# All nodes in the cluster are trusted; do NOT expose the TCP port publicly.


async def send_message(writer: asyncio.StreamWriter, msg: Message) -> None:
    data = pickle.dumps(msg, protocol=pickle.HIGHEST_PROTOCOL)
    header = len(data).to_bytes(HEADER_SIZE, "big")
    writer.write(header + data)
    await writer.drain()


async def recv_message(reader: asyncio.StreamReader) -> Message | None:
    header = await reader.readexactly(HEADER_SIZE)
    length = int.from_bytes(header, "big")
    data = await reader.readexactly(length)
    return pickle.loads(data)  # noqa: S301 – trusted internal network only


class ConnectionPool:
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
        while not q.empty():
            try:
                r, w = q.get_nowait()
            except asyncio.QueueEmpty:
                break
            if not w.is_closing():
                reader, writer = r, w
                break
            # stale connection – discard and try next
        if writer is None:
            try:
                reader, writer = await self._new_connection(host, port)
            except Exception as e:
                logger.debug("Cannot connect to %s: %s", address, e)
                return None
        try:
            await send_message(writer, msg)
            response = await asyncio.wait_for(recv_message(reader), timeout=5.0)
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
            return None

    async def close_all(self) -> None:
        for q in self._queues.values():
            while not q.empty():
                try:
                    _, writer = q.get_nowait()
                    writer.close()
                except asyncio.QueueEmpty:
                    break
        self._queues.clear()
