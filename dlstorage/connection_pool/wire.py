import asyncio
import pickle
import socket

from dlstorage.types import Message

HEADER_SIZE = 4  # 4-byte big-endian length prefix

# Wire format: pickle is used so that Message payloads can carry arbitrary
# Python objects (e.g. any value stored in the LocalStore).
# All nodes in the cluster are trusted; do NOT expose the TCP port publicly.


def send_message(sock: socket.socket, msg: Message) -> None:
    data = pickle.dumps(msg, protocol=pickle.HIGHEST_PROTOCOL)
    header = len(data).to_bytes(HEADER_SIZE, "big")
    sock.sendall(header + data)


async def send_message_async(writer: asyncio.StreamWriter, msg: Message) -> None:
    data = pickle.dumps(msg, protocol=pickle.HIGHEST_PROTOCOL)
    header = len(data).to_bytes(HEADER_SIZE, "big")
    writer.write(header + data)
    await writer.drain()


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


def recv_message(sock: socket.socket) -> Message:
    header = _recvexactly(sock, HEADER_SIZE)
    length = int.from_bytes(header, "big")
    data = _recvexactly(sock, length)
    return pickle.loads(data)


async def recv_message_async(reader: asyncio.StreamReader) -> Message | None:
    header = await reader.readexactly(HEADER_SIZE)
    length = int.from_bytes(header, "big")
    data = await reader.readexactly(length)
    return pickle.loads(data)
