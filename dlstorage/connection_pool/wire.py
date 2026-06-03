import asyncio
import pickle
import socket

import msgspec.msgpack

from dlstorage.types import Message

HEADER_SIZE = 4  # 4-byte big-endian length prefix

# Wire format: msgspec msgpack with a pickle fallback enc_hook for arbitrary
# Python objects stored as values.  All nodes in the cluster are trusted;
# do NOT expose the TCP port publicly.
#
# Encoding strategy: msgspec encodes Message natively (it is a dataclass).
# Payload values that are not msgpack-native (e.g. custom Python objects) are
# serialised to bytes by the enc_hook and arrive on the wire as raw bytes.
# The dec_hook re-hydrates those bytes via pickle.
#
# Thread safety: we use the module-level msgspec.msgpack.encode/decode
# functions (not Encoder/Decoder instances) because the instance API reuses
# an internal buffer and is not thread-safe.  The functional API is re-entrant.


def _enc_hook(obj: object) -> bytes:
    """Fallback encoder: pickle non-msgpack-native objects to bytes."""
    return pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)


def _dec_hook(type: type, obj: object) -> object:
    """Fallback decoder: unpickle bytes back to the original object."""
    if type is object and isinstance(obj, bytes):
        try:
            return pickle.loads(obj)  # noqa: S301 — trusted internal cluster
        except Exception:
            return obj
    raise TypeError(f"unexpected type {type}")


def encode_message(msg: Message) -> bytes:
    """Serialise a Message to msgpack bytes (public, for pre-encoding fan-out)."""
    return msgspec.msgpack.encode(msg, enc_hook=_enc_hook)


def decode_message(data: bytes) -> Message:
    """Deserialise msgpack bytes back to a Message."""
    return msgspec.msgpack.decode(data, type=Message, dec_hook=_dec_hook)


def _decode(data: bytes) -> Message:
    return decode_message(data)


def send_message(sock: socket.socket, msg: Message) -> None:
    data = encode_message(msg)
    header = len(data).to_bytes(HEADER_SIZE, "big")
    sock.sendall(header + data)


async def send_message_async(writer: asyncio.StreamWriter, msg: Message) -> None:
    data = encode_message(msg)
    header = len(data).to_bytes(HEADER_SIZE, "big")
    writer.write(header + data)
    # No drain() — let the kernel batch writes; backpressure is handled by the
    # pool's response read which naturally throttles the send rate.


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
    return _decode(data)


async def recv_message_async(reader: asyncio.StreamReader) -> Message | None:
    header = await reader.readexactly(HEADER_SIZE)
    length = int.from_bytes(header, "big")
    data = await reader.readexactly(length)
    return _decode(data)
