from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class MessageType(str, Enum):
    # Gossip
    PING = "ping"
    PONG = "pong"
    PEER_LIST = "peer_list"
    PEER_ANNOUNCE = "peer_announce"
    PEER_LEAVE = "peer_leave"
    # Store
    GET = "get"
    SET = "set"
    DELETE = "delete"
    # Responses
    OK = "ok"
    NOT_FOUND = "not_found"
    ERROR = "error"

    def is_gossip(self) -> bool:
        return self in {
            MessageType.PING,
            MessageType.PONG,
            MessageType.PEER_LIST,
            MessageType.PEER_ANNOUNCE,
            MessageType.PEER_LEAVE,
        }


@dataclass
class NodeInfo:
    host: str
    port: int

    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}"

    def __hash__(self):
        return hash(self.address)

    def __eq__(self, other):
        return isinstance(other, NodeInfo) and self.address == other.address

    def __repr__(self):
        return f"NodeInfo({self.address})"

    def to_dict(self) -> dict:
        return {"host": self.host, "port": self.port}

    @classmethod
    def from_dict(cls, d: dict) -> "NodeInfo":
        return cls(host=d["host"], port=d["port"])

    @classmethod
    def from_address(cls, address: str) -> "NodeInfo":
        host, port = address.rsplit(":", 1)
        return cls(host=host, port=int(port))


@dataclass
class Message:
    type: MessageType
    payload: Any = None

    def to_dict(self) -> dict:
        return {"type": self.type.value, "payload": self.payload}

    @classmethod
    def from_dict(cls, d: dict) -> "Message":
        return cls(type=MessageType(d["type"]), payload=d.get("payload"))
