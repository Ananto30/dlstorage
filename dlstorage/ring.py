import hashlib
from typing import Sequence
from .types import NodeInfo


def _score(node: NodeInfo, key: str) -> int:
    """HRW (Highest Random Weight) score for a node/key pair."""
    h = hashlib.sha256(f"{node.address}:{key}".encode()).digest()
    return int.from_bytes(h, "big")


class RendezvousRing:
    """
    Rendezvous (HRW) hashing.

    Simpler than consistent hashing:
    - No virtual nodes needed
    - Minimal key remapping when nodes join/leave
    - Each key always maps to the node with the highest hash(node+key) score

    Usage:
        ring = RendezvousRing()
        ring.add(NodeInfo("127.0.0.1", 7000))
        ring.add(NodeInfo("127.0.0.1", 7001))
        node = ring.get_node("my-key")
        replicas = ring.get_nodes("my-key", n=2)
    """

    def __init__(self):
        self._nodes: set[NodeInfo] = set()

    def add(self, node: NodeInfo) -> None:
        self._nodes.add(node)

    def remove(self, node: NodeInfo) -> None:
        self._nodes.discard(node)

    def get_node(self, key: str) -> NodeInfo | None:
        """Return the primary node responsible for this key."""
        if not self._nodes:
            return None
        return max(self._nodes, key=lambda n: _score(n, key))

    def get_nodes(self, key: str, n: int = 1) -> list[NodeInfo]:
        """Return top-n nodes for replication."""
        if not self._nodes:
            return []
        ranked = sorted(self._nodes, key=lambda node: _score(node, key), reverse=True)
        return ranked[:n]

    def nodes(self) -> list[NodeInfo]:
        return list(self._nodes)

    def __len__(self) -> int:
        return len(self._nodes)

    def __repr__(self) -> str:
        return f"RendezvousRing(nodes={len(self._nodes)})"