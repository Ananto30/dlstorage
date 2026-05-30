from __future__ import annotations

from dlstorage.discovery.interface import Discovery
from dlstorage.types import NodeInfo


class GossipDiscovery(Discovery):
    """
    Bootstraps from a single seed node, then maintains peers through gossip.
    Seed node is expected to be stable and well-known, but other nodes may come and go.

    Nodes exchange peer lists on a regular interval, nodes announce their join, so new nodes are discovered quickly and failed nodes are removed after a short timeout.

    Args:
        seed: Address of the bootstrap seed node in ``"host:port"`` format.
    """

    def __init__(self, seed: str) -> None:
        self.seed: NodeInfo = NodeInfo.from_address(seed)
        self._peers: set[NodeInfo] = set()

    def get_peers(self) -> list[NodeInfo]:
        return list(self._peers) + [self.seed]

    def add_peer(self, peer: NodeInfo) -> bool:
        """Add a peer. Returns True if it was new."""
        if peer not in self._peers:
            self._peers.add(peer)
            return True
        return False

    def remove_peer(self, peer: NodeInfo) -> None:
        """Remove a peer (no-op if unknown)."""
        self._peers.discard(peer)

    def __repr__(self) -> str:
        return f"GossipDiscovery(seed={self.seed.address!r}, peers={len(self._peers)})"
