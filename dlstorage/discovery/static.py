from __future__ import annotations

from dlstorage.discovery.interface import Discovery
from dlstorage.types import NodeInfo


class StaticDiscovery(Discovery):
    """
    Fixed peer list – no network calls.

    Args:
        peers: List of address strings in ``"host:port"`` format.
    """

    def __init__(self, peers: list[str]) -> None:
        self._peers: list[NodeInfo] = [NodeInfo.from_address(p) for p in peers]

    def get_peers(self) -> list[NodeInfo]:
        return list(self._peers)

    def __repr__(self) -> str:
        return f"StaticDiscovery(peers={[p.address for p in self._peers]})"
