"""
Discovery backends for StorageNode.

Three plugins are provided:

- StaticDiscovery  – hardcoded peer list; ideal for local dev / tests.
- DNSDiscovery     – resolves SRV records; ideal for Kubernetes headless services.
- GossipDiscovery  – bootstraps from a single seed then learns peers through gossip;
                     the StorageNode keeps its internal peer set up-to-date by calling
                     add_peer / remove_peer as PEER_ANNOUNCE / PEER_LEAVE messages arrive.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from .types import NodeInfo


@runtime_checkable
class Discovery(Protocol):
    """Minimal interface every discovery backend must satisfy."""

    def get_peers(self) -> list[NodeInfo]:
        """Return the current list of known peer NodeInfos (excluding self)."""
        ...


class StaticDiscovery:
    """
    Fixed peer list – no network calls.

    Args:
        peers: List of address strings in ``"host:port"`` format.

    Example::

        discovery = StaticDiscovery(["127.0.0.1:7001", "127.0.0.1:7002"])
    """

    def __init__(self, peers: list[str]) -> None:
        self._peers: list[NodeInfo] = [NodeInfo.from_address(p) for p in peers]

    def get_peers(self) -> list[NodeInfo]:
        return list(self._peers)

    def __repr__(self) -> str:
        return f"StaticDiscovery(peers={[p.address for p in self._peers]})"


class DNSDiscovery:
    """
    Resolves peers via DNS SRV records – designed for Kubernetes headless services.

    Requires ``dnspython`` (``pip install dlstorage[dns]``).

    Args:
        service_name: DNS name to resolve, e.g. ``"_storage._tcp.my-svc.default.svc.cluster.local"``.

    Example::

        discovery = DNSDiscovery("_storage._tcp.dlstorage.default.svc.cluster.local")
    """

    def __init__(self, service_name: str) -> None:
        self.service_name = service_name

    def get_peers(self) -> list[NodeInfo]:
        try:
            import dns.resolver  # type: ignore[import]
        except ImportError as exc:
            raise ImportError(
                "dnspython is required for DNSDiscovery. "
                "Install it with: pip install dlstorage[dns]"
            ) from exc

        answers = dns.resolver.resolve(self.service_name, "SRV")
        return [NodeInfo(host=str(r.target).rstrip("."), port=r.port) for r in answers]

    def __repr__(self) -> str:
        return f"DNSDiscovery(service={self.service_name!r})"


class GossipDiscovery:
    """
    Bootstraps from a single seed node, then maintains peers through gossip.

    The StorageNode handles the async networking; this class is the mutable
    peer registry that the node reads from and writes to.

    Args:
        seed: Address of the bootstrap seed node in ``"host:port"`` format.

    Example::

        discovery = GossipDiscovery("192.168.1.10:7000")
    """

    def __init__(self, seed: str) -> None:
        self.seed: NodeInfo = NodeInfo.from_address(seed)
        self._peers: set[NodeInfo] = set()

    def get_peers(self) -> list[NodeInfo]:
        """Return current known peers (snapshot)."""
        return list(self._peers)

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
