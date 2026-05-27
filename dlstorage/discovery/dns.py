from __future__ import annotations

from dlstorage.discovery.interface import Discovery
from dlstorage.types import NodeInfo


class DNSDiscovery(Discovery):
    """
    Resolves peers via DNS SRV records – designed for Kubernetes headless services.

    Requires ``dnspython`` (``pip install dlstorage[dns]``).

    Args:
        service_name: DNS name to resolve,
            e.g. ``"_storage._tcp.my-svc.default.svc.cluster.local"``.
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
