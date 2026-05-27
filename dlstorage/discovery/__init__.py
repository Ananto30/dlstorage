from .dns import DNSDiscovery
from .gossip import GossipDiscovery
from .interface import Discovery
from .static import StaticDiscovery

__all__ = [
    "Discovery",
    "StaticDiscovery",
    "DNSDiscovery",
    "GossipDiscovery",
]
