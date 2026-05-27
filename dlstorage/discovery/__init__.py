from .interface import Discovery
from .static import StaticDiscovery
from .dns import DNSDiscovery
from .gossip import GossipDiscovery

__all__ = ["Discovery", "StaticDiscovery", "DNSDiscovery", "GossipDiscovery"]
