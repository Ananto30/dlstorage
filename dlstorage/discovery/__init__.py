from .a_record import ARecordDiscovery
from .dns import DNSDiscovery
from .gossip import GossipDiscovery
from .interface import Discovery
from .static import StaticDiscovery

__all__ = [
    "Discovery",
    "ARecordDiscovery",
    "StaticDiscovery",
    "DNSDiscovery",
    "GossipDiscovery",
]
