from __future__ import annotations

import logging
import socket

from dlstorage.discovery.interface import Discovery
from dlstorage.types import NodeInfo

logger = logging.getLogger(__name__)


class ARecordDiscovery(Discovery):
    """
    Resolves peers via DNS A records – works with Docker Compose headless
    service names, plain hostnames, or any DNS that returns multiple A records.

    Args:
        hostname: DNS name to resolve, e.g. ``"storage"`` in Docker Compose.
        port: Storage port every peer listens on.
    """

    def __init__(self, hostname: str, port: int) -> None:
        self.hostname = hostname
        self.port = port

    def get_peers(self) -> list[NodeInfo]:
        try:
            results = socket.getaddrinfo(
                self.hostname, self.port, proto=socket.IPPROTO_TCP
            )
        except socket.gaierror as exc:
            raise RuntimeError(
                f"ARecordDiscovery: failed to resolve {self.hostname!r}: {exc}"
            ) from exc

        seen: set[str] = set()
        peers: list[NodeInfo] = []
        for _family, _type, _proto, _canonname, sockaddr in results:
            ip = sockaddr[0]
            if ip not in seen:
                seen.add(str(ip))
                peers.append(NodeInfo(host=str(ip), port=self.port))

        return peers

    def __repr__(self) -> str:
        return f"ARecordDiscovery(hostname={self.hostname!r}, port={self.port})"
