import asyncio
import logging
import random
import time

from dlstorage.connection_pool.interface import (AsyncConnectionPool,
                                                 ConnectionPool)
from dlstorage.discovery.gossip import GossipDiscovery
from dlstorage.discovery.interface import Discovery
from dlstorage.ring.interface import Ring
from dlstorage.types import Message, MessageType, NodeInfo

logger = logging.getLogger(__name__)


_GOSSIP_INTERVAL = 5.0  # seconds between gossip rounds
_GOSSIP_FANOUT = 3  # peers to gossip with per round


class Gossip:
    """Gossip protocol implementation for disseminating updates across the cluster."""

    def __init__(
        self,
        node_info: NodeInfo,
        ring: Ring,
        pool: ConnectionPool,
        discovery: Discovery,
    ) -> None:
        self.info = node_info
        self._ring = ring
        self._pool = pool
        self._discovery = discovery

    def announce_join(self) -> None:
        self._broadcast(
            Message(
                MessageType.PEER_ANNOUNCE,
                {"peer": self.info.to_dict()},
            )
        )

    def announce_leave(self) -> None:
        self._broadcast(
            Message(
                MessageType.PEER_LEAVE,
                {"peer": self.info.to_dict()},
            )
        )

    def dispatch(self, msg: Message) -> Message:
        return handle_message(self, msg)

    def sync_peers(self) -> None:
        if not isinstance(self._discovery, GossipDiscovery):
            return  # Only gossip discovery needs peer syncing

        seed = self._discovery.seed
        resp = self._pool.execute(
            seed.host,
            seed.port,
            Message(MessageType.PEER_LIST, {"requester": self.info.to_dict()}),
        )
        if resp and resp.type == MessageType.PEER_LIST:
            for raw in resp.payload.get("peers", []):
                peer = NodeInfo.from_dict(raw)
                self._discovery.add_peer(peer)
                self._ring.add(peer)
        self._discovery.add_peer(seed)
        self._ring.add(seed)

    def gossip_loop(self) -> None:
        """Periodically exchange peer lists and evict unresponsive nodes."""
        while True:
            try:
                time.sleep(_GOSSIP_INTERVAL)
                peers = [n for n in self._ring.nodes() if n != self.info]
                if not peers:
                    continue
                sample = random.sample(peers, min(_GOSSIP_FANOUT, len(peers)))
                for peer in sample:
                    try:
                        resp = self._pool.execute(
                            peer.host,
                            peer.port,
                            Message(
                                MessageType.PEER_LIST,
                                {"requester": self.info.to_dict()},
                            ),
                        )
                        if resp is None:
                            self._evict(peer)
                            continue
                        if resp.type == MessageType.PEER_LIST:
                            known = set(self._ring.nodes())
                            for raw in resp.payload.get("peers", []):
                                new_peer = NodeInfo.from_dict(raw)
                                if new_peer != self.info and new_peer not in known:
                                    self._ring.add(new_peer)
                                    if isinstance(self._discovery, GossipDiscovery):
                                        self._discovery.add_peer(new_peer)
                                    logger.debug(
                                        "Discovered new peer via gossip: %s", new_peer
                                    )
                    except Exception as exc:
                        logger.debug("Gossip error with %s: %s", peer, exc)
                        self._evict(peer)
            except Exception as exc:
                logger.debug("Gossip round error: %s", exc)

    def _evict(self, peer: NodeInfo) -> None:
        logger.debug("Peer unresponsive, evicting: %s", peer)
        self._ring.remove(peer)
        if isinstance(self._discovery, GossipDiscovery):
            self._discovery.remove_peer(peer)
        self._pool.close_peer(peer.host, peer.port)

    def _broadcast(self, msg: Message) -> None:
        for peer in self._ring.nodes():
            if peer != self.info:
                self._pool.execute(peer.host, peer.port, msg)


class AsyncGossip:
    """Async gossip protocol — mirrors Gossip but uses asyncio throughout."""

    def __init__(
        self,
        node_info: NodeInfo,
        ring: Ring,
        pool: AsyncConnectionPool,
        discovery: Discovery,
    ) -> None:
        self.info = node_info
        self._ring = ring
        self._pool = pool
        self._discovery = discovery

    async def announce_join(self) -> None:
        await self._broadcast(
            Message(MessageType.PEER_ANNOUNCE, {"peer": self.info.to_dict()})
        )

    async def announce_leave(self) -> None:
        await self._broadcast(
            Message(MessageType.PEER_LEAVE, {"peer": self.info.to_dict()})
        )

    async def sync_peers(self) -> None:
        if not isinstance(self._discovery, GossipDiscovery):
            return
        seed = self._discovery.seed
        resp = await self._pool.execute(
            seed.host,
            seed.port,
            Message(MessageType.PEER_LIST, {"requester": self.info.to_dict()}),
        )
        if resp and resp.type == MessageType.PEER_LIST:
            for raw in resp.payload.get("peers", []):
                peer = NodeInfo.from_dict(raw)
                self._discovery.add_peer(peer)
                self._ring.add(peer)
        self._discovery.add_peer(seed)
        self._ring.add(seed)

    def dispatch(self, msg: Message) -> Message:
        return handle_message(self, msg)

    async def gossip_loop(self) -> None:
        """Periodically exchange peer lists and evict unresponsive nodes."""
        while True:
            try:
                await asyncio.sleep(_GOSSIP_INTERVAL)
                peers = [n for n in self._ring.nodes() if n != self.info]
                if not peers:
                    continue
                sample = random.sample(peers, min(_GOSSIP_FANOUT, len(peers)))
                await asyncio.gather(
                    *[self._gossip_with(p) for p in sample],
                    return_exceptions=True,
                )
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.debug("Gossip round error: %s", exc)

    async def _gossip_with(self, peer: NodeInfo) -> None:
        try:
            resp = await self._pool.execute(
                peer.host,
                peer.port,
                Message(MessageType.PEER_LIST, {"requester": self.info.to_dict()}),
            )
            if resp is None:
                self._evict(peer)
                return
            if resp.type == MessageType.PEER_LIST:
                known = set(self._ring.nodes())
                for raw in resp.payload.get("peers", []):
                    new_peer = NodeInfo.from_dict(raw)
                    if new_peer != self.info and new_peer not in known:
                        self._ring.add(new_peer)
                        if isinstance(self._discovery, GossipDiscovery):
                            self._discovery.add_peer(new_peer)
                        logger.debug("Discovered new peer via gossip: %s", new_peer)
        except Exception as exc:
            logger.debug("Gossip error with %s: %s", peer, exc)
            self._evict(peer)

    def _evict(self, peer: NodeInfo) -> None:
        logger.debug("Peer unresponsive, evicting: %s", peer)
        self._ring.remove(peer)
        if isinstance(self._discovery, GossipDiscovery):
            self._discovery.remove_peer(peer)
        self._pool.close_peer(peer.host, peer.port)

    async def _broadcast(self, msg: Message) -> None:
        peers = [n for n in self._ring.nodes() if n != self.info]
        await asyncio.gather(
            *[self._pool.execute(p.host, p.port, msg) for p in peers],
            return_exceptions=True,
        )


def handle_message(self, msg: Message) -> Message:
    match msg.type:
        case MessageType.PING:
            return Message(MessageType.PONG, {"node": self.info.to_dict()})

        case MessageType.PEER_LIST:
            peers = [n.to_dict() for n in self._ring.nodes() if n != self.info]
            return Message(MessageType.PEER_LIST, {"peers": peers})

        case MessageType.PEER_ANNOUNCE:
            peer = NodeInfo.from_dict(msg.payload["peer"])
            if peer not in self._ring.nodes():
                self._ring.add(peer)
                if isinstance(self._discovery, GossipDiscovery):
                    self._discovery.add_peer(peer)
                logger.debug("Peer joined: %s", peer)
            return Message(MessageType.OK, {})

        case MessageType.PEER_LEAVE:
            peer = NodeInfo.from_dict(msg.payload["peer"])
            self._ring.remove(peer)
            if isinstance(self._discovery, GossipDiscovery):
                self._discovery.remove_peer(peer)
            self._pool.close_peer(peer.host, peer.port)
            logger.debug("Peer left: %s", peer)
            return Message(MessageType.OK, {})

        case _:
            return Message(
                MessageType.ERROR,
                {"error": f"unknown message type: {msg.type}"},
            )
