import logging

from dlstorage.connection_pool.interface import (AsyncConnectionPool,
                                                 ConnectionPool)
from dlstorage.discovery.gossip import GossipDiscovery
from dlstorage.discovery.interface import Discovery
from dlstorage.ring.interface import Ring
from dlstorage.store.interface import VersionedStore
from dlstorage.types import Message, MessageType, NodeInfo

logger = logging.getLogger(__name__)


def dispatch(msg: Message, store: VersionedStore) -> Message:
    match msg.type:
        # Gossip messages are handled by peer_comm module

        # Store
        case MessageType.GET:
            key = msg.payload["key"]
            entry = store.get_versioned(key)
            if entry is None:
                return Message(MessageType.NOT_FOUND, {"key": key, "ts": 0})
            value, ts, is_tombstone = entry
            if is_tombstone:
                return Message(
                    MessageType.NOT_FOUND, {"key": key, "ts": ts, "tombstone": True}
                )
            return Message(MessageType.OK, {"key": key, "value": value, "ts": ts})

        case MessageType.SET:
            key = msg.payload["key"]
            value = msg.payload["value"]
            ttl = msg.payload.get("ttl")
            ts = msg.payload.get("ts")
            store.set(key, value, ttl, ts=ts)
            return Message(MessageType.OK, {"key": key})

        case MessageType.DELETE:
            key = msg.payload["key"]
            ts = msg.payload.get("ts")
            deleted = store.delete(key, ts=ts)
            if deleted:
                return Message(MessageType.OK, {"key": key})
            return Message(MessageType.NOT_FOUND, {"key": key})

        case _:
            return Message(
                MessageType.ERROR,
                {"error": f"unknown message type: {msg.type}"},
            )
