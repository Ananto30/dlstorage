"""
First-Hit-Wins (FHW) consistency policy.

Returns the first non-tombstone result in ring-preference order.
"""

from __future__ import annotations

from typing import Any

from dlstorage.node.interface import AsyncReplicaHandle, ReplicaHandle

from .interface import AsyncMergeResolver, MergeResolver


class FHW(MergeResolver):
    """
    Return the first non-tombstone value in ring-preference order.
    FHW = First-Hit-Wins.

    The problem with FHW is that it can return stale data, because it doesn't consider timestamps at all.  It just takes the first value it gets back, which might not be the most recent one.

    However, it has the advantage of being very fast and putting minimal load on the replicas, since it can return as soon as it gets a single response.
    """

    def read_resolve(self, key: str, replica_handles: list[ReplicaHandle]) -> Any:
        for h in replica_handles:
            try:
                r = h.get_versioned(key)
                if r is None:
                    continue
                value, _, is_tombstone = r
                if not is_tombstone:
                    return value
            except Exception:
                continue

        return None


class AsyncFHW(AsyncMergeResolver):
    """
    Async version of FHW.
    """

    async def read_resolve(
        self, key: str, replica_handles: list[AsyncReplicaHandle]
    ) -> Any:
        for h in replica_handles:
            try:
                r = await h.get_versioned(key)
                if r is None:
                    continue
                value, _, is_tombstone = r
                if not is_tombstone:
                    return value
            except Exception:
                continue

        return None
