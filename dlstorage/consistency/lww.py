"""
Last-Write-Wins (LWW) consistency policy.

Picks the replica result with the highest timestamp.
"""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from dlstorage.node.interface import AsyncReplicaHandle, ReplicaHandle

from .interface import AsyncMergeResolver, MergeResolver

_thread_pool = ThreadPoolExecutor(max_workers=4)


class LWW(MergeResolver):
    """
    Return the value with the highest write timestamp.
    LWW = Last-Write-Wins.

    The problem with LWW is that it requires all replicas to respond before it can return a value, which can lead to higher read latency and more load on the replicas.

    However, it has the advantage of always returning the most up-to-date value, as long as the timestamps are properly synchronized across replicas.
    """

    def read_resolve(self, key: str, replica_handles: list[ReplicaHandle]) -> Any:
        # Threadpool doesnt help much here
        # def fetch(h: ReplicaHandle):
        #     try:
        #         return h.get_versioned(key)
        #     except Exception:
        #         return None
        # results = list(_thread_pool.map(fetch, replica_handles))

        results = []
        for h in replica_handles:
            try:
                r = h.get_versioned(key)
            except Exception:
                r = None
            results.append(r)

        best_value, best_ts, best_tombstone = None, -1, False
        for r in results:
            if r is None:
                continue
            value, ts, is_tombstone = r
            if ts > best_ts:
                best_ts, best_value, best_tombstone = ts, value, is_tombstone

        return None if (best_ts < 0 or best_tombstone) else best_value


class AsyncLWW(AsyncMergeResolver):
    """
    Async version of LWW.
    """

    async def read_resolve(
        self, key: str, replica_handles: list[AsyncReplicaHandle]
    ) -> Any:
        async def get_versioned(h: AsyncReplicaHandle):
            try:
                return await h.get_versioned(key)
            except Exception:
                return None

        results = await asyncio.gather(
            *[get_versioned(h) for h in replica_handles],
            return_exceptions=True,
        )

        best_value, best_ts, best_tombstone = None, -1, False
        for r in results:
            if isinstance(r, BaseException) or r is None:
                continue
            value, ts, is_tombstone = r
            if ts > best_ts:
                best_ts, best_value, best_tombstone = ts, value, is_tombstone

        return None if (best_ts < 0 or best_tombstone) else best_value
