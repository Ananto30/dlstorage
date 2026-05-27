"""
Merge policy protocol for dlstorage.

A ``MergePolicy`` is responsible for resolving conflicts between different
replica versions.  It decides *which* value should be returned after
collecting per-replica versioned results.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from dlstorage.node.interface import AsyncReplicaHandle, ReplicaHandle


@runtime_checkable
class MergeResolver(Protocol):
    """
    Conflict-resolution strategy for storage reads.

    Writes are always fanned out to all replicas, so ignoring write_resolve.
    """

    def read_resolve(self, key: str, replica_handles: list[ReplicaHandle]) -> Any:
        """
        Pick the winning value from versioned read results.

        Each element is either ``(value, ts_ns, is_tombstone)`` or ``None``
        (replica unreachable / key absent).
        """
        ...


@runtime_checkable
class AsyncMergeResolver(Protocol):
    """
    Async version of MergeResolver.
    """

    async def read_resolve(
        self, key: str, replica_handles: list[AsyncReplicaHandle]
    ) -> Any:
        """
        Pick the winning value from versioned read results.

        Each element is either ``(value, ts_ns, is_tombstone)`` or ``None``
        (replica unreachable / key absent).
        """
        ...
