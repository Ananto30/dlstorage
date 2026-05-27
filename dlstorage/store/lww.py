import threading
import time
from typing import Any

from dlstorage.store.interface import VersionedStore

# Sentinel stored as the value of a tombstone entry.  Using object identity
# means no user value can accidentally collide with it.
_TOMBSTONE = object()


class LocalLWWStore(VersionedStore):
    """
    Thread-safe in-memory store with Last-Write-Wins (LWW) conflict resolution.

    Every entry carries a nanosecond wall-clock timestamp (``time.time_ns()``).
    A write is silently rejected when the stored timestamp is already newer,
    so a stale replica that missed some writes cannot overwrite fresher data
    once it reconnects.  Deletes are also timestamped (tombstone pattern) to
    prevent a late-arriving write with an older timestamp from resurrecting a
    key that was deleted after the writing node went offline.
    """

    def __init__(self):
        # key -> (value_or_TOMBSTONE, expires_at, ts_ns)
        self._data: dict[str, tuple[Any, float | None, int]] = {}
        self._lock = threading.RLock()

    def set(
        self, key: str, value: Any, ttl: float | None = None, **kwargs: Any
    ) -> bool:
        """Returns True if stored, False if rejected (stale ts)."""
        ts = kwargs.get("ts")
        if ts is None:
            ts = time.time_ns()

        expires_at = time.monotonic() + ttl if ttl is not None else None

        with self._lock:
            existing = self._data.get(key)
            if existing is not None and existing[2] > ts:
                return False  # newer write already present, so discard

            self._data[key] = (value, expires_at, ts)
            return True

    def get(self, key: str) -> Any:
        """Return value or None if missing/expired/tombstoned."""
        result = self.get_versioned(key)
        if result is None:
            return None

        _value, _ts, is_tombstone = result
        return None if is_tombstone else _value

    def get_versioned(self, key: str) -> tuple[Any, int, bool] | None:
        """
        Return ``(value, ts_ns, is_tombstone)`` or ``None`` if the key has no entry.

        Tombstone entries have ``is_tombstone=True`` and ``value=None``.
        They are returned (rather than hidden) so callers can participate in
        LWW conflict resolution when merging results from multiple replicas.
        """
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return None

            value, expires_at, ts = entry
            if value is _TOMBSTONE:
                return (None, ts, True)

            if expires_at is not None and time.monotonic() > expires_at:
                del self._data[key]
                return None

            return (value, ts, False)

    def delete(self, key: str, **kwargs: Any) -> bool:
        """
        Stores a tombstone so older writes cannot resurrect the key.

        Returns True if a live (non-tombstone) entry existed.
        Tombstones have no TTL and persist until the node restarts.
        """
        ts = kwargs.get("ts")
        if ts is None:
            ts = time.time_ns()

        with self._lock:
            existing = self._data.get(key)
            lived = existing is not None and existing[0] is not _TOMBSTONE
            if existing is None or existing[2] <= ts:
                self._data[key] = (_TOMBSTONE, None, ts)
            return lived

    def exists(self, key: str) -> bool:
        return self.get(key) is not None

    def keys(self) -> list[str]:
        with self._lock:
            now = time.monotonic()
            return [
                k
                for k, (v, exp, _ts) in self._data.items()
                if v is not _TOMBSTONE and (exp is None or exp > now)
            ]

    def scan(self, pattern: str) -> list[tuple[str, Any]]:
        """Return (key, value) pairs for all live, non-expired keys containing *pattern*."""
        with self._lock:
            now = time.monotonic()
            return [
                (k, v)
                for k, (v, exp, _ts) in self._data.items()
                if v is not _TOMBSTONE and pattern in k and (exp is None or exp > now)
            ]

    def scan_keys(self, pattern: str) -> list[str]:
        """Return keys matching *pattern*."""
        with self._lock:
            now = time.monotonic()
            return [
                k
                for k, (v, exp, _ts) in self._data.items()
                if v is not _TOMBSTONE and pattern in k and (exp is None or exp > now)
            ]

    def flush_all(self) -> None:
        """Delete all keys."""
        with self._lock:
            self._data.clear()

    def flush_keys(self, pattern: str) -> int:
        """Delete keys matching *pattern*. Returns count deleted."""
        with self._lock:
            to_delete = [k for k in self._data if pattern in k]
            for k in to_delete:
                del self._data[k]
            return len(to_delete)

    def clear(self) -> None:
        with self._lock:
            self._data.clear()

    def purge_expired(self) -> int:
        """Remove all expired non-tombstone keys. Returns count removed."""
        with self._lock:
            now = time.monotonic()
            expired = [
                k
                for k, (v, exp, _ts) in self._data.items()
                if v is not _TOMBSTONE and exp is not None and exp <= now
            ]
            for k in expired:
                del self._data[k]
            return len(expired)

    def __len__(self) -> int:
        return len(self.keys())

    def __repr__(self) -> str:
        return f"LocalStore(keys={len(self)})"
