import threading
import time
from typing import Any, Iterator


class LocalStore:
    """
    Simple thread-safe in-memory store.
    Stores any Python object. Supports optional TTL per key.
    """

    def __init__(self):
        self._data: dict[str, tuple[Any, float | None]] = (
            {}
        )  # key -> (value, expires_at)
        self._lock = threading.RLock()

    def set(self, key: str, value: Any, ttl: float | None = None) -> None:
        """Store a value. ttl is in seconds; None means no expiry."""
        expires_at = time.monotonic() + ttl if ttl is not None else None
        with self._lock:
            self._data[key] = (value, expires_at)

    def get(self, key: str) -> Any:
        """Return value or None if missing/expired."""
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return None
            value, expires_at = entry
            if expires_at is not None and time.monotonic() > expires_at:
                del self._data[key]
                return None
            return value

    def delete(self, key: str) -> bool:
        """Delete a key. Returns True if it existed."""
        with self._lock:
            return self._data.pop(key, None) is not None

    def exists(self, key: str) -> bool:
        return self.get(key) is not None

    def keys(self) -> list[str]:
        with self._lock:
            now = time.monotonic()
            return [k for k, (_, exp) in self._data.items() if exp is None or exp > now]

    def clear(self) -> None:
        with self._lock:
            self._data.clear()

    def purge_expired(self) -> int:
        """Remove all expired keys. Returns count removed."""
        with self._lock:
            now = time.monotonic()
            expired = [
                k
                for k, (_, exp) in self._data.items()
                if exp is not None and exp <= now
            ]
            for k in expired:
                del self._data[k]
            return len(expired)

    def __len__(self) -> int:
        return len(self.keys())

    def __repr__(self) -> str:
        return f"LocalStore(keys={len(self)})"
