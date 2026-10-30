"""Atomic operations on the shared keyspace; no socket I/O belongs here."""

from __future__ import annotations

import re
import threading

from mini_redis.store import Store

MIN_INTEGER = -(2**63)
MAX_INTEGER = 2**63 - 1
INTEGER_PATTERN = re.compile(rb"(?:0|-?[1-9][0-9]*)")


class InvalidInteger(ValueError):
    """An increment cannot be applied without changing the value's meaning."""


class Service:
    """Serialize whole operations, including multi-key reads and mutations.

    The service owns its store. Callers must not access that store directly
    after passing it here, or they would bypass the atomicity boundary.
    """

    def __init__(self, store: Store | None = None) -> None:
        self._store = store if store is not None else Store()
        self._lock = threading.Lock()

    def set(self, key: bytes, value: bytes, seconds: int | None = None) -> None:
        with self._lock:
            self._store.set(key, value, seconds)

    def get(self, key: bytes) -> bytes | None:
        with self._lock:
            return self._store.get(key)

    def delete(self, keys: tuple[bytes, ...]) -> int:
        with self._lock:
            return self._store.delete(keys)

    def exists(self, keys: tuple[bytes, ...]) -> int:
        with self._lock:
            return self._store.exists(keys)

    def increment(self, key: bytes) -> int:
        with self._lock:
            raw = self._store.get(key)
            value = 0
            if raw is not None:
                # Bound conversion before int(), including Python's digit limit.
                if len(raw) > 20 or INTEGER_PATTERN.fullmatch(raw) is None:
                    raise InvalidInteger("value is not an integer or out of range")
                value = int(raw)
                if not MIN_INTEGER <= value <= MAX_INTEGER:
                    raise InvalidInteger("value is not an integer or out of range")
            if value == MAX_INTEGER:
                raise InvalidInteger("increment would overflow")
            value += 1
            self._store.set(key, str(value).encode("ascii"), keep_ttl=True)
            return value

    def expire(self, key: bytes, seconds: int) -> int:
        with self._lock:
            return self._store.expire(key, seconds)

    def ttl(self, key: bytes) -> int:
        with self._lock:
            return self._store.ttl(key)

    def cleanup(self, batch_size: int = 100) -> int:
        with self._lock:
            return self._store.cleanup(batch_size)
