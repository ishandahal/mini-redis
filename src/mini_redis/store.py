"""In-memory byte-string storage with lazy and bounded active expiration."""

from __future__ import annotations

import math
from collections import OrderedDict
from dataclasses import dataclass

from mini_redis.clock import Clock, SystemClock

# Bound arithmetic and document a deliberately smaller subset than Redis.
MAX_TTL_SECONDS = 2**31 - 1


@dataclass
class Entry:
    value: bytes
    deadline: float | None = None
    expires_at: float | None = None


class Store:
    """Own entries; callers serialize access through Service."""

    def __init__(self, clock: Clock | None = None) -> None:
        self._clock = clock if clock is not None else SystemClock()
        # Rotation lets cleanup inspect bounded batches without full snapshots
        # or an iterator invalidated by concurrent commands between batches.
        self._entries: OrderedDict[bytes, Entry] = OrderedDict()

    def set(
        self,
        key: bytes,
        value: bytes,
        seconds: int | None = None,
        *,
        keep_ttl: bool = False,
    ) -> None:
        if seconds is not None:
            self._validate_seconds(seconds)
            entry = Entry(
                value,
                self._clock.monotonic() + seconds,
                self._clock.wall_time() + seconds,
            )
        elif keep_ttl and key in self._entries:
            # Used only after a live read within the same service operation.
            old = self._entries[key]
            entry = Entry(value, old.deadline, old.expires_at)
        else:
            entry = Entry(value)
        self._entries[key] = entry

    def _live_entry(self, key: bytes, now: float) -> Entry | None:
        entry = self._entries.get(key)
        if entry is not None and entry.deadline is not None and now >= entry.deadline:
            del self._entries[key]
            return None
        return entry

    def get(self, key: bytes) -> bytes | None:
        entry = self._live_entry(key, self._clock.monotonic())
        return None if entry is None else entry.value

    def delete(self, keys: tuple[bytes, ...]) -> int:
        now = self._clock.monotonic()
        removed = 0
        for key in keys:
            if self._live_entry(key, now) is not None:
                del self._entries[key]
                removed += 1
        return removed

    def exists(self, keys: tuple[bytes, ...]) -> int:
        now = self._clock.monotonic()
        return sum(self._live_entry(key, now) is not None for key in keys)

    def expire(self, key: bytes, seconds: int) -> int:
        self._validate_seconds(seconds)
        now = self._clock.monotonic()
        entry = self._live_entry(key, now)
        if entry is None:
            return 0
        entry.deadline = now + seconds
        entry.expires_at = self._clock.wall_time() + seconds
        return 1

    def ttl(self, key: bytes) -> int:
        now = self._clock.monotonic()
        entry = self._live_entry(key, now)
        if entry is None:
            return -2
        if entry.deadline is None:
            return -1
        return math.ceil(entry.deadline - now)

    def cleanup(self, batch_size: int = 100) -> int:
        """Inspect at most batch_size entries, rotating survivors fairly."""
        if batch_size <= 0:
            raise ValueError("cleanup batch size must be positive")
        now = self._clock.monotonic()
        removed = 0
        for _ in range(min(batch_size, len(self._entries))):
            key, entry = self._entries.popitem(last=False)
            if entry.deadline is not None and now >= entry.deadline:
                removed += 1
            else:
                self._entries[key] = entry
        return removed

    @staticmethod
    def _validate_seconds(seconds: int) -> None:
        if not 1 <= seconds <= MAX_TTL_SECONDS:
            raise ValueError("expiration must be between 1 and 2147483647 seconds")
