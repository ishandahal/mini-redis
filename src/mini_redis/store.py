"""In-memory byte-string storage for Mini Redis."""

from __future__ import annotations


class Store:
    """Own the server's key/value state.

    Keys and values are opaque bytes.  Networking and command parsing belong to
    higher layers; this class only implements storage operations.
    """

    def __init__(self) -> None:
        self._entries: dict[bytes, bytes] = {}

    def set(self, key: bytes, value: bytes) -> None:
        """Store ``value`` at ``key``, replacing an existing value."""
        self._entries[key] = value

    def get(self, key: bytes) -> bytes | None:
        """Return the value at ``key``, or ``None`` when it is absent."""
        return self._entries.get(key)

    def delete(self, keys: tuple[bytes, ...]) -> int:
        """Delete ``keys`` and return how many entries were removed."""
        removed = 0
        for key in keys:
            if key in self._entries:
                del self._entries[key]
                removed += 1
        return removed

    def exists(self, keys: tuple[bytes, ...]) -> int:
        """Count supplied existing keys, including repeated occurrences."""
        return sum(key in self._entries for key in keys)
