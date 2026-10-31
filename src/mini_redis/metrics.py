"""Thread-safe, bounded-cardinality operational counters."""

import threading
from collections import Counter


class Metrics:
    def __init__(self):
        self._lock = threading.Lock()
        self._counts = Counter(
            dict.fromkeys(
                (
                    "current_connections",
                    "total_connections",
                    "command_errors",
                    "protocol_errors",
                    "keyspace_hits",
                    "keyspace_misses",
                    "expired_keys_removed",
                    "persistence_records_written",
                    "bytes_read",
                    "bytes_written",
                    "idle_timeouts",
                    "write_timeouts",
                    "rejected_connections",
                ),
                0,
            )
        )

    def add(self, name: str, amount: int = 1) -> None:
        with self._lock:
            self._counts[name] += amount

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return dict(self._counts)
