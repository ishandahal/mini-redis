"""Validated settings shared by the CLI and embedded server."""

import math
import threading
from dataclasses import dataclass

# Keep socket and threading timeout conversion within platform-supported ranges.
MAX_TIMEOUT = min(threading.TIMEOUT_MAX, 2**31 - 1)


@dataclass(frozen=True)
class Config:
    host: str = "127.0.0.1"
    port: int = 6379
    max_clients: int = 100
    appendonly: str | None = None
    max_buffer_size: int = 1024 * 1024
    max_bulk_string_size: int = 512 * 1024
    max_array_length: int = 1024
    idle_timeout: float = 300.0
    expiration_interval: float = 0.1
    shutdown_timeout: float = 2.0
    log_level: str = "INFO"

    def __post_init__(self):
        if not isinstance(self.host, str) or not self.host or "\x00" in self.host:
            raise ValueError("host must be a nonempty IPv4 address or hostname")
        if type(self.port) is not int or not 0 <= self.port <= 65535:
            raise ValueError("port must be between 0 and 65535")
        for name, label, minimum in (
            ("max_clients", "maximum clients", 1),
            ("max_buffer_size", "maximum buffer size", 1),
            ("max_bulk_string_size", "maximum bulk string size", 0),
            ("max_array_length", "maximum array length", 1),
        ):
            value = getattr(self, name)
            if type(value) is not int or value < minimum:
                raise ValueError(
                    f"{label} must be {'positive' if minimum else 'nonnegative'}"
                )
        for name in ("idle_timeout", "expiration_interval", "shutdown_timeout"):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not 0 < value <= MAX_TIMEOUT
                or not math.isfinite(value)
            ):
                raise ValueError(
                    f"{name} must be finite and between 0 (exclusive) and {MAX_TIMEOUT}"
                )
        if self.max_clients > 2**31 - 1:
            raise ValueError("maximum clients exceeds supported socket backlog")
        if not isinstance(self.log_level, str) or self.log_level not in {
            "DEBUG",
            "INFO",
            "WARNING",
            "ERROR",
            "CRITICAL",
        }:
            raise ValueError("invalid log level")
        if self.appendonly is not None and (
            not isinstance(self.appendonly, str)
            or not self.appendonly
            or "\x00" in self.appendonly
        ):
            raise ValueError("appendonly must be a nonempty file path")
