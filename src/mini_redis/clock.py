"""Time sources for live expiration and future persistence."""

import time
from typing import Protocol


class Clock(Protocol):
    def monotonic(self) -> float: ...

    def wall_time(self) -> float: ...


class SystemClock:
    def monotonic(self) -> float:
        return time.monotonic()

    def wall_time(self) -> float:
        return time.time()
