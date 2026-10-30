"""Deterministic clocks: expiration tests never wait for a TTL to elapse."""

import pytest


class FakeClock:
    def __init__(self):
        self.now = 100.0
        self.wall = 1_700_000_000.0

    def monotonic(self):
        return self.now

    def wall_time(self):
        return self.wall

    def advance(self, seconds):
        self.now += seconds
        self.wall += seconds


@pytest.fixture
def clock():
    return FakeClock()
