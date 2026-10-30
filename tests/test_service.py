"""Check the atomicity boundary independently of the TCP transport."""

import threading
from concurrent.futures import ThreadPoolExecutor

from mini_redis.service import Service
from mini_redis.store import Store


def test_increment_holds_lock_between_read_and_write() -> None:
    first_read = threading.Event()
    release = threading.Event()
    second_started = threading.Event()
    second_read = threading.Event()

    class PausedStore(Store):
        def get(self, key):
            value = super().get(key)
            if not first_read.is_set():
                first_read.set()
                assert release.wait(timeout=2)
            else:
                second_read.set()
            return value

    service = Service(PausedStore())

    def second_increment():
        second_started.set()
        return service.increment(b"counter")

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(service.increment, b"counter")
        try:
            assert first_read.wait(timeout=1)
            second = pool.submit(second_increment)
            assert second_started.wait(timeout=1)
            assert not second_read.wait(timeout=0.05)
        finally:
            release.set()
        assert first.result(timeout=1) == 1
        assert second.result(timeout=1) == 2
