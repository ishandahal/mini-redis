"""Unit tests for socket-independent in-memory storage."""

from mini_redis.store import Store


def test_set_get_and_replace_opaque_bytes() -> None:
    store = Store()
    key = b"\x00 key"

    assert store.get(key) is None

    store.set(key, b"first\xff")
    assert store.get(key) == b"first\xff"

    store.set(key, b"")
    assert store.get(key) == b""


def test_delete_counts_each_existing_key_at_most_once() -> None:
    store = Store()
    store.set(b"first", b"1")
    store.set(b"second", b"2")

    assert store.delete((b"first", b"missing", b"first", b"second")) == 2
    assert store.get(b"first") is None
    assert store.get(b"second") is None
