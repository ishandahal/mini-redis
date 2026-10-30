"""Expiration semantics across storage, commands, and atomic service operations."""

import pytest

from mini_redis.commands import BulkString, CommandError, CommandProcessor, Integer
from mini_redis.service import InvalidInteger, Service
from mini_redis.store import Store


def test_deadline_boundary_and_rounding(clock):
    store = Store(clock)
    assert store.ttl(b"missing") == -2
    store.set(b"permanent", b"")
    assert store.ttl(b"permanent") == -1
    store.set(b"key", b"value", 2)
    assert store.ttl(b"key") == 2
    clock.advance(1.25)
    assert store.ttl(b"key") == 1
    assert store.get(b"key") == b"value"
    clock.advance(0.75)
    assert store.get(b"key") is None
    assert store.ttl(b"key") == -2


@pytest.mark.parametrize("operation", ["get", "exists", "delete", "expire", "ttl"])
def test_every_operation_hides_expired_keys(clock, operation):
    store = Store(clock)
    store.set(b"key", b"value", 1)
    clock.advance(1)
    if operation in ("exists", "delete"):
        result = getattr(store, operation)((b"key", b"key"))
        assert result == 0
    elif operation == "expire":
        assert store.expire(b"key", 10) == 0
    elif operation == "ttl":
        assert store.ttl(b"key") == -2
    else:
        assert store.get(b"key") is None
    assert b"key" not in store._entries


def test_replacement_refresh_and_clear_expiration(clock):
    store = Store(clock)
    store.set(b"key", b"first", 2)
    clock.advance(1)
    store.set(b"key", b"second", 10)
    assert store.ttl(b"key") == 10
    assert store.expire(b"key", 3) == 1
    assert store.ttl(b"key") == 3
    store.set(b"key", b"third")
    clock.advance(20)
    assert store.get(b"key") == b"third"
    assert store.ttl(b"key") == -1
    assert store.expire(b"missing", 1) == 0


def test_wall_clock_changes_do_not_change_live_deadline(clock):
    store = Store(clock)
    store.set(b"key", b"value", 10)
    assert store._entries[b"key"].expires_at == clock.wall + 10
    clock.wall += 100_000
    assert store.ttl(b"key") == 10
    clock.wall -= 200_000
    clock.advance(10)
    assert store.get(b"key") is None


def test_increment_preserves_ttl_and_recreates_expired_key(clock):
    service = Service(Store(clock))
    service.set(b"counter", b"4", 5)
    clock.advance(2)
    assert service.increment(b"counter") == 5
    assert service.ttl(b"counter") == 3
    clock.advance(3)
    assert service.increment(b"counter") == 1
    assert service.ttl(b"counter") == -1


@pytest.mark.parametrize("value", [b"invalid", b"9223372036854775807"])
def test_failed_increment_preserves_value_and_deadline(clock, value):
    service = Service(Store(clock))
    service.set(b"key", value, 5)
    with pytest.raises(InvalidInteger):
        service.increment(b"key")
    assert service.get(b"key") == value
    assert service.ttl(b"key") == 5


@pytest.mark.parametrize(
    "seconds",
    [b"0", b"-1", b"+1", b"1.5", b" 1", b"", b"2147483648", b"9" * 5000, b"\xff"],
)
@pytest.mark.parametrize("command", [b"SET", b"EXPIRE"])
def test_invalid_expiration_does_not_mutate(clock, seconds, command):
    service = Service(Store(clock))
    commands = CommandProcessor(service)
    service.set(b"key", b"original", 5)
    request = (
        (command, b"key", b"replacement", b"EX", seconds)
        if command == b"SET"
        else (command, b"key", seconds)
    )
    assert isinstance(commands.execute(request), CommandError)
    assert service.get(b"key") == b"original"
    assert service.ttl(b"key") == 5


@pytest.mark.parametrize(
    "tokens",
    [
        (b"SET", b"key", b"new", b"PX", b"1"),
        (b"SET", b"key", b"new", b"EX"),
        (b"SET", b"key", b"new", b"EX", b"1", b"extra"),
        (b"EXPIRE", b"key"),
        (b"EXPIRE", b"key", b"1", b"extra"),
        (b"TTL",),
        (b"TTL", b"key", b"extra"),
    ],
)
def test_invalid_options_and_arity_preserve_state(clock, tokens):
    service = Service(Store(clock))
    service.set(b"key", b"original", 5)
    assert isinstance(CommandProcessor(service).execute(tokens), CommandError)
    assert service.get(b"key") == b"original"
    assert service.ttl(b"key") == 5


def test_commands_accept_case_insensitive_options_and_expire(clock):
    commands = CommandProcessor(Service(Store(clock)))
    commands.execute((b"set", b"", b"\x00\xff", b"ex", b"02"))
    assert commands.execute((b"ttl", b"")) == Integer(2)
    assert commands.execute((b"expire", b"", b"3")) == Integer(1)
    clock.advance(3)
    assert commands.execute((b"GET", b"")) == BulkString(None)
    assert commands.execute((b"TTL", b"")) == Integer(-2)


def test_cleanup_is_bounded_fair_and_preserves_live_entries(clock):
    store = Store(clock)
    store.set(b"permanent", b"value")
    store.set(b"live", b"value", 100)
    for key in (b"a", b"b", b"c"):
        store.set(key, b"value", 1)
    clock.advance(1)
    assert store.cleanup(2) == 0
    assert len(store._entries) == 5
    assert store.cleanup(2) == 2
    # Replacing an expired key before cleanup must protect its new value.
    store.set(b"c", b"new", 10)
    assert store.cleanup(2) == 0
    assert store.get(b"c") == b"new"
    assert store.get(b"live") == b"value"
    assert store.get(b"permanent") == b"value"
