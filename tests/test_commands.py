"""Unit tests for command validation and execution without sockets."""

import pytest

from mini_redis.commands import (
    BulkString,
    CommandError,
    CommandProcessor,
    Integer,
    SimpleString,
)
from mini_redis.service import Service


@pytest.fixture
def commands() -> CommandProcessor:
    return CommandProcessor(Service())


def test_ping_is_case_insensitive_and_rejects_arguments(
    commands: CommandProcessor,
) -> None:
    assert commands.execute((b"pInG",)) == SimpleString(b"PONG")
    assert commands.execute((b"PING", b"extra")) == CommandError(
        "wrong number of arguments for 'ping' command"
    )


def test_set_get_replace_and_missing_key(commands: CommandProcessor) -> None:
    assert commands.execute((b"GET", b"key")) == BulkString(None)
    assert commands.execute((b"SET", b"key", b"first")) == SimpleString(b"OK")
    assert commands.execute((b"GET", b"key")) == BulkString(b"first")

    assert commands.execute((b"set", b"key", b"second")) == SimpleString(b"OK")
    assert commands.execute((b"get", b"key")) == BulkString(b"second")


def test_del_supports_multiple_and_duplicate_keys(commands: CommandProcessor) -> None:
    commands.execute((b"SET", b"first", b"1"))
    commands.execute((b"SET", b"second", b"2"))

    assert commands.execute(
        (b"DEL", b"first", b"missing", b"first", b"second")
    ) == Integer(2)
    assert commands.execute((b"GET", b"first")) == BulkString(None)


@pytest.mark.parametrize(
    ("tokens", "command"),
    [
        ((b"SET", b"key"), "set"),
        ((b"SET", b"key", b"value", b"extra"), "set"),
        ((b"GET",), "get"),
        ((b"GET", b"one", b"two"), "get"),
        ((b"DEL",), "del"),
    ],
)
def test_wrong_argument_counts_do_not_mutate(
    commands: CommandProcessor, tokens: tuple[bytes, ...], command: str
) -> None:
    commands.execute((b"SET", b"key", b"original"))

    assert commands.execute(tokens) == CommandError(
        f"wrong number of arguments for '{command}' command"
    )
    assert commands.execute((b"GET", b"key")) == BulkString(b"original")


def test_unknown_and_empty_commands_return_errors(commands: CommandProcessor) -> None:
    assert commands.execute((b"NOPE",)) == CommandError("unknown command")
    assert commands.execute(()) == CommandError("unknown command")


def test_exists_counts_duplicates_and_empty_values(commands: CommandProcessor) -> None:
    commands.execute((b"SET", b"key", b""))
    assert commands.execute((b"eXiStS", b"key", b"missing", b"key")) == Integer(2)
    assert isinstance(commands.execute((b"EXISTS",)), CommandError)


@pytest.mark.parametrize(
    "value", [b"0", b"1", b"-1", b"-9223372036854775808", b"9223372036854775806"]
)
def test_incr_accepts_canonical_signed_integers(commands, value) -> None:
    commands.execute((b"SET", b"key", value))
    expected = int(value) + 1
    assert commands.execute((b"iNcR", b"key")) == Integer(expected)
    assert commands.execute((b"GET", b"key")) == BulkString(str(expected).encode())


@pytest.mark.parametrize(
    "value",
    [
        b"",
        b"+1",
        b"01",
        b"-0",
        b"-01",
        b" 1",
        b"1 ",
        b"1\n",
        b"1.0",
        b"abc",
        b"\xff",
        b"9223372036854775807",
        b"9223372036854775808",
        b"-9223372036854775809",
        b"9" * 5000,
    ],
)
def test_incr_errors_preserve_value(commands, value) -> None:
    commands.execute((b"SET", b"key", value))
    assert isinstance(commands.execute((b"INCR", b"key")), CommandError)
    assert commands.execute((b"GET", b"key")) == BulkString(value)


def test_incr_missing_key_and_arity(commands: CommandProcessor) -> None:
    assert commands.execute((b"INCR", b"key")) == Integer(1)
    assert isinstance(commands.execute((b"INCR",)), CommandError)
    assert isinstance(commands.execute((b"INCR", b"key", b"extra")), CommandError)
    assert commands.execute((b"GET", b"key")) == BulkString(b"1")
