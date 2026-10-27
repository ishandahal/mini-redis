"""Unit tests for command validation and execution without sockets."""

import pytest

from mini_redis.commands import (
    BulkString,
    CommandError,
    CommandProcessor,
    Integer,
    SimpleString,
)
from mini_redis.store import Store


@pytest.fixture
def commands() -> CommandProcessor:
    return CommandProcessor(Store())


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
