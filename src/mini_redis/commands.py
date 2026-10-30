"""Command validation and execution independent of sockets and wire framing."""

from __future__ import annotations

from dataclasses import dataclass

from mini_redis.service import InvalidInteger, Service
from mini_redis.store import MAX_TTL_SECONDS


@dataclass(frozen=True)
class SimpleString:
    """A successful textual command result."""

    value: bytes


@dataclass(frozen=True)
class BulkString:
    """An optional byte-string command result."""

    value: bytes | None


@dataclass(frozen=True)
class Integer:
    """An integer command result."""

    value: int


@dataclass(frozen=True)
class CommandError:
    """A client-correctable command error."""

    message: str


CommandResult = SimpleString | BulkString | Integer | CommandError


class CommandProcessor:
    """Validate commands and execute them against a shared store."""

    def __init__(self, service: Service) -> None:
        self._service = service

    def execute(self, request: tuple[bytes, ...]) -> CommandResult:
        """Execute one tokenized request without knowing its wire protocol."""
        if not request:
            return CommandError("unknown command")

        command, *arguments = request
        normalized_command = command.upper()

        if normalized_command == b"PING":
            if arguments:
                return _wrong_argument_count("ping")
            return SimpleString(b"PONG")

        if normalized_command == b"SET":
            if len(arguments) not in (2, 4):
                return _wrong_argument_count("set")
            seconds = None
            if len(arguments) == 4:
                if arguments[2].upper() != b"EX":
                    return CommandError("unsupported SET option")
                seconds = _expiration_seconds(arguments[3])
                if seconds is None:
                    return CommandError("invalid expiration seconds")
            key, value = arguments[:2]
            self._service.set(key, value, seconds)
            return SimpleString(b"OK")

        if normalized_command == b"EXPIRE":
            if len(arguments) != 2:
                return _wrong_argument_count("expire")
            seconds = _expiration_seconds(arguments[1])
            if seconds is None:
                return CommandError("invalid expiration seconds")
            return Integer(self._service.expire(arguments[0], seconds))

        if normalized_command == b"TTL":
            if len(arguments) != 1:
                return _wrong_argument_count("ttl")
            return Integer(self._service.ttl(arguments[0]))

        if normalized_command == b"GET":
            if len(arguments) != 1:
                return _wrong_argument_count("get")
            return BulkString(self._service.get(arguments[0]))

        if normalized_command == b"DEL":
            if not arguments:
                return _wrong_argument_count("del")
            return Integer(self._service.delete(tuple(arguments)))

        if normalized_command == b"EXISTS":
            if not arguments:
                return _wrong_argument_count("exists")
            return Integer(self._service.exists(tuple(arguments)))

        if normalized_command == b"INCR":
            if len(arguments) != 1:
                return _wrong_argument_count("incr")
            try:
                return Integer(self._service.increment(arguments[0]))
            except InvalidInteger as error:
                return CommandError(str(error))

        return CommandError("unknown command")


def _wrong_argument_count(command: str) -> CommandError:
    return CommandError(f"wrong number of arguments for '{command}' command")


def _expiration_seconds(raw: bytes) -> int | None:
    # ASCII digits only; leading zeros are accepted within this length bound.
    if not raw or len(raw) > 10 or not raw.isdigit():
        return None
    seconds = int(raw)
    return seconds if 1 <= seconds <= MAX_TTL_SECONDS else None
