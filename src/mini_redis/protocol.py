"""Incremental RESP2 request decoding and response encoding."""

from __future__ import annotations

from dataclasses import dataclass

from mini_redis.commands import (
    BulkString,
    CommandError,
    CommandResult,
    Integer,
    SimpleString,
)

CRLF = b"\r\n"
DEFAULT_MAX_BUFFER_SIZE = 1024 * 1024
DEFAULT_MAX_BULK_STRING_SIZE = 512 * 1024
DEFAULT_MAX_ARRAY_LENGTH = 1024


class ProtocolError(ValueError):
    """The input cannot be a valid request in Mini Redis's RESP2 subset."""


@dataclass(frozen=True)
class IncompleteRequest:
    """The current bytes are a valid prefix but do not form a full request."""


@dataclass(frozen=True)
class DecodedRequest:
    """One decoded request and the number of bytes it occupies in the buffer."""

    request: tuple[bytes, ...]
    bytes_consumed: int


ParseResult = DecodedRequest | IncompleteRequest


def decode_request(
    data: bytes | bytearray,
    *,
    max_buffer_size: int = DEFAULT_MAX_BUFFER_SIZE,
    max_bulk_string_size: int = DEFAULT_MAX_BULK_STRING_SIZE,
    max_array_length: int = DEFAULT_MAX_ARRAY_LENGTH,
) -> ParseResult:
    """Decode one RESP2 array-of-bulk-strings request without consuming input."""
    _require_positive_limit("maximum buffer size", max_buffer_size)
    _require_nonnegative_limit("maximum bulk string size", max_bulk_string_size)
    _require_positive_limit("maximum array length", max_array_length)

    if len(data) > max_buffer_size:
        raise ProtocolError(f"buffer exceeds maximum of {max_buffer_size} bytes")
    if not data:
        return IncompleteRequest()
    if data[0] != ord("*"):
        raise ProtocolError("expected an array")

    array_header = _decode_length(
        data,
        offset=1,
        maximum=max_array_length,
        kind="array",
    )
    if array_header is None:
        return IncompleteRequest()

    array_length, cursor = array_header
    request: list[bytes] = []

    for _ in range(array_length):
        if cursor == len(data):
            return IncompleteRequest()
        if data[cursor] != ord("$"):
            raise ProtocolError("request elements must be bulk strings")

        bulk_header = _decode_length(
            data,
            offset=cursor + 1,
            maximum=max_bulk_string_size,
            kind="bulk string",
        )
        if bulk_header is None:
            return IncompleteRequest()

        bulk_length, value_start = bulk_header
        value_end = value_start + bulk_length
        terminator_end = value_end + len(CRLF)
        if len(data) < terminator_end:
            return IncompleteRequest()
        if data[value_end:terminator_end] != CRLF:
            raise ProtocolError("invalid bulk string terminator")

        request.append(bytes(data[value_start:value_end]))
        cursor = terminator_end

    return DecodedRequest(tuple(request), cursor)


def encode_response(result: CommandResult) -> bytes:
    """Encode a protocol-independent command result as RESP2."""
    if isinstance(result, SimpleString):
        _validate_inline_response(result.value)
        return b"+" + result.value + CRLF
    if isinstance(result, BulkString):
        if result.value is None:
            return b"$-1" + CRLF
        return (
            b"$" + str(len(result.value)).encode("ascii") + CRLF + result.value + CRLF
        )
    if isinstance(result, Integer):
        return b":" + str(result.value).encode("ascii") + CRLF
    if isinstance(result, CommandError):
        return encode_error(result.message)
    raise AssertionError(f"unsupported command result: {result!r}")


def encode_error(message: str) -> bytes:
    """Encode a safe, single-line RESP2 error response."""
    encoded_message = message.encode("ascii")
    _validate_inline_response(encoded_message)
    return b"-ERR " + encoded_message + CRLF


def encode_protocol_error(error: ProtocolError) -> bytes:
    """Encode a malformed-request error before its connection is closed."""
    return encode_error(f"Protocol error: {error}")


def _decode_length(
    data: bytes | bytearray,
    *,
    offset: int,
    maximum: int,
    kind: str,
) -> tuple[int, int] | None:
    """Decode one non-negative decimal length and its CRLF terminator."""
    value = 0
    digits = 0
    cursor = offset

    while cursor < len(data):
        byte = data[cursor]
        if ord("0") <= byte <= ord("9"):
            digits += 1
            value = value * 10 + byte - ord("0")
            if value > maximum:
                raise ProtocolError(f"{kind} length exceeds maximum of {maximum}")
            cursor += 1
            continue

        if byte == ord("\r"):
            if cursor + 1 == len(data):
                return None
            if data[cursor + 1] != ord("\n"):
                raise ProtocolError(f"invalid {kind} length terminator")
            if digits == 0:
                raise ProtocolError(f"invalid {kind} length")
            return value, cursor + len(CRLF)

        raise ProtocolError(f"invalid {kind} length")

    return None


def _validate_inline_response(value: bytes) -> None:
    if b"\r" in value or b"\n" in value:
        raise ValueError("inline RESP2 responses cannot contain CR or LF")


def _require_positive_limit(name: str, value: int) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be positive")


def _require_nonnegative_limit(name: str, value: int) -> None:
    if value < 0:
        raise ValueError(f"{name} must not be negative")
