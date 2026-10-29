"""Unit tests for incremental RESP2 decoding and response encoding."""

from __future__ import annotations

import pytest

from mini_redis.commands import (
    BulkString,
    CommandError,
    CommandResult,
    Integer,
    SimpleString,
)
from mini_redis.protocol import (
    DecodedRequest,
    IncompleteRequest,
    ProtocolError,
    decode_request,
    encode_response,
)


def encode_request(*parts: bytes) -> bytes:
    encoded = bytearray(f"*{len(parts)}\r\n".encode("ascii"))
    for part in parts:
        encoded.extend(f"${len(part)}\r\n".encode("ascii"))
        encoded.extend(part)
        encoded.extend(b"\r\n")
    return bytes(encoded)


def test_decodes_one_request_and_reports_unconsumed_remainder() -> None:
    request = encode_request(b"SET", b"key", b"value")
    remainder = encode_request(b"PING")

    assert decode_request(request + remainder) == DecodedRequest(
        (b"SET", b"key", b"value"), len(request)
    )


def test_complete_request_before_an_incomplete_request_is_available() -> None:
    first_request = encode_request(b"PING")
    incomplete_second_request = encode_request(b"GET", b"key")[:-1]
    buffer = bytearray(first_request + incomplete_second_request)

    parsed = decode_request(buffer)
    assert parsed == DecodedRequest((b"PING",), len(first_request))

    del buffer[: parsed.bytes_consumed]
    assert isinstance(decode_request(buffer), IncompleteRequest)


def test_every_fragmented_prefix_is_incomplete_without_consuming_bytes() -> None:
    request = encode_request(b"SET", b"\x00 key\r\n", b"\xff value\r\n")

    for split_at in range(len(request)):
        assert isinstance(decode_request(request[:split_at]), IncompleteRequest)

    assert decode_request(request) == DecodedRequest(
        (b"SET", b"\x00 key\r\n", b"\xff value\r\n"), len(request)
    )


def test_empty_array_and_empty_bulk_string_are_valid() -> None:
    assert decode_request(b"*0\r\n") == DecodedRequest((), 4)

    request = encode_request(b"SET", b"", b"")
    assert decode_request(request) == DecodedRequest((b"SET", b"", b""), len(request))


@pytest.mark.parametrize(
    ("data", "message"),
    [
        (b"+PING\r\n", "expected an array"),
        (b"*\r\n", "invalid array length"),
        (b"*x\r\n", "invalid array length"),
        (b"*1\n", "invalid array length"),
        (b"*-1\r\n", "invalid array length"),
        (b"*1\r\n+PING\r\n", "request elements must be bulk strings"),
        (b"*1\r\n*1\r\n$4\r\nPING\r\n", "request elements must be bulk strings"),
        (b"*1\r\n$-1\r\n", "invalid bulk string length"),
        (b"*1\r\n$4\r\nPING\rX", "invalid bulk string terminator"),
    ],
)
def test_rejects_malformed_or_unsupported_requests(data: bytes, message: str) -> None:
    with pytest.raises(ProtocolError, match=message):
        decode_request(data)


def test_enforces_buffer_limit_at_its_boundary() -> None:
    request = encode_request(b"PING")

    assert isinstance(
        decode_request(request[:-1], max_buffer_size=len(request)), IncompleteRequest
    )
    assert isinstance(
        decode_request(request, max_buffer_size=len(request)), DecodedRequest
    )
    with pytest.raises(ProtocolError, match="buffer exceeds maximum"):
        decode_request(request, max_buffer_size=len(request) - 1)


def test_enforces_array_length_limit_as_soon_as_it_is_known() -> None:
    request = encode_request(b"PING", b"extra")

    assert isinstance(
        decode_request(encode_request(b"PING"), max_array_length=2), DecodedRequest
    )
    assert isinstance(decode_request(request, max_array_length=2), DecodedRequest)
    with pytest.raises(ProtocolError, match="array length exceeds maximum of 1"):
        decode_request(request, max_array_length=1)


def test_enforces_bulk_string_limit_and_allows_empty_values() -> None:
    request = encode_request(b"GET", b"key")

    assert isinstance(
        decode_request(encode_request(b"GET", b"ke"), max_bulk_string_size=3),
        DecodedRequest,
    )
    assert isinstance(decode_request(request, max_bulk_string_size=3), DecodedRequest)
    with pytest.raises(ProtocolError, match="bulk string length exceeds maximum of 2"):
        decode_request(request, max_bulk_string_size=2)

    empty_request = encode_request(b"")
    assert isinstance(
        decode_request(empty_request, max_bulk_string_size=0), DecodedRequest
    )


@pytest.mark.parametrize(
    ("result", "encoded"),
    [
        (SimpleString(b"PONG"), b"+PONG\r\n"),
        (BulkString(b"hello\r\n\x00\xff"), b"$9\r\nhello\r\n\x00\xff\r\n"),
        (BulkString(b""), b"$0\r\n\r\n"),
        (BulkString(None), b"$-1\r\n"),
        (Integer(42), b":42\r\n"),
        (Integer(-2), b":-2\r\n"),
        (CommandError("unknown command"), b"-ERR unknown command\r\n"),
    ],
)
def test_encodes_every_supported_response(
    result: CommandResult, encoded: bytes
) -> None:
    assert encode_response(result) == encoded


def test_rejects_crlf_in_inline_responses() -> None:
    with pytest.raises(ValueError, match="cannot contain CR or LF"):
        encode_response(SimpleString(b"not\r\nsafe"))

    with pytest.raises(ValueError, match="cannot contain CR or LF"):
        encode_response(CommandError("not\nsafe"))
