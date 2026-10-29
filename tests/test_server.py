"""Integration tests that communicate with Mini Redis over real TCP sockets."""

from __future__ import annotations

import socket
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import pytest

from mini_redis.server import MiniRedisServer


@contextmanager
def running_server(**server_options: int) -> Iterator[tuple[str, int]]:
    """Run a server on an OS-assigned port and propagate worker failures."""
    server = MiniRedisServer(port=0, **server_options)
    server.start()

    with ThreadPoolExecutor(max_workers=1) as executor:
        serving = executor.submit(server.serve_forever)
        try:
            yield server.address
        finally:
            server.close()
            serving.result(timeout=2)


def encode_request(*parts: bytes) -> bytes:
    encoded = bytearray(f"*{len(parts)}\r\n".encode("ascii"))
    for part in parts:
        encoded.extend(f"${len(part)}\r\n".encode("ascii"))
        encoded.extend(part)
        encoded.extend(b"\r\n")
    return bytes(encoded)


def receive_exactly(client: socket.socket, length: int) -> bytes:
    response = bytearray()
    while len(response) < length:
        chunk = client.recv(length - len(response))
        if chunk == b"":
            raise AssertionError("server disconnected before completing a response")
        response.extend(chunk)
    return bytes(response)


def test_fragmented_ping_receives_pong_only_after_complete_resp_request() -> None:
    request = encode_request(b"PING")

    with (
        running_server() as address,
        socket.create_connection(address, timeout=1) as client,
    ):
        client.settimeout(0.05)
        client.sendall(request[:-1])

        with pytest.raises(TimeoutError):
            client.recv(1)

        client.sendall(request[-1:])
        client.settimeout(1)

        assert receive_exactly(client, len(b"+PONG\r\n")) == b"+PONG\r\n"


def test_server_accepts_a_new_client_after_disconnect() -> None:
    with running_server() as address:
        for _ in range(2):
            with socket.create_connection(address, timeout=1) as client:
                client.sendall(encode_request(b"PING"))

                assert receive_exactly(client, len(b"+PONG\r\n")) == b"+PONG\r\n"


def test_command_error_does_not_end_the_connection() -> None:
    with (
        running_server() as address,
        socket.create_connection(address, timeout=1) as client,
    ):
        error = b"-ERR unknown command\r\n"
        client.sendall(encode_request(b"HELLO"))
        assert receive_exactly(client, len(error)) == error

        client.sendall(encode_request(b"PING"))
        assert receive_exactly(client, len(b"+PONG\r\n")) == b"+PONG\r\n"


def test_sequential_connections_share_command_state() -> None:
    with running_server() as address:
        with socket.create_connection(address, timeout=1) as first_client:
            first_client.sendall(encode_request(b"SET", b"greeting", b"hello"))
            assert receive_exactly(first_client, len(b"+OK\r\n")) == b"+OK\r\n"

        with socket.create_connection(address, timeout=1) as second_client:
            second_client.sendall(encode_request(b"GET", b"greeting"))
            assert receive_exactly(second_client, len(b"$5\r\nhello\r\n")) == (
                b"$5\r\nhello\r\n"
            )

            second_client.sendall(encode_request(b"DEL", b"greeting", b"greeting"))
            assert receive_exactly(second_client, len(b":1\r\n")) == b":1\r\n"

            second_client.sendall(encode_request(b"GET", b"greeting"))
            assert receive_exactly(second_client, len(b"$-1\r\n")) == b"$-1\r\n"


def test_pipelined_binary_safe_commands_return_ordered_responses() -> None:
    key = b"\x00 key\r\n"
    value = b"\xffhello world\r\n"
    requests = (
        encode_request(b"SET", key, value)
        + encode_request(b"GET", key)
        + encode_request(b"DEL", key)
    )
    responses = (
        b"+OK\r\n" + f"${len(value)}\r\n".encode("ascii") + value + b"\r\n" + b":1\r\n"
    )

    with (
        running_server() as address,
        socket.create_connection(address, timeout=1) as client,
    ):
        client.sendall(requests)

        assert receive_exactly(client, len(responses)) == responses


def test_protocol_error_closes_only_the_malformed_connection() -> None:
    with running_server() as address:
        with socket.create_connection(address, timeout=1) as malformed_client:
            response = b"-ERR Protocol error: expected an array\r\n"
            malformed_client.sendall(b"PING\r\n")
            assert receive_exactly(malformed_client, len(response)) == response
            assert malformed_client.recv(1) == b""

        with socket.create_connection(address, timeout=1) as healthy_client:
            healthy_client.sendall(encode_request(b"PING"))
            assert receive_exactly(healthy_client, len(b"+PONG\r\n")) == b"+PONG\r\n"


def test_buffer_limit_returns_an_error_and_closes_the_connection() -> None:
    with (
        running_server(max_buffer_size=8) as address,
        socket.create_connection(address, timeout=1) as client,
    ):
        response = b"-ERR Protocol error: buffer exceeds maximum of 8 bytes\r\n"
        client.sendall(encode_request(b"PING"))

        assert receive_exactly(client, len(response)) == response
        assert client.recv(1) == b""
