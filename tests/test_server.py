"""Integration tests that communicate with Mini Redis over real TCP sockets."""

from __future__ import annotations

import socket
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import pytest

from mini_redis.server import MiniRedisServer


@contextmanager
def running_server() -> Iterator[tuple[str, int]]:
    """Run a server on an OS-assigned port and propagate worker failures."""
    server = MiniRedisServer(port=0)
    server.start()

    with ThreadPoolExecutor(max_workers=1) as executor:
        serving = executor.submit(server.serve_forever)
        try:
            yield server.address
        finally:
            server.close()
            serving.result(timeout=2)


def receive_until_crlf(client: socket.socket) -> bytes:
    """Read one complete temporary-protocol response from a TCP stream."""
    response = bytearray()
    while not response.endswith(b"\r\n"):
        chunk = client.recv(4096)
        if chunk == b"":
            raise AssertionError("server disconnected before completing a response")
        response.extend(chunk)
    return bytes(response)


def test_fragmented_ping_receives_pong_only_after_terminator() -> None:
    with (
        running_server() as address,
        socket.create_connection(address, timeout=1) as client,
    ):
        client.settimeout(0.05)
        client.sendall(b"PI")

        with pytest.raises(TimeoutError):
            client.recv(1)

        client.sendall(b"NG\r")

        with pytest.raises(TimeoutError):
            client.recv(1)

        client.sendall(b"\n")
        client.settimeout(1)

        assert receive_until_crlf(client) == b"PONG\r\n"


def test_server_accepts_a_new_client_after_disconnect() -> None:
    with running_server() as address:
        for _ in range(2):
            with socket.create_connection(address, timeout=1) as client:
                client.sendall(b"PING\r\n")

                assert receive_until_crlf(client) == b"PONG\r\n"


def test_unknown_command_does_not_end_the_connection() -> None:
    with (
        running_server() as address,
        socket.create_connection(address, timeout=1) as client,
    ):
        client.sendall(b"HELLO\r\n")

        assert receive_until_crlf(client) == b"ERROR unknown command\r\n"

        client.sendall(b"PING\r\n")

        assert receive_until_crlf(client) == b"PONG\r\n"
