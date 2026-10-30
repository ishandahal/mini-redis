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


def test_idle_client_does_not_block_shared_state_access() -> None:
    with (
        running_server() as address,
        socket.create_connection(address, timeout=2) as first,
        socket.create_connection(address, timeout=2) as second,
    ):
        first.sendall(encode_request(b"SET", b"key", b"value"))
        assert receive_exactly(first, 5) == b"+OK\r\n"
        second.sendall(encode_request(b"GET", b"key"))
        assert receive_exactly(second, 11) == b"$5\r\nvalue\r\n"
        second.sendall(encode_request(b"EXISTS", b"key", b"missing", b"key"))
        assert receive_exactly(second, 4) == b":2\r\n"


def test_concurrent_increments_have_no_lost_updates() -> None:
    import threading

    clients, increments = 8, 100
    barrier = threading.Barrier(clients)
    with running_server() as address:

        def increment() -> list[int]:
            with (
                socket.create_connection(address, timeout=5) as client,
                client.makefile("rb") as replies,
            ):
                barrier.wait(timeout=5)
                client.sendall(encode_request(b"INCR", b"counter") * increments)
                return [int(replies.readline()[1:-2]) for _ in range(increments)]

        with ThreadPoolExecutor(max_workers=clients) as pool:
            results = list(pool.map(lambda _: increment(), range(clients)))
        assert sorted(value for batch in results for value in batch) == list(
            range(1, clients * increments + 1)
        )
        assert all(batch == sorted(batch) for batch in results)
        with socket.create_connection(address, timeout=2) as client:
            client.sendall(encode_request(b"GET", b"counter"))
            assert receive_exactly(client, 9) == b"$3\r\n800\r\n"


def test_client_limit_rejects_and_releases_slots(monkeypatch) -> None:
    import threading

    released = threading.Event()
    original = MiniRedisServer._run_client

    def tracked_worker(self, connection):
        try:
            original(self, connection)
        finally:
            released.set()

    monkeypatch.setattr(MiniRedisServer, "_run_client", tracked_worker)
    with running_server(max_clients=1) as address:
        with socket.create_connection(address, timeout=2) as first:
            first.sendall(encode_request(b"PING"))
            assert receive_exactly(first, 7) == b"+PONG\r\n"
            with socket.create_connection(address, timeout=2) as rejected:
                error = b"-ERR maximum clients reached\r\n"
                assert receive_exactly(rejected, len(error)) == error
                assert rejected.recv(1) == b""
            first.sendall(encode_request(b"PING"))
            assert receive_exactly(first, 7) == b"+PONG\r\n"
            # A protocol failure must release the slot just like a disconnect.
            first.sendall(b"invalid\r\n")
            error = b"-ERR Protocol error: expected an array\r\n"
            assert receive_exactly(first, len(error)) == error
            assert first.recv(1) == b""
        assert released.wait(timeout=2)
        with socket.create_connection(address, timeout=2) as replacement:
            replacement.sendall(encode_request(b"PING"))
            assert receive_exactly(replacement, 7) == b"+PONG\r\n"


def test_close_unblocks_and_joins_idle_workers() -> None:
    server = MiniRedisServer(port=0)
    server.start()
    with ThreadPoolExecutor(max_workers=1) as pool:
        serving = pool.submit(server.serve_forever)
        try:
            with socket.create_connection(server.address, timeout=2) as client:
                client.sendall(encode_request(b"PING"))
                assert receive_exactly(client, 7) == b"+PONG\r\n"
                with server._clients_lock:
                    workers = list(server._clients.values())
                server.close()
                assert client.recv(1) == b""
                assert all(not worker.is_alive() for worker in workers)
                assert not server._clients
                serving.result(timeout=2)
        finally:
            server.close()


def test_blocked_response_write_does_not_hold_service_lock() -> None:
    import threading

    writing = threading.Event()
    release = threading.Event()

    class SlowConnection:
        def sendall(self, data):
            writing.set()
            assert release.wait(timeout=3)

    server = MiniRedisServer()
    with ThreadPoolExecutor(max_workers=2) as pool:
        blocked = pool.submit(
            server._respond_to_complete_requests,
            SlowConnection(),
            bytearray(encode_request(b"INCR", b"counter")),
        )
        try:
            assert writing.wait(timeout=2)
            from mini_redis.commands import Integer

            other = pool.submit(server._commands.execute, (b"INCR", b"counter"))
            assert other.result(timeout=1) == Integer(2)
        finally:
            release.set()
        assert blocked.result(timeout=2)


@pytest.mark.parametrize("limit", [0, -1])
def test_invalid_client_limit_fails_before_start(limit) -> None:
    with pytest.raises(ValueError, match="maximum clients must be positive"):
        MiniRedisServer(max_clients=limit)
