"""Operational behavior through real sockets, processes, and injected failures."""

import logging
import selectors
import signal
import socket
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress

import pytest
from test_server import encode_request, receive_exactly

from mini_redis.config import Config
from mini_redis.persistence import AppendOnlyLog
from mini_redis.server import MiniRedisServer
from mini_redis.store import Store


@pytest.mark.parametrize(
    "settings",
    [
        {"idle_timeout": 0},
        {"idle_timeout": float("nan")},
        {"expiration_interval": float("inf")},
        {"shutdown_timeout": -1},
        {"max_buffer_size": 0},
        {"max_bulk_string_size": -1},
        {"max_array_length": 0},
        {"port": -1},
        {"max_clients": 1.5},
        {"host": ""},
        {"log_level": "BOGUS"},
        {"appendonly": ""},
    ],
)
def test_configuration_rejects_invalid_settings(settings):
    with pytest.raises(ValueError):
        Config(**settings)


def test_idle_timeout_releases_slot(monkeypatch):
    released = threading.Event()
    original = MiniRedisServer._run_client

    def tracked(self, connection):
        try:
            original(self, connection)
        finally:
            released.set()

    monkeypatch.setattr(MiniRedisServer, "_run_client", tracked)
    server = MiniRedisServer(port=0, max_clients=1, idle_timeout=0.05)
    server.start()
    with ThreadPoolExecutor(1) as pool:
        serving = pool.submit(server.serve_forever)
        try:
            with socket.create_connection(server.address, timeout=2) as client:
                client.sendall(encode_request(b"PING"))
                assert receive_exactly(client, 7) == b"+PONG\r\n"
                assert client.recv(1) == b""
            assert released.wait(2)
            with socket.create_connection(server.address, timeout=2) as client:
                client.sendall(encode_request(b"PING"))
                assert receive_exactly(client, 7) == b"+PONG\r\n"
        finally:
            server.close()
            serving.result(2)
    assert server.metrics.snapshot()["idle_timeouts"] >= 1
    assert server.metrics.snapshot()["current_connections"] == 0


def test_counters_and_logs_cover_errors_expiration_and_persistence(
    tmp_path, clock, caplog
):
    caplog.set_level(logging.DEBUG)
    store = Store(clock)
    server = MiniRedisServer(port=0, store=store, appendonly=str(tmp_path / "data.aof"))
    server.start()
    requests = (
        encode_request(b"SET", b"key", b"private-value", b"EX", b"1")
        + encode_request(b"GET", b"key")
        + encode_request(b"UNKNOWN-PRIVATE-TEXT")
    )
    response = b"+OK\r\n$13\r\nprivate-value\r\n-ERR unknown command\r\n"
    with ThreadPoolExecutor(1) as pool:
        serving = pool.submit(server.serve_forever)
        try:
            with socket.create_connection(server.address, timeout=2) as client:
                client.sendall(requests)
                assert receive_exactly(client, len(response)) == response
                clock.advance(1)
                client.sendall(encode_request(b"GET", b"key"))
                assert receive_exactly(client, 5) == b"$-1\r\n"
                client.sendall(b"invalid")
                error = b"-ERR Protocol error: expected an array\r\n"
                assert receive_exactly(client, len(error)) == error
                assert client.recv(1) == b""
        finally:
            server.close()
            serving.result(2)
    counts = server.metrics.snapshot()
    assert counts["current_connections"] == 0
    assert counts["total_connections"] == 1
    assert counts["commands_GET"] == 2
    assert counts["commands_SET"] == counts["commands_UNKNOWN"] == 1
    assert counts["command_errors"] == counts["protocol_errors"] == 1
    assert counts["keyspace_hits"] == counts["keyspace_misses"] == 1
    assert counts["expired_keys_removed"] == counts["persistence_records_written"] == 1
    assert (
        counts["bytes_read"] == len(requests) + len(encode_request(b"GET", b"key")) + 7
    )
    assert counts["bytes_written"] == len(response) + 5 + len(error)
    assert "shutdown complete counters=" in caplog.text
    assert "private-value" not in caplog.text
    assert "UNKNOWN-PRIVATE-TEXT" not in caplog.text


@pytest.mark.parametrize("signum", [signal.SIGINT, signal.SIGTERM])
def test_signal_shutdown_releases_port_and_replays_log(tmp_path, signum):
    path = tmp_path / "data.aof"
    process = subprocess.Popen(
        [sys.executable, "-m", "mini_redis", "--port", "0", "--appendonly", str(path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            assert selector.select(5), "server did not announce startup"
        line = process.stdout.readline()
        assert "listening on" in line
        port = int(line.strip().rsplit(":", 1)[1])
        with socket.create_connection(("127.0.0.1", port), timeout=2) as client:
            client.sendall(encode_request(b"SET", b"key", b"value"))
            assert receive_exactly(client, 5) == b"+OK\r\n"
            # Keep an incomplete request pending during termination.
            client.sendall(b"*2\r\n$3\r\nGET\r\n")
            process.send_signal(signum)
            _, stderr = process.communicate(timeout=4)
            assert process.returncode == 0, stderr
            assert "shutdown complete" in stderr
            assert client.recv(1) == b""
        with socket.socket() as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            probe.bind(("127.0.0.1", port))
        store = Store()
        log = AppendOnlyLog(path)
        try:
            log.replay(store)
            assert store.get(b"key") == b"value"
        finally:
            log.close()
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=2)


def test_shutdown_waits_for_active_write_and_reports_deadline(tmp_path, monkeypatch):
    entered = threading.Event()
    release = threading.Event()
    original = AppendOnlyLog._sync

    def blocked_sync(self):
        entered.set()
        assert release.wait(3)
        original(self)

    monkeypatch.setattr(AppendOnlyLog, "_sync", blocked_sync)
    path = tmp_path / "active.aof"
    server = MiniRedisServer(port=0, appendonly=str(path), shutdown_timeout=0.05)
    server.start()
    with ThreadPoolExecutor(1) as pool:
        serving = pool.submit(server.serve_forever)
        try:
            with socket.create_connection(server.address, timeout=2) as client:
                client.sendall(encode_request(b"SET", b"key", b"value"))
                assert entered.wait(2)
                with pytest.raises(TimeoutError, match="shutdown deadline"):
                    server.close()
                assert not server._closed.is_set()
                assert not server._service._log._file.closed
                release.set()
                assert server._closed.wait(2)
                server.close()
                # The serving caller may also have reached its deadline.
                with suppress(TimeoutError):
                    serving.result(2)
        finally:
            release.set()
            assert server._closed.wait(2)
    store = Store()
    with_log = AppendOnlyLog(path)
    try:
        with_log.replay(store)
        assert store.get(b"key") == b"value"
    finally:
        with_log.close()


def test_command_execution_is_not_aborted_by_idle_timeout(monkeypatch):
    entered = threading.Event()
    release = threading.Event()
    server = MiniRedisServer(port=0, idle_timeout=0.02)
    original = server._commands.execute

    def slow(request):
        entered.set()
        assert release.wait(2)
        return original(request)

    monkeypatch.setattr(server._commands, "execute", slow)
    server.start()
    with ThreadPoolExecutor(1) as pool:
        serving = pool.submit(server.serve_forever)
        try:
            with socket.create_connection(server.address, timeout=2) as client:
                client.sendall(encode_request(b"PING"))
                assert entered.wait(2)
                # Deliberately outlast the socket timeout while executing a command.
                assert not release.wait(0.05)
                release.set()
                assert receive_exactly(client, 7) == b"+PONG\r\n"
        finally:
            release.set()
            server.close()
            serving.result(2)


def test_cli_forces_error_exit_when_disk_never_returns(tmp_path):
    script = """
import threading
from mini_redis.persistence import AppendOnlyLog
from mini_redis.__main__ import main

def stuck(self):
    print("disk blocked", flush=True)
    threading.Event().wait()

AppendOnlyLog._sync = stuck
raise SystemExit(main())
"""
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            script,
            "--port",
            "0",
            "--shutdown-timeout",
            "0.1",
            "--appendonly",
            str(tmp_path / "stuck.aof"),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            assert selector.select(5)
            line = process.stdout.readline()
            port = int(line.strip().rsplit(":", 1)[1])
            with socket.create_connection(("127.0.0.1", port), timeout=2) as client:
                client.sendall(encode_request(b"SET", b"key", b"value"))
                assert selector.select(2)
                assert process.stdout.readline().strip() == "disk blocked"
                process.send_signal(signal.SIGTERM)
                _, stderr = process.communicate(timeout=3)
                assert process.returncode == 1
                assert "forcing unsuccessful exit" in stderr
                assert "shutdown complete" not in stderr
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=2)


def test_metrics_updates_are_thread_safe():
    from mini_redis.metrics import Metrics

    metrics = Metrics()

    def add_batch(_):
        for _ in range(1000):
            metrics.add("commands_PING")

    with ThreadPoolExecutor(8) as pool:
        list(pool.map(add_batch, range(8)))
    snapshot = metrics.snapshot()
    assert snapshot["commands_PING"] == 8000
    snapshot["commands_PING"] = 0
    assert metrics.snapshot()["commands_PING"] == 8000
