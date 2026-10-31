"""Run a self-contained TCP demo: uv run python scripts/demo.py.

Uses temporary files and OS-assigned loopback ports. No existing server or
redis-cli is required. Server logs are printed only if a demonstration fails.
"""

from __future__ import annotations

import selectors
import signal
import socket
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path


def request(*parts: bytes) -> bytes:
    return b"*%d\r\n" % len(parts) + b"".join(
        b"$%d\r\n" % len(part) + part + b"\r\n" for part in parts
    )


def reply(stream):
    line = stream.readline()
    if not line.endswith(b"\r\n"):
        raise RuntimeError("server disconnected during a response")
    kind, body = line[:1], line[1:-2]
    if kind in (b"+", b"-"):
        return line[:-2]
    if kind == b":":
        return int(body)
    if kind == b"$":
        length = int(body)
        if length == -1:
            return None
        data = stream.read(length + 2)
        if len(data) != length + 2 or not data.endswith(b"\r\n"):
            raise RuntimeError("incomplete bulk response")
        return data[:-2]
    raise RuntimeError(f"unexpected response: {line!r}")


@contextmanager
def client(address):
    with (
        socket.create_connection(address, timeout=5) as connection,
        connection.makefile("rb") as stream,
    ):
        yield connection, stream


def expect(actual, expected):
    if actual != expected:
        raise RuntimeError(f"expected {expected!r}, received {actual!r}")


@contextmanager
def server(directory):
    log_path = directory / "server.log"
    with log_path.open("w+") as log:
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "mini_redis",
                "--port",
                "0",
                "--appendonly",
                str(directory / "demo.aof"),
            ],
            stdout=subprocess.PIPE,
            stderr=log,
            text=True,
        )
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ)
                if not selector.select(5):
                    raise RuntimeError("server startup timed out")
            line = process.stdout.readline().strip()
            if not line.startswith("Mini Redis listening on "):
                raise RuntimeError(f"server did not start: {line}")
            port = int(line.rsplit(":", 1)[1])
            print(f"Started a temporary server on 127.0.0.1:{port}")
            yield ("127.0.0.1", port)
            process.send_signal(signal.SIGTERM)
            process.communicate(timeout=5)
            expect(process.returncode, 0)
            print("SIGTERM shutdown completed successfully")
        except BaseException:
            log.seek(0)
            print(log.read(), file=sys.stderr)
            raise
        finally:
            if process.poll() is None:
                process.kill()
            process.communicate(timeout=5)


def main():
    with tempfile.TemporaryDirectory(prefix="mini-redis-demo-") as temporary:
        directory = Path(temporary)
        with server(directory) as address:
            with client(address) as (connection, stream):
                # Write in fragments; TCP may still coalesce these writes.
                encoded = request(b"PING")
                connection.sendall(encoded[:5])
                connection.sendall(encoded[5:])
                expect(reply(stream), b"+PONG")
                print("Fragmented PING -> PONG")

                value = b"hello\x00\xff\r\nworld"
                connection.sendall(
                    request(b"SET", b"binary", value) + request(b"GET", b"binary")
                )
                expect(reply(stream), b"+OK")
                expect(reply(stream), value)
                print("Pipelined SET/GET preserved binary bytes")

                connection.sendall(request(b"NO_SUCH_COMMAND") + request(b"PING"))
                expect(reply(stream), b"-ERR unknown command")
                expect(reply(stream), b"+PONG")
                print("An invalid command leaves the connection usable")

            def increment(_):
                with client(address) as (connection, stream):
                    connection.sendall(request(b"INCR", b"counter") * 10)
                    return [reply(stream) for _ in range(10)]

            with ThreadPoolExecutor(4) as pool:
                results = list(pool.map(increment, range(4)))
            expect(
                sorted(value for batch in results for value in batch),
                list(range(1, 41)),
            )
            print(
                "Four concurrent clients performed 40 increments without lost updates"
            )

            with client(address) as (connection, stream):
                connection.sendall(
                    request(b"SET", b"short-lived", b"value", b"EX", b"1")
                )
                expect(reply(stream), b"+OK")
                deadline = time.monotonic() + 5
                while True:
                    connection.sendall(request(b"GET", b"short-lived"))
                    if reply(stream) is None:
                        break
                    if time.monotonic() >= deadline:
                        raise RuntimeError("expiration did not occur")
                    time.sleep(0.05)  # Manual demo only; TTL tests use fake time.
                connection.sendall(request(b"TTL", b"short-lived"))
                expect(reply(stream), -2)
                print("SET EX expired; GET -> null and TTL -> -2")

        with server(directory) as address, client(address) as (connection, stream):
            connection.sendall(
                request(b"GET", b"binary")
                + request(b"GET", b"counter")
                + request(b"GET", b"short-lived")
            )
            expect(reply(stream), value)
            expect(reply(stream), b"40")
            expect(reply(stream), None)
            print(
                "Restart recovered binary data and counter; expired data stayed absent"
            )
    print("Demo complete. Temporary data and logs removed.")


if __name__ == "__main__":
    main()
