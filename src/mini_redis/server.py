"""Thread-per-client TCP server for Mini Redis."""

from __future__ import annotations

import logging
import socket
import threading
import time
from contextlib import suppress

from mini_redis.commands import CommandProcessor
from mini_redis.protocol import (
    DEFAULT_MAX_ARRAY_LENGTH,
    DEFAULT_MAX_BUFFER_SIZE,
    DEFAULT_MAX_BULK_STRING_SIZE,
    DecodedRequest,
    IncompleteRequest,
    ProtocolError,
    decode_request,
    encode_error,
    encode_protocol_error,
    encode_response,
)
from mini_redis.service import Service
from mini_redis.store import Store

logger = logging.getLogger(__name__)


class MiniRedisServer:
    """Serve ordered RESP2 requests concurrently across client connections.

    ``start`` creates the listening socket synchronously. Keeping socket creation
    separate from ``serve_forever`` lets callers discover an operating-system-
    assigned port before starting the blocking accept loop.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6379,
        store: Store | None = None,
        max_buffer_size: int = DEFAULT_MAX_BUFFER_SIZE,
        max_bulk_string_size: int = DEFAULT_MAX_BULK_STRING_SIZE,
        max_array_length: int = DEFAULT_MAX_ARRAY_LENGTH,
        max_clients: int = 100,
    ) -> None:
        if max_clients <= 0:
            raise ValueError("maximum clients must be positive")
        self._max_clients = max_clients
        self._host = host
        self._port = port
        self._service = Service(store)
        self._commands = CommandProcessor(self._service)
        self._maintenance: threading.Thread | None = None
        self._max_buffer_size = max_buffer_size
        self._max_bulk_string_size = max_bulk_string_size
        self._max_array_length = max_array_length
        self._listener: socket.socket | None = None
        self._clients: dict[socket.socket, threading.Thread] = {}
        self._clients_lock = threading.Lock()
        self._stopping = threading.Event()

    @property
    def address(self) -> tuple[str, int]:
        """Return the address bound by ``start``."""
        if self._listener is None:
            raise RuntimeError("server has not been started")

        host, port = self._listener.getsockname()[:2]
        return str(host), int(port)

    def start(self) -> None:
        """Create, bind, and listen on the server socket."""
        if self._listener is not None:
            raise RuntimeError("server has already been started")

        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind((self._host, self._port))
            listener.listen(self._max_clients)
        except OSError:
            listener.close()
            raise

        self._stopping.clear()
        self._listener = listener
        self._maintenance = threading.Thread(
            target=self._cleanup_loop, name="mini-redis-cleanup", daemon=False
        )
        try:
            self._maintenance.start()
        except Exception:
            self._maintenance = None
            self.close()
            raise

    def _cleanup_loop(self) -> None:
        while not self._stopping.wait(0.1):
            self._service.cleanup(batch_size=100)

    def serve_forever(self) -> None:
        """Accept clients and dispatch workers until ``close`` is called."""
        listener = self._listener
        if listener is None:
            raise RuntimeError("server must be started before it can serve")

        try:
            while not self._stopping.is_set():
                try:
                    connection, _ = listener.accept()
                except OSError:
                    if self._stopping.is_set():
                        break
                    raise

                with self._clients_lock:
                    stopping = self._stopping.is_set()
                    admitted = not stopping and len(self._clients) < self._max_clients
                    if admitted:
                        worker = threading.Thread(
                            target=self._run_client,
                            args=(connection,),
                            name="mini-redis-client",
                            daemon=False,
                        )
                        self._clients[connection] = worker
                        try:
                            # Start under the registry lock so close() can only
                            # observe workers that are safe to join.
                            worker.start()
                        except Exception:
                            del self._clients[connection]
                            _close_socket(connection)
                            raise
                if not admitted:
                    with connection:
                        if not stopping:
                            # A rejected peer must not indefinitely stall accept.
                            connection.settimeout(0.1)
                            with suppress(OSError):
                                connection.sendall(
                                    encode_error("maximum clients reached")
                                )
        finally:
            self.close()

    def close(self) -> None:
        """Stop admission, unblock client sockets, and join workers."""
        with self._clients_lock:
            self._stopping.set()
            listener = self._listener
            self._listener = None
            clients = list(self._clients.items())

        if listener is not None:
            _close_socket(listener)
        for connection, _ in clients:
            _close_socket(connection)

        deadline = time.monotonic() + 2
        if self._maintenance is not None:
            self._maintenance.join(timeout=max(0, deadline - time.monotonic()))
        for _, worker in clients:
            worker.join(timeout=max(0, deadline - time.monotonic()))

    def _run_client(self, connection: socket.socket) -> None:
        """Isolate client failures and always release the admission slot."""
        try:
            self._serve_client(connection)
        except OSError:
            pass  # Disconnects and sockets closed during shutdown are normal.
        except Exception:
            logger.exception("unexpected client worker failure")
        finally:
            _close_socket(connection)
            with self._clients_lock:
                del self._clients[connection]

    def _serve_client(self, connection: socket.socket) -> None:
        """Decode and execute complete RESP2 requests from one TCP byte stream."""
        buffer = bytearray()

        with connection:
            while not self._stopping.is_set():
                try:
                    chunk = connection.recv(4096)
                except OSError:
                    if self._stopping.is_set():
                        return
                    raise

                if chunk == b"":
                    return

                buffer.extend(chunk)
                if not self._respond_to_complete_requests(connection, buffer):
                    return

    def _respond_to_complete_requests(
        self, connection: socket.socket, buffer: bytearray
    ) -> bool:
        """Respond to buffered requests, returning false after a protocol error."""
        while True:
            try:
                parsed = decode_request(
                    buffer,
                    max_buffer_size=self._max_buffer_size,
                    max_bulk_string_size=self._max_bulk_string_size,
                    max_array_length=self._max_array_length,
                )
            except ProtocolError as error:
                connection.sendall(encode_protocol_error(error))
                return False

            if isinstance(parsed, IncompleteRequest):
                return True
            if not isinstance(parsed, DecodedRequest):
                raise AssertionError(f"unsupported parse result: {parsed!r}")

            del buffer[: parsed.bytes_consumed]
            result = self._commands.execute(parsed.request)
            connection.sendall(encode_response(result))


def _close_socket(sock: socket.socket) -> None:
    """Close a socket while treating an already-closed socket as harmless."""
    with suppress(OSError):
        sock.shutdown(socket.SHUT_RDWR)
    with suppress(OSError):
        sock.close()
