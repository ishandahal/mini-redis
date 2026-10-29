"""The synchronous TCP server for Mini Redis."""

from __future__ import annotations

import socket
import threading
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
    encode_protocol_error,
    encode_response,
)
from mini_redis.store import Store


class MiniRedisServer:
    """Serve RESP2 requests to one active client at a time.

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
    ) -> None:
        self._host = host
        self._port = port
        self._commands = CommandProcessor(store if store is not None else Store())
        self._max_buffer_size = max_buffer_size
        self._max_bulk_string_size = max_bulk_string_size
        self._max_array_length = max_array_length
        self._listener: socket.socket | None = None
        self._active_client: socket.socket | None = None
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
            listener.listen(1)
        except OSError:
            listener.close()
            raise

        self._stopping.clear()
        self._listener = listener

    def serve_forever(self) -> None:
        """Accept and serve clients sequentially until ``close`` is called."""
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

                self._active_client = connection
                try:
                    self._serve_client(connection)
                except OSError:
                    if self._stopping.is_set():
                        break
                finally:
                    self._active_client = None
        finally:
            self.close()

    def close(self) -> None:
        """Stop accepting clients and unblock any active socket operation."""
        self._stopping.set()

        active_client = self._active_client
        if active_client is not None:
            _close_socket(active_client)

        listener = self._listener
        self._listener = None
        if listener is not None:
            _close_socket(listener)

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
    sock.close()
