"""The synchronous TCP server used by the first learning milestone."""

from __future__ import annotations

import socket
import threading
from contextlib import suppress

COMMAND_TERMINATOR = b"\r\n"
PING_COMMAND = b"PING"
PONG_RESPONSE = b"PONG\r\n"
UNKNOWN_COMMAND_RESPONSE = b"ERROR unknown command\r\n"


class MiniRedisServer:
    """Serve the temporary line protocol to one active client at a time.

    ``start`` creates the listening socket synchronously. Keeping socket creation
    separate from ``serve_forever`` lets callers discover an operating-system-
    assigned port before starting the blocking accept loop.
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 6379) -> None:
        self._host = host
        self._port = port
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
        """Read complete CRLF-terminated commands from one TCP byte stream."""
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
                self._respond_to_complete_commands(connection, buffer)

    @staticmethod
    def _respond_to_complete_commands(
        connection: socket.socket, buffer: bytearray
    ) -> None:
        """Consume and respond to every complete command currently buffered."""
        while (terminator_index := buffer.find(COMMAND_TERMINATOR)) != -1:
            command = bytes(buffer[:terminator_index])
            del buffer[: terminator_index + len(COMMAND_TERMINATOR)]

            if command.upper() == PING_COMMAND:
                connection.sendall(PONG_RESPONSE)
            else:
                connection.sendall(UNKNOWN_COMMAND_RESPONSE)


def _close_socket(sock: socket.socket) -> None:
    """Close a socket while treating an already-closed socket as harmless."""
    with suppress(OSError):
        sock.shutdown(socket.SHUT_RDWR)
    sock.close()
