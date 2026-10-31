"""Thread-per-client TCP server for Mini Redis."""

from __future__ import annotations

import json
import logging
import socket
import threading
from contextlib import suppress

from mini_redis.commands import CommandError, CommandProcessor
from mini_redis.config import Config
from mini_redis.persistence import AppendOnlyLog
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
        appendonly: str | None = None,
        idle_timeout: float = 300.0,
        expiration_interval: float = 0.1,
        shutdown_timeout: float = 2.0,
        config: Config | None = None,
    ) -> None:
        self.config = config or Config(
            host=host,
            port=port,
            max_clients=max_clients,
            appendonly=appendonly,
            max_buffer_size=max_buffer_size,
            max_bulk_string_size=max_bulk_string_size,
            max_array_length=max_array_length,
            idle_timeout=idle_timeout,
            expiration_interval=expiration_interval,
            shutdown_timeout=shutdown_timeout,
        )
        host, port, max_clients, appendonly = (
            self.config.host,
            self.config.port,
            self.config.max_clients,
            self.config.appendonly,
        )
        max_buffer_size = self.config.max_buffer_size
        max_bulk_string_size = self.config.max_bulk_string_size
        max_array_length = self.config.max_array_length
        self._close_lock = threading.Lock()
        self._close_started = False
        self._closed = threading.Event()
        self._close_error: Exception | None = None
        self._max_clients = max_clients
        self._host = host
        self._port = port
        self._store = store if store is not None else Store()
        self.metrics = self._store.metrics
        self._appendonly = appendonly
        self._service = Service(self._store)
        self._commands = CommandProcessor(self._service)
        self._maintenance: threading.Thread | None = None
        self._maintenance_error: Exception | None = None
        self._max_buffer_size = max_buffer_size
        self._max_bulk_string_size = max_bulk_string_size
        self._max_array_length = max_array_length
        self._listener: socket.socket | None = None
        self._clients: dict[socket.socket, threading.Thread] = {}
        self._clients_lock = threading.Lock()
        self._stopping = threading.Event()
        self._stop_requested = False

    @property
    def address(self) -> tuple[str, int]:
        """Return the address bound by ``start``."""
        if self._listener is None:
            raise RuntimeError("server has not been started")

        host, port = self._listener.getsockname()[:2]
        return str(host), int(port)

    def start(self) -> None:
        """Create, bind, and listen on the server socket."""
        if self._close_started:
            raise RuntimeError("closed server cannot be restarted")
        if self._listener is not None:
            raise RuntimeError("server has already been started")

        if self._appendonly is not None:
            log = AppendOnlyLog(self._appendonly, self.metrics)
            try:
                log.replay(self._store)
            except Exception:
                log.close()
                raise
            self._service = Service(self._store, log)
            self._commands = CommandProcessor(self._service)

        try:
            listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        except OSError:
            self._service.close()
            raise
        try:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind((self._host, self._port))
            listener.listen(self._max_clients)
            listener.settimeout(0.1)
        except OSError:
            listener.close()
            self._service.close()
            raise

        self._listener = listener
        self._maintenance = threading.Thread(
            target=self._cleanup_loop, name="mini-redis-cleanup", daemon=False
        )
        try:
            self._maintenance.start()
            logger.info(
                "server started address=%s max_clients=%d persistence=%s idle_timeout=%s",
                self.address,
                self._max_clients,
                self._appendonly is not None,
                self.config.idle_timeout,
            )
        except Exception:
            self._maintenance = None
            self.close()
            raise

    def _cleanup_loop(self) -> None:
        try:
            while not self._stopping.wait(self.config.expiration_interval):
                self._service.cleanup(batch_size=100)
        except Exception as error:
            self._maintenance_error = error
            logger.exception("expiration cleanup failed")
            self.request_stop()

    def serve_forever(self) -> None:
        """Accept clients and dispatch workers until ``close`` is called."""
        listener = self._listener
        if listener is None:
            raise RuntimeError("server must be started before it can serve")

        try:
            while not self._stopping.is_set() and not self._stop_requested:
                try:
                    connection, _ = listener.accept()
                except TimeoutError:
                    continue
                except OSError:
                    if self._stopping.is_set():
                        break
                    raise

                self.metrics.add("total_connections")
                connection.settimeout(self.config.idle_timeout)
                with self._clients_lock:
                    stopping = self._stopping.is_set() or self._stop_requested
                    admitted = not stopping and len(self._clients) < self._max_clients
                    if admitted:
                        worker = threading.Thread(
                            target=self._run_client,
                            args=(connection,),
                            name="mini-redis-client",
                            daemon=False,
                        )
                        self._clients[connection] = worker
                        self.metrics.add("current_connections")
                        try:
                            # Start under the registry lock so close() can only
                            # observe workers that are safe to join.
                            worker.start()
                        except Exception:
                            del self._clients[connection]
                            self.metrics.add("current_connections", -1)
                            _close_socket(connection)
                            raise
                if not admitted:
                    with connection:
                        if not stopping:
                            self.metrics.add("rejected_connections")
                            logger.warning("maximum clients reached")
                            # A rejected peer must not indefinitely stall accept.
                            connection.settimeout(0.1)
                            with suppress(OSError):
                                self._send(
                                    connection, encode_error("maximum clients reached")
                                )
            if self._maintenance_error is not None:
                raise OSError("expiration cleanup failed") from self._maintenance_error
        finally:
            self.close()

    def request_stop(self) -> None:
        """Signal handlers only request shutdown; the accept loop does the work."""
        self._stop_requested = True

    def close(self) -> None:
        """Wait a bounded time for one shared shutdown operation.

        A timed-out embedded caller must retain the process until the remaining
        non-daemon workers finish. The CLI exits unsuccessfully on this condition.
        """
        with self._close_lock:
            if not self._close_started:
                self._close_started = True
                self.request_stop()
                self._stopping.set()
                threading.Thread(
                    target=self._finish_close, name="mini-redis-shutdown", daemon=False
                ).start()
        if not self._closed.wait(self.config.shutdown_timeout):
            raise TimeoutError(
                "shutdown deadline exceeded; workers or persistence still busy"
            )
        if self._close_error is not None:
            raise self._close_error

    def _finish_close(self) -> None:
        logger.info("shutdown started")
        try:
            with self._clients_lock:
                listener = self._listener
                self._listener = None
                clients = list(self._clients.items())
            if listener is not None:
                _close_socket(listener)
            for connection, _ in clients:
                _close_socket(connection)
            if self._maintenance is not None:
                self._maintenance.join()
            for _, worker in clients:
                worker.join()
            self._service.close()
            logger.info(
                "shutdown complete counters=%s",
                json.dumps(self.metrics.snapshot(), sort_keys=True),
            )
        except Exception as error:
            self._close_error = error
            logger.exception("shutdown failed")
        finally:
            self._closed.set()

    def _run_client(self, connection: socket.socket) -> None:
        """Isolate client failures and always release the admission slot."""
        logger.debug("connection opened")
        try:
            self._serve_client(connection)
        except TimeoutError:
            self.metrics.add("write_timeouts")
            logger.info("connection write timeout")
        except OSError:
            pass  # Disconnects and sockets closed during shutdown are normal.
        except Exception:
            logger.exception("unexpected client worker failure")
        finally:
            _close_socket(connection)
            with self._clients_lock:
                del self._clients[connection]
                self.metrics.add("current_connections", -1)
            logger.debug("connection closed")

    def _serve_client(self, connection: socket.socket) -> None:
        """Decode and execute complete RESP2 requests from one TCP byte stream."""
        buffer = bytearray()

        with connection:
            while not self._stopping.is_set() and not self._stop_requested:
                try:
                    chunk = connection.recv(4096)
                except TimeoutError:
                    self.metrics.add("idle_timeouts")
                    logger.info("connection idle timeout")
                    return
                except OSError:
                    if self._stopping.is_set():
                        return
                    raise

                if chunk == b"":
                    return

                self.metrics.add("bytes_read", len(chunk))
                buffer.extend(chunk)
                if not self._respond_to_complete_requests(connection, buffer):
                    return

    def _respond_to_complete_requests(
        self, connection: socket.socket, buffer: bytearray
    ) -> bool:
        """Respond to buffered requests, returning false after a protocol error."""
        while not self._stopping.is_set() and not self._stop_requested:
            try:
                parsed = decode_request(
                    buffer,
                    max_buffer_size=self._max_buffer_size,
                    max_bulk_string_size=self._max_bulk_string_size,
                    max_array_length=self._max_array_length,
                )
            except ProtocolError as error:
                self.metrics.add("protocol_errors")
                logger.warning("protocol violation: %s", error)
                self._send(connection, encode_protocol_error(error))
                return False

            if isinstance(parsed, IncompleteRequest):
                return True
            if not isinstance(parsed, DecodedRequest):
                raise AssertionError(f"unsupported parse result: {parsed!r}")

            del buffer[: parsed.bytes_consumed]
            command = parsed.request[0].upper() if parsed.request else b""
            name = (
                command.decode("ascii")
                if command
                in {
                    b"PING",
                    b"SET",
                    b"GET",
                    b"DEL",
                    b"EXISTS",
                    b"INCR",
                    b"EXPIRE",
                    b"TTL",
                }
                else "UNKNOWN"
            )
            self.metrics.add("commands_" + name)
            result = self._commands.execute(parsed.request)
            if isinstance(result, CommandError):
                self.metrics.add("command_errors")
            self._send(connection, encode_response(result))
        return False

    def _send(self, connection: socket.socket, data: bytes) -> None:
        connection.sendall(data)
        self.metrics.add("bytes_written", len(data))


def _close_socket(sock: socket.socket) -> None:
    """Close a socket while treating an already-closed socket as harmless."""
    with suppress(OSError):
        sock.shutdown(socket.SHUT_RDWR)
    with suppress(OSError):
        sock.close()
