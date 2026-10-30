"""Binary-safe append-only mutations, with synchronous durability and replay."""

from __future__ import annotations

import logging
import os
import re
import sys
from pathlib import Path

from mini_redis.store import Entry, Store

logger = logging.getLogger(__name__)


class PersistenceError(OSError):
    """The durable log cannot safely accept more mutations."""


class CorruptLog(PersistenceError):
    """A complete log record is malformed or invalid."""


def encode_record(parts: tuple[bytes, ...]) -> bytes:
    return b"*%d\r\n" % len(parts) + b"".join(
        b"$%d\r\n" % len(part) + part + b"\r\n" for part in parts
    )


class AppendOnlyLog:
    """Owned by one service; calls are serialized by its operation lock."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._file = self.path.open("a+b")
        self._failed = False
        try:
            # Persist the directory entry as well as contents before serving writes.
            directory = os.open(self.path.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        except OSError:
            self._file.close()
            raise

    def replay(self, store: Store) -> None:
        # Import here to keep wire response types out of the service dependency graph.
        from mini_redis.protocol import (
            IncompleteRequest,
            ProtocolError,
            decode_request,
        )

        self._file.seek(0)
        buffer = bytearray()
        valid_end = 0
        count = 0
        while True:
            chunk = self._file.read(65536)
            buffer.extend(chunk)
            while buffer:
                try:
                    parsed = decode_request(
                        buffer,
                        max_buffer_size=sys.maxsize,
                        max_bulk_string_size=sys.maxsize,
                        max_array_length=sys.maxsize,
                    )
                except ProtocolError as error:
                    raise CorruptLog(
                        f"invalid log framing at byte {valid_end}"
                    ) from error
                if isinstance(parsed, IncompleteRequest):
                    break
                self._apply_record(store, parsed.request, valid_end)
                del buffer[: parsed.bytes_consumed]
                valid_end += parsed.bytes_consumed
                count += 1
            if not chunk:
                break
        if buffer:
            # Remove the suffix before appending, otherwise it would hide later records.
            self._file.truncate(valid_end)
            self._sync()
            logger.warning("recovered incomplete log suffix at byte %d", valid_end)
        self._file.seek(0, os.SEEK_END)
        logger.info("replayed %d persistence records", count)

    @staticmethod
    def _apply_record(store: Store, parts: tuple[bytes, ...], offset: int) -> None:
        if len(parts) == 4 and parts[0] == b"PUT":
            raw = parts[3]
            if raw == b"-1":
                expiration = None
            elif len(raw) <= 19 and re.fullmatch(rb"(?:0|[1-9][0-9]*)", raw):
                expiration = int(raw) / 1000
            else:
                raise CorruptLog(f"invalid expiration at byte {offset}")
            store.restore(parts[1], parts[2], expiration)
        elif len(parts) >= 2 and parts[0] == b"DELETE":
            store.delete(parts[1:])
        else:
            raise CorruptLog(f"invalid mutation at byte {offset}")

    def ensure_writable(self) -> None:
        if self._failed or self._file.closed:
            raise PersistenceError("persistence unavailable; mutations disabled")

    def put(self, key: bytes, entry: Entry) -> None:
        expiration = (
            b"-1"
            if entry.expires_at is None
            else str(int(entry.expires_at * 1000)).encode("ascii")
        )
        self.append((b"PUT", key, entry.value, expiration))

    def append(self, parts: tuple[bytes, ...]) -> None:
        self.ensure_writable()
        record = encode_record(parts)
        try:
            if self._file.write(record) != len(record):
                raise OSError("short append-only write")
            self._sync()
        except OSError as error:
            self._failed = True
            logger.exception("persistence write failed; mutations disabled")
            raise PersistenceError(
                "persistence write failed; mutations disabled"
            ) from error

    def _sync(self) -> None:
        self._file.flush()
        os.fsync(self._file.fileno())

    def close(self) -> None:
        self._file.close()
