"""Restart, ordering, and failure boundaries of the durable mutation log."""

from concurrent.futures import ThreadPoolExecutor

import pytest

from mini_redis.commands import CommandError, CommandProcessor
from mini_redis.persistence import (
    AppendOnlyLog,
    CorruptLog,
    PersistenceError,
    encode_record,
)
from mini_redis.service import Service
from mini_redis.store import Store


def reopen(path, clock):
    store = Store(clock)
    log = AppendOnlyLog(path)
    try:
        log.replay(store)
    except Exception:
        log.close()
        raise
    return Service(store, log)


def test_binary_roundtrip_and_mutation_order(tmp_path, clock):
    path = tmp_path / "data.aof"
    service = reopen(path, clock)
    service.set(b"\x00\xff\r\n", b"\xff\x00\r\n")
    service.set(b"", b"")
    service.set(b"counter", b"5", 60)
    assert service.increment(b"counter") == 6
    service.set(b"deleted", b"old")
    assert service.delete((b"deleted", b"deleted", b"missing")) == 1
    service.set(b"plain", b"old", 1)
    service.set(b"plain", b"new")
    service.set(b"extended", b"value")
    service.expire(b"extended", 120)
    service.close()
    clock.advance(20)
    restored = reopen(path, clock)
    try:
        assert restored.get(b"\x00\xff\r\n") == b"\xff\x00\r\n"
        assert restored.get(b"") == b""
        assert restored.get(b"counter") == b"6"
        assert restored.ttl(b"counter") == 40
        assert restored.get(b"deleted") is None
        assert restored.get(b"plain") == b"new"
        assert restored.ttl(b"plain") == -1
        assert restored.ttl(b"extended") == 100
    finally:
        restored.close()


def test_expired_replacement_cannot_resurrect_older_value(tmp_path, clock):
    path = tmp_path / "data.aof"
    service = reopen(path, clock)
    service.set(b"key", b"old")
    service.set(b"key", b"temporary", 1)
    service.close()
    clock.advance(1)
    restored = reopen(path, clock)
    assert restored.get(b"key") is None
    restored.close()


def test_every_partial_tail_is_removed_before_future_appends(tmp_path, clock):
    complete = encode_record((b"PUT", b"first", b"value", b"-1"))
    tail = encode_record((b"PUT", b"partial", b"\x00\xff", b"-1"))
    path = tmp_path / "data.aof"
    for boundary in range(1, len(tail)):
        path.write_bytes(complete + tail[:boundary])
        service = reopen(path, clock)
        assert path.read_bytes() == complete
        assert service.get(b"first") == b"value"
        assert service.get(b"partial") is None
        service.set(b"next", b"ok")
        service.close()
        restored = reopen(path, clock)
        assert restored.get(b"next") == b"ok"
        restored.close()


@pytest.mark.parametrize(
    "bad",
    [
        b"garbage",
        b"*1\r\n$3\r\nBAD\r\n",
        encode_record((b"PUT", b"key", b"value", b"-2")),
        encode_record((b"PUT", b"key", b"value", b"1.5")),
        encode_record((b"DELETE",)),
    ],
)
def test_corruption_is_rejected_without_changing_file(tmp_path, clock, bad):
    path = tmp_path / "data.aof"
    original = bad + encode_record((b"PUT", b"later", b"v", b"-1"))
    path.write_bytes(original)
    with pytest.raises(CorruptLog):
        reopen(path, clock)
    assert path.read_bytes() == original


@pytest.mark.parametrize("operation", ["set", "delete", "increment", "expire"])
@pytest.mark.parametrize("failure", ["write", "short", "flush", "fsync"])
def test_disk_failure_preserves_memory_and_disables_writes(
    tmp_path, clock, monkeypatch, operation, failure
):
    store = Store(clock)
    log = AppendOnlyLog(tmp_path / "data.aof")
    service = Service(store, log)
    service.set(b"key", b"5", 60)
    original = log._file

    class FailingFile:
        closed = False

        def write(self, data):
            if failure == "write":
                original.write(data[:5])
                raise OSError("injected partial write")
            if failure == "short":
                return original.write(data[:5])
            return original.write(data)

        def flush(self):
            if failure == "flush":
                raise OSError("injected flush failure")
            original.flush()

        def fileno(self):
            return original.fileno()

        def close(self):
            original.close()
            self.closed = True

    log._file = FailingFile()
    if failure == "fsync":

        def fail_sync(fd):
            raise OSError("injected fsync failure")

        monkeypatch.setattr("mini_redis.persistence.os.fsync", fail_sync)
    operations = {
        "set": lambda: service.set(b"key", b"new"),
        "delete": lambda: service.delete((b"key",)),
        "increment": lambda: service.increment(b"key"),
        "expire": lambda: service.expire(b"key", 120),
    }
    try:
        with pytest.raises(PersistenceError):
            operations[operation]()
        assert service.get(b"key") == b"5"
        assert service.ttl(b"key") == 60
        for mutate in operations.values():
            with pytest.raises(PersistenceError):
                mutate()
        result = CommandProcessor(service).execute((b"SET", b"key", b"new"))
        assert isinstance(result, CommandError)
    finally:
        service.close()


def test_concurrent_acknowledged_increments_survive_restart(tmp_path, clock):
    path = tmp_path / "data.aof"
    service = reopen(path, clock)
    try:
        with ThreadPoolExecutor(max_workers=8) as pool:
            values = list(pool.map(lambda _: service.increment(b"counter"), range(80)))
        assert sorted(values) == list(range(1, 81))
    finally:
        service.close()
    restored = reopen(path, clock)
    assert restored.get(b"counter") == b"80"
    restored.close()


def test_reads_noops_and_invalid_commands_do_not_append(tmp_path, clock):
    path = tmp_path / "data.aof"
    service = reopen(path, clock)
    commands = CommandProcessor(service)
    service.set(b"key", b"not-an-integer", 1)
    before = path.read_bytes()
    for request in [
        (b"GET", b"key"),
        (b"TTL", b"key"),
        (b"INCR", b"key"),
        (b"SET", b"key", b"v", b"EX", b"0"),
        (b"DEL", b"missing"),
        (b"EXPIRE", b"missing", b"5"),
    ]:
        commands.execute(request)
    clock.advance(2)
    service.cleanup()
    assert path.read_bytes() == before
    service.close()


def test_memory_is_only_changed_after_sync(tmp_path, clock, monkeypatch):
    store = Store(clock)
    log = AppendOnlyLog(tmp_path / "data.aof")
    service = Service(store, log)
    service.set(b"key", b"old")
    original_sync = log._sync

    def inspect_sync():
        assert store.get(b"key") == b"old"
        original_sync()

    monkeypatch.setattr(log, "_sync", inspect_sync)
    service.set(b"key", b"new")
    assert store.get(b"key") == b"new"
    service.close()


def test_replay_is_independent_of_public_protocol_limits(tmp_path, clock):
    path = tmp_path / "data.aof"
    service = reopen(path, clock)
    value = b"x" * (1024 * 1024 + 1)
    service.set(b"large", value)
    service.close()
    restored = reopen(path, clock)
    assert restored.get(b"large") == value
    restored.close()
