# Mini Redis

Mini Redis is a small Redis-inspired server built as a learning project. Its
goal is to explore TCP servers, streaming protocols, concurrency, expiration,
persistence, failure handling, testing, and observability without attempting to
reproduce all of Redis.

The behavior and implementation sequence are defined in [PLAN.md](PLAN.md).

## Current status: Milestone 6

Mini Redis currently provides:

- a Python 3.12+ package using a `src` layout;
- project and dependency management with `uv`;
- a thread-per-client TCP server listening on `127.0.0.1:6379` by default;
- an incremental RESP2 array-of-bulk-strings request parser;
- RESP2 simple-string, error, integer, bulk-string, and null response encoding;
- correct buffering when requests are split across arbitrary TCP reads;
- pipelined execution of multiple requests received together;
- binary-safe keys and values, including empty byte strings;
- per-connection buffer, bulk-string, and request-array limits;
- case-insensitive command matching and an error response for unknown commands;
- an in-memory byte-string store shared across concurrent connections;
- socket-independent command handling for `PING`, `SET`, `GET`, `DEL`, `EXISTS`,
  `INCR`, `EXPIRE`, and `TTL`;
- compatibility with `redis-cli` for the supported command subset;
- a shared service lock making each keyspace operation atomic;
- a configurable maximum of 100 active clients by default;
- lazy expiration and bounded background cleanup using an injectable clock;
- optional append-only persistence with synchronous disk writes and startup replay;
- tests with pytest; and
- formatting and linting with Ruff.

Each connection has its own worker and input buffer. An idle client does not
block other clients. Commands remain ordered within a connection; ordering
between connections is unspecified. Key expiration and optional persistence are supported. This project is not production-ready and must not be exposed to
untrusted networks.

## Development

Create the environment and install development dependencies:

```shell
uv sync --dev
```

Inspect the command-line interface and start the server:

```shell
uv run mini-redis --help
uv run mini-redis
```

From another terminal, send a RESP2-encoded `PING` with netcat:

```shell
printf '*1\r\n$4\r\nPING\r\n' | nc 127.0.0.1 6379
```

The response should be `+PONG`. Press Ctrl-C in the server terminal to stop it.
If `redis-cli` is installed locally, the equivalent compatibility check is:

```shell
redis-cli -h 127.0.0.1 -p 6379 PING
```

### Host and port

The bind address and port can be selected from the command line:

```shell
uv run mini-redis --host 127.0.0.1 --port 6380
```

`--host` must resolve to a usable local IPv4 bind address, or it may be the
wildcard address `0.0.0.0`. The current server creates an IPv4 socket, so IPv6
addresses such as `::1` are not supported. Keep the default `127.0.0.1` for
local development. Binding to a LAN address or `0.0.0.0` can expose this
unauthenticated server to other computers.

`--port` accepts values from `0` through `65535`. Port `0` asks the operating
system to select an available ephemeral port; the server prints the selected
port after it starts. Other ports must be available, and privileged ports may
require additional operating-system permissions.

### RESP2 subset

Requests are RESP2 arrays containing bulk strings. Command names are matched
case-insensitively; keys and values are opaque bytes. For example,
`SET greeting hello` is encoded as:

```text
*3\r\n
$3\r\n
SET\r\n
$8\r\n
greeting\r\n
$5\r\n
hello\r\n
```

The server accepts fragmented and pipelined requests. A malformed frame receives
a RESP2 error when possible and closes that connection. A well-formed request
with an unknown command or invalid argument count receives an error while
keeping the connection usable.

The initial per-connection limits are a 1 MiB unread-input buffer, 512 KiB bulk
strings, and 1,024 request-array elements. These learning-oriented limits are
not Redis-compatible limits.

Run the quality checks:

```shell
uv run pytest
uv run ruff check .
uv run ruff format --check .
```

Apply automatic formatting and safe lint fixes with:

```shell
uv run ruff format .
uv run ruff check --fix .
```

### Concurrent clients and atomic operations

The command processor validates requests, then calls a shared `Service`. The
service owns the store and holds one lock for each complete operation, including
multi-key operations and the entire read/modify/write sequence of `INCR`.
Socket reads and writes occur outside that lock. Worker failures are isolated,
and disconnects release their client slot. Closing the server unblocks client
sockets and joins workers with a shared two-second wait budget. Full
signal-driven graceful shutdown remains milestone 7.

Set a different client limit with:

```shell
uv run mini-redis --port 6380 --max-clients 20
```

When all slots are occupied, an additional connection receives
`-ERR maximum clients reached` and closes. Existing clients keep working. The
limit must be a positive integer. Idle timeouts are not implemented yet, so an
idle connection occupies its slot until it disconnects or the server closes.

`EXISTS key [key ...]` counts supplied existing keys, including duplicates.
For example, if `key` exists, `EXISTS key key missing` returns `2`.

`INCR key` treats a missing key as zero, increments it, stores decimal bytes,
and returns the new integer. Existing values must use canonical signed decimal
notation: `0`, or an optional minus followed by a nonzero digit and further
digits. Leading zeros, `+`, `-0`, whitespace, decimal points, and non-ASCII digits
are rejected. Both the existing value and result must fit signed 64-bit range.
Errors leave the key unchanged.

The concurrency tests coordinate clients with barriers/events and verify 800
increments across eight clients without lost updates, continued progress with
an idle client or blocked response writer, client-limit enforcement and slot
reuse, and worker cleanup on shutdown.

### Key expiration

Use `SET key value EX seconds` to store a value with a lifetime, `EXPIRE key
seconds` to assign or replace a lifetime, and `TTL key` to inspect it. For example:

```shell
redis-cli SET session hello EX 60
redis-cli TTL session
redis-cli EXPIRE session 120
```

Expiration arguments accept 1–10 ASCII digits, including leading zeros, with a
numeric value from 1 through 2,147,483,647 seconds. Signs, whitespace, zero,
negative values, and fractions are rejected without changing the key. `EX` is
case-insensitive; other SET options are unsupported. These are intentional
Mini Redis limits rather than full Redis compatibility.

`TTL` rounds remaining seconds **up**, returns `-1` for a key with no expiration,
and `-2` for a missing or expired key. At the exact deadline the key is expired.
Plain `SET` clears an old expiration; `SET ... EX` replaces it; successful `INCR`
preserves it. Incrementing an expired key creates a value of `1` with no TTL.

Live expiration uses a monotonic clock, so calendar-clock changes do not change
an existing lifetime. Entries also record a Unix expiration timestamp for future
persistence. Without `--appendonly`, data is entirely in memory and lost on restart.
Every key operation checks expiration. A maintenance thread additionally inspects
up to 100 entries every 100 milliseconds, rotating through the store and taking
the service lock for just one batch. Cleanup reclaims memory; correctness does
not depend on it running promptly. Server close stops and joins this worker.
Expiration tests advance a fake clock instead of waiting for real TTLs.

### Append-only persistence

Enable persistence by choosing a log file (its parent directory must exist):

```shell
uv run mini-redis --appendonly ./mini-redis.aof
```

Restart with the same path to recover data before the server accepts connections.
Without this option the server remains memory-only. Use one server process per
log file; concurrent processes sharing a file are unsupported.

Successful mutations append binary-safe RESP2 records using internal `PUT` and
`DELETE` operations. `PUT` records contain the resulting value and an absolute
Unix expiration in milliseconds (`-1` means no expiration). These operations
are not public commands. Reads, failed validation, missing-key no-ops, and
expiration cleanup do not append records.

The service lock covers preparing the mutation, writing its record, flushing,
calling `fsync`, and applying the result to memory. A success response follows
these steps. Every mutation pays the disk-sync cost, and other keyspace commands
wait while it completes. Persisted expiration is rounded down to milliseconds;
replay uses wall-clock time to account for downtime, then resumes monotonic
expiration. An expired replacement also removes any older value during replay.

An incomplete final record is logged and truncated before new records are
appended. Malformed framing or invalid complete records cause startup to fail;
the log is not silently repaired. Back up a corrupt log before manual recovery.
The format has no checksum, so corruption that still forms valid records is not
detectable. The log grows indefinitely; compaction is not implemented.

If append, flush, or `fsync` fails, memory is not changed by that command and all
further mutations are rejected until restart. Reads remain available. A failed
or unacknowledged mutation may nevertheless appear after restart if its record
reached disk: callers must not assume an error proves the write never happened.
This is not an exactly-once guarantee or a hardware-level durability guarantee.

Closing the server closes the log under the service lock. An in-progress disk
operation must finish before the file can close; a stalled filesystem can exceed
the existing worker join budget. Full signal handling and shutdown hardening
remain milestone 7.
