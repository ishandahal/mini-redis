# Mini Redis

Mini Redis is a small Redis-inspired server built as a learning project. Its
goal is to explore TCP servers, streaming protocols, concurrency, expiration,
persistence, failure handling, testing, and observability without attempting to
reproduce all of Redis.

**Development note:** This project is almost entirely vibe coded, and the
codebase has not been thoroughly scrutinized. Treat it as a learning experiment,
not carefully reviewed or production-ready software.

The behavior and implementation sequence are defined in [PLAN.md](PLAN.md).
See [the architecture walkthrough and invariant coverage](docs/ARCHITECTURE.md)
for how the finished code fits together. “Version 1” refers to the learning
scope; the package version remains `0.1.0`.

## Current status: Version 1 scope complete (Milestones 0–8)

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
- validated configuration, idle timeouts, signal-driven shutdown, and operational counters;
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
sockets and waits for workers and persistence with a configurable two-second
grace period. See operational behavior below.

Set a different client limit with:

```shell
uv run mini-redis --port 6380 --max-clients 20
```

When all slots are occupied, an additional connection receives
`-ERR maximum clients reached` and closes. Existing clients keep working. The
limit must be a positive integer. Idle connections are closed after 300 seconds by default, releasing their slots.

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
an existing lifetime. Entries also record a Unix expiration timestamp for
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

Closing the server closes the log under the service lock after workers finish.
An in-progress disk operation must finish before the file can close. The shutdown
policy below explicitly handles an exceeded grace period.


### Operational behavior

`SIGINT` (Ctrl-C) and `SIGTERM` request the same idempotent shutdown. The listener
checks the request within its 100 ms accept polling interval, stops admission,
and closes client sockets to unblock reads and writes. Already executing commands
finish, while further pipelined commands are not started. A command may finish
without its client receiving the reply. Cleanup and client threads are joined,
then the append-only file closes and the server logs a final counter summary.

The grace period defaults to two seconds. Python cannot cancel a thread blocked
inside filesystem I/O. If the grace period expires, the CLI logs a critical error
and forces process exit with status 1; this is an unsuccessful shutdown, with the
same recovery/acknowledgment ambiguity as a crash. Normal shutdown exits with
status 0. OS-level termination timing remains outside the application's control.
For embedded use, `close()` raises `TimeoutError` instead of killing the process;
non-daemon cleanup continues and callers can wait or retry `close()`. A closed
server instance cannot be restarted; create a new instance.

Configuration is validated by an immutable `Config` object before serving clients.
Pass `config=Config(...)` when embedding (it takes precedence over constructor
settings), or use these CLI options:

| Option | Default | Purpose |
| --- | --- | --- |
| `--idle-timeout` | `300` | Seconds waiting for more socket input; also bounds response writes |
| `--expiration-interval` | `0.1` | Seconds between bounded cleanup passes |
| `--shutdown-timeout` | `2` | Seconds allowed for coordinated shutdown |
| `--max-buffer-size` | `1048576` | Maximum unread request bytes per client |
| `--max-bulk-string-size` | `524288` | Maximum bytes in one bulk string (zero is allowed) |
| `--max-array-length` | `1024` | Maximum request elements |
| `--log-level` | `INFO` | DEBUG, INFO, WARNING, ERROR, or CRITICAL |

Durations must be finite, positive, and no greater than the smaller of
`threading.TIMEOUT_MAX` and 2,147,483,647 seconds. Receiving more bytes resets the idle wait;
this is not a total request deadline. Executing commands are not timed out by the
socket timeout. A client that stops reading responses is disconnected if a write
times out. Expiration still uses the injected clock independently of socket waits.

Logs include timestamps, severity, component, and event details. They explain
startup, replay, recovery, limits, protocol errors, timeouts, persistence errors,
and shutdown. Connection lifecycle messages are at DEBUG. Stored keys/values and
unrecognized command text are not included in normal logs.

Thread-safe counters are available from `server.metrics.snapshot()` and logged
as JSON at normal shutdown. Total connections includes rejected connections;
current connections counts admitted workers. Commands are counted by supported
name, with unknown names grouped into `UNKNOWN` to bound memory use. Keyspace
hits/misses count GET results. Expired-key removal counts lazy and active cleanup;
persistence records count successfully synchronized new records, excluding replay.
Byte reads count received input; byte writes count completed response writes
(including rejection/error replies), excluding partial failed writes. Counters
also track command/protocol errors, rejected connections, and read/write timeouts.


## Self-contained demonstration

After `uv sync --dev`, run:

```shell
uv run python scripts/demo.py
```

The script launches its own server processes on OS-assigned loopback ports and
uses a temporary append-only file. It demonstrates fragmented requests, binary
values, pipelining, command errors, four concurrent incrementing clients, a real
one-second expiration, SIGTERM shutdown, and recovery after restart. Each step
checks its responses and prints its result. It requires no redis-cli or existing
server and removes its temporary files at the end. Failures print server logs
and produce a nonzero exit. The signal demo targets Unix-like systems.

## Limitations and failure model

- **Local learning use:** no authentication, ACLs, or TLS. All connected clients
  share one keyspace. IPv4 only. Do not expose the server to untrusted networks.
- **A deliberate Redis subset:** byte-string values only, eight commands, and
  restricted RESP2 requests. TTL rounds up; expiration accepts only positive
  whole seconds within the documented range. This is not full Redis compatibility.
- **Resource limits are partial:** clients and requests have limits, but total
  keyspace memory and append-only file size do not. No eviction or compaction.
  An idle timeout is not a total request deadline; a trickling client can retain
  its slot. Fragmented input is reparsed, so adversarial input can consume CPU.
- **Persistence has explicit boundaries:** one process per log, no file locking,
  checksums, snapshots, or replication. Replay treats the local log as trusted and
  does not apply public request-size limits. Corruption that forms valid records
  is not detectable. Back up a damaged file before manual recovery.
- **Acknowledgments can be ambiguous:** synchronous flush/fsync precedes success,
  but a durable write may outlive a missing/error response. Errors disable further
  mutations, not reads. Retrying INCR after a lost reply can increment twice.
  Memory-only mode loses all state on restart; fsync is not a hardware guarantee.
- **Shutdown can fail:** active operations may complete after sockets close. A
  grace-period timeout forces CLI exit status 1 and uses crash-recovery semantics;
  it cannot make a blocked disk operation finish safely. Startup replay itself
  has no deadline. Embedded callers must manage unfinished cleanup on timeout.
- **Throughput is intentionally limited:** thread per client and one coarse
  service lock, including synchronous disk writes. No performance target or
  guarantee of fairness. No broad cross-platform or hardware-failure validation.

Deferred extensions are authentication/ACLs, pub/sub, transactions, snapshots,
an asynchronous transport, more data types, and alternative fsync policies with
benchmarks. Replication and clustering are outside this version's scope. See
[PLAN.md section 17](PLAN.md#17-stretch-goals) before adding further features.
