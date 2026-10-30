# Mini Redis: Implementation Plan and Specification

- **Status:** Active living specification; Milestones 0–6 complete
- **Target:** Version 1 learning implementation
- **Language:** Python 3.12+
- **Last updated:** 2026-10-30

Sections 4–15 describe the target version 1 system, not only the features that
exist today. Section 16 is the implementation tracker. Completed milestones are
marked with checked boxes; later milestones are labeled `Not started`.

## 1. Purpose

Mini Redis is a small Redis-inspired server built to learn backend and systems
engineering. It is not intended to replace Redis or reproduce its full command
set. The project should favor understandable behavior, explicit invariants, and
testable components over feature count or benchmark performance.

The finished system will be a long-running process that:

- listens for TCP connections;
- accepts commands using a useful subset of RESP2;
- stores byte-string keys and values in shared memory;
- serves multiple clients concurrently;
- supports key expiration;
- persists mutations in an append-only file;
- handles invalid input and expected failures without crashing;
- shuts down cleanly; and
- exposes useful logs and basic operational counters.

The primary deliverable is understanding. Every major component should have a
clear responsibility, documented invariants, and tests that demonstrate both
normal and failure behavior.

## 2. Product Mental Model

Mini Redis is a separate data service:

```text
application process
       |
       | TCP connection carrying RESP2 messages
       v
Mini Redis server
       |
       +-- shared in-memory keyspace
       +-- expiration metadata
       `-- append-only persistence file
```

All connected clients share one keyspace. Data belongs to the server, not to a
connection. A client may disconnect and another client may retrieve or modify
the same key.

The server initially binds only to `127.0.0.1`. Version 1 has no authentication
or authorization. Consequently, any process on the same computer that can
connect to the configured port can access all data. This is appropriate for a
local learning project, but not for an internet-facing deployment.

In a typical application, the Redis client is a trusted backend process, not
the human using a website or mobile app. The backend authenticates the human,
authorizes the requested application operation, and then uses its own Redis
connection. A Redis key identifies data; knowing a key is not proof of
authorization. Mini Redis version 1 deliberately does not implement this
application security boundary and must not be exposed to untrusted networks.

## 3. Learning Objectives

By completing version 1, the implementer should be able to explain:

1. Why TCP is a byte stream rather than a message transport.
2. How a streaming parser handles fragmented and coalesced input.
3. How protocol, command, storage, persistence, and transport concerns differ.
4. How shared mutable state creates races between clients.
5. What makes a command atomic and where the atomicity boundary lives.
6. How TTL behavior depends on clock choice and expiration strategy.
7. What an append-only log can and cannot guarantee after a crash.
8. How a server coordinates listener shutdown, active connections, worker
   threads, and pending disk writes.
9. How resource limits, logging, and error handling affect operability.
10. How unit, integration, concurrency, and failure tests complement each
    other.

## 4. Version 1 Scope

### 4.1 Required commands

| Command | Required behavior |
| --- | --- |
| `PING` | Return `PONG`. |
| `SET key value` | Store or replace a value and clear any existing TTL. |
| `SET key value EX seconds` | Store or replace a value with a positive integer TTL. |
| `GET key` | Return the value, or null if the key is absent or expired. |
| `DEL key [key ...]` | Delete keys and return the number actually removed. |
| `EXISTS key [key ...]` | Return how many supplied keys currently exist. |
| `EXPIRE key seconds` | Assign a positive integer TTL; return whether it was assigned. |
| `TTL key` | Return remaining whole seconds, `-1` for no TTL, or `-2` if absent. |
| `INCR key` | Atomically increment a base-10 signed integer stored as bytes. |

Command names are ASCII and case-insensitive. Keys and values are otherwise
opaque byte strings. Version 1 does not interpret values as UTF-8 except when a
command such as `INCR` explicitly requires a textual integer representation.
Empty keys and empty values are valid.

### 4.2 Required server behavior

- Accept multiple simultaneous TCP clients.
- Preserve command order within each connection.
- Support multiple commands sent without waiting for earlier replies.
- Handle requests split across arbitrary TCP reads.
- Handle several requests received in one TCP read.
- Keep one shared keyspace across all connections.
- Never expose an expired key as present.
- Recover acknowledged mutations from the append-only file subject to the
  documented failure model.
- Reject invalid commands and arguments without terminating the server.
- Apply resource limits to untrusted protocol input.
- Shut down gracefully on `SIGINT` and `SIGTERM`.

### 4.3 Explicit non-goals

Version 1 will not provide:

- production readiness or a security guarantee;
- full Redis or RESP compatibility;
- Redis data structures beyond byte strings;
- authentication or ACLs;
- TLS;
- replication, clustering, sharding, or failover;
- snapshots in Redis RDB format;
- eviction under memory pressure;
- Lua scripting, modules, streams, or keyspace notifications;
- Redis performance characteristics;
- zero-downtime upgrades; or
- compatibility with every Redis edge case.

## 5. Architecture

The intended dependency direction is:

```text
CLI/configuration
      |
      v
TCP server/connection workers
      |
      v
RESP parser and encoder
      |
      v
command validation and execution
      |
      v
atomic service boundary
      |                \
      v                 v
in-memory store     append-only log
      |
      v
injected clock
```

### 5.1 Component responsibilities

#### TCP server

- Own the listening socket and active client sockets.
- Accept connections and create one worker thread per client.
- Read bytes, retain incomplete input, and pass complete requests to the
  protocol layer.
- Write complete encoded responses, accounting for partial socket writes.
- Apply connection-level timeouts and input-buffer limits.
- Coordinate graceful shutdown.

It must not contain storage rules or command-specific mutation logic.

#### RESP codec

- Incrementally decode supported RESP2 request values from a byte buffer.
- Distinguish an incomplete request from an invalid request.
- Encode supported response values.
- Enforce nesting, array-length, bulk-length, and buffer limits.

It must not execute commands or access the store.

#### Command layer

- Convert a decoded request into a recognized command and validated arguments.
- Define command arity and option rules.
- Convert domain results and domain errors into protocol-independent response
  values.

It must not read sockets or parse raw RESP framing.

#### Service/atomicity boundary

- Serialize operations that must appear atomic.
- Coordinate mutation order between the store and append-only log.
- Ensure concurrent clients cannot observe partial mutations.
- Make mutation ordering explicit and testable.

Version 1 may use one coarse-grained `threading.Lock`. Correctness and clarity
take priority over parallel throughput.

#### Store

- Own key/value entries and expiration metadata.
- Provide operations used by commands rather than exposing its dictionary.
- Remove or ignore expired entries before answering any operation.
- Use the injected clock rather than calling time functions directly.

It must not know about TCP, RESP, client connections, or log files.

#### Append-only log

- Append canonical mutation records.
- Flush records according to the configured durability policy.
- Replay valid records at startup.
- Distinguish a truncated final record from corruption in the middle of a file.
- Close cleanly during shutdown.

It must not interpret external client permissions or send responses.

#### Clock

- Supply monotonic time for expiration during a running process.
- Supply wall-clock Unix time for expiration persisted across restarts.
- Be replaceable with a deterministic fake in tests.

### 5.2 Suggested source layout

The exact filenames may evolve, but responsibilities should remain separated:

```text
pyproject.toml
src/mini_redis/
    __init__.py
    __main__.py
    config.py
    server.py
    protocol.py
    commands.py
    service.py
    store.py
    persistence.py
    clock.py
    errors.py
tests/
    unit/
    integration/
```

Runtime code should use the Python standard library. `pytest` is the initial
development dependency.

## 6. Protocol Specification

### 6.1 Milestone protocol versus final protocol

The first networking milestone may use a disposable CRLF-terminated line
protocol for `PING`. Its purpose is to expose socket lifecycle and buffering
without simultaneously building RESP.

The version 1 public protocol is a subset of RESP2. Once RESP support exists,
the line protocol should be removed rather than maintained as a second public
interface.

### 6.2 Accepted requests

Clients send a RESP2 array containing bulk strings only. The first element is
the command name; remaining elements are arguments.

For example, `SET greeting hello` is encoded as:

```text
*3\r\n
$3\r\n
SET\r\n
$8\r\n
greeting\r\n
$5\r\n
hello\r\n
```

The bytes above are one logical request. TCP may deliver them in any number of
reads.

### 6.3 Encoded responses

The server supports these RESP2 response forms:

- simple strings, such as `+OK\r\n` and `+PONG\r\n`;
- errors, such as `-ERR unknown command\r\n`;
- integers, such as `:1\r\n`;
- bulk strings;
- null bulk strings, encoded as `$-1\r\n`.

### 6.4 Parsing contract

For any current input buffer, the parser must produce exactly one of:

1. one complete decoded request and the unconsumed remainder;
2. an `IncompleteRequest` result without consuming bytes; or
3. a protocol error.

Repeated parsing of the remainder enables pipelining. Receiving more bytes
after an incomplete result enables fragmentation handling.

A malformed RESP frame produces an error response and closes that connection.
A valid RESP request containing an invalid command or arguments produces an
error response but leaves the connection usable.

### 6.5 Initial limits

Defaults should be configurable, but version 1 begins with:

- maximum buffered unread input per connection: 1 MiB;
- maximum bulk-string length: 512 KiB;
- maximum request array length: 1,024 elements;
- maximum aggregate nesting: one level, because requests must be arrays of bulk
  strings; and
- maximum simultaneous clients: 100.

Crossing a protocol input limit returns an error when possible and closes the
connection. These are learning-oriented defaults, not Redis-compatible limits.

## 7. Command Semantics

### 7.1 General rules

- Validation completes before a mutation begins.
- A command either completes as one atomic operation or returns an error.
- Errors do not partially mutate state.
- Extra or missing arguments are errors.
- Unknown command names are errors.
- Expired keys behave exactly like absent keys to public commands.

### 7.2 `PING`

```text
PING
```

- Accept no arguments in version 1.
- Return the RESP simple string `PONG`.

### 7.3 `SET`

```text
SET key value
SET key value EX seconds
```

- `seconds` must contain 1–10 ASCII digits and represent a value from 1 through
  2,147,483,647; leading zeros are accepted.
- A successful `SET` returns `OK`.
- Replacing a key replaces both its value and expiration metadata.
- `SET` without `EX` removes an existing TTL.
- Value replacement and TTL assignment are one atomic mutation.

### 7.4 `GET`

```text
GET key
```

- Return the stored bytes as a bulk string.
- Return a null bulk string when the key is absent or expired.

### 7.5 `DEL`

```text
DEL key [key ...]
```

- Accept at least one key.
- Return the count of existing, unexpired keys removed.
- Repeated occurrences of the same key count at most once because it can only
  be removed once.

### 7.6 `EXISTS`

```text
EXISTS key [key ...]
```

- Accept at least one key.
- Return the number of supplied keys that exist and are unexpired.
- As in Redis, repeated occurrences of an existing key are counted repeatedly.

### 7.7 `EXPIRE`

```text
EXPIRE key seconds
```

- `seconds` must contain 1–10 ASCII digits and represent a value from 1 through
  2,147,483,647; leading zeros are accepted.
- Return `1` when a TTL is assigned to an existing, unexpired key.
- Return `0` when the key does not exist or is already expired.

### 7.8 `TTL`

```text
TTL key
```

- Return `-2` when the key is absent or expired.
- Return `-1` when the key exists without an expiration.
- Otherwise return the remaining positive number of seconds, rounded up.
- A key whose deadline has been reached is expired, regardless of rounding.

### 7.9 `INCR`

```text
INCR key
```

- A missing key is treated as the integer zero.
- The existing value must be the canonical base-10 representation of a signed
  integer accepted by the command parser.
- The result must remain within signed 64-bit range.
- Invalid or overflowing values return an error without changing the key.
- A successful increment preserves an existing TTL.
- Return the new value as a RESP integer.

The exact accepted integer grammar must be captured in tests before
implementation. At minimum, optional leading `-` and decimal digits are
supported; whitespace and decimal points are not.

## 8. Expiration Model

Each entry contains:

```text
value: bytes
monotonic deadline: optional float
wall-clock expiration: optional Unix timestamp
```

The monotonic deadline governs expiration while the process is running, so a
wall-clock adjustment does not unexpectedly extend or shorten a live TTL. The
wall-clock expiration is persisted. During replay, the remaining duration is
calculated from wall-clock time and converted into a new monotonic deadline.

Expiration has two mechanisms:

1. **Lazy expiration:** every operation touching a key checks its deadline and
   removes it if expired. This is required for observable correctness.
2. **Active cleanup:** a background maintenance task periodically removes
   expired entries to reclaim memory. This is best-effort and must not be
   required for correctness.

Active cleanup may initially scan the dictionary because simplicity matters
more than asymptotic performance. A heap or sampling strategy is a possible
later optimization, to be justified by measurement. Cleanup must acquire the
same state lock used by commands, and it should release that lock between
bounded batches so maintenance cannot monopolize the server.

## 9. Concurrency and Atomicity

Version 1 uses a thread per client and a shared store. A coarse-grained service
lock establishes the following guarantees:

- Each command appears to take effect at one point between request receipt and
  response transmission.
- Mutating commands have a single total order across all clients.
- `INCR` is a read-modify-write operation under one lock.
- `SET ... EX ...` cannot expose a new value with an old or missing TTL between
  its internal steps.
- Persistence order matches mutation order.
- A connection's commands execute in request order.

The server does not promise a deterministic order between commands arriving on
different connections. Tests must coordinate threads with barriers or events
rather than relying on timing sleeps.

Locking should be introduced only around shared state and persistence ordering.
Socket reads and writes must not occur while holding the global service lock.

## 10. Persistence Specification

### 10.1 Append-only strategy

Persistence is optional through configuration. When enabled, each successful
client-initiated mutation is represented as one canonical, binary-safe record
in an append-only file. Lazy and active removal of already-expired keys is not
logged because the original absolute expiration remains sufficient during
replay. Records use RESP2 framing but an internal operation vocabulary:

- `PUT key value expiration_unix_ms`, where `-1` means no expiration; and
- `DELETE key [key ...]`.

The persistence format is internal and is not part of the public client command
set. Canonical records describe resulting mutations rather than preserving the
client's original spelling or relative TTL.

### 10.2 Mutation ordering

For a validated mutation:

1. Acquire the service lock.
2. Determine the canonical mutation and absolute expiration.
3. Append the complete record.
4. Flush according to the durability policy.
5. Apply the mutation to memory.
6. Release the lock.
7. Send the response.

If appending or flushing fails, the in-memory mutation is not applied and the
client receives an error. Once persistence succeeds, the in-memory application
is expected to be deterministic and non-failing.

The initial durability policy is `always`: flush Python and operating-system
buffers and call `fsync` before acknowledging each mutation. This is slow but
gives a simple initial contract. A later `every-second` policy may be added to
study the durability/performance tradeoff.

### 10.3 Startup replay

- Replay records in file order before accepting clients.
- Skip `PUT` records whose persisted expiration is already past.
- Apply unexpired records with a newly computed monotonic deadline.
- Accept and ignore one incomplete record only when it is the final file
  suffix, and report the recovery in logs.
- Treat malformed or invalid data before the final incomplete suffix as
  corruption and refuse to start.
- Never silently discard an unexplained middle-of-file error.

### 10.4 Documented failure boundary

Even with `fsync`, this project does not claim hardware-level production
durability. There is also a small crash window after a log record is durable but
before the client receives success. After restart, such a mutation may be
present even though the client never observed an acknowledgment. Version 1
does not attempt distributed exactly-once semantics.

## 11. Error and Connection Behavior

Expected errors include:

- unknown command;
- wrong argument count;
- invalid option or integer;
- integer overflow;
- malformed RESP;
- request or buffer limit exceeded;
- persistence failure;
- connection reset or timeout; and
- server shutdown during a connection.

Rules:

- User and protocol errors must not print Python tracebacks to the client.
- Unexpected internal exceptions are logged with context, the affected
  connection is closed, and the server continues when safe.
- Values should not be included in normal logs.
- A client disconnect (`recv` returns `b""`) is normal, not an error.
- One failed connection must not terminate the listener or other clients.
- Persistence failure places the server into a state that rejects further
  mutations until the failure is explicitly resolved or the server restarts;
  reads may continue. This prevents silent divergence from the durable log.

## 12. Graceful Shutdown

On `SIGINT` or `SIGTERM`, the server should:

1. Mark itself as stopping so shutdown runs only once.
2. Stop accepting new connections and close the listening socket.
3. Signal active client workers to stop and unblock their socket reads.
4. Allow an already executing command to finish.
5. Join worker and expiration threads with a bounded timeout.
6. Flush and close the append-only file.
7. Emit a final operational summary and exit.

The design must not depend on daemon threads being killed during interpreter
exit. Tests must prove that the server releases its port and leaves a replayable
log after shutdown.

## 13. Observability

Use Python's standard `logging` module. Logs should include:

- startup configuration without secrets;
- bound address and port;
- connection opened and closed at debug level;
- protocol violations and resource-limit events;
- persistence replay summary and recovery actions;
- persistence failures;
- shutdown start and completion; and
- unexpected exceptions with tracebacks.

Maintain thread-safe counters for at least:

- current and total connections;
- commands processed by command name;
- command errors;
- protocol errors;
- keyspace hits and misses;
- expired keys removed;
- persistence records written; and
- bytes read and written.

Version 1 may emit counters at shutdown. A Redis-like `INFO` command is an
optional extension rather than a core requirement.

## 14. Configuration and CLI

The server should be runnable with:

```text
python -m mini_redis
```

Required configuration options:

- bind address, default `127.0.0.1`;
- port, default `6379`, with port `0` supported in integration tests;
- optional append-only file path;
- log level;
- maximum clients;
- protocol buffer and bulk-string limits;
- client idle timeout; and
- active-expiration interval.

The initial defaults are a 300-second idle timeout and a 100-millisecond active
expiration interval. Tests may override both. A command that is already
executing is not aborted merely because the connection reaches its idle
timeout.

Invalid configuration must fail before the server begins accepting clients.
Configuration should be represented by a validated data object rather than
read from global variables throughout the codebase.

## 15. Testing Strategy

### 15.1 Unit tests

#### Protocol

- Every supported request and response representation.
- One request split at every possible byte boundary.
- Multiple requests in one buffer.
- Complete request plus incomplete next request.
- Invalid prefixes, lengths, terminators, and nested types.
- Bulk strings containing CRLF, null bytes, and non-UTF-8 bytes.
- Every resource limit at, below, and above its boundary.

#### Store and commands

- Each success path and argument error.
- Replacement and deletion behavior.
- Duplicate keys in `DEL` and `EXISTS`.
- Integer grammar, overflow, and TTL preservation for `INCR`.
- Expiration using a fake clock with no real sleeping.
- `SET` TTL replacement and clearing behavior.

#### Persistence

- Round-trip arbitrary binary keys and values.
- Replay ordering.
- Expired-record handling.
- Truncated final record recovery.
- Middle-of-file corruption rejection.
- Simulated append and `fsync` failures.

### 15.2 Integration tests

- Start a real server on an ephemeral port and communicate over TCP.
- Verify fragmentation and pipelining through sockets.
- Verify one client observes another client's mutations.
- Verify many clients can increment one key without lost updates.
- Verify a malformed client does not affect healthy clients.
- Verify connection and request limits.
- Restart against an existing log and verify recovered state.
- Shut down with active clients and verify bounded termination.

Integration tests should use deterministic synchronization and bounded waits.
Arbitrary sleeps are a last resort.

### 15.3 Compatibility checks

Once the RESP2 subset is implemented, supported commands should be exercised
with `redis-cli`. Passing these checks demonstrates limited interoperability,
not full Redis compatibility.

## 16. Milestones and Exit Criteria

Each milestone should be implemented as a small vertical slice. Do not begin a
later milestone merely because the happy path works; meet its exit criteria and
record the important lesson first.

### Milestone 0: Project foundation — Complete

Deliver:

- [x] package and test layout;
- [x] CLI entry point;
- [x] formatter/linter/test configuration; and
- [x] a short README linking to this specification.

Exit criteria:

- [x] a clean environment can install the project and run the test suite;
- [x] the CLI provides useful `--help` output.

### Milestone 1: One-client `PING` — Complete

Deliver:

- [x] raw TCP listener;
- [x] one connection at a time;
- [x] temporary CRLF line framing;
- [x] `PING`/`PONG`; and
- [x] a real socket integration test.

Exit criteria:

- [x] fragmented `PING\r\n` works;
- [x] disconnects release resources;
- [x] the test does not depend on a hard-coded available port.

### Milestone 2: In-memory command core — Complete

Deliver:

- [x] separated command and store layers;
- [x] `SET`, `GET`, and `DEL`; and
- [x] focused unit tests independent of sockets.

Exit criteria:

- [x] two sequential connections observe the same state;
- [x] networking code contains no direct dictionary operations.

### Milestone 3: RESP2 streaming protocol — Complete

Deliver:

- [x] incremental RESP2 request parser;
- [x] RESP2 response encoder;
- [x] protocol limits; and
- [x] removal of the temporary line protocol.

Exit criteria:

- [x] arbitrary fragmentation and pipelining tests pass;
- [x] binary keys and values round-trip;
- [x] supported commands work through `redis-cli`.

### Milestone 4: Concurrent clients — Complete

Deliver:

- [x] thread-per-client workers;
- [x] maximum-client enforcement;
- [x] service lock and explicit atomicity boundary; and
- [x] `EXISTS` and `INCR`.

Exit criteria:

- [x] concurrent increments have no lost updates;
- [x] a stalled client does not prevent another client from completing commands;
- [x] no socket I/O occurs under the global service lock.

Implementation decisions and lesson:

- One shared service owns the store and serializes complete keyspace operations.
  Client sockets and worker tracking use a separate registry lock. Concurrency
  in the transport must not split a read-modify-write operation or put network
  waits inside the state lock.
- `--max-clients` defaults to 100. Excess connections receive an error and close;
  worker cleanup releases their occupied slots even after protocol failures.
- `INCR` accepts `0` or `-?[1-9][0-9]*`, within signed 64-bit range. It rejects
  leading zeros, `+`, `-0`, whitespace, and overflow without changing state.
- Tests cover concurrent increments, idle clients, blocked response writes,
  admission limits, slot reuse, and joining idle workers during close.

### Milestone 5: Expiration — Complete

Deliver:

- [x] injected clock;
- [x] `SET ... EX`, `EXPIRE`, and `TTL`;
- [x] lazy expiration; and
- [x] active cleanup thread.

Exit criteria:

- [x] all TTL tests use fake time;
- [x] expired keys are never observable;
- [x] replacing a key has the specified TTL behavior.

Implementation decisions and lesson:

- Store entries hold bytes, an optional monotonic deadline, and an optional Unix
  expiration timestamp. Live deadlines are unaffected by wall-clock changes.
- Expiration inputs accept 1–10 ASCII digits, including leading zeros, and values
  from 1 through 2,147,483,647 seconds. This bounded subset rejects invalid or
  excessive durations before mutation or numeric conversion.
- Lazy checks make expired keys absent for every operation. Multi-key commands
  sample time once; `INCR` preserves expiration while plain `SET` clears it.
- An ordered dictionary allows cleanup to rotate through at most 100 entries per
  100-millisecond pass without a full key snapshot. Each batch holds the service
  lock; the maintenance worker is stopped and joined on server close.
- Fake-clock tests cover exact deadlines, rounding, wall-clock changes, failed
  mutations, replacement, increment semantics, bounded cleanup, TCP commands,
  and cleanup worker lifecycle. Background reclamation is never required for
  observable expiration correctness.

### Milestone 6: Append-only persistence — Complete

Deliver:

- [x] canonical binary-safe records;
- [x] synchronous append and replay;
- [x] absolute persisted expiration;
- [x] tail-truncation handling; and
- [x] read-only degraded behavior after persistence failure.

Exit criteria:

- [x] acknowledged state survives restart;
- [x] expired state is not resurrected;
- [x] corruption and partial-write behaviors are tested and documented.


Implementation decisions and lesson:

- `--appendonly PATH` enables synchronous `PUT`/`DELETE` records; replay finishes
  before the listener opens. The service lock orders log writes and memory changes.
- File flush and `fsync` precede memory application and acknowledgment. A disk
  failure disables further mutations until restart while reads remain available.
- Absolute millisecond expirations account for downtime. An expired replacement
  suppresses older values rather than resurrecting them during replay.
- Recovery truncates an incomplete final suffix before appending. Invalid complete
  records or malformed framing fail startup. The format does not detect corruption
  that remains syntactically and semantically valid.
- Tests cover binary round trips, concurrent mutation ordering, TCP restart,
  expiration, every truncation point, and partial-write/flush/fsync failures.
- A durable record can exist without a success reply, including after an I/O error.
  This ambiguity is part of the failure model; retries are not exactly-once.
- The log is single-process, has no compaction, and synchronous disk operations can
  delay shutdown beyond the worker join budget. Operational hardening remains next.

### Milestone 7: Operational hardening — Not started

Deliver:

- graceful signal-driven shutdown;
- connection timeouts;
- structured, useful logging;
- operational counters;
- configuration validation; and
- end-to-end failure tests.

Exit criteria:

- shutdown is bounded with idle and active clients;
- the port and file handles are released;
- expected client mistakes do not produce server crashes;
- logs explain startup, recovery, important failures, and shutdown.

### Milestone 8: Review and explanation — Not started

Deliver:

- architecture documentation reflecting the code that actually exists;
- a limitations and failure-model section in the README;
- a manual demonstration script; and
- a final code and test review.

Exit criteria:

- the implementer can trace a command from socket bytes through response bytes;
- every core invariant has at least one test;
- deferred work is named rather than hidden behind TODOs.

## 17. Stretch Goals

Stretch goals are chosen for additional learning, not Redis completeness. Add
at most one at a time after version 1:

1. `AUTH` plus a small ACL model to explore authentication and authorization.
2. Pub/sub to introduce server-initiated messages and connection mode changes.
3. `MULTI`/`EXEC` transactions to explore queued commands and atomic batches.
4. Snapshot persistence to compare logs with point-in-time state.
5. An `asyncio` or selector-based transport to compare concurrency models while
   preserving the command and store layers.
6. Additional data structures such as lists or sets.
7. Configurable `fsync` policies and a benchmark demonstrating the tradeoff.

Replication and clustering should not be attempted until the single-node
failure model is well understood.

## 18. Core Invariants

These invariants govern implementation and review:

1. TCP read boundaries never define request boundaries.
2. Parsing incomplete input never consumes or loses bytes.
3. A valid request is executed at most once by a connection worker.
4. Public operations never return expired data.
5. Command validation happens before mutation.
6. A failed command does not partially mutate state.
7. Every successful command is atomic from other clients' perspective.
8. Persistent mutation order matches visible mutation order.
9. Socket I/O does not occur while holding the global state lock.
10. One client's failure does not crash the server or corrupt another client.
11. The store has no dependency on networking or RESP.
12. Tests control time; they do not wait for real TTLs to pass.
13. Graceful shutdown has a bounded completion path.
14. Values and secrets are not emitted in normal logs.

Any implementation choice that violates an invariant requires an explicit
specification change, not an undocumented exception.

## 19. Decision Process

Before implementing a feature:

1. State the externally observable behavior.
2. Identify affected invariants and failure modes.
3. Write or outline the most important tests.
4. Choose the smallest design that satisfies them.
5. Implement the vertical slice.
6. Review complexity before adding another abstraction.

When the implementation teaches us that this specification is wrong or
needlessly complex, update the specification and record the reason. The plan is
a living contract, not a substitute for learning.

## 20. References

- [Redis serialization protocol](https://redis.io/docs/latest/develop/reference/protocol-spec/)
- [Redis command reference](https://redis.io/docs/latest/commands/)
- [Redis `EXPIRE` command](https://redis.io/docs/latest/commands/expire/)
- [Redis persistence](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/)
- [Redis security](https://redis.io/docs/latest/operate/oss_and_stack/management/security/)
