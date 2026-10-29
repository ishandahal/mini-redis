# Mini Redis

Mini Redis is a small Redis-inspired server built as a learning project. Its
goal is to explore TCP servers, streaming protocols, concurrency, expiration,
persistence, failure handling, testing, and observability without attempting to
reproduce all of Redis.

The behavior and implementation sequence are defined in [PLAN.md](PLAN.md).

## Current status: Milestone 3

Mini Redis currently provides:

- a Python 3.12+ package using a `src` layout;
- project and dependency management with `uv`;
- a synchronous TCP server listening on `127.0.0.1:6379` by default;
- an incremental RESP2 array-of-bulk-strings request parser;
- RESP2 simple-string, error, integer, bulk-string, and null response encoding;
- correct buffering when requests are split across arbitrary TCP reads;
- pipelined execution of multiple requests received together;
- binary-safe keys and values, including empty byte strings;
- per-connection buffer, bulk-string, and request-array limits;
- case-insensitive command matching and an error response for unknown commands;
- an in-memory byte-string store shared across sequential connections;
- socket-independent command handling for `PING`, `SET`, `GET`, and `DEL`;
- compatibility with `redis-cli` for the supported command subset;
- sequential client connections;
- tests with pytest; and
- formatting and linting with Ruff.

The server handles one active client at a time. An idle connected client blocks
later clients from being processed until it disconnects. Concurrent clients,
expiration, and persistence belong to later milestones. This project is not
production-ready and must not be exposed to untrusted networks.

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
