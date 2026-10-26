# Mini Redis

Mini Redis is a small Redis-inspired server built as a learning project. Its
goal is to explore TCP servers, streaming protocols, concurrency, expiration,
persistence, failure handling, testing, and observability without attempting to
reproduce all of Redis.

The behavior and implementation sequence are defined in [PLAN.md](PLAN.md).

## Current status: Milestone 1

Mini Redis currently provides:

- a Python 3.12+ package using a `src` layout;
- project and dependency management with `uv`;
- a synchronous TCP server listening on `127.0.0.1:6379` by default;
- a temporary CRLF-terminated line protocol supporting `PING`/`PONG`;
- correct buffering when a command is split across TCP reads;
- case-insensitive command matching and an error response for unknown commands;
- multiple commands over the same connection;
- sequential client connections;
- tests with pytest; and
- formatting and linting with Ruff.

The server handles one active client at a time. An idle connected client blocks
later clients from being processed until it disconnects. There are not yet
request-size limits. Concurrent clients, storage, and the RESP2 protocol belong
to later milestones. This project is not production-ready and must not be
exposed to untrusted networks.

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

From another terminal, send a PING with netcat:

```shell
printf 'PING\r\n' | nc 127.0.0.1 6379
```

The response should be `PONG`. Press Ctrl-C in the server terminal to stop it.

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

### Temporary protocol

Commands are ASCII lines terminated by `\r\n`. Command names are matched
case-insensitively:

```text
PING\r\n  -> PONG\r\n
hello\r\n -> ERROR unknown command\r\n
```

This line protocol is temporary and will be replaced by the RESP2 subset in
Milestone 3.

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
