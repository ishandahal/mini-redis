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
- sequential client connections;
- tests with pytest; and
- formatting and linting with Ruff.

The server handles one active client at a time. Concurrent clients, storage, and
the RESP2 protocol belong to later milestones. This project is not
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

From another terminal, send a PING with netcat:

```shell
printf 'PING\r\n' | nc 127.0.0.1 6379
```

The response should be `PONG`. Press Ctrl-C in the server terminal to stop it.

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
