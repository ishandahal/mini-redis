# Mini Redis

Mini Redis is a small Redis-inspired server built as a learning project. Its
goal is to explore TCP servers, streaming protocols, concurrency, expiration,
persistence, failure handling, testing, and observability without attempting to
reproduce all of Redis.

The behavior and implementation sequence are defined in [PLAN.md](PLAN.md).

## Current status

Milestone 0 establishes the project foundation:

- a Python 3.12+ package using a `src` layout;
- project and dependency management with `uv`;
- a minimal command-line entry point;
- tests with pytest; and
- formatting and linting with Ruff.

The TCP server begins in Milestone 1. This project is not production-ready and
must not be exposed to untrusted networks.

## Development

Create the environment and install development dependencies:

```shell
uv sync --dev
```

Inspect the command-line interface:

```shell
uv run mini-redis --help
```

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
