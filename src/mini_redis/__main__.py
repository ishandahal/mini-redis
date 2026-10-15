"""Command-line entry point for Mini Redis."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from mini_redis import __version__
from mini_redis.server import MiniRedisServer


def port_number(value: str) -> int:
    """Parse a TCP port, including port zero for OS-assigned test ports."""
    try:
        port = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("port must be an integer") from error

    if not 0 <= port <= 65535:
        raise argparse.ArgumentTypeError("port must be between 0 and 65535")
    return port


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser without reading process-global arguments."""
    parser = argparse.ArgumentParser(
        prog="mini-redis",
        description=(
            "A Redis-inspired server built to learn backend and systems engineering. "
            "Milestone 1 implements the temporary PING line protocol."
        ),
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="address on which to listen (default: %(default)s)",
    )
    parser.add_argument(
        "--port",
        default=6379,
        type=port_number,
        help="TCP port on which to listen; 0 asks the OS to choose (default: %(default)s)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the Mini Redis command-line interface."""
    parser = build_parser()
    arguments = parser.parse_args(argv)
    server = MiniRedisServer(host=arguments.host, port=arguments.port)

    try:
        server.start()
    except OSError as error:
        print(f"mini-redis: unable to start server: {error}", file=sys.stderr)
        return 1

    host, port = server.address
    print(f"Mini Redis listening on {host}:{port}", flush=True)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nMini Redis stopped", file=sys.stderr)
    finally:
        server.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
