"""Command-line entry point for Mini Redis."""

from __future__ import annotations

import argparse
import logging
import os
import signal
from collections.abc import Sequence

from mini_redis import __version__
from mini_redis.config import Config
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


def positive_integer(value: str) -> int:
    """Parse a positive client limit before opening the listener."""
    try:
        result = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be a positive integer") from error
    if result <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return result


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser without reading process-global arguments."""
    parser = argparse.ArgumentParser(
        prog="mini-redis",
        description=(
            "A Redis-inspired server built to learn backend and systems engineering. "
            "Milestone 7 adds operational hardening to the persistent "
            "in-memory server."
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
    parser.add_argument(
        "--max-clients",
        type=positive_integer,
        default=100,
        help="maximum simultaneous clients (default: %(default)s)",
    )
    parser.add_argument(
        "--appendonly", metavar="PATH", help="durable append-only log path"
    )
    parser.add_argument("--idle-timeout", type=float, default=300.0, metavar="SECONDS")
    parser.add_argument(
        "--expiration-interval", type=float, default=0.1, metavar="SECONDS"
    )
    parser.add_argument(
        "--shutdown-timeout", type=float, default=2.0, metavar="SECONDS"
    )
    parser.add_argument("--max-buffer-size", type=int, default=1024 * 1024)
    parser.add_argument("--max-bulk-string-size", type=int, default=512 * 1024)
    parser.add_argument("--max-array-length", type=int, default=1024)
    parser.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
        default="INFO",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the Mini Redis command-line interface."""
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        config = Config(**vars(arguments))
    except ValueError as error:
        parser.error(str(error))
    logging.basicConfig(
        level=config.log_level, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    logger = logging.getLogger(__name__)
    server = MiniRedisServer(config=config)
    previous = {}

    def stop(signum, frame):
        server.request_stop()

    for signum in (signal.SIGINT, signal.SIGTERM):
        previous[signum] = signal.signal(signum, stop)
    try:
        try:
            server.start()
        except OSError as error:
            logger.error("unable to start server: %s", error)
            return 1
        host, port = server.address
        print(f"Mini Redis listening on {host}:{port}", flush=True)
        server.serve_forever()
    except TimeoutError:
        # Python cannot cancel a thread stuck in filesystem I/O. Do not hang
        # interpreter shutdown on its non-daemon worker after the grace period.
        logger.critical("shutdown deadline exceeded; forcing unsuccessful exit")
        os._exit(1)
    except OSError as error:
        logger.error("server failed: %s", error)
        return 1
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
