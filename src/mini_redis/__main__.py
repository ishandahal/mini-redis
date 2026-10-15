"""Command-line entry point for Mini Redis."""

from __future__ import annotations

import argparse
from collections.abc import Sequence

from mini_redis import __version__


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser without reading process-global arguments."""
    parser = argparse.ArgumentParser(
        prog="mini-redis",
        description=(
            "A Redis-inspired server built to learn backend and systems engineering. "
            "The TCP server will be added in Milestone 1."
        ),
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the Mini Redis command-line interface."""
    parser = build_parser()
    parser.parse_args(argv)
    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
