"""End-to-end checks for the installed command-line module."""

from __future__ import annotations

import subprocess
import sys


def run_cli(*arguments: str) -> subprocess.CompletedProcess[str]:
    """Run the CLI through the interpreter used by the test environment."""
    return subprocess.run(
        [sys.executable, "-m", "mini_redis", *arguments],
        check=False,
        capture_output=True,
        text=True,
        timeout=5,
    )


def test_help_describes_the_project() -> None:
    result = run_cli("--help")

    assert result.returncode == 0
    assert "usage: mini-redis" in result.stdout
    assert "Redis-inspired server" in result.stdout
    assert "Milestone 1" in result.stdout
    assert result.stderr == ""


def test_no_arguments_prints_help() -> None:
    result = run_cli()

    assert result.returncode == 0
    assert "usage: mini-redis" in result.stdout
    assert result.stderr == ""


def test_version_is_available() -> None:
    result = run_cli("--version")

    assert result.returncode == 0
    assert result.stdout == "mini-redis 0.1.0\n"
    assert result.stderr == ""
