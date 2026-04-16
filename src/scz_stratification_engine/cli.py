"""Command-line interface for the scz stratification engine."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable, Sequence

from . import __version__

STRICT_OPEN_COMMANDS = (
    "ingest",
    "audit",
    "harmonize",
    "define-splits",
    "build-features",
    "build-targets",
    "train-baselines",
    "train",
    "eval",
    "report",
)

DEFAULT_CONFIG_PATH = "config/strict_open_v0.toml"


def _build_help_handler(parser: argparse.ArgumentParser) -> Callable[[argparse.Namespace], int]:
    def handler(_args: argparse.Namespace) -> int:
        parser.print_help()
        return 0

    return handler


def _build_stub_handler(command_name: str) -> Callable[[argparse.Namespace], int]:
    def handler(_args: argparse.Namespace) -> int:
        print(
            f"strict-open {command_name} is not implemented yet in PR2; this command is a bootstrap stub.",
            file=sys.stderr,
        )
        return 1

    return handler


def build_parser() -> argparse.ArgumentParser:
    """Create the top-level CLI parser."""

    parser = argparse.ArgumentParser(
        prog="scz-stratification",
        description="Bootstrap CLI for the strict-open v0 public feasibility engine.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.set_defaults(handler=_build_help_handler(parser))

    subparsers = parser.add_subparsers(dest="command")
    strict_open_parser = subparsers.add_parser(
        "strict-open",
        help="Commands for the strict-open v0 namespace.",
        description="Bootstrap commands for the strict-open v0 public feasibility engine.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    strict_open_parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG_PATH,
        help="Path to the strict-open v0 config file.",
    )
    strict_open_parser.set_defaults(handler=_build_help_handler(strict_open_parser))

    strict_open_subparsers = strict_open_parser.add_subparsers(dest="strict_open_command")
    for command_name in STRICT_OPEN_COMMANDS:
        command_parser = strict_open_subparsers.add_parser(
            command_name,
            help=f"Stub command for strict-open {command_name}.",
            description=f"Stub command for strict-open {command_name}.",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        command_parser.add_argument(
            "--config",
            default=DEFAULT_CONFIG_PATH,
            help="Path to the strict-open v0 config file.",
        )
        command_parser.set_defaults(handler=_build_stub_handler(command_name))

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CLI and return an exit code."""

    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    handler: Callable[[argparse.Namespace], int] = args.handler
    return handler(args)


if __name__ == "__main__":
    raise SystemExit(main())
