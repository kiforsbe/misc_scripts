import argparse
import sys
from typing import Optional, Sequence

from .commands import COMMAND_MODULES


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Manage Plex watch history and inspect Plex SQLite library databases."
    )
    subparsers = parser.add_subparsers(dest="command")
    for module in COMMAND_MODULES:
        module.register(subparsers)
    return parser


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    raw_argv = list(argv) if argv is not None else sys.argv[1:]
    subcommands = {
        "transfer-watch-status",
        "transfer-playlists",
        "sync-metadata-playlists",
        "list-playlists",
        "remove-playlists",
        "list-libraries",
        "list-accounts",
    }
    if raw_argv and not raw_argv[0].startswith("-") and raw_argv[0] not in subcommands:
        raw_argv = ["transfer-watch-status", *raw_argv]
    if raw_argv and raw_argv[0].startswith("-") and raw_argv[0] not in {"-h", "--help"}:
        raw_argv = ["transfer-watch-status", *raw_argv]
    return build_parser().parse_args(raw_argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    try:
        args = parse_args(argv)
        handler = getattr(args, "command_handler", None)
        if handler is None:
            build_parser().print_help()
            return 1
        return int(handler(args))
    except KeyboardInterrupt:
        print("Aborted.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())