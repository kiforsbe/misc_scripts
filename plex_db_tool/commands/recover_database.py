import sys
from argparse import Namespace, _SubParsersAction
from pathlib import Path
from typing import Optional

from ..infrastructure import PlexDatabaseLocator, backup_database_file, recover_sqlite_database


def register(subparsers: _SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "recover-database",
        help="Recover a malformed Plex SQLite library database.",
    )
    parser.set_defaults(command="recover-database", command_handler=run)
    parser.add_argument(
        "--path",
        required=True,
        help="Path to the Plex library folder or Plex SQLite database file.",
    )
    parser.add_argument(
        "--output",
        help=(
            "Path for the recovered database file. "
            "Defaults to the source filename with '.recovered' before the extension."
        ),
    )
    parser.add_argument(
        "--in-place",
        action="store_true",
        help=(
            "Replace the source database with the recovered copy after successful recovery. "
            "The original file is preserved with a '.bak' suffix."
        ),
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite the output file if it already exists.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print progress details during recovery.",
    )


def _default_output_path(source_path: Path) -> Path:
    return source_path.with_name(f"{source_path.stem}.recovered{source_path.suffix}")


def run(args: Namespace) -> int:
    source_db_path = PlexDatabaseLocator.resolve_local_db_path(
        args.path,
        label="path",
        path_arg_name="path",
        validate=False,
    )
    output_path = Path(args.output).expanduser().resolve() if args.output else _default_output_path(source_db_path)

    if output_path.resolve() == source_db_path.resolve():
        print("Error: output path must differ from source path.", file=sys.stderr)
        return 1

    if output_path.exists() and not args.overwrite:
        print(
            f"Error: output path already exists: {output_path}. Use --overwrite to replace it.",
            file=sys.stderr,
        )
        return 1

    output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        recover_sqlite_database(source_db_path, output_path, verbose=args.verbose)
    except Exception as exc:
        print(f"Database recovery failed: {exc}", file=sys.stderr)
        return 1

    if args.in_place:
        backup_path = backup_database_file(source_db_path)
        source_db_path.replace(backup_path)
        output_path.replace(source_db_path)
        print(
            f"Recovered database in place. Original file preserved as: {backup_path}",
        )
    else:
        print(f"Recovered database written to: {output_path}")

    return 0
