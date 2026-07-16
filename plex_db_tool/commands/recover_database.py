import sys
import sqlite3
from argparse import Namespace, _SubParsersAction
from pathlib import Path
from typing import List, Optional, Tuple

from ..infrastructure import (
    PlexDatabaseLocator,
    backup_database_file,
    check_plex_database_integrity,
    recover_sqlite_database,
)


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


def _is_tokenizer_or_collation_error(message: str) -> bool:
    lower = message.lower()
    return "unknown tokenizer" in lower or "no such collation sequence" in lower


def _classify_error_message(message: str) -> Tuple[str, List[str]]:
    lower = message.lower()
    if "database is locked" in lower or "database table is locked" in lower or "busy" in lower:
        return (
            "The Plex database is currently locked by another process.",
            [
                "Stop Plex Media Server and any SQLite browser/editor using this DB, then retry.",
                "Wait a few seconds after Plex exits before retrying.",
            ],
        )
    if "permission denied" in lower or "access is denied" in lower or "readonly" in lower:
        return (
            "Permission error while accessing the database file.",
            [
                "Run the command from an account that has read/write access to the Plex DB folder.",
                "Clear read-only attributes and ensure antivirus is not blocking file replacement.",
            ],
        )
    if _is_tokenizer_or_collation_error(message):
        return (
            "Python's sqlite runtime cannot evaluate Plex-specific tokenizer/collation objects.",
            [
                "This does not always mean the DB is corrupted.",
                "Use recover-database to create a clean copy, or use --sqlite-engine=plex for write commands.",
            ],
        )
    if "malformed" in lower or "corrupt" in lower or "database disk image is malformed" in lower:
        return (
            "Database corruption was detected.",
            [
                "Recovery can salvage readable rows, but some corrupt rows may be skipped.",
                "Review recovery logs with --verbose and keep the .bak backup before in-place replacement.",
            ],
        )
    if "no such file" in lower or "cannot find the file" in lower:
        return (
            "The source database path could not be found.",
            [
                "Verify --path points to the Plex folder or com.plexapp.plugins.library.db file.",
            ],
        )
    if "disk i/o error" in lower:
        return (
            "Disk I/O error while reading or writing the database.",
            [
                "Check free space and disk health, then retry recovery.",
                "Prefer writing --output to a healthy local drive.",
            ],
        )
    return (
        "Recovery failed due to an unexpected database error.",
        ["Re-run with --verbose for more detail and verify file access + DB path."],
    )


def _print_preflight_diagnostics(source_db_path: Path, verbose: bool) -> None:
    print(f"Preflight check: {source_db_path}")
    source_uri = f"file:{source_db_path.as_posix()}?mode=ro"
    try:
        with sqlite3.connect(source_uri, uri=True, timeout=30) as connection:
            row = connection.execute("PRAGMA integrity_check").fetchone()
            result = "" if row is None else str(row[0] or "")
            if result.lower() == "ok":
                print("Reason: Database integrity check passed with Python sqlite.")
                print("Opportunity: Recovery will still generate a clean copy if you want a safety rebuild.")
                return
            if result:
                print(f"Reason: Built-in integrity check reported: {result}")
                print("Opportunity: Recovery will attempt to salvage readable rows into a repaired copy.")
                return
            print("Reason: Built-in integrity check returned no result.")
            print("Opportunity: Recovery will attempt a best-effort rebuild.")
            return
    except sqlite3.Error as exc:
        message = str(exc)
        reason, fixes = _classify_error_message(message)
        print(f"Reason: {reason}")
        if _is_tokenizer_or_collation_error(message):
            plex_ok = check_plex_database_integrity(source_db_path, verbose=verbose)
            if plex_ok:
                print("Preflight detail: Plex SQLite integrity check returned OK.")
                print("Opportunity: DB is likely structurally healthy; runtime compatibility is the primary issue.")
            else:
                print("Preflight detail: Plex SQLite integrity check did not return OK.")
                print("Opportunity: Recovery will attempt to salvage readable rows.")
        else:
            print("Opportunity: Recovery will attempt to salvage readable rows where possible.")
        for fix in fixes:
            print(f"Fix: {fix}")


def _print_failure_guidance(exc: Exception) -> None:
    reason, fixes = _classify_error_message(str(exc))
    print(f"Reason: {reason}", file=sys.stderr)
    for fix in fixes:
        print(f"Fix: {fix}", file=sys.stderr)


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
    _print_preflight_diagnostics(source_db_path, verbose=args.verbose)
    try:
        recover_sqlite_database(source_db_path, output_path, verbose=args.verbose)
    except Exception as exc:
        print(f"Database recovery failed: {exc}", file=sys.stderr)
        _print_failure_guidance(exc)
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
