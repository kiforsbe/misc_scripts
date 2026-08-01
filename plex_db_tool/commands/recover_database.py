import sys
import sqlite3
from collections import Counter
from argparse import Namespace, _SubParsersAction
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ..infrastructure import (
    PlexDatabase,
    PlexDatabaseLocator,
    backup_database_file,
    check_plex_database_integrity,
    find_plex_sqlite_executable,
    recover_sqlite_database,
)


DEFAULT_EPISODE_DATE_ADDED_DRIFT_HOURS = 24.0


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
    parser.add_argument(
        "--check-episode-date-added",
        action="store_true",
        help=(
            "Inspect file-backed Date Added values against file timestamps and report whether a future fix "
            "can be applied safely. Does not modify the database."
        ),
    )
    parser.add_argument(
        "--apply-episode-date-added-fix",
        action="store_true",
        help=(
            "Update file-backed metadata_items.added_at values that fall outside the allowed drift. "
            "Uses the closest available file creation or modified timestamp."
        ),
    )
    parser.add_argument(
        "--episode-date-added-max-drift-hours",
        type=float,
        default=DEFAULT_EPISODE_DATE_ADDED_DRIFT_HOURS,
        help=(
            "Maximum allowed drift in hours between metadata_items.added_at and the closest file "
            "creation/modified timestamp when using --check-episode-date-added. Default: 24."
        ),
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


def _format_timestamp(value: Optional[int]) -> str:
    if value is None:
        return "missing"
    try:
        return datetime.fromtimestamp(int(value)).isoformat(sep=" ", timespec="seconds")
    except (OverflowError, OSError, TypeError, ValueError):
        return str(value)


def _format_hours(value: float) -> str:
    return f"{value:.2f}".rstrip("0").rstrip(".")


def _load_table_columns(connection: sqlite3.Connection, table_name: str) -> Dict[str, sqlite3.Row]:
    return {
        row["name"]: row
        for row in connection.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    }


def _get_write_probe_status(source_db_path: Path) -> Tuple[bool, str]:
    try:
        with sqlite3.connect(str(source_db_path), timeout=1) as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute("ROLLBACK")
        return True, "BEGIN IMMEDIATE succeeded; the DB accepted a write lock probe."
    except sqlite3.Error as exc:
        return False, str(exc)


def _choose_repair_timestamp(record: Dict[str, Any]) -> Optional[int]:
    best_match = record["best_match"]
    if best_match is not None:
        return int(best_match["timestamp_value"])

    for file_path in sorted(set(record["file_paths"])):
        path = Path(file_path)
        if not path.exists():
            continue
        stat_result = path.stat()
        return min(int(stat_result.st_ctime), int(stat_result.st_mtime))
    return None


def _build_row_label(record: Dict[str, Any]) -> str:
    return " / ".join(
        part for part in (record["series_title"], record["season_title"], record["episode_title"]) if part
    ) or f"metadata:{record['metadata_item_id']}"


def _build_mismatch_reason(record: Dict[str, Any]) -> str:
    best_match = record["best_match"]
    if record["added_at"] is None:
        return "missing added_at"
    if best_match is None:
        return "no accessible file timestamps"
    drift_hours = best_match["drift_seconds"] / 3600.0
    return (
        f"closest {best_match['timestamp_label']} time differs by {_format_hours(drift_hours)} hours "
        f"at {best_match['file_path']}"
    )


def _apply_date_added_fixes(source_db_path: Path, rows_to_fix: List[Dict[str, Any]]) -> int:
    writable_rows = []
    for row in rows_to_fix:
        repair_timestamp = _choose_repair_timestamp(row)
        if repair_timestamp is None:
            continue
        writable_rows.append((int(row["metadata_item_id"]), repair_timestamp))

    if not writable_rows:
        print("Fix summary: no flagged rows had accessible file timestamps to apply.")
        return 0

    database = PlexDatabase(source_db_path, readonly=False, sqlite_engine="builtin")
    try:
        database.connection.execute("BEGIN IMMEDIATE")
        for metadata_item_id, added_at in writable_rows:
            database.execute_metadata_items_write(
                "UPDATE metadata_items SET added_at = ? WHERE id = ?",
                (added_at, metadata_item_id),
            )
        database.connection.commit()
    except Exception:
        database.connection.rollback()
        raise
    finally:
        database.close()

    print(f"Fix summary: updated {len(writable_rows)} metadata_items.added_at row(s).")
    return len(writable_rows)


def _inspect_episode_date_added(source_db_path: Path, max_drift_hours: float, verbose: bool, apply_fix: bool) -> int:
    if max_drift_hours <= 0:
        print("Error: --episode-date-added-max-drift-hours must be greater than zero.", file=sys.stderr)
        return 1

    tolerance_seconds = int(max_drift_hours * 3600)
    source_uri = f"file:{source_db_path.as_posix()}?mode=ro"

    print("Date Added audit")
    print(f"Drift tolerance: {_format_hours(max_drift_hours)} hours")

    try:
        with sqlite3.connect(source_uri, uri=True, timeout=30) as connection:
            connection.row_factory = sqlite3.Row

            table_columns = {
                table_name: _load_table_columns(connection, table_name)
                for table_name in ("metadata_items", "media_items", "media_parts")
            }
            missing_tables = [table_name for table_name, columns in table_columns.items() if not columns]
            if missing_tables:
                print(
                    "Schema status: missing required tables: " + ", ".join(sorted(missing_tables)),
                    file=sys.stderr,
                )
                return 1

            required_columns = {
                "metadata_items": {"id", "parent_id", "metadata_type", "title", "added_at", "created_at", "updated_at"},
                "media_items": {"id", "metadata_item_id"},
                "media_parts": {"media_item_id", "file"},
            }
            schema_errors: List[str] = []
            for table_name, expected_columns in required_columns.items():
                missing_columns = sorted(expected_columns.difference(table_columns[table_name]))
                if missing_columns:
                    schema_errors.append(f"{table_name}: missing columns {', '.join(missing_columns)}")
            if schema_errors:
                print("Schema status: incompatible for episode Date Added audit.", file=sys.stderr)
                for message in schema_errors:
                    print(f"Schema detail: {message}", file=sys.stderr)
                return 1

            table_sql_rows = connection.execute(
                """
                SELECT name, sql
                FROM sqlite_master
                WHERE type = 'table'
                  AND name IN ('metadata_items', 'media_items', 'media_parts')
                ORDER BY name
                """
            ).fetchall()
            trigger_rows = connection.execute(
                """
                SELECT name, tbl_name, sql
                FROM sqlite_master
                WHERE type = 'trigger'
                  AND (tbl_name = 'metadata_items' OR sql LIKE '%metadata_items%')
                ORDER BY name
                """
            ).fetchall()

            write_ok, write_message = _get_write_probe_status(source_db_path)
            plex_sqlite_path = find_plex_sqlite_executable()

            print("Schema status: metadata_items/media_items/media_parts support the audit query.")
            print(f"Schema detail: metadata_items.added_at present = {'added_at' in table_columns['metadata_items']}")
            print(f"Schema detail: media_parts.file present = {'file' in table_columns['media_parts']}")
            print(f"Write probe: {'OK' if write_ok else 'blocked'}")
            print(f"Write detail: {write_message}")
            print(
                "Update feasibility: "
                + (
                    "metadata_items.added_at looks directly writable on this DB shape."
                    if write_ok and not trigger_rows
                    else "writes may need extra care because triggers or locking are present."
                )
            )
            if trigger_rows:
                print(f"Trigger detail: found {len(trigger_rows)} metadata_items-related trigger(s).")
            else:
                print("Trigger detail: no metadata_items-related triggers were found in sqlite_master.")
            if plex_sqlite_path is not None:
                print(f"Plex SQLite: available at {plex_sqlite_path}")
            else:
                print("Plex SQLite: not found; future write fixes would use builtin sqlite only.")

            if verbose:
                for row in table_sql_rows:
                    print(f"Table SQL [{row['name']}]: {row['sql']}")
                for row in trigger_rows:
                    print(f"Trigger SQL [{row['name']}]: {row['sql']}")

            rows = connection.execute(
                """
                SELECT
                    md.id AS metadata_item_id,
                    md.metadata_type AS metadata_type,
                    md.title AS episode_title,
                    md.added_at AS added_at,
                    md.created_at AS created_at,
                    md.updated_at AS updated_at,
                    parent.title AS season_title,
                    grandparent.title AS series_title,
                    mp.file AS file_path
                FROM metadata_items md
                JOIN media_items mi ON mi.metadata_item_id = md.id
                JOIN media_parts mp ON mp.media_item_id = mi.id
                LEFT JOIN metadata_items parent ON md.parent_id = parent.id
                LEFT JOIN metadata_items grandparent ON parent.parent_id = grandparent.id
                WHERE mp.file IS NOT NULL
                ORDER BY md.id, mp.id
                """
            ).fetchall()
    except sqlite3.Error as exc:
        print(f"Episode Date Added audit failed: {exc}", file=sys.stderr)
        _print_failure_guidance(exc)
        return 1

    grouped_rows: Dict[int, Dict[str, Any]] = {}
    observed_types: Counter[int] = Counter()
    for row in rows:
        metadata_item_id = int(row["metadata_item_id"])
        metadata_type = int(row["metadata_type"] or 0)
        observed_types[metadata_type] += 1
        record = grouped_rows.setdefault(
            metadata_item_id,
            {
                "metadata_item_id": metadata_item_id,
                "metadata_type": metadata_type,
                "episode_title": row["episode_title"],
                "season_title": row["season_title"],
                "series_title": row["series_title"],
                "added_at": row["added_at"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "file_paths": [],
            },
        )
        record["file_paths"].append(str(row["file_path"]))

    if not grouped_rows:
        print("Date Added scan: no file-backed metadata rows were found via metadata_items -> media_items -> media_parts.")
        return 0

    audit_rows: List[Dict[str, Any]] = []
    missing_files = 0
    missing_added_at = 0
    for record in grouped_rows.values():
        best_match: Optional[Dict[str, Any]] = None
        existing_file_count = 0
        missing_path_count = 0
        for file_path in sorted(set(record["file_paths"])):
            path = Path(file_path)
            if not path.exists():
                missing_path_count += 1
                continue
            existing_file_count += 1
            stat_result = path.stat()
            timestamp_candidates = [
                ("created", int(stat_result.st_ctime)),
                ("modified", int(stat_result.st_mtime)),
            ]
            if record["added_at"] is None:
                continue
            added_at = int(record["added_at"])
            for label, candidate_timestamp in timestamp_candidates:
                drift_seconds = abs(added_at - candidate_timestamp)
                if best_match is None or drift_seconds < best_match["drift_seconds"]:
                    best_match = {
                        "timestamp_label": label,
                        "timestamp_value": candidate_timestamp,
                        "drift_seconds": drift_seconds,
                        "file_path": file_path,
                    }

        if existing_file_count == 0:
            missing_files += 1
        if record["added_at"] is None:
            missing_added_at += 1

        audit_rows.append(
            {
                **record,
                "existing_file_count": existing_file_count,
                "missing_path_count": missing_path_count,
                "best_match": best_match,
            }
        )

    mismatches = [
        row
        for row in audit_rows
        if row["added_at"] is None
        or row["best_match"] is None
        or int(row["best_match"]["drift_seconds"]) > tolerance_seconds
    ]
    within_tolerance = len(audit_rows) - len(mismatches)

    print(f"Date Added scan: {len(audit_rows)} file-backed metadata item(s) with file rows found.")
    if observed_types:
        observed_summary = ", ".join(
            f"type {metadata_type}={count}"
            for metadata_type, count in sorted(observed_types.items(), key=lambda item: (item[0], item[1]))
        )
        print(f"Metadata shape: observed metadata types among file-backed rows: {observed_summary}")
    print(f"Audit summary: {within_tolerance} within tolerance, {len(mismatches)} outside tolerance or incomplete.")
    if missing_added_at:
        print(f"Audit detail: {missing_added_at} row(s) have no added_at value.")
    if missing_files:
        print(f"Audit detail: {missing_files} row(s) have no currently accessible file path on disk.")

    for row in mismatches:
        reason = _build_mismatch_reason(row)
        label = _build_row_label(row)
        planned_added_at = _choose_repair_timestamp(row)
        print(
            "Mismatch: "
            f"id={row['metadata_item_id']} type={row['metadata_type']} label={label!r} "
            f"added_at={_format_timestamp(row['added_at'])} reason={reason}"
        )
        if planned_added_at is None:
            print(
                "Planned mutation: "
                f"id={row['metadata_item_id']} action=skip reason=no_accessible_repair_timestamp"
            )
        else:
            print(
                "Planned mutation: "
                f"UPDATE metadata_items SET added_at={_format_timestamp(planned_added_at)} "
                f"WHERE id={row['metadata_item_id']} current_added_at={_format_timestamp(row['added_at'])}"
            )

    if apply_fix and mismatches:
        _apply_date_added_fixes(source_db_path, mismatches)
    elif apply_fix:
        print("Fix summary: no rows required updating.")

    return 0


def run(args: Namespace) -> int:
    source_db_path = PlexDatabaseLocator.resolve_local_db_path(
        args.path,
        label="path",
        path_arg_name="path",
        validate=False,
    )

    if getattr(args, "check_episode_date_added", False) or getattr(args, "apply_episode_date_added_fix", False):
        if getattr(args, "apply_episode_date_added_fix", False) and not args.in_place:
            print(
                "Error: --apply-episode-date-added-fix writes directly to the source DB. Pass --in-place to confirm.",
                file=sys.stderr,
            )
            return 1
        if getattr(args, "apply_episode_date_added_fix", False) and args.in_place:
            backup_path = backup_database_file(source_db_path, verbose=args.verbose)
            print(f"Backup created before Date Added fix: {backup_path}")
        _print_preflight_diagnostics(source_db_path, verbose=args.verbose)
        return _inspect_episode_date_added(
            source_db_path,
            max_drift_hours=float(args.episode_date_added_max_drift_hours),
            verbose=args.verbose,
            apply_fix=bool(getattr(args, "apply_episode_date_added_fix", False)),
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
