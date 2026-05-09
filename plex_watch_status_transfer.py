import argparse
import csv
import json
import os
import sqlite3
import subprocess
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
import time
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

try:
    from guessit_wrapper import guessit_wrapper
except Exception:
    guessit_wrapper = None


PLEX_DB_FILENAME = "com.plexapp.plugins.library.db"
PLEX_PROCESS_NAMES = (
    "Plex Media Server.exe",
    "Plex Media Server",
)


@dataclass
class TableColumn:
    name: str
    declared_type: str
    not_null: bool
    default_value: Optional[str]
    primary_key_position: int


@dataclass
class PlexSchema:
    media_parts_columns: Dict[str, TableColumn]
    metadata_item_views_columns: Dict[str, TableColumn]

    @property
    def size_column(self) -> Optional[str]:
        for candidate in ("size", "file_size", "total_size"):
            if candidate in self.media_parts_columns:
                return candidate
        return None

    @property
    def duration_column(self) -> Optional[str]:
        return "duration" if "duration" in self.media_parts_columns else None


@dataclass
class ParsedIdentity:
    title_key: Optional[str]
    season: Optional[int]
    episode: Optional[int]


@dataclass
class MediaRecord:
    guid: str
    title: Optional[str]
    year: Optional[int]
    file_path: str
    basename: str
    basename_key: str
    file_size: Optional[int]
    duration: Optional[int]
    parent_guid: Optional[str]
    grandparent_guid: Optional[str]
    parsed_identity: ParsedIdentity


@dataclass
class WatchHistory:
    guid: str
    watch_count: int
    last_viewed_at: Optional[int]
    row_ids: List[int]


@dataclass
class MatchCandidate:
    target: MediaRecord
    confidence: float
    reason: str


@dataclass
class MatchResult:
    source: MediaRecord
    source_history: WatchHistory
    status: str
    confidence: float
    reason: str
    target: Optional[MediaRecord]
    target_history: Optional[WatchHistory]
    notes: List[str]


@dataclass
class PlannedMutation:
    action: str
    target_guid: str
    details: Dict[str, Any]


def normalize_basename(file_path: str) -> str:
    return Path(file_path).name.casefold()


def normalize_title(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    collapsed = " ".join(value.casefold().replace("_", " ").split())
    return collapsed or None


def safe_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def maybe_parse_filename(basename: str) -> ParsedIdentity:
    if not guessit_wrapper:
        return ParsedIdentity(None, None, None)

    try:
        result = guessit_wrapper(basename)
    except Exception:
        return ParsedIdentity(None, None, None)

    return ParsedIdentity(
        title_key=normalize_title(result.get("title")),
        season=safe_int(result.get("season")),
        episode=safe_int(result.get("episode")),
    )


def connect_db(db_path: Path, readonly: bool) -> sqlite3.Connection:
    if readonly:
        uri = f"file:{db_path.as_posix()}?mode=ro"
        connection = sqlite3.connect(uri, uri=True)
    else:
        connection = sqlite3.connect(str(db_path))
    connection.row_factory = sqlite3.Row
    return connection


def load_table_columns(connection: sqlite3.Connection, table_name: str) -> Dict[str, TableColumn]:
    rows = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    if not rows:
        raise RuntimeError(f"Required table not found: {table_name}")
    return {
        row["name"]: TableColumn(
            name=row["name"],
            declared_type=row["type"] or "",
            not_null=bool(row["notnull"]),
            default_value=row["dflt_value"],
            primary_key_position=int(row["pk"]),
        )
        for row in rows
    }


def inspect_schema(connection: sqlite3.Connection) -> PlexSchema:
    return PlexSchema(
        media_parts_columns=load_table_columns(connection, "media_parts"),
        metadata_item_views_columns=load_table_columns(connection, "metadata_item_views"),
    )


def build_media_inventory(
    connection: sqlite3.Connection,
    schema: PlexSchema,
    library_filters: Sequence[str],
) -> List[MediaRecord]:
    size_select = f", mp.{schema.size_column} AS file_size" if schema.size_column else ", NULL AS file_size"
    duration_select = (
        f", mp.{schema.duration_column} AS duration" if schema.duration_column else ", NULL AS duration"
    )

    query = f"""
    SELECT
        md.id AS metadata_item_id,
        md.guid AS guid,
        md.title AS title,
        md.year AS year,
        mp.file AS file_path,
        ls.name AS library_section,
        parent.guid AS parent_guid,
        grandparent.guid AS grandparent_guid
        {size_select}
        {duration_select}
    FROM metadata_items md
    JOIN media_items mi ON md.id = mi.metadata_item_id
    JOIN media_parts mp ON mi.id = mp.media_item_id
    LEFT JOIN library_sections ls ON md.library_section_id = ls.id
    LEFT JOIN metadata_items parent ON md.parent_id = parent.id
    LEFT JOIN metadata_items grandparent ON parent.parent_id = grandparent.id
    WHERE mp.file IS NOT NULL
    """
    params: List[Any] = []
    if library_filters:
        placeholders = ", ".join("?" for _ in library_filters)
        query += f" AND ls.name IN ({placeholders})"
        params.extend(library_filters)

    inventory: List[MediaRecord] = []
    for row in connection.execute(query, params):
        file_path = row["file_path"]
        basename = Path(file_path).name
        inventory.append(
            MediaRecord(
                guid=row["guid"],
                title=row["title"],
                year=safe_int(row["year"]),
                file_path=file_path,
                basename=basename,
                basename_key=normalize_basename(file_path),
                file_size=safe_int(row["file_size"]),
                duration=safe_int(row["duration"]),
                parent_guid=row["parent_guid"],
                grandparent_guid=row["grandparent_guid"],
                parsed_identity=maybe_parse_filename(basename),
            )
        )
    return inventory


def build_watch_history(
    connection: sqlite3.Connection,
    library_filters: Sequence[str],
) -> Dict[str, WatchHistory]:
    query = """
    SELECT
        md.guid AS guid,
        COUNT(miv.rowid) AS watch_count,
        MAX(miv.viewed_at) AS last_viewed_at,
        GROUP_CONCAT(miv.rowid) AS row_ids
    FROM metadata_item_views miv
    JOIN metadata_items md ON md.guid = miv.guid
    LEFT JOIN library_sections ls ON md.library_section_id = ls.id
    WHERE miv.guid IS NOT NULL
    """
    params: List[Any] = []
    if library_filters:
        placeholders = ", ".join("?" for _ in library_filters)
        query += f" AND ls.name IN ({placeholders})"
        params.extend(library_filters)
    query += " GROUP BY md.guid"

    history: Dict[str, WatchHistory] = {}
    for row in connection.execute(query, params):
        row_ids = []
        if row["row_ids"]:
            row_ids = [int(value) for value in str(row["row_ids"]).split(",") if value]
        history[row["guid"]] = WatchHistory(
            guid=row["guid"],
            watch_count=int(row["watch_count"] or 0),
            last_viewed_at=safe_int(row["last_viewed_at"]),
            row_ids=row_ids,
        )
    return history


def index_target_inventory(target_inventory: Iterable[MediaRecord]) -> Dict[str, List[MediaRecord]]:
    by_basename: Dict[str, List[MediaRecord]] = defaultdict(list)

    for record in target_inventory:
        by_basename[record.basename_key].append(record)

    return by_basename


def select_best_candidate(
    source: MediaRecord,
    candidates: Sequence[MediaRecord],
    match_mode: str,
) -> Optional[MatchCandidate]:
    if not candidates:
        return None

    scored: List[MatchCandidate] = []
    source_title_key = normalize_title(source.title) or source.parsed_identity.title_key
    for candidate in candidates:
        confidence = 0.0
        reasons: List[str] = []

        if candidate.basename_key == source.basename_key:
            confidence += 0.55
            reasons.append("basename")

        if source.file_size is not None and candidate.file_size == source.file_size:
            confidence += 0.2
            reasons.append("file_size")

        if source.duration is not None and candidate.duration == source.duration:
            confidence += 0.1
            reasons.append("duration")

        candidate_title_key = normalize_title(candidate.title) or candidate.parsed_identity.title_key
        if source_title_key and candidate_title_key and source_title_key == candidate_title_key:
            confidence += 0.1
            reasons.append("title")

        if source.year is not None and candidate.year == source.year:
            confidence += 0.05
            reasons.append("year")

        if source.parsed_identity.season is not None and candidate.parsed_identity.season == source.parsed_identity.season:
            confidence += 0.08
            reasons.append("season")

        if source.parsed_identity.episode is not None and candidate.parsed_identity.episode == source.parsed_identity.episode:
            confidence += 0.12
            reasons.append("episode")

        scored.append(MatchCandidate(candidate, min(confidence, 1.0), "+".join(reasons) or "weak"))

    scored.sort(key=lambda item: item.confidence, reverse=True)
    top = scored[0]
    if len(scored) > 1 and abs(scored[0].confidence - scored[1].confidence) < 0.05:
        return None

    thresholds = {
        "strict": 0.8,
        "balanced": 0.65,
        "loose": 0.5,
    }
    if top.confidence < thresholds[match_mode]:
        return None
    return top


def find_match(
    source: MediaRecord,
    target_indexes: Dict[str, List[MediaRecord]],
    match_mode: str,
    min_confidence: float,
) -> Tuple[Optional[MediaRecord], float, str, List[str]]:
    notes: List[str] = []
    basename_candidates = list(target_indexes.get(source.basename_key, []))
    if not basename_candidates:
        return None, 0.0, "basename_exact_missing", notes

    candidate = select_best_candidate(source, basename_candidates, match_mode)
    if candidate and candidate.confidence >= min_confidence:
        return candidate.target, candidate.confidence, candidate.reason, notes

    notes.append(f"basename candidates: {len(basename_candidates)}")
    return None, 0.0, "basename_exact_ambiguous", notes


def collect_matches(
    source_inventory: Sequence[MediaRecord],
    source_history: Dict[str, WatchHistory],
    target_inventory: Sequence[MediaRecord],
    target_history: Dict[str, WatchHistory],
    match_mode: str,
    min_confidence: float,
) -> List[MatchResult]:
    target_indexes = index_target_inventory(target_inventory)
    target_by_guid = {item.guid: item for item in target_inventory}
    results: List[MatchResult] = []

    for source in source_inventory:
        history = source_history.get(source.guid)
        if not history or history.watch_count <= 0:
            continue

        target, confidence, reason, notes = find_match(source, target_indexes, match_mode, min_confidence)
        if not target:
            results.append(
                MatchResult(
                    source=source,
                    source_history=history,
                    status="unmatched",
                    confidence=confidence,
                    reason=reason,
                    target=None,
                    target_history=None,
                    notes=notes,
                )
            )
            continue

        if target.guid not in target_by_guid:
            results.append(
                MatchResult(
                    source=source,
                    source_history=history,
                    status="unmatched",
                    confidence=0.0,
                    reason="target_missing_after_match",
                    target=None,
                    target_history=None,
                    notes=notes,
                )
            )
            continue

        results.append(
            MatchResult(
                source=source,
                source_history=history,
                status="matched",
                confidence=confidence,
                reason=reason,
                target=target,
                target_history=target_history.get(target.guid, WatchHistory(target.guid, 0, None, [])),
                notes=notes,
            )
        )

    return results


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def resolve_insert_defaults(
    schema: PlexSchema,
    target: MediaRecord,
    account_id: Optional[int],
    viewed_at: int,
) -> Dict[str, Any]:
    values: Dict[str, Any] = {}
    columns = schema.metadata_item_views_columns
    if "guid" in columns:
        values["guid"] = target.guid
    if "viewed_at" in columns:
        values["viewed_at"] = viewed_at
    if "account_id" in columns:
        if account_id is None and columns["account_id"].not_null and columns["account_id"].default_value is None:
            raise RuntimeError("Target metadata_item_views requires account_id; pass --account-id.")
        if account_id is not None:
            values["account_id"] = account_id
    if "parent_guid" in columns and target.parent_guid is not None:
        values["parent_guid"] = target.parent_guid
    if "grandparent_guid" in columns and target.grandparent_guid is not None:
        values["grandparent_guid"] = target.grandparent_guid

    unsupported_required = []
    for column in columns.values():
        if column.primary_key_position:
            continue
        if column.name in values:
            continue
        if column.not_null and column.default_value is None:
            unsupported_required.append(column.name)
    if unsupported_required:
        raise RuntimeError(
            "Target metadata_item_views has unsupported required columns: " + ", ".join(sorted(unsupported_required))
        )
    return values


def plan_mutations(
    matches: Sequence[MatchResult],
    schema: PlexSchema,
    account_id: Optional[int],
    conflict_policy: str,
) -> List[PlannedMutation]:
    mutations: List[PlannedMutation] = []
    for match in matches:
        if match.status != "matched" or not match.target or not match.target_history:
            continue

        source_history = match.source_history
        target_history = match.target_history
        delta = source_history.watch_count - target_history.watch_count

        if conflict_policy == "skip" and target_history.watch_count > 0:
            continue

        if conflict_policy == "overwrite" and target_history.watch_count > source_history.watch_count:
            delta = 0

        if delta > 0:
            viewed_at = source_history.last_viewed_at or int(datetime.now().timestamp())
            insert_values = resolve_insert_defaults(schema, match.target, account_id, viewed_at)
            mutations.append(
                PlannedMutation(
                    action="insert_views",
                    target_guid=match.target.guid,
                    details={
                        "count": delta,
                        "values": insert_values,
                        "source_watch_count": source_history.watch_count,
                        "target_watch_count": target_history.watch_count,
                    },
                )
            )

        if (
            source_history.last_viewed_at
            and target_history.watch_count > 0
            and source_history.watch_count == target_history.watch_count
            and (target_history.last_viewed_at or 0) < source_history.last_viewed_at
            and target_history.row_ids
        ):
            mutations.append(
                PlannedMutation(
                    action="update_latest_view",
                    target_guid=match.target.guid,
                    details={
                        "row_id": target_history.row_ids[-1],
                        "viewed_at": source_history.last_viewed_at,
                    },
                )
            )
    return mutations


def apply_mutations(connection: sqlite3.Connection, mutations: Sequence[PlannedMutation]) -> None:
    for mutation in mutations:
        if mutation.action == "insert_views":
            values = mutation.details["values"]
            columns = list(values)
            placeholders = ", ".join("?" for _ in columns)
            quoted_columns = ", ".join(quote_identifier(column) for column in columns)
            sql = f"INSERT INTO metadata_item_views ({quoted_columns}) VALUES ({placeholders})"
            row_values = [values[column] for column in columns]
            for _ in range(int(mutation.details["count"])):
                connection.execute(sql, row_values)
        elif mutation.action == "update_latest_view":
            connection.execute(
                "UPDATE metadata_item_views SET viewed_at = ? WHERE rowid = ?",
                (mutation.details["viewed_at"], mutation.details["row_id"]),
            )
        else:
            raise RuntimeError(f"Unsupported mutation action: {mutation.action}")


def emit_report(report_path: Optional[Path], matches: Sequence[MatchResult], mutations: Sequence[PlannedMutation]) -> None:
    payload = {
        "summary": {
            "matched": sum(1 for item in matches if item.status == "matched"),
            "unmatched": sum(1 for item in matches if item.status == "unmatched"),
            "planned_mutations": len(mutations),
        },
        "matches": [
            {
                "status": match.status,
                "confidence": round(match.confidence, 3),
                "reason": match.reason,
                "notes": match.notes,
                "source": {
                    "guid": match.source.guid,
                    "title": match.source.title,
                    "file_path": match.source.file_path,
                    "basename": match.source.basename,
                    "watch_count": match.source_history.watch_count,
                    "last_viewed_at": match.source_history.last_viewed_at,
                },
                "target": None if not match.target else {
                    "guid": match.target.guid,
                    "title": match.target.title,
                    "file_path": match.target.file_path,
                    "basename": match.target.basename,
                    "existing_watch_count": match.target_history.watch_count if match.target_history else 0,
                    "existing_last_viewed_at": match.target_history.last_viewed_at if match.target_history else None,
                },
            }
            for match in matches
        ],
        "mutations": [asdict(mutation) for mutation in mutations],
    }

    if report_path is None:
        print(json.dumps(payload, indent=2))
        return

    if report_path.suffix.casefold() == ".csv":
        with report_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=[
                    "status",
                    "confidence",
                    "reason",
                    "source_guid",
                    "source_title",
                    "source_basename",
                    "source_watch_count",
                    "source_last_viewed_at",
                    "target_guid",
                    "target_title",
                    "target_basename",
                    "target_watch_count",
                    "target_last_viewed_at",
                    "notes",
                ],
            )
            writer.writeheader()
            for match in matches:
                writer.writerow(
                    {
                        "status": match.status,
                        "confidence": round(match.confidence, 3),
                        "reason": match.reason,
                        "source_guid": match.source.guid,
                        "source_title": match.source.title,
                        "source_basename": match.source.basename,
                        "source_watch_count": match.source_history.watch_count,
                        "source_last_viewed_at": match.source_history.last_viewed_at,
                        "target_guid": match.target.guid if match.target else None,
                        "target_title": match.target.title if match.target else None,
                        "target_basename": match.target.basename if match.target else None,
                        "target_watch_count": match.target_history.watch_count if match.target_history else None,
                        "target_last_viewed_at": match.target_history.last_viewed_at if match.target_history else None,
                        "notes": "; ".join(match.notes),
                    }
                )
        return

    with report_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Transfer Plex watch history between two Plex SQLite library databases using exact basename matching without path dependence."
    )
    parser.add_argument(
        "--source-path",
        required=True,
        help="Path to the source Plex location. Can be the DB file itself or a folder containing com.plexapp.plugins.library.db.",
    )
    parser.add_argument(
        "--target-path",
        required=True,
        help="Path to the target Plex location. Can be the DB file itself or a folder containing com.plexapp.plugins.library.db.",
    )
    parser.add_argument(
        "--source-library",
        action="append",
        default=[],
        help="Source library section name to include. Repeat to include multiple sections.",
    )
    parser.add_argument(
        "--target-library",
        action="append",
        default=[],
        help="Target library section name to include. Repeat to include multiple sections.",
    )
    parser.add_argument(
        "--match-mode",
        choices=["strict", "balanced", "loose"],
        default="balanced",
        help="Controls how strict duplicate resolution is when multiple target rows share the exact same basename.",
    )
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=0.65,
        help="Minimum confidence required to resolve duplicate exact-basename candidates.",
    )
    parser.add_argument(
        "--conflict-policy",
        choices=["merge", "overwrite", "skip"],
        default="merge",
        help="How to handle target items that already have watch history.",
    )
    parser.add_argument(
        "--account-id",
        type=int,
        default=None,
        help="Account id to use when the target metadata_item_views schema requires it.",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=None,
        help="Optional path for a JSON or CSV report. Defaults to printing JSON to stdout.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write the planned mutations into the target DB. Without this flag the tool is dry-run only.",
    )
    return parser.parse_args(argv)


def find_db_candidates(base_path: Path) -> List[Path]:
    if base_path.is_file():
        return [base_path]

    common_candidate = base_path / "Plug-in Support" / "Databases" / PLEX_DB_FILENAME
    candidates: List[Path] = []
    if common_candidate.exists():
        candidates.append(common_candidate)

    candidates.extend(sorted(path for path in base_path.rglob(PLEX_DB_FILENAME) if path.is_file()))

    unique_candidates: List[Path] = []
    seen = set()
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved not in seen:
            seen.add(resolved)
            unique_candidates.append(resolved)
    return unique_candidates


def validate_plex_db(db_path: Path) -> None:
    try:
        connection = connect_db(db_path, readonly=True)
    except sqlite3.Error as exc:
        raise RuntimeError(f"Failed to open SQLite database: {db_path}: {exc}") from exc

    try:
        inspect_schema(connection)
    finally:
        connection.close()


def resolve_db_path(path_value: str, label: str) -> Path:
    base_path = Path(path_value).expanduser().resolve()
    if not base_path.exists():
        raise FileNotFoundError(f"{label} path not found: {base_path}")

    candidates = find_db_candidates(base_path)
    if not candidates:
        raise FileNotFoundError(
            f"Could not find {PLEX_DB_FILENAME} under {base_path} for {label}."
        )
    if len(candidates) > 1:
        choices = "\n".join(f"- {candidate}" for candidate in candidates)
        raise RuntimeError(
            f"Found multiple {PLEX_DB_FILENAME} files under {base_path} for {label}. Narrow the path.\n{choices}"
        )

    db_path = candidates[0]
    if db_path.name != PLEX_DB_FILENAME:
        raise RuntimeError(
            f"{label} path resolved to {db_path}, but the filename must be {PLEX_DB_FILENAME}."
        )

    validate_plex_db(db_path)
    return db_path


def get_running_plex_processes() -> List[str]:
    if os.name == "nt":
        command = ["tasklist", "/FO", "CSV", "/NH"]
        result = subprocess.run(command, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            return []

        running = []
        for line in result.stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            process_name = line.split(",", 1)[0].strip().strip('"')
            if process_name in PLEX_PROCESS_NAMES:
                running.append(process_name)
        return running

    command = ["ps", "-A", "-o", "comm="]
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        return []

    running = []
    for line in result.stdout.splitlines():
        process_name = Path(line.strip()).name
        if process_name in PLEX_PROCESS_NAMES:
            running.append(process_name)
    return running


def wait_for_plex_shutdown(poll_interval_seconds: float = 3.0) -> None:
    running = get_running_plex_processes()
    if not running:
        return

    print("Plex is currently running. Stop Plex Media Server before applying changes.")
    print("Waiting for Plex to exit before continuing...")
    while running:
        print("Still running: " + ", ".join(sorted(set(running))))
        time.sleep(poll_interval_seconds)
        running = get_running_plex_processes()
    print("Plex is no longer running. Continuing with apply mode.")


def print_summary(matches: Sequence[MatchResult], mutations: Sequence[PlannedMutation], apply: bool) -> None:
    matched = sum(1 for item in matches if item.status == "matched")
    unmatched = sum(1 for item in matches if item.status == "unmatched")
    print(f"Matched watched items: {matched}")
    print(f"Unmatched watched items: {unmatched}")
    print(f"Planned mutations: {len(mutations)}")
    print("Mode: apply" if apply else "Mode: dry-run")


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    source_db = resolve_db_path(args.source_path, "source")
    target_db = resolve_db_path(args.target_path, "target")

    if args.apply:
        wait_for_plex_shutdown()

    source_connection = connect_db(source_db, readonly=True)
    target_connection = connect_db(target_db, readonly=not args.apply)

    try:
        source_schema = inspect_schema(source_connection)
        target_schema = inspect_schema(target_connection)

        source_inventory = build_media_inventory(source_connection, source_schema, args.source_library)
        target_inventory = build_media_inventory(target_connection, target_schema, args.target_library)
        source_history = build_watch_history(source_connection, args.source_library)
        target_history = build_watch_history(target_connection, args.target_library)

        matches = collect_matches(
            source_inventory=source_inventory,
            source_history=source_history,
            target_inventory=target_inventory,
            target_history=target_history,
            match_mode=args.match_mode,
            min_confidence=args.min_confidence,
        )
        mutations = plan_mutations(matches, target_schema, args.account_id, args.conflict_policy)

        if args.apply:
            target_connection.execute("BEGIN IMMEDIATE")
            apply_mutations(target_connection, mutations)
            target_connection.commit()

        emit_report(args.report, matches, mutations)
        print_summary(matches, mutations, args.apply)
        return 0
    finally:
        source_connection.close()
        target_connection.close()


if __name__ == "__main__":
    raise SystemExit(main())