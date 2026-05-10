import argparse
import csv
import errno
import json
import os
import sqlite3
import subprocess
import sys
import time
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, TextIO, Tuple


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
    library_section_id: Optional[int]
    library_section_name: Optional[str]
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
    dry_run_status: str = "unknown"
    account_status: str = "not_applicable"
    library_status: str = "not_requested"


@dataclass
class PlannedMutation:
    action: str
    target_guid: str
    details: Dict[str, Any]


@dataclass(frozen=True)
class TableColumnSpec:
    name: str
    width: Optional[int] = None


@dataclass(frozen=True)
class PlexLibrarySection:
    id: int
    name: str
    section_type: Optional[int]
    agent: Optional[str]
    scanner: Optional[str]
    language: Optional[str]
    public: Optional[int]


@dataclass(frozen=True)
class PlexAccount:
    id: int
    name: str
    default_audio_language: Optional[str]
    default_subtitle_language: Optional[str]
    auto_select_audio: Optional[int]
    auto_select_subtitle: Optional[int]


class PlexFilenameParser:
    _guessit_wrapper = None
    _guessit_loaded = False

    @staticmethod
    def normalize_basename(file_path: str) -> str:
        return Path(file_path).name.casefold()

    @staticmethod
    def normalize_title(value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        collapsed = " ".join(value.casefold().replace("_", " ").split())
        return collapsed or None

    @staticmethod
    def safe_int(value: Any) -> Optional[int]:
        if value is None or value == "":
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @classmethod
    def _get_guessit_wrapper(cls):
        if not cls._guessit_loaded:
            cls._guessit_loaded = True
            try:
                from guessit_wrapper import guessit_wrapper
            except Exception:
                cls._guessit_wrapper = None
            else:
                cls._guessit_wrapper = guessit_wrapper
        return cls._guessit_wrapper

    @classmethod
    def parse_identity(cls, basename: str) -> ParsedIdentity:
        guessit_wrapper = cls._get_guessit_wrapper()
        if not guessit_wrapper:
            return ParsedIdentity(None, None, None)

        try:
            result = guessit_wrapper(basename)
        except Exception:
            return ParsedIdentity(None, None, None)

        return ParsedIdentity(
            title_key=cls.normalize_title(result.get("title")),
            season=cls.safe_int(result.get("season")),
            episode=cls.safe_int(result.get("episode")),
        )


class PlexDatabase:
    def __init__(self, db_path: Path, readonly: bool) -> None:
        self.db_path = db_path
        self.readonly = readonly
        self.connection = self._connect()

    def _connect(self) -> sqlite3.Connection:
        if self.readonly:
            uri = f"file:{self.db_path.as_posix()}?mode=ro"
            connection = sqlite3.connect(uri, uri=True)
        else:
            connection = sqlite3.connect(str(self.db_path))
        connection.row_factory = sqlite3.Row
        return connection

    def close(self) -> None:
        self.connection.close()

    def load_table_columns(self, table_name: str) -> Dict[str, TableColumn]:
        rows = self.connection.execute(f"PRAGMA table_info({table_name})").fetchall()
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

    def inspect_schema(self) -> PlexSchema:
        return PlexSchema(
            media_parts_columns=self.load_table_columns("media_parts"),
            metadata_item_views_columns=self.load_table_columns("metadata_item_views"),
        )

    def build_media_inventory(
        self,
        schema: PlexSchema,
        library_filters: Sequence[str],
    ) -> List[MediaRecord]:
        size_select = f", mp.{schema.size_column} AS file_size" if schema.size_column else ", NULL AS file_size"
        duration_select = (
            f", mp.{schema.duration_column} AS duration" if schema.duration_column else ", NULL AS duration"
        )

        query = f"""
        SELECT
            md.guid AS guid,
            md.title AS title,
            md.year AS year,
            mp.file AS file_path,
            ls.id AS library_section_id,
            ls.name AS library_section_name,
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
        for row in self.connection.execute(query, params):
            file_path = row["file_path"]
            basename = Path(file_path).name
            inventory.append(
                MediaRecord(
                    guid=row["guid"],
                    title=row["title"],
                    year=PlexFilenameParser.safe_int(row["year"]),
                    file_path=file_path,
                    library_section_id=PlexFilenameParser.safe_int(row["library_section_id"]),
                    library_section_name=row["library_section_name"],
                    basename=basename,
                    basename_key=PlexFilenameParser.normalize_basename(file_path),
                    file_size=PlexFilenameParser.safe_int(row["file_size"]),
                    duration=PlexFilenameParser.safe_int(row["duration"]),
                    parent_guid=row["parent_guid"],
                    grandparent_guid=row["grandparent_guid"],
                    parsed_identity=PlexFilenameParser.parse_identity(basename),
                )
            )
        return inventory

    def build_watch_history(self, library_filters: Sequence[str]) -> Dict[str, WatchHistory]:
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
        for row in self.connection.execute(query, params):
            row_ids: List[int] = []
            if row["row_ids"]:
                row_ids = [int(value) for value in str(row["row_ids"]).split(",") if value]
            history[row["guid"]] = WatchHistory(
                guid=row["guid"],
                watch_count=int(row["watch_count"] or 0),
                last_viewed_at=PlexFilenameParser.safe_int(row["last_viewed_at"]),
                row_ids=row_ids,
            )
        return history

    def list_library_sections(self) -> List[PlexLibrarySection]:
        query = """
        SELECT
            id,
            name,
            section_type,
            agent,
            scanner,
            language,
            public
        FROM library_sections
        ORDER BY name COLLATE NOCASE, id
        """
        return [
            PlexLibrarySection(
                id=int(row["id"]),
                name=str(row["name"] or ""),
                section_type=PlexFilenameParser.safe_int(row["section_type"]),
                agent=row["agent"],
                scanner=row["scanner"],
                language=row["language"],
                public=PlexFilenameParser.safe_int(row["public"]),
            )
            for row in self.connection.execute(query)
        ]

    def list_accounts(self) -> List[PlexAccount]:
        query = """
        SELECT
            id,
            name,
            default_audio_language,
            default_subtitle_language,
            auto_select_audio,
            auto_select_subtitle
        FROM accounts
        ORDER BY name COLLATE NOCASE, id
        """
        return [
            PlexAccount(
                id=int(row["id"]),
                name=str(row["name"] or ""),
                default_audio_language=row["default_audio_language"],
                default_subtitle_language=row["default_subtitle_language"],
                auto_select_audio=PlexFilenameParser.safe_int(row["auto_select_audio"]),
                auto_select_subtitle=PlexFilenameParser.safe_int(row["auto_select_subtitle"]),
            )
            for row in self.connection.execute(query)
        ]

    def apply_mutations(self, mutations: Sequence[PlannedMutation]) -> None:
        for mutation in mutations:
            if mutation.action == "insert_views":
                values = mutation.details["values"]
                columns = list(values)
                placeholders = ", ".join("?" for _ in columns)
                quoted_columns = ", ".join(self.quote_identifier(column) for column in columns)
                sql = f"INSERT INTO metadata_item_views ({quoted_columns}) VALUES ({placeholders})"
                row_values = [values[column] for column in columns]
                for _ in range(int(mutation.details["count"])):
                    self.connection.execute(sql, row_values)
            elif mutation.action == "update_latest_view":
                self.connection.execute(
                    "UPDATE metadata_item_views SET viewed_at = ? WHERE rowid = ?",
                    (mutation.details["viewed_at"], mutation.details["row_id"]),
                )
            else:
                raise RuntimeError(f"Unsupported mutation action: {mutation.action}")

    def begin_immediate(self) -> None:
        self.connection.execute("BEGIN IMMEDIATE")

    def commit(self) -> None:
        self.connection.commit()

    @staticmethod
    def quote_identifier(identifier: str) -> str:
        return '"' + identifier.replace('"', '""') + '"'


class PlexDatabaseLocator:
    PLEX_DB_FILENAME = "com.plexapp.plugins.library.db"

    @classmethod
    def find_db_candidates(cls, base_path: Path) -> List[Path]:
        if base_path.is_file():
            return [base_path.resolve()]

        common_candidate = base_path / "Plug-in Support" / "Databases" / cls.PLEX_DB_FILENAME
        candidates: List[Path] = []
        if common_candidate.exists():
            candidates.append(common_candidate)

        candidates.extend(sorted(path for path in base_path.rglob(cls.PLEX_DB_FILENAME) if path.is_file()))

        unique_candidates: List[Path] = []
        seen = set()
        for candidate in candidates:
            resolved = candidate.resolve()
            if resolved not in seen:
                seen.add(resolved)
                unique_candidates.append(resolved)
        return unique_candidates

    @classmethod
    def validate_plex_db(cls, db_path: Path) -> None:
        try:
            database = PlexDatabase(db_path, readonly=True)
        except sqlite3.Error as exc:
            raise RuntimeError(f"Failed to open SQLite database: {db_path}: {exc}") from exc

        try:
            database.inspect_schema()
        finally:
            database.close()

    @classmethod
    def resolve_db_path(cls, path_value: str, label: str) -> Path:
        base_path = Path(path_value).expanduser().resolve()
        if not base_path.exists():
            raise FileNotFoundError(f"{label} path not found: {base_path}")

        candidates = cls.find_db_candidates(base_path)
        if not candidates:
            raise FileNotFoundError(
                f"Could not find {cls.PLEX_DB_FILENAME} under {base_path} for {label}."
            )
        if len(candidates) > 1:
            choices = "\n".join(f"- {candidate}" for candidate in candidates)
            raise RuntimeError(
                f"Found multiple {cls.PLEX_DB_FILENAME} files under {base_path} for {label}. Narrow the path.\n{choices}"
            )

        db_path = candidates[0]
        if db_path.name != cls.PLEX_DB_FILENAME:
            raise RuntimeError(
                f"{label} path resolved to {db_path}, but the filename must be {cls.PLEX_DB_FILENAME}."
            )

        cls.validate_plex_db(db_path)
        return db_path


class PlexEnvironment:
    PLEX_PROCESS_NAMES = (
        "Plex Media Server.exe",
        "Plex Media Server",
    )

    @classmethod
    def get_running_plex_processes(cls) -> List[str]:
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
                if process_name in cls.PLEX_PROCESS_NAMES:
                    running.append(process_name)
            return running

        command = ["ps", "-A", "-o", "comm="]
        result = subprocess.run(command, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            return []

        running = []
        for line in result.stdout.splitlines():
            process_name = Path(line.strip()).name
            if process_name in cls.PLEX_PROCESS_NAMES:
                running.append(process_name)
        return running

    @classmethod
    def wait_for_plex_shutdown(cls, poll_interval_seconds: float = 3.0) -> None:
        running = cls.get_running_plex_processes()
        if not running:
            return

        print("Plex is currently running. Stop Plex Media Server before applying changes.")
        print("Waiting for Plex to exit before continuing...")
        while running:
            print("Still running: " + ", ".join(sorted(set(running))))
            time.sleep(poll_interval_seconds)
            running = cls.get_running_plex_processes()
        print("Plex is no longer running. Continuing with apply mode.")


class PlexMatcher:
    MATCH_THRESHOLDS = {
        "strict": 0.8,
        "balanced": 0.65,
        "loose": 0.5,
    }

    def __init__(self, match_mode: str, min_confidence: float) -> None:
        self.match_mode = match_mode
        self.min_confidence = min_confidence

    @staticmethod
    def index_target_inventory(target_inventory: Iterable[MediaRecord]) -> Dict[str, List[MediaRecord]]:
        by_basename: Dict[str, List[MediaRecord]] = defaultdict(list)
        for record in target_inventory:
            by_basename[record.basename_key].append(record)
        return by_basename

    def select_best_candidate(
        self,
        source: MediaRecord,
        candidates: Sequence[MediaRecord],
    ) -> Optional[MatchCandidate]:
        if not candidates:
            return None

        scored: List[MatchCandidate] = []
        source_title_key = PlexFilenameParser.normalize_title(source.title) or source.parsed_identity.title_key
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

            candidate_title_key = PlexFilenameParser.normalize_title(candidate.title) or candidate.parsed_identity.title_key
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

        if top.confidence < self.MATCH_THRESHOLDS[self.match_mode]:
            return None
        return top

    def find_match(
        self,
        source: MediaRecord,
        target_indexes: Dict[str, List[MediaRecord]],
    ) -> Tuple[Optional[MediaRecord], float, str, List[str]]:
        notes: List[str] = []
        basename_candidates = list(target_indexes.get(source.basename_key, []))
        if not basename_candidates:
            return None, 0.0, "basename_exact_missing", notes

        candidate = self.select_best_candidate(source, basename_candidates)
        if candidate and candidate.confidence >= self.min_confidence:
            return candidate.target, candidate.confidence, candidate.reason, notes

        notes.append(f"basename candidates: {len(basename_candidates)}")
        return None, 0.0, "basename_exact_ambiguous", notes

    def collect_matches(
        self,
        source_inventory: Sequence[MediaRecord],
        source_history: Dict[str, WatchHistory],
        target_inventory: Sequence[MediaRecord],
        target_history: Dict[str, WatchHistory],
    ) -> List[MatchResult]:
        target_indexes = self.index_target_inventory(target_inventory)
        results: List[MatchResult] = []

        for source in source_inventory:
            history = source_history.get(source.guid)
            if not history or history.watch_count <= 0:
                continue

            target, confidence, reason, notes = self.find_match(source, target_indexes)
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
                        dry_run_status="unmatched",
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
                    dry_run_status="matched",
                )
            )
        return results


class PlexMutationPlanner:
    def __init__(self, schema: PlexSchema, account_id: Optional[int], conflict_policy: str) -> None:
        self.schema = schema
        self.account_id = account_id
        self.conflict_policy = conflict_policy

    def requires_account_id(self) -> bool:
        column = self.schema.metadata_item_views_columns.get("account_id")
        return bool(column and column.not_null and column.default_value is None)

    def resolve_insert_defaults(self, target: MediaRecord, viewed_at: int) -> Tuple[Optional[Dict[str, Any]], str, Optional[str]]:
        values: Dict[str, Any] = {}
        columns = self.schema.metadata_item_views_columns
        account_status = "not_needed"
        if "guid" in columns:
            values["guid"] = target.guid
        if "viewed_at" in columns:
            values["viewed_at"] = viewed_at
        if "account_id" in columns:
            if self.account_id is None and columns["account_id"].not_null and columns["account_id"].default_value is None:
                return None, "missing_required", "Target metadata_item_views requires account_id; pass --account-id."
            if self.account_id is not None:
                values["account_id"] = self.account_id
                account_status = "provided"
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
            return None, account_status, (
                "Target metadata_item_views has unsupported required columns: " + ", ".join(sorted(unsupported_required))
            )
        return values, account_status, None

    def plan_mutations(self, matches: Sequence[MatchResult]) -> List[PlannedMutation]:
        mutations: List[PlannedMutation] = []
        for match in matches:
            if match.status != "matched" or not match.target or not match.target_history:
                continue

            source_history = match.source_history
            target_history = match.target_history
            delta = source_history.watch_count - target_history.watch_count
            match.account_status = "not_needed"
            match.dry_run_status = "in_sync"

            if self.conflict_policy == "skip" and target_history.watch_count > 0:
                match.dry_run_status = "skipped_conflict"
                continue

            if self.conflict_policy == "overwrite" and target_history.watch_count > source_history.watch_count:
                delta = 0
                match.dry_run_status = "target_ahead"

            if delta > 0:
                viewed_at = source_history.last_viewed_at or int(datetime.now().timestamp())
                insert_values, account_status, error_message = self.resolve_insert_defaults(match.target, viewed_at)
                match.account_status = account_status
                if error_message:
                    match.dry_run_status = "missing_required_account" if account_status == "missing_required" else "blocked_required_columns"
                    match.notes.append(error_message)
                    continue
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
                match.dry_run_status = "ready_insert"
                continue

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
                match.dry_run_status = "ready_update"
        return mutations


class PlexReportWriter:
    MATCH_ROW_COLUMNS = (
        "status",
        "dry_run_status",
        "account_status",
        "library_status",
        "confidence",
        "reason",
        "source_library",
        "source_guid",
        "source_title",
        "source_filename",
        "source_path",
        "source_watch_count",
        "source_last_viewed_at",
        "target_library",
        "target_guid",
        "target_title",
        "target_filename",
        "target_path",
        "target_watch_count",
        "target_last_viewed_at",
        "notes",
    )
    TABLE_DEFAULT_COLUMNS = (
        "status",
        "source_filename",
        "source_watch_count",
        "target_filename",
        "target_watch_count",
    )
    TABLE_MANDATORY_COLUMNS = (
        "status",
        "source_filename",
    )
    TABLE_COLUMN_LABELS = {
        "status": "status",
        "dry_run_status": "dry_run",
        "account_status": "acct",
        "library_status": "library",
        "confidence": "conf",
        "reason": "reason",
        "id": "id",
        "source_library": "src_lib",
        "source_guid": "src_guid",
        "source_title": "src_title",
        "source_filename": "src_file",
        "source_path": "src_path",
        "source_watch_count": "src_cnt",
        "source_last_viewed_at": "src_seen",
        "name": "name",
        "section_type": "type",
        "agent": "agent",
        "scanner": "scanner",
        "language": "lang",
        "public": "public",
        "default_audio_language": "audio_lang",
        "default_subtitle_language": "sub_lang",
        "auto_select_audio": "auto_audio",
        "auto_select_subtitle": "auto_sub",
        "target_library": "tgt_lib",
        "target_guid": "tgt_guid",
        "target_title": "tgt_title",
        "target_filename": "tgt_file",
        "target_path": "tgt_path",
        "target_watch_count": "tgt_cnt",
        "target_last_viewed_at": "tgt_seen",
        "notes": "notes",
    }
    TABLE_NUMERIC_COLUMNS = {
        "confidence",
        "id",
        "section_type",
        "public",
        "auto_select_audio",
        "auto_select_subtitle",
        "source_watch_count",
        "source_last_viewed_at",
        "target_watch_count",
        "target_last_viewed_at",
    }
    TABLE_COLUMN_MAX_WIDTHS = {
        "status": 10,
        "dry_run_status": 24,
        "account_status": 18,
        "library_status": 14,
        "confidence": 10,
        "reason": 28,
        "id": 6,
        "source_library": 24,
        "source_guid": 40,
        "source_title": 36,
        "source_filename": 44,
        "source_path": 56,
        "source_watch_count": 6,
        "source_last_viewed_at": 18,
        "name": 28,
        "section_type": 6,
        "agent": 28,
        "scanner": 24,
        "language": 10,
        "public": 6,
        "default_audio_language": 12,
        "default_subtitle_language": 12,
        "auto_select_audio": 10,
        "auto_select_subtitle": 9,
        "target_library": 24,
        "target_guid": 40,
        "target_title": 36,
        "target_filename": 44,
        "target_path": 56,
        "target_watch_count": 6,
        "target_last_viewed_at": 18,
        "notes": 40,
    }
    TABLE_FALLBACK_MAX_WIDTH = 32

    def build_payload(self, matches: Sequence[MatchResult], mutations: Sequence[PlannedMutation]) -> Dict[str, Any]:
        payload = {
            "summary": {
                "matched": sum(1 for item in matches if item.status == "matched"),
                "unmatched": sum(1 for item in matches if item.status == "unmatched"),
                "planned_mutations": len(mutations),
            },
            "matches": [
                {
                    "status": match.status,
                    "dry_run_status": match.dry_run_status,
                    "account_status": match.account_status,
                    "library_status": match.library_status,
                    "confidence": round(match.confidence, 3),
                    "reason": match.reason,
                    "notes": match.notes,
                    "source": {
                        "guid": match.source.guid,
                        "library": match.source.library_section_name,
                        "title": match.source.title,
                        "file_path": match.source.file_path,
                        "basename": match.source.basename,
                        "watch_count": match.source_history.watch_count,
                        "last_viewed_at": match.source_history.last_viewed_at,
                    },
                    "target": None if not match.target else {
                        "guid": match.target.guid,
                        "library": match.target.library_section_name,
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

        return payload

    def build_match_rows(self, matches: Sequence[MatchResult]) -> List[Dict[str, Any]]:
        return [
            {
                "status": match.status,
                "dry_run_status": match.dry_run_status,
                "account_status": match.account_status,
                "library_status": match.library_status,
                "confidence": round(match.confidence, 3),
                "reason": match.reason,
                "source_library": match.source.library_section_name,
                "source_guid": match.source.guid,
                "source_title": match.source.title,
                "source_filename": match.source.basename,
                "source_path": match.source.file_path,
                "source_watch_count": match.source_history.watch_count,
                "source_last_viewed_at": match.source_history.last_viewed_at,
                "target_library": match.target.library_section_name if match.target else None,
                "target_guid": match.target.guid if match.target else None,
                "target_title": match.target.title if match.target else None,
                "target_filename": match.target.basename if match.target else None,
                "target_path": match.target.file_path if match.target else None,
                "target_watch_count": match.target_history.watch_count if match.target_history else None,
                "target_last_viewed_at": match.target_history.last_viewed_at if match.target_history else None,
                "notes": "; ".join(match.notes),
            }
            for match in matches
        ]

    def emit_console(
        self,
        console_format: str,
        matches: Sequence[MatchResult],
        mutations: Sequence[PlannedMutation],
        columns: Optional[Sequence[TableColumnSpec]] = None,
    ) -> None:
        payload = self.build_payload(matches, mutations)
        rows = self.build_match_rows(matches)

        try:
            if console_format == "json":
                print(json.dumps(payload, indent=2))
                sys.stdout.flush()
                return

            if console_format == "csv":
                self._write_csv_rows(sys.stdout, rows, self.MATCH_ROW_COLUMNS)
                sys.stdout.flush()
                return

            if console_format == "table":
                resolved_columns = self.resolve_table_columns(columns)
                self._write_table(sys.stdout, rows, resolved_columns)
                sys.stdout.flush()
                return
        except OSError as exc:
            if self._is_broken_pipe_error(exc):
                self._suppress_stdout_after_pipe_error()
                return
            raise

        raise RuntimeError(f"Unsupported console format: {console_format}")

    def emit_report(
        self,
        report_path: Optional[Path],
        report_format: str,
        matches: Sequence[MatchResult],
        mutations: Sequence[PlannedMutation],
        columns: Optional[Sequence[TableColumnSpec]] = None,
    ) -> None:
        if report_path is None:
            return

        resolved_format = self.resolve_report_format(report_path, report_format)
        payload = self.build_payload(matches, mutations)
        rows = self.build_match_rows(matches)

        if resolved_format == "json":
            with report_path.open("w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2)
            return

        if resolved_format == "csv":
            with report_path.open("w", newline="", encoding="utf-8") as handle:
                self._write_csv_rows(handle, rows, self.MATCH_ROW_COLUMNS)
            return

        if resolved_format == "table":
            with report_path.open("w", encoding="utf-8") as handle:
                self._write_table(handle, rows, self.resolve_table_columns(columns))
            return

        raise RuntimeError(f"Unsupported report format: {resolved_format}")

    @classmethod
    def resolve_report_format(cls, report_path: Path, report_format: str) -> str:
        if report_format != "auto":
            return report_format

        if report_path.suffix.casefold() == ".csv":
            return "csv"
        if report_path.suffix.casefold() in {".table", ".txt"}:
            return "table"
        return "json"

    @classmethod
    def resolve_table_columns(cls, columns: Optional[Sequence[TableColumnSpec]]) -> List[TableColumnSpec]:
        requested = list(columns or [TableColumnSpec(name) for name in cls.TABLE_DEFAULT_COLUMNS])
        resolved: List[TableColumnSpec] = []
        seen = set()

        for column in cls.TABLE_MANDATORY_COLUMNS:
            if column not in seen:
                resolved.append(TableColumnSpec(column, cls.TABLE_COLUMN_MAX_WIDTHS.get(column)))
                seen.add(column)

        for column in requested:
            if column.name not in cls.MATCH_ROW_COLUMNS:
                supported = ", ".join(cls.MATCH_ROW_COLUMNS)
                raise RuntimeError(f"Unsupported column '{column.name}'. Supported columns: {supported}")
            if column.name in seen:
                continue
            resolved.append(
                TableColumnSpec(
                    name=column.name,
                    width=column.width if column.width is not None else cls.TABLE_COLUMN_MAX_WIDTHS.get(column.name),
                )
            )
            seen.add(column.name)
        return resolved

    @staticmethod
    def _stringify_cell(value: Any) -> str:
        if value is None:
            return ""
        return str(value)

    @staticmethod
    def _truncate_cell(value: str, width: int) -> str:
        if width < 1:
            raise RuntimeError("Table column width must be at least 1.")
        if len(value) <= width:
            return value
        if width <= 3:
            return "." * width
        return value[: width - 3] + "..."

    @staticmethod
    def _is_broken_pipe_error(exc: OSError) -> bool:
        return exc.errno in {errno.EPIPE, errno.EINVAL, errno.ECONNRESET}

    @staticmethod
    def _suppress_stdout_after_pipe_error() -> None:
        devnull = open(os.devnull, "w", encoding="utf-8")
        try:
            os.dup2(devnull.fileno(), sys.stdout.fileno())
        except (AttributeError, OSError):
            pass
        sys.stdout = devnull

    @classmethod
    def detach_redirected_stdout(cls) -> None:
        try:
            if sys.stdout.isatty():
                return
        except (AttributeError, OSError, ValueError):
            return

        try:
            sys.stdout.flush()
        except OSError as exc:
            if not cls._is_broken_pipe_error(exc):
                raise
        cls._suppress_stdout_after_pipe_error()

    @classmethod
    def _write_csv_rows(cls, handle: TextIO, rows: Sequence[Dict[str, Any]], fieldnames: Sequence[str]) -> None:
        writer = csv.DictWriter(handle, fieldnames=list(fieldnames), lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in fieldnames})

    @classmethod
    def write_table_rows(cls, handle: TextIO, rows: Sequence[Dict[str, Any]], columns: Sequence[TableColumnSpec]) -> None:
        cls._write_table(handle, rows, columns)

    @classmethod
    def _write_table(cls, handle: TextIO, rows: Sequence[Dict[str, Any]], columns: Sequence[TableColumnSpec]) -> None:
        if not columns:
            raise RuntimeError("Table output requires at least one column.")

        labels = {
            column.name: cls.TABLE_COLUMN_LABELS.get(column.name, column.name)
            for column in columns
        }
        max_widths = {
            column.name: max(
                len(labels[column.name]),
                column.width if column.width is not None else cls.TABLE_FALLBACK_MAX_WIDTH,
            )
            for column in columns
        }
        widths = {
            column.name: len(labels[column.name])
            for column in columns
        }
        string_rows: List[Dict[str, str]] = []
        for row in rows:
            string_row = {}
            for column in columns:
                value = cls._truncate_cell(
                    cls._stringify_cell(row.get(column.name)),
                    max_widths[column.name],
                )
                string_row[column.name] = value
                widths[column.name] = max(widths[column.name], len(value))
            string_rows.append(string_row)

        header = " | ".join(labels[column.name].ljust(widths[column.name]) for column in columns)
        separator = "-+-".join("-" * widths[column.name] for column in columns)
        handle.write(header + "\n")
        handle.write(separator + "\n")
        for row in string_rows:
            handle.write(
                " | ".join(
                    cls._format_table_cell(row[column.name], widths[column.name], column.name)
                    for column in columns
                )
                + "\n"
            )

    @classmethod
    def _format_table_cell(cls, value: str, width: int, column_name: str) -> str:
        if column_name in cls.TABLE_NUMERIC_COLUMNS:
            return value.rjust(width)
        return value.ljust(width)

    @staticmethod
    def parse_columns(columns_value: Optional[str]) -> Optional[List[TableColumnSpec]]:
        if not columns_value:
            return None
        columns: List[TableColumnSpec] = []
        for raw_column in columns_value.split(","):
            column = raw_column.strip()
            if not column:
                continue
            if ":" not in column:
                columns.append(TableColumnSpec(column))
                continue
            name, width_text = column.rsplit(":", 1)
            name = name.strip()
            width_text = width_text.strip()
            if not name:
                raise RuntimeError("Column overrides must include a column name before ':'.")
            if not width_text.isdigit():
                raise RuntimeError(f"Invalid width override '{column}'. Use column_name:width.")
            width = int(width_text)
            if width < 1:
                raise RuntimeError(f"Invalid width override '{column}'. Width must be at least 1.")
            columns.append(TableColumnSpec(name, width))
        return columns or None

    @classmethod
    def print_summary(
        cls,
        matches: Sequence[MatchResult],
        mutations: Sequence[PlannedMutation],
        apply: bool,
        stream: TextIO = sys.stdout,
    ) -> None:
        matched = sum(1 for item in matches if item.status == "matched")
        unmatched = sum(1 for item in matches if item.status == "unmatched")
        try:
            print(f"Matched watched items: {matched}", file=stream)
            print(f"Unmatched watched items: {unmatched}", file=stream)
            print(f"Planned mutations: {len(mutations)}", file=stream)
            print("Mode: apply" if apply else "Mode: dry-run", file=stream)
            stream.flush()
        except OSError as exc:
            if cls._is_broken_pipe_error(exc):
                if stream is sys.stdout:
                    cls._suppress_stdout_after_pipe_error()
                return
            raise


class PlexWatchStatusTransferApp:
    DRY_RUN_FILTER_MODES = {"all", "warnings", "errors"}

    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.report_writer = PlexReportWriter()

    @classmethod
    def build_parser(cls) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(
            description="Manage Plex watch history and inspect Plex SQLite library databases."
        )
        subparsers = parser.add_subparsers(dest="command")

        transfer_parser = subparsers.add_parser(
            "transfer",
            help="Transfer Plex watch history between two Plex library databases.",
            description="Transfer Plex watch history between two Plex SQLite library databases using exact basename matching without path dependence.",
        )
        transfer_parser.set_defaults(command="transfer")
        transfer_parser.add_argument(
            "--source-path",
            required=True,
            help="Path to the source Plex location. Can be the DB file itself or a folder containing com.plexapp.plugins.library.db.",
        )
        transfer_parser.add_argument(
            "--target-path",
            required=True,
            help="Path to the target Plex location. Can be the DB file itself or a folder containing com.plexapp.plugins.library.db.",
        )
        transfer_parser.add_argument(
            "--source-library",
            action="append",
            default=[],
            help="Source library section name to include. Repeat to include multiple sections.",
        )
        transfer_parser.add_argument(
            "--target-library",
            action="append",
            default=[],
            help="Target library section name to include. Repeat to include multiple sections.",
        )
        transfer_parser.add_argument(
            "--match-mode",
            choices=["strict", "balanced", "loose"],
            default="balanced",
            help="Controls how strict duplicate resolution is when multiple target rows share the exact same basename.",
        )
        transfer_parser.add_argument(
            "--min-confidence",
            type=float,
            default=0.65,
            help="Minimum confidence required to resolve duplicate exact-basename candidates.",
        )
        transfer_parser.add_argument(
            "--conflict-policy",
            choices=["merge", "overwrite", "skip"],
            default="merge",
            help="How to handle target items that already have watch history.",
        )
        transfer_parser.add_argument(
            "--account-id",
            type=int,
            default=None,
            help="Account id to use when the target metadata_item_views schema requires it.",
        )
        transfer_parser.add_argument(
            "--report",
            type=Path,
            default=None,
            help="Optional path for a JSON, CSV, or plain-text table report.",
        )
        transfer_parser.add_argument(
            "--report-format",
            choices=["auto", "json", "csv", "table"],
            default="auto",
            help="Explicit report format. Defaults to auto-detecting from the report file extension.",
        )
        transfer_parser.add_argument(
            "--console-format",
            choices=["json", "csv", "table"],
            default="table",
            help="Console output format for match results.",
        )
        transfer_parser.add_argument(
            "--columns",
            default=None,
            help=(
                "Comma-separated column list for table output. Use column or column:width. "
                f"Mandatory columns are: {', '.join(PlexReportWriter.TABLE_MANDATORY_COLUMNS)}"
            ),
        )
        transfer_parser.add_argument(
            "--apply",
            action="store_true",
            help="Write the planned mutations into the target DB. Without this flag the tool is dry-run only.",
        )
        transfer_parser.add_argument(
            "--dry-run-status-filter",
            choices=["all", "warnings", "errors"],
            default="all",
            help=(
                "Dry-run row filter mode. Use 'all' to show every row, 'warnings' to show unmatched rows, "
                "or 'errors' to show the remaining problem rows."
            ),
        )

        list_libraries_parser = subparsers.add_parser(
            "list-libraries",
            help="List Plex library sections in a Plex library database.",
        )
        list_libraries_parser.set_defaults(command="list-libraries")
        list_libraries_parser.add_argument(
            "--path",
            required=True,
            help="Path to the Plex location or DB file to inspect.",
        )

        list_accounts_parser = subparsers.add_parser(
            "list-accounts",
            help="List Plex accounts in a Plex library database.",
        )
        list_accounts_parser.set_defaults(command="list-accounts")
        list_accounts_parser.add_argument(
            "--path",
            required=True,
            help="Path to the Plex location or DB file to inspect.",
        )

        return parser

    @classmethod
    def parse_args(cls, argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
        raw_argv = list(argv) if argv is not None else sys.argv[1:]
        subcommands = {"transfer", "list-libraries", "list-accounts"}
        if raw_argv and not raw_argv[0].startswith("-") and raw_argv[0] not in subcommands:
            raw_argv = ["transfer", *raw_argv]
        if raw_argv and raw_argv[0].startswith("-") and raw_argv[0] not in {"-h", "--help"}:
            raw_argv = ["transfer", *raw_argv]
        return cls.build_parser().parse_args(raw_argv)

    @classmethod
    def main(cls, argv: Optional[Sequence[str]] = None) -> int:
        app = cls(cls.parse_args(argv))
        return app.run()

    def run(self) -> int:
        if self.args.command == "list-libraries":
            return self.run_list_libraries()
        if self.args.command == "list-accounts":
            return self.run_list_accounts()
        return self.run_transfer()

    def run_transfer(self) -> int:
        dry_run_filter_mode = self.args.dry_run_status_filter
        dry_run_filters_active = dry_run_filter_mode != "all"

        if self.args.apply and dry_run_filters_active:
            raise RuntimeError("Dry-run filters cannot be used with --apply.")

        source_db_path = PlexDatabaseLocator.resolve_db_path(self.args.source_path, "source")
        target_db_path = PlexDatabaseLocator.resolve_db_path(self.args.target_path, "target")

        if self.args.apply:
            PlexEnvironment.wait_for_plex_shutdown()

        source_database = PlexDatabase(source_db_path, readonly=True)
        target_database = PlexDatabase(target_db_path, readonly=not self.args.apply)

        try:
            source_schema = source_database.inspect_schema()
            target_schema = target_database.inspect_schema()

            source_inventory = source_database.build_media_inventory(source_schema, self.args.source_library)
            target_inventory = target_database.build_media_inventory(target_schema, self.args.target_library)
            target_inventory_all = target_inventory
            if not self.args.apply:
                target_inventory_all = target_database.build_media_inventory(target_schema, [])
            source_history = source_database.build_watch_history(self.args.source_library)
            target_history = target_database.build_watch_history(self.args.target_library)

            matcher = PlexMatcher(self.args.match_mode, self.args.min_confidence)
            matches = matcher.collect_matches(
                source_inventory=source_inventory,
                source_history=source_history,
                target_inventory=target_inventory,
                target_history=target_history,
            )
            self.annotate_library_statuses(matches, matcher, target_inventory_all, bool(self.args.target_library))

            mutation_planner = PlexMutationPlanner(target_schema, self.args.account_id, self.args.conflict_policy)
            mutations = mutation_planner.plan_mutations(matches)
            columns = self.report_writer.parse_columns(self.args.columns)
            filtered_matches = matches
            if dry_run_filters_active:
                filtered_matches = self.filter_dry_run_matches(
                    matches,
                    dry_run_filter_mode,
                )
            filtered_target_guids = {
                match.target.guid
                for match in filtered_matches
                if match.target is not None
            }
            filtered_mutations = [mutation for mutation in mutations if mutation.target_guid in filtered_target_guids]

            if self.args.apply:
                target_database.begin_immediate()
                target_database.apply_mutations(mutations)
                target_database.commit()

            output_matches = matches if self.args.apply else filtered_matches
            output_mutations = mutations if self.args.apply else filtered_mutations

            self.report_writer.emit_console(self.args.console_format, output_matches, output_mutations, columns)
            self.report_writer.emit_report(self.args.report, self.args.report_format, output_matches, output_mutations, columns)
            summary_stream = sys.stderr if self.args.console_format in {"json", "csv"} else sys.stdout
            self.report_writer.print_summary(output_matches, output_mutations, self.args.apply, stream=summary_stream)
            if not self.args.apply and dry_run_filters_active:
                print(f"Displayed rows: {len(output_matches)} of {len(matches)}", file=summary_stream)
            self.report_writer.detach_redirected_stdout()
            return 0
        finally:
            source_database.close()
            target_database.close()

    @staticmethod
    def filter_dry_run_matches(
        matches: Sequence[MatchResult],
        dry_run_filter_mode: str,
    ) -> List[MatchResult]:
        if dry_run_filter_mode == "all":
            return list(matches)

        filtered_matches: List[MatchResult] = []
        for match in matches:
            if not PlexWatchStatusTransferApp.match_in_filter_mode(match, dry_run_filter_mode):
                continue
            filtered_matches.append(match)
        return filtered_matches

    @staticmethod
    def is_warning_match(match: MatchResult) -> bool:
        if match.status == "unmatched":
            return True
        return False

    @staticmethod
    def is_error_match(match: MatchResult) -> bool:
        if PlexWatchStatusTransferApp.is_warning_match(match):
            return False
        if match.dry_run_status in {
            "skipped_conflict",
            "target_ahead",
            "missing_required_account",
            "blocked_required_columns",
        }:
            return True
        if match.library_status in {"needed", "blocked", "not_found"}:
            return True
        return False

    @staticmethod
    def match_in_filter_mode(match: MatchResult, dry_run_filter_mode: str) -> bool:
        if dry_run_filter_mode == "warnings":
            return PlexWatchStatusTransferApp.is_warning_match(match)
        if dry_run_filter_mode == "errors":
            return PlexWatchStatusTransferApp.is_error_match(match)
        return True

    @staticmethod
    def annotate_library_statuses(
        matches: Sequence[MatchResult],
        matcher: PlexMatcher,
        target_inventory_all: Sequence[MediaRecord],
        has_target_library_filter: bool,
    ) -> None:
        if not has_target_library_filter:
            for match in matches:
                match.library_status = "not_requested"
            return

        full_indexes = matcher.index_target_inventory(target_inventory_all)
        for match in matches:
            full_candidates = list(full_indexes.get(match.source.basename_key, []))
            full_match = matcher.select_best_candidate(match.source, full_candidates) if full_candidates else None

            if match.status == "matched" and match.target is not None:
                if full_match is None or full_match.target.guid == match.target.guid:
                    match.library_status = "matched"
                else:
                    match.library_status = "needed"
                    match.notes.append(
                        "target library filter influenced the selected match"
                    )
                continue

            if not full_candidates:
                match.library_status = "not_found"
                continue

            if full_match is not None:
                match.library_status = "blocked"
                match.notes.append(
                    "matching target exists outside the selected target library"
                )
                if full_match.target.library_section_name:
                    match.notes.append(
                        f"matching target library: {full_match.target.library_section_name}"
                    )
                continue

            match.library_status = "needed"
            match.notes.append("basename candidates exist, but target library scoping or ambiguity prevented a match")

    def run_list_libraries(self) -> int:
        database = PlexDatabase(PlexDatabaseLocator.resolve_db_path(self.args.path, "path"), readonly=True)
        try:
            rows = [
                {
                    "id": item.id,
                    "name": item.name,
                    "section_type": item.section_type,
                    "agent": item.agent,
                    "scanner": item.scanner,
                    "language": item.language,
                    "public": item.public,
                }
                for item in database.list_library_sections()
            ]
            self.report_writer.write_table_rows(
                sys.stdout,
                rows,
                [
                    TableColumnSpec("id"),
                    TableColumnSpec("name"),
                    TableColumnSpec("section_type"),
                    TableColumnSpec("agent"),
                    TableColumnSpec("scanner"),
                    TableColumnSpec("language"),
                    TableColumnSpec("public"),
                ],
            )
            return 0
        finally:
            database.close()

    def run_list_accounts(self) -> int:
        database = PlexDatabase(PlexDatabaseLocator.resolve_db_path(self.args.path, "path"), readonly=True)
        try:
            rows = [
                {
                    "id": item.id,
                    "name": item.name,
                    "default_audio_language": item.default_audio_language,
                    "default_subtitle_language": item.default_subtitle_language,
                    "auto_select_audio": item.auto_select_audio,
                    "auto_select_subtitle": item.auto_select_subtitle,
                }
                for item in database.list_accounts()
            ]
            self.report_writer.write_table_rows(
                sys.stdout,
                rows,
                [
                    TableColumnSpec("id"),
                    TableColumnSpec("name"),
                    TableColumnSpec("default_audio_language"),
                    TableColumnSpec("default_subtitle_language"),
                    TableColumnSpec("auto_select_audio"),
                    TableColumnSpec("auto_select_subtitle"),
                ],
            )
            return 0
        finally:
            database.close()


if __name__ == "__main__":
    raise SystemExit(PlexWatchStatusTransferApp.main())
