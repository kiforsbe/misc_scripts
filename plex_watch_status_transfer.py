import argparse
import csv
import errno
import hashlib
import importlib
import json
import os
import sqlite3
import subprocess
import sys
import time
import uuid
from collections import defaultdict
from contextlib import contextmanager
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
    metadata_item_settings_columns: Dict[str, TableColumn]
    custom_channels_columns: Dict[str, TableColumn]
    play_queues_columns: Dict[str, TableColumn]
    play_queue_items_columns: Dict[str, TableColumn]
    play_queue_generators_columns: Dict[str, TableColumn]

    @property
    def size_column(self) -> Optional[str]:
        for candidate in ("size", "file_size", "total_size"):
            if candidate in self.media_parts_columns:
                return candidate
        return None

    @property
    def duration_column(self) -> Optional[str]:
        return "duration" if "duration" in self.media_parts_columns else None

    @property
    def supports_playlists(self) -> bool:
        return bool(
            self.play_queues_columns
            and (
                self.play_queue_generators_columns
                or (self.custom_channels_columns and self.play_queue_items_columns)
            )
        )


@dataclass
class ParsedIdentity:
    title_key: Optional[str]
    season: Optional[int]
    episode: Optional[int]


@dataclass
class MediaRecord:
    metadata_item_id: int
    guid: str
    metadata_type: Optional[int]
    title: Optional[str]
    year: Optional[int]
    item_index: Optional[int]
    originally_available_at: Optional[int]
    file_path: str
    library_section_id: Optional[int]
    library_section_name: Optional[str]
    basename: str
    basename_key: str
    file_size: Optional[int]
    duration: Optional[int]
    parent_title: Optional[str]
    parent_index: Optional[int]
    parent_guid: Optional[str]
    grandparent_title: Optional[str]
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


@dataclass(frozen=True)
class PlexPlaylistItem:
    play_queue_item_id: Optional[int]
    metadata_item_id: Optional[int]
    order_value: float
    media: Optional[MediaRecord]
    title: Optional[str]
    library_section_name: Optional[str]
    in_scope: bool
    notes: List[str]

    @property
    def display_label(self) -> str:
        if self.media and self.media.basename:
            return self.media.basename
        if self.title:
            return self.title
        if self.metadata_item_id is not None:
            return f"metadata:{self.metadata_item_id}"
        return "unknown item"


@dataclass(frozen=True)
class PlexPlaylist:
    id: int
    name: str
    description: Optional[str]
    play_queue_id: Optional[int]
    account_id: Optional[int]
    items: List[PlexPlaylistItem]
    storage_model: str = "custom"

    @property
    def scoped_items(self) -> List[PlexPlaylistItem]:
        return [item for item in self.items if item.in_scope]

    @property
    def is_empty_in_scope(self) -> bool:
        return not self.scoped_items


@dataclass(frozen=True)
class PlaylistMatchResult:
    source_item: PlexPlaylistItem
    status: str
    target: Optional[MediaRecord]
    confidence: float
    reason: str
    notes: List[str]


@dataclass
class PlaylistTransferPlan:
    source_playlist: PlexPlaylist
    target_playlist_name: Optional[str]
    action: str
    status: str
    source_item_count: int
    matched_items: List[PlaylistMatchResult]
    transfer_items: List[MediaRecord]
    unmatched_items: List[PlaylistMatchResult]
    existing_target_playlist: Optional[PlexPlaylist]
    notes: List[str]

    @property
    def matched_item_count(self) -> int:
        return len(self.matched_items)

    @property
    def transfer_item_count(self) -> int:
        return len(self.transfer_items)

    @property
    def unmatched_item_count(self) -> int:
        return len(self.unmatched_items)


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

    @staticmethod
    def sqlite_icu_root_collation(left: Optional[str], right: Optional[str]) -> int:
        if left is None:
            return 0 if right is None else -1
        if right is None:
            return 1

        left_key = str(left).casefold()
        right_key = str(right).casefold()
        if left_key < right_key:
            return -1
        if left_key > right_key:
            return 1
        return 0

    def _connect(self) -> sqlite3.Connection:
        if self.readonly:
            uri = f"file:{self.db_path.as_posix()}?mode=ro"
            connection = sqlite3.connect(uri, uri=True)
        else:
            connection = sqlite3.connect(str(self.db_path))
        connection.row_factory = sqlite3.Row
        connection.create_collation("icu_root", self.sqlite_icu_root_collation)
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
            metadata_item_settings_columns=self.load_optional_table_columns("metadata_item_settings"),
            custom_channels_columns=self.load_optional_table_columns("custom_channels"),
            play_queues_columns=self.load_optional_table_columns("play_queues"),
            play_queue_items_columns=self.load_optional_table_columns("play_queue_items"),
            play_queue_generators_columns=self.load_optional_table_columns("play_queue_generators"),
        )

    def load_optional_table_columns(self, table_name: str) -> Dict[str, TableColumn]:
        rows = self.connection.execute(f"PRAGMA table_info({table_name})").fetchall()
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
            md.id AS metadata_item_id,
            md.guid AS guid,
            md.metadata_type AS metadata_type,
            md.title AS title,
            md.year AS year,
            md."index" AS item_index,
            md.originally_available_at AS originally_available_at,
            mp.file AS file_path,
            ls.id AS library_section_id,
            ls.name AS library_section_name,
            parent.title AS parent_title,
            parent."index" AS parent_index,
            parent.guid AS parent_guid,
            grandparent.title AS grandparent_title,
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
                    metadata_item_id=int(row["metadata_item_id"]),
                    guid=row["guid"],
                    metadata_type=PlexFilenameParser.safe_int(row["metadata_type"]),
                    title=row["title"],
                    year=PlexFilenameParser.safe_int(row["year"]),
                    item_index=PlexFilenameParser.safe_int(row["item_index"]),
                    originally_available_at=PlexFilenameParser.safe_int(row["originally_available_at"]),
                    file_path=file_path,
                    library_section_id=PlexFilenameParser.safe_int(row["library_section_id"]),
                    library_section_name=row["library_section_name"],
                    basename=basename,
                    basename_key=PlexFilenameParser.normalize_basename(file_path),
                    file_size=PlexFilenameParser.safe_int(row["file_size"]),
                    duration=PlexFilenameParser.safe_int(row["duration"]),
                    parent_title=row["parent_title"],
                    parent_index=PlexFilenameParser.safe_int(row["parent_index"]),
                    parent_guid=row["parent_guid"],
                    grandparent_title=row["grandparent_title"],
                    grandparent_guid=row["grandparent_guid"],
                    parsed_identity=PlexFilenameParser.parse_identity(basename),
                )
            )
        return inventory

    def build_watch_history(
        self,
        library_filters: Sequence[str],
        account_id: Optional[int] = None,
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
        if account_id is not None:
            query += " AND miv.account_id = ?"
            params.append(account_id)
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

    def build_inventory_by_metadata_id(self, inventory: Sequence[MediaRecord]) -> Dict[int, MediaRecord]:
        return {
            item.metadata_item_id: item
            for item in inventory
        }

    def list_playlists(
        self,
        schema: PlexSchema,
        scoped_inventory: Sequence[MediaRecord],
        inventory_all: Optional[Sequence[MediaRecord]] = None,
    ) -> List[PlexPlaylist]:
        if not schema.supports_playlists:
            raise RuntimeError(
                "This Plex DB schema does not expose the playlist tables required by this tool."
            )

        scoped_by_id = self.build_inventory_by_metadata_id(scoped_inventory)
        all_by_id = scoped_by_id if inventory_all is None else self.build_inventory_by_metadata_id(inventory_all)

        def collect_playlists(query: str, storage_model: str) -> List[PlexPlaylist]:
            playlists: List[PlexPlaylist] = []
            current_playlist_id: Optional[int] = None
            current_items: List[PlexPlaylistItem] = []
            current_header: Optional[Dict[str, Any]] = None

            def flush_current() -> None:
                if current_header is None:
                    return
                playlists.append(
                    PlexPlaylist(
                        id=int(current_header["playlist_id"]),
                        name=str(current_header["playlist_name"] or ""),
                        description=current_header["playlist_description"],
                        play_queue_id=PlexFilenameParser.safe_int(current_header["play_queue_id"]),
                        account_id=PlexFilenameParser.safe_int(current_header["account_id"]),
                        items=list(current_items),
                        storage_model=storage_model,
                    )
                )

            for row in self.connection.execute(query):
                playlist_id = int(row["playlist_id"])
                if current_playlist_id != playlist_id:
                    flush_current()
                    current_playlist_id = playlist_id
                    current_header = dict(row)
                    current_items = []

                metadata_item_id = PlexFilenameParser.safe_int(row["metadata_item_id"])
                if metadata_item_id is None:
                    continue

                scoped_media = scoped_by_id.get(metadata_item_id)
                media = scoped_media or all_by_id.get(metadata_item_id)
                notes: List[str] = []
                in_scope = scoped_media is not None
                if scoped_media is None and media is not None:
                    notes.append("outside selected source library scope")
                if media is None:
                    notes.append("no file-backed media row available for this playlist item")

                current_items.append(
                    PlexPlaylistItem(
                        play_queue_item_id=PlexFilenameParser.safe_int(row["play_queue_item_id"]),
                        metadata_item_id=metadata_item_id,
                        order_value=float(row["order_value"] or 0.0),
                        media=media,
                        title=row["item_title"],
                        library_section_name=row["item_library_name"],
                        in_scope=in_scope,
                        notes=notes,
                    )
                )

            flush_current()
            return playlists

        playlists_by_name: Dict[str, PlexPlaylist] = {}

        if schema.custom_channels_columns and schema.play_queue_items_columns:
            custom_query = """
            SELECT
                cc.id AS playlist_id,
                cc.name AS playlist_name,
                cc.description AS playlist_description,
                pq.id AS play_queue_id,
                pq.account_id AS account_id,
                pqi.id AS play_queue_item_id,
                pqi.metadata_item_id AS metadata_item_id,
                pqi."order" AS order_value,
                md.title AS item_title,
                ls.name AS item_library_name
            FROM custom_channels cc
            LEFT JOIN (
                SELECT playlist_id, MAX(id) AS play_queue_id
                FROM play_queues
                WHERE playlist_id IS NOT NULL
                GROUP BY playlist_id
            ) latest_queue ON latest_queue.playlist_id = cc.id
            LEFT JOIN play_queues pq ON pq.id = latest_queue.play_queue_id
            LEFT JOIN play_queue_items pqi ON pqi.play_queue_id = pq.id
            LEFT JOIN metadata_items md ON md.id = pqi.metadata_item_id
            LEFT JOIN library_sections ls ON ls.id = md.library_section_id
            ORDER BY cc.name COLLATE NOCASE, cc.id, pqi."order", pqi.id
            """
            for playlist in collect_playlists(custom_query, "custom"):
                playlists_by_name[playlist.name.casefold()] = playlist

        if schema.play_queue_generators_columns:
            metadata_query = """
            SELECT
                playlist.id AS playlist_id,
                playlist.title AS playlist_name,
                NULL AS playlist_description,
                latest_queue.play_queue_id AS play_queue_id,
                latest_queue.account_id AS account_id,
                generator.id AS play_queue_item_id,
                generator.metadata_item_id AS metadata_item_id,
                generator."order" AS order_value,
                md.title AS item_title,
                ls.name AS item_library_name
            FROM metadata_items playlist
            LEFT JOIN (
                SELECT pq.playlist_id, pq.id AS play_queue_id, pq.account_id
                FROM play_queues pq
                INNER JOIN (
                    SELECT playlist_id, MAX(id) AS play_queue_id
                    FROM play_queues
                    WHERE playlist_id IS NOT NULL
                    GROUP BY playlist_id
                ) latest ON latest.play_queue_id = pq.id
            ) latest_queue ON latest_queue.playlist_id = playlist.id
            LEFT JOIN play_queue_generators generator ON generator.playlist_id = playlist.id
            LEFT JOIN metadata_items md ON md.id = generator.metadata_item_id
            LEFT JOIN library_sections ls ON ls.id = md.library_section_id
            WHERE playlist.metadata_type = 15
            ORDER BY playlist.title COLLATE NOCASE, playlist.id, generator."order", generator.id
            """
            for playlist in collect_playlists(metadata_query, "metadata"):
                playlists_by_name[playlist.name.casefold()] = playlist

        return sorted(playlists_by_name.values(), key=lambda playlist: (playlist.name.casefold(), playlist.id))

    def find_existing_playlist_by_name(
        self,
        playlists: Sequence[PlexPlaylist],
        name: str,
    ) -> Optional[PlexPlaylist]:
        target_key = name.casefold()
        for playlist in playlists:
            if playlist.name.casefold() == target_key:
                return playlist
        return None

    def infer_preferred_account_id(self) -> Optional[int]:
        discovered_ids = {
            int(row[0])
            for row in self.connection.execute(
                """
                SELECT DISTINCT account_id FROM metadata_item_views WHERE account_id IS NOT NULL
                UNION
                SELECT DISTINCT account_id FROM metadata_item_settings WHERE account_id IS NOT NULL
                UNION
                SELECT DISTINCT account_id FROM metadata_item_accounts WHERE account_id IS NOT NULL
                """
            )
            if row[0] is not None
        }
        if len(discovered_ids) == 1:
            return next(iter(discovered_ids))

        named_accounts = [
            int(row["id"])
            for row in self.connection.execute(
                "SELECT id FROM accounts WHERE COALESCE(name, '') != '' ORDER BY id"
            )
        ]
        if len(named_accounts) == 1:
            return named_accounts[0]
        return None

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
            elif mutation.action == "replace_views":
                row_ids = [int(row_id) for row_id in mutation.details["row_ids"]]
                if row_ids:
                    placeholders = ", ".join("?" for _ in row_ids)
                    self.connection.execute(
                        f"DELETE FROM metadata_item_views WHERE rowid IN ({placeholders})",
                        row_ids,
                    )

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
            elif mutation.action == "upsert_settings":
                details = mutation.details
                account_id = details["account_id"]
                existing_row = self.connection.execute(
                    """
                    SELECT id
                    FROM metadata_item_settings
                    WHERE guid = ?
                      AND ((account_id = ?) OR (account_id IS NULL AND ? IS NULL))
                    ORDER BY id
                    LIMIT 1
                    """,
                    (details["guid"], account_id, account_id),
                ).fetchone()

                if existing_row:
                    self.connection.execute(
                        """
                        UPDATE metadata_item_settings
                        SET view_count = ?,
                            last_viewed_at = ?,
                            updated_at = ?,
                            changed_at = ?
                        WHERE id = ?
                        """,
                        (
                            details["view_count"],
                            details["last_viewed_at"],
                            details["updated_at"],
                            details["changed_at"],
                            existing_row["id"],
                        ),
                    )
                else:
                    self.connection.execute(
                        """
                        INSERT INTO metadata_item_settings (
                            account_id,
                            guid,
                            view_count,
                            last_viewed_at,
                            created_at,
                            updated_at,
                            changed_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            account_id,
                            details["guid"],
                            details["view_count"],
                            details["last_viewed_at"],
                            details["created_at"],
                            details["updated_at"],
                            details["changed_at"],
                        ),
                    )
            elif mutation.action == "refresh_view_rows":
                details = mutation.details
                self.connection.execute(
                    """
                    UPDATE metadata_item_views
                    SET account_id = ?,
                        metadata_type = ?,
                        library_section_id = ?,
                        grandparent_title = ?,
                        parent_index = ?,
                        parent_title = ?,
                        "index" = ?,
                        title = ?,
                        viewed_at = COALESCE(viewed_at, ?),
                        grandparent_guid = ?,
                        originally_available_at = ?,
                        view_type = ?
                    WHERE guid = ?
                      AND (? IS NULL OR account_id IS NULL OR account_id = ?)
                    """,
                    (
                        details["account_id"],
                        details["metadata_type"],
                        details["library_section_id"],
                        details["grandparent_title"],
                        details["parent_index"],
                        details["parent_title"],
                        details["index"],
                        details["title"],
                        details["viewed_at"],
                        details["grandparent_guid"],
                        details["originally_available_at"],
                        details["view_type"],
                        details["guid"],
                        details["account_id"],
                        details["account_id"],
                    ),
                )
            elif mutation.action == "create_playlist":
                details = mutation.details
                if details.get("storage_model") == "metadata" or self.prefers_metadata_playlist_storage():
                    self.create_metadata_playlist(details)
                else:
                    now = int(datetime.now().timestamp())
                    cursor = self.connection.execute(
                        """
                        INSERT INTO custom_channels (
                            name,
                            description,
                            ordering,
                            visibility,
                            displayed_on,
                            content_rating
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            details["name"],
                            details.get("description"),
                            details.get("ordering", 0),
                            details.get("visibility"),
                            details.get("displayed_on"),
                            details.get("content_rating"),
                        ),
                    )
                    playlist_id = int(cursor.lastrowid)
                    play_queue_cursor = self.connection.execute(
                        """
                        INSERT INTO play_queues (
                            client_identifier,
                            account_id,
                            playlist_id,
                            play_queue_generator_id,
                            version,
                            created_at,
                            updated_at,
                            metadata_type,
                            total_items_count,
                            extra_data
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            details.get("client_identifier") or uuid.uuid4().hex,
                            details.get("account_id"),
                            playlist_id,
                            None,
                            1,
                            now,
                            now,
                            details.get("metadata_type"),
                            len(details.get("metadata_item_ids", [])),
                            details.get("extra_data"),
                        ),
                    )
                    self.insert_playlist_items(
                        int(play_queue_cursor.lastrowid),
                        details.get("metadata_item_ids", []),
                    )
            elif mutation.action == "merge_playlist_items":
                details = mutation.details
                if details.get("storage_model") == "metadata":
                    self.insert_playlist_generators(
                        int(details["playlist_id"]),
                        details.get("metadata_item_ids", []),
                        self.next_playlist_generator_order(int(details["playlist_id"])),
                    )
                    self.refresh_metadata_playlist_totals(int(details["playlist_id"]))
                else:
                    self.insert_playlist_items(
                        int(details["play_queue_id"]),
                        details.get("metadata_item_ids", []),
                        self.next_playlist_order(int(details["play_queue_id"])),
                    )
                    self.refresh_playlist_queue_totals(int(details["play_queue_id"]))
            elif mutation.action == "replace_playlist_items":
                details = mutation.details
                if details.get("storage_model") == "metadata":
                    playlist_id = int(details["playlist_id"])
                    self.connection.execute(
                        "DELETE FROM play_queue_generators WHERE playlist_id = ?",
                        (playlist_id,),
                    )
                    self.insert_playlist_generators(
                        playlist_id,
                        details.get("metadata_item_ids", []),
                    )
                    self.refresh_metadata_playlist_totals(playlist_id)
                else:
                    play_queue_id = int(details["play_queue_id"])
                    self.connection.execute(
                        "DELETE FROM play_queue_items WHERE play_queue_id = ?",
                        (play_queue_id,),
                    )
                    self.insert_playlist_items(
                        play_queue_id,
                        details.get("metadata_item_ids", []),
                    )
                    self.refresh_playlist_queue_totals(play_queue_id)
            else:
                raise RuntimeError(f"Unsupported mutation action: {mutation.action}")

    def prefers_metadata_playlist_storage(self) -> bool:
        row = self.connection.execute(
            """
            SELECT 1
            FROM metadata_items playlist
            JOIN play_queue_generators generator ON generator.playlist_id = playlist.id
            WHERE playlist.metadata_type = 15
            LIMIT 1
            """
        ).fetchone()
        return row is not None

    @contextmanager
    def temporarily_disable_metadata_fts_triggers(self):
        trigger_rows = self.connection.execute(
            """
            SELECT name, sql
            FROM sqlite_master
            WHERE type = 'trigger'
              AND tbl_name = 'metadata_items'
              AND name LIKE 'fts4_metadata_titles_%'
            ORDER BY name
            """
        ).fetchall()
        try:
            for row in trigger_rows:
                self.connection.execute(f'DROP TRIGGER IF EXISTS {self.quote_identifier(row["name"])}')
            yield
        finally:
            for row in trigger_rows:
                if row["sql"]:
                    self.connection.execute(str(row["sql"]))

    def build_playlist_extra_data(self, metadata_item_ids: Sequence[int], owner_id: Optional[int] = None) -> str:
        placeholders = ", ".join("?" for _ in metadata_item_ids)
        section_ids: List[str] = []
        if metadata_item_ids:
            query = f"""
            SELECT DISTINCT library_section_id
            FROM metadata_items
            WHERE id IN ({placeholders})
              AND library_section_id IS NOT NULL
            ORDER BY library_section_id
            """
            section_ids = [
                str(int(row["library_section_id"]))
                for row in self.connection.execute(query, [int(item_id) for item_id in metadata_item_ids])
            ]
        section_ids_value = ",".join(section_ids)
        owner_value = str(owner_id if owner_id is not None else 1)
        return json.dumps(
            {
                "pv:durationInSeconds": "1",
                "pv:owner": owner_value,
                "pv:sectionIDs": section_ids_value,
                "url": (
                    f"pv%3AdurationInSeconds=1&pv%3Aowner={owner_value}&pv%3AsectionIDs="
                    + section_ids_value
                ),
            },
            separators=(",", ":"),
        )

    def ensure_metadata_item_account(self, metadata_item_id: int, account_id: Optional[int]) -> None:
        if account_id is None:
            return
        existing = self.connection.execute(
            "SELECT 1 FROM metadata_item_accounts WHERE metadata_item_id = ? AND account_id = ? LIMIT 1",
            (metadata_item_id, account_id),
        ).fetchone()
        if existing is not None:
            return
        self.connection.execute(
            "INSERT INTO metadata_item_accounts (account_id, metadata_item_id) VALUES (?, ?)",
            (account_id, metadata_item_id),
        )

    def create_metadata_playlist(self, details: Dict[str, Any]) -> None:
        metadata_item_ids = [int(item_id) for item_id in details.get("metadata_item_ids", [])]
        account_id = PlexFilenameParser.safe_int(details.get("account_id"))
        now = int(datetime.now().timestamp())
        title = str(details["name"])
        total_duration_seconds = 0
        if metadata_item_ids:
            placeholders = ", ".join("?" for _ in metadata_item_ids)
            duration_row = self.connection.execute(
                f"SELECT COALESCE(SUM(duration), 0) AS total_duration FROM metadata_items WHERE id IN ({placeholders})",
                metadata_item_ids,
            ).fetchone()
            total_duration_seconds = int((duration_row["total_duration"] or 0) / 1000)
        extra_data = details.get("extra_data") or self.build_playlist_extra_data(metadata_item_ids, owner_id=account_id)
        guid = details.get("guid") or f"com.plexapp.agents.none://{uuid.uuid4()}"
        hash_value = hashlib.sha1(f"{guid}|{title}|{now}".encode("utf-8")).hexdigest()
        with self.temporarily_disable_metadata_fts_triggers():
            cursor = self.connection.execute(
                """
                INSERT INTO metadata_items (
                    metadata_type,
                    guid,
                    media_item_count,
                    title,
                    title_sort,
                    original_title,
                    studio,
                    tagline,
                    summary,
                    content_rating,
                    "index",
                    absolute_index,
                    duration,
                    user_thumb_url,
                    user_art_url,
                    user_banner_url,
                    user_music_url,
                    user_fields,
                    tags_genre,
                    tags_collection,
                    tags_director,
                    tags_writer,
                    tags_star,
                    added_at,
                    created_at,
                    updated_at,
                    tags_country,
                    extra_data,
                    hash,
                    changed_at,
                    resources_changed_at,
                    edition_title,
                    slug,
                    is_adult,
                    user_clear_logo_url,
                    user_square_art_url
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    15,
                    guid,
                    len(metadata_item_ids),
                    title,
                    title,
                    "",
                    "",
                    "",
                    details.get("description") or "",
                    "",
                    0,
                    1,
                    total_duration_seconds,
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    now,
                    now,
                    now,
                    "",
                    extra_data,
                    hash_value,
                    now,
                    now,
                    "",
                    "",
                    0,
                    "",
                    "",
                ),
            )
        playlist_id = int(cursor.lastrowid)
        self.ensure_metadata_item_account(playlist_id, account_id)
        self.insert_playlist_generators(playlist_id, metadata_item_ids)

    def next_playlist_generator_order(self, playlist_id: int) -> float:
        row = self.connection.execute(
            'SELECT MAX("order") AS max_order FROM play_queue_generators WHERE playlist_id = ?',
            (playlist_id,),
        ).fetchone()
        current = float(row["max_order"] or 0.0)
        if current < 1000.0:
            return 1000.0
        return current + 1000.0

    def insert_playlist_generators(
        self,
        playlist_id: int,
        metadata_item_ids: Sequence[int],
        starting_order: float = 1000.0,
    ) -> None:
        now = int(datetime.now().timestamp())
        order_value = starting_order
        for metadata_item_id in metadata_item_ids:
            self.connection.execute(
                'INSERT INTO play_queue_generators (playlist_id, metadata_item_id, uri, "limit", continuous, "order", created_at, updated_at, changed_at, recursive, type, extra_data) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (playlist_id, int(metadata_item_id), "", None, 0, order_value, now, now, now, 0, None, None),
            )
            order_value += 1000.0

    def refresh_metadata_playlist_totals(self, playlist_id: int) -> None:
        now = int(datetime.now().timestamp())
        count_row = self.connection.execute(
            'SELECT COUNT(*) AS item_count FROM play_queue_generators WHERE playlist_id = ?',
            (playlist_id,),
        ).fetchone()
        item_count = int(count_row["item_count"] or 0)
        duration_row = self.connection.execute(
            """
            SELECT COALESCE(SUM(md.duration), 0) AS total_duration
            FROM play_queue_generators generator
            JOIN metadata_items md ON md.id = generator.metadata_item_id
            WHERE generator.playlist_id = ?
            """,
            (playlist_id,),
        ).fetchone()
        total_duration_seconds = int((duration_row["total_duration"] or 0) / 1000)
        with self.temporarily_disable_metadata_fts_triggers():
            self.connection.execute(
                """
                UPDATE metadata_items
                SET media_item_count = ?,
                    duration = ?,
                    updated_at = ?,
                    changed_at = ?,
                    resources_changed_at = ?
                WHERE id = ?
                """,
                (item_count, total_duration_seconds, now, now, now, playlist_id),
            )

    def next_playlist_order(self, play_queue_id: int) -> float:
        row = self.connection.execute(
            'SELECT MAX("order") AS max_order FROM play_queue_items WHERE play_queue_id = ?',
            (play_queue_id,),
        ).fetchone()
        current = float(row["max_order"] or 0.0)
        if current < 1000.0:
            return 1000.0
        return current + 1000.0

    def insert_playlist_items(
        self,
        play_queue_id: int,
        metadata_item_ids: Sequence[int],
        starting_order: float = 1000.0,
    ) -> None:
        order_value = starting_order
        for metadata_item_id in metadata_item_ids:
            self.connection.execute(
                'INSERT INTO play_queue_items (play_queue_id, metadata_item_id, "order", up_next, play_queue_generator_id) VALUES (?, ?, ?, ?, ?)',
                (play_queue_id, int(metadata_item_id), order_value, None, None),
            )
            order_value += 1000.0

    def refresh_playlist_queue_totals(self, play_queue_id: int) -> None:
        now = int(datetime.now().timestamp())
        count_row = self.connection.execute(
            'SELECT COUNT(*) AS item_count FROM play_queue_items WHERE play_queue_id = ?',
            (play_queue_id,),
        ).fetchone()
        item_count = int(count_row["item_count"] or 0)
        self.connection.execute(
            """
            UPDATE play_queues
            SET total_items_count = ?,
                updated_at = ?,
                version = COALESCE(version, 0) + 1,
                last_added_play_queue_item_id = (
                    SELECT id
                    FROM play_queue_items
                    WHERE play_queue_id = ?
                    ORDER BY "order" DESC, id DESC
                    LIMIT 1
                )
            WHERE id = ?
            """,
            (item_count, now, play_queue_id, play_queue_id),
        )

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

    def supports_item_settings(self) -> bool:
        return bool(self.schema.metadata_item_settings_columns)

    def build_item_settings_values(
        self,
        guid: str,
        view_count: int,
        last_viewed_at: Optional[int],
    ) -> Optional[Dict[str, Any]]:
        if not self.supports_item_settings():
            return None

        timestamp = int(datetime.now().timestamp())
        return {
            "account_id": self.account_id,
            "guid": guid,
            "view_count": view_count,
            "last_viewed_at": last_viewed_at,
            "created_at": timestamp,
            "updated_at": timestamp,
            "changed_at": timestamp,
        }

    def build_view_refresh_values(
        self,
        target: MediaRecord,
        viewed_at: Optional[int],
    ) -> Dict[str, Any]:
        return {
            "account_id": self.account_id,
            "guid": target.guid,
            "metadata_type": target.metadata_type,
            "library_section_id": target.library_section_id,
            "grandparent_title": target.grandparent_title,
            "parent_index": target.parent_index,
            "parent_title": target.parent_title,
            "index": target.item_index,
            "title": target.title,
            "viewed_at": viewed_at,
            "grandparent_guid": target.grandparent_guid,
            "originally_available_at": target.originally_available_at,
            "view_type": 0,
        }

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
            resulting_watch_count = target_history.watch_count
            resulting_last_viewed_at = target_history.last_viewed_at
            source_last_viewed_at = source_history.last_viewed_at or 0
            target_last_viewed_at = target_history.last_viewed_at or 0
            target_count_ahead = target_history.watch_count > source_history.watch_count
            target_timestamp_ahead = (
                target_history.watch_count == source_history.watch_count
                and target_last_viewed_at > source_last_viewed_at
            )
            needs_target_change = False
            match.account_status = "not_needed"
            match.dry_run_status = "in_sync"

            if target_count_ahead or target_timestamp_ahead:
                match.dry_run_status = "target_ahead"

            if self.conflict_policy == "skip" and target_history.watch_count > 0:
                if delta > 0 or (
                    source_history.last_viewed_at
                    and source_history.watch_count == target_history.watch_count
                    and source_last_viewed_at != target_last_viewed_at
                ):
                    match.dry_run_status = "skipped_conflict"
                continue

            if self.conflict_policy == "overwrite" and target_count_ahead:
                viewed_at = source_history.last_viewed_at or int(datetime.now().timestamp())
                insert_values, account_status, error_message = self.resolve_insert_defaults(match.target, viewed_at)
                match.account_status = account_status
                if error_message:
                    match.dry_run_status = "missing_required_account" if account_status == "missing_required" else "blocked_required_columns"
                    match.notes.append(error_message)
                    continue
                mutations.append(
                    PlannedMutation(
                        action="replace_views",
                        target_guid=match.target.guid,
                        details={
                            "row_ids": list(target_history.row_ids),
                            "count": source_history.watch_count,
                            "values": insert_values,
                            "source_watch_count": source_history.watch_count,
                            "target_watch_count": target_history.watch_count,
                        },
                    )
                )
                resulting_watch_count = source_history.watch_count
                resulting_last_viewed_at = viewed_at
                match.dry_run_status = "ready_overwrite"
                needs_target_change = True
            elif self.conflict_policy == "overwrite" and target_timestamp_ahead and target_history.row_ids:
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
                resulting_watch_count = target_history.watch_count
                resulting_last_viewed_at = source_history.last_viewed_at
                match.dry_run_status = "ready_overwrite"
                needs_target_change = True

            elif delta > 0:
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
                resulting_watch_count = source_history.watch_count
                resulting_last_viewed_at = viewed_at
                match.dry_run_status = "ready_insert"
                needs_target_change = True
            elif (
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
                resulting_watch_count = target_history.watch_count
                resulting_last_viewed_at = source_history.last_viewed_at
                match.dry_run_status = "ready_update"
                needs_target_change = True

            if not needs_target_change:
                continue

            settings_values = self.build_item_settings_values(
                match.target.guid,
                resulting_watch_count,
                resulting_last_viewed_at,
            )
            if settings_values is not None:
                mutations.append(
                    PlannedMutation(
                        action="upsert_settings",
                        target_guid=match.target.guid,
                        details=settings_values,
                    )
                )
            mutations.append(
                PlannedMutation(
                    action="refresh_view_rows",
                    target_guid=match.target.guid,
                    details=self.build_view_refresh_values(match.target, resulting_last_viewed_at),
                )
            )
        return mutations


class PlexPlaylistPlanner:
    def __init__(
        self,
        matcher: PlexMatcher,
        conflict_policy: str,
        include_empty_playlists: bool,
    ) -> None:
        self.matcher = matcher
        self.conflict_policy = conflict_policy
        self.include_empty_playlists = include_empty_playlists

    @staticmethod
    def resolve_unique_name(existing_names: Sequence[str], desired_name: str) -> str:
        existing_keys = {name.casefold() for name in existing_names}
        base_name = desired_name
        if base_name.casefold() not in existing_keys:
            return base_name

        first_candidate = f"{base_name} (Imported)"
        if first_candidate.casefold() not in existing_keys:
            return first_candidate

        suffix = 2
        while True:
            candidate = f"{base_name} (Imported {suffix})"
            if candidate.casefold() not in existing_keys:
                return candidate
            suffix += 1

    def build_item_match(
        self,
        source_item: PlexPlaylistItem,
        target_indexes: Dict[str, List[MediaRecord]],
        full_indexes: Dict[str, List[MediaRecord]],
        has_target_library_filter: bool,
    ) -> PlaylistMatchResult:
        notes = list(source_item.notes)
        if source_item.media is None:
            return PlaylistMatchResult(
                source_item=source_item,
                status="unmatched",
                target=None,
                confidence=0.0,
                reason="source_media_missing",
                notes=notes,
            )

        target, confidence, reason, match_notes = self.matcher.find_match(source_item.media, target_indexes)
        notes.extend(match_notes)
        if target is not None:
            return PlaylistMatchResult(
                source_item=source_item,
                status="matched",
                target=target,
                confidence=confidence,
                reason=reason,
                notes=notes,
            )

        if has_target_library_filter:
            full_candidates = list(full_indexes.get(source_item.media.basename_key, []))
            full_match = self.matcher.select_best_candidate(source_item.media, full_candidates) if full_candidates else None
            if full_match is not None:
                notes.append("matching target exists outside the selected target library")
                if full_match.target.library_section_name:
                    notes.append(f"matching target library: {full_match.target.library_section_name}")

        return PlaylistMatchResult(
            source_item=source_item,
            status="unmatched",
            target=None,
            confidence=confidence,
            reason=reason,
            notes=notes,
        )

    def plan_transfers(
        self,
        source_playlists: Sequence[PlexPlaylist],
        target_playlists: Sequence[PlexPlaylist],
        target_inventory: Sequence[MediaRecord],
        target_inventory_all: Sequence[MediaRecord],
        target_account_id: Optional[int] = None,
        has_target_library_filter: bool = False,
    ) -> Tuple[List[PlaylistTransferPlan], List[PlannedMutation]]:
        target_indexes = self.matcher.index_target_inventory(target_inventory)
        full_indexes = self.matcher.index_target_inventory(target_inventory_all)
        target_by_name = {
            playlist.name.casefold(): playlist
            for playlist in target_playlists
        }
        reserved_names = [playlist.name for playlist in target_playlists]

        plans: List[PlaylistTransferPlan] = []
        mutations: List[PlannedMutation] = []

        for source_playlist in source_playlists:
            scoped_items = source_playlist.scoped_items
            notes: List[str] = []
            if not self.include_empty_playlists and not scoped_items:
                notes.append("empty in the selected source library scope; excluded by default")
                plans.append(
                    PlaylistTransferPlan(
                        source_playlist=source_playlist,
                        target_playlist_name=None,
                        action="skip_empty",
                        status="skipped_empty",
                        source_item_count=0,
                        matched_items=[],
                        transfer_items=[],
                        unmatched_items=[],
                        existing_target_playlist=None,
                        notes=notes,
                    )
                )
                continue

            matched_items: List[PlaylistMatchResult] = []
            unmatched_items: List[PlaylistMatchResult] = []
            for source_item in scoped_items:
                match_result = self.build_item_match(
                    source_item,
                    target_indexes,
                    full_indexes,
                    has_target_library_filter,
                )
                if match_result.status == "matched":
                    matched_items.append(match_result)
                else:
                    unmatched_items.append(match_result)

            existing_target_playlist = target_by_name.get(source_playlist.name.casefold())
            transfer_items = [
                item.target
                for item in matched_items
                if item.target is not None
            ]
            action = "create_new"
            status = "ready_create"
            target_playlist_name = source_playlist.name

            if not transfer_items:
                notes.append("no playlist items matched in the selected target library scope")
                plans.append(
                    PlaylistTransferPlan(
                        source_playlist=source_playlist,
                        target_playlist_name=None,
                        action="skip_unmatched",
                        status="no_transferable_items",
                        source_item_count=len(scoped_items),
                        matched_items=matched_items,
                        transfer_items=[],
                        unmatched_items=unmatched_items,
                        existing_target_playlist=existing_target_playlist,
                        notes=notes,
                    )
                )
                continue

            if existing_target_playlist is None:
                mutations.append(
                    PlannedMutation(
                        action="create_playlist",
                        target_guid=f"playlist:{source_playlist.name}",
                        details={
                            "name": target_playlist_name,
                            "description": source_playlist.description,
                            "account_id": target_account_id,
                            "metadata_type": transfer_items[0].metadata_type if transfer_items else None,
                            "metadata_item_ids": [item.metadata_item_id for item in transfer_items],
                            "storage_model": "metadata",
                        },
                    )
                )
            elif self.conflict_policy == "skip":
                action = "skip_existing"
                status = "skipped_conflict"
                notes.append("target playlist already exists; conflict policy is skip")
                transfer_items = []
            elif self.conflict_policy == "unique":
                action = "create_unique"
                target_playlist_name = self.resolve_unique_name(reserved_names, source_playlist.name)
                reserved_names.append(target_playlist_name)
                mutations.append(
                    PlannedMutation(
                        action="create_playlist",
                        target_guid=f"playlist:{target_playlist_name}",
                        details={
                            "name": target_playlist_name,
                            "description": source_playlist.description,
                            "account_id": target_account_id,
                            "metadata_type": transfer_items[0].metadata_type if transfer_items else None,
                            "metadata_item_ids": [item.metadata_item_id for item in transfer_items],
                            "storage_model": "metadata",
                        },
                    )
                )
            elif self.conflict_policy == "replace":
                action = "replace_existing"
                status = "ready_replace"
                if existing_target_playlist.storage_model == "custom" and existing_target_playlist.play_queue_id is None:
                    status = "blocked_missing_target_queue"
                    notes.append("target playlist exists but has no play queue row")
                    transfer_items = []
                else:
                    mutations.append(
                        PlannedMutation(
                            action="replace_playlist_items",
                            target_guid=f"playlist:{existing_target_playlist.name}",
                            details={
                                "play_queue_id": existing_target_playlist.play_queue_id,
                                "playlist_id": existing_target_playlist.id,
                                "storage_model": existing_target_playlist.storage_model,
                                "metadata_item_ids": [item.metadata_item_id for item in transfer_items],
                            },
                        )
                    )
            else:
                action = "merge_existing"
                status = "ready_merge"
                if existing_target_playlist.storage_model == "custom" and existing_target_playlist.play_queue_id is None:
                    status = "blocked_missing_target_queue"
                    notes.append("target playlist exists but has no play queue row")
                    transfer_items = []
                else:
                    existing_target_ids = {
                        item.media.metadata_item_id
                        for item in existing_target_playlist.items
                        if item.media is not None
                    }
                    unique_transfer_items = [
                        item
                        for item in transfer_items
                        if item.metadata_item_id not in existing_target_ids
                    ]
                    duplicate_count = len(transfer_items) - len(unique_transfer_items)
                    if duplicate_count:
                        notes.append(f"{duplicate_count} matched items already exist in the target playlist")
                    transfer_items = unique_transfer_items
                    if transfer_items:
                        mutations.append(
                            PlannedMutation(
                                action="merge_playlist_items",
                                target_guid=f"playlist:{existing_target_playlist.name}",
                                details={
                                    "play_queue_id": existing_target_playlist.play_queue_id,
                                    "playlist_id": existing_target_playlist.id,
                                    "storage_model": existing_target_playlist.storage_model,
                                    "metadata_item_ids": [item.metadata_item_id for item in transfer_items],
                                },
                            )
                        )
                    else:
                        status = "in_sync"

            plans.append(
                PlaylistTransferPlan(
                    source_playlist=source_playlist,
                    target_playlist_name=target_playlist_name,
                    action=action,
                    status=status,
                    source_item_count=len(scoped_items),
                    matched_items=matched_items,
                    transfer_items=transfer_items,
                    unmatched_items=unmatched_items,
                    existing_target_playlist=existing_target_playlist,
                    notes=notes,
                )
            )

        return plans, mutations


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
        "playlist_id": "pl_id",
        "source_playlist": "src_playlist",
        "target_playlist": "tgt_playlist",
        "action": "action",
        "source_item_count": "src_items",
        "matched_item_count": "matched",
        "transfer_item_count": "transfer",
        "existing_item_count": "existing",
        "unmatched_item_count": "unmatched",
        "unmatched_items": "unmatched_items",
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
        "playlist_id",
        "source_item_count",
        "matched_item_count",
        "transfer_item_count",
        "existing_item_count",
        "unmatched_item_count",
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
        "playlist_id": 6,
        "source_playlist": 30,
        "target_playlist": 30,
        "action": 16,
        "source_item_count": 8,
        "matched_item_count": 8,
        "transfer_item_count": 8,
        "existing_item_count": 8,
        "unmatched_item_count": 10,
        "unmatched_items": 52,
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
    _questionary_module = None
    _questionary_checked = False
    _questionary_warning_shown = False
    PLAYLIST_ROW_COLUMNS = (
        "playlist_id",
        "source_playlist",
        "target_playlist",
        "status",
        "action",
        "source_item_count",
        "matched_item_count",
        "transfer_item_count",
        "existing_item_count",
        "unmatched_item_count",
        "notes",
        "unmatched_items",
    )
    PLAYLIST_TABLE_COLUMNS = (
        TableColumnSpec("playlist_id"),
        TableColumnSpec("source_playlist"),
        TableColumnSpec("target_playlist"),
        TableColumnSpec("status"),
        TableColumnSpec("action"),
        TableColumnSpec("matched_item_count"),
        TableColumnSpec("transfer_item_count"),
        TableColumnSpec("unmatched_item_count"),
        TableColumnSpec("notes"),
    )
    PLAYLIST_LIST_ROW_COLUMNS = (
        "playlist_id",
        "source_playlist",
        "source_item_count",
        "status",
        "notes",
    )
    PLAYLIST_LIST_TABLE_COLUMNS = (
        TableColumnSpec("playlist_id"),
        TableColumnSpec("source_playlist"),
        TableColumnSpec("source_item_count"),
        TableColumnSpec("status"),
        TableColumnSpec("notes"),
    )

    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.report_writer = PlexReportWriter()
        self.interactive_transfer = False

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
            default=None,
            help="Path to the source Plex location. Can be the DB file itself or a folder containing com.plexapp.plugins.library.db.",
        )
        transfer_parser.add_argument(
            "--target-path",
            default=None,
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
            "--source-account-id",
            type=int,
            default=None,
            help="Account id to use when reading source watch history.",
        )
        transfer_parser.add_argument(
            "--target-account-id",
            type=int,
            default=None,
            help="Account id to use when reading and writing target Plex watch state.",
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

        transfer_playlists_parser = subparsers.add_parser(
            "transfer-playlists",
            help="Transfer Plex playlists between two Plex library databases.",
            description="Transfer Plex playlists between two Plex SQLite library databases using filename-based item matching.",
        )
        transfer_playlists_parser.set_defaults(command="transfer-playlists")
        transfer_playlists_parser.add_argument(
            "--source-path",
            default=None,
            help="Path to the source Plex location or DB file.",
        )
        transfer_playlists_parser.add_argument(
            "--target-path",
            default=None,
            help="Path to the target Plex location or DB file.",
        )
        transfer_playlists_parser.add_argument(
            "--source-library",
            action="append",
            default=[],
            help="Source library section name to search for playlist items. Repeat to include multiple sections.",
        )
        transfer_playlists_parser.add_argument(
            "--target-library",
            action="append",
            default=[],
            help="Target library section name to search for playlist item matches. Repeat to include multiple sections.",
        )
        transfer_playlists_parser.add_argument(
            "--source-account-id",
            type=int,
            default=None,
            help="Optional source account id associated with the source Plex database.",
        )
        transfer_playlists_parser.add_argument(
            "--target-account-id",
            type=int,
            default=None,
            help="Target account id to associate with created or updated target playlists.",
        )
        transfer_playlists_parser.add_argument(
            "--playlist",
            action="append",
            default=[],
            help="Playlist id or exact playlist name to transfer. Repeat to include multiple playlists.",
        )
        transfer_playlists_parser.add_argument(
            "--playlist-conflict-policy",
            choices=["unique", "merge", "replace", "skip"],
            default="unique",
            help="How to handle target playlists that already exist with the same name.",
        )
        transfer_playlists_parser.add_argument(
            "--include-empty-playlists",
            action="store_true",
            help="Include playlists that are empty after applying the selected source library scope.",
        )
        transfer_playlists_parser.add_argument(
            "--match-mode",
            choices=["strict", "balanced", "loose"],
            default="balanced",
            help="Controls how strict duplicate resolution is when multiple target rows share the exact same basename.",
        )
        transfer_playlists_parser.add_argument(
            "--min-confidence",
            type=float,
            default=0.65,
            help="Minimum confidence required to resolve duplicate exact-basename candidates.",
        )
        transfer_playlists_parser.add_argument(
            "--report",
            type=Path,
            default=None,
            help="Optional path for a JSON, CSV, or plain-text table report.",
        )
        transfer_playlists_parser.add_argument(
            "--report-format",
            choices=["auto", "json", "csv", "table"],
            default="auto",
            help="Explicit report format. Defaults to auto-detecting from the report file extension.",
        )
        transfer_playlists_parser.add_argument(
            "--console-format",
            choices=["json", "csv", "table"],
            default="table",
            help="Console output format for playlist transfer results.",
        )
        transfer_playlists_parser.add_argument(
            "--apply",
            action="store_true",
            help="Write the planned playlist mutations into the target DB. Without this flag the tool is dry-run only.",
        )

        list_playlists_parser = subparsers.add_parser(
            "list-playlists",
            help="List playlists in a Plex library database.",
        )
        list_playlists_parser.set_defaults(command="list-playlists")
        list_playlists_parser.add_argument(
            "--path",
            required=True,
            help="Path to the Plex location or DB file to inspect.",
        )
        list_playlists_parser.add_argument(
            "--library",
            action="append",
            default=[],
            help="Library section name to scope playlist item discovery. Repeat to include multiple sections.",
        )
        list_playlists_parser.add_argument(
            "--include-empty-playlists",
            action="store_true",
            help="Include playlists that are empty after library scoping.",
        )
        list_playlists_parser.add_argument(
            "--console-format",
            choices=["json", "csv", "table"],
            default="table",
            help="Console output format for playlist listing.",
        )
        list_playlists_parser.add_argument(
            "--report",
            type=Path,
            default=None,
            help="Optional path for a JSON, CSV, or plain-text table report.",
        )
        list_playlists_parser.add_argument(
            "--report-format",
            choices=["auto", "json", "csv", "table"],
            default="auto",
            help="Explicit report format. Defaults to auto-detecting from the report file extension.",
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
        subcommands = {"transfer", "transfer-playlists", "list-playlists", "list-libraries", "list-accounts"}
        if raw_argv and not raw_argv[0].startswith("-") and raw_argv[0] not in subcommands:
            raw_argv = ["transfer", *raw_argv]
        if raw_argv and raw_argv[0].startswith("-") and raw_argv[0] not in {"-h", "--help"}:
            raw_argv = ["transfer", *raw_argv]
        return cls.build_parser().parse_args(raw_argv)

    @classmethod
    def main(cls, argv: Optional[Sequence[str]] = None) -> int:
        try:
            app = cls(cls.parse_args(argv))
            return app.run()
        except KeyboardInterrupt:
            print("Aborted.", file=sys.stderr)
            return 130

    def run(self) -> int:
        if self.args.command == "list-libraries":
            return self.run_list_libraries()
        if self.args.command == "list-accounts":
            return self.run_list_accounts()
        if self.args.command == "list-playlists":
            return self.run_list_playlists()
        if self.args.command == "transfer-playlists":
            return self.run_transfer_playlists()
        return self.run_transfer()

    @staticmethod
    def load_questionary_module():
        if not PlexWatchStatusTransferApp._questionary_checked:
            PlexWatchStatusTransferApp._questionary_checked = True
            try:
                PlexWatchStatusTransferApp._questionary_module = importlib.import_module("questionary")
            except Exception:
                PlexWatchStatusTransferApp._questionary_module = None
        return PlexWatchStatusTransferApp._questionary_module

    @classmethod
    def maybe_warn_questionary_unavailable(cls) -> None:
        if cls.load_questionary_module() is not None or cls._questionary_warning_shown:
            return
        cls._questionary_warning_shown = True
        print(
            "questionary is not installed; using plain text prompts instead of the interactive selector."
        )

    @staticmethod
    def require_questionary_selection(selected: Any) -> Any:
        if selected is None:
            raise KeyboardInterrupt
        return selected

    @classmethod
    def prompt_questionary_checkbox(
        cls,
        prompt: str,
        choices: Sequence[Any],
        instruction: Optional[str] = None,
        selection_prompt: str = "Selection",
    ) -> Optional[List[Any]]:
        questionary = cls.load_questionary_module()
        if questionary is None:
            return None

        print(prompt)
        prompt_kwargs: Dict[str, Any] = {
            "choices": list(choices),
        }
        if instruction is not None:
            prompt_kwargs["instruction"] = instruction
        selected = questionary.checkbox(selection_prompt, **prompt_kwargs).ask()
        return list(cls.require_questionary_selection(selected))

    @classmethod
    def prompt_questionary_select(
        cls,
        prompt: str,
        choices: Sequence[Any],
        default: Optional[str] = None,
        selection_prompt: str = "Selection",
    ) -> Optional[Any]:
        questionary = cls.load_questionary_module()
        if questionary is None:
            return None

        print(prompt)
        selected = questionary.select(selection_prompt, choices=list(choices), default=default).ask()
        return cls.require_questionary_selection(selected)

    @staticmethod
    def prompt_with_default(prompt: str, default: Optional[str] = None) -> str:
        while True:
            suffix = f" [{default}]" if default not in (None, "") else ""
            value = input(f"{prompt}{suffix}: ").strip()
            if value:
                return value
            if default not in (None, ""):
                return str(default)
            print("A value is required.")

    @classmethod
    def prompt_int_with_default(cls, prompt: str, default: Optional[int] = None) -> int:
        while True:
            raw_value = cls.prompt_with_default(prompt, None if default is None else str(default))
            try:
                return int(raw_value)
            except ValueError:
                print("Enter a whole number.")

    @staticmethod
    def describe_account(account: PlexAccount) -> str:
        if account.name:
            return f"{account.id}: {account.name}"
        return f"{account.id}: (unnamed account)"

    @classmethod
    def prompt_account_id(
        cls,
        prompt: str,
        accounts: Sequence[PlexAccount],
        default: Optional[int] = None,
    ) -> int:
        valid_ids = {account.id for account in accounts}
        if not valid_ids:
            raise RuntimeError("No Plex accounts were found in the selected database.")

        print(prompt)
        for account in accounts:
            print(f"  {cls.describe_account(account)}")

        if default is not None and default not in valid_ids:
            print(f"Default account id {default} is not present in this database.")
            default = None

        while True:
            selected_id = cls.prompt_int_with_default("Choose account id", default)
            if selected_id in valid_ids:
                return selected_id
            print("Choose one of the listed account ids.")

    @staticmethod
    def infer_interactive_account_defaults(
        source_accounts: Sequence[PlexAccount],
        target_accounts: Sequence[PlexAccount],
        source_default: Optional[int],
        target_default: Optional[int],
    ) -> Tuple[Optional[int], Optional[int]]:
        def named_account_map(accounts: Sequence[PlexAccount]) -> Dict[str, PlexAccount]:
            return {
                account.name.casefold(): account
                for account in accounts
                if account.name
            }

        resolved_source_default = source_default
        resolved_target_default = target_default

        source_named = named_account_map(source_accounts)
        target_named = named_account_map(target_accounts)
        shared_names = sorted(set(source_named) & set(target_named))
        if len(shared_names) == 1:
            shared_name = shared_names[0]
            if resolved_source_default is None:
                resolved_source_default = source_named[shared_name].id
            if resolved_target_default is None:
                resolved_target_default = target_named[shared_name].id

        if resolved_source_default is None and len(source_named) == 1:
            resolved_source_default = next(iter(source_named.values())).id
        if resolved_target_default is None and len(target_named) == 1:
            resolved_target_default = next(iter(target_named.values())).id

        return resolved_source_default, resolved_target_default

    @staticmethod
    def describe_library_section(section: PlexLibrarySection) -> str:
        if section.name:
            return f"{section.id}: {section.name}"
        return f"{section.id}: (unnamed library)"

    @classmethod
    def prompt_library_filters(
        cls,
        prompt: str,
        libraries: Sequence[PlexLibrarySection],
        default_names: Sequence[str],
    ) -> List[str]:
        library_by_id = {library.id: library for library in libraries}
        library_by_name = {library.name.casefold(): library for library in libraries if library.name}

        invalid_defaults = [name for name in default_names if name.casefold() not in library_by_name]
        if invalid_defaults:
            print(
                "Ignoring default libraries not present in this database: "
                + ", ".join(invalid_defaults)
            )
            default_names = [name for name in default_names if name.casefold() in library_by_name]

        default_values = list(default_names) if default_names else [
            library.name
            for library in libraries
            if library.name
        ]
        default_value_set = set(default_values)

        questionary = cls.load_questionary_module()
        if questionary is not None:
            choices = [
                questionary.Choice(
                    title=f"{library.id}: {library.name}",
                    value=library.name,
                    checked=library.name in default_value_set,
                )
                for library in libraries
            ]
            return cls.prompt_questionary_checkbox(
                prompt,
                choices=choices,
                instruction="Space to toggle, Enter to confirm, Esc to cancel",
                selection_prompt="Libraries",
            )

        cls.maybe_warn_questionary_unavailable()

        print(prompt)
        for library in libraries:
            print(f"  {cls.describe_library_section(library)}")

        default_label = ", ".join(default_names) if default_names else "all"
        prompt_suffix = f" [{default_label}]" if default_label else ""
        while True:
            raw_value = input(
                f"Choose library ids or names (comma-separated, Enter for all libraries){prompt_suffix}: "
            ).strip()
            if not raw_value:
                return list(default_names)

            selections: List[str] = []
            seen = set()
            valid = True
            for token in (part.strip() for part in raw_value.split(",")):
                if not token:
                    continue
                library: Optional[PlexLibrarySection] = None
                if token.isdigit():
                    library = library_by_id.get(int(token))
                if library is None:
                    library = library_by_name.get(token.casefold())
                if library is None:
                    print(f"Choose only listed libraries. Invalid selection: {token}")
                    valid = False
                    break
                if library.name not in seen:
                    seen.add(library.name)
                    selections.append(library.name)
            if valid:
                return selections

    @staticmethod
    def prompt_yes_no(prompt: str, default: bool = False) -> bool:
        default_hint = "[Y/n]" if default else "[y/N]"
        while True:
            value = input(f"{prompt} {default_hint}: ").strip().casefold()
            if not value:
                return default
            if value in {"y", "yes"}:
                return True
            if value in {"n", "no"}:
                return False
            print("Answer yes or no.")

    @classmethod
    def prompt_choice(
        cls,
        prompt: str,
        choices: Sequence[str],
        default: Optional[str] = None,
    ) -> str:
        valid = {choice.casefold(): choice for choice in choices}
        selected = cls.prompt_questionary_select(prompt, choices, default, selection_prompt="Conflict behavior")
        if selected is not None:
            return str(selected)

        cls.maybe_warn_questionary_unavailable()

        while True:
            rendered_choices = "/".join(choices)
            raw_value = cls.prompt_with_default(f"{prompt} ({rendered_choices})", default)
            selected = valid.get(raw_value.casefold())
            if selected is not None:
                return selected
            print("Choose one of the listed values.")

    @classmethod
    def prompt_playlist_filters(
        cls,
        prompt: str,
        playlists: Sequence[PlexPlaylist],
        default_selectors: Sequence[str],
        include_empty_playlists: bool,
    ) -> List[str]:
        playlist_by_id = {str(playlist.id): playlist for playlist in playlists}
        playlist_by_name = {playlist.name.casefold(): playlist for playlist in playlists}

        default_values: List[str] = []
        if default_selectors:
            default_values = list(default_selectors)
        else:
            default_values = [
                str(playlist.id)
                for playlist in playlists
                if include_empty_playlists or not playlist.is_empty_in_scope
            ]
        default_value_set = set(default_values)

        questionary = cls.load_questionary_module()
        if questionary is not None:
            choices = []
            for playlist in playlists:
                status = "empty" if playlist.is_empty_in_scope else f"{len(playlist.scoped_items)} in scope"
                title = f"{playlist.id}: {playlist.name} ({status})"
                choices.append(
                    questionary.Choice(
                        title=title,
                        value=str(playlist.id),
                        checked=str(playlist.id) in default_value_set,
                    )
                )
            return cls.prompt_questionary_checkbox(prompt, choices, selection_prompt="Playlists")

        cls.maybe_warn_questionary_unavailable()

        print(prompt)
        for playlist in playlists:
            status = "empty" if playlist.is_empty_in_scope else f"{len(playlist.scoped_items)} in scope"
            print(f"  {playlist.id}: {playlist.name} ({status})")

        default_label = ", ".join(default_values) if default_values else "none"
        while True:
            raw_value = input(
                f"Choose playlist ids or exact names (comma-separated, Enter for default selection) [{default_label}]: "
            ).strip()
            if not raw_value:
                return list(default_values)

            selections: List[str] = []
            seen = set()
            valid = True
            for token in (part.strip() for part in raw_value.split(",")):
                if not token:
                    continue
                playlist = playlist_by_id.get(token) or playlist_by_name.get(token.casefold())
                if playlist is None:
                    print(f"Choose only listed playlists. Invalid selection: {token}")
                    valid = False
                    break
                playlist_id = str(playlist.id)
                if playlist_id not in seen:
                    seen.add(playlist_id)
                    selections.append(playlist_id)
            if valid:
                return selections

    @staticmethod
    def apply_planned_mutations(target_db_path: Path, mutations: Sequence[PlannedMutation]) -> None:
        PlexEnvironment.wait_for_plex_shutdown()
        database = PlexDatabase(target_db_path, readonly=False)
        try:
            database.begin_immediate()
            database.apply_mutations(mutations)
            database.commit()
        finally:
            database.close()

    @classmethod
    def resolve_playlist_selection(
        cls,
        playlists: Sequence[PlexPlaylist],
        selectors: Sequence[str],
        include_empty_playlists: bool,
    ) -> List[PlexPlaylist]:
        playlist_by_id = {str(playlist.id): playlist for playlist in playlists}
        playlist_by_name = {playlist.name.casefold(): playlist for playlist in playlists}

        if not selectors:
            selected = list(playlists)
        else:
            selected = []
            seen = set()
            for selector in selectors:
                playlist = playlist_by_id.get(str(selector)) or playlist_by_name.get(str(selector).casefold())
                if playlist is None:
                    raise RuntimeError(f"Playlist not found in source DB selection: {selector}")
                if playlist.id in seen:
                    continue
                seen.add(playlist.id)
                selected.append(playlist)

        if include_empty_playlists:
            return selected
        return [playlist for playlist in selected if not playlist.is_empty_in_scope]

    @classmethod
    def build_playlist_rows(cls, plans: Sequence[PlaylistTransferPlan]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for plan in plans:
            unmatched_labels = "; ".join(item.source_item.display_label for item in plan.unmatched_items[:5])
            if len(plan.unmatched_items) > 5:
                unmatched_labels += f"; +{len(plan.unmatched_items) - 5} more"
            rows.append(
                {
                    "playlist_id": plan.source_playlist.id,
                    "source_playlist": plan.source_playlist.name,
                    "target_playlist": plan.target_playlist_name,
                    "status": plan.status,
                    "action": plan.action,
                    "source_item_count": plan.source_item_count,
                    "matched_item_count": plan.matched_item_count,
                    "transfer_item_count": plan.transfer_item_count,
                    "existing_item_count": len(plan.existing_target_playlist.items) if plan.existing_target_playlist else 0,
                    "unmatched_item_count": plan.unmatched_item_count,
                    "notes": "; ".join(plan.notes),
                    "unmatched_items": unmatched_labels,
                }
            )
        return rows

    def emit_playlist_outputs(
        self,
        plans: Sequence[PlaylistTransferPlan],
        mutations: Sequence[PlannedMutation],
        console_format: str,
        report_path: Optional[Path],
        report_format: str,
    ) -> None:
        rows = self.build_playlist_rows(plans)
        payload = {
            "summary": {
                "playlists": len(plans),
                "planned_mutations": len(mutations),
                "playlists_with_unmatched_items": sum(1 for plan in plans if plan.unmatched_items),
            },
            "playlists": [
                {
                    "id": plan.source_playlist.id,
                    "source_name": plan.source_playlist.name,
                    "target_name": plan.target_playlist_name,
                    "status": plan.status,
                    "action": plan.action,
                    "source_item_count": plan.source_item_count,
                    "matched_item_count": plan.matched_item_count,
                    "transfer_item_count": plan.transfer_item_count,
                    "notes": plan.notes,
                    "unmatched_items": [
                        {
                            "label": item.source_item.display_label,
                            "reason": item.reason,
                            "notes": item.notes,
                        }
                        for item in plan.unmatched_items
                    ],
                }
                for plan in plans
            ],
            "mutations": [asdict(mutation) for mutation in mutations],
        }

        if console_format == "json":
            print(json.dumps(payload, indent=2))
        elif console_format == "csv":
            self.report_writer._write_csv_rows(sys.stdout, rows, self.PLAYLIST_ROW_COLUMNS)
        else:
            self.report_writer.write_table_rows(sys.stdout, rows, self.PLAYLIST_TABLE_COLUMNS)

        if report_path is None:
            return

        resolved_format = self.report_writer.resolve_report_format(report_path, report_format)
        if resolved_format == "json":
            with report_path.open("w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2)
        elif resolved_format == "csv":
            with report_path.open("w", newline="", encoding="utf-8") as handle:
                self.report_writer._write_csv_rows(handle, rows, self.PLAYLIST_ROW_COLUMNS)
        else:
            with report_path.open("w", encoding="utf-8") as handle:
                self.report_writer.write_table_rows(handle, rows, self.PLAYLIST_TABLE_COLUMNS)

    def emit_playlist_listing_outputs(
        self,
        rows: Sequence[Dict[str, Any]],
        console_format: str,
        report_path: Optional[Path],
        report_format: str,
    ) -> None:
        payload = {
            "summary": {
                "playlists": len(rows),
                "empty": sum(1 for row in rows if row.get("status") == "empty"),
            },
            "playlists": list(rows),
        }

        if console_format == "json":
            print(json.dumps(payload, indent=2))
        elif console_format == "csv":
            self.report_writer._write_csv_rows(sys.stdout, rows, self.PLAYLIST_LIST_ROW_COLUMNS)
        else:
            self.report_writer.write_table_rows(sys.stdout, rows, self.PLAYLIST_LIST_TABLE_COLUMNS)

        if report_path is None:
            return

        resolved_format = self.report_writer.resolve_report_format(report_path, report_format)
        if resolved_format == "json":
            with report_path.open("w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2)
        elif resolved_format == "csv":
            with report_path.open("w", newline="", encoding="utf-8") as handle:
                self.report_writer._write_csv_rows(handle, rows, self.PLAYLIST_LIST_ROW_COLUMNS)
        else:
            with report_path.open("w", encoding="utf-8") as handle:
                self.report_writer.write_table_rows(handle, rows, self.PLAYLIST_LIST_TABLE_COLUMNS)

    @staticmethod
    def print_playlist_summary(
        plans: Sequence[PlaylistTransferPlan],
        mutations: Sequence[PlannedMutation],
        apply: bool,
        stream: TextIO = sys.stdout,
    ) -> None:
        try:
            print(f"Selected playlists: {len(plans)}", file=stream)
            print(
                f"Playlists with unmatched items: {sum(1 for plan in plans if plan.unmatched_items)}",
                file=stream,
            )
            print(f"Planned playlist mutations: {len(mutations)}", file=stream)
            print("Mode: apply" if apply else "Mode: dry-run", file=stream)
            stream.flush()
        except OSError as exc:
            if PlexReportWriter._is_broken_pipe_error(exc):
                if stream is sys.stdout:
                    PlexReportWriter._suppress_stdout_after_pipe_error()
                return
            raise

    @staticmethod
    def print_playlist_unmatched_details(
        plans: Sequence[PlaylistTransferPlan],
        stream: TextIO = sys.stdout,
    ) -> None:
        try:
            for plan in plans:
                if not plan.unmatched_items:
                    continue
                print(f"Unmatched items for playlist '{plan.source_playlist.name}':", file=stream)
                for item in plan.unmatched_items:
                    extra = f" ({'; '.join(item.notes)})" if item.notes else ""
                    print(f"  - {item.source_item.display_label}: {item.reason}{extra}", file=stream)
            stream.flush()
        except OSError as exc:
            if PlexReportWriter._is_broken_pipe_error(exc):
                if stream is sys.stdout:
                    PlexReportWriter._suppress_stdout_after_pipe_error()
                return
            raise

    @classmethod
    def populate_missing_transfer_args(cls, args: argparse.Namespace) -> bool:
        required_values = (
            args.source_path,
            args.target_path,
            args.source_account_id,
            args.target_account_id,
        )
        if all(value is not None for value in required_values):
            return False

        print("Interactive transfer setup")
        args.source_path = cls.prompt_with_default("Source Plex path", args.source_path)
        args.target_path = cls.prompt_with_default("Target Plex path", args.target_path)

        source_db_path = PlexDatabaseLocator.resolve_db_path(args.source_path, "source")
        target_db_path = PlexDatabaseLocator.resolve_db_path(args.target_path, "target")

        source_database = PlexDatabase(source_db_path, readonly=True)
        try:
            source_libraries = source_database.list_library_sections()
            source_accounts = source_database.list_accounts()
        finally:
            source_database.close()

        target_database = PlexDatabase(target_db_path, readonly=True)
        try:
            target_libraries = target_database.list_library_sections()
            target_accounts = target_database.list_accounts()
        finally:
            target_database.close()

        source_account_default, target_account_default = cls.infer_interactive_account_defaults(
            source_accounts,
            target_accounts,
            args.source_account_id,
            args.target_account_id,
        )

        args.source_library = cls.prompt_library_filters(
            "Source libraries:",
            source_libraries,
            args.source_library,
        )
        args.target_library = cls.prompt_library_filters(
            "Target libraries:",
            target_libraries,
            args.target_library,
        )

        args.source_account_id = cls.prompt_account_id(
            "Source accounts:",
            source_accounts,
            source_account_default,
        )
        args.target_account_id = cls.prompt_account_id(
            "Target accounts:",
            target_accounts,
            target_account_default,
        )
        return True

    @classmethod
    def populate_missing_playlist_transfer_args(cls, args: argparse.Namespace) -> bool:
        needs_paths = args.source_path is None or args.target_path is None
        needs_playlist_selection = not args.playlist and getattr(sys.stdin, "isatty", lambda: False)()
        needs_target_account = args.target_account_id is None and getattr(sys.stdin, "isatty", lambda: False)()
        if not needs_paths and not needs_playlist_selection and not needs_target_account:
            return False

        print("Interactive playlist transfer setup")
        args.source_path = cls.prompt_with_default("Source Plex path", args.source_path)
        args.target_path = cls.prompt_with_default("Target Plex path", args.target_path)

        source_db_path = PlexDatabaseLocator.resolve_db_path(args.source_path, "source")
        target_db_path = PlexDatabaseLocator.resolve_db_path(args.target_path, "target")

        source_database = PlexDatabase(source_db_path, readonly=True)
        try:
            source_schema = source_database.inspect_schema()
            source_libraries = source_database.list_library_sections()
            args.source_library = cls.prompt_library_filters(
                "Source libraries:",
                source_libraries,
                args.source_library,
            )
            scoped_inventory = source_database.build_media_inventory(source_schema, args.source_library)
            all_inventory = scoped_inventory if not args.source_library else source_database.build_media_inventory(source_schema, [])
            playlists = source_database.list_playlists(source_schema, scoped_inventory, all_inventory)
        finally:
            source_database.close()

        target_database = PlexDatabase(target_db_path, readonly=True)
        try:
            target_schema = target_database.inspect_schema()
            target_libraries = target_database.list_library_sections()
            target_accounts = target_database.list_accounts()
            args.target_library = cls.prompt_library_filters(
                "Target libraries:",
                target_libraries,
                args.target_library,
            )
            target_inventory = target_database.build_media_inventory(target_schema, args.target_library)
            target_inventory_all = (
                target_inventory
                if not args.target_library
                else target_database.build_media_inventory(target_schema, [])
            )
            target_playlists = target_database.list_playlists(target_schema, target_inventory, target_inventory_all)
        finally:
            target_database.close()

        if args.target_account_id is None:
            args.target_account_id = cls.prompt_account_id(
                "Target accounts:",
                target_accounts,
                args.target_account_id,
            )

        if not args.include_empty_playlists:
            print("Empty playlists are excluded by default. Use --include-empty-playlists to include them.")

        args.playlist = cls.prompt_playlist_filters(
            "Select playlists to transfer:",
            playlists,
            args.playlist,
            args.include_empty_playlists,
        )

        selected_playlists = cls.resolve_playlist_selection(
            playlists,
            args.playlist,
            args.include_empty_playlists,
        )
        target_playlist_names = {playlist.name.casefold() for playlist in target_playlists}
        has_playlist_conflicts = any(
            playlist.name.casefold() in target_playlist_names
            for playlist in selected_playlists
        )
        if has_playlist_conflicts:
            args.playlist_conflict_policy = cls.prompt_choice(
                "Choose playlist conflict policy:",
                ["unique", "merge", "replace", "skip"],
                args.playlist_conflict_policy,
            )
        return True

    def run_transfer(self) -> int:
        self.interactive_transfer = self.populate_missing_transfer_args(self.args)
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
            source_account_id = self.args.source_account_id
            target_account_id = self.args.target_account_id

            source_inventory = source_database.build_media_inventory(source_schema, self.args.source_library)
            target_inventory = target_database.build_media_inventory(target_schema, self.args.target_library)
            target_inventory_all = target_inventory
            if not self.args.apply:
                target_inventory_all = target_database.build_media_inventory(target_schema, [])
            source_history = source_database.build_watch_history(self.args.source_library, source_account_id)
            target_history = target_database.build_watch_history(self.args.target_library, target_account_id)

            matcher = PlexMatcher(self.args.match_mode, self.args.min_confidence)
            matches = matcher.collect_matches(
                source_inventory=source_inventory,
                source_history=source_history,
                target_inventory=target_inventory,
                target_history=target_history,
            )
            self.annotate_library_statuses(matches, matcher, target_inventory_all, bool(self.args.target_library))

            mutation_planner = PlexMutationPlanner(target_schema, target_account_id, self.args.conflict_policy)
            mutations = mutation_planner.plan_mutations(matches)
            columns = self.report_writer.parse_columns(self.args.columns)
            filtered_matches = matches
            if dry_run_filters_active:
                filtered_matches = self.filter_dry_run_matches(
                    matches,
                    dry_run_filter_mode,
                )
            elif self.interactive_transfer and not self.args.apply:
                filtered_matches = self.filter_matches_with_planned_mutations(matches, mutations)
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

            if self.interactive_transfer and not self.args.apply:
                should_apply = self.prompt_yes_no("Apply these changes?", default=False)
                if should_apply:
                    self.apply_planned_mutations(target_db_path, mutations)
                    self.report_writer.print_summary(matches, mutations, True, stream=summary_stream)
                else:
                    print("Changes were not applied.", file=summary_stream)

            self.report_writer.detach_redirected_stdout()
            return 0
        finally:
            source_database.close()
            target_database.close()

    def run_transfer_playlists(self) -> int:
        self.interactive_transfer = self.populate_missing_playlist_transfer_args(self.args)

        source_db_path = PlexDatabaseLocator.resolve_db_path(self.args.source_path, "source")
        target_db_path = PlexDatabaseLocator.resolve_db_path(self.args.target_path, "target")

        if self.args.apply:
            PlexEnvironment.wait_for_plex_shutdown()

        source_database = PlexDatabase(source_db_path, readonly=True)
        target_database = PlexDatabase(target_db_path, readonly=not self.args.apply)

        try:
            source_schema = source_database.inspect_schema()
            target_schema = target_database.inspect_schema()
            if not source_schema.supports_playlists:
                raise RuntimeError("Source DB does not expose the playlist tables required for playlist transfer.")
            if not target_schema.supports_playlists:
                raise RuntimeError("Target DB does not expose the playlist tables required for playlist transfer.")

            source_inventory = source_database.build_media_inventory(source_schema, self.args.source_library)
            source_inventory_all = source_inventory if not self.args.source_library else source_database.build_media_inventory(source_schema, [])
            target_inventory = target_database.build_media_inventory(target_schema, self.args.target_library)
            target_inventory_all = target_inventory if not self.args.target_library else target_database.build_media_inventory(target_schema, [])

            source_playlists = source_database.list_playlists(source_schema, source_inventory, source_inventory_all)
            selected_playlists = self.resolve_playlist_selection(
                source_playlists,
                self.args.playlist,
                self.args.include_empty_playlists,
            )
            target_playlists = target_database.list_playlists(target_schema, target_inventory, target_inventory_all)
            target_account_id = self.args.target_account_id
            if target_account_id is None:
                raise RuntimeError(
                    "Playlist transfer requires --target-account-id. Pass it explicitly or use interactive mode to choose a target account."
                )

            if not self.args.include_empty_playlists:
                print("Empty playlists are excluded by default. Use --include-empty-playlists to include them.")

            matcher = PlexMatcher(self.args.match_mode, self.args.min_confidence)
            planner = PlexPlaylistPlanner(
                matcher,
                self.args.playlist_conflict_policy,
                self.args.include_empty_playlists,
            )
            plans, mutations = planner.plan_transfers(
                selected_playlists,
                target_playlists,
                target_inventory,
                target_inventory_all,
                target_account_id=target_account_id,
                has_target_library_filter=bool(self.args.target_library),
            )

            if self.args.apply and mutations:
                target_database.begin_immediate()
                target_database.apply_mutations(mutations)
                target_database.commit()

            self.emit_playlist_outputs(
                plans,
                mutations,
                self.args.console_format,
                self.args.report,
                self.args.report_format,
            )
            summary_stream = sys.stderr if self.args.console_format in {"json", "csv"} else sys.stdout
            self.print_playlist_summary(plans, mutations, self.args.apply, summary_stream)
            self.print_playlist_unmatched_details(plans, summary_stream)

            if self.interactive_transfer and not self.args.apply:
                print("Dry-run only: no playlist changes have been written yet.", file=summary_stream)
                should_apply = self.prompt_yes_no(
                    "Write these playlist changes to the target Plex DB now?",
                    default=False,
                )
                if should_apply:
                    self.apply_planned_mutations(target_db_path, mutations)
                    self.print_playlist_summary(plans, mutations, True, summary_stream)
                else:
                    print("No playlist changes were written.", file=summary_stream)

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
    def filter_matches_with_planned_mutations(
        matches: Sequence[MatchResult],
        mutations: Sequence[PlannedMutation],
    ) -> List[MatchResult]:
        planned_target_guids = {mutation.target_guid for mutation in mutations}
        return [
            match
            for match in matches
            if match.target is not None and match.target.guid in planned_target_guids
        ]

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

    def run_list_playlists(self) -> int:
        database = PlexDatabase(PlexDatabaseLocator.resolve_db_path(self.args.path, "path"), readonly=True)
        try:
            schema = database.inspect_schema()
            if not schema.supports_playlists:
                raise RuntimeError("This Plex DB does not expose the playlist tables required for playlist listing.")

            scoped_inventory = database.build_media_inventory(schema, self.args.library)
            all_inventory = scoped_inventory if not self.args.library else database.build_media_inventory(schema, [])
            playlists = database.list_playlists(schema, scoped_inventory, all_inventory)
            selected_playlists = playlists if self.args.include_empty_playlists else [
                playlist
                for playlist in playlists
                if not playlist.is_empty_in_scope
            ]
            rows = [
                {
                    "playlist_id": playlist.id,
                    "source_playlist": playlist.name,
                    "source_item_count": len(playlist.scoped_items),
                    "status": "empty" if playlist.is_empty_in_scope else "available",
                    "notes": "empty in selected source library scope" if playlist.is_empty_in_scope else "",
                }
                for playlist in selected_playlists
            ]
            self.emit_playlist_listing_outputs(rows, self.args.console_format, self.args.report, self.args.report_format)
            return 0
        finally:
            database.close()


if __name__ == "__main__":
    raise SystemExit(PlexWatchStatusTransferApp.main())
