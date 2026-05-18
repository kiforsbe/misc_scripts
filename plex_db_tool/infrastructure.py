import hashlib
import json
import os
import sqlite3
import subprocess
import time
import uuid
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from .models import (
    MediaRecord,
    ParsedIdentity,
    PlannedMutation,
    PlexAccount,
    PlexLibrarySection,
    PlexPlaylist,
    PlexPlaylistItem,
    PlexSchema,
    TableColumn,
    WatchHistory,
)


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
        return {item.metadata_item_id: item for item in inventory}

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
                        added_at=PlexFilenameParser.safe_int(current_header.get("playlist_added_at")),
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

        playlists: List[PlexPlaylist] = []
        if schema.custom_channels_columns and schema.play_queue_items_columns:
            custom_query = """
            SELECT
                cc.id AS playlist_id,
                cc.name AS playlist_name,
                cc.description AS playlist_description,
                COALESCE(pq.created_at, pq.updated_at) AS playlist_added_at,
                pq.id AS play_queue_id,
                COALESCE(pq.account_id, mia.account_id) AS account_id,
                pqi.id AS play_queue_item_id,
                pqi.metadata_item_id AS metadata_item_id,
                pqi."order" AS order_value,
                md.title AS item_title,
                ls.name AS item_library_name
            FROM custom_channels cc
            LEFT JOIN metadata_item_accounts mia ON mia.metadata_item_id = cc.id
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
            playlists.extend(collect_playlists(custom_query, "custom"))

        if schema.play_queue_generators_columns:
            metadata_query = """
            SELECT
                playlist.id AS playlist_id,
                playlist.title AS playlist_name,
                NULL AS playlist_description,
                playlist.added_at AS playlist_added_at,
                latest_queue.play_queue_id AS play_queue_id,
                COALESCE(latest_queue.account_id, mia.account_id) AS account_id,
                generator.id AS play_queue_item_id,
                generator.metadata_item_id AS metadata_item_id,
                generator."order" AS order_value,
                md.title AS item_title,
                ls.name AS item_library_name
            FROM metadata_items playlist
            LEFT JOIN metadata_item_accounts mia ON mia.metadata_item_id = playlist.id
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
            playlists.extend(collect_playlists(metadata_query, "metadata"))

        return sorted(playlists, key=lambda playlist: (playlist.name.casefold(), playlist.id))

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
                    playlist_added_at = PlexFilenameParser.safe_int(details.get("added_at")) or now
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
                            playlist_added_at,
                            playlist_added_at,
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
                    self.update_metadata_playlist_added_at(
                        playlist_id,
                        PlexFilenameParser.safe_int(details.get("added_at")),
                    )
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
                    self.update_custom_playlist_added_at(
                        play_queue_id,
                        PlexFilenameParser.safe_int(details.get("added_at")),
                    )
            elif mutation.action == "delete_playlist":
                details = mutation.details
                if details.get("storage_model") == "metadata":
                    playlist_id = int(details["playlist_id"])
                    self.connection.execute(
                        "DELETE FROM play_queue_generators WHERE playlist_id = ?",
                        (playlist_id,),
                    )
                    self.connection.execute(
                        "DELETE FROM play_queues WHERE playlist_id = ?",
                        (playlist_id,),
                    )
                    self.connection.execute(
                        "DELETE FROM metadata_item_accounts WHERE metadata_item_id = ?",
                        (playlist_id,),
                    )
                    self.connection.execute(
                        "DELETE FROM metadata_items WHERE id = ?",
                        (playlist_id,),
                    )
                else:
                    playlist_id = int(details["playlist_id"])
                    play_queue_ids = [
                        int(row["id"])
                        for row in self.connection.execute(
                            "SELECT id FROM play_queues WHERE playlist_id = ?",
                            (playlist_id,),
                        ).fetchall()
                    ]
                    if play_queue_ids:
                        placeholders = ", ".join("?" for _ in play_queue_ids)
                        self.connection.execute(
                            f"DELETE FROM play_queue_items WHERE play_queue_id IN ({placeholders})",
                            play_queue_ids,
                        )
                    self.connection.execute(
                        "DELETE FROM play_queues WHERE playlist_id = ?",
                        (playlist_id,),
                    )
                    self.connection.execute(
                        "DELETE FROM metadata_item_accounts WHERE metadata_item_id = ?",
                        (playlist_id,),
                    )
                    self.connection.execute(
                        "DELETE FROM custom_channels WHERE id = ?",
                        (playlist_id,),
                    )
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
        playlist_added_at = PlexFilenameParser.safe_int(details.get("added_at")) or now
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
                    playlist_added_at,
                    playlist_added_at,
                    playlist_added_at,
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

    def update_metadata_playlist_added_at(self, playlist_id: int, added_at: Optional[int]) -> None:
        if added_at is None:
            return
        with self.temporarily_disable_metadata_fts_triggers():
            self.connection.execute(
                "UPDATE metadata_items SET added_at = ? WHERE id = ?",
                (added_at, playlist_id),
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

    def update_custom_playlist_added_at(self, play_queue_id: int, added_at: Optional[int]) -> None:
        if added_at is None:
            return
        self.connection.execute(
            "UPDATE play_queues SET created_at = ? WHERE id = ?",
            (added_at, play_queue_id),
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