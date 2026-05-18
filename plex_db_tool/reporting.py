import csv
import errno
import json
import os
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, TextIO

from .models import MatchResult, PlannedMutation, TableColumnSpec


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
        "added_items": "added_items",
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
        "added_items": 52,
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

        labels = {column.name: cls.TABLE_COLUMN_LABELS.get(column.name, column.name) for column in columns}
        max_widths = {
            column.name: max(
                len(labels[column.name]),
                column.width if column.width is not None else cls.TABLE_FALLBACK_MAX_WIDTH,
            )
            for column in columns
        }
        widths = {column.name: len(labels[column.name]) for column in columns}
        string_rows: List[Dict[str, str]] = []
        for row in rows:
            string_row = {}
            for column in columns:
                value = cls._truncate_cell(cls._stringify_cell(row.get(column.name)), max_widths[column.name])
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