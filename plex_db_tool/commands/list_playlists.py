import json
import sys
from argparse import ArgumentParser, Namespace, _SubParsersAction
from pathlib import Path
from typing import Any, Dict, Optional, Sequence

from ..cli_support import PlexCliSupport
from ..infrastructure import PlexDatabase, PlexDatabaseLocator
from ..reporting import PlexReportWriter


def register(subparsers: _SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "list-playlists",
        help="List playlists in a Plex library database.",
    )
    parser.set_defaults(command="list-playlists", command_handler=run)
    parser.add_argument(
        "--path",
        required=True,
        help="Path to the Plex location or DB file to inspect.",
    )
    parser.add_argument(
        "--library",
        action="append",
        default=[],
        help="Library section name to scope playlist item discovery. Repeat to include multiple sections.",
    )
    parser.add_argument(
        "--include-empty-playlists",
        action="store_true",
        help="Include playlists that are empty after library scoping.",
    )
    parser.add_argument(
        "--console-format",
        choices=["json", "csv", "table"],
        default="table",
        help="Console output format for playlist listing.",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=None,
        help="Optional path for a JSON, CSV, or plain-text table report.",
    )
    parser.add_argument(
        "--report-format",
        choices=["auto", "json", "csv", "table"],
        default="auto",
        help="Explicit report format. Defaults to auto-detecting from the report file extension.",
    )


def run(args: Namespace) -> int:
    database = PlexDatabase(PlexDatabaseLocator.resolve_db_path(args.path, "path"), readonly=True)
    report_writer = PlexReportWriter()
    try:
        schema = database.inspect_schema()
        if not schema.supports_playlists:
            raise RuntimeError("This Plex DB does not expose the playlist tables required for playlist listing.")

        scoped_inventory = database.build_media_inventory(schema, args.library)
        all_inventory = scoped_inventory if not args.library else database.build_media_inventory(schema, [])
        playlists = database.list_playlists(schema, scoped_inventory, all_inventory)
        selected_playlists = playlists if args.include_empty_playlists else [
            playlist
            for playlist in playlists
            if not playlist.is_empty_in_scope
        ]
        rows = [
            {
                "playlist_id": playlist.id,
                "source_playlist": playlist.name,
                "account_id": playlist.account_id,
                "source_item_count": len(playlist.scoped_items),
                "status": "empty" if playlist.is_empty_in_scope else "available",
                "notes": "empty in selected source library scope" if playlist.is_empty_in_scope else "",
            }
            for playlist in selected_playlists
        ]
        emit_playlist_listing_outputs(rows, args.console_format, args.report, args.report_format, report_writer)
        return 0
    finally:
        database.close()


def emit_playlist_listing_outputs(
    rows: Sequence[Dict[str, Any]],
    console_format: str,
    report_path: Optional[Path],
    report_format: str,
    report_writer: PlexReportWriter,
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
        report_writer._write_csv_rows(sys.stdout, rows, PlexCliSupport.PLAYLIST_LIST_ROW_COLUMNS)
    else:
        report_writer.write_table_rows(sys.stdout, rows, PlexCliSupport.PLAYLIST_LIST_TABLE_COLUMNS)

    if report_path is None:
        return

    resolved_format = report_writer.resolve_report_format(report_path, report_format)
    if resolved_format == "json":
        with report_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
    elif resolved_format == "csv":
        with report_path.open("w", newline="", encoding="utf-8") as handle:
            report_writer._write_csv_rows(handle, rows, PlexCliSupport.PLAYLIST_LIST_ROW_COLUMNS)
    else:
        with report_path.open("w", encoding="utf-8") as handle:
            report_writer.write_table_rows(handle, rows, PlexCliSupport.PLAYLIST_LIST_TABLE_COLUMNS)