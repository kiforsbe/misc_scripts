import json
import sys
from argparse import Namespace, _SubParsersAction
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, TextIO

from ..cli_support import PlexCliSupport
from ..infrastructure import PlexDatabase, PlexDatabaseLocator, PlexEnvironment
from ..models import PlannedMutation, PlexPlaylist, TableColumnSpec
from ..reporting import PlexReportWriter


REMOVE_PLAYLIST_ROW_COLUMNS = (
    "playlist_id",
    "source_playlist",
    "account_id",
    "source_item_count",
    "status",
    "action",
    "notes",
)

REMOVE_PLAYLIST_TABLE_COLUMNS = (
    TableColumnSpec("playlist_id"),
    TableColumnSpec("source_playlist"),
    TableColumnSpec("account_id"),
    TableColumnSpec("source_item_count"),
    TableColumnSpec("status"),
    TableColumnSpec("action"),
    TableColumnSpec("notes"),
)


def register(subparsers: _SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "remove-playlists",
        help="Remove playlists from a Plex library database.",
        description="Remove playlists from a Plex SQLite library database by playlist id or exact name.",
    )
    parser.set_defaults(command="remove-playlists", command_handler=run)
    parser.add_argument(
        "--path",
        default=None,
        help="Path to the Plex location or DB file.",
    )
    parser.add_argument(
        "--library",
        action="append",
        default=[],
        help="Library section name to scope playlist item discovery. Repeat to include multiple sections.",
    )
    parser.add_argument(
        "--playlist",
        action="append",
        default=[],
        help="Playlist id or exact playlist name to remove. Repeat to include multiple playlists.",
    )
    parser.add_argument(
        "--include-empty-playlists",
        action="store_true",
        help="Include playlists that are empty after applying the selected library scope.",
    )
    parser.add_argument(
        "--console-format",
        choices=["json", "csv", "table"],
        default="table",
        help="Console output format for playlist removal results.",
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
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Delete the selected playlists from the DB. Without this flag the tool is dry-run only.",
    )


def run(args: Namespace) -> int:
    interactive_remove = populate_missing_remove_args(args)

    db_path = PlexDatabaseLocator.resolve_db_path(args.path, "path")
    if args.apply:
        PlexEnvironment.wait_for_plex_shutdown()

    database = PlexDatabase(db_path, readonly=not args.apply)
    report_writer = PlexReportWriter()
    try:
        schema = database.inspect_schema()
        if not schema.supports_playlists:
            raise RuntimeError("This Plex DB does not expose the playlist tables required for playlist removal.")

        scoped_inventory = database.build_media_inventory(schema, args.library)
        all_inventory = scoped_inventory if not args.library else database.build_media_inventory(schema, [])
        playlists = database.list_playlists(schema, scoped_inventory, all_inventory)
        selected_playlists = resolve_remove_selection(playlists, args.playlist, args.include_empty_playlists)
        mutations = build_remove_mutations(selected_playlists)

        if args.apply and mutations:
            database.begin_immediate()
            database.apply_mutations(mutations)
            database.commit()

        emit_remove_outputs(
            selected_playlists,
            mutations,
            args.console_format,
            args.report,
            args.report_format,
            report_writer,
            args.apply,
        )

        summary_stream = sys.stderr if args.console_format in {"json", "csv"} else sys.stdout
        print_remove_summary(selected_playlists, args.apply, summary_stream)

        if interactive_remove and not args.apply:
            print("Dry-run only: no playlists have been removed yet.", file=summary_stream)
            should_apply = PlexCliSupport.prompt_yes_no(
                "Delete these playlists from the Plex DB now?",
                default=False,
            )
            if should_apply:
                PlexCliSupport.apply_planned_mutations(db_path, mutations)
                print_remove_summary(selected_playlists, True, summary_stream)
            else:
                print("No playlists were removed.", file=summary_stream)

        report_writer.detach_redirected_stdout()
        return 0
    finally:
        database.close()


def populate_missing_remove_args(args: Namespace) -> bool:
    is_interactive = getattr(sys.stdin, "isatty", lambda: False)()
    needs_path = args.path is None
    needs_playlist_selection = not args.playlist and is_interactive
    if not needs_path and not needs_playlist_selection:
        return False

    if not is_interactive and (needs_path or not args.playlist):
        missing_values = []
        if needs_path:
            missing_values.append("--path")
        if not args.playlist:
            missing_values.append("--playlist")
        raise RuntimeError(
            "Playlist removal requires "
            + " and ".join(missing_values)
            + " in non-interactive mode. Pass playlist ids from list-playlists or run in interactive mode."
        )

    print("Interactive playlist removal setup")
    args.path = PlexCliSupport.prompt_with_default("Plex path", args.path)

    db_path = PlexDatabaseLocator.resolve_db_path(args.path, "path")
    database = PlexDatabase(db_path, readonly=True)
    try:
        schema = database.inspect_schema()
        libraries = database.list_library_sections()
        args.library = PlexCliSupport.prompt_library_filters(
            "Libraries:",
            libraries,
            args.library,
        )
        scoped_inventory = database.build_media_inventory(schema, args.library)
        all_inventory = scoped_inventory if not args.library else database.build_media_inventory(schema, [])
        playlists = database.list_playlists(schema, scoped_inventory, all_inventory)
    finally:
        database.close()

    selectable_playlists = [
        playlist
        for playlist in playlists
        if args.include_empty_playlists or not playlist.is_empty_in_scope
    ]
    if not selectable_playlists:
        raise RuntimeError("No playlists are available for removal in the selected library scope.")

    if not args.include_empty_playlists:
        print("Empty playlists are excluded by default. Use --include-empty-playlists to include them.")

    args.playlist = PlexCliSupport.prompt_playlist_filters(
        "Select playlists to remove:",
        playlists,
        args.playlist,
        args.include_empty_playlists,
        select_all_by_default=False,
        allow_empty_selection=False,
    )
    return True


def resolve_remove_selection(
    playlists: Sequence[PlexPlaylist],
    selectors: Sequence[str],
    include_empty_playlists: bool,
) -> List[PlexPlaylist]:
    if not selectors:
        raise RuntimeError(
            "Playlist removal requires at least one --playlist selection. Use list-playlists to discover ids or run interactively."
        )

    selected_playlists = PlexCliSupport.resolve_playlist_selection(
        playlists,
        selectors,
        include_empty_playlists,
    )
    if not selected_playlists:
        raise RuntimeError("No playlists matched the requested removal selection.")
    return selected_playlists


def build_remove_mutations(playlists: Sequence[PlexPlaylist]) -> List[PlannedMutation]:
    return [
        PlannedMutation(
            action="delete_playlist",
            target_guid=f"playlist:{playlist.name}:{playlist.id}",
            details={
                "playlist_id": playlist.id,
                "play_queue_id": playlist.play_queue_id,
                "storage_model": playlist.storage_model,
            },
        )
        for playlist in playlists
    ]


def build_remove_rows(playlists: Sequence[PlexPlaylist], apply: bool) -> List[Dict[str, Any]]:
    action = "deleted" if apply else "delete"
    status = "deleted" if apply else "planned"
    return [
        {
            "playlist_id": playlist.id,
            "source_playlist": playlist.name,
            "account_id": playlist.account_id,
            "source_item_count": len(playlist.scoped_items),
            "status": status,
            "action": action,
            "notes": "empty in selected library scope" if playlist.is_empty_in_scope else "",
        }
        for playlist in playlists
    ]


def emit_remove_outputs(
    playlists: Sequence[PlexPlaylist],
    mutations: Sequence[PlannedMutation],
    console_format: str,
    report_path: Optional[Path],
    report_format: str,
    report_writer: PlexReportWriter,
    apply: bool,
) -> None:
    rows = build_remove_rows(playlists, apply)
    payload = {
        "summary": {
            "playlists": len(playlists),
            "planned_mutations": len(mutations),
            "mode": "apply" if apply else "dry-run",
        },
        "playlists": list(rows),
        "mutations": [
            mutation.details | {"action": mutation.action, "target_guid": mutation.target_guid}
            for mutation in mutations
        ],
    }

    if console_format == "json":
        print(json.dumps(payload, indent=2))
    elif console_format == "csv":
        report_writer._write_csv_rows(sys.stdout, rows, REMOVE_PLAYLIST_ROW_COLUMNS)
    else:
        report_writer.write_table_rows(sys.stdout, rows, REMOVE_PLAYLIST_TABLE_COLUMNS)

    if report_path is None:
        return

    resolved_format = report_writer.resolve_report_format(report_path, report_format)
    if resolved_format == "json":
        with report_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
    elif resolved_format == "csv":
        with report_path.open("w", newline="", encoding="utf-8") as handle:
            report_writer._write_csv_rows(handle, rows, REMOVE_PLAYLIST_ROW_COLUMNS)
    else:
        with report_path.open("w", encoding="utf-8") as handle:
            report_writer.write_table_rows(handle, rows, REMOVE_PLAYLIST_TABLE_COLUMNS)


def print_remove_summary(
    playlists: Sequence[PlexPlaylist],
    apply: bool,
    stream: TextIO = sys.stdout,
) -> None:
    try:
        print(f"Selected playlists: {len(playlists)}", file=stream)
        print(f"Playlist items in scope: {sum(len(playlist.scoped_items) for playlist in playlists)}", file=stream)
        print("Mode: apply" if apply else "Mode: dry-run", file=stream)
        stream.flush()
    except OSError as exc:
        if PlexReportWriter._is_broken_pipe_error(exc):
            if stream is sys.stdout:
                PlexReportWriter._suppress_stdout_after_pipe_error()
            return
        raise
