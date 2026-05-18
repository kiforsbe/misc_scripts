import json
import sys
from argparse import ArgumentParser, Namespace, _SubParsersAction
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, TextIO

from ..cli_support import PlexCliSupport
from ..infrastructure import PlexDatabase, PlexDatabaseLocator, PlexEnvironment
from ..models import PlannedMutation, PlaylistTransferPlan, PlexPlaylist
from ..planners import PlexMatcher, PlexPlaylistPlanner
from ..reporting import PlexReportWriter


def register(subparsers: _SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "transfer-playlists",
        help="Transfer Plex playlists between two Plex library databases.",
        description="Transfer Plex playlists between two Plex SQLite library databases using filename-based item matching.",
    )
    parser.set_defaults(command="transfer-playlists", command_handler=run)
    parser.add_argument(
        "--source-path",
        default=None,
        help="Path to the source Plex location or DB file.",
    )
    parser.add_argument(
        "--target-path",
        default=None,
        help="Path to the target Plex location or DB file.",
    )
    parser.add_argument(
        "--source-library",
        action="append",
        default=[],
        help="Source library section name to search for playlist items. Repeat to include multiple sections.",
    )
    parser.add_argument(
        "--target-library",
        action="append",
        default=[],
        help="Target library section name to search for playlist item matches. Repeat to include multiple sections.",
    )
    parser.add_argument(
        "--source-account-id",
        type=int,
        default=None,
        help="Optional source account id associated with the source Plex database.",
    )
    parser.add_argument(
        "--target-account-id",
        type=int,
        default=None,
        help="Target account id to associate with created or updated target playlists.",
    )
    parser.add_argument(
        "--playlist",
        action="append",
        default=[],
        help="Playlist id or exact playlist name to transfer. Repeat to include multiple playlists.",
    )
    parser.add_argument(
        "--playlist-conflict-policy",
        choices=["unique", "merge", "replace", "skip"],
        default="unique",
        help="How to handle target playlists that already exist with the same name.",
    )
    parser.add_argument(
        "--include-empty-playlists",
        action="store_true",
        help="Include playlists that are empty after applying the selected source library scope.",
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
        "--console-format",
        choices=["json", "csv", "table"],
        default="table",
        help="Console output format for playlist transfer results.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write the planned playlist mutations into the target DB. Without this flag the tool is dry-run only.",
    )


def run(args: Namespace) -> int:
    interactive_transfer = populate_missing_playlist_transfer_args(args)

    source_db_path = PlexDatabaseLocator.resolve_db_path(args.source_path, "source")
    target_db_path = PlexDatabaseLocator.resolve_db_path(args.target_path, "target")

    if args.apply:
        PlexEnvironment.wait_for_plex_shutdown()

    source_database = PlexDatabase(source_db_path, readonly=True)
    target_database = PlexDatabase(target_db_path, readonly=not args.apply)
    report_writer = PlexReportWriter()

    try:
        source_schema = source_database.inspect_schema()
        target_schema = target_database.inspect_schema()
        if not source_schema.supports_playlists:
            raise RuntimeError("Source DB does not expose the playlist tables required for playlist transfer.")
        if not target_schema.supports_playlists:
            raise RuntimeError("Target DB does not expose the playlist tables required for playlist transfer.")

        source_inventory = source_database.build_media_inventory(source_schema, args.source_library)
        source_inventory_all = source_inventory if not args.source_library else source_database.build_media_inventory(source_schema, [])
        target_inventory = target_database.build_media_inventory(target_schema, args.target_library)
        target_inventory_all = target_inventory if not args.target_library else target_database.build_media_inventory(target_schema, [])

        source_playlists = source_database.list_playlists(source_schema, source_inventory, source_inventory_all)
        selected_playlists = PlexCliSupport.resolve_playlist_selection(
            source_playlists,
            args.playlist,
            args.include_empty_playlists,
        )
        target_playlists = target_database.list_playlists(target_schema, target_inventory, target_inventory_all)
        target_account_id = args.target_account_id
        if target_account_id is None:
            raise RuntimeError(
                "Playlist transfer requires --target-account-id. Pass it explicitly or use interactive mode to choose a target account."
            )

        if not args.include_empty_playlists:
            print("Empty playlists are excluded by default. Use --include-empty-playlists to include them.")

        matcher = PlexMatcher(args.match_mode, args.min_confidence)
        planner = PlexPlaylistPlanner(
            matcher,
            args.playlist_conflict_policy,
            args.include_empty_playlists,
        )
        plans, mutations = planner.plan_transfers(
            selected_playlists,
            target_playlists,
            target_inventory,
            target_inventory_all,
            target_account_id=target_account_id,
            has_target_library_filter=bool(args.target_library),
        )

        if args.apply and mutations:
            target_database.begin_immediate()
            target_database.apply_mutations(mutations)
            target_database.commit()

        emit_playlist_outputs(
            plans,
            mutations,
            args.console_format,
            args.report,
            args.report_format,
            report_writer,
        )
        summary_stream = sys.stderr if args.console_format in {"json", "csv"} else sys.stdout
        print_playlist_summary(plans, mutations, args.apply, summary_stream)
        print_playlist_unmatched_details(plans, summary_stream)

        if interactive_transfer and not args.apply:
            print("Dry-run only: no playlist changes have been written yet.", file=summary_stream)
            should_apply = PlexCliSupport.prompt_yes_no(
                "Write these playlist changes to the target Plex DB now?",
                default=False,
            )
            if should_apply:
                PlexCliSupport.apply_planned_mutations(target_db_path, mutations)
                print_playlist_summary(plans, mutations, True, summary_stream)
            else:
                print("No playlist changes were written.", file=summary_stream)

        report_writer.detach_redirected_stdout()
        return 0
    finally:
        source_database.close()
        target_database.close()


def format_unix_timestamp(timestamp: Optional[int]) -> str:
    if timestamp is None:
        return ""
    return datetime.fromtimestamp(timestamp).isoformat(sep=" ", timespec="seconds")


def build_playlist_rows(plans: Sequence[PlaylistTransferPlan]) -> List[Dict[str, Any]]:
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
                "source_added_at": format_unix_timestamp(plan.source_playlist.added_at),
                "target_added_at": format_unix_timestamp(
                    plan.existing_target_playlist.added_at if plan.existing_target_playlist else None
                ),
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
    plans: Sequence[PlaylistTransferPlan],
    mutations: Sequence[PlannedMutation],
    console_format: str,
    report_path: Optional[Path],
    report_format: str,
    report_writer: PlexReportWriter,
) -> None:
    rows = build_playlist_rows(plans)
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
                "source_added_at": plan.source_playlist.added_at,
                "source_added_at_display": format_unix_timestamp(plan.source_playlist.added_at),
                "target_added_at": plan.existing_target_playlist.added_at if plan.existing_target_playlist else None,
                "target_added_at_display": format_unix_timestamp(
                    plan.existing_target_playlist.added_at if plan.existing_target_playlist else None
                ),
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
        report_writer._write_csv_rows(sys.stdout, rows, PlexCliSupport.PLAYLIST_ROW_COLUMNS)
    else:
        report_writer.write_table_rows(sys.stdout, rows, PlexCliSupport.PLAYLIST_TABLE_COLUMNS)

    if report_path is None:
        return

    resolved_format = report_writer.resolve_report_format(report_path, report_format)
    if resolved_format == "json":
        with report_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
    elif resolved_format == "csv":
        with report_path.open("w", newline="", encoding="utf-8") as handle:
            report_writer._write_csv_rows(handle, rows, PlexCliSupport.PLAYLIST_ROW_COLUMNS)
    else:
        with report_path.open("w", encoding="utf-8") as handle:
            report_writer.write_table_rows(handle, rows, PlexCliSupport.PLAYLIST_TABLE_COLUMNS)


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


def populate_missing_playlist_transfer_args(args: Namespace) -> bool:
    needs_paths = args.source_path is None or args.target_path is None
    needs_playlist_selection = not args.playlist and getattr(sys.stdin, "isatty", lambda: False)()
    needs_target_account = args.target_account_id is None and getattr(sys.stdin, "isatty", lambda: False)()
    if not needs_paths and not needs_playlist_selection and not needs_target_account:
        return False

    print("Interactive playlist transfer setup")
    args.source_path = PlexCliSupport.prompt_with_default("Source Plex path", args.source_path)
    args.target_path = PlexCliSupport.prompt_with_default("Target Plex path", args.target_path)

    source_db_path = PlexDatabaseLocator.resolve_db_path(args.source_path, "source")
    target_db_path = PlexDatabaseLocator.resolve_db_path(args.target_path, "target")

    source_database = PlexDatabase(source_db_path, readonly=True)
    try:
        source_schema = source_database.inspect_schema()
        source_libraries = source_database.list_library_sections()
        args.source_library = PlexCliSupport.prompt_library_filters(
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
        args.target_library = PlexCliSupport.prompt_library_filters(
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
        args.target_account_id = PlexCliSupport.prompt_account_id(
            "Target accounts:",
            target_accounts,
            args.target_account_id,
        )

    if not args.include_empty_playlists:
        print("Empty playlists are excluded by default. Use --include-empty-playlists to include them.")

    args.playlist = PlexCliSupport.prompt_playlist_filters(
        "Select playlists to transfer:",
        playlists,
        args.playlist,
        args.include_empty_playlists,
    )

    selected_playlists = PlexCliSupport.resolve_playlist_selection(
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
        args.playlist_conflict_policy = PlexCliSupport.prompt_choice(
            "Choose playlist conflict policy:",
            ["unique", "merge", "replace", "skip"],
            args.playlist_conflict_policy,
        )
    return True


