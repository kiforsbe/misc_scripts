import json
import os
import sys
from argparse import Namespace, _SubParsersAction
from dataclasses import asdict
from datetime import datetime, timedelta
from pathlib import Path
import re
from typing import Any, Dict, List, Optional, Sequence, Set, TextIO, Tuple

from ..cli_support import PlexCliSupport
from ..infrastructure import PlexDatabase, PlexDatabaseLocator, PlexEnvironment, PlexFilenameParser
from ..models import MediaRecord, PlannedMutation, PlexPlaylist, TableColumnSpec
from ..planners import PlexMatcher, PlexPlaylistPlanner
from ..reporting import PlexReportWriter


SYNC_ROW_COLUMNS = (
    "playlist_id",
    "group_key",
    "source_playlist",
    "target_playlist",
    "target_account_id",
    "source_added_at",
    "target_added_at",
    "status",
    "action",
    "source_item_count",
    "matched_item_count",
    "transfer_item_count",
    "added_items",
    "existing_item_count",
    "unmatched_item_count",
    "notes",
    "unmatched_items",
)
SYNC_TABLE_DEFAULT_COLUMNS = (
    "target_playlist",
    "status",
    "action",
    "added_items",
    "matched_item_count",
    "unmatched_item_count",
    "notes",
)
SYNC_TABLE_MANDATORY_COLUMNS = (
    "status",
    "target_playlist",
)
SYNC_TABLE_COLUMN_ALIASES = {
    "matched": "matched_item_count",
    "unmatched": "unmatched_item_count",
}
SYNC_TABLE_COLUMN_WIDTHS = {
    "playlist_id": 6,
    "group_key": 40,
    "source_playlist": 32,
    "target_playlist": 32,
    "target_account_id": 8,
    "source_added_at": 19,
    "target_added_at": 19,
    "status": 20,
    "action": 18,
    "source_item_count": 10,
    "matched_item_count": 10,
    "transfer_item_count": 10,
    "added_items": 56,
    "existing_item_count": 10,
    "unmatched_item_count": 10,
    "notes": 48,
    "unmatched_items": 56,
}


def register(subparsers: _SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "sync-metadata-playlists",
        help="Create or update Plex playlists from grouped metadata JSON.",
        description=(
            "Create or update one Plex playlist per selected metadata group from JSON exports "
            "such as series_completeness_checker.py output."
        ),
    )
    parser.set_defaults(command="sync-metadata-playlists", command_handler=run)
    parser.add_argument(
        "--input-json",
        type=Path,
        required=True,
        help="Path to a grouped metadata JSON file, such as series_completeness_checker.py output.",
    )
    parser.add_argument(
        "--target-path",
        default=None,
        help="Path to the target Plex location or DB file.",
    )
    parser.add_argument(
        "--target-library",
        action="append",
        default=[],
        help=(
            "Target library section name to search for playlist items. Repeat to include multiple sections. "
            "Required in non-interactive mode."
        ),
    )
    parser.add_argument(
        "--target-account-id",
        type=int,
        default=None,
        help="Target account id to associate with created or updated playlists. Required in non-interactive mode.",
    )
    prefix_group = parser.add_mutually_exclusive_group()
    prefix_group.add_argument(
        "--playlist-prefix",
        default="",
        help="Optional text to prepend to each generated playlist name.",
    )
    prefix_group.add_argument(
        "--playlist-status-prefix",
        action="store_true",
        help="Automatically prefix each generated playlist name with the group's status, such as [Incomplete] or [Complete].",
    )
    suffix_group = parser.add_mutually_exclusive_group()
    suffix_group.add_argument(
        "--playlist-suffix",
        default="",
        help="Optional text to append to each generated playlist name.",
    )
    suffix_group.add_argument(
        "--playlist-status-suffix",
        action="store_true",
        help="Automatically append each generated playlist name with the group's status, such as [Incomplete] or [Complete].",
    )
    suffix_group.add_argument(
        "--playlist-complete-suffix",
        default="",
        help="Optional text to append when a group has all expected episodes available.",
    )
    parser.add_argument(
        "--playlist-conflict-policy",
        choices=["unique", "merge", "replace", "skip"],
        default="replace",
        help="How to handle target playlists that already exist with the same name.",
    )
    parser.add_argument(
        "--include-empty-playlists",
        action="store_true",
        help="Create or update playlists even when no group files match the selected target library.",
    )
    parser.add_argument(
        "--include-earlier-episodes",
        action="store_true",
        help=(
            "Allow updates to add episodes earlier than the first episode already present in an existing playlist. "
            "By default those earlier episodes are skipped."
        ),
    )
    parser.add_argument(
        "--restore-removed-playlists",
        action="store_true",
        help=(
            "Restore playlists that were previously created by sync-metadata-playlists and later removed. "
            "By default those removed playlists are not recreated."
        ),
    )
    parser.add_argument(
        "--restore-removed-playlist-items",
        action="store_true",
        help=(
            "Restore items that are missing from an existing synced playlist even when they appear to have been "
            "removed locally in Plex. By default those local removals are preserved."
        ),
    )
    parser.add_argument(
        "--status-filter",
        metavar="FILTERS",
        help=(
            "Filter groups by completeness and watch status. Use +status to include only specific statuses, "
            "-status to exclude specific statuses, or plain status names for exact match. "
            "Available completion statuses: complete, incomplete, complete_with_extras, no_episode_numbers, "
            "unknown_total_episodes, not_series, no_metadata, no_metadata_manager, unknown, movie. "
            "Available watch statuses: watched, watched_partial, unwatched."
        ),
    )
    parser.add_argument(
        "--modified",
        metavar="EXPR",
        help=(
            "Filter by modified datetime. Supports single expressions like '<2026-01-01' or '>=2026-01-01T12:00', "
            "closed ranges like '2026-01-01..2026-01-31', and combined conditions like '>=2026-01-01, <2026-02-01'."
        ),
    )
    parser.add_argument(
        "--episodes-found",
        metavar="EXPR",
        help=(
            "Filter by the number of episodes found in a group. Supports single expressions like '12' or '>=12', "
            "closed ranges like '12..24', and combined conditions like '>=12, <25'."
        ),
    )
    parser.add_argument(
        "--episodes-expected",
        metavar="EXPR",
        help=(
            "Filter by the expected episode count from metadata. Supports single expressions like '12' or '<=24', "
            "closed ranges like '12..24', and combined conditions like '>=12, <25'."
        ),
    )
    parser.add_argument(
        "--sort",
        action="store_true",
        help="Sort groups alphabetically by playlist title before planning mutations.",
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
        help="Console output format for playlist sync results.",
    )
    parser.add_argument(
        "--columns",
        default=None,
        help=(
            "Comma-separated column list for table output. Use column or column:width. "
            f"Mandatory columns are: {', '.join(SYNC_TABLE_MANDATORY_COLUMNS)}"
        ),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write the planned playlist mutations into the target DB. Without this flag the tool is dry-run only.",
    )
    parser.add_argument(
        "--one-of-one-playlist",
        default="",
        help=(
            "Optional playlist name that combines all selected groups whose original metadata reports "
            "episodes_expected=1 into one playlist."
        ),
    )
    parser.add_argument(
        "--virtual-playlist-watching",
        nargs="?",
        const="!Watching",
        default=None,
        metavar="NAME",
        help=(
            "Create a synthetic watching playlist from episodes whose MAL status is Watching. "
            "Defaults to '!Watching'; pass NAME to rename it."
        ),
    )


def run(args: Namespace) -> int:
    interactive_sync = populate_missing_sync_args(args)
    report_writer = PlexReportWriter()
    payload = load_group_payload(args.input_json)
    groups = normalize_groups(payload)
    groups = filter_groups(
        groups,
        status_filter=args.status_filter,
        modified=args.modified,
        episodes_found=args.episodes_found,
        episodes_expected=args.episodes_expected,
        sort_groups=args.sort,
    )
    groups = collapse_one_of_one_groups(groups, args.one_of_one_playlist)
    groups = add_virtual_watching_playlist(groups, args.virtual_playlist_watching)

    if not groups:
        print("No groups matched the selected metadata filters.")
        return 0

    target_db_path = resolve_target_db_path(args.target_path)
    if args.apply:
        PlexEnvironment.wait_for_plex_shutdown()

    database = PlexDatabase(target_db_path, readonly=not args.apply)
    try:
        schema = database.inspect_schema()
        if not schema.supports_playlists:
            raise RuntimeError("Target DB does not expose the playlist tables required for playlist creation.")

        target_inventory = database.build_media_inventory(schema, args.target_library)
        target_inventory_all = target_inventory if not args.target_library else database.build_media_inventory(schema, [])
        target_playlists = database.list_playlists(schema, target_inventory, target_inventory_all)
        deleted_metadata_playlists = database.list_deleted_metadata_playlists()
        target_account_id = args.target_account_id
        if target_account_id is None:
            raise RuntimeError(
                "Playlist sync requires --target-account-id. Pass it explicitly or use interactive mode to choose a target account."
            )

        plans, mutations = plan_group_playlists(
            groups,
            target_inventory,
            target_playlists,
            target_account_id,
            args.playlist_conflict_policy,
            args.include_empty_playlists,
            args.include_earlier_episodes,
            args.restore_removed_playlists,
            args.restore_removed_playlist_items,
            deleted_metadata_playlists,
            args.playlist_prefix,
            args.playlist_status_prefix,
            args.playlist_suffix,
            args.playlist_status_suffix,
            args.playlist_complete_suffix,
        )
        columns = report_writer.parse_columns(args.columns)

        if args.apply and mutations:
            database.begin_immediate()
            database.apply_mutations(mutations)
            database.commit()

        emit_sync_outputs(plans, mutations, args.console_format, args.report, args.report_format, report_writer, columns)
        summary_stream = sys.stderr if args.console_format in {"json", "csv"} else sys.stdout
        print_plan_summary(plans, mutations, args.apply, summary_stream)
        print_plan_unmatched_details(plans, summary_stream)

        if interactive_sync and not args.apply:
            print("Dry-run only: no playlist changes have been written yet.", file=summary_stream)
            should_apply = PlexCliSupport.prompt_yes_no(
                "Write these playlist changes to the target Plex DB now?",
                default=False,
            )
            if should_apply:
                PlexCliSupport.apply_planned_mutations(target_db_path, mutations)
                print_plan_summary(plans, mutations, True, summary_stream)
            else:
                print("No playlist changes were written.", file=summary_stream)

        report_writer.detach_redirected_stdout()
        return 0
    finally:
        database.close()


def populate_missing_sync_args(args: Namespace) -> bool:
    needs_target_library = not args.target_library
    needs_target_account = args.target_account_id is None
    is_interactive = getattr(sys.stdin, "isatty", lambda: False)()

    if not needs_target_library and not needs_target_account:
        return False
    if not is_interactive:
        missing: List[str] = []
        if needs_target_library:
            missing.append("--target-library")
        if needs_target_account:
            missing.append("--target-account-id")
        missing_values = ", ".join(missing)
        raise RuntimeError(
            f"Playlist sync requires {missing_values}. Pass them explicitly or use interactive mode to choose them."
        )

    print("Interactive metadata playlist sync setup")
    target_db_path = resolve_target_db_path(args.target_path)
    if args.target_path is None:
        args.target_path = str(target_db_path)

    database = PlexDatabase(target_db_path, readonly=True)
    try:
        libraries = database.list_library_sections()
        accounts = database.list_accounts()
    finally:
        database.close()

    if needs_target_library:
        args.target_library = PlexCliSupport.prompt_library_filters(
            "Target libraries:",
            libraries,
            args.target_library,
        )
    if not args.target_library:
        raise RuntimeError("Playlist sync requires at least one target library.")

    if args.target_account_id is None:
        inferred_account_id = None
        if len(accounts) == 1:
            inferred_account_id = accounts[0].id
        args.target_account_id = PlexCliSupport.prompt_account_id(
            "Target accounts:",
            accounts,
            inferred_account_id,
        )
    return True


def load_group_payload(json_path: Path) -> Dict[str, Any]:
    with json_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise RuntimeError(f"Metadata JSON must contain an object at the top level: {json_path}")
    groups = payload.get("groups")
    if not isinstance(groups, dict):
        raise RuntimeError(f"Metadata JSON is missing a 'groups' object: {json_path}")
    return payload


def resolve_target_db_path(path_value: Optional[str]) -> Path:
    if path_value:
        return PlexDatabaseLocator.resolve_db_path(path_value, "target")

    local_app_data = os.environ.get("LOCALAPPDATA")
    if local_app_data:
        default_plex_root = Path(local_app_data) / "Plex Media Server"
        if default_plex_root.exists():
            return PlexDatabaseLocator.resolve_db_path(str(default_plex_root), "target")

    raise RuntimeError(
        "Target Plex path is required when the standard LOCALAPPDATA Plex Media Server folder is not available. "
        "Pass --target-path explicitly."
    )


def normalize_groups(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for group_key, raw_group in payload.get("groups", {}).items():
        normalized_group = normalize_group(str(group_key), raw_group)
        if normalized_group is not None:
            normalized.append(normalized_group)
    return normalized


def normalize_group(group_key: str, raw_group: Any) -> Optional[Dict[str, Any]]:
    if isinstance(raw_group, dict):
        group_data = dict(raw_group)
        files = group_data.get("files", [])
        if not isinstance(files, list):
            files = []
    elif isinstance(raw_group, list):
        files = list(raw_group)
        group_data = {"files": files}
    else:
        return None

    first_file = files[0] if files and isinstance(files[0], dict) else {}
    title = str(group_data.get("title") or first_file.get("title") or group_key)
    season = safe_int(group_data.get("season"))
    if season is None:
        season = safe_int(first_file.get("season"))
    playlist_name = f"{title} S{season:02d}" if season else title

    status = str(group_data.get("status") or "unknown")
    first_file_type = str(first_file.get("type", "")).lower()
    metadata_type = str(group_data.get("type", "")).lower()
    if "movie" in first_file_type or "movie" in metadata_type:
        status = "movie"

    episodes_found = safe_int(group_data.get("episodes_found"))
    if episodes_found is None:
        episodes_found = count_episode_like_files(files)
    raw_episodes_expected = safe_int(group_data.get("episodes_expected")) or 0
    episodes_expected = raw_episodes_expected

    return {
        "group_key": group_key,
        "group_data": group_data,
        "files": [file_info for file_info in files if isinstance(file_info, dict)],
        "title": title,
        "playlist_name": playlist_name,
        "season": season,
        "status": status,
        "episodes_found": episodes_found,
        "episodes_expected": episodes_expected,
        "raw_episodes_expected": raw_episodes_expected,
        "watch_status": classify_watch_status(group_data, files, status),
        "modified_at": get_group_modified_datetime(group_data, files),
    }


def collapse_one_of_one_groups(groups: Sequence[Dict[str, Any]], playlist_name: str) -> List[Dict[str, Any]]:
    resolved_name = playlist_name.strip()
    if not resolved_name:
        return list(groups)

    selected = [group for group in groups if is_one_of_one_group(group)]
    if not selected:
        return list(groups)

    combined_group = build_one_of_one_collection_group(selected, resolved_name)
    collapsed: List[Dict[str, Any]] = []
    inserted = False
    for group in groups:
        if is_one_of_one_group(group):
            if not inserted:
                collapsed.append(combined_group)
                inserted = True
            continue
        collapsed.append(group)
    return collapsed


def is_one_of_one_group(group: Dict[str, Any]) -> bool:
    if (safe_int(group.get("raw_episodes_expected")) or 0) != 1:
        return False
    if (safe_int(group.get("episodes_found")) or 0) != 1:
        return False
    return not group_has_episode_markers(group)


def add_virtual_watching_playlist(groups: Sequence[Dict[str, Any]], playlist_name: Optional[str]) -> List[Dict[str, Any]]:
    if not playlist_name:
        return list(groups)

    watching_files = collect_watching_episode_files(groups)
    if not watching_files:
        return list(groups)

    watching_group = build_watching_collection_group(watching_files, groups, playlist_name)
    return [*list(groups), watching_group]


def collect_watching_episode_files(groups: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    watching_files: List[Dict[str, Any]] = []
    seen_keys: Set[str] = set()

    for group in groups:
        for file_info in group.get("files", []):
            mal_status = file_info.get("myanimelist_watch_status") or (group.get("group_data") or {}).get("myanimelist_watch_status")
            if not is_watching_mal_status(mal_status) or is_episode_already_watched(file_info, group):
                continue

            candidate_key = build_episode_identity_key(file_info)
            if not candidate_key or candidate_key in seen_keys:
                continue
            seen_keys.add(candidate_key)
            watching_files.append(dict(file_info))

    return sort_episode_files_for_playlist(watching_files)


def is_watching_mal_status(status: Any) -> bool:
    if not isinstance(status, dict):
        return False
    my_status = str(status.get("my_status") or "").strip().casefold()
    return my_status in {"watching", "watching (season)", "watching_season"}


def is_episode_already_watched(file_info: Dict[str, Any], group: Dict[str, Any]) -> bool:
    if file_info.get("episode_watched"):
        return True

    plex_status = file_info.get("plex_watch_status") or {}
    if plex_status.get("watched") or plex_status.get("view_offset", 0) > 0:
        return True

    mal_status = file_info.get("myanimelist_watch_status") or (group.get("group_data") or {}).get("myanimelist_watch_status")
    if not isinstance(mal_status, dict):
        return False

    watched_episodes = safe_int(mal_status.get("my_watched_episodes"))
    episode_number = safe_int(file_info.get("episode"))
    if watched_episodes is None or episode_number is None:
        return False
    return episode_number <= watched_episodes


def build_episode_identity_key(file_info: Dict[str, Any]) -> Optional[str]:
    source_path = str(file_info.get("filepath") or file_info.get("file_path") or "")
    metadata_item_id = safe_int(file_info.get("metadata_item_id"))
    if metadata_item_id is not None:
        return f"metadata:{metadata_item_id}"
    normalized_path = normalize_path_key(source_path)
    if normalized_path:
        return f"path:{normalized_path}"
    basename = str(file_info.get("filename") or Path(source_path).name or "")
    season = safe_int(file_info.get("season")) or 0
    episode = safe_int(file_info.get("episode")) or 0
    return f"fallback:{basename}:{season}:{episode}"


def sort_episode_files_for_playlist(files: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def sort_key(file_info: Dict[str, Any]) -> Tuple[Any, ...]:
        source_path = str(file_info.get("filepath") or file_info.get("file_path") or "")
        created_time = resolve_file_created_time(file_info)
        display_title = str(
            file_info.get("episode_title")
            or file_info.get("title")
            or file_info.get("filename")
            or Path(source_path).name
            or ""
        ).casefold()
        season = safe_int(file_info.get("season")) or 0
        episode = safe_int(file_info.get("episode")) or 0
        return (
            created_time if created_time is not None else datetime.max,
            season,
            episode,
            display_title,
            normalize_path_key(source_path),
        )

    return sorted(files, key=sort_key)


def resolve_file_created_time(file_info: Dict[str, Any]) -> Optional[datetime]:
    created_time = file_info.get("created_time")
    if isinstance(created_time, (int, float)):
        try:
            return datetime.fromtimestamp(float(created_time))
        except (ValueError, OSError, OverflowError):
            pass

    source_path = str(file_info.get("filepath") or file_info.get("file_path") or "")
    if source_path and os.path.exists(source_path):
        try:
            return datetime.fromtimestamp(os.path.getctime(source_path))
        except (OSError, ValueError, OverflowError):
            pass

    modified_time = file_info.get("modified_time")
    if isinstance(modified_time, (int, float)):
        try:
            return datetime.fromtimestamp(float(modified_time))
        except (ValueError, OSError, OverflowError):
            return None

    if source_path and os.path.exists(source_path):
        try:
            return datetime.fromtimestamp(os.path.getmtime(source_path))
        except (OSError, ValueError, OverflowError):
            return None
    return None


def build_watching_collection_group(
    files: Sequence[Dict[str, Any]],
    groups: Sequence[Dict[str, Any]],
    playlist_name: str = "!Watching",
) -> Dict[str, Any]:
    sorted_files = sort_episode_files_for_playlist(files)
    modified_candidates = [candidate for candidate in (resolve_file_created_time(file_info) for file_info in sorted_files) if candidate is not None]
    source_group_keys = []
    source_playlists = []
    for group in groups:
        source_group_keys.append(str(group.get("group_key") or ""))
        source_playlists.append(str(group.get("playlist_name") or group.get("title") or group.get("group_key") or ""))

    resolved_name = (playlist_name or "!Watching").strip() or "!Watching"

    return {
        "group_key": "collection:watching",
        "group_data": {
            "source_group_keys": source_group_keys,
            "source_playlists": source_playlists,
            "group_count": len(source_group_keys),
            "myanimelist_watch_status": {"my_status": "Watching"},
        },
        "files": list(sorted_files),
        "title": resolved_name,
        "playlist_name": resolved_name,
        "season": None,
        "status": "watching",
        "episodes_found": len(sorted_files),
        "episodes_expected": len(sorted_files),
        "raw_episodes_expected": len(sorted_files),
        "watch_status": "unwatched",
        "modified_at": max(modified_candidates) if modified_candidates else None,
    }


def build_one_of_one_collection_group(groups: Sequence[Dict[str, Any]], playlist_name: str) -> Dict[str, Any]:
    files: List[Dict[str, Any]] = []
    source_group_keys: List[str] = []
    source_playlists: List[str] = []
    modified_candidates: List[datetime] = []
    watch_statuses: Set[str] = set()
    status_values: Set[str] = set()
    total_found = 0
    total_expected = 0

    for group in groups:
        files.extend(group.get("files", []))
        source_group_keys.append(str(group.get("group_key") or ""))
        source_playlists.append(str(group.get("playlist_name") or group.get("title") or group.get("group_key") or ""))
        modified_at = group.get("modified_at")
        if isinstance(modified_at, datetime):
            modified_candidates.append(modified_at)
        watch_statuses.add(str(group.get("watch_status") or "unwatched"))
        status_values.add(str(group.get("status") or "unknown"))
        total_found += safe_int(group.get("episodes_found")) or 0
        total_expected += safe_int(group.get("raw_episodes_expected")) or 0

    if watch_statuses == {"watched"}:
        watch_status = "watched"
    elif "watched_partial" in watch_statuses or ("watched" in watch_statuses and "unwatched" in watch_statuses):
        watch_status = "watched_partial"
    else:
        watch_status = "unwatched"

    if len(status_values) == 1:
        status = next(iter(status_values))
    else:
        status = "unknown"

    return {
        "group_key": "collection:one_of_one",
        "group_data": {
            "source_group_keys": source_group_keys,
            "source_playlists": source_playlists,
            "group_count": len(source_group_keys),
        },
        "files": files,
        "title": playlist_name,
        "playlist_name": playlist_name,
        "season": None,
        "status": status,
        "episodes_found": total_found,
        "episodes_expected": total_expected,
        "raw_episodes_expected": total_expected,
        "watch_status": watch_status,
        "modified_at": max(modified_candidates) if modified_candidates else None,
    }


def group_has_episode_markers(group: Dict[str, Any]) -> bool:
    if safe_int(group.get("season")) is not None:
        return True

    group_data = group.get("group_data") or {}
    if safe_int(group_data.get("season")) is not None:
        return True

    for file_info in group.get("files", []):
        if safe_int(file_info.get("season")) is not None:
            return True

        episode_value = file_info.get("episode")
        if isinstance(episode_value, list):
            if any(safe_int(value) is not None for value in episode_value):
                return True
        elif safe_int(episode_value) is not None:
            return True

        file_type = str(file_info.get("type") or "").casefold()
        if file_type in {"episode", "tv episode"}:
            return True

    return False


def count_episode_like_files(files: Sequence[Dict[str, Any]]) -> int:
    episode_count = 0
    for file_info in files:
        episode_value = file_info.get("episode")
        if isinstance(episode_value, list):
            episode_count += len([value for value in episode_value if value is not None])
        elif episode_value is not None:
            episode_count += 1
        else:
            episode_count += 1
    return episode_count


def classify_watch_status(group_data: Dict[str, Any], files: Sequence[Dict[str, Any]], status: str) -> str:
    if status == "movie":
        for file_info in files:
            if file_info.get("episode_watched"):
                return "watched"
            plex_status = file_info.get("plex_watch_status") or {}
            if plex_status.get("view_offset", 0) > 0:
                return "watched_partial"
        return "unwatched"

    watch_status = group_data.get("watch_status") or {}
    watched_episodes = safe_int(watch_status.get("watched_episodes")) or 0
    partially_watched_episodes = safe_int(watch_status.get("partially_watched_episodes")) or 0
    episodes_found = safe_int(group_data.get("episodes_found"))
    if episodes_found is None:
        episodes_found = count_episode_like_files(files)

    if episodes_found <= 0:
        return "unwatched"
    if watched_episodes == episodes_found:
        return "watched"
    if watched_episodes > 0 or partially_watched_episodes > 0:
        return "watched_partial"
    return "unwatched"


def safe_int(value: Any) -> Optional[int]:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def normalize_groups_for_filter(groups: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [dict(group) for group in groups]


def filter_groups(
    groups: Sequence[Dict[str, Any]],
    status_filter: Optional[str],
    modified: Optional[str],
    episodes_found: Optional[str],
    episodes_expected: Optional[str],
    sort_groups: bool,
) -> List[Dict[str, Any]]:
    filtered = normalize_groups_for_filter(groups)

    if status_filter:
        filtered = apply_status_filter(filtered, status_filter)
    if modified:
        conditions = parse_modified_conditions(modified)
        filtered = [
            group for group in filtered
            if group["modified_at"] is not None
            and all(matches_modified_expression(group["modified_at"], operator, target_value, is_date_only) for operator, target_value, is_date_only in conditions)
        ]
    if episodes_found:
        conditions = parse_numeric_conditions(episodes_found, "--episodes-found")
        filtered = [
            group for group in filtered
            if all(matches_numeric_expression(int(group["episodes_found"] or 0), operator, target_value) for operator, target_value in conditions)
        ]
    if episodes_expected:
        conditions = parse_numeric_conditions(episodes_expected, "--episodes-expected")
        filtered = [
            group for group in filtered
            if all(matches_numeric_expression(int(group["episodes_expected"] or 0), operator, target_value) for operator, target_value in conditions)
        ]
    if sort_groups:
        filtered.sort(key=lambda group: str(group["playlist_name"]).casefold())
    return filtered


def apply_status_filter(groups: Sequence[Dict[str, Any]], status_filter: str) -> List[Dict[str, Any]]:
    all_statuses = {
        "complete",
        "incomplete",
        "complete_with_extras",
        "no_episode_numbers",
        "unknown_total_episodes",
        "not_series",
        "no_metadata",
        "no_metadata_manager",
        "unknown",
        "movie",
    }
    all_watch_statuses = {"watched", "watched_partial", "unwatched"}

    include_statuses: Set[str] = set()
    exclude_statuses: Set[str] = set()
    plain_statuses: Set[str] = set()
    include_watch_statuses: Set[str] = set()
    exclude_watch_statuses: Set[str] = set()
    plain_watch_statuses: Set[str] = set()

    for filter_item in status_filter.split():
        if filter_item.startswith("+"):
            status = filter_item[1:]
            if status in all_statuses:
                include_statuses.add(status)
            elif status in all_watch_statuses:
                include_watch_statuses.add(status)
        elif filter_item.startswith("-"):
            status = filter_item[1:]
            if status in all_statuses:
                exclude_statuses.add(status)
            elif status in all_watch_statuses:
                exclude_watch_statuses.add(status)
        elif filter_item in all_statuses:
            plain_statuses.add(filter_item)
        elif filter_item in all_watch_statuses:
            plain_watch_statuses.add(filter_item)

    if plain_statuses:
        final_statuses = plain_statuses
    elif include_statuses:
        final_statuses = include_statuses - exclude_statuses
    elif exclude_statuses:
        final_statuses = all_statuses - exclude_statuses
    else:
        final_statuses = all_statuses

    if plain_watch_statuses:
        final_watch_statuses = plain_watch_statuses
    elif include_watch_statuses:
        final_watch_statuses = include_watch_statuses - exclude_watch_statuses
    elif exclude_watch_statuses:
        final_watch_statuses = all_watch_statuses - exclude_watch_statuses
    else:
        final_watch_statuses = all_watch_statuses

    return [
        group for group in groups
        if group["status"] in final_statuses and group["watch_status"] in final_watch_statuses
    ]


def get_group_modified_datetime(group_data: Dict[str, Any], files: Sequence[Dict[str, Any]]) -> Optional[datetime]:
    group_metadata = group_data.get("group_metadata") or {}
    avg_modified_time = group_metadata.get("avg_modified_time")
    if isinstance(avg_modified_time, (int, float)):
        try:
            return datetime.fromtimestamp(avg_modified_time)
        except (ValueError, OSError, OverflowError):
            pass

    newest_mtime: Optional[float] = None
    for file_info in files:
        modified_time = file_info.get("modified_time")
        if isinstance(modified_time, (int, float)):
            file_mtime = float(modified_time)
        else:
            source_path = file_info.get("filepath") or file_info.get("file_path")
            if not source_path or not os.path.exists(source_path):
                continue
            try:
                file_mtime = os.path.getmtime(source_path)
            except OSError:
                continue
        if newest_mtime is None or file_mtime > newest_mtime:
            newest_mtime = file_mtime

    if newest_mtime is None:
        return None
    return datetime.fromtimestamp(newest_mtime)


def normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is not None:
        return value.astimezone().replace(tzinfo=None)
    return value


def parse_smart_datetime(value: str) -> Tuple[Optional[datetime], bool]:
    text = (value or "").strip()
    if not text:
        return None, False

    lowered = text.lower()
    now = datetime.now()
    if lowered == "now":
        return now, False
    if lowered == "today":
        return datetime(now.year, now.month, now.day), True
    if lowered == "yesterday":
        today = datetime(now.year, now.month, now.day)
        return today - timedelta(days=1), True
    if lowered == "tomorrow":
        today = datetime(now.year, now.month, now.day)
        return today + timedelta(days=1), True

    is_date_only = bool(__import__("re").fullmatch(r"\d{4}-\d{2}-\d{2}", text))
    iso_candidate = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(iso_candidate)
        return normalize_datetime(parsed), is_date_only
    except ValueError:
        pass

    formats = [
        "%Y/%m/%d",
        "%Y.%m.%d",
        "%Y%m%d",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%Y/%m/%d %H:%M:%S",
        "%d.%m.%Y",
        "%d.%m.%Y %H:%M",
        "%d.%m.%Y %H:%M:%S",
    ]
    for fmt in formats:
        try:
            parsed = datetime.strptime(text, fmt)
            date_only = fmt in ("%Y/%m/%d", "%Y.%m.%d", "%Y%m%d", "%d.%m.%Y")
            return parsed, date_only
        except ValueError:
            continue

    if __import__("re").fullmatch(r"\d{10,13}", text):
        try:
            timestamp = int(text)
            if len(text) == 13:
                timestamp = timestamp / 1000
            return datetime.fromtimestamp(timestamp), False
        except (ValueError, OSError):
            pass
    return None, False


def parse_modified_expression(expression: str) -> Tuple[str, datetime, bool]:
    import re

    expr = (expression or "").strip()
    match = re.match(r"^(<=|>=|<|>|==|=|!=)\s*(.+)$", expr)
    if match:
        operator = match.group(1)
        raw_value = match.group(2).strip()
    else:
        operator = "="
        raw_value = expr

    parsed_dt, is_date_only = parse_smart_datetime(raw_value)
    if parsed_dt is None:
        raise ValueError(
            f"Invalid --modified expression '{expression}'. Use forms like '<2026-01-01' or '>=2026-01-01T15:30'."
        )
    return operator, parsed_dt, is_date_only


def parse_modified_conditions(expression: str) -> List[Tuple[str, datetime, bool]]:
    expr = (expression or "").strip()
    if not expr:
        raise ValueError("--modified cannot be empty")
    if ".." in expr:
        parts = expr.split("..")
        if len(parts) != 2 or not parts[0].strip() or not parts[1].strip():
            raise ValueError(f"Invalid --modified range '{expression}'. Use format like '2026-01-01..2026-01-31'.")
        start_condition = parse_modified_expression(f">={parts[0].strip()}")
        end_condition = parse_modified_expression(f"<={parts[1].strip()}")
        if normalize_datetime(start_condition[1]) > normalize_datetime(end_condition[1]):
            raise ValueError(f"Invalid --modified range '{expression}'. Range start must be earlier than or equal to range end.")
        return [start_condition, end_condition]
    if "," in expr:
        parts = [part.strip() for part in expr.split(",") if part.strip()]
        if not parts:
            raise ValueError("--modified cannot be empty")
        return [parse_modified_expression(part) for part in parts]
    return [parse_modified_expression(expr)]


def matches_modified_expression(actual: datetime, operator: str, target_value: datetime, is_date_only: bool) -> bool:
    normalized_actual = normalize_datetime(actual)
    normalized_target = normalize_datetime(target_value)
    if is_date_only and operator in ("=", "==", "!="):
        is_equal = normalized_actual.date() == normalized_target.date()
        return (not is_equal) if operator == "!=" else is_equal
    if operator in ("=", "=="):
        return normalized_actual == normalized_target
    if operator == "!=":
        return normalized_actual != normalized_target
    if operator == "<":
        return normalized_actual < normalized_target
    if operator == "<=":
        return normalized_actual <= normalized_target
    if operator == ">":
        return normalized_actual > normalized_target
    if operator == ">=":
        return normalized_actual >= normalized_target
    return False


def parse_numeric_expression(expression: str, argument_name: str) -> Tuple[str, int]:
    import re

    expr = (expression or "").strip()
    match = re.match(r"^(<=|>=|<|>|==|=|!=)\s*(.+)$", expr)
    if match:
        operator = match.group(1)
        raw_value = match.group(2).strip()
    else:
        operator = "="
        raw_value = expr
    if not re.fullmatch(r"-?\d+", raw_value):
        raise ValueError(
            f"Invalid {argument_name} expression '{expression}'. Use integer values like '<12', '>=24', or '=13'."
        )
    return operator, int(raw_value)


def parse_numeric_conditions(expression: str, argument_name: str) -> List[Tuple[str, int]]:
    expr = (expression or "").strip()
    if not expr:
        raise ValueError(f"{argument_name} cannot be empty")
    if ".." in expr:
        parts = expr.split("..")
        if len(parts) != 2 or not parts[0].strip() or not parts[1].strip():
            raise ValueError(f"Invalid {argument_name} range '{expression}'. Use format like '12..24'.")
        start_condition = parse_numeric_expression(f">={parts[0].strip()}", argument_name)
        end_condition = parse_numeric_expression(f"<={parts[1].strip()}", argument_name)
        if start_condition[1] > end_condition[1]:
            raise ValueError(f"Invalid {argument_name} range '{expression}'. Range start must be less than or equal to range end.")
        return [start_condition, end_condition]
    if "," in expr:
        parts = [part.strip() for part in expr.split(",") if part.strip()]
        if not parts:
            raise ValueError(f"{argument_name} cannot be empty")
        return [parse_numeric_expression(part, argument_name) for part in parts]
    return [parse_numeric_expression(expr, argument_name)]


def matches_numeric_expression(value: int, operator: str, target_value: int) -> bool:
    if operator in ("=", "=="):
        return value == target_value
    if operator == "!=":
        return value != target_value
    if operator == "<":
        return value < target_value
    if operator == "<=":
        return value <= target_value
    if operator == ">":
        return value > target_value
    if operator == ">=":
        return value >= target_value
    return False


def plan_group_playlists(
    groups: Sequence[Dict[str, Any]],
    target_inventory: Sequence[MediaRecord],
    target_playlists: Sequence[PlexPlaylist],
    target_account_id: int,
    conflict_policy: str,
    include_empty_playlists: bool,
    include_earlier_episodes: bool,
    restore_removed_playlists: bool,
    restore_removed_playlist_items: bool,
    deleted_metadata_playlists: Sequence[PlexPlaylist],
    playlist_prefix: str,
    playlist_status_prefix: bool,
    playlist_suffix: str,
    playlist_status_suffix: bool,
    playlist_complete_suffix: str,
) -> Tuple[List[Dict[str, Any]], List[PlannedMutation]]:
    matcher = PlexMatcher("balanced", 0.65)
    target_indexes = matcher.index_target_inventory(target_inventory)
    path_index = build_path_index(target_inventory)
    reserved_names = [playlist.name for playlist in target_playlists]
    deleted_sync_group_keys = {
        group_key
        for group_key in (extract_sync_group_key(playlist.description) for playlist in deleted_metadata_playlists)
        if group_key
    }
    plans: List[Dict[str, Any]] = []
    mutations: List[PlannedMutation] = []

    for group in groups:
        desired_name = build_playlist_name(
            group["playlist_name"],
            playlist_prefix,
            playlist_status_prefix,
            str(group.get("status") or "unknown"),
            playlist_suffix,
            playlist_status_suffix,
            playlist_complete_suffix,
            group_has_all_expected_episodes(group),
        )
        playlist_name = desired_name
        existing_playlist, stale_playlists = choose_existing_sync_playlist(
            target_playlists,
            desired_name,
            group["group_key"],
            target_account_id,
        )
        existing_playlist_id = existing_playlist.id if existing_playlist else None
        if (
            playlist_name.casefold() in {name.casefold() for name in reserved_names if existing_playlist is None or name.casefold() != existing_playlist.name.casefold()}
            and conflict_policy == "unique"
        ):
            playlist_name = PlexPlaylistPlanner.resolve_unique_name(reserved_names, desired_name)

        matched_records, unmatched_files = resolve_group_metadata_item_ids(group, target_indexes, path_index, matcher)

        skipped_earlier_records: List[MediaRecord] = []
        sync_records = list(matched_records)
        if existing_playlist is not None and not include_earlier_episodes:
            sync_records, skipped_earlier_records = filter_earlier_sync_records(existing_playlist, matched_records)

        existing_ids_ordered: List[int] = []
        append_records: List[MediaRecord] = []
        locally_removed_records: List[MediaRecord] = []
        if existing_playlist is not None:
            existing_ids_ordered = playlist_media_ids(existing_playlist)
            sync_records, append_records, locally_removed_records = plan_playlist_item_updates(
                existing_playlist,
                sync_records,
                restore_removed_playlist_items,
            )
        metadata_ids = [record.metadata_item_id for record in sync_records]

        for stale_playlist in stale_playlists:
            mutations.append(
                PlannedMutation(
                    action="delete_playlist",
                    target_guid=f"playlist:{stale_playlist.name}:{stale_playlist.id}",
                    details={
                        "playlist_id": stale_playlist.id,
                        "play_queue_id": stale_playlist.play_queue_id,
                        "storage_model": stale_playlist.storage_model,
                    },
                )
            )

        added_at = int(group["modified_at"].timestamp()) if group["modified_at"] is not None else None
        description = build_playlist_description(group)
        mutation_annotations = build_sync_mutation_annotations(group)
        status = "ready_create"
        action = "create_new"
        notes: List[str] = []
        existing_item_count = len(existing_playlist.items) if existing_playlist else 0
        added_records: List[MediaRecord] = []
        group_count = safe_int((group.get("group_data") or {}).get("group_count")) or 0
        if group_count > 1:
            notes.append(f"combined groups: {group_count}")
        if unmatched_files:
            notes.append(f"unmatched files: {len(unmatched_files)}")
        if skipped_earlier_records:
            notes.append(f"skipped earlier episodes: {format_media_record_labels(skipped_earlier_records)}")
        if locally_removed_records:
            notes.append(
                "local db removals preserved: "
                + format_media_record_labels(locally_removed_records)
                + " (pass --restore-removed-playlist-items to restore them)"
            )
        if stale_playlists:
            notes.append(f"stale null-account duplicates: {len(stale_playlists)}")

        rename_note = determine_rename_note(target_playlists, existing_playlist, desired_name, playlist_name, conflict_policy)
        if rename_note is not None:
            notes.append(rename_note)

        deleted_sync_playlist = existing_playlist is None and group["group_key"] in deleted_sync_group_keys
        if deleted_sync_playlist and not restore_removed_playlists:
            notes.append("playlist was previously removed; pass --restore-removed-playlists to recreate it")

        if existing_playlist is not None and should_update_existing_playlist_metadata(existing_playlist, playlist_name, description):
            resolved_playlist_name = resolve_updated_playlist_name(
                target_playlists,
                existing_playlist,
                desired_name,
                playlist_name,
                conflict_policy,
            )
            if resolved_playlist_name is not None:
                playlist_name = resolved_playlist_name
                mutations.append(
                    PlannedMutation(
                        action="update_playlist_metadata",
                        target_guid=f"playlist:{existing_playlist.id}",
                        details={
                            "playlist_id": existing_playlist.id,
                            "storage_model": existing_playlist.storage_model,
                            "name": playlist_name,
                            "description": description,
                            **mutation_annotations,
                        },
                    )
                )

        if not metadata_ids and not include_empty_playlists:
            status = "no_transferable_items"
            action = "skip_unmatched"
        elif deleted_sync_playlist and not restore_removed_playlists:
            status = "skipped_removed"
            action = "skip_removed"
        elif existing_playlist is None:
            added_records = list(sync_records)
            mutations.append(
                PlannedMutation(
                    action="create_playlist",
                    target_guid=f"playlist:{playlist_name}",
                    details={
                        "name": playlist_name,
                        "description": description,
                        "added_at": added_at,
                        "account_id": target_account_id,
                        "metadata_item_ids": metadata_ids,
                        "storage_model": "metadata",
                        **mutation_annotations,
                    },
                )
            )
        elif conflict_policy == "skip":
            status = "skipped_conflict"
            action = "skip_existing"
        elif conflict_policy == "unique":
            added_records = list(sync_records)
            mutations.append(
                PlannedMutation(
                    action="create_playlist",
                    target_guid=f"playlist:{playlist_name}",
                    details={
                        "name": playlist_name,
                        "description": description,
                        "added_at": added_at,
                        "account_id": target_account_id,
                        "metadata_item_ids": metadata_ids,
                        "storage_model": "metadata",
                        **mutation_annotations,
                    },
                )
            )
            status = "ready_unique"
            action = "create_unique"
        elif conflict_policy == "merge":
            added_records = list(append_records)
            new_ids = [record.metadata_item_id for record in added_records]
            if new_ids:
                mutations.append(
                    PlannedMutation(
                        action="merge_playlist_items",
                        target_guid=f"playlist:{existing_playlist.name}:{existing_playlist.id}",
                        details={
                            "playlist_id": existing_playlist.id,
                            "play_queue_id": existing_playlist.play_queue_id,
                            "storage_model": existing_playlist.storage_model,
                            "metadata_item_ids": new_ids,
                            **mutation_annotations,
                        },
                    )
                )
                notes.append(f"new playlist items: {len(new_ids)}")
                status = "ready_merge"
                action = "merge_existing"
            else:
                if locally_removed_records:
                    status = "local_change_noop"
                    action = "no_change_local"
                else:
                    status = "already_synced"
                    action = "no_change"
        else:
            added_records = list(append_records)
            if existing_ids_ordered == metadata_ids:
                if locally_removed_records:
                    status = "local_change_noop"
                    action = "no_change_local"
                else:
                    status = "already_synced"
                    action = "no_change"
            elif is_append_only_update(existing_ids_ordered, metadata_ids):
                new_ids = [record.metadata_item_id for record in added_records]
                mutations.append(
                    PlannedMutation(
                        action="merge_playlist_items",
                        target_guid=f"playlist:{existing_playlist.name}:{existing_playlist.id}",
                        details={
                            "playlist_id": existing_playlist.id,
                            "play_queue_id": existing_playlist.play_queue_id,
                            "storage_model": existing_playlist.storage_model,
                            "metadata_item_ids": new_ids,
                            **mutation_annotations,
                        },
                    )
                )
                notes.append(f"new playlist items: {len(new_ids)}")
                status = "ready_merge"
                action = "append_existing"
            else:
                mutations.append(
                    PlannedMutation(
                        action="replace_playlist_items",
                        target_guid=f"playlist:{existing_playlist.name}:{existing_playlist.id}",
                        details={
                            "playlist_id": existing_playlist.id,
                            "play_queue_id": existing_playlist.play_queue_id,
                            "storage_model": existing_playlist.storage_model,
                            "added_at": added_at,
                            "metadata_item_ids": metadata_ids,
                            **mutation_annotations,
                        },
                    )
                )
                status = "ready_replace"
                action = "replace_existing"

        plans.append(
            {
                "playlist_id": existing_playlist.id if existing_playlist else None,
                "playlist_name": playlist_name,
                "desired_name": desired_name,
                "group_key": group["group_key"],
                "source_playlist": group["playlist_name"],
                "target_playlist": playlist_name,
                "target_account_id": target_account_id,
                "source_added_at": format_unix_timestamp(added_at),
                "target_added_at": format_unix_timestamp(existing_playlist.added_at if existing_playlist else None),
                "status": status,
                "action": action,
                "source_item_count": len(group["files"]),
                "matched_item_count": len(metadata_ids),
                "transfer_item_count": len(metadata_ids),
                "added_items": format_media_record_labels(added_records),
                "added_item_labels": list_media_record_labels(added_records),
                "existing_item_count": existing_item_count,
                "source_file_count": len(group["files"]),
                "unmatched_file_count": len(unmatched_files),
                "unmatched_item_count": len(unmatched_files),
                "unmatched_files": unmatched_files,
                "unmatched_items": format_unmatched_items(unmatched_files),
                "existing_playlist_id": existing_playlist_id,
                "notes": notes,
            }
        )
        reserved_names.append(playlist_name)

    removed_playlists = find_removed_sync_playlists(target_playlists, groups, target_account_id)
    for removed_playlist in removed_playlists:
        group_key = extract_sync_group_key(removed_playlist.description)
        mutations.append(
            PlannedMutation(
                action="delete_playlist",
                target_guid=f"playlist:{removed_playlist.name}:{removed_playlist.id}",
                details={
                    "playlist_id": removed_playlist.id,
                    "play_queue_id": removed_playlist.play_queue_id,
                    "storage_model": removed_playlist.storage_model,
                },
            )
        )
        plans.append(
            {
                "playlist_id": removed_playlist.id,
                "playlist_name": removed_playlist.name,
                "desired_name": removed_playlist.name,
                "group_key": group_key,
                "source_playlist": removed_playlist.name,
                "target_playlist": removed_playlist.name,
                "target_account_id": removed_playlist.account_id,
                "source_added_at": "",
                "target_added_at": format_unix_timestamp(removed_playlist.added_at),
                "status": "ready_delete",
                "action": "delete_missing",
                "source_item_count": 0,
                "matched_item_count": 0,
                "transfer_item_count": 0,
                "added_items": "",
                "added_item_labels": [],
                "existing_item_count": len(removed_playlist.items),
                "source_file_count": 0,
                "unmatched_file_count": 0,
                "unmatched_item_count": 0,
                "unmatched_files": [],
                "unmatched_items": "",
                "existing_playlist_id": removed_playlist.id,
                "notes": ["group removed from current metadata selection"],
            }
        )

    return plans, mutations


def build_playlist_description(group: Dict[str, Any]) -> str:
    pieces = [f"status={group['status']}", f"watch={group['watch_status']}"]
    pieces.append(f"episodes_found={group['episodes_found']}")
    if int(group["episodes_expected"] or 0) > 0:
        pieces.append(f"episodes_expected={group['episodes_expected']}")
    group_data = group.get("group_data") or {}
    group_count = safe_int(group_data.get("group_count"))
    if group_count and group_count > 1:
        pieces.append(f"combined_groups={group_count}")
    pieces.append(f"group_key={group['group_key']}")
    return "Generated from metadata JSON: " + ", ".join(pieces)


def build_sync_mutation_annotations(group: Dict[str, Any]) -> Dict[str, Any]:
    group_data = group.get("group_data") or {}
    source_group_keys = [
        str(value)
        for value in group_data.get("source_group_keys", [])
        if str(value or "").strip()
    ]
    source_playlists = [
        str(value)
        for value in group_data.get("source_playlists", [])
        if str(value or "").strip()
    ]
    if not source_group_keys and not source_playlists:
        return {}

    annotations: Dict[str, Any] = {}
    if source_group_keys:
        annotations["source_group_keys"] = source_group_keys
    if source_playlists:
        annotations["source_playlists"] = source_playlists
    return annotations


def build_playlist_name(
    base_name: str,
    prefix: str,
    use_status_prefix: bool,
    status: str,
    suffix: str,
    use_status_suffix: bool,
    complete_suffix: str = "",
    is_complete: bool = False,
) -> str:
    resolved_prefix = prefix
    if use_status_prefix:
        resolved_prefix += build_status_prefix(status)
    resolved_suffix = suffix
    if use_status_suffix:
        resolved_suffix += build_status_suffix(status)
    if is_complete and complete_suffix:
        resolved_suffix += complete_suffix
    return f"{resolved_prefix}{base_name}{resolved_suffix}".strip()


def build_status_prefix(status: str) -> str:
    return build_status_tag(status) + " "


def build_status_suffix(status: str) -> str:
    return " " + build_status_tag(status)


def build_status_tag(status: str) -> str:
    normalized_status = (status or "unknown").strip().replace("-", "_")
    display_status = " ".join(part for part in normalized_status.split("_") if part)
    display_status = display_status.title() if display_status else "Unknown"
    return f"[{display_status}]"


def group_has_all_expected_episodes(group: Dict[str, Any]) -> bool:
    episodes_found = safe_int(group.get("episodes_found")) or 0
    episodes_expected = safe_int(group.get("episodes_expected")) or 0
    if episodes_expected > 0:
        return episodes_found >= episodes_expected
    return str(group.get("status") or "").casefold() in {"complete", "complete_with_extras"}


def extract_sync_group_key(description: Optional[str]) -> Optional[str]:
    if not description:
        return None
    match = re.search(r"(?:^|,\s*)group_key=(.+)$", description)
    if not match:
        return None
    group_key = match.group(1).strip()
    return group_key or None


def choose_existing_sync_playlist(
    playlists: Sequence[PlexPlaylist],
    desired_name: str,
    group_key: str,
    target_account_id: Optional[int],
) -> Tuple[Optional[PlexPlaylist], List[PlexPlaylist]]:
    keyed_candidates = [playlist for playlist in playlists if extract_sync_group_key(playlist.description) == group_key]
    if keyed_candidates:
        return choose_playlist_candidate(keyed_candidates, target_account_id)
    return PlexPlaylistPlanner.choose_existing_playlist(playlists, desired_name, target_account_id)


def choose_playlist_candidate(
    candidates: Sequence[PlexPlaylist],
    target_account_id: Optional[int],
) -> Tuple[Optional[PlexPlaylist], List[PlexPlaylist]]:
    if not candidates:
        return None, []

    if target_account_id is not None:
        exact_account_matches = [playlist for playlist in candidates if playlist.account_id == target_account_id]
        if exact_account_matches:
            preferred_pool = exact_account_matches
        else:
            stale_candidates = [playlist for playlist in candidates if playlist.account_id is None]
            return None, stale_candidates
    else:
        preferred_pool = list(candidates)

    selected = max(
        preferred_pool,
        key=lambda playlist: (
            playlist.added_at or 0,
            playlist.play_queue_id or 0,
            playlist.id,
        ),
    )
    stale_candidates = [playlist for playlist in candidates if playlist.id != selected.id and playlist.account_id is None]
    return selected, stale_candidates


def find_removed_sync_playlists(
    playlists: Sequence[PlexPlaylist],
    groups: Sequence[Dict[str, Any]],
    target_account_id: Optional[int],
) -> List[PlexPlaylist]:
    active_group_keys = {str(group.get("group_key") or "") for group in groups}
    removed: List[PlexPlaylist] = []
    seen_playlist_ids: Set[int] = set()

    for playlist in playlists:
        group_key = extract_sync_group_key(playlist.description)
        if not group_key or group_key in active_group_keys:
            continue
        if target_account_id is not None and playlist.account_id not in {target_account_id, None}:
            continue
        if playlist.id in seen_playlist_ids:
            continue
        if playlist.items and playlist.is_empty_in_scope:
            continue
        seen_playlist_ids.add(playlist.id)
        removed.append(playlist)

    return removed


def should_update_existing_playlist_metadata(existing_playlist: PlexPlaylist, playlist_name: str, description: str) -> bool:
    return existing_playlist.name != playlist_name or (existing_playlist.description or "") != description


def resolve_updated_playlist_name(
    playlists: Sequence[PlexPlaylist],
    existing_playlist: PlexPlaylist,
    desired_name: str,
    playlist_name: str,
    conflict_policy: str,
) -> Optional[str]:
    if existing_playlist.name.casefold() == playlist_name.casefold():
        return playlist_name

    conflicting_playlist = find_conflicting_playlist_name(playlists, playlist_name, existing_playlist.id)
    if conflicting_playlist is None:
        return playlist_name
    if conflict_policy == "unique":
        existing_names = [playlist.name for playlist in playlists if playlist.id != existing_playlist.id]
        return PlexPlaylistPlanner.resolve_unique_name(existing_names, desired_name)
    return existing_playlist.name


def determine_rename_note(
    playlists: Sequence[PlexPlaylist],
    existing_playlist: Optional[PlexPlaylist],
    desired_name: str,
    playlist_name: str,
    conflict_policy: str,
) -> Optional[str]:
    if existing_playlist is None or existing_playlist.name.casefold() == playlist_name.casefold():
        return None
    conflicting_playlist = find_conflicting_playlist_name(playlists, playlist_name, existing_playlist.id)
    if conflicting_playlist is None:
        return f"rename existing playlist '{existing_playlist.name}' -> '{playlist_name}'"
    if conflict_policy == "unique":
        resolved_name = PlexPlaylistPlanner.resolve_unique_name(
            [playlist.name for playlist in playlists if playlist.id != existing_playlist.id],
            desired_name,
        )
        return (
            f"rename existing playlist '{existing_playlist.name}' -> '{resolved_name}' "
            f"because '{playlist_name}' is already used by playlist {conflicting_playlist.id}"
        )
    return (
        f"kept existing playlist name '{existing_playlist.name}' because desired name '{playlist_name}' "
        f"is already used by playlist {conflicting_playlist.id}"
    )


def find_conflicting_playlist_name(
    playlists: Sequence[PlexPlaylist],
    playlist_name: str,
    excluded_playlist_id: Optional[int],
) -> Optional[PlexPlaylist]:
    target_key = playlist_name.casefold()
    for playlist in playlists:
        if excluded_playlist_id is not None and playlist.id == excluded_playlist_id:
            continue
        if playlist.name.casefold() == target_key:
            return playlist
    return None


def format_unix_timestamp(timestamp: Optional[int]) -> str:
    if timestamp is None:
        return ""
    return datetime.fromtimestamp(timestamp).isoformat(sep=" ", timespec="seconds")


def format_unmatched_items(unmatched_files: Sequence[str]) -> str:
    unmatched_labels = "; ".join(unmatched_files[:5])
    if len(unmatched_files) > 5:
        unmatched_labels += f"; +{len(unmatched_files) - 5} more"
    return unmatched_labels


def playlist_media_ids(playlist: PlexPlaylist) -> List[int]:
    return [
        item.media.metadata_item_id
        for item in playlist.items
        if item.media is not None
    ]


def media_record_episode_key(record: MediaRecord) -> Optional[Tuple[int, int, str]]:
    season = record.parent_index
    if season is None:
        season = record.parsed_identity.season
    episode = record.item_index
    if episode is None:
        episode = record.parsed_identity.episode
    if episode is None:
        return None
    return (season or 0, episode, normalize_path_key(record.file_path))


def filter_earlier_sync_records(
    existing_playlist: PlexPlaylist,
    matched_records: Sequence[MediaRecord],
) -> Tuple[List[MediaRecord], List[MediaRecord]]:
    existing_keys = [
        media_record_episode_key(item.media)
        for item in existing_playlist.items
        if item.media is not None
    ]
    comparable_existing_keys = [key for key in existing_keys if key is not None]
    if not comparable_existing_keys:
        return list(matched_records), []

    earliest_existing_key = min(comparable_existing_keys)
    kept_records: List[MediaRecord] = []
    skipped_records: List[MediaRecord] = []
    for record in matched_records:
        record_key = media_record_episode_key(record)
        if record_key is not None and record_key < earliest_existing_key:
            skipped_records.append(record)
            continue
        kept_records.append(record)
    return kept_records, skipped_records


def plan_playlist_item_updates(
    existing_playlist: PlexPlaylist,
    sync_records: Sequence[MediaRecord],
    restore_removed_playlist_items: bool,
) -> Tuple[List[MediaRecord], List[MediaRecord], List[MediaRecord]]:
    existing_ids = playlist_media_ids(existing_playlist)
    existing_id_set = set(existing_ids)
    if not existing_ids:
        return list(sync_records), list(sync_records), []
    if restore_removed_playlist_items:
        append_records = [record for record in sync_records if record.metadata_item_id not in existing_id_set]
        return list(sync_records), append_records, []

    desired_indices_for_existing = [
        index
        for index, record in enumerate(sync_records)
        if record.metadata_item_id in existing_id_set
    ]
    if not desired_indices_for_existing:
        return list(sync_records), list(sync_records), []

    max_existing_index = max(desired_indices_for_existing)
    kept_records: List[MediaRecord] = []
    append_records: List[MediaRecord] = []
    locally_removed_records: List[MediaRecord] = []
    for index, record in enumerate(sync_records):
        if record.metadata_item_id in existing_id_set:
            kept_records.append(record)
            continue
        if index <= max_existing_index:
            locally_removed_records.append(record)
            continue
        kept_records.append(record)
        append_records.append(record)
    return kept_records, append_records, locally_removed_records


def is_append_only_update(existing_ids: Sequence[int], desired_ids: Sequence[int]) -> bool:
    if len(existing_ids) > len(desired_ids):
        return False
    return list(desired_ids[: len(existing_ids)]) == list(existing_ids)


def format_media_record_label(record: MediaRecord) -> str:
    season = record.parent_index
    if season is None:
        season = record.parsed_identity.season
    episode = record.item_index
    if episode is None:
        episode = record.parsed_identity.episode
    if episode is not None:
        if season is not None:
            prefix = f"S{season:02d}E{episode:02d}"
        else:
            prefix = f"E{episode:02d}"
        title = record.title or record.basename
        return f"{prefix} {title}" if title else prefix
    return record.basename or record.title or f"metadata:{record.metadata_item_id}"


def list_media_record_labels(records: Sequence[MediaRecord]) -> List[str]:
    return [format_media_record_label(record) for record in records]


def format_media_record_labels(records: Sequence[MediaRecord]) -> str:
    labels = list_media_record_labels(records)
    if not labels:
        return ""
    rendered = "; ".join(labels[:5])
    if len(labels) > 5:
        rendered += f"; +{len(labels) - 5} more"
    return rendered


def build_path_index(inventory: Sequence[MediaRecord]) -> Dict[str, List[MediaRecord]]:
    index: Dict[str, List[MediaRecord]] = {}
    for record in inventory:
        normalized_path = normalize_path_key(record.file_path)
        index.setdefault(normalized_path, []).append(record)
    return index


def normalize_path_key(path_value: str) -> str:
    return str(path_value).replace("\\", "/").casefold()


def resolve_group_metadata_item_ids(
    group: Dict[str, Any],
    target_indexes: Dict[str, List[MediaRecord]],
    path_index: Dict[str, List[MediaRecord]],
    matcher: PlexMatcher,
) -> Tuple[List[MediaRecord], List[str]]:
    matched_records: List[MediaRecord] = []
    unmatched_files: List[str] = []
    seen_ids: Set[int] = set()

    for file_info in group["files"]:
        matched_record = resolve_file_record(file_info, target_indexes, path_index, matcher)
        if matched_record is None:
            unmatched_files.append(str(file_info.get("filename") or file_info.get("filepath") or file_info.get("file_path") or "unknown"))
            continue
        if matched_record.metadata_item_id in seen_ids:
            continue
        seen_ids.add(matched_record.metadata_item_id)
        matched_records.append(matched_record)

    return matched_records, unmatched_files


def resolve_file_record(
    file_info: Dict[str, Any],
    target_indexes: Dict[str, List[MediaRecord]],
    path_index: Dict[str, List[MediaRecord]],
    matcher: PlexMatcher,
) -> Optional[MediaRecord]:
    source_path = str(file_info.get("filepath") or file_info.get("file_path") or "")
    if source_path:
        exact_candidates = path_index.get(normalize_path_key(source_path), [])
        exact_record = choose_candidate_by_size(exact_candidates, safe_int(file_info.get("file_size")))
        if exact_record is not None:
            return exact_record

    basename = str(file_info.get("filename") or Path(source_path).name)
    if not basename:
        return None

    source_record = MediaRecord(
        metadata_item_id=-1,
        guid="",
        metadata_type=None,
        title=file_info.get("title"),
        year=safe_int(file_info.get("year")),
        item_index=safe_int(file_info.get("episode")),
        originally_available_at=None,
        file_path=source_path or basename,
        library_section_id=None,
        library_section_name=None,
        basename=basename,
        basename_key=PlexFilenameParser.normalize_basename(source_path or basename),
        file_size=safe_int(file_info.get("file_size")),
        duration=safe_int(file_info.get("duration")),
        parent_title=file_info.get("title"),
        parent_index=safe_int(file_info.get("season")),
        parent_guid=None,
        grandparent_title=None,
        grandparent_guid=None,
        parsed_identity=PlexFilenameParser.parse_identity(basename),
    )

    matched_record, _, _, _ = matcher.find_match(source_record, target_indexes)
    return matched_record


def choose_candidate_by_size(candidates: Sequence[MediaRecord], expected_size: Optional[int]) -> Optional[MediaRecord]:
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]
    if expected_size is not None:
        sized_candidates = [candidate for candidate in candidates if candidate.file_size == expected_size]
        if len(sized_candidates) == 1:
            return sized_candidates[0]
    return None


def emit_plan_summary(plans: Sequence[Dict[str, Any]], applied: bool) -> None:
    print_plan_summary(plans, [], applied)


def build_sync_rows(plans: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for plan in plans:
        row = {field_name: plan.get(field_name) for field_name in SYNC_ROW_COLUMNS}
        row["notes"] = "; ".join(str(note) for note in plan.get("notes", []))
        row["unmatched_items"] = format_unmatched_items(plan.get("unmatched_files", []))
        rows.append(row)
    return rows


def build_sync_payload(plans: Sequence[Dict[str, Any]], mutations: Sequence[PlannedMutation]) -> Dict[str, Any]:
    return {
        "summary": {
            "playlists": len(plans),
            "planned_mutations": len(mutations),
            "playlists_with_unmatched_items": sum(1 for plan in plans if plan.get("unmatched_file_count")),
        },
        "playlists": [
            {
                "playlist_id": plan.get("playlist_id"),
                "group_key": plan.get("group_key"),
                "source_name": plan.get("source_playlist"),
                "target_name": plan.get("target_playlist"),
                "target_account_id": plan.get("target_account_id"),
                "source_added_at": plan.get("source_added_at"),
                "target_added_at": plan.get("target_added_at"),
                "status": plan.get("status"),
                "action": plan.get("action"),
                "source_item_count": plan.get("source_item_count"),
                "matched_item_count": plan.get("matched_item_count"),
                "transfer_item_count": plan.get("transfer_item_count"),
                "added_items": list(plan.get("added_item_labels", [])),
                "existing_item_count": plan.get("existing_item_count"),
                "unmatched_item_count": plan.get("unmatched_item_count"),
                "notes": plan.get("notes", []),
                "unmatched_items": list(plan.get("unmatched_files", [])),
            }
            for plan in plans
        ],
        "mutations": [asdict(mutation) for mutation in mutations],
    }


def resolve_sync_columns(columns: Optional[Sequence[TableColumnSpec]]) -> List[TableColumnSpec]:
    requested = list(columns or [TableColumnSpec(name) for name in SYNC_TABLE_DEFAULT_COLUMNS])
    requested_by_name = {
        SYNC_TABLE_COLUMN_ALIASES.get(column.name, column.name): column
        for column in requested
    }
    resolved: List[TableColumnSpec] = []
    seen = set()

    for column_name in SYNC_TABLE_MANDATORY_COLUMNS:
        if column_name not in seen:
            requested_column = requested_by_name.get(column_name)
            resolved.append(
                TableColumnSpec(
                    column_name,
                    requested_column.width
                    if requested_column is not None and requested_column.width is not None
                    else SYNC_TABLE_COLUMN_WIDTHS.get(column_name),
                )
            )
            seen.add(column_name)

    for column in requested:
        column_name = SYNC_TABLE_COLUMN_ALIASES.get(column.name, column.name)
        if column_name not in SYNC_ROW_COLUMNS:
            supported = ", ".join(SYNC_ROW_COLUMNS)
            aliases = ", ".join(sorted(SYNC_TABLE_COLUMN_ALIASES))
            raise RuntimeError(
                f"Unsupported column '{column.name}'. Supported columns: {supported}. "
                f"Shorthand aliases: {aliases}"
            )
        if column_name in seen:
            continue
        resolved.append(
            TableColumnSpec(
                column_name,
                column.width if column.width is not None else SYNC_TABLE_COLUMN_WIDTHS.get(column_name),
            )
        )
        seen.add(column_name)
    return resolved


def emit_sync_outputs(
    plans: Sequence[Dict[str, Any]],
    mutations: Sequence[PlannedMutation],
    console_format: str,
    report_path: Optional[Path],
    report_format: str,
    report_writer: PlexReportWriter,
    columns: Optional[Sequence[TableColumnSpec]],
) -> None:
    rows = build_sync_rows(plans)
    payload = build_sync_payload(plans, mutations)

    if console_format == "json":
        print(json.dumps(payload, indent=2))
    elif console_format == "csv":
        report_writer._write_csv_rows(sys.stdout, rows, SYNC_ROW_COLUMNS)
    else:
        report_writer.write_table_rows(sys.stdout, rows, resolve_sync_columns(columns))

    if report_path is None:
        return

    resolved_format = report_writer.resolve_report_format(report_path, report_format)
    if resolved_format == "json":
        with report_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
    elif resolved_format == "csv":
        with report_path.open("w", newline="", encoding="utf-8") as handle:
            report_writer._write_csv_rows(handle, rows, SYNC_ROW_COLUMNS)
    else:
        with report_path.open("w", encoding="utf-8") as handle:
            report_writer.write_table_rows(handle, rows, resolve_sync_columns(columns))


def print_plan_summary(
    plans: Sequence[Dict[str, Any]],
    mutations: Sequence[PlannedMutation],
    apply: bool,
    stream: TextIO = sys.stdout,
) -> None:
    try:
        print(f"Planned playlists: {len(plans)}", file=stream)
        print(
            f"Playlists with unmatched items: {sum(1 for plan in plans if plan.get('unmatched_file_count'))}",
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


def print_plan_unmatched_details(
    plans: Sequence[Dict[str, Any]],
    stream: TextIO = sys.stdout,
) -> None:
    try:
        for plan in plans:
            unmatched_files = plan.get("unmatched_files", [])
            if not unmatched_files:
                continue
            print(f"Unmatched items for playlist '{plan['target_playlist']}':", file=stream)
            for unmatched_file in unmatched_files:
                print(f"  - {unmatched_file}", file=stream)
        stream.flush()
    except OSError as exc:
        if PlexReportWriter._is_broken_pipe_error(exc):
            if stream is sys.stdout:
                PlexReportWriter._suppress_stdout_after_pipe_error()
            return
        raise
