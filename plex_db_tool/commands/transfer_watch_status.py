import sys
from argparse import ArgumentParser, Namespace, _SubParsersAction
from pathlib import Path
from typing import List, Sequence

from ..cli_support import PlexCliSupport
from ..infrastructure import PlexDatabase, PlexDatabaseLocator, PlexEnvironment
from ..models import MatchResult, MediaRecord, PlannedMutation
from ..planners import PlexMatcher, PlexMutationPlanner
from ..reporting import PlexReportWriter


def register(subparsers: _SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "transfer_watch_status",
        help="Transfer Plex watch history between two Plex library databases.",
        description="Transfer Plex watch history between two Plex SQLite library databases using exact basename matching without path dependence.",
    )
    parser.set_defaults(command="transfer_watch_status", command_handler=run)
    parser.add_argument(
        "--source-path",
        default=None,
        help="Path to the source Plex location. Can be the DB file itself or a folder containing com.plexapp.plugins.library.db.",
    )
    parser.add_argument(
        "--target-path",
        default=None,
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
        "--source-account-id",
        type=int,
        default=None,
        help="Account id to use when reading source watch history.",
    )
    parser.add_argument(
        "--target-account-id",
        type=int,
        default=None,
        help="Account id to use when reading and writing target Plex watch state.",
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
        help="Console output format for match results.",
    )
    parser.add_argument(
        "--columns",
        default=None,
        help=(
            "Comma-separated column list for table output. Use column or column:width. "
            f"Mandatory columns are: {', '.join(PlexReportWriter.TABLE_MANDATORY_COLUMNS)}"
        ),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write the planned mutations into the target DB. Without this flag the tool is dry-run only.",
    )
    parser.add_argument(
        "--dry-run-status-filter",
        choices=["all", "warnings", "errors"],
        default="all",
        help=(
            "Dry-run row filter mode. Use 'all' to show every row, 'warnings' to show unmatched rows, "
            "or 'errors' to show the remaining problem rows."
        ),
    )


def run(args: Namespace) -> int:
    interactive_transfer = populate_missing_transfer_args(args)
    dry_run_filter_mode = args.dry_run_status_filter
    dry_run_filters_active = dry_run_filter_mode != "all"

    if args.apply and dry_run_filters_active:
        raise RuntimeError("Dry-run filters cannot be used with --apply.")

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
        source_account_id = args.source_account_id
        target_account_id = args.target_account_id

        source_inventory = source_database.build_media_inventory(source_schema, args.source_library)
        target_inventory = target_database.build_media_inventory(target_schema, args.target_library)
        target_inventory_all = target_inventory
        if not args.apply:
            target_inventory_all = target_database.build_media_inventory(target_schema, [])
        source_history = source_database.build_watch_history(args.source_library, source_account_id)
        target_history = target_database.build_watch_history(args.target_library, target_account_id)

        matcher = PlexMatcher(args.match_mode, args.min_confidence)
        matches = matcher.collect_matches(
            source_inventory=source_inventory,
            source_history=source_history,
            target_inventory=target_inventory,
            target_history=target_history,
        )
        annotate_library_statuses(matches, matcher, target_inventory_all, bool(args.target_library))

        mutation_planner = PlexMutationPlanner(target_schema, target_account_id, args.conflict_policy)
        mutations = mutation_planner.plan_mutations(matches)
        columns = report_writer.parse_columns(args.columns)
        filtered_matches = matches
        if dry_run_filters_active:
            filtered_matches = filter_dry_run_matches(matches, dry_run_filter_mode)
        elif interactive_transfer and not args.apply:
            filtered_matches = filter_matches_with_planned_mutations(matches, mutations)
        filtered_target_guids = {
            match.target.guid
            for match in filtered_matches
            if match.target is not None
        }
        filtered_mutations = [mutation for mutation in mutations if mutation.target_guid in filtered_target_guids]

        if args.apply:
            target_database.begin_immediate()
            target_database.apply_mutations(mutations)
            target_database.commit()

        output_matches = matches if args.apply else filtered_matches
        output_mutations = mutations if args.apply else filtered_mutations

        report_writer.emit_console(args.console_format, output_matches, output_mutations, columns)
        report_writer.emit_report(args.report, args.report_format, output_matches, output_mutations, columns)
        summary_stream = sys.stderr if args.console_format in {"json", "csv"} else sys.stdout
        report_writer.print_summary(output_matches, output_mutations, args.apply, stream=summary_stream)
        if not args.apply and dry_run_filters_active:
            print(f"Displayed rows: {len(output_matches)} of {len(matches)}", file=summary_stream)

        if interactive_transfer and not args.apply:
            should_apply = PlexCliSupport.prompt_yes_no("Apply these changes?", default=False)
            if should_apply:
                PlexCliSupport.apply_planned_mutations(target_db_path, mutations)
                report_writer.print_summary(matches, mutations, True, stream=summary_stream)
            else:
                print("Changes were not applied.", file=summary_stream)

        report_writer.detach_redirected_stdout()
        return 0
    finally:
        source_database.close()
        target_database.close()


def populate_missing_transfer_args(args: Namespace) -> bool:
    required_values = (
        args.source_path,
        args.target_path,
        args.source_account_id,
        args.target_account_id,
    )
    if all(value is not None for value in required_values):
        return False

    print("Interactive transfer setup")
    args.source_path = PlexCliSupport.prompt_with_default("Source Plex path", args.source_path)
    args.target_path = PlexCliSupport.prompt_with_default("Target Plex path", args.target_path)

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

    source_account_default, target_account_default = PlexCliSupport.infer_interactive_account_defaults(
        source_accounts,
        target_accounts,
        args.source_account_id,
        args.target_account_id,
    )

    args.source_library = PlexCliSupport.prompt_library_filters(
        "Source libraries:",
        source_libraries,
        args.source_library,
    )
    args.target_library = PlexCliSupport.prompt_library_filters(
        "Target libraries:",
        target_libraries,
        args.target_library,
    )

    args.source_account_id = PlexCliSupport.prompt_account_id(
        "Source accounts:",
        source_accounts,
        source_account_default,
    )
    args.target_account_id = PlexCliSupport.prompt_account_id(
        "Target accounts:",
        target_accounts,
        target_account_default,
    )
    return True


def filter_dry_run_matches(
    matches: Sequence[MatchResult],
    dry_run_filter_mode: str,
) -> List[MatchResult]:
    if dry_run_filter_mode == "all":
        return list(matches)

    filtered_matches: List[MatchResult] = []
    for match in matches:
        if not match_in_filter_mode(match, dry_run_filter_mode):
            continue
        filtered_matches.append(match)
    return filtered_matches


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


def is_warning_match(match: MatchResult) -> bool:
    if match.status == "unmatched":
        return True
    return False


def is_error_match(match: MatchResult) -> bool:
    if is_warning_match(match):
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


def match_in_filter_mode(match: MatchResult, dry_run_filter_mode: str) -> bool:
    if dry_run_filter_mode == "warnings":
        return is_warning_match(match)
    if dry_run_filter_mode == "errors":
        return is_error_match(match)
    return True


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
                match.notes.append("target library filter influenced the selected match")
            continue

        if not full_candidates:
            match.library_status = "not_found"
            continue

        if full_match is not None:
            match.library_status = "blocked"
            match.notes.append("matching target exists outside the selected target library")
            if full_match.target.library_section_name:
                match.notes.append(f"matching target library: {full_match.target.library_section_name}")
            continue

        match.library_status = "needed"
        match.notes.append("basename candidates exist, but target library scoping or ambiguity prevented a match")