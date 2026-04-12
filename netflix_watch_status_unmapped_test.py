import argparse
import csv
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from netflix_title_parser import parse_netflix_title
from netflix_watch_status import NetflixWatchStatusAnalyzer, get_metadata_manager


DEFAULT_INPUT_CSV = Path(__file__).with_name("NetflixViewingHistory_unmapped_imdb_titles.csv")


def color_text(text: str, color: str) -> str:
    colors = {
        "red": "\033[91m",
        "green": "\033[92m",
        "yellow": "\033[93m",
        "reset": "\033[0m",
    }
    return f"{colors.get(color, '')}{text}{colors['reset']}"


def _clean_text(value: Optional[str]) -> Optional[str]:
    cleaned = (value or "").strip()
    return cleaned or None


def _clean_int(value: Optional[str]) -> Optional[int]:
    cleaned = _clean_text(value)
    if cleaned is None:
        return None
    try:
        return int(cleaned)
    except ValueError:
        return None


def _provider_name(provider: Any) -> str:
    if provider is None:
        return ""
    return type(provider).__name__


def _build_test_cases(csv_path: Path) -> List[Dict[str, Optional[str]]]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return [
            {
                "netflix_original_title": _clean_text(row.get("netflix_original_title")),
                "netflix_title": _clean_text(row.get("netflix_title")),
                "season_name": _clean_text(row.get("season_name")),
                "netflix_episode_title": _clean_text(row.get("netflix_episode_title")),
                "expected_type": _clean_text(row.get("expected_type")),
                "expected_title": _clean_text(row.get("title")),
                "expected_year": row.get("year"),
                "expected_source_id": _clean_text(row.get("source_id")),
                "expected_episode_title": _clean_text(row.get("episode_title")),
                "had_override": _clean_text(row.get("had_override")),
            }
            for row in reader
            if _clean_text(row.get("netflix_original_title"))
        ]


def test_unmapped_rows(
    csv_path: Path,
    verbosity: int = 0,
    override_path: Optional[str] = None,
    contains: Optional[str] = None,
    limit: Optional[int] = None,
) -> int:
    test_cases = _build_test_cases(csv_path)
    if contains:
        needle = contains.casefold()
        test_cases = [
            case for case in test_cases
            if needle in (case["netflix_original_title"] or "").casefold()
        ]
    if limit is not None:
        test_cases = test_cases[: max(0, limit)]

    metadata_manager = get_metadata_manager()
    analyzer = NetflixWatchStatusAnalyzer(
        metadata_manager=metadata_manager,
        episode_title_overrides=None,
    )
    if override_path:
        from netflix_watch_status import load_episode_title_overrides

        analyzer = NetflixWatchStatusAnalyzer(
            metadata_manager=metadata_manager,
            episode_title_overrides=load_episode_title_overrides(override_path),
        )

    raw_entries = [
        (case["netflix_original_title"], datetime(2026, 1, 1), parse_netflix_title(case["netflix_original_title"]))
        for case in test_cases
        if case["netflix_original_title"] is not None
    ]
    prefix_counts = analyzer._build_prefix_counts(raw_entries)

    pass_count = 0
    fail_count = 0
    title_miss_count = 0
    episode_miss_count = 0
    failed_cases: List[Dict[str, Any]] = []

    for case, (_, _, parsed) in zip(test_cases, raw_entries):
        raw_title = case["netflix_original_title"] or ""
        media_kind, resolved_title, resolved_title_year, resolved_total_seasons, metadata_type, metadata_provider, metadata_parent_id, metadata_average_rating, metadata_num_votes, metadata_runtime_minutes, metadata_genres, metadata_title_type, metadata_sources = analyzer._classify_entry(parsed, prefix_counts)
        resolved_season, resolved_episode, resolved_episode_title, resolved_episode_source_id, resolved_episode_rating, resolved_episode_votes, resolved_episode_year = analyzer._resolve_episode_metadata(
            parsed=parsed,
            media_kind=media_kind,
            metadata_type=metadata_type,
            metadata_provider=metadata_provider,
            metadata_parent_id=metadata_parent_id,
            resolved_title=resolved_title,
            resolved_total_seasons=resolved_total_seasons,
        )

        title_found = metadata_provider is not None and bool(metadata_parent_id)
        episode_required = media_kind == "series" and bool(
            parsed.episode_title
            or parsed.episode is not None
            or analyzer._derive_episode_title(parsed, resolved_title)
        )
        episode_found = bool(resolved_episode_source_id) or resolved_episode is not None

        mismatches: List[str] = []
        expected_type = case["expected_type"]
        expected_title = case["expected_title"]
        expected_year = _clean_int(case["expected_year"])
        expected_source_id = case["expected_source_id"]
        expected_episode_title = case["expected_episode_title"]

        if expected_type and media_kind != expected_type:
            mismatches.append(f"expected_type={expected_type!r} actual={media_kind!r}")
        if expected_title and resolved_title != expected_title:
            mismatches.append(f"title={expected_title!r} actual={resolved_title!r}")
        if expected_year is not None and resolved_title_year != expected_year:
            mismatches.append(f"year={expected_year!r} actual={resolved_title_year!r}")
        if expected_source_id and metadata_parent_id != expected_source_id:
            mismatches.append(f"source_id={expected_source_id!r} actual={metadata_parent_id!r}")
        if expected_episode_title and resolved_episode_title != expected_episode_title:
            mismatches.append(f"episode_title={expected_episode_title!r} actual={resolved_episode_title!r}")

        if not title_found:
            mismatches.append("title metadata not found")
            title_miss_count += 1
        elif episode_required and not episode_found:
            mismatches.append("episode metadata not found")
            episode_miss_count += 1

        output_lines = [
            f"Netflix title: {raw_title}",
            f"Parsed title: {parsed.title} | media_kind={parsed.media_kind} | episode_title={parsed.episode_title!r}",
            f"Resolved title: {resolved_title!r} | media_kind={media_kind!r} | provider={_provider_name(metadata_provider)} | source_id={metadata_parent_id!r}",
            f"Resolved episode: season={resolved_season!r} episode={resolved_episode!r} title={resolved_episode_title!r} source_id={resolved_episode_source_id!r}",
        ]

        if not mismatches:
            pass_count += 1
            if verbosity >= 2:
                for line in output_lines:
                    print(line)
                print(color_text("  ✓ Test passed", "green"))
        else:
            fail_count += 1
            failed_cases.append(
                {
                    "output_lines": output_lines,
                    "mismatches": mismatches,
                }
            )
            if verbosity >= 2:
                for line in output_lines:
                    print(line)
                for mismatch in mismatches:
                    print(color_text(f"  ✘ {mismatch}", "red"))

    if verbosity >= 1 and failed_cases:
        print("\nFailed cases:")
        print("=" * 60)
        for failed_case in failed_cases:
            for line in failed_case["output_lines"]:
                print(line)
            for mismatch in failed_case["mismatches"]:
                print(color_text(f"  ✘ {mismatch}", "red"))
            print()

    if verbosity >= 1 or fail_count > 0:
        print()
    print(color_text(f"{pass_count} / {len(test_cases)} cases passed.", "green" if fail_count == 0 else "yellow"))
    print(f"Title metadata misses: {title_miss_count}")
    print(f"Episode metadata misses: {episode_miss_count}")
    if fail_count == 0:
        print(color_text("All test cases passed!", "green"))
        return 0

    print(color_text(f"{fail_count} test case(s) failed.", "red"))
    return 1


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run netflix_watch_status metadata checks against an exported unmapped-rows CSV."
    )
    parser.add_argument(
        "csv_path",
        nargs="?",
        default=str(DEFAULT_INPUT_CSV),
        help="Path to a netflix_watch_status unmapped CSV export.",
    )
    parser.add_argument(
        "-v",
        "--verbosity",
        action="count",
        default=0,
        help="Increase verbosity level (use -v for failures only, -vv for all cases).",
    )
    parser.add_argument(
        "--episode-title-overrides",
        metavar="FILE",
        help="Optional override CSV to load before running the test rig.",
    )
    parser.add_argument(
        "--contains",
        metavar="TEXT",
        help="Only run rows whose netflix_original_title contains this text.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Only run the first N rows after any filtering.",
    )
    args = parser.parse_args()

    raise SystemExit(
        test_unmapped_rows(
            csv_path=Path(args.csv_path).expanduser().resolve(),
            verbosity=args.verbosity,
            override_path=args.episode_title_overrides,
            contains=args.contains,
            limit=args.limit,
        )
    )


if __name__ == "__main__":
    main()