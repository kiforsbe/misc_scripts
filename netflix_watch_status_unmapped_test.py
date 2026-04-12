import argparse
import csv
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from netflix_title_parser import parse_netflix_title
from netflix_watch_status import NetflixWatchStatusAnalyzer, get_metadata_manager


DEFAULT_INPUT_CSV = Path(__file__).with_name("NetflixViewingHistory_unmapped_imdb_titles.csv")


def _configure_utf8_output() -> None:
    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        if stream is None or not hasattr(stream, "reconfigure"):
            continue
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            continue


def _open_csv_with_fallback_encodings(csv_path: Path):
    encodings = ("utf-8-sig", "cp1252", "latin-1")
    last_error: Optional[UnicodeDecodeError] = None
    for encoding in encodings:
        try:
            handle = csv_path.open("r", encoding=encoding, newline="")
            try:
                handle.read(1)
                handle.seek(0)
            except Exception:
                handle.close()
                raise
            return handle
        except UnicodeDecodeError as exc:
            last_error = exc
            continue

    if last_error is not None:
        raise last_error
    raise UnicodeDecodeError("utf-8", b"", 0, 1, f"Unable to decode CSV file: {csv_path}")


def _progress_interval(total_cases: int) -> int:
    if total_cases <= 10:
        return 1
    if total_cases <= 100:
        return 10
    return 25


def _print_progress(
    processed_count: int,
    total_cases: int,
    pass_count: int,
    fail_count: int,
    started_at: float,
    *,
    force: bool = False,
) -> None:
    if total_cases <= 0:
        return

    interval = _progress_interval(total_cases)
    if not force and processed_count % interval != 0 and processed_count != total_cases:
        return

    elapsed_seconds = time.perf_counter() - started_at
    percent = (processed_count / total_cases) * 100
    print(
        f"Progress: {processed_count}/{total_cases} ({percent:.1f}%) | "
        f"passed={pass_count} failed={fail_count} elapsed={elapsed_seconds:.1f}s",
        file=sys.stderr,
        flush=True,
    )


def color_text(text: str, color: str) -> str:
    colors = {
        "red": "\033[91m",
        "green": "\033[92m",
        "yellow": "\033[93m",
        "reset": "\033[0m",
    }
    return f"{colors.get(color, '')}{text}{colors['reset']}"


def _repair_mojibake(text: str) -> str:
    suspicious_markers = ("Ã", "â€", "â€™", "â€œ", "â€", "Â")
    if not any(marker in text for marker in suspicious_markers):
        return text
    try:
        repaired = text.encode("cp1252").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text
    return repaired


def _clean_text(value: Optional[str]) -> Optional[str]:
    cleaned = _repair_mojibake((value or "").strip())
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


def _query_sqlite_rows(db_path: str, sql: str, params: tuple[Any, ...], limit: int = 10) -> List[Dict[str, Any]]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql, params).fetchmany(limit)
    return [dict(row) for row in rows]


def _diagnose_imdb_title(provider: Any, query_text: str) -> List[str]:
    if not getattr(provider, "_db_path", None):
        return []
    normalized = (query_text or "").strip().casefold()
    if not normalized:
        return []

    exact_rows = _query_sqlite_rows(
        provider._db_path,
        """
        SELECT id, title, original_title, year, type
        FROM title_basics
        WHERE lower(title) = ? OR lower(original_title) = ?
        ORDER BY year DESC, id
        """,
        (normalized, normalized),
    )
    like_rows = _query_sqlite_rows(
        provider._db_path,
        """
        SELECT id, title, original_title, year, type
        FROM title_basics
        WHERE lower(title) LIKE ? OR lower(original_title) LIKE ?
        ORDER BY year DESC, id
        """,
        (f"%{normalized}%", f"%{normalized}%"),
    )
    aka_rows = _query_sqlite_rows(
        provider._db_path,
        """
        SELECT titleId, title, region, language, is_original_title
        FROM title_akas
        WHERE lower(title) = ? OR lower(title) LIKE ?
        ORDER BY is_original_title DESC, titleId
        """,
        (normalized, f"%{normalized}%"),
    )

    lines = [f"DB imdb title exact matches for {query_text!r}: {len(exact_rows)}"]
    lines.extend(f"    {row}" for row in exact_rows[:5])
    lines.append(f"DB imdb title LIKE matches for {query_text!r}: {len(like_rows)}")
    lines.extend(f"    {row}" for row in like_rows[:5])
    lines.append(f"DB imdb AKA matches for {query_text!r}: {len(aka_rows)}")
    lines.extend(f"    {row}" for row in aka_rows[:5])
    return lines


def _diagnose_imdb_episode(provider: Any, parent_id: str, episode_title: Optional[str]) -> List[str]:
    if not getattr(provider, "_db_path", None) or not parent_id:
        return []
    parent_numeric = int(str(parent_id)[2:]) if str(parent_id).startswith("tt") else int(parent_id)
    normalized = (episode_title or "").strip().casefold()

    summary_rows = _query_sqlite_rows(
        provider._db_path,
        """
        SELECT e.season, e.episode, t.title, t.year
        FROM title_episodes e
        LEFT JOIN episode_titles t ON e.id = t.id
        WHERE e.parent_id = ?
        ORDER BY COALESCE(e.season, 0), COALESCE(e.episode, 0), e.id
        """,
        (parent_numeric,),
    )
    lines = [f"DB imdb episodes for {parent_id}: {len(summary_rows)} rows"]
    lines.extend(f"    {row}" for row in summary_rows[:10])

    if normalized:
        title_match_rows = _query_sqlite_rows(
            provider._db_path,
            """
            SELECT e.season, e.episode, t.title, t.year
            FROM title_episodes e
            LEFT JOIN episode_titles t ON e.id = t.id
            WHERE e.parent_id = ?
              AND (lower(t.title) = ? OR lower(t.title) LIKE ?)
            ORDER BY COALESCE(e.season, 0), COALESCE(e.episode, 0), e.id
            """,
            (parent_numeric, normalized, f"%{normalized}%"),
        )
        aka_match_rows = _query_sqlite_rows(
            provider._db_path,
            """
            SELECT e.season, e.episode, a.title, a.region, a.language
            FROM title_episodes e
            JOIN title_akas a ON e.id = a.titleId
            WHERE e.parent_id = ?
              AND (lower(a.title) = ? OR lower(a.title) LIKE ?)
            ORDER BY COALESCE(e.season, 0), COALESCE(e.episode, 0), e.id
            """,
            (parent_numeric, normalized, f"%{normalized}%"),
        )
        lines.append(f"DB imdb episode title matches for {episode_title!r}: {len(title_match_rows)}")
        lines.extend(f"    {row}" for row in title_match_rows[:5])
        lines.append(f"DB imdb episode AKA matches for {episode_title!r}: {len(aka_match_rows)}")
        lines.extend(f"    {row}" for row in aka_match_rows[:5])

    return lines


def _diagnose_anime_title(provider: Any, query_text: str) -> List[str]:
    if not getattr(provider, "_db_path", None):
        return []
    normalized = (query_text or "").strip().casefold()
    if not normalized:
        return []

    exact_rows = _query_sqlite_rows(
        provider._db_path,
        """
        SELECT id, title, synonym, type, episodes, year, season_number, base_title
        FROM anime_synonym_view
        WHERE lower(synonym) = ? OR lower(title) = ?
        ORDER BY year DESC, id
        """,
        (normalized, normalized),
    )
    like_rows = _query_sqlite_rows(
        provider._db_path,
        """
        SELECT id, title, synonym, type, episodes, year, season_number, base_title
        FROM anime_synonym_view
        WHERE lower(synonym) LIKE ? OR lower(title) LIKE ?
        ORDER BY year DESC, id
        """,
        (f"%{normalized}%", f"%{normalized}%"),
    )

    lines = [f"DB anime title exact matches for {query_text!r}: {len(exact_rows)}"]
    lines.extend(f"    {row}" for row in exact_rows[:5])
    lines.append(f"DB anime title LIKE matches for {query_text!r}: {len(like_rows)}")
    lines.extend(f"    {row}" for row in like_rows[:5])
    return lines


def _diagnose_anime_episode(provider: Any, parent_id: str) -> List[str]:
    if not getattr(provider, "_db_path", None) or not parent_id:
        return []
    parent_numeric = int(parent_id)
    base_rows = _query_sqlite_rows(
        provider._db_path,
        "SELECT id, title, episodes, season_number, base_title, year FROM anime_title WHERE id = ?",
        (parent_numeric,),
    )
    lines = [f"DB anime title rows for {parent_id}: {len(base_rows)}"]
    lines.extend(f"    {row}" for row in base_rows[:5])
    if not base_rows:
        return lines

    base_title = base_rows[0].get("base_title") or base_rows[0].get("title")
    sibling_rows = _query_sqlite_rows(
        provider._db_path,
        """
        SELECT id, title, episodes, season_number, year, base_title
        FROM anime_title
        WHERE base_title = ?
        ORDER BY season_number, year, id
        """,
        (base_title,),
    )
    lines.append(f"DB anime sibling season rows for {base_title!r}: {len(sibling_rows)}")
    lines.extend(f"    {row}" for row in sibling_rows[:10])
    lines.append("DB anime provider does not store episode titles; episode lookup is season/episode-count based.")
    return lines


def _collect_db_diagnostics(
    metadata_manager: Any,
    parsed: Any,
    resolved_title: str,
    metadata_provider: Any,
    metadata_parent_id: Optional[str],
    resolved_episode_title: Optional[str],
    title_found: bool,
    episode_required: bool,
    episode_found: bool,
) -> List[str]:
    lines: List[str] = []
    title_queries = list(dict.fromkeys(filter(None, [parsed.raw_title, parsed.title, resolved_title])))

    if not title_found and metadata_manager is not None:
        for provider in getattr(metadata_manager, "providers", []):
            provider_name = _provider_name(provider)
            lines.append(f"DB diagnostics for provider={provider_name}")
            for query_text in title_queries:
                if provider_name == "IMDbDataProvider":
                    lines.extend(_diagnose_imdb_title(provider, query_text))
                elif provider_name == "AnimeDataProvider":
                    lines.extend(_diagnose_anime_title(provider, query_text))

    if title_found and episode_required and not episode_found and metadata_provider is not None:
        provider_name = _provider_name(metadata_provider)
        lines.append(f"DB episode diagnostics for provider={provider_name}")
        if provider_name == "IMDbDataProvider":
            lines.extend(_diagnose_imdb_episode(metadata_provider, str(metadata_parent_id), resolved_episode_title or parsed.episode_title))
        elif provider_name == "AnimeDataProvider":
            lines.extend(_diagnose_anime_episode(metadata_provider, str(metadata_parent_id)))

    return lines


def _build_test_cases(csv_path: Path) -> List[Dict[str, Optional[str]]]:
    with _open_csv_with_fallback_encodings(csv_path) as handle:
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
    db_diagnostics: bool = False,
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
    total_cases = len(test_cases)
    started_at = time.perf_counter()

    pass_count = 0
    fail_count = 0
    title_miss_count = 0
    episode_miss_count = 0
    failed_cases: List[Dict[str, Any]] = []

    print(
        f"Running {total_cases} netflix_watch_status unmapped case(s)...",
        file=sys.stderr,
        flush=True,
    )

    for index, (case, (_, _, parsed)) in enumerate(zip(test_cases, raw_entries), start=1):
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
        if db_diagnostics and mismatches:
            output_lines.extend(
                _collect_db_diagnostics(
                    metadata_manager=metadata_manager,
                    parsed=parsed,
                    resolved_title=resolved_title,
                    metadata_provider=metadata_provider,
                    metadata_parent_id=metadata_parent_id,
                    resolved_episode_title=resolved_episode_title,
                    title_found=title_found,
                    episode_required=episode_required,
                    episode_found=episode_found,
                )
            )

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

        _print_progress(index, total_cases, pass_count, fail_count, started_at)

    if total_cases % _progress_interval(total_cases) != 0:
        _print_progress(total_cases, total_cases, pass_count, fail_count, started_at, force=True)

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
    _configure_utf8_output()
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
    parser.add_argument(
        "--db-diagnostics",
        action="store_true",
        help="For failed rows, run direct SQLite queries against provider caches and print the results.",
    )
    args = parser.parse_args()

    raise SystemExit(
        test_unmapped_rows(
            csv_path=Path(args.csv_path).expanduser().resolve(),
            verbosity=args.verbosity,
            override_path=args.episode_title_overrides,
            contains=args.contains,
            limit=args.limit,
            db_diagnostics=args.db_diagnostics,
        )
    )


if __name__ == "__main__":
    main()