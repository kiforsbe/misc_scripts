from datetime import datetime
from types import SimpleNamespace

from netflix_title_parser import ParsedNetflixTitle, parse_netflix_title
from netflix_watch_status import NetflixHistoryEntry, NetflixWatchStatusAnalyzer, _derive_episode_title, build_watch_table_rows


def test_inferred_series_title_preserves_episode_title_suffix():
    raw_titles = [
        "SAKAMOTO DAYS: Each One's Mission",
        "SAKAMOTO DAYS: Hard Mode",
        "SAKAMOTO DAYS: Mutual Fans",
        "SAKAMOTO DAYS: Exam, Stage Three",
        "SAKAMOTO DAYS: Kanaguri",
        "SAKAMOTO DAYS: Have a nice fight",
        "SAKAMOTO DAYS: Slice Slice Dance",
    ]

    analyzer = NetflixWatchStatusAnalyzer(metadata_manager=None)
    raw_entries = [(title, datetime(2026, 1, 1), parse_netflix_title(title)) for title in raw_titles]
    prefix_counts = analyzer._build_prefix_counts(raw_entries)

    results = []
    for raw_title, _, parsed in raw_entries:
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
        results.append((raw_title, media_kind, resolved_title, resolved_episode_title, resolved_season, resolved_episode))

    assert prefix_counts["SAKAMOTO DAYS"] == 7
    assert results == [
        ("SAKAMOTO DAYS: Each One's Mission", "series", "SAKAMOTO DAYS", "Each One's Mission", None, None),
        ("SAKAMOTO DAYS: Hard Mode", "series", "SAKAMOTO DAYS", "Hard Mode", None, None),
        ("SAKAMOTO DAYS: Mutual Fans", "series", "SAKAMOTO DAYS", "Mutual Fans", None, None),
        ("SAKAMOTO DAYS: Exam, Stage Three", "series", "SAKAMOTO DAYS", "Exam, Stage Three", None, None),
        ("SAKAMOTO DAYS: Kanaguri", "series", "SAKAMOTO DAYS", "Kanaguri", None, None),
        ("SAKAMOTO DAYS: Have a nice fight", "series", "SAKAMOTO DAYS", "Have a nice fight", None, None),
        ("SAKAMOTO DAYS: Slice Slice Dance", "series", "SAKAMOTO DAYS", "Slice Slice Dance", None, None),
    ]


def test_derive_episode_title_handles_metadata_title_case_difference():
    entry = NetflixHistoryEntry(
        raw_title="SAKAMOTO DAYS: Each One's Mission",
        watched_at=datetime(2026, 1, 1),
        parsed=parse_netflix_title("SAKAMOTO DAYS: Each One's Mission"),
        media_kind="series",
        resolved_title="Sakamoto Days",
    )

    assert _derive_episode_title(entry) == "Each One's Mission"


def test_series_runtime_aggregates_from_seasons_and_loose_episodes():
    entries = [
        NetflixHistoryEntry(
            raw_title="Show: Loose Episode",
            watched_at=datetime(2026, 1, 1),
            parsed=ParsedNetflixTitle(raw_title="Show: Loose Episode", title="Show: Loose Episode", media_kind="movie"),
            media_kind="series",
            resolved_title="Show",
            resolved_episode_title="Loose Episode",
            metadata_runtime_minutes=24,
        ),
        NetflixHistoryEntry(
            raw_title="Show: Season 1: First",
            watched_at=datetime(2026, 1, 2),
            parsed=ParsedNetflixTitle(raw_title="Show: Season 1: First", title="Show", media_kind="series", season=1, season_title="Season 1", is_explicit_series=True),
            media_kind="series",
            resolved_title="Show",
            resolved_season=1,
            resolved_episode=1,
            resolved_episode_title="First",
            metadata_runtime_minutes=24,
        ),
        NetflixHistoryEntry(
            raw_title="Show: Season 1: Second",
            watched_at=datetime(2026, 1, 3),
            parsed=ParsedNetflixTitle(raw_title="Show: Season 1: Second", title="Show", media_kind="series", season=1, season_title="Season 1", is_explicit_series=True),
            media_kind="series",
            resolved_title="Show",
            resolved_season=1,
            resolved_episode=2,
            resolved_episode_title="Second",
            metadata_runtime_minutes=24,
        ),
    ]

    rows = build_watch_table_rows(entries)
    series_row = next(row for row in rows if row.item_type == "series" and row.title == "Show")

    assert series_row.runtime_minutes == "1h 12m"


def test_series_runtime_aggregates_from_metadata_season_rows():
    class Provider:
        def list_episodes(self, parent_id):
            assert parent_id == "show-1"
            return [
                SimpleNamespace(season=1, episode=1, title="First", year=2026, id="ep-1", rating=None, votes=None),
                SimpleNamespace(season=1, episode=2, title="Second", year=2026, id="ep-2", rating=None, votes=None),
            ]

    provider = Provider()
    entries = [
        NetflixHistoryEntry(
            raw_title="Show: First",
            watched_at=datetime(2026, 1, 1),
            parsed=ParsedNetflixTitle(raw_title="Show: First", title="Show: First", media_kind="movie"),
            media_kind="series",
            resolved_title="Show",
            resolved_season=1,
            resolved_episode=1,
            resolved_episode_title="First",
            metadata_type="tv",
            metadata_provider=provider,
            metadata_parent_id="show-1",
            metadata_runtime_minutes=24,
        ),
        NetflixHistoryEntry(
            raw_title="Show: Second",
            watched_at=datetime(2026, 1, 2),
            parsed=ParsedNetflixTitle(raw_title="Show: Second", title="Show: Second", media_kind="movie"),
            media_kind="series",
            resolved_title="Show",
            resolved_season=1,
            resolved_episode=2,
            resolved_episode_title="Second",
            metadata_type="tv",
            metadata_provider=provider,
            metadata_parent_id="show-1",
            metadata_runtime_minutes=24,
        ),
    ]

    rows = build_watch_table_rows(entries)
    series_row = next(row for row in rows if row.item_type == "series" and row.title == "Show")

    assert series_row.runtime_minutes == "48m"