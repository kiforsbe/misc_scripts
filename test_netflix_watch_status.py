from datetime import datetime

from netflix_title_parser import parse_netflix_title
from netflix_watch_status import NetflixHistoryEntry, NetflixWatchStatusAnalyzer, _derive_episode_title


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