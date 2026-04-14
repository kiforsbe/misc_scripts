import csv
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from tempfile import TemporaryDirectory

from netflix_title_parser import ParsedNetflixTitle, adapt_lookup_titles, parse_netflix_title
from netflix_watch_status import (
    DEFAULT_EPISODE_TITLE_OVERRIDES_FILE,
    NetflixHistoryEntry,
    NetflixWatchStatusAnalyzer,
    WatchTableRow,
    _derive_episode_title,
    _row_watch_state,
    build_unmapped_imdb_override_rows,
    build_watch_table_rows,
    export_unmapped_imdb_overrides,
    load_episode_title_overrides,
    summarize_unmapped_imdb_override_rows,
)


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


def test_adapt_lookup_titles_matches_known_titles_without_pattern_assumptions():
    assert adapt_lookup_titles(
        '"Terzetto" / "A Distant Road Home" ("Haruka Naru Ieji")',
        ("Terzetto", "Other Episode"),
    ) == (
        '"Terzetto" / "A Distant Road Home" ("Haruka Naru Ieji")',
        "Terzetto",
    )


def test_resolve_episode_metadata_falls_back_to_known_episode_title_when_provider_lookup_misses():
    class Provider:
        def __init__(self):
            self.calls = []

        def list_episodes(self, parent_id, season=None):
            return [SimpleNamespace(season=1, episode=1, title="Terzetto", year=2021, id="ep-1", rating=7.8, votes=1200)]

        def find_episode_by_title(self, parent_id, episode_title, season=None):
            self.calls.append((parent_id, episode_title, season))
            return None

    analyzer = NetflixWatchStatusAnalyzer(metadata_manager=None)
    provider = Provider()
    parsed = ParsedNetflixTitle(
        raw_title='Godzilla Singular Point: "Terzetto" / "A Distant Road Home" ("Haruka Naru Ieji")',
        title="Godzilla Singular Point",
        media_kind="series",
        episode_title='"Terzetto" / "A Distant Road Home" ("Haruka Naru Ieji")',
        is_explicit_series=True,
    )

    resolved = analyzer._resolve_episode_metadata(
        parsed=parsed,
        media_kind="series",
        metadata_type="tv",
        metadata_provider=provider,
        metadata_parent_id="tt1234567",
        resolved_title="Godzilla Singular Point",
        resolved_total_seasons=1,
    )

    assert resolved == (1, 1, "Terzetto", "ep-1", 7.8, 1200, 2021)
    assert provider.calls == [
        ("tt1234567", '"Terzetto" / "A Distant Road Home" ("Haruka Naru Ieji")', None),
        ("tt1234567", "Terzetto", None),
    ]


def test_load_episode_title_overrides_normalizes_custom_keys():
    with TemporaryDirectory() as temp_dir:
        override_path = Path(temp_dir) / "episode_overrides.csv"
        override_path.write_text(
            "netflix_original_title,title,year,source_id,episode_title\n"
            '  Some Show: Weird Netflix Title  ,Actual IMDb Title,2024,tt1234567,Actual Episode Title\n',
            encoding="utf-8",
        )

        overrides = load_episode_title_overrides(str(override_path))

    assert overrides["some show: weird netflix title"].title == "Actual IMDb Title"
    assert overrides["some show: weird netflix title"].year == 2024
    assert overrides["some show: weird netflix title"].source_id == "tt1234567"
    assert overrides["some show: weird netflix title"].episode_title == "Actual Episode Title"


def test_load_episode_title_overrides_supports_title_only_rows():
    with TemporaryDirectory() as temp_dir:
        override_path = Path(temp_dir) / "episode_overrides.csv"
        override_path.write_text(
            "netflix_original_title,title,year,source_id,episode_title\n"
            'Some Show,Canonical Title,,,\n',
            encoding="utf-8",
        )

        overrides = load_episode_title_overrides(str(override_path))

    assert overrides["some show"].title == "Canonical Title"
    assert overrides["some show"].year is None
    assert overrides["some show"].source_id is None
    assert overrides["some show"].episode_title is None


def test_load_episode_title_overrides_allows_omitting_optional_mapping_columns():
    with TemporaryDirectory() as temp_dir:
        override_path = Path(temp_dir) / "episode_overrides.csv"
        override_path.write_text(
            "netflix_original_title,title,year\n"
            'Some Show,Canonical Title,2024\n',
            encoding="utf-8",
        )

        overrides = load_episode_title_overrides(str(override_path))

    assert overrides["some show"].title == "Canonical Title"
    assert overrides["some show"].year == 2024
    assert overrides["some show"].source_id is None
    assert overrides["some show"].episode_title is None


def test_load_episode_title_overrides_legacy_split_keys_still_work_without_optional_mapping_columns():
    with TemporaryDirectory() as temp_dir:
        override_path = Path(temp_dir) / "episode_overrides.csv"
        override_path.write_text(
            "netflix_title,season_name,netflix_episode_title,title,year\n"
            'Known Show,Season 1,Missing Episode,Canonical Show,2024\n',
            encoding="utf-8",
        )

        overrides = load_episode_title_overrides(str(override_path))

    analyzer = NetflixWatchStatusAnalyzer(metadata_manager=None, episode_title_overrides=overrides)
    parsed = ParsedNetflixTitle(
        raw_title="Known Show: Season 1: Missing Episode",
        title="Known Show",
        media_kind="series",
        season=1,
        season_title="Season 1",
        episode_title="Missing Episode",
        is_explicit_series=True,
    )

    resolved = analyzer._classify_entry(parsed, prefix_counts={})

    assert resolved[0] == "series"
    assert resolved[1] == "Canonical Show"
    assert resolved[2] == 2024
    assert analyzer._get_episode_title_override(parsed, resolved[1]) is None


def test_load_episode_title_overrides_supports_split_netflix_keys():
    with TemporaryDirectory() as temp_dir:
        override_path = Path(temp_dir) / "episode_overrides.csv"
        override_path.write_text(
            "netflix_title,season_name,netflix_episode_title,title,year,source_id,episode_title\n"
            'Known Show,Season 1,Missing Episode,Canonical Show,2024,tt1234567,Canonical Episode\n',
            encoding="utf-8",
        )

        overrides = load_episode_title_overrides(str(override_path))

    analyzer = NetflixWatchStatusAnalyzer(metadata_manager=None, episode_title_overrides=overrides)
    parsed = ParsedNetflixTitle(
        raw_title="Known Show: Season 1: Missing Episode",
        title="Known Show",
        media_kind="series",
        season=1,
        season_title="Season 1",
        episode_title="Missing Episode",
        is_explicit_series=True,
    )

    resolved = analyzer._classify_entry(parsed, prefix_counts={})

    assert resolved[0] == "series"
    assert resolved[1] == "Canonical Show"
    assert analyzer._get_episode_title_override(parsed, resolved[1]) == "Canonical Episode"


def test_load_episode_title_overrides_prefers_netflix_original_title_key_for_mapping():
    with TemporaryDirectory() as temp_dir:
        override_path = Path(temp_dir) / "episode_overrides.csv"
        override_path.write_text(
            "netflix_original_title,netflix_title,season_name,netflix_episode_title,title,year,source_id,episode_title\n"
            'Known Show: Season 1: Missing Episode,Known Show,Season 1,Missing Episode,Canonical Show,2024,tt1234567,Canonical Episode\n',
            encoding="utf-8",
        )

        overrides = load_episode_title_overrides(str(override_path))

    analyzer = NetflixWatchStatusAnalyzer(metadata_manager=None, episode_title_overrides=overrides)
    parsed = ParsedNetflixTitle(
        raw_title="Known Show: Season 1: Missing Episode",
        title="Known Show",
        media_kind="series",
        season=1,
        season_title="Season 1",
        episode_title="Missing Episode",
        is_explicit_series=True,
    )

    resolved = analyzer._classify_entry(parsed, prefix_counts={})

    assert resolved[0] == "series"
    assert resolved[1] == "Canonical Show"
    assert resolved[2] == 2024
    assert resolved[6] == "tt1234567"
    assert analyzer._get_episode_title_override(parsed, resolved[1]) == "Canonical Episode"


def test_default_episode_title_overrides_include_dandadan_imdb_mapping():
    overrides = load_episode_title_overrides(str(DEFAULT_EPISODE_TITLE_OVERRIDES_FILE))

    override = overrides["dan da dan: season 1: to a kinder world"]

    assert override.title == "Dandadan"
    assert override.year == 2024
    assert override.source_id == "tt30217403"
    assert override.episode_title == "Yasashii sekai e"


def test_classify_entry_uses_title_override_table_for_non_matching_show_titles():
    class MetadataManager:
        def __init__(self):
            self.calls = []

        def find_title(self, query, year=None, preferred_type=None):
            self.calls.append((query, year, preferred_type))
            if query != "Dandadan":
                return None
            return (
                SimpleNamespace(
                    title="Dandadan",
                    type="tv",
                    id="tt30217403",
                    year=2024,
                    total_seasons=3,
                    sources=("imdb",),
                ),
                object(),
            )

    metadata_manager = MetadataManager()
    analyzer = NetflixWatchStatusAnalyzer(
        metadata_manager=metadata_manager,
        episode_title_overrides={
            "DAN DA DAN": {"title": "Dandadan", "year": 2024, "source_id": "tt30217403"},
        },
    )

    parsed = parse_netflix_title("DAN DA DAN")

    resolved = analyzer._classify_entry(parsed, prefix_counts={})

    assert resolved[0] == "series"
    assert resolved[1] == "Dandadan"
    assert resolved[2] == 2024
    assert resolved[3] == 3
    assert resolved[4] == "tv"
    assert resolved[6] == "tt30217403"
    assert metadata_manager.calls == [("Dandadan", 2024, "tv")]


def test_classify_entry_prefers_movie_match_for_implicit_single_colon_titles():
    class MetadataManager:
        def __init__(self):
            self.calls = []

        def find_title(self, query, year=None, preferred_type=None):
            self.calls.append((query, year, preferred_type))
            if query != "Mission: Cross":
                return None
            return (
                SimpleNamespace(
                    title="Mission: Cross",
                    type="movie",
                    id="tt1234567",
                    year=2023,
                    sources=("imdb",),
                ),
                object(),
            )

    analyzer = NetflixWatchStatusAnalyzer(metadata_manager=MetadataManager())

    resolved = analyzer._classify_entry(parse_netflix_title("Mission: Cross"), prefix_counts={})

    assert resolved[0] == "movie"
    assert resolved[1] == "Mission: Cross"
    assert analyzer.metadata_manager.calls == [("Mission: Cross", None, "movie")]


def test_classify_entry_falls_back_to_series_match_for_implicit_single_colon_titles():
    class MetadataManager:
        def __init__(self):
            self.calls = []

        def find_title(self, query, year=None, preferred_type=None):
            self.calls.append((query, year, preferred_type))
            if query != "A.I.C.O.":
                return None
            return (
                SimpleNamespace(
                    title="A.I.C.O. Incarnation",
                    type="tv",
                    id="tt7493752",
                    year=2018,
                    total_seasons=1,
                    sources=("imdb",),
                ),
                object(),
            )

    analyzer = NetflixWatchStatusAnalyzer(metadata_manager=MetadataManager())

    resolved = analyzer._classify_entry(parse_netflix_title("A.I.C.O.: Awakening"), prefix_counts={})

    assert resolved[0] == "series"
    assert resolved[1] == "A.I.C.O. Incarnation"
    assert analyzer.metadata_manager.calls == [
        ("A.I.C.O.: Awakening", None, "movie"),
        ("A.I.C.O.", None, "tv"),
    ]


def test_classify_entry_merges_imdb_metadata_into_anime_series_matches():
    class AnimeProvider:
        pass

    class IMDbProvider:
        pass

    class MetadataManager:
        def __init__(self):
            self.calls = []

        def find_title(self, query, year=None, preferred_type=None):
            self.calls.append(("find_title", query, year, preferred_type))
            if query != "A.I.C.O.":
                return None
            return (
                SimpleNamespace(
                    title="A.I.C.O. Incarnation",
                    type="anime_series",
                    id="36039",
                    year=2018,
                    total_seasons=1,
                    rating=6.7,
                    sources=("https://myanimelist.net/anime/36039",),
                ),
                AnimeProvider(),
            )

        def find_title_from_provider(self, query, provider_name, year=None, preferred_type=None):
            self.calls.append(("find_title_from_provider", query, provider_name, year, preferred_type))
            if query != "A.I.C.O." or provider_name != "imdbdataprovider":
                return (None, None)
            return (
                SimpleNamespace(
                    title="A.I.C.O. Incarnation",
                    type="tv",
                    id="tt8116380",
                    year=2018,
                    total_seasons=1,
                    sources=("https://www.imdb.com/title/tt8116380/",),
                ),
                IMDbProvider(),
            )

    analyzer = NetflixWatchStatusAnalyzer(metadata_manager=MetadataManager())

    resolved = analyzer._classify_entry(parse_netflix_title("A.I.C.O.: Awakening"), prefix_counts={})

    assert resolved[0] == "series"
    assert resolved[1] == "A.I.C.O. Incarnation"
    assert resolved[4] == "tv"
    assert resolved[6] == "tt8116380"
    assert type(resolved[5]).__name__ == "IMDbProvider"
    assert resolved[11] == "anime_series"
    assert resolved[12] == (
        "https://myanimelist.net/anime/36039",
        "https://www.imdb.com/title/tt8116380/",
    )


def test_classify_entry_skips_imdb_enrichment_when_series_match_already_has_imdb_reference():
    class AnimeProvider:
        pass

    class MetadataManager:
        def __init__(self):
            self.calls = []

        def find_title(self, query, year=None, preferred_type=None):
            self.calls.append(("find_title", query, year, preferred_type))
            if query != "A.I.C.O.":
                return None
            return (
                SimpleNamespace(
                    title="A.I.C.O. Incarnation",
                    type="anime_series",
                    id="tt8116380",
                    year=2018,
                    total_seasons=1,
                    rating=6.7,
                    sources=(
                        "https://myanimelist.net/anime/36039",
                        "https://www.imdb.com/title/tt8116380/",
                    ),
                ),
                AnimeProvider(),
            )

        def find_title_from_provider(self, query, provider_name, year=None, preferred_type=None):
            self.calls.append(("find_title_from_provider", query, provider_name, year, preferred_type))
            raise AssertionError("IMDb enrichment lookup should be skipped when the initial match already has an IMDb reference")

    analyzer = NetflixWatchStatusAnalyzer(metadata_manager=MetadataManager())

    resolved = analyzer._classify_entry(parse_netflix_title("A.I.C.O.: Awakening"), prefix_counts={})

    assert resolved[0] == "series"
    assert resolved[1] == "A.I.C.O. Incarnation"
    assert resolved[4] == "anime_series"
    assert resolved[6] == "tt8116380"
    assert type(resolved[5]).__name__ == "AnimeProvider"
    assert resolved[12] == (
        "https://myanimelist.net/anime/36039",
        "https://www.imdb.com/title/tt8116380/",
    )
    assert analyzer.metadata_manager.calls == [
        ("find_title", "A.I.C.O.: Awakening", None, "movie"),
        ("find_title", "A.I.C.O.", None, "tv"),
    ]


def test_resolve_episode_metadata_uses_title_override_table_for_non_matching_titles():
    class Provider:
        def __init__(self):
            self.calls = []

        def list_episodes(self, parent_id):
            assert parent_id == "tt1234567"
            return [
                SimpleNamespace(
                    season=1,
                    episode=2,
                    title="Midsummer Devil Festival/'Manatsu Oni Matsuri'",
                    year=2021,
                    id="ep-2",
                    rating=7.6,
                    votes=1100,
                ),
            ]

        def find_episode_by_title(self, parent_id, episode_title, season=None):
            self.calls.append((parent_id, episode_title, season))
            if episode_title == "Midsummer Devil Festival/'Manatsu Oni Matsuri'":
                return SimpleNamespace(
                    season=1,
                    episode=2,
                    title="Midsummer Devil Festival/'Manatsu Oni Matsuri'",
                    year=2021,
                    id="ep-2",
                    rating=7.6,
                    votes=1100,
                )
            return None

    analyzer = NetflixWatchStatusAnalyzer(
        metadata_manager=None,
        episode_title_overrides={
            "Godzilla Singular Point: Gamesome": {
                "episode_title": "Midsummer Devil Festival/'Manatsu Oni Matsuri'",
            },
        },
    )
    provider = Provider()

    parsed = ParsedNetflixTitle(
        raw_title="Godzilla Singular Point: Gamesome",
        title="Godzilla Singular Point",
        media_kind="series",
        episode_title="Gamesome",
        is_explicit_series=True,
    )

    resolved = analyzer._resolve_episode_metadata(
        parsed=parsed,
        media_kind="series",
        metadata_type="tv",
        metadata_provider=provider,
        metadata_parent_id="tt1234567",
        resolved_title="Godzilla Singular Point",
        resolved_total_seasons=1,
    )

    assert resolved == (1, 2, "Midsummer Devil Festival/'Manatsu Oni Matsuri'", "ep-2", 7.6, 1100, 2021)
    assert provider.calls == [
        ("tt1234567", "Midsummer Devil Festival/'Manatsu Oni Matsuri'", None),
    ]


def test_resolve_episode_metadata_uses_episode_title_override_key_without_series_prefix():
    class Provider:
        def __init__(self):
            self.calls = []

        def list_episodes(self, parent_id):
            assert parent_id == "tt1234567"
            return [
                SimpleNamespace(
                    season=1,
                    episode=2,
                    title="Midsummer Devil Festival/'Manatsu Oni Matsuri'",
                    year=2021,
                    id="ep-2",
                    rating=7.6,
                    votes=1100,
                ),
            ]

        def find_episode_by_title(self, parent_id, episode_title, season=None):
            self.calls.append((parent_id, episode_title, season))
            if episode_title == "Midsummer Devil Festival/'Manatsu Oni Matsuri'":
                return SimpleNamespace(
                    season=1,
                    episode=2,
                    title="Midsummer Devil Festival/'Manatsu Oni Matsuri'",
                    year=2021,
                    id="ep-2",
                    rating=7.6,
                    votes=1100,
                )
            return None

    analyzer = NetflixWatchStatusAnalyzer(
        metadata_manager=None,
        episode_title_overrides={
            "Gamesome": {
                "episode_title": "Midsummer Devil Festival/'Manatsu Oni Matsuri'",
            },
        },
    )
    provider = Provider()

    parsed = ParsedNetflixTitle(
        raw_title="Godzilla Singular Point: Gamesome",
        title="Godzilla Singular Point",
        media_kind="series",
        episode_title="Gamesome",
        is_explicit_series=True,
    )

    resolved = analyzer._resolve_episode_metadata(
        parsed=parsed,
        media_kind="series",
        metadata_type="tv",
        metadata_provider=provider,
        metadata_parent_id="tt1234567",
        resolved_title="Godzilla Singular Point",
        resolved_total_seasons=1,
    )

    assert resolved == (1, 2, "Midsummer Devil Festival/'Manatsu Oni Matsuri'", "ep-2", 7.6, 1100, 2021)
    assert provider.calls == [
        ("tt1234567", "Midsummer Devil Festival/'Manatsu Oni Matsuri'", None),
    ]


def test_resolve_episode_metadata_uses_full_raw_entry_override_key_for_inferred_series_titles():
    class Provider:
        def __init__(self):
            self.calls = []

        def list_episodes(self, parent_id):
            assert parent_id == "tt1234567"
            return [
                SimpleNamespace(
                    season=1,
                    episode=2,
                    title="Midsummer Devil Festival/'Manatsu Oni Matsuri'",
                    year=2021,
                    id="ep-2",
                    rating=7.6,
                    votes=1100,
                ),
            ]

        def find_episode_by_title(self, parent_id, episode_title, season=None):
            self.calls.append((parent_id, episode_title, season))
            if episode_title == "Midsummer Devil Festival/'Manatsu Oni Matsuri'":
                return SimpleNamespace(
                    season=1,
                    episode=2,
                    title="Midsummer Devil Festival/'Manatsu Oni Matsuri'",
                    year=2021,
                    id="ep-2",
                    rating=7.6,
                    votes=1100,
                )
            return None

    analyzer = NetflixWatchStatusAnalyzer(
        metadata_manager=None,
        episode_title_overrides={
            "Godzilla Singular Point: Gamesome": {
                "episode_title": "Midsummer Devil Festival/'Manatsu Oni Matsuri'",
            },
        },
    )
    provider = Provider()
    parsed = parse_netflix_title("Godzilla Singular Point: Gamesome")

    resolved = analyzer._resolve_episode_metadata(
        parsed=parsed,
        media_kind="series",
        metadata_type="tv",
        metadata_provider=provider,
        metadata_parent_id="tt1234567",
        resolved_title="Godzilla Singular Point",
        resolved_total_seasons=1,
    )

    assert resolved == (1, 2, "Midsummer Devil Festival/'Manatsu Oni Matsuri'", "ep-2", 7.6, 1100, 2021)
    assert provider.calls == [
        ("tt1234567", "Midsummer Devil Festival/'Manatsu Oni Matsuri'", None),
    ]


def test_raw_entry_override_can_supply_both_series_and_episode_titles():
    class MetadataManager:
        def __init__(self):
            self.calls = []

        def find_title(self, query, preferred_type=None):
            self.calls.append((query, preferred_type))
            if query != "Dandadan":
                return None
            return (
                SimpleNamespace(
                    title="Dandadan",
                    type="tv",
                    id="tt30217403",
                    year=2024,
                    total_seasons=3,
                    sources=("imdb",),
                ),
                object(),
            )

    class Provider:
        def __init__(self):
            self.calls = []

        def list_episodes(self, parent_id, season=None):
            assert parent_id == "tt30217403"
            return [
                SimpleNamespace(
                    season=2,
                    episode=12,
                    title="Gekitotsu! Uch\ufffd kaij\ufffd tai kyodai robo!",
                    year=2025,
                    id="ep-24",
                    rating=8.8,
                    votes=900,
                ),
            ]

        def find_episode_by_title(self, parent_id, episode_title, season=None):
            self.calls.append((parent_id, episode_title, season))
            if episode_title == "Gekitotsu! Uch\ufffd kaij\ufffd tai kyodai robo!":
                return SimpleNamespace(
                    season=2,
                    episode=12,
                    title="Gekitotsu! Uch\ufffd kaij\ufffd tai kyodai robo!",
                    year=2025,
                    id="ep-24",
                    rating=8.8,
                    votes=900,
                )
            return None

    analyzer = NetflixWatchStatusAnalyzer(
        metadata_manager=MetadataManager(),
        episode_title_overrides={
            "DAN DA DAN: Season 2: Clash! Space Kaiju vs. Giant Robot!": {
                "title": "Dandadan",
                "episode_title": "Gekitotsu! Uch\ufffd kaij\ufffd tai kyodai robo!",
            },
        },
    )
    parsed = parse_netflix_title("DAN DA DAN: Season 2: Clash! Space Kaiju vs. Giant Robot!")
    resolved = analyzer._classify_entry(parsed, prefix_counts={})

    assert resolved[0] == "series"
    assert resolved[1] == "Dandadan"
    assert resolved[6] == "tt30217403"

    provider = Provider()
    episode_resolved = analyzer._resolve_episode_metadata(
        parsed=parsed,
        media_kind=resolved[0],
        metadata_type=resolved[4],
        metadata_provider=provider,
        metadata_parent_id=resolved[6],
        resolved_title=resolved[1],
        resolved_total_seasons=resolved[3],
    )

    assert episode_resolved == (2, 12, "Gekitotsu! Uch\ufffd kaij\ufffd tai kyodai robo!", "ep-24", 8.8, 900, 2025)
    assert provider.calls == [
        ("tt30217403", "Gekitotsu! Uch\ufffd kaij\ufffd tai kyodai robo!", 2),
    ]


def test_build_unmapped_imdb_override_rows_includes_title_and_episode_failures():
    entries = [
        NetflixHistoryEntry(
            raw_title="Unknown Show",
            watched_at=datetime(2026, 1, 1),
            parsed=parse_netflix_title("Unknown Show"),
            media_kind="movie",
            resolved_title="Unknown Show",
            expected_type="movie",
        ),
        NetflixHistoryEntry(
            raw_title="Known Show: Missing Episode",
            watched_at=datetime(2026, 1, 2),
            parsed=ParsedNetflixTitle(
                raw_title="Known Show: Missing Episode",
                title="Known Show",
                media_kind="series",
                episode_title="Missing Episode",
                is_explicit_series=True,
            ),
            media_kind="series",
            resolved_title="Known Show",
            expected_type="series",
            metadata_type="tv",
            metadata_parent_id="tt1234567",
            metadata_sources=("imdb",),
        ),
        NetflixHistoryEntry(
            raw_title="Known Show: Found Episode",
            watched_at=datetime(2026, 1, 3),
            parsed=ParsedNetflixTitle(
                raw_title="Known Show: Found Episode",
                title="Known Show",
                media_kind="series",
                episode_title="Found Episode",
                is_explicit_series=True,
            ),
            media_kind="series",
            resolved_title="Known Show",
            expected_type="series",
            metadata_type="tv",
            metadata_parent_id="tt1234567",
            metadata_sources=("imdb",),
            resolved_episode=2,
            resolved_episode_source_id="tt7654321",
            resolved_episode_title="Found Episode",
        ),
    ]

    rows = build_unmapped_imdb_override_rows(
        entries,
        overrides={
            "Known Show: Missing Episode": {
                "title": "Known Show",
                "year": 2024,
                "source_id": "tt1234567",
                "episode_title": "Canonical Missing Episode",
            },
        },
    )

    assert rows == [
        {
            "netflix_original_title": "Known Show: Missing Episode",
            "netflix_title": "Known Show",
            "season_name": "",
            "netflix_episode_title": "Missing Episode",
            "expected_type": "series",
            "title": "Known Show",
            "year": "2024",
            "source_id": "tt1234567",
            "episode_title": "Canonical Missing Episode",
            "found_source": "imdb",
            "had_override": "yes",
        },
        {
            "netflix_original_title": "Unknown Show",
            "netflix_title": "Unknown Show",
            "season_name": "",
            "netflix_episode_title": "",
            "expected_type": "movie",
            "title": "",
            "year": "",
            "source_id": "",
            "episode_title": "",
            "found_source": "",
            "had_override": "",
        },
    ]


def test_build_unmapped_imdb_override_rows_preserves_non_imdb_title_hints():
    entries = [
        NetflixHistoryEntry(
            raw_title="A.I.C.O.: Awakening",
            watched_at=datetime(2026, 1, 1),
            parsed=ParsedNetflixTitle(
                raw_title="A.I.C.O.: Awakening",
                title="A.I.C.O.",
                media_kind="movie",
                episode_title="Awakening",
                has_implicit_split=True,
            ),
            media_kind="series",
            resolved_title="A.I.C.O. Incarnation",
            resolved_title_year=2018,
            metadata_type="anime_series",
            metadata_parent_id="36039",
            metadata_sources=("https://myanimelist.net/anime/36039",),
        ),
    ]

    rows = build_unmapped_imdb_override_rows(entries)

    assert rows == [
        {
            "netflix_original_title": "A.I.C.O.: Awakening",
            "netflix_title": "A.I.C.O.",
            "season_name": "",
            "netflix_episode_title": "Awakening",
            "expected_type": "",
            "title": "A.I.C.O. Incarnation",
            "year": "2018",
            "source_id": "",
            "episode_title": "",
            "found_source": "anime",
            "had_override": "",
        }
    ]


def test_build_unmapped_imdb_override_rows_uses_override_imdb_metadata_when_entry_is_unresolved():
    entries = [
        NetflixHistoryEntry(
            raw_title="DAN DA DAN: Season 1: To a Kinder World",
            watched_at=datetime(2026, 1, 1),
            parsed=ParsedNetflixTitle(
                raw_title="DAN DA DAN: Season 1: To a Kinder World",
                title="DAN DA DAN",
                media_kind="series",
                season=1,
                season_title="Season 1",
                episode_title="To a Kinder World",
                is_explicit_series=True,
            ),
            media_kind="series",
            resolved_title="DAN DA DAN",
            expected_type="series",
        ),
    ]

    overrides = load_episode_title_overrides(str(DEFAULT_EPISODE_TITLE_OVERRIDES_FILE))
    rows = build_unmapped_imdb_override_rows(entries, overrides=overrides)

    assert rows == [
        {
            "netflix_original_title": "DAN DA DAN: Season 1: To a Kinder World",
            "netflix_title": "DAN DA DAN",
            "season_name": "Season 1",
            "netflix_episode_title": "To a Kinder World",
            "expected_type": "series",
            "title": "Dandadan",
            "year": "2024",
            "source_id": "tt30217403",
            "episode_title": "Yasashii sekai e",
            "found_source": "imdb",
            "had_override": "yes",
        }
    ]


def test_summarize_unmapped_imdb_override_rows_counts_override_breakdown():
    stats = summarize_unmapped_imdb_override_rows(
        [
            {
                "netflix_original_title": "Known Show: Missing Episode",
                "netflix_title": "Known Show",
                "season_name": "",
                "netflix_episode_title": "Missing Episode",
                "expected_type": "series",
                "title": "",
                "year": "",
                "source_id": "tt1234567",
                "episode_title": "",
                "found_source": "imdb",
                "had_override": "yes",
            },
            {
                "netflix_original_title": "Unknown Show",
                "netflix_title": "Unknown Show",
                "season_name": "",
                "netflix_episode_title": "",
                "expected_type": "movie",
                "title": "",
                "year": "",
                "source_id": "",
                "episode_title": "",
                "found_source": "",
                "had_override": "",
            },
        ]
    )

    assert stats == {
        "total": 2,
        "with_override": 1,
        "without_override": 1,
    }


def test_exported_unmapped_csv_round_trips_as_override_input_when_filled_in():
    entries = [
        NetflixHistoryEntry(
            raw_title="Roundtrip Show: Season 1: Missing Episode",
            watched_at=datetime(2026, 1, 2),
            parsed=ParsedNetflixTitle(
                raw_title="Roundtrip Show: Season 1: Missing Episode",
                title="Roundtrip Show",
                media_kind="series",
                season=1,
                season_title="Season 1",
                episode_title="Missing Episode",
                is_explicit_series=True,
            ),
            media_kind="series",
            resolved_title="Roundtrip Show",
            expected_type="series",
            metadata_type="tv",
            metadata_parent_id="tt7654321",
            metadata_sources=("imdb",),
        ),
    ]

    with TemporaryDirectory() as temp_dir:
        override_path = Path(temp_dir) / "unmapped_roundtrip.csv"
        export_unmapped_imdb_overrides(entries, str(override_path))

        with override_path.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))

        assert rows == [
            {
                "netflix_original_title": "Roundtrip Show: Season 1: Missing Episode",
                "netflix_title": "Roundtrip Show",
                "season_name": "Season 1",
                "netflix_episode_title": "Missing Episode",
                "expected_type": "series",
                "title": "",
                "year": "",
                "source_id": "tt7654321",
                "episode_title": "",
                "found_source": "imdb",
                "had_override": "",
            }
        ]

        rows[0]["title"] = "Roundtrip Canonical Show"
        rows[0]["year"] = "2024"
        rows[0]["episode_title"] = "Roundtrip Canonical Episode"

        with override_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

        overrides = load_episode_title_overrides(str(override_path))

    analyzer = NetflixWatchStatusAnalyzer(metadata_manager=None, episode_title_overrides=overrides)
    parsed = entries[0].parsed

    resolved = analyzer._classify_entry(parsed, prefix_counts={})

    assert resolved[0] == "series"
    assert resolved[1] == "Roundtrip Canonical Show"
    assert resolved[2] == 2024
    assert resolved[6] == "tt7654321"
    assert analyzer._get_episode_title_override(parsed, resolved[1]) == "Roundtrip Canonical Episode"


def test_row_watch_state_uses_progress_for_series_and_seasons():
    assert _row_watch_state(WatchTableRow(level=0, title="Show", item_type="series", episode="0/13")) == "unwatched"
    assert _row_watch_state(WatchTableRow(level=1, title="Season 1", item_type="season", episode="5/13")) == "partial"
    assert _row_watch_state(WatchTableRow(level=1, title="Season 1", item_type="season", episode="13/13")) == "watched"
    assert _row_watch_state(WatchTableRow(level=2, title="Episode 1", item_type="episode", views="0")) == "unwatched"


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