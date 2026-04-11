import argparse
import csv
import importlib
import json
import os
import sys
import re
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from netflix_title_parser import ParsedNetflixTitle, parse_netflix_title

tqdm_progress: Any

try:
    tqdm_progress = importlib.import_module("tqdm").tqdm
except ImportError:
    class _TqdmProgressFallback:
        def __init__(self, iterable=None, total=None, desc=None, unit=None, disable=False, file=None, **kwargs):
            self.iterable = iterable
            self.total = total or (len(iterable) if iterable is not None else 0)
            self.desc = desc
            self.unit = unit
            self.disable = disable
            self.file = file or sys.stderr
            self.current = 0
            if not self.disable and self.desc:
                print(f"{self.desc}...", file=self.file)

        def __iter__(self):
            if self.iterable is None:
                return iter(())
            for item in self.iterable:
                yield item
                self.update(1)

        def update(self, n=1):
            self.current += n
            if self.disable or self.total <= 0:
                return
            step = max(1, self.total // 10)
            if self.current % step == 0 or self.current == self.total:
                percent = (self.current / self.total) * 100
                print(
                    f"{self.desc or 'Progress'}: {self.current}/{self.total} ({percent:.1f}%)",
                    file=self.file,
                )

        def close(self):
            return None

    tqdm_progress = _TqdmProgressFallback


try:
    video_optimizer_dir = os.path.join(os.path.dirname(__file__), "video-optimizer-v2")
    if video_optimizer_dir not in sys.path:
        sys.path.append(video_optimizer_dir)

    AnimeDataProvider = importlib.import_module("anime_metadata").AnimeDataProvider
    IMDbDataProvider = importlib.import_module("imdb_metadata").IMDbDataProvider
    MetadataManagerClass = importlib.import_module("metadata_provider").MetadataManager

    METADATA_MANAGER: Any = None

    def get_metadata_manager() -> Any:
        global METADATA_MANAGER
        if MetadataManagerClass is None:
            return None
        if METADATA_MANAGER is None:
            anime_provider = AnimeDataProvider()
            imdb_provider = IMDbDataProvider()
            METADATA_MANAGER = MetadataManagerClass([anime_provider, imdb_provider])
        return METADATA_MANAGER

except ImportError:
    MetadataManagerClass = None

    def get_metadata_manager() -> Any:
        return None


DATE_FORMATS = (
    "%m/%d/%y",
    "%m/%d/%Y",
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%d/%m/%y",
    "%d/%m/%Y",
)
SERIES_METADATA_TYPES = {"tv", "anime_series"}
MOVIE_METADATA_TYPES = {"movie", "anime_movie"}
DEFAULT_TABLE_COLUMNS = ("title", "year", "season", "season_title", "episode", "episode_title", "views")
TABLE_COLUMN_DEFINITIONS = {
    "title": {"header": "Title", "align": "left", "max_width": 38},
    "year": {"header": "Year", "align": "right", "max_width": 10},
    "season": {"header": "Season", "align": "right", "max_width": 6},
    "season_title": {"header": "Season Title", "align": "left", "max_width": 18},
    "episode": {"header": "Episode", "align": "right", "max_width": 7},
    "episode_title": {"header": "Episode Title", "align": "left", "max_width": 34},
    "views": {"header": "Views", "align": "left", "max_width": 48},
}
BROKEN_HISTORY_TITLE_RE = re.compile(
    r"^:\s*(?:episode\s+\d+|chapter\s+\d+|\d+(?:st|nd|rd|th)\b.*)$",
    re.IGNORECASE,
)
DATE_LIKE_TITLE_RE = re.compile(r"^\d{1,4}[/-]\d{1,2}[/-]\d{1,4}$")


def iter_progress(iterable, *, total: Optional[int] = None, desc: str = "Progress", unit: str = "item"):
    return tqdm_progress(iterable, total=total, desc=desc, unit=unit, file=sys.stderr)


def should_reject_history_title(raw_title: str) -> bool:
    cleaned = raw_title.strip()
    if not cleaned:
        return True
    if BROKEN_HISTORY_TITLE_RE.match(cleaned):
        return True
    if DATE_LIKE_TITLE_RE.match(cleaned):
        return True
    return False


def parse_history_date(raw_date: str) -> Optional[datetime]:
    cleaned = raw_date.strip()
    if not cleaned:
        return None

    for date_format in DATE_FORMATS:
        try:
            return datetime.strptime(cleaned, date_format)
        except ValueError:
            continue

    try:
        return datetime.fromisoformat(cleaned)
    except ValueError:
        return None


def _normalize_lookup_text(text: Optional[str]) -> str:
    if not text:
        return ""
    return re.sub(r"[^a-z0-9]+", " ", text.casefold()).strip()


def _metadata_match_is_compatible(
    query: str,
    raw_title: str,
    inferred_series_title: Optional[str],
    resolved_kind: str,
    resolved_title: Optional[str],
) -> bool:
    normalized_resolved = _normalize_lookup_text(resolved_title)
    normalized_query = _normalize_lookup_text(query)
    normalized_raw = _normalize_lookup_text(raw_title)
    normalized_inferred = _normalize_lookup_text(inferred_series_title)

    if normalized_inferred:
        if resolved_kind == "series":
            return normalized_resolved == normalized_inferred or normalized_inferred in normalized_resolved
        return False

    if not normalized_resolved:
        return False

    return (
        normalized_resolved == normalized_query
        or normalized_resolved == normalized_raw
        or normalized_resolved in normalized_query
        or normalized_query in normalized_resolved
        or normalized_resolved in normalized_raw
        or normalized_raw in normalized_resolved
    )


@dataclass
class NetflixHistoryEntry:
    raw_title: str
    watched_at: datetime
    parsed: ParsedNetflixTitle
    media_kind: str
    resolved_title: str
    metadata_type: Optional[str] = None
    metadata_provider: Any = field(default=None, repr=False, compare=False)
    metadata_parent_id: Optional[str] = None
    resolved_season: Optional[int] = None
    resolved_episode: Optional[int] = None
    resolved_episode_title: Optional[str] = None
    resolved_title_year: Optional[int] = None
    resolved_episode_year: Optional[int] = None
    resolved_total_seasons: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "raw_title": self.raw_title,
            "watched_at": self.watched_at.date().isoformat(),
            "parsed": asdict(self.parsed),
            "media_kind": self.media_kind,
            "resolved_title": self.resolved_title,
            "metadata_type": self.metadata_type,
            "metadata_parent_id": self.metadata_parent_id,
            "resolved_season": self.resolved_season,
            "resolved_episode": self.resolved_episode,
            "resolved_episode_title": self.resolved_episode_title,
            "resolved_title_year": self.resolved_title_year,
            "resolved_episode_year": self.resolved_episode_year,
            "resolved_total_seasons": self.resolved_total_seasons,
        }


@dataclass
class WatchTableRow:
    level: int
    title: str
    year: str = ""
    season: str = ""
    season_title: str = ""
    episode: str = ""
    episode_title: str = ""
    views: str = ""


@dataclass
class MovieWatchStatus:
    title: str
    watch_count: int = 0
    metadata_type: Optional[str] = None
    first_watched: Optional[datetime] = None
    last_watched: Optional[datetime] = None

    def add_entry(self, entry: NetflixHistoryEntry) -> None:
        self.watch_count += 1
        self.metadata_type = self.metadata_type or entry.metadata_type
        self.first_watched = entry.watched_at if self.first_watched is None else min(self.first_watched, entry.watched_at)
        self.last_watched = entry.watched_at if self.last_watched is None else max(self.last_watched, entry.watched_at)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "title": self.title,
            "watch_count": self.watch_count,
            "metadata_type": self.metadata_type,
            "first_watched": self.first_watched.date().isoformat() if self.first_watched else None,
            "last_watched": self.last_watched.date().isoformat() if self.last_watched else None,
        }


@dataclass
class SeriesWatchStatus:
    title: str
    watch_count: int = 0
    metadata_type: Optional[str] = None
    seasons: set[int] = field(default_factory=set)
    episode_keys: set[Tuple[Optional[int], Optional[int], Optional[str]]] = field(default_factory=set)
    first_watched: Optional[datetime] = None
    last_watched: Optional[datetime] = None

    def add_entry(self, entry: NetflixHistoryEntry) -> None:
        self.watch_count += 1
        self.metadata_type = self.metadata_type or entry.metadata_type
        season_number = _entry_season(entry)
        if season_number is not None:
            self.seasons.add(season_number)
        episode_label = (entry.parsed.episode_title or "").casefold() or None
        if _entry_episode(entry) is None and episode_label is None:
            episode_label = entry.raw_title.casefold()
        self.episode_keys.add(
            (
                _entry_season(entry),
                _entry_episode(entry),
                episode_label,
            )
        )
        self.first_watched = entry.watched_at if self.first_watched is None else min(self.first_watched, entry.watched_at)
        self.last_watched = entry.watched_at if self.last_watched is None else max(self.last_watched, entry.watched_at)

    @property
    def unique_episode_count(self) -> int:
        return len(self.episode_keys)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "title": self.title,
            "watch_count": self.watch_count,
            "unique_episode_count": self.unique_episode_count,
            "metadata_type": self.metadata_type,
            "seasons": sorted(self.seasons),
            "first_watched": self.first_watched.date().isoformat() if self.first_watched else None,
            "last_watched": self.last_watched.date().isoformat() if self.last_watched else None,
        }


class NetflixWatchStatusAnalyzer:
    def __init__(self, metadata_manager: Any = None):
        self.metadata_manager = metadata_manager
        self._metadata_cache: Dict[Tuple[str, Optional[str]], Tuple[Optional[str], Optional[str], Optional[int], Optional[int], Optional[str], Any, Optional[str]]] = {}

    def load_entries(self, csv_path: str) -> List[NetflixHistoryEntry]:
        raw_entries: List[Tuple[str, datetime, ParsedNetflixTitle]] = []

        with open(csv_path, "r", encoding="utf-8-sig", newline="") as handle:
            rows = list(csv.DictReader(handle))

        for row in iter_progress(rows, total=len(rows), desc="Reading Netflix history", unit="row"):
            raw_title = (row.get("Title") or "").strip()
            raw_date = (row.get("Date") or "").strip()
            watched_at = parse_history_date(raw_date)
            if should_reject_history_title(raw_title) or watched_at is None:
                continue

            parsed = parse_netflix_title(raw_title)
            raw_entries.append((raw_title, watched_at, parsed))

        prefix_counts = self._build_prefix_counts(raw_entries)
        standalone_entries, episodic_entries = self._split_classification_candidates(raw_entries, prefix_counts)
        entries: List[NetflixHistoryEntry] = []

        entries.extend(
            self._classify_entries_batch(
                standalone_entries,
                prefix_counts,
                desc="Classifying standalone titles",
            )
        )
        entries.extend(
            self._classify_entries_batch(
                episodic_entries,
                prefix_counts,
                desc="Classifying episodic titles",
            )
        )

        entries.sort(key=lambda entry: (entry.watched_at, entry.raw_title.casefold()))

        return entries

    def _split_classification_candidates(
        self,
        raw_entries: List[Tuple[str, datetime, ParsedNetflixTitle]],
        prefix_counts: Dict[str, int],
    ) -> Tuple[List[Tuple[str, datetime, ParsedNetflixTitle]], List[Tuple[str, datetime, ParsedNetflixTitle]]]:
        standalone_entries: List[Tuple[str, datetime, ParsedNetflixTitle]] = []
        episodic_entries: List[Tuple[str, datetime, ParsedNetflixTitle]] = []

        for raw_entry in raw_entries:
            _, _, parsed = raw_entry
            inferred_series_title = self._infer_series_title(parsed, prefix_counts)
            is_episodic_candidate = (
                parsed.is_explicit_series
                or parsed.episode is not None
                or parsed.episode_title is not None
                or inferred_series_title is not None
            )
            if is_episodic_candidate:
                episodic_entries.append(raw_entry)
            else:
                standalone_entries.append(raw_entry)

        return standalone_entries, episodic_entries

    def _classify_entries_batch(
        self,
        raw_entries: List[Tuple[str, datetime, ParsedNetflixTitle]],
        prefix_counts: Dict[str, int],
        desc: str,
    ) -> List[NetflixHistoryEntry]:
        if not raw_entries:
            print(f"{desc}: 0 entries", file=sys.stderr)
            return []

        started_at = time.perf_counter()
        entries: List[NetflixHistoryEntry] = []
        for raw_title, watched_at, parsed in iter_progress(
            raw_entries,
            total=len(raw_entries),
            desc=desc,
            unit="entry",
        ):
            media_kind, resolved_title, resolved_title_year, resolved_total_seasons, metadata_type, metadata_provider, metadata_parent_id = self._classify_entry(parsed, prefix_counts)
            resolved_season, resolved_episode, resolved_episode_title, resolved_episode_year = self._resolve_episode_metadata(
                parsed=parsed,
                media_kind=media_kind,
                metadata_type=metadata_type,
                metadata_provider=metadata_provider,
                metadata_parent_id=metadata_parent_id,
                resolved_title=resolved_title,
                resolved_total_seasons=resolved_total_seasons,
            )
            entries.append(
                NetflixHistoryEntry(
                    raw_title=raw_title,
                    watched_at=watched_at,
                    parsed=parsed,
                    media_kind=media_kind,
                    resolved_title=resolved_title,
                    metadata_type=metadata_type,
                    metadata_provider=metadata_provider,
                    metadata_parent_id=metadata_parent_id,
                    resolved_season=resolved_season,
                    resolved_episode=resolved_episode,
                    resolved_episode_title=resolved_episode_title,
                    resolved_title_year=resolved_title_year,
                    resolved_episode_year=resolved_episode_year,
                    resolved_total_seasons=resolved_total_seasons,
                )
            )

        elapsed = time.perf_counter() - started_at
        rate = len(raw_entries) / elapsed if elapsed > 0 else 0.0
        print(f"{desc} completed: {len(raw_entries)} entries in {elapsed:.2f}s ({rate:.1f} entries/s)", file=sys.stderr)
        return entries

    def _build_prefix_counts(
        self, raw_entries: List[Tuple[str, datetime, ParsedNetflixTitle]]
    ) -> Dict[str, int]:
        prefix_to_titles: Dict[str, set[str]] = {}
        for raw_title, _, parsed in raw_entries:
            if parsed.is_explicit_series or ":" not in raw_title:
                continue

            tokens = [token.strip() for token in raw_title.split(":") if token.strip()]
            for index in range(1, len(tokens)):
                prefix = ": ".join(tokens[:index]).strip()
                if not prefix:
                    continue
                prefix_to_titles.setdefault(prefix, set()).add(raw_title)

        return {prefix: len(titles) for prefix, titles in prefix_to_titles.items()}

    def _infer_series_title(self, parsed: ParsedNetflixTitle, prefix_counts: Dict[str, int]) -> Optional[str]:
        if parsed.is_explicit_series or ":" not in parsed.raw_title:
            return None

        tokens = [token.strip() for token in parsed.raw_title.split(":") if token.strip()]
        best_match = None
        for index in range(1, len(tokens)):
            prefix = ": ".join(tokens[:index]).strip()
            if prefix_counts.get(prefix, 0) < 2:
                continue
            best_match = prefix

        return best_match

    def analyze(self, entries: List[NetflixHistoryEntry]) -> Dict[str, Any]:
        movies: Dict[str, MovieWatchStatus] = {}
        series: Dict[str, SeriesWatchStatus] = {}

        for entry in iter_progress(entries, total=len(entries), desc="Aggregating watch status", unit="entry"):
            if entry.media_kind == "series":
                key = entry.resolved_title.casefold()
                if key not in series:
                    series[key] = SeriesWatchStatus(title=entry.resolved_title)
                series[key].add_entry(entry)
                continue

            key = entry.resolved_title.casefold()
            if key not in movies:
                movies[key] = MovieWatchStatus(title=entry.resolved_title)
            movies[key].add_entry(entry)

        return {
            "summary": {
                "entries": len(entries),
                "unique_movies": len(movies),
                "unique_series": len(series),
            },
            "movies": [movie.to_dict() for movie in sorted(movies.values(), key=lambda item: item.title.casefold())],
            "series": [show.to_dict() for show in sorted(series.values(), key=lambda item: item.title.casefold())],
            "entries_data": [entry.to_dict() for entry in entries],
        }

    def _resolve_episode_metadata(
        self,
        parsed: ParsedNetflixTitle,
        media_kind: str,
        metadata_type: Optional[str],
        metadata_provider: Any,
        metadata_parent_id: Optional[str],
        resolved_title: str,
        resolved_total_seasons: Optional[int],
    ) -> Tuple[Optional[int], Optional[int], Optional[str], Optional[int]]:
        resolved_season = parsed.season
        resolved_episode = parsed.episode
        resolved_episode_title = parsed.episode_title
        resolved_episode_year: Optional[int] = None

        if (
            media_kind != "series"
            or metadata_type != "tv"
            or metadata_provider is None
            or metadata_parent_id is None
        ):
            return resolved_season, resolved_episode, resolved_episode_title, resolved_episode_year

        lookup_season = resolved_season
        if lookup_season is None and resolved_episode is not None and resolved_total_seasons == 1:
            lookup_season = 1

        if (
            lookup_season is not None
            and resolved_episode is not None
            and not resolved_episode_title
            and hasattr(metadata_provider, "get_episode_info")
        ):
            try:
                episode_info = metadata_provider.get_episode_info(
                    metadata_parent_id,
                    lookup_season,
                    resolved_episode,
                )
            except Exception:
                episode_info = None

            if episode_info is not None and episode_info.title:
                resolved_season = episode_info.season
                resolved_episode_title = episode_info.title
                resolved_episode_year = episode_info.year

        if resolved_episode is not None or not hasattr(metadata_provider, "find_episode_by_title"):
            return resolved_season, resolved_episode, resolved_episode_title, resolved_episode_year

        episode_title = self._derive_episode_title_for_lookup(parsed, resolved_title)
        if not episode_title:
            return resolved_season, resolved_episode, resolved_episode_title, resolved_episode_year

        try:
            episode_info = metadata_provider.find_episode_by_title(
                metadata_parent_id,
                episode_title,
                season=resolved_season,
            )
        except Exception:
            return resolved_season, resolved_episode, resolved_episode_title, resolved_episode_year

        if episode_info is None:
            return resolved_season, resolved_episode, resolved_episode_title, resolved_episode_year

        return episode_info.season, episode_info.episode, episode_info.title or resolved_episode_title, episode_info.year

    def _derive_episode_title_for_lookup(self, parsed: ParsedNetflixTitle, resolved_title: str) -> Optional[str]:
        if parsed.episode_title:
            return parsed.episode_title

        if parsed.episode is not None:
            return None

        prefix = resolved_title.strip()
        raw_title = parsed.raw_title.strip()
        if prefix and raw_title.startswith(prefix):
            suffix = raw_title[len(prefix):].lstrip(" :")
            if suffix:
                if parsed.season_title and suffix.startswith(parsed.season_title):
                    suffix = suffix[len(parsed.season_title):].lstrip(" :")
                return suffix or None
        return None

    def _classify_entry(
        self, parsed: ParsedNetflixTitle, prefix_counts: Dict[str, int]
    ) -> Tuple[str, str, Optional[int], Optional[int], Optional[str], Any, Optional[str]]:
        default_kind = "series" if parsed.is_explicit_series else parsed.media_kind
        default_title = parsed.title if parsed.is_explicit_series else parsed.raw_title
        inferred_series_title = self._infer_series_title(parsed, prefix_counts)

        if self.metadata_manager is None:
            if inferred_series_title:
                return "series", inferred_series_title, None, None, None, None, None
            return default_kind, default_title, None, None, None, None, None

        queries: List[str] = []
        if parsed.is_explicit_series:
            queries.append(parsed.title)
        else:
            if inferred_series_title:
                queries.append(inferred_series_title)
            queries.append(parsed.raw_title)
            if parsed.title != parsed.raw_title:
                queries.append(parsed.title)

        for query in queries:
            preferred_type = "tv" if (
                parsed.is_explicit_series
                or parsed.episode is not None
                or parsed.episode_title is not None
                or inferred_series_title is not None
            ) else None
            resolved_kind, resolved_title, resolved_title_year, resolved_total_seasons, metadata_type, metadata_provider, metadata_parent_id = self._lookup_metadata(
                query,
                preferred_type=preferred_type,
            )
            if resolved_kind is None:
                continue
            if not _metadata_match_is_compatible(
                query=query,
                raw_title=parsed.raw_title,
                inferred_series_title=inferred_series_title,
                resolved_kind=resolved_kind,
                resolved_title=resolved_title,
            ):
                continue
            if parsed.is_explicit_series and resolved_kind == "movie":
                return "series", parsed.title or resolved_title or default_title, resolved_title_year, resolved_total_seasons, metadata_type, metadata_provider, metadata_parent_id
            return resolved_kind, resolved_title or default_title, resolved_title_year, resolved_total_seasons, metadata_type, metadata_provider, metadata_parent_id

        if inferred_series_title:
            return "series", inferred_series_title, None, None, None, None, None

        return default_kind, default_title, None, None, None, None, None

    def _lookup_metadata(
        self,
        query: str,
        preferred_type: Optional[str] = None,
    ) -> Tuple[Optional[str], Optional[str], Optional[int], Optional[int], Optional[str], Any, Optional[str]]:
        cache_key = query.casefold().strip()
        if not cache_key:
            return None, None, None, None, None, None, None

        metadata_cache_key = (cache_key, preferred_type)
        if metadata_cache_key in self._metadata_cache:
            return self._metadata_cache[metadata_cache_key]

        try:
            match = self.metadata_manager.find_title(query, preferred_type=preferred_type)
        except Exception:
            self._metadata_cache[metadata_cache_key] = (None, None, None, None, None, None, None)
            return None, None, None, None, None, None, None

        if not match or not match[0]:
            self._metadata_cache[metadata_cache_key] = (None, None, None, None, None, None, None)
            return None, None, None, None, None, None, None

        title_info = match[0]
        provider = match[1] if len(match) > 1 else None
        metadata_type = getattr(title_info, "type", None)
        resolved_total_seasons = getattr(title_info, "total_seasons", None)
        if metadata_type in SERIES_METADATA_TYPES:
            media_kind = "series"
        elif metadata_type in MOVIE_METADATA_TYPES:
            media_kind = "movie"
        else:
            media_kind = None

        resolved_title = getattr(title_info, "title", query)
        resolved_title_year = getattr(title_info, "year", None) or getattr(title_info, "start_year", None)
        metadata_parent_id = getattr(title_info, "id", None)
        self._metadata_cache[metadata_cache_key] = (media_kind, resolved_title, resolved_title_year, resolved_total_seasons, metadata_type, provider, metadata_parent_id)
        return media_kind, resolved_title, resolved_title_year, resolved_total_seasons, metadata_type, provider, metadata_parent_id


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Read a Netflix viewing-history CSV and list unique watched movies and series."
    )
    parser.add_argument("csv_path", help="Path to NetflixViewingHistory.csv")
    parser.add_argument(
        "--no-metadata",
        action="store_true",
        help="Skip IMDb/anime metadata lookups and rely only on title parsing.",
    )
    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument(
        "--json",
        action="store_true",
        help="Print JSON instead of the text summary.",
    )
    output_group.add_argument(
        "--table",
        action="store_true",
        help="Print a treegrid-style table grouped by title, season, and episode.",
    )
    parser.add_argument(
        "--columns",
        default=",".join(DEFAULT_TABLE_COLUMNS),
        help=(
            "Comma-separated table columns for --table. "
            f"Available: {', '.join(TABLE_COLUMN_DEFINITIONS.keys())}."
        ),
    )
    return parser


def visible_text_width(text: str) -> int:
    return len(text)


def pad_console_cell(text: str, width: int, align: str) -> str:
    padding = max(0, width - visible_text_width(text))
    if align == "right":
        return f"{' ' * padding}{text}"
    return f"{text}{' ' * padding}"


def truncate_console_text(text: str, max_width: int) -> str:
    if max_width <= 0:
        return ""
    if len(text) <= max_width:
        return text
    if max_width <= 3:
        return text[:max_width]
    return f"{text[:max_width - 3]}..."


def safe_write_line(text: str = "") -> None:
    try:
        print(text)
    except BrokenPipeError:
        raise SystemExit(0)
    except OSError as exc:
        if getattr(exc, "errno", None) == 22:
            raise SystemExit(0)
        raise
    except UnicodeEncodeError:
        encoding = sys.stdout.encoding or "utf-8"
        try:
            sys.stdout.buffer.write((text + "\n").encode(encoding, errors="replace"))
        except BrokenPipeError:
            raise SystemExit(0)
        except OSError as exc:
            if getattr(exc, "errno", None) == 22:
                raise SystemExit(0)
            raise


def _format_date_list(watched_at_values: List[datetime]) -> str:
    return ", ".join(value.date().isoformat() for value in sorted(watched_at_values))


def _format_leaf_views(watched_at_values: List[datetime]) -> str:
    count = len(watched_at_values)
    if count <= 0:
        return "0"
    return f"{count} ({_format_date_list(watched_at_values)})"


def _format_group_views(watched_at_values: List[datetime]) -> str:
    return ""


def _format_progress(watched_count: int, total_count: int) -> str:
    if total_count <= 0:
        return ""
    return f"{watched_count}/{total_count}"


def parse_table_columns(raw_columns: str) -> List[str]:
    columns: List[str] = []
    seen: set[str] = set()
    for part in raw_columns.split(","):
        column = part.strip().lower()
        if not column or column in seen:
            continue
        if column not in TABLE_COLUMN_DEFINITIONS:
            available = ", ".join(TABLE_COLUMN_DEFINITIONS.keys())
            raise ValueError(f"Unknown table column '{column}'. Available columns: {available}")
        columns.append(column)
        seen.add(column)

    if not columns:
        raise ValueError("At least one table column must be selected")

    return columns


def _entry_season(entry: NetflixHistoryEntry) -> Optional[int]:
    return entry.resolved_season if entry.resolved_season is not None else entry.parsed.season


def _entry_episode(entry: NetflixHistoryEntry) -> Optional[int]:
    return entry.resolved_episode if entry.resolved_episode is not None else entry.parsed.episode


def _entry_title_year(entry: NetflixHistoryEntry) -> Optional[int]:
    return entry.resolved_title_year


def _entry_episode_year(entry: NetflixHistoryEntry) -> Optional[int]:
    return entry.resolved_episode_year


def _list_metadata_episodes(title_entries: List[NetflixHistoryEntry]) -> List[Any]:
    for entry in title_entries:
        if (
            entry.metadata_type != "tv"
            or entry.metadata_provider is None
            or entry.metadata_parent_id is None
            or not hasattr(entry.metadata_provider, "list_episodes")
        ):
            continue
        try:
            return entry.metadata_provider.list_episodes(entry.metadata_parent_id)
        except Exception:
            return []
    return []


def _derive_episode_title(entry: NetflixHistoryEntry) -> str:
    if entry.resolved_episode_title:
        return entry.resolved_episode_title

    if entry.parsed.episode_title:
        return entry.parsed.episode_title

    if _entry_episode(entry) is not None:
        return f"Episode {_entry_episode(entry)}"

    prefix = entry.resolved_title.strip()
    raw_title = entry.raw_title.strip()
    if prefix and raw_title.startswith(prefix):
        suffix = raw_title[len(prefix):].lstrip(" :")
        if suffix:
            if entry.parsed.season_title and suffix.startswith(entry.parsed.season_title):
                suffix = suffix[len(entry.parsed.season_title):].lstrip(" :")
            return suffix or raw_title

    return raw_title


def _episode_sort_key(item: tuple[tuple[Optional[int], str], List[NetflixHistoryEntry]]) -> tuple[int, int, str]:
    (episode_number, episode_title_key), _ = item
    return (
        1 if episode_number is None else 0,
        episode_number or 0,
        episode_title_key,
    )


def _infer_missing_season_number(title_entries: List[NetflixHistoryEntry]) -> Optional[int]:
    season_numbers = sorted(
        season_number
        for season_number in (_entry_season(entry) for entry in title_entries)
        if season_number is not None
    )
    if not season_numbers:
        return None

    missing_numbers = [
        number
        for number in range(1, season_numbers[-1] + 1)
        if number not in season_numbers
    ]
    if len(missing_numbers) != 1:
        return None

    return missing_numbers[0]


def build_watch_table_rows(entries: List[NetflixHistoryEntry]) -> List[WatchTableRow]:
    title_groups: Dict[str, List[NetflixHistoryEntry]] = {}
    for entry in entries:
        title_groups.setdefault(entry.resolved_title.casefold(), []).append(entry)

    rows: List[WatchTableRow] = []
    sorted_title_groups = sorted(title_groups.items(), key=lambda item: item[1][0].resolved_title.casefold())
    for _, title_entries in iter_progress(
        sorted_title_groups,
        total=len(sorted_title_groups),
        desc="Building table rows",
        unit="title",
    ):
        title_entries = sorted(title_entries, key=lambda entry: (entry.watched_at, entry.raw_title.casefold()))
        title = title_entries[0].resolved_title
        title_kind = title_entries[0].media_kind
        watched_at_values = [entry.watched_at for entry in title_entries]

        if title_kind == "movie":
            rows.append(
                WatchTableRow(
                    level=0,
                    title=title,
                    year=str(_entry_title_year(title_entries[0]) or ""),
                    episode_title="",
                    views=_format_leaf_views(watched_at_values),
                )
            )
            continue

        rows.append(
            WatchTableRow(
                level=0,
                title=title,
                year=str(_entry_title_year(title_entries[0]) or ""),
                episode_title="",
                views=_format_group_views(watched_at_values),
            )
        )

        metadata_episodes = _list_metadata_episodes(title_entries)
        if metadata_episodes:
            watched_episode_groups: Dict[tuple[int, int], List[NetflixHistoryEntry]] = {}
            metadata_episode_keys = {
                (episode_info.season, episode_info.episode)
                for episode_info in metadata_episodes
                if episode_info.season is not None and episode_info.episode is not None
            }
            unmatched_entries: List[NetflixHistoryEntry] = []
            for entry in title_entries:
                season_number = _entry_season(entry)
                episode_number = _entry_episode(entry)
                key = (season_number, episode_number)
                if (
                    season_number is not None
                    and episode_number is not None
                    and key in metadata_episode_keys
                ):
                    watched_episode_groups.setdefault((season_number, episode_number), []).append(entry)
                else:
                    unmatched_entries.append(entry)

            rows[-1].episode = _format_progress(len(watched_episode_groups), len(metadata_episode_keys))

            direct_entries: List[NetflixHistoryEntry] = []
            extra_season_groups: Dict[tuple[Optional[int], str], List[NetflixHistoryEntry]] = {}
            for entry in unmatched_entries:
                season_number = _entry_season(entry)
                season_title = entry.parsed.season_title or ""
                if season_number is None and not season_title:
                    direct_entries.append(entry)
                    continue
                extra_season_groups.setdefault((season_number, season_title), []).append(entry)

            for entry in _build_episode_rows(direct_entries, level=1):
                rows.append(entry)

            season_title_overrides: Dict[int, str] = {}
            for entry in title_entries:
                season_number = _entry_season(entry)
                season_title = entry.parsed.season_title or ""
                if season_number is not None and season_title:
                    season_title_overrides.setdefault(season_number, season_title)

            metadata_season_map: Dict[int, List[Any]] = {}
            for episode_info in metadata_episodes:
                if episode_info.season is None or episode_info.episode is None:
                    continue
                metadata_season_map.setdefault(episode_info.season, []).append(episode_info)

            processed_extra_season_keys: set[tuple[Optional[int], str]] = set()
            for season_number in sorted(metadata_season_map):
                display_season_title = season_title_overrides.get(season_number) or f"Season {season_number}"
                watched_count = 0
                season_rows: List[WatchTableRow] = []
                for episode_info in sorted(metadata_season_map[season_number], key=lambda item: item.episode):
                    key = (season_number, episode_info.episode)
                    group_entries = watched_episode_groups.get(key)
                    if group_entries:
                        watched_count += 1
                        watched_at_values = [entry.watched_at for entry in group_entries]
                        first_entry = sorted(group_entries, key=lambda entry: (entry.watched_at, entry.raw_title.casefold()))[0]
                        episode_title = _derive_episode_title(first_entry)
                        season_rows.append(
                            WatchTableRow(
                                level=2,
                                title=episode_title,
                                year=str(_entry_episode_year(first_entry) or _entry_title_year(first_entry) or ""),
                                season=str(season_number),
                                season_title=display_season_title,
                                episode=str(episode_info.episode),
                                episode_title=episode_title,
                                views=_format_leaf_views(watched_at_values),
                            )
                        )
                        continue

                    synthetic_title = f"{episode_info.title or f'Episode {episode_info.episode}'} *"
                    season_rows.append(
                        WatchTableRow(
                            level=2,
                            title=synthetic_title,
                            year=str(episode_info.year or _entry_title_year(title_entries[0]) or ""),
                            season=str(season_number),
                            season_title=display_season_title,
                            episode=str(episode_info.episode),
                            episode_title=synthetic_title,
                            views="0",
                        )
                    )

                rows.append(
                    WatchTableRow(
                        level=1,
                        title=display_season_title,
                        season=str(season_number),
                        episode=_format_progress(watched_count, len(metadata_season_map[season_number])),
                        episode_title="",
                        views=_format_group_views([]),
                    )
                )
                rows.extend(season_rows)

                extra_key = (season_number, season_title_overrides.get(season_number, ""))
                if extra_key in extra_season_groups:
                    processed_extra_season_keys.add(extra_key)

            remaining_extra_seasons = [
                item for item in extra_season_groups.items() if item[0] not in processed_extra_season_keys
            ]
            for (season_number, season_title), season_entries in sorted(
                remaining_extra_seasons,
                key=lambda item: (
                    1 if item[0][0] is None else 0,
                    item[0][0] or 0,
                    item[0][1].casefold(),
                ),
            ):
                display_season_title = season_title or (f"Season {season_number}" if season_number is not None else "")
                rows.append(
                    WatchTableRow(
                        level=1,
                        title=display_season_title or title,
                        season=str(season_number) if season_number is not None else "",
                        episode_title="",
                        views=_format_group_views([entry.watched_at for entry in season_entries]),
                    )
                )
                for entry in _build_episode_rows(
                    season_entries,
                    level=2,
                    season_override=season_number,
                    season_title_override=display_season_title if display_season_title else None,
                ):
                    rows.append(entry)

            continue

        season_groups: Dict[tuple[Optional[int], str], List[NetflixHistoryEntry]] = {}
        direct_entries: List[NetflixHistoryEntry] = []
        for entry in title_entries:
            season_number = _entry_season(entry)
            season_title = entry.parsed.season_title or ""
            if season_number is None and not season_title:
                direct_entries.append(entry)
                continue
            season_groups.setdefault((season_number, season_title), []).append(entry)

        inferred_missing_season = None
        if direct_entries:
            inferred_missing_season = _infer_missing_season_number(title_entries)
            if inferred_missing_season is not None:
                inferred_season_title = f"Season {inferred_missing_season}"
                season_groups.setdefault((inferred_missing_season, inferred_season_title), []).extend(direct_entries)
                direct_entries = []

        for entry in _build_episode_rows(direct_entries, level=1):
            rows.append(entry)

        sorted_seasons = sorted(
            season_groups.items(),
            key=lambda item: (
                1 if item[0][0] is None else 0,
                item[0][0] or 0,
                item[0][1].casefold(),
            ),
        )
        for (season_number, season_title), season_entries in sorted_seasons:
            display_season_title = season_title or (f"Season {season_number}" if season_number is not None else "")
            rows.append(
                WatchTableRow(
                    level=1,
                    title=display_season_title or title,
                    season=str(season_number) if season_number is not None else "",
                    episode_title="",
                    views=_format_group_views([entry.watched_at for entry in season_entries]),
                )
            )
            season_override = season_number if season_number is not None else None
            season_title_override = display_season_title if display_season_title else None
            for entry in _build_episode_rows(
                season_entries,
                level=2,
                season_override=season_override,
                season_title_override=season_title_override,
            ):
                rows.append(entry)

    return rows


def _build_episode_rows(
    entries: List[NetflixHistoryEntry],
    level: int,
    season_override: Optional[int] = None,
    season_title_override: Optional[str] = None,
) -> List[WatchTableRow]:
    episode_groups: Dict[tuple[Optional[int], str], List[NetflixHistoryEntry]] = {}
    for entry in entries:
        episode_title = _derive_episode_title(entry)
        episode_groups.setdefault(
            (_entry_episode(entry), episode_title.casefold()),
            [],
        ).append(entry)

    rows: List[WatchTableRow] = []
    for (_, _), group_entries in sorted(episode_groups.items(), key=_episode_sort_key):
        watched_at_values = [entry.watched_at for entry in group_entries]
        first_entry = sorted(group_entries, key=lambda entry: (entry.watched_at, entry.raw_title.casefold()))[0]
        episode_title = _derive_episode_title(first_entry)
        episode_number = _entry_episode(first_entry)
        resolved_season = season_override if season_override is not None else _entry_season(first_entry)
        resolved_season_title = season_title_override if season_title_override is not None else (first_entry.parsed.season_title or "")
        rows.append(
            WatchTableRow(
                level=level,
                title=episode_title,
                year=str(_entry_episode_year(first_entry) or _entry_title_year(first_entry) or ""),
                season=str(resolved_season) if resolved_season is not None else "",
                season_title=resolved_season_title,
                episode=str(episode_number) if episode_number is not None else "",
                episode_title=episode_title,
                views=_format_leaf_views(watched_at_values),
            )
        )
    return rows


def render_watch_table(entries: List[NetflixHistoryEntry], selected_columns: List[str]) -> None:
    rows = build_watch_table_rows(entries)
    if not rows:
        return

    headers = [TABLE_COLUMN_DEFINITIONS[column]["header"] for column in selected_columns]
    alignments = [TABLE_COLUMN_DEFINITIONS[column]["align"] for column in selected_columns]
    max_widths = [TABLE_COLUMN_DEFINITIONS[column]["max_width"] for column in selected_columns]
    widths = [min(len(header), max_widths[index]) for index, header in enumerate(headers)]
    row_cells: List[List[str]] = []
    for row in rows:
        row_values = {
            "title": f"{'  ' * row.level}{row.title}",
            "year": row.year,
            "season": row.season,
            "season_title": row.season_title,
            "episode": row.episode,
            "episode_title": row.episode_title,
            "views": row.views,
        }
        cells = [row_values[column] for column in selected_columns]
        row_cells.append(cells)
        for index, cell in enumerate(cells):
            widths[index] = min(max(widths[index], len(cell)), max_widths[index])

    header_line = _render_table_line(headers, widths, alignments)
    separator_line = _render_table_separator(widths, alignments)
    safe_write_line(header_line)
    safe_write_line(separator_line)
    for cells in row_cells:
        safe_write_line(_render_table_line(cells, widths, alignments))


def _render_table_line(values: tuple[str, ...] | List[str], widths: tuple[int, ...] | List[int], alignments: tuple[str, ...] | List[str]) -> str:
    rendered_cells = []
    for index, value in enumerate(values):
        fitted = pad_console_cell(truncate_console_text(value, widths[index]), widths[index], alignments[index])
        rendered_cells.append(fitted)
    return f"| {' | '.join(rendered_cells)} |"


def _render_table_separator(widths: tuple[int, ...] | List[int], alignments: tuple[str, ...] | List[str]) -> str:
    segments: List[str] = []
    for width, alignment in zip(widths, alignments):
        segment_width = max(3, width)
        if alignment == "right":
            segments.append(f"{'-' * (segment_width - 1)}:")
        else:
            segments.append(f":{'-' * (segment_width - 1)}")
    return f"| {' | '.join(segments)} |"


def print_text_summary(results: Dict[str, Any]) -> None:
    summary = results["summary"]
    safe_write_line(f"Entries: {summary['entries']}")
    safe_write_line(f"Unique movies: {summary['unique_movies']}")
    safe_write_line(f"Unique series: {summary['unique_series']}")
    safe_write_line()

    safe_write_line("Movies")
    for movie in results["movies"]:
        safe_write_line(
            f"- {movie['title']} "
            f"(views: {movie['watch_count']}, last watched: {movie['last_watched']})"
        )

    safe_write_line()
    safe_write_line("Series")
    for show in results["series"]:
        season_text = ", ".join(str(season) for season in show["seasons"]) if show["seasons"] else "?"
        safe_write_line(
            f"- {show['title']} "
            f"(entries: {show['watch_count']}, unique episodes: {show['unique_episode_count']}, "
            f"seasons: {season_text}, last watched: {show['last_watched']})"
        )


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()

    try:
        selected_columns = parse_table_columns(args.columns)
    except ValueError as exc:
        parser.error(str(exc))
        return

    metadata_manager = None if args.no_metadata else get_metadata_manager()
    analyzer = NetflixWatchStatusAnalyzer(metadata_manager=metadata_manager)
    entries = analyzer.load_entries(args.csv_path)
    results = analyzer.analyze(entries)

    if args.json:
        print(json.dumps(results, indent=2))
        return

    if args.table:
        render_watch_table(entries, selected_columns)
        return

    print_text_summary(results)


if __name__ == "__main__":
    main()