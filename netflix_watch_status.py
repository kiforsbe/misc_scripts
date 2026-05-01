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
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

from netflix_title_parser import ParsedNetflixTitle, adapt_lookup_titles, parse_netflix_title

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
DEFAULT_TABLE_COLUMNS = ("title", "year", "season", "season_title", "episode", "episode_title", "views", "watch_dates")
TABLE_COLUMN_DEFINITIONS = {
    "title": {"header": "Title", "align": "left", "max_width": 38},
    "year": {"header": "Year", "align": "right", "max_width": 10},
    "source_id": {"header": "Source ID", "align": "left", "max_width": 14},
    "title_type": {"header": "Title Type", "align": "left", "max_width": 14},
    "runtime_minutes": {"header": "Runtime", "align": "right", "max_width": 8},
    "genres": {"header": "Genres", "align": "left", "max_width": 28},
    "average_rating": {"header": "Rating", "align": "center", "max_width": 6},
    "num_votes": {"header": "Votes", "align": "right", "max_width": 10},
    "season": {"header": "Season", "align": "right", "max_width": 6},
    "season_title": {"header": "Season Title", "align": "left", "max_width": 18},
    "episode": {"header": "Episode", "align": "right", "max_width": 7},
    "episode_title": {"header": "Episode Title", "align": "left", "max_width": 34},
    "views": {"header": "Views", "align": "right", "max_width": 7},
    "watch_dates": {"header": "Watch Dates", "align": "left", "max_width": 40},
}
BROKEN_HISTORY_TITLE_RE = re.compile(
    r"^:\s*(?:episode\s+\d+|chapter\s+\d+|\d+(?:st|nd|rd|th)\b.*)$",
    re.IGNORECASE,
)
DATE_LIKE_TITLE_RE = re.compile(r"^\d{1,4}[/-]\d{1,2}[/-]\d{1,4}$")
THUMBNAIL_CACHE_DIR = Path.home() / ".video_metadata_cache" / "netflix_watch_status"
THUMBNAIL_CACHE_FILE = THUMBNAIL_CACHE_DIR / "thumbnail_cache.json"
THUMBNAIL_CACHE_SUCCESS_TTL_SECONDS = 30 * 24 * 60 * 60
THUMBNAIL_CACHE_FAILURE_TTL_SECONDS = 3 * 24 * 60 * 60
UNMAPPED_IMDB_REPORT_SUFFIX = "_unmapped_imdb_titles.csv"
THUMBNAIL_META_KEYS = {
    "og:image",
    "og:image:url",
    "og:image:secure_url",
    "twitter:image",
    "twitter:image:src",
}
THUMBNAIL_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
DEFAULT_EPISODE_TITLE_OVERRIDES_FILE = Path(__file__).with_name("netflix_episode_title_overrides.csv")


def _configure_utf8_output() -> None:
    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        if stream is None or not hasattr(stream, "reconfigure"):
            continue
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            continue


def _repair_mojibake(text: str) -> str:
    suspicious_markers = ("Ã", "â€", "â€™", "â€œ", "â€\x9d", "Â")
    if not any(marker in text for marker in suspicious_markers):
        return text
    try:
        repaired = text.encode("cp1252").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text
    return repaired


def _clean_csv_text(value: Optional[str]) -> str:
    return _repair_mojibake((value or "").strip())


def _open_csv_with_fallback_encodings(path: Path):
    encodings = ("utf-8-sig", "cp1252", "latin-1")
    last_error: Optional[UnicodeDecodeError] = None
    for encoding in encodings:
        try:
            handle = path.open("r", encoding=encoding, newline="")
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
    raise UnicodeDecodeError("utf-8", b"", 0, 1, f"Unable to decode CSV file: {path}")


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


def _normalize_episode_title_override_key(raw_title: Optional[str]) -> str:
    return _normalize_episode_title_override_key_parts(raw_title)


def _normalize_episode_title_override_key_parts(
    title: Optional[str],
    season_name: Optional[str] = None,
    netflix_episode_title: Optional[str] = None,
) -> str:
    normalized_title = re.sub(r"\s+", " ", title or "").strip().casefold()
    if not normalized_title:
        return ""

    normalized_season_name = re.sub(r"\s+", " ", season_name or "").strip().casefold()
    normalized_episode_title = re.sub(r"\s+", " ", netflix_episode_title or "").strip().casefold()
    if not normalized_season_name and not normalized_episode_title:
        return normalized_title

    return "\x1f".join((normalized_title, normalized_season_name, normalized_episode_title))


@dataclass(frozen=True)
class NetflixTitleOverride:
    title: Optional[str] = None
    year: Optional[int] = None
    source_id: Optional[str] = None
    episode_title: Optional[str] = None


def _build_episode_title_override_lookup_keys(
    parsed: ParsedNetflixTitle,
    *,
    inferred_series_title: Optional[str] = None,
    derived_episode_title: Optional[str] = None,
) -> Tuple[str, ...]:
    candidates = (
        _normalize_episode_title_override_key_parts(parsed.title, parsed.season_title, parsed.episode_title),
        _normalize_episode_title_override_key_parts(parsed.title, parsed.season_title, derived_episode_title),
        _normalize_episode_title_override_key_parts(parsed.title, parsed.season_title),
        _normalize_episode_title_override_key(parsed.raw_title),
        _normalize_episode_title_override_key(parsed.title),
        _normalize_episode_title_override_key(inferred_series_title),
        _normalize_episode_title_override_key(parsed.episode_title),
        _normalize_episode_title_override_key(derived_episode_title),
    )
    return tuple(dict.fromkeys(candidate for candidate in candidates if candidate))


def _coerce_episode_title_override(raw_value: Any, *, source_label: str) -> NetflixTitleOverride:
    if isinstance(raw_value, NetflixTitleOverride):
        return NetflixTitleOverride(
            title=_clean_csv_text(raw_value.title) or None,
            year=raw_value.year,
            source_id=_clean_csv_text(raw_value.source_id) or None,
            episode_title=_clean_csv_text(raw_value.episode_title) or None,
        )

    if isinstance(raw_value, str):
        normalized_value = _clean_csv_text(raw_value)
        if not normalized_value:
            return NetflixTitleOverride()
        return NetflixTitleOverride(
            title=normalized_value,
            episode_title=normalized_value,
        )

    if not isinstance(raw_value, dict):
        raise ValueError(
            "Episode title override values must be strings or objects with title/year/source_id/episode_title fields: "
            f"{source_label}"
        )

    unknown_fields = sorted(set(raw_value.keys()) - {"title", "year", "source_id", "episode_title", "found_source"})
    if unknown_fields:
        raise ValueError(
            f"Episode title override contains unsupported fields {unknown_fields}: {source_label}"
        )

    title = raw_value.get("title")
    year = raw_value.get("year")
    source_id = raw_value.get("source_id")
    episode_title = raw_value.get("episode_title")
    if title is not None and not isinstance(title, str):
        raise ValueError(f"Episode title override field 'title' must be a string: {source_label}")
    if year is not None and not isinstance(year, int):
        raise ValueError(f"Episode title override field 'year' must be an integer: {source_label}")
    if source_id is not None and not isinstance(source_id, str):
        raise ValueError(f"Episode title override field 'source_id' must be a string: {source_label}")
    if episode_title is not None and not isinstance(episode_title, str):
        raise ValueError(f"Episode title override field 'episode_title' must be a string: {source_label}")

    return NetflixTitleOverride(
        title=_clean_csv_text(title) or None,
        year=year,
        source_id=_clean_csv_text(source_id) or None,
        episode_title=_clean_csv_text(episode_title) or None,
    )


def _load_episode_title_overrides_from_path(path: Path, *, required: bool) -> Dict[str, NetflixTitleOverride]:
    if not path.exists():
        if required:
            raise ValueError(f"Episode title override file not found: {path}")
        return {}

    try:
        with _open_csv_with_fallback_encodings(path) as handle:
            reader = csv.DictReader(handle)
            fieldnames = set(reader.fieldnames or ())
            required_columns = {"title", "year"}
            has_primary_key = "netflix_original_title" in fieldnames or "netflix_title" in fieldnames
            if not required_columns.issubset(fieldnames) or not has_primary_key:
                raise ValueError(
                    "Episode title override CSV must include columns "
                    f"{sorted(required_columns)} and one of ['netflix_original_title', 'netflix_title']: {path}. "
                    "Optional columns: ['episode_title', 'source_id', 'found_source', 'season_name', 'netflix_episode_title']"
                )

            overrides: Dict[str, NetflixTitleOverride] = {}
            for row in reader:
                raw_original_key = _clean_csv_text(row.get("netflix_original_title"))
                raw_split_key = _clean_csv_text(row.get("netflix_title"))
                normalized_key = _normalize_episode_title_override_key(raw_original_key)
                if not normalized_key:
                    normalized_key = _normalize_episode_title_override_key_parts(
                        raw_split_key,
                        _clean_csv_text(row.get("season_name")),
                        _clean_csv_text(row.get("netflix_episode_title")),
                    )
                raw_year = _clean_csv_text(row.get("year"))
                if raw_year:
                    try:
                        normalized_year = int(raw_year)
                    except ValueError as exc:
                        raise ValueError(
                            f"Episode title override year must be an integer: {path} ({raw_original_key or raw_split_key})"
                        ) from exc
                else:
                    normalized_year = None
                normalized_value = NetflixTitleOverride(
                    title=_clean_csv_text(row.get("title")) or None,
                    year=normalized_year,
                    source_id=_clean_csv_text(row.get("source_id")) or None,
                    episode_title=_clean_csv_text(row.get("episode_title")) or None,
                )
                if not normalized_key or (
                    not normalized_value.title
                    and normalized_value.year is None
                    and not normalized_value.source_id
                    and not normalized_value.episode_title
                ):
                    continue
                overrides[normalized_key] = normalized_value
    except csv.Error as exc:
        raise ValueError(f"Episode title override CSV is not valid: {path} ({exc})") from exc

    return overrides


def load_episode_title_overrides(override_path: Optional[str] = None) -> Dict[str, NetflixTitleOverride]:
    overrides = _load_episode_title_overrides_from_path(
        DEFAULT_EPISODE_TITLE_OVERRIDES_FILE,
        required=False,
    )
    if override_path:
        overrides.update(_load_episode_title_overrides_from_path(Path(override_path), required=True))
    return overrides


def _normalize_lookup_text(text: Optional[str]) -> str:
    if not text:
        return ""
    return re.sub(r"[^a-z0-9]+", " ", text.casefold()).strip()


def _starts_with_casefold(text: str, prefix: str) -> bool:
    if not prefix:
        return False
    return text[:len(prefix)].casefold() == prefix.casefold()


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

    def _matches_lookup(candidate_source: str) -> bool:
        lookup_candidates = adapt_lookup_titles(candidate_source, (resolved_title,))
        return any(_normalize_lookup_text(candidate) == normalized_resolved for candidate in lookup_candidates[1:])

    if normalized_inferred:
        if resolved_kind == "series":
            return (
                normalized_resolved == normalized_inferred
                or normalized_inferred in normalized_resolved
                or _matches_lookup(inferred_series_title or "")
            )
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
        or _matches_lookup(query)
        or _matches_lookup(raw_title)
    )


def _provider_name(provider: Any) -> str:
    if provider is None:
        return ""
    return type(provider).__name__


def _has_imdb_title_reference(metadata_parent_id: Optional[str], metadata_sources: Tuple[str, ...]) -> bool:
    if isinstance(metadata_parent_id, str) and re.fullmatch(r"tt\d+", metadata_parent_id):
        return True
    return any("imdb" in source.casefold() for source in metadata_sources)


def _merge_metadata_sources(*source_groups: Tuple[str, ...]) -> Tuple[str, ...]:
    merged_sources: List[str] = []
    for source_group in source_groups:
        for source in source_group:
            cleaned = str(source or "").strip()
            if cleaned and cleaned not in merged_sources:
                merged_sources.append(cleaned)
    return tuple(merged_sources)


def _build_imdb_enrichment_queries(query: str, resolved_title: Optional[str]) -> Tuple[str, ...]:
    candidates: List[str] = []
    for candidate in (query, resolved_title):
        cleaned = str(candidate or "").strip()
        if cleaned and cleaned.casefold() not in {value.casefold() for value in candidates}:
            candidates.append(cleaned)
    return tuple(candidates)


@dataclass
class NetflixHistoryEntry:
    raw_title: str
    watched_at: datetime
    parsed: ParsedNetflixTitle
    media_kind: str
    resolved_title: str
    expected_type: str = ""
    metadata_type: Optional[str] = None
    metadata_provider: Any = field(default=None, repr=False, compare=False)
    metadata_parent_id: Optional[str] = None
    resolved_season: Optional[int] = None
    resolved_episode: Optional[int] = None
    resolved_episode_title: Optional[str] = None
    resolved_episode_source_id: Optional[str] = None
    resolved_episode_rating: Optional[float] = None
    resolved_episode_votes: Optional[int] = None
    resolved_title_year: Optional[int] = None
    resolved_episode_year: Optional[int] = None
    resolved_total_seasons: Optional[int] = None
    metadata_average_rating: Optional[float] = None
    metadata_num_votes: Optional[int] = None
    metadata_runtime_minutes: Optional[int] = None
    metadata_genres: Tuple[str, ...] = field(default_factory=tuple)
    metadata_title_type: Optional[str] = None
    metadata_sources: Tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "raw_title": self.raw_title,
            "watched_at": self.watched_at.date().isoformat(),
            "parsed": asdict(self.parsed),
            "media_kind": self.media_kind,
            "expected_type": self.expected_type,
            "resolved_title": self.resolved_title,
            "metadata_type": self.metadata_type,
            "metadata_parent_id": self.metadata_parent_id,
            "resolved_season": self.resolved_season,
            "resolved_episode": self.resolved_episode,
            "resolved_episode_title": self.resolved_episode_title,
            "resolved_episode_source_id": self.resolved_episode_source_id,
            "resolved_episode_rating": self.resolved_episode_rating,
            "resolved_episode_votes": self.resolved_episode_votes,
            "resolved_title_year": self.resolved_title_year,
            "resolved_episode_year": self.resolved_episode_year,
            "resolved_total_seasons": self.resolved_total_seasons,
            "metadata_average_rating": self.metadata_average_rating,
            "metadata_num_votes": self.metadata_num_votes,
            "metadata_runtime_minutes": self.metadata_runtime_minutes,
            "metadata_genres": list(self.metadata_genres),
            "metadata_title_type": self.metadata_title_type,
            "metadata_sources": list(self.metadata_sources),
        }


@dataclass
class WatchTableRow:
    level: int
    title: str
    year: str = ""
    source_id: str = ""
    season: str = ""
    season_title: str = ""
    episode: str = ""
    episode_title: str = ""
    views: str = ""
    watch_dates: str = ""
    title_type: str = ""
    runtime_minutes: str = ""
    genres: str = ""
    average_rating: str = ""
    num_votes: str = ""
    item_type: str = ""
    row_id: str = ""
    parent_id: Optional[str] = None
    has_children: bool = False
    thumbnail_status: str = "not_requested"
    thumbnail_url: str = ""


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
    def __init__(self, metadata_manager: Any = None, episode_title_overrides: Optional[Dict[str, Any]] = None):
        self.metadata_manager = metadata_manager
        self._metadata_cache: Dict[Tuple[str, Optional[int], Optional[str]], Tuple[Any, ...]] = {}
        self._episode_title_overrides = {
            normalized_key: normalized_value
            for raw_key, raw_value in (episode_title_overrides or {}).items()
            for normalized_key, normalized_value in [
                (
                    _normalize_episode_title_override_key(raw_key),
                    _coerce_episode_title_override(
                        raw_value,
                        source_label=f"inline override ({raw_key})",
                    ),
                )
            ]
            if normalized_key and (
                normalized_value.title
                or normalized_value.year is not None
                or normalized_value.source_id
                or normalized_value.episode_title
            )
        }

    def _get_title_override(self, *titles: Optional[str]) -> Optional[NetflixTitleOverride]:
        merged_title: Optional[str] = None
        merged_year: Optional[int] = None
        merged_source_id: Optional[str] = None
        merged_episode_title: Optional[str] = None
        for title in titles:
            override = self._episode_title_overrides.get(_normalize_episode_title_override_key(title))
            if override is None:
                continue
            if merged_title is None and override.title:
                merged_title = override.title
            if merged_year is None and override.year is not None:
                merged_year = override.year
            if merged_source_id is None and override.source_id:
                merged_source_id = override.source_id
            if merged_episode_title is None and override.episode_title:
                merged_episode_title = override.episode_title
            if merged_title and merged_year is not None and merged_source_id and merged_episode_title:
                break
        if not merged_title and merged_year is None and not merged_source_id and not merged_episode_title:
            return None
        return NetflixTitleOverride(
            title=merged_title,
            year=merged_year,
            source_id=merged_source_id,
            episode_title=merged_episode_title,
        )

    def load_entries(self, csv_path: str) -> List[NetflixHistoryEntry]:
        raw_entries: List[Tuple[str, datetime, ParsedNetflixTitle]] = []

        with _open_csv_with_fallback_encodings(Path(csv_path)) as handle:
            rows = list(csv.DictReader(handle))

        for row in iter_progress(rows, total=len(rows), desc="Reading Netflix history", unit="row"):
            raw_title = _clean_csv_text(row.get("Title"))
            raw_date = _clean_csv_text(row.get("Date"))
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
            media_kind, resolved_title, resolved_title_year, resolved_total_seasons, metadata_type, metadata_provider, metadata_parent_id, metadata_average_rating, metadata_num_votes, metadata_runtime_minutes, metadata_genres, metadata_title_type, metadata_sources = self._classify_entry(parsed, prefix_counts)
            resolved_season, resolved_episode, resolved_episode_title, resolved_episode_source_id, resolved_episode_rating, resolved_episode_votes, resolved_episode_year = self._resolve_episode_metadata(
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
                    expected_type=media_kind,
                    resolved_title=resolved_title,
                    metadata_type=metadata_type,
                    metadata_provider=metadata_provider,
                    metadata_parent_id=metadata_parent_id,
                    resolved_season=resolved_season,
                    resolved_episode=resolved_episode,
                    resolved_episode_title=resolved_episode_title,
                    resolved_episode_source_id=resolved_episode_source_id,
                    resolved_episode_rating=resolved_episode_rating,
                    resolved_episode_votes=resolved_episode_votes,
                    resolved_title_year=resolved_title_year,
                    resolved_episode_year=resolved_episode_year,
                    resolved_total_seasons=resolved_total_seasons,
                    metadata_average_rating=metadata_average_rating,
                    metadata_num_votes=metadata_num_votes,
                    metadata_runtime_minutes=metadata_runtime_minutes,
                    metadata_genres=metadata_genres,
                    metadata_title_type=metadata_title_type,
                    metadata_sources=metadata_sources,
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

    def _candidate_series_titles_from_colons(self, parsed: ParsedNetflixTitle) -> Tuple[str, ...]:
        if parsed.is_explicit_series or ":" not in parsed.raw_title:
            return ()

        tokens = [token.strip() for token in parsed.raw_title.split(":") if token.strip()]
        candidates: List[str] = []
        for index in range(len(tokens) - 1, 0, -1):
            prefix = ": ".join(tokens[:index]).strip()
            suffix = ": ".join(tokens[index:]).strip()
            if not prefix or not suffix:
                continue
            candidates.append(prefix)
        return tuple(dict.fromkeys(candidates))

    def _derive_episode_title_from_prefix(
        self,
        raw_title: str,
        series_prefix: str,
        season_title: Optional[str] = None,
    ) -> Optional[str]:
        prefix = series_prefix.strip()
        cleaned_raw_title = raw_title.strip()
        if not prefix or not _starts_with_casefold(cleaned_raw_title, prefix):
            return None

        suffix = cleaned_raw_title[len(prefix):].lstrip(" :")
        if not suffix:
            return None

        if season_title and suffix.startswith(season_title):
            suffix = suffix[len(season_title):].lstrip(" :")

        return suffix or None

    def _classify_colon_delimited_episode_title(
        self,
        parsed: ParsedNetflixTitle,
    ) -> Optional[Tuple[str, str, Optional[int], Optional[int], Optional[str], Any, Optional[str], Optional[float], Optional[int], Optional[int], Tuple[str, ...], Optional[str], Tuple[str, ...]]]:
        for query in self._candidate_series_titles_from_colons(parsed):
            resolved_kind, resolved_title, resolved_title_year, resolved_total_seasons, metadata_type, metadata_provider, metadata_parent_id, metadata_average_rating, metadata_num_votes, metadata_runtime_minutes, metadata_genres, metadata_title_type, metadata_sources = self._lookup_metadata(
                query,
                preferred_type="tv",
            )
            if resolved_kind != "series":
                continue
            if not _metadata_match_is_compatible(
                query=query,
                raw_title=parsed.raw_title,
                inferred_series_title=query,
                resolved_kind=resolved_kind,
                resolved_title=resolved_title,
            ):
                continue

            if metadata_provider is None or metadata_parent_id is None:
                continue
            if not hasattr(metadata_provider, "find_episode_by_title"):
                continue

            lookup_candidates = self._derive_episode_title_lookup_candidates(
                parsed,
                resolved_title or query,
                metadata_provider,
                metadata_parent_id,
            )
            if not lookup_candidates:
                continue

            episode_info = None
            for lookup_title in lookup_candidates:
                try:
                    episode_info = metadata_provider.find_episode_by_title(
                        metadata_parent_id,
                        lookup_title,
                        season=parsed.season,
                    )
                except Exception:
                    episode_info = None
                if episode_info is not None:
                    break

            if episode_info is None:
                episode_info = self._resolve_episode_from_known_list(
                    metadata_provider,
                    metadata_parent_id,
                    parsed.season,
                    lookup_candidates,
                )

            if episode_info is None:
                continue

            return (
                resolved_kind,
                resolved_title or query,
                resolved_title_year,
                resolved_total_seasons,
                metadata_type,
                metadata_provider,
                metadata_parent_id,
                metadata_average_rating,
                metadata_num_votes,
                metadata_runtime_minutes,
                metadata_genres,
                metadata_title_type,
                metadata_sources,
            )

        return None

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

    def _get_episode_title_override(self, parsed: ParsedNetflixTitle, resolved_title: str) -> Optional[str]:
        lookup_title = self._derive_episode_title(parsed, resolved_title)
        episode_override_keys = list(
            _build_episode_title_override_lookup_keys(
                parsed,
                derived_episode_title=lookup_title,
            )
        )
        override = self._get_title_override(*episode_override_keys)
        return override.episode_title if override is not None else None

    def _resolve_episode_metadata(
        self,
        parsed: ParsedNetflixTitle,
        media_kind: str,
        metadata_type: Optional[str],
        metadata_provider: Any,
        metadata_parent_id: Optional[str],
        resolved_title: str,
        resolved_total_seasons: Optional[int],
    ) -> Tuple[Optional[int], Optional[int], Optional[str], Optional[str], Optional[float], Optional[int], Optional[int]]:
        resolved_season = parsed.season
        resolved_episode = parsed.episode
        resolved_episode_title = self._derive_episode_title(parsed, resolved_title)
        resolved_episode_source_id: Optional[str] = None
        resolved_episode_rating: Optional[float] = None
        resolved_episode_votes: Optional[int] = None
        resolved_episode_year: Optional[int] = None

        if (
            media_kind != "series"
            or metadata_type != "tv"
            or metadata_provider is None
            or metadata_parent_id is None
        ):
            return resolved_season, resolved_episode, resolved_episode_title, resolved_episode_source_id, resolved_episode_rating, resolved_episode_votes, resolved_episode_year

        lookup_season = resolved_season
        if lookup_season is None and resolved_episode is not None and resolved_total_seasons == 1:
            lookup_season = 1

        if (
            lookup_season is not None
            and resolved_episode is not None
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

            if episode_info is not None:
                resolved_season = episode_info.season
                if episode_info.title and not resolved_episode_title:
                    resolved_episode_title = episode_info.title
                resolved_episode_source_id = getattr(episode_info, "id", None)
                resolved_episode_rating = getattr(episode_info, "rating", None)
                resolved_episode_votes = getattr(episode_info, "votes", None)
                resolved_episode_year = episode_info.year

        if resolved_episode is not None or not hasattr(metadata_provider, "find_episode_by_title"):
            return resolved_season, resolved_episode, resolved_episode_title, resolved_episode_source_id, resolved_episode_rating, resolved_episode_votes, resolved_episode_year

        episode_title_candidates = self._derive_episode_title_lookup_candidates(
            parsed,
            resolved_title,
            metadata_provider,
            metadata_parent_id,
        )
        if not episode_title_candidates:
            return resolved_season, resolved_episode, resolved_episode_title, resolved_episode_source_id, resolved_episode_rating, resolved_episode_votes, resolved_episode_year

        episode_info = None
        for lookup_title in episode_title_candidates:
            try:
                episode_info = metadata_provider.find_episode_by_title(
                    metadata_parent_id,
                    lookup_title,
                    season=resolved_season,
                )
            except Exception:
                episode_info = None
            if episode_info is not None:
                break

        if episode_info is None:
            episode_info = self._resolve_episode_from_known_list(
                metadata_provider,
                metadata_parent_id,
                resolved_season,
                episode_title_candidates,
            )

        if episode_info is None:
            return resolved_season, resolved_episode, resolved_episode_title, resolved_episode_source_id, resolved_episode_rating, resolved_episode_votes, resolved_episode_year

        return (
            episode_info.season,
            episode_info.episode,
            episode_info.title or resolved_episode_title,
            getattr(episode_info, "id", None),
            getattr(episode_info, "rating", None),
            getattr(episode_info, "votes", None),
            episode_info.year,
        )

    def _derive_episode_title(self, parsed: ParsedNetflixTitle, resolved_title: str) -> Optional[str]:
        if parsed.episode_title:
            return parsed.episode_title

        if parsed.episode is not None:
            return None

        return self._derive_episode_title_from_prefix(parsed.raw_title, resolved_title, parsed.season_title)

    def _derive_episode_title_lookup_candidates(
        self,
        parsed: ParsedNetflixTitle,
        resolved_title: str,
        metadata_provider: Any,
        metadata_parent_id: Optional[str],
    ) -> Tuple[str, ...]:
        lookup_title = self._derive_episode_title(parsed, resolved_title)
        override_title = self._get_episode_title_override(parsed, resolved_title)
        if not override_title and not lookup_title:
            return ()

        known_titles: Tuple[str, ...] = ()
        if metadata_provider is not None and metadata_parent_id is not None and hasattr(metadata_provider, "list_episodes"):
            try:
                episodes = metadata_provider.list_episodes(metadata_parent_id, parsed.season)
            except TypeError:
                try:
                    episodes = metadata_provider.list_episodes(metadata_parent_id)
                except Exception:
                    episodes = []
            except Exception:
                episodes = []
            known_titles = tuple(
                episode.title
                for episode in episodes
                if getattr(episode, "title", None)
            )

        candidates: List[str] = []
        if override_title:
            candidates.extend(adapt_lookup_titles(override_title, known_titles))
        if lookup_title:
            candidates.extend(adapt_lookup_titles(lookup_title, known_titles))
        return tuple(dict.fromkeys(candidate for candidate in candidates if candidate))

    def _resolve_episode_from_known_list(
        self,
        metadata_provider: Any,
        metadata_parent_id: Optional[str],
        season: Optional[int],
        lookup_candidates: Tuple[str, ...],
    ) -> Any:
        if metadata_provider is None or metadata_parent_id is None or not hasattr(metadata_provider, "list_episodes"):
            return None
        if not lookup_candidates:
            return None

        try:
            episodes = metadata_provider.list_episodes(metadata_parent_id, season)
        except TypeError:
            try:
                episodes = metadata_provider.list_episodes(metadata_parent_id)
            except Exception:
                return None
        except Exception:
            return None

        if not episodes:
            return None

        episode_by_title = {
            str(episode.title).casefold(): episode
            for episode in episodes
            if getattr(episode, "title", None)
        }

        for candidate in lookup_candidates[1:]:
            matched_episode = episode_by_title.get(candidate.casefold())
            if matched_episode is not None:
                return matched_episode

        return None

    def _classify_entry(
        self, parsed: ParsedNetflixTitle, prefix_counts: Dict[str, int]
    ) -> Tuple[str, str, Optional[int], Optional[int], Optional[str], Any, Optional[str], Optional[float], Optional[int], Optional[int], Tuple[str, ...], Optional[str], Tuple[str, ...]]:
        default_kind = "series" if parsed.is_explicit_series else parsed.media_kind
        default_title = parsed.title if parsed.is_explicit_series else parsed.raw_title
        inferred_series_title = self._infer_series_title(parsed, prefix_counts)
        effective_override = self._get_title_override(
            *_build_episode_title_override_lookup_keys(
                parsed,
                inferred_series_title=inferred_series_title,
            )
        )
        series_title_override = effective_override.title if effective_override is not None else None
        series_title_override_year = effective_override.year if effective_override is not None else None
        series_title_override_source_id = effective_override.source_id if effective_override is not None else None

        if self.metadata_manager is None:
            if series_title_override:
                return "series", series_title_override, series_title_override_year, None, None, None, series_title_override_source_id, None, None, None, (), None, ()
            if inferred_series_title:
                return "series", inferred_series_title, None, None, None, None, None, None, None, None, (), None, ()
            return default_kind, default_title, None, None, None, None, None, None, None, None, (), None, ()

        queries: List[Tuple[str, bool, Optional[int], Optional[str]]] = []
        seen_queries: set[Tuple[str, Optional[int], Optional[str]]] = set()

        def add_query(
            query: Optional[str],
            *,
            authoritative: bool = False,
            year: Optional[int] = None,
            preferred_type: Optional[str] = None,
        ) -> None:
            cache_key = ((query or "").casefold().strip(), year, preferred_type)
            if not cache_key or cache_key in seen_queries:
                return
            seen_queries.add(cache_key)
            queries.append((query or "", authoritative, year, preferred_type))

        add_query(
            series_title_override,
            authoritative=series_title_override is not None,
            year=series_title_override_year,
            preferred_type="tv" if series_title_override is not None else None,
        )
        if parsed.is_explicit_series:
            add_query(parsed.title, preferred_type="tv")
        else:
            if inferred_series_title:
                add_query(inferred_series_title, preferred_type="tv")
            if parsed.has_implicit_split and not inferred_series_title:
                add_query(parsed.raw_title, preferred_type="movie")
                if parsed.title != parsed.raw_title:
                    add_query(parsed.title, preferred_type="tv")
            else:
                add_query(parsed.raw_title)
                if parsed.title != parsed.raw_title:
                    add_query(parsed.title)

        for query, authoritative_query, query_year, preferred_type_override in queries:
            preferred_type = preferred_type_override
            if preferred_type is None:
                preferred_type = "tv" if (
                    series_title_override is not None
                    or authoritative_query
                    or parsed.is_explicit_series
                    or parsed.episode is not None
                    or (parsed.episode_title is not None and not parsed.has_implicit_split)
                    or inferred_series_title is not None
                ) else None
            resolved_kind, resolved_title, resolved_title_year, resolved_total_seasons, metadata_type, metadata_provider, metadata_parent_id, metadata_average_rating, metadata_num_votes, metadata_runtime_minutes, metadata_genres, metadata_title_type, metadata_sources = self._lookup_metadata(
                query,
                year=query_year,
                preferred_type=preferred_type,
            )
            if resolved_kind is None:
                continue
            if not authoritative_query and not _metadata_match_is_compatible(
                query=query,
                raw_title=parsed.raw_title,
                inferred_series_title=inferred_series_title,
                resolved_kind=resolved_kind,
                resolved_title=resolved_title,
            ):
                continue
            if parsed.is_explicit_series and resolved_kind == "movie":
                return "series", parsed.title or resolved_title or default_title, resolved_title_year or series_title_override_year, resolved_total_seasons, metadata_type, metadata_provider, metadata_parent_id or series_title_override_source_id, metadata_average_rating, metadata_num_votes, metadata_runtime_minutes, metadata_genres, metadata_title_type, metadata_sources
            return resolved_kind, resolved_title or default_title, resolved_title_year or series_title_override_year, resolved_total_seasons, metadata_type, metadata_provider, metadata_parent_id or series_title_override_source_id, metadata_average_rating, metadata_num_votes, metadata_runtime_minutes, metadata_genres, metadata_title_type, metadata_sources

        colon_delimited_match = self._classify_colon_delimited_episode_title(parsed)
        if colon_delimited_match is not None:
            return colon_delimited_match

        if series_title_override:
            return "series", series_title_override, series_title_override_year, None, None, None, series_title_override_source_id, None, None, None, (), None, ()

        if inferred_series_title:
            return "series", inferred_series_title, None, None, None, None, None, None, None, None, (), None, ()

        if parsed.has_implicit_split:
            return "series", parsed.title or default_title, None, None, None, None, None, None, None, None, (), None, ()

        return default_kind, default_title, None, None, None, None, None, None, None, None, (), None, ()

    def _lookup_metadata(
        self,
        query: str,
        year: Optional[int] = None,
        preferred_type: Optional[str] = None,
    ) -> Tuple[Optional[str], Optional[str], Optional[int], Optional[int], Optional[str], Any, Optional[str], Optional[float], Optional[int], Optional[int], Tuple[str, ...], Optional[str], Tuple[str, ...]]:
        cache_key = query.casefold().strip()
        if not cache_key:
            return None, None, None, None, None, None, None, None, None, None, (), None, ()

        metadata_cache_key = (cache_key, year, preferred_type)
        if metadata_cache_key in self._metadata_cache:
            return self._metadata_cache[metadata_cache_key]

        try:
            match = self.metadata_manager.find_title(query, year=year, preferred_type=preferred_type)
        except TypeError:
            try:
                match = self.metadata_manager.find_title(query, preferred_type=preferred_type)
            except Exception:
                self._metadata_cache[metadata_cache_key] = (None, None, None, None, None, None, None, None, None, None, (), None, ())
                return None, None, None, None, None, None, None, None, None, None, (), None, ()
        except Exception:
            self._metadata_cache[metadata_cache_key] = (None, None, None, None, None, None, None, None, None, None, (), None, ())
            return None, None, None, None, None, None, None, None, None, None, (), None, ()

        if not match or not match[0]:
            self._metadata_cache[metadata_cache_key] = (None, None, None, None, None, None, None, None, None, None, (), None, ())
            return None, None, None, None, None, None, None, None, None, None, (), None, ()

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
        metadata_average_rating = getattr(title_info, "rating", None)
        metadata_num_votes = getattr(title_info, "votes", None)
        metadata_runtime_minutes = getattr(title_info, "runtime_minutes", None)
        metadata_genres = tuple(getattr(title_info, "genres", []) or ())
        metadata_title_type = getattr(title_info, "type", None)
        metadata_sources = tuple(getattr(title_info, "sources", []) or ())

        if (
            media_kind == "series"
            and _provider_name(provider) != "IMDbDataProvider"
            and not _has_imdb_title_reference(metadata_parent_id, metadata_sources)
            and hasattr(self.metadata_manager, "find_title_from_provider")
        ):
            for imdb_query in _build_imdb_enrichment_queries(query, resolved_title):
                try:
                    imdb_match = self.metadata_manager.find_title_from_provider(
                        imdb_query,
                        "imdbdataprovider",
                        year=year,
                        preferred_type="tv",
                    )
                except TypeError:
                    try:
                        imdb_match = self.metadata_manager.find_title_from_provider(imdb_query, "imdbdataprovider")
                    except Exception:
                        imdb_match = (None, None)
                except Exception:
                    imdb_match = (None, None)

                imdb_info = imdb_match[0] if imdb_match else None
                imdb_provider = imdb_match[1] if imdb_match and len(imdb_match) > 1 else None
                if imdb_info is None or getattr(imdb_info, "type", None) != "tv":
                    continue

                provider = imdb_provider or provider
                metadata_parent_id = getattr(imdb_info, "id", None) or metadata_parent_id
                resolved_total_seasons = getattr(imdb_info, "total_seasons", None) or resolved_total_seasons
                metadata_sources = _merge_metadata_sources(
                    metadata_sources,
                    tuple(getattr(imdb_info, "sources", []) or ()),
                )
                metadata_type = "tv"
                break

        self._metadata_cache[metadata_cache_key] = (media_kind, resolved_title, resolved_title_year, resolved_total_seasons, metadata_type, provider, metadata_parent_id, metadata_average_rating, metadata_num_votes, metadata_runtime_minutes, metadata_genres, metadata_title_type, metadata_sources)
        return media_kind, resolved_title, resolved_title_year, resolved_total_seasons, metadata_type, provider, metadata_parent_id, metadata_average_rating, metadata_num_votes, metadata_runtime_minutes, metadata_genres, metadata_title_type, metadata_sources


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
    output_group.add_argument(
        "--webapp-export",
        metavar="FILE",
        help="Export a standalone local HTML webapp report.",
    )
    parser.add_argument(
        "--columns",
        default=",".join(DEFAULT_TABLE_COLUMNS),
        help=(
            "Comma-separated table columns for --table. "
            f"Available: {', '.join(TABLE_COLUMN_DEFINITIONS.keys())}."
        ),
    )
    parser.add_argument(
        "--episode-title-overrides",
        metavar="FILE",
        help=(
            "Optional CSV file mapping raw Netflix CSV titles to canonical metadata. "
            "Primary key column: netflix_original_title. Legacy netflix_title with optional season_name and "
            "netflix_episode_title is still accepted for backward compatibility. "
            "Fill-in mapping columns: title, year, optional source_id, and optional episode_title "
            "(used only for episode metadata lookup). "
            "These overrides are merged on top of netflix_episode_title_overrides.csv when present."
        ),
    )
    return parser


def visible_text_width(text: str) -> int:
    return len(text)


def pad_console_cell(text: str, width: int, align: str) -> str:
    padding = max(0, width - visible_text_width(text))
    if align == "right":
        return f"{' ' * padding}{text}"
    if align == "center":
        left_padding = padding // 2
        right_padding = padding - left_padding
        return f"{' ' * left_padding}{text}{' ' * right_padding}"
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


def _format_watch_years(watched_at_values: List[datetime]) -> str:
    years = sorted({value.year for value in watched_at_values})
    return ", ".join(str(year) for year in years)


def _format_watch_dates(watched_at_values: List[datetime]) -> str:
    unique_dates = sorted({value.date().isoformat() for value in watched_at_values})
    return ", ".join(unique_dates)


def _format_leaf_views(watched_at_values: List[datetime]) -> str:
    return str(len(watched_at_values))


def _format_group_views(watched_at_values: List[datetime]) -> str:
    return ""


def _format_leaf_watch_dates(watched_at_values: List[datetime]) -> str:
    if not watched_at_values:
        return ""
    return _format_watch_dates(watched_at_values)


def _format_group_watch_dates(watched_at_values: List[datetime]) -> str:
    return ""


def _format_average_rating(value: Optional[float]) -> str:
    if value is None:
        return ""
    return f"{value:.1f}"


def _format_num_votes(value: Optional[int]) -> str:
    if value is None:
        return ""
    return f"{value:,}"


def _format_runtime_minutes(value: Optional[int]) -> str:
    if value is None:
        return ""
    hours, minutes = divmod(value, 60)
    parts: List[str] = []
    if hours:
        parts.append(f"{hours}h")
    if minutes or not parts:
        parts.append(f"{minutes}m")
    return " ".join(parts)


def _parse_runtime_minutes(text: str) -> Optional[int]:
    cleaned = (text or "").strip().lower()
    if not cleaned:
        return None

    total = 0
    matched = False
    for value_text, unit in re.findall(r"(\d+)\s*([hm])", cleaned):
        matched = True
        value = int(value_text)
        if unit == "h":
            total += value * 60
        else:
            total += value

    return total if matched else None


def _format_genres(values: Tuple[str, ...]) -> str:
    return ", ".join(values)


def _format_year_range(values: List[Optional[int]]) -> str:
    years = sorted({int(value) for value in values if value is not None})
    if not years:
        return ""
    if len(years) == 1:
        return str(years[0])
    return f"{years[0]}-{years[-1]}"


def _format_aggregate_rating(values: List[Optional[float]]) -> str:
    ratings = [float(value) for value in values if value is not None]
    if not ratings:
        return ""
    return _format_average_rating(sum(ratings) / len(ratings))


def _format_aggregate_votes(values: List[Optional[int]]) -> str:
    votes = [int(value) for value in values if value is not None]
    if not votes:
        return ""
    return _format_num_votes(sum(votes))


def _format_aggregate_runtime(runtime_minutes: Optional[int], item_count: int) -> str:
    if runtime_minutes is None or item_count <= 0:
        return ""
    return _format_runtime_minutes(runtime_minutes * item_count)


def _aggregate_direct_child_runtime(
    rows: List[WatchTableRow],
    parent_index: int,
    episode_runtime_minutes: Optional[int] = None,
) -> str:
    total_minutes = 0
    found_runtime = False
    parent_level = rows[parent_index].level

    for child_row in rows[parent_index + 1:]:
        if child_row.level <= parent_level:
            break
        if child_row.level != parent_level + 1:
            continue

        runtime_minutes = _parse_runtime_minutes(child_row.runtime_minutes)
        if runtime_minutes is None and child_row.item_type == "episode" and episode_runtime_minutes is not None:
            runtime_minutes = episode_runtime_minutes
        if runtime_minutes is None:
            continue
        total_minutes += runtime_minutes
        found_runtime = True

    if not found_runtime:
        return rows[parent_index].runtime_minutes
    return _format_runtime_minutes(total_minutes)


def _format_source_id(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(int(value)) if isinstance(value, float) and value.is_integer() else str(value)
    return str(value).strip()


def _entry_metadata_row_fields(
    entry: NetflixHistoryEntry,
    *,
    row_item_type: str,
    source_id_override: Optional[Any] = None,
    runtime_minutes_override: Optional[str] = None,
    average_rating_override: Optional[float] = None,
    num_votes_override: Optional[int] = None,
) -> Dict[str, str]:
    source_id = source_id_override if source_id_override is not None else (
        entry.resolved_episode_source_id if row_item_type == "episode" else entry.metadata_parent_id
    )
    average_rating = average_rating_override if average_rating_override is not None else (
        entry.resolved_episode_rating if row_item_type == "episode" else entry.metadata_average_rating
    )
    num_votes = num_votes_override if num_votes_override is not None else (
        entry.resolved_episode_votes if row_item_type == "episode" else entry.metadata_num_votes
    )
    return {
        "source_id": "" if row_item_type == "season" else _format_source_id(source_id),
        "title_type": entry.metadata_title_type or entry.metadata_type or "",
        "runtime_minutes": runtime_minutes_override if runtime_minutes_override is not None else _format_runtime_minutes(entry.metadata_runtime_minutes),
        "genres": "" if row_item_type in {"season", "episode"} else _format_genres(entry.metadata_genres),
        "average_rating": "" if row_item_type == "episode" and average_rating is None else _format_average_rating(average_rating),
        "num_votes": "" if row_item_type == "episode" and num_votes is None else _format_num_votes(num_votes),
    }


class _ThumbnailMetaParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.image_url: str = ""

    def handle_starttag(self, tag: str, attrs: List[tuple[str, Optional[str]]]) -> None:
        if self.image_url:
            return

        normalized_attrs = {
            (key or "").lower(): value or ""
            for key, value in attrs
        }
        lower_tag = tag.lower()

        if lower_tag == "meta":
            meta_key = (
                normalized_attrs.get("property")
                or normalized_attrs.get("name")
                or normalized_attrs.get("itemprop")
                or ""
            ).strip().lower()
            content = normalized_attrs.get("content", "").strip()
            if meta_key in THUMBNAIL_META_KEYS and content:
                self.image_url = content
            return

        if lower_tag == "link":
            rel = normalized_attrs.get("rel", "").strip().lower()
            href = normalized_attrs.get("href", "").strip()
            if rel == "image_src" and href:
                self.image_url = href


def _thumbnail_domain_rank(source_url: str) -> tuple[int, str]:
    netloc = urlparse(source_url).netloc.casefold()
    preferred_domains = (
        "myanimelist.net",
        "imdb.com",
        "anilist.co",
        "themoviedb.org",
        "wikipedia.org",
    )
    for index, domain in enumerate(preferred_domains):
        if domain in netloc:
            return index, netloc
    return len(preferred_domains), netloc


def _iter_preferred_thumbnail_sources(sources: Tuple[str, ...]) -> List[str]:
    unique_sources: List[str] = []
    seen: set[str] = set()
    for source in sources:
        cleaned = source.strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        unique_sources.append(cleaned)
    return sorted(unique_sources, key=_thumbnail_domain_rank)


def _load_thumbnail_cache() -> Dict[str, Dict[str, Any]]:
    try:
        return json.loads(THUMBNAIL_CACHE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_thumbnail_cache(cache: Dict[str, Dict[str, Any]]) -> None:
    try:
        THUMBNAIL_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        THUMBNAIL_CACHE_FILE.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        return


def _thumbnail_cache_ttl(status: str) -> int:
    return THUMBNAIL_CACHE_SUCCESS_TTL_SECONDS if status == "available" else THUMBNAIL_CACHE_FAILURE_TTL_SECONDS


def _fetch_thumbnail_from_source_url(source_url: str) -> Dict[str, str]:
    request = Request(
        source_url,
        headers={
            "User-Agent": THUMBNAIL_USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        },
    )
    try:
        with urlopen(request, timeout=10) as response:
            final_url = response.geturl()
            content_type = (response.headers.get("Content-Type") or "").casefold()
            if content_type.startswith("image/"):
                return {"status": "available", "url": final_url}
            payload = response.read().decode("utf-8", errors="ignore")
    except HTTPError:
        return {"status": "http_error", "url": ""}
    except URLError:
        return {"status": "fetch_error", "url": ""}
    except Exception:
        return {"status": "fetch_error", "url": ""}

    parser = _ThumbnailMetaParser()
    try:
        parser.feed(payload)
    except Exception:
        return {"status": "parse_error", "url": ""}

    if not parser.image_url:
        return {"status": "no_image", "url": ""}

    return {
        "status": "available",
        "url": urljoin(final_url, parser.image_url),
    }


def _resolve_thumbnail_for_sources(
    sources: Tuple[str, ...],
    cache: Dict[str, Dict[str, Any]],
) -> tuple[Dict[str, str], bool]:
    now = int(time.time())
    candidates = _iter_preferred_thumbnail_sources(sources)
    if not candidates:
        return {"status": "no_source", "url": ""}, False

    dirty = False
    last_result = {"status": "no_source", "url": ""}
    for source_url in candidates:
        cached = cache.get(source_url)
        if cached and int(cached.get("expires_at", 0) or 0) > now:
            cached_result = {
                "status": str(cached.get("status") or "no_image"),
                "url": str(cached.get("url") or ""),
            }
            if cached_result["status"] == "available":
                return cached_result, dirty
            last_result = cached_result
            continue

        resolved = _fetch_thumbnail_from_source_url(source_url)
        cache[source_url] = {
            "status": resolved["status"],
            "url": resolved["url"],
            "expires_at": now + _thumbnail_cache_ttl(resolved["status"]),
            "checked_at": now,
        }
        dirty = True
        if resolved["status"] == "available":
            return resolved, dirty
        last_result = resolved

    return last_result, dirty


def _thumbnail_url_from_imdb_id(metadata_parent_id: Any) -> str:
    if not isinstance(metadata_parent_id, str):
        return ""
    if not re.fullmatch(r"tt\d+", metadata_parent_id):
        return ""
    return f"https://images.metahub.space/poster/medium/{metadata_parent_id}/img"


def _resolve_thumbnail_for_entry(
    entry: NetflixHistoryEntry,
    cache: Dict[str, Dict[str, Any]],
) -> tuple[Dict[str, str], bool]:
    imdb_thumbnail_url = _thumbnail_url_from_imdb_id(entry.metadata_parent_id)
    if imdb_thumbnail_url:
        return {"status": "available", "url": imdb_thumbnail_url}, False
    return _resolve_thumbnail_for_sources(entry.metadata_sources, cache)


def _apply_row_thumbnails(rows: List[WatchTableRow], entries: List[NetflixHistoryEntry]) -> None:
    title_entries: Dict[str, NetflixHistoryEntry] = {}
    for entry in entries:
        key = entry.resolved_title.casefold()
        current = title_entries.get(key)
        if current is None:
            title_entries[key] = entry
            continue
        if not current.metadata_parent_id and entry.metadata_parent_id:
            title_entries[key] = entry
            continue
        if not current.metadata_sources and entry.metadata_sources:
            title_entries[key] = entry

    if not title_entries:
        for row in rows:
            row.thumbnail_status = "no_source"
        return

    cache = _load_thumbnail_cache()
    dirty = False
    thumbnails_by_title: Dict[str, Dict[str, str]] = {}
    for title_key, entry in title_entries.items():
        thumbnail_result, cache_dirty = _resolve_thumbnail_for_entry(entry, cache)
        dirty = dirty or cache_dirty
        thumbnails_by_title[title_key] = thumbnail_result

    if dirty:
        _save_thumbnail_cache(cache)

    current_thumbnail = {"status": "no_source", "url": ""}
    for row in rows:
        if row.level == 0:
            current_thumbnail = thumbnails_by_title.get(row.title.casefold(), {"status": "no_source", "url": ""})
        row.thumbnail_status = current_thumbnail["status"]
        row.thumbnail_url = current_thumbnail["url"]


def _format_progress(watched_count: int, total_count: int) -> str:
    if total_count <= 0:
        return ""
    return f"{watched_count}/{total_count}"


def load_template_asset(filename: str) -> str:
    return Path(__file__).with_name(filename).read_text(encoding="utf-8")


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
    if prefix and _starts_with_casefold(raw_title, prefix):
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


def _finalize_watch_table_rows(rows: List[WatchTableRow]) -> List[WatchTableRow]:
    stack: Dict[int, str] = {}
    child_counts: Dict[str, int] = {}

    for index, row in enumerate(rows):
        row.row_id = f"row-{index}"
        row.parent_id = stack.get(row.level - 1) if row.level > 0 else None
        stack[row.level] = row.row_id

        for level in list(stack):
            if level > row.level:
                del stack[level]

        if row.parent_id:
            child_counts[row.parent_id] = child_counts.get(row.parent_id, 0) + 1

    for row in rows:
        row.has_children = child_counts.get(row.row_id, 0) > 0

    return rows


def _row_thumbnail_payload(row: WatchTableRow) -> Dict[str, str]:
    return {
        "status": row.thumbnail_status,
        "url": row.thumbnail_url,
        "alt": f"Artwork for {row.title}" if row.title else "Artwork placeholder",
    }


def _strip_unwatched_suffix(text: str) -> str:
    return text[:-2] if text.endswith(" *") else text


def _parse_progress_counts(value: str) -> Tuple[Optional[int], Optional[int]]:
    if not value or "/" not in value:
        return None, None
    left, right = value.split("/", 1)
    try:
        watched_count = int(left.strip())
        total_count = int(right.strip())
    except ValueError:
        return None, None
    if total_count < 0 or watched_count < 0:
        return None, None
    return watched_count, total_count


def _row_watch_state(row: WatchTableRow) -> str:
    if row.item_type == "episode":
        return "unwatched" if row.views == "0" else "watched"

    if row.item_type in {"season", "series"}:
        watched_count, total_count = _parse_progress_counts(row.episode)
        if watched_count is not None and total_count is not None and total_count > 0:
            if watched_count == 0:
                return "unwatched"
            if watched_count >= total_count:
                return "watched"
            return "partial"

    return "watched" if row.views else "aggregate"


def _serialize_watch_table_row(row: WatchTableRow) -> Dict[str, Any]:
    display_title = _strip_unwatched_suffix(row.title)
    display_episode_title = _strip_unwatched_suffix(row.episode_title)
    watch_state = _row_watch_state(row)
    search_text = " ".join(
        part for part in [
            row.item_type,
            row.title,
            display_title,
            row.year,
            row.source_id,
            row.season,
            row.season_title,
            row.episode,
            row.episode_title,
            display_episode_title,
            row.views,
            row.watch_dates,
            row.title_type,
            row.runtime_minutes,
            row.genres,
            row.average_rating,
            row.num_votes,
        ] if part
    ).casefold()
    return {
        "id": row.row_id,
        "parent_id": row.parent_id,
        "level": row.level,
        "item_type": row.item_type,
        "title": row.title,
        "display_title": display_title,
        "year": row.year,
        "source_id": row.source_id,
        "season": row.season,
        "season_title": row.season_title,
        "episode": row.episode,
        "episode_title": row.episode_title,
        "display_episode_title": display_episode_title,
        "views": row.views,
        "watch_dates": row.watch_dates,
        "title_type": row.title_type,
        "runtime_minutes": row.runtime_minutes,
        "genres": row.genres,
        "average_rating": row.average_rating,
        "num_votes": row.num_votes,
        "has_children": row.has_children,
        "watch_state": watch_state,
        "thumbnail": _row_thumbnail_payload(row),
        "search_text": search_text,
    }


def build_webapp_payload(
    csv_path: str,
    results: Dict[str, Any],
    entries: List[NetflixHistoryEntry],
    selected_columns: List[str],
) -> Dict[str, Any]:
    rows = build_watch_table_rows(entries)
    _apply_row_thumbnails(rows, entries)
    columns = [
        {
            "key": column,
            "header": TABLE_COLUMN_DEFINITIONS[column]["header"],
            "align": TABLE_COLUMN_DEFINITIONS[column]["align"],
        }
        for column in TABLE_COLUMN_DEFINITIONS
    ]
    return {
        "meta": {
            "title": "Netflix watch status report",
            "source_csv": str(Path(csv_path).resolve()),
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "columns": selected_columns,
        },
        "summary": results["summary"],
        "movies": results["movies"],
        "series": results["series"],
        "columns": columns,
        "rows": [_serialize_watch_table_row(row) for row in rows],
    }


def render_webapp_html(payload: Dict[str, Any]) -> str:
    template = load_template_asset("netflix_watch_status_webapp_template.html")
    css = load_template_asset("netflix_watch_status_webapp_template.css")
    script = load_template_asset("netflix_watch_status_webapp_template.js")
    serialized = json.dumps(payload, ensure_ascii=False).replace("</", "<\\/")
    return (
        template
        .replace("/*NETFLIX_WATCH_STATUS_CSS*/", css)
        .replace("/*NETFLIX_WATCH_STATUS_JSON*/", serialized)
        .replace("/*NETFLIX_WATCH_STATUS_JS*/", script)
    )


def export_webapp_report(
    csv_path: str,
    results: Dict[str, Any],
    entries: List[NetflixHistoryEntry],
    selected_columns: List[str],
    output_path: str,
) -> Path:
    target_path = Path(output_path).expanduser().resolve()
    target_path.parent.mkdir(parents=True, exist_ok=True)
    html = render_webapp_html(build_webapp_payload(csv_path, results, entries, selected_columns))
    target_path.write_text(html, encoding="utf-8")
    return target_path


def _entry_has_imdb_title_metadata(entry: NetflixHistoryEntry) -> bool:
    if isinstance(entry.metadata_parent_id, str) and re.fullmatch(r"tt\d+", entry.metadata_parent_id):
        return True
    return any("imdb" in source.casefold() for source in entry.metadata_sources)


def _entry_requires_episode_mapping(entry: NetflixHistoryEntry) -> bool:
    if entry.media_kind != "series":
        return False
    return bool(entry.parsed.episode_title or entry.parsed.episode is not None or _derive_episode_title(entry))


def _entry_has_imdb_episode_metadata(entry: NetflixHistoryEntry) -> bool:
    if isinstance(entry.resolved_episode_source_id, str) and re.fullmatch(r"tt\d+", entry.resolved_episode_source_id):
        return True
    return False


def _entry_imdb_mapping_status(entry: NetflixHistoryEntry) -> str:
    title_mapped = _entry_has_imdb_title_metadata(entry)
    episode_required = _entry_requires_episode_mapping(entry)
    episode_mapped = _entry_has_imdb_episode_metadata(entry)
    if title_mapped and (not episode_required or episode_mapped):
        return "mapped"
    if title_mapped:
        return "episode_unmapped"
    return "title_unmapped"


def _imdb_mapping_status_rank(status: str) -> int:
    if status == "title_unmapped":
        return 2
    if status == "episode_unmapped":
        return 1
    return 0


def _entry_found_source(entry: NetflixHistoryEntry) -> str:
    labels: List[str] = []
    if entry.metadata_title_type in {"anime_series", "anime_movie"} or any(
        any(domain in source.casefold() for domain in ("myanimelist", "anilist", "anime-planet", "anidb", "kitsu"))
        for source in entry.metadata_sources
    ):
        labels.append("anime")
    if _entry_has_imdb_title_metadata(entry) or _entry_has_imdb_episode_metadata(entry):
        labels.append("imdb")
    return "+".join(dict.fromkeys(labels))


def build_unmapped_imdb_override_rows(
    entries: List[NetflixHistoryEntry],
    overrides: Optional[Dict[str, NetflixTitleOverride]] = None,
) -> List[Dict[str, str]]:
    rows_by_title: Dict[Tuple[str, str, str], Dict[str, str]] = {}
    overrides = {
        _normalize_episode_title_override_key(raw_title): _coerce_episode_title_override(
            override,
            source_label=f"build_unmapped_imdb_override_rows[{raw_title}]",
        )
        for raw_title, override in (overrides or {}).items()
    }
    for entry in entries:
        netflix_title = (entry.parsed.title or entry.raw_title).strip()
        season_name = (entry.parsed.season_title or "").strip()
        netflix_episode_title = (entry.parsed.episode_title or "").strip()
        if not netflix_title:
            continue
        override = next(
            (overrides[key] for key in _build_episode_title_override_lookup_keys(entry.parsed) if key in overrides),
            None,
        )
        imdb_mapping_status = _entry_imdb_mapping_status(entry)
        if imdb_mapping_status == "mapped" and override is None:
            continue
        had_override = "yes" if override is not None else ""
        title_mapped = _entry_has_imdb_title_metadata(entry)
        found_source = _entry_found_source(entry)

        has_resolved_title_hint = bool(entry.resolved_title and entry.resolved_title != netflix_title)
        mapped_title = entry.resolved_title if has_resolved_title_hint else ""
        if not mapped_title and override is not None and override.title:
            mapped_title = override.title
        mapped_year = str(entry.resolved_title_year or "") if (title_mapped or has_resolved_title_hint) else ""
        if not mapped_year and override is not None and override.year is not None:
            mapped_year = str(override.year)
        mapped_source_id = entry.metadata_parent_id or "" if title_mapped else ""
        if not mapped_source_id and override is not None and override.source_id:
            mapped_source_id = override.source_id
        mapped_episode_title = override.episode_title if override is not None and override.episode_title else ""
        row_key = (netflix_title, season_name, netflix_episode_title)
        row = rows_by_title.get(row_key)
        if row is None:
            rows_by_title[row_key] = {
                "netflix_original_title": entry.raw_title.strip(),
                "netflix_title": netflix_title,
                "season_name": season_name,
                "netflix_episode_title": netflix_episode_title,
                "expected_type": entry.expected_type,
                "imdb_mapping_status": imdb_mapping_status,
                "title": mapped_title,
                "year": mapped_year,
                "source_id": mapped_source_id,
                "episode_title": mapped_episode_title,
                "found_source": found_source,
                "had_override": had_override,
            }
            continue

        if not row["title"] and mapped_title:
            row["title"] = mapped_title
        if not row["year"] and mapped_year:
            row["year"] = mapped_year
        if not row["source_id"] and mapped_source_id:
            row["source_id"] = mapped_source_id
        if not row["episode_title"] and mapped_episode_title:
            row["episode_title"] = mapped_episode_title
        if not row["found_source"] and found_source:
            row["found_source"] = found_source
        if not row["expected_type"] and entry.expected_type:
            row["expected_type"] = entry.expected_type
        if _imdb_mapping_status_rank(imdb_mapping_status) > _imdb_mapping_status_rank(row["imdb_mapping_status"]):
            row["imdb_mapping_status"] = imdb_mapping_status
        if not row["had_override"] and had_override:
            row["had_override"] = had_override

    return sorted(
        rows_by_title.values(),
        key=lambda item: (
            item["netflix_title"].casefold(),
            item["season_name"].casefold(),
            item["netflix_episode_title"].casefold(),
        ),
    )


def summarize_unmapped_imdb_override_rows(rows: List[Dict[str, str]]) -> Dict[str, int]:
    failed_rows = [row for row in rows if row.get("imdb_mapping_status") != "mapped"]
    override_pass_rows = [
        row for row in rows
        if row.get("imdb_mapping_status") == "mapped" and row.get("had_override") == "yes"
    ]
    with_override = sum(1 for row in failed_rows if row.get("had_override") == "yes")
    return {
        "export_total": len(rows),
        "failed": len(failed_rows),
        "override_passed": len(override_pass_rows),
        "total": len(failed_rows),
        "with_override": with_override,
        "without_override": len(failed_rows) - with_override,
    }


def export_unmapped_imdb_overrides(
    entries: List[NetflixHistoryEntry],
    output_path: str,
    overrides: Optional[Dict[str, NetflixTitleOverride]] = None,
) -> Path:
    target_path = Path(output_path).expanduser().resolve()
    target_path.parent.mkdir(parents=True, exist_ok=True)
    rows = build_unmapped_imdb_override_rows(entries, overrides=overrides)
    with target_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "netflix_original_title",
                "netflix_title",
                "season_name",
                "netflix_episode_title",
                "expected_type",
                "imdb_mapping_status",
                "title",
                "year",
                "source_id",
                "episode_title",
                "found_source",
                "had_override",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)
    return target_path


def default_unmapped_imdb_output_path(csv_path: str) -> Path:
    source_path = Path(csv_path).expanduser()
    return Path.cwd() / f"{source_path.stem}{UNMAPPED_IMDB_REPORT_SUFFIX}"


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
                    watch_dates=_format_leaf_watch_dates(watched_at_values),
                    **_entry_metadata_row_fields(title_entries[0], row_item_type="movie"),
                    item_type="movie",
                )
            )
            continue

        series_row_index = len(rows)
        rows.append(
            WatchTableRow(
                level=0,
                title=title,
                year=str(_entry_title_year(title_entries[0]) or ""),
                episode_title="",
                views=_format_group_views(watched_at_values),
                watch_dates=_format_group_watch_dates(watched_at_values),
                **_entry_metadata_row_fields(title_entries[0], row_item_type="series"),
                item_type="series",
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
                season_episode_infos = metadata_season_map[season_number]
                watched_count = 0
                season_rows: List[WatchTableRow] = []
                for episode_info in sorted(season_episode_infos, key=lambda item: item.episode):
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
                                watch_dates=_format_leaf_watch_dates(watched_at_values),
                                **_entry_metadata_row_fields(
                                    first_entry,
                                    row_item_type="episode",
                                    source_id_override=getattr(episode_info, "id", None) or first_entry.resolved_episode_source_id,
                                    average_rating_override=getattr(episode_info, "rating", None),
                                    num_votes_override=getattr(episode_info, "votes", None),
                                ),
                                item_type="episode",
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
                            watch_dates="",
                            **_entry_metadata_row_fields(
                                title_entries[0],
                                row_item_type="episode",
                                source_id_override=getattr(episode_info, "id", None),
                                average_rating_override=getattr(episode_info, "rating", None),
                                num_votes_override=getattr(episode_info, "votes", None),
                            ),
                            item_type="episode",
                        )
                    )

                rows.append(
                    WatchTableRow(
                        level=1,
                        title=display_season_title,
                        year=_format_year_range([episode_info.year for episode_info in season_episode_infos]),
                        season=str(season_number),
                        episode=_format_progress(watched_count, len(metadata_season_map[season_number])),
                        episode_title="",
                        views=_format_group_views([]),
                        watch_dates=_format_group_watch_dates([]),
                        **_entry_metadata_row_fields(
                            title_entries[0],
                            row_item_type="season",
                            runtime_minutes_override=_format_aggregate_runtime(title_entries[0].metadata_runtime_minutes, len(season_episode_infos)),
                            average_rating_override=(sum([float(v) for v in [getattr(episode_info, "rating", None) for episode_info in season_episode_infos] if v is not None]) / len([v for v in [getattr(episode_info, "rating", None) for episode_info in season_episode_infos] if v is not None])) if [v for v in [getattr(episode_info, "rating", None) for episode_info in season_episode_infos] if v is not None] else None,
                            num_votes_override=sum([int(v) for v in [getattr(episode_info, "votes", None) for episode_info in season_episode_infos] if v is not None]) if [v for v in [getattr(episode_info, "votes", None) for episode_info in season_episode_infos] if v is not None] else None,
                        ),
                        item_type="season",
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
                        year=_format_year_range([_entry_episode_year(entry) or _entry_title_year(entry) for entry in season_entries]),
                        season=str(season_number) if season_number is not None else "",
                        episode_title="",
                        views=_format_group_views([entry.watched_at for entry in season_entries]),
                        watch_dates=_format_group_watch_dates([entry.watched_at for entry in season_entries]),
                        **_entry_metadata_row_fields(
                            season_entries[0],
                            row_item_type="season",
                            runtime_minutes_override=_format_aggregate_runtime(season_entries[0].metadata_runtime_minutes, len(season_entries)),
                            average_rating_override=(sum([float(v) for v in [entry.resolved_episode_rating for entry in season_entries] if v is not None]) / len([v for v in [entry.resolved_episode_rating for entry in season_entries] if v is not None])) if [v for v in [entry.resolved_episode_rating for entry in season_entries] if v is not None] else None,
                            num_votes_override=sum([int(v) for v in [entry.resolved_episode_votes for entry in season_entries] if v is not None]) if [v for v in [entry.resolved_episode_votes for entry in season_entries] if v is not None] else None,
                        ),
                        item_type="season",
                    )
                )
                for entry in _build_episode_rows(
                    season_entries,
                    level=2,
                    season_override=season_number,
                    season_title_override=display_season_title if display_season_title else None,
                ):
                    rows.append(entry)

            rows[series_row_index].runtime_minutes = _aggregate_direct_child_runtime(
                rows,
                series_row_index,
                episode_runtime_minutes=title_entries[0].metadata_runtime_minutes,
            )

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
                    year=_format_year_range([_entry_episode_year(entry) or _entry_title_year(entry) for entry in season_entries]),
                    season=str(season_number) if season_number is not None else "",
                    episode_title="",
                    views=_format_group_views([entry.watched_at for entry in season_entries]),
                    watch_dates=_format_group_watch_dates([entry.watched_at for entry in season_entries]),
                    **_entry_metadata_row_fields(
                        season_entries[0],
                        row_item_type="season",
                        runtime_minutes_override=_format_aggregate_runtime(season_entries[0].metadata_runtime_minutes, len(season_entries)),
                        average_rating_override=(sum([float(v) for v in [entry.resolved_episode_rating for entry in season_entries] if v is not None]) / len([v for v in [entry.resolved_episode_rating for entry in season_entries] if v is not None])) if [v for v in [entry.resolved_episode_rating for entry in season_entries] if v is not None] else None,
                        num_votes_override=sum([int(v) for v in [entry.resolved_episode_votes for entry in season_entries] if v is not None]) if [v for v in [entry.resolved_episode_votes for entry in season_entries] if v is not None] else None,
                    ),
                    item_type="season",
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

        rows[series_row_index].runtime_minutes = _aggregate_direct_child_runtime(
            rows,
            series_row_index,
            episode_runtime_minutes=title_entries[0].metadata_runtime_minutes,
        )

    return _finalize_watch_table_rows(rows)


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
                watch_dates=_format_leaf_watch_dates(watched_at_values),
                **_entry_metadata_row_fields(first_entry, row_item_type="episode"),
                item_type="episode",
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
            "source_id": row.source_id,
            "title_type": row.title_type,
            "runtime_minutes": row.runtime_minutes,
            "genres": row.genres,
            "average_rating": row.average_rating,
            "num_votes": row.num_votes,
            "season": row.season,
            "season_title": row.season_title,
            "episode": row.episode,
            "episode_title": row.episode_title,
            "views": row.views,
            "watch_dates": row.watch_dates,
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
    _configure_utf8_output()
    parser = build_argument_parser()
    args = parser.parse_args()

    try:
        selected_columns = parse_table_columns(args.columns)
    except ValueError as exc:
        parser.error(str(exc))
        return

    try:
        episode_title_overrides = load_episode_title_overrides(args.episode_title_overrides)
    except ValueError as exc:
        parser.error(str(exc))
        return

    metadata_manager = None if args.no_metadata else get_metadata_manager()
    analyzer = NetflixWatchStatusAnalyzer(
        metadata_manager=metadata_manager,
        episode_title_overrides=episode_title_overrides,
    )
    entries = analyzer.load_entries(args.csv_path)
    results = analyzer.analyze(entries)
    unmapped_rows = build_unmapped_imdb_override_rows(entries, overrides=episode_title_overrides)
    unmapped_stats = summarize_unmapped_imdb_override_rows(unmapped_rows)
    unmapped_output_path = export_unmapped_imdb_overrides(
        entries,
        str(default_unmapped_imdb_output_path(args.csv_path)),
        overrides=episode_title_overrides,
    )

    if args.json:
        print(json.dumps(results, indent=2, ensure_ascii=False))
        return

    if args.table:
        render_watch_table(entries, selected_columns)
        return

    if args.webapp_export:
        output_path = export_webapp_report(args.csv_path, results, entries, selected_columns, args.webapp_export)
        safe_write_line(f"Webapp exported to: {output_path}")
        safe_write_line(f"IMDb-unmapped override rows: {unmapped_stats['total']}")
        safe_write_line(f"IMDb-unmapped rows with existing override: {unmapped_stats['with_override']}")
        safe_write_line(f"IMDb-unmapped rows without override: {unmapped_stats['without_override']}")
        safe_write_line(
            f"IMDb override CSV rows exported: {unmapped_stats['export_total']} ({unmapped_stats['failed']} failed, {unmapped_stats['override_passed']} passed via override)"
        )
        safe_write_line(f"IMDb-unmapped override template exported to: {unmapped_output_path}")
        return

    print_text_summary(results)
    safe_write_line()
    safe_write_line(f"IMDb-unmapped override rows: {unmapped_stats['total']}")
    safe_write_line(f"IMDb-unmapped rows with existing override: {unmapped_stats['with_override']}")
    safe_write_line(f"IMDb-unmapped rows without override: {unmapped_stats['without_override']}")
    safe_write_line(
        f"IMDb override CSV rows exported: {unmapped_stats['export_total']} ({unmapped_stats['failed']} failed, {unmapped_stats['override_passed']} passed via override)"
    )
    safe_write_line(f"IMDb-unmapped override template exported to: {unmapped_output_path}")


if __name__ == "__main__":
    main()