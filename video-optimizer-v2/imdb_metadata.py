import gzip
import inspect
import logging
import math
import os
import re
import sqlite3
import time
from email.utils import parsedate_to_datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union

# pyright: reportMissingImports=false, reportMissingModuleSource=false
import datetime
import requests
from rapidfuzz import fuzz, process
from tqdm import tqdm

from metadata_provider import BaseMetadataProvider, EpisodeInfo, MatchResult, TitleInfo


CacheValue = Union[MatchResult, EpisodeInfo, List[EpisodeInfo]]
RowLike = Union[sqlite3.Row, Dict[str, Any]]


class IMDbDataProvider(BaseMetadataProvider):
    DATASETS = {
        "title.basics": "https://datasets.imdbws.com/title.basics.tsv.gz",
        "title.episode": "https://datasets.imdbws.com/title.episode.tsv.gz",
        "title.ratings": "https://datasets.imdbws.com/title.ratings.tsv.gz",
        "title.akas": "https://datasets.imdbws.com/title.akas.tsv.gz",
    }

    REQUIRED_COLUMNS = {
        "title.basics": [
            "tconst",
            "titleType",
            "primaryTitle",
            "originalTitle",
            "isAdult",
            "startYear",
            "endYear",
            "runtimeMinutes",
            "genres",
        ],
        "title.episode": ["tconst", "parentTconst", "seasonNumber", "episodeNumber"],
        "title.ratings": ["tconst", "averageRating", "numVotes"],
        "title.akas": ["titleId", "title", "language", "region"],
    }

    MIN_VOTES_THRESHOLD = 200
    RECENT_YEAR_CUTOFF = 1975
    FILTER_ADULT_CONTENT = True
    ALLOWED_TITLE_TYPES = ["movie", "tvSeries", "tvMiniSeries"]
    FILTER_AKA_LANGUAGES = True
    AKA_ALLOWED_LANGUAGES = ("en", "ja", "jp")

    MAX_RETRIES = 3
    YEAR_TOLERANCE = 2
    FTS_LIMIT_WITH_YEAR = 200
    FTS_LIMIT_WITHOUT_YEAR = 300
    FUZZY_MATCH_LIMIT = 200
    MAX_CANDIDATES = 1000
    EPISODE_FUZZY_LIMIT = 750

    YEAR_EXACT_BONUS = 40
    YEAR_CLOSE_BONUS = 20
    VOTE_BONUS_CAP = 20
    VOTE_BONUS_MULTIPLIER = 4
    YEAR_RECENCY_MAX_BONUS = 20
    YEAR_RECENCY_DECAY_YEARS = 10
    EXACT_PRIMARY_MATCH_SCORE = 1000.0
    EXACT_ALIAS_MATCH_SCORE = 950.0
    PREFIX_SCORE_BASE = 700.0
    PREFIX_SCORE_RANGE = 140.0
    PREFIX_SCORE_CAP = 899.0
    FUZZY_SCORE_BASE = 400.0
    FUZZY_SCORE_RANGE = 260.0
    FUZZY_SCORE_CAP = 749.0

    SEARCH_CACHE_MAX = 1000
    SEARCH_CACHE_EVICT = 500
    TITLE_CACHE_MAX = 5000
    TITLE_CACHE_EVICT = 1000
    EXACT_MATCH_LIMIT = 50
    PREFIX_MATCH_LIMIT = 100
    SQL_TIMING_ENABLED = False

    TITLE_TYPE_CODES = {
        "movie": 1,
        "tvSeries": 2,
        "tvMiniSeries": 3,
        "tvEpisode": 4,
    }
    TITLE_TYPE_NAMES = {
        1: "movie",
        2: "tvSeries",
        3: "tvMiniSeries",
    }
    PREFERRED_TYPE_HINT_CODES = {
        "movie": {TITLE_TYPE_CODES["movie"]},
        "tv": {TITLE_TYPE_CODES["tvSeries"], TITLE_TYPE_CODES["tvMiniSeries"]},
    }

    def __init__(self):
        super().__init__("imdb", provider_weight=0.9)
        self._search_cache: Dict[str, CacheValue] = {}
        self._title_cache: Dict[int, TitleInfo] = {}
        self._db_path = os.path.join(self.cache_dir, "imdb_data.db")
        self.CACHE_EXPIRY_DATASETS = list(self.DATASETS.keys())
        self._connection_pool: List[sqlite3.Connection] = []
        self._pool_size = 3
        self._db_loaded_once = False
        self._db_loaded_until_ts: Optional[int] = None
        self._init_database()
        self._load_cache_duration()

    def _clear_loaded_state(self) -> None:
        self._db_loaded_once = False
        self._db_loaded_until_ts = None

    def _mark_database_loaded(self, now_ts: Optional[int] = None) -> None:
        resolved_now_ts = now_ts if now_ts is not None else int(time.time())
        self._db_loaded_once = True
        try:
            self._db_loaded_until_ts = int(self.get_cache_expiry().timestamp())
        except Exception:
            self._db_loaded_until_ts = resolved_now_ts + int(self.cache_duration.total_seconds())

    def _get_connection(self) -> sqlite3.Connection:
        if self._connection_pool:
            return self._connection_pool.pop()

        conn = sqlite3.connect(self._db_path, timeout=30.0)
        conn.execute("PRAGMA query_only=ON")
        conn.execute("PRAGMA cache_size=50000")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA mmap_size=268435456")
        conn.row_factory = sqlite3.Row
        return conn

    def _return_connection(self, conn: sqlite3.Connection) -> None:
        if len(self._connection_pool) < self._pool_size:
            self._connection_pool.append(conn)
        else:
            conn.close()

    def _close_connection_pool(self) -> None:
        while self._connection_pool:
            conn = self._connection_pool.pop()
            try:
                conn.close()
            except Exception:
                pass

    def _reset_database_for_reload(self) -> None:
        self._close_connection_pool()
        self._search_cache.clear()
        self._title_cache.clear()
        self._clear_loaded_state()

        for suffix in ("", "-wal", "-shm"):
            db_file = f"{self._db_path}{suffix}"
            try:
                os.remove(db_file)
            except FileNotFoundError:
                continue
            except PermissionError:
                logging.warning("Could not remove %s during IMDb reload", db_file)

        self._init_database()

    def _init_database(self) -> None:
        with sqlite3.connect(self._db_path, timeout=30.0) as conn:
            conn.execute("PRAGMA busy_timeout=30000")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=10000")
            conn.execute("PRAGMA temp_store=MEMORY")
            conn.execute("PRAGMA page_size=32768")
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS title_core (
                    id INTEGER PRIMARY KEY,
                    title TEXT NOT NULL,
                    original_title TEXT,
                    type INTEGER NOT NULL,
                    year INTEGER,
                    end_year INTEGER,
                    runtime_minutes INTEGER,
                    genres TEXT,
                    rating INTEGER,
                    votes INTEGER
                ) WITHOUT ROWID;

                CREATE TABLE IF NOT EXISTS title_search (
                    title_id INTEGER NOT NULL,
                    search_title TEXT NOT NULL,
                    search_title_lower TEXT NOT NULL,
                    is_primary INTEGER NOT NULL,
                    PRIMARY KEY (title_id, search_title)
                ) WITHOUT ROWID;

                CREATE TABLE IF NOT EXISTS episode_core (
                    id INTEGER PRIMARY KEY,
                    parent_id INTEGER NOT NULL,
                    season INTEGER,
                    episode INTEGER,
                    title TEXT NOT NULL,
                    year INTEGER,
                    rating INTEGER,
                    votes INTEGER
                ) WITHOUT ROWID;

                CREATE TABLE IF NOT EXISTS episode_search (
                    episode_id INTEGER NOT NULL,
                    parent_id INTEGER NOT NULL,
                    search_title TEXT NOT NULL,
                    search_title_lower TEXT NOT NULL,
                    is_primary INTEGER NOT NULL,
                    PRIMARY KEY (episode_id, search_title)
                ) WITHOUT ROWID;

                CREATE TABLE IF NOT EXISTS data_version (
                    dataset TEXT PRIMARY KEY,
                    updated INTEGER,
                    expires_at INTEGER,
                    default_ttl INTEGER,
                    last_modified INTEGER
                ) WITHOUT ROWID;

                CREATE VIRTUAL TABLE IF NOT EXISTS title_fts USING fts5(
                    title,
                    title_id UNINDEXED,
                    tokenize='porter unicode61'
                );
                """
            )
            conn.commit()

    def _load_cache_duration(self) -> None:
        try:
            super()._load_cache_duration()
        except Exception as exc:
            logging.debug("Could not load cache duration for IMDb provider: %s", exc)

    def _persist_cache_duration(self) -> None:
        try:
            super()._persist_cache_duration()
        except Exception:
            logging.debug("Could not persist cache duration for IMDb provider")

    def _upsert_dataset_version(
        self,
        conn: sqlite3.Connection,
        dataset_name: str,
        source_ts: Optional[int] = None,
    ) -> None:
        now_ts = int(time.time())
        expires_at = now_ts + int(self.cache_duration.total_seconds())
        src_ts = int(source_ts) if source_ts else now_ts
        ttl_days = int(self.cache_duration.days) or self.DEFAULT_TTL_DAYS

        try:
            row = conn.execute(
                "SELECT default_ttl FROM data_version WHERE dataset = ? LIMIT 1",
                (dataset_name,),
            ).fetchone()
            if row and row[0]:
                ttl_days = int(row[0])
        except Exception:
            pass

        conn.execute(
            """
            INSERT OR REPLACE INTO data_version (dataset, expires_at, default_ttl, last_modified, updated)
            VALUES (?, ?, ?, ?, ?)
            """,
            (dataset_name, expires_at, ttl_days, src_ts, now_ts),
        )

    def _is_data_current(self) -> bool:
        conn = self._get_connection()
        try:
            ds_list = list(getattr(self, "CACHE_EXPIRY_DATASETS", []) or [])
            now_ts = int(time.time())
            count_current = 0

            if ds_list:
                placeholders = ",".join("?" for _ in ds_list)
                cur = conn.execute(
                    f"SELECT dataset, expires_at, updated FROM data_version WHERE dataset IN ({placeholders})",
                    tuple(ds_list),
                )
                rows = {row[0]: row for row in cur.fetchall()}

                for dataset_name in ds_list:
                    row = rows.get(dataset_name)
                    if not row:
                        continue
                    expires_at = row[1]
                    updated = row[2]
                    try:
                        if expires_at and int(expires_at) > now_ts:
                            count_current += 1
                            continue
                    except Exception:
                        pass
                    try:
                        if updated and int(updated) + int(self.cache_duration.total_seconds()) > now_ts:
                            count_current += 1
                        else:
                            return False
                    except Exception:
                        return False

                if count_current != len(ds_list):
                    return False

            row = conn.execute("SELECT 1 FROM title_core LIMIT 1").fetchone()
            return row is not None
        except Exception:
            return False
        finally:
            self._return_connection(conn)

    def _has_title_search_data(self) -> bool:
        conn = self._get_connection()
        try:
            return conn.execute("SELECT 1 FROM title_search LIMIT 1").fetchone() is not None
        except Exception:
            return False
        finally:
            self._return_connection(conn)

    def _has_episode_title_data(self) -> bool:
        conn = self._get_connection()
        try:
            return conn.execute("SELECT 1 FROM episode_core WHERE title != '' LIMIT 1").fetchone() is not None
        except Exception:
            return False
        finally:
            self._return_connection(conn)

    def _verify_data_integrity(self) -> None:
        with sqlite3.connect(self._db_path) as conn:
            logging.info("title_core=%s", conn.execute("SELECT COUNT(*) FROM title_core").fetchone()[0])
            logging.info("title_search=%s", conn.execute("SELECT COUNT(*) FROM title_search").fetchone()[0])
            logging.info("episode_core=%s", conn.execute("SELECT COUNT(*) FROM episode_core").fetchone()[0])
            logging.info("episode_search=%s", conn.execute("SELECT COUNT(*) FROM episode_search").fetchone()[0])

    def _ensure_data_loaded(self) -> None:
        now_ts = int(time.time())
        if getattr(self, "_db_loaded_once", False):
            loaded_until_ts = getattr(self, "_db_loaded_until_ts", None)
            if loaded_until_ts is None or now_ts < loaded_until_ts:
                return
            self._clear_loaded_state()

        if self._is_data_current() and self._has_title_search_data() and self._has_episode_title_data():
            self._mark_database_loaded(now_ts)
            return

        self._reset_database_for_reload()

        dataset_paths: Dict[str, str] = {}
        source_timestamps: Dict[str, int] = {}
        for dataset_name in self.DATASETS:
            dataset_path, source_ts = self._ensure_dataset_cache_file(dataset_name)
            dataset_paths[dataset_name] = dataset_path
            source_timestamps[dataset_name] = source_ts

        self._rebuild_read_optimized_tables(dataset_paths, source_timestamps)
        self._verify_data_integrity()
        self._optimize_database_for_reads()
        self._mark_database_loaded(now_ts)

    def _ensure_dataset_cache_file(self, dataset_name: str) -> Tuple[str, int]:
        url = self.DATASETS[dataset_name]
        gz_cache = os.path.join(self.cache_dir, f"{dataset_name}.tsv.gz")
        source_last_modified_ts: Optional[int] = None

        need_download = True
        if os.path.exists(gz_cache):
            mtime = os.path.getmtime(gz_cache)
            age_days = (time.time() - mtime) / (24 * 60 * 60)
            if age_days < self.cache_duration.days:
                need_download = False

        if need_download:
            for attempt in range(self.MAX_RETRIES):
                try:
                    response = requests.get(url, stream=True)
                    response.raise_for_status()
                    total_size = int(response.headers.get("content-length", 0))
                    lm_header = response.headers.get("last-modified")
                    if lm_header:
                        try:
                            source_last_modified_ts = int(parsedate_to_datetime(lm_header).timestamp())
                        except Exception:
                            source_last_modified_ts = None

                    with tqdm(total=total_size, desc=f"Downloading {dataset_name}", unit="B", unit_scale=True) as pbar:
                        with open(gz_cache, "wb") as handle:
                            for chunk in response.iter_content(chunk_size=8192):
                                if chunk:
                                    handle.write(chunk)
                                    pbar.update(len(chunk))
                    break
                except Exception as exc:
                    logging.error("Error downloading %s (attempt %s): %s", dataset_name, attempt + 1, exc)
                    if attempt == self.MAX_RETRIES - 1:
                        raise
        else:
            source_last_modified_ts = int(os.path.getmtime(gz_cache))

        return gz_cache, source_last_modified_ts or int(time.time())

    def _rebuild_read_optimized_tables(
        self,
        dataset_paths: Dict[str, str],
        source_timestamps: Dict[str, int],
    ) -> None:
        ratings_by_id, qualifying_title_ids = self._load_ratings_map(dataset_paths["title.ratings"])

        with sqlite3.connect(self._db_path, timeout=60.0) as conn:
            self._timed_execute(conn, "PRAGMA synchronous=OFF")
            self._timed_execute(conn, "PRAGMA temp_store=MEMORY")
            self._timed_execute(conn, "PRAGMA cache_size=100000")

            self._timed_execute(conn, "DELETE FROM title_fts")
            self._timed_execute(conn, "DELETE FROM title_search")
            self._timed_execute(conn, "DELETE FROM episode_search")
            self._timed_execute(conn, "DELETE FROM episode_core")
            self._timed_execute(conn, "DELETE FROM title_core")

            conn.executescript(
                """
                CREATE TEMP TABLE temp_episode_basics (
                    id INTEGER PRIMARY KEY,
                    title TEXT NOT NULL,
                    title_lower TEXT NOT NULL,
                    year INTEGER,
                    rating INTEGER,
                    votes INTEGER
                ) WITHOUT ROWID;

                CREATE TEMP TABLE temp_episode_links (
                    id INTEGER PRIMARY KEY,
                    parent_id INTEGER NOT NULL,
                    season INTEGER,
                    episode INTEGER
                ) WITHOUT ROWID;

                CREATE TEMP TABLE temp_aka_staging (
                    title_id INTEGER NOT NULL,
                    title TEXT NOT NULL,
                    title_lower TEXT NOT NULL,
                    PRIMARY KEY (title_id, title_lower)
                ) WITHOUT ROWID;
                """
            )

            surviving_parent_ids = self._load_title_basics_into_final_tables(
                conn,
                dataset_paths["title.basics"],
                ratings_by_id,
                qualifying_title_ids,
            )
            self._load_episode_links_into_temp(conn, dataset_paths["title.episode"], surviving_parent_ids)
            self._materialize_episode_core(conn)
            self._populate_primary_search_tables(conn)
            self._load_aka_search_rows(conn, dataset_paths["title.akas"])

            for dataset_name, source_ts in source_timestamps.items():
                self._upsert_dataset_version(conn, dataset_name, source_ts)

            conn.executescript(
                """
                DROP TABLE IF EXISTS temp_aka_staging;
                DROP TABLE IF EXISTS temp_episode_links;
                DROP TABLE IF EXISTS temp_episode_basics;
                """
            )
            conn.commit()

    def _load_ratings_map(self, dataset_path: str) -> Tuple[Dict[int, Tuple[Optional[int], Optional[int]]], set[int]]:
        ratings_by_id: Dict[int, Tuple[Optional[int], Optional[int]]] = {}
        qualifying_title_ids: set[int] = set()
        required_cols = self.REQUIRED_COLUMNS["title.ratings"]

        with gzip.open(dataset_path, "rt", encoding="utf-8") as handle:
            header = handle.readline().strip().split("\t")
            col_indices = [header.index(col) for col in required_cols]
            with tqdm(desc="Processing title.ratings", unit="rows") as pbar:
                for line in handle:
                    if not line.strip():
                        continue
                    fields = line.rstrip("\n\r").split("\t")
                    row_data = self._extract_row_values(fields, required_cols, col_indices)
                    data_dict = dict(zip(required_cols, row_data))
                    title_id = self._parse_tconst_int(data_dict.get("tconst"))
                    if title_id is None:
                        continue
                    rating = self._as_float(data_dict.get("averageRating"))
                    votes = self._as_int(data_dict.get("numVotes"))
                    rating_int = int(rating * 10) if rating is not None else None
                    ratings_by_id[title_id] = (rating_int, votes)
                    if self.MIN_VOTES_THRESHOLD is None or (votes is not None and votes >= self.MIN_VOTES_THRESHOLD):
                        qualifying_title_ids.add(title_id)
                    pbar.update(1)

        return ratings_by_id, qualifying_title_ids

    def _load_title_basics_into_final_tables(
        self,
        conn: sqlite3.Connection,
        dataset_path: str,
        ratings_by_id: Dict[int, Tuple[Optional[int], Optional[int]]],
        qualifying_title_ids: set[int],
    ) -> set[int]:
        required_cols = self.REQUIRED_COLUMNS["title.basics"]
        surviving_parent_ids: set[int] = set()
        title_core_batch: List[Tuple[Any, ...]] = []
        episode_basics_batch: List[Tuple[Any, ...]] = []
        chunk_size = 100000

        with gzip.open(dataset_path, "rt", encoding="utf-8") as handle:
            header = handle.readline().strip().split("\t")
            col_indices = [header.index(col) for col in required_cols]
            with tqdm(desc="Processing title.basics", unit="rows") as pbar:
                for line in handle:
                    if not line.strip():
                        continue
                    fields = line.rstrip("\n\r").split("\t")
                    row_data = self._extract_row_values(fields, required_cols, col_indices)
                    if not self._should_keep_title("title.basics", row_data, required_cols):
                        continue

                    data_dict = dict(zip(required_cols, row_data))
                    title_id = self._parse_tconst_int(data_dict.get("tconst"))
                    if title_id is None:
                        continue

                    title_type = self._as_str(data_dict.get("titleType"))
                    rating_int, votes = ratings_by_id.get(title_id, (None, None))

                    if title_type == "tvEpisode":
                        title = self._normalize_space_collapsed_text(self._as_str(data_dict.get("primaryTitle")))
                        if title:
                            episode_basics_batch.append(
                                (title_id, title, title.lower(), self._as_int(data_dict.get("startYear")), rating_int, votes)
                            )
                        if len(episode_basics_batch) >= chunk_size:
                            conn.executemany(
                                "INSERT OR REPLACE INTO temp_episode_basics (id, title, title_lower, year, rating, votes) VALUES (?, ?, ?, ?, ?, ?)",
                                episode_basics_batch,
                            )
                            episode_basics_batch = []
                        pbar.update(1)
                        continue

                    if self.MIN_VOTES_THRESHOLD is not None and title_id not in qualifying_title_ids:
                        continue

                    title = self._normalize_space_collapsed_text(self._as_str(data_dict.get("primaryTitle")))
                    if not title:
                        continue
                    title_core_batch.append(
                        (
                            title_id,
                            title,
                            self._normalize_space_collapsed_text(self._as_str(data_dict.get("originalTitle"))) or None,
                            self.TITLE_TYPE_CODES.get(title_type or "movie", self.TITLE_TYPE_CODES["movie"]),
                            self._as_int(data_dict.get("startYear")),
                            self._as_int(data_dict.get("endYear")),
                            self._as_int(data_dict.get("runtimeMinutes")),
                            self._compress_genres(self._as_str(data_dict.get("genres"))),
                            rating_int,
                            votes,
                        )
                    )
                    surviving_parent_ids.add(title_id)
                    if len(title_core_batch) >= chunk_size:
                        conn.executemany(
                            """
                            INSERT OR REPLACE INTO title_core (
                                id, title, original_title, type, year, end_year, runtime_minutes, genres, rating, votes
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            title_core_batch,
                        )
                        title_core_batch = []
                    pbar.update(1)

        if title_core_batch:
            self._timed_executemany(
                conn,
                """
                INSERT OR REPLACE INTO title_core (
                    id, title, original_title, type, year, end_year, runtime_minutes, genres, rating, votes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                title_core_batch,
            )
        if episode_basics_batch:
            self._timed_executemany(
                conn,
                "INSERT OR REPLACE INTO temp_episode_basics (id, title, title_lower, year, rating, votes) VALUES (?, ?, ?, ?, ?, ?)",
                episode_basics_batch,
            )

        return surviving_parent_ids

    def _load_episode_links_into_temp(
        self,
        conn: sqlite3.Connection,
        dataset_path: str,
        surviving_parent_ids: set[int],
    ) -> None:
        required_cols = self.REQUIRED_COLUMNS["title.episode"]
        batch: List[Tuple[Any, ...]] = []
        chunk_size = 100000

        with gzip.open(dataset_path, "rt", encoding="utf-8") as handle:
            header = handle.readline().strip().split("\t")
            col_indices = [header.index(col) for col in required_cols]
            with tqdm(desc="Processing title.episode", unit="rows") as pbar:
                for line in handle:
                    if not line.strip():
                        continue
                    fields = line.rstrip("\n\r").split("\t")
                    row_data = self._extract_row_values(fields, required_cols, col_indices)
                    data_dict = dict(zip(required_cols, row_data))
                    episode_id = self._parse_tconst_int(data_dict.get("tconst"))
                    parent_id = self._parse_tconst_int(data_dict.get("parentTconst"))
                    if episode_id is None or parent_id is None or parent_id not in surviving_parent_ids:
                        continue
                    batch.append((episode_id, parent_id, self._as_int(data_dict.get("seasonNumber")), self._as_int(data_dict.get("episodeNumber"))))
                    if len(batch) >= chunk_size:
                        conn.executemany(
                            "INSERT OR REPLACE INTO temp_episode_links (id, parent_id, season, episode) VALUES (?, ?, ?, ?)",
                            batch,
                        )
                        batch = []
                    pbar.update(1)

        if batch:
            self._timed_executemany(
                conn,
                "INSERT OR REPLACE INTO temp_episode_links (id, parent_id, season, episode) VALUES (?, ?, ?, ?)",
                batch,
            )

    def _materialize_episode_core(self, conn: sqlite3.Connection) -> None:
        self._timed_execute(
            conn,
            """
            INSERT OR REPLACE INTO episode_core (id, parent_id, season, episode, title, year, rating, votes)
            SELECT l.id, l.parent_id, l.season, l.episode, b.title, b.year, b.rating, b.votes
            FROM temp_episode_links l
            JOIN temp_episode_basics b ON b.id = l.id
            """
        )

    def _populate_primary_search_tables(self, conn: sqlite3.Connection) -> None:
        self._timed_execute(
            conn,
            """
            INSERT OR IGNORE INTO title_search (title_id, search_title, search_title_lower, is_primary)
            SELECT id, title, lower(title), 1 FROM title_core WHERE title != ''
            """
        )
        self._timed_execute(
            conn,
            """
            INSERT OR IGNORE INTO episode_search (episode_id, parent_id, search_title, search_title_lower, is_primary)
            SELECT id, parent_id, title, lower(title), 1 FROM episode_core WHERE title != ''
            """
        )

    def _load_aka_search_rows(self, conn: sqlite3.Connection, dataset_path: str) -> None:
        required_cols = self.REQUIRED_COLUMNS["title.akas"]
        batch: List[Tuple[Any, ...]] = []
        chunk_size = 100000

        with gzip.open(dataset_path, "rt", encoding="utf-8") as handle:
            header = handle.readline().strip().split("\t")
            col_indices = [header.index(col) for col in required_cols]
            with tqdm(desc="Processing title.akas", unit="rows") as pbar:
                for line in handle:
                    if not line.strip():
                        continue
                    fields = line.rstrip("\n\r").split("\t")
                    row_data = self._extract_row_values(fields, required_cols, col_indices)
                    compressed = self._compress_row("title.akas", row_data, required_cols, None)
                    if not compressed:
                        continue
                    title_id, title, title_lower, _language, _region = compressed["row"]
                    batch.append((title_id, title, title_lower))
                    if len(batch) >= chunk_size:
                        conn.executemany(
                            "INSERT OR IGNORE INTO temp_aka_staging (title_id, title, title_lower) VALUES (?, ?, ?)",
                            batch,
                        )
                        batch = []
                    pbar.update(1)

        if batch:
            self._timed_executemany(
                conn,
                "INSERT OR IGNORE INTO temp_aka_staging (title_id, title, title_lower) VALUES (?, ?, ?)",
                batch,
            )

        self._timed_execute(
            conn,
            """
            INSERT OR IGNORE INTO title_search (title_id, search_title, search_title_lower, is_primary)
            SELECT a.title_id, a.title, a.title_lower, 0
            FROM temp_aka_staging a
            JOIN title_core t ON t.id = a.title_id
            """
        )
        self._timed_execute(
            conn,
            """
            INSERT OR IGNORE INTO episode_search (episode_id, parent_id, search_title, search_title_lower, is_primary)
            SELECT a.title_id, e.parent_id, a.title, a.title_lower, 0
            FROM temp_aka_staging a
            JOIN episode_core e ON e.id = a.title_id
            """
        )

    @staticmethod
    def _extract_row_values(fields: Sequence[str], required_cols: Sequence[str], col_indices: Sequence[int]) -> List[Any]:
        row_data: List[Any] = []
        integer_fields = {"startYear", "endYear", "runtimeMinutes", "seasonNumber", "episodeNumber", "numVotes"}
        for index, col_idx in enumerate(col_indices):
            value: Any = None
            if col_idx < len(fields):
                value = fields[col_idx]
                if value == "\\N" or value == "":
                    value = None
                elif required_cols[index] in integer_fields:
                    try:
                        value = int(value)
                    except (TypeError, ValueError):
                        value = None
                elif required_cols[index] == "isAdult":
                    value = 1 if value == "1" else 0
                elif required_cols[index] == "averageRating":
                    try:
                        value = float(value)
                    except (TypeError, ValueError):
                        value = None
            row_data.append(value)
        return row_data

    @staticmethod
    def _compress_genres(genres: Optional[str]) -> Optional[str]:
        if not genres:
            return None
        return ",".join(genres.split(",")[:3])

    @staticmethod
    def _as_str(value: Any) -> Optional[str]:
        return value if isinstance(value, str) else None

    @staticmethod
    def _as_int(value: Any) -> Optional[int]:
        return value if isinstance(value, int) else None

    @staticmethod
    def _as_float(value: Any) -> Optional[float]:
        return value if isinstance(value, float) else None

    @staticmethod
    def _row_value(row: RowLike, key: str) -> Any:
        if isinstance(row, sqlite3.Row):
            return row[key] if key in row.keys() else None
        return row.get(key)

    @staticmethod
    def _parse_tconst_int(value: Any) -> Optional[int]:
        if isinstance(value, str) and value.startswith("tt"):
            try:
                return int(value[2:])
            except (TypeError, ValueError):
                return None
        if isinstance(value, int):
            return value
        return None

    @staticmethod
    def _normalize_space_collapsed_text(text: Optional[str]) -> str:
        return " ".join((text or "").split())

    @staticmethod
    def _normalize_aka_language_code(code: Optional[str]) -> Optional[str]:
        normalized = IMDbDataProvider._normalize_space_collapsed_text(code).lower() or None
        if normalized == "jp":
            return "ja"
        return normalized

    @classmethod
    def _get_allowed_aka_languages(cls) -> Tuple[str, ...]:
        normalized_languages: List[str] = []
        for language in getattr(cls, "AKA_ALLOWED_LANGUAGES", ()):
            normalized = cls._normalize_aka_language_code(language)
            if normalized and normalized not in normalized_languages:
                normalized_languages.append(normalized)
        return tuple(normalized_languages)

    @classmethod
    def _get_allowed_aka_locale_codes(cls) -> Tuple[str, ...]:
        normalized_codes: List[str] = []
        for code in getattr(cls, "AKA_ALLOWED_LANGUAGES", ()):
            normalized = cls._normalize_space_collapsed_text(code).lower() or None
            if normalized and normalized not in normalized_codes:
                normalized_codes.append(normalized)
        return tuple(normalized_codes)

    @classmethod
    def _should_keep_aka_locale(cls, language: Optional[str], region: Optional[str]) -> bool:
        if not getattr(cls, "FILTER_AKA_LANGUAGES", True):
            return True
        normalized_language = cls._normalize_aka_language_code(language)
        normalized_region = cls._normalize_space_collapsed_text(region).lower() or None
        if normalized_language is None and normalized_region is None:
            return True
        allowed_codes = cls._get_allowed_aka_locale_codes()
        return (
            (normalized_language in allowed_codes if normalized_language else False)
            or (normalized_region in allowed_codes if normalized_region else False)
        )

    def _should_keep_title(
        self,
        dataset_name: str,
        row_data: List[Any],
        required_cols: List[str],
        include_episode_basics: bool = True,
    ) -> bool:
        if dataset_name == "title.basics":
            data_dict = dict(zip(required_cols, row_data))
            title_type = self._as_str(data_dict.get("titleType")) or ""
            is_episode = title_type == "tvEpisode"
            if is_episode and not include_episode_basics:
                return False
            if not is_episode and self.ALLOWED_TITLE_TYPES is not None and title_type not in self.ALLOWED_TITLE_TYPES:
                return False
            year = self._as_int(data_dict.get("startYear"))
            if self.RECENT_YEAR_CUTOFF is not None and year is not None and year < self.RECENT_YEAR_CUTOFF:
                return False
            if self.FILTER_ADULT_CONTENT and data_dict.get("isAdult") == 1:
                return False
        elif dataset_name == "title.ratings":
            votes = self._as_int(dict(zip(required_cols, row_data)).get("numVotes"))
            if self.MIN_VOTES_THRESHOLD is not None and votes is not None and votes < self.MIN_VOTES_THRESHOLD:
                return False
        return True

    def _compress_row(
        self,
        dataset_name: str,
        row_data: List[Any],
        required_cols: List[str],
        linkable_tconst_ints: Optional[set[int]],
    ) -> Optional[Dict[str, Tuple[Any, ...]]]:
        data_dict = dict(zip(required_cols, row_data))

        if dataset_name == "title.akas":
            title_id = self._parse_tconst_int(data_dict.get("titleId"))
            if title_id is None:
                return None
            if linkable_tconst_ints is not None and title_id not in linkable_tconst_ints:
                return None
            title = self._normalize_space_collapsed_text(self._as_str(data_dict.get("title")))
            if not title:
                return None
            language = self._normalize_space_collapsed_text(self._as_str(data_dict.get("language"))) or None
            region = self._normalize_space_collapsed_text(self._as_str(data_dict.get("region"))) or None
            if not self._should_keep_aka_locale(language, region):
                return None
            return {"row": (title_id, title, title.lower(), language, region)}

        return None

    def _apply_candidate_bonuses(
        self,
        row: RowLike,
        score: float,
        year: Optional[int],
        max_score: Optional[float] = None,
    ) -> float:
        total_score = score
        row_year = self._row_value(row, "year")
        if year is not None and isinstance(row_year, int):
            if row_year == year:
                total_score += self.YEAR_EXACT_BONUS
            elif abs(row_year - year) <= 1:
                total_score += self.YEAR_CLOSE_BONUS
        votes = self._row_value(row, "votes")
        if isinstance(votes, int) and votes > 0:
            total_score += min(self.VOTE_BONUS_CAP, self.VOTE_BONUS_MULTIPLIER * math.log10(max(1, votes)))
        if isinstance(row_year, int):
            current_year = datetime.datetime.now().year
            age = max(0, current_year - row_year)
            if age < self.YEAR_RECENCY_DECAY_YEARS:
                total_score += self.YEAR_RECENCY_MAX_BONUS * (self.YEAR_RECENCY_DECAY_YEARS - age) / float(self.YEAR_RECENCY_DECAY_YEARS)
        if max_score is not None:
            return min(total_score, max_score)
        return total_score

    def _get_exact_candidates(self, conn: sqlite3.Connection, title_lower: str, year: Optional[int]) -> List[sqlite3.Row]:
        year_clause = ""
        params: List[Any] = [title_lower]
        if year is not None:
            year_clause = " AND (c.year BETWEEN ? AND ? OR c.year IS NULL)"
            params.extend([year - self.YEAR_TOLERANCE, year + self.YEAR_TOLERANCE])
        params.append(self.EXACT_MATCH_LIMIT)

        return conn.execute(
            f"""
            SELECT c.id, c.title, c.type, c.year, c.end_year, c.runtime_minutes, c.genres,
                   CASE WHEN c.rating IS NULL THEN NULL ELSE c.rating / 10.0 END AS rating,
                   c.votes, CASE WHEN s.is_primary = 1 THEN 2 ELSE 1 END AS match_rank
            FROM title_search s
            JOIN title_core c ON c.id = s.title_id
            WHERE s.search_title_lower = ?
              {year_clause}
            ORDER BY s.is_primary DESC, c.votes DESC
            LIMIT ?
            """,
            tuple(params),
        ).fetchall()

    def _select_exact_match(
        self,
        candidates: List[sqlite3.Row],
        conn: sqlite3.Connection,
        year: Optional[int],
    ) -> Tuple[Optional[TitleInfo], float]:
        best_match = None
        best_score = -1.0
        for row in candidates:
            base_score = self.EXACT_PRIMARY_MATCH_SCORE if row["match_rank"] == 2 else self.EXACT_ALIAS_MATCH_SCORE
            total_score = self._apply_candidate_bonuses(row, base_score, year, max_score=base_score)
            if total_score > best_score:
                best_score = total_score
                best_match = self._create_title_info_from_row_fast(row, conn)
        return best_match, best_score

    @staticmethod
    def _normalize_title_for_similarity(title: str, compact: bool = False) -> str:
        normalized = (title or "").casefold()
        normalized = re.sub(r"[^\w\s]", " ", normalized)
        normalized = re.sub(r"\s+", " ", normalized).strip()
        if compact:
            return normalized.replace(" ", "")
        return normalized

    def _score_partial_title_match(
        self,
        query: str,
        candidate_title: str,
        row: RowLike,
        year: Optional[int],
    ) -> float:
        query_normalized = self._normalize_title_for_similarity(query)
        candidate_normalized = self._normalize_title_for_similarity(candidate_title)
        query_compact = self._normalize_title_for_similarity(query, compact=True)
        candidate_compact = self._normalize_title_for_similarity(candidate_title, compact=True)
        if not query_normalized or not candidate_normalized:
            return 0.0

        ratio = max(
            fuzz.ratio(query_normalized, candidate_normalized) / 100.0,
            fuzz.ratio(query_compact, candidate_compact) / 100.0,
        )
        query_words = set(query_normalized.split())
        candidate_words = set(candidate_normalized.split())
        coverage = (len(query_words & candidate_words) / len(query_words)) if query_words else 0.0
        quality = min(1.0, (ratio * 0.7) + (coverage * 0.3))

        is_prefix_match = (
            candidate_normalized.startswith(query_normalized)
            or candidate_compact.startswith(query_compact)
        )
        if is_prefix_match:
            base_score = self.PREFIX_SCORE_BASE + (quality * self.PREFIX_SCORE_RANGE)
            max_score = self.PREFIX_SCORE_CAP
        else:
            base_score = self.FUZZY_SCORE_BASE + (quality * self.FUZZY_SCORE_RANGE)
            max_score = self.FUZZY_SCORE_CAP

        return self._apply_candidate_bonuses(row, base_score, year, max_score=max_score)

    @staticmethod
    def _build_prefix_search_patterns(title_lower: str) -> List[str]:
        normalized = re.sub(r"\s+", " ", title_lower).strip()
        if not normalized:
            return []

        trimmed = normalized.rstrip(" .,:;!?/\\|_+-")
        if len(re.sub(r"[^a-z0-9]+", "", trimmed)) < 2:
            return []

        patterns = [
            f"{trimmed} %",
            f"{trimmed}:%",
            f"{trimmed}-%",
            f"{trimmed}.%",
            f"{trimmed}/%",
        ]
        if trimmed != normalized:
            patterns.append(f"{normalized}%")
        return list(dict.fromkeys(pattern for pattern in patterns if pattern))

    def _get_prefix_candidates(self, conn: sqlite3.Connection, title_lower: str, year: Optional[int]) -> List[sqlite3.Row]:
        patterns = self._build_prefix_search_patterns(title_lower)
        if not patterns:
            return []

        year_clause = ""
        params: List[Any] = [self.MIN_VOTES_THRESHOLD or 0]
        if year is not None:
            year_clause = " AND (c.year BETWEEN ? AND ? OR c.year IS NULL)"
            params.extend([year - self.YEAR_TOLERANCE, year + self.YEAR_TOLERANCE])

        pattern_clause = " OR ".join("s.search_title_lower LIKE ?" for _ in patterns)
        params.extend(patterns)
        params.append(self.PREFIX_MATCH_LIMIT)

        return conn.execute(
            f"""
            SELECT c.id, c.title, c.type, c.year, c.end_year, c.runtime_minutes, c.genres,
                   CASE WHEN c.rating IS NULL THEN NULL ELSE c.rating / 10.0 END AS rating,
                   c.votes, s.search_title AS matched_title,
                   CASE WHEN s.is_primary = 1 THEN 2 ELSE 1 END AS match_rank
            FROM title_search s
            JOIN title_core c ON c.id = s.title_id
            WHERE c.votes >= ?
              {year_clause}
              AND ({pattern_clause})
            ORDER BY LENGTH(s.search_title_lower), s.is_primary DESC, c.votes DESC
            LIMIT ?
            """,
            tuple(params),
        ).fetchall()

    def _extract_matches(self, query: str, choices: Dict[int, str], limit: int) -> List[Tuple[str, float, int]]:
        extractor = getattr(process, "extract", None)
        if callable(extractor):
            raw_matches = extractor(query, choices, scorer=fuzz.ratio, limit=limit)
            if isinstance(raw_matches, Iterable):
                matches: List[Tuple[str, float, int]] = []
                for candidate_title, score, candidate_id in raw_matches:
                    matches.append((str(candidate_title), float(score), int(candidate_id)))
                return matches
        scored = [(candidate_title, float(fuzz.ratio(query, candidate_title)), candidate_id) for candidate_id, candidate_title in choices.items()]
        scored.sort(key=lambda item: item[1], reverse=True)
        return scored[:limit]

    def _get_fuzzy_candidates(self, conn: sqlite3.Connection, title_lower: str, year: Optional[int]) -> List[sqlite3.Row]:
        candidates: List[sqlite3.Row] = []
        try:
            for fts_query in self._build_fts_queries(title_lower):
                if year is not None:
                    cursor = conn.execute(
                        """
                        SELECT c.id, c.title, c.type, c.year, c.end_year, c.runtime_minutes, c.genres,
                               CASE WHEN c.rating IS NULL THEN NULL ELSE c.rating / 10.0 END AS rating,
                               c.votes, f.score, f.title AS matched_title
                        FROM (
                            SELECT title_id, title, bm25(title_fts, 10.0) AS score
                            FROM title_fts
                            WHERE title_fts MATCH ?
                            ORDER BY score
                            LIMIT ?
                        ) f
                        JOIN title_core c ON c.id = f.title_id
                        WHERE (c.year BETWEEN ? AND ? OR c.year IS NULL)
                          AND c.votes >= ?
                        ORDER BY f.score, c.votes DESC
                        LIMIT ?
                        """,
                        (fts_query, self.FTS_LIMIT_WITH_YEAR, year - self.YEAR_TOLERANCE, year + self.YEAR_TOLERANCE, self.MIN_VOTES_THRESHOLD or 0, self.FTS_LIMIT_WITH_YEAR),
                    )
                else:
                    cursor = conn.execute(
                        """
                        SELECT c.id, c.title, c.type, c.year, c.end_year, c.runtime_minutes, c.genres,
                               CASE WHEN c.rating IS NULL THEN NULL ELSE c.rating / 10.0 END AS rating,
                               c.votes, f.score, f.title AS matched_title
                        FROM (
                            SELECT title_id, title, bm25(title_fts, 10.0) AS score
                            FROM title_fts
                            WHERE title_fts MATCH ?
                            ORDER BY score
                            LIMIT ?
                        ) f
                        JOIN title_core c ON c.id = f.title_id
                        WHERE c.votes >= ?
                        ORDER BY f.score, c.votes DESC
                        LIMIT ?
                        """,
                        (fts_query, self.FTS_LIMIT_WITHOUT_YEAR, self.MIN_VOTES_THRESHOLD or 0, self.FTS_LIMIT_WITHOUT_YEAR),
                    )
                candidates = cursor.fetchall()
                if candidates:
                    break
        except Exception as exc:
            logging.debug("FTS search failed: %s", exc)
        return candidates[: self.MAX_CANDIDATES]

    def _select_best_fuzzy_match(
        self,
        conn: sqlite3.Connection,
        query: str,
        candidates: List[Dict[str, Any]],
        year: Optional[int],
    ) -> Optional[MatchResult]:
        if not candidates:
            return None

        search_dict: Dict[int, str] = {}
        candidate_by_id: Dict[int, Dict[str, Any]] = {}
        for row in candidates:
            row_id = int(row["id"])
            if row_id in candidate_by_id:
                continue
            candidate_by_id[row_id] = row
            search_dict[row_id] = str(row.get("matched_title") or row["title"])

        title_matches = self._extract_matches(query, search_dict, self.FUZZY_MATCH_LIMIT)
        best_match = None
        best_score = 0.0
        for matched_title, _fuzzy_score, row_id in title_matches:
            row = candidate_by_id.get(row_id)
            if row is None:
                continue
            total_score = self._score_partial_title_match(query, matched_title, row, year)
            if total_score > best_score:
                best_score = total_score
                best_match = self._create_title_info_from_row_fast(row, conn)

        if best_match is None:
            return None
        return MatchResult(info=best_match, score=best_score, provider_weight=self.provider_weight)

    def find_title(self, title: str, year: Optional[int] = None) -> Optional[MatchResult]:
        self._ensure_data_loaded()
        cache_key = f"{title.lower()}_{year}"
        cached = self._search_cache.get(cache_key)
        if isinstance(cached, MatchResult):
            return cached

        conn = self._get_connection()
        try:
            title_lower = re.sub(r"\s+", " ", title.lower()).strip()
            exact_candidates = self._get_exact_candidates(conn, title_lower, year)
            if exact_candidates:
                best_match, best_score = self._select_exact_match(exact_candidates, conn, year)
                if best_match is not None:
                    result = MatchResult(info=best_match, score=best_score, provider_weight=self.provider_weight)
                    self._search_cache[cache_key] = result
                    return result

            candidates = [dict(row) for row in self._get_fuzzy_candidates(conn, title_lower, year)]
            result = self._select_best_fuzzy_match(conn, title, candidates, year)
            if result is not None:
                self._search_cache[cache_key] = result
                if len(self._search_cache) > self.SEARCH_CACHE_MAX:
                    oldest_keys = list(self._search_cache.keys())[: self.SEARCH_CACHE_EVICT]
                    for key in oldest_keys:
                        del self._search_cache[key]
                return result

            prefix_candidates = [dict(row) for row in self._get_prefix_candidates(conn, title_lower, year)]
            result = self._select_best_fuzzy_match(conn, title, prefix_candidates, year)
            if result is not None:
                self._search_cache[cache_key] = result
                if len(self._search_cache) > self.SEARCH_CACHE_MAX:
                    oldest_keys = list(self._search_cache.keys())[: self.SEARCH_CACHE_EVICT]
                    for key in oldest_keys:
                        del self._search_cache[key]
                return result
        finally:
            self._return_connection(conn)

        return None

    def find_title_with_type_hint(self, title: str, preferred_type: str, year: Optional[int] = None) -> Optional[MatchResult]:
        self._ensure_data_loaded()
        preferred_type_codes = self.PREFERRED_TYPE_HINT_CODES.get(preferred_type)
        if not preferred_type_codes:
            return self.find_title(title, year)

        cache_key = f"{title.lower()}_{year}_{preferred_type}"
        cached = self._search_cache.get(cache_key)
        if isinstance(cached, MatchResult):
            return cached

        conn = self._get_connection()
        try:
            title_lower = re.sub(r"\s+", " ", title.lower()).strip()
            exact_candidates = [row for row in self._get_exact_candidates(conn, title_lower, year) if row["type"] in preferred_type_codes]
            if exact_candidates:
                best_match, best_score = self._select_exact_match(exact_candidates, conn, year)
                if best_match is not None:
                    result = MatchResult(info=best_match, score=best_score, provider_weight=self.provider_weight)
                    self._search_cache[cache_key] = result
                    return result

            candidates = [dict(row) for row in self._get_fuzzy_candidates(conn, title_lower, year) if row["type"] in preferred_type_codes]
            result = self._select_best_fuzzy_match(conn, title, candidates, year)
            if result is not None:
                self._search_cache[cache_key] = result
                return result

            prefix_candidates = [
                dict(row)
                for row in self._get_prefix_candidates(conn, title_lower, year)
                if row["type"] in preferred_type_codes
            ]
            result = self._select_best_fuzzy_match(conn, title, prefix_candidates, year)
            if result is not None:
                self._search_cache[cache_key] = result
                return result
        finally:
            self._return_connection(conn)

        fallback = self.find_title(title, year)
        if fallback is not None:
            self._search_cache[cache_key] = fallback
        return fallback

    def _create_title_info_from_row_fast(self, row: RowLike, conn: sqlite3.Connection) -> TitleInfo:
        row_id = int(self._row_value(row, "id"))
        cached = self._title_cache.get(row_id)
        if cached is not None:
            return cached

        title_type = self.TITLE_TYPE_NAMES.get(int(self._row_value(row, "type")), "movie")
        media_type = "movie" if title_type == "movie" else "tv"

        total_episodes = None
        total_seasons = None
        if media_type == "tv":
            summary = conn.execute(
                "SELECT COUNT(*) AS episode_count, MAX(season) AS max_season FROM episode_core WHERE parent_id = ?",
                (row_id,),
            ).fetchone()
            if summary is not None:
                total_episodes = summary[0] if summary[0] > 0 else None
                total_seasons = summary[1]

        genres_value = self._row_value(row, "genres")
        genres = genres_value.split(",") if isinstance(genres_value, str) and genres_value else []
        end_year = self._row_value(row, "end_year")
        rating_value = self._row_value(row, "rating")
        tconst = f"tt{row_id:07d}"
        title_info = TitleInfo(
            id=tconst,
            title=str(self._row_value(row, "title")),
            type=media_type,
            year=self._row_value(row, "year"),
            start_year=self._row_value(row, "year"),
            end_year=end_year,
            rating=float(rating_value) if isinstance(rating_value, (float, int)) else None,
            votes=self._row_value(row, "votes"),
            runtime_minutes=self._row_value(row, "runtime_minutes"),
            genres=genres,
            tags=[],
            status="Ended" if media_type == "tv" and end_year else ("Continuing" if media_type == "tv" else None),
            total_episodes=total_episodes,
            total_seasons=total_seasons,
            sources=[f"https://www.imdb.com/title/{tconst}/"],
            plot=None,
        )
        self._title_cache[row_id] = title_info
        if len(self._title_cache) > self.TITLE_CACHE_MAX:
            oldest_keys = list(self._title_cache.keys())[: self.TITLE_CACHE_EVICT]
            for key in oldest_keys:
                del self._title_cache[key]
        return title_info

    def _parse_parent_id(self, parent_id: str) -> Optional[int]:
        if parent_id.startswith("tt"):
            try:
                return int(parent_id[2:])
            except (TypeError, ValueError):
                return None
        try:
            return int(parent_id)
        except (TypeError, ValueError):
            return None

    def get_episode_info(self, parent_id: str, season: int, episode: int) -> Optional[EpisodeInfo]:
        self._ensure_data_loaded()
        cache_key = f"{parent_id}_{season}_{episode}"
        cached = self._search_cache.get(cache_key)
        if isinstance(cached, EpisodeInfo):
            return cached

        internal_parent_id = self._parse_parent_id(parent_id)
        if internal_parent_id is None:
            return None

        conn = self._get_connection()
        try:
            row = conn.execute(
                "SELECT id, title, year, rating, votes FROM episode_core WHERE parent_id = ? AND season = ? AND episode = ? LIMIT 1",
                (internal_parent_id, season, episode),
            ).fetchone()
            if row is None:
                return None
            episode_info = EpisodeInfo(
                title=row["title"],
                season=season,
                episode=episode,
                parent_id=parent_id,
                id=f"tt{row['id']:07d}",
                year=row["year"],
                rating=float(row["rating"] / 10.0) if row["rating"] else None,
                votes=row["votes"],
            )
            self._search_cache[cache_key] = episode_info
            return episode_info
        finally:
            self._return_connection(conn)

    def list_episodes(self, parent_id: str, season: Optional[int] = None) -> List[EpisodeInfo]:
        self._ensure_data_loaded()
        cache_key = f"episodes_{parent_id}_{season}"
        cached = self._search_cache.get(cache_key)
        if isinstance(cached, list):
            return cached

        internal_parent_id = self._parse_parent_id(parent_id)
        if internal_parent_id is None:
            return []

        conn = self._get_connection()
        try:
            params: List[Any] = [internal_parent_id]
            sql = "SELECT id, season, episode, title, year, rating, votes FROM episode_core WHERE parent_id = ?"
            if season is not None:
                sql += " AND season = ?"
                params.append(season)
            sql += " ORDER BY season, episode, id"
            rows = conn.execute(sql, tuple(params)).fetchall()

            episodes: List[EpisodeInfo] = []
            for row in rows:
                if row["season"] is None or row["episode"] is None:
                    continue
                episodes.append(
                    EpisodeInfo(
                        title=row["title"],
                        season=row["season"],
                        episode=row["episode"],
                        parent_id=parent_id,
                        id=f"tt{row['id']:07d}",
                        year=row["year"],
                        rating=float(row["rating"] / 10.0) if row["rating"] else None,
                        votes=row["votes"],
                    )
                )
            self._search_cache[cache_key] = episodes
            return episodes
        finally:
            self._return_connection(conn)

    @staticmethod
    def _normalize_episode_lookup_title(text: Optional[str]) -> str:
        normalized = re.sub(r"[^\w\s]", " ", (text or "").lower())
        return re.sub(r"\s+", " ", normalized).strip()

    @classmethod
    def _score_episode_title_match(cls, normalized_query: str, candidate_title: Optional[str]) -> Optional[int]:
        normalized_title = cls._normalize_episode_lookup_title(candidate_title)
        if not normalized_title:
            return None
        score = fuzz.ratio(normalized_query, normalized_title)
        if normalized_query == normalized_title:
            score += 100
        elif normalized_query in normalized_title or normalized_title in normalized_query:
            score += 25
        return int(score)

    def find_episode_by_title(self, parent_id: str, episode_title: str, season: Optional[int] = None) -> Optional[EpisodeInfo]:
        self._ensure_data_loaded()
        normalized_query = self._normalize_episode_lookup_title(episode_title)
        if not normalized_query:
            return None

        internal_parent_id = self._parse_parent_id(parent_id)
        if internal_parent_id is None:
            return None

        conn = self._get_connection()
        try:
            params: List[Any] = [internal_parent_id, normalized_query]
            exact_sql = (
                "SELECT e.id, e.season, e.episode, e.title, e.year, e.rating, e.votes "
                "FROM episode_search s JOIN episode_core e ON e.id = s.episode_id "
                "WHERE s.parent_id = ? AND s.search_title_lower = ?"
            )
            if season is not None:
                exact_sql += " AND e.season = ?"
                params.append(season)
            exact_sql += " ORDER BY s.is_primary DESC, COALESCE(e.votes, 0) DESC, e.season, e.episode LIMIT 1"
            exact_row = conn.execute(exact_sql, tuple(params)).fetchone()
            if exact_row is not None:
                return EpisodeInfo(
                    title=exact_row["title"],
                    season=exact_row["season"],
                    episode=exact_row["episode"],
                    parent_id=parent_id,
                    id=f"tt{exact_row['id']:07d}",
                    year=exact_row["year"],
                    rating=float(exact_row["rating"] / 10.0) if exact_row["rating"] else None,
                    votes=exact_row["votes"],
                )

            token_params: List[Any] = [internal_parent_id]
            fuzzy_sql = (
                "SELECT s.search_title, e.id, e.season, e.episode, e.title, e.year, e.rating, e.votes "
                "FROM episode_search s JOIN episode_core e ON e.id = s.episode_id "
                "WHERE s.parent_id = ?"
            )
            if season is not None:
                fuzzy_sql += " AND e.season = ?"
                token_params.append(season)
            tokens = [token for token in normalized_query.split() if token]
            for token in tokens[:4]:
                fuzzy_sql += " AND s.search_title_lower LIKE ?"
                token_params.append(f"%{token}%")
            fuzzy_sql += " ORDER BY COALESCE(e.votes, 0) DESC LIMIT ?"
            token_params.append(self.EPISODE_FUZZY_LIMIT)

            candidate_rows = conn.execute(fuzzy_sql, tuple(token_params)).fetchall()
            if not candidate_rows:
                fallback_params: List[Any] = [internal_parent_id]
                fallback_sql = (
                    "SELECT s.search_title, e.id, e.season, e.episode, e.title, e.year, e.rating, e.votes "
                    "FROM episode_search s JOIN episode_core e ON e.id = s.episode_id "
                    "WHERE s.parent_id = ?"
                )
                if season is not None:
                    fallback_sql += " AND e.season = ?"
                    fallback_params.append(season)
                fallback_sql += " ORDER BY COALESCE(e.votes, 0) DESC LIMIT ?"
                fallback_params.append(self.EPISODE_FUZZY_LIMIT)
                candidate_rows = conn.execute(fallback_sql, tuple(fallback_params)).fetchall()
            if not candidate_rows:
                return None

            best_row = None
            best_score = -1
            for row in candidate_rows:
                score = self._score_episode_title_match(normalized_query, row["search_title"])
                if score is None:
                    continue
                if row["votes"]:
                    score += int(min(20, math.log10(max(1, row["votes"])) * 3))
                if score > best_score:
                    best_score = score
                    best_row = row

            if best_row is None or best_score < 70:
                return None

            return EpisodeInfo(
                title=best_row["title"],
                season=best_row["season"],
                episode=best_row["episode"],
                parent_id=parent_id,
                id=f"tt{best_row['id']:07d}",
                year=best_row["year"],
                rating=float(best_row["rating"] / 10.0) if best_row["rating"] else None,
                votes=best_row["votes"],
            )
        finally:
            self._return_connection(conn)

    def _build_fts_queries(self, title: str) -> List[str]:
        normalized = re.sub(r"[^\w\s]", " ", title.lower())
        normalized = re.sub(r"\s+", " ", normalized).strip()
        words = normalized.split()
        if not words:
            return []
        and_query = " ".join(f"{word}*" for word in words)
        if len(words) == 1:
            return [and_query]
        or_query = " OR ".join(f"{word}*" for word in words)
        return [and_query, or_query]

    def _get_timed_sql_context(self, sql: str) -> Tuple[str, str]:
        frame = inspect.currentframe()
        caller = frame.f_back.f_back if frame is not None and frame.f_back is not None else None
        location = "<unknown>"
        if caller is not None:
            location = f"{os.path.basename(caller.f_code.co_filename)}:{caller.f_lineno} in {caller.f_code.co_name}"
        sql_preview = " ".join(sql.strip().split())
        if len(sql_preview) > 180:
            sql_preview = f"{sql_preview[:177]}..."
        return location, sql_preview

    def _timed_execute(self, conn: sqlite3.Connection, sql: str, params: Tuple[Any, ...] = ()) -> sqlite3.Cursor:
        if not self.SQL_TIMING_ENABLED:
            return conn.execute(sql, params)
        location, sql_preview = self._get_timed_sql_context(sql)
        started_at = time.perf_counter()
        try:
            return conn.execute(sql, params)
        finally:
            elapsed_ms = (time.perf_counter() - started_at) * 1000
            print(f"[sql {elapsed_ms:9.2f} ms] {location} :: {sql_preview}")

    def _timed_executemany(self, conn: sqlite3.Connection, sql: str, seq_of_params: Iterable[Tuple[Any, ...]]) -> sqlite3.Cursor:
        if not self.SQL_TIMING_ENABLED:
            return conn.executemany(sql, seq_of_params)
        location, sql_preview = self._get_timed_sql_context(sql)
        started_at = time.perf_counter()
        try:
            return conn.executemany(sql, seq_of_params)
        finally:
            elapsed_ms = (time.perf_counter() - started_at) * 1000
            print(f"[sql-many {elapsed_ms:9.2f} ms] {location} :: {sql_preview}")

    def _optimize_database_for_reads(self) -> None:
        with sqlite3.connect(self._db_path, timeout=60.0) as conn:
            self._timed_execute(conn, "CREATE INDEX IF NOT EXISTS idx_title_core_type_year ON title_core(type, year)")
            self._timed_execute(conn, "CREATE INDEX IF NOT EXISTS idx_title_core_votes ON title_core(votes DESC)")
            self._timed_execute(conn, "CREATE INDEX IF NOT EXISTS idx_title_search_lower_id ON title_search(search_title_lower, title_id)")
            self._timed_execute(conn, "CREATE INDEX IF NOT EXISTS idx_episode_core_parent_season_episode ON episode_core(parent_id, season, episode)")
            self._timed_execute(conn, "CREATE INDEX IF NOT EXISTS idx_episode_search_parent_title_episode ON episode_search(parent_id, search_title_lower, episode_id)")
            self._timed_execute(conn, "CREATE INDEX IF NOT EXISTS idx_episode_search_episode_id ON episode_search(episode_id)")
            self._timed_execute(conn, "PRAGMA journal_mode=WAL")
            self._timed_execute(conn, "PRAGMA wal_autocheckpoint=1000")
            self._timed_execute(conn, "PRAGMA synchronous=NORMAL")
            self._timed_execute(conn, "PRAGMA cache_size=50000")
            self._timed_execute(conn, "PRAGMA mmap_size=268435456")
            self._timed_execute(conn, "ANALYZE")
            self._timed_execute(conn, "PRAGMA optimize")
            conn.commit()
            self._timed_execute(conn, "DELETE FROM title_fts")
            self._timed_execute(conn, "INSERT INTO title_fts (title, title_id) SELECT search_title, title_id FROM title_search")
            self._timed_execute(conn, "INSERT INTO title_fts(title_fts) VALUES('optimize')")
            conn.commit()

    def refresh_data(self) -> None:
        self.set_cache_expiry(0)
        self._search_cache.clear()
        self._title_cache.clear()
        self._clear_loaded_state()
        self._ensure_data_loaded()
