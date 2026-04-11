import math
import os
import gzip
import logging
import requests
import sqlite3
import time
import re
from typing import Optional, Dict, List, Tuple
from rapidfuzz import fuzz, process
from tqdm import tqdm
from metadata_provider import BaseMetadataProvider, TitleInfo, EpisodeInfo, MatchResult

class IMDbDataProvider(BaseMetadataProvider):
    DATASETS = {
        "title.basics": "https://datasets.imdbws.com/title.basics.tsv.gz",
        "title.episode": "https://datasets.imdbws.com/title.episode.tsv.gz",
        "title.ratings": "https://datasets.imdbws.com/title.ratings.tsv.gz",
        "title.akas": "https://datasets.imdbws.com/title.akas.tsv.gz",
    }
    # Define columns we actually need from each dataset - minimal set only
    # Removed columns for space efficiency:
    #   title.basics: (none removed - all columns included for completeness)
    #   title.episode: (none removed - all are essential)
    #   title.ratings: (none removed - all are essential) 
    #   title.akas: deduplicated unique titles only; drop ordering/language metadata
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
        "title.akas": ["titleId", "title"],
    }
    
    # Configurable filtering options - set to None to disable specific filters
    # IMPORTANT: Titles with fewer than MIN_VOTES_THRESHOLD votes are excluded from the
    # database entirely (both during ratings import and via post-load cleanup of title_basics).
    # This keeps the DB small and searches fast by only including well-known titles.
    MIN_VOTES_THRESHOLD = 1000     # Minimum votes required to store a title in the DB (None = no filter)
    RECENT_YEAR_CUTOFF = 1900     # Only keep titles from this year onwards (None = no filter)
    FILTER_ADULT_CONTENT = False  # Filter out adult content keywords (False = no filter)
    ALLOWED_TITLE_TYPES = ['movie', 'tvSeries', 'tvMiniSeries']  # None = allow all types

    MAX_RETRIES = 3

    # Search tuning parameters
    YEAR_TOLERANCE = 2              # +/- years when matching by year in FTS queries
    FTS_LIMIT_WITH_YEAR = 200       # Max FTS candidates when year is provided
    FTS_LIMIT_WITHOUT_YEAR = 300    # Max FTS candidates when year is not provided
    FUZZY_MATCH_LIMIT = 200         # Max results from fuzzy title matching
    MAX_CANDIDATES = 1000           # Hard cap on total candidate rows

    # Scoring bonuses for title matching
    YEAR_EXACT_BONUS = 200          # Bonus when candidate year matches exactly
    YEAR_CLOSE_BONUS = 100          # Bonus when candidate year is within +/- 1
    VOTE_BONUS_CAP = 150            # Maximum popularity bonus from votes
    VOTE_BONUS_MULTIPLIER = 30      # log10(votes) multiplier for popularity bonus

    # In-memory search cache limits
    SEARCH_CACHE_MAX = 1000         # Evict when cache exceeds this size
    SEARCH_CACHE_EVICT = 500        # Number of oldest entries to remove on eviction
    TITLE_CACHE_MAX = 5000          # Cached TitleInfo objects by IMDb id
    TITLE_CACHE_EVICT = 1000        # Number of cached TitleInfo objects to evict
    EXACT_MATCH_LIMIT = 50          # Hard cap for exact title and AKA candidate scans

    def __init__(self):
        super().__init__("imdb", provider_weight=0.9)
        self._search_cache = {}  # Cache recent search results
        self._title_cache = {}   # Cache for title info objects
        self._db_path = os.path.join(self.cache_dir, "imdb_data.db")
        self.CACHE_EXPIRY_DATASETS = list(self.DATASETS.keys())
        self._connection_pool = []  # Connection pool for better performance
        self._pool_size = 3
        self._init_database()
        self._load_cache_duration()
        expired = self._get_expired_datasets(self.CACHE_EXPIRY_DATASETS)
        if expired:
            # Do not force a download during provider construction.
            # This allows cache status inspection (e.g. metadata_cache_manager status)
            # without triggering a refresh. Actual reload happens lazily on first lookup.
            logging.info("Expired IMDb datasets detected on init: %s. Reload will occur on next data access.", ",".join(expired))
    
    def _get_connection(self) -> sqlite3.Connection:
        """Get a connection from the pool or create a new one"""
        if self._connection_pool:
            return self._connection_pool.pop()
        
        conn = sqlite3.connect(self._db_path, timeout=30.0)
        # Optimize for read operations
        conn.execute("PRAGMA query_only=ON")
        conn.execute("PRAGMA cache_size=50000")  # Large cache
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA mmap_size=268435456")  # 256MB memory-mapped I/O
        conn.row_factory = sqlite3.Row
        return conn
    
    def _return_connection(self, conn: sqlite3.Connection) -> None:
        """Return a connection to the pool"""
        if len(self._connection_pool) < self._pool_size:
            self._connection_pool.append(conn)
        else:
            conn.close()

    def _init_database(self) -> None:
        """Initialize SQLite database with optimized schema"""
        try:
            with sqlite3.connect(self._db_path, timeout=30.0) as conn:
                conn.execute("PRAGMA busy_timeout=30000")  # 30 second timeout
                conn.execute("PRAGMA synchronous=NORMAL")
                conn.execute("PRAGMA cache_size=10000")
                conn.execute("PRAGMA temp_store=MEMORY")
                conn.execute("PRAGMA page_size=32768")  # Larger pages for better compression
                conn.execute("PRAGMA auto_vacuum=INCREMENTAL")  # Reclaim space when data is deleted
                self._drop_legacy_aka_schema(conn)
                
                # Create tables with optimized schema - compressed storage
                conn.executescript(
                    """ 
                    CREATE TABLE IF NOT EXISTS title_basics (
                        id INTEGER PRIMARY KEY,  -- Use integer ID for space efficiency
                        title TEXT NOT NULL,
                        original_title TEXT,
                        title_lower TEXT NOT NULL,
                        type INTEGER NOT NULL,  -- 1=movie, 2=tvSeries, 3=tvMiniSeries
                        is_adult INTEGER,  -- 0=false, 1=true
                        year INTEGER,
                        end_year INTEGER,
                        runtime_minutes INTEGER,
                        genres TEXT
                    ) WITHOUT ROWID;
                    
                    CREATE TABLE IF NOT EXISTS title_ratings (
                        id INTEGER PRIMARY KEY,
                        rating INTEGER,  -- Store as integer (rating * 10)
                        votes INTEGER
                    ) WITHOUT ROWID;

                    CREATE TABLE IF NOT EXISTS title_episodes (
                        id INTEGER PRIMARY KEY,
                        parent_id INTEGER NOT NULL,
                        season INTEGER,
                        episode INTEGER
                    ) WITHOUT ROWID;

                    CREATE TABLE IF NOT EXISTS episode_titles (
                        id INTEGER PRIMARY KEY,
                        title TEXT NOT NULL,
                        title_lower TEXT NOT NULL,
                        year INTEGER
                    ) WITHOUT ROWID;

                    CREATE TABLE IF NOT EXISTS title_akas (
                        titleId INTEGER NOT NULL,
                        title TEXT NOT NULL,
                        title_key TEXT NOT NULL,
                        PRIMARY KEY (titleId, title_key)
                    ) WITHOUT ROWID;

                    CREATE TABLE IF NOT EXISTS data_version (
                        dataset TEXT PRIMARY KEY,
                        updated INTEGER,  -- Legacy timestamp field
                        expires_at INTEGER,  -- Absolute expiry timestamp (epoch seconds)
                        default_ttl INTEGER,  -- Default TTL in days
                        last_modified INTEGER  -- Source last-modified timestamp (epoch seconds)
                    ) WITHOUT ROWID;

                    -- Compressed view for search operations
                    CREATE VIEW IF NOT EXISTS search_view AS
                    SELECT 
                        b.id,
                        b.title,
                        b.title_lower,
                        b.type,
                        b.year,
                        b.end_year,
                        b.genres,
                        CASE WHEN r.rating IS NULL THEN NULL ELSE r.rating / 10.0 END as rating,
                        r.votes
                    FROM title_basics b
                    LEFT JOIN title_ratings r ON b.id = r.id;
                    
                    -- Standalone FTS5 virtual table for fast title searching across deduplicated AKAs.
                    CREATE VIRTUAL TABLE IF NOT EXISTS title_fts USING fts5(
                        title,
                        titleId UNINDEXED,
                        tokenize='porter unicode61'
                    );
                """
                )                
                # Only create essential indexes initially - others will be added after data load
                conn.executescript(
                    """
                    -- Essential indexes for data loading
                    CREATE INDEX IF NOT EXISTS idx_episodes_parent_temp ON title_episodes(parent_id);
                    -- Index for akas
                    CREATE INDEX IF NOT EXISTS idx_akas_titleId ON title_akas(titleId);
                    CREATE INDEX IF NOT EXISTS idx_akas_title_key ON title_akas(title_key, titleId);
                    DROP INDEX IF EXISTS idx_akas_title;
                    """
                )

                conn.commit()
        except Exception as e:
            logging.error(f"Failed to initialize database: {str(e)}")
            raise

    def _drop_legacy_aka_schema(self, conn: sqlite3.Connection) -> None:
        """Drop legacy AKA/FTS tables when their schema no longer matches the compact layout."""
        aka_columns = [row[1] for row in conn.execute("PRAGMA table_info(title_akas)").fetchall()]
        if aka_columns and aka_columns != ["titleId", "title", "title_key"]:
            conn.execute("DROP TABLE IF EXISTS title_fts")
            conn.execute("DROP TABLE IF EXISTS title_akas")

        fts_columns = [row[1] for row in conn.execute("PRAGMA table_info(title_fts)").fetchall()]
        if fts_columns and fts_columns != ["title", "titleId"]:
            conn.execute("DROP TABLE IF EXISTS title_fts")

    def _load_cache_duration(self) -> None:
        try:
            # Use base-class persistence. Avoid storing cache duration in
            # data_version rows that are reserved for source datasets.
            super()._load_cache_duration()
        except Exception as e:
            logging.debug(f"Could not load cache duration for IMDb provider: {e}")

    def _persist_cache_duration(self) -> None:
        try:
            super()._persist_cache_duration()
        except Exception:
            logging.debug("Could not persist cache duration for IMDb provider")

    def _upsert_dataset_version(self, conn: sqlite3.Connection, dataset_name: str, source_ts: Optional[int] = None) -> None:
        """Persist expiry metadata for one dataset in data_version."""
        now_ts = int(time.time())
        expires_at = now_ts + int(self.cache_duration.total_seconds())
        src_ts = int(source_ts) if source_ts else now_ts

        # Preserve an existing positive default_ttl when present.
        ttl_days = int(self.cache_duration.days)
        try:
            row = conn.execute(
                "SELECT default_ttl FROM data_version WHERE dataset = ? LIMIT 1",
                (dataset_name,),
            ).fetchone()
            if row and row[0] is not None:
                existing_ttl = int(row[0])
                if existing_ttl > 0:
                    ttl_days = existing_ttl
            if ttl_days <= 0:
                ttl_days = self.DEFAULT_TTL_DAYS
        except Exception:
            if ttl_days <= 0:
                ttl_days = self.DEFAULT_TTL_DAYS

        conn.execute(
            """
            INSERT OR REPLACE INTO data_version (dataset, expires_at, default_ttl, last_modified, updated)
            VALUES (?, ?, ?, ?, ?)
            """,
            (dataset_name, expires_at, ttl_days, src_ts, now_ts),
        )

    def _is_data_current(self) -> bool:
        """Check if the database contains current data"""
        return self._is_data_current_in_db(
            main_table="title_basics",
            datasets=getattr(self, "CACHE_EXPIRY_DATASETS", None),
        )

    def _verify_data_integrity(self) -> None:
        """Verify that all datasets are properly linked"""
        logging.info("Verifying data integrity...")
        
        with sqlite3.connect(self._db_path) as conn:
            # Check basic counts
            cursor = conn.execute("SELECT COUNT(*) FROM title_basics")
            basics_count = cursor.fetchone()[0]
            
            cursor = conn.execute("SELECT COUNT(*) FROM title_ratings")
            ratings_count = cursor.fetchone()[0]

            cursor = conn.execute("SELECT COUNT(*) FROM title_episodes")
            episodes_count = cursor.fetchone()[0]

            cursor = conn.execute("SELECT COUNT(*) FROM episode_titles")
            episode_titles_count = cursor.fetchone()[0]

            cursor = conn.execute("SELECT COUNT(*) FROM title_akas")
            akas_count = cursor.fetchone()[0]
            
            logging.info(f"Data integrity check:")
            logging.info(f"  title_basics: {basics_count}")
            logging.info(f"  title_ratings: {ratings_count}")
            logging.info(f"  title_episodes: {episodes_count}")
            logging.info(f"  episode_titles: {episode_titles_count}")
            logging.info(f"  title_akas: {akas_count}")
            
            # Check for orphaned ratings
            cursor = conn.execute("""
                SELECT COUNT(*) FROM title_ratings r 
                WHERE NOT EXISTS (SELECT 1 FROM title_basics b WHERE b.id = r.id)
            """)
            orphaned_ratings = cursor.fetchone()[0]
            
            # Check for orphaned episodes
            cursor = conn.execute("""
                SELECT COUNT(*) FROM title_episodes e 
                WHERE NOT EXISTS (SELECT 1 FROM title_basics b WHERE b.id = e.parent_id)
            """)
            orphaned_episodes = cursor.fetchone()[0]

            cursor = conn.execute("""
                SELECT COUNT(*) FROM episode_titles t
                WHERE NOT EXISTS (SELECT 1 FROM title_episodes e WHERE e.id = t.id)
            """)
            orphaned_episode_titles = cursor.fetchone()[0]

            cursor = conn.execute("""
                SELECT COUNT(*) FROM title_akas a
                WHERE NOT EXISTS (SELECT 1 FROM title_basics b WHERE b.id = a.titleId)
            """)
            orphaned_akas = cursor.fetchone()[0]
            
            logging.info(f"  Orphaned ratings: {orphaned_ratings}")
            logging.info(f"  Orphaned episodes: {orphaned_episodes}")
            logging.info(f"  Orphaned episode titles: {orphaned_episode_titles}")
            logging.info(f"  Orphaned AKAs: {orphaned_akas}")

    def _ensure_data_loaded(self) -> None:
        """Ensure database contains current IMDb data"""
        if self._is_data_current() and self._has_episode_title_data() and self._has_compact_aka_layout():
            logging.info("Database contains current IMDb data")
            return

        if self._is_data_current() and not self._has_episode_title_data():
            logging.info("IMDb cache is current but lacks compact episode title data. Reloading datasets.")
        elif self._is_data_current() and not self._has_compact_aka_layout():
            logging.info("IMDb cache is current but still uses the legacy AKA layout. Reloading datasets.")

        logging.info("Loading IMDb datasets into database...")
        
        # Load datasets in order of dependency:
        # 1. title.basics first (filtered by type/year/adult)
        # 2. title.ratings second (filtered by MIN_VOTES_THRESHOLD)
        # 3. Then purge basics entries that didn't meet the vote threshold
        # 4. title.episode and title.akas last (only for surviving titles)
        # 5. Prune child tables against the final surviving title set
        #
        # Note: episodes and akas are implicitly filtered by the vote threshold
        # because they load processed_tconst_ints from the already-purged
        # title_basics table. _compress_row checks parent IDs against this set,
        # so episodes/akas for low-vote titles are never inserted. Compact
        # episode titles are staged from title.basics earlier in the pipeline,
        # so they are pruned after title.episode establishes the surviving
        # episode id set.
        self._load_dataset_to_db("title.basics")
        self._load_dataset_to_db("title.ratings")
        
        # Remove titles from title_basics that have no matching rating entry.
        # Since title.ratings already filters out titles below MIN_VOTES_THRESHOLD,
        # this ensures the entire database only contains titles with enough votes.
        if self.MIN_VOTES_THRESHOLD is not None:
            self._purge_low_vote_titles()
        
        self._load_dataset_to_db("title.episode")
        self._load_dataset_to_db("title.akas")
        self._prune_child_tables()
        
        # Verify data integrity
        self._verify_data_integrity()
        
        # Optimize database for read operations
        self._optimize_database_for_reads()

    def _purge_low_vote_titles(self) -> None:
        """Remove titles from title_basics that don't have a matching entry in title_ratings.
        
        Since title_ratings is already filtered by MIN_VOTES_THRESHOLD during import,
        any title_basics entry without a rating row is guaranteed to have fewer than
        MIN_VOTES_THRESHOLD votes. This keeps the entire database clean.
        """
        logging.info(f"Purging titles with fewer than {self.MIN_VOTES_THRESHOLD} votes from title_basics...")
        with sqlite3.connect(self._db_path, timeout=60.0) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM title_basics")
            before_count = cursor.fetchone()[0]
            
            # Delete basics entries that have no corresponding rating
            # (i.e. they were filtered out due to low vote count)
            conn.execute("""
                DELETE FROM title_basics
                WHERE type != 4 AND id NOT IN (SELECT id FROM title_ratings)
            """)
            conn.commit()
            
            cursor = conn.execute("SELECT COUNT(*) FROM title_basics")
            after_count = cursor.fetchone()[0]
            removed = before_count - after_count
            logging.info(f"Purged {removed} low-vote titles ({before_count} -> {after_count} remaining)")

    def _prune_child_tables(self) -> None:
        """Remove child rows that do not point at the surviving filtered title set."""
        logging.info("Pruning child tables against surviving title_basics rows...")
        with sqlite3.connect(self._db_path, timeout=60.0) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM title_episodes")
            episode_before = cursor.fetchone()[0]
            cursor = conn.execute("SELECT COUNT(*) FROM episode_titles")
            episode_titles_before = cursor.fetchone()[0]
            cursor = conn.execute("SELECT COUNT(*) FROM title_akas")
            akas_before = cursor.fetchone()[0]

            conn.execute(
                """
                DELETE FROM title_episodes
                WHERE parent_id NOT IN (SELECT id FROM title_basics)
                """
            )
            conn.execute(
                """
                DELETE FROM episode_titles
                WHERE id NOT IN (SELECT id FROM title_episodes)
                """
            )
            conn.execute(
                """
                DELETE FROM title_akas
                WHERE titleId NOT IN (SELECT id FROM title_basics)
                """
            )
            conn.commit()

            cursor = conn.execute("SELECT COUNT(*) FROM title_episodes")
            episode_after = cursor.fetchone()[0]
            cursor = conn.execute("SELECT COUNT(*) FROM episode_titles")
            episode_titles_after = cursor.fetchone()[0]
            cursor = conn.execute("SELECT COUNT(*) FROM title_akas")
            akas_after = cursor.fetchone()[0]

            logging.info(
                "Pruned child rows: title_episodes %s -> %s, episode_titles %s -> %s, title_akas %s -> %s",
                episode_before,
                episode_after,
                episode_titles_before,
                episode_titles_after,
                akas_before,
                akas_after,
            )

    def _load_dataset_to_db(self, dataset_name: str) -> None:
        """Load a dataset into the database"""
        url = self.DATASETS[dataset_name]
        gz_cache = os.path.join(self.cache_dir, f"{dataset_name}.tsv.gz")
        source_last_modified_ts: Optional[int] = None

        # Only download if file is missing or older than cache_duration
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
                    total_size = int(response.headers.get("content-length", 0))
                    lm_header = response.headers.get("last-modified")
                    if lm_header:
                        try:
                            from email.utils import parsedate_to_datetime

                            source_last_modified_ts = int(parsedate_to_datetime(lm_header).timestamp())
                        except Exception:
                            source_last_modified_ts = None

                    with tqdm(
                        total=total_size,
                        desc=f"Downloading {dataset_name}",
                        unit="B",
                        unit_scale=True,
                    ) as pbar:
                        with open(gz_cache, "wb") as f:
                            for chunk in response.iter_content(chunk_size=8192):
                                if chunk:
                                    f.write(chunk)
                                    pbar.update(len(chunk))
                    break
                except Exception as e:
                    logging.error(
                        f"Error downloading {dataset_name} (attempt {attempt + 1}): {str(e)}"
                    )
                    if attempt < self.MAX_RETRIES - 1:
                        logging.info("Retrying...")
                        continue
                    raise
        else:
            try:
                source_last_modified_ts = int(os.path.getmtime(gz_cache))
            except Exception:
                source_last_modified_ts = int(time.time())
        
        # Parse and insert into database with aggressive filtering
        try:
            with sqlite3.connect(self._db_path, timeout=60.0) as conn:
                # Optimize for bulk inserts - simpler approach
                conn.execute("PRAGMA synchronous=OFF")
                conn.execute("PRAGMA temp_store=MEMORY")
                conn.execute("PRAGMA cache_size=100000")

                # Clear existing data
                if dataset_name == "title.basics":
                    conn.execute("DELETE FROM title_basics")
                    conn.execute("DELETE FROM episode_titles")
                elif dataset_name == "title.ratings":
                    conn.execute("DELETE FROM title_ratings")
                elif dataset_name == "title.episode":
                    conn.execute("DELETE FROM title_episodes")
                elif dataset_name == "title.akas":
                    conn.execute("DELETE FROM title_akas")
                    conn.execute("DELETE FROM title_fts")
                
                # Track processed tconst integers for cross-dataset linking
                processed_tconst_ints = set()
                
                # Load existing tconst integers if this isn't the first dataset
                if dataset_name != "title.basics":
                    cursor = conn.execute("SELECT id FROM title_basics")
                    for row in cursor.fetchall():
                        processed_tconst_ints.add(row[0])
                    logging.info(f"Loaded {len(processed_tconst_ints)} existing tconst integers for {dataset_name}")
                
                # Read and process data in chunks with aggressive filtering
                chunk_size = 100000
                processed_rows = 0
                kept_rows = 0

                with gzip.open(gz_cache, "rt", encoding="utf-8") as f:
                    # Read header
                    header = f.readline().strip().split("\t")
                    required_cols = self.REQUIRED_COLUMNS[dataset_name]
                    col_indices = [
                        header.index(col) for col in required_cols if col in header
                    ]
                    
                    batch = []
                    episode_title_batch = []
                    pbar = tqdm(desc=f"Processing {dataset_name}", unit="rows")

                    for line in f:
                        if not line.strip():
                            continue

                        fields = line.rstrip("\n\r").split("\t")
                        processed_rows += 1

                        # Extract required fields
                        row_data = []
                        for i, col_idx in enumerate(col_indices):
                            if col_idx < len(fields):
                                value = fields[col_idx]
                                if value == "\\N" or value == "":
                                    value = None
                                elif required_cols[i] in ["startYear", "endYear", "runtimeMinutes", "seasonNumber", "episodeNumber", "numVotes"]:
                                    try:
                                        value = int(value) if value else None
                                    except (ValueError, TypeError):
                                        value = None
                                elif required_cols[i] == "isAdult":
                                    try:
                                        value = int(value) if value in ["0", "1"] else 0
                                    except (ValueError, TypeError):
                                        value = 0
                                elif required_cols[i] == "averageRating":
                                    try:
                                        value = float(value) if value else None
                                    except (ValueError, TypeError):
                                        value = None
                            else:
                                value = None
                            row_data.append(value)

                        # Apply aggressive filtering
                        should_keep = self._should_keep_title(dataset_name, row_data, required_cols)
                        if not should_keep:
                            continue

                        kept_rows += 1
                        # Convert to compressed format
                        compressed_row = self._compress_row(dataset_name, row_data, required_cols, processed_tconst_ints)
                        if compressed_row:
                            if dataset_name == "title.basics" and compressed_row.get("episode_row"):
                                episode_title_batch.append(compressed_row["episode_row"])
                            elif compressed_row.get("row"):
                                batch.append(compressed_row['row'])

                            pending_rows = len(batch) + len(episode_title_batch)
                            if pending_rows >= chunk_size:
                                inserted_rows = len(batch) + len(episode_title_batch)
                                if batch:
                                    self._insert_compressed_batch(conn, dataset_name, batch)
                                    batch = []
                                if episode_title_batch:
                                    self._insert_episode_title_batch(conn, episode_title_batch)
                                    episode_title_batch = []
                                pbar.update(inserted_rows)

                    # Insert remaining batches
                    if batch:
                        self._insert_compressed_batch(conn, dataset_name, batch)
                        pbar.update(len(batch))
                    if episode_title_batch:
                        self._insert_episode_title_batch(conn, episode_title_batch)
                        pbar.update(len(episode_title_batch))

                    pbar.close()

                # Update version metadata used by cache freshness/status checks.
                self._upsert_dataset_version(conn, dataset_name, source_last_modified_ts)

                # Rebuild FTS only after deduplicated AKA rows are populated.
                if dataset_name == "title.akas":
                    self._rebuild_title_fts(conn)
                
                conn.commit()
                logging.info(f"Loaded {kept_rows}/{processed_rows} rows from {dataset_name} (filtered {processed_rows - kept_rows})")
                
                # Additional debugging for episodes and ratings
                if dataset_name == "title.ratings":
                    cursor = conn.execute("SELECT COUNT(*) FROM title_ratings")
                    count = cursor.fetchone()[0]
                    logging.info(f"Total ratings in database: {count}")
                elif dataset_name == "title.episode":
                    cursor = conn.execute("SELECT COUNT(*) FROM title_episodes")
                    count = cursor.fetchone()[0]
                    logging.info(f"Total episodes in database: {count}")
                elif dataset_name == "title.basics":
                    cursor = conn.execute("SELECT COUNT(*) FROM title_basics")
                    count = cursor.fetchone()[0]
                    logging.info(f"Total titles in database: {count}")
                    cursor = conn.execute("SELECT COUNT(*) FROM episode_titles")
                    episode_title_count = cursor.fetchone()[0]
                    logging.info(f"Total compact episode titles in database: {episode_title_count}")
                elif dataset_name == "title.akas":
                    cursor = conn.execute("SELECT COUNT(*) FROM title_akas")
                    count = cursor.fetchone()[0]
                    logging.info(f"Total AKAs in database: {count}")

        except Exception as e:
            logging.error(f"Error processing {dataset_name}: {str(e)}")
            raise

    def _should_keep_title(self, dataset_name: str, row_data: List, required_cols: List[str]) -> bool:
        """Apply configurable filtering to keep only relevant titles"""
        if dataset_name == "title.basics":
            # Extract data by column name
            data_dict = dict(zip(required_cols, row_data))
            title_type = data_dict.get('titleType', '')

            # Keep episode rows in a dedicated compact table for episode title lookup.
            if title_type == 'tvEpisode':
                return True
            
            # Filter by title type - configurable list of allowed types
            if self.ALLOWED_TITLE_TYPES is not None:
                if title_type not in self.ALLOWED_TITLE_TYPES:
                    return False
            
            # Filter by year - only content from specified year onwards (if configured)
            if self.RECENT_YEAR_CUTOFF is not None:
                year = data_dict.get('startYear')
                if year and year < self.RECENT_YEAR_CUTOFF:
                    return False
                    
            # Filter out adult content (if enabled)
            if self.FILTER_ADULT_CONTENT:
                title = data_dict.get('primaryTitle', '').lower()
                if any(word in title for word in ['adult', 'xxx', 'porn']):
                    return False
                    
        elif dataset_name == "title.ratings":
            # Filter by minimum votes threshold (if configured).
            # Titles that don't meet this threshold won't have a rating row,
            # and will subsequently be purged from title_basics too (see _purge_low_vote_titles).
            if self.MIN_VOTES_THRESHOLD is not None:
                data_dict = dict(zip(required_cols, row_data))
                votes = data_dict.get('numVotes', 0)
                if votes and votes < self.MIN_VOTES_THRESHOLD:
                    return False
        
        return True    
    def _compress_row(self, dataset_name: str, row_data: List, required_cols: List[str], 
                      processed_tconst_ints: set) -> Optional[Dict]:
        """Convert row data to compressed format using tconst integers as IDs"""
        data_dict = dict(zip(required_cols, row_data))
        
        if dataset_name == "title.basics":
            tconst = data_dict['tconst']
            
            # Extract integer from tconst (e.g., "tt0123456" -> 123456)
            try:
                tconst_int = int(tconst[2:]) if tconst.startswith('tt') else None
            except (ValueError, TypeError):
                tconst_int = None
            
            if tconst_int is None:
                return None
            
            if data_dict['titleType'] == 'tvEpisode':
                return {
                    'episode_row': (
                        tconst_int,
                        data_dict['primaryTitle'] or '',
                        (data_dict['primaryTitle'] or '').lower(),
                        data_dict['startYear'],
                    )
                }

            # Map title type to integer
            type_map = {'movie': 1, 'tvSeries': 2, 'tvMiniSeries': 3}
            type_int = type_map.get(data_dict['titleType'], 1)
            
            # Compress genres - only keep first 3, joined with commas
            genres = data_dict.get('genres', '')
            if genres:
                genre_list = genres.split(',')[:3]  # Only keep first 3 genres
                genres = ','.join(genre_list)
            
            title = data_dict['primaryTitle'] or ''
            original_title = data_dict['originalTitle'] or ''
            
            processed_tconst_ints.add(tconst_int)
            
            return {
                'row': (tconst_int, title, original_title, title.lower(), type_int, 
                       data_dict['isAdult'], data_dict['startYear'], data_dict['endYear'], 
                       data_dict['runtimeMinutes'], genres)
            }
            
        elif dataset_name == "title.ratings":
            tconst = data_dict['tconst']
            
            # Extract integer from tconst
            try:
                tconst_int = int(tconst[2:]) if tconst.startswith('tt') else None
            except (ValueError, TypeError):
                tconst_int = None
            
            if tconst_int and tconst_int in processed_tconst_ints:
                # Store rating as integer (rating * 10) to save space
                rating = data_dict.get('averageRating')
                rating_int = int(rating * 10) if rating else None
                
                return {
                    'row': (tconst_int, rating_int, data_dict.get('numVotes'))
                }
            else:
                # Skip this rating - no corresponding title in basics
                return None
                
        elif dataset_name == "title.episode":
            tconst = data_dict['tconst']
            parent_tconst = data_dict['parentTconst']
            
            # Extract integers from both tconsts
            try:
                tconst_int = int(tconst[2:]) if tconst.startswith('tt') else None
                parent_tconst_int = int(parent_tconst[2:]) if parent_tconst.startswith('tt') else None
            except (ValueError, TypeError):
                tconst_int = None
                parent_tconst_int = None
            
            if parent_tconst_int and parent_tconst_int in processed_tconst_ints and tconst_int:
                return {
                    'row': (tconst_int, parent_tconst_int,
                           data_dict.get('seasonNumber'), data_dict.get('episodeNumber'))
                }
            else:
                # Skip this episode - no corresponding parent series in basics
                return None
        elif dataset_name == "title.akas":
            # Convert titleId to integer
            titleId = data_dict['titleId']
            try:
                titleId_int = int(titleId[2:]) if isinstance(titleId, str) and titleId.startswith('tt') else int(titleId)
            except (ValueError, TypeError):
                return None
            # Only keep akas for titles we have in basics
            if titleId_int not in processed_tconst_ints:
                return None
            normalized_title = re.sub(r"\s+", " ", (data_dict.get('title', '') or '').strip())
            if not normalized_title:
                return None
            return {
                'row': (
                    titleId_int,
                    normalized_title,
                    self._normalize_title_key(normalized_title),
                )
            }
        
        return None

    def _insert_episode_title_batch(self, conn: sqlite3.Connection, batch: List[Tuple]) -> None:
        """Insert compact episode title rows."""
        conn.executemany(
            "INSERT OR REPLACE INTO episode_titles (id, title, title_lower, year) VALUES (?, ?, ?, ?)",
            batch,
        )

    def _rebuild_title_fts(self, conn: sqlite3.Connection) -> None:
        """Rebuild the standalone FTS table from deduplicated AKA rows."""
        conn.execute("DELETE FROM title_fts")
        conn.execute(
            "INSERT INTO title_fts (title, titleId) SELECT title, titleId FROM title_akas"
        )
    
    def _insert_compressed_batch(self, conn: sqlite3.Connection, dataset_name: str, batch: List[Tuple]) -> None:
        """Insert compressed batch data"""        
        if dataset_name == "title.basics":
            conn.executemany(
                "INSERT OR REPLACE INTO title_basics (id, title, original_title, title_lower, type, is_adult, year, end_year, runtime_minutes, genres) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                batch
            )
        elif dataset_name == "title.ratings":
            conn.executemany(
                "INSERT OR REPLACE INTO title_ratings (id, rating, votes) VALUES (?, ?, ?)",
                batch
            )
        elif dataset_name == "title.episode":
            conn.executemany(
                "INSERT OR REPLACE INTO title_episodes (id, parent_id, season, episode) VALUES (?, ?, ?, ?)",
                batch
            )
        elif dataset_name == "title.akas":
            conn.executemany(
                "INSERT OR IGNORE INTO title_akas (titleId, title, title_key) VALUES (?, ?, ?)",
                batch
            )

    def _optimize_database_for_reads(self) -> None:
        """Optimize database for read-only operations after data loading"""
        logging.info("Optimizing database for read operations...")
        
        with sqlite3.connect(self._db_path, timeout=60.0) as conn:
            # Create all indexes for fast queries - including covering indexes
            conn.executescript(
                """
                -- Primary indexes for title_basics
                CREATE INDEX IF NOT EXISTS idx_title_lower ON title_basics(title_lower);
                CREATE INDEX IF NOT EXISTS idx_title_type ON title_basics(type);
                CREATE INDEX IF NOT EXISTS idx_start_year ON title_basics(year);
                CREATE INDEX IF NOT EXISTS idx_title_type_year ON title_basics(type, year);

                -- Composite index for fast lookups by type, year, and title
                CREATE INDEX IF NOT EXISTS idx_title_type_year_title ON title_basics(type, year, title_lower);

                -- Covering index for exact title matches (includes all needed columns)
                CREATE INDEX IF NOT EXISTS idx_title_covering ON title_basics(title_lower, year, type, title, genres);

                -- Prefix index for fuzzy search optimization
                CREATE INDEX IF NOT EXISTS idx_title_prefix ON title_basics(substr(title_lower, 1, 2));

                -- Indexes for title_ratings
                CREATE INDEX IF NOT EXISTS idx_ratings_votes ON title_ratings(votes DESC);
                CREATE INDEX IF NOT EXISTS idx_ratings_covering ON title_ratings(id, rating, votes);

                -- Indexes for episodes
                CREATE INDEX IF NOT EXISTS idx_episodes_parent ON title_episodes(parent_id);
                CREATE INDEX IF NOT EXISTS idx_episodes_season_ep ON title_episodes(parent_id, season, episode);

                -- Additional index for fast episode lookup by id
                CREATE INDEX IF NOT EXISTS idx_episodes_id ON title_episodes(id);
                CREATE INDEX IF NOT EXISTS idx_episode_titles_lower ON episode_titles(title_lower);

                -- Indexes for akas
                CREATE INDEX IF NOT EXISTS idx_akas_titleId ON title_akas(titleId);
                CREATE INDEX IF NOT EXISTS idx_akas_title_key ON title_akas(title_key, titleId);

                DROP INDEX IF EXISTS idx_title_episode_titles;
                DROP INDEX IF EXISTS idx_akas_title;
                """
            )
            
            # Optimize for read-only operations
            conn.execute("PRAGMA journal_mode=WAL")  # Enable WAL mode after loading
            conn.execute("PRAGMA wal_autocheckpoint=1000")  # Auto-checkpoint every 1000 pages
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=50000")  # Large cache for reads
            conn.execute("PRAGMA mmap_size=268435456")  # 256MB memory-mapped I/O
            
            # Optimize database file
            conn.execute("PRAGMA optimize")
            
            # Analyze tables for better query planning
            conn.execute("ANALYZE")

            conn.commit()

            # Rebuild the file after full reload so deleted large tables/indexes actually shrink on disk.
            conn.execute("PRAGMA journal_mode=DELETE")
            conn.execute("VACUUM")
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("INSERT INTO title_fts(title_fts) VALUES('optimize')")
            conn.commit()
            logging.info("Database optimization complete")

    def _has_episode_title_data(self) -> bool:
        """Return True when the cache includes compact episode title rows required for title-based episode lookup."""
        try:
            with sqlite3.connect(self._db_path, timeout=5.0) as conn:
                row = conn.execute(
                    "SELECT 1 FROM episode_titles WHERE title IS NOT NULL AND title != '' LIMIT 1"
                ).fetchone()
                return row is not None
        except Exception:
            return False

    def _has_compact_aka_layout(self) -> bool:
        """Return True when the cache uses the deduplicated AKA schema and standalone FTS layout."""
        try:
            with sqlite3.connect(self._db_path, timeout=5.0) as conn:
                aka_columns = [row[1] for row in conn.execute("PRAGMA table_info(title_akas)").fetchall()]
                fts_columns = [row[1] for row in conn.execute("PRAGMA table_info(title_fts)").fetchall()]
                return aka_columns == ["titleId", "title", "title_key"] and fts_columns == ["title", "titleId"]
        except Exception:
            return False

    def _normalize_title_key(self, title: str) -> str:
        """Normalize a title into the compact key stored for AKA exact matching."""
        return re.sub(r"\s+", " ", (title or "").casefold()).strip()

    def _apply_candidate_bonuses(self, row: sqlite3.Row | dict, score: float, year: Optional[int]) -> float:
        """Apply common year and popularity bonuses to a candidate score."""
        total_score = score

        if year and row["year"]:
            if row["year"] == year:
                total_score += self.YEAR_EXACT_BONUS
            elif abs(row["year"] - year) <= 1:
                total_score += self.YEAR_CLOSE_BONUS

        if row["votes"]:
            vote_bonus = min(self.VOTE_BONUS_CAP, self.VOTE_BONUS_MULTIPLIER * math.log10(max(1, row["votes"])))
            total_score += vote_bonus

        return total_score

    def _get_exact_candidates(
        self, conn: sqlite3.Connection, title_lower: str, title_key: str, year: Optional[int]
    ) -> List[sqlite3.Row]:
        """Return exact primary-title or AKA matches before falling back to FTS."""
        year_clause = ""
        primary_params: list = [title_lower, self.MIN_VOTES_THRESHOLD or 0]
        aka_params: list = [title_key, self.MIN_VOTES_THRESHOLD or 0]
        if year:
            year_clause = " AND (s.year BETWEEN ? AND ? OR s.year IS NULL)"
            primary_params.extend([year - self.YEAR_TOLERANCE, year + self.YEAR_TOLERANCE])
            aka_params.extend([year - self.YEAR_TOLERANCE, year + self.YEAR_TOLERANCE])
        primary_params.append(self.EXACT_MATCH_LIMIT)
        aka_params.append(self.EXACT_MATCH_LIMIT)

        primary_cursor = conn.execute(
            f"""
            SELECT s.id, s.title, s.type, s.year, s.end_year, s.genres, s.rating, s.votes,
                   2 AS match_rank
            FROM search_view s
            WHERE s.title_lower = ?
            AND s.type IN (1, 2, 3)
            AND s.votes >= ?
            {year_clause}
            ORDER BY s.votes DESC
            LIMIT ?
            """,
            tuple(primary_params),
        )
        primary_matches = primary_cursor.fetchall()
        if primary_matches:
            return primary_matches

        if not title_key:
            return []

        aka_cursor = conn.execute(
            f"""
            SELECT s.id, s.title, s.type, s.year, s.end_year, s.genres, s.rating, s.votes,
                   1 AS match_rank
            FROM title_akas a
            JOIN search_view s ON a.titleId = s.id
            WHERE a.title_key = ?
            AND s.type IN (1, 2, 3)
            AND s.votes >= ?
            {year_clause}
            ORDER BY s.votes DESC
            LIMIT ?
            """,
            tuple(aka_params),
        )
        return aka_cursor.fetchall()

    def _select_exact_match(
        self,
        candidates: List[sqlite3.Row],
        conn: sqlite3.Connection,
        year: Optional[int],
    ) -> Tuple[Optional[TitleInfo], float]:
        """Select the best candidate from exact primary-title or AKA matches."""
        best_match = None
        best_score = -1.0

        for row in candidates:
            base_score = 1000.0 if row["match_rank"] == 2 else 925.0
            total_score = self._apply_candidate_bonuses(row, base_score, year)
            if total_score > best_score:
                best_score = total_score
                best_match = self._create_title_info_from_row_fast(row, conn)

        return best_match, best_score

    def find_title(
        self, title: str, year: Optional[int] = None
    ) -> Optional[MatchResult]:
        """Find title information using database queries"""
        self._ensure_data_loaded()

        # Check cache first
        cache_key = f"{title.lower()}_{year}"
        if cache_key in self._search_cache:
            return self._search_cache[cache_key]

        best_match = None
        best_score = 0

        # Use connection pooling for better performance
        conn = self._get_connection()
        try:
            title_lower = re.sub(r"\s+", " ", title.lower()).strip()
            title_key = self._normalize_title_key(title)

            exact_candidates = self._get_exact_candidates(conn, title_lower, title_key, year)
            if exact_candidates:
                best_match, best_score = self._select_exact_match(exact_candidates, conn, year)
            else:
                candidates = [dict(row) for row in self._get_fuzzy_candidates(conn, title_lower, year)]

                search_dict = {r["id"]: r["title"] for r in candidates}
                candidate_by_id = {r["id"]: r for r in candidates}

                title_matches = process.extract(
                    title, search_dict, scorer=fuzz.ratio, limit=self.FUZZY_MATCH_LIMIT
                )

                for _matched_title, fuzzy_score, row_id in title_matches:
                    row = candidate_by_id.get(row_id)
                    if not row:
                        continue

                    total_score = self._apply_candidate_bonuses(row, float(fuzzy_score), year)
                    row["score"] = total_score

                    if total_score > best_score:
                        best_score = total_score
                        best_match = self._create_title_info_from_row_fast(row, conn)

        finally:
            self._return_connection(conn)

        if best_match:
            result = MatchResult(
                info=best_match, score=best_score, provider_weight=self.provider_weight
            )

            # Cache result with size management
            self._search_cache[cache_key] = result
            if len(self._search_cache) > self.SEARCH_CACHE_MAX:
                # Remove oldest entries more efficiently
                oldest_keys = list(self._search_cache.keys())[:self.SEARCH_CACHE_EVICT]
                for key in oldest_keys:
                    del self._search_cache[key]

            return result

        return None

    def _get_fuzzy_candidates(self, conn: sqlite3.Connection, title_lower: str, year: Optional[int]) -> List[sqlite3.Row]:
        """Get optimized candidate list using FTS5, similar to anime_metadata.py"""
        candidates = []
        try:
            for fts_query in self._build_fts_queries(title_lower):
                if year:
                    cursor = conn.execute(
                        """
                        SELECT s.id, s.title, s.type, s.year, s.end_year, s.genres, s.rating, s.votes, f.score
                        FROM (
                            SELECT titleId, MIN(bm25(title_fts, 10.0)) AS score
                            FROM title_fts
                            WHERE title_fts MATCH ?
                            GROUP BY titleId
                            ORDER BY score
                            LIMIT ?
                        ) f
                        JOIN search_view s ON f.titleId = s.id
                        WHERE s.type IN (1, 2, 3)
                        AND (s.year BETWEEN ? AND ? OR s.year IS NULL)
                        AND s.votes >= ?
                        ORDER BY f.score, s.votes DESC
                        LIMIT ?
                        """,
                        (
                            fts_query,
                            self.FTS_LIMIT_WITH_YEAR,
                            year - self.YEAR_TOLERANCE,
                            year + self.YEAR_TOLERANCE,
                            self.MIN_VOTES_THRESHOLD or 0,
                            self.FTS_LIMIT_WITH_YEAR,
                        ),
                    )
                else:
                    cursor = conn.execute(
                        """
                        SELECT s.id, s.title, s.type, s.year, s.end_year, s.genres, s.rating, s.votes, f.score
                        FROM (
                            SELECT titleId, MIN(bm25(title_fts, 10.0)) AS score
                            FROM title_fts
                            WHERE title_fts MATCH ?
                            GROUP BY titleId
                            ORDER BY score
                            LIMIT ?
                        ) f
                        JOIN search_view s ON f.titleId = s.id
                        WHERE s.type IN (1, 2, 3)
                        AND s.votes >= ?
                        ORDER BY f.score, s.votes DESC
                        LIMIT ?
                        """,
                        (
                            fts_query,
                            self.FTS_LIMIT_WITHOUT_YEAR,
                            self.MIN_VOTES_THRESHOLD or 0,
                            self.FTS_LIMIT_WITHOUT_YEAR,
                        ),
                    )
                candidates = cursor.fetchall()
                if candidates:
                    break
        except Exception as e:
            logging.debug(f"FTS search failed: {e}")
        return candidates[:self.MAX_CANDIDATES]  # Limit total candidates

    def _create_title_info_from_row_fast(self, row: sqlite3.Row | dict, conn: sqlite3.Connection) -> TitleInfo:
        """Fast version of _create_title_info_from_row using existing connection"""
        cached = self._title_cache.get(row["id"])
        if cached:
            return cached

        # Map type integer back to string
        type_map = {1: 'movie', 2: 'tvSeries', 3: 'tvMiniSeries'}
        title_type = type_map.get(row["type"], 'movie')
        media_type = "movie" if title_type == "movie" else "tv"

        # Get episode count for TV shows using existing connection (much faster)
        total_episodes = None
        total_seasons = None
        if media_type == "tv":
            cursor = conn.execute(
                """
                SELECT COUNT(*) as episode_count, MAX(season) as max_season
                FROM title_episodes 
                WHERE parent_id = ?
            """,
                (row["id"],),
            )
            result = cursor.fetchone()
            if result:
                total_episodes = result[0] if result[0] > 0 else None
                total_seasons = result[1]

        genres = row["genres"].split(",") if row["genres"] else []        # Reconstruct tconst from ID (ID is the tconst integer)
        tconst = f"tt{row['id']:07d}"# Derive status for TV series based on end_year
        status = None
        if media_type == "tv":
            if "end_year" in row.keys() and row["end_year"]:
                status = "Ended"
            else:
                status = "Continuing"  # or "Unknown" - could be either continuing or just no end year data

        title_info = TitleInfo(
            id=tconst or str(row["id"]),
            title=row["title"],
            type=media_type,
            year=row["year"],
            start_year=row["year"],
            end_year=row["end_year"] if "end_year" in row.keys() else None,
            rating=float(row["rating"]) if row["rating"] else None,
            votes=row["votes"],
            genres=genres,
            tags=[],  # IMDb doesn't provide tags in basic dataset
            status=status,
            total_episodes=total_episodes,
            total_seasons=total_seasons,
            sources=[f"https://www.imdb.com/title/{tconst}/"],
            plot=None,  # Plot not available in basic dataset
        )

        self._title_cache[row["id"]] = title_info
        if len(self._title_cache) > self.TITLE_CACHE_MAX:
            oldest_keys = list(self._title_cache.keys())[:self.TITLE_CACHE_EVICT]
            for key in oldest_keys:
                del self._title_cache[key]

        return title_info

    def get_episode_info(
        self, parent_id: str, season: int, episode: int
    ) -> Optional[EpisodeInfo]:
        """Get episode information from database"""
        self._ensure_data_loaded()

        # Check param episode is it is not just a single int, but an list, if so, just use the first item in the list
        if isinstance(episode, list):
            episode = episode[0]

        # Check cache for episode info
        episode_key = f"{parent_id}_{season}_{episode}"
        if episode_key in self._search_cache:
            return self._search_cache[episode_key]

        conn = self._get_connection()
        try:            # Convert parent_id to internal ID if it's a tconst
            internal_parent_id = None
            # Ensure parent_id is a string before calling startswith
            parent_id_str = str(parent_id)
            if parent_id_str.startswith('tt'):
                # Extract integer from tconst (e.g., "tt0123456" -> 123456)
                try:
                    internal_parent_id = int(parent_id_str[2:])
                except (ValueError, TypeError):
                    pass
            else:
                try:
                    internal_parent_id = int(parent_id_str)
                except ValueError:
                    pass

            if not internal_parent_id:
                return None            # Find episode using optimized query
            cursor = conn.execute(
                """
                SELECT e.id, t.title, t.year, r.rating, r.votes
                FROM title_episodes e
                LEFT JOIN episode_titles t ON e.id = t.id
                LEFT JOIN title_ratings r ON e.id = r.id
                WHERE e.parent_id = ? AND e.season = ? AND e.episode = ?
                LIMIT 1
            """,
                (internal_parent_id, season, episode),
            )

            row = cursor.fetchone()
            if row:
                episode_info = EpisodeInfo(
                    title=row["title"],
                    season=season,
                    episode=episode,
                    parent_id=parent_id,
                    year=row["year"],
                    rating=(
                        float(row["rating"] / 10.0) if row["rating"] else None
                    ),
                    votes=row["votes"],
                )
                
                # Cache the result
                self._search_cache[episode_key] = episode_info
                return episode_info

        finally:
            self._return_connection(conn)

        return None

    def find_episode_by_title(
        self, parent_id: str, episode_title: str, season: Optional[int] = None
    ) -> Optional[EpisodeInfo]:
        """Find episode information by episode title, optionally constrained to a season."""
        self._ensure_data_loaded()

        normalized_query = re.sub(r"[^\w\s]", " ", (episode_title or "").lower())
        normalized_query = re.sub(r"\s+", " ", normalized_query).strip()
        if not normalized_query:
            return None

        parent_id_str = str(parent_id)
        internal_parent_id = None
        if parent_id_str.startswith("tt"):
            try:
                internal_parent_id = int(parent_id_str[2:])
            except (ValueError, TypeError):
                internal_parent_id = None
        else:
            try:
                internal_parent_id = int(parent_id_str)
            except (ValueError, TypeError):
                internal_parent_id = None

        if not internal_parent_id:
            return None

        conn = self._get_connection()
        try:
            params = [internal_parent_id]
            sql = """
                SELECT e.id, e.season, e.episode, t.title, t.year, r.rating, r.votes
                FROM title_episodes e
                LEFT JOIN episode_titles t ON e.id = t.id
                LEFT JOIN title_ratings r ON e.id = r.id
                WHERE e.parent_id = ?
            """
            if season is not None:
                sql += " AND e.season = ?"
                params.append(season)

            cursor = conn.execute(sql, tuple(params))
            candidates = cursor.fetchall()
            if not candidates:
                return None

            best_row = None
            best_score = -1
            for row in candidates:
                row_title = row["title"] or ""
                normalized_title = re.sub(r"[^\w\s]", " ", row_title.lower())
                normalized_title = re.sub(r"\s+", " ", normalized_title).strip()
                if not normalized_title:
                    continue

                score = fuzz.ratio(normalized_query, normalized_title)
                if normalized_query == normalized_title:
                    score += 100
                elif normalized_query in normalized_title or normalized_title in normalized_query:
                    score += 25

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
                year=best_row["year"],
                rating=(float(best_row["rating"] / 10.0) if best_row["rating"] else None),
                votes=best_row["votes"],
            )
        finally:
            self._return_connection(conn)
    
    def _build_fts_queries(self, title: str) -> List[str]:
        """Build primary and fallback FTS5 queries from title for fuzzy search."""
        # Normalize and tokenize
        normalized = title.lower()
        normalized = re.sub(r'\s+', ' ', normalized).strip()

        # Remove punctuation and special characters
        normalized = re.sub(r'[^\w\s]', ' ', normalized)

        # Remove words like "the", "a", "an" and other common stop words
        #normalized = re.sub(r'\b(the|a|an|and|of|in|to|for|with)\b', '', normalized)

        # Remove common japanese stop words in romaji
        #normalized = re.sub(r'\b(wa|no|ni|de|o|ka|ga|e|kara|made|yori|to|ya)\b', '', normalized)

        # Split into words and build FTS5 query
        words = normalized.split()
        if not words:
            return []

        and_query = ' '.join([f'{word}*' for word in words])
        if len(words) == 1:
            return [and_query]

        or_query = ' OR '.join([f'{word}*' for word in words])
        return [and_query, or_query]
    
    def refresh_data(self) -> None:
        """Invalidate cache and immediately reload/refresh the data"""
        logging.info("Refreshing IMDb database...")
        self.set_cache_expiry(0)
        self._search_cache.clear()
        self._title_cache.clear()
        self._ensure_data_loaded()
        logging.info("IMDb database refreshed successfully")

