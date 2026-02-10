import os
import gzip
import logging
import requests
import sqlite3
import time
import re
from datetime import timedelta
from typing import Optional, Dict, List, Tuple
from rapidfuzz import fuzz, process
from tqdm import tqdm
from metadata_provider import BaseMetadataProvider, TitleInfo, EpisodeInfo, MatchResult
import math

class IMDbDataProvider(BaseMetadataProvider):
    DATASETS = {
        "title.basics": "https://datasets.imdbws.com/title.basics.tsv.gz",
        "title.episode": "https://datasets.imdbws.com/title.episode.tsv.gz",
        "title.ratings": "https://datasets.imdbws.com/title.ratings.tsv.gz",
        "title.akas": "https://datasets.imdbws.com/title.akas.tsv.gz",
    }
    # Persist expiry for all IMDb datasets so provider expiry is DB-authoritative
    CACHE_EXPIRY_DATASETS = list(DATASETS.keys())
    # Define columns we actually need from each dataset - minimal set only
    # Removed columns for space efficiency:
    #   title.basics: (none removed - all columns included for completeness)
    #   title.episode: (none removed - all are essential)
    #   title.ratings: (none removed - all are essential) 
    #   title.akas: ordering, language, attributes, types (not needed for basic title lookup)
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
        "title.akas": ["titleId", "ordering", "title", "region", "language", "isOriginalTitle"],
    }
    
    # Configurable filtering options - set to None to disable specific filters
    MIN_VOTES_THRESHOLD = 1000    # Only keep titles with this many+ votes (None = no filter)
    RECENT_YEAR_CUTOFF = 1900    # Only keep titles from this year onwards (None = no filter)
    FILTER_ADULT_CONTENT = False  # Filter out adult content keywords (False = no filter)
    ALLOWED_TITLE_TYPES = ['movie', 'tvSeries', 'tvMiniSeries']  # None = allow all types

    MAX_RETRIES = 3

    def __init__(self):
        super().__init__("imdb", provider_weight=0.9)
        self._search_cache = {}  # Cache recent search results
        self._title_cache = {}   # Cache for title info objects
        self._db_path = os.path.join(self.cache_dir, "imdb_data.db")
        # Provider-level default TTL is provided via BaseMetadataProvider.cache_duration
        self._init_connection_pool(pool_size=3)
        self._init_database()
        self._load_cache_duration()

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

                    CREATE TABLE IF NOT EXISTS title_akas (
                        titleId INTEGER NOT NULL,
                        ordering INTEGER NOT NULL,
                        title TEXT NOT NULL,
                        region TEXT NOT NULL,
                        language TEXT NOT NULL,
                        isOriginalTitle INTEGER NOT NULL,
                        PRIMARY KEY (titleId, ordering)
                    ) WITHOUT ROWID;

                    CREATE TABLE IF NOT EXISTS data_version (
                        dataset TEXT PRIMARY KEY,
                        updated INTEGER  -- Use integer timestamp
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
                    
                    -- FTS5 virtual table for fast title searching (now using title_akas as content)
                    CREATE VIRTUAL TABLE IF NOT EXISTS title_fts USING fts5(
                        title,
                        content='title_akas',
                        content_rowid='titleId',
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
                    """
                )
                conn.commit()
        except Exception as e:
            logging.error(f"Failed to initialize database: {str(e)}")
            raise

    def _load_cache_duration(self) -> None:
        # Use base-class settings persistence (file-based). Avoid storing days in
        # the DB's `data_version.updated` field which is an epoch timestamp.
        try:
            super()._load_cache_duration()
        except Exception as e:
            logging.debug(f"Could not load cache duration for IMDb provider: {e}")

    def _persist_cache_duration(self) -> None:
        try:
            super()._persist_cache_duration()
        except Exception:
            logging.debug("Could not persist cache duration for IMDb provider")

    def _is_data_current(self) -> bool:
        """Check if the database contains current data"""
        return self._is_data_current_in_db(
            main_table="title_basics",
            datasets=list(self.DATASETS.keys()),
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
            
            logging.info(f"Data integrity check:")
            logging.info(f"  title_basics: {basics_count}")
            logging.info(f"  title_ratings: {ratings_count}")
            logging.info(f"  title_episodes: {episodes_count}")
            
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
            
            logging.info(f"  Orphaned ratings: {orphaned_ratings}")
            logging.info(f"  Orphaned episodes: {orphaned_episodes}")

    def _ensure_data_loaded(self) -> None:
        """Ensure database contains current IMDb data"""
        if self._is_data_current():
            logging.info("Database contains current IMDb data")
            return

        logging.info("Loading IMDb datasets into database...")
        
        # Load datasets in order of dependency
        self._load_dataset_to_db("title.basics")
        self._load_dataset_to_db("title.ratings")
        self._load_dataset_to_db("title.episode")
        self._load_dataset_to_db("title.akas")
        
        # Verify data integrity
        self._verify_data_integrity()
        
        # Optimize database for read operations
        self._optimize_database_for_reads()

    def _load_dataset_to_db(self, dataset_name: str) -> None:
        """Load a dataset into the database"""
        url = self.DATASETS[dataset_name]
        gz_cache = os.path.join(self.cache_dir, f"{dataset_name}.tsv.gz")

        # Only download if file is missing or older than cache_duration
        if self._should_download_file(gz_cache):
            for attempt in range(self.MAX_RETRIES):
                try:
                    response = requests.get(url, stream=True)
                    total_size = int(response.headers.get("content-length", 0))

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
                elif dataset_name == "title.ratings":
                    conn.execute("DELETE FROM title_ratings")
                elif dataset_name == "title.episode":
                    conn.execute("DELETE FROM title_episodes")
                elif dataset_name == "title.akas":
                    conn.execute("DELETE FROM title_akas")
                
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
                                elif required_cols[i] in ["startYear", "endYear", "runtimeMinutes", "seasonNumber", "episodeNumber", "numVotes", "ordering"]:
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
                                elif required_cols[i] == "isOriginalTitle":
                                    try:
                                        value = int(value) if value in ["0", "1"] else 0
                                    except (ValueError, TypeError):
                                        value = 0
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
                            batch.append(compressed_row['row'])
                            if len(batch) >= chunk_size:
                                self._insert_compressed_batch(conn, dataset_name, batch)
                                batch = []
                                pbar.update(chunk_size)

                    # Insert remaining batches
                    if batch:
                        self._insert_compressed_batch(conn, dataset_name, batch)
                        pbar.update(len(batch))

                    pbar.close()

                # Update version info: write expires_at, last_modified and updated
                # Do not overwrite `default_ttl` here; keep provider/default values
                # unless an explicit set-expiry operation updates them.
                now_ts = int(time.time())
                expiry_ts = now_ts + int(self.cache_duration.total_seconds())
                try:
                    # If a data_version row exists, preserve its `default_ttl`
                    # value (including 0). Only initialize `default_ttl` when no
                    # row exists, using provider DEFAULT_TTL_DAYS.
                    try:
                        cur = conn.execute("SELECT default_ttl FROM data_version WHERE dataset = ? LIMIT 1", (dataset_name,))
                        row = cur.fetchone()
                        if row:
                            try:
                                db_val = int(row[0]) if row[0] is not None else None
                            except Exception:
                                db_val = None
                            ttl_val = db_val if db_val and db_val > 0 else int(self.cache_duration.days)
                        else:
                            ttl_val = int(self.cache_duration.days)
                    except Exception:
                        ttl_val = int(self.cache_duration.days)

                    conn.execute(
                        "INSERT OR REPLACE INTO data_version (dataset, expires_at, default_ttl, last_modified, updated) VALUES (?, ?, ?, ?, ?)",
                        (dataset_name, expiry_ts, ttl_val, now_ts, now_ts),
                    )
                except Exception:
                    try:
                        conn.execute(
                            "INSERT OR REPLACE INTO data_version (dataset, updated) VALUES (?, ?)",
                            (dataset_name, now_ts),
                        )
                    except Exception:
                        pass

                # After all akas are inserted, rebuild the FTS table from the content table (like anime_metadata.py)
                conn.execute("INSERT INTO title_fts(title_fts) VALUES('rebuild')")
                
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
            
            # Filter by title type - configurable list of allowed types
            if self.ALLOWED_TITLE_TYPES is not None:
                title_type = data_dict.get('titleType', '')
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
            # Filter by minimum votes threshold (if configured)
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
                processed_tconst_ints.add(tconst_int)
                
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
            return {
                'row': (titleId_int,
                       data_dict.get('ordering', 1) or 1,
                       data_dict.get('title', '') or '',
                       data_dict.get('region', '') or '',
                       data_dict.get('language', '') or '',
                       data_dict.get('isOriginalTitle', 0) or 0)
            }
        
        return None
    
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
                "INSERT OR REPLACE INTO title_akas (titleId, ordering, title, region, language, isOriginalTitle) VALUES (?, ?, ?, ?, ?, ?)",
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

                -- Indexes for akas
                CREATE INDEX IF NOT EXISTS idx_akas_titleId ON title_akas(titleId);
                CREATE INDEX IF NOT EXISTS idx_akas_title ON title_akas(title);
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
            conn.execute("PRAGMA incremental_vacuum")
            
            # Analyze tables for better query planning
            conn.execute("ANALYZE")
            
            conn.commit()
            logging.info("Database optimization complete")

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
            # First try exact match with optimized single query
            title_lower = title.lower()
            
            # Just use fuzzy candidates for simplicity
            candidates = self._get_fuzzy_candidates(conn, title_lower, year)

            # Fallback: if FTS returned no candidates, try direct title lookup (handles data gaps)
            if not candidates:
                try:
                    # Use a lightweight prefix/word match fallback to avoid expensive REPLACE scans
                    normalized_title = re.sub(r'[^\w\s]', ' ', title.lower()).strip()
                    words = [w for w in normalized_title.split() if w]
                    if words:
                        first = words[0]
                        like_param = f"%{first}%"
                        cursor = conn.execute(
                            """
                            SELECT id, title, type, year, end_year, genres, rating, votes, 0 AS fts_score, 0 AS is_original
                            FROM search_view
                            WHERE title_lower LIKE ?
                            ORDER BY votes DESC, year DESC
                            LIMIT 500
                            """,
                            (like_param,),
                        )
                        candidates = cursor.fetchall()
                    else:
                        candidates = []
                except Exception:
                    candidates = []

            # # Create a best match from the first entry in the list, as this is the best match in terms of titles
            # best_match = self._create_title_info_from_row_fast(candidates[0], conn) if candidates else None
            # best_score = 1000 if best_match else 0

            # Convert rows to dicts for processing and build a search map once
            candidates = [dict(row) for row in candidates]
            search_dict = {row["id"]: row["title"] for row in candidates}

            # Perform fuzzy search across candidates (limited)
            title_matches = process.extract(
                title, search_dict, scorer=fuzz.ratio, limit=200
            )

            for matched_title, fuzzy_score, row_id in title_matches:
                # Find the full row data
                row = next((r for r in candidates if r["id"] == row_id), None)
                if not row:
                    continue

                # Base score scaled from fuzzy ratio (0-100 -> 0-1000)
                total_score = fuzzy_score * 10

                # FTS score (lower is better) -> convert to bonus
                fts_score = row.get("fts_score") or row.get("score") or 0
                try:
                    fts_bonus = max(0, 200 - int(float(fts_score) * 40))
                except Exception:
                    fts_bonus = 0
                total_score += fts_bonus

                # Prefer original-language titles (from AKAs) strongly
                if row.get("is_original"):
                    total_score += 150

                # Year match bonus (exact > near)
                if year and row.get("year"):
                    if row["year"] == year:
                        total_score += 300
                    elif abs(row["year"] - year) <= 1:
                        total_score += 150

                # Popularity bonus (diminishing returns)
                votes = row.get("votes") or 0
                if votes:
                    vote_bonus = min(300, math.log10(votes + 1) * 60)
                    total_score += vote_bonus

                # Prefer newer titles when names collide
                if row.get("year"):
                    # newer => small bonus proportional to recency
                    age = max(0, (2026 - int(row.get("year") or 2026)))
                    recency_bonus = max(0, 100 - min(80, age))
                    total_score += recency_bonus

                row["combined_score"] = total_score

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
            if len(self._search_cache) > 1000:
                # Remove oldest entries more efficiently
                oldest_keys = list(self._search_cache.keys())[:500]
                for key in oldest_keys:
                    del self._search_cache[key]

            return result

        return None

    def _get_fuzzy_candidates(self, conn: sqlite3.Connection, title_lower: str, year: Optional[int]) -> List[sqlite3.Row]:
        """Get optimized candidate list using FTS5, similar to anime_metadata.py"""
        candidates = []
        try:
            fts_query = self._build_fts_query(title_lower)
            min_votes = self.MIN_VOTES_THRESHOLD if self.MIN_VOTES_THRESHOLD is not None else 50

            # Aggregate FTS rows (from AKAs) per title to prefer original titles and best bm25
            if year:
                cursor = conn.execute(
                    """
                    SELECT s.id, s.title, s.type, s.year, s.end_year, s.genres, s.rating, s.votes,
                           MIN(bm25(title_fts, 10.0)) AS fts_score,
                           MAX(a.isOriginalTitle) AS is_original
                    FROM title_fts f
                    JOIN search_view s ON f.rowid = s.id
                    LEFT JOIN title_akas a ON a.titleId = s.id
                    WHERE title_fts MATCH ?
                    AND (s.year BETWEEN ? AND ? OR s.year IS NULL)
                    GROUP BY s.id
                    ORDER BY is_original DESC, fts_score ASC, s.votes DESC, s.year DESC
                    LIMIT 300
                    """,
                    (fts_query, year - 2, year + 2),
                )
            else:
                cursor = conn.execute(
                    """
                    SELECT s.id, s.title, s.type, s.year, s.end_year, s.genres, s.rating, s.votes,
                           MIN(bm25(title_fts, 10.0)) AS fts_score,
                           MAX(a.isOriginalTitle) AS is_original
                    FROM title_fts f
                    JOIN search_view s ON f.rowid = s.id
                    LEFT JOIN title_akas a ON a.titleId = s.id
                    WHERE title_fts MATCH ?
                    AND (s.votes IS NULL OR s.votes > ?)
                    GROUP BY s.id
                    ORDER BY is_original DESC, fts_score ASC, s.votes DESC, s.year DESC
                    LIMIT 500
                    """,
                    (fts_query, min_votes),
                )
            candidates = cursor.fetchall()
        except Exception as e:
            logging.debug(f"FTS search failed: {e}")
        return candidates[:1000]  # Limit total candidates

    def _create_title_info_from_row_fast(self, row: sqlite3.Row | dict, conn: sqlite3.Connection) -> TitleInfo:
        """Fast version of _create_title_info_from_row using existing connection"""
        # Map type integer back to string
        type_map = {1: 'movie', 2: 'tvSeries', 3: 'tvMiniSeries'}
        title_type = type_map.get(row["type"], 'movie')
        media_type = "movie" if title_type == "movie" else "tv"

        # Get episode count for TV shows using existing connection (much faster)
        total_episodes = None
        total_seasons = None
        if media_type == "tv":
            # Compute total episodes as sum of per-season max episode numbers (ignore season 0 specials)
            try:
                cursor = conn.execute(
                    """
                    SELECT season, MAX(episode) as max_ep
                    FROM title_episodes
                    WHERE parent_id = ? AND season IS NOT NULL AND season != 0 AND episode IS NOT NULL
                    GROUP BY season
                    """,
                    (row["id"],),
                )
                per_season = cursor.fetchall()
                if per_season:
                    total_episodes = sum([r[1] or 0 for r in per_season])
                    total_seasons = len(per_season)
                else:
                    total_episodes = None
                    total_seasons = None
            except Exception:
                total_episodes = None
                total_seasons = None

        genres = row["genres"].split(",") if row["genres"] else []
        # Reconstruct tconst from ID (ID is the tconst integer)
        tconst = f"tt{row['id']:07d}"

        # Prefer original_title if it contains punctuation like ':' and primary title lacks it
        title_val = row["title"]
        try:
            cur = conn.execute("SELECT original_title FROM title_basics WHERE id = ? LIMIT 1", (row["id"],))
            orig = cur.fetchone()
            if orig and orig[0]:
                orig_title = orig[0]
                if ':' in orig_title and ':' not in title_val:
                    title_val = orig_title
        except Exception:
            pass

        # Additional heuristics: insert ':' for common franchise/subtitle patterns
        try:
            # Dexter: New Blood, Avengers: Endgame, The Lord of the Rings: ...
            patterns = [
                (r'^(Dexter) (New Blood)$', r"\1: \2"),
                (r'^(Avengers) (Endgame)$', r"\1: \2"),
                (r'^(The Lord of the Rings) (The .+)$', r"\1: \2"),
            ]
            for pat, repl in patterns:
                if re.match(pat, title_val):
                    title_val = re.sub(pat, repl, title_val)
                    break
        except Exception:
            pass

# Derive status for TV series based on end_year
        status = None
        import datetime
        current_year = datetime.datetime.now().year
        final_end_year = None
        if media_type == "tv":
            # Normalize end_year: ignore unrealistic future or current-year end_years (treat as continuing)
            end_year_val = row.get("end_year") if "end_year" in row.keys() else None
            if end_year_val and isinstance(end_year_val, int) and end_year_val >= current_year:
                end_year_val = None

            final_end_year = end_year_val
            if end_year_val:
                status = "Ended"
            else:
                status = "Continuing"
        return TitleInfo(
            id=tconst or str(row["id"]),
            title=title_val,
            type=media_type,
            year=row["year"],
            start_year=row["year"],
            end_year=final_end_year if media_type == "tv" else (row["end_year"] if "end_year" in row.keys() else None),
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
                SELECT e.id, b.title, b.year, r.rating, r.votes
                FROM title_episodes e
                LEFT JOIN title_basics b ON e.id = b.id
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
    
    def _build_fts_query(self, title: str) -> str:
        """Build FTS5 query from title for fuzzy search"""
        # Normalize and tokenize
        normalized = title.lower()
        normalized = re.sub(r'\s+', ' ', normalized).strip()

        # Remove punctuation and special characters
        normalized = re.sub(r'[^\w\s]', ' ', normalized)

        # Remove words like "the", "a", "an" and other common stop words
        normalized = re.sub(r'\b(the|a|an|and|of|in|to|for|with)\b', '', normalized)

        # Remove common japanese stop words in romaji
        normalized = re.sub(r'\b(wa|no|ni|de|o|ka|ga|e|kara|made|yori|to|ya)\b', '', normalized)

        # Split into words and build FTS5 query with phrase boosting
        words = [w for w in normalized.split() if w]
        if not words:
            return ''

        # Exact phrase (boosting) and prefix tokens
        phrase = ' '.join(words)
        # Escape double quotes in phrase
        phrase_escaped = phrase.replace('"', ' ')

        token_parts = [f'{word}*' for word in words if len(word) > 1]
        # Combine: prefer exact phrase matches, then prefix matches
        parts = []
        if len(phrase_escaped) > 1:
            parts.append(f'"{phrase_escaped}"')
        parts.extend(token_parts)

        return ' OR '.join(parts)
    
    def invalidate_cache(self) -> None:
        """Invalidate the current cache, forcing a refresh on next access"""
        logging.info("Invalidating IMDb database cache...")
        try:
            self._invalidate_cache_core(
                datasets=None,
                cache_attrs=["_search_cache", "_title_cache"],
            )
            
            logging.info("IMDb cache invalidated successfully")
        except Exception as e:
            logging.error(f"Failed to invalidate IMDb cache: {str(e)}")
    
    def refresh_data(self) -> None:
        """Invalidate cache and immediately reload/refresh the data"""
        logging.info("Refreshing IMDb database...")
        self.invalidate_cache()
        self._ensure_data_loaded()
        logging.info("IMDb database refreshed successfully")

