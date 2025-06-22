import os
import json
import logging
import requests
import sqlite3
from typing import Optional, Dict, List, Tuple
from rapidfuzz import fuzz, process
from tqdm import tqdm
from metadata_provider import BaseMetadataProvider, TitleInfo, EpisodeInfo, MatchResult

class AnimeDataProvider(BaseMetadataProvider):
    ANIME_DB_URL = "https://raw.githubusercontent.com/manami-project/anime-offline-database/master/anime-offline-database.json"
    MAX_RETRIES = 3
    
    # Define relevance scores for different title types
    TITLE_WEIGHTS = {
        'main': 1.0,      # Main title gets full weight
        'english': 0.9,   # English title slightly less
        'synonym': 0.8    # Synonyms get lower base weight
    }
    
    def __init__(self):
        super().__init__('anime', provider_weight=1.0)
        self._search_cache = {}  # Cache recent search results
        self._db_path = os.path.join(self.cache_dir, "anime_data.db")
        self._connection_pool = []  # Connection pool for better performance
        self._pool_size = 3
        self._init_database()
    
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
                
                # Create tables with optimized schema - compressed storage
                conn.executescript(
                    """ 
                    CREATE TABLE IF NOT EXISTS anime_basics (
                        id TEXT PRIMARY KEY,
                        title TEXT NOT NULL,
                        title_lower TEXT NOT NULL,
                        type INTEGER NOT NULL,  -- 1=movie, 2=tvSeries
                        episodes INTEGER,
                        status TEXT,
                        season_year INTEGER,
                        season_name TEXT,
                        sources TEXT,
                        tags TEXT
                    ) WITHOUT ROWID;
                    
                    CREATE TABLE IF NOT EXISTS anime_synonyms (
                        anime_id TEXT NOT NULL,
                        title TEXT NOT NULL,
                        title_lower TEXT NOT NULL,
                        relevance REAL NOT NULL,
                        FOREIGN KEY (anime_id) REFERENCES anime_basics(id)
                    );
                    
                    CREATE TABLE IF NOT EXISTS data_version (
                        dataset TEXT PRIMARY KEY,
                        updated INTEGER  -- Use integer timestamp
                    ) WITHOUT ROWID;

                    -- View for search operations
                    CREATE VIEW IF NOT EXISTS search_view AS
                    SELECT 
                        id,
                        title,
                        title_lower,
                        type,
                        episodes,
                        status,
                        season_year,
                        season_name,
                        sources,
                        tags
                    FROM anime_basics;
                    
                    -- FTS5 virtual table for fast title searching
                    CREATE VIRTUAL TABLE IF NOT EXISTS anime_fts USING fts5(
                        title, 
                        title_normalized,
                        content='anime_basics',
                        content_rowid='rowid',
                        tokenize='porter unicode61'
                    );
                """
                )
                
                # Essential indexes for data loading
                conn.executescript(
                    """
                    CREATE INDEX IF NOT EXISTS idx_synonyms_anime_id ON anime_synonyms(anime_id);
                    CREATE INDEX IF NOT EXISTS idx_synonyms_title_lower ON anime_synonyms(title_lower);
                """
                )
                conn.commit()
        except Exception as e:
            logging.error(f"Failed to initialize anime database: {str(e)}")
            raise

    def _is_data_current(self) -> bool:
        """Check if the database contains current data"""
        try:
            with sqlite3.connect(self._db_path) as conn:
                cursor = conn.execute(
                    """
                    SELECT COUNT(*) FROM data_version 
                    WHERE dataset = 'anime_offline_database'
                    AND updated > strftime('%s', 'now', '-7 days')
                """
                )
                count = cursor.fetchone()[0]

                # Also check if we have data in anime_basics
                cursor = conn.execute("SELECT COUNT(*) FROM anime_basics LIMIT 1")
                has_data = cursor.fetchone()[0] > 0

                return count >= 1 and has_data
        except Exception:
            return False

    def load_database(self) -> None:
        """Load the anime database into SQLite, downloading if needed"""
        if self._is_data_current():
            logging.info("Database contains current anime data")
            return

        logging.info("Loading anime database...")
        
        temp_json = os.path.join(self.cache_dir, "temp_anime.json")
        
        # Download and process with retries
        for attempt in range(self.MAX_RETRIES):
            try:
                logging.info(f"Downloading anime database (attempt {attempt + 1}/{self.MAX_RETRIES})...")
                
                # Download JSON
                response = requests.get(self.ANIME_DB_URL, stream=True)
                total_size = int(response.headers.get('content-length', 0))
                
                with tqdm(total=total_size, desc="Downloading anime database", unit='B', unit_scale=True) as pbar:
                    with open(temp_json, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                                pbar.update(len(chunk))
                
                # Parse and insert into database
                self._load_json_to_db(temp_json)
                
                # Optimize database for read operations
                self._optimize_database_for_reads()
                
                # Clean up temp file
                try:
                    os.remove(temp_json)
                except:
                    pass
                
                return
                
            except Exception as e:
                logging.error(f"Error processing anime database (attempt {attempt + 1}): {str(e)}")
                if attempt < self.MAX_RETRIES - 1:
                    logging.info("Retrying...")
                    continue
                raise
        
        raise RuntimeError("Failed to load anime database after multiple attempts")

    def _load_json_to_db(self, json_file: str) -> None:
        """Load JSON data into SQLite database"""
        try:
            with sqlite3.connect(self._db_path, timeout=60.0) as conn:
                # Optimize for bulk inserts
                conn.execute("PRAGMA synchronous=OFF")
                conn.execute("PRAGMA temp_store=MEMORY")
                conn.execute("PRAGMA cache_size=100000")

                # Clear existing data
                conn.execute("DELETE FROM anime_basics")
                conn.execute("DELETE FROM anime_synonyms")
                
                # Parse JSON and convert to database records
                with tqdm(desc="Processing anime database", unit='entries') as pbar:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        
                        anime_batch = []
                        synonyms_batch = []
                        fts_batch = []
                        
                        for entry in data['data']:
                            # Extract main entry data
                            anime_id = entry.get('sources', [''])[0] or f"anime_{len(anime_batch)}"
                            title = entry['title']
                            
                            # Map type to integer
                            type_int = 1 if entry.get('type') == 'MOVIE' else 2
                            
                            record = (
                                anime_id,
                                title,
                                title.lower(),
                                type_int,
                                self.safe_int(entry.get('episodes')),
                                entry.get('status'),
                                self.safe_int(entry.get('animeSeason', {}).get('year')),
                                entry.get('animeSeason', {}).get('season'),
                                ','.join(entry.get('sources', [])),
                                ','.join(entry.get('tags', []))
                            )
                            anime_batch.append(record)
                            
                            # Add to FTS
                            fts_batch.append((len(anime_batch), title, self._normalize_title(title)))
                            
                            # Extract synonyms
                            for synonym in entry.get('synonyms', []):
                                if synonym and synonym != title:
                                    synonyms_batch.append((
                                        anime_id,
                                        synonym,
                                        synonym.lower(),
                                        self.TITLE_WEIGHTS['synonym']
                                    ))
                            
                            pbar.update(1)
                        
                        # Bulk insert anime data
                        conn.executemany(
                            """INSERT OR REPLACE INTO anime_basics 
                               (id, title, title_lower, type, episodes, status, season_year, season_name, sources, tags) 
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            anime_batch
                        )
                        
                        # Bulk insert synonyms
                        conn.executemany(
                            """INSERT OR REPLACE INTO anime_synonyms 
                               (anime_id, title, title_lower, relevance) 
                               VALUES (?, ?, ?, ?)""",
                            synonyms_batch
                        )
                        
                        # Bulk insert FTS data
                        conn.executemany(
                            "INSERT OR REPLACE INTO anime_fts (rowid, title, title_normalized) VALUES (?, ?, ?)",
                            fts_batch
                        )
                
                # Update version info
                import time
                conn.execute(
                    """INSERT OR REPLACE INTO data_version (dataset, updated) VALUES (?, ?)""",
                    ('anime_offline_database', int(time.time())),
                )
                
                conn.commit()
                logging.info(f"Loaded {len(anime_batch)} anime entries and {len(synonyms_batch)} synonyms")
                
        except Exception as e:
            logging.error(f"Error loading anime JSON to database: {str(e)}")
            raise

    def _optimize_database_for_reads(self) -> None:
        """Optimize database for read-only operations after data loading"""
        logging.info("Optimizing anime database for read operations...")
        
        with sqlite3.connect(self._db_path, timeout=60.0) as conn:
            # Create indexes for fast queries
            conn.executescript(
                """
                -- Primary indexes for anime_basics
                CREATE INDEX IF NOT EXISTS idx_anime_title_lower ON anime_basics(title_lower);
                CREATE INDEX IF NOT EXISTS idx_anime_type ON anime_basics(type);
                CREATE INDEX IF NOT EXISTS idx_anime_year ON anime_basics(season_year);
                CREATE INDEX IF NOT EXISTS idx_anime_type_year ON anime_basics(type, season_year);
                
                -- Covering index for exact title matches
                CREATE INDEX IF NOT EXISTS idx_anime_covering ON anime_basics(title_lower, season_year, type, title, episodes);
                """
            )
            
            # Optimize for read-only operations
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA wal_autocheckpoint=1000")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=50000")
            conn.execute("PRAGMA mmap_size=268435456")
            
            # Optimize database file
            conn.execute("PRAGMA optimize")
            conn.execute("PRAGMA incremental_vacuum")
            
            # Analyze tables for better query planning
            conn.execute("ANALYZE")
            
            conn.commit()
            logging.info("Anime database optimization complete")

    def find_title(self, title: str, year: Optional[int] = None) -> Optional[MatchResult]:
        """Find title information using database queries"""
        self.load_database()

        # Check cache first
        cache_key = f"{title.lower()}_{year}"
        if cache_key in self._search_cache:
            return self._search_cache[cache_key]

        best_match = None
        best_score = 0

        conn = self._get_connection()
        try:
            # First try exact match in main titles
            title_lower = title.lower()
            
            cursor = conn.execute(
                """
                SELECT * FROM search_view 
                WHERE title_lower = ?
                ORDER BY season_year DESC NULLS LAST
                LIMIT 1
                """,
                (title_lower,)
            )
            
            exact_match = cursor.fetchone()
            if exact_match:
                score = 100 * self.TITLE_WEIGHTS['main']
                
                # Add year match bonus
                if year and exact_match['season_year']:
                    if exact_match['season_year'] == year:
                        score += 20
                    elif abs(exact_match['season_year'] - year) <= 1:
                        score += 10
                
                # Add bonus for having episodes count
                if exact_match['episodes']:
                    score += 10
                
                if score > best_score:
                    best_score = score
                    best_match = exact_match

            # Try exact match in synonyms
            if not best_match:
                cursor = conn.execute(
                    """
                    SELECT a.*, s.relevance FROM anime_synonyms s
                    JOIN search_view a ON s.anime_id = a.id
                    WHERE s.title_lower = ?
                    ORDER BY s.relevance DESC, a.season_year DESC NULLS LAST
                    LIMIT 1
                    """,
                    (title_lower,)
                )
                
                synonym_match = cursor.fetchone()
                if synonym_match:
                    score = 100 * synonym_match['relevance']
                    
                    # Add year match bonus
                    if year and synonym_match['season_year']:
                        if synonym_match['season_year'] == year:
                            score += 20
                        elif abs(synonym_match['season_year'] - year) <= 1:
                            score += 10
                    
                    if synonym_match['episodes']:
                        score += 10
                    
                    if score > best_score:
                        best_score = score
                        best_match = synonym_match

            # If no exact match, try fuzzy matching using FTS5
            if not best_match:
                candidates = self._get_fuzzy_candidates(conn, title_lower, year)
                
                # Convert to search dict for fuzzy matching
                search_dict = {row['id']: row['title'] for row in candidates}
                
                if search_dict:
                    title_matches = process.extract(
                        title, search_dict, scorer=fuzz.ratio, limit=10
                    )

                    for matched_title, fuzzy_score, row_id in title_matches:
                        if fuzzy_score < 80:
                            continue
                        
                        # Find the full row data
                        row = next((r for r in candidates if r['id'] == row_id), None)
                        if not row:
                            continue

                        total_score = fuzzy_score * self.TITLE_WEIGHTS['main']

                        # Add year match bonus
                        if year and row['season_year']:
                            if row['season_year'] == year:
                                total_score += 20
                            elif abs(row['season_year'] - year) <= 1:
                                total_score += 10

                        # Add bonus for having episodes count
                        if row['episodes']:
                            total_score += 10

                        if total_score > best_score:
                            best_score = total_score
                            best_match = row

        finally:
            self._return_connection(conn)

        if best_match:
            title_info = self._create_title_info_from_row(best_match)
            result = MatchResult(
                info=title_info, score=best_score, provider_weight=self.provider_weight
            )

            # Cache result with size management
            self._search_cache[cache_key] = result
            if len(self._search_cache) > 1000:
                # Remove oldest entries
                oldest_keys = list(self._search_cache.keys())[:500]
                for key in oldest_keys:
                    del self._search_cache[key]

            return result

        return None

    def _get_fuzzy_candidates(self, conn: sqlite3.Connection, title_lower: str, year: Optional[int]) -> List[Dict]:
        """Get optimized candidate list using FTS5"""
        candidates = []
        
        # Strategy 1: FTS5 full-text search
        try:
            fts_query = self._build_fts_query(title_lower)
            if year:
                cursor = conn.execute(
                    """
                    SELECT s.*, fts.rank
                    FROM anime_fts fts
                    JOIN search_view s ON fts.rowid = s.rowid
                    WHERE anime_fts MATCH ?
                    AND (s.season_year BETWEEN ? AND ? OR s.season_year IS NULL)
                    ORDER BY fts.rank
                    LIMIT 100
                """,
                    (fts_query, year - 2, year + 2),
                )
            else:
                cursor = conn.execute(
                    """
                    SELECT s.*, fts.rank
                    FROM anime_fts fts
                    JOIN search_view s ON fts.rowid = s.rowid
                    WHERE anime_fts MATCH ?
                    ORDER BY fts.rank
                    LIMIT 150
                """,
                    (fts_query,),
                )
            
            fts_results = cursor.fetchall()
            candidates.extend([dict(row) for row in fts_results])
            
        except Exception as e:
            logging.debug(f"FTS search failed: {e}")
        
        return candidates[:200]  # Limit total candidates

    def _create_title_info_from_row(self, row) -> TitleInfo:
        """Create TitleInfo object from database row"""
        # Map type integer back to string
        type_map = {1: 'movie', 2: 'tvSeries'}
        media_type = type_map.get(row['type'], 'tvSeries')
        
        tags = row['tags'].split(',') if row['tags'] else []
        sources = row['sources'].split(',') if row['sources'] else []
        
        return TitleInfo(
            id=row['id'],
            title=row['title'],
            type=media_type,
            year=row['season_year'],
            status=row['status'],
            total_episodes=row['episodes'],
            tags=tags,
            sources=sources
        )

    def get_episode_info(self, parent_id: str, season: int, episode: int) -> Optional[EpisodeInfo]:
        """Get episode information if the title is a TV show"""
        self.load_database()

        # Check cache for episode info
        episode_key = f"{parent_id}_{season}_{episode}"
        if episode_key in self._search_cache:
            return self._search_cache[episode_key]

        conn = self._get_connection()
        try:
            # Find the anime by ID or in sources
            cursor = conn.execute(
                """
                SELECT * FROM search_view 
                WHERE id = ? OR sources LIKE ?
                LIMIT 1
                """,
                (parent_id, f'%{parent_id}%')
            )

            row = cursor.fetchone()
            if row:
                episode_info = EpisodeInfo(
                    title=f"Episode {episode}",  # Anime databases typically don't have episode titles
                    season=season,
                    episode=episode,
                    parent_id=parent_id,
                    year=row['season_year']
                )
                
                # Cache the result
                self._search_cache[episode_key] = episode_info
                return episode_info

        finally:
            self._return_connection(conn)

        return None

    def _normalize_title(self, title: str) -> str:
        """Normalize title for better searching"""
        import re
        
        # Convert to lowercase
        normalized = title.lower()
        
        # Remove punctuation and special characters
        normalized = re.sub(r'[^\w\s]', ' ', normalized)
        
        # Remove extra spaces
        normalized = ' '.join(normalized.split())
        
        return normalized
    
    def _build_fts_query(self, title: str) -> str:
        """Build FTS5 query from title"""
        # Normalize and tokenize
        words = self._normalize_title(title).split()
        
        # Create FTS5 query with different strategies
        if len(words) == 1:
            # Single word - use prefix matching
            return f'"{words[0]}"*'
        elif len(words) <= 3:
            # Few words - require all words (AND)
            return ' AND '.join([f'"{word}"*' for word in words])
        else:
            # Many words - use phrase search for first few words
            main_phrase = ' '.join(words[:3])
            return f'"{main_phrase}"'
    
    def __del__(self):
        """Clean up connection pool on destruction"""
        while self._connection_pool:
            conn = self._connection_pool.pop()
            conn.close()