import os
import re
import json
import logging
import requests
import sqlite3
import zipfile
import time
from typing import Optional
from tqdm import tqdm
from metadata_provider import BaseMetadataProvider, TitleInfo, EpisodeInfo, MatchResult
import enum

class AnimeType(enum.IntEnum):
    TV = 1
    MOVIE = 2
    OVA = 3
    ONA = 4
    SPECIAL = 5
    UNKNOWN = 0

ANIME_TYPE_TEXT_TO_ID = {
    "TV": AnimeType.TV,
    "MOVIE": AnimeType.MOVIE,
    "OVA": AnimeType.OVA,
    "ONA": AnimeType.ONA,
    "SPECIAL": AnimeType.SPECIAL,
}
ANIME_TYPE_ID_TO_TEXT = {v: k for k, v in ANIME_TYPE_TEXT_TO_ID.items()}
ANIME_TYPE_ID_TO_TEXT[AnimeType.UNKNOWN] = "UNKNOWN"

class AnimeStatus(enum.IntEnum):
    FINISHED = 1
    ONGOING = 2
    UPCOMING = 3
    UNKNOWN = 0

ANIME_STATUS_TEXT_TO_ID = {
    "FINISHED": AnimeStatus.FINISHED,
    "ONGOING": AnimeStatus.ONGOING,
    "UPCOMING": AnimeStatus.UPCOMING,
}
ANIME_STATUS_ID_TO_TEXT = {v: k for k, v in ANIME_STATUS_TEXT_TO_ID.items()}
ANIME_STATUS_ID_TO_TEXT[AnimeStatus.UNKNOWN] = "UNKNOWN"

class AnimeDataProvider(BaseMetadataProvider):
    ANIME_DB_URL = "https://github.com/manami-project/anime-offline-database/archive/refs/tags/latest.zip"
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
                conn.execute("PRAGMA busy_timeout=30000")
                conn.execute("PRAGMA synchronous=NORMAL")
                conn.execute("PRAGMA cache_size=10000")
                conn.execute("PRAGMA temp_store=MEMORY")
                conn.execute("PRAGMA page_size=32768")
                conn.execute("PRAGMA auto_vacuum=INCREMENTAL")
                
                # Table for main anime entries
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS anime_title (
                        id INTEGER PRIMARY KEY,
                        title TEXT NOT NULL,
                        type INTEGER,                    -- maps to anime_type.id
                        episodes INTEGER,
                        status INTEGER,                  -- maps to anime_status.id
                        year INTEGER,
                        duration INTEGER,
                        tags TEXT
                    )
                """)
                
                # Table for mapping type codes to text (text is unique, id reused)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS anime_type (
                        id INTEGER PRIMARY KEY,
                        text TEXT NOT NULL UNIQUE
                    )
                """)
                
                # Table for mapping status codes to text (text is unique, id reused)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS anime_status (
                        id INTEGER PRIMARY KEY,
                        text TEXT NOT NULL UNIQUE
                    )
                """)

                # Table for sources (MAL id to URL)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS sources (
                        id INTEGER NOT NULL,
                        url TEXT NOT NULL,
                        FOREIGN KEY(id) REFERENCES anime_title(id)
                    )
                """)
                
                # Table for synonyms (including main title for FTS)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS synonyms (
                        id INTEGER NOT NULL,
                        title TEXT NOT NULL,
                        FOREIGN KEY(id) REFERENCES anime_title(id)
                    )
                """)
                
                # Table for related anime (MAL id to MAL id)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS related (
                        id INTEGER NOT NULL,
                        related_id INTEGER NOT NULL,
                        FOREIGN KEY(id) REFERENCES anime_title(id),
                        FOREIGN KEY(related_id) REFERENCES anime_title(id)
                    )
                """)
                
                # Table for tracking data version
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS data_version (
                        dataset TEXT PRIMARY KEY,
                        updated INTEGER
                    )
                """)
                
                conn.execute("CREATE INDEX IF NOT EXISTS idx_synonyms_id ON synonyms(id)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_sources_id ON sources(id)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_related_src_id ON related(id)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_related_dst_id ON related(related_id)")
                
                # View for searching anime by synonym title, joining type and status text
                conn.execute("""
                    CREATE VIEW IF NOT EXISTS anime_synonym_view AS
                    SELECT
                        a.id,
                        a.title,
                        s.title AS synonym,
                        ty.text AS type,
                        a.episodes,
                        st.text AS status,
                        a.year,
                        a.duration,
                        a.tags
                    FROM synonyms s
                    JOIN anime_title a ON s.id = a.id
                    LEFT JOIN anime_type ty ON a.type = ty.id
                    LEFT JOIN anime_status st ON a.status = st.id
                """)
                
                # FTS5 virtual table for fast title/synonym search
                conn.execute("""
                    CREATE VIRTUAL TABLE IF NOT EXISTS anime_fts USING fts5(
                        id UNINDEXED,
                        title,
                        content='synonyms',
                        content_rowid='rowid',
                        tokenize='porter unicode61'
                    )
                """)
                conn.commit()
        except Exception as e:
            logging.error(f"Failed to initialize anime database: {str(e)}")
            raise

    def _get_type_id(self, type_str: str) -> int:
        """Map type string to AnimeType enum value"""
        if not type_str:
            return AnimeType.UNKNOWN
        type_str = type_str.strip().upper()
        return int(ANIME_TYPE_TEXT_TO_ID.get(type_str, AnimeType.UNKNOWN))

    def _get_type_text(self, type_id: int) -> str:
        """Map type id to string"""
        try:
            anime_type = AnimeType(type_id)
            return ANIME_TYPE_ID_TO_TEXT.get(anime_type, "UNKNOWN")
        except ValueError:
            return "UNKNOWN"

    def _get_status_id(self, status_str: str) -> int:
        """Map status string to AnimeStatus enum value"""
        if not status_str:
            return AnimeStatus.UNKNOWN
        status_str = status_str.strip().upper()
        return int(ANIME_STATUS_TEXT_TO_ID.get(status_str, AnimeStatus.UNKNOWN))

    def _get_status_text(self, status_id: int) -> str:
        """Map status id to string"""
        try:
            anime_status = AnimeStatus(status_id)
            return ANIME_STATUS_ID_TO_TEXT.get(anime_status, "UNKNOWN")
        except ValueError:
            return "UNKNOWN"

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

                # Also check if we have data in anime_title
                cursor = conn.execute("SELECT COUNT(*) FROM anime_title LIMIT 1")
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

        zip_path = os.path.join(self.cache_dir, "anime-offline-database-latest.zip")
        zip_expiry_days = 7

        try:
            # Check if cached zip exists and is fresh
            need_download = True
            if os.path.exists(zip_path):
                mtime = os.path.getmtime(zip_path)
                age_days = (time.time() - mtime) / 86400
                if age_days < zip_expiry_days:
                    need_download = False

            if need_download:
                logging.info("Downloading anime database zip file from remote server...")
                try:
                    response = requests.get(self.ANIME_DB_URL, stream=True)
                    response.raise_for_status()
                    total_size = int(response.headers.get('content-length', 0))
                    with open(zip_path, "wb") as f, tqdm(total=total_size, desc="Downloading anime database", unit='B', unit_scale=True) as pbar:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                                pbar.update(len(chunk))
                except Exception as e:
                    logging.error(f"Failed to download anime database zip: {e}")
                    raise

            # Extract anime-offline-database.json from the zip in memory
            try:
                with zipfile.ZipFile(zip_path, "r") as zf:
                    json_filename = None
                    for name in zf.namelist():
                        if name.endswith("anime-offline-database.json"):
                            json_filename = name
                            break
                    if not json_filename:
                        raise RuntimeError("anime-offline-database.json not found in zip file")
                    with zf.open(json_filename) as json_file:
                        json_bytes = json_file.read()
                        json_str = json_bytes.decode("utf-8")
            except Exception as e:
                logging.error(f"Failed to extract anime-offline-database.json from zip: {e}")
                raise

            # Write the JSON string to a temporary file for compatibility with _load_json_to_db
            temp_json = os.path.join(self.cache_dir, "temp_anime.json")
            try:
                with open(temp_json, "w", encoding="utf-8") as f:
                    f.write(json_str)
            except Exception as e:
                logging.error(f"Failed to write temp JSON file: {e}")
                raise

            # Parse and insert into database
            self._load_json_to_db(temp_json)

            # Clean up temp file, but keep the zip
            try:
                os.remove(temp_json)
            except Exception:
                pass

        except Exception as e:
            logging.error(f"Error processing anime database: {e}")
            raise

        return

    def _load_json_to_db(self, json_file: str) -> None:
        """Load JSON data into SQLite database"""
        try:
            with sqlite3.connect(self._db_path, timeout=60.0) as conn:
                conn.execute("PRAGMA synchronous=OFF")
                conn.execute("PRAGMA temp_store=MEMORY")
                conn.execute("PRAGMA cache_size=100000")

                # Clear existing data
                conn.execute("DELETE FROM anime_title")
                conn.execute("DELETE FROM anime_type")
                conn.execute("DELETE FROM anime_status")
                conn.execute("DELETE FROM synonyms")
                conn.execute("DELETE FROM sources")
                conn.execute("DELETE FROM related")

                # Prepare enums for type/status
                for type_id, type_text in ANIME_TYPE_ID_TO_TEXT.items():
                    conn.execute(
                        "INSERT OR IGNORE INTO anime_type (id, text) VALUES (?, ?)",
                        (int(type_id), type_text)
                    )
                for status_id, status_text in ANIME_STATUS_ID_TO_TEXT.items():
                    conn.execute(
                        "INSERT OR IGNORE INTO anime_status (id, text) VALUES (?, ?)",
                        (int(status_id), status_text)
                    )

                with tqdm(desc="Processing anime database", unit='entries') as pbar:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)

                        anime_batch = []
                        synonyms_batch = []
                        sources_batch = []
                        related_batch = []

                        for entry in data['data']:
                            # Get MAL id from sources
                            mal_id = None
                            for src in entry.get('sources', []):
                                if "myanimelist.net/anime/" in src:
                                    try:
                                        mal_id = int(src.rstrip('/').split('/')[-1])
                                        break
                                    except Exception:
                                        continue
                            if not mal_id:
                                print(f"Skipping entry without valid MAL id: {entry.get('title', 'Unknown')}")
                                continue

                            title = entry['title']
                            type_id = self._get_type_id(entry.get('type'))
                            episodes = self.safe_int(entry.get('episodes'))
                            status_id = self._get_status_id(entry.get('status'))
                            year = self.safe_int(entry.get('animeSeason', {}).get('year'))
                            duration = self.safe_int(entry.get('duration'))
                            tags = ','.join(entry.get('tags', []))

                            anime_batch.append(
                                (mal_id, title, type_id, episodes, status_id, year, duration, tags)
                            )

                            # Main title as synonym
                            synonyms_batch.append((mal_id, title))
                            # Do NOT insert into fts_batch here

                            # Synonyms
                            for synonym in entry.get('synonyms', []):
                                if synonym and synonym != title:
                                    synonyms_batch.append((mal_id, synonym))
                                    # Do NOT insert into fts_batch here

                            # Other sources
                            for src in entry.get('sources', []):
                                if "myanimelist.net/anime/" not in src:
                                    sources_batch.append((mal_id, src))

                            # Related anime
                            for rel in entry.get('relatedAnime', []):
                                rel_id = None
                                if "myanimelist.net/anime/" in rel:
                                    try:
                                        rel_id = int(rel.rstrip('/').split('/')[-1])
                                        related_batch.append((mal_id, rel_id))
                                        break
                                    except Exception:
                                        print(f"Skipping related anime without valid MAL id: {rel}")
                                        continue

                            pbar.update(1)

                        # Bulk insert
                        conn.executemany(
                            """INSERT OR REPLACE INTO anime_title 
                               (id, title, type, episodes, status, year, duration, tags)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                            anime_batch
                        )
                        conn.executemany(
                            "INSERT INTO synonyms (id, title) VALUES (?, ?)",
                            synonyms_batch
                        )
                        # REMOVE: conn.executemany("INSERT INTO anime_fts (rowid, title, id) VALUES (?, ?, ?)", fts_batch)
                        conn.executemany(
                            "INSERT INTO sources (id, url) VALUES (?, ?)",
                            sources_batch
                        )
                        conn.executemany(
                            "INSERT INTO related (id, related_id) VALUES (?, ?)",
                            related_batch
                        )

                # Update version info
                conn.execute(
                    """INSERT OR REPLACE INTO data_version (dataset, updated) VALUES (?, ?)""",
                    ('anime_offline_database', int(time.time())),
                )
                conn.commit()
                logging.info(f"Loaded {len(anime_batch)} anime entries and {len(synonyms_batch)} synonyms")

                # After inserting all data, rebuild the FTS table from the content table
                conn.execute("INSERT INTO anime_fts(anime_fts) VALUES('rebuild')")

        except Exception as e:
            logging.error(f"Error loading anime JSON to database: {str(e)}")
            raise

    def find_title(self, title: str, year: Optional[int] = None) -> Optional[MatchResult]:
        """Find title information using database queries"""
        self.load_database()
        cache_key = f"{title.lower()}_{year if year else ''}"
        if cache_key in self._search_cache:
            return self._search_cache[cache_key]
        best_score = 200
        conn = self._get_connection()
        try:
            candidates = []
            # 0. Try exact match first
            cursor = conn.execute(
                "SELECT * FROM anime_synonym_view WHERE synonym = ?",
                (title,)
            )
            candidates = cursor.fetchmany(1)
            if candidates is None or len(candidates) == 0:
                # 1. Build FTS5 query string for the title
                fts_query = self._build_fts_query(title)
                # 2. Query FTS5 table for candidate synonyms
                cursor = conn.execute(
                    """
                    SELECT s.id, s.title, bm25(anime_fts, 10.0) AS score
                    FROM anime_fts f
                    JOIN synonyms s ON f.rowid = s.rowid
                    WHERE anime_fts MATCH ?
                    ORDER BY score
                    """,
                    (fts_query,)
                )
                candidates = cursor.fetchmany(50)
            # 3. The first candidate is the top scorer, much better results than fuzzer
            if candidates and len(candidates) > 0:
                top_candidate = candidates[0]
                anime_data = conn.execute(
                    "SELECT * FROM anime_synonym_view WHERE id = ?", (top_candidate['id'],)
                ).fetchone()
        finally:
            self._return_connection(conn)
        if anime_data:
            title_info = self._create_title_info_from_row(anime_data)
            result = MatchResult(
                info=title_info, score=best_score, provider_weight=self.provider_weight
            )
            self._search_cache[cache_key] = result
            if len(self._search_cache) > 1000:
                oldest_keys = list(self._search_cache.keys())[:500]
                for key in oldest_keys:
                    del self._search_cache[key]
            return result
        return None

    def _create_title_info_from_row(self, row) -> TitleInfo:
        """Create TitleInfo object from database row"""
        # Use the already joined type and status text from the view if present
        type_text = row['type'] if 'type' in row.keys() else "UNKNOWN"
        # Look up the AnimeType enum
        type_enum = AnimeType.__members__.get(type_text.upper(), AnimeType.UNKNOWN)
        # Map the enum to the "anime_series" or "anime_movie"
        if type_enum == AnimeType.MOVIE:
            type_text = "anime_movie"
        else:
            type_text = "anime_series"

        status_text = row['status'] if 'status' in row.keys() else "UNKNOWN"
        tags = row['tags'].split(',') if row['tags'] else []
        # Get sources
        try:
            conn = self._get_connection()
            cursor = conn.execute("SELECT url FROM sources WHERE id = ?", (row['id'],))
            urls = cursor.fetchall()
            sources = [r['url'] for r in urls]
            # Prepend the myanimelist source to sources, construct it from the row['id']
            if row['id']:
                sources.insert(0, f"https://myanimelist.net/anime/{row['id']}")
        finally:
            self._return_connection(conn)
        return TitleInfo(
            id=row['id'],
            title=row['title'],
            type=type_text,
            year=row['year'],
            status=status_text,
            total_episodes=row['episodes'],
            tags=tags,
            sources=sources
        )

    def get_episode_info(self, parent_id: str, season: int, episode: int) -> Optional[EpisodeInfo]:
        """Get episode information if the title is a TV show"""
        self.load_database()
        episode_key = f"{parent_id}_{season}_{episode}"
        if episode_key in self._search_cache:
            return self._search_cache[episode_key]
        conn = self._get_connection()
        try:
            cursor = conn.execute(
                "SELECT * FROM anime_title WHERE id = ?",
                (parent_id,)
            )
            row = cursor.fetchone()
            if row:
                episode_info = EpisodeInfo(
                    title=f"Episode {episode}",
                    season=season,
                    episode=episode,
                    parent_id=parent_id,
                    year=row['year']
                )
                # Check if the title is a TV show
                if episode is None:
                    return None
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

        # Split into words and build FTS5 query
        words = normalized.split()
        normalized = ' OR '.join([f'{word}*' for word in words])

        return normalized