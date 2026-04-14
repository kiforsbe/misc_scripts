# pyright: reportMissingImports=false, reportMissingModuleSource=false
import os
import re
import json
import logging
import requests
import sqlite3
import zstandard as zstd
import time
from difflib import SequenceMatcher
from datetime import timedelta
import datetime
from typing import Optional
from tqdm import tqdm
from metadata_provider import BaseMetadataProvider, TitleInfo, EpisodeInfo, MatchResult
import enum

try:
    from rapidfuzz import fuzz as rapidfuzz_fuzz
except ImportError:
    rapidfuzz_fuzz = None

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
    ANIME_DB_URL = "https://github.com/manami-project/anime-offline-database/releases/download/latest/anime-offline-database-minified.json.zst"
    MAX_RETRIES = 3
    
    # Define relevance scores for different title types
    TITLE_WEIGHTS = {
        'main': 1.0,      # Main title gets full weight
        'english': 0.9,   # English title slightly less
        'synonym': 0.8    # Synonyms get lower base weight
    }
    
    # Season parsing regex patterns
    SEASON_PATTERNS = [
        r'\b(\d+)(?:st|nd|rd|th)\s+Season\b',     # "2nd Season", "3rd Season"
        r'\bSeason\s+(\d+)\b',                    # "Season 3", "Season 2"
        r'\bS(\d+)\b',                            # "S2", "S3"
        r'\b(\d+)(?:st|nd|rd|th)\s+Series\b',     # "2nd Series"
        r'\bSeries\s+(\d+)\b',                    # "Series 2"
        r'\bPart\s+(\d+)\b',                      # "Part 2"
        r'\b(\d+)(?:st|nd|rd|th)\s+Part\b',       # "2nd Part"
        r'\bII\b',                                # Roman numeral II = 2
        r'\bIII\b',                               # Roman numeral III = 3
        r'\bIV\b',                                # Roman numeral IV = 4
        r'\bV\b',                                 # Roman numeral V = 5
        r'\bVI\b',                                # Roman numeral VI = 6
        r'\bVII\b',                               # Roman numeral VII = 7
        r'\bVIII\b',                              # Roman numeral VIII = 8
        r'\bIX\b',                                # Roman numeral IX = 9
        r'\bX\b',                                 # Roman numeral X = 10
    ]
    
    # Roman numeral to number mapping
    ROMAN_TO_NUMBER = {
        'II': 2, 'III': 3, 'IV': 4, 'V': 5, 'VI': 6, 'VII': 7, 'VIII': 8, 'IX': 9, 'X': 10
    }
    
    def __init__(self):
        super().__init__('anime', provider_weight=1.0)
        # Provider-level default TTL is provided via BaseMetadataProvider.cache_duration
        self._search_cache = {}  # Cache recent search results
        # In-process guard to avoid re-checking/reloading DB on every lookup.
        self._db_loaded_once = False
        self._db_loaded_until_ts: Optional[int] = None
        self._db_path = os.path.join(self.cache_dir, "anime_data.db")
        # Which data_version.dataset rows represent this provider's sources
        self.CACHE_EXPIRY_DATASETS = ['anime_offline_database']
        self._init_connection_pool(pool_size=3)
        self._init_database()
        self._load_cache_duration()
        expired = self._get_expired_datasets(self.CACHE_EXPIRY_DATASETS)
        if expired:
            # Do not force refresh during construction; this allows status checks
            # without triggering network/database work. Reload happens on first lookup.
            logging.info("Expired anime datasets detected on init: %s. Reload will occur on next data access.", ",".join(expired))

    # Year-based recency boost: newer titles receive up to this many points
    YEAR_RECENCY_MAX_BONUS = 20.0
    # Age (in years) at which bonus falls to zero
    YEAR_RECENCY_DECAY_YEARS = 10
    EXACT_MATCH_SCORE = 1000.0
    PREFIX_SCORE_BASE = 700.0
    PREFIX_SCORE_RANGE = 140.0
    PREFIX_SCORE_CAP = 899.0
    FUZZY_SCORE_BASE = 400.0
    FUZZY_SCORE_RANGE = 260.0
    FUZZY_SCORE_CAP = 749.0

    def _parse_season_from_title(self, title: str) -> tuple[Optional[int], str]:
        """
        Parse season number from title and return (season_number, base_title)
        
        Args:
            title: The anime title to parse
            
        Returns:
            Tuple of (season_number, base_title) where:
            - season_number is None if no season found, or the detected season number
            - base_title is the title with season indicators removed
        """
        if not title:
            return None, title
            
        original_title = title
        season_number = None
        
        # Try each pattern
        for pattern in self.SEASON_PATTERNS:
            match = re.search(pattern, title, re.IGNORECASE)
            if match:
                if pattern in [r'\bII\b', r'\bIII\b', r'\bIV\b', r'\bV\b', r'\bVI\b', r'\bVII\b', r'\bVIII\b', r'\bIX\b', r'\bX\b']:
                    # Roman numeral
                    roman = match.group(0).upper()
                    season_number = self.ROMAN_TO_NUMBER.get(roman)
                    if season_number:
                        # Remove the roman numeral and surrounding spaces
                        title = re.sub(r'\s*' + re.escape(roman) + r'\s*', ' ', title, flags=re.IGNORECASE).strip()
                        break
                else:
                    # Regular pattern with captured group
                    season_number = int(match.group(1))
                    # Remove the entire matched pattern
                    title = re.sub(pattern, '', title, flags=re.IGNORECASE).strip()
                    # Clean up extra spaces
                    title = re.sub(r'\s+', ' ', title).strip()
                    break
        
        # If no season found, assume season 1
        if season_number is None:
            season_number = 1
            
        # Clean up the base title
        base_title = title.strip()
        # Remove leading/trailing dashes or colons that might be left over
        base_title = re.sub(r'^[-:\s]+|[-:\s]+$', '', base_title).strip()
        
        return season_number, base_title

    def _extract_base_title(self, title: str) -> str:
        """
        Extract the base title by removing common subtitles and season indicators
        
        Args:
            title: The full anime title
            
        Returns:
            The base title without season indicators or subtitles
        """
        # First remove season indicators
        _, base_title = self._parse_season_from_title(title)
        
        # Remove common subtitle patterns (after colon, dash, or parentheses)
        # But be careful not to remove essential parts of the title
        patterns_to_remove = [
            r'\s*-\s*[^-]*$',  # Remove everything after the last dash
            r'\s*:\s*[^:]*$',  # Remove everything after the last colon
            r'\s*\([^)]*\)$',  # Remove trailing parentheses content
        ]
        
        for pattern in patterns_to_remove:
            # Only remove if the remaining title is still substantial (> 3 chars)
            potential_title = re.sub(pattern, '', base_title).strip()
            if len(potential_title) > 3:
                base_title = potential_title
                break
                
        return base_title.strip()

    def _group_anime_by_base_title(self, anime_entries: list) -> dict:
        """
        Group anime entries by their base title for season detection
        
        Args:
            anime_entries: List of anime entry dictionaries
            
        Returns:
            Dictionary mapping base_title -> list of entries sorted by year
        """
        groups = {}
        
        for entry in anime_entries:
            # Skip movies and specials
            type_id = self._get_type_id(entry.get('type'))
            if type_id in [AnimeType.MOVIE, AnimeType.SPECIAL]:
                continue
                
            title = entry.get('title', '')
            base_title = self._extract_base_title(title)
            
            if base_title not in groups:
                groups[base_title] = []
            groups[base_title].append(entry)
        
        # Sort each group by year
        for base_title, entries in groups.items():
            entries.sort(key=lambda x: x.get('animeSeason', {}).get('year', 0) or 0)
            
        return groups

    def _derive_seasons_from_years(self, grouped_entries: dict) -> dict:
        """
        Derive season numbers from publication years for entries that don't have explicit seasons
        
        Args:
            grouped_entries: Dictionary mapping base_title -> list of entries sorted by year
            
        Returns:
            Dictionary mapping anime_id -> season_number
        """
        season_assignments = {}
        
        for base_title, entries in grouped_entries.items():
            if len(entries) <= 1:
                # Single entry, assign season 1
                for entry in entries:
                    mal_id = self._get_mal_id_from_entry(entry)
                    if mal_id:
                        # Check if title already has explicit season
                        explicit_season, _ = self._parse_season_from_title(entry.get('title', ''))
                        if explicit_season and explicit_season > 1:
                            season_assignments[mal_id] = explicit_season
                        else:
                            season_assignments[mal_id] = 1
                continue
            
            # Multiple entries - derive seasons from years and titles
            season_counter = 1
            last_year = None
            
            for entry in entries:
                mal_id = self._get_mal_id_from_entry(entry)
                if not mal_id:
                    continue
                    
                title = entry.get('title', '')
                year = entry.get('animeSeason', {}).get('year')
                
                # First check for explicit season in title
                explicit_season, _ = self._parse_season_from_title(title)
                if explicit_season and explicit_season > 1:
                    season_assignments[mal_id] = explicit_season
                    season_counter = max(season_counter, explicit_season + 1)
                    last_year = year
                    continue
                
                # If no explicit season, derive from position in chronological order
                if last_year is None:
                    # First entry
                    season_assignments[mal_id] = 1
                    season_counter = 2
                else:
                    # Subsequent entries
                    if year and year > last_year:
                        # Published later, likely next season
                        season_assignments[mal_id] = season_counter
                        season_counter += 1
                    else:
                        # Same year or earlier, might be special/ova/sequel
                        # Check if title suggests it's a continuation
                        if self._title_suggests_continuation(title):
                            season_assignments[mal_id] = season_counter
                            season_counter += 1
                        else:
                            # Treat as same season or special
                            season_assignments[mal_id] = 1
                
                last_year = year
        
        return season_assignments

    def _get_mal_id_from_entry(self, entry: dict) -> Optional[int]:
        """Extract MAL ID from anime entry sources"""
        for src in entry.get('sources', []):
            if "myanimelist.net/anime/" in src:
                try:
                    return int(src.rstrip('/').split('/')[-1])
                except (ValueError, IndexError):
                    continue
        return None

    def _title_suggests_continuation(self, title: str) -> bool:
        """Check if title suggests it's a continuation/sequel"""
        continuation_keywords = [
            'final', 'last', 'end', 'conclusion', 'finale',
            'next', 'continue', 'sequel', 'second', 'third',
            'new', 'kai', 'shippuden', 'brotherhood',
            'advance', 'evolution', 'revolution'
        ]
        
        title_lower = title.lower()
        return any(keyword in title_lower for keyword in continuation_keywords)

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
                        tags TEXT,
                        score REAL,                      -- median score from anime database
                        season_number INTEGER DEFAULT 1, -- parsed season number
                        base_title TEXT                  -- title without season indicators
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
                        a.tags,
                        a.score,
                        a.season_number,
                        a.base_title
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

    def _load_cache_duration(self) -> None:
        # Use base-class settings persistence (file-based). Avoid storing days in
        # the DB's `data_version.updated` field which is an epoch timestamp.
        try:
            super()._load_cache_duration()
        except Exception as e:
            logging.debug(f"Could not load cache duration for Anime provider: {e}")

    def _persist_cache_duration(self) -> None:
        # Defer to base class JSON-backed persistence to avoid polluting
        # `data_version.updated` which stores epoch timestamps for datasets.
        try:
            super()._persist_cache_duration()
        except Exception:
            logging.debug("Could not persist cache duration for Anime provider")
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
        return self._is_data_current_in_db(
            main_table="anime_title",
            datasets=getattr(self, "CACHE_EXPIRY_DATASETS", None),
        )

    def load_database(self) -> None:
        """Load the anime database into SQLite, downloading if needed"""
        now_ts = int(time.time())

        # Fast-path: DB already validated/loaded in this process and not expired.
        if self._db_loaded_once:
            if self._db_loaded_until_ts is None or now_ts < self._db_loaded_until_ts:
                return
            # Past in-memory expiry, force re-validation below.
            self._db_loaded_once = False

        if self._is_data_current():
            logging.info("Database contains current anime data")
            self._db_loaded_once = True
            try:
                self._db_loaded_until_ts = int(self.get_cache_expiry().timestamp())
            except Exception:
                self._db_loaded_until_ts = now_ts + int(self.cache_duration.total_seconds())
            return

        logging.info("Loading anime database...")

        zst_path = os.path.join(self.cache_dir, "anime-offline-database-minified.json.zst")

        try:
            # Check if cached zst file exists and is fresh
            if self._should_download_file(zst_path):
                logging.info("Downloading anime database from remote server...")
                try:
                    response = requests.get(self.ANIME_DB_URL, stream=True)
                    response.raise_for_status()
                    total_size = int(response.headers.get('content-length', 0))
                    with open(zst_path, "wb") as f, tqdm(total=total_size, desc="Downloading anime database", unit='B', unit_scale=True) as pbar:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                                pbar.update(len(chunk))
                except Exception as e:
                    logging.error(f"Failed to download anime database: {e}")
                    raise

            # Decompress the zst file
            try:
                with open(zst_path, "rb") as zst_file:
                    dctx = zstd.ZstdDecompressor()
                    try:
                        # Try regular decompression first
                        json_bytes = dctx.decompress(zst_file.read())
                    except zstd.ZstdError as e:
                        # If we get a "could not determine content size in frame header" error,
                        # use streaming decompression instead
                        if "could not determine content size in frame header" in str(e):
                            logging.info("Using streaming decompression due to missing content size in zst header")
                            zst_file.seek(0)  # Reset file position
                            reader = dctx.stream_reader(zst_file)
                            json_bytes = reader.read()
                        else:
                            raise
                    json_str = json_bytes.decode("utf-8")
            except Exception as e:
                logging.error(f"Failed to decompress anime database: {e}")
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

            # Clean up temp file, but keep the zst file
            try:
                os.remove(temp_json)
            except Exception:
                pass

        except Exception as e:
            logging.error(f"Error processing anime database: {e}")
            raise

        self._db_loaded_once = True
        try:
            self._db_loaded_until_ts = int(self.get_cache_expiry().timestamp())
        except Exception:
            self._db_loaded_until_ts = now_ts + int(self.cache_duration.total_seconds())

        return

    def _load_json_to_db(self, json_file: str) -> None:
        """Load JSON data into SQLite database"""
        try:
            with sqlite3.connect(self._db_path, timeout=60.0) as conn:
                conn.execute("PRAGMA synchronous=OFF")
                conn.execute("PRAGMA temp_store=MEMORY")
                conn.execute("PRAGMA cache_size=100000")

                # Check current schema and drop tables if outdated
                cursor = conn.execute("PRAGMA table_info(anime_title)")
                columns = [row[1] for row in cursor.fetchall()]
                if 'season_number' not in columns or 'base_title' not in columns or 'score' not in columns:
                    logging.info("Dropping outdated database schema to recreate with new columns")
                    # Drop all tables to recreate with new schema
                    conn.execute("DROP TABLE IF EXISTS anime_title")
                    conn.execute("DROP TABLE IF EXISTS anime_type") 
                    conn.execute("DROP TABLE IF EXISTS anime_status")
                    conn.execute("DROP TABLE IF EXISTS synonyms")
                    conn.execute("DROP TABLE IF EXISTS sources")
                    conn.execute("DROP TABLE IF EXISTS related")
                    conn.execute("DROP TABLE IF EXISTS data_version")
                    conn.execute("DROP VIEW IF EXISTS anime_synonym_view")
                    conn.execute("DROP TABLE IF EXISTS anime_fts")
                    # Recreate schema 
                    self._init_database()
                else:
                    # Clear existing data if schema is current
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
                        
                        # First pass: collect all entries for season analysis
                        all_entries = data['data']
                        
                        # Group entries by base title for season detection
                        grouped_entries = self._group_anime_by_base_title(all_entries)
                        
                        # Derive season numbers
                        season_assignments = self._derive_seasons_from_years(grouped_entries)

                        anime_batch = []
                        synonyms_batch = []
                        sources_batch = []
                        related_batch = []

                        for entry in all_entries:
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
                                logging.debug(f"Skipping entry without valid MAL id: {entry.get('title', 'Unknown')}")
                                continue

                            title = entry['title']
                            type_id = self._get_type_id(entry.get('type'))
                            episodes = self.safe_int(entry.get('episodes'))
                            status_id = self._get_status_id(entry.get('status'))
                            year = self.safe_int(entry.get('animeSeason', {}).get('year'))
                            duration = self.safe_int(entry.get('duration'))
                            tags = ','.join(entry.get('tags', []))
                            
                            # Extract score from JSON data
                            score = None
                            if 'score' in entry and 'median' in entry['score']:
                                try:
                                    score = float(entry['score']['median'])
                                except (ValueError, TypeError):
                                    score = None
                            
                            # Get season information
                            season_number = season_assignments.get(mal_id, 1)
                            _, base_title = self._parse_season_from_title(title)
                            if not base_title:
                                base_title = self._extract_base_title(title)

                            anime_batch.append(
                                (mal_id, title, type_id, episodes, status_id, year, duration, tags, score, season_number, base_title)
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
                               (id, title, type, episodes, status, year, duration, tags, score, season_number, base_title)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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

                # Update version info: write expires_at, last_modified and updated
                # Do not overwrite `default_ttl` here; that value should be
                # controlled by explicit user `set-expiry` operations or the
                # provider's initial schema defaults. Avoid setting it to 0
                # when cache_duration is sub-day.
                now_ts = int(time.time())
                expiry_ts = now_ts + int(self.cache_duration.total_seconds())
                try:
                    # If a data_version row exists, preserve its `default_ttl` value
                    # (even if zero). Only initialize `default_ttl` when no row
                    # exists at all by using provider DEFAULT_TTL_DAYS.
                    try:
                        cur = conn.execute("SELECT default_ttl FROM data_version WHERE dataset = ? LIMIT 1", ('anime_offline_database',))
                        row = cur.fetchone()
                        if row:
                            # Use DB value only if it's a positive, non-blank integer
                            try:
                                db_val = int(row[0]) if row[0] is not None else None
                            except Exception:
                                db_val = None
                            ttl_val = db_val if db_val and db_val > 0 else int(self.cache_duration.days)
                        else:
                            # No existing row: initialize default_ttl from configured cache_duration days
                            ttl_val = int(self.cache_duration.days)
                    except Exception:
                        ttl_val = int(self.cache_duration.days)

                    if ttl_val <= 0:
                        ttl_val = self.DEFAULT_TTL_DAYS

                    conn.execute(
                        "INSERT OR REPLACE INTO data_version (dataset, expires_at, default_ttl, last_modified, updated) VALUES (?, ?, ?, ?, ?)",
                        ('anime_offline_database', expiry_ts, ttl_val, now_ts, now_ts),
                    )
                except Exception:
                    try:
                        conn.execute(
                            "INSERT OR REPLACE INTO data_version (dataset, updated) VALUES (?, ?)",
                            ('anime_offline_database', now_ts),
                        )
                    except Exception:
                        pass
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
        best_score = 0.0
        anime_data = None  # Initialize to prevent UnboundLocalError
        prepared_query = self._prepare_similarity_query(title)
        conn = self._get_connection()
        try:
            candidates = []
            # 0. Try exact match first
            cursor = conn.execute(
                """
                SELECT * FROM anime_synonym_view
                WHERE synonym = ? COLLATE NOCASE OR title = ? COLLATE NOCASE
                ORDER BY CASE
                    WHEN synonym = ? COLLATE NOCASE THEN 0
                    WHEN title = ? COLLATE NOCASE THEN 1
                    ELSE 2
                END,
                LENGTH(synonym),
                year DESC
                LIMIT 1
                """,
                (title, title, title, title)
            )
            candidates = cursor.fetchmany(1)
            if candidates:
                anime_data = candidates[0]
                best_score = self._score_title_match_prepared(
                    prepared_query,
                    anime_data['synonym'],
                    candidate_year=self._row_or_dict_value(anime_data, 'year'),
                )
            else:
                prefix_pattern = f"{title.strip()}%"
                cursor = conn.execute(
                    """
                    SELECT * FROM anime_synonym_view
                    WHERE synonym LIKE ? COLLATE NOCASE OR title LIKE ? COLLATE NOCASE
                    ORDER BY LENGTH(synonym), year DESC
                    LIMIT 25
                    """,
                    (prefix_pattern, prefix_pattern)
                )
                direct_candidates = cursor.fetchall()
                if direct_candidates:
                    ranked_direct_candidates = self._rank_candidates(
                        title,
                        direct_candidates,
                        conn=conn,
                        prepared_query=prepared_query,
                    )
                    if ranked_direct_candidates:
                        anime_data = ranked_direct_candidates[0]
                        best_score = self._best_score_for_anime_id(
                            conn,
                            title,
                            anime_data['id'],
                            prepared_query=prepared_query,
                        )

            if anime_data is None:
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
                candidates = cursor.fetchall()  # Get all candidates for post-filtering
                
                # Post-filter candidates to prefer better matches
                if candidates:
                    candidates = self._rank_candidates(
                        title,
                        candidates,
                        conn=conn,
                        prepared_query=prepared_query,
                    )
                    
            # 3. The first candidate is the top scorer, much better results than fuzzer
            if anime_data is None and candidates and len(candidates) > 0:
                top_candidate = candidates[0]
                anime_data = top_candidate
                if anime_data:
                    best_score = self._best_score_for_anime_id(
                        conn,
                        title,
                        anime_data['id'],
                        bm25_score=top_candidate['score'] if 'score' in top_candidate.keys() else None,
                        prepared_query=prepared_query,
                    )
        finally:
            self._return_connection(conn)
        if anime_data:
            title_info = self._create_title_info_from_row(anime_data)
            result = MatchResult(
                info=title_info, score=best_score, provider_weight=self.provider_weight
            )
            self._search_cache[cache_key] = result
            if len(self._search_cache) > 1000:
                try:
                    oldest = next(iter(self._search_cache))
                    del self._search_cache[oldest]
                except Exception:
                    self._search_cache.clear()
            return result

    def _create_title_info_from_row(self, row) -> TitleInfo:
        """Create TitleInfo object from a database row for anime."""
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
        
        # Get season information
        season_number = row['season_number'] if 'season_number' in row.keys() else 1
        base_title = row['base_title'] if 'base_title' in row.keys() else row['title']
        
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
            
        # For anime series, if this is not season 1, append season info to title
        display_title = row['title']
        if type_text == "anime_series" and season_number > 1:
            # Check if season is already in the title
            if str(season_number) not in display_title and not any(
                pattern in display_title.lower() 
                for pattern in [f's{season_number}', f'season {season_number}', f'{season_number}nd', f'{season_number}rd', f'{season_number}th']
            ):
                display_title = f"{base_title} Season {season_number}"
            
        return TitleInfo(
            id=row['id'],
            title=display_title,
            type=type_text,
            year=row['year'],
            status=status_text,
            rating=row['score'] if 'score' in row.keys() else None,  # Use score from database as rating
            runtime_minutes=None,
            total_episodes=row['episodes'],
            total_seasons=season_number if type_text == "anime_series" else None,
            tags=tags,
            sources=sources
        )

    def get_episode_info(self, parent_id: int, season: Optional[int], episode: int) -> Optional[EpisodeInfo]:
        """Get episode information with season/episode calculation"""
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
            if row and episode is not None:
                # Get the base title to find all seasons
                base_title = row['base_title'] if 'base_title' in row.keys() else row['title']
                
                # Find all seasons of this anime ordered by season number
                cursor = conn.execute(
                    """
                    SELECT id, title, episodes, season_number, year, status
                    FROM anime_title 
                    WHERE base_title = ?
                    ORDER BY season_number, year
                    """,
                    (base_title,)
                )
                all_seasons = cursor.fetchall()
                
                calculated_season = season
                calculated_episode = episode
                
                # If no explicit season provided, calculate from absolute episode number
                if season is None and all_seasons:
                    episode_counter = 0
                    found = False
                    
                    for season_row in all_seasons:
                        season_episodes = season_row['episodes'] or 12  # Default to 12 if unknown
                        season_num = season_row['season_number']
                        season_status = season_row['status']
                        
                        # For ongoing or upcoming series with low episode counts, be more lenient
                        # and assume episodes belong to the current season
                        if season_status in (AnimeStatus.ONGOING, AnimeStatus.UPCOMING) and season_episodes < 12:
                            # For ongoing/upcoming series with less than 12 episodes registered,
                            # assume this is the current season and accept higher episode numbers
                            if episode <= episode_counter + 50:  # Reasonable limit for a single season
                                calculated_season = season_num
                                calculated_episode = episode - episode_counter
                                found = True
                                break
                        elif episode <= episode_counter + season_episodes:
                            # Episode falls within this season
                            calculated_season = season_num
                            calculated_episode = episode - episode_counter
                            found = True
                            break
                        
                        episode_counter += season_episodes
                    
                    # If episode number is beyond all known seasons, create new season
                    if not found:
                        # Get the last season number
                        last_season = max(s['season_number'] for s in all_seasons) if all_seasons else 0
                        remaining_episodes = episode - episode_counter
                        
                        if remaining_episodes > 0:
                            # Start a new season
                            calculated_season = last_season + 1
                            calculated_episode = remaining_episodes
                
                episode_info = EpisodeInfo(
                    title=f"Episode {calculated_episode}",
                    season=calculated_season if calculated_season is not None else 1,
                    episode=calculated_episode,
                    parent_id=str(parent_id),
                    year=row['year']
                )
                
                self._search_cache[episode_key] = episode_info
                return episode_info
        finally:
            self._return_connection(conn)
        return None

    def _rank_candidates(self, query_title: str, candidates: list, conn=None, prepared_query=None) -> list:
        """
        Re-rank candidates based on title similarity to prefer better matches.
        Prioritizes:
        1. Exact matches
        2. Longer title matches (more words in common)
        3. Higher proportion of query words present
        """
        if not candidates:
            return candidates

        scored_candidates = []
        best_candidate_by_id = {}
        best_score_by_id = {}
        for candidate in candidates:
            candidate_id = self._row_or_dict_value(candidate, 'id')
            if candidate_id is None:
                continue
            bm25_score = candidate['score'] if 'score' in candidate.keys() else None
            if conn is not None:
                combined_score = self._best_score_for_anime_id(
                    conn,
                    query_title,
                    candidate_id,
                    bm25_score=bm25_score,
                    prepared_query=prepared_query,
                )
            else:
                resolved_prepared_query = prepared_query or self._prepare_similarity_query(query_title)
                combined_score = self._score_title_match_prepared(
                    resolved_prepared_query,
                    candidate['title'] if 'title' in candidate else '',
                    bm25_score=bm25_score,
                    candidate_year=self._row_or_dict_value(candidate, 'year'),
                )
            if candidate_id not in best_score_by_id or combined_score > best_score_by_id[candidate_id]:
                best_score_by_id[candidate_id] = combined_score
                best_candidate_by_id[candidate_id] = candidate

        for candidate_id, candidate in best_candidate_by_id.items():
            scored_candidates.append((best_score_by_id[candidate_id], candidate))
        
        # Sort by combined score (descending) and return candidates only
        scored_candidates.sort(key=lambda x: x[0], reverse=True)
        if conn is not None:
            ranked_rows = []
            for _score, candidate in scored_candidates:
                row = conn.execute(
                    "SELECT * FROM anime_synonym_view WHERE id = ? ORDER BY year DESC LIMIT 1",
                    (candidate['id'],),
                ).fetchone()
                if row is not None:
                    ranked_rows.append(row)
            return ranked_rows
        return [candidate for score, candidate in scored_candidates]

    def _best_score_for_anime_id(
        self,
        conn,
        query_title: str,
        anime_id: int,
        bm25_score: Optional[float] = None,
        prepared_query=None,
    ) -> float:
        rows = conn.execute(
            "SELECT * FROM anime_synonym_view WHERE id = ?",
            (anime_id,),
        ).fetchall()
        best_score = 0.0
        seen_titles = set()
        resolved_prepared_query = prepared_query or self._prepare_similarity_query(query_title)
        for row in rows:
            candidate_year = self._row_or_dict_value(row, 'year')
            for candidate_text in (self._row_or_dict_value(row, 'title'), self._row_or_dict_value(row, 'synonym')):
                if not candidate_text or candidate_text in seen_titles:
                    continue
                seen_titles.add(candidate_text)
                score = self._score_title_match_prepared(
                    resolved_prepared_query,
                    candidate_text,
                    bm25_score=bm25_score,
                    candidate_year=candidate_year,
                )
                if score > best_score:
                    best_score = score
        return best_score

    def _prepare_similarity_query(self, title: str):
        query_normalized = self._normalize_title_for_similarity(title)
        query_compact = self._normalize_title_for_similarity(title, compact=True)
        query_words = frozenset(query_normalized.split()) if query_normalized else frozenset()
        return query_normalized, query_compact, query_words

    @staticmethod
    def _row_or_dict_value(row, key: str):
        if row is None:
            return None
        if isinstance(row, dict):
            return row.get(key)
        try:
            return row[key] if key in row.keys() else None
        except Exception:
            return None

    def _score_title_match(
        self,
        query_title: str,
        candidate_title: str,
        bm25_score: Optional[float] = None,
        candidate_year: Optional[int] = None,
    ) -> float:
        prepared_query = self._prepare_similarity_query(query_title)
        return self._score_title_match_prepared(
            prepared_query,
            candidate_title,
            bm25_score=bm25_score,
            candidate_year=candidate_year,
        )

    def _score_title_match_prepared(
        self,
        prepared_query,
        candidate_title: str,
        bm25_score: Optional[float] = None,
        candidate_year: Optional[int] = None,
    ) -> float:
        """Score how well a candidate title matches a pre-normalized query."""
        query_normalized, query_compact, query_words = prepared_query
        candidate_normalized = self._normalize_title_for_similarity(candidate_title)
        candidate_compact = self._normalize_title_for_similarity(candidate_title, compact=True)

        if not query_normalized or not candidate_normalized:
            return 0.0

        if query_normalized == candidate_normalized or query_compact == candidate_compact:
            return self.EXACT_MATCH_SCORE

        ratio = max(
            self._similarity_ratio(query_normalized, candidate_normalized),
            self._similarity_ratio(query_compact, candidate_compact),
        )
        candidate_words = set(candidate_normalized.split())
        coverage = (len(query_words & candidate_words) / len(query_words)) if query_words else 0.0
        quality = min(1.0, (ratio * 0.7) + (coverage * 0.3))

        if bm25_score is not None:
            quality = min(1.0, quality + min(0.05, max(0.0, -float(bm25_score)) / 100.0))

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

        # Apply year-based recency bonus (newer titles preferred)
        year_bonus = 0.0
        try:
            if candidate_year is not None:
                current_year = datetime.datetime.now().year
                age = current_year - int(candidate_year)
                if age < 0:
                    age = 0
                if age < self.YEAR_RECENCY_DECAY_YEARS:
                    year_bonus = (
                        self.YEAR_RECENCY_MAX_BONUS
                        * (self.YEAR_RECENCY_DECAY_YEARS - age)
                        / float(self.YEAR_RECENCY_DECAY_YEARS)
                    )
        except Exception:
            year_bonus = 0.0

        return min(base_score + year_bonus, max_score)

    def _similarity_ratio(self, left: str, right: str) -> float:
        if rapidfuzz_fuzz is not None:
            return rapidfuzz_fuzz.ratio(left, right) / 100.0
        return SequenceMatcher(None, left, right).ratio()

    def _normalize_title_for_similarity(self, title: str, compact: bool = False) -> str:
        normalized = (title or '').casefold()
        normalized = re.sub(r'[^\w\s]', ' ', normalized)
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        if compact:
            return normalized.replace(' ', '')
        return normalized

    def _looks_like_acronym_query(self, title: str) -> bool:
        tokens = re.findall(r'\w+', (title or '').casefold())
        if len(tokens) < 3:
            return False
        return all(len(token) == 1 for token in tokens)

    def _build_fts_query(self, title: str) -> str:
        """Build FTS5 query from title for fuzzy search"""
        # Normalize and tokenize
        normalized = title.lower()
        normalized = re.sub(r'\s+', ' ', normalized).strip()

        # Remove punctuation and special characters
        normalized = re.sub(r'[^\w\s]', ' ', normalized)

        if not self._looks_like_acronym_query(title):
            # Remove words like "the", "a", "an" and other common English stop words
            normalized = re.sub(r'\b(the|a|an|and|of|in|to|for|with)\b', '', normalized)

            # DO NOT remove "no" as it's semantically important in Japanese titles
            # Only remove other less important Japanese particles
            normalized = re.sub(r'\b(wa|ni|de|o|ka|ga|e|kara|made|yori|to|ya)\b', '', normalized)

        # Split into words and build FTS5 query
        words = [w for w in normalized.split() if w]  # Filter empty strings
        
        if not words:
            return title.lower()
        
        # Use simple space-separated words with wildcards
        # FTS5 will treat consecutive words as a phrase match by default
        normalized = ' '.join([f'{word}*' for word in words])

        return normalized
    
    def refresh_data(self) -> None:
        """Invalidate cache and immediately reload/refresh the data"""
        logging.info("Refreshing anime database...")
        self._db_loaded_once = False
        self._db_loaded_until_ts = None
        previous_duration = self.cache_duration
        self.set_cache_expiry(0)
        self._search_cache.clear()
        self.load_database()

        # Restore configured TTL so refreshed data does not remain immediately expired.
        if self.cache_duration != previous_duration:
            self.set_cache_duration(previous_duration)

        try:
            self._db_loaded_until_ts = int(self.get_cache_expiry().timestamp())
        except Exception:
            self._db_loaded_until_ts = int(time.time()) + int(self.cache_duration.total_seconds())
        logging.info("Anime database refreshed successfully")