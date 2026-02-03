from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Optional, Any, Tuple
from datetime import datetime, timedelta
from functools import lru_cache
import os
import logging
import requests
import json
import sqlite3
import time
from datetime import datetime, timedelta

@dataclass
class TitleInfo:
    """Common structure for both movies and TV shows"""
    id: str
    title: str
    type: str  # 'movie', 'tv', 'anime_movie', 'anime_series'
    year: Optional[int] = None
    start_year: Optional[int] = None
    end_year: Optional[int] = None
    rating: Optional[float] = None
    votes: Optional[int] = None
    genres: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    status: Optional[str] = None
    total_episodes: Optional[int] = None
    total_seasons: Optional[int] = None
    sources: List[str] = field(default_factory=list)
    plot: Optional[str] = None

@dataclass
class EpisodeInfo:
    title: str
    season: int
    episode: int
    parent_id: str  # Reference to parent TitleInfo
    year: Optional[int] = None
    rating: Optional[float] = None
    votes: Optional[int] = None
    plot: Optional[str] = None
    air_date: Optional[str] = None
    
    def __post_init__(self):
        if not self.title:
            self.title = f"Episode {self.episode}"

@dataclass
class MatchResult:
    """Container for a match result with quality score"""
    info: Optional[TitleInfo]
    score: float  # Score between 0-100
    provider_weight: float = 1.0  # Provider-specific weight for multi-provider scenarios
    
    @property
    def weighted_score(self) -> float:
        """Get the score weighted by provider weight"""
        return self.score * self.provider_weight

class BaseMetadataProvider(ABC):
    """Base class for metadata providers with common functionality"""
    
    def __init__(self, cache_dir: str, cache_duration: timedelta = timedelta(days=7), provider_weight: float = 1.0):
        self.cache_dir = os.path.join(os.path.expanduser("~"), ".video_metadata_cache", cache_dir)
        self.cache_duration = cache_duration
        self.provider_weight = provider_weight
        self.ensure_cache_dir()

    def set_cache_duration(self, duration: timedelta) -> datetime:
        """Set cache TTL using a timedelta; returns the new expiry timestamp."""
        if duration < timedelta(0):
            raise ValueError("cache duration must be non-negative")
        self.cache_duration = duration
        # Persist both the configured duration and the computed expiry (now + duration)
        self._persist_cache_duration()
        return self.get_cache_expiry()

    def set_cache_expiry(self, days: int) -> datetime:
        """Set cache TTL using a whole number of days; returns new expiry timestamp."""
        if days is None or days < 0:
            raise ValueError("days must be a non-negative integer")
        return self.set_cache_duration(timedelta(days=days))

    def get_cache_expiry(self) -> datetime:
        """Return the timestamp when the cache should be considered stale."""
        # 1) If an explicit persisted expiry exists (from set_cache_expiry), prefer it
        persisted = getattr(self, "_persisted_cache_expiry", None)
        if persisted:
            return persisted

        # 2) Prefer computing provider expiry as the earliest expiry among the
        # configured datasets in the provider DB. Prefer `expires_at` (new)
        # column, fall back to `updated` for existing rows for backward
        # compatibility.
        try:
            db_path = getattr(self, "_db_path", None)
            datasets = getattr(self, "CACHE_EXPIRY_DATASETS", None)
            if db_path and datasets and os.path.exists(db_path):
                placeholders = ','.join('?' for _ in datasets)
                conn = sqlite3.connect(db_path, timeout=5.0)
                # Try to read the new column first; if it doesn't exist the
                # query will raise and we'll fall back to reading `updated`.
                try:
                    cur = conn.execute(f"SELECT MIN(expires_at) FROM data_version WHERE dataset IN ({placeholders})", tuple(datasets))
                    row = cur.fetchone()
                    if row and row[0]:
                        try:
                            return datetime.fromtimestamp(int(row[0]))
                        except Exception:
                            pass
                except Exception:
                    # Older DBs may not have `expires_at` yet — try `updated`.
                    try:
                        cur = conn.execute(f"SELECT MIN(updated) FROM data_version WHERE dataset IN ({placeholders})", tuple(datasets))
                        row = cur.fetchone()
                        if row and row[0]:
                            try:
                                return datetime.fromtimestamp(int(row[0]))
                            except Exception:
                                pass
                    except Exception:
                        pass
                finally:
                    conn.close()
        except Exception:
            pass

        # 3) Default fallback: now + configured duration
        return datetime.now() + self.cache_duration

    def cache_summary(self) -> dict:
        """Lightweight cache metadata for UIs/CLI."""
        expiry = self.get_cache_expiry()
        now = datetime.now()
        time_to_expiry = max(0.0, (expiry - now).total_seconds())
        return {
            "cache_dir": self.cache_dir,
            # Configured TTL (informational)
            "cache_duration_seconds": self.cache_duration.total_seconds(),
            # Remaining time until expiry (decreases as expiry approaches)
            "cache_time_to_expiry_seconds": time_to_expiry,
            "cache_expiry": expiry,
        }
    
    # Hooks for subclasses to persist/load cache duration using their storage (e.g., SQLite)
    def _persist_cache_duration(self) -> None:  # pragma: no cover - override in subclasses
        # Persist cache expiry to provider DB `data_version` rows for the
        # datasets listed in `self.CACHE_EXPIRY_DATASETS` (provider should set).
        # This writes the expiry timestamp for each dataset so the DB remains
        # authoritative about when each source expires.
        expiry_ts = int((datetime.now() + self.cache_duration).timestamp())
        db_path = getattr(self, "_db_path", None)
        datasets = getattr(self, "CACHE_EXPIRY_DATASETS", None)

        if not db_path or not datasets:
            logging.debug("No _db_path or CACHE_EXPIRY_DATASETS defined; keeping expiry in-memory only")
            try:
                self._persisted_cache_expiry = datetime.fromtimestamp(expiry_ts)
            except Exception:
                self._persisted_cache_expiry = None
            return

        try:
            conn = sqlite3.connect(db_path, timeout=5.0)
            # Ensure schema has required columns. Add `expires_at` (new), and
            # keep `updated` for backward compatibility. If we added
            # `expires_at`, migrate existing `updated` values into it.
            cols = [r[1] for r in conn.execute("PRAGMA table_info(data_version)").fetchall()]
            if 'expires_at' not in cols:
                try:
                    conn.execute("ALTER TABLE data_version ADD COLUMN expires_at INTEGER")
                    # Migrate existing `updated` values into `expires_at` when
                    # `expires_at` is NULL so old rows continue to represent
                    # expiry semantics.
                    try:
                        conn.execute("UPDATE data_version SET expires_at = updated WHERE expires_at IS NULL")
                    except Exception:
                        pass
                except Exception:
                    pass
            if 'default_ttl' not in cols:
                try:
                    conn.execute("ALTER TABLE data_version ADD COLUMN default_ttl INTEGER")
                except Exception:
                    pass
            if 'last_modified' not in cols:
                try:
                    conn.execute("ALTER TABLE data_version ADD COLUMN last_modified INTEGER")
                except Exception:
                    pass

            now_ts = int(time.time())
            ttl_days = int(self.cache_duration.days)
            for ds in datasets:
                conn.execute(
                    "INSERT OR REPLACE INTO data_version (dataset, expires_at, default_ttl, last_modified) VALUES (?, ?, ?, ?)",
                    (ds, expiry_ts, ttl_days, now_ts),
                )
            conn.commit()
            conn.close()
            # Persisted expiry for provider is the earliest expiry among datasets
            try:
                self._persisted_cache_expiry = datetime.fromtimestamp(expiry_ts)
            except Exception:
                self._persisted_cache_expiry = None
        except Exception as e:
            logging.debug(f"Could not persist cache expiry to provider DB: {e}")
            try:
                self._persisted_cache_expiry = datetime.fromtimestamp(expiry_ts)
            except Exception:
                self._persisted_cache_expiry = None

    def _load_cache_duration(self) -> None:  # pragma: no cover - override in subclasses
        # Load persisted expiry timestamps for all datasets in
        # `self.CACHE_EXPIRY_DATASETS` and set the provider expiry to the
        # earliest (minimum) timestamp — the first source to expire triggers
        # a provider refresh.
        self._persisted_cache_expiry = None
        db_path = getattr(self, "_db_path", None)
        datasets = getattr(self, "CACHE_EXPIRY_DATASETS", None)
        if not db_path or not datasets:
            logging.debug("No _db_path or CACHE_EXPIRY_DATASETS defined for loading cache expiry")
            return

        try:
            conn = sqlite3.connect(db_path, timeout=5.0)
            expiries = []
            ttls = []
            last_mods = []
            for ds in datasets:
                # Read both `expires_at` and `updated` so we can prefer the new
                # column but fall back to the old one when necessary.
                cur = conn.execute("SELECT expires_at, updated, default_ttl, last_modified FROM data_version WHERE dataset = ? LIMIT 1", (ds,))
                row = cur.fetchone()
                if row:
                    # prefer expires_at (row[0]) and fall back to updated (row[1])
                    expires_val = None
                    if row[0] is not None:
                        expires_val = row[0]
                    elif row[1] is not None:
                        expires_val = row[1]
                    if expires_val:
                        try:
                            expiries.append(int(expires_val))
                        except Exception:
                            pass
                    if len(row) > 2 and row[2] is not None:
                        try:
                            ttls.append(int(row[2]))
                        except Exception:
                            pass
                    if len(row) > 3 and row[3] is not None:
                        try:
                            last_mods.append(int(row[3]))
                        except Exception:
                            pass
            conn.close()
            if ttls:
                # If DB supplies a default_ttl for any dataset, use the smallest
                # as the provider-level default
                try:
                    self.cache_duration = timedelta(days=min(ttls))
                except Exception:
                    pass
            if expiries:
                min_ts = min(expiries)
                try:
                    self._persisted_cache_expiry = datetime.fromtimestamp(min_ts)
                except Exception:
                    self._persisted_cache_expiry = None
            if last_mods:
                try:
                    self._data_version_last_modified = datetime.fromtimestamp(max(last_mods))
                except Exception:
                    self._data_version_last_modified = None
        except Exception as e:
            logging.debug(f"Could not read cache expiry from provider DB: {e}")
    
    def ensure_cache_dir(self):
        """Create cache directory if it doesn't exist"""
        os.makedirs(self.cache_dir, exist_ok=True)
    
    def is_cache_valid(self, cache_file: str) -> bool:
        """Check if cached data is still valid"""
        if not os.path.exists(cache_file):
            return False
        mtime = datetime.fromtimestamp(os.path.getmtime(cache_file))
        return datetime.now() - mtime < self.cache_duration
    
    @abstractmethod
    def find_title(self, title: str, year: Optional[int] = None) -> Optional[MatchResult]:
        """Find title information for either a movie or TV show"""
        pass
    
    @abstractmethod
    def get_episode_info(self, parent_id: str, season: int, episode: int) -> Optional[EpisodeInfo]:
        """Get episode information if the title is a TV show"""
        pass
    
    @abstractmethod
    def invalidate_cache(self) -> None:
        """Invalidate the current cache, forcing a refresh on next access"""
        pass
    
    @abstractmethod
    def refresh_data(self) -> None:
        """Invalidate cache and immediately reload/refresh the data"""
        pass

    def _download_with_resume(self, url: str, target_file: str, temp_suffix: str = '.download') -> bool:
        """
        Download a file with resume capability and atomic write.
        
        Args:
            url: The URL to download from
            target_file: The final path where the file should be saved
            temp_suffix: Suffix for temporary files during download
            
        Returns:
            bool: True if download was successful, False otherwise
        """
        temp_file = target_file + temp_suffix
        headers = {}
        mode = 'wb'
        
        # Check if we can resume a previous download
        if os.path.exists(temp_file):
            temp_size = os.path.getsize(temp_file)
            headers['Range'] = f'bytes={temp_size}-'
            mode = 'ab'
        
        try:
            # Make the request with resume headers if applicable
            response = requests.get(url, stream=True, headers=headers)
            
            if response.status_code == 416:  # Range not satisfiable
                # File is already complete, just rename it
                os.rename(temp_file, target_file)
                return True
                
            response.raise_for_status()
            
            # Get total file size for validation
            total_size = int(response.headers.get('content-length', 0))
            if 'Range' in headers:
                total_size += temp_size
            
            with open(temp_file, mode) as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            # Verify file size
            if os.path.getsize(temp_file) != total_size and total_size > 0:
                raise ValueError("Downloaded file size doesn't match expected size")
            
            # Atomic move
            os.replace(temp_file, target_file)
            return True
            
        except Exception as e:
            logging.error(f"Error downloading {url}: {str(e)}")
            # Keep partial download for future resume
            return False

    @staticmethod
    def safe_int(value: Any) -> Optional[int]:
        """Safely convert a value to int, returning None on failure"""
        if value is None:
            return None
        try:
            if isinstance(value, str):
                value = value.strip().lower()
                if value in ('', 'na', 'n/a', 'none', '\\n'):
                    return None
            return int(float(value))  # Handle both integer and float strings
        except (ValueError, TypeError):
            return None

class MetadataManager:
    """Proxy class to handle multiple metadata providers with caching"""
    
    def __init__(self, providers: List[BaseMetadataProvider]):
        self.providers = providers
        # Cache for find_title results - key is (title, year)
        self._title_cache = {}
    
    def find_title(self, title: str, year: Optional[int] = None) -> Tuple[Optional[TitleInfo], Optional[BaseMetadataProvider]]:
        """Try all providers and return the best match and the provider that found it (cached)"""
        # Check cache first
        cache_key = (title, year)
        if cache_key in self._title_cache:
            return self._title_cache[cache_key]
        
        best_result: Optional[MatchResult] = None
        best_provider = None
        
        for provider in self.providers:
            result = provider.find_title(title, year)
            if result and result.info:  # Make sure we have both a result and title info
                if not best_result or result.weighted_score > best_result.weighted_score:
                    best_result = result
                    best_provider = provider
        
        # Cache the result (even if None)
        result_tuple = (best_result.info if best_result else None, best_provider)
        self._title_cache[cache_key] = result_tuple
        return result_tuple
    
    def get_episode_info(self, provider: BaseMetadataProvider, parent_id: str, season: int, episode: int) -> Optional[EpisodeInfo]:
        """Get episode info from a specific provider"""
        return provider.get_episode_info(parent_id, season, episode)