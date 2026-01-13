from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Optional, Any, Tuple
from datetime import datetime, timedelta
from functools import lru_cache
import os
import logging
import requests

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
        self._persist_cache_duration()
        return self.get_cache_expiry()

    def set_cache_expiry(self, days: int) -> datetime:
        """Set cache TTL using a whole number of days; returns new expiry timestamp."""
        if days is None or days < 0:
            raise ValueError("days must be a non-negative integer")
        return self.set_cache_duration(timedelta(days=days))

    def get_cache_expiry(self) -> datetime:
        """Return the timestamp when the cache should be considered stale."""
        return datetime.now() + self.cache_duration

    def cache_summary(self) -> dict:
        """Lightweight cache metadata for UIs/CLI."""
        return {
            "cache_dir": self.cache_dir,
            "cache_duration_seconds": self.cache_duration.total_seconds(),
            "cache_expiry": self.get_cache_expiry(),
        }
    
    # Hooks for subclasses to persist/load cache duration using their storage (e.g., SQLite)
    def _persist_cache_duration(self) -> None:  # pragma: no cover - override in subclasses
        return

    def _load_cache_duration(self) -> None:  # pragma: no cover - override in subclasses
        return
    
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