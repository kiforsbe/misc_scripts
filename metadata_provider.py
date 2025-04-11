from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
import os
import logging
from pathlib import Path

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
    genres: List[str] = None
    tags: List[str] = None
    status: Optional[str] = None
    total_episodes: Optional[int] = None
    total_seasons: Optional[int] = None
    sources: List[str] = None
    plot: Optional[str] = None
    
    def __post_init__(self):
        self.genres = self.genres or []
        self.tags = self.tags or []
        self.sources = self.sources or []

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

class BaseMetadataProvider(ABC):
    """Base class for metadata providers with common functionality"""
    
    def __init__(self, cache_dir: str, cache_duration: timedelta = timedelta(days=7)):
        self.cache_dir = os.path.join(os.path.expanduser("~"), ".video_metadata_cache", cache_dir)
        self.cache_duration = cache_duration
        self.ensure_cache_dir()
    
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
    def find_title(self, title: str, year: Optional[int] = None) -> Optional[TitleInfo]:
        """Find title information for either a movie or TV show"""
        pass
    
    @abstractmethod
    def get_episode_info(self, parent_id: str, season: int, episode: int) -> Optional[EpisodeInfo]:
        """Get episode information if the title is a TV show"""
        pass

class MetadataManager:
    """Proxy class to handle multiple metadata providers"""
    
    def __init__(self, providers: List[BaseMetadataProvider]):
        self.providers = providers
    
    def find_title(self, title: str, year: Optional[int] = None) -> Tuple[Optional[TitleInfo], Optional[BaseMetadataProvider]]:
        """Try all providers and return the best match and the provider that found it"""
        best_match = None
        best_provider = None
        best_score = 0
        
        for provider in self.providers:
            match = provider.find_title(title, year)
            if match:
                # Calculate a relevance score based on exact matches and year
                score = 0
                if match.title.lower() == title.lower():
                    score += 100
                elif title.lower() in match.title.lower():
                    score += 80
                
                if year and match.year and match.year == year:
                    score += 50
                elif year and match.year and abs(match.year - year) <= 1:
                    score += 30
                
                if score > best_score:
                    best_score = score
                    best_match = match
                    best_provider = provider
        
        return best_match, best_provider
    
    def get_episode_info(self, provider: BaseMetadataProvider, parent_id: str, season: int, episode: int) -> Optional[EpisodeInfo]:
        """Get episode info from a specific provider"""
        return provider.get_episode_info(parent_id, season, episode)