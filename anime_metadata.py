import os
import json
import logging
import requests
from typing import Optional, Dict, List, Tuple
from rapidfuzz import fuzz, process
from tqdm import tqdm
from metadata_provider import BaseMetadataProvider, TitleInfo, EpisodeInfo, MatchResult
from dataclasses import dataclass
from collections import defaultdict

@dataclass
class TitleEntry:
    """Helper class to store title information with relevance scores"""
    entry_id: int  # Index in anime_db['data']
    title_type: str  # 'main', 'english', 'synonym'
    relevance: float  # Base relevance score for this title type

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
        super().__init__('anime', provider_weight=1.0)  # Anime DB gets full weight for anime content
        self.anime_db = None
        self.title_index: Dict[str, List[TitleEntry]] = defaultdict(list)
        self._db_loaded = False
    
    def load_database(self) -> None:
        """Load the anime database into memory, downloading if needed"""
        if self.anime_db is not None:
            logging.info("Using already loaded anime database from memory")
            return
        
        cache_file = os.path.join(self.cache_dir, "anime-offline-database.json")
        temp_cache = cache_file + '.download'
        
        for attempt in range(self.MAX_RETRIES):
            try:
                # First try to load from cache if valid
                if self.is_cache_valid(cache_file) and self._verify_file_integrity(cache_file):
                    logging.info("Loading cached anime database...")
                    with open(cache_file, 'r', encoding='utf-8') as f:
                        data = f.read()
                        try:
                            self.anime_db = json.loads(data)
                            logging.info("Successfully loaded anime database from cache")
                            return
                        except json.JSONDecodeError:
                            logging.warning("Cached anime database is corrupted, will download fresh copy")
                
                logging.info(f"Downloading fresh anime database (attempt {attempt + 1}/{self.MAX_RETRIES})...")
                
                # Use download with resume capability
                if self._download_with_resume(self.ANIME_DB_URL, cache_file):
                    # Verify the downloaded file
                    if not self._verify_file_integrity(cache_file):
                        logging.error("Downloaded anime database is corrupted")
                        if os.path.exists(cache_file):
                            os.remove(cache_file)
                        continue
                    
                    # Try to parse the JSON
                    try:
                        with open(cache_file, 'r', encoding='utf-8') as f:
                            self.anime_db = json.load(f)
                        logging.info("Successfully loaded fresh anime database")
                        return
                    except json.JSONDecodeError:
                        logging.error("Downloaded anime database has invalid JSON")
                        if os.path.exists(cache_file):
                            os.remove(cache_file)
                        continue
                
            except Exception as e:
                logging.error(f"Error during anime database download/load (attempt {attempt + 1}): {str(e)}")
                if attempt < self.MAX_RETRIES - 1:
                    logging.info("Retrying...")
                    continue
        
        # If all attempts failed, try to load from expired cache as last resort
        if os.path.exists(cache_file):
            try:
                logging.warning("All download attempts failed. Attempting to load expired cache as fallback...")
                with open(cache_file, 'r', encoding='utf-8') as f:
                    self.anime_db = json.load(f)
                logging.info("Successfully loaded anime database from expired cache")
                return
            except Exception as cache_e:
                logging.error(f"Error loading cached database: {str(cache_e)}")
        
        self.anime_db = None
        raise RuntimeError("Failed to load anime database after multiple attempts")
    
    def _build_title_index(self) -> None:
        """Build an efficient index of titles and their variants"""
        if not self.anime_db or 'data' not in self.anime_db:
            return
            
        self.title_index.clear()
        
        for idx, entry in enumerate(self.anime_db['data']):
            # Add main title
            main_title = entry.get('title')
            if main_title:
                self.title_index[main_title].append(
                    TitleEntry(idx, 'main', self.TITLE_WEIGHTS['main'])
                )
            
            # Add synonyms
            for synonym in entry.get('synonyms', []):
                if synonym and synonym != main_title:
                    self.title_index[synonym].append(
                        TitleEntry(idx, 'synonym', self.TITLE_WEIGHTS['synonym'])
                    )
    
    def _get_entry_by_id(self, entry_id: int) -> Optional[dict]:
        """Get anime entry by its index in the database"""
        if not self.anime_db or 'data' not in self.anime_db:
            return None
        try:
            return self.anime_db['data'][entry_id]
        except IndexError:
            return None
    
    def find_title(self, title: str, year: Optional[int] = None) -> Optional[MatchResult]:
        """Find title information for either a movie or TV show"""
        if not self.anime_db:
            self.load_database()
        
        if not self.anime_db or 'data' not in self.anime_db:
            return None
            
        # Build title index if not already done
        if not self._db_loaded:
            self._build_title_index()
            self._db_loaded = True
        
        # First try exact matches with weighted relevance
        best_match = None
        best_score = 0
        best_entry = None
        
        # Look for exact matches first
        if title in self.title_index:
            for title_entry in self.title_index[title]:
                entry = self._get_entry_by_id(title_entry.entry_id)
                if not entry:
                    continue
                
                # Calculate base score from title type
                score = 100 * title_entry.relevance
                
                # Add year match bonus if applicable
                if year:
                    entry_year = self.safe_int(entry.get('animeSeason', {}).get('year'))
                    if entry_year:
                        if entry_year == year:
                            score += 20
                        elif abs(entry_year - year) <= 1:
                            score += 10
                
                # Add bonus for number of sources (+5 per source up to +15)
                source_bonus = min(15, len(entry.get('sources', [])) * 5)
                score += source_bonus
                
                # Add bonus for having episodes count (+10)
                episodes_count = self.safe_int(entry.get('episodes'))
                if episodes_count:
                    score += 10
                
                if score > best_score:
                    best_score = score
                    best_entry = entry
        
        # If no good exact match, try fuzzy matching
        if not best_entry:
            matches = process.extract(
                title,
                list(self.title_index.keys()),
                scorer=fuzz.ratio,
                limit=5
            )
            
            for matched_title, fuzzy_score, _ in matches:
                if fuzzy_score < 80:  # Minimum similarity threshold
                    continue
                    
                for title_entry in self.title_index[matched_title]:
                    entry = self._get_entry_by_id(title_entry.entry_id)
                    if not entry:
                        continue
                    
                    # Calculate score based on fuzzy match and title type
                    score = fuzzy_score * title_entry.relevance
                    
                    # Add year match bonus
                    if year:
                        entry_year = self.safe_int(entry.get('animeSeason', {}).get('year'))
                        if entry_year:
                            if entry_year == year:
                                score += 20
                            elif abs(entry_year - year) <= 1:
                                score += 10
                    
                    # Add source and episode count bonuses
                    source_bonus = min(15, len(entry.get('sources', [])) * 5)
                    score += source_bonus
                    
                    episodes_count = self.safe_int(entry.get('episodes'))
                    if episodes_count:
                        score += 10
                    
                    if score > best_score:
                        best_score = score
                        best_entry = entry
        
        if best_entry:
            # Map anime type to our unified type system
            media_type = 'movie' if best_entry.get('type') == 'MOVIE' else 'tvSeries'
            
            match_info = TitleInfo(
                id=best_entry.get('sources', [''])[0],  # Use first source as ID
                title=best_entry['title'],
                type=media_type,
                year=self.safe_int(best_entry.get('animeSeason', {}).get('year')),
                status=best_entry.get('status'),
                total_episodes=self.safe_int(best_entry.get('episodes')),
                tags=best_entry.get('tags', []),
                sources=best_entry.get('sources', [])
            )
            
            return MatchResult(
                info=match_info,
                score=best_score,
                provider_weight=self.provider_weight
            )
        
        return None
    
    def get_episode_info(self, parent_id: str, season: int, episode: int) -> Optional[EpisodeInfo]:
        """Get episode information if the title is a TV show"""
        if not self.anime_db:
            self.load_database()
        
        # Find the anime by source ID
        anime_entry = next(
            (entry for entry in self.anime_db['data'] if parent_id in entry.get('sources', [])),
            None
        )
        
        if anime_entry:
            return EpisodeInfo(
                title=f"{anime_entry['title']} - Episode {episode}",  # Anime databases typically don't have episode titles
                season=season,
                episode=episode,
                parent_id=parent_id,
                year=anime_entry.get('animeSeason', {}).get('year')
            )
        
        return None