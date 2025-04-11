import os
import json
import logging
import requests
from typing import Optional
from rapidfuzz import fuzz, process
from metadata_provider import BaseMetadataProvider, TitleInfo, EpisodeInfo

class AnimeDataProvider(BaseMetadataProvider):
    ANIME_DB_URL = "https://raw.githubusercontent.com/manami-project/anime-offline-database/master/anime-offline-database.json"
    
    def __init__(self):
        super().__init__('anime')
        self.anime_db = None
        self.title_cache = {}
    
    def load_database(self) -> None:
        """Load the anime database into memory, downloading if needed"""
        if self.anime_db is not None:
            return
        
        cache_file = os.path.join(self.cache_dir, "anime-offline-database.json")
        
        try:
            if self.is_cache_valid(cache_file):
                logging.info("Loading cached anime database")
                with open(cache_file, 'r', encoding='utf-8') as f:
                    self.anime_db = json.load(f)
                    return
            
            logging.info("Downloading fresh anime database...")
            response = requests.get(self.ANIME_DB_URL)
            response.raise_for_status()
            
            self.anime_db = response.json()
            
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.anime_db, f, ensure_ascii=False, indent=2)
            
        except Exception as e:
            logging.error(f"Error loading anime database: {e}")
            if os.path.exists(cache_file):
                try:
                    with open(cache_file, 'r', encoding='utf-8') as f:
                        self.anime_db = json.load(f)
                except Exception as cache_e:
                    logging.error(f"Error loading cached database: {cache_e}")
                    self.anime_db = None
    
    def find_title(self, title: str, year: Optional[int] = None) -> Optional[TitleInfo]:
        """Find title information for either a movie or TV show"""
        if not self.anime_db:
            self.load_database()
        
        if not self.anime_db or 'data' not in self.anime_db:
            return None
        
        # Create title cache if not already done
        if not self.title_cache:
            for entry in self.anime_db['data']:
                # Check all possible titles (synonyms, english, etc)
                all_titles = [entry['title']] + entry.get('synonyms', [])
                if entry.get('title', {}).get('english'):
                    all_titles.append(entry['title']['english'])
                
                for t in all_titles:
                    if t:
                        self.title_cache[t] = entry
        
        # Find the best match using fuzzy string matching
        best_match = process.extractOne(
            title,
            list(self.title_cache.keys()),
            scorer=fuzz.ratio,
            score_cutoff=80
        )
        
        if best_match:
            title_matched, score = best_match
            entry = self.title_cache[title_matched]
            
            # If year is provided, verify it matches
            if year:
                entry_year = entry.get('animeSeason', {}).get('year')
                if entry_year and abs(int(entry_year) - year) > 1:  # Allow 1 year difference
                    return None
            
            # Map anime type to our unified type system
            media_type = 'anime_movie' if entry.get('type') == 'MOVIE' else 'anime_series'
            
            return TitleInfo(
                id=entry.get('sources', [''])[0],  # Use first source as ID
                title=entry['title'],
                type=media_type,
                year=entry.get('animeSeason', {}).get('year'),
                status=entry.get('status'),
                total_episodes=entry.get('episodes'),
                tags=entry.get('tags', []),
                sources=entry.get('sources', [])
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