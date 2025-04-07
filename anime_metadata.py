import os
import json
import logging
import requests
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Any
from rapidfuzz import fuzz, process

class AnimeDBManager:
    ANIME_DB_URL = "https://raw.githubusercontent.com/manami-project/anime-offline-database/master/anime-offline-database.json"
    CACHE_DIR = os.path.join(os.path.expanduser("~"), ".video_metadata_cache", "anime")
    CACHE_DURATION = timedelta(days=7)  # Update database weekly
    
    def __init__(self):
        self.ensure_cache_dir()
        self.anime_db = None
        self.title_cache = {}
    
    def ensure_cache_dir(self):
        """Create cache directory if it doesn't exist"""
        os.makedirs(self.CACHE_DIR, exist_ok=True)
    
    def is_cache_valid(self) -> bool:
        """Check if cached database is still valid"""
        cache_file = os.path.join(self.CACHE_DIR, "anime-offline-database.json")
        if not os.path.exists(cache_file):
            return False
        
        mtime = datetime.fromtimestamp(os.path.getmtime(cache_file))
        return datetime.now() - mtime < self.CACHE_DURATION
    
    def load_database(self) -> None:
        """Load the anime database, downloading if needed"""
        if self.anime_db is not None:
            return
        
        cache_file = os.path.join(self.CACHE_DIR, "anime-offline-database.json")
        
        try:
            if self.is_cache_valid():
                logging.info("Loading cached anime database")
                with open(cache_file, 'r', encoding='utf-8') as f:
                    self.anime_db = json.load(f)
                    return
            
            logging.info("Downloading fresh anime database...")
            response = requests.get(self.ANIME_DB_URL)
            response.raise_for_status()
            
            self.anime_db = response.json()
            
            # Cache the database
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.anime_db, f, ensure_ascii=False, indent=2)
            
        except Exception as e:
            logging.error(f"Error loading anime database: {e}")
            # Try to load from cache even if expired
            if os.path.exists(cache_file):
                try:
                    with open(cache_file, 'r', encoding='utf-8') as f:
                        self.anime_db = json.load(f)
                except Exception as cache_e:
                    logging.error(f"Error loading cached database: {cache_e}")
                    self.anime_db = None
    
    def find_best_match(self, title: str, year: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """Find the best matching anime in the database using fuzzy string matching"""
        if not self.anime_db:
            self.load_database()
        
        if not self.anime_db or 'data' not in self.anime_db:
            return None
        
        # Create a list of tuples (title, anime_entry) if not already cached
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
            entry = self.title_cache[best_match[0]]
            # If year is provided, verify it matches
            if year:
                entry_year = entry.get('animeSeason', {}).get('year')
                if entry_year and abs(int(entry_year) - year) > 1:  # Allow 1 year difference
                    return None
            return entry
        
        return None
    
    def get_episode_info(self, anime_entry: Dict[str, Any], episode_number: int) -> Dict[str, Any]:
        """Get episode specific information"""
        return {
            'title': anime_entry['title'],
            'type': anime_entry.get('type', ''),
            'episodes': anime_entry.get('episodes', 0),
            'status': anime_entry.get('status', ''),
            'season': anime_entry.get('animeSeason', {}),
            'tags': anime_entry.get('tags', []),
            'sources': anime_entry.get('sources', []),
            'episode': episode_number
        }