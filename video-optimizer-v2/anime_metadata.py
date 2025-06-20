import os
import json
import logging
import requests
import pandas as pd
from typing import Optional, Dict, List, Tuple
from rapidfuzz import fuzz, process
from tqdm import tqdm
from metadata_provider import BaseMetadataProvider, TitleInfo, EpisodeInfo, MatchResult
from dataclasses import dataclass

@dataclass
class TitleEntry:
    """Helper class to store title information with relevance scores"""
    title: str
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
        super().__init__('anime', provider_weight=1.0)
        self._df = None  # Main dataframe
        self._title_index = None  # Title search index
        self._synonyms_df = None  # Synonyms dataframe
    
    def load_database(self) -> None:
        """Load the anime database into memory, downloading if needed"""
        if self._df is not None:
            logging.info("Using already loaded anime database from memory")
            return
        
        cache_file = os.path.join(self.cache_dir, "anime.parquet")
        synonyms_cache = os.path.join(self.cache_dir, "anime_synonyms.parquet")
        title_index_cache = os.path.join(self.cache_dir, "anime_title_index.parquet")
        temp_json = os.path.join(self.cache_dir, "temp_anime.json")
        
        # Load each cache file independently
        # Main database
        if self.is_cache_valid(cache_file):
            try:
                with tqdm(desc="Loading cached anime database", unit='B', unit_scale=True) as pbar:
                    self._df = pd.read_parquet(cache_file)
                    pbar.update(os.path.getsize(cache_file))
            except Exception as e:
                logging.warning(f"Failed to load cached database: {str(e)}")
                self._df = None

        # Synonyms
        if self.is_cache_valid(synonyms_cache):
            try:
                with tqdm(desc="Loading cached synonyms", unit='B', unit_scale=True) as pbar:
                    self._synonyms_df = pd.read_parquet(synonyms_cache)
                    pbar.update(os.path.getsize(synonyms_cache))
            except Exception as e:
                logging.warning(f"Failed to load cached synonyms: {str(e)}")
                self._synonyms_df = None

        # Title index
        if self.is_cache_valid(title_index_cache):
            try:
                with tqdm(desc="Loading cached title index", unit='B', unit_scale=True) as pbar:
                    title_df = pd.read_parquet(title_index_cache)
                    self._title_index = {
                        row.title: TitleEntry(title=row.title, title_type=row.title_type, relevance=row.relevance)
                        for row in title_df.itertuples()
                    }
                    pbar.update(os.path.getsize(title_index_cache))
            except Exception as e:
                logging.warning(f"Failed to load cached title index: {str(e)}")
                self._title_index = None

        # If any cache failed to load, proceed with full download
        if self._df is None or self._synonyms_df is None or self._title_index is None:
        
            # Download and process with retries
            for attempt in range(self.MAX_RETRIES):
                try:
                    logging.info(f"Downloading fresh anime database (attempt {attempt + 1}/{self.MAX_RETRIES})...")
                    
                    # Download JSON
                    response = requests.get(self.ANIME_DB_URL, stream=True)
                    total_size = int(response.headers.get('content-length', 0))
                    
                    with tqdm(total=total_size, desc="Downloading anime database", unit='B', unit_scale=True) as pbar:
                        with open(temp_json, 'wb') as f:
                            for chunk in response.iter_content(chunk_size=8192):
                                if chunk:
                                    f.write(chunk)
                                    pbar.update(len(chunk))
                    
                    # Parse JSON and convert to dataframes
                    with tqdm(desc="Processing anime database", unit='entries') as pbar:
                        with open(temp_json, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                            
                            # Convert main data to dataframe
                            records = []
                            synonyms = []
                            
                            for entry in data['data']:
                                # Extract main entry data
                                record = {
                                    'id': entry.get('sources', [''])[0],  # Use first source as ID
                                    'title': entry['title'],
                                    'type': 'movie' if entry.get('type') == 'MOVIE' else 'tvSeries',
                                    'episodes': self.safe_int(entry.get('episodes')),
                                    'status': entry.get('status'),
                                    'season_year': self.safe_int(entry.get('animeSeason', {}).get('year')),
                                    'season_name': entry.get('animeSeason', {}).get('season'),
                                    'sources': ','.join(entry.get('sources', [])),
                                    'tags': ','.join(entry.get('tags', []))
                                }
                                records.append(record)
                                
                                # Extract synonyms
                                for synonym in entry.get('synonyms', []):
                                    if synonym and synonym != entry['title']:
                                        synonyms.append({
                                            'id': record['id'],
                                            'title': synonym,
                                            'relevance': self.TITLE_WEIGHTS['synonym']
                                        })
                                
                                pbar.update(1)
                            
                            # Create main dataframe
                            self._df = pd.DataFrame.from_records(records)
                            
                            # Create synonyms dataframe
                            self._synonyms_df = pd.DataFrame.from_records(synonyms)
                    
                    # Save to parquet with optimized compression
                    with tqdm(desc="Saving database cache", unit='B', unit_scale=True) as pbar:
                        self._df.to_parquet(
                            cache_file,
                            compression='brotli',
                            index=False
                        )
                        pbar.update(os.path.getsize(cache_file))
                        
                        self._synonyms_df.to_parquet(
                            synonyms_cache,
                            compression='brotli',
                            index=False
                        )
                        pbar.update(os.path.getsize(synonyms_cache))
                        
                        # Build and save title index
                        self._build_title_index()
                        
                        # Save title index to parquet
                        pd.DataFrame([
                            {'title': entry.title, 'title_type': entry.title_type, 'relevance': entry.relevance}
                            for entry in self._title_index.values()
                        ]).to_parquet(
                            title_index_cache,
                            compression='brotli',
                            index=False
                        )
                        pbar.update(os.path.getsize(title_index_cache))
                    
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
        
    def _build_title_index(self) -> None:
        """Build an efficient search index of titles"""
        if self._df is None or self._synonyms_df is None:
            return
            
        # Create title lookup dictionary
        titles = {}
        
        # Add main titles
        with tqdm(desc="Building title index", total=len(self._df) + len(self._synonyms_df)) as pbar:
            # Add main titles
            main_titles_df = pd.DataFrame({
                'title': self._df['title'],
                'title_type': 'main',
                'relevance': self.TITLE_WEIGHTS['main']
            })
            titles.update({
                row.title: TitleEntry(title=row.title, title_type=row.title_type, relevance=row.relevance)
                for row in main_titles_df.itertuples()
            })
            pbar.update(len(main_titles_df))
            
            # Add synonyms
            synonyms_df = pd.DataFrame({
                'title': self._synonyms_df['title'],
                'title_type': 'synonym',
                'relevance': self._synonyms_df['relevance']
            })
            titles.update({
                row.title: TitleEntry(title=row.title, title_type=row.title_type, relevance=row.relevance)
                for row in synonyms_df.itertuples()
            })
            pbar.update(len(synonyms_df))
        
        self._title_index = titles
    
    def find_title(self, title: str, year: Optional[int] = None) -> Optional[MatchResult]:
        """Find title information for either a movie or TV show"""
        if self._df is None:
            self.load_database()
        
        if self._df is None or self._title_index is None:
            return None
        
        # First try exact matches
        best_match = None
        best_score = 0
        
        # Look for exact matches first
        if title in self._title_index:
            entry = self._title_index[title]
            main_entry = None
            
            # Find corresponding main entry
            if entry.title_type == 'main':
                main_matches = self._df[self._df['title'] == entry.title]
                if not main_matches.empty:
                    main_entry = main_matches.iloc[0]
            else:
                # Handle synonyms safely
                if self._synonyms_df is not None:
                    synonym_matches = self._synonyms_df[self._synonyms_df['title'] == entry.title]
                    if not synonym_matches.empty:
                        synonym_ids = synonym_matches['id'].tolist()
                        main_matches = self._df[self._df['id'].isin(synonym_ids)]
                        if not main_matches.empty:
                            main_entry = main_matches.iloc[0]
            
            if main_entry is not None:
                score = 100 * entry.relevance
                
                # Add year match bonus
                if year and pd.notna(main_entry['season_year']):
                    entry_year = self.safe_int(main_entry['season_year'])
                    if entry_year and entry_year == year:
                        score += 20
                    elif entry_year and abs(entry_year - year) <= 1:
                        score += 10
                
                # Add bonus for having episodes count
                if pd.notna(main_entry['episodes']):
                    score += 10
                
                if score > best_score:
                    best_score = score
                    best_match = main_entry
        # If no exact match, try fuzzy matching
        if best_match is None:
            try:
                title_list = [str(k) for k in self._title_index.keys()]
                matches = process.extract(
                    title,
                    title_list,
                    scorer=fuzz.ratio,
                    limit=5
                )
                
                for matched_title, fuzzy_score, _ in matches:
                    if fuzzy_score < 80:
                        continue
                    
                    entry = self._title_index[matched_title]
                    main_entry = None
                    
                    # Find corresponding main entry
                    if entry.title_type == 'main':
                        main_matches = self._df[self._df['title'] == entry.title]
                        if not main_matches.empty:
                            main_entry = main_matches.iloc[0]
                    else:
                        # Handle synonyms safely
                        if self._synonyms_df is not None:
                            synonym_matches = self._synonyms_df[self._synonyms_df['title'] == entry.title]
                            if not synonym_matches.empty:
                                synonym_ids = synonym_matches['id'].tolist()
                                main_matches = self._df[self._df['id'].isin(synonym_ids)]
                                if not main_matches.empty:
                                    main_entry = main_matches.iloc[0]
                    
                    if main_entry is not None:
                        score = fuzzy_score * entry.relevance
                        
                        # Add year match bonus
                        if year and pd.notna(main_entry['season_year']):
                            entry_year = self.safe_int(main_entry['season_year'])
                            if entry_year and entry_year == year:
                                score += 20
                            elif entry_year and abs(entry_year - year) <= 1:
                                score += 10
                        
                        # Add bonus for having episodes count
                        if pd.notna(main_entry['episodes']):
                            score += 10
                        
                        if score > best_score:
                            best_score = score
                            best_match = main_entry
            except Exception as e:
                logging.error(f"Error during fuzzy matching: {str(e)}")
        
        if best_match is not None:
            tags = best_match['tags'].split(',') if pd.notna(best_match['tags']) else []
            sources = best_match['sources'].split(',') if pd.notna(best_match['sources']) else []
            
            match_info = TitleInfo(
                id=best_match['id'],
                title=best_match['title'],
                type=best_match['type'],
                year=self.safe_int(best_match['season_year']),
                status=best_match['status'],
                total_episodes=self.safe_int(best_match['episodes']),
                tags=tags,
                sources=sources
            )
            
            return MatchResult(
                info=match_info,
                score=best_score,
                provider_weight=self.provider_weight
            )
        
        return None
    
    def get_episode_info(self, parent_id: str, season: int, episode: int) -> Optional[EpisodeInfo]:
        """Get episode information if the title is a TV show"""
        if self._df is None:
            self.load_database()
        
        if self._df is None:
            return None
        
        # Find the anime by source ID
        anime_entry = self._df[self._df['sources'].str.contains(parent_id, na=False)]
        
        if not anime_entry.empty:
            entry = anime_entry.iloc[0]
            return EpisodeInfo(
                title=f"Episode {episode}",  # Anime databases typically don't have episode titles
                season=season,
                episode=episode,
                parent_id=parent_id,
                year=self.safe_int(entry['season_year'])
            )
        
        return None