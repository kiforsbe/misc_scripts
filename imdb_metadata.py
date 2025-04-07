import os
import gzip
import json
import logging
import requests
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
from typing import Dict, Optional, List, Any

class IMDbDataManager:
    DATASETS = {
        'title.basics': 'https://datasets.imdbws.com/title.basics.tsv.gz',
        'title.episode': 'https://datasets.imdbws.com/title.episode.tsv.gz',
        'title.ratings': 'https://datasets.imdbws.com/title.ratings.tsv.gz',
        'title.akas': 'https://datasets.imdbws.com/title.akas.tsv.gz'
    }
    
    CACHE_DIR = os.path.join(os.path.expanduser("~"), ".video_metadata_cache", "imdb")
    CACHE_DURATION = timedelta(days=7)  # Update datasets weekly
    
    def __init__(self):
        self.ensure_cache_dir()
        self.title_basics = None
        self.title_episodes = None
        self.title_ratings = None
        self.title_akas = None
        self.tv_series_cache = {}
        self.movie_cache = {}
    
    def ensure_cache_dir(self):
        """Create cache directory if it doesn't exist"""
        os.makedirs(self.CACHE_DIR, exist_ok=True)
    
    def is_cache_valid(self, dataset_name: str) -> bool:
        """Check if cached dataset is still valid"""
        cache_file = os.path.join(self.CACHE_DIR, f"{dataset_name}.parquet")
        if not os.path.exists(cache_file):
            return False
        
        mtime = datetime.fromtimestamp(os.path.getmtime(cache_file))
        return datetime.now() - mtime < self.CACHE_DURATION
    
    def download_dataset(self, dataset_name: str) -> None:
        """Download and process an IMDb dataset"""
        url = self.DATASETS[dataset_name]
        cache_file = os.path.join(self.CACHE_DIR, f"{dataset_name}.parquet")
        
        if self.is_cache_valid(dataset_name):
            logging.info(f"Loading cached {dataset_name} dataset")
            return pd.read_parquet(cache_file)
        
        logging.info(f"Downloading {dataset_name} dataset...")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        # Decompress and read TSV
        with gzip.open(response.raw) as gz_file:
            df = pd.read_csv(gz_file, sep='\t', low_memory=False)
        
        # Save to parquet for faster future loading
        df.to_parquet(cache_file)
        return df
    
    def load_datasets(self) -> None:
        """Load all necessary IMDb datasets"""
        try:
            self.title_basics = self.download_dataset('title.basics')
            self.title_episodes = self.download_dataset('title.episode')
            self.title_ratings = self.download_dataset('title.ratings')
            self.title_akas = self.download_dataset('title.akas')
            
            # Create indexes for faster lookups
            self.title_basics.set_index('tconst', inplace=True)
            self.title_episodes.set_index('tconst', inplace=True)
            self.title_ratings.set_index('tconst', inplace=True)
            
            logging.info("Successfully loaded all IMDb datasets")
        except Exception as e:
            logging.error(f"Error loading IMDb datasets: {e}")
            raise
    
    def find_best_match(self, title: str, year: Optional[int] = None, media_type: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Find the best matching title in IMDb datasets"""
        if not self.title_basics is not None:
            self.load_datasets()
        
        # Filter by media type if specified
        type_filter = self.title_basics['titleType'].isin(['movie']) if media_type == 'movie' \
                     else self.title_basics['titleType'].isin(['tvSeries', 'tvMiniSeries']) if media_type == 'tv' \
                     else self.title_basics['titleType'].isin(['movie', 'tvSeries', 'tvMiniSeries'])
        
        # Search in primary titles and alternative titles
        matches = []
        
        # Search in primary titles
        title_matches = self.title_basics[
            (self.title_basics['primaryTitle'].str.lower() == title.lower()) &
            type_filter
        ]
        
        if year:
            title_matches = title_matches[
                (self.title_basics['startYear'].astype(str) == str(year))
            ]
        
        for _, row in title_matches.iterrows():
            matches.append({
                'id': row.name,
                'title': row['primaryTitle'],
                'year': row['startYear'],
                'type': row['titleType'],
                'score': 100  # Exact match
            })
        
        if not matches:
            # Search in alternative titles
            aka_matches = self.title_akas[
                self.title_akas['title'].str.lower() == title.lower()
            ]
            
            for _, row in aka_matches.iterrows():
                title_data = self.title_basics.loc[row['titleId']]
                if title_data['titleType'] in ['movie', 'tvSeries', 'tvMiniSeries']:
                    matches.append({
                        'id': row['titleId'],
                        'title': title_data['primaryTitle'],
                        'year': title_data['startYear'],
                        'type': title_data['titleType'],
                        'score': 90  # Alternative title match
                    })
        
        if matches:
            # Sort by score and year (if provided)
            matches.sort(key=lambda x: (-x['score'], abs(int(x['year']) - year) if year else 0))
            best_match = matches[0]
            
            # Enhance with ratings
            if best_match['id'] in self.title_ratings.index:
                ratings = self.title_ratings.loc[best_match['id']]
                best_match.update({
                    'rating': ratings['averageRating'],
                    'votes': ratings['numVotes']
                })
            
            return best_match
        
        return None
    
    def get_episode_info(self, series_id: str, season: int, episode: int) -> Optional[Dict[str, Any]]:
        """Get specific episode information for a TV series"""
        if series_id not in self.tv_series_cache:
            # Get all episodes for the series
            episodes = self.title_episodes[self.title_episodes['parentTconst'] == series_id]
            self.tv_series_cache[series_id] = episodes
        
        episodes = self.tv_series_cache[series_id]
        episode_match = episodes[
            (episodes['seasonNumber'].astype(int) == season) &
            (episodes['episodeNumber'].astype(int) == episode)
        ]
        
        if not episode_match.empty:
            episode_id = episode_match.index[0]
            episode_data = self.title_basics.loc[episode_id]
            
            info = {
                'title': episode_data['primaryTitle'],
                'plot': episode_data['plot'] if 'plot' in episode_data else None,
                'season': season,
                'episode': episode
            }
            
            # Add ratings if available
            if episode_id in self.title_ratings.index:
                ratings = self.title_ratings.loc[episode_id]
                info.update({
                    'rating': ratings['averageRating'],
                    'votes': ratings['numVotes']
                })
            
            return info
        
        return None
    
    def get_series_info(self, series_id: str) -> Optional[Dict[str, Any]]:
        """Get comprehensive information about a TV series"""
        if not self.title_basics is not None:
            self.load_datasets()
        
        if series_id in self.title_basics.index:
            series_data = self.title_basics.loc[series_id]
            
            info = {
                'title': series_data['primaryTitle'],
                'start_year': series_data['startYear'],
                'end_year': series_data['endYear'],
                'genres': series_data['genres'].split(',') if pd.notna(series_data['genres']) else [],
                'type': series_data['titleType']
            }
            
            # Add ratings if available
            if series_id in self.title_ratings.index:
                ratings = self.title_ratings.loc[series_id]
                info.update({
                    'rating': ratings['averageRating'],
                    'votes': ratings['numVotes']
                })
            
            # Get total number of seasons
            if series_id in self.tv_series_cache:
                episodes = self.tv_series_cache[series_id]
            else:
                episodes = self.title_episodes[self.title_episodes['parentTconst'] == series_id]
                self.tv_series_cache[series_id] = episodes
            
            if not episodes.empty:
                info['total_seasons'] = episodes['seasonNumber'].astype(int).max()
                info['total_episodes'] = len(episodes)
            
            return info
        
        return None