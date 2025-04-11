import os
import gzip
import logging
import requests
import pandas as pd
from datetime import timedelta
from typing import Optional
from rapidfuzz import fuzz, process
from metadata_provider import BaseMetadataProvider, TitleInfo, EpisodeInfo

class IMDbDataProvider(BaseMetadataProvider):
    DATASETS = {
        'title.basics': 'https://datasets.imdbws.com/title.basics.tsv.gz',
        'title.episode': 'https://datasets.imdbws.com/title.episode.tsv.gz',
        'title.ratings': 'https://datasets.imdbws.com/title.ratings.tsv.gz',
        'title.akas': 'https://datasets.imdbws.com/title.akas.tsv.gz'
    }
    
    def __init__(self):
        super().__init__('imdb')
        self.title_basics = None
        self.title_episodes = None
        self.title_ratings = None
        self.title_akas = None
        self.tv_series_cache = {}
    
    def download_dataset(self, dataset_name: str) -> pd.DataFrame:
        """Download and process an IMDb dataset"""
        url = self.DATASETS[dataset_name]
        cache_file = os.path.join(self.cache_dir, f"{dataset_name}.parquet")
        
        if self.is_cache_valid(cache_file):
            logging.info(f"Loading cached {dataset_name} dataset")
            return pd.read_parquet(cache_file)
        
        logging.info(f"Downloading {dataset_name} dataset...")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        with gzip.open(response.raw) as gz_file:
            df = pd.read_csv(gz_file, sep='\t', low_memory=False)
        
        df.to_parquet(cache_file)
        return df
    
    def load_datasets(self) -> None:
        """Load all necessary IMDb datasets"""
        try:
            self.title_basics = self.download_dataset('title.basics')
            self.title_episodes = self.download_dataset('title.episode')
            self.title_ratings = self.download_dataset('title.ratings')
            self.title_akas = self.download_dataset('title.akas')
            
            self.title_basics.set_index('tconst', inplace=True)
            self.title_episodes.set_index('tconst', inplace=True)
            self.title_ratings.set_index('tconst', inplace=True)
            
            logging.info("Successfully loaded all IMDb datasets")
        except Exception as e:
            logging.error(f"Error loading IMDb datasets: {e}")
            raise
    
    def find_title(self, title: str, year: Optional[int] = None) -> Optional[TitleInfo]:
        """Find title information for either a movie or TV show"""
        if not self.title_basics is not None:
            self.load_datasets()
        
        # Search in both movies and TV shows
        type_filter = self.title_basics['titleType'].isin(['movie', 'tvSeries', 'tvMiniSeries'])
        
        # Search in primary titles
        title_matches = process.extract(
            title,
            self.title_basics[type_filter]['primaryTitle'].to_dict(),
            scorer=fuzz.ratio,
            limit=5
        )
        
        best_match = None
        best_score = 0
        
        for matched_title, score, idx in title_matches:
            if score < 80:  # Minimum similarity threshold
                continue
            
            row = self.title_basics.loc[idx]
            if year and row['startYear'] != 'NA':
                try:
                    if abs(int(row['startYear']) - year) > 1:  # Allow 1 year difference
                        continue
                except ValueError:
                    continue
            
            # Calculate match score including year match
            total_score = score
            if year and row['startYear'] != 'NA' and int(row['startYear']) == year:
                total_score += 20
            
            if total_score > best_score:
                best_score = total_score
                
                # Get rating information if available
                rating = None
                votes = None
                if idx in self.title_ratings.index:
                    rating_data = self.title_ratings.loc[idx]
                    rating = float(rating_data['averageRating'])
                    votes = int(rating_data['numVotes'])
                
                # Map IMDb type to our unified type system
                media_type = 'movie' if row['titleType'] == 'movie' else 'tv'
                
                # Get episode count for TV shows
                total_episodes = None
                total_seasons = None
                if media_type == 'tv':
                    if idx in self.tv_series_cache:
                        episodes = self.tv_series_cache[idx]
                    else:
                        episodes = self.title_episodes[self.title_episodes['parentTconst'] == idx]
                        self.tv_series_cache[idx] = episodes
                    
                    if not episodes.empty:
                        total_seasons = episodes['seasonNumber'].astype(int).max()
                        total_episodes = len(episodes)
                
                best_match = TitleInfo(
                    id=idx,
                    title=row['primaryTitle'],
                    type=media_type,
                    year=int(row['startYear']) if row['startYear'] != 'NA' else None,
                    start_year=int(row['startYear']) if row['startYear'] != 'NA' else None,
                    end_year=int(row['endYear']) if row['endYear'] != 'NA' else None,
                    rating=rating,
                    votes=votes,
                    genres=row['genres'].split(',') if pd.notna(row['genres']) else [],
                    total_episodes=total_episodes,
                    total_seasons=total_seasons
                )
        
        return best_match
    
    def get_episode_info(self, parent_id: str, season: int, episode: int) -> Optional[EpisodeInfo]:
        """Get episode information if the title is a TV show"""
        if not self.title_basics is not None:
            self.load_datasets()
        
        if parent_id not in self.tv_series_cache:
            episodes = self.title_episodes[self.title_episodes['parentTconst'] == parent_id]
            self.tv_series_cache[parent_id] = episodes
        
        episodes = self.tv_series_cache[parent_id]
        episode_match = episodes[
            (episodes['seasonNumber'].astype(int) == season) &
            (episodes['episodeNumber'].astype(int) == episode)
        ]
        
        if not episode_match.empty:
            episode_id = episode_match.index[0]
            episode_data = self.title_basics.loc[episode_id]
            
            rating = None
            votes = None
            if episode_id in self.title_ratings.index:
                rating_data = self.title_ratings.loc[episode_id]
                rating = float(rating_data['averageRating'])
                votes = int(rating_data['numVotes'])
            
            return EpisodeInfo(
                title=episode_data['primaryTitle'],
                season=season,
                episode=episode,
                parent_id=parent_id,
                year=int(episode_data['startYear']) if episode_data['startYear'] != 'NA' else None,
                rating=rating,
                votes=votes
            )
        
        return None