import os
import gzip
import logging
import requests
import pandas as pd
from datetime import timedelta
from typing import Optional
from rapidfuzz import fuzz, process
from tqdm import tqdm
from metadata_provider import BaseMetadataProvider, TitleInfo, EpisodeInfo, MatchResult
import io
import tempfile

class IMDbDataProvider(BaseMetadataProvider):
    DATASETS = {
        'title.basics': 'https://datasets.imdbws.com/title.basics.tsv.gz',
        'title.episode': 'https://datasets.imdbws.com/title.episode.tsv.gz',
        'title.ratings': 'https://datasets.imdbws.com/title.ratings.tsv.gz',
        'title.akas': 'https://datasets.imdbws.com/title.akas.tsv.gz'
    }
    MAX_RETRIES = 3
    
    def __init__(self):
        super().__init__('imdb', provider_weight=0.9)  # IMDB gets slightly lower weight than anime DB for anime
        self.title_basics = None
        self.title_episode = None
        self.title_ratings = None
        self.title_akas = None
        self.tv_series_cache = {}
    
    def download_dataset(self, dataset_name: str) -> pd.DataFrame:
        """Download and process an IMDb dataset with resume capability"""
        url = self.DATASETS[dataset_name]
        cache_file = os.path.join(self.cache_dir, f"{dataset_name}.parquet")
        gz_cache = os.path.join(self.cache_dir, f"{dataset_name}.tsv.gz")
        
        # First check if we have a valid parquet cache
        if self.is_cache_valid(cache_file) and self._verify_file_integrity(cache_file):
            try:
                with tqdm(desc=f"Loading cached {dataset_name}", unit='B', unit_scale=True) as pbar:
                    df = pd.read_parquet(cache_file)
                    pbar.update(os.path.getsize(cache_file))
                return df
            except Exception as e:
                logging.warning(f"Failed to load cached parquet for {dataset_name}: {str(e)}")
        
        # Download and process with retries
        for attempt in range(self.MAX_RETRIES):
            try:
                logging.info(f"Processing {dataset_name} dataset (attempt {attempt + 1}/{self.MAX_RETRIES})...")
                
                # Download gzipped file with resume capability and progress bar
                response = requests.get(url, stream=True)
                total_size = int(response.headers.get('content-length', 0))
                block_size = 8192
                
                with tqdm(total=total_size, desc=f"Downloading {dataset_name}", unit='B', unit_scale=True) as pbar:
                    with open(gz_cache, 'wb') as f:
                        for data in response.iter_content(block_size):
                            if data:
                                f.write(data)
                                pbar.update(len(data))
                
                # Verify the gzipped file
                try:
                    with gzip.open(gz_cache, 'rb') as test_read:
                        test_read.read(1024)  # Try reading a small chunk to verify
                except Exception as gz_error:
                    logging.error(f"Downloaded {dataset_name} file is corrupted: {str(gz_error)}")
                    os.remove(gz_cache)
                    if attempt < self.MAX_RETRIES - 1:
                        continue
                    raise
                
                # Parse the gzipped TSV with progress bar
                with tqdm(desc=f"Parsing {dataset_name}", unit='B', unit_scale=True) as pbar:
                    try:
                        with gzip.open(gz_cache, 'rb') as gz_file:
                            # Read and parse in chunks to show progress
                            chunks = []
                            while True:
                                chunk = gz_file.read(block_size)
                                if not chunk:
                                    break
                                chunks.append(chunk)
                                pbar.update(len(chunk))
                            data = b''.join(chunks)
                            df = pd.read_csv(io.BytesIO(data), sep='\t', low_memory=False)
                    except Exception as parse_error:
                        logging.error(f"Error parsing {dataset_name}: {str(parse_error)}")
                        if attempt < self.MAX_RETRIES - 1:
                            continue
                        raise
                
                # Save to parquet cache with progress
                temp_parquet = cache_file + '.tmp'
                try:
                    with tqdm(desc=f"Saving {dataset_name} cache", unit='B', unit_scale=True) as pbar:
                        df.to_parquet(temp_parquet)
                        pbar.update(os.path.getsize(temp_parquet))
                    os.replace(temp_parquet, cache_file)
                except Exception as write_error:
                    logging.error(f"Error writing parquet file: {str(write_error)}")
                    if os.path.exists(temp_parquet):
                        os.remove(temp_parquet)
                    if attempt < self.MAX_RETRIES - 1:
                        continue
                    raise
                
                # Clean up gzip file after successful processing
                try:
                    os.remove(gz_cache)
                except:
                    pass
                    
                return df
                    
            except Exception as e:
                logging.error(f"Error processing {dataset_name} (attempt {attempt + 1}): {str(e)}")
                if attempt < self.MAX_RETRIES - 1:
                    logging.info("Retrying...")
                    continue
                raise
        
        raise RuntimeError(f"Failed to process {dataset_name} dataset after {self.MAX_RETRIES} attempts")
    
    def load_datasets(self) -> None:
        """Load all necessary IMDb datasets"""
        try:
            logging.info("Starting IMDb datasets loading process...")
            datasets = list(self.DATASETS.keys())
            
            # Create overall progress bar for dataset loading
            with tqdm(total=len(datasets), desc="Loading IMDb datasets") as pbar:
                for dataset_name in datasets:
                    pbar.set_description(f"Loading {dataset_name}")
                    if dataset_name == 'title.basics':
                        self.title_basics = self.download_dataset(dataset_name)
                    elif dataset_name == 'title.episode':
                        self.title_episode = self.download_dataset(dataset_name)
                    elif dataset_name == 'title.ratings':
                        self.title_ratings = self.download_dataset(dataset_name)
                    elif dataset_name == 'title.akas':
                        self.title_akas = self.download_dataset(dataset_name)
                    pbar.update(1)
            
            with tqdm(desc="Setting indices", total=3) as pbar:
                self.title_basics.set_index('tconst', inplace=True)
                pbar.update(1)
                self.title_episode.set_index('tconst', inplace=True)
                pbar.update(1)
                self.title_ratings.set_index('tconst', inplace=True)
                pbar.update(1)
            
            logging.info("Successfully loaded and indexed all IMDb datasets")
            
        except Exception as e:
            logging.error(f"Error loading IMDb datasets: {str(e)}")
            raise
    
    def find_title(self, title: str, year: Optional[int] = None) -> Optional[MatchResult]:
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
            limit=50
        )
        
        best_match = None
        best_score = 0
        
        for matched_title, score, idx in title_matches:
            if score < 80:  # Minimum similarity threshold
                continue
            
            row = self.title_basics.loc[idx]
            
            # Calculate base score from title match
            total_score = score
            
            # Add year match bonus
            if year and row['startYear'] != 'NA':
                row_year = self.safe_int(row['startYear'])
                if row_year and year:
                    if row_year == year:
                        total_score += 20
                    elif abs(row_year - year) <= 1:
                        total_score += 10
            
            # Add popularity bonus based on number of votes
            if idx in self.title_ratings.index:
                rating_data = self.title_ratings.loc[idx]
                votes = self.safe_int(rating_data['numVotes']) or 0
                # Log scale bonus for vote count (max +15 points)
                vote_bonus = min(15, (votes / 10000))  # 100k votes = +10 points
                total_score += vote_bonus
            
            if total_score > best_score:
                best_score = total_score
                rating = None
                votes = None
                
                if idx in self.title_ratings.index:
                    rating_data = self.title_ratings.loc[idx]
                    try:
                        rating = float(rating_data['averageRating'])
                        votes = self.safe_int(rating_data['numVotes'])
                    except (ValueError, TypeError):
                        pass
                
                # Map IMDb type to our unified type system
                media_type = 'movie' if row['titleType'] == 'movie' else 'tv'
                
                # Get episode count for TV shows
                total_episodes = None
                total_seasons = None
                if media_type == 'tv':
                    if idx in self.tv_series_cache:
                        episodes = self.tv_series_cache[idx]
                    else:
                        episodes = self.title_episode[self.title_episode['parentTconst'] == idx]
                        self.tv_series_cache[idx] = episodes
                    
                    if not episodes.empty:
                        try:
                            season_numbers = episodes['seasonNumber'].dropna()
                            if not season_numbers.empty:
                                total_seasons = self.safe_int(season_numbers.astype(int).max())
                            total_episodes = len(episodes)
                        except (ValueError, TypeError):
                            pass
                
                start_year = self.safe_int(row['startYear'])
                end_year = self.safe_int(row['endYear'])
                
                best_match = TitleInfo(
                    id=idx,
                    title=row['primaryTitle'],
                    type=media_type,
                    year=start_year,
                    start_year=start_year,
                    end_year=end_year,
                    rating=rating,
                    votes=votes,
                    genres=row['genres'].split(',') if pd.notna(row['genres']) else [],
                    total_episodes=total_episodes,
                    total_seasons=total_seasons
                )
        
        if best_match:
            return MatchResult(
                info=best_match,
                score=best_score,
                provider_weight=self.provider_weight
            )
        
        return None
    
    def get_episode_info(self, parent_id: str, season: int, episode: int) -> Optional[EpisodeInfo]:
        """Get episode information if the title is a TV show"""
        if not self.title_basics is not None:
            self.load_datasets()
        
        if parent_id not in self.tv_series_cache:
            episodes = self.title_episode[self.title_episode['parentTconst'] == parent_id]
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
                try:
                    rating = float(rating_data['averageRating'])
                    votes = self.safe_int(rating_data['numVotes'])
                except (ValueError, TypeError):
                    pass
            
            year = self.safe_int(episode_data['startYear'])
            
            return EpisodeInfo(
                title=episode_data['primaryTitle'],
                season=season,
                episode=episode,
                parent_id=parent_id,
                year=year,
                rating=rating,
                votes=votes
            )
        
        return None