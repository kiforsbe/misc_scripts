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
    
    # Define columns we actually need from each dataset
    REQUIRED_COLUMNS = {
        'title.basics': ['tconst', 'primaryTitle', 'titleType', 'startYear', 'endYear', 'genres'],
        'title.episode': ['tconst', 'parentTconst', 'seasonNumber', 'episodeNumber'],
        'title.ratings': ['tconst', 'averageRating', 'numVotes'],
        'title.akas': ['titleId', 'title', 'region', 'isOriginalTitle']
    }
    
    MAX_RETRIES = 3
    
    def __init__(self):
        super().__init__('imdb', provider_weight=0.9)
        self._datasets = {}  # Store dataset references, load on demand
        self._tv_series_cache = {}
    
    def _load_dataset_if_needed(self, dataset_name: str) -> pd.DataFrame:
        """Lazy load datasets only when needed"""
        if dataset_name not in self._datasets:
            df = self.download_dataset(dataset_name)
            # Set index for efficient lookups
            if dataset_name == 'title.episode':
                df = df.set_index('tconst', drop=False)
            elif dataset_name in ['title.basics', 'title.ratings']:
                df = df.set_index('tconst')
            self._datasets[dataset_name] = df
        return self._datasets[dataset_name]
    
    def download_dataset(self, dataset_name: str) -> pd.DataFrame:
        """Download and process an IMDb dataset with resume capability"""
        url = self.DATASETS[dataset_name]
        cache_file = os.path.join(self.cache_dir, f"{dataset_name}.parquet")
        gz_cache = os.path.join(self.cache_dir, f"{dataset_name}.tsv.gz")
        
        # First check if we have a valid parquet cache
        if self.is_cache_valid(cache_file) and self._verify_file_integrity(cache_file):
            try:
                with tqdm(desc=f"Loading cached {dataset_name}", unit='B', unit_scale=True) as pbar:
                    # Read only required columns
                    df = pd.read_parquet(
                        cache_file,
                        columns=self.REQUIRED_COLUMNS[dataset_name]
                    )
                    pbar.update(os.path.getsize(cache_file))
                return df
            except Exception as e:
                logging.warning(f"Failed to load cached parquet for {dataset_name}: {str(e)}")
        
        # Download and process with retries
        for attempt in range(self.MAX_RETRIES):
            try:
                # Download gzipped file with resume capability and progress
                response = requests.get(url, stream=True)
                total_size = int(response.headers.get('content-length', 0))
                
                with tqdm(total=total_size, desc=f"Downloading {dataset_name}", unit='B', unit_scale=True) as pbar:
                    with open(gz_cache, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                                pbar.update(len(chunk))
                
                # Parse the gzipped TSV
                with tqdm(desc=f"Parsing {dataset_name}", unit='B', unit_scale=True) as pbar:
                    try:
                        # Only read required columns to save memory
                        df = pd.read_csv(
                            gz_cache,
                            sep='\t',
                            usecols=self.REQUIRED_COLUMNS[dataset_name],
                            low_memory=False
                        )
                        
                        # Clean and optimize data types
                        if 'startYear' in df.columns:
                            df['startYear'] = pd.to_numeric(df['startYear'], errors='coerce')
                        if 'endYear' in df.columns:
                            df['endYear'] = pd.to_numeric(df['endYear'], errors='coerce')
                        if 'seasonNumber' in df.columns:
                            df['seasonNumber'] = pd.to_numeric(df['seasonNumber'], errors='coerce')
                        if 'episodeNumber' in df.columns:
                            df['episodeNumber'] = pd.to_numeric(df['episodeNumber'], errors='coerce')
                        if 'numVotes' in df.columns:
                            df['numVotes'] = pd.to_numeric(df['numVotes'], errors='coerce')
                        if 'averageRating' in df.columns:
                            df['averageRating'] = pd.to_numeric(df['averageRating'], errors='coerce')
                        
                        pbar.update(os.path.getsize(gz_cache))
                    except Exception as parse_error:
                        logging.error(f"Error parsing {dataset_name}: {str(parse_error)}")
                        if attempt < self.MAX_RETRIES - 1:
                            continue
                        raise
                
                # Save to parquet cache with optimized compression
                temp_parquet = cache_file + '.tmp'
                try:
                    with tqdm(desc=f"Saving {dataset_name} cache", unit='B', unit_scale=True) as pbar:
                        df.to_parquet(
                            temp_parquet,
                            compression='brotli',  # Better compression than default
                            index=False
                        )
                        pbar.update(os.path.getsize(temp_parquet))
                    os.replace(temp_parquet, cache_file)
                except Exception as write_error:
                    logging.error(f"Error writing parquet file: {str(write_error)}")
                    if os.path.exists(temp_parquet):
                        os.remove(temp_parquet)
                    if attempt < self.MAX_RETRIES - 1:
                        continue
                    raise
                
                # Clean up gzip file
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

    def find_title(self, title: str, year: Optional[int] = None) -> Optional[MatchResult]:
        """Find title information for either a movie or TV show"""
        # Lazy load only the required datasets
        basics_df = self._load_dataset_if_needed('title.basics')
        ratings_df = self._load_dataset_if_needed('title.ratings')
        
        # Search in movies and TV shows
        type_filter = basics_df['titleType'].isin(['movie', 'tvSeries', 'tvMiniSeries'])
        search_df = basics_df[type_filter]
        
        # Search in primary titles
        title_matches = process.extract(
            title,
            search_df['primaryTitle'].to_dict(),
            scorer=fuzz.ratio,
            limit=50
        )
        
        best_match = None
        best_score = 0
        
        for matched_title, score, idx in title_matches:
            if score < 80:
                continue
            
            row = search_df.loc[idx]
            total_score = score
            
            # Add year match bonus
            if year and pd.notna(row['startYear']):
                row_year = self.safe_int(row['startYear'])
                if row_year and row_year == year:
                    total_score += 20
                elif row_year and abs(row_year - year) <= 1:
                    total_score += 10
            
            # Add popularity bonus based on votes
            if idx in ratings_df.index:
                rating_data = ratings_df.loc[idx]
                if pd.notna(rating_data['numVotes']):
                    votes = self.safe_int(rating_data['numVotes'])
                    if votes:
                        vote_bonus = min(15, (votes / 10000))
                        total_score += vote_bonus
            
            if total_score > best_score:
                best_score = total_score
                rating = None
                votes = None
                
                if idx in ratings_df.index:
                    rating_data = ratings_df.loc[idx]
                    if pd.notna(rating_data['averageRating']):
                        rating = float(rating_data['averageRating'])
                    if pd.notna(rating_data['numVotes']):
                        votes = self.safe_int(rating_data['numVotes'])
                
                # Map IMDb type to unified type
                media_type = 'movie' if row['titleType'] == 'movie' else 'tv'
                
                # Get episode count for TV shows
                total_episodes = None
                total_seasons = None
                if media_type == 'tv':
                    episodes_df = self._get_episodes(idx)
                    if not episodes_df.empty:
                        total_seasons = self.safe_int(episodes_df['seasonNumber'].max())
                        total_episodes = len(episodes_df)
                
                genres = row['genres'].split(',') if pd.notna(row['genres']) else []
                
                best_match = TitleInfo(
                    id=idx,
                    title=row['primaryTitle'],
                    type=media_type,
                    year=self.safe_int(row['startYear']),
                    start_year=self.safe_int(row['startYear']),
                    end_year=self.safe_int(row['endYear']),
                    rating=rating,
                    votes=votes,
                    genres=genres,
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

    def _get_episodes(self, series_id: str) -> pd.DataFrame:
        """Get episodes dataframe for a TV series with caching"""
        if series_id not in self._tv_series_cache:
            episodes_df = self._load_dataset_if_needed('title.episode')
            if 'parentTconst' in episodes_df.columns:
                # Convert series_id to string and filter
                series_id_str = str(series_id)
                filtered_df = episodes_df[episodes_df['parentTconst'].astype(str) == series_id_str].copy()
                
                # Convert season and episode numbers to numeric
                filtered_df['seasonNumber'] = pd.to_numeric(filtered_df['seasonNumber'], errors='coerce')
                filtered_df['episodeNumber'] = pd.to_numeric(filtered_df['episodeNumber'], errors='coerce')
                
                # Sort by season and episode
                filtered_df = filtered_df.sort_values(['seasonNumber', 'episodeNumber'])
                
                self._tv_series_cache[series_id] = filtered_df
            else:
                logging.error("'parentTconst' column is missing in the 'title.episode' dataset.")
                self._tv_series_cache[series_id] = pd.DataFrame()
        return self._tv_series_cache[series_id]

    def get_episode_info(self, parent_id: str, season: int, episode: int) -> Optional[EpisodeInfo]:
        """Get episode information if the title is a TV show"""
        episodes_df = self._get_episodes(parent_id)
        if episodes_df.empty:
            return None
            
        # Handle NaN values in season/episode numbers
        episode_match = episodes_df[
            (episodes_df['seasonNumber'].fillna(-1) == season) &
            (episodes_df['episodeNumber'].fillna(-1) == episode)
        ]
        
        if not episode_match.empty:
            episode_id = episode_match.index[0]
            basics_df = self._load_dataset_if_needed('title.basics')
            ratings_df = self._load_dataset_if_needed('title.ratings')
            
            # Handle potential missing episode data
            try:
                episode_data = basics_df.loc[episode_id]
                
                rating = None
                votes = None
                if episode_id in ratings_df.index:
                    rating_data = ratings_df.loc[episode_id]
                    if pd.notna(rating_data['averageRating']):
                        rating = float(rating_data['averageRating'])
                    if pd.notna(rating_data['numVotes']):
                        votes = self.safe_int(rating_data['numVotes'])
                
                return EpisodeInfo(
                    title=episode_data['primaryTitle'],
                    season=season,
                    episode=episode,
                    parent_id=parent_id,
                    year=self.safe_int(episode_data['startYear']),
                    rating=rating,
                    votes=votes
                )
            except KeyError as e:
                logging.error(f"Failed to find episode data for ID {episode_id}: {str(e)}")
            except Exception as e:
                logging.error(f"Error processing episode data: {str(e)}")
        
        return None