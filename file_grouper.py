import argparse
import json
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Set, Any, Optional
import fnmatch
import re

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    # Fallback progress indicator
    class tqdm:
        def __init__(self, iterable=None, total=None, desc=None, unit=None, disable=False):
            self.iterable = iterable
            self.total = total or (len(iterable) if iterable else 0)
            self.desc = desc
            self.current = 0
            self.disable = disable
            if not disable and desc:
                print(f"{desc}...")
        
        def __iter__(self):
            if self.iterable:
                for item in self.iterable:
                    yield item
                    self.update(1)
            return self
        
        def __enter__(self):
            return self
        
        def __exit__(self, *args):
            if not self.disable and self.desc:
                print(f"{self.desc} completed.")
        
        def update(self, n=1):
            self.current += n
            if not self.disable and self.total > 0:
                percent = (self.current / self.total) * 100
                if self.current % max(1, self.total // 10) == 0 or self.current == self.total:
                    print(f"  Progress: {self.current}/{self.total} ({percent:.1f}%)")
        
        def set_description(self, desc):
            self.desc = desc
        
        def set_postfix(self, **kwargs):
            # Simple implementation for fallback
            pass

class CustomJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle complex data structures from guessit and other sources."""
    
    def default(self, obj):
        # Handle Path objects
        if isinstance(obj, Path):
            return str(obj)
        
        # Handle sets
        if isinstance(obj, set):
            return list(obj)
        
        # Handle any object with __dict__ attribute (custom objects)
        if hasattr(obj, '__dict__'):
            return obj.__dict__
        
        # Handle objects that have a string representation but aren't basic types
        if hasattr(obj, '__str__') and not isinstance(obj, (str, int, float, bool, list, dict, type(None))):
            return str(obj)
        
        # Let the base class handle other cases
        return super().default(obj)

try:
    from guessit_wrapper import guessit_wrapper
except ImportError:
    print("Error: guessit_wrapper library not found. Install with: pip install guessit_wrapper")
    sys.exit(1)

try:
    # Load this library from subfolder video-optimizer-v2
    sys.path.append(os.path.join(os.path.dirname(__file__), 'video-optimizer-v2'))
    from metadata_provider import MetadataManager, BaseMetadataProvider, TitleInfo
    from anime_metadata import AnimeDataProvider
    from imdb_metadata import IMDbDataProvider
    from plex_metadata import PlexMetadataProvider, PlexWatchStatus
    from myanimelist_watch_status import MyAnimeListWatchStatusProvider, MyAnimeListWatchStatus
    
    # Initialize metadata manager as a global variable
    METADATA_MANAGER = None
    PLEX_PROVIDER = None

    def get_metadata_manager():
        """Get or initialize the metadata manager"""
        global METADATA_MANAGER
        if (METADATA_MANAGER is None):
            # Initialize providers
            anime_provider = AnimeDataProvider()
            imdb_provider = IMDbDataProvider()
            METADATA_MANAGER = MetadataManager([anime_provider, imdb_provider])
        return METADATA_MANAGER
    
    def get_plex_provider():
        """Get or initialize the Plex provider"""
        global PLEX_PROVIDER
        if PLEX_PROVIDER is None:
            PLEX_PROVIDER = PlexMetadataProvider()
        return PLEX_PROVIDER
except ImportError:
    print("Warning: metadata_provider not found. Enhanced metadata features will be disabled.")
    MetadataManager = None
    BaseMetadataProvider = None
    TitleInfo = None
    PlexMetadataProvider = None
    PlexWatchStatus = None
    MyAnimeListWatchStatusProvider = None
    MyAnimeListWatchStatus = None

class FileGrouper:
    """Groups files based on filename metadata extracted using guessit."""
    
    def __init__(self, metadata_manager = None, plex_provider = None, myanimelist_xml_path = None):
        self.groups = defaultdict(list)
        self.metadata = {}
        self.enhanced_metadata = {}  # Store metadata from providers
        self.group_metadata = {}     # Store metadata for groups
        self.title_metadata = {}     # Store unique title metadata
        self.metadata_manager = metadata_manager
        self.plex_provider = plex_provider
        self.myanimelist_xml_path = myanimelist_xml_path
        self._mal_provider = None

    @staticmethod
    def _escape_pattern_for_fnmatch(pattern: str) -> str:
        """Escape square brackets in patterns to match them literally."""
        # Replace literal square brackets with escaped versions for fnmatch
        # fnmatch uses [] for character classes, but we want to match literal brackets
        escaped = pattern.replace('[', r'\[').replace(']', r'\]')
        return escaped
    
    @staticmethod
    def _matches_pattern(filename: str, pattern: str) -> bool:
        """Check if filename matches pattern, handling literal square brackets."""
        # If pattern contains square brackets, we need special handling
        if '[' in pattern or ']' in pattern:
            # For patterns with brackets, use a different approach
            # Convert pattern to a simple string match with wildcards
            
            # Escape regex special characters except * and ?
            escaped_pattern = re.escape(pattern)
            # Restore * and ? as wildcards
            escaped_pattern = escaped_pattern.replace(r'\*', '.*').replace(r'\?', '.')
            # Add anchors for full match
            regex_pattern = f'^{escaped_pattern}$'
            
            try:
                return bool(re.match(regex_pattern, filename, re.IGNORECASE))
            except re.error:
                # Fallback to simple string matching if regex fails
                return pattern.replace('*', '') in filename
        else:
            # Use standard fnmatch for patterns without brackets
            return fnmatch.fnmatch(filename, pattern)
        
    def discover_files(self, input_paths: List[str], excluded_paths: List[str] | None = None,
                      include_patterns: List[str] | None = None, exclude_patterns: List[str] | None = None,
                      recursive: bool = False, show_progress: bool = True) -> List[Path]:
        """Discover files based on input paths and filtering criteria."""
        excluded_paths = excluded_paths or []
        include_patterns = include_patterns or ['*']
        exclude_patterns = exclude_patterns or []
        
        discovered_files = []
        excluded_path_objects = [Path(p).resolve() for p in excluded_paths]
        
        # First pass: discover all candidate files
        with tqdm(input_paths, desc="Discovering files", unit="path", disable=not show_progress) as pbar:
            for input_path in pbar:
                path_obj = Path(input_path)
                if not path_obj.exists():
                    print(f"Warning: Path does not exist: {input_path}")
                    continue
                    
                if path_obj.is_file():
                    discovered_files.append(path_obj)
                else:
                    # Find files based on recursion setting
                    if recursive:
                        file_pattern = path_obj.rglob('*')
                    else:
                        file_pattern = path_obj.glob('*')
                    
                    path_files = []
                    for file_path in file_pattern:
                        if file_path.is_file():
                            # Check if file is in excluded paths
                            if any(self._is_path_excluded(file_path, exc_path) for exc_path in excluded_path_objects):
                                continue
                            path_files.append(file_path)
                    
                    discovered_files.extend(path_files)
                    pbar.set_postfix(found=len(discovered_files))
        
        # Apply include/exclude patterns with progress
        filtered_files = []
        with tqdm(discovered_files, desc="Filtering files", unit="file", disable=not show_progress) as pbar:
            for file_path in pbar:
                filename = file_path.name
                
                # Check include patterns using improved matching
                if not any(self._matches_pattern(filename, pattern) for pattern in include_patterns):
                    continue

                # Check exclude patterns using improved matching
                if any(self._matches_pattern(filename, pattern) for pattern in exclude_patterns):
                    continue
                    
                filtered_files.append(file_path)
                pbar.set_postfix(matched=len(filtered_files))
                
        return filtered_files
    
    def _is_path_excluded(self, file_path: Path, excluded_path: Path) -> bool:
        """Check if file_path is within excluded_path."""
        try:
            file_path.resolve().relative_to(excluded_path)
            return True
        except ValueError:
            return False
    
    def extract_metadata(self, file_path: Path) -> Dict[str, Any]:
        """Extract metadata from filename using guessit."""
        try:
            metadata = guessit_wrapper(file_path.name)
            # Convert guessit result to regular dict and add file info
            result = dict(metadata)
            result['filepath'] = str(file_path)
            result['filename'] = file_path.name
            result['file_size'] = file_path.stat().st_size if file_path.exists() else 0

            # Add enhanced metadata reference if available
            if self.metadata_manager and MetadataManager:
                title = result.get('title')
                year = result.get('year')
                season = result.get('season')
                if title:
                    try:
                        # Search for metadata using the base title - let the provider handle season mapping
                        enhanced_info, provider = self.metadata_manager.find_title(title, year)
                        
                        if enhanced_info:
                            # Use the actual metadata ID as the key
                            metadata_id = enhanced_info.id
                            
                            # Only store metadata once per unique ID
                            if metadata_id not in self.title_metadata:
                                title_metadata = {
                                    'metadata': self._serialize_title_info(enhanced_info),
                                    'provider': provider.__class__.__name__ if provider else None
                                }
                                
                                # Add MyAnimeList watch status at title level if available
                                if self.myanimelist_xml_path and MyAnimeListWatchStatusProvider:
                                    try:
                                        sources = enhanced_info.sources or []
                                        for source in sources:
                                            if 'myanimelist' in source:
                                                if self._mal_provider is None:
                                                    self._mal_provider = MyAnimeListWatchStatusProvider(self.myanimelist_xml_path)
                                                
                                                mal_status = self._mal_provider.get_watch_status(source)
                                                if mal_status:
                                                    title_metadata['myanimelist_watch_status'] = self._serialize_mal_watch_status(mal_status)
                                                break
                                    except Exception:
                                        pass
                                
                                self.title_metadata[metadata_id] = title_metadata
                        
                            # Add reference to title metadata by ID
                            result['metadata_id'] = metadata_id
                            
                            # Add episode info if it's a TV show
                            if metadata_id in self.title_metadata:
                                enhanced_info_dict = self.title_metadata[metadata_id]['metadata']
                                provider = self.title_metadata[metadata_id]['provider']
                                if enhanced_info_dict.get('type') in ['tv', 'anime_series']:
                                    season = result.get('season')
                                    episode = result.get('episode')
                                    
                                    # For anime series, try to get episode info even without explicit season
                                    if episode and provider and ('anime' in provider.lower() or enhanced_info_dict.get('type') == 'anime_series'):
                                        try:
                                            # Store original episode number from filename parsing
                                            original_episode = result.get('episode')
                                            original_season = result.get('season') 
                                            
                                            # Get anime provider for episode info lookup
                                            anime_provider = next((p for p in self.metadata_manager.providers if p.__class__.__name__ == provider), None)
                                            if anime_provider and hasattr(anime_provider, 'get_episode_info'):
                                                # Find title in anime provider to get the anime ID
                                                original_title = result.get('title')
                                                title_result = anime_provider.find_title(original_title, year)
                                                if title_result:
                                                    # Use anime provider's episode lookup with anime ID, season, and episode
                                                    episode_info = anime_provider.get_episode_info(title_result.info.id, original_season, original_episode)
                                                    if episode_info:
                                                        # Save original file-based episode number and season
                                                        result['original_episode'] = original_episode
                                                        if original_season:
                                                            result['original_season'] = original_season
                                                        
                                                        # Update with season and episode number from anime metadata
                                                        # Trust the metadata provider to correctly identify season/episode for each file
                                                        if episode_info.season:
                                                            result['season'] = episode_info.season
                                                        if episode_info.episode:
                                                            result['episode'] = episode_info.episode  # This is the in-season episode number
                                                        
                                                        # Store the enhanced episode info
                                                        if episode_info:
                                                            result['episode_info'] = self._serialize_episode_info(episode_info)
                                        except Exception as anime_error:
                                            # Fallback: if anime-specific parsing fails, continue without episode info
                                            pass
                                    elif season and episode and provider:
                                        # Non-anime series with explicit season/episode
                                        season_title = f"{title} Season {season}"
                                        enhanced_info, _ = self.metadata_manager.find_title(season_title, year)
                                        if enhanced_info:                                            
                                            episode_info = self.metadata_manager.get_episode_info(
                                                next(p for p in self.metadata_manager.providers if p.__class__.__name__ == provider),
                                                enhanced_info.id, season, episode
                                            )
                                            if episode_info:
                                                result['episode_info'] = self._serialize_episode_info(episode_info)
                    except Exception as metadata_error:
                        print(f"Warning: Enhanced metadata lookup failed for {file_path.name}: {metadata_error}")
            
            # Add Plex watch status if available
            if self.plex_provider and PlexMetadataProvider:
                try:
                    watch_status = self.plex_provider.get_watch_status(str(file_path))
                    if watch_status:
                        result['plex_watch_status'] = self._serialize_plex_watch_status(watch_status)
                except Exception as plex_error:
                    # Silently continue if Plex lookup fails (database might be locked, etc.)
                    pass

            # Derive per-episode watch status from title-level MyAnimeList data
            plex_watched = False
            mal_episode_watched = False
            
            if result.get('plex_watch_status'):
                plex_status = result['plex_watch_status']
                plex_watched = plex_status.get('watched', False) or plex_status.get('view_offset', 0) > 0
            
            # Check if this episode is watched according to MyAnimeList
            metadata_id = result.get('metadata_id')
            if metadata_id and metadata_id in self.title_metadata:
                mal_status = self.title_metadata[metadata_id].get('myanimelist_watch_status')
                if mal_status:
                    # Use original (absolute) episode number for MyAnimeList comparison, not in-season episode number
                    episode_num = result.get('original_episode') or result.get('episode')
                    mal_watched_episodes = mal_status.get('my_watched_episodes', 0)
                    if episode_num and episode_num <= mal_watched_episodes:
                        mal_episode_watched = True
            
            # Set combined episode watch status
            if plex_watched or mal_episode_watched:
                result['episode_watched'] = True
                result['watch_source'] = []
                if plex_watched:
                    result['watch_source'].append('plex')
                if mal_episode_watched:
                    result['watch_source'].append('myanimelist')
            else:
                result['episode_watched'] = False

            return result
        except Exception as e:
            print(f"Warning: Could not extract metadata from {file_path.name}: {e}")
            return {
                'filepath': str(file_path),
                'filename': file_path.name,
                'file_size': file_path.stat().st_size if file_path.exists() else 0
            }
    
    def group_files(self, files: List[Path], group_by: List[str] | None = None, show_progress: bool = True) -> Dict[str, List[Dict]]:
        """Group files based on specified metadata fields."""
        group_by = group_by or ['title', 'year']
        
        self.groups.clear()
        self.metadata.clear()
        self.group_metadata.clear()
        
        # Extract metadata with progress tracking
        with tqdm(files, desc="Extracting metadata", unit="file", disable=not show_progress) as pbar:
            for file_path in pbar:
                metadata = self.extract_metadata(file_path)
                self.metadata[str(file_path)] = metadata
                pbar.set_postfix(file=file_path.name[:30] + "..." if len(file_path.name) > 30 else file_path.name)
        
        # Group files with progress tracking
        with tqdm(files, desc="Grouping files", unit="file", disable=not show_progress) as pbar:
            for file_path in pbar:
                metadata = self.metadata[str(file_path)]
                # Create group key based on specified fields (case insensitive)
                group_key_parts = []
                for field in group_by:
                    value = metadata.get(field, 'Unknown')
                    if isinstance(value, list):
                        value = ', '.join(str(v) for v in value)
                    # Convert to lowercase for case insensitive grouping
                    value_str = str(value).lower() if value != 'Unknown' else 'Unknown'
                    group_key_parts.append(self._sanitize_group_key(f"{field}:{value_str}"))

                group_key = ' | '.join(group_key_parts)
                self.groups[group_key].append(metadata)
                pbar.set_postfix(groups=len(self.groups))

        # Get group metadata after all files are processed
        with tqdm(self.groups.items(), desc="Processing group metadata", unit="group", disable=not show_progress) as pbar:
            for group_key, group_files in pbar:
                group_metadata = self._get_group_metadata(group_files, group_by)
                if group_metadata:
                    self.group_metadata[group_key] = group_metadata
                pbar.set_postfix(group=group_key[:40] + "..." if len(group_key) > 40 else group_key)
        
        return dict(self.groups)
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary statistics of grouped files."""
        total_files = sum(len(files) for files in self.groups.values())
        total_size = sum(
            sum(file_info.get('file_size', 0) for file_info in files)
            for files in self.groups.values()
        )
        
        # Calculate watch status summary (include both Plex and MAL data)
        watched_files = 0
        total_watch_count = 0
        files_with_progress = 0
        mal_watched_files = 0
        
        for files in self.groups.values():
            for file_info in files:
                # Use combined episode watch status
                if file_info.get('episode_watched'):
                    watched_files += 1
                    watch_sources = file_info.get('watch_source', [])
                    if 'myanimelist' in watch_sources:
                        mal_watched_files += 1
                
                # Plex-specific stats
                plex_status = file_info.get('plex_watch_status')
                if plex_status:
                    total_watch_count += plex_status.get('watch_count', 0)
                    if plex_status.get('view_offset', 0) > 0:
                        files_with_progress += 1
        
        summary = {
            'total_files': total_files,
            'total_groups': len(self.groups),
            'total_size_bytes': total_size,
            'total_size_mb': round(total_size / (1024 * 1024), 2)
        }
        
        # Add watch status summary if any files have watch data
        if watched_files > 0 or total_watch_count > 0 or files_with_progress > 0 or mal_watched_files > 0:
            summary['watch_summary'] = {
                'watched_files': watched_files,
                'unwatched_files': total_files - watched_files,
                'total_watch_count': total_watch_count,
                'files_with_progress': files_with_progress,
                'mal_watched_files': mal_watched_files
            }
            # Keep plex_summary for backwards compatibility
            summary['plex_summary'] = {
                'watched_files': watched_files,
                'unwatched_files': total_files - watched_files,
                'total_watch_count': total_watch_count,
                'files_with_progress': files_with_progress
            }
        
        return summary
    
    def export_to_json(self, output_path: str, include_summary: bool = True) -> None:
        """Export grouped data to JSON file."""
        # Create title_metadata dict with complete metadata including MyAnimeList watch status
        # Now using metadata IDs directly as keys
        title_metadata_export = {}
        for metadata_id, value in self.title_metadata.items():
            # Export the complete metadata including MyAnimeList watch status
            metadata_dict = value['metadata'].copy()
            
            # Add MyAnimeList watch status if it exists at title level
            if 'myanimelist_watch_status' in value:
                metadata_dict['myanimelist_watch_status'] = value['myanimelist_watch_status']
            
            title_metadata_export[str(metadata_id)] = metadata_dict
        
        export_data = {
            'groups': dict(self.groups),
            'title_metadata': title_metadata_export
        }
        
        if include_summary:
            export_data['summary'] = self.get_summary()
            
            # Add group-level watch status summary using combined watch status
            group_summaries = {}
            for group_name, group_files in self.groups.items():
                group_watched = sum(1 for f in group_files if f.get('episode_watched', False))
                group_total_watches = 0
                group_with_progress = 0
                group_mal_watched = sum(1 for f in group_files if 'myanimelist' in f.get('watch_source', []))
                
                for file_info in group_files:
                    plex_status = file_info.get('plex_watch_status')
                    if plex_status:
                        group_total_watches += plex_status.get('watch_count', 0)
                        if plex_status.get('view_offset', 0) > 0:
                            group_with_progress += 1
                
                if group_watched > 0 or group_total_watches > 0 or group_with_progress > 0 or group_mal_watched > 0:
                    group_summaries[group_name] = {
                        'total_files': len(group_files),
                        'watched_files': group_watched,
                        'unwatched_files': len(group_files) - group_watched,
                        'total_watch_count': group_total_watches,
                        'files_with_progress': group_with_progress,
                        'mal_watched_files': group_mal_watched
                    }
            
            if group_summaries:
                export_data['group_watch_summaries'] = group_summaries
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False, cls=CustomJSONEncoder)
    
    def _serialize_title_info(self, title_info) -> Dict[str, Any]:
        """Convert TitleInfo object to serializable dict"""
        if not title_info:
            return {}
        return {
            'id': title_info.id,
            'title': title_info.title,
            'type': title_info.type,
            'year': title_info.year,
            'start_year': title_info.start_year,
            'end_year': title_info.end_year,
            'rating': title_info.rating,
            'votes': title_info.votes,
            'genres': title_info.genres,
            'tags': title_info.tags,
            'status': title_info.status,
            'total_episodes': title_info.total_episodes,
            'total_seasons': title_info.total_seasons,
            'sources': title_info.sources,
            'plot': title_info.plot
        }
    
    def _serialize_episode_info(self, episode_info) -> Dict[str, Any]:
        """Convert EpisodeInfo object to serializable dict"""
        if not episode_info:
            return {}
        return {
            'title': episode_info.title,
            'season': episode_info.season,
            'episode': episode_info.episode,
            'parent_id': episode_info.parent_id,
            'year': episode_info.year,
            'rating': episode_info.rating,
            'votes': episode_info.votes,
            'plot': episode_info.plot,
            'air_date': episode_info.air_date
        }
    
    def _serialize_plex_watch_status(self, watch_status: PlexWatchStatus) -> Dict[str, Any]:
        """Convert PlexWatchStatus object to serializable dict"""
        if not watch_status:
            return {}
        return {
            'watched': watch_status.watched,
            'watch_count': watch_status.watch_count,
            'last_watched': watch_status.last_watched.isoformat() if watch_status.last_watched else None,
            'view_offset': watch_status.view_offset,
            'duration': watch_status.duration,
            'progress_percent': round(watch_status.progress_percent, 1),
            'plex_title': watch_status.plex_title,
            'plex_year': watch_status.plex_year,
            'library_section': watch_status.library_section
        }
    
    def _serialize_mal_watch_status(self, watch_status: MyAnimeListWatchStatus) -> Dict[str, Any]:
        """Convert MyAnimeListWatchStatus object to serializable dict"""
        if not watch_status:
            return {}
        return {
            'series_animedb_id': watch_status.series_animedb_id,
            'series_title': watch_status.series_title,
            'my_status': watch_status.my_status,
            'my_watched_episodes': watch_status.my_watched_episodes,
            'my_score': watch_status.my_score,
            'my_start_date': watch_status.my_start_date,
            'my_finish_date': watch_status.my_finish_date,
            'my_times_watched': watch_status.my_times_watched,
            'my_rewatching': watch_status.my_rewatching,
            'series_episodes': watch_status.series_episodes,
            'progress_percent': round(watch_status.progress_percent, 1)
        }
    
    def _get_group_metadata(self, group_files: List[Dict], group_by: List[str]) -> Dict[str, Any]:
        """Get metadata for a group based on the first file's metadata or title metadata if grouping by title"""
        if not group_files:
            return {}
            
        # If grouping by title, add title metadata directly to the group
        if 'title' in group_by and self.metadata_manager:
            first_file = group_files[0]
            metadata_id = first_file.get('metadata_id')
            if metadata_id and metadata_id in self.title_metadata:
                return self.title_metadata[metadata_id]['metadata']
        
        return {}

    @staticmethod
    def _sanitize_group_key(key: str) -> str:
        """Remove problematic characters from group key for safe string handling."""
        # Remove quotes, backslashes, and other problematic characters
        return re.sub(r'[\'"\\\r\n\t\b\f]', '', key)

def main():
    """Command-line interface."""
    parser = argparse.ArgumentParser(
        description='Group files based on filename metadata using guessit',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s /path/to/movies
  %(prog)s /path/to/media --exclude-paths /path/to/media/trash
  %(prog)s /path/to/files --include-patterns "*.mkv" "*.mp4" --exclude-patterns "*sample*"
  %(prog)s /path/to/files --group-by title year season --export metadata.json
  %(prog)s /path/to/files --recursive -v 2 --export metadata.json
        """
    )
    
    parser.add_argument('input_paths', nargs='+', 
                       help='Input paths to search for files')
    parser.add_argument('--exclude-paths', nargs='*', default=[],
                       help='Paths to exclude from search')
    parser.add_argument('--include-patterns', nargs='*', default=['*'],
                       help='Wildcard patterns for files to include (default: *)')
    parser.add_argument('--exclude-patterns', nargs='*', default=[],
                       help='Wildcard patterns for files to exclude')
    parser.add_argument('--group-by', nargs='*', default=['title', 'year'],
                       help='Metadata fields to group by (default: title year)')
    parser.add_argument('--export', metavar='FILE',
                       help='Export results to JSON file')
    parser.add_argument('--recursive', '-r', action='store_true',
                       help='Recursively search subdirectories (default: False)')
    parser.add_argument('--verbose', '-v', type=int, choices=[0, 1, 2], default=1,
                       help='Verbosity level: 0=silent, 1=normal, 2=detailed with metadata (default: 1)')
    parser.add_argument('--quiet', '-q', action='store_true',
                       help='Same as --verbose 0')
    parser.add_argument('--myanimelist-xml', metavar='PATH_OR_URL',
                       help='Path to MyAnimeList XML file or URL for watch status lookup')
    
    args = parser.parse_args()
    
    # Handle quiet flag
    if args.quiet:
        verbosity = 0
    else:
        verbosity = args.verbose
    
    # Create file grouper instance
    grouper = FileGrouper(
        get_metadata_manager() if MetadataManager else None,
        get_plex_provider() if PlexMetadataProvider else None,
        args.myanimelist_xml if hasattr(args, 'myanimelist_xml') else None
    )
    
    # Discover files
    if verbosity >= 1:
        print("Discovering files...")
    
    files = grouper.discover_files(
        args.input_paths,
        args.exclude_paths,
        args.include_patterns,
        args.exclude_patterns,
        args.recursive
    )
    
    if not files:
        if verbosity >= 1:
            print("No files found matching criteria.")
        return
    
    if verbosity >= 1:
        print(f"Found {len(files)} files")
        print("Extracting metadata and grouping...")
    
    # Group files
    groups = grouper.group_files(files, args.group_by)
    
    # Display results
    if verbosity >= 1:
        summary = grouper.get_summary()
        print(f"\nSummary:")
        print(f"Total files: {summary['total_files']}")
        print(f"Total groups: {summary['total_groups']}")
        print(f"Total size: {summary['total_size_mb']} MB")
        
        # Display Plex watch status summary if available
        if 'plex_summary' in summary:
            plex_sum = summary['plex_summary']
            print(f"Watch Status: {plex_sum['watched_files']} watched, {plex_sum['unwatched_files']} unwatched")
            print(f"Total watches: {plex_sum['total_watch_count']}, Files with progress: {plex_sum['files_with_progress']}")
        
        print(f"\nGroups:")
        for group_name, group_files in groups.items():
            # Calculate group watch status using combined episode watch status
            group_watched = sum(1 for f in group_files if f.get('episode_watched', False))
            group_total_watches = sum(f.get('plex_watch_status', {}).get('watch_count', 0) for f in group_files)
            
            watch_info = ""
            if group_watched > 0 or group_total_watches > 0:
                watch_info = f" [Watch: {group_watched}/{len(group_files)} watched, {group_total_watches} total views]"
            
            print(f"\n{group_name} ({len(group_files)} files){watch_info}:")
            for file_info in group_files:
                size_mb = file_info.get('file_size', 0) / (1024 * 1024)
                
                # Add watch status to file display using combined status
                watch_display = ""
                if file_info.get('episode_watched'):
                    watch_sources = file_info.get('watch_source', [])
                    plex_status = file_info.get('plex_watch_status')
                    watch_count = plex_status.get('watch_count', 1) if plex_status else 1
                    
                    watch_display = f" [✓ Watched {watch_count}x"
                    if 'myanimelist' in watch_sources:
                        watch_display += " (MAL)"
                    if plex_status and plex_status.get('last_watched'):
                        last_watched = plex_status['last_watched'][:10]
                        watch_display += f" on {last_watched}"
                    watch_display += "]"
                elif file_info.get('plex_watch_status', {}).get('view_offset', 0) > 0:
                    progress = file_info['plex_watch_status'].get('progress_percent', 0)
                    watch_display = f" [⏸ {progress:.1f}% watched]"
                else:
                    watch_display = " [○ Unwatched]"
                
                print(f"  - {file_info['filename']} ({size_mb:.1f} MB){watch_display}")
                
                # Level 2 verbosity: show metadata as compact JSON
                if verbosity >= 2:
                    # Create a copy without filepath for cleaner output
                    metadata_copy = file_info.copy()
                    metadata_copy.pop('filepath', None)
                    metadata_copy.pop('filename', None)  # Already shown above  
                    
                    print(f"    {json.dumps(metadata_copy, separators=(',', ':'), ensure_ascii=False, cls=CustomJSONEncoder)}")
                    # Show enhanced metadata if available
                    metadata_id = file_info.get('metadata_id')
                    if metadata_id and metadata_id in grouper.title_metadata:
                        enhanced_data = grouper.title_metadata[metadata_id]['metadata']
                        print(f"    {json.dumps(enhanced_data, separators=(',', ':'), ensure_ascii=False, cls=CustomJSONEncoder)}")
    
    # Export if requested
    if args.export:
        grouper.export_to_json(args.export)
        if verbosity >= 1:
            print(f"Exported data to: {args.export}")


if __name__ == '__main__':
    main()
