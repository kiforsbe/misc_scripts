import argparse
import json
import os
from pathlib import Path
from typing import Dict, List, Any, Optional
import argparse
import json
import os

from video_thumbnail_generator import VideoThumbnailGenerator
from file_grouper import FileGrouper, CustomJSONEncoder

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

# Try to get metadata manager - it may not be available if dependencies aren't installed
try:
    from file_grouper import get_metadata_manager, get_plex_provider
    metadata_manager_available = True
    plex_provider_available = True
except ImportError:
    metadata_manager_available = False
    plex_provider_available = False
    def get_metadata_manager():
        return None
    def get_plex_provider():
        return None

# MetadataManager class may not be available as a direct import
MetadataManager = None

class SeriesCompletenessChecker:
    """Checks series collection completeness using FileGrouper and metadata providers."""
    
    def __init__(self, metadata_manager=None, plex_provider=None, myanimelist_xml_path=None, metadata_only=False):
        """Initialize the checker.
        
        Args:
            metadata_manager: Metadata manager for looking up series info
            plex_provider: Plex provider for watch status
            myanimelist_xml_path: Path to MyAnimeList XML file
            metadata_only: If True, skip FileGrouper initialization (for loading from JSON)
        """
        self.metadata_only = metadata_only
        if not metadata_only:
            self.file_grouper = FileGrouper(metadata_manager, plex_provider, myanimelist_xml_path)
        else:
            self.file_grouper = None
        self.metadata_manager = metadata_manager
        self.plex_provider = plex_provider
        self.myanimelist_xml_path = myanimelist_xml_path
        self.completeness_results = {}
    
    def load_results(self, input_path: str) -> Dict[str, Any]:
        """Load analysis results from a previously saved JSON file.
        
        Args:
            input_path: Path to the JSON file containing results
            
        Returns:
            Dictionary containing the loaded results
        """
        with open(input_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def analyze_series_collection(self, files: List[Path], show_progress: bool = True) -> Dict[str, Any]:
        """Analyze series collection for completeness."""
        if self.metadata_only or not self.file_grouper:
            raise RuntimeError("Cannot analyze files when initialized in metadata_only mode. Use load_results() instead.")
        
        # Group files by title and season with progress tracking
        groups = self.file_grouper.group_files(files, ['title', 'season'], show_progress)

        # Export title metadata for completeness analysis - include MyAnimeList watch status
        # Now using metadata IDs as keys, but we'll add season-specific entries after analysis
        title_metadata_export = {}
        for metadata_id, value in self.file_grouper.title_metadata.items():
            # Export the complete metadata including MyAnimeList watch status
            metadata_dict = value['metadata'].copy()
            
            # Add MyAnimeList watch status if it exists at title level
            if 'myanimelist_watch_status' in value:
                metadata_dict['myanimelist_watch_status'] = value['myanimelist_watch_status']
            
            title_metadata_export[str(metadata_id)] = metadata_dict
        
        # Analyze each group for completeness
        results = {
            'groups': {},
            'title_metadata': title_metadata_export,
            'completeness_summary': {
                'total_series': 0,
                'complete_series': 0,
                'incomplete_series': 0,
                'unknown_series': 0,
                'total_episodes_found': 0,
                'total_episodes_expected': 0
            }
        }
        
        # Analyze completeness with progress tracking
        with tqdm(groups.items(), desc="Analyzing completeness", unit="series", disable=not show_progress) as pbar:
            for group_key, group_files in pbar:
                analysis = self._analyze_group_completeness(group_key, group_files)
                results['groups'][group_key] = analysis
                
                # Update summary
                results['completeness_summary']['total_series'] += 1
                if analysis['status'] == 'complete':
                    results['completeness_summary']['complete_series'] += 1
                elif analysis['status'] == 'incomplete':
                    results['completeness_summary']['incomplete_series'] += 1
                else:
                    results['completeness_summary']['unknown_series'] += 1

                results['completeness_summary']['total_episodes_found'] += analysis['episodes_found']
                results['completeness_summary']['total_episodes_expected'] += analysis.get('episodes_expected', 0)
                
                # Update progress with current series name
                title = analysis.get('title', 'Unknown')[:30]
                if len(analysis.get('title', '')) > 30:
                    title += "..."
                pbar.set_postfix(current=title)
        
        # Find proper season-specific metadata IDs from the database
        for group_key, analysis in results['groups'].items():
            mal_status = analysis.get('myanimelist_watch_status')
            if mal_status and mal_status.get('_season_specific'):
                title = analysis.get('title')
                season = analysis.get('season')
                
                if title and season and self.metadata_manager:
                    # Query the metadata manager to find the proper season-specific entry
                    try:
                        # For season > 1, try to find season-specific title
                        if season > 1:
                            season_title = f"{title} Season {season}"
                            enhanced_info, provider = self.metadata_manager.find_title(season_title)
                            if not enhanced_info:
                                # Try alternative formats
                                season_title = f"{title} {season}"
                                enhanced_info, provider = self.metadata_manager.find_title(season_title)
                        else:
                            # For season 1, use the original title
                            enhanced_info, provider = self.metadata_manager.find_title(title)
                        
                        if enhanced_info:
                            # Use the proper metadata ID from the database
                            season_metadata_id = enhanced_info.id
                            
                            # Create or update the metadata entry
                            season_metadata = {
                                'id': season_metadata_id,
                                'title': enhanced_info.title,
                                'type': getattr(enhanced_info, 'type', 'anime_series'),
                                'year': getattr(enhanced_info, 'year', None),
                                'myanimelist_watch_status': mal_status,
                                '_season_number': season
                            }
                            
                            # Add any additional metadata from enhanced_info
                            for attr in ['genres', 'rating', 'plot', 'sources']:
                                if hasattr(enhanced_info, attr):
                                    season_metadata[attr] = getattr(enhanced_info, attr)
                            
                            # Add to title_metadata
                            title_metadata_export[str(season_metadata_id)] = season_metadata
                            
                            # Update the group to reference the proper metadata ID
                            analysis['title_id'] = season_metadata_id
                            if 'myanimelist_watch_status' in analysis:
                                analysis['myanimelist_watch_status']['series_animedb_id'] = season_metadata_id
                        
                    except Exception as e:
                        print(f"Warning: Could not find season-specific metadata for {title} Season {season}: {e}")
                        # Keep the original metadata_id as fallback
                        pass
        
        # Update the results with the enhanced title_metadata
        results['title_metadata'] = title_metadata_export
        
        return results
    
    def _analyze_group_completeness(self, group_key: str, group_files: List[Dict]) -> Dict[str, Any]:
        """Analyze a single group for completeness."""
        if not group_files:
            return {'status': 'unknown', 'episodes_found': 0}
        
        # Extract basic info from group
        first_file = group_files[0]
        title = first_file.get('title', 'Unknown')
        season = first_file.get('season')

        # Enhanced episode parsing using existing metadata system + direct anime metadata fallback
        episode_files = []
        extra_files = []
        episode_numbers = []
        enhanced_episode_info = {}  # Store enhanced metadata for each file
        anime_metadata_available = False
        
        # Try to get anime metadata system for direct parsing if needed
        anime_metadata = None
        try:
            # Check if we have anime providers in the metadata manager
            if self.metadata_manager and hasattr(self.metadata_manager, 'providers'):
                for provider in self.metadata_manager.providers:
                    if 'anime' in provider.__class__.__name__.lower():
                        anime_metadata = provider
                        break
        except:
            pass
        
        for file_info in group_files:
            # Check if this file already has enhanced episode info from FileGrouper
            episode_info = file_info.get('episode_info')
            if episode_info:
                # File already has enhanced episode information
                enhanced_episode_info[file_info.get('file_path', '')] = episode_info
                anime_metadata_available = True
                
                # Use the enhanced episode info to classify files
                if episode_info.get('episode_type') in ['OP', 'ED', 'Special', 'OVA']:
                    extra_files.append(file_info)
                else:
                    episode_files.append(file_info)
                    ep_num = episode_info.get('episode')  # Fixed: use 'episode' not 'episode_number'
                    if ep_num:
                        episode_numbers.append(ep_num)
            else:
                # Try direct anime metadata parsing if FileGrouper didn't extract episode info
                file_path = file_info.get('file_path', '')
                if isinstance(file_path, Path):
                    filename = file_path.name
                else:
                    filename = str(file_path).split('/')[-1] if '/' in str(file_path) else str(file_path).split('\\')[-1]
                
                direct_episode_info = None
                if anime_metadata and hasattr(anime_metadata, 'get_episode_info'):
                    try:
                        direct_episode_info = anime_metadata.get_episode_info(filename)
                        if direct_episode_info:
                            enhanced_episode_info[filename] = direct_episode_info
                            anime_metadata_available = True
                    except:
                        pass
                
                if direct_episode_info:
                    # Use direct anime metadata
                    if direct_episode_info.get('episode_type') in ['OP', 'ED', 'Special', 'OVA']:
                        extra_files.append(file_info)
                    else:
                        episode_files.append(file_info)
                        ep_num = direct_episode_info.get('episode')  # Fixed: use 'episode' not 'episode_number'
                        if ep_num:
                            episode_numbers.append(ep_num)
                else:
                    # Fallback to original guessit-based logic
                    file_type = file_info.get('type')
                    if isinstance(file_type, list):
                        if 'extra' in [t.lower() for t in file_type]:
                            extra_files.append(file_info)
                        else:
                            episode_files.append(file_info)
                            episode = file_info.get('episode')
                            if isinstance(episode, list):
                                episode_numbers.extend(episode)
                            elif episode is not None:
                                episode_numbers.append(episode)
                    elif isinstance(file_type, str) and file_type.lower() == 'extra':
                        extra_files.append(file_info)
                    else:
                        episode_files.append(file_info)
                        episode = file_info.get('episode')
                        if isinstance(episode, list):
                            episode_numbers.extend(episode)
                        elif episode is not None:
                            episode_numbers.append(episode)

        episodes_found = len(episode_files)
        episode_numbers = sorted(set(episode_numbers)) if episode_numbers else []
        
        # Calculate watch status for the group using combined episode watch status
        watched_count = sum(1 for f in episode_files if f.get('episode_watched', False))
        partially_watched_count = 0
        total_watch_count = 0
        mal_watch_status = None
        
        # Get title-level MyAnimeList watch status and calculate season-specific status
        metadata_id = first_file.get('metadata_id')
        if metadata_id and hasattr(self, 'file_grouper'):
            title_metadata = getattr(self.file_grouper, 'title_metadata', {})
            if metadata_id in title_metadata:
                series_mal_status = title_metadata[metadata_id].get('myanimelist_watch_status')
                if series_mal_status and season:
                    # Calculate season-specific MyAnimeList watch status
                    mal_watch_status = self._calculate_season_specific_mal_status(
                        series_mal_status, season, episode_files
                    )
                else:
                    mal_watch_status = series_mal_status
        
        # Count partially watched and total watch count from Plex
        for file_info in episode_files:
            plex_status = file_info.get('plex_watch_status')
            if plex_status:
                total_watch_count += plex_status.get('watch_count', 0)
                if not file_info.get('episode_watched', False) and plex_status.get('view_offset', 0) > 0:
                    partially_watched_count += 1
        
        result = {
            'title': title,
            'season': season,
            'episodes_found': episodes_found,
            'episodes_expected': 0,
            'status': 'unknown',
            'episode_numbers': episode_numbers,
            'missing_episodes': [],
            'extra_episodes': [],
            'files': group_files,
            'extra_files': extra_files,
            'watch_status': {
                'watched_episodes': watched_count,
                'partially_watched_episodes': partially_watched_count,
                'unwatched_episodes': episodes_found - watched_count - partially_watched_count,
                'total_watch_count': total_watch_count,
                'completion_percent': (watched_count / episodes_found * 100) if episodes_found > 0 else 0
            },
            'myanimelist_watch_status': mal_watch_status
        }

        # Check metadata for expected episode count
        if self.metadata_manager:
            metadata_id = first_file.get('metadata_id')
            if metadata_id and metadata_id in self.file_grouper.title_metadata:
                metadata = self.file_grouper.title_metadata[metadata_id]['metadata']

                # --- Check for "movie" type in file or metadata ---
                file_type = first_file.get('type', '')
                metadata_type = metadata.get('type', '')
                file_type_str = ''
                if isinstance(file_type, list):
                    file_type_str = ' '.join(str(t).lower() for t in file_type)
                elif isinstance(file_type, str):
                    file_type_str = file_type.lower()
                metadata_type_str = str(metadata_type).lower()
                if "movie" in file_type_str or "movie" in metadata_type_str:
                    total_episodes = metadata.get('total_episodes')
                    expected_episodes = total_episodes
                    result['status'] = 'movie'
                    result['episodes_expected'] = expected_episodes or 1
                    return result
                # --- end movie check ---

                if 'series' or 'tv' in metadata.get('type', '').lower():
                    # For series series, check total episodes
                    total_episodes = metadata.get('total_episodes')
                    if total_episodes:
                        result['episodes_expected'] = total_episodes
                        
                        # Use enhanced metadata to determine expected episodes for this specific season
                        enhanced_info, provider = self.metadata_manager.find_title(title)
                        if enhanced_info and provider and hasattr(provider, 'get_episode_info') and episode_numbers:
                            # Find the total episodes for this specific season by checking if there are more episodes
                            max_episode_in_season = max(episode_numbers)
                            
                            # Check if there's a next episode after our max to determine season completion
                            # For season-based anime, check if the next episode would be in a different season
                            next_episode_original = None
                            # Try to find the original episode number that corresponds to our max in-season episode
                            for file_info in group_files:
                                if file_info.get('episode') == max_episode_in_season:
                                    next_episode_original = file_info.get('original_episode', 0) + 1
                                    break
                            
                            expected_episodes = max_episode_in_season  # Default to what we have
                            if next_episode_original:
                                # Check if the next episode exists and is in the same season
                                next_episode_info = provider.get_episode_info(enhanced_info.id, None, next_episode_original)
                                if next_episode_info and next_episode_info.season == season:
                                    # Only add it as expected if it's within the total episode count for the series
                                    series_total_episodes = enhanced_info.total_episodes if enhanced_info else None
                                    if series_total_episodes and next_episode_original <= series_total_episodes:
                                        # Next episode is in same season and within series bounds, so we're missing it
                                        expected_episodes = max_episode_in_season + 1
                            
                            result['enhanced_metadata'] = {
                                'title': enhanced_info.title,
                                'total_episodes': expected_episodes,
                                'provider': provider.__class__.__name__ if provider else None
                            }
                        else:
                            # Fallback: use the maximum episode number we found as expected
                            expected_episodes = max(episode_numbers) if episode_numbers else total_episodes
                        
                        result['episodes_expected'] = expected_episodes
                        
                        # Check for missing episodes
                        if episode_numbers:
                            max_episode = max(episode_numbers)
                            expected_range = list(range(1, int(expected_episodes) + 1))
                            missing = [ep for ep in expected_range if ep not in episode_numbers]
                            extra = [ep for ep in episode_numbers if ep > expected_episodes]
                            
                            result['missing_episodes'] = missing
                            result['extra_episodes'] = extra
                            
                            if not missing and not extra:
                                result['status'] = 'complete'
                            elif missing:
                                result['status'] = 'incomplete'
                            else:
                                result['status'] = 'complete_with_extras'
                        else:
                            result['status'] = 'no_episode_numbers'
                    else:
                        result['status'] = 'unknown_total_episodes'
                else:
                    result['status'] = 'not_series'
            else:
                result['status'] = 'no_metadata'
        else:
            result['status'] = 'no_metadata_manager'
        
        return result
    
    def _calculate_season_specific_mal_status(self, series_mal_status: Dict[str, Any], season: int, episode_files: List[Dict]) -> Optional[Dict[str, Any]]:
        """Calculate season-specific MyAnimeList watch status based on which episodes in this season are watched.
        Only fills in missing values - does not override existing MAL data."""
        if not series_mal_status:
            return None
        
        # Create a copy of the series MAL status to modify
        season_mal_status = series_mal_status.copy()
        
        # Only calculate and override values if they are missing or blank
        # Preserve existing MAL data when it exists
        existing_status = season_mal_status.get('my_status')
        existing_watched = season_mal_status.get('my_watched_episodes')
        existing_episodes = season_mal_status.get('series_episodes')
        
        # If we already have complete MAL data, don't override it
        if existing_status and existing_watched is not None and existing_episodes:
            # Just add metadata about it being season-specific if needed
            if season and not season_mal_status.get('_season_specific'):
                season_mal_status['_season_specific'] = True
                season_mal_status['_original_series_status'] = existing_status
                season_mal_status['_original_series_watched'] = existing_watched
            return season_mal_status
        
        # Only calculate values that are missing
        total_episodes_in_season = len(episode_files)
        
        # Count how many episodes in this season are watched according to MAL
        mal_watched_in_season = 0
        for file_info in episode_files:
            if file_info.get('episode_watched', False):
                watch_source = file_info.get('watch_source', [])
                if 'myanimelist' in watch_source:
                    mal_watched_in_season += 1
        
        # Only fill in missing values
        if not existing_status:
            # Calculate season-specific status
            if mal_watched_in_season == 0:
                season_mal_status['my_status'] = 'Plan to Watch'
            elif mal_watched_in_season == total_episodes_in_season:
                season_mal_status['my_status'] = 'Completed'
            else:
                season_mal_status['my_status'] = 'Watching'
        
        if existing_watched is None:
            season_mal_status['my_watched_episodes'] = mal_watched_in_season
        
        if not existing_episodes:
            season_mal_status['series_episodes'] = total_episodes_in_season
        
        # Calculate progress percentage if missing
        if 'progress_percent' not in season_mal_status:
            watched = season_mal_status.get('my_watched_episodes', 0)
            total = season_mal_status.get('series_episodes', total_episodes_in_season)
            season_mal_status['progress_percent'] = (watched / total * 100) if total > 0 else 0
        
        # Add a note indicating this is season-specific vs series-wide
        season_mal_status['_season_specific'] = True
        season_mal_status['_original_series_status'] = series_mal_status.get('my_status')
        season_mal_status['_original_series_watched'] = series_mal_status.get('my_watched_episodes')
        
        return season_mal_status

    def _format_episode_ranges(self, episodes: List[int]) -> str:
        """Format episode list as smart ranges (e.g., [1,2,3,5,6,8] -> '[1-3, 5-6, 8]')."""
        if not episodes:
            return ""
        
        sorted_episodes = sorted(episodes)
        ranges = []
        start = sorted_episodes[0]
        end = start
        
        for i in range(1, len(sorted_episodes)):
            if sorted_episodes[i] == end + 1:
                end = sorted_episodes[i]
            else:
                if start == end:
                    ranges.append(str(start))
                else:
                    ranges.append(f"{start}-{end}")
                start = end = sorted_episodes[i]
        
        # Add the last range
        if start == end:
            ranges.append(str(start))
        else:
            ranges.append(f"{start}-{end}")
        
        return f"[{', '.join(ranges)}]"
    
    def export_results(self, results: Dict[str, Any], output_path: str) -> None:
        """Export analysis results to JSON file."""
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False, cls=CustomJSONEncoder)
    
    def export_webapp(self, results: Dict[str, Any], output_path: str, use_relative_thumbnails: bool = False, thumbnail_relative_path: str = None) -> None:
        """Export analysis results as a standalone HTML webapp."""
        import os
        from pathlib import Path
        
        # Get the directory of this script to find template files
        script_dir = Path(__file__).parent

        # Read template files
        html_template_path = script_dir / 'series_completeness_webapp_template.html'
        css_template_path = script_dir / 'series_completeness_webapp_template.css'
        js_template_path = script_dir / 'series_completeness_webapp_template.js'

        try:
            with open(html_template_path, 'r', encoding='utf-8') as f:
                html_template = f.read()
            with open(css_template_path, 'r', encoding='utf-8') as f:
                css_content = f.read()
            with open(js_template_path, 'r', encoding='utf-8') as f:
                js_content = f.read()
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Template file not found: {e}. Make sure all template files are in the same directory as this script.")

        # --- Merge thumbnail index into results ---
        # Try to find the thumbnail index JSON in the thumbnail dir (default or from config)
        thumbnail_dir = results.get('thumbnail_dir') or os.path.expanduser('~/.video_thumbnail_cache')
        thumbnail_index_path = os.path.join(thumbnail_dir, 'thumbnail_index.json')
        thumbnail_index = []
        if os.path.exists(thumbnail_index_path):
            try:
                with open(thumbnail_index_path, 'r', encoding='utf-8') as tf:
                    loaded = json.load(tf)
                    if isinstance(loaded, list):
                        # Convert absolute paths to relative paths if needed for bundle export
                        if use_relative_thumbnails and thumbnail_relative_path:
                            # Make a deep copy to avoid modifying the original
                            import copy
                            thumbnail_index = copy.deepcopy(loaded)
                            for entry in thumbnail_index:
                                if entry.get('static_thumbnail'):
                                    # Use forward slashes for web compatibility
                                    rel_path = thumbnail_relative_path + '/' + os.path.basename(entry['static_thumbnail'])
                                    entry['static_thumbnail'] = rel_path.replace('\\', '/')
                                if entry.get('animated_thumbnail'):
                                    # Use forward slashes for web compatibility
                                    rel_path = thumbnail_relative_path + '/' + os.path.basename(entry['animated_thumbnail'])
                                    entry['animated_thumbnail'] = rel_path.replace('\\', '/')
                        else:
                            # Keep absolute paths for non-bundle mode
                            thumbnail_index = loaded
            except Exception as e:
                thumbnail_index = []
                # Optionally print warning
                # print(f"Warning: Could not load thumbnail index: {e}")
        results['thumbnails'] = thumbnail_index
        # Prepare data for embedding (minify JSON)
        json_data = json.dumps(results, separators=(',', ':'), cls=CustomJSONEncoder)
        
        # Replace placeholders in HTML template
        html_content = html_template.replace('[[embedded_css]]', css_content)
        html_content = html_content.replace('[[embedded_js]]', js_content)
        html_content = html_content.replace('[[embedded_json]]', json_data)
        
        # Write the final HTML file
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
    
    def export_bundle(self, results: Dict[str, Any], bundle_path: str, verbosity: int = 1) -> None:
        """Export a complete bundle with webapp, metadata JSON, and thumbnails.
        
        Note: Thumbnails should already be generated in the bundle's thumbnails folder.
        
        Args:
            results: Analysis results to export
            bundle_path: Root directory for the bundle
            verbosity: Verbosity level for logging
        """
        bundle_root = Path(bundle_path)
        bundle_root.mkdir(parents=True, exist_ok=True)
        
        if verbosity >= 1:
            print(f"\nExporting bundle to {bundle_root}...")
        
        # Define paths
        metadata_path = bundle_root / 'metadata.json'
        webapp_path = bundle_root / 'webapp.html'
        thumbnails_dir = bundle_root / 'thumbnails'
        
        # Export metadata.json
        if verbosity >= 1:
            print(f"  Writing metadata to {metadata_path}...")
        self.export_results(results, str(metadata_path))
        
        # Count thumbnails already in bundle directory
        if thumbnails_dir.exists():
            thumbnail_files = list(thumbnails_dir.glob('*.webp'))
            if verbosity >= 1:
                print(f"  Bundle contains {len(thumbnail_files)} thumbnail files")
        
        # Export webapp with relative thumbnail paths
        if verbosity >= 1:
            print(f"  Writing webapp to {webapp_path}...")
        self.export_webapp(results, str(webapp_path), use_relative_thumbnails=True, thumbnail_relative_path='thumbnails')
        
        if verbosity >= 1:
            print(f"\nBundle export complete!")
            print(f"  Bundle location: {bundle_root}")
            print(f"  Open {webapp_path} in a web browser to view")
    
    def _format_metadata_value(self, value: Any, max_length: int = 20) -> str:
        """Format a metadata value for display with smart truncation."""
        if value is None:
            return "N/A"
        
        if isinstance(value, (list, tuple)):
            if not value:
                return "N/A"
            # Join list items with commas, truncate if needed
            formatted = ", ".join(str(item) for item in value)
        elif isinstance(value, dict):
            # For dict, show key count or first few keys
            if not value:
                return "N/A"
            formatted = f"{{{len(value)} keys}}"
        elif isinstance(value, bool):
            formatted = "Yes" if value else "No"
        elif isinstance(value, (int, float)):
            formatted = str(value)
        else:
            formatted = str(value)
        
        # Truncate if too long
        if len(formatted) > max_length:
            formatted = formatted[:max_length - 3] + "...";
        
        return formatted
    
    def print_summary(self, results: Dict[str, Any], verbosity: int = 1, show_metadata_fields: List[str] | None = None) -> None:
        """Print completeness summary."""
        summary = results['completeness_summary']

        print(f"\n=== Series Collection Completeness Summary ===")
        print(f"Total series titles: {summary['total_series']}")
        print(f"Complete: {summary['complete_series']}")
        print(f"Incomplete: {summary['incomplete_series']}")
        print(f"Unknown status: {summary['unknown_series']}")
        print(f"Episodes found: {summary['total_episodes_found']}")
        print(f"Episodes expected: {summary['total_episodes_expected']}")
        
        if summary['total_episodes_expected'] > 0:
            completion_rate = (summary['total_episodes_found'] / summary['total_episodes_expected']) * 100
            print(f"Collection completion rate: {completion_rate:.1f}%")
        
        # Add combined watch status summary (Plex + MAL)
        total_watched = 0
        total_episodes = 0
        total_partially_watched = 0
        mal_series_count = 0
        
        for analysis in results['groups'].values():
            watch_status = analysis.get('watch_status', {})
            total_watched += watch_status.get('watched_episodes', 0)
            total_episodes += analysis.get('episodes_found', 0)
            total_partially_watched += watch_status.get('partially_watched_episodes', 0)
            if analysis.get('myanimelist_watch_status'):
                mal_series_count += 1
        
        if total_watched > 0 or total_partially_watched > 0:
            print(f"\n=== Watch Status Summary ===")
            print(f"Watched episodes: {total_watched}/{total_episodes} ({total_watched/total_episodes*100:.1f}%)")
            print(f"Partially watched: {total_partially_watched}")
            print(f"Unwatched episodes: {total_episodes - total_watched - total_partially_watched}")
            if mal_series_count > 0:
                print(f"Series with MyAnimeList data: {mal_series_count}")
        
        # One-line summary for each series
        if verbosity >= 1:
            print(f"\n=== Series ===")
            for group_key, analysis in sorted(results['groups'].items()):
                self._print_one_line_summary(analysis, show_metadata_fields)
    
    def _print_one_line_summary(self, analysis: Dict[str, Any], show_metadata_fields: List[str] | None = None) -> None:
        """Print a concise one-line summary for a series."""
        status = analysis['status']
        title = analysis['title']
        season = analysis.get('season')
        episodes_found = analysis['episodes_found']
        episodes_expected = analysis.get('episodes_expected', 0)
        watch_status = analysis.get('watch_status', {})
        
        # Status emoji
        status_emoji = {
            'complete': '✅',
            'incomplete': '❌', 
            'complete_with_extras': '⚠️',
            'no_episode_numbers': '❓',
            'unknown_total_episodes': '❓',
            'not_series': 'ℹ️',
            'movie': '🎬',
            'no_metadata': '❓',
            'no_metadata_manager': '❓',
            'unknown': '❓'
        }.get(status, '❓')
        
        # Format title with season
        title_str = title
        if season:
            title_str += f" S{season:02d}"

        # Adjust title length based on whether metadata will be shown
        base_title_length = 60  # Reduced to make room for watch status
        metadata_space = 0
        
        if show_metadata_fields:
            # Reserve space for metadata (estimate ~15 chars per field)
            metadata_space = len(show_metadata_fields) * 15
            title_length = max(20, base_title_length - metadata_space // 2)
        else:
            title_length = base_title_length

        # Truncate title to maximum title_length characters with ellipsis
        if len(title_str) > title_length:
            title_str = title_str[:title_length - 3] + "..."
        
        # Add episode info (watched, missing, extra) using combined watch status
        extra_info = []
        
        # Add watched episodes info - show actual watched episode numbers
        watched_episodes = watch_status.get('watched_episodes', 0)
        if watched_episodes > 0:
            # Always show the actual watched episode numbers, not just the count
            watched_episode_nums = []
            if analysis.get('files'):
                for file_info in analysis['files']:
                    if file_info.get('episode_watched'):
                        episode = file_info.get('episode')
                        if isinstance(episode, list):
                            watched_episode_nums.extend(episode)
                        elif episode is not None:
                            watched_episode_nums.append(episode)
            
            if watched_episode_nums:
                watched_range = self._format_episode_ranges(sorted(set(watched_episode_nums)))
                mal_status = analysis.get('myanimelist_watch_status')
                if mal_status:
                    extra_info.append(f"Watched: {watched_range} (Combined)")
                else:
                    extra_info.append(f"Watched: {watched_range}")

        if analysis.get('missing_episodes'):
            missing_range = self._format_episode_ranges(analysis['missing_episodes'])
            extra_info.append(f"Missing: {missing_range}")
        
        if analysis.get('extra_episodes'):
            extra_range = self._format_episode_ranges(analysis['extra_episodes'])
            extra_info.append(f"Extra: {extra_range}")
        
        # Add metadata fields if requested
        metadata_info = []
        if show_metadata_fields and analysis.get('files'):
            # Get metadata from the first file's title metadata
            first_file = analysis['files'][0]
            metadata_id = first_file.get('metadata_id')
            
            if metadata_id and hasattr(self, 'file_grouper'):
                title_metadata = getattr(self.file_grouper, 'title_metadata', {})
                if metadata_id in title_metadata:
                    metadata = title_metadata[metadata_id]['metadata']
                    
                    for field in show_metadata_fields:
                        value = metadata.get(field)
                        formatted_value = self._format_metadata_value(value, max_length=12)
                        metadata_info.append(f"{field.capitalize()}: {formatted_value}")
        
        # Build the complete line
        episodes_expected_str = str(episodes_expected) if episodes_expected else '?'
        
        # Combine all info parts
        all_info = metadata_info + extra_info # join lists
        extra_info_str = f" | {', '.join(all_info)}" if all_info else ""
        
        line = f"{status_emoji} {title_str:<{title_length}} {episodes_found:>4}/{episodes_expected_str:<4}{extra_info_str}"
        
        print(line)

def _handle_refresh_bundle_mode(checker: 'SeriesCompletenessChecker', bundle_dir: str, verbosity: int) -> None:
    """Handle --refresh-bundle mode: regenerate webapp from existing bundle metadata."""
    bundle_root = Path(bundle_dir)
    metadata_path = bundle_root / 'metadata.json'
    
    if not metadata_path.exists():
        print(f"Error: metadata.json not found in bundle directory: {metadata_path}")
        return
    
    if verbosity >= 1:
        print(f"Loading metadata from {metadata_path}...")
    
    results = checker.load_results(str(metadata_path))
    
    if verbosity >= 1:
        print(f"Regenerating bundle webapp...")
    
    webapp_path = bundle_root / 'webapp.html'
    checker.export_webapp(results, str(webapp_path), use_relative_thumbnails=True, thumbnail_relative_path='thumbnails')
    
    if verbosity >= 1:
        print(f"✓ Bundle webapp regenerated: {webapp_path}")

def _handle_refresh_webapp_mode(checker: 'SeriesCompletenessChecker', json_path: str, output_path: str, verbosity: int) -> None:
    """Handle --webapp-refresh mode: regenerate standalone webapp from JSON."""
    json_file = Path(json_path)
    
    if not json_file.exists():
        print(f"Error: JSON file not found: {json_file}")
        return
    
    if verbosity >= 1:
        print(f"Loading metadata from {json_file}...")
    
    results = checker.load_results(str(json_file))
    
    # Determine output path (default to same directory as JSON with .html extension)
    if output_path:
        output_file = Path(output_path)
    else:
        output_file = json_file.with_suffix('.html')
    
    if verbosity >= 1:
        print(f"Regenerating standalone webapp...")
    
    checker.export_webapp(results, str(output_file))
    
    if verbosity >= 1:
        print(f"✓ Webapp regenerated: {output_file}")

def _handle_thumbnail_generation(files: List[Path], args, verbosity: int) -> Optional[str]:
    """Handle thumbnail generation and return the thumbnail directory path."""
    should_generate_thumbnails = args.generate_thumbnails
    thumbnail_dir = args.thumbnail_dir
    
    # If using bundle mode, override thumbnail settings
    if hasattr(args, 'export_bundle') and args.export_bundle:
        should_generate_thumbnails = True
        bundle_root = Path(args.export_bundle)
        thumbnail_dir = str(bundle_root / 'thumbnails')
    
    if not should_generate_thumbnails:
        return None
    
    thumbnail_dir_expanded = os.path.expanduser(thumbnail_dir)
    
    # If using bundle mode, check global cache first
    if hasattr(args, 'export_bundle') and args.export_bundle:
        import shutil
        global_cache_dir = os.path.expanduser('~/.video_thumbnail_cache')
        if verbosity >= 1:
            print(f"Checking global cache at {global_cache_dir} for existing thumbnails...")
        
        # Create the bundle thumbnails directory
        os.makedirs(thumbnail_dir_expanded, exist_ok=True)
        
        # Check global cache and copy existing thumbnails
        global_generator = VideoThumbnailGenerator(global_cache_dir, max_height=480)
        copied_from_cache = 0
        
        for file_info in files:
            file_path = file_info if isinstance(file_info, (str, Path)) else file_info.get('path')
            existing = global_generator.get_thumbnail_for_video(str(file_path))
            
            # If thumbnails exist in cache, copy them to bundle
            if existing.get('static_thumbnail') and existing.get('animated_thumbnail'):
                try:
                    static_dest = os.path.join(thumbnail_dir_expanded, os.path.basename(existing['static_thumbnail']))
                    animated_dest = os.path.join(thumbnail_dir_expanded, os.path.basename(existing['animated_thumbnail']))
                    
                    shutil.copy2(existing['static_thumbnail'], static_dest)
                    shutil.copy2(existing['animated_thumbnail'], animated_dest)
                    copied_from_cache += 1
                except Exception as e:
                    if verbosity >= 2:
                        print(f"Could not copy cached thumbnails for {file_path}: {e}")
        
        if verbosity >= 1 and copied_from_cache > 0:
            print(f"Copied {copied_from_cache} thumbnail pairs from global cache")
    
    # Generate thumbnails (will skip files that already have thumbnails in target dir)
    generator = VideoThumbnailGenerator(thumbnail_dir_expanded, max_height=480)
    thumbnail_index = generator.generate_thumbnails_for_videos(
        files, verbose=verbosity, force_regenerate=False, show_progress=(verbosity >= 1)
    )
    generator.save_thumbnail_index(thumbnail_index, verbose=verbosity)
    
    return thumbnail_dir_expanded

def _apply_status_filters(results: Dict[str, Any], status_filters: List[str]) -> None:
    """Apply status filters to results and update summary."""
    all_statuses = {'complete', 'incomplete', 'complete_with_extras', 'no_episode_numbers', 
                   'unknown_total_episodes', 'not_series', 'no_metadata', 'no_metadata_manager', 'unknown'}
    
    # Parse include/exclude patterns
    include_statuses = set()
    exclude_statuses = set()
    plain_statuses = set()
    
    for filter_item in status_filters:
        if filter_item.startswith('+'):
            status = filter_item[1:]
            if status in all_statuses:
                include_statuses.add(status)
        elif filter_item.startswith('-'):
            status = filter_item[1:]
            if status in all_statuses:
                exclude_statuses.add(status)
        elif filter_item in all_statuses:
            plain_statuses.add(filter_item)
    
    # Determine final filter set
    if plain_statuses:
        final_statuses = plain_statuses
    elif include_statuses:
        final_statuses = include_statuses - exclude_statuses
    elif exclude_statuses:
        final_statuses = all_statuses - exclude_statuses
    else:
        final_statuses = all_statuses
    
    # Apply filtering
    filtered_groups = {}
    for group_key, analysis in results['groups'].items():
        if analysis['status'] in final_statuses:
            filtered_groups[group_key] = analysis
    results['groups'] = filtered_groups
    
    # Recalculate summary
    _recalculate_summary(results)

def _apply_mal_status_filters(results: Dict[str, Any], mal_status_filters: List[str]) -> None:
    """Apply MyAnimeList status filters to results and update summary."""
    mal_status_map = {
        'watching': 'Watching',
        'completed': 'Completed', 
        'on-hold': 'On-Hold',
        'dropped': 'Dropped',
        'plan-to-watch': 'Plan to Watch'
    }
    all_mal_statuses = set(mal_status_map.keys())
    
    # Parse include/exclude patterns
    include_mal_statuses = set()
    exclude_mal_statuses = set()
    plain_mal_statuses = set()
    
    for filter_item in mal_status_filters:
        if filter_item.startswith('+'):
            status = filter_item[1:]
            if status in all_mal_statuses:
                include_mal_statuses.add(status)
        elif filter_item.startswith('-'):
            status = filter_item[1:]
            if status in all_mal_statuses:
                exclude_mal_statuses.add(status)
        elif filter_item in all_mal_statuses:
            plain_mal_statuses.add(filter_item)
    
    # Determine final filter set
    if plain_mal_statuses:
        final_mal_statuses = plain_mal_statuses
    elif include_mal_statuses:
        final_mal_statuses = include_mal_statuses - exclude_mal_statuses
    elif exclude_mal_statuses:
        final_mal_statuses = all_mal_statuses - exclude_mal_statuses
    else:
        final_mal_statuses = all_mal_statuses
    
    # Convert to actual MAL status values for filtering
    final_mal_status_values = {mal_status_map[status] for status in final_mal_statuses}
    
    # Apply filtering
    filtered_groups = {}
    for group_key, analysis in results['groups'].items():
        mal_status = analysis.get('myanimelist_watch_status')
        if mal_status and mal_status.get('my_status'):
            if mal_status['my_status'] in final_mal_status_values:
                filtered_groups[group_key] = analysis
        elif not mal_status:
            # Series has no MAL status - only include if no positive filters specified
            if not plain_mal_statuses and not include_mal_statuses:
                filtered_groups[group_key] = analysis
    
    results['groups'] = filtered_groups
    
    # Recalculate summary
    _recalculate_summary(results)

def _recalculate_summary(results: Dict[str, Any]) -> None:
    """Recalculate summary statistics after filtering."""
    filtered_groups = results['groups']
    total_series = len(filtered_groups)
    complete_series = sum(1 for a in filtered_groups.values() if a['status'] in ['complete', 'complete_with_extras'])
    incomplete_series = sum(1 for a in filtered_groups.values() if a['status'] in ['incomplete', 'no_episode_numbers'])
    unknown_series = total_series - complete_series - incomplete_series
    total_episodes_found = sum(a['episodes_found'] for a in filtered_groups.values())
    total_episodes_expected = sum(a.get('episodes_expected', 0) for a in filtered_groups.values())

    results['completeness_summary'].update({
        'total_series': total_series,
        'complete_series': complete_series,
        'incomplete_series': incomplete_series,
        'unknown_series': unknown_series,
        'total_episodes_found': total_episodes_found,
        'total_episodes_expected': total_episodes_expected
    })

def main():
    """Command-line interface for series completeness checker."""
    parser = argparse.ArgumentParser(
        description='Check series collection completeness using filename metadata',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s /path/to/series
  %(prog)s /path/to/series --exclude-paths /path/to/series/trash
  %(prog)s /path/to/series --include-patterns "*.mkv" "*.mp4" --recursive
  %(prog)s /path/to/series --export series_completeness.json -v 3
  %(prog)s /path/to/series --status-filter "incomplete no_episode_numbers"
  %(prog)s /path/to/series --status-filter "+complete +complete_with_extras"
  %(prog)s /path/to/series --status-filter "-unknown -no_metadata"
  %(prog)s /path/to/series --status-filter "+incomplete -complete_with_extras"
  %(prog)s /path/to/series --mal-status-filter "watching completed"
  %(prog)s /path/to/series --mal-status-filter "+completed +on-hold"
  %(prog)s /path/to/series --mal-status-filter "-dropped -plan-to-watch"
  %(prog)s /path/to/series --show-metadata year rating
  %(prog)s /path/to/series --show-metadata genres director --status-filter "complete"
  %(prog)s /path/to/series --generate-thumbnails --thumbnail-dir ~/.video_thumbnail_cache
  %(prog)s --refresh-bundle /path/to/bundle/dir
  %(prog)s --webapp-refresh /path/to/series_completeness.json
  %(prog)s --webapp-refresh /path/to/series.json --webapp-export /path/to/output.html
        """
    )
    parser.add_argument('input_paths', nargs='*',
                       help='Input paths to search for series files (not required when using --refresh-bundle or --webapp-refresh)')
    parser.add_argument('--refresh-bundle', metavar='BUNDLE_DIR',
                       help='Regenerate webapp from existing bundle metadata.json. Provide path to bundle root directory.')
    parser.add_argument('--webapp-refresh', metavar='JSON_FILE',
                       help='Regenerate standalone webapp from existing metadata JSON file.')
    parser.add_argument('--exclude-paths', nargs='*', default=[],
                       help='Paths to exclude from search')
    parser.add_argument('--include-patterns', nargs='*', default=['*.mkv', '*.mp4', '*.avi'],
                       help='Wildcard patterns for files to include (default: *.mkv *.mp4 *.avi)')
    parser.add_argument('--exclude-patterns', nargs='*', default=[],
                       help='Wildcard patterns for files to exclude')
    parser.add_argument('--export', metavar='FILE',
                       help='Export results to JSON file')
    parser.add_argument('--webapp-export', metavar='FILE',
                       help='Export results as a standalone HTML webapp')
    parser.add_argument('--export-bundle', metavar='DIR',
                       help='Export complete bundle (webapp + metadata.json + thumbnails) to specified directory. '
                            'This overrides --export, --webapp-export, and --generate-thumbnails. '
                            'Cannot be used together with those options.')
    parser.add_argument('--recursive', '-r', action='store_true',
                       help='Recursively search subdirectories (default: False)')
    parser.add_argument('--verbose', '-v', type=int, choices=[0, 1, 2, 3], default=1,
                       help='Verbosity level: 0=silent, 1=summary, 2=detailed, 3=very detailed (default: 1)')
    parser.add_argument('--quiet', '-q', action='store_true',
                       help='Same as --verbose 0')
    parser.add_argument('--status-filter', metavar='FILTERS',
                       help='Filter results by status. Use +status to include only specific statuses, '
                            '-status to exclude specific statuses, or plain status names for exact match. '
                            'Available statuses: complete, incomplete, complete_with_extras, no_episode_numbers, '
                            'unknown_total_episodes, not_series, no_metadata, no_metadata_manager, unknown. '
                            'Examples: "complete incomplete", "+complete +incomplete", "-unknown -no_metadata"')
    parser.add_argument('--mal-status-filter', metavar='FILTERS',
                       help='Filter results by MyAnimeList watch status. Use +status to include only specific statuses, '
                            '-status to exclude specific statuses, or plain status names for exact match. '
                            'Available MAL statuses: watching, completed, on-hold, dropped, plan-to-watch. '
                            'Examples: "watching completed", "+completed +on-hold", "-dropped -plan-to-watch"')
    parser.add_argument('--show-metadata', nargs='*', metavar='FIELD',
                       help='Show metadata fields in summary lines. Available fields depend on metadata source. '
                            'Common fields: year, rating, genres, director, actors, plot, runtime, imdb_id. '
                            'Example: --show-metadata year rating genres')
    parser.add_argument('--generate-thumbnails', action='store_true',
                       help='Generate static and animated webp thumbnails for each video file and store in thumbnail dir')
    parser.add_argument('--thumbnail-dir', default='~/.video_thumbnail_cache',
                       help='Directory to store video thumbnails (default: ~/.video_thumbnail_cache)')
    parser.add_argument('--myanimelist-xml', metavar='PATH_OR_URL',
                       help='Path to MyAnimeList XML file (can be .gz) or URL for watch status lookup')
    
    args = parser.parse_args()
    
    # Check for refresh modes
    refresh_mode = False
    if hasattr(args, 'refresh_bundle') and args.refresh_bundle:
        refresh_mode = 'bundle'
    elif hasattr(args, 'webapp_refresh') and args.webapp_refresh:
        refresh_mode = 'webapp'
    
    # Validate input_paths requirement (not needed in refresh modes)
    if not refresh_mode and (not args.input_paths or len(args.input_paths) == 0):
        parser.error('input_paths is required when not using --refresh-bundle or --webapp-refresh')
    
    # Check for conflicts with --export-bundle
    if hasattr(args, 'export_bundle') and args.export_bundle:
        conflicts = []
        if args.export:
            conflicts.append('--export')
        if args.webapp_export:
            conflicts.append('--webapp-export')
        if args.generate_thumbnails:
            conflicts.append('--generate-thumbnails')
        
        if conflicts:
            parser.error(f"--export-bundle cannot be used together with: {', '.join(conflicts)}. "
                        f"The --export-bundle option automatically enables and configures these features.")
    
    # Handle quiet flag
    if args.quiet:
        verbosity = 0
    else:
        verbosity = args.verbose

    # Get metadata manager and plex provider (skip in refresh modes)
    metadata_manager = None
    plex_provider = None
    
    if not refresh_mode:
        try:
            metadata_manager = get_metadata_manager()
            if not metadata_manager and verbosity >= 1:
                print("Warning: No metadata manager available. Completeness checking will be limited.")
        except Exception as e:
            if verbosity >= 1:
                print(f"Warning: Could not initialize metadata manager: {e}")
            metadata_manager = None
        
        try:
            plex_provider = get_plex_provider()
        except Exception as e:
            if verbosity >= 2:
                print(f"Warning: Could not initialize Plex provider: {e}")
            plex_provider = None
    
    # Create checker instance with MyAnimeList support
    # Use metadata_only mode when in refresh mode to skip FileGrouper initialization
    checker = SeriesCompletenessChecker(
        metadata_manager, 
        plex_provider,
        args.myanimelist_xml if hasattr(args, 'myanimelist_xml') else None,
        metadata_only=bool(refresh_mode)
    )

    # Handle refresh modes
    if refresh_mode == 'bundle':
        _handle_refresh_bundle_mode(checker, args.refresh_bundle, verbosity)
        return
    elif refresh_mode == 'webapp':
        _handle_refresh_webapp_mode(checker, args.webapp_refresh, args.webapp_export, verbosity)
        return

    # Discover files
    if verbosity >= 1:
        print("Discovering series files...")
    files = checker.file_grouper.discover_files(
        args.input_paths,
        args.exclude_paths,
        args.include_patterns,
        args.exclude_patterns,
        args.recursive
    )
    if not files:
        if verbosity >= 1:
            print("No series files found matching criteria.")
        return

    # Handle thumbnail generation
    thumbnail_dir_expanded = _handle_thumbnail_generation(files, args, verbosity)
    
    if verbosity >= 1:
        print(f"Found {len(files)} files")
        print("Analyzing series collection for completeness...")
    
    # Analyze collection
    results = checker.analyze_series_collection(files)
    
    # Store thumbnail directory in results for later use
    if thumbnail_dir_expanded:
        results['thumbnail_dir'] = thumbnail_dir_expanded
    
    # Apply filters if requested
    if args.status_filter:
        status_filters = args.status_filter.split()
        _apply_status_filters(results, status_filters)
    
    if hasattr(args, 'mal_status_filter') and args.mal_status_filter:
        mal_status_filters = args.mal_status_filter.split()
        _apply_mal_status_filters(results, mal_status_filters)
    
    # Display results
    if verbosity >= 1:
        checker.print_summary(results, verbosity, args.show_metadata)
    
    # Export based on mode
    if hasattr(args, 'export_bundle') and args.export_bundle:
        # Bundle export mode - exports everything to the bundle directory
        checker.export_bundle(results, args.export_bundle, verbosity)
    else:
        # Individual export modes
        if args.export:
            checker.export_results(results, args.export)
            if verbosity >= 1:
                print(f"\nExported results to: {args.export}")
        if args.webapp_export:
            checker.export_webapp(results, args.webapp_export)
            if verbosity >= 1:
                print(f"\nExported webapp to: {args.webapp_export}")

if __name__ == '__main__':
    main()