from pathlib import Path
from typing import Dict, List, Any, Optional
from collections import defaultdict
from datetime import datetime
import json
import os
import sys
import argparse
import logging
import hashlib
import subprocess
import tempfile

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    # Fallback progress indicator
    class tqdm:
        def __init__(self, iterable, desc="", unit="", disable=False):
            self.iterable = iterable
            self.desc = desc
            
        def __iter__(self):
            return iter(self.iterable)
        
        def __enter__(self):
            return self
        
        def __exit__(self, *args):
            pass

# Import the FileGrouper class and related components
try:
    from file_grouper import FileGrouper, CustomJSONEncoder
except ImportError:
    print("ERROR: file_grouper module not found. Please ensure file_grouper.py is in the same directory.")
    sys.exit(1)

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

# Try to load metadata providers from video-optimizer-v2
try:
    sys.path.append(os.path.join(os.path.dirname(__file__), 'video-optimizer-v2'))
    from metadata_provider import MetadataManager, BaseMetadataProvider, TitleInfo
    from anime_metadata import AnimeDataProvider
    from imdb_metadata import IMDbDataProvider
    from plex_metadata import PlexMetadataProvider, PlexWatchStatus
    from myanimelist_watch_status import MyAnimeListWatchStatusProvider, MyAnimeListWatchStatus
except ImportError:
    print("Warning: video-optimizer-v2 metadata providers not found. Enhanced metadata features will be disabled.")
    MetadataManager = None
    BaseMetadataProvider = None
    TitleInfo = None
    PlexMetadataProvider = None
    PlexWatchStatus = None
    MyAnimeListWatchStatusProvider = None
    MyAnimeListWatchStatus = None


class LatestEpisodesViewer:
    """Generates a web interface showing the latest episodes by download date."""
    
    def __init__(self, metadata_manager=None, plex_provider=None, myanimelist_xml_path=None):
        self.file_grouper = FileGrouper(metadata_manager, plex_provider, myanimelist_xml_path)
        self.metadata_manager = metadata_manager
        self.plex_provider = plex_provider
        self.myanimelist_provider = None
        
        if myanimelist_xml_path and MyAnimeListWatchStatusProvider is not None:
            try:
                self.myanimelist_provider = MyAnimeListWatchStatusProvider(myanimelist_xml_path)
            except Exception as e:
                print(f"Warning: Could not load MyAnimeList data from {myanimelist_xml_path}: {e}")
        elif myanimelist_xml_path and MyAnimeListWatchStatusProvider is None:
            print("Warning: MyAnimeList functionality not available (video-optimizer-v2 not found)")
    
    def analyze_latest_episodes(self, files: List[Path], show_progress: bool = True, max_episodes: int = 100) -> Dict[str, Any]:
        """Analyze files and return the latest episodes by download date."""
        
        # First, sort files by modification time and limit to max_episodes
        print(f"Sorting {len(files)} files by download date and taking the latest {max_episodes}...")
        files_with_mtime = []
        for file_path in files:
            try:
                file_stat = file_path.stat()
                files_with_mtime.append((file_path, file_stat.st_mtime, file_stat.st_size))
            except Exception as e:
                logging.warning(f"Error getting file stats for {file_path}: {e}")
                continue
        
        # Sort by modification time (newest first) and limit
        files_with_mtime.sort(key=lambda x: x[1], reverse=True)
        latest_files = files_with_mtime[:max_episodes]
        
        # Extract metadata from only the latest files
        print(f"Extracting metadata from {len(latest_files)} latest episodes...")
        episodes_data = []
        
        progress_iter = tqdm(latest_files, desc="Processing files", unit="file", disable=not show_progress) if show_progress else latest_files
        
        for file_path, mtime_timestamp, file_size in progress_iter:
            try:
                # Convert timestamp to datetime
                mtime = datetime.fromtimestamp(mtime_timestamp)
                
                # Extract metadata using FileGrouper
                metadata = self.file_grouper.extract_metadata(file_path)
                
                # Skip files that don't appear to be episodes
                if not metadata.get('episode') or not metadata.get('title'):
                    continue
                
                episode_info = {
                    'file_path': str(file_path),
                    'file_name': file_path.name,
                    'download_date': mtime.isoformat(),
                    'download_timestamp': mtime_timestamp,
                    'file_size': file_size,
                    'metadata': metadata
                }
                
                episodes_data.append(episode_info)
                
            except Exception as e:
                logging.warning(f"Error processing {file_path}: {e}")
                continue
        
        # Group by series for watch status analysis
        series_groups = defaultdict(list)
        for episode in episodes_data:
            title = episode['metadata'].get('title', 'Unknown')
            series_groups[title].append(episode)
        
        # Enhance with series-level metadata and watch status
        print("Enhancing with series metadata...")
        enhanced_episodes = []
        
        for episode in tqdm(episodes_data, desc="Enhancing episodes", disable=not show_progress):
            enhanced_episode = self._enhance_episode_with_series_data(episode, series_groups)
            enhanced_episodes.append(enhanced_episode)
        
        # Create summary statistics
        summary = self._create_summary(enhanced_episodes, series_groups)
        
        results = {
            'episodes': enhanced_episodes,
            'series_groups': dict(series_groups),
            'summary': summary,
            'title_metadata': self.file_grouper.title_metadata,
            'thumbnails': [],  # Will be populated by caller if requested
            'generated_at': datetime.now().isoformat()
        }
        
        return results
    
    def _enhance_episode_with_series_data(self, episode: Dict[str, Any], series_groups: Dict[str, List]) -> Dict[str, Any]:
        """Enhance episode data with series-level information."""
        enhanced = episode.copy()
        
        title = episode['metadata'].get('title', 'Unknown')
        season = episode['metadata'].get('season', 1)
        episode_num = episode['metadata'].get('episode')
        
        # Get series episodes for context
        series_episodes = series_groups.get(title, [])
        enhanced['series_episode_count'] = len(series_episodes)
        
        # Try to get enhanced metadata
        if self.metadata_manager:
            try:
                title_info, provider = self.metadata_manager.find_title(title)
                if title_info:
                    enhanced['series_metadata'] = {
                        'id': title_info.id,
                        'title': title_info.title,
                        'type': title_info.type,
                        'year': title_info.year,
                        'rating': title_info.rating,
                        'genres': title_info.genres,
                        'sources': title_info.sources,
                        'total_episodes': title_info.total_episodes,
                        'plot': title_info.plot
                    }
                    
                    # Get MyAnimeList source URL for linking
                    if title_info.sources:
                        mal_sources = [src for src in title_info.sources if 'myanimelist.net' in src]
                        if mal_sources:
                            enhanced['myanimelist_url'] = mal_sources[0]
                    
                    # Get episode-specific metadata if available
                    if episode_num and provider:
                        try:
                            episode_info = provider.get_episode_info(title_info.id, season, episode_num)
                            if episode_info:
                                enhanced['episode_metadata'] = {
                                    'title': episode_info.title,
                                    'rating': episode_info.rating,
                                    'plot': episode_info.plot,
                                    'air_date': episode_info.air_date
                                }
                        except Exception as e:
                            logging.debug(f"Could not get episode metadata for {title} S{season}E{episode_num}: {e}")
                    
                    # Get MyAnimeList watch status
                    if self.myanimelist_provider and title_info.sources:
                        for source in title_info.sources:
                            if 'myanimelist.net' in source:
                                try:
                                    mal_status = self.myanimelist_provider.get_watch_status(source)
                                    if mal_status:
                                        enhanced['myanimelist_watch_status'] = {
                                            'status': mal_status.my_status,
                                            'watched_episodes': mal_status.my_watched_episodes,
                                            'score': mal_status.my_score,
                                            'total_episodes': mal_status.series_episodes,
                                            'progress_percent': mal_status.progress_percent
                                        }
                                        break
                                except Exception as e:
                                    logging.debug(f"Could not get MAL status for {source}: {e}")
            
            except Exception as e:
                logging.debug(f"Could not enhance metadata for {title}: {e}")
        
        # Get Plex watch status
        if self.plex_provider:
            try:
                plex_status = self.plex_provider.get_watch_status(episode['file_path'])
                if plex_status:
                    enhanced['plex_watch_status'] = {
                        'watched': plex_status.watched,
                        'watch_count': plex_status.watch_count,
                        'progress_percent': plex_status.progress_percent,
                        'last_watched': plex_status.last_watched.isoformat() if plex_status.last_watched else None
                    }
            except Exception as e:
                logging.debug(f"Could not get Plex status for {episode['file_path']}: {e}")
        
        return enhanced
    
    def _create_summary(self, episodes: List[Dict], series_groups: Dict[str, List]) -> Dict[str, Any]:
        """Create summary statistics."""
        total_episodes = len(episodes)
        unique_series = len(series_groups)
        
        # Calculate watch status distribution
        watch_status_counts = {
            'watched': 0,
            'partially_watched': 0,
            'not_watched': 0,
            'unknown': 0
        }
        
        mal_status_counts = defaultdict(int)
        
        for episode in episodes:
            # Plex watch status
            plex_status = episode.get('plex_watch_status')
            if plex_status:
                if plex_status['watched']:
                    watch_status_counts['watched'] += 1
                elif plex_status['progress_percent'] > 0:
                    watch_status_counts['partially_watched'] += 1
                else:
                    watch_status_counts['not_watched'] += 1
            else:
                watch_status_counts['unknown'] += 1
            
            # MyAnimeList status
            mal_status = episode.get('myanimelist_watch_status')
            if mal_status:
                status = mal_status['status'].lower().replace(' ', '_')
                mal_status_counts[status] += 1
            else:
                mal_status_counts['no_mal_data'] += 1
        
        # Get date range
        if episodes:
            latest_date = max(episode['download_timestamp'] for episode in episodes)
            oldest_date = min(episode['download_timestamp'] for episode in episodes)
            date_range_days = (latest_date - oldest_date) / (24 * 3600)
        else:
            latest_date = oldest_date = date_range_days = 0
        
        return {
            'total_episodes': total_episodes,
            'unique_series': unique_series,
            'watch_status_distribution': dict(watch_status_counts),
            'mal_status_distribution': dict(mal_status_counts),
            'date_range_days': int(date_range_days),
            'latest_download': datetime.fromtimestamp(latest_date).isoformat() if latest_date else None,
            'oldest_download': datetime.fromtimestamp(oldest_date).isoformat() if oldest_date else None
        }
    
    def export_webapp(self, results: Dict[str, Any], output_path: str) -> None:
        """Export results as a standalone HTML webapp."""
        
        # Read template files
        template_dir = Path(__file__).parent
        
        # Define template file paths
        css_template_path = template_dir / "latest_episodes_webapp_template.css"
        js_template_path = template_dir / "latest_episodes_webapp_template.js"
        html_template_path = template_dir / "latest_episodes_webapp_template.html"
        
        # Check if all template files exist
        missing_files = []
        if not css_template_path.exists():
            missing_files.append(str(css_template_path))
        if not js_template_path.exists():
            missing_files.append(str(js_template_path))
        if not html_template_path.exists():
            missing_files.append(str(html_template_path))
        
        if missing_files:
            print(f"ERROR: Required template files not found:")
            for file in missing_files:
                print(f"  - {file}")
            print("\nPlease ensure these template files are in the same directory as this script:")
            print("  - latest_episodes_webapp_template.html")
            print("  - latest_episodes_webapp_template.css")
            print("  - latest_episodes_webapp_template.js")
            raise FileNotFoundError(f"Missing template files: {', '.join(missing_files)}")
        
        # Read template files
        with open(css_template_path, 'r', encoding='utf-8') as f:
            css_content = f.read()
        
        with open(js_template_path, 'r', encoding='utf-8') as f:
            js_content = f.read()
        
        with open(html_template_path, 'r', encoding='utf-8') as f:
            html_template = f.read()
        
        # Generate HTML with embedded data
        html_content = self._generate_html_from_template(results, html_template, css_content, js_content)
        
        # Write the complete HTML file
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"Latest Episodes webapp exported to: {output_path}")
    
    def _generate_html_from_template(self, results: Dict[str, Any], html_template: str, css_content: str, js_content: str) -> str:
        """Generate the HTML from template with embedded data."""
        
        # Serialize data for JavaScript
        json_data = json.dumps(results, cls=CustomJSONEncoder, indent=2)
        
        # Replace placeholders in template
        html_content = html_template.replace('[[embedded_css]]', css_content)
        html_content = html_content.replace('[[embedded_js]]', js_content)
        html_content = html_content.replace('[[embedded_json]]', json_data)
        
        return html_content
    
    def discover_files(self, input_paths: List[str], excluded_paths: Optional[List[str]] = None,
                      include_patterns: Optional[List[str]] = None, exclude_patterns: Optional[List[str]] = None,
                      recursive: bool = False, show_progress: bool = True) -> List[Path]:
        """Discover video files using FileGrouper."""
        return self.file_grouper.discover_files(
            input_paths, excluded_paths, include_patterns, exclude_patterns, recursive, show_progress
        )
    
    def generate_thumbnails(self, episodes_data: List[Dict[str, Any]], thumbnail_dir: Optional[str] = None, 
                          max_height: int = 480, verbose: int = 1) -> List[Dict[str, Any]]:
        """Generate thumbnails for episodes and return thumbnail index."""
        if thumbnail_dir is None:
            thumbnail_dir = os.path.expanduser("~/.video_thumbnail_cache")
        else:
            thumbnail_dir = os.path.expanduser(thumbnail_dir)
        os.makedirs(thumbnail_dir, exist_ok=True)

        thumbnail_index = []
        video_files = [episode['file_path'] for episode in episodes_data]
        
        for video_path_str in tqdm(video_files, desc="Generating thumbnails", unit="file", disable=verbose < 1):
            h = hashlib.sha256(video_path_str.encode("utf-8")).hexdigest()
            static_thumb = os.path.join(thumbnail_dir, f"{h}_static.webp")
            video_thumb = os.path.join(thumbnail_dir, f"{h}_video.webp")

            static_exists = os.path.exists(static_thumb)
            video_exists = os.path.exists(video_thumb)
            if static_exists and video_exists:
                thumbnail_index.append({
                    "video": video_path_str,
                    "static_thumbnail": static_thumb,
                    "animated_thumbnail": video_thumb
                })
                continue

            # Get video duration (in seconds)
            try:
                cmd = [
                    "ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", video_path_str
                ]
                result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                duration = float(result.stdout.strip())
            except Exception as e:
                if verbose >= 2:
                    print(f"Could not get duration for {video_path_str}: {e}")
                continue
            if duration <= 0:
                if verbose >= 2:
                    print(f"Invalid duration for {video_path_str}")
                continue

            # --- Static thumbnail (20% in) ---
            if not static_exists:
                static_time = duration * 0.2
                static_cmd = [
                    "ffmpeg", "-y", "-ss", str(static_time), "-i", video_path_str,
                    "-vframes", "1", "-vf", f"scale=-2:{max_height}", "-f", "webp", static_thumb
                ]
                try:
                    subprocess.run(static_cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                except subprocess.CalledProcessError as e:
                    if verbose >= 2:
                        print(f"Failed to generate static thumbnail for {video_path_str}: {e}")
                    static_thumb = None

            # --- Animated thumbnail (extract frames, skip failed frames) ---
            if not video_exists:
                frame_times = [duration * (i / 100) for i in range(5, 100, 5)]
                with tempfile.TemporaryDirectory() as tmpdir:
                    frame_files = []
                    for idx, t in enumerate(frame_times):
                        frame_file = os.path.join(tmpdir, f"frame_{idx:02d}.webp")
                        frame_cmd = [
                            "ffmpeg", "-y", "-noaccurate_seek", "-ss", str(t), "-i", video_path_str,
                            "-vframes", "1", "-vf", f"scale=-2:{max_height}", "-f", "webp", frame_file
                        ]
                        try:
                            subprocess.run(frame_cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                            if os.path.exists(frame_file) and os.path.getsize(frame_file) > 0:
                                frame_files.append(frame_file)
                        except subprocess.CalledProcessError as e:
                            if verbose >= 2:
                                print(f"Failed to extract frame {idx} for {video_path_str}: {e}")
                    
                    # Combine frames into animated webp (2 fps, only valid frames)
                    if frame_files:
                        anim_cmd = [
                            "ffmpeg", "-y", "-framerate", "2", "-i", os.path.join(tmpdir, "frame_%02d.webp"),
                            "-vf", f"scale=-2:{max_height}", "-loop", "0", "-f", "webp", video_thumb
                        ]
                        try:
                            subprocess.run(anim_cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                        except subprocess.CalledProcessError as e:
                            if verbose >= 2:
                                print(f"Failed to generate animated thumbnail for {video_path_str}: {e}")
                            video_thumb = None

            # Add to index if we have at least one thumbnail
            static_valid = static_exists or (static_thumb and os.path.exists(static_thumb))
            video_valid = video_exists or (video_thumb and os.path.exists(video_thumb))
            
            if static_valid or video_valid:
                thumbnail_index.append({
                    "video": video_path_str,
                    "static_thumbnail": static_thumb if static_valid else None,
                    "animated_thumbnail": video_thumb if video_valid else None
                })

        return thumbnail_index


def main():
    """Command-line interface for Latest Episodes Viewer."""
    parser = argparse.ArgumentParser(
        description='Generate a web interface showing the latest episodes by download date',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s /path/to/episodes --export latest_episodes.html
  %(prog)s /path/to/episodes --max-episodes 50 --recursive
  %(prog)s /path/to/episodes --exclude-paths /path/to/episodes/trash
  %(prog)s /path/to/episodes --include-patterns "*.mkv" "*.mp4" --export episodes.html
  %(prog)s /path/to/episodes --myanimelist-xml /path/to/animelist.xml --export episodes.html
  %(prog)s /path/to/episodes --verbose 3 --max-episodes 200 --export latest.html
        """
    )
    
    parser.add_argument('input_paths', nargs='+',
                       help='Input paths to search for episode files')
    parser.add_argument('--exclude-paths', nargs='*', default=[],
                       help='Paths to exclude from search')
    parser.add_argument('--include-patterns', nargs='*', default=['*.mkv', '*.mp4', '*.avi'],
                       help='Wildcard patterns for files to include (default: *.mkv *.mp4 *.avi)')
    parser.add_argument('--exclude-patterns', nargs='*', default=[],
                       help='Wildcard patterns for files to exclude')
    parser.add_argument('--export', metavar='FILE', required=True,
                       help='Export results to HTML webapp file')
    parser.add_argument('--recursive', '-r', action='store_true',
                       help='Recursively search subdirectories (default: False)')
    parser.add_argument('--max-episodes', type=int, default=100,
                       help='Maximum number of latest episodes to include (default: 100)')
    parser.add_argument('--verbose', '-v', type=int, choices=[0, 1, 2, 3], default=1,
                       help='Verbosity level: 0=errors only, 1=warnings, 2=info, 3=debug (default: 1)')
    parser.add_argument('--quiet', '-q', action='store_true',
                       help='Same as --verbose 0')
    parser.add_argument('--myanimelist-xml', metavar='PATH_OR_URL',
                       help='Path to MyAnimeList XML file or URL for watch status lookup')
    parser.add_argument('--generate-thumbnails', action='store_true',
                       help='Generate thumbnails for episodes (requires ffmpeg)')
    parser.add_argument('--thumbnail-dir', metavar='PATH', 
                       help='Directory to store thumbnails (default: ~/.video_thumbnail_cache)')
    
    args = parser.parse_args()
    
    # Handle quiet flag
    if args.quiet:
        args.verbose = 0
    
    # Set up logging
    if args.verbose == 0:
        logging.basicConfig(level=logging.ERROR)
    elif args.verbose == 1:
        logging.basicConfig(level=logging.WARNING)
    elif args.verbose == 2:
        logging.basicConfig(level=logging.INFO)
    elif args.verbose == 3:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.ERROR)
    
    # Initialize metadata providers
    metadata_manager = None
    plex_provider = None
    
    try:
        if metadata_manager_available:
            metadata_manager = get_metadata_manager()
            if args.verbose >= 1:
                print("Metadata providers initialized successfully")
        
        if plex_provider_available:
            plex_provider = get_plex_provider()
            if plex_provider and plex_provider.is_available() and args.verbose >= 1:
                print("Plex provider initialized successfully")
        
    except Exception as e:
        if args.verbose >= 1:
            print(f"Warning: Could not initialize metadata providers: {e}")
    
    # Check if template files exist before proceeding
    template_dir = Path(__file__).parent
    required_templates = [
        "latest_episodes_webapp_template.html",
        "latest_episodes_webapp_template.css",
        "latest_episodes_webapp_template.js"
    ]
    
    missing_templates = []
    for template in required_templates:
        if not (template_dir / template).exists():
            missing_templates.append(template)
    
    if missing_templates:
        print(f"ERROR: Required template files not found:")
        for template in missing_templates:
            print(f"  - {template}")
        print("\nPlease ensure all template files are in the same directory as this script.")
        return 1
    
    # Initialize the viewer
    viewer = LatestEpisodesViewer(
        metadata_manager=metadata_manager,
        plex_provider=plex_provider,
        myanimelist_xml_path=args.myanimelist_xml
    )
    
    try:
        # Discover files
        if args.verbose >= 1:
            print(f"Discovering files in: {', '.join(args.input_paths)}")
        
        files = viewer.discover_files(
            args.input_paths,
            args.exclude_paths,
            args.include_patterns,
            args.exclude_patterns,
            args.recursive,
            args.verbose >= 1
        )
        
        if not files:
            print("No video files found matching the criteria.")
            return 1
        
        if args.verbose >= 1:
            print(f"Found {len(files)} video files")
        
        # Analyze latest episodes
        results = viewer.analyze_latest_episodes(
            files,
            show_progress=(args.verbose >= 1),
            max_episodes=args.max_episodes
        )
        
        if not results['episodes']:
            print("No episodes found with valid metadata.")
            return 1
        
        # Generate thumbnails if requested
        if args.generate_thumbnails:
            print("Generating thumbnails...")
            try:
                thumbnails = viewer.generate_thumbnails(
                    results['episodes'],
                    thumbnail_dir=args.thumbnail_dir,
                    verbose=args.verbose
                )
                results['thumbnails'] = thumbnails
                print(f"Generated thumbnails for {len(thumbnails)} episodes")
            except Exception as e:
                print(f"Warning: Thumbnail generation failed: {e}")
                if args.verbose >= 2:
                    import traceback
                    traceback.print_exc()
        
        # Export webapp
        viewer.export_webapp(results, args.export)
        
        # Print summary
        if args.verbose >= 1:
            summary = results['summary']
            print(f"\\nSummary:")
            print(f"  Total Episodes: {summary['total_episodes']}")
            print(f"  Unique Series: {summary['unique_series']}")
            print(f"  Date Range: {summary['date_range_days']} days")
            if summary['latest_download']:
                latest = datetime.fromisoformat(summary['latest_download']).strftime('%Y-%m-%d %H:%M')
                print(f"  Latest Download: {latest}")
            
            # Watch status distribution
            watch_dist = summary['watch_status_distribution']
            if any(watch_dist.values()):
                print(f"  Watch Status:")
                for status, count in watch_dist.items():
                    if count > 0:
                        print(f"    {status.replace('_', ' ').title()}: {count}")
        
        return 0
        
    except KeyboardInterrupt:
        print("\\nOperation cancelled by user")
        return 1
    except Exception as e:
        print(f"Error: {e}")
        if args.verbose >= 2:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == '__main__':
    main()