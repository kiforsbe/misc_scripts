import argparse
import json
import os
from pathlib import Path
from typing import Dict, List, Any, Optional
from enum import Enum
from dataclasses import dataclass, field, asdict

from video_thumbnail_generator import VideoThumbnailGenerator
from file_grouper import FileGrouper, CustomJSONEncoder

# Enums for status values
class SeriesStatus(str, Enum):
    """Enum for series completeness status values."""
    COMPLETE = 'complete'
    INCOMPLETE = 'incomplete'
    COMPLETE_WITH_EXTRAS = 'complete_with_extras'
    NO_EPISODE_NUMBERS = 'no_episode_numbers'
    UNKNOWN_TOTAL_EPISODES = 'unknown_total_episodes'
    NOT_SERIES = 'not_series'
    MOVIE = 'movie'
    NO_METADATA = 'no_metadata'
    NO_METADATA_MANAGER = 'no_metadata_manager'
    UNKNOWN = 'unknown'

class MALStatus(str, Enum):
    """Enum for MyAnimeList status values."""
    WATCHING = 'Watching'
    COMPLETED = 'Completed'
    ON_HOLD = 'On-Hold'
    DROPPED = 'Dropped'
    PLAN_TO_WATCH = 'Plan to Watch'
    COMPLETED_SEASON = 'Completed (Season)'

# Status emoji mapping
STATUS_EMOJI = {
    SeriesStatus.COMPLETE: '✅',
    SeriesStatus.INCOMPLETE: '❌',
    SeriesStatus.COMPLETE_WITH_EXTRAS: '⚠️',
    SeriesStatus.NO_EPISODE_NUMBERS: '❓',
    SeriesStatus.UNKNOWN_TOTAL_EPISODES: '❓',
    SeriesStatus.NOT_SERIES: 'ℹ️',
    SeriesStatus.MOVIE: '🎬',
    SeriesStatus.NO_METADATA: '❓',
    SeriesStatus.NO_METADATA_MANAGER: '❓',
    SeriesStatus.UNKNOWN: '❓'
}

# Dataclasses for complex data structures
@dataclass
class WatchStatus:
    """Watch status information for a series."""
    watched_episodes: int = 0
    partially_watched_episodes: int = 0
    unwatched_episodes: int = 0
    total_watch_count: int = 0
    completion_percent: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'WatchStatus':
        """Create from dictionary."""
        return cls(
            watched_episodes=data.get('watched_episodes', 0),
            partially_watched_episodes=data.get('partially_watched_episodes', 0),
            unwatched_episodes=data.get('unwatched_episodes', 0),
            total_watch_count=data.get('total_watch_count', 0),
            completion_percent=data.get('completion_percent', 0.0)
        )

@dataclass
class SeriesAnalysis:
    """Analysis result for a series or season."""
    title: str
    status: SeriesStatus
    episodes_found: int
    episodes_expected: int
    season: Optional[int] = None
    metadata_id: Optional[str] = None
    episode_numbers: List[int] = field(default_factory=list)
    missing_episodes: List[int] = field(default_factory=list)
    extra_episodes: List[int] = field(default_factory=list)
    files: List[Dict] = field(default_factory=list)
    extra_files: List[Dict] = field(default_factory=list)
    watch_status: Optional[WatchStatus] = None
    myanimelist_watch_status: Optional[Dict] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = {
            'title': self.title,
            'season': self.season,
            'metadata_id': self.metadata_id,
            'episodes_found': self.episodes_found,
            'episodes_expected': self.episodes_expected,
            'status': self.status.value if isinstance(self.status, SeriesStatus) else self.status,
            'episode_numbers': self.episode_numbers,
            'missing_episodes': self.missing_episodes,
            'extra_episodes': self.extra_episodes,
            'files': self.files,
            'extra_files': self.extra_files,
            'watch_status': self.watch_status.to_dict() if isinstance(self.watch_status, WatchStatus) else self.watch_status,
            'myanimelist_watch_status': self.myanimelist_watch_status
        }
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SeriesAnalysis':
        """Create from dictionary."""
        watch_status_data = data.get('watch_status')
        watch_status = WatchStatus.from_dict(watch_status_data) if watch_status_data else None
        
        status = data.get('status', 'unknown')
        if isinstance(status, str):
            try:
                status = SeriesStatus(status)
            except ValueError:
                status = SeriesStatus.UNKNOWN
        
        return cls(
            title=data.get('title', 'Unknown'),
            season=data.get('season'),
            metadata_id=data.get('metadata_id'),
            episodes_found=data.get('episodes_found', 0),
            episodes_expected=data.get('episodes_expected', 0),
            status=status,
            episode_numbers=data.get('episode_numbers', []),
            missing_episodes=data.get('missing_episodes', []),
            extra_episodes=data.get('extra_episodes', []),
            files=data.get('files', []),
            extra_files=data.get('extra_files', []),
            watch_status=watch_status,
            myanimelist_watch_status=data.get('myanimelist_watch_status')
        )


class ResultsFilter:
    """Handles filtering of series analysis results."""
    
    def __init__(self):
        """Initialize the results filter."""
        self.all_statuses = {status.value for status in SeriesStatus}
        self.mal_status_map = {
            'watching': MALStatus.WATCHING.value,
            'completed': MALStatus.COMPLETED.value,
            'on-hold': MALStatus.ON_HOLD.value,
            'dropped': MALStatus.DROPPED.value,
            'plan-to-watch': MALStatus.PLAN_TO_WATCH.value
        }
        self.all_mal_status_keys = set(self.mal_status_map.keys())
    
    def parse_filter_patterns(self, filter_items: List[str], valid_values: set) -> set:
        """Parse filter patterns with +/- prefixes.
        
        Args:
            filter_items: List of filter strings (may have +/- prefixes)
            valid_values: Set of valid values to filter against
            
        Returns:
            Final set of values to filter by
        """
        include = set()
        exclude = set()
        plain = set()
        
        for item in filter_items:
            if item.startswith('+'):
                value = item[1:]
                if value in valid_values:
                    include.add(value)
            elif item.startswith('-'):
                value = item[1:]
                if value in valid_values:
                    exclude.add(value)
            elif item in valid_values:
                plain.add(item)
        
        # Determine final filter set
        if plain:
            return plain
        elif include:
            return include - exclude
        elif exclude:
            return valid_values - exclude
        else:
            return valid_values
    
    def apply_status_filter(self, results: Dict[str, Any], status_filters: List[str]) -> None:
        """Apply status filters to results and update summary.
        
        Args:
            results: Results dictionary to filter (modified in place)
            status_filters: List of status filter strings
        """
        final_statuses = self.parse_filter_patterns(status_filters, self.all_statuses)
        
        # Apply filtering
        filtered_groups = {
            key: analysis
            for key, analysis in results['groups'].items()
            if analysis['status'] in final_statuses
        }
        results['groups'] = filtered_groups
        
        # Recalculate summary
        self._recalculate_summary(results)
    
    def apply_mal_status_filter(self, results: Dict[str, Any], mal_status_filters: List[str]) -> None:
        """Apply MyAnimeList status filters to results and update summary.
        
        Args:
            results: Results dictionary to filter (modified in place)
            mal_status_filters: List of MAL status filter strings
        """
        final_mal_statuses = self.parse_filter_patterns(mal_status_filters, self.all_mal_status_keys)
        
        # Convert to actual MAL status values for filtering
        final_mal_status_values = {self.mal_status_map[status] for status in final_mal_statuses}
        
        # Apply filtering
        filtered_groups = {}
        for group_key, analysis in results['groups'].items():
            mal_status = analysis.get('myanimelist_watch_status')
            if mal_status and mal_status.get('my_status'):
                if mal_status['my_status'] in final_mal_status_values:
                    filtered_groups[group_key] = analysis
            elif not mal_status:
                # Series has no MAL status - only include if no positive filters specified
                has_positive_filter = any(f.startswith('+') for f in mal_status_filters) or \
                                     any(f in self.all_mal_status_keys and not f.startswith('-') 
                                         for f in mal_status_filters)
                if not has_positive_filter:
                    filtered_groups[group_key] = analysis
        
        results['groups'] = filtered_groups
        
        # Recalculate summary
        self._recalculate_summary(results)
    
    def _recalculate_summary(self, results: Dict[str, Any]) -> None:
        """Recalculate summary statistics after filtering.
        
        Args:
            results: Results dictionary to update (modified in place)
        """
        filtered_groups = results['groups']
        total_series = len(filtered_groups)
        complete_series = sum(1 for a in filtered_groups.values() 
                            if a['status'] in [SeriesStatus.COMPLETE.value, 
                                              SeriesStatus.COMPLETE_WITH_EXTRAS.value])
        incomplete_series = sum(1 for a in filtered_groups.values() 
                              if a['status'] in [SeriesStatus.INCOMPLETE.value, 
                                                SeriesStatus.NO_EPISODE_NUMBERS.value])
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


class CommandLineArgumentParser:
    """Handles command-line argument parsing and validation."""
    
    def __init__(self):
        """Initialize the argument parser."""
        self.parser = self._create_parser()
    
    def _create_parser(self) -> argparse.ArgumentParser:
        """Create the argument parser with all options.
        
        Returns:
            Configured ArgumentParser instance
        """
        parser = argparse.ArgumentParser(
            description='Check series collection completeness using filename metadata',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog=self._get_usage_examples()
        )
        
        self._add_arguments(parser)
        return parser
    
    def _get_usage_examples(self) -> str:
        """Get usage examples text for help.
        
        Returns:
            Formatted usage examples string
        """
        return """
Common Usage Examples:

Basic completeness analysis (no exports or thumbnails):
  %(prog)s /path/to/series
  %(prog)s /path/to/series --recursive --export results.json --verbose 2

Filtered completeness analysis:
  %(prog)s /path/to/series --status-filter "+complete +complete_with_extras" --show-metadata year rating

MyAnimeList filtered analysis:
  %(prog)s /path/to/series --status-filter "incomplete" --mal-status-filter "+watching +plan-to-watch" --myanimelist-xml ~/myanimelist.xml
  %(prog)s /path/to/series --export series.json --webapp-export series.html --generate-thumbnails --myanimelist-xml ~/myanimelist.xml
  %(prog)s /path/to/series --export-bundle /path/to/output/bundle --recursive

Advanced Options:
  %(prog)s /path/to/series --exclude-paths /path/to/series/trash --include-patterns "*.mkv" "*.mp4"
  %(prog)s /path/to/series --status-filter "-unknown -no_metadata" --show-metadata genres director year
  %(prog)s /path/to/series --mal-status-filter "+completed +on-hold" --status-filter "complete" --myanimelist-xml ~/myanimelist.xml

Refresh Operations:
  %(prog)s --refresh-bundle /path/to/bundle/dir --myanimelist-xml ~/myanimelist.xml --refresh-bundle-metadata
  %(prog)s --webapp-refresh /path/to/series.json --webapp-export /path/to/updated.html --myanimelist-xml ~/myanimelist.xml
        """
    
    def _add_arguments(self, parser: argparse.ArgumentParser) -> None:
        """Add all command-line arguments to parser.
        
        Args:
            parser: ArgumentParser instance to add arguments to
        """
        # Positional arguments
        parser.add_argument('input_paths', nargs='*',
                           help='Input paths to search for series files (not required when using --refresh-bundle or --webapp-refresh)')
        
        # Refresh mode arguments
        parser.add_argument('--refresh-bundle', metavar='BUNDLE_DIR',
                           help='Regenerate webapp from existing bundle metadata.json. Provide path to bundle root directory. '
                                'Can be combined with --myanimelist-xml to refresh MAL metadata.')
        parser.add_argument('--refresh-bundle-metadata', action='store_true',
                           help='When used with --refresh-bundle, also save the refreshed metadata back to metadata.json. '
                                'This updates the stored metadata file with any MAL status changes.')
        parser.add_argument('--webapp-refresh', metavar='JSON_FILE',
                           help='Regenerate standalone webapp from existing metadata JSON file. '
                                'Can be combined with --myanimelist-xml to refresh MAL metadata.')
        
        # File discovery arguments
        parser.add_argument('--exclude-paths', nargs='*', default=[],
                           help='Paths to exclude from search')
        parser.add_argument('--include-patterns', nargs='*', default=['*.mkv', '*.mp4', '*.avi'],
                           help='Wildcard patterns for files to include (default: *.mkv *.mp4 *.avi)')
        parser.add_argument('--exclude-patterns', nargs='*', default=[],
                           help='Wildcard patterns for files to exclude')
        
        # Export arguments
        parser.add_argument('--export', metavar='FILE',
                           help='Export results to JSON file')
        parser.add_argument('--webapp-export', nargs='?', const=True, metavar='FILE',
                           help='Export results as a standalone HTML webapp. If filename omitted, derives name from --export argument (requires --export).')
        parser.add_argument('--export-bundle', metavar='DIR',
                           help='Export complete bundle (webapp + metadata.json + thumbnails) to specified directory. '
                                'This overrides --export, --webapp-export, and --generate-thumbnails. '
                                'Cannot be used together with those options.')
        
        # Search arguments
        parser.add_argument('--recursive', '-r', action='store_true',
                           help='Recursively search subdirectories (default: False)')
        
        # Output arguments
        parser.add_argument('--verbose', '-v', type=int, choices=[0, 1, 2, 3], default=1,
                           help='Verbosity level: 0=silent, 1=summary, 2=detailed, 3=very detailed (default: 1)')
        parser.add_argument('--quiet', '-q', action='store_true',
                           help='Same as --verbose 0')
        
        # Filter arguments
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
        
        # Metadata arguments
        parser.add_argument('--show-metadata', nargs='*', metavar='FIELD',
                           help='Show metadata fields in summary lines. Available fields depend on metadata source. '
                                'Common fields: year, rating, genres, director, actors, plot, runtime, imdb_id. '
                                'Example: --show-metadata year rating genres')
        parser.add_argument('--myanimelist-xml', metavar='PATH_OR_URL',
                           help='Path to MyAnimeList XML file (can be .gz) or URL for watch status lookup')
        
        # Thumbnail arguments
        parser.add_argument('--generate-thumbnails', action='store_true',
                           help='Generate static and animated webp thumbnails for each video file and store in thumbnail dir')
        parser.add_argument('--thumbnail-dir', default='~/.video_thumbnail_cache',
                           help='Directory to store video thumbnails (default: ~/.video_thumbnail_cache)')
    
    def parse_args(self, args=None):
        """Parse and validate command-line arguments.
        
        Args:
            args: Optional list of argument strings (defaults to sys.argv)
            
        Returns:
            Parsed arguments namespace with 'refresh_mode' attribute added
        """
        parsed = self.parser.parse_args(args)
        self._validate_args(parsed)
        # Add refresh_mode attribute for convenience
        parsed.refresh_mode = self._get_refresh_mode(parsed)
        return parsed
    
    def _validate_args(self, args) -> None:
        """Validate argument combinations and conflicts.
        
        Args:
            args: Parsed arguments namespace
            
        Raises:
            SystemExit: If validation fails
        """
        # Check for refresh modes
        refresh_mode = self._get_refresh_mode(args)
        
        # Validate input_paths requirement (not needed in refresh modes)
        if not refresh_mode and (not args.input_paths or len(args.input_paths) == 0):
            self.parser.error('At least one input path is required unless using --refresh-bundle or --webapp-refresh')
        
        # Check for incompatible arguments in refresh modes
        if refresh_mode:
            self._validate_refresh_mode_args(args, refresh_mode)
        
        # Check for conflicts with --export-bundle
        if hasattr(args, 'export_bundle') and args.export_bundle:
            self._validate_bundle_export_args(args)
        
        # Validate and derive webapp export filename
        self._validate_webapp_export(args)
        
        # Handle quiet flag
        if args.quiet:
            args.verbose = 0
    
    def _get_refresh_mode(self, args) -> Optional[str]:
        """Determine which refresh mode is active.
        
        Args:
            args: Parsed arguments namespace
            
        Returns:
            'bundle', 'webapp', or None
        """
        if hasattr(args, 'refresh_bundle') and args.refresh_bundle:
            return 'bundle'
        elif hasattr(args, 'webapp_refresh') and args.webapp_refresh:
            return 'webapp'
        return None
    
    def _validate_refresh_mode_args(self, args, refresh_mode: str) -> None:
        """Validate arguments in refresh mode.
        
        Args:
            args: Parsed arguments namespace
            refresh_mode: The active refresh mode ('bundle' or 'webapp')
        """
        incompatible = []
        if args.input_paths:
            incompatible.append('input_paths')
        if hasattr(args, 'generate_thumbnails') and args.generate_thumbnails:
            incompatible.append('--generate-thumbnails')
        if hasattr(args, 'export') and args.export:
            incompatible.append('--export')
        if hasattr(args, 'export_bundle') and args.export_bundle:
            incompatible.append('--export-bundle')
        
        if incompatible:
            mode_flag = '--refresh-bundle' if refresh_mode == 'bundle' else '--webapp-refresh'
            self.parser.error(f'{mode_flag} cannot be used with: {", ".join(incompatible)}')
    
    def _validate_bundle_export_args(self, args) -> None:
        """Validate bundle export arguments.
        
        Args:
            args: Parsed arguments namespace
        """
        conflicts = []
        if hasattr(args, 'export') and args.export:
            conflicts.append('--export')
        if hasattr(args, 'webapp_export') and args.webapp_export:
            conflicts.append('--webapp-export')
        if hasattr(args, 'generate_thumbnails') and args.generate_thumbnails:
            conflicts.append('--generate-thumbnails')
        
        if conflicts:
            self.parser.error(f'--export-bundle cannot be used with: {", ".join(conflicts)}')
    
    def _validate_webapp_export(self, args) -> None:
        """Validate and derive webapp export filename.
        
        Args:
            args: Parsed arguments namespace (modified in place)
        """
        if args.webapp_export:
            if args.webapp_export is True:
                # Derive filename from --export argument
                if not args.export:
                    self.parser.error('--webapp-export without filename requires --export to be specified')
                args.webapp_export = Path(args.export).with_suffix('.html')
            else:
                args.webapp_export = Path(args.webapp_export)


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
                results['groups'][group_key] = analysis.to_dict()
                
                # Update summary
                results['completeness_summary']['total_series'] += 1
                if analysis.status in [SeriesStatus.COMPLETE, SeriesStatus.COMPLETE_WITH_EXTRAS]:
                    results['completeness_summary']['complete_series'] += 1
                elif analysis.status in [SeriesStatus.INCOMPLETE, SeriesStatus.NO_EPISODE_NUMBERS]:
                    results['completeness_summary']['incomplete_series'] += 1
                else:
                    results['completeness_summary']['unknown_series'] += 1

                results['completeness_summary']['total_episodes_found'] += analysis.episodes_found
                results['completeness_summary']['total_episodes_expected'] += analysis.episodes_expected
                
                # Update progress with current series name
                display_title = analysis.title[:30]
                if len(analysis.title) > 30:
                    display_title += "..."
                pbar.set_postfix(current=display_title)
        
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
    
    def _analyze_group_completeness(self, group_key: str, group_files: List[Dict]) -> SeriesAnalysis:
        """Analyze a single group for completeness.
        
        Returns:
            SeriesAnalysis instance
        """
        if not group_files:
            return SeriesAnalysis(
                title='Unknown',
                status=SeriesStatus.UNKNOWN,
                episodes_found=0,
                episodes_expected=0
            )
        
        # Extract basic info from group
        first_file = group_files[0]
        title = first_file.get('title', 'Unknown')
        season = first_file.get('season')
        metadata_id = first_file.get('metadata_id')
        
        # Parse episode files and extract episode information
        episode_files, extra_files, episode_numbers, enhanced_episode_info = self._parse_episode_files(group_files)
        
        episodes_found = len(episode_files)
        
        # Calculate watch status
        watch_status, mal_watch_status = self._calculate_watch_status(episode_files, first_file, season)
        
        # Build base result
        result = self._build_base_result(
            title, season, metadata_id, episodes_found, episode_numbers,
            extra_files, group_files, watch_status, mal_watch_status
        )
        
        # Determine completeness status from metadata
        self._determine_completeness_status(result, first_file, episode_numbers, title, season, group_files)
        
        return result
    
    def _calculate_season_specific_mal_status(self, series_mal_status: Dict[str, Any], season: int, episode_files: List[Dict]) -> Optional[Dict[str, Any]]:
        """Calculate season-specific MyAnimeList watch status from series-level status.
        
        Args:
            series_mal_status: The series-level MAL watch status
            season: The season number to calculate status for
            episode_files: List of episode file info dicts for this season
            
        Returns:
            Season-specific MAL watch status dict with watched_episodes count
        """
        if not series_mal_status or not episode_files:
            return None
        
        # Get the episode numbers for this season
        season_episode_numbers = []
        for file_info in episode_files:
            # Try to get the original absolute episode number
            original_ep = file_info.get('original_episode')
            if original_ep:
                season_episode_numbers.append(original_ep)
            else:
                # Fallback to regular episode number
                ep = file_info.get('episode')
                if ep:
                    season_episode_numbers.append(ep)
        
        if not season_episode_numbers:
            return None
        
        # Get watched episode count from series-level status
        series_watched_count = series_mal_status.get('watched_episodes', 0)
        
        # Count how many of this season's episodes have been watched according to MAL
        season_watched_count = 0
        for ep_num in season_episode_numbers:
            if ep_num <= series_watched_count:
                season_watched_count += 1
        
        # Create season-specific status
        season_status = {
            'status': series_mal_status.get('status'),
            'score': series_mal_status.get('score'),
            'watched_episodes': season_watched_count,
            'total_episodes': len(season_episode_numbers),
            'tags': series_mal_status.get('tags', [])
        }
        
        # Update status based on completion
        if season_watched_count == len(season_episode_numbers):
            if season_status['status'] == 'Watching':
                season_status['status'] = 'Completed (Season)'
        elif season_watched_count > 0:
            if season_status['status'] in ['Completed', 'Plan to Watch']:
                season_status['status'] = 'Watching'
        
        return season_status

    def _get_anime_metadata_provider(self):
        """Get anime metadata provider from metadata manager.
        
        Returns:
            Anime metadata provider instance or None if not available
        """
        if not self.metadata_manager or not hasattr(self.metadata_manager, 'providers'):
            return None
        
        try:
            for provider in self.metadata_manager.providers:
                if 'anime' in provider.__class__.__name__.lower():
                    return provider
        except:
            pass
        
        return None

    def _extract_episode_info_from_file(self, file_info: Dict, anime_metadata) -> Optional[Dict]:
        """Extract enhanced episode information from a file.
        
        Args:
            file_info: File information dictionary
            anime_metadata: Anime metadata provider instance
            
        Returns:
            Enhanced episode info dict or None
        """
        # Check if file already has enhanced episode info from FileGrouper
        episode_info = file_info.get('episode_info')
        if episode_info:
            return episode_info
        
        # Try direct anime metadata parsing if FileGrouper didn't extract episode info
        if not anime_metadata or not hasattr(anime_metadata, 'get_episode_info'):
            return None
        
        file_path = file_info.get('file_path', '')
        if isinstance(file_path, Path):
            filename = file_path.name
        else:
            filename = str(file_path).split('/')[-1] if '/' in str(file_path) else str(file_path).split('\\')[-1]
        
        try:
            return anime_metadata.get_episode_info(filename)
        except:
            return None

    def _classify_file_by_episode_info(self, file_info: Dict, episode_info: Dict, 
                                       episode_files: List[Dict], extra_files: List[Dict],
                                       episode_numbers: List[int]) -> None:
        """Classify a file as episode or extra based on episode info.
        
        Args:
            file_info: File information dictionary
            episode_info: Enhanced episode information
            episode_files: List to append episode files to (modified in place)
            extra_files: List to append extra files to (modified in place)
            episode_numbers: List to append episode numbers to (modified in place)
        """
        if episode_info.get('episode_type') in ['OP', 'ED', 'Special', 'OVA']:
            extra_files.append(file_info)
        else:
            episode_files.append(file_info)
            ep_num = episode_info.get('episode')
            if ep_num:
                episode_numbers.append(ep_num)

    def _classify_file_by_guessit(self, file_info: Dict, episode_files: List[Dict], 
                                   extra_files: List[Dict], episode_numbers: List[int]) -> None:
        """Classify a file using fallback guessit-based logic.
        
        Args:
            file_info: File information dictionary
            episode_files: List to append episode files to (modified in place)
            extra_files: List to append extra files to (modified in place)
            episode_numbers: List to append episode numbers to (modified in place)
        """
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

    def _parse_episode_files(self, group_files: List[Dict]) -> tuple:
        """Parse episode files and extract episode information.
        
        Args:
            group_files: List of file information dictionaries
            
        Returns:
            Tuple of (episode_files, extra_files, episode_numbers, enhanced_episode_info)
        """
        episode_files = []
        extra_files = []
        episode_numbers = []
        enhanced_episode_info = {}
        
        anime_metadata = self._get_anime_metadata_provider()
        
        for file_info in group_files:
            episode_info = self._extract_episode_info_from_file(file_info, anime_metadata)
            
            if episode_info:
                # Store enhanced metadata
                file_path = file_info.get('file_path', '')
                enhanced_episode_info[file_path if file_path else ''] = episode_info
                
                # Classify file
                self._classify_file_by_episode_info(
                    file_info, episode_info, episode_files, extra_files, episode_numbers
                )
            else:
                # Fallback to guessit-based logic
                self._classify_file_by_guessit(
                    file_info, episode_files, extra_files, episode_numbers
                )
        
        episode_numbers = sorted(set(episode_numbers)) if episode_numbers else []
        return episode_files, extra_files, episode_numbers, enhanced_episode_info

    def _calculate_watch_status(self, episode_files: List[Dict], first_file: Dict, 
                                season: Optional[int]) -> tuple:
        """Calculate watch status for a group of episodes.
        
        Args:
            episode_files: List of episode file information dicts
            first_file: First file in the group for metadata lookup
            season: Season number or None
            
        Returns:
            Tuple of (watch_status_dict, mal_watch_status)
        """
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
        
        episodes_found = len(episode_files)
        watch_status = WatchStatus(
            watched_episodes=watched_count,
            partially_watched_episodes=partially_watched_count,
            unwatched_episodes=episodes_found - watched_count - partially_watched_count,
            total_watch_count=total_watch_count,
            completion_percent=(watched_count / episodes_found * 100) if episodes_found > 0 else 0
        )
        
        return watch_status, mal_watch_status

    def _build_base_result(self, title: str, season: Optional[int], metadata_id: Optional[str],
                           episodes_found: int, episode_numbers: List[int], 
                           extra_files: List[Dict], group_files: List[Dict],
                           watch_status: WatchStatus, mal_watch_status: Optional[Dict]) -> SeriesAnalysis:
        """Build the base SeriesAnalysis for a group.
        
        Args:
            title: Series title
            season: Season number or None
            metadata_id: Metadata ID for the series
            episodes_found: Number of episode files found
            episode_numbers: List of episode numbers
            extra_files: List of extra file information dicts
            group_files: All files in the group
            watch_status: Watch status dataclass instance
            mal_watch_status: MyAnimeList watch status or None
            
        Returns:
            SeriesAnalysis instance
        """
        return SeriesAnalysis(
            title=title,
            season=season,
            metadata_id=metadata_id,
            episodes_found=episodes_found,
            episodes_expected=0,
            status=SeriesStatus.UNKNOWN,
            episode_numbers=episode_numbers,
            missing_episodes=[],
            extra_episodes=[],
            files=group_files,
            extra_files=extra_files,
            watch_status=watch_status,
            myanimelist_watch_status=mal_watch_status
        )

    def _check_movie_type(self, first_file: Dict, metadata: Dict, result: SeriesAnalysis) -> bool:
        """Check if the content is a movie and update result accordingly.
        
        Args:
            first_file: First file in the group
            metadata: Metadata dictionary from provider
            result: SeriesAnalysis instance to update
            
        Returns:
            True if it's a movie, False otherwise
        """
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
            result.status = SeriesStatus.MOVIE
            result.episodes_expected = expected_episodes or 1
            return True
        
        return False

    def _determine_expected_episodes(self, title: str, season: Optional[int], 
                                     total_episodes: int, episode_numbers: List[int],
                                     group_files: List[Dict]) -> int:
        """Determine the expected number of episodes for a season.
        
        Args:
            title: Series title
            season: Season number or None
            total_episodes: Total episodes from metadata
            episode_numbers: List of episode numbers found
            group_files: All files in the group
            
        Returns:
            Expected number of episodes
        """
        if not episode_numbers:
            return total_episodes
        
        # Try to use enhanced metadata for season-specific episode count
        enhanced_info, provider = self.metadata_manager.find_title(title)
        if not enhanced_info or not provider or not hasattr(provider, 'get_episode_info'):
            return max(episode_numbers)
        
        max_episode_in_season = max(episode_numbers)
        
        # Check if there's a next episode after our max to determine season completion
        next_episode_original = None
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
                    expected_episodes = max_episode_in_season + 1
        
        return expected_episodes

    def _determine_completeness_status(self, result: SeriesAnalysis, first_file: Dict,
                                       episode_numbers: List[int], title: str,
                                       season: Optional[int], group_files: List[Dict]) -> None:
        """Determine completeness status by checking metadata.
        
        Args:
            result: SeriesAnalysis instance to update (modified in place)
            first_file: First file in the group
            episode_numbers: List of episode numbers
            title: Series title
            season: Season number or None
            group_files: All files in the group
        """
        if not self.metadata_manager:
            result['status'] = SeriesStatus.NO_METADATA_MANAGER.value
            return
        
        metadata_id = first_file.get('metadata_id')
        if not metadata_id or metadata_id not in self.file_grouper.title_metadata:
            result['status'] = SeriesStatus.NO_METADATA.value
            return
        
        metadata = self.file_grouper.title_metadata[metadata_id]['metadata']
        
        # Check for movie type
        if self._check_movie_type(first_file, metadata, result):
            return
        
        # Check if it's a series
        if 'series' not in metadata.get('type', '').lower() and 'tv' not in metadata.get('type', '').lower():
            result.status = SeriesStatus.NOT_SERIES
            return
        
        # Process series completeness
        total_episodes = metadata.get('total_episodes')
        if not total_episodes:
            result.status = SeriesStatus.UNKNOWN_TOTAL_EPISODES
            return
        
        result.episodes_expected = total_episodes
        
        # Determine expected episodes for this season
        expected_episodes = self._determine_expected_episodes(
            title, season, total_episodes, episode_numbers, group_files
        )
        
        result.episodes_expected = expected_episodes
        
        # Check for missing/extra episodes
        if not episode_numbers:
            result.status = SeriesStatus.NO_EPISODE_NUMBERS
            return
        
        expected_range = list(range(1, int(expected_episodes) + 1))
        missing = [ep for ep in expected_range if ep not in episode_numbers]
        extra = [ep for ep in episode_numbers if ep > expected_episodes]
        
        result.missing_episodes = missing
        result.extra_episodes = extra
        
        if not missing and not extra:
            result.status = SeriesStatus.COMPLETE
        elif missing:
            result.status = SeriesStatus.INCOMPLETE
        else:
            result.status = SeriesStatus.COMPLETE_WITH_EXTRAS

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

        # Set thumbnail directory for webapp to use for hash-based lookup
        # The webapp JavaScript will calculate thumbnail paths on-the-fly using SHA256 hash of filenames
        if use_relative_thumbnails and thumbnail_relative_path:
            # For bundle exports, use relative path
            results['thumbnail_dir'] = thumbnail_relative_path
        else:
            # For standalone exports, use absolute path or default
            results['thumbnail_dir'] = results.get('thumbnail_dir') or os.path.expanduser('~/.video_thumbnail_cache')
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
        
        # Status emoji - convert string status to enum and get emoji
        try:
            status_enum = SeriesStatus(status) if isinstance(status, str) else status
            status_emoji = STATUS_EMOJI.get(status_enum, '❓')
        except (ValueError, KeyError):
            status_emoji = '❓'
        
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

    def _copy_thumbnails_from_global_cache(self, target_dir: str, files: List[Path], verbosity: int) -> int:
        """Copy existing thumbnails from global cache to target directory.
        
        Args:
            target_dir: Target directory for thumbnails
            files: List of video file paths
            verbosity: Verbosity level for logging
            
        Returns:
            Number of thumbnail pairs copied
        """
        import shutil
        global_cache_dir = os.path.expanduser('~/.video_thumbnail_cache')
        
        if not os.path.exists(global_cache_dir):
            return 0
        
        if verbosity >= 1:
            print(f"Checking global cache at {global_cache_dir} for existing thumbnails...")
        
        os.makedirs(target_dir, exist_ok=True)
        
        global_generator = VideoThumbnailGenerator(global_cache_dir, max_height=480)
        copied_from_cache = 0
        
        for file_info in files:
            file_path = file_info if isinstance(file_info, (str, Path)) else file_info.get('path')
            existing = global_generator.get_thumbnail_for_video(str(file_path))
            
            # If thumbnails exist in cache, copy them to target
            if existing.get('static_thumbnail') and existing.get('animated_thumbnail'):
                try:
                    static_dest = os.path.join(target_dir, os.path.basename(existing['static_thumbnail']))
                    animated_dest = os.path.join(target_dir, os.path.basename(existing['animated_thumbnail']))
                    
                    shutil.copy2(existing['static_thumbnail'], static_dest)
                    shutil.copy2(existing['animated_thumbnail'], animated_dest)
                    copied_from_cache += 1
                except Exception as e:
                    if verbosity >= 2:
                        print(f"Could not copy cached thumbnails for {file_path}: {e}")
        
        if verbosity >= 1 and copied_from_cache > 0:
            print(f"Copied {copied_from_cache} thumbnail pairs from global cache")
        
        return copied_from_cache

    def _setup_thumbnail_generator(self, thumbnail_dir: str, files: List[Path] = None, 
                                   use_global_cache: bool = False, verbosity: int = 1) -> VideoThumbnailGenerator:
        """Set up thumbnail generator with optional global cache copying.
        
        Args:
            thumbnail_dir: Target directory for thumbnails
            files: List of video file paths (required if use_global_cache is True)
            use_global_cache: Whether to copy from global cache first
            verbosity: Verbosity level for logging
            
        Returns:
            VideoThumbnailGenerator instance
        """
        thumbnail_dir_expanded = os.path.expanduser(thumbnail_dir)
        
        if use_global_cache and files:
            self._copy_thumbnails_from_global_cache(thumbnail_dir_expanded, files, verbosity)
        
        return VideoThumbnailGenerator(thumbnail_dir_expanded, max_height=480)

def _refresh_myanimelist_metadata(results: Dict[str, Any], myanimelist_xml_path: str, verbosity: int) -> None:
    """Refresh MyAnimeList metadata in loaded results.
    
    Args:
        results: The loaded results dictionary to update
        myanimelist_xml_path: Path to MyAnimeList XML file
        verbosity: Verbosity level for output
    """
    if verbosity >= 1:
        print(f"Refreshing MyAnimeList metadata from {myanimelist_xml_path}...")
    
    # Import MyAnimeList watch status module
    try:
        import sys
        video_optimizer_path = Path(__file__).parent / 'video-optimizer-v2'
        if video_optimizer_path.exists() and str(video_optimizer_path) not in sys.path:
            sys.path.insert(0, str(video_optimizer_path))
        
        from myanimelist_watch_status import MyAnimeListWatchStatusProvider
        
        # Load MyAnimeList data
        mal_provider = MyAnimeListWatchStatusProvider(myanimelist_xml_path)
        
        # Helper function to serialize MAL status
        def serialize_mal_status(mal_status):
            return {
                'series_animedb_id': mal_status.series_animedb_id,
                'series_title': mal_status.series_title,
                'my_status': mal_status.my_status,
                'my_watched_episodes': mal_status.my_watched_episodes,
                'my_score': mal_status.my_score,
                'score': mal_status.my_score,  # Alias for consistency
                'my_start_date': mal_status.my_start_date,
                'my_finish_date': mal_status.my_finish_date,
                'my_times_watched': mal_status.my_times_watched,
                'my_rewatching': mal_status.my_rewatching,
                'series_episodes': mal_status.series_episodes,
                'progress_percent': mal_status.progress_percent
            }
        
        # Update title_metadata with fresh MAL data
        title_metadata_found = 0
        title_metadata_updated = 0
        title_metadata_unchanged = 0
        title_metadata_cleared = 0
        update_details = []
        
        for metadata_id, metadata_entry in results.get('title_metadata', {}).items():
            # Check if this metadata entry has MyAnimeList sources
            # The sources are directly in the metadata_entry, not nested under 'metadata'
            sources = metadata_entry.get('sources', [])
            
            # Look for MyAnimeList source
            mal_source = None
            for source in sources:
                if isinstance(source, str) and 'myanimelist' in source.lower():
                    mal_source = source
                    break
            
            if mal_source:
                title_metadata_found += 1
                # Get fresh MAL status from provider
                mal_status = mal_provider.get_watch_status(mal_source)
                if mal_status:
                    old_mal = metadata_entry.get('myanimelist_watch_status', {})
                    new_mal = serialize_mal_status(mal_status)
                    
                    # Check if anything actually changed
                    changed = False
                    changes = []
                    
                    if old_mal.get('my_status') != new_mal.get('my_status'):
                        changed = True
                        changes.append(f"status: {old_mal.get('my_status', 'None')} -> {new_mal.get('my_status')}")
                    
                    if old_mal.get('my_watched_episodes') != new_mal.get('my_watched_episodes'):
                        changed = True
                        changes.append(f"watched: {old_mal.get('my_watched_episodes', 0)} -> {new_mal.get('my_watched_episodes')}")
                    
                    if old_mal.get('my_score') != new_mal.get('my_score'):
                        changed = True
                        changes.append(f"score: {old_mal.get('my_score', 0)} -> {new_mal.get('my_score')}")
                    
                    if changed:
                        metadata_entry['myanimelist_watch_status'] = new_mal
                        title_metadata_updated += 1
                        title = metadata_entry.get('title', f'ID:{metadata_id}')
                        update_details.append(f"  • {title}: {', '.join(changes)}")
                    else:
                        title_metadata_unchanged += 1
                else:
                    # Clear MAL status if not found in new data
                    if 'myanimelist_watch_status' in metadata_entry:
                        metadata_entry.pop('myanimelist_watch_status')
                        title = metadata_entry.get('title', f'ID:{metadata_id}')
                        update_details.append(f"  • {title}: CLEARED (not found in MAL XML)")
                        title_metadata_cleared += 1
        
        # Add missing title_metadata entries for series in groups that don't have metadata yet
        # This ensures new series with MAL data get proper title_metadata entries
        title_metadata = results.get('title_metadata', {})
        for group_key, analysis in results['groups'].items():
            metadata_id = analysis.get('title_id')
            if not metadata_id and analysis.get('files'):
                metadata_id = analysis['files'][0].get('metadata_id')
            
            if metadata_id and metadata_id not in title_metadata:
                # Create minimal metadata entry
                title_metadata[metadata_id] = {
                    'title': analysis.get('title', f'ID:{metadata_id}'),
                    'sources': []
                }
        
        # Update each series group with fresh MAL metadata
        groups_updated = 0
        groups_unchanged = 0
        for group_key, analysis in results['groups'].items():
            try:
                title = analysis.get('title', '')
                season = analysis.get('season')
                
                # Get the metadata_id for this series (prefer title_id, fallback to first file's metadata_id)
                metadata_id = analysis.get('title_id')
                if not metadata_id and analysis.get('files'):
                    metadata_id = analysis['files'][0].get('metadata_id')
                
                if metadata_id:
                    # Get MAL status from title_metadata
                    metadata_entry = results.get('title_metadata', {}).get(metadata_id, {})
                    mal_metadata = metadata_entry.get('myanimelist_watch_status')
                    
                    # If no MAL metadata in title_metadata, try to fetch it directly from the provider
                    if not mal_metadata and metadata_id:
                        # For Season 2+, search season-specific entries FIRST, then fall back to base title
                        mal_status = None
                        season_specific_match = None
                        
                        if season and season > 1:
                            # Search for season-specific MAL entries before checking metadata_id
                            title_lower = title.lower()
                            
                            # Extract base title by removing common separators and trailing parts
                            # This handles titles like "Title - Subtitle", "Title: Subtitle", etc.
                            base_title_parts = title_lower.replace(':', ' ').replace(' - ', ' ').split()
                            # Get first significant words (ignore articles)
                            significant_words = [w for w in base_title_parts if w not in ['the', 'a', 'an']]
                            # Use first 1-3 significant words as search terms to match partial titles
                            search_terms = significant_words[:3] if significant_words else base_title_parts[:3]
                            
                            for mal_id, mal_entry in mal_provider.anime_status_map.items():
                                entry_title_lower = mal_entry.series_title.lower()
                                
                                # Check if entry title contains any of our search terms
                                has_title_match = any(term in entry_title_lower for term in search_terms if term)
                                
                                if has_title_match:
                                    # Check for season indicators
                                    has_season_indicator = (
                                        f'part {season}' in entry_title_lower or
                                        f'season {season}' in entry_title_lower or
                                        (season == 2 and ('2nd' in entry_title_lower or 'part 2' in entry_title_lower or 'part two' in entry_title_lower or 'ii' in entry_title_lower.split())) or
                                        (season == 3 and ('3rd' in entry_title_lower or 'part 3' in entry_title_lower or 'part three' in entry_title_lower or 'iii' in entry_title_lower.split())) or
                                        (season == 4 and ('4th' in entry_title_lower or 'part 4' in entry_title_lower or 'part four' in entry_title_lower or 'iv' in entry_title_lower.split())) or
                                        (season >= 5 and (f'{season}th' in entry_title_lower or f'part {season}' in entry_title_lower))
                                    )
                                    
                                    if has_season_indicator:
                                        season_specific_match = mal_entry
                                        if verbosity >= 2:
                                            print(f"  Found season-specific MAL entry for '{title}' Season {season}: ID {mal_id} - {mal_entry.series_title}")
                                        break
                        
                        # Prioritize season-specific match, otherwise use metadata_id lookup
                        if season_specific_match:
                            mal_status = season_specific_match
                        else:
                            # Fall back to metadata_id lookup (for Season 1 or when no season-specific entry exists)
                            mal_status = mal_provider.anime_status_map.get(str(metadata_id))
                        
                        if mal_status:
                            mal_metadata = serialize_mal_status(mal_status)
                            # Store it in title_metadata for consistency
                            if metadata_id in results.get('title_metadata', {}):
                                results['title_metadata'][metadata_id]['myanimelist_watch_status'] = mal_metadata
                    
                    if mal_metadata:
                        # Calculate season-specific watch status if applicable
                        if season and analysis.get('files'):
                            episode_files = [f for f in analysis['files'] if f.get('episode') is not None]
                            if episode_files:
                                # Create a temporary checker instance to use the calculation method
                                temp_checker = SeriesCompletenessChecker(metadata_only=True)
                                season_mal = temp_checker._calculate_season_specific_mal_status(
                                    mal_metadata, season, episode_files
                                )
                                if season_mal:
                                    mal_metadata = season_mal
                        
                        # Ensure mal_metadata is still valid after season calculation
                        if not mal_metadata:
                            continue
                        
                        # Check if this is actually different from what we had
                        old_mal = analysis.get('myanimelist_watch_status') or {}
                        if (old_mal.get('my_status') != mal_metadata.get('my_status') or
                            old_mal.get('my_watched_episodes') != mal_metadata.get('my_watched_episodes') or
                            old_mal.get('my_score') != mal_metadata.get('my_score')):
                            analysis['myanimelist_watch_status'] = mal_metadata
                            groups_updated += 1
                        else:
                            # Even if MAL status unchanged, still update it
                            analysis['myanimelist_watch_status'] = mal_metadata
                            groups_unchanged += 1
                        
                        # Always recalculate watch_status based on MAL data when refreshing
                        # This ensures the UI reflects MAL watch status
                        episodes_found = analysis.get('episodes_found', 0)
                        mal_watched = mal_metadata.get('my_watched_episodes', 0)
                        
                        old_watch_status = analysis.get('watch_status', {})
                        new_watch_status = {
                            'watched_episodes': mal_watched,
                            'partially_watched_episodes': 0,
                            'unwatched_episodes': max(0, episodes_found - mal_watched),
                            'total_watch_count': mal_watched,
                            'completion_percent': (mal_watched / episodes_found * 100) if episodes_found > 0 else 0
                        }
                        analysis['watch_status'] = new_watch_status
                    else:
                        # Clear existing MAL metadata if not found
                        if 'myanimelist_watch_status' in analysis:
                            analysis.pop('myanimelist_watch_status')
                            groups_updated += 1
                        else:
                            groups_unchanged += 1
            except Exception as e:
                if verbosity >= 2:
                    print(f"Warning: Error processing group '{group_key}': {e}")
                    import traceback
                    traceback.print_exc()
        
        if verbosity >= 1:
            print(f"✓ MyAnimeList metadata refresh complete:")
            print(f"  Title metadata: {title_metadata_found} found, {title_metadata_updated} updated, {title_metadata_unchanged} unchanged, {title_metadata_cleared} cleared")
            print(f"  Series groups: {groups_updated} updated, {groups_unchanged} unchanged")
            
            if verbosity >= 2 and update_details:
                print(f"\nDetailed changes:")
                for detail in update_details[:20]:  # Limit to first 20 to avoid spam
                    print(detail)
                if len(update_details) > 20:
                    print(f"  ... and {len(update_details) - 20} more")
    
    except ImportError as e:
        if verbosity >= 1:
            print(f"Warning: Could not load MyAnimeList module: {e}")
    except Exception as e:
        if verbosity >= 1:
            print(f"Warning: Error refreshing MyAnimeList metadata: {e}")

def _handle_refresh_bundle_mode(checker: 'SeriesCompletenessChecker', bundle_dir: str, verbosity: int, update_metadata: bool = False) -> None:
    """Handle --refresh-bundle mode: regenerate webapp from existing bundle metadata.
    
    Args:
        checker: SeriesCompletenessChecker instance
        bundle_dir: Path to bundle directory
        verbosity: Verbosity level
        update_metadata: If True, also save refreshed metadata back to metadata.json
    """
    bundle_root = Path(bundle_dir)
    metadata_path = bundle_root / 'metadata.json'
    
    if not metadata_path.exists():
        print(f"Error: metadata.json not found in bundle directory: {metadata_path}")
        return
    
    if verbosity >= 1:
        print(f"Loading metadata from {metadata_path}...")
    
    results = checker.load_results(str(metadata_path))
    
    # Refresh MyAnimeList metadata if provided
    if checker.myanimelist_xml_path:
        _refresh_myanimelist_metadata(results, checker.myanimelist_xml_path, verbosity)
    
    # Save updated metadata if requested
    if update_metadata:
        if verbosity >= 1:
            print(f"Saving updated metadata to {metadata_path}...")
        
        # Ensure thumbnail_dir is set for bundle (webapp will calculate paths on-the-fly)
        if 'thumbnail_dir' not in results:
            results['thumbnail_dir'] = 'thumbnails'
        
        checker.export_results(results, str(metadata_path))
        if verbosity >= 1:
            print(f"✓ Metadata updated: {metadata_path}")
    
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
    
    # Refresh MyAnimeList metadata if provided
    if checker.myanimelist_xml_path:
        _refresh_myanimelist_metadata(results, checker.myanimelist_xml_path, verbosity)
    
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

def _handle_thumbnail_generation(files: List[Path], args, verbosity: int, checker: SeriesCompletenessChecker = None) -> Optional[str]:
    """Handle thumbnail generation and return the thumbnail directory path.
    
    Args:
        files: List of video file paths
        args: Command-line arguments
        verbosity: Verbosity level for logging
        checker: SeriesCompletenessChecker instance (optional, for using helper methods)
        
    Returns:
        Path to thumbnail directory or None
    """
    should_generate_thumbnails = args.generate_thumbnails
    thumbnail_dir = args.thumbnail_dir
    
    # If using bundle mode, override thumbnail settings
    if hasattr(args, 'export_bundle') and args.export_bundle:
        should_generate_thumbnails = True
        bundle_root = Path(args.export_bundle)
        thumbnail_dir = str(bundle_root / 'thumbnails')
    
    if not should_generate_thumbnails:
        return None
    
    # Use helper method if checker instance is available
    use_global_cache = hasattr(args, 'export_bundle') and args.export_bundle
    if checker:
        generator = checker._setup_thumbnail_generator(
            thumbnail_dir, files=files, use_global_cache=use_global_cache, verbosity=verbosity
        )
    else:
        # Fallback to direct implementation if no checker instance
        thumbnail_dir_expanded = os.path.expanduser(thumbnail_dir)
        generator = VideoThumbnailGenerator(thumbnail_dir_expanded, max_height=480)
    
    # Generate thumbnails (will skip files that already have thumbnails in target dir)
    thumbnail_index = generator.generate_thumbnails_for_videos(
        files, verbose=verbosity, force_regenerate=False, show_progress=(verbosity >= 1)
    )
    generator.save_thumbnail_index(thumbnail_index, verbose=verbosity)
    
    return generator.thumbnail_dir

def _apply_status_filters(results: Dict[str, Any], status_filters: List[str]) -> None:
    """Apply status filters to results and update summary.
    
    This is a backward compatibility wrapper for ResultsFilter.
    """
    filter_handler = ResultsFilter()
    filter_handler.apply_status_filter(results, status_filters)

def _apply_mal_status_filters(results: Dict[str, Any], mal_status_filters: List[str]) -> None:
    """Apply MyAnimeList status filters to results and update summary.
    
    This is a backward compatibility wrapper for ResultsFilter.
    """
    filter_handler = ResultsFilter()
    filter_handler.apply_mal_status_filter(results, mal_status_filters)

def _recalculate_summary(results: Dict[str, Any]) -> None:
    """Recalculate summary statistics after filtering.
    
    This is a backward compatibility wrapper for ResultsFilter.
    """
    filter_handler = ResultsFilter()
    filter_handler._recalculate_summary(results)

def main():
    """Command-line interface for series completeness checker."""
    # Parse and validate arguments using CommandLineArgumentParser
    arg_parser = CommandLineArgumentParser()
    args = arg_parser.parse_args()
    
    verbosity = args.verbose
    refresh_mode = args.refresh_mode
    webapp_export_path = args.webapp_export
    
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
        update_metadata = args.refresh_bundle_metadata if hasattr(args, 'refresh_bundle_metadata') else False
        _handle_refresh_bundle_mode(checker, args.refresh_bundle, verbosity, update_metadata)
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
        if webapp_export_path:
            checker.export_webapp(results, webapp_export_path)
            if verbosity >= 1:
                print(f"\nExported webapp to: {webapp_export_path}")

if __name__ == '__main__':
    main()