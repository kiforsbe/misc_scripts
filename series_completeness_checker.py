import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional
from collections import defaultdict

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

# Import the FileGrouper class and related components
from file_grouper import FileGrouper, CustomJSONEncoder

# Try to get metadata manager - it may not be available if dependencies aren't installed
try:
    from file_grouper import get_metadata_manager
    metadata_manager_available = True
except ImportError:
    metadata_manager_available = False
    def get_metadata_manager():
        return None

# MetadataManager class may not be available as a direct import
MetadataManager = None

class SeriesCompletenessChecker:
    """Checks series collection completeness using FileGrouper and metadata providers."""
    
    def __init__(self, metadata_manager=None):
        self.file_grouper = FileGrouper(metadata_manager)
        self.metadata_manager = metadata_manager
        self.completeness_results = {}
    
    def analyze_series_collection(self, files: List[Path], show_progress: bool = True) -> Dict[str, Any]:
        """Analyze series collection for completeness."""
        # Group files by title and season with progress tracking
        groups = self.file_grouper.group_files(files, ['title', 'season'], show_progress)

        # Export title metadata for completeness analysis
        title_metadata_export = {}
        for key, value in self.file_grouper.title_metadata.items():
            title_metadata_export[key] = value['metadata']
        
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
        
        return results
    
    def _analyze_group_completeness(self, group_key: str, group_files: List[Dict]) -> Dict[str, Any]:
        """Analyze a single group for completeness."""
        if not group_files:
            return {'status': 'unknown', 'episodes_found': 0}
        
        # Extract basic info from group
        first_file = group_files[0]
        title = first_file.get('title', 'Unknown')
        season = first_file.get('season')
        episodes_found = len(group_files)
        
        # Get episode numbers
        episode_numbers = []
        for file_info in group_files:
            episode = file_info.get('episode')
            if isinstance(episode, list):
                episode_numbers.extend(episode)
            elif episode is not None:
                episode_numbers.append(episode)
        
        episode_numbers = sorted(set(episode_numbers)) if episode_numbers else []
        
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
        }

        # Check metadata for expected episode count
        if self.metadata_manager:
            title_metadata_key = first_file.get('title_metadata_key')
            if title_metadata_key and title_metadata_key in self.file_grouper.title_metadata:
                metadata = self.file_grouper.title_metadata[title_metadata_key]['metadata']

                if 'series' in metadata.get('type', '').lower():
                    # For series series, check total episodes
                    total_episodes = metadata.get('total_episodes')
                    if total_episodes:
                        result['episodes_expected'] = total_episodes
                        
                        # Determine completeness
                        if season:
                            # For seasonal series, we need to determine episodes per season
                            # This is simplified - in reality you'd need season-specific episode counts
                            expected_episodes = self._estimate_season_episodes(metadata, season)
                            result['episodes_expected'] = expected_episodes
                        else:
                            expected_episodes = total_episodes
                        
                        # Check for missing episodes
                        if episode_numbers:
                            max_episode = max(episode_numbers)
                            expected_range = list(range(1, expected_episodes + 1))
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
    
    def _estimate_season_episodes(self, metadata: Dict[str, Any], season: int) -> int:
        """Estimate episodes for a specific season (simplified implementation)."""
        total_episodes = metadata.get('total_episodes', 0)
        total_seasons = metadata.get('total_seasons', 1)
        
        if total_seasons and total_episodes:
            # Simple estimation: divide total episodes by total seasons
            return total_episodes // total_seasons
        
        # Default fallback for series seasons
        return 12
    
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
    
    def print_summary(self, results: Dict[str, Any], verbosity: int = 1) -> None:
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
        
        # One-line summary for each series
        if verbosity >= 1:
            print(f"\n=== Series ===")
            for group_key, analysis in sorted(results['groups'].items()):
                self._print_one_line_summary(analysis)
    
    def _print_one_line_summary(self, analysis: Dict[str, Any]) -> None:
        """Print a concise one-line summary for a series."""
        status = analysis['status']
        title = analysis['title']
        season = analysis.get('season')
        episodes_found = analysis['episodes_found']
        episodes_expected = analysis.get('episodes_expected', 0)
        
        # Status emoji
        status_emoji = {
            'complete': '✅',
            'incomplete': '❌', 
            'complete_with_extras': '⚠️ ',
            'no_episode_numbers': '❓',
            'unknown_total_episodes': '❓',
            'not_series': 'ℹ️ ',
            'no_metadata': '❓',
            'no_metadata_manager': '❓',
            'unknown': '❓'
        }.get(status, '❓')
        
        # Format title with season
        title_str = title
        if season:
            title_str += f" S{season:02d}"

        # Limit title length for display
        title_length = 40

        # Truncate title to maximum title_length characters with ellipsis
        if len(title_str) > title_length:
            title_str = title_str[:title_length - 3] + "..."
        
        # Add missing/extra episode info
        extra_info = []
        if analysis.get('missing_episodes'):
            missing_range = self._format_episode_ranges(analysis['missing_episodes'])
            extra_info.append(f"Missing: {missing_range}")
        
        if analysis.get('extra_episodes'):
            extra_range = self._format_episode_ranges(analysis['extra_episodes'])
            extra_info.append(f"Extra: {extra_range}")
        
        # Build the complete line as one formatted string with all parts as parameters
        extra_info_str = f" | {' | '.join(extra_info)}" if extra_info else ""
        episodes_expected_str = str(episodes_expected) if episodes_expected else '?'
        line = f"{status_emoji} {title_str:<{title_length}} {episodes_found:>4}/{episodes_expected_str:<4}{extra_info_str}"
        
        print(line)
    
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
        """
    )
    
    parser.add_argument('input_paths', nargs='+',
                       help='Input paths to search for series files')
    parser.add_argument('--exclude-paths', nargs='*', default=[],
                       help='Paths to exclude from search')
    parser.add_argument('--include-patterns', nargs='*', default=['*.mkv', '*.mp4', '*.avi'],
                       help='Wildcard patterns for files to include (default: *.mkv *.mp4 *.avi)')
    parser.add_argument('--exclude-patterns', nargs='*', default=[],
                       help='Wildcard patterns for files to exclude')
    parser.add_argument('--export', metavar='FILE',
                       help='Export results to JSON file')
    parser.add_argument('--recursive', '-r', action='store_true',
                       help='Recursively search subdirectories (default: False)')
    parser.add_argument('--verbose', '-v', type=int, choices=[0, 1, 2, 3], default=1,
                       help='Verbosity level: 0=silent, 1=summary, 2=detailed, 3=very detailed (default: 1)')
    parser.add_argument('--quiet', '-q', action='store_true',
                       help='Same as --verbose 0')
    parser.add_argument('--incomplete-only', action='store_true',
                       help='Only show incomplete series series')
    parser.add_argument('--complete-only', action='store_true',
                       help='Only show complete series series')
    
    args = parser.parse_args()
    
    # Handle quiet flag
    if args.quiet:
        verbosity = 0
    else:
        verbosity = args.verbose

    # Get metadata manager
    try:
        metadata_manager = get_metadata_manager()
        if not metadata_manager and verbosity >= 1:
            print("Warning: No metadata manager available. Completeness checking will be limited.")
    except Exception as e:
        if verbosity >= 1:
            print(f"Warning: Could not initialize metadata manager: {e}")
        metadata_manager = None
    
    # Create checker instance
    checker = SeriesCompletenessChecker(metadata_manager)

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
    
    if verbosity >= 1:
        print(f"Found {len(files)} files")
        print("Analyzing series collection for completeness...")
    
    # Analyze collection
    results = checker.analyze_series_collection(files)

    # Filter results if requested
    if args.incomplete_only or args.complete_only:
        filtered_groups = {}
        for group_key, analysis in results['groups'].items():
            status = analysis['status']
            if args.incomplete_only and status in ['incomplete', 'no_episode_numbers']:
                filtered_groups[group_key] = analysis
            elif args.complete_only and status in ['complete', 'complete_with_extras']:
                filtered_groups[group_key] = analysis
        results['groups'] = filtered_groups
        
        # Recalculate summary for filtered results
        total_series = len(filtered_groups)
        complete_series = sum(1 for a in filtered_groups.values() if a['status'] in ['complete', 'complete_with_extras'])
        incomplete_series = sum(1 for a in filtered_groups.values() if a['status'] in ['incomplete', 'no_episode_numbers'])
        unknown_series = total_series - complete_series - incomplete_series

        results['completeness_summary'].update({
            'total_series': total_series,
            'complete_series': complete_series,
            'incomplete_series': incomplete_series,
            'unknown_series': unknown_series
        })
    
    # Display results
    if verbosity >= 1:
        checker.print_summary(results, verbosity)
    
    # Export if requested
    if args.export:
        checker.export_results(results, args.export)
        if verbosity >= 1:
            print(f"\nExported results to: {args.export}")

if __name__ == '__main__':
    main()