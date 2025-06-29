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
    
    def __init__(self, metadata_manager=None, plex_provider=None):
        self.file_grouper = FileGrouper(metadata_manager, plex_provider)
        self.metadata_manager = metadata_manager
        self.plex_provider = plex_provider
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
        
        # Calculate watch status for the group
        watched_count = 0
        partially_watched_count = 0
        total_watch_count = 0
        
        for file_info in group_files:
            plex_status = file_info.get('plex_watch_status')
            if plex_status:
                if plex_status.get('watched'):
                    watched_count += 1
                elif plex_status.get('view_offset', 0) > 0:
                    partially_watched_count += 1
                total_watch_count += plex_status.get('watch_count', 0)
        
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
            'watch_status': {
                'watched_episodes': watched_count,
                'partially_watched_episodes': partially_watched_count,
                'unwatched_episodes': episodes_found - watched_count - partially_watched_count,
                'total_watch_count': total_watch_count,
                'completion_percent': (watched_count / episodes_found * 100) if episodes_found > 0 else 0
            }
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
    
    def export_webapp(self, results: Dict[str, Any], output_path: str) -> None:
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
            formatted = formatted[:max_length - 3] + "..."
        
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
        
        # Add watch status summary if Plex data is available
        total_watched = sum(analysis['watch_status']['watched_episodes'] for analysis in results['groups'].values())
        total_episodes = sum(analysis['episodes_found'] for analysis in results['groups'].values())
        total_partially_watched = sum(analysis['watch_status']['partially_watched_episodes'] for analysis in results['groups'].values())
        
        if total_watched > 0 or total_partially_watched > 0:
            print(f"\n=== Watch Status Summary ===")
            print(f"Watched episodes: {total_watched}/{total_episodes} ({total_watched/total_episodes*100:.1f}%)")
            print(f"Partially watched: {total_partially_watched}")
            print(f"Unwatched episodes: {total_episodes - total_watched - total_partially_watched}")
        
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
        
        # Add episode info (watched, missing, extra)
        extra_info = []
        
        # Add watched episodes info as ranges
        watched_episodes = []
        if analysis.get('files'):
            for file_info in analysis['files']:
                plex_status = file_info.get('plex_watch_status')
                if plex_status and plex_status.get('watched'):
                    episode = file_info.get('episode')
                    if isinstance(episode, list):
                        watched_episodes.extend(episode)
                    elif episode is not None:
                        watched_episodes.append(episode)
        
        if watched_episodes:
            watched_range = self._format_episode_ranges(sorted(set(watched_episodes)))
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
            title_metadata_key = first_file.get('title_metadata_key')
            
            if title_metadata_key and hasattr(self, 'file_grouper'):
                title_metadata = getattr(self.file_grouper, 'title_metadata', {})
                if title_metadata_key in title_metadata:
                    metadata = title_metadata[title_metadata_key]['metadata']
                    
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
  %(prog)s /path/to/series --show-metadata year rating
  %(prog)s /path/to/series --show-metadata genres director --status-filter "complete"        """
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
    parser.add_argument('--webapp-export', metavar='FILE',
                       help='Export results as a standalone HTML webapp')
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
    parser.add_argument('--show-metadata', nargs='*', metavar='FIELD',
                       help='Show metadata fields in summary lines. Available fields depend on metadata source. '
                            'Common fields: year, rating, genres, director, actors, plot, runtime, imdb_id. '
                            'Example: --show-metadata year rating genres')
    
    args = parser.parse_args()
    
    # Handle quiet flag
    if args.quiet:
        verbosity = 0
    else:
        verbosity = args.verbose

    # Get metadata manager and plex provider
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
    
    # Create checker instance
    checker = SeriesCompletenessChecker(metadata_manager, plex_provider)

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
    status_filters = None
    if args.status_filter:
        # Split the string into individual filter items
        status_filters = args.status_filter.split()
    
    if status_filters:
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
            # Plain statuses take precedence (exact match mode)
            final_statuses = plain_statuses
        elif include_statuses:
            # Include mode: start with empty set, add includes, remove excludes
            final_statuses = include_statuses - exclude_statuses
        elif exclude_statuses:
            # Exclude mode: start with all, remove excludes
            final_statuses = all_statuses - exclude_statuses
        else:
            # No valid filters, show all
            final_statuses = all_statuses
        
        # Apply filtering
        filtered_groups = {}
        for group_key, analysis in results['groups'].items():
            if analysis['status'] in final_statuses:
                filtered_groups[group_key] = analysis
        results['groups'] = filtered_groups
        
        # Recalculate summary for filtered results
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
    
    # Display results
    if verbosity >= 1:
        checker.print_summary(results, verbosity, args.show_metadata)
    
    # Export if requested
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