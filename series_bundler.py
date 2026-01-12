import argparse
import glob
import os
import shutil
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import re

# Emoji support detection
def _detect_emoji_support():
    """Detect if the terminal/environment supports emoji."""
    # Check if running in a Windows terminal that supports Unicode
    if sys.platform == 'win32':
        # Check if stdout encoding supports Unicode
        try:
            encoding = sys.stdout.encoding or ''
            # Windows Terminal, new Command Prompt, and PowerShell 7+ support UTF-8
            if 'utf-8' in encoding.lower() or 'utf8' in encoding.lower():
                return True
            # Try to encode a test emoji
            '✅'.encode(encoding)
            return True
        except (UnicodeEncodeError, AttributeError, LookupError):
            return False
    else:
        # Unix-like systems usually support emoji
        try:
            encoding = sys.stdout.encoding or 'utf-8'
            '✅'.encode(encoding)
            return True
        except (UnicodeEncodeError, AttributeError):
            return False

EMOJI_SUPPORT = _detect_emoji_support()

# Emoji constants with fallbacks
class Emoji:
    """Emoji characters with ASCII fallbacks."""
    if EMOJI_SUPPORT:
        CHECK = '✅'
        CROSS = '❌'
        WARNING = '⚠️'
        COMPLETE = '✅'
        INCOMPLETE = '❌'
        FOLDER = '📁'
        FILE = '📄'
        CALENDAR = '📅'
        PACKAGE = '📦'
        CHART = '📊'
        STAR = '⭐'
    else:
        CHECK = ''
        CROSS = ''
        WARNING = ''
        COMPLETE = ''
        INCOMPLETE = ''
        FOLDER = ''
        FILE = ''
        CALENDAR = ''
        PACKAGE = ''
        CHART = ''
        STAR = ''

try:
    from colorama import Fore, Style, init as colorama_init
    colorama_init(autoreset=True)
    COLORAMA_AVAILABLE = True
except ImportError:
    COLORAMA_AVAILABLE = False
    # Fallback to empty strings
    class Fore:
        GREEN = CYAN = YELLOW = RED = MAGENTA = BLUE = WHITE = RESET = ""
    class Style:
        BRIGHT = DIM = RESET_ALL = ""

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    # Simple fallback
    class tqdm:
        def __init__(self, iterable=None, total=None, desc=None, unit=None, disable=False):
            self.iterable = iterable
            self.disable = disable
            if not disable and desc:
                print(f"{desc}...")
        
        def __iter__(self):
            if self.iterable:
                for item in self.iterable:
                    yield item
            return self
        
        def __enter__(self):
            return self
        
        def __exit__(self, *args):
            if not self.disable:
                print("Done.")
        
        def set_postfix(self, **kwargs):
            # Simple implementation for fallback
            pass

from guessit_wrapper import guessit_wrapper
from file_grouper import FileGrouper


class SeriesBundler:
    """
    Groups series files and creates organized folder structures for archiving.
    
    The bundler analyzes files using guessit to extract metadata, groups them by
    series, release group, and resolution, then creates folder names following
    the pattern: [Release Group] Series Name (YYYY) (xx-yy) (Resolution)
    """
    
    def __init__(self, verbose: int = 0, use_colors: bool = True, myanimelist_xml_path: Optional[str] = None, metadata_manager=None):
        """
        Initialize the SeriesBundler.
        
        Args:
            verbose: Verbosity level (0=quiet, 1=normal, 2=verbose)
            use_colors: Whether to use color formatting in output
            myanimelist_xml_path: Path to MyAnimeList XML file for watch status lookup
            metadata_manager: MetadataManager instance for title lookups
        """
        self.verbose = verbose
        self.use_colors = use_colors and COLORAMA_AVAILABLE
        
        # Resolve wildcards if MAL path is provided
        if myanimelist_xml_path:
            # Resolve wildcards if present
            resolved_path = self._resolve_wildcard_path(myanimelist_xml_path)
            if resolved_path:
                myanimelist_xml_path = resolved_path
            else:
                self._log(f"Warning: No files found matching pattern: {myanimelist_xml_path}", 1)
                myanimelist_xml_path = None
        
        # Initialize FileGrouper with metadata support
        self.file_grouper = FileGrouper(
            metadata_manager=metadata_manager,
            plex_provider=None,  # Not using Plex in series bundler
            myanimelist_xml_path=myanimelist_xml_path
        )
        
        # Log metadata manager availability
        if metadata_manager:
            self._log("MetadataManager initialized successfully", 2)
        else:
            self._log("Warning: MetadataManager not available - MAL lookups will not work", 1)
    
    def _get_emoji(self, emoji_type: str) -> str:
        """Get emoji with fallback support.
        
        Args:
            emoji_type: Type of emoji ('complete', 'incomplete', 'warning', 'check', 'cross', 'folder', 'file', 'calendar', 'package', 'chart', 'star')
            
        Returns:
            Emoji character or ASCII fallback
        """
        emoji_map = {
            'complete': Emoji.COMPLETE,
            'incomplete': Emoji.INCOMPLETE,
            'warning': Emoji.WARNING,
            'check': Emoji.CHECK,
            'cross': Emoji.CROSS,
            'folder': Emoji.FOLDER,
            'file': Emoji.FILE,
            'calendar': Emoji.CALENDAR,
            'package': Emoji.PACKAGE,
            'chart': Emoji.CHART,
            'star': Emoji.STAR
        }
        return emoji_map.get(emoji_type.lower(), '')
        
    def _resolve_wildcard_path(self, path_pattern: str) -> Optional[str]:
        """Resolve wildcard path pattern to the latest matching file.
        
        Args:
            path_pattern: File path pattern, may contain wildcards (* or ?)
            
        Returns:
            Path to the latest file matching the pattern, or None if no matches
        """
        # Expand user home directory
        expanded_pattern = os.path.expanduser(path_pattern)
        
        # Check if pattern contains wildcards
        if '*' not in expanded_pattern and '?' not in expanded_pattern:
            # No wildcard, return as-is if file exists
            return expanded_pattern if os.path.isfile(expanded_pattern) else None
        
        # Find all matching files
        matching_files = glob.glob(expanded_pattern)
        
        if not matching_files:
            return None
        
        # Filter to only files (not directories)
        matching_files = [f for f in matching_files if os.path.isfile(f)]
        
        if not matching_files:
            return None
        
        # Find the file with the latest creation time
        latest_file = max(matching_files, key=lambda f: os.path.getctime(f))
        
        # Log which file was selected
        if len(matching_files) > 1:
            latest_date = datetime.fromtimestamp(os.path.getctime(latest_file)).strftime('%Y-%m-%d %H:%M:%S')
            self._log(f"Found {len(matching_files)} files matching pattern '{path_pattern}'", 1)
            self._log(f"Using latest file: {latest_file} (created: {latest_date})", 1)
        
        return latest_file
    
    def _log(self, message: str, level: int = 1):
        """Log message if verbosity level is sufficient."""
        if self.verbose >= level:
            print(message)
    
    def _color(self, text: str, color: str = "") -> str:
        """Apply color to text if colors are enabled."""
        if self.use_colors and color:
            return f"{color}{text}{Style.RESET_ALL}"
        return text
    
    def _clean_filename(self, name: str) -> str:
        """Clean filename/folder name for filesystem compatibility."""
        # Remove or replace characters that might cause issues
        invalid_chars = ['<', '>', ':', '"', '|', '?', '*']
        for char in invalid_chars:
            name = name.replace(char, '')
        
        # Replace forward slashes with dashes
        name = name.replace('/', '-')
        name = name.replace('\\', '-')
        
        # Remove multiple spaces and strip
        name = re.sub(r'\s+', ' ', name).strip()
        
        return name
    
    def _format_episode_range(self, episodes: List) -> str:
        """
        Format episode numbers as a range string.
        
        Args:
            episodes: List of episode numbers (can be int or float)
            
        Returns:
            Formatted range string (e.g., "01-03", "05", "12_5", or "12-13+")
        """
        if not episodes:
            return "00"
        
        # Sort episodes, handling both int and float
        sorted_episodes = sorted(set(episodes))
        
        if len(sorted_episodes) == 1:
            episode = sorted_episodes[0]
            if isinstance(episode, float):
                # Format decimal episodes like "12.5" -> "12_5"
                return f"{episode:04.1f}".replace('.', '_')
            else:
                return f"{episode:02d}"
        else:
            # For ranges, check if we have decimal episodes
            has_decimals = any(isinstance(ep, float) for ep in sorted_episodes)
            start_ep = sorted_episodes[0]
            end_ep = sorted_episodes[-1]
            
            if has_decimals:
                # If we have decimal episodes, show range with "+" to indicate there are episodes in between
                start_str = f"{start_ep:04.1f}".replace('.', '_') if isinstance(start_ep, float) else f"{start_ep:02d}"
                end_str = f"{end_ep:04.1f}".replace('.', '_') if isinstance(end_ep, float) else f"{end_ep:02d}"
                
                # Check if there are episodes between start and end
                if len(sorted_episodes) > 2 or any(isinstance(ep, float) for ep in sorted_episodes[1:-1]):
                    return f"{start_str}-{end_str}+"
                else:
                    return f"{start_str}-{end_str}"
            else:
                return f"{start_ep:02d}-{end_ep:02d}"
    
    def _format_missing_episodes(self, episodes: List, total_episodes: int = None, season_start: int = None) -> str:
        """
        Format missing episode ranges.
        
        Args:
            episodes: List of episode numbers present
            total_episodes: Total expected episodes (optional)
            season_start: Starting episode for the season (optional, for multi-season anime)
            
        Returns:
            Formatted string of missing episodes (e.g., "26-27,37-38,42,49-50")
        """
        if not episodes:
            return ""
        
        sorted_episodes = sorted(set(int(ep) for ep in episodes if isinstance(ep, (int, float))))
        if not sorted_episodes:
            return ""
        
        # Determine range to check
        min_ep = season_start if season_start else sorted_episodes[0]
        max_ep = total_episodes if total_episodes else sorted_episodes[-1]
        
        # Find missing episodes
        all_eps = set(range(min_ep, max_ep + 1))
        present_eps = set(sorted_episodes)
        missing_eps = sorted(all_eps - present_eps)
        
        if not missing_eps:
            return ""
        
        # Format missing episodes as ranges
        ranges = []
        range_start = missing_eps[0]
        range_end = missing_eps[0]
        
        for i in range(1, len(missing_eps)):
            current = missing_eps[i]
            if current == range_end + 1:
                range_end = current
            else:
                # Gap in missing episodes, save current range
                if range_start == range_end:
                    ranges.append(f"{range_start}")
                else:
                    ranges.append(f"{range_start}-{range_end}")
                range_start = current
                range_end = current
        
        # Add final range
        if range_start == range_end:
            ranges.append(f"{range_start}")
        else:
            ranges.append(f"{range_start}-{range_end}")
        
        return ",".join(ranges)
    
    def _get_total_episodes(self, title: str, season: int = None) -> int:
        """
        Get total episodes for a series, summing across seasons if needed.
        
        Args:
            title: Series title
            season: Season number (if known)
            
        Returns:
            Total episode count, or 0 if not found
        """
        total_episodes = 0
        base_title = title.lower()
        
        # Look through all metadata for matching titles
        for tid, tmeta in self.file_grouper.title_metadata.items():
            tmal = tmeta.get('myanimelist_watch_status')
            if tmal:
                # Check if this is the same series (matching base title)
                tmal_title = tmal.get('series_title', '').lower()
                if base_title in tmal_title or tmal_title in base_title:
                    total_episodes += tmal.get('series_episodes', 0)
        
        return total_episodes
    
    def generate_folder_name(self, metadata_list: List[Dict], group_key: Optional[str] = None) -> str:
        """
        Generate folder name based on series metadata.
        
        Args:
            metadata_list: List of file metadata dictionaries (from FileGrouper)
            group_key: Optional group key to lookup MAL metadata
            
        Returns:
            Generated folder name following the pattern:
            [Release Group] Series Name (YYYY) (xx-yy) (Resolution) [Complete/Incomplete]
        """
        if not metadata_list:
            return "Unknown"
        
        # Use the first file as reference for common attributes
        first_file = metadata_list[0]
        
        title = first_file.get('title', 'Unknown')
        year = first_file.get('year', '')
        release_group = first_file.get('release_group', 'Unknown')
        screen_size = first_file.get('screen_size', 'Unknown')
        season = first_file.get('season')
        
        if self.verbose >= 2:
            self._log(f"  Debug generate_folder_name: title='{title}', season='{season}', year='{year}'", 2)
        
        # Collect all episode numbers (can be int or float)
        # For multi-season shows, use original_episode if available to show absolute episode numbers
        episodes = []
        for metadata in metadata_list:
            # Prefer original_episode for folder naming (shows absolute episode numbers like 26-50)
            # Fall back to episode (season-specific like 01-25)
            episode = metadata.get('original_episode') if 'original_episode' in metadata else metadata.get('episode')
            if isinstance(episode, list):
                episodes.extend(episode)
            elif episode is not None:
                episodes.append(episode)
        
        # Format episode range
        episode_range = self._format_episode_range(episodes)
        
        # Check MAL metadata for total episodes to add to range
        total_episodes = None
        using_original_episodes = any('original_episode' in m for m in metadata_list)
        
        if group_key:
            # Get metadata_id from first file in the group
            metadata_id = metadata_list[0].get('metadata_id')
            
            if metadata_id and metadata_id in self.file_grouper.title_metadata:
                title_meta = self.file_grouper.title_metadata[metadata_id]
                mal_watch_status = title_meta.get('myanimelist_watch_status')
                
                if mal_watch_status:
                    season_episodes = mal_watch_status.get('series_episodes', 0)
                    
                    # If using original (absolute) episode numbers for multi-season anime,
                    # calculate total across all seasons
                    if using_original_episodes and season and season > 1:
                        # For multi-season anime, we need to sum episodes across all seasons
                        # Look through all metadata for the same base title
                        total_episodes = 0
                        base_title = title.lower()  # Use the title from the file
                        
                        for tid, tmeta in self.file_grouper.title_metadata.items():
                            tmal = tmeta.get('myanimelist_watch_status')
                            if tmal:
                                # Check if this is the same series (matching base title)
                                tmal_title = tmal.get('series_title', '').lower()
                                if base_title in tmal_title or tmal_title in base_title:
                                    total_episodes += tmal.get('series_episodes', 0)
                        
                        # If we couldn't find multiple seasons or total is zero, estimate from max episode
                        if total_episodes == 0 or total_episodes <= season_episodes:
                            # Estimate total from the maximum episode number we have
                            max_ep = max(episodes) if episodes else 0
                            total_episodes = max(max_ep, season_episodes * season)
                    else:
                        # For single season or season-specific numbering, use season's episode count
                        total_episodes = season_episodes
        
        # Add total episodes to range if incomplete
        found_episodes = len(set(episodes))
        if total_episodes and total_episodes > 0:
            # For multi-season anime using original episodes, check if THIS season is complete
            season_is_complete = False
            if using_original_episodes and season and season > 1:
                # Calculate the expected episode count for this season only
                season_episodes_dict = {}
                base_title = title.lower()
                
                for tid, tmeta in self.file_grouper.title_metadata.items():
                    tmal = tmeta.get('myanimelist_watch_status')
                    if tmal:
                        tmal_title = tmal.get('series_title', '').lower()
                        if base_title in tmal_title or tmal_title in base_title:
                            tmal_season = tmal.get('season_number', 1)
                            tmal_eps = tmal.get('series_episodes', 0)
                            if tmal_season not in season_episodes_dict or tmal_eps > season_episodes_dict[tmal_season]:
                                season_episodes_dict[tmal_season] = tmal_eps
                
                # Get this season's episode count
                current_season_episodes = season_episodes_dict.get(season, 0)
                if current_season_episodes > 0 and found_episodes >= current_season_episodes:
                    season_is_complete = True
            else:
                # For single season or season-specific numbering
                if found_episodes >= total_episodes:
                    season_is_complete = True
            
            if not season_is_complete:
                # Incomplete: show "(01-03 of 25, missing X-Y)"
                episode_range = f"{episode_range} of {total_episodes}"
                
                # Calculate season start for missing episode detection
                season_start_ep = 1
                if using_original_episodes and season and season > 1:
                    # For multi-season anime, calculate where this season starts
                    base_title = title.lower()
                    season_episodes_dict = {}
                    
                    for tid, tmeta in self.file_grouper.title_metadata.items():
                        tmal = tmeta.get('myanimelist_watch_status')
                        if tmal:
                            tmal_title = tmal.get('series_title', '').lower()
                            if base_title in tmal_title or tmal_title in base_title:
                                tmal_season = tmal.get('season_number', 1)
                                tmal_eps = tmal.get('series_episodes', 0)
                                if tmal_season not in season_episodes_dict or tmal_eps > season_episodes_dict[tmal_season]:
                                    season_episodes_dict[tmal_season] = tmal_eps
                    
                    # Sum episodes from seasons before current season
                    for s_num in sorted(season_episodes_dict.keys()):
                        if s_num < season:
                            season_start_ep += season_episodes_dict[s_num]
                
                # Add missing episodes to the range
                missing_text = self._format_missing_episodes(episodes, total_episodes, season_start_ep)
                if missing_text:
                    episode_range = f"{episode_range}, missing {missing_text}"
            # If complete, just show the range without "of X"
        
        # Clean components for filesystem compatibility
        clean_title = self._clean_filename(str(title))
        clean_release_group = self._clean_filename(str(release_group))
        
        # Build folder name
        folder_parts = [f"[{clean_release_group}]", clean_title]
        
        # Add season indicator if season > 1
        if season and season > 1:
            folder_parts.append(f"S{season}")
        
        if year:
            folder_parts.append(f"({year})")
        
        folder_parts.append(f"({episode_range})")
        folder_parts.append(f"({screen_size})")
        
        folder_name = " ".join(folder_parts)
        
        self._log(f"Generated folder name: {folder_name}", 2)
        return folder_name
    
    def analyze_files(self, file_paths: List[Path]) -> Dict[str, List[Dict]]:
        """
        Analyze files and group them by series using FileGrouper.
        
        Args:
            file_paths: List of file paths to analyze
            
        Returns:
            Dictionary mapping group keys to lists of file metadata
        """
        self._log(f"Analyzing {len(file_paths)} files...", 1)
        
        # Use FileGrouper to extract metadata and group files
        # Group by: title, year, release_group, screen_size, season (for series bundling)
        groups = self.file_grouper.group_files(
            file_paths,
            group_by=['title', 'year', 'release_group', 'screen_size', 'season'],
            show_progress=(self.verbose >= 1)
        )
        
        self._log(f"Found {len(groups)} series groups", 1)
        return groups
    
    def validate_series_consistency(self, group_metadata: List[Dict]) -> Tuple[bool, List[str]]:
        """
        Validate that files in a group belong to the same series.
        
        Args:
            group_metadata: List of file metadata for a group (from FileGrouper)
            
        Returns:
            Tuple of (is_valid, list_of_warnings)
        """
        if len(group_metadata) <= 1:
            return True, []
        
        warnings = []
        first_file = group_metadata[0]
        
        # Extract metadata directly (not under 'guessit' key)
        title = first_file.get('title', 'Unknown')
        for metadata in group_metadata[1:]:
            if metadata.get('title', 'Unknown') != title:
                warnings.append(f"Title mismatch: '{title}' vs '{metadata.get('title', 'Unknown')}'")
        
        # Check release group consistency
        release_group = first_file.get('release_group', 'Unknown')
        for metadata in group_metadata[1:]:
            if metadata.get('release_group', 'Unknown') != release_group:
                warnings.append(f"Release group mismatch: '{release_group}' vs '{metadata.get('release_group', 'Unknown')}'")
        
        # Check resolution consistency
        screen_size = first_file.get('screen_size', 'Unknown')
        for metadata in group_metadata[1:]:
            if metadata.get('screen_size', 'Unknown') != screen_size:
                warnings.append(f"Resolution mismatch: '{screen_size}' vs '{metadata.get('screen_size', 'Unknown')}'")
        
        return len(warnings) == 0, warnings
    
    def create_bundles(self, destination: str, copy_files: bool = False, dry_run: bool = False) -> Dict[str, Dict]:
        """
        Create bundle folders and organize files.
        
        Args:
            destination: Root destination directory
            copy_files: If True, copy files; if False, move files
            dry_run: If True, don't actually move/copy files
            
        Returns:
            Dictionary mapping original group keys to dict containing folder path and newest file date
        """
        if not self.file_grouper.groups:
            self._log("No series groups found. Run analyze_files() first.", 1)
            return {}
        
        destination_path = Path(destination)
        if not dry_run:
            destination_path.mkdir(parents=True, exist_ok=True)
        
        results = {}
        action_word = "Copying" if copy_files else "Moving"
        
        self._log(f"{action_word} files to bundles{'(DRY RUN)' if dry_run else ''}...", 1)
        
        # Sort groups by season
        sorted_groups = sorted(
            self.file_grouper.groups.items(),
            key=lambda x: (
                x[1][0].get('title', ''),
                x[1][0].get('season') or 0
            )
        )
        
        for group_key, group_metadata in sorted_groups:
            # Sort files within group by episode number
            group_metadata = sorted(
                group_metadata,
                key=lambda x: (x.get('episode') or 0)
            )
            # Validate group consistency
            is_valid, warnings = self.validate_series_consistency(group_metadata)
            
            if warnings:
                self._log(f"Warnings for group {group_key}:", 1)
                for warning in warnings:
                    self._log(f"  - {warning}", 1)
            
            # Generate folder name (with MAL completeness info if available)
            folder_name = self.generate_folder_name(group_metadata, group_key)
            folder_path = destination_path / folder_name
            
            first_title = group_metadata[0].get('title', 'Unknown')
            self._log(f"\nProcessing group: {first_title}", 1)
            self._log(f"  Folder: {folder_name}", 1)
            self._log(f"  Files: {len(group_metadata)}", 1)
            
            if not dry_run:
                folder_path.mkdir(parents=True, exist_ok=True)
            
            # Process files in this group
            success_count = 0
            error_count = 0
            newest_file_time = None
            
            for metadata in group_metadata:
                source_path = Path(metadata['filepath'])
                filename = metadata['filename']
                dest_path = folder_path / filename
                
                try:
                    # Track the newest file modification time
                    if source_path.exists():
                        file_mtime = source_path.stat().st_mtime
                        if newest_file_time is None or file_mtime > newest_file_time:
                            newest_file_time = file_mtime
                    
                    if dry_run:
                        action = "copy" if copy_files else "move"
                        self._log(f"  Would {action}: {filename}", 2)
                    else:
                        if copy_files:
                            shutil.copy2(source_path, dest_path)
                            self._log(f"  Copied: {filename}", 2)
                        else:
                            shutil.move(source_path, dest_path)
                            self._log(f"  Moved: {filename}", 2)
                    
                    success_count += 1
                    
                except Exception as e:
                    self._log(f"  Error processing {filename}: {e}", 1)
                    error_count += 1
            
            # Set folder modified date to newest file date
            if not dry_run and newest_file_time is not None and folder_path.exists():
                try:
                    os.utime(str(folder_path), (newest_file_time, newest_file_time))
                    newest_date_str = datetime.fromtimestamp(newest_file_time).strftime('%Y-%m-%d')
                    self._log(f"  Set folder date to: {newest_date_str}", 1)
                except Exception as e:
                    self._log(f"  Warning: Could not set folder date: {e}", 1)
            
            status_word = "Would process" if dry_run else "Processed"
            self._log(f"  {status_word} {success_count} files successfully", 1)
            if error_count > 0:
                self._log(f"  {error_count} files had errors", 1)
            
            # Store folder path and newest file date
            results[group_key] = {
                'folder_path': str(folder_path),
                'newest_file_date': datetime.fromtimestamp(newest_file_time).strftime('%Y-%m-%d') if newest_file_time else None
            }
        
        return results
    
    def get_summary(self) -> Dict:
        """
        Get summary statistics about the analyzed files.
        
        Returns:
            Dictionary containing summary information
        """
        total_files = sum(len(group) for group in self.file_grouper.groups.values())
        total_size = sum(
            sum(metadata.get('file_size', 0) for metadata in group)
            for group in self.file_grouper.groups.values()
        )
        
        # Group statistics
        group_sizes = [len(group) for group in self.file_grouper.groups.values()]
        
        return {
            'total_files': total_files,
            'total_groups': len(self.file_grouper.groups),
            'total_size_bytes': total_size,
            'total_size_mb': round(total_size / (1024 * 1024), 2),
            'average_files_per_group': round(sum(group_sizes) / len(group_sizes), 1) if group_sizes else 0,
            'largest_group_size': max(group_sizes) if group_sizes else 0,
            'smallest_group_size': min(group_sizes) if group_sizes else 0
        }
    
    def print_summary(self):
        """Print a summary of the analysis."""
        if not self.file_grouper.groups:
            print("No series groups found.")
            return
        
        summary = self.get_summary()
        
        print(self._color("\n=== Series Bundler Summary ===", Fore.CYAN + Style.BRIGHT))
        file_emoji = self._get_emoji('file')
        print(f"Total files: {file_emoji} {self._color(str(summary['total_files']), Fore.GREEN)}")
        folder_emoji = self._get_emoji('folder')
        print(f"Total groups: {folder_emoji} {self._color(str(summary['total_groups']), Fore.GREEN)}")
        package_emoji = self._get_emoji('package')
        size_text = f"{summary['total_size_mb']} MB"
        print(f"Total size: {package_emoji} {self._color(size_text, Fore.GREEN)}")
        star_emoji = self._get_emoji('star')
        print(f"Average files per group: {star_emoji} {self._color(str(summary['average_files_per_group']), Fore.YELLOW)}")
        largest_text = f"{summary['largest_group_size']} files"
        print(f"  Largest group: {self._color(largest_text, Fore.MAGENTA)}")
        smallest_text = f"{summary['smallest_group_size']} files"
        print(f"  Smallest group: {self._color(smallest_text, Fore.MAGENTA)}")
        
        # Debug: show group_metadata status
        if self.verbose >= 2:
            print(f"\nFileGrouper has {len(self.file_grouper.title_metadata)} title metadata entries")
            print(f"FileGrouper has {len(self.file_grouper.group_metadata)} group metadata entries")
        
        print(self._color("\n=== Groups ===", Fore.CYAN + Style.BRIGHT))
        # Sort groups by season (extract from group_key)
        sorted_groups = sorted(
            self.file_grouper.groups.items(),
            key=lambda x: (
                x[1][0].get('title', ''),  # Sort by title first
                x[1][0].get('season') or 0  # Then by season number
            )
        )
        
        for i, (group_key, group_metadata) in enumerate(sorted_groups, 1):
            # Sort files within group by episode number
            group_metadata = sorted(
                group_metadata,
                key=lambda x: (x.get('episode') or 0)
            )
            first_file = group_metadata[0]
            
            # Extract metadata directly (not under 'guessit' key)
            title = first_file.get('title', 'Unknown')
            release_group = first_file.get('release_group', 'Unknown')
            screen_size = first_file.get('screen_size', 'Unknown')
            
            # Get episode range (can include decimal episodes)
            episodes = []
            for metadata in group_metadata:
                episode = metadata.get('episode')
                if isinstance(episode, list):
                    episodes.extend(episode)
                elif episode is not None:
                    episodes.append(episode)
            
            # Episodes are already sorted because group_metadata was sorted
            episode_range = self._format_episode_range(episodes)
            folder_name = self.generate_folder_name(group_metadata, group_key)
            
            print(f"{i:2d}. {title} [{release_group}] ({screen_size}) - {len(group_metadata)} files")
            print(f"    Episodes: {episode_range}")
            
            # Get MAL info from FileGrouper's title_metadata via first file's metadata_id
            metadata_id = first_file.get('metadata_id')
            
            if self.verbose >= 2:
                self._log(f"    Debug: Checking group_key='{group_key}' in group_metadata", 2)
                self._log(f"    Debug: Available group keys: {list(self.file_grouper.group_metadata.keys())}", 2)
                self._log(f"    Debug: metadata_id='{metadata_id}'", 2)
                self._log(f"    Debug: Available title_metadata keys: {list(self.file_grouper.title_metadata.keys())}", 2)
            
            if metadata_id and metadata_id in self.file_grouper.title_metadata:
                title_meta = self.file_grouper.title_metadata[metadata_id]
                
                if self.verbose >= 2:
                    self._log(f"    Debug: title_meta keys: {list(title_meta.keys())}", 2)
                    # Show full content if really verbose
                    if self.verbose >= 3:
                        import json
                        self._log(f"    Debug: title_meta content: {json.dumps(title_meta, indent=2, default=str)}", 2)
                
                mal_watch_status = title_meta.get('myanimelist_watch_status')
                
                if self.verbose >= 2:
                    self._log(f"    Debug: mal_watch_status={mal_watch_status}", 2)
                    
                    if mal_watch_status:
                        total_episodes = mal_watch_status.get('series_episodes', 0)
                        my_status = mal_watch_status.get('my_status', 'Unknown')
                        my_watched = mal_watch_status.get('my_watched_episodes', 0)
                        my_score = mal_watch_status.get('my_score', 0)
                        
                        found_episodes = len(set(episodes))
                        is_complete = found_episodes >= total_episodes and total_episodes > 0
                        completeness_emoji = self._get_emoji('complete') if is_complete else self._get_emoji('incomplete')
                        completeness_text = "Complete" if is_complete else f"Incomplete ({found_episodes}/{total_episodes})"
                        
                        print(f"    MAL: {my_status} | Watched: {my_watched}/{total_episodes} | Score: {my_score}")
                        print(f"    Bundle: {completeness_emoji} {completeness_text}")
            
            print(f"    {self._get_emoji('folder')} {folder_name}")


def discover_video_files(paths: List[str], recursive: bool = False) -> List[Path]:
    """
    Discover video files in the given paths.
    
    Args:
        paths: List of file or directory paths
        recursive: Whether to search recursively in directories
        
    Returns:
        List of video file paths
    """
    video_extensions = {
        '.mkv', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', 
        '.m4v', '.mpg', '.mpeg', '.3gp', '.ogv', '.ts', '.m2ts'
    }
    
    discovered_files = []
    
    for path_str in paths:
        path = Path(path_str)
        
        if not path.exists():
            print(f"Warning: Path does not exist: {path_str}")
            continue
        
        if path.is_file():
            if path.suffix.lower() in video_extensions:
                discovered_files.append(path)
        else:
            # Directory
            pattern = path.rglob('*') if recursive else path.glob('*')
            for file_path in pattern:
                if file_path.is_file() and file_path.suffix.lower() in video_extensions:
                    discovered_files.append(file_path)
    
    return discovered_files


def get_user_confirmation(message: str, default: bool = False) -> bool:
    """
    Get user confirmation with yes/no prompt.
    
    Args:
        message: The confirmation message to display
        default: Default value if user just presses Enter
        
    Returns:
        True if user confirms, False otherwise
    """
    default_text = " [Y/n]" if default else " [y/N]"
    
    while True:
        try:
            response = input(f"{message}{default_text}: ").strip().lower()
            
            if not response:  # User just pressed Enter
                return default
            
            if response in ['y', 'yes']:
                return True
            elif response in ['n', 'no']:
                return False
            else:
                print("Please enter 'y' for yes or 'n' for no.")
        
        except (KeyboardInterrupt, EOFError):
            print("\nOperation cancelled by user.")
            return False


def detect_drag_drop_mode(args) -> bool:
    """
    Detect if the script is being run in drag-and-drop mode.
    
    Args:
        args: Parsed command line arguments
        
    Returns:
        True if this appears to be drag-and-drop usage
    """
    # Drag-drop mode indicators:
    # 1. No destination specified
    # 2. All paths are files (not directories)
    # 3. No special flags set
    
    if args.destination:
        return False
    
    if args.summary_only or args.dry_run or args.copy:
        return False
    
    # Check if all paths are existing files
    all_files = True
    for path_str in args.paths:
        path = Path(path_str)
        if not path.exists() or not path.is_file():
            all_files = False
            break
    
    return all_files


def interactive_bundle_mode(files: List[Path], bundler: SeriesBundler) -> int:
    """
    Handle interactive bundling mode for drag-and-drop scenarios.
    
    Args:
        files: List of file paths to bundle
        bundler: Initialized SeriesBundler instance
        
    Returns:
        Exit code (0 for success, 1 for failure/cancellation)
    """
    print(f"=== Interactive Bundling Mode ===")
    print(f"Found {len(files)} files to bundle.")
    print()
    
    # Analyze files
    print("Analyzing files...")
    bundler.analyze_files(files)
    
    if not bundler.file_grouper.groups:
        print("No series groups found. Files may not be recognized as series episodes.")
        print("\nPress Enter to exit...")
        input()
        return 1
    
    # Show what would be created
    print(bundler._color("\n=== Dry Run Preview ===", Fore.YELLOW + Style.BRIGHT))
    bundler.print_summary()
    
    # Determine destination - use parent directory of first file
    first_file_dir = Path(files[0]).parent
    destination = first_file_dir
    
    print(bundler._color("\n=== Proposed Structure ===", Fore.CYAN + Style.BRIGHT))
    folder_emoji = bundler._get_emoji('folder')
    print(f"Destination: {folder_emoji} {bundler._color(str(destination), Fore.CYAN)}")
    print()
    
    # Sort groups by season and files within groups by episode
    sorted_groups = sorted(
        bundler.file_grouper.groups.items(),
        key=lambda x: (
            x[1][0].get('title', ''),
            x[1][0].get('season') or 0
        )
    )
    
    # Show detailed preview
    for group_key, group_metadata in sorted_groups:
        # Sort files within group by episode number
        group_metadata = sorted(
            group_metadata,
            key=lambda x: (x.get('episode') or 0)
        )
        folder_name = bundler.generate_folder_name(group_metadata, group_key)
        folder_path = destination / folder_name
        
        # Find newest file date in this group
        newest_file_time = None
        for metadata in group_metadata:
            source_path = Path(metadata['filepath'])
            if source_path.exists():
                file_mtime = source_path.stat().st_mtime
                if newest_file_time is None or file_mtime > newest_file_time:
                    newest_file_time = file_mtime
        
        newest_date_str = datetime.fromtimestamp(newest_file_time).strftime('%Y-%m-%d') if newest_file_time else "Unknown"
        
        # Print folder and files
        folder_emoji = bundler._get_emoji('folder')
        calendar_emoji = bundler._get_emoji('calendar')
        print(bundler._color(f"{folder_emoji} {folder_name}/ ({newest_date_str})", Fore.CYAN + Style.BRIGHT))
        
        # Collect present episodes for missing episode detection
        present_episodes = []
        for metadata in group_metadata:
            # Use original_episode for absolute numbering if available, fall back to episode
            ep_num = metadata.get('original_episode') if 'original_episode' in metadata else metadata.get('episode')
            if ep_num is not None:
                # Keep as float to preserve decimal episodes like 14.5
                present_episodes.append(float(ep_num))
        
        if present_episodes:
            present_set = set(present_episodes)
            min_ep = min(present_episodes)
            max_ep = max(present_episodes)
            
            # Get total episodes for the series to determine actual range
            title = group_metadata[0].get('title', '')
            season = group_metadata[0].get('season')
            total_episodes = bundler._get_total_episodes(title, season)
            
            # Calculate the starting episode for this season
            season_start_ep = 1  # Default for season 1 or unknown season
            season_episodes = {}  # Track episodes per season
            
            # Always populate season_episodes for multi-season shows
            metadata_id = group_metadata[0].get('metadata_id')
            if metadata_id and metadata_id in bundler.file_grouper.title_metadata:
                title_meta = bundler.file_grouper.title_metadata[metadata_id]
                base_title = title.lower()
                
                # Find all seasons and their episode counts
                for tid, tmeta in bundler.file_grouper.title_metadata.items():
                    tmal = tmeta.get('myanimelist_watch_status')
                    if tmal:
                        tmal_title = tmal.get('series_title', '').lower()
                        if base_title in tmal_title or tmal_title in base_title:
                            tmal_season = tmal.get('season_number', 1)
                            tmal_eps = tmal.get('series_episodes', 0)
                            # Only keep highest episode count for each season (in case of duplicates)
                            if tmal_season not in season_episodes or tmal_eps > season_episodes[tmal_season]:
                                season_episodes[tmal_season] = tmal_eps
            
            if season and season > 1:
                # For multi-season anime, calculate where this season starts
                # by summing episodes from previous seasons
                for s_num in sorted(season_episodes.keys()):
                    if s_num < season:
                        season_start_ep += season_episodes[s_num]
            
            # Determine the full episode range to check
            if total_episodes:
                # Check from season start to end of current season only
                min_check = season_start_ep
                # Calculate the end of current season
                if season in season_episodes:
                    # Use the episode count for this specific season
                    max_check = season_start_ep + season_episodes[season] - 1
                else:
                    # Fallback: use the max present episode
                    max_check = max_ep
            else:
                # Just check the range of present episodes
                min_check = min_ep
                max_check = max_ep
            
            # Build display with missing episode markers
            file_emoji = bundler._get_emoji('file')
            cross_emoji = bundler._get_emoji('cross')
            
            # Create a sorted list of all episodes to display (integers + bonus episodes)
            all_episodes = []
            for ep in range(int(min_check), int(max_check) + 1):
                all_episodes.append(ep)
                # Check for bonus episodes between this ep and next (e.g., 14.5)
                for m in group_metadata:
                    m_ep = m.get('original_episode') if 'original_episode' in m else m.get('episode')
                    if m_ep is not None:
                        m_ep_float = float(m_ep)
                        # If bonus episode exists between current and next integer
                        if ep < m_ep_float < ep + 1:
                            all_episodes.append(m_ep_float)
            
            # Sort to ensure correct order
            all_episodes.sort()
            
            # Display each episode
            for ep in all_episodes:
                if ep in present_set:
                    # Find the metadata for this episode
                    metadata = None
                    for m in group_metadata:
                        m_ep = m.get('original_episode') if 'original_episode' in m else m.get('episode')
                        if m_ep is not None and float(m_ep) == ep:
                            metadata = m
                            break
                    
                    if metadata:
                        # Display actual file
                        filename = metadata['filename']
                        source_path = Path(metadata['filepath'])
                        
                        # Get file modification time
                        if source_path.exists():
                            file_mtime = source_path.stat().st_mtime
                            file_date_str = datetime.fromtimestamp(file_mtime).strftime('%Y-%m-%d')
                        else:
                            file_date_str = "Unknown"
                        
                        print(f"   {file_emoji} {filename} {bundler._color(f'({file_date_str})', Style.DIM + Fore.WHITE)}")
                else:
                    # Display missing episode marker (only for integer episodes)
                    if ep == int(ep):
                        print(bundler._color(f"   {cross_emoji} {title} - {int(ep):02d}", Style.DIM + Fore.RED))
        else:
            # No episode info, just display files normally
            file_emoji = bundler._get_emoji('file')
            for metadata in group_metadata:
                filename = metadata['filename']
                source_path = Path(metadata['filepath'])
                
                # Get file modification time
                if source_path.exists():
                    file_mtime = source_path.stat().st_mtime
                    file_date_str = datetime.fromtimestamp(file_mtime).strftime('%Y-%m-%d')
                else:
                    file_date_str = "Unknown"
                
                print(f"   {file_emoji} {filename} {bundler._color(f'({file_date_str})', Style.DIM + Fore.WHITE)}")
        
        print()
    
    # Get user confirmation
    print(bundler._color("This will:", Fore.YELLOW + Style.BRIGHT))
    print(f"1. Create folder structure in: {bundler._color(str(destination), Fore.CYAN)}")
    print(f"2. Move {bundler._color(str(len(files)), Fore.MAGENTA)} files into {bundler._color(str(len(bundler.file_grouper.groups)), Fore.MAGENTA)} organized folders")
    print(f"3. Original files will be {bundler._color('moved', Fore.RED)} (not copied)")
    print()
    
    if not get_user_confirmation("Do you want to proceed with bundling?", default=False):
        print(bundler._color("Operation cancelled by user.", Fore.YELLOW))
        print("\nPress Enter to exit...")
        input()
        return 1
    
    # Perform the actual bundling
    print("\nBundling files...")
    try:
        results = bundler.create_bundles(
            destination=str(destination),
            copy_files=False,  # Move files
            dry_run=False
        )
        
        if results:
            complete_emoji = bundler._get_emoji('complete')
            print(bundler._color(f"\n{complete_emoji} Successfully created {len(results)} bundle folders!", Fore.GREEN + Style.BRIGHT))
            print(f"Files have been organized in: {bundler._color(str(destination), Fore.CYAN)}")
            calendar_emoji = bundler._get_emoji('calendar')
            print(bundler._color(f"\n{calendar_emoji} Folder dates set to newest file:", Fore.YELLOW))
            for group_key, result_info in results.items():
                folder_name = Path(result_info['folder_path']).name
                newest_date = result_info.get('newest_file_date', 'Unknown')
                folder_emoji = bundler._get_emoji('folder')
                print(f"  {folder_emoji} {bundler._color(folder_name, Fore.CYAN)}: {bundler._color(newest_date, Style.DIM + Fore.WHITE)}")
        else:
            cross_emoji = bundler._get_emoji('cross')
            print(bundler._color(f"{cross_emoji} No files were bundled.", Fore.RED))
            print("\nPress Enter to exit...")
            input()
            return 1
            
    except Exception as e:
        cross_emoji = bundler._get_emoji('cross')
        print(bundler._color(f"{cross_emoji} Error during bundling: {e}", Fore.RED))
        print("\nPress Enter to exit...")
        input()
        return 1
    
    print("\nPress Enter to exit...")
    input()
    return 0


def _get_metadata_manager():
    """Get or create metadata manager instance."""
    try:
        # Add video-optimizer-v2 to path if not already there
        video_optimizer_path = Path(__file__).parent / 'video-optimizer-v2'
        if video_optimizer_path.exists() and str(video_optimizer_path) not in sys.path:
            sys.path.insert(0, str(video_optimizer_path))
        
        from metadata_provider import MetadataManager
        from anime_metadata import AnimeDataProvider
        from imdb_metadata import IMDbDataProvider
        
        # Create providers
        providers = [
            AnimeDataProvider(),
            IMDbDataProvider()
        ]
        
        return MetadataManager(providers)
    except ImportError as e:
        print(f"Warning: Could not load metadata providers: {e}")
        return None
    except Exception as e:
        print(f"Warning: Error creating metadata manager: {e}")
        return None


def main():
    """Command-line interface for Series Bundler."""
    parser = argparse.ArgumentParser(
        description="Bundle series files into organized folders for archiving",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Drag-and-drop mode (interactive)
  python series_bundler.py file1.mkv file2.mkv file3.mkv

  # Analyze files in current directory
  python series_bundler.py . --summary-only

  # Bundle files to destination (dry run)
  python series_bundler.py /path/to/series -d /path/to/archive --dry-run

  # Actually move files to organized folders
  python series_bundler.py /path/to/series -d /path/to/archive

  # Copy files instead of moving them
  python series_bundler.py /path/to/series -d /path/to/archive --copy
        """
    )
    
    parser.add_argument(
        'paths',
        nargs='+',
        help='Paths to files or directories to process'
    )
    
    parser.add_argument(
        '-d', '--destination',
        help='Destination directory for bundled folders'
    )
    
    parser.add_argument(
        '--copy',
        action='store_true',
        help='Copy files instead of moving them'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be done without actually doing it'
    )
    
    parser.add_argument(
        '-r', '--recursive',
        action='store_true',
        help='Search directories recursively for video files'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='count',
        default=0,
        help='Increase verbosity (use -v, -vv, or -vvv)'
    )
    
    parser.add_argument(
        '--summary-only',
        action='store_true',
        help='Only show summary, don\'t create bundles'
    )
    
    parser.add_argument(
        '--no-interactive',
        action='store_true',
        help='Disable interactive mode even for drag-and-drop scenarios'
    )
    
    parser.add_argument(
        '--no-color',
        action='store_true',
        help='Disable color formatting in output'
    )
    
    parser.add_argument(
        '--myanimelist-xml',
        metavar='PATH',
        help='Path to MyAnimeList XML file (can be .gz) for watch status lookup and completeness checking. '
             'Supports wildcards (* and ?) - will use the latest file by creation time if multiple matches found.'
    )
    
    args = parser.parse_args()
    
    # Check for drag-and-drop mode
    is_drag_drop = detect_drag_drop_mode(args) and not args.no_interactive
    
    if is_drag_drop:
        # Handle drag-and-drop mode
        print("Series Bundler - Drag & Drop Mode")
        print("=" * 40)
        
        # Filter only video files from the provided paths
        video_files = []
        for path_str in args.paths:
            path = Path(path_str)
            if path.exists() and path.is_file():
                # Check if it's a video file
                video_extensions = {
                    '.mkv', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', 
                    '.m4v', '.mpg', '.mpeg', '.3gp', '.ogv', '.ts', '.m2ts'
                }
                if path.suffix.lower() in video_extensions:
                    video_files.append(path)
                else:
                    print(f"Skipping non-video file: {path.name}")
        
        if not video_files:
            print("No video files found in the provided paths.")
            print("\nPress Enter to exit...")
            input()
            return 1
        
        # Initialize bundler
        bundler = SeriesBundler(
            verbose=1, 
            use_colors=not args.no_color,
            myanimelist_xml_path=args.myanimelist_xml if hasattr(args, 'myanimelist_xml') else None,
            metadata_manager=_get_metadata_manager()
        )  # Always use some verbosity in interactive mode
        
        # Run interactive bundling
        return interactive_bundle_mode(video_files, bundler)
    
    else:
        # Handle normal mode
        if args.verbose >= 1:
            print("Discovering video files...")
        
        files = discover_video_files(args.paths, args.recursive)
        
        if not files:
            print("No video files found.")
            print("\nPress Enter to exit...")
            input()
            return 1
        
        if args.verbose >= 1:
            print(f"Found {len(files)} video files")
        
        # Initialize bundler
        bundler = SeriesBundler(
            verbose=args.verbose, 
            use_colors=not args.no_color,
            myanimelist_xml_path=args.myanimelist_xml if hasattr(args, 'myanimelist_xml') else None,
            metadata_manager=_get_metadata_manager()
        )
        
        # Analyze files
        bundler.analyze_files(files)
        
        # Show summary
        bundler.print_summary()
        
        # Create bundles if destination is specified and not summary-only
        if args.destination and not args.summary_only:
            if args.dry_run or args.verbose >= 1:
                action = "copy" if args.copy else "move"
                print(f"\nWould {action} files to: {args.destination}")
            
            results = bundler.create_bundles(
                destination=args.destination,
                copy_files=args.copy,
                dry_run=args.dry_run
            )
            
            if results and not args.dry_run:
                print(f"\nSuccessfully created {len(results)} bundle folders")
        
        elif not args.destination and not args.summary_only:
            print("\nUse -d/--destination to specify where to create bundle folders")
            print("Or use --summary-only to just analyze files")
            print("Or drag-and-drop files directly onto this script for interactive mode")
        
        # Pause before exit in normal mode (unless it's a simple summary)
        if not args.summary_only or args.verbose >= 1:
            print("\nPress Enter to exit...")
            input()
        
        return 0


if __name__ == '__main__':
    sys.exit(main())
