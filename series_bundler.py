import argparse
import os
import shutil
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import re

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


class SeriesBundler:
    """
    Groups series files and creates organized folder structures for archiving.
    
    The bundler analyzes files using guessit to extract metadata, groups them by
    series, release group, and resolution, then creates folder names following
    the pattern: [Release Group] Series Name (YYYY) (xx-yy) (Resolution)
    """
    
    def __init__(self, verbose: int = 0):
        """
        Initialize the SeriesBundler.
        
        Args:
            verbose: Verbosity level (0=quiet, 1=normal, 2=verbose)
        """
        self.verbose = verbose
        self.file_metadata = {}
        self.series_groups = defaultdict(list)
        
    def _log(self, message: str, level: int = 1):
        """Log message if verbosity level is sufficient."""
        if self.verbose >= level:
            print(message)
    
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
    
    def extract_metadata(self, file_path: Path) -> Dict:
        """
        Extract metadata from a file using guessit.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Dictionary containing extracted metadata
        """
        try:
            metadata = guessit_wrapper(file_path.name)
            result = dict(metadata)
            result['filepath'] = str(file_path)
            result['filename'] = file_path.name
            result['file_size'] = file_path.stat().st_size if file_path.exists() else 0
            
            # Import re at function level for episode title processing
            import re
            
            # Handle episode field that might contain misclassified years or title numbers
            if 'episode' in result:
                episode = result['episode']
                
                # If episode is a list, assume first number is part of title, smaller number is episode
                if isinstance(episode, list) and len(episode) >= 2:
                    # Sort to find the smallest number (likely the actual episode)
                    sorted_episodes = sorted(episode)
                    smallest_episode = sorted_episodes[0]
                    
                    # The other numbers are likely part of the title
                    title_numbers = [ep for ep in episode if ep != smallest_episode]
                    
                    # Add title numbers to the title
                    current_title = result.get('title', '')
                    for title_num in title_numbers:
                        if current_title and str(title_num) not in current_title:
                            current_title = f"{current_title} {title_num}"
                    result['title'] = current_title
                    
                    # Keep only the smallest number as the episode
                    result['episode'] = smallest_episode
                    
                    self._log(f"Moved title number(s) {title_numbers} from episode to title, kept episode {smallest_episode}", 2)
            
            # Normalize episode information and title
            # Handle cases where episode_title contains series subtitle/season name vs episode number
            if 'episode_title' in result:
                episode_title = result['episode_title']
                
                # Case 1: episode_title contains just a number (like "01") and no episode field
                if 'episode' not in result and isinstance(episode_title, str) and episode_title.isdigit():
                    result['episode'] = int(episode_title)
                    # Remove episode_title since it was actually the episode number
                    del result['episode_title']
                
                # Case 2: episode_title contains episode number among other text
                elif 'episode' not in result and isinstance(episode_title, str):
                    numbers = re.findall(r'\d+', episode_title)
                    if numbers and len(numbers) == 1 and episode_title.strip().isdigit():
                        # Only if episode_title is purely numeric
                        result['episode'] = int(numbers[0])
                        del result['episode_title']
                
                # Case 3: Decimal episode number (e.g., "12.5" becomes episode=12, episode_title="5")
                elif ('episode' in result and isinstance(episode_title, str) and 
                      episode_title.isdigit() and len(episode_title) <= 2):
                    # This looks like the decimal part of an episode number
                    main_episode = result['episode']
                    decimal_part = episode_title
                    # Reconstruct as decimal episode number
                    result['episode'] = float(f"{main_episode}.{decimal_part}")
                    # Remove episode_title since we've incorporated it into the episode number
                    del result['episode_title']
                
                # Case 4: episode_title looks like a series subtitle (contains non-numeric content)
                # and we already have an episode number
                elif ('episode' in result and isinstance(episode_title, str) and 
                      not episode_title.isdigit() and 
                      not re.match(r'^\d+$', episode_title.strip())):
                    # This looks like a series subtitle, append it to the title
                    original_title = result.get('title', '')
                    if original_title and episode_title:
                        # Combine title with episode_title (which is likely a subtitle/season name)
                        result['title'] = f"{original_title} - {episode_title}"
                        # Remove episode_title since we've incorporated it into the main title
                        del result['episode_title']
            
            # Handle alternative_title field - often contains series subtitle
            if 'alternative_title' in result:
                alternative_title = result['alternative_title']
                original_title = result.get('title', '')
                
                if original_title and alternative_title:
                    # Combine title with alternative_title
                    result['title'] = f"{original_title} - {alternative_title}"
                    # Remove alternative_title since we've incorporated it into the main title
                    del result['alternative_title']
            
            self._log(f"Extracted metadata for {file_path.name}: {result.get('title', 'Unknown')}", 2)
            return result
            
        except Exception as e:
            self._log(f"Warning: Could not extract metadata from {file_path.name}: {e}", 1)
            return {
                'filepath': str(file_path),
                'filename': file_path.name,
                'file_size': file_path.stat().st_size if file_path.exists() else 0,
                'title': 'Unknown',
                'type': 'unknown'
            }
    
    def _get_grouping_key(self, metadata: Dict) -> str:
        """
        Generate a grouping key for files that should be bundled together.
        
        Args:
            metadata: File metadata from guessit
            
        Returns:
            String key for grouping files
        """
        title = metadata.get('title', 'Unknown')
        year = metadata.get('year', '')
        release_group = metadata.get('release_group', 'Unknown')
        screen_size = metadata.get('screen_size', 'Unknown')
        season = metadata.get('season', 1)  # Default to season 1 if not specified
        
        # Create a key that groups files of the same series, release group, resolution, and season
        key_parts = [
            f"title:{str(title).lower()}",
            f"year:{year}",
            f"release_group:{str(release_group).lower()}",
            f"screen_size:{str(screen_size).lower()}",
            f"season:{season}"
        ]
        
        return " | ".join(key_parts)
    
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
    
    def generate_folder_name(self, metadata_list: List[Dict]) -> str:
        """
        Generate folder name based on series metadata.
        
        Args:
            metadata_list: List of file metadata dictionaries
            
        Returns:
            Generated folder name following the pattern:
            [Release Group] Series Name (YYYY) (xx-yy) (Resolution)
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
        
        # Build the series title with season if applicable
        series_title = str(title)
        if season and season > 1:
            series_title = f"{title} S{season}"
        
        # Collect all episode numbers (can be int or float)
        episodes = []
        for metadata in metadata_list:
            episode = metadata.get('episode')
            if isinstance(episode, list):
                episodes.extend(episode)
            elif episode is not None:
                episodes.append(episode)
        
        # Format episode range
        episode_range = self._format_episode_range(episodes)
        
        # Clean components for filesystem compatibility
        clean_title = self._clean_filename(series_title)
        clean_release_group = self._clean_filename(str(release_group))
        
        # Build folder name
        folder_parts = [f"[{clean_release_group}]", clean_title]
        
        if year:
            folder_parts.append(f"({year})")
        
        folder_parts.append(f"({episode_range})")
        folder_parts.append(f"({screen_size})")
        
        folder_name = " ".join(folder_parts)
        
        self._log(f"Generated folder name: {folder_name}", 2)
        return folder_name
    
    def analyze_files(self, file_paths: List[Path]) -> Dict[str, List[Dict]]:
        """
        Analyze files and group them by series.
        
        Args:
            file_paths: List of file paths to analyze
            
        Returns:
            Dictionary mapping group keys to lists of file metadata
        """
        self._log(f"Analyzing {len(file_paths)} files...", 1)
        
        # Extract metadata for all files
        with tqdm(file_paths, desc="Extracting metadata", disable=self.verbose == 0) as pbar:
            for file_path in pbar:
                metadata = self.extract_metadata(file_path)
                self.file_metadata[str(file_path)] = metadata
                
                # Group files
                group_key = self._get_grouping_key(metadata)
                self.series_groups[group_key].append(metadata)
                
                if self.verbose >= 1:
                    pbar.set_postfix(groups=len(self.series_groups))
        
        self._log(f"Found {len(self.series_groups)} series groups", 1)
        return dict(self.series_groups)
    
    def validate_series_consistency(self, group_metadata: List[Dict]) -> Tuple[bool, List[str]]:
        """
        Validate that files in a group belong to the same series.
        
        Args:
            group_metadata: List of file metadata for a group
            
        Returns:
            Tuple of (is_valid, list_of_warnings)
        """
        if len(group_metadata) <= 1:
            return True, []
        
        warnings = []
        first_file = group_metadata[0]
        
        # Check title consistency
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
        if not self.series_groups:
            self._log("No series groups found. Run analyze_files() first.", 1)
            return {}
        
        destination_path = Path(destination)
        if not dry_run:
            destination_path.mkdir(parents=True, exist_ok=True)
        
        results = {}
        action_word = "Copying" if copy_files else "Moving"
        
        self._log(f"{action_word} files to bundles{'(DRY RUN)' if dry_run else ''}...", 1)
        
        for group_key, group_metadata in self.series_groups.items():
            # Validate group consistency
            is_valid, warnings = self.validate_series_consistency(group_metadata)
            
            if warnings:
                self._log(f"Warnings for group {group_key}:", 1)
                for warning in warnings:
                    self._log(f"  - {warning}", 1)
            
            # Generate folder name
            folder_name = self.generate_folder_name(group_metadata)
            folder_path = destination_path / folder_name
            
            self._log(f"\nProcessing group: {group_metadata[0].get('title', 'Unknown')}", 1)
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
                    newest_date_str = datetime.fromtimestamp(newest_file_time).strftime('%Y-%m-%d %H:%M:%S')
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
                'newest_file_date': datetime.fromtimestamp(newest_file_time).strftime('%Y-%m-%d %H:%M:%S') if newest_file_time else None
            }
        
        return results
    
    def get_summary(self) -> Dict:
        """
        Get summary statistics about the analyzed files.
        
        Returns:
            Dictionary containing summary information
        """
        total_files = sum(len(group) for group in self.series_groups.values())
        total_size = sum(
            sum(metadata.get('file_size', 0) for metadata in group)
            for group in self.series_groups.values()
        )
        
        # Group statistics
        group_sizes = [len(group) for group in self.series_groups.values()]
        
        return {
            'total_files': total_files,
            'total_groups': len(self.series_groups),
            'total_size_bytes': total_size,
            'total_size_mb': round(total_size / (1024 * 1024), 2),
            'average_files_per_group': round(sum(group_sizes) / len(group_sizes), 1) if group_sizes else 0,
            'largest_group_size': max(group_sizes) if group_sizes else 0,
            'smallest_group_size': min(group_sizes) if group_sizes else 0
        }
    
    def print_summary(self):
        """Print a summary of the analysis."""
        if not self.series_groups:
            print("No series groups found.")
            return
        
        summary = self.get_summary()
        
        print(f"\n=== Series Bundler Summary ===")
        print(f"Total files: {summary['total_files']}")
        print(f"Total groups: {summary['total_groups']}")
        print(f"Total size: {summary['total_size_mb']} MB")
        print(f"Average files per group: {summary['average_files_per_group']}")
        print(f"Largest group: {summary['largest_group_size']} files")
        print(f"Smallest group: {summary['smallest_group_size']} files")
        
        print(f"\n=== Groups ===")
        for i, (group_key, group_metadata) in enumerate(self.series_groups.items(), 1):
            first_file = group_metadata[0]
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
            
            episode_range = self._format_episode_range(episodes)
            folder_name = self.generate_folder_name(group_metadata)
            
            print(f"{i:2d}. {title} [{release_group}] ({screen_size}) - {len(group_metadata)} files")
            print(f"    Episodes: {episode_range}")
            print(f"    Folder: {folder_name}")


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
    
    if not bundler.series_groups:
        print("No series groups found. Files may not be recognized as series episodes.")
        print("\nPress Enter to exit...")
        input()
        return 1
    
    # Show what would be created
    print(f"\n=== Dry Run Preview ===")
    bundler.print_summary()
    
    # Determine destination - use parent directory of first file
    first_file_dir = Path(files[0]).parent
    destination = first_file_dir
    
    print(f"\n=== Proposed Structure ===")
    print(f"Destination: {destination}")
    print()
    
    # Show detailed preview
    for group_key, group_metadata in bundler.series_groups.items():
        folder_name = bundler.generate_folder_name(group_metadata)
        folder_path = destination / folder_name
        
        # Find newest file date in this group
        newest_file_time = None
        for metadata in group_metadata:
            source_path = Path(metadata['filepath'])
            if source_path.exists():
                file_mtime = source_path.stat().st_mtime
                if newest_file_time is None or file_mtime > newest_file_time:
                    newest_file_time = file_mtime
        
        newest_date_str = datetime.fromtimestamp(newest_file_time).strftime('%Y-%m-%d %H:%M:%S') if newest_file_time else "Unknown"
        
        print(f"📁 {folder_name}/")
        print(f"   📅 Folder date will be set to: {newest_date_str}")
        for metadata in group_metadata:
            filename = metadata['filename']
            print(f"   📄 {filename}")
        print()
    
    # Get user confirmation
    print("This will:")
    print(f"1. Create folder structure in: {destination}")
    print(f"2. Move {len(files)} files into {len(bundler.series_groups)} organized folders")
    print(f"3. Original files will be moved (not copied)")
    print()
    
    if not get_user_confirmation("Do you want to proceed with bundling?", default=False):
        print("Operation cancelled by user.")
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
            print(f"\n✅ Successfully created {len(results)} bundle folders!")
            print(f"Files have been organized in: {destination}")
            print("\nFolder dates set to newest file:")
            for group_key, result_info in results.items():
                folder_name = Path(result_info['folder_path']).name
                newest_date = result_info.get('newest_file_date', 'Unknown')
                print(f"  📁 {folder_name}: {newest_date}")
        else:
            print("❌ No files were bundled.")
            print("\nPress Enter to exit...")
            input()
            return 1
            
    except Exception as e:
        print(f"❌ Error during bundling: {e}")
        print("\nPress Enter to exit...")
        input()
        return 1
    
    print("\nPress Enter to exit...")
    input()
    return 0


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
        bundler = SeriesBundler(verbose=1)  # Always use some verbosity in interactive mode
        
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
        bundler = SeriesBundler(verbose=args.verbose)
        
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
