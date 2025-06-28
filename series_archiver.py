import argparse
import json
import os
import shutil
import sys
import re
import binascii
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Protocol

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False


class ProgressReporter(Protocol):
    """Protocol for progress reporting callbacks."""
    
    def on_start(self, total_files: int, action_desc: str) -> None:
        """Called when archiving starts."""
        ...
    
    def on_group_start(self, group_name: str, file_count: int) -> None:
        """Called when processing a group starts."""
        ...
    
    def on_file_processed(self, filename: str, success: bool, error_msg: Optional[str] = None) -> None:
        """Called when a file is processed."""
        ...
    
    def on_group_complete(self, group_name: str, success_count: int, error_count: int) -> None:
        """Called when a group is completed."""
        ...
    
    def on_complete(self, total_groups: int) -> None:
        """Called when all archiving is complete."""
        ...


class CLIProgressReporter:
    """CLI-based progress reporter using tqdm if available."""
    
    def __init__(self, verbose: int = 0, use_progress_bars: bool = True):
        self.verbose = verbose
        self.use_progress_bars = use_progress_bars and TQDM_AVAILABLE
        self.overall_pbar = None
        self.group_pbar = None
        
    def on_start(self, total_files: int, action_desc: str) -> None:
        """Called when archiving starts."""
        if self.use_progress_bars and self.verbose >= 0:
            self.overall_pbar = tqdm(
                total=total_files,
                desc=f"{action_desc} files",
                unit="file",
                disable=self.verbose == 0
            )
    
    def on_group_start(self, group_name: str, file_count: int) -> None:
        """Called when processing a group starts."""
        if self.use_progress_bars and self.verbose >= 1:
            group_desc = f"{group_name[:30]}..." if len(group_name) > 30 else group_name
            self.group_pbar = tqdm(
                total=file_count,
                desc=group_desc,
                unit="file",
                leave=False,
                disable=False
            )
    
    def on_file_processed(self, filename: str, success: bool, error_msg: Optional[str] = None) -> None:
        """Called when a file is processed."""
        if self.use_progress_bars:
            # Update group progress bar
            if self.group_pbar and self.verbose >= 1:
                display_name = filename[:40] + "..." if len(filename) > 40 else filename
                self.group_pbar.set_postfix_str(display_name)
                self.group_pbar.update(1)
            
            # Update overall progress bar
            if self.overall_pbar:
                self.overall_pbar.update(1)
            
            # Handle errors
            if not success and error_msg:
                if self.use_progress_bars:
                    tqdm.write(f"  Error processing {filename}: {error_msg}")
                else:
                    print(f"  Error processing {filename}: {error_msg}")
    
    def on_group_complete(self, group_name: str, success_count: int, error_count: int) -> None:
        """Called when a group is completed."""
        if self.group_pbar:
            self.group_pbar.close()
            self.group_pbar = None
        
        print(f"  Processed {success_count} files successfully")
        if error_count > 0:
            print(f"  {error_count} files had errors")
    
    def on_complete(self, total_groups: int) -> None:
        """Called when all archiving is complete."""
        if self.overall_pbar:
            self.overall_pbar.close()
            self.overall_pbar = None


class SeriesArchiver:
    """
    A class for archiving anime series files based on series completeness checker output.
    Organizes files into folders following the pattern:
    [release_group] show_name (start_ep-last_ep) (resolution)
    """
    
    def __init__(self, verbose: int = 0, progress_reporter: Optional[ProgressReporter] = None):
        self.data: Optional[Dict] = None
        self.groups: Dict = {}
        self.verbose = verbose
        self.progress_reporter = progress_reporter
        
    def _log(self, message: str, level: int = 1):
        """Log message if verbosity level is sufficient."""
        if self.verbose >= level:
            print(message)
    
    def load_data(self, json_file_path: str) -> bool:
        """Load series data from JSON file."""
        try:
            with open(json_file_path, 'r', encoding='utf-8') as f:
                self.data = json.load(f)
            if not isinstance(self.data, dict):
                return False
            self.groups = self.data.get('groups', {})
            self._log(f"Loaded {len(self.groups)} groups from {json_file_path}", 2)
            return True
        except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
            print(f"Error loading data: {e}")
            return False
    
    def list_groups(self, show_details: bool = False) -> List[Tuple[str, Dict]]:
        """Get list of all groups with their details."""
        if not self.groups:
            return []
        
        group_list = []
        for group_key, group_data in self.groups.items():
            title = group_data.get('title', 'Unknown')
            episodes_found = group_data.get('episodes_found', 0)
            episodes_expected = group_data.get('episodes_expected', 0)
            status = group_data.get('status', 'unknown')
            
            details = {
                'title': title,
                'episodes_found': episodes_found,
                'episodes_expected': episodes_expected,
                'status': status,
                'data': group_data
            }
            
            if show_details:
                files = group_data.get('files', [])
                if files:
                    details['release_group'] = files[0].get('release_group', 'Unknown')
                    details['screen_size'] = files[0].get('screen_size', 'Unknown')
                    details['folder_name'] = self.generate_folder_name(group_data)
            
            group_list.append((group_key, details))
        
        return group_list
    
    def get_group_details(self, group_key: str) -> Optional[Dict]:
        """Get detailed information about a specific group."""
        return self.groups.get(group_key)
    
    def generate_folder_name(self, group_data: Dict) -> str:
        """Generate folder name following the specified pattern."""
        files = group_data.get('files', [])
        if not files:
            return "Unknown"
        
        # Get common attributes from files
        release_group = files[0].get('release_group', 'Unknown')
        title = group_data.get('title', 'Unknown')
        
        # Get episode range
        episode_numbers = sorted(group_data.get('episode_numbers', []))
        if episode_numbers:
            start_ep = min(episode_numbers)
            last_ep = max(episode_numbers)
            episode_range = f"{start_ep:02d}-{last_ep:02d}" if start_ep != last_ep else f"{start_ep:02d}"
        else:
            episode_range = "00"
        
        # Get resolution
        screen_size = files[0].get('screen_size', 'Unknown')
        
        # Clean title for filesystem
        clean_title = self._clean_filename(title)
        
        return f"[{release_group}] {clean_title} ({episode_range}) ({screen_size})"
    
    def _clean_filename(self, filename: str) -> str:
        """Clean filename by removing invalid characters."""
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '')
        return filename.strip()
    
    def _extract_crc_from_filename(self, filename: str) -> Optional[str]:
        """Extract CRC32 hash from filename if present. Pattern: [FFFFFFFF] where F is hex."""
        # Pattern matches [8 hex digits] at the end before file extension
        pattern = r'\[([A-Fa-f0-9]{8})\]'
        match = re.search(pattern, filename)
        return match.group(1).upper() if match else None
    
    def _calculate_file_crc32(self, filepath: str) -> str:
        """Calculate CRC32 hash of a file."""
        crc = 0
        with open(filepath, 'rb') as f:
            while chunk := f.read(8192):
                crc = binascii.crc32(chunk, crc)
        return f"{crc & 0xffffffff:08X}"
    
    def _format_episode_ranges(self, episodes: List[int]) -> str:
        """Format episode list as smart ranges (e.g., [1,2,3,5,6,8] -> '1-3, 5-6, 8')."""
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
        
        return f"{', '.join(ranges)}"
    
    def _get_watched_episodes(self, group_data: Dict) -> List[int]:
        """Extract watched episode numbers from group data."""
        watched_episodes = []
        files = group_data.get('files', [])
        
        for file_info in files:
            plex_status = file_info.get('plex_watch_status')
            if plex_status and plex_status.get('watched'):
                episode = file_info.get('episode')
                if isinstance(episode, list):
                    watched_episodes.extend(episode)
                elif episode is not None:
                    watched_episodes.append(episode)
        
        return sorted(set(watched_episodes)) if watched_episodes else []
    
    def _verify_file_crc(self, filepath: str, expected_crc: Optional[str] = None) -> Tuple[bool, str, str]:
        """
        Verify CRC32 of a file against expected CRC from filename or provided CRC.
        
        Returns:
            Tuple of (is_valid, expected_crc, actual_crc)
        """
        filename = os.path.basename(filepath)
        
        if expected_crc is None:
            expected_crc = self._extract_crc_from_filename(filename)
        
        if expected_crc is None:
            return False, "N/A", "N/A"
        
        try:
            actual_crc = self._calculate_file_crc32(filepath)
            return expected_crc.upper() == actual_crc.upper(), expected_crc.upper(), actual_crc.upper()
        except Exception as e:
            self._log(f"Error calculating CRC for {filename}: {e}", 1)
            return False, expected_crc.upper(), "ERROR"
    
    def check_files_crc(self, files_or_groups, is_groups: bool = False) -> Dict[str, Dict]:
        """
        Check CRC32 of files.
        
        Args:
            files_or_groups: List of file paths or dict of group data
            is_groups: If True, treats input as groups data from JSON
            
        Returns:
            Dict with CRC check results
        """
        results = {}
        files_to_check = []
        
        if is_groups:
            # Extract files from groups data - handle both dict and list inputs
            if isinstance(files_or_groups, dict):
                groups_dict = files_or_groups
            else:
                # Assume it's a list of (key, group_data) tuples
                groups_dict = dict(files_or_groups)
                
            for group_key, group_data in groups_dict.items():
                for file_info in group_data.get('files', []):
                    filepath = file_info.get('filepath')
                    if filepath and os.path.exists(filepath):
                        files_to_check.append({
                            'filepath': filepath,
                            'filename': file_info.get('filename', os.path.basename(filepath)),
                            'group': group_data.get('title', 'Unknown'),
                            'group_key': group_key
                        })
        else:
            # Direct file list
            for filepath in files_or_groups:
                if os.path.isfile(filepath):
                    files_to_check.append({
                        'filepath': filepath,
                        'filename': os.path.basename(filepath),
                        'group': 'Standalone',
                        'group_key': 'standalone'
                    })
        
        if not files_to_check:
            return results
        
        valid_count = 0
        invalid_count = 0
        no_crc_count = 0
        
        # Check CRC with progress
        desc = "Checking file CRC32"
        with tqdm(files_to_check, desc=desc, unit="file", disable=self.verbose == 0) as pbar:
            for file_info in pbar:
                filepath = file_info['filepath']
                filename = file_info['filename']
                
                if self.verbose >= 1:
                    pbar.set_postfix_str(filename[:40] + "..." if len(filename) > 40 else filename)
                
                is_valid, expected_crc, actual_crc = self._verify_file_crc(filepath)
                
                # Determine status
                if expected_crc == "N/A":
                    status = "no_crc"
                    no_crc_count += 1
                elif is_valid:
                    status = "valid"
                    valid_count += 1
                else:
                    status = "invalid"
                    invalid_count += 1
                
                results[filepath] = {
                    'filename': filename,
                    'group': file_info['group'],
                    'group_key': file_info['group_key'],
                    'status': status,
                    'expected_crc': expected_crc,
                    'actual_crc': actual_crc,
                    'is_valid': is_valid
                }
                
                # Log issues
                if status == "invalid":
                    self._log(f"CRC MISMATCH: {filename} (Expected: {expected_crc}, Actual: {actual_crc})", 1)
                elif status == "no_crc" and self.verbose >= 2:
                    self._log(f"No CRC in filename: {filename}", 2)
        
        # Print summary
        total_files = len(files_to_check)
        print(f"\nCRC Check Summary:")
        print(f"  Total files: {total_files}")
        print(f"  Valid CRC: {valid_count}")
        print(f"  Invalid CRC: {invalid_count}")
        print(f"  No CRC in filename: {no_crc_count}")
        
        if invalid_count > 0:
            print(f"\n⚠️  {invalid_count} files failed CRC validation!")
        elif valid_count > 0:
            print(f"✅ All {valid_count} files with CRC passed validation!")
        
        return results
    
    def archive_groups(self, selected_groups: List[str], destination_root: str, 
                      copy_files: bool = False, dry_run: bool = False, verify_crc: bool = False) -> Dict[str, str]:
        """
        Archive selected groups to destination folders.
        
        Args:
            selected_groups: List of group keys to archive
            destination_root: Root directory for output folders
            copy_files: If True, copy files instead of moving them
            dry_run: If True, show what would be done without actually doing it
            verify_crc: If True, verify CRC32 after file operations
            
        Returns:
            Dict mapping group keys to their destination folders
        """
        results = {}
        
        if not os.path.exists(destination_root):
            if not dry_run:
                os.makedirs(destination_root, exist_ok=True)
            self._log(f"{'Would create' if dry_run else 'Created'} destination root: {destination_root}")
        
        # Calculate total files for progress reporting
        total_files = 0
        for group_key in selected_groups:
            group_data = self.groups.get(group_key)
            if group_data:
                files = group_data.get('files', [])
                # Count only files that exist
                for file_info in files:
                    source_path = file_info.get('filepath')
                    if source_path and os.path.exists(source_path):
                        total_files += 1
        
        # Notify progress reporter of start
        action_desc = "Copying" if copy_files else "Moving"
        if dry_run:
            action_desc = f"Simulating {action_desc.lower()}"
        
        if self.progress_reporter:
            self.progress_reporter.on_start(total_files, action_desc)
        
        processed_files = []  # Track processed files for CRC verification
        
        for group_key in selected_groups:
            group_data = self.groups.get(group_key)
            if not group_data:
                print(f"Warning: Group '{group_key}' not found")
                continue
            
            folder_name = self.generate_folder_name(group_data)
            dest_folder = os.path.join(destination_root, folder_name)
            
            if not dry_run and not os.path.exists(dest_folder):
                os.makedirs(dest_folder, exist_ok=True)
            
            action_word = "Would process" if dry_run else "Processing"
            group_title = group_data.get('title', 'Unknown')
            print(f"\n{action_word} group: {group_title}")
            self._log(f"Destination folder: {dest_folder}")
            
            # Process files
            files = group_data.get('files', [])
            success_count = 0
            error_count = 0
            
            # Filter valid files for this group
            valid_files = []
            for file_info in files:
                source_path = file_info.get('filepath')
                if source_path and os.path.exists(source_path):
                    valid_files.append(file_info)
            
            # Notify progress reporter of group start
            if self.progress_reporter:
                self.progress_reporter.on_group_start(group_title, len(valid_files))
            
            for file_info in valid_files:
                source_path = file_info.get('filepath')
                filename = file_info.get('filename', os.path.basename(source_path))
                dest_path = os.path.join(dest_folder, filename)
                
                success = False
                error_msg = None
                
                try:
                    if dry_run:
                        action = "copy" if copy_files else "move"
                        self._log(f"  Would {action}: {filename}", 2)
                        # Simulate some work for dry run
                        if TQDM_AVAILABLE:
                            import time
                            time.sleep(0.01)  # Small delay to make progress visible
                    else:
                        if copy_files:
                            shutil.copy2(source_path, dest_path)
                            self._log(f"  Copied: {filename}", 2)
                        else:
                            shutil.move(source_path, dest_path)
                            self._log(f"  Moved: {filename}", 2)
                        
                        # Track processed file for CRC verification
                        if verify_crc:
                            processed_files.append({
                                'source_path': source_path,
                                'dest_path': dest_path,
                                'filename': filename,
                                'group_title': group_title
                            })
                    
                    success = True
                    success_count += 1
                    
                except Exception as e:
                    error_msg = str(e)
                    error_count += 1
                
                # Notify progress reporter of file completion
                if self.progress_reporter:
                    self.progress_reporter.on_file_processed(filename, success, error_msg)
            
            # Notify progress reporter of group completion
            if self.progress_reporter:
                self.progress_reporter.on_group_complete(group_title, success_count, error_count)
            else:
                # Fallback output if no progress reporter
                status_word = "Would process" if dry_run else "Processed"
                print(f"  {status_word} {success_count} files successfully")
                if error_count > 0:
                    print(f"  {error_count} files had errors")
            
            results[group_key] = dest_folder
        
        # Notify progress reporter of completion
        if self.progress_reporter:
            self.progress_reporter.on_complete(len(selected_groups))
        
        # Verify CRC after operations if requested
        if verify_crc and processed_files and not dry_run:
            print(f"\n=== CRC VERIFICATION ===")
            crc_results = {}
            
            with tqdm(processed_files, desc="Verifying file integrity", unit="file", disable=self.verbose == 0) as pbar:
                for file_info in pbar:
                    dest_path = file_info['dest_path']
                    filename = file_info['filename']
                    
                    if self.verbose >= 1:
                        pbar.set_postfix_str(filename[:40] + "..." if len(filename) > 40 else filename)
                    
                    is_valid, expected_crc, actual_crc = self._verify_file_crc(dest_path)
                    
                    if expected_crc != "N/A":
                        crc_results[dest_path] = {
                            'filename': filename,
                            'group': file_info['group_title'],
                            'is_valid': is_valid,
                            'expected_crc': expected_crc,
                            'actual_crc': actual_crc
                        }
                        
                        if not is_valid:
                            print(f"⚠️  CRC MISMATCH: {filename} (Expected: {expected_crc}, Actual: {actual_crc})")
            
            # CRC summary
            if crc_results:
                total_checked = len(crc_results)
                valid_count = sum(1 for r in crc_results.values() if r['is_valid'])
                invalid_count = total_checked - valid_count
                
                print(f"\nCRC Verification Summary:")
                print(f"  Files checked: {total_checked}")
                print(f"  Valid: {valid_count}")
                print(f"  Invalid: {invalid_count}")
                
                if invalid_count == 0:
                    print("✅ All files passed CRC verification!")
                else:
                    print(f"❌ {invalid_count} files failed CRC verification!")
        
        return results
    
    def get_summary(self) -> Dict:
        """Get summary statistics about loaded data."""
        if not self.data:
            return {}
        
        completeness = self.data.get('completeness_summary', {})
        
        # Calculate watch status summary
        total_watched = 0
        total_episodes = 0
        total_partially_watched = 0
        
        for group_data in self.groups.values():
            watch_status = group_data.get('watch_status', {})
            total_watched += watch_status.get('watched_episodes', 0)
            total_episodes += group_data.get('episodes_found', 0)
            total_partially_watched += watch_status.get('partially_watched_episodes', 0)
        
        summary = {
            'total_series': completeness.get('total_series', 0),
            'complete_series': completeness.get('complete_series', 0),
            'incomplete_series': completeness.get('incomplete_series', 0),
            'total_episodes_found': completeness.get('total_episodes_found', 0),
            'total_episodes_expected': completeness.get('total_episodes_expected', 0),
            'total_watched': total_watched,
            'total_episodes': total_episodes,
            'total_partially_watched': total_partially_watched
        }
        
        return summary


def cmd_list(args):
    """Handle the list command."""
    archiver = SeriesArchiver(verbose=args.verbose)
    
    if not archiver.load_data(args.input_json):
        return 1
    
    # Display summary
    if args.verbose > 0:
        summary = archiver.get_summary()
        print(f"Summary: {summary.get('total_series', 0)} series, "
              f"{summary.get('complete_series', 0)} complete, "
              f"{summary.get('incomplete_series', 0)} incomplete")
        
        # Add watch status summary if available
        total_watched = summary.get('total_watched', 0)
        total_episodes = summary.get('total_episodes', 0)
        total_partially_watched = summary.get('total_partially_watched', 0)
        
        if total_watched > 0 or total_partially_watched > 0:
            print(f"Watch Status: {total_watched}/{total_episodes} watched ({total_watched/total_episodes*100:.1f}%)")
            if total_partially_watched > 0:
                print(f"Partially watched: {total_partially_watched}")
        
        print()
    
    # List groups
    groups = archiver.list_groups(show_details=args.verbose > 0)
    if not groups:
        print("No groups found in the data.")
        return 0
    
    # Add original indices to groups for preservation
    indexed_groups = [(i + 1, group_key, details) for i, (group_key, details) in enumerate(groups)]
    
    # Filter by status if requested
    if hasattr(args, 'status_filter') and args.status_filter:
        all_statuses = {'complete', 'incomplete', 'complete_with_extras', 'no_episode_numbers', 
                       'unknown_total_episodes', 'not_series', 'no_metadata', 'no_metadata_manager', 'unknown'}
        
        # Parse include/exclude patterns
        status_filters = args.status_filter.split()
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
        
        # Apply filtering while preserving original indices
        filtered_indexed_groups = []
        for original_index, group_key, details in indexed_groups:
            if details['status'] in final_statuses:
                filtered_indexed_groups.append((original_index, group_key, details))
        indexed_groups = filtered_indexed_groups
    
    # Sort alphabetically if requested while preserving original indices
    if hasattr(args, 'sort') and args.sort:
        indexed_groups.sort(key=lambda x: x[2]['title'].lower())  # Sort by title (case-insensitive)
    
    if not indexed_groups:
        print("No groups found matching the filter criteria.")
        return 0
    
    print("Available series groups:")
    print("=" * 80)  # Consistent width
    
    for original_index, group_key, details in indexed_groups:
        # Status emoji from series_completeness_checker.py
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
        }.get(details['status'], '❓')
        
        # Get watch info and other episode info
        group_data = details.get('data', {})
        watched_episodes = archiver._get_watched_episodes(group_data)
        missing_episodes = group_data.get('missing_episodes', [])
        extra_episodes = group_data.get('extra_episodes', [])
        
        # Build episode info list
        episode_info = []
        if watched_episodes:
            watched_range = archiver._format_episode_ranges(watched_episodes)
            episode_info.append(f"Watched: {watched_range}/{details['episodes_expected']}")
        
        if missing_episodes:
            missing_range = archiver._format_episode_ranges(missing_episodes)
            episode_info.append(f"Missing: {missing_range}")
        
        if extra_episodes:
            extra_range = archiver._format_episode_ranges(extra_episodes)
            episode_info.append(f"Extra: {extra_range}")

        episode_info_str = ", ".join(episode_info) if episode_info else ""
        
        # Format title with proper truncation
        title_length = 45
        title_str = details['title']
        if len(title_str) > title_length:
            title_str = title_str[:title_length - 3] + "..."
        
        if args.verbose == 0:
            title_length = 35  # Reduced to make room for watch info
            title_str = details['title']
            if len(title_str) > title_length:
                title_str = title_str[:title_length - 3] + "..."
            
            print(f"{original_index:4d}. {status_emoji} {title_str:<{title_length}} {details['episodes_found']:>3}/{details['episodes_expected']:<3} | {episode_info_str}")
        else:
            print(f"{original_index:4d}. {status_emoji} {details['title']}")
            print(f"    Episodes: {details['episodes_found']}/{details['episodes_expected']} ({details['status']})")
            if watched_episodes:
                watched_range = archiver._format_episode_ranges(watched_episodes)
                print(f"    Watched: {watched_range}")
            if missing_episodes:
                missing_range = archiver._format_episode_ranges(missing_episodes)
                print(f"    Missing: {missing_range}")
            if extra_episodes:
                extra_range = archiver._format_episode_ranges(extra_episodes)
                print(f"    Extra: {extra_range}")
            if 'folder_name' in details:
                print(f"    Output folder: {details['folder_name']}")
            if args.verbose > 1:
                print(f"    Key: {group_key}")
            print()
    
    return 0


def cmd_archive(args):
    """Handle the archive command."""
    # Create progress reporter if needed
    progress_reporter = None
    if hasattr(args, 'no_progress') and args.no_progress:
        # No progress reporting requested
        pass
    else:
        # Use CLI progress reporter by default, but disable progress bars during dry-run
        progress_reporter = CLIProgressReporter(
            verbose=args.verbose,
            use_progress_bars=TQDM_AVAILABLE and not args.dry_run
        )
    
    archiver = SeriesArchiver(verbose=args.verbose, progress_reporter=progress_reporter)
    
    if not archiver.load_data(args.input_json):
        return 1
    
    # Parse group selection
    groups = archiver.list_groups()
    if not groups:
        print("No groups available for archiving.")
        return 0
    
    # Handle selection
    if args.select.lower() == 'all':
        selected_groups = [group_key for group_key, _ in groups]
    else:
        try:
            indices = [int(x.strip()) for x in args.select.split(',') if x.strip()]
            selected_groups = []
            for idx in indices:
                if 1 <= idx <= len(groups):
                    selected_groups.append(groups[idx - 1][0])
                else:
                    print(f"Warning: Invalid selection {idx}, skipping.")
        except ValueError:
            print("Error: Invalid selection format. Use comma-separated numbers or 'all'.")
            return 1
    
    if not selected_groups:
        print("No valid groups selected.")
        return 1
    
    # Show what will be processed
    if args.verbose > 0 or args.dry_run:
        action = "copy" if args.copy else "move"
        print(f"Will {action} {len(selected_groups)} series to: {args.destination}")
        for group_key in selected_groups:
            group_data = archiver.get_group_details(group_key)
            if group_data:
                print(f"  - {group_data.get('title', 'Unknown')}")
        print()
    
    # Perform archiving
    action_header = "DRY RUN" if args.dry_run else "ARCHIVING"
    if args.verbose > 0:
        print(f"=== {action_header} ===")
    
    results = archiver.archive_groups(
        selected_groups=selected_groups,
        destination_root=args.destination,
        copy_files=args.copy,
        dry_run=args.dry_run,
        verify_crc=getattr(args, 'verify_crc', False)
    )
    
    if results:
        action_word = "Would complete" if args.dry_run else "Completed"
        print(f"\n{action_word} archiving {len(results)} series.")
        if args.dry_run:
            print("Use without --dry-run to actually perform the operation.")
    else:
        print("No series were processed.")
    
    return 0


def cmd_check_crc(args):
    """Handle the check-crc command."""
    archiver = SeriesArchiver(verbose=args.verbose)
    
    if args.input_json:
        # Check CRC for files in JSON groups
        if not archiver.load_data(args.input_json):
            return 1
        
        # Filter groups if requested
        groups_to_check = archiver.groups
        if hasattr(args, 'select') and args.select:
            if args.select.lower() == 'all':
                pass  # Use all groups
            else:
                try:
                    all_groups = list(archiver.groups.items())
                    indices = [int(x.strip()) for x in args.select.split(',') if x.strip()]
                    selected_group_keys = []
                    for idx in indices:
                        if 1 <= idx <= len(all_groups):
                            selected_group_keys.append(all_groups[idx - 1][0])
                        else:
                            print(f"Warning: Invalid selection {idx}, skipping.")
                    
                    groups_to_check = {k: v for k, v in archiver.groups.items() if k in selected_group_keys}
                except ValueError:
                    print("Error: Invalid selection format. Use comma-separated numbers or 'all'.")
                    return 1
        
        if not groups_to_check:
            print("No groups selected for CRC checking.")
            return 1
        
        print(f"Checking CRC for {len(groups_to_check)} groups...")
        archiver.check_files_crc(groups_to_check, is_groups=True)
    
    elif args.files:
        # Check CRC for individual files
        valid_files = [f for f in args.files if os.path.isfile(f)]
        if not valid_files:
            print("No valid files provided for CRC checking.")
            return 1
        
        print(f"Checking CRC for {len(valid_files)} files...")
        archiver.check_files_crc(valid_files, is_groups=False)
    
    else:
        print("Error: Must provide either --input-json or files for CRC checking.")
        return 1
    
    return 0


def main():
    """Main entry point for command-line interface."""
    parser = argparse.ArgumentParser(
        description="Archive anime series files based on series completeness checker output",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('-v', '--verbose', action='count', default=0,
                       help='Increase verbosity (use -v, -vv, or -vvv)')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands', required=True)
    
    # List command
    list_parser = subparsers.add_parser('list', aliases=['ls'], 
                                       help='List available series groups')
    list_parser.add_argument('input_json', help='JSON file from series_completeness_checker.py')
    list_parser.add_argument('--status-filter', metavar='FILTERS',
                            help='Filter results by status. Use +status to include only specific statuses, '
                                 '-status to exclude specific statuses, or plain status names for exact match. '
                                 'Available statuses: complete, incomplete, complete_with_extras, no_episode_numbers, '
                                 'unknown_total_episodes, not_series, no_metadata, no_metadata_manager, unknown. '
                                 'Examples: "complete incomplete", "+complete +incomplete", "-unknown -no_metadata"')
    list_parser.add_argument('--sort', action='store_true',
                            help='Sort series alphabetically by title')
    
    # Archive command
    archive_parser = subparsers.add_parser('archive', 
                                          help='Archive selected series groups')
    archive_parser.add_argument('input_json', help='JSON file from series_completeness_checker.py')
    archive_parser.add_argument('destination', 
                               help='Destination root directory for archived series')
    archive_parser.add_argument('--select', type=str, required=True,
                               help='Comma-separated list of group numbers or "all" (e.g., "1,3,5" or "all")')
    archive_parser.add_argument('--copy', action='store_true',
                               help='Copy files instead of moving them')
    archive_parser.add_argument('--dry-run', action='store_true',
                               help='Show what would be done without actually doing it')
    archive_parser.add_argument('--no-progress', action='store_true',
                               help='Disable progress bars and use simple text output')
    archive_parser.add_argument('--verify-crc', action='store_true',
                               help='Verify CRC32 of files after archiving (if CRC is present in filename)')
    
    # Check CRC command
    crc_parser = subparsers.add_parser('check-crc', aliases=['crc'],
                                      help='Check CRC32 of files')
    crc_group = crc_parser.add_mutually_exclusive_group(required=True)
    crc_group.add_argument('--input-json', metavar='FILE',
                          help='JSON file from series_completeness_checker.py')
    crc_group.add_argument('--files', nargs='+', metavar='FILE',
                          help='Individual files to check')
    crc_parser.add_argument('--select', type=str,
                           help='For JSON input: comma-separated list of group numbers or "all" (e.g., "1,3,5" or "all")')
    
    args = parser.parse_args()
    
    # Validate input file for commands that need it
    if hasattr(args, 'input_json') and args.input_json and not Path(args.input_json).exists():
        print(f"Error: Input file '{args.input_json}' not found.")
        return 1
    
    # Handle commands
    if args.command in ['list', 'ls']:
        return cmd_list(args)
    elif args.command == 'archive':
        return cmd_archive(args)
    elif args.command in ['check-crc', 'crc']:
        return cmd_check_crc(args)
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())
