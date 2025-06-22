import argparse
import json
import os
import shutil
import sys
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
    
    def archive_groups(self, selected_groups: List[str], destination_root: str, 
                      copy_files: bool = False, dry_run: bool = False) -> Dict[str, str]:
        """
        Archive selected groups to destination folders.
        
        Args:
            selected_groups: List of group keys to archive
            destination_root: Root directory for output folders
            copy_files: If True, copy files instead of moving them
            dry_run: If True, show what would be done without actually doing it
            
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
        
        return results
    
    def get_summary(self) -> Dict:
        """Get summary statistics about loaded data."""
        if not self.data:
            return {}
        
        completeness = self.data.get('completeness_summary', {})
        return {
            'total_series': completeness.get('total_series', 0),
            'complete_series': completeness.get('complete_series', 0),
            'incomplete_series': completeness.get('incomplete_series', 0),
            'total_episodes_found': completeness.get('total_episodes_found', 0),
            'total_episodes_expected': completeness.get('total_episodes_expected', 0)
        }


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
        print()
    
    # List groups
    groups = archiver.list_groups(show_details=args.verbose > 0)
    if not groups:
        print("No groups found in the data.")
        return 0
    
    print("Available series groups:")
    if args.verbose == 0:
        print("=" * 50)
    else:
        print("=" * 100)
    
    for i, (group_key, details) in enumerate(groups, 1):
        status_indicator = "✓" if details['status'] == 'complete' else "⚠" if details['status'] == 'incomplete' else "?"
        
        if args.verbose == 0:
            print(f"{i:2d}. {status_indicator} {details['title']} ({details['episodes_found']}/{details['episodes_expected']})")
        else:
            print(f"{i:2d}. {status_indicator} {details['title']}")
            print(f"    Episodes: {details['episodes_found']}/{details['episodes_expected']} ({details['status']})")
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
        dry_run=args.dry_run
    )
    
    if results:
        action_word = "Would complete" if args.dry_run else "Completed"
        print(f"\n{action_word} archiving {len(results)} series.")
        if args.dry_run:
            print("Use without --dry-run to actually perform the operation.")
    else:
        print("No series were processed.")
    
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
    
    args = parser.parse_args()
    
    # Validate input file
    if not Path(args.input_json).exists():
        print(f"Error: Input file '{args.input_json}' not found.")
        return 1
    
    # Handle commands
    if args.command in ['list', 'ls']:
        return cmd_list(args)
    elif args.command == 'archive':
        return cmd_archive(args)
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())
