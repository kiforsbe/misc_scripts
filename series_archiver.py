import argparse
import json
import os
import shutil
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple


class SeriesArchiver:
    """
    A class for archiving anime series files based on series completeness checker output.
    Organizes files into folders following the pattern:
    [release_group] show_name (start_ep-last_ep) (resolution)
    """
    
    def __init__(self, verbose: int = 0):
        self.data: Optional[Dict] = None
        self.groups: Dict = {}
        self.verbose = verbose
        
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
            print(f"\n{action_word} group: {group_data.get('title')}")
            self._log(f"Destination folder: {dest_folder}")
            
            # Process files
            files = group_data.get('files', [])
            success_count = 0
            error_count = 0
            
            for file_info in files:
                source_path = file_info.get('filepath')
                if not source_path or not os.path.exists(source_path):
                    self._log(f"  Warning: Source file not found: {source_path}")
                    error_count += 1
                    continue
                
                filename = file_info.get('filename', os.path.basename(source_path))
                dest_path = os.path.join(dest_folder, filename)
                
                try:
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
                    print(f"  Error processing {filename}: {e}")
                    error_count += 1
            
            status_word = "Would process" if dry_run else "Processed"
            print(f"  {status_word} {success_count} files successfully")
            if error_count > 0:
                print(f"  {error_count} files had errors")
            
            results[group_key] = dest_folder
        
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
    archiver = SeriesArchiver(verbose=args.verbose)
    
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
        formatter_class=argparse.RawDescriptionHelpFormatter,
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
