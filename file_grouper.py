import argparse
import json
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Set, Any, Optional
import fnmatch

try:
    from guessit import guessit
except ImportError:
    print("Error: guessit library not found. Install with: pip install guessit")
    sys.exit(1)


class FileGrouper:
    """Groups files based on filename metadata extracted using guessit."""
    
    def __init__(self):
        self.groups = defaultdict(list)
        self.metadata = {}
        self.file_extensions = {'.mkv', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', 
                               '.m4v', '.mpg', '.mpeg', '.3gp', '.ogv', '.ts', '.m2ts'}
    
    def discover_files(self, input_paths: List[str], excluded_paths: List[str] | None = None,
                      include_patterns: List[str] | None = None, exclude_patterns: List[str] | None = None,
                      recursive: bool = False) -> List[Path]:
        """Discover files based on input paths and filtering criteria."""
        excluded_paths = excluded_paths or []
        include_patterns = include_patterns or ['*']
        exclude_patterns = exclude_patterns or []
        
        discovered_files = []
        excluded_path_objects = [Path(p).resolve() for p in excluded_paths]
        
        for input_path in input_paths:
            path_obj = Path(input_path)
            if not path_obj.exists():
                print(f"Warning: Path does not exist: {input_path}")
                continue
                
            if path_obj.is_file():
                discovered_files.append(path_obj)
            else:
                # Find files based on recursion setting
                if recursive:
                    file_pattern = path_obj.rglob('*')
                else:
                    file_pattern = path_obj.glob('*')
                
                for file_path in file_pattern:
                    if file_path.is_file():
                        # Check if file is in excluded paths
                        if any(self._is_path_excluded(file_path, exc_path) for exc_path in excluded_path_objects):
                            continue
                        discovered_files.append(file_path)
        
        # Apply include/exclude patterns
        filtered_files = []
        for file_path in discovered_files:
            filename = file_path.name
            
            # Check include patterns
            if not any(fnmatch.fnmatch(filename, pattern) for pattern in include_patterns):
                continue
                
            # Check exclude patterns
            if any(fnmatch.fnmatch(filename, pattern) for pattern in exclude_patterns):
                continue
                
            filtered_files.append(file_path)
        
        return filtered_files
    
    def _is_path_excluded(self, file_path: Path, excluded_path: Path) -> bool:
        """Check if file_path is within excluded_path."""
        try:
            file_path.resolve().relative_to(excluded_path)
            return True
        except ValueError:
            return False
    
    def extract_metadata(self, file_path: Path) -> Dict[str, Any]:
        """Extract metadata from filename using guessit."""
        try:
            metadata = guessit(file_path.name)
            # Convert guessit result to regular dict and add file info
            result = dict(metadata)
            result['filepath'] = str(file_path)
            result['filename'] = file_path.name
            result['file_size'] = file_path.stat().st_size if file_path.exists() else 0
            return result
        except Exception as e:
            print(f"Warning: Could not extract metadata from {file_path.name}: {e}")
            return {
                'filepath': str(file_path),
                'filename': file_path.name,
                'file_size': file_path.stat().st_size if file_path.exists() else 0
            }
    
    def group_files(self, files: List[Path], group_by: List[str] | None = None) -> Dict[str, List[Dict]]:
        """Group files based on specified metadata fields."""
        group_by = group_by or ['title', 'year']
        
        self.groups.clear()
        self.metadata.clear()
        
        for file_path in files:
            metadata = self.extract_metadata(file_path)
            self.metadata[str(file_path)] = metadata
            
            # Create group key based on specified fields
            group_key_parts = []
            for field in group_by:
                value = metadata.get(field, 'Unknown')
                if isinstance(value, list):
                    value = ', '.join(str(v) for v in value)
                group_key_parts.append(f"{field}:{value}")
            
            group_key = ' | '.join(group_key_parts)
            self.groups[group_key].append(metadata)
        
        return dict(self.groups)
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary statistics of grouped files."""
        total_files = sum(len(files) for files in self.groups.values())
        total_size = sum(
            sum(file_info.get('file_size', 0) for file_info in files)
            for files in self.groups.values()
        )
        
        return {
            'total_files': total_files,
            'total_groups': len(self.groups),
            'total_size_bytes': total_size,
            'total_size_mb': round(total_size / (1024 * 1024), 2),
            'groups': {
                group_name: {
                    'file_count': len(files),
                    'total_size_bytes': sum(f.get('file_size', 0) for f in files)
                }
                for group_name, files in self.groups.items()
            }
        }
    
    def export_to_json(self, output_path: str, include_summary: bool = True) -> None:
        """Export grouped data to JSON file."""
        export_data = {
            'groups': dict(self.groups),
            'metadata': self.metadata
        }
        
        if include_summary:
            export_data['summary'] = self.get_summary()
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False)
        
        print(f"Exported data to: {output_path}")


def main():
    """Command-line interface."""
    parser = argparse.ArgumentParser(
        description='Group files based on filename metadata using guessit',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s /path/to/movies
  %(prog)s /path/to/media --exclude-paths /path/to/media/trash
  %(prog)s /path/to/files --include-patterns "*.mkv" "*.mp4" --exclude-patterns "*sample*"
  %(prog)s /path/to/files --group-by title year season --export metadata.json
  %(prog)s /path/to/files --recursive -v 2 --export metadata.json
        """
    )
    
    parser.add_argument('input_paths', nargs='+', 
                       help='Input paths to search for files')
    parser.add_argument('--exclude-paths', nargs='*', default=[],
                       help='Paths to exclude from search')
    parser.add_argument('--include-patterns', nargs='*', default=['*'],
                       help='Wildcard patterns for files to include (default: *)')
    parser.add_argument('--exclude-patterns', nargs='*', default=[],
                       help='Wildcard patterns for files to exclude')
    parser.add_argument('--group-by', nargs='*', default=['title', 'year'],
                       help='Metadata fields to group by (default: title year)')
    parser.add_argument('--export', metavar='FILE',
                       help='Export results to JSON file')
    parser.add_argument('--recursive', '-r', action='store_true',
                       help='Recursively search subdirectories (default: False)')
    parser.add_argument('--verbose', '-v', type=int, choices=[0, 1, 2], default=1,
                       help='Verbosity level: 0=silent, 1=normal, 2=detailed with metadata (default: 1)')
    parser.add_argument('--quiet', '-q', action='store_true',
                       help='Same as --verbose 0')
    
    args = parser.parse_args()
    
    # Handle quiet flag
    if args.quiet:
        verbosity = 0
    else:
        verbosity = args.verbose
    
    # Create file grouper instance
    grouper = FileGrouper()
    
    # Discover files
    if verbosity >= 1:
        print("Discovering files...")
    
    files = grouper.discover_files(
        args.input_paths,
        args.exclude_paths,
        args.include_patterns,
        args.exclude_patterns,
        args.recursive
    )
    
    if not files:
        if verbosity >= 1:
            print("No files found matching criteria.")
        return
    
    if verbosity >= 1:
        print(f"Found {len(files)} files")
        print("Extracting metadata and grouping...")
    
    # Group files
    groups = grouper.group_files(files, args.group_by)
    
    # Display results
    if verbosity >= 1:
        summary = grouper.get_summary()
        print(f"\nSummary:")
        print(f"Total files: {summary['total_files']}")
        print(f"Total groups: {summary['total_groups']}")
        print(f"Total size: {summary['total_size_mb']} MB")
        
        print(f"\nGroups:")
        for group_name, group_files in groups.items():
            print(f"\n{group_name} ({len(group_files)} files):")
            for file_info in group_files:
                size_mb = file_info.get('file_size', 0) / (1024 * 1024)
                print(f"  - {file_info['filename']} ({size_mb:.1f} MB)")
                
                # Level 2 verbosity: show metadata as compact JSON
                if verbosity >= 2:
                    # Create a copy without filepath for cleaner output
                    metadata_copy = file_info.copy()
                    metadata_copy.pop('filepath', None)
                    metadata_copy.pop('filename', None)  # Already shown above
                    print(f"    {json.dumps(metadata_copy, separators=(',', ':'), ensure_ascii=False)}")
    
    # Export if requested
    if args.export:
        grouper.export_to_json(args.export)
        if verbosity >= 1:
            print(f"Exported data to: {args.export}")


if __name__ == '__main__':
    main()
