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

try:
    # Load this library from subfolder video-optimizer-v2
    sys.path.append(os.path.join(os.path.dirname(__file__), 'video-optimizer-v2'))
    from metadata_provider import MetadataManager, BaseMetadataProvider, TitleInfo
    from anime_metadata import AnimeDataProvider
    from imdb_metadata import IMDbDataProvider
    
    # Initialize metadata manager as a global variable
    METADATA_MANAGER = None

    def get_metadata_manager():
        """Get or initialize the metadata manager"""
        global METADATA_MANAGER
        if (METADATA_MANAGER is None):
            # Initialize providers
            anime_provider = AnimeDataProvider()
            imdb_provider = IMDbDataProvider()
            METADATA_MANAGER = MetadataManager([anime_provider, ]) #imdb_provider
        return METADATA_MANAGER
except ImportError:
    print("Warning: metadata_provider not found. Enhanced metadata features will be disabled.")
    MetadataManager = None
    BaseMetadataProvider = None
    TitleInfo = None

class FileGrouper:
    """Groups files based on filename metadata extracted using guessit."""
    
    def __init__(self, metadata_manager = None):
        self.groups = defaultdict(list)
        self.metadata = {}
        self.enhanced_metadata = {}  # Store metadata from providers
        self.group_metadata = {}     # Store metadata for groups
        self.metadata_manager = metadata_manager
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
        except ValueError:            return False
    
    def extract_metadata(self, file_path: Path) -> Dict[str, Any]:
        """Extract metadata from filename using guessit."""
        try:
            metadata = guessit(file_path.name)
            # Convert guessit result to regular dict and add file info
            result = dict(metadata)
            result['filepath'] = str(file_path)
            result['filename'] = file_path.name
            result['file_size'] = file_path.stat().st_size if file_path.exists() else 0
            
            # Add enhanced metadata if available
            if self.metadata_manager and MetadataManager:
                title = result.get('title')
                year = result.get('year')
                if title:
                    try:
                        enhanced_info, provider = self.metadata_manager.find_title(title, year)
                        if enhanced_info:
                            enhanced_key = str(file_path)
                            self.enhanced_metadata[enhanced_key] = self._serialize_title_info(enhanced_info)
                            
                            # Add episode info if it's a TV show
                            if enhanced_info.type in ['tv', 'anime_series']:
                                season = result.get('season')
                                episode = result.get('episode')
                                if season and episode and provider:
                                    episode_info = self.metadata_manager.get_episode_info(provider, enhanced_info.id, season, episode)
                                    if episode_info:
                                        self.enhanced_metadata[enhanced_key]['episode_info'] = self._serialize_episode_info(episode_info)
                    except Exception as metadata_error:
                        print(f"Warning: Enhanced metadata lookup failed for {file_path.name}: {metadata_error}")
            
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
        self.group_metadata.clear()
        
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
        
        # Get group metadata after all files are processed
        for group_key, group_files in self.groups.items():
            self.group_metadata[group_key] = self._get_group_metadata(group_files)
        
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
            'metadata': self.metadata,
            'enhanced_metadata': self.enhanced_metadata,
            'group_metadata': self.group_metadata
        }
        
        if include_summary:
            export_data['summary'] = self.get_summary()
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False)
            print(f"Exported data to: {output_path}")
    
    def _serialize_title_info(self, title_info) -> Dict[str, Any]:
        """Convert TitleInfo object to serializable dict"""
        if not title_info:
            return {}
        return {
            'id': title_info.id,
            'title': title_info.title,
            'type': title_info.type,
            'year': title_info.year,
            'start_year': title_info.start_year,
            'end_year': title_info.end_year,
            'rating': title_info.rating,
            'votes': title_info.votes,
            'genres': title_info.genres,
            'tags': title_info.tags,
            'status': title_info.status,
            'total_episodes': title_info.total_episodes,
            'total_seasons': title_info.total_seasons,
            'sources': title_info.sources,
            'plot': title_info.plot
        }
    
    def _serialize_episode_info(self, episode_info) -> Dict[str, Any]:
        """Convert EpisodeInfo object to serializable dict"""
        if not episode_info:
            return {}
        return {
            'title': episode_info.title,
            'season': episode_info.season,
            'episode': episode_info.episode,
            'parent_id': episode_info.parent_id,
            'year': episode_info.year,
            'rating': episode_info.rating,
            'votes': episode_info.votes,
            'plot': episode_info.plot,
            'air_date': episode_info.air_date
        }
    
    def _get_group_metadata(self, group_files: List[Dict]) -> Dict[str, Any]:
        """Get metadata for a group based on the first file's metadata"""
        if not group_files or not self.metadata_manager:
            return {}
            
        # Use the first file to get group metadata
        first_file = group_files[0]
        title = first_file.get('title')
        year = first_file.get('year')
        
        if title:
            enhanced_info, provider = self.metadata_manager.find_title(title, year)
            if enhanced_info:
                return self._serialize_title_info(enhanced_info)
        
        return {}

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
    grouper = FileGrouper(get_metadata_manager() if MetadataManager else None)
    
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
                                        
                    # Convert non-serializable objects to strings
                    for key, value in metadata_copy.items():
                        if hasattr(value, '__str__') and not isinstance(value, (str, int, float, bool, list, dict, type(None))):
                            metadata_copy[key] = str(value)
                        elif isinstance(value, list):
                            metadata_copy[key] = [str(item) if hasattr(item, '__str__') and not isinstance(item, (str, int, float, bool, dict, type(None))) else item for item in value]
                    
                    print(f"    {json.dumps(metadata_copy, separators=(',', ':'), ensure_ascii=False)}")
                    
                    # Show enhanced metadata if available
                    enhanced_key = file_info.get('filepath')
                    if enhanced_key and enhanced_key in grouper.enhanced_metadata:
                        enhanced_data = grouper.enhanced_metadata[enhanced_key]
                        print(f"    {json.dumps(enhanced_data, separators=(',', ':'), ensure_ascii=False)}")
    
    # Export if requested
    if args.export:
        grouper.export_to_json(args.export)
        if verbosity >= 1:
            print(f"Exported data to: {args.export}")


if __name__ == '__main__':
    main()
