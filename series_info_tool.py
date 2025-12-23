from pathlib import Path
from typing import Dict, List, Any, Optional, Set
import argparse
import sys
import webbrowser
import subprocess
import logging


class LogOutputTracker(logging.Handler):
    """Custom logging handler to track if any log output was produced."""
    
    def __init__(self) -> None:
        super().__init__()
        self.output_produced = False
    
    def emit(self, record: logging.LogRecord) -> None:
        """Mark that output was produced when a log record is emitted."""
        self.output_produced = True


# Import dependencies from the reference codebase
try:
    from file_grouper import FileGrouper, get_metadata_manager, get_plex_provider
except ImportError as e:
    # Early logging setup for critical errors
    logging.basicConfig(level=logging.ERROR, format='%(levelname)s: %(message)s')
    logging.error(f"Required modules not found: {e}")
    logging.error("Make sure file_grouper.py is available in the same directory.")
    sys.exit(1)


class SeriesInfoTool:
    """Tool to extract and display series information for video files."""
    
    def __init__(
        self, 
        metadata_manager: Optional[Any] = None, 
        plex_provider: Optional[Any] = None,
        myanimelist_xml_path: Optional[str] = None
    ) -> None:
        """Initialize the series info tool.
        
        Args:
            metadata_manager: Metadata manager instance for fetching show metadata
            plex_provider: Plex provider for watch status information
            myanimelist_xml_path: Path to MyAnimeList XML file for watch status
        """
        self.file_grouper = FileGrouper(
            metadata_manager, 
            plex_provider, 
            myanimelist_xml_path
        )
        self.metadata_manager = metadata_manager
        self.plex_provider = plex_provider
        self.myanimelist_xml_path = myanimelist_xml_path
    
    def get_show_info(self, files: List[Path]) -> Dict[str, Any]:
        """Get show information for the provided files, grouped by show.
        
        Args:
            files: List of video file paths to analyze
            
        Returns:
            Dictionary mapping group keys to show information including metadata
        """
        logging.info(f"Analyzing {len(files)} file(s)")
        
        # Group files by title to consolidate shows
        groups = self.file_grouper.group_files(
            files, 
            ['title'], 
            show_progress=False
        )
        
        logging.debug(f"Grouped files into {len(groups)} show(s)")
        
        show_info = {}
        for group_key, group_files in groups.items():
            if not group_files:
                continue
            
            first_file = group_files[0]
            metadata_id = first_file.get('metadata_id')
            title = first_file.get('title', 'Unknown')
            
            # Get metadata from FileGrouper's title_metadata cache
            metadata = {}
            myanimelist_watch_status = None
            
            if metadata_id and metadata_id in self.file_grouper.title_metadata:
                title_data = self.file_grouper.title_metadata[metadata_id]
                metadata = title_data.get('metadata', {})
                myanimelist_watch_status = title_data.get('myanimelist_watch_status')
            
            show_info[group_key] = {
                'title': title,
                'metadata': metadata,
                'myanimelist_watch_status': myanimelist_watch_status,
                'files': group_files,
                'metadata_id': metadata_id
            }
        
        return show_info
    
    def extract_urls(self, show_info: Dict[str, Any]) -> List[str]:
        """Extract MyAnimeList URLs from show information.
        
        Args:
            show_info: Dictionary of show information
            
        Returns:
            List of unique MyAnimeList URLs
        """
        urls: Set[str] = set()
        
        for info in show_info.values():
            metadata = info.get('metadata', {})
            
            # Try to get MyAnimeList URL from metadata
            mal_url = metadata.get('myanimelist_url')
            if mal_url:
                logging.debug(f"Found MAL URL in metadata: {mal_url}")
                urls.add(mal_url)
            
            # Also check watch status for URL
            mal_watch_status = info.get('myanimelist_watch_status')
            if mal_watch_status and isinstance(mal_watch_status, dict):
                series_url = mal_watch_status.get('series_url')
                if series_url:
                    logging.debug(f"Found MAL URL in watch status: {series_url}")
                    urls.add(series_url)
        
        logging.info(f"Extracted {len(urls)} unique URL(s)")
        return sorted(urls)
    
    def display_info(self, show_info: Dict[str, Any], extended_metadata: bool = False) -> None:
        """Display show information on stdout.
        
        Args:
            show_info: Dictionary of show information to display
            extended_metadata: Whether to show extended metadata fields
        """
        logging.info(f"Displaying information for {len(show_info)} show(s)")
        
        for group_key, info in show_info.items():
            title = info['title']
            metadata = info.get('metadata', {})
            mal_watch_status = info.get('myanimelist_watch_status')
            
            print(f"\n{'=' * 70}")
            print(f"  {title}")
            print(f"{'=' * 70}")
            
            # Display general metadata fields (ordered by importance)
            metadata_fields = [
                ('Type', 'type'),
                ('Year', 'year'),
                ('Status', 'status'),
                ('Rating', 'rating'),
                ('Runtime', 'runtime'),
                ('Genres', 'genres'),
                ('Director', 'director'),
                ('Writers', 'writers'),
                ('Actors', 'actors'),
                ('Country', 'country'),
                ('Language', 'language'),
                ('Awards', 'awards'),
                ('Plot', 'plot'),
            ]
            
            general_info_found = False
            for display_name, field_name in metadata_fields:
                if field_name in metadata and metadata[field_name]:
                    general_info_found = True
                    value = metadata[field_name]
                    if isinstance(value, list):
                        value = ', '.join(str(v) for v in value)
                    elif isinstance(value, str) and len(value) > 100:
                        value = value[:97] + '...'
                    print(f"{display_name}: {value}")
            
            # Display additional metadata that wasn't in the standard fields
            if metadata:
                other_fields = []
                excluded_prefixes = ('myanimelist_', 'imdb_', 'tmdb_')
                excluded_fields = {'title', 'sources', 'tags'}  # Exclude title, handle sources/tags separately
                standard_fields = {f[1] for f in metadata_fields}
                
                for key, value in metadata.items():
                    if (key not in standard_fields and 
                        key not in excluded_fields and
                        not any(key.startswith(prefix) for prefix in excluded_prefixes) and
                        value and not key.startswith('_')):
                        other_fields.append((key, value))
                
                if other_fields:
                    if general_info_found:
                        print()  # Add spacing
                    for key, value in other_fields:
                        display_key = key.replace('_', ' ').title()
                        if isinstance(value, list):
                            value = ', '.join(str(v) for v in value)
                        elif isinstance(value, str) and len(value) > 100:
                            value = value[:97] + '...'
                        print(f"{display_key}: {value}")
            
            # Display tags (shortened unless extended mode)
            if 'tags' in metadata and metadata['tags']:
                tags = metadata['tags']
                if isinstance(tags, list):
                    tags_str = ', '.join(str(t) for t in tags)
                else:
                    tags_str = str(tags)
                
                if not extended_metadata and len(tags_str) > 80:
                    tags_str = tags_str[:77] + '...'
                
                print(f"Tags: {tags_str}")
            
            # Display MyAnimeList specific metadata
            print(f"\n--- MyAnimeList Information ---")
            
            mal_metadata_fields = [
                ('URL', 'myanimelist_url'),
                ('MAL ID', 'myanimelist_id'),
                ('Score', 'myanimelist_score'),
                ('Rank', 'myanimelist_rank'),
                ('Popularity', 'myanimelist_popularity'),
                ('Members', 'myanimelist_members'),
                ('Favorites', 'myanimelist_favorites'),
                ('Status', 'myanimelist_status'),
                ('Type', 'myanimelist_type'),
                ('Episodes', 'myanimelist_episodes'),
                ('Aired', 'myanimelist_aired'),
                ('Premiered', 'myanimelist_premiered'),
                ('Broadcast', 'myanimelist_broadcast'),
                ('Producers', 'myanimelist_producers'),
                ('Licensors', 'myanimelist_licensors'),
                ('Studios', 'myanimelist_studios'),
                ('Source', 'myanimelist_source'),
                ('Duration', 'myanimelist_duration'),
                ('Rating', 'myanimelist_rating'),
                ('Genres', 'myanimelist_genres'),
                ('Themes', 'myanimelist_themes'),
                ('Demographics', 'myanimelist_demographics'),
                ('Synopsis', 'myanimelist_synopsis'),
            ]
            
            mal_info_found = False
            for display_name, field_name in mal_metadata_fields:
                if field_name in metadata and metadata[field_name]:
                    mal_info_found = True
                    value = metadata[field_name]
                    if isinstance(value, list):
                        value = ', '.join(str(v) for v in value)
                    elif isinstance(value, str) and len(value) > 150:
                        value = value[:147] + '...'
                    print(f"{display_name}: {value}")
            
            # Display watch status from MyAnimeList
            if mal_watch_status and isinstance(mal_watch_status, dict):
                mal_info_found = True
                print(f"\n--- MyAnimeList Watch Status ---")
                
                watch_status_fields = [
                    ('My Status', 'my_status'),
                    ('My Score', 'my_score'),
                    ('My Watched Episodes', 'my_watched_episodes'),
                    ('My Start Date', 'my_start_date'),
                    ('My Finish Date', 'my_finish_date'),
                    ('Series Episodes', 'series_episodes'),
                    ('Series URL', 'series_url'),
                ]
                
                for display_name, field_name in watch_status_fields:
                    if field_name in mal_watch_status and mal_watch_status[field_name]:
                        value = mal_watch_status[field_name]
                        print(f"{display_name}: {value}")
            
            if not mal_info_found:
                print("(No MyAnimeList information available)")
            
            # Display IMDb specific metadata
            imdb_metadata_fields = [
                ('IMDb ID', 'imdb_id'),
                ('IMDb Rating', 'imdb_rating'),
                ('IMDb Votes', 'imdb_votes'),
                ('Metascore', 'metascore'),
            ]
            
            imdb_info_found = False
            for display_name, field_name in imdb_metadata_fields:
                if field_name in metadata and metadata[field_name]:
                    if not imdb_info_found:
                        print(f"\n--- IMDb Information ---")
                        imdb_info_found = True
                    value = metadata[field_name]
                    print(f"{display_name}: {value}")
            
            # Display source URLs (only MyAnimeList unless extended mode)
            sources = metadata.get('sources', [])
            if sources:
                if extended_metadata:
                    print(f"\n--- Source URLs ---")
                    for source_url in sources:
                        print(f"  • {source_url}")
                else:
                    # Only show MyAnimeList URL
                    mal_sources = [s for s in sources if 'myanimelist' in s.lower()]
                    if mal_sources:
                        print(f"\n--- Source URLs ---")
                        for source_url in mal_sources:
                            print(f"  • {source_url}")
            
            # Show file count and sample files
            file_count = len(info['files'])
            print(f"\n--- Files ({file_count}) ---")
            
            # Show up to 5 sample files
            for i, file_info in enumerate(info['files'][:5]):
                # Get path from the file_info dict - could be under 'path', 'filepath', or in metadata
                file_path = file_info.get('path') or file_info.get('filepath')
                if file_path:
                    if isinstance(file_path, Path):
                        file_path = file_path.name
                    elif isinstance(file_path, str):
                        file_path = Path(file_path).name
                    print(f"  • {file_path}")
                else:
                    # Fallback to filename if available
                    filename = file_info.get('filename', 'Unknown')
                    print(f"  • {filename}")
            
            if file_count > 5:
                print(f"  ... and {file_count - 5} more")
    
    def copy_urls_to_clipboard(self, urls: List[str]) -> None:
        """Copy URLs to clipboard using Windows clip.exe.
        
        Args:
            urls: List of URLs to copy
        """
        if not urls:
            print("No URLs found to copy")
            return
        
        url_text = '\n'.join(urls)
        
        # Windows clipboard using clip.exe
        try:
            process = subprocess.Popen(
                ['clip'], 
                stdin=subprocess.PIPE, 
                shell=True
            )
            process.communicate(url_text.encode('utf-16'))
            logging.info(f"Successfully copied {len(urls)} URL(s) to clipboard")
            print(f"✓ Copied {len(urls)} URL(s) to clipboard:")
            for url in urls:
                print(f"  • {url}")
        except Exception as e:
            logging.error(f"Error copying to clipboard: {e}")
            sys.exit(1)
    
    def open_urls_in_browser(self, urls: List[str]) -> None:
        """Open URLs in the default browser.
        
        Args:
            urls: List of URLs to open
        """
        if not urls:
            logging.warning("No URLs found to open")
            return
        
        logging.info(f"Opening {len(urls)} URL(s) in browser")
        print(f"Opening {len(urls)} URL(s) in browser:")
        for url in urls:
            print(f"  • {url}")
            try:
                webbrowser.open(url)
                logging.debug(f"Opened URL: {url}")
            except Exception as e:
                logging.error(f"Error opening {url}: {e}")
        
        print(f"\n✓ Opened {len(urls)} URL(s) in default browser")


def main() -> None:
    """Command-line interface for series info tool."""
    parser = argparse.ArgumentParser(
        description='Get series information and MyAnimeList URLs for video files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Display information about files
  %(prog)s file1.mkv file2.mkv
  
  # Copy MyAnimeList URLs to clipboard
  %(prog)s --copy file1.mkv file2.mkv
  
  # Open MyAnimeList URLs in browser
  %(prog)s --open file1.mkv file2.mkv
  
  # Use with MyAnimeList XML for watch status
  %(prog)s --mal-xml animelist.xml file1.mkv file2.mkv

This tool is designed for Windows shell:sendto and drag-drop operations.
        """
    )
    
    parser.add_argument(
        'files', 
        nargs='+', 
        type=Path,
        help='Video files to get information for'
    )
    parser.add_argument(
        '--copy', '-c', 
        action='store_true',
        help='Copy MyAnimeList URLs to clipboard (Windows only)'
    )
    parser.add_argument(
        '--open', '-o', 
        action='store_true',
        help='Open MyAnimeList URLs in default browser'
    )
    parser.add_argument(
        '--mal-xml', 
        metavar='PATH_OR_URL',
        help='Path to MyAnimeList XML file (can be .gz) or URL for watch status lookup'
    )
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='WARNING',
        help='Set logging level (default: WARNING)'
    )
    parser.add_argument(
        '--extended-metadata',
        action='store_true',
        help='Show extended metadata fields (title, full tags, all sources, etc.)'
    )
    
    args = parser.parse_args()
    
    # Configure logging with output tracker
    log_tracker = LogOutputTracker()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(levelname)s: %(message)s',
        handlers=[
            logging.StreamHandler(),
            log_tracker
        ]
    )
    
    # Validate files exist
    valid_files: List[Path] = []
    for file_path in args.files:
        if file_path.exists() and file_path.is_file():
            valid_files.append(file_path)
            logging.debug(f"Valid file: {file_path}")
        else:
            logging.warning(f"{file_path} does not exist or is not a file")
    
    if not valid_files:
        logging.error("No valid files provided")
        sys.exit(1)
    
    # Get metadata manager and plex provider
    try:
        metadata_manager = get_metadata_manager()
        logging.debug("Metadata manager initialized successfully")
    except Exception as e:
        logging.warning(f"Could not initialize metadata manager: {e}")
        metadata_manager = None
    
    try:
        plex_provider = get_plex_provider()
        logging.debug("Plex provider initialized successfully")
    except Exception as e:
        logging.warning(f"Could not initialize Plex provider: {e}")
        plex_provider = None
    
    # Create tool instance
    tool = SeriesInfoTool(
        metadata_manager, 
        plex_provider,
        args.mal_xml if hasattr(args, 'mal_xml') else None
    )
    
    # Get show info
    logging.info("Retrieving show information...")
    show_info = tool.get_show_info(valid_files)
    
    if not show_info:
        logging.error("No show information found for the provided files")
        sys.exit(1)
    
    # Execute requested action
    if args.copy:
        logging.info("Executing copy URLs action")
        urls = tool.extract_urls(show_info)
        tool.copy_urls_to_clipboard(urls)
    elif args.open:
        logging.info("Executing open URLs action")
        urls = tool.extract_urls(show_info)
        tool.open_urls_in_browser(urls)
    else:
        # Default: display info
        logging.info("Executing display info action")
        tool.display_info(show_info, args.extended_metadata)
        
        # Keep window open until user exits (useful for drag-drop on Windows)
        print("\n" + "=" * 70)
        input("Press Enter to exit...")
        return
    
    # Keep window open if any log output was produced
    if log_tracker.output_produced:
        print("\n" + "=" * 70)
        input("Press Enter to exit...")


if __name__ == '__main__':
    main()
