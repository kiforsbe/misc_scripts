from pathlib import Path
from typing import Dict, List, Any, Optional, Set
import argparse
import sys
import webbrowser
import subprocess
import logging
import json


class Colors:
    """ANSI color codes for terminal output."""
    RESET = '\033[0m'
    BOLD = '\033[1m'
    DIM = '\033[2m'
    
    # Foreground colors
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'
    GRAY = '\033[90m'


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
            
            # Check sources list for MyAnimeList URLs
            sources = metadata.get('sources', [])
            if sources:
                for source_url in sources:
                    if 'myanimelist' in str(source_url).lower():
                        logging.debug(f"Found MAL URL in sources: {source_url}")
                        urls.add(source_url)
            
            # Also check watch status for URL
            mal_watch_status = info.get('myanimelist_watch_status')
            if mal_watch_status and isinstance(mal_watch_status, dict):
                series_url = mal_watch_status.get('series_url')
                if series_url:
                    logging.debug(f"Found MAL URL in watch status: {series_url}")
                    urls.add(series_url)
        
        logging.info(f"Extracted {len(urls)} unique URL(s)")
        return sorted(urls)
    
    def display_info(self, show_info: Dict[str, Any], extended_metadata: bool = False, format_type: str = 'default') -> None:
        """Display show information on stdout.
        
        Args:
            show_info: Dictionary of show information to display
            extended_metadata: Whether to show extended metadata fields
            format_type: Output format (default, aligned, color, json)
        """
        logging.info(f"Displaying information for {len(show_info)} show(s)")
        
        # JSON format
        if format_type == 'json':
            self._display_json(show_info, extended_metadata)
            return
        
        # Text-based formats
        use_colors = (format_type == 'color')
        use_alignment = (format_type in ['aligned', 'color'])
        
        for group_key, info in show_info.items():
            title = info['title']
            metadata = info.get('metadata', {})
            mal_watch_status = info.get('myanimelist_watch_status')
            
            # Header
            if use_colors:
                print(f"\n{Colors.CYAN}{Colors.BOLD}{'=' * 70}{Colors.RESET}")
                print(f"{Colors.CYAN}{Colors.BOLD}  {title}{Colors.RESET}")
                print(f"{Colors.CYAN}{Colors.BOLD}{'=' * 70}{Colors.RESET}")
            else:
                print(f"\n{'=' * 70}")
                print(f"  {title}")
                print(f"{'=' * 70}")
            
            # Display general metadata fields
            metadata_fields = [
                ('Type', 'type'),
                ('Year', 'year'),
                ('Status', 'status'),
                ('Rating', 'rating'),
                ('ID', 'id'),
                ('Episodes', 'total_episodes'),
                ('Seasons', 'total_seasons'),
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
            displayed_fields = []
            
            for display_name, field_name in metadata_fields:
                if field_name in metadata and metadata[field_name]:
                    general_info_found = True
                    value = metadata[field_name]
                    if isinstance(value, list):
                        value = ', '.join(str(v) for v in value)
                    elif isinstance(value, str) and len(value) > 100:
                        value = value[:97] + '...'
                    displayed_fields.append((display_name, value))
            
            # Add tags to displayed fields if present
            if 'tags' in metadata and metadata['tags']:
                tags = metadata['tags']
                if isinstance(tags, list):
                    tags_str = ', '.join(str(t) for t in tags)
                else:
                    tags_str = str(tags)
                
                if not extended_metadata and len(tags_str) > 80:
                    tags_str = tags_str[:77] + '...'
                
                displayed_fields.append(('Tags', tags_str))
                general_info_found = True
            
            if use_alignment and displayed_fields:
                # Cap label width at 20 characters for better visual balance
                max_label_len = min(max(len(label) for label, _ in displayed_fields), 20)
                for label, value in displayed_fields:
                    if use_colors:
                        print(f"{Colors.YELLOW}{label.rjust(max_label_len)}{Colors.RESET}: {value}")
                    else:
                        print(f"{label.rjust(max_label_len)}: {value}")
            else:
                for label, value in displayed_fields:
                    print(f"{label}: {value}")
            
            # Display additional metadata
            if metadata:
                other_fields = []
                excluded_prefixes = ('myanimelist_', 'imdb_', 'tmdb_')
                excluded_fields = {'title', 'sources', 'tags', 'id', 'total_episodes', 'total_seasons'}
                standard_fields = {f[1] for f in metadata_fields}
                
                for key, value in metadata.items():
                    if (key not in standard_fields and 
                        key not in excluded_fields and
                        not any(key.startswith(prefix) for prefix in excluded_prefixes) and
                        value and not key.startswith('_')):
                        display_key = key.replace('_', ' ').title()
                        if isinstance(value, list):
                            value = ', '.join(str(v) for v in value)
                        elif isinstance(value, str) and len(value) > 100:
                            value = value[:97] + '...'
                        other_fields.append((display_key, value))
                
                if other_fields:
                    if general_info_found:
                        print()
                    if use_alignment:
                        # Cap label width at 20 characters for better visual balance
                        max_label_len = min(max(len(label) for label, _ in other_fields), 20)
                        for label, value in other_fields:
                            if use_colors:
                                print(f"{Colors.YELLOW}{label.rjust(max_label_len)}{Colors.RESET}: {value}")
                            else:
                                print(f"{label.rjust(max_label_len)}: {value}")
                    else:
                        for label, value in other_fields:
                            print(f"{label}: {value}")
            
            # MyAnimeList section
            self._display_mal_section(metadata, mal_watch_status, use_colors, use_alignment)
            
            # IMDb section
            self._display_imdb_section(metadata, use_colors, use_alignment)
            
            # Source URLs
            self._display_sources(metadata, extended_metadata, use_colors)
            
            # Files section
            self._display_files(info, use_colors)
    
    def _display_mal_section(self, metadata: Dict[str, Any], mal_watch_status: Optional[Dict[str, Any]], 
                            use_colors: bool, use_alignment: bool) -> None:
        """Display MyAnimeList metadata section."""
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
        mal_fields_to_display = []
        
        for display_name, field_name in mal_metadata_fields:
            if field_name in metadata and metadata[field_name]:
                mal_info_found = True
                value = metadata[field_name]
                if isinstance(value, list):
                    value = ', '.join(str(v) for v in value)
                elif isinstance(value, str) and len(value) > 150:
                    value = value[:147] + '...'
                mal_fields_to_display.append((display_name, value))
        
        # Display watch status from MyAnimeList
        if mal_watch_status and isinstance(mal_watch_status, dict):
            mal_info_found = True
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
                    mal_fields_to_display.append((display_name, str(mal_watch_status[field_name])))
        
        if mal_info_found:
            if use_colors:
                print(f"\n{Colors.MAGENTA}{Colors.BOLD}--- MyAnimeList Information ---{Colors.RESET}")
            else:
                print(f"\n--- MyAnimeList Information ---")
            
            if use_alignment and mal_fields_to_display:
                max_label_len = max(len(label) for label, _ in mal_fields_to_display)
                for label, value in mal_fields_to_display:
                    if use_colors:
                        print(f"{Colors.YELLOW}{label.rjust(max_label_len)}{Colors.RESET}: {value}")
                    else:
                        print(f"{label.rjust(max_label_len)}: {value}")
            else:
                for label, value in mal_fields_to_display:
                    print(f"{label}: {value}")
        else:
            if use_colors:
                print(f"\n{Colors.MAGENTA}{Colors.BOLD}--- MyAnimeList Information ---{Colors.RESET}")
                print(f"{Colors.DIM}(No MyAnimeList information available){Colors.RESET}")
            else:
                print(f"\n--- MyAnimeList Information ---")
                print("(No MyAnimeList information available)")
    
    def _display_imdb_section(self, metadata: Dict[str, Any], use_colors: bool, use_alignment: bool) -> None:
        """Display IMDb metadata section."""
        imdb_metadata_fields = [
            ('IMDb ID', 'imdb_id'),
            ('IMDb Rating', 'imdb_rating'),
            ('IMDb Votes', 'imdb_votes'),
            ('Metascore', 'metascore'),
        ]
        
        imdb_fields_to_display = []
        for display_name, field_name in imdb_metadata_fields:
            if field_name in metadata and metadata[field_name]:
                imdb_fields_to_display.append((display_name, str(metadata[field_name])))
        
        if imdb_fields_to_display:
            if use_colors:
                print(f"\n{Colors.BLUE}{Colors.BOLD}--- IMDb Information ---{Colors.RESET}")
            else:
                print(f"\n--- IMDb Information ---")
            
            if use_alignment:
                max_label_len = max(len(label) for label, _ in imdb_fields_to_display)
                for label, value in imdb_fields_to_display:
                    if use_colors:
                        print(f"{Colors.YELLOW}{label.rjust(max_label_len)}{Colors.RESET}: {value}")
                    else:
                        print(f"{label.rjust(max_label_len)}: {value}")
            else:
                for label, value in imdb_fields_to_display:
                    print(f"{label}: {value}")
    
    def _display_sources(self, metadata: Dict[str, Any], extended_metadata: bool, use_colors: bool) -> None:
        """Display source URLs."""
        sources = metadata.get('sources', [])
        if sources:
            if extended_metadata:
                if use_colors:
                    print(f"\n{Colors.GREEN}{Colors.BOLD}--- Source URLs ---{Colors.RESET}")
                else:
                    print(f"\n--- Source URLs ---")
                for source_url in sources:
                    if use_colors:
                        print(f"  {Colors.CYAN}•{Colors.RESET} {source_url}")
                    else:
                        print(f"  • {source_url}")
            else:
                mal_sources = [s for s in sources if 'myanimelist' in s.lower()]
                if mal_sources:
                    if use_colors:
                        print(f"\n{Colors.GREEN}{Colors.BOLD}--- Source URLs ---{Colors.RESET}")
                    else:
                        print(f"\n--- Source URLs ---")
                    for source_url in mal_sources:
                        if use_colors:
                            print(f"  {Colors.CYAN}•{Colors.RESET} {source_url}")
                        else:
                            print(f"  • {source_url}")
    
    def _display_files(self, info: Dict[str, Any], use_colors: bool) -> None:
        """Display files section."""
        file_count = len(info['files'])
        if use_colors:
            print(f"\n{Colors.GREEN}{Colors.BOLD}--- Files ({file_count}) ---{Colors.RESET}")
        else:
            print(f"\n--- Files ({file_count}) ---")
        
        for i, file_info in enumerate(info['files'][:5]):
            file_path = file_info.get('path') or file_info.get('filepath')
            if file_path:
                if isinstance(file_path, Path):
                    file_path = file_path.name
                elif isinstance(file_path, str):
                    file_path = Path(file_path).name
                if use_colors:
                    print(f"  {Colors.CYAN}•{Colors.RESET} {file_path}")
                else:
                    print(f"  • {file_path}")
            else:
                filename = file_info.get('filename', 'Unknown')
                if use_colors:
                    print(f"  {Colors.CYAN}•{Colors.RESET} {filename}")
                else:
                    print(f"  • {filename}")
        
        if file_count > 5:
            if use_colors:
                print(f"  {Colors.DIM}... and {file_count - 5} more{Colors.RESET}")
            else:
                print(f"  ... and {file_count - 5} more")
    
    def _display_json(self, show_info: Dict[str, Any], extended_metadata: bool) -> None:
        """Display show information as JSON."""
        output = []
        
        for group_key, info in show_info.items():
            show_data = {
                'title': info['title'],
                'metadata': info.get('metadata', {}),
                'myanimelist_watch_status': info.get('myanimelist_watch_status'),
                'files': []
            }
            
            # Add file information
            for file_info in info['files']:
                file_path = file_info.get('path') or file_info.get('filepath')
                if file_path:
                    if isinstance(file_path, Path):
                        file_path = str(file_path)
                    show_data['files'].append({
                        'path': file_path,
                        'filename': Path(file_path).name
                    })
                else:
                    show_data['files'].append({
                        'filename': file_info.get('filename', 'Unknown')
                    })
            
            # Filter metadata if not extended
            if not extended_metadata:
                filtered_metadata = {}
                for key, value in show_data['metadata'].items():
                    if key not in ['title', 'tags'] or key == 'myanimelist_url':
                        filtered_metadata[key] = value
                show_data['metadata'] = filtered_metadata
            
            output.append(show_data)
        
        print(json.dumps(output, indent=2, ensure_ascii=False))
    
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
  
  # Different output formats
  %(prog)s --format aligned file1.mkv       # Aligned text
  %(prog)s --format color file1.mkv         # Colorful output
  %(prog)s --format json file1.mkv          # JSON output

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
        choices=['DEBUG', 'DEBUG2', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='WARNING',
        help='Set logging level (default: WARNING). DEBUG2 includes verbose regex debugging from guessit.'
    )
    parser.add_argument(
        '--extended-metadata',
        action='store_true',
        help='Show extended metadata fields (title, full tags, all sources, etc.)'
    )
    parser.add_argument(
        '--format',
        choices=['default', 'aligned', 'color', 'json'],
        default='default',
        help='Output format: default (simple text), aligned (better spacing), color (ANSI colors), json (machine-readable)'
    )
    parser.add_argument(
        '--keep-window-open',
        action='store_true',
        help='Keep window open after execution (useful for drag-drop on Windows)'
    )
    
    args = parser.parse_args()
    
    # Handle custom DEBUG2 level for verbose regex debugging
    if args.log_level == 'DEBUG2':
        # DEBUG2 is just DEBUG with rebulk logging enabled
        log_level = logging.DEBUG
        rebulk_log_level = logging.DEBUG
    else:
        log_level = getattr(logging, args.log_level)
        # Suppress rebulk/guessit verbose debugging unless DEBUG2
        rebulk_log_level = logging.WARNING
    
    # Configure logging with output tracker
    log_tracker = LogOutputTracker()
    logging.basicConfig(
        level=log_level,
        format='%(levelname)s: %(message)s',
        handlers=[
            logging.StreamHandler(),
            log_tracker
        ]
    )
    
    # Configure rebulk logger to suppress regex debugging unless DEBUG2
    logging.getLogger('rebulk').setLevel(rebulk_log_level)
    logging.getLogger('guessit').setLevel(rebulk_log_level)
    
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
        tool.display_info(show_info, args.extended_metadata, args.format)
    
    # Keep window open if requested via command line option
    if args.keep_window_open:
        print("\n" + "=" * 70)
        input("Press Enter to exit...")


if __name__ == '__main__':
    main()
