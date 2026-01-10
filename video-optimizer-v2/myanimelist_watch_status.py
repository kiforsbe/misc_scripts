import xml.etree.ElementTree as ET
import re
import requests
import gzip
from dataclasses import dataclass
from typing import Optional
from datetime import datetime
from functools import lru_cache
import logging

@dataclass
class MyAnimeListWatchStatus:
    """Watch status information from MyAnimeList XML"""
    series_animedb_id: int
    series_title: str
    my_status: str  # "Watching", "Completed", "On-Hold", "Dropped", "Plan to Watch"
    my_watched_episodes: int = 0
    my_score: int = 0
    my_start_date: Optional[str] = None
    my_finish_date: Optional[str] = None
    my_times_watched: int = 0
    my_rewatching: bool = False
    series_episodes: int = 0
    progress_percent: float = 0.0

class MyAnimeListWatchStatusProvider:
    """Provider for querying MyAnimeList XML for watch status"""
    
    def __init__(self, xml_path_or_url: str):
        """
        Initialize MyAnimeList watch status provider
        
        Args:
            xml_path_or_url: Path to local XML file (can be .gz), or URL to remote XML
        """
        self.xml_path_or_url = xml_path_or_url
        self.anime_status_map = {}
        self._load_xml()

    def _load_xml(self):
        """Load and parse the MyAnimeList XML data"""
        try:
            if self.xml_path_or_url.startswith('http://') or self.xml_path_or_url.startswith('https://'):
                response = requests.get(self.xml_path_or_url, timeout=30)
                response.raise_for_status()
                
                # Check if response is gzipped
                if response.headers.get('content-encoding') == 'gzip' or self.xml_path_or_url.endswith('.gz'):
                    xml_content = gzip.decompress(response.content)
                else:
                    xml_content = response.content
                    
                root = ET.fromstring(xml_content)
            else:
                # Local file
                if self.xml_path_or_url.endswith('.gz'):
                    with gzip.open(self.xml_path_or_url, 'rt', encoding='utf-8') as f:
                        tree = ET.parse(f)
                        root = tree.getroot()
                else:
                    tree = ET.parse(self.xml_path_or_url)
                    root = tree.getroot()
            
            # Parse each anime entry
            for anime in root.findall('anime'):
                try:
                    animedb_id = anime.findtext('series_animedb_id')
                    if animedb_id:
                        status_data = self._parse_anime_entry(anime)
                        self.anime_status_map[animedb_id] = status_data
                except Exception as e:
                    logging.warning(f"Error parsing anime entry: {e}")
                    
        except Exception as e:
            logging.error(f"Error loading MyAnimeList XML from {self.xml_path_or_url}: {e}")
            raise

    def _parse_anime_entry(self, anime_element) -> MyAnimeListWatchStatus:
        """Parse a single anime entry from XML"""
        def get_text_or_default(element_name: str, default=''):
            elem = anime_element.find(element_name)
            return elem.text if elem is not None and elem.text else default
        
        def get_int_or_default(element_name: str, default=0):
            try:
                return int(get_text_or_default(element_name, str(default)))
            except ValueError:
                return default
        
        def get_bool_from_int(element_name: str, default=False):
            try:
                return int(get_text_or_default(element_name, '0')) == 1
            except ValueError:
                return default
        
        series_animedb_id = get_int_or_default('series_animedb_id')
        series_title = get_text_or_default('series_title')
        my_status = get_text_or_default('my_status')
        my_watched_episodes = get_int_or_default('my_watched_episodes')
        my_score = get_int_or_default('my_score')
        my_start_date = get_text_or_default('my_start_date')
        my_finish_date = get_text_or_default('my_finish_date')
        my_times_watched = get_int_or_default('my_times_watched')
        my_rewatching = get_bool_from_int('my_rewatching')
        series_episodes = get_int_or_default('series_episodes')
        
        # Calculate progress percentage
        progress_percent = 0.0
        if series_episodes > 0 and my_watched_episodes > 0:
            progress_percent = min((my_watched_episodes / series_episodes) * 100, 100.0)
        
        # Clean up date fields (MAL uses "0000-00-00" for empty dates)
        if my_start_date == "0000-00-00":
            my_start_date = None
        if my_finish_date == "0000-00-00":
            my_finish_date = None
        
        return MyAnimeListWatchStatus(
            series_animedb_id=series_animedb_id,
            series_title=series_title,
            my_status=my_status,
            my_watched_episodes=my_watched_episodes,
            my_score=my_score,
            my_start_date=my_start_date,
            my_finish_date=my_finish_date,
            my_times_watched=my_times_watched,
            my_rewatching=my_rewatching,
            series_episodes=series_episodes,
            progress_percent=progress_percent
        )

    @lru_cache(maxsize=512)
    def get_watch_status(self, source_url: str) -> Optional[MyAnimeListWatchStatus]:
        """Get watch status for a given MyAnimeList source URL (cached)
        
        Args:
            source_url: URL containing MyAnimeList anime ID
            
        Returns:
            MyAnimeListWatchStatus object if found, None otherwise
        """
        # Extract the animedb_id from the end of the source URL
        match = re.search(r'myanimelist.*?(\d+)$', source_url)
        if not match:
            return None
        
        animedb_id = match.group(1)
        return self.anime_status_map.get(animedb_id)
