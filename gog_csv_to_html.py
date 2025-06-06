import csv
import sys
import os
import argparse
import json
import requests
import re
from datetime import datetime
from typing import List, Dict, Any, Optional
from collections import defaultdict
from jinja2 import Environment, FileSystemLoader
import urllib.parse
import time
import sqlite3
import json
from bs4 import BeautifulSoup
from bs4.element import Tag
import gzip
import zlib
import brotli

class GOGMediaCache:
    def __init__(self, db_path: str = "gog_media_cache.db"):
        self.db_path = db_path
    
    def get_cached_media(self, game_id: str, game_title: str) -> Optional[Dict[str, Any]]:
        """Get cached media data for a game"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Try to find by ID first, then by title
        cursor.execute('SELECT trailer_id, gameplay_id, images, background_image, square_icon, vertical_cover FROM games WHERE id = ? OR title = ?', 
                      (game_id, game_title))
        result = cursor.fetchone()
        
        conn.close()
        
        if result:
            trailer_id, gameplay_id, images_json, bg_image, square_icon, vertical_cover = result
            return {
                'trailer_id': trailer_id if trailer_id else None,
                'gameplay_id': gameplay_id if gameplay_id else None,
                'images': json.loads(images_json) if images_json else [],
                'background_image': bg_image if bg_image else '',
                'square_icon': square_icon if square_icon else '',
                'vertical_cover': vertical_cover if vertical_cover else ''
            }
        
        return None
    
    def cache_media_data(self, game_id: str, game_title: str, media_data: Dict[str, Any]):
        """Cache media data for a game"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO games 
                (id, title, trailer_id, gameplay_id, images, background_image, square_icon, vertical_cover)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                game_id,
                game_title,
                media_data.get('trailer_id', ''),
                media_data.get('gameplay_id', ''),
                json.dumps(media_data.get('images', [])),
                media_data.get('background_image', ''),
                media_data.get('square_icon', ''),
                media_data.get('vertical_cover', '')
            ))
            
            conn.commit()
        except Exception as e:
            print(f"❌ Error caching media for {game_title}: {e}")
        finally:
            conn.close()
    
    def get_cache_stats(self) -> Dict[str, int]:
        """Get statistics about the cache"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT COUNT(*) FROM games')
        total_games = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM games WHERE trailer_id IS NOT NULL AND trailer_id != ""')
        games_with_trailers = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM games WHERE gameplay_id IS NOT NULL AND gameplay_id != ""')
        games_with_gameplay = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM games WHERE images IS NOT NULL AND images != "[]"')
        games_with_images = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            'total_games': total_games,
            'games_with_trailers': games_with_trailers,
            'games_with_gameplay': games_with_gameplay,
            'games_with_images': games_with_images
        }

class GOGCSVToHTML:
    def __init__(self):
        self.raw_games_data = []
        self.games_data = []
        
        # Initialize media cache
        self.media_cache: GOGMediaCache | None = GOGMediaCache()

        # Initialize Jinja2 environment with custom delimiters
        template_dir = os.path.dirname(__file__)
        self.env = Environment(
            loader=FileSystemLoader(template_dir),
            block_start_string='[%', block_end_string='%]',
            variable_start_string='[[', variable_end_string=']]',
            comment_start_string='[#', comment_end_string='#]'
        )
        
        # Rate limiting for API calls
        self.last_api_call = 0
        self.api_delay = 0.5  # 500ms between calls
    
    def rate_limit(self):
        """Simple rate limiting for API calls"""
        current_time = time.time()
        time_since_last = current_time - self.last_api_call
        if time_since_last < self.api_delay:
            time.sleep(self.api_delay - time_since_last)
        self.last_api_call = time.time()
    
    def search_youtube_trailer(self, game_title: str) -> Optional[str]:
        """Search for game trailer on YouTube and return video ID"""
        try:
            self.rate_limit()
            
            # Clean up game title for search
            search_query = f"{game_title} official trailer"
            encoded_query = urllib.parse.quote(search_query)
            
            # Use YouTube search URL (we'll extract video ID from search results)
            search_url = f"https://www.youtube.com/results?search_query={encoded_query}"
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept-Encoding': 'gzip, deflate, br'
            }
            
            response = requests.get(search_url, headers=headers, timeout=10)
            if response.status_code == 200:
                # Decompress response if needed
                content = self._decompress_response(response)
                
                # Extract video ID from search results
                video_id_pattern = r'"videoId":"([a-zA-Z0-9_-]{11})"'
                matches = re.findall(video_id_pattern, content)
                
                if matches:
                    # Return the first video ID found
                    return matches[0]
            
        except Exception as e:
            print(f"  ⚠️  YouTube trailer search failed for '{game_title}': {e}")
        
        return None
    
    def search_youtube_gameplay(self, game_title: str) -> Optional[str]:
        """Search for game gameplay footage on YouTube and return video ID"""
        try:
            self.rate_limit()
            
            # Clean up game title for search
            search_query = f"{game_title} gameplay walkthrough"
            encoded_query = urllib.parse.quote(search_query)
            
            # Use YouTube search URL (we'll extract video ID from search results)
            search_url = f"https://www.youtube.com/results?search_query={encoded_query}"
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept-Encoding': 'gzip, deflate, br'
            }
            
            response = requests.get(search_url, headers=headers, timeout=10)
            if response.status_code == 200:
                # Decompress response if needed
                content = self._decompress_response(response)
                
                # Extract video ID from search results
                video_id_pattern = r'"videoId":"([a-zA-Z0-9_-]{11})"'
                matches = re.findall(video_id_pattern, content)
                
                if matches:
                    # Return the first video ID found (skip first one in case it's the same as trailer)
                    return matches[1] if len(matches) > 1 else matches[0]
            
        except Exception as e:
            print(f"  ⚠️  YouTube gameplay search failed for '{game_title}': {e}")
        
        return None
    
    def search_game_images(self, game_title: str, developer: str = "", max_count: int = 0) -> List[str]:
        """Search for game screenshots and artwork"""
        try:
            self.rate_limit()
            
            # Search query combining game title and developer
            search_terms = [game_title]
            if developer:
                search_terms.append(developer)
            search_terms.extend(["screenshot", "gameplay", "artwork"])
            
            search_query = " ".join(search_terms)
            encoded_query = urllib.parse.quote(search_query)
            
            # Use Bing image search with updated parameters
            search_url = f"https://www.bing.com/images/search?q={encoded_query}&form=HDRSC2&first=1&count={max_count or 10}"
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
            
            response = requests.get(search_url, headers=headers, timeout=15)
            if response.status_code == 200:
                valid_images = []
                
                try:
                    # Decompress response if needed
                    content = self._decompress_response(response)
                    soup = BeautifulSoup(content, 'html.parser')

                    # Find all images by looking up all divs with class "imgpt" containing as first child an "a" with class "iusc" and attribute "m".
                    # The attribute "m" contains a JSON string with the image URL.
                    image_divs = soup.find_all('div', class_='imgpt')
                    for div in image_divs:
                        # Check if div is a Tag (not just PageElement) before calling find()
                        if isinstance(div, Tag):
                            a_tag = div.find('a', class_='iusc')
                            if a_tag and isinstance(a_tag, Tag) and 'm' in a_tag.attrs:
                                try:
                                    m_data = json.loads(str(a_tag['m']))
                                    image_url = m_data.get('murl')
                                    if image_url and image_url.startswith(('http://', 'https://')):
                                        # Check if we've reached the maximum count
                                        if max_count and len(valid_images) >= max_count:
                                            break
                                        else:
                                            valid_images.append(image_url)
                                except json.JSONDecodeError:
                                    continue
                except Exception as e:
                    print(f"  ⚠️  Error parsing image search results for '{game_title}': {e}")
                    return []

                return valid_images
        except Exception as e:
            print(f"  ⚠️  Image search failed for '{game_title}': {e}")

        return []

    def _decompress_response(self, response: requests.Response) -> str:
        """Decompress HTTP response based on Content-Encoding header and content inspection"""
        content_encoding = response.headers.get('Content-Encoding', '').lower()
        content = response.content
        
        # First, try to detect compression from content magic bytes
        def detect_compression_from_content():
            if len(content) >= 2:
                # Check for gzip magic bytes (1f 8b)
                if content[:2] == b'\x1f\x8b':
                    return 'gzip'
                # Check for deflate (zlib) magic bytes (78 9c, 78 01, 78 da, etc.)
                if content[:2] in [b'\x78\x9c', b'\x78\x01', b'\x78\xda', b'\x78\x5e']:
                    return 'deflate'
            # Check for brotli magic bytes (sequence starting with specific patterns)
            if len(content) >= 4 and content[:4] in [b'\xce\xb2\xcf\x81', b'\x81\x16\x00\x00']:
                return 'br'
            return None
        
        # Detect compression type from content first, then fall back to headers
        detected_compression = detect_compression_from_content()

        # Log mismatch in detected vs declared compression
        #print(f"  ⚠️  Warning missmatching compression (detected: {detected_compression}, header: {content_encoding})")

        # Always use the actual detected compression
        compression_type = detected_compression
        
        try:
            if compression_type == 'gzip':
                return gzip.decompress(content).decode('utf-8')
            elif compression_type == 'deflate':
                return zlib.decompress(content).decode('utf-8')
            elif compression_type == 'br':
                return brotli.decompress(content).decode('utf-8')
            else:
                # No compression detected or unsupported compression type
                # Try to decode as text directly
                try:
                    return response.text
                except UnicodeDecodeError:
                    # If text decoding fails, try UTF-8 directly on content
                    return content.decode('utf-8', errors='replace')
        except Exception as e:
            print(f"  ⚠️  Error decompressing response (detected: {detected_compression}, header: {content_encoding}): {e}")
            # Fallback to regular text if decompression fails
            try:
                return response.text
            except UnicodeDecodeError:
                return content.decode('utf-8', errors='replace')

    def fetch_media_for_game(self, game: Dict[str, Any]) -> Dict[str, Any]:
        """Fetch trailer and images for a specific game, using cache when available"""
        game_id = game.get('game_id', game.get('title', 'unknown'))
        game_title = game.get('title', '')
        developers = game.get('developers', '')
        developer = developers.split(', ')[0] if developers else ""
        
        #print(f"  🔍 Searching media for: {game_title}")
        
        # Fetch new data if not in cache or cache is empty
        media_data: Dict[str, Any] | None = {
            'trailer_id': None,
            'gameplay_id': None,
            'images': [],
            'background_image': '',
            'square_icon': '',
            'vertical_cover': ''
        }

        # Check cache first
        if self.media_cache:
            temp = self.media_cache.get_cached_media(game_id, game_title)
            media_data = temp if temp else media_data
            if media_data:
                #print(f"    💾 Found cached media data")
                # Check if we have meaningful data
                has_trailer = bool(media_data.get('trailer_id'))
                has_gameplay = bool(media_data.get('gameplay_id'))
                has_images = bool(media_data.get('images'))
                
                if has_trailer and has_gameplay and has_images:
                    #print(f"    ✅ Using cached data (trailer: {has_trailer}, gameplay: {has_gameplay}, images: {len(media_data.get('images', []))})")
                    return media_data
                else:
                    print(f"    🔄 Cached data is empty, fetching new data...")
        
        # Search for YouTube trailer if missing
        if not bool(media_data.get('trailer_id')):
            print(f"    📺 Searching for trailer...")
            trailer_id = self.search_youtube_trailer(game_title)
            if trailer_id:
                media_data['trailer_id'] = trailer_id
                print(f"    📺 Found trailer: {trailer_id}")
        
        # Search for YouTube gameplay if missing
        if not bool(media_data.get('gameplay_id')):
            print(f"    🎮 Searching for gameplay footage...")
            gameplay_id = self.search_youtube_gameplay(game_title)
            if gameplay_id:
                media_data['gameplay_id'] = gameplay_id
                print(f"    🎮 Found gameplay: {gameplay_id}")
        
        # Search for game images
        if not bool(media_data.get('images')):
            print(f"    🖼️ Searching for images...")
            images = self.search_game_images(game_title, developer, 10)
            if images:
                media_data['images'] = images
                print(f"    🖼️  Found {len(images)} images")
        
        # Cache the results (even if empty, to avoid repeated failed searches)
        if self.media_cache:
            self.media_cache.cache_media_data(game_id, game_title, media_data)
            print(f"    💾 Cached media data for future use")
        
        return media_data
    
    def load_csv_data(self, csv_file_path: str) -> bool:
        """Load games data from CSV file"""
        try:
            with open(csv_file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                self.raw_games_data = list(reader)
            
            print(f"Loaded {len(self.raw_games_data)} raw game records from {csv_file_path}")
            
            # Consolidate games by game_id
            self.games_data = self.consolidate_games_data()
            print(f"Consolidated to {len(self.games_data)} unique games")
            
            return True
        except Exception as e:
            print(f"Error loading CSV file: {e}")
            return False
    
    def consolidate_games_data(self) -> List[Dict[str, Any]]:
        """Consolidate games data by game_id, merging duplicate entries"""
        games_by_id = defaultdict(list)
        
        # Group games by game_id
        for game in self.raw_games_data:
            game_id = game.get('game_id', game.get('title', 'unknown'))
            games_by_id[game_id].append(game)
        
        consolidated_games = []
        
        for game_id, game_entries in games_by_id.items():
            if len(game_entries) == 1:
                # Single entry, use as-is
                consolidated_games.append(game_entries[0])
            else:
                # Multiple entries, merge them
                merged_game = self.merge_game_entries(game_entries)
                merged_game['game_id'] = game_id
                consolidated_games.append(merged_game)
        
        return consolidated_games
    
    def merge_game_entries(self, entries: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Merge multiple game entries into a single consolidated entry"""
        if not entries:
            return {}
        
        # Start with the first entry as base
        merged = entries[0].copy()
        
        # Combine playtime (sum all hours)
        total_playtime = 0
        for entry in entries:
            try:
                hours = float(entry.get('playtime_hours', 0) or 0)
                total_playtime += hours
            except (ValueError, TypeError):
                pass
        merged['playtime_hours'] = str(total_playtime) if total_playtime > 0 else '0'
        
        # Collect all platforms
        platforms = set()
        for entry in entries:
            platform = entry.get('platform', '').strip()
            if platform:
                platforms.add(platform)
        merged['platforms'] = list(platforms)
        merged['platform'] = ', '.join(sorted(platforms)) if platforms else 'unknown'
        
        # Use the latest dates
        latest_last_played = self.get_latest_date([e.get('last_played', '') for e in entries])
        latest_purchase_date = self.get_latest_date([e.get('purchase_date', '') for e in entries])
        
        if latest_last_played:
            merged['last_played'] = latest_last_played
        if latest_purchase_date:
            merged['purchase_date'] = latest_purchase_date
        
        # Merge genres, tags, developers (deduplicate)
        all_genres = set()
        all_tags = set()
        all_developers = set()
        
        for entry in entries:
            # Genres
            genres = entry.get('genres', '').split(', ') if entry.get('genres') else []
            all_genres.update([g.strip() for g in genres if g.strip()])
            
            # Tags
            tags = entry.get('tags', '').split(', ') if entry.get('tags') else []
            all_tags.update([t.strip() for t in tags if t.strip()])
            
            # Developers
            developers = entry.get('developers', '').split(', ') if entry.get('developers') else []
            all_developers.update([d.strip() for d in developers if d.strip()])
        
        merged['genres'] = ', '.join(sorted(all_genres)) if all_genres else ''
        merged['tags'] = ', '.join(sorted(all_tags)) if all_tags else ''
        merged['developers'] = ', '.join(sorted(all_developers)) if all_developers else ''
        
        # Use the highest rating
        highest_rating = 0
        for entry in entries:
            try:
                rating = float(entry.get('my_rating', 0) or 0)
                if rating > highest_rating:
                    highest_rating = rating
                    merged['my_rating'] = entry.get('my_rating', '')
            except (ValueError, TypeError):
                pass
        
        # Use the longest description
        longest_desc = ''
        for entry in entries:
            desc = entry.get('description', '') or ''
            if len(desc) > len(longest_desc):
                longest_desc = desc
        merged['description'] = longest_desc
        
        return merged
    
    def get_latest_date(self, dates: List[str]) -> str:
        """Get the latest date from a list of date strings"""
        valid_dates = []
        
        for date_str in dates:
            if not date_str or not date_str.strip():
                continue
            
            try:
                # Try to parse ISO format
                if '-' in date_str and len(date_str) >= 10:
                    date_part = date_str[:10]
                    dt = datetime.strptime(date_part, '%Y-%m-%d')
                    valid_dates.append((dt, date_str))
            except ValueError:
                continue
        
        if valid_dates:
            # Return the original string of the latest date
            return max(valid_dates, key=lambda x: x[0])[1]
        
        return ''
    
    def format_playtime(self, hours_str: str) -> str:
        """Format playtime hours into readable format"""
        try:
            hours = float(hours_str or 0)
            if hours == 0:
                return "Not played"
            elif hours < 1:
                minutes = int(hours * 60)
                return f"{minutes}m"
            elif hours < 100:
                return f"{hours:.1f}h"
            else:
                return f"{hours:.0f}h"
        except (ValueError, TypeError):
            return "Not played"
    
    def format_date(self, date_str: str) -> str:
        """Format date string for display"""
        if not date_str or date_str.strip() == '':
            return "Unknown"
        # Handle different date formats
        try:
            # Try ISO format first (YYYY-MM-DD)
            if '-' in date_str and len(date_str) >= 10:
                date_part = date_str[:10]  # Take first 10 chars to handle datetime strings
                dt = datetime.strptime(date_part, '%Y-%m-%d')
                return dt.strftime('%b %d, %Y')
            else:
                return date_str
        except ValueError:
            return date_str or "Unknown"
    
    def get_platform_icon(self, platform: str) -> str:
        """Get platform icon/emoji"""
        platform_icons = {
            'gog': '🎮',
            'steam': '🚂',
            'epic': '🏪',
            'xboxone': '🎯',
            'xbox': '🎯',
            'amazon': '📦',
            'ubisoft': '🌀',
            'origin': '🔶',
            'battlenet': '⚔️',
            'playstation': '🎮',
        }
        return platform_icons.get(platform.lower() if platform else '', '🎮')
    
    def get_platform_icons(self, platforms: List[str]) -> str:
        """Get platform icons for multiple platforms"""
        if not platforms:
            return '🎮'
        
        icons = []
        for platform in platforms:
            icon = self.get_platform_icon(platform)
            if icon not in icons:
                icons.append(icon)
        
        return ' '.join(icons)
    
    def get_rating_stars(self, rating_str: str) -> str:
        """Convert numeric rating to star display"""
        try:
            rating = float(rating_str or 0)
            if rating == 0:
                return ""
            
            # Assuming rating is out of 5
            if rating > 5:            rating = rating / 2  # Convert from 10-point scale
            
            full_stars = int(rating)
            half_star = 1 if (rating - full_stars) >= 0.5 else 0
            empty_stars = 5 - full_stars - half_star
            
            return '★' * full_stars + '☆' * half_star + '☆' * empty_stars
        except (ValueError, TypeError):
            return ""
    def generate_html(self, output_file: str | None = None, fetch_media: bool = True) -> str:
        """Generate HTML file from games data using Jinja2 template"""
        if not self.games_data:
            print("No games data to convert")
            return ""
        
        if not output_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"gog_games_library_{timestamp}.html"
        
        # Count statistics
        total_games = len(self.games_data)
        played_games = len([g for g in self.games_data if float(g.get('playtime_hours', 0) or 0) > 0])
        total_hours = sum(float(g.get('playtime_hours', 0) or 0) for g in self.games_data)
        
        # Get all unique platforms
        all_platforms = set()
        for game in self.games_data:
            platforms = game.get('platforms', [])
            if isinstance(platforms, list):
                all_platforms.update(platforms)
            else:
                # Fallback for single platform string
                platform = game.get('platform', 'Unknown')
                if platform != 'Unknown':
                    all_platforms.add(platform)
        
        # Sort games by title
        sorted_games = sorted(self.games_data, key=lambda x: x.get('title', '').lower())
        
        # Fetch media data if requested
        if fetch_media:
            print("🎬 Fetching media data for games...")
            for i, game in enumerate(sorted_games):
                print(f"📺 Processing {i+1}/{len(sorted_games)}: {game.get('title', 'Unknown')}")
                media_data = self.fetch_media_for_game(game)
                game.update(media_data)
        
        # Prepare games data for JavaScript
        js_games_data = []
        for game in sorted_games:
            js_game = {
                'id': game.get('game_id', game.get('title', 'unknown')),
                'title': game.get('title', 'Unknown Game'),
                'description': game.get('description', ''),
                'platforms': game.get('platforms', []),
                'platform_display': game.get('platform', 'unknown'),
                'playtime_hours': float(game.get('playtime_hours', 0) or 0),
                'playtime_display': self.format_playtime(game.get('playtime_hours', '0')),
                'release_date': self.format_date(game.get('release_date', '')),
                'purchase_date': self.format_date(game.get('purchase_date', '')),
                'last_played': self.format_date(game.get('last_played', '')),
                'my_rating': game.get('my_rating', ''),
                'rating_stars': self.get_rating_stars(game.get('my_rating', '')),
                'genres': game.get('genres', '').split(', ') if game.get('genres') else [],
                'developers': game.get('developers', '').split(', ') if game.get('developers') else [],
                'tags': game.get('tags', '').split(', ') if game.get('tags') else [],
                'is_played': float(game.get('playtime_hours', 0) or 0) > 0,
                'is_recent': game.get('last_played', '') not in ['Unknown', ''],
                # Image URLs
                'background_image': game.get('background_image', ''),
                'square_icon': game.get('square_icon', ''),
                'vertical_cover': game.get('vertical_cover', ''),
                # Media data
                'trailer_id': game.get('trailer_id', ''),
                'gameplay_id': game.get('gameplay_id', ''),
                'images': game.get('images', [])
            }
            js_games_data.append(js_game)
        
        # Prepare embedded JSON data for template
        embedded_json = json.dumps(js_games_data, ensure_ascii=False, separators=(',', ':'))
        
        # Prepare template variables
        template_vars = {
            'embedded_json': embedded_json,
            'total_games': total_games,
            'played_games': played_games,
            'total_hours': f"{total_hours:.0f}",
            'platform_count': len(all_platforms)
        }
        
        # Load and render template
        try:
            template = self.env.get_template('gog_csv_to_html_template.html')
            html_content = template.render(**template_vars)
        except Exception as e:
            print(f"❌ Error loading or rendering template: {e}")
            return ""
        
        # Write to file
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            print(f"✅ HTML file generated successfully: {output_file}")
            print(f"📊 Converted {total_games} games (consolidated from {len(self.raw_games_data)} raw records)")
            print(f"🎮 {played_games} games played ({total_hours:.1f} hours total)")
            print(f"🌐 {len(all_platforms)} platforms: {', '.join(sorted(all_platforms))}")
            
            return output_file
            
        except Exception as e:
            print(f"❌ Error writing HTML file: {e}")
            return ""


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Convert GOG Galaxy CSV export to modern HTML page')
    parser.add_argument('csv_file', help='Path to CSV file from GOG Galaxy Exporter')
    parser.add_argument('-o', '--output', help='Output HTML file name')
    parser.add_argument('--no-media', action='store_true', help='Skip fetching trailer and image data')
    parser.add_argument('--no-cache', action='store_true', help='Skip using media cache')
    parser.add_argument('--open', action='store_true', help='Open the generated HTML file in browser')
    parser.add_argument('--cache-stats', action='store_true', help='Show cache statistics and exit')
    
    args = parser.parse_args()
    
    # Show cache stats if requested
    if args.cache_stats:
        cache = GOGMediaCache()
        stats = cache.get_cache_stats()
        print("📊 Media Cache Statistics:")
        print(f"  Total games cached: {stats['total_games']}")
        print(f"  Games with trailers: {stats['games_with_trailers']}")
        print(f"  Games with gameplay: {stats['games_with_gameplay']}")
        print(f"  Games with images: {stats['games_with_images']}")
        return
    
    # Check if CSV file exists
    if not os.path.exists(args.csv_file):
        print(f"❌ CSV file not found: {args.csv_file}")
        sys.exit(1)
    
    # Create converter and process
    converter = GOGCSVToHTML()
    
    # Disable cache if requested
    if args.no_cache:
        converter.media_cache = None
        print("🚫 Media cache disabled")
    
    print("📖 Loading CSV data...")
    if not converter.load_csv_data(args.csv_file):
        sys.exit(1)
    
    print("🏗️  Generating HTML...")
    output_file = converter.generate_html(args.output, fetch_media=not args.no_media)
    
    if output_file:
        print(f"✨ Done! Open {output_file} in your browser to view your game library.")
        
        # Show cache usage info
        if not args.no_cache and not args.no_media and converter.media_cache:
            cache_stats = converter.media_cache.get_cache_stats()
            print(f"💾 Media cache now contains {cache_stats['total_games']} games")
        
        # Optionally open in browser
        if args.open:
            import webbrowser
            webbrowser.open(f'file://{os.path.abspath(output_file)}')
    else:
        print("❌ Failed to generate HTML file")
        sys.exit(1)

if __name__ == "__main__":
    main()

