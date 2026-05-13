import argparse
import hashlib
import importlib
import time
import json
import os
import shutil
import sys
import re
import binascii
import warnings
from difflib import SequenceMatcher
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Protocol
from presentation import Presenter, color_text, get_emoji, Colors

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

try:
    libtorrent = importlib.import_module('libtorrent')
    LIBTORRENT_AVAILABLE = True
except ImportError:
    libtorrent = None
    LIBTORRENT_AVAILABLE = False

try:
    bencodepy = importlib.import_module('bencodepy')
    BENCODEPY_AVAILABLE = True
except ImportError:
    bencodepy = None
    BENCODEPY_AVAILABLE = False


def _safe_console_print(text: str = "") -> None:
    """Print text with a best-effort encoding fallback for Windows consoles."""
    encoding = getattr(sys.stdout, 'encoding', None) or 'utf-8'
    try:
        print(text)
    except UnicodeEncodeError:
        safe_text = text.encode(encoding, errors='replace').decode(encoding, errors='replace')
        print(safe_text)


def _call_libtorrent_without_deprecation_warnings(callback, *args, **kwargs):
    """Call libtorrent APIs while suppressing deprecation warning noise in CLI output."""
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', DeprecationWarning)
        return callback(*args, **kwargs)


def _configure_libtorrent_session(session: Any) -> None:
    """Apply libtorrent session settings through the non-deprecated settings API."""
    if hasattr(session, 'apply_settings'):
        session.apply_settings({
            'listen_interfaces': '0.0.0.0:6881,[::]:6881'
        })


def _create_add_torrent_params(torrent_info: Any, save_path: Path) -> Any:
    """Create add_torrent_params using the current libtorrent API."""
    params = libtorrent.add_torrent_params()
    params.ti = torrent_info
    params.save_path = str(save_path)
    if hasattr(libtorrent, 'torrent_flags') and hasattr(params, 'flags'):
        params.flags |= libtorrent.torrent_flags.sequential_download
    return params


def _apply_match_to_add_torrent_params(
    params: Any,
    priorities: List[int],
    rename_targets: Optional[Dict[int, str]] = None,
) -> None:
    """Preconfigure matched-file priorities and rename mapping before add_torrent()."""
    if hasattr(libtorrent, 'torrent_flags') and hasattr(params, 'flags'):
        params.flags |= libtorrent.torrent_flags.default_dont_download

    if priorities:
        params.file_priorities = priorities

    if rename_targets:
        for file_index, target_path in rename_targets.items():
            params.renamed_files[file_index] = target_path


def _enable_sequential_download(handle: Any) -> None:
    """Enable sequential download through torrent flags when supported."""
    if hasattr(handle, 'set_flags') and hasattr(libtorrent, 'torrent_flags'):
        handle.set_flags(libtorrent.torrent_flags.sequential_download)
        return
    raise RuntimeError('Current libtorrent flags API is unavailable; sequential download cannot be enabled without deprecated APIs.')


def _format_byte_size(size_bytes: int) -> str:
    """Return a compact human-readable byte size string."""
    size_value = float(size_bytes)
    for unit in ('B', 'KiB', 'MiB', 'GiB', 'TiB'):
        if size_value < 1024.0 or unit == 'TiB':
            return f"{size_value:.2f} {unit}"
        size_value /= 1024.0


def _format_libtorrent_error(error_value: Any) -> str:
    """Render a libtorrent error_code as readable text."""
    if not error_value:
        return ''

    try:
        error_message = error_value.message()
    except Exception:
        error_message = str(error_value)

    try:
        error_code = error_value.value()
    except Exception:
        error_code = None

    if error_code == 0:
        return ''
    if not error_message or error_message in {'No error', 'The operation completed successfully'}:
        return ''
    if error_code is None:
        return error_message
    return f"{error_message} ({error_code})"


def _get_libtorrent_status(handle: Any) -> Any:
    """Fetch torrent status with piece bitfields when the binding supports them."""
    if hasattr(libtorrent, 'status_flags_t'):
        try:
            status_flags = libtorrent.status_flags_t.query_pieces | libtorrent.status_flags_t.query_verified_pieces
            return handle.status(status_flags)
        except Exception:
            pass
    return handle.status()


def _get_libtorrent_file_piece_spans(
    torrent_info: Any,
    matched_index: Optional[int],
    matched_file_size: int,
) -> List[Tuple[int, int]]:
    """Return the overlapping byte count for each torrent piece covering the matched file."""
    if torrent_info is None or matched_index is None or matched_file_size <= 0:
        return []

    try:
        first_mapping = torrent_info.map_file(matched_index, 0, 1)
        last_mapping = torrent_info.map_file(matched_index, matched_file_size - 1, 1)
        piece_length = int(torrent_info.piece_length())
    except Exception:
        return []

    file_start = int(first_mapping.piece) * piece_length + int(first_mapping.start)
    file_end = file_start + matched_file_size
    first_piece = int(first_mapping.piece)
    last_piece = int(last_mapping.piece)

    spans = []
    for piece_index in range(first_piece, last_piece + 1):
        piece_start = piece_index * piece_length
        piece_end = piece_start + int(torrent_info.piece_size(piece_index))
        overlap_start = max(file_start, piece_start)
        overlap_end = min(file_end, piece_end)
        if overlap_end > overlap_start:
            spans.append((piece_index, overlap_end - overlap_start))

    return spans


def _get_libtorrent_valid_piece_indexes(status: Any) -> Set[int]:
    """Return the set of piece indexes libtorrent currently considers valid."""
    if status is None:
        return set()

    valid_piece_indexes = set()
    for attribute_name in ('pieces', 'verified_pieces'):
        try:
            piece_flags = getattr(status, attribute_name, None) or []
        except Exception:
            piece_flags = []

        for piece_index, is_valid in enumerate(piece_flags):
            if is_valid:
                valid_piece_indexes.add(piece_index)

    return valid_piece_indexes


def _build_libtorrent_piece_bitfield(total_piece_count: int, valid_piece_indexes: Set[int]) -> List[bool]:
    """Build a libtorrent-compatible piece bitfield from verified piece indexes."""
    if total_piece_count <= 0:
        return []

    piece_bitfield = [False] * total_piece_count
    for piece_index in valid_piece_indexes:
        if 0 <= piece_index < total_piece_count:
            piece_bitfield[piece_index] = True

    return piece_bitfield


def _estimate_missing_piece_repair_bytes(
    torrent_info: Any,
    file_piece_spans: List[Tuple[int, int]],
    baseline_valid_piece_indexes: Set[int],
) -> Tuple[int, int]:
    """Estimate repair download bytes by counting whole missing torrent pieces."""
    missing_piece_indexes = [piece_index for piece_index, _ in file_piece_spans if piece_index not in baseline_valid_piece_indexes]
    if not missing_piece_indexes:
        return 0, 0

    estimated_bytes = 0
    for piece_index in missing_piece_indexes:
        try:
            estimated_bytes += int(torrent_info.piece_size(piece_index))
        except Exception:
            pass

    return estimated_bytes, len(missing_piece_indexes)


def _get_libtorrent_file_ok_bytes(
    status: Any,
    file_piece_spans: List[Tuple[int, int]],
    matched_file_size: int,
    baseline_valid_piece_indexes: Optional[Set[int]] = None,
) -> Optional[int]:
    """Estimate how many bytes of the matched file are currently backed by valid pieces."""
    if matched_file_size <= 0 or not file_piece_spans:
        return None

    valid_piece_indexes = _get_libtorrent_valid_piece_indexes(status)
    if baseline_valid_piece_indexes:
        valid_piece_indexes.update(baseline_valid_piece_indexes)

    valid_bytes = 0
    for piece_index, overlap_bytes in file_piece_spans:
        if piece_index not in valid_piece_indexes:
            continue
        valid_bytes += overlap_bytes

    return min(valid_bytes, matched_file_size)


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
    
    def __init__(self, verbose: int = 0, progress_reporter: Optional[ProgressReporter] = None, use_colors: bool = True):
        self.data: Optional[Dict] = None
        self.groups: Dict = {}
        self.verbose = verbose
        self.progress_reporter = progress_reporter
        self.use_colors = use_colors
        
    def _log(self, message: str, level: int = 1):
        """Log message if verbosity level is sufficient."""
        self._log_progress_message(message, level)

    def _log_progress_message(self, message: str, level: int = 1) -> None:
        """Write a message without corrupting an active tqdm progress bar."""
        if self.verbose < level:
            return

        if TQDM_AVAILABLE:
            tqdm.write(message)
            return

        _safe_console_print(message)
    
    def _color(self, text: str, color: str = "") -> str:
        """Apply color to text if colors are enabled."""
        return color_text(text, color, use_colors=self.use_colors)
    
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

            # Check if the group is a movie
            files = group_data.get('files', [])
            first_file_type = ""
            if files and isinstance(files, list) and files[0]:
                first_file_type = str(files[0].get('type', '')).lower()
            metadata_type = str(group_data.get('type', '')).lower()
            if "movie" in first_file_type or "movie" in metadata_type:
                status = "movie"
            
            # Get season info and format title if season > 1
            season = group_data.get('season')
            if not season and files:
                season = files[0].get('season')
            
            formatted_title = f"{title} S{season:02d}" if season else title

            details = {
                'title': formatted_title,
                'episodes_found': episodes_found,
                'episodes_expected': episodes_expected,
                'status': status,
                'data': group_data
            }
            
            if show_details:
                files = group_data.get('files', [])
                if files:
                    details['release_group'] = files[0].get('release_group', 'Unknown')
                    details['screen_size'] = self._get_group_screen_size(group_data)
                    details['folder_name'] = self.generate_folder_name(group_data)
            
            group_list.append((group_key, details))
        
        return group_list
    
    def get_group_details(self, group_key: str) -> Optional[Dict]:
        """Get detailed information about a specific group."""
        return self.groups.get(group_key)
    
    def _format_episode_range(self, episodes: List) -> str:
        """Format episode numbers as a range string, handling both int and float episodes."""
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

    def _extract_resolution_from_text(self, text: str) -> Optional[str]:
        """Extract a resolution token like 720p or 1920x1080 from text."""
        if not text:
            return None

        patterns = [
            r'(?<!\d)(\d{3,4}p)(?!\d)',
            r'(?<!\d)(\d{3,4}x\d{3,4})(?!\d)'
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1).lower()

        return None

    def _get_group_screen_size(self, group_data: Dict) -> str:
        """Resolve the best available resolution for a group."""
        files = group_data.get('files', [])
        if not files:
            return "Unknown"

        preferred_keys = (
            'screen_size',
            'resolution',
            'video_resolution',
            'display_resolution'
        )

        for file_info in files:
            for key in preferred_keys:
                value = file_info.get(key)
                if value:
                    return str(value)

        for file_info in files:
            for key in ('filename', 'filepath', 'file_path'):
                value = file_info.get(key)
                if value:
                    resolution = self._extract_resolution_from_text(str(value))
                    if resolution:
                        return resolution

        return "Unknown"
    
    def generate_folder_name(self, group_data: Dict) -> str:
        """Generate folder name following the pattern: [Release Group] Series Name (YYYY) (xx-yy) (Resolution)"""
        files = group_data.get('files', [])
        if not files:
            return "Unknown"
        
        # Get common attributes from files
        first_file = files[0]
        release_group = first_file.get('release_group', 'Unknown')
        title = group_data.get('title', 'Unknown')
        year = group_data.get('year') or first_file.get('year')
        season = group_data.get('season') or first_file.get('season')
        screen_size = self._get_group_screen_size(group_data)
        
        # Determine if this is a movie
        is_movie = False
        first_file_type = str(first_file.get('type', '')).lower()
        metadata_type = str(group_data.get('type', '')).lower()
        if "movie" in first_file_type or "movie" in metadata_type:
            is_movie = True

        # Build the series title with season if applicable
        series_title = str(title)
        if season and season > 1:
            series_title = f"{title} S{season}"

        # Get episode range
        if is_movie:
            episode_range = str(year) if year else "Movie"
        else:
            # Collect all episode numbers (can be int or float)
            episodes = []
            for file_info in files:
                episode = file_info.get('episode')
                if isinstance(episode, list):
                    episodes.extend(episode)
                elif episode is not None:
                    episodes.append(episode)
            
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
        
        return folder_name
    
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
    
    def _extract_crc_from_filename(self, filename: str) -> Optional[str]:
        """Extract CRC32 hash from filename if present. Pattern: [FFFFFFFF] where F is hex."""
        # Pattern matches [8 hex digits] at the end before file extension
        pattern = r'\[([A-Fa-f0-9]{8})\]'
        match = re.search(pattern, filename)
        return match.group(1).upper() if match else None

    def _create_crc_progress_bar(self, filepath: str, total_bytes: Optional[int] = None, label: str = 'CRC'):
        """Create a byte progress bar using the same presentation for CRC and torrent workflows."""
        if not (TQDM_AVAILABLE and self.verbose >= 1):
            return None

        display_name = os.path.basename(filepath)
        if len(display_name) > 36:
            display_name = display_name[:33] + '...'

        return tqdm(
            total=total_bytes,
            desc=f"{label} {display_name}",
            unit='B',
            unit_scale=True,
            unit_divisor=1024,
            leave=False,
            disable=False,
            position=1
        )

    def _update_crc_progress_bar(self, progress_bar, completed_bytes: int, total_bytes: Optional[int] = None) -> None:
        """Update a CRC progress bar to the latest byte counters."""
        if progress_bar is None:
            return

        if total_bytes and (progress_bar.total is None or progress_bar.total != total_bytes):
            progress_bar.total = total_bytes

        if progress_bar.total is not None:
            completed_bytes = min(completed_bytes, int(progress_bar.total))

        progress_bar.n = max(0, completed_bytes)
        progress_bar.refresh()
    
    def _calculate_file_crc32(self, filepath: str, show_progress: bool = False) -> str:
        """Calculate CRC32 hash of a file."""
        crc = 0
        file_progress = None
        try:
            if show_progress:
                file_progress = self._create_crc_progress_bar(filepath, os.path.getsize(filepath))

            with open(filepath, 'rb') as f:
                while chunk := f.read(8192):
                    crc = binascii.crc32(chunk, crc)
                    if file_progress is not None:
                        file_progress.update(len(chunk))
        finally:
            if file_progress is not None:
                file_progress.close()

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
            # Check for new episode_watched field first
            episode_watched = file_info.get('episode_watched', False)
            if episode_watched:
                episode = file_info.get('episode')
                if isinstance(episode, list):
                    watched_episodes.extend(episode)
                elif episode is not None:
                    watched_episodes.append(episode)
            else:
                # Fallback to plex_watch_status for backward compatibility
                plex_status = file_info.get('plex_watch_status')
                if plex_status and plex_status.get('watched'):
                    episode = file_info.get('episode')
                    if isinstance(episode, list):
                        watched_episodes.extend(episode)
                    elif episode is not None:
                        watched_episodes.append(episode)
        
        return sorted(set(watched_episodes)) if watched_episodes else []

    def _get_watch_status_classification(self, group_data: Dict) -> str:
        """Determine watch status classification for a group."""
        # Check if this is a movie based on type
        files = group_data.get('files', [])
        if files:
            first_file_type = str(files[0].get('type', '')).lower()
            metadata_type = str(group_data.get('type', '')).lower()
            if "movie" in first_file_type or "movie" in metadata_type:
                # For movies, check if any file has been watched
                for file_info in files:
                    episode_watched = file_info.get('episode_watched', False)
                    if episode_watched:
                        return "watched"
                    # Fallback to plex_watch_status
                    plex_status = file_info.get('plex_watch_status')
                    if plex_status and plex_status.get('watched'):
                        return "watched"
                return "unwatched"
        
        # For series, use watch_status data if available
        watch_status = group_data.get('watch_status', {})
        watched_episodes = watch_status.get('watched_episodes', 0)
        partially_watched_episodes = watch_status.get('partially_watched_episodes', 0)
        episodes_found = group_data.get('episodes_found', 0)
        
        if episodes_found == 0:
            return "unwatched"
        
        if watched_episodes == episodes_found:
            return "watched"
        elif watched_episodes > 0 or partially_watched_episodes > 0:
            return "watched_partial"
        else:
            return "unwatched"
    
    def _verify_file_crc(
        self,
        filepath: str,
        expected_crc: Optional[str] = None,
        torrent_recovery: Optional[Dict[str, Any]] = None
    ) -> Tuple[bool, str, str]:
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
            actual_crc = self._calculate_file_crc32(filepath, show_progress=True)
            return expected_crc.upper() == actual_crc.upper(), expected_crc.upper(), actual_crc.upper()
        except Exception as e:
            self._log(f"Error calculating CRC for {filename}: {e}", 1)
            return False, expected_crc.upper(), "ERROR"

    def _is_libtorrent_checking_state(self, state: Any) -> bool:
        """Return True while libtorrent is still checking existing file data."""
        if isinstance(state, str):
            normalized = state.lower()
            return normalized in {
                'queued for checking',
                'checking files',
                'checking resume data',
                'queued_for_checking',
                'checking_files',
                'checking_resume_data',
            }
        if isinstance(state, int):
            return state in {0, 1, 7}
        return False

    def _extract_torrent_status_progress(
        self,
        status: Any,
        fallback_total: Optional[int] = None
    ) -> Tuple[float, int, Optional[int], Optional[str]]:
        """Extract byte progress counters from a libtorrent status object."""
        progress = 0.0
        completed_bytes = 0
        total_bytes = fallback_total
        error_text = None

        try:
            progress = float(getattr(status, 'progress', 0.0) or 0.0)
        except Exception:
            progress = 0.0

        try:
            wanted_done = getattr(status, 'total_wanted_done', None)
            wanted_total = getattr(status, 'total_wanted', None)
            if isinstance(wanted_total, (int, float)) and wanted_total > 0:
                total_bytes = int(wanted_total)
            if isinstance(wanted_done, (int, float)) and wanted_done >= 0:
                completed_bytes = int(wanted_done)
            elif total_bytes:
                completed_bytes = int(progress * total_bytes)
        except Exception:
            if total_bytes:
                completed_bytes = int(progress * total_bytes)

        try:
            errc = getattr(status, 'errc', None)
            if errc:
                err_text = _format_libtorrent_error(errc)
                if err_text:
                    error_text = err_text
        except Exception:
            pass

        return progress, completed_bytes, total_bytes, error_text

    def _verify_file_against_torrent_source(
        self,
        filepath: str,
        match: Dict[str, Any],
        torrent_recovery: Dict[str, Any]
    ) -> Tuple[bool, str]:
        """Verify file integrity by hashing the matched file's torrent pieces directly from disk."""
        if not LIBTORRENT_AVAILABLE:
            return False, 'ERROR'

        failed_path = Path(filepath)
        expected_size = int(match.get('size', 0) or 0)
        verify_progress = None

        try:
            torrent_info = libtorrent.torrent_info(str(match['torrent_path']))
            matched_index, _ = self._find_matched_torrent_file_index(torrent_info, match)
            file_piece_spans = _get_libtorrent_file_piece_spans(torrent_info, matched_index, expected_size)
            if matched_index is None or not file_piece_spans or expected_size <= 0:
                return False, 'ERROR'

            verify_progress = self._create_crc_progress_bar(filepath, expected_size, label='Verify')

            valid_piece_indexes = set()
            verified_overlap_bytes = 0
            total_pieces = len(file_piece_spans)
            for piece_number, (piece_index, overlap_bytes) in enumerate(file_piece_spans, start=1):
                single_piece_valid = self._scan_local_valid_piece_indexes(
                    torrent_info,
                    match,
                    failed_path.parent,
                    failed_path,
                    matched_index,
                    [(piece_index, overlap_bytes)],
                )
                if piece_index in single_piece_valid:
                    valid_piece_indexes.add(piece_index)
                    verified_overlap_bytes += overlap_bytes

                if verify_progress is not None:
                    self._update_crc_progress_bar(verify_progress, piece_number and min(expected_size, sum(span for _, span in file_piece_spans[:piece_number])) or 0, expected_size)
                    verify_progress.set_postfix_str(f"file-ok {verified_overlap_bytes / expected_size:.2%}")

            if verified_overlap_bytes >= expected_size:
                return True, 'TORRENT_OK'
            return False, 'TORRENT_DAMAGED'
        except Exception as exc:
            self._log(f"Torrent-backed verification failed for {filepath}: {exc}", 1)
            return False, 'ERROR'
        finally:
            if verify_progress is not None:
                verify_progress.close()

    def _check_file_integrity(
        self,
        filepath: str,
        filename: Optional[str] = None,
        torrent_recovery: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Check file integrity using filename CRC when present, else torrent metadata when available."""
        resolved_name = filename or os.path.basename(filepath)
        result = {
            'status': 'no_crc',
            'expected_crc': 'N/A',
            'actual_crc': 'N/A',
            'is_valid': False,
            'verification_source': 'none'
        }

        expected_crc = self._extract_crc_from_filename(resolved_name)
        if expected_crc is not None:
            is_valid, expected_crc, actual_crc = self._verify_file_crc(filepath, expected_crc=expected_crc)
            result.update({
                'status': 'valid' if is_valid else 'invalid',
                'expected_crc': expected_crc,
                'actual_crc': actual_crc,
                'is_valid': is_valid,
                'verification_source': 'filename_crc'
            })
            return result

        if not (torrent_recovery and torrent_recovery.get('enabled') and torrent_recovery.get('torrent_files_path')):
            return result

        failed_result = {
            'filename': resolved_name
        }
        best_match, inspected_count = self._find_best_torrent_match(
            filepath,
            failed_result,
            torrent_recovery.get('torrent_files_path')
        )
        if not best_match:
            return result

        is_valid, actual_status = self._verify_file_against_torrent_source(filepath, best_match, torrent_recovery)
        result.update({
            'status': 'valid' if is_valid else 'invalid',
            'expected_crc': 'TORRENT',
            'actual_crc': actual_status,
            'is_valid': is_valid,
            'verification_source': 'torrent',
            'torrent_match': best_match,
            'inspected_torrent_count': inspected_count
        })
        return result

    def _derive_expected_crc_from_torrent_match(
        self,
        filepath: str,
        actual_crc: str,
        match: Dict[str, Any],
        torrent_recovery: Dict[str, Any]
    ) -> Tuple[bool, str]:
        """Derive an expected CRC from torrent-backed content for a matched file."""
        if not LIBTORRENT_AVAILABLE:
            return False, 'N/A'

        temporary_root = Path(filepath).parent / '.torrent_verify'
        torrent_progress = None
        try:
            temporary_root.mkdir(parents=True, exist_ok=True)
            torrent_info = libtorrent.torrent_info(str(match['torrent_path']))
            session = libtorrent.session()
            try:
                _configure_libtorrent_session(session)
            except Exception:
                pass

            handle = session.add_torrent(
                self._build_matched_torrent_params(torrent_info, match, temporary_root)
            )
            try:
                _enable_sequential_download(handle)
            except Exception:
                pass
            try:
                handle.resume()
            except Exception:
                pass
            try:
                _call_libtorrent_without_deprecation_warnings(handle.force_recheck)
            except Exception:
                pass
            self._prioritize_matched_torrent_file(handle, torrent_info, match)

            timeout_seconds = int(torrent_recovery.get('timeout_seconds') or 120)
            deadline = time.time() + timeout_seconds
            verification_paths = self._resolve_session_candidate_paths(temporary_root, match)
            expected_size = match.get('size')
            torrent_progress = self._create_crc_progress_bar(filepath, expected_size)

            while time.time() < deadline:
                progress = 0.0
                completed_bytes = 0
                total_bytes = expected_size
                try:
                    status = handle.status()
                    progress = float(getattr(status, 'progress', 0.0) or 0.0)

                    wanted_done = getattr(status, 'total_wanted_done', None)
                    wanted_total = getattr(status, 'total_wanted', None)
                    if isinstance(wanted_total, (int, float)) and wanted_total > 0:
                        total_bytes = int(wanted_total)
                    if isinstance(wanted_done, (int, float)) and wanted_done >= 0:
                        completed_bytes = int(wanted_done)
                    elif total_bytes:
                        completed_bytes = int(progress * total_bytes)
                except Exception:
                    pass

                if torrent_progress is not None and total_bytes:
                    self._update_crc_progress_bar(torrent_progress, completed_bytes, total_bytes)

                if progress >= 0.999:
                    for candidate_path in verification_paths:
                        if not candidate_path.exists() or not candidate_path.is_file():
                            continue
                        try:
                            candidate_crc = self._calculate_file_crc32(str(candidate_path), show_progress=True)
                        except Exception:
                            continue

                        try:
                            session.remove_torrent(handle)
                        except Exception:
                            pass
                        self._cleanup_verification_root(temporary_root)
                        return candidate_crc == actual_crc, candidate_crc

                time.sleep(1.0)

            try:
                session.remove_torrent(handle)
            except Exception:
                pass
        except Exception as exc:
            self._log(f"Torrent-backed verification failed for {filepath}: {exc}", 1)
        finally:
            if torrent_progress is not None:
                torrent_progress.close()
            self._cleanup_verification_root(temporary_root)

        return False, 'N/A'

    def _cleanup_verification_root(self, save_path: Path) -> None:
        """Remove the temporary verification workspace."""
        try:
            if save_path.exists() and save_path.name == '.torrent_verify':
                shutil.rmtree(save_path, ignore_errors=True)
        except Exception as exc:
            self._log(f"Could not clean temporary torrent verification path {save_path}: {exc}", 2)

    def _normalize_match_name(self, value: str) -> str:
        """Normalize torrent and media names for filename similarity matching."""
        name = os.path.basename(value or '')
        name = re.sub(r'\.[^.]+$', '', name)
        name = re.sub(r'\[[A-Fa-f0-9]{8}\]', ' ', name)
        name = re.sub(r'[._\-\[\]\(\)]+', ' ', name)
        name = re.sub(r'\s+', ' ', name).strip().lower()
        return name

    def _filename_similarity(self, left: str, right: str) -> float:
        """Return a similarity score between two filenames."""
        left_name = self._normalize_match_name(left)
        right_name = self._normalize_match_name(right)

        if not left_name or not right_name:
            return 0.0
        if left_name == right_name:
            return 1.0
        if left_name in right_name or right_name in left_name:
            return 0.95
        return SequenceMatcher(None, left_name, right_name).ratio()

    def _bdecode(self, payload: bytes) -> Any:
        """Decode bencoded torrent data, using bencodepy when available."""
        if BENCODEPY_AVAILABLE:
            return bencodepy.decode(payload)

        def _decode_at(index: int) -> Tuple[Any, int]:
            token = payload[index:index + 1]
            if token == b'i':
                end = payload.index(b'e', index)
                return int(payload[index + 1:end]), end + 1
            if token == b'l':
                index += 1
                items = []
                while payload[index:index + 1] != b'e':
                    item, index = _decode_at(index)
                    items.append(item)
                return items, index + 1
            if token == b'd':
                index += 1
                items = {}
                while payload[index:index + 1] != b'e':
                    key, index = _decode_at(index)
                    value, index = _decode_at(index)
                    items[key] = value
                return items, index + 1
            if token.isdigit():
                colon_index = payload.index(b':', index)
                length = int(payload[index:colon_index])
                start = colon_index + 1
                end = start + length
                return payload[start:end], end
            raise ValueError(f'Invalid bencode token at index {index}')

        decoded, end_index = _decode_at(0)
        if end_index != len(payload):
            raise ValueError('Unexpected trailing data in torrent payload')
        return decoded

    def _decode_torrent_text(self, value: Any) -> str:
        """Decode torrent bytes to a safe display string."""
        if isinstance(value, bytes):
            return value.decode('utf-8', errors='replace')
        return str(value)

    def _read_torrent_metadata_with_libtorrent(self, torrent_path: str) -> Optional[Dict[str, Any]]:
        """Read torrent metadata through libtorrent when it is available."""
        return None

    def _read_torrent_metadata(self, torrent_path: str) -> Optional[Dict[str, Any]]:
        """Read a .torrent file and return simplified metadata for matching."""
        try:
            with open(torrent_path, 'rb') as torrent_file:
                raw_metadata = self._bdecode(torrent_file.read())
        except Exception as exc:
            self._log(f"Could not parse torrent '{torrent_path}': {exc}", 2)
            return None

        if not isinstance(raw_metadata, dict):
            return None

        info = raw_metadata.get(b'info')
        if not isinstance(info, dict):
            return None

        torrent_name = self._decode_torrent_text(info.get(b'name', Path(torrent_path).stem))
        files = []

        if b'files' in info and isinstance(info[b'files'], list):
            for file_entry in info[b'files']:
                if not isinstance(file_entry, dict):
                    continue
                path_parts = file_entry.get(b'path') or file_entry.get(b'path.utf-8')
                if not isinstance(path_parts, list):
                    continue
                relative_parts = [self._decode_torrent_text(part) for part in path_parts]
                relative_path = os.path.join(*relative_parts) if relative_parts else ''
                if not relative_path:
                    continue
                files.append({
                    'relative_path': relative_path,
                    'display_name': os.path.basename(relative_path),
                    'size': file_entry.get(b'length')
                })
        else:
            files.append({
                'relative_path': torrent_name,
                'display_name': os.path.basename(torrent_name),
                'size': info.get(b'length')
            })

        return {
            'torrent_path': torrent_path,
            'torrent_name': torrent_name,
            'files': files
        }

    def _collect_candidate_torrent_files(self, failed_filepath: str, torrent_files_path: Optional[str]) -> List[str]:
        """Collect candidate .torrent files, prioritizing ones next to the failed file."""
        candidates = []
        seen = set()

        local_folder = Path(failed_filepath).parent
        if local_folder.exists():
            for torrent_path in sorted(local_folder.glob('*.torrent')):
                resolved = str(torrent_path.resolve())
                if resolved not in seen:
                    candidates.append(resolved)
                    seen.add(resolved)

        if torrent_files_path:
            torrent_root = Path(torrent_files_path)
            if torrent_root.exists():
                for torrent_path in sorted(torrent_root.rglob('*.torrent')):
                    resolved = str(torrent_path.resolve())
                    if resolved not in seen:
                        candidates.append(resolved)
                        seen.add(resolved)

        return candidates

    def _score_torrent_candidate(self, failed_filename: str, torrent_path: str) -> float:
        """Score a torrent file by filename similarity before parsing its contents."""
        return self._filename_similarity(failed_filename, Path(torrent_path).stem)

    def _find_best_torrent_match(
        self,
        failed_filepath: str,
        failed_result: Dict[str, Any],
        torrent_files_path: Optional[str]
    ) -> Tuple[Optional[Dict[str, Any]], int]:
        """Find the best torrent/file match for a failed media file."""
        failed_filename = failed_result.get('filename') or os.path.basename(failed_filepath)
        candidates = self._collect_candidate_torrent_files(failed_filepath, torrent_files_path)
        if not candidates:
            return None, 0

        ranked_candidates = sorted(
            candidates,
            key=lambda candidate: (
                1 if Path(candidate).parent == Path(failed_filepath).parent else 0,
                self._score_torrent_candidate(failed_filename, candidate)
            ),
            reverse=True
        )

        likely_suspects = []
        remaining_candidates = []
        for index, candidate in enumerate(ranked_candidates):
            candidate_score = self._score_torrent_candidate(failed_filename, candidate)
            if index < 10 or candidate_score >= 0.45 or Path(candidate).parent == Path(failed_filepath).parent:
                likely_suspects.append(candidate)
            else:
                remaining_candidates.append(candidate)

        search_order = likely_suspects + remaining_candidates
        best_match = None
        inspected_count = 0

        for torrent_path in search_order:
            metadata = self._read_torrent_metadata(torrent_path)
            inspected_count += 1
            if not metadata:
                continue

            torrent_name_score = self._score_torrent_candidate(failed_filename, torrent_path)
            for file_entry in metadata['files']:
                file_score = self._filename_similarity(failed_filename, file_entry['display_name'])
                total_score = max(file_score, torrent_name_score * 0.85)
                if file_score >= 0.995:
                    total_score = 1.0

                candidate_match = {
                    'torrent_path': metadata['torrent_path'],
                    'torrent_name': metadata['torrent_name'],
                    'relative_path': file_entry['relative_path'],
                    'display_name': file_entry['display_name'],
                    'size': file_entry.get('size'),
                    'torrent_name_score': torrent_name_score,
                    'file_score': file_score,
                    'total_score': total_score
                }

                if best_match is None or candidate_match['total_score'] > best_match['total_score']:
                    best_match = candidate_match

            if best_match and best_match['total_score'] >= 0.99:
                break

        if best_match and best_match['total_score'] >= 0.55:
            return best_match, inspected_count
        return None, inspected_count

    def _resolve_session_candidate_paths(self, root: Path, match: Dict[str, Any]) -> List[Path]:
        """Resolve likely filesystem locations for a matched file under a torrent session root."""
        relative_path = Path(match['relative_path'])
        torrent_name = Path(match['torrent_name'])

        candidates = []
        seen = set()
        for candidate in (
            root / relative_path,
            root / torrent_name / relative_path,
            root / torrent_name.name / relative_path,
            root / relative_path.name
        ):
            resolved_key = str(candidate)
            if resolved_key not in seen:
                candidates.append(candidate)
                seen.add(resolved_key)

        return candidates

    def _get_torrent_file_paths_in_order(self, torrent_path: str) -> List[str]:
        """Return torrent file paths in declared order using parsed torrent metadata."""
        metadata = self._read_torrent_metadata(torrent_path)
        if not metadata:
            return []

        return [
            str(file_entry.get('relative_path', ''))
            for file_entry in metadata.get('files', [])
            if file_entry.get('relative_path')
        ]

    def _resolve_session_file_path(
        self,
        root: Path,
        torrent_name: str,
        relative_path: str,
        preferred_path: Optional[Path] = None,
    ) -> Optional[Path]:
        """Resolve the current on-disk path for a torrent file during recovery."""
        if preferred_path is not None and preferred_path.exists():
            return preferred_path

        candidate_match = {
            'relative_path': relative_path,
            'torrent_name': torrent_name,
        }
        for candidate in self._resolve_session_candidate_paths(root, candidate_match):
            if candidate.exists():
                return candidate

        return None

    def _resolve_in_place_target_path(
        self,
        root: Path,
        torrent_name: str,
        relative_path: str,
        preferred_path: Optional[Path] = None,
    ) -> Path:
        """Resolve a stable in-place path for a torrent file without recreating the torrent root layout."""
        existing_path = self._resolve_session_file_path(
            root,
            torrent_name,
            relative_path,
            preferred_path=preferred_path,
        )
        if existing_path is not None:
            return existing_path

        if preferred_path is not None:
            return preferred_path

        relative_name = Path(relative_path).name
        return root / relative_name

    def _scan_local_valid_piece_indexes(
        self,
        torrent_info: Any,
        match: Dict[str, Any],
        save_path: Path,
        failed_path: Path,
        matched_index: Optional[int],
        file_piece_spans: List[Tuple[int, int]],
    ) -> Set[int]:
        """Hash locally available torrent pieces so repair can reuse already valid file data."""
        if matched_index is None or not file_piece_spans:
            return set()

        try:
            torrent_file_paths = self._get_torrent_file_paths_in_order(str(match['torrent_path']))
        except Exception:
            return set()

        if not torrent_file_paths:
            return set()

        valid_piece_indexes = set()
        torrent_name = str(match.get('torrent_name', ''))
        for piece_index, _ in file_piece_spans:
            try:
                piece_size = int(torrent_info.piece_size(piece_index))
                file_slices = torrent_info.map_block(piece_index, 0, piece_size)
                expected_hash = bytes(torrent_info.hash_for_piece(piece_index))
            except Exception:
                continue

            piece_chunks = []
            piece_complete = True
            for file_slice in file_slices:
                try:
                    file_index = int(file_slice.file_index)
                    slice_offset = int(file_slice.offset)
                    slice_size = int(file_slice.size)
                except Exception:
                    piece_complete = False
                    break

                if file_index < 0 or file_index >= len(torrent_file_paths):
                    piece_complete = False
                    break

                preferred_path = failed_path if file_index == matched_index else None
                resolved_path = self._resolve_session_file_path(
                    save_path,
                    torrent_name,
                    torrent_file_paths[file_index],
                    preferred_path=preferred_path,
                )
                if resolved_path is None:
                    piece_complete = False
                    break

                try:
                    with open(resolved_path, 'rb') as source_file:
                        source_file.seek(slice_offset)
                        chunk = source_file.read(slice_size)
                except OSError:
                    piece_complete = False
                    break

                if len(chunk) != slice_size:
                    piece_complete = False
                    break

                piece_chunks.append(chunk)

            if not piece_complete:
                continue

            if hashlib.sha1(b''.join(piece_chunks)).digest() == expected_hash:
                valid_piece_indexes.add(piece_index)

        return valid_piece_indexes

    def _attempt_torrent_file_recovery(
        self,
        failed_filepath: str,
        failed_result: Dict[str, Any],
        match: Dict[str, Any],
        torrent_recovery: Dict[str, Any]
    ) -> Tuple[bool, str]:
        """Attempt to replace a corrupt file with a matching file from torrent-managed content."""
        if failed_result.get('verification_source') == 'torrent':
            return self._attempt_libtorrent_piece_recovery(
                failed_filepath,
                match,
                torrent_recovery
            )

        expected_crc = failed_result.get('expected_crc')
        if not expected_crc or expected_crc == 'N/A':
            return False, 'Expected CRC is not available for recovery.'

        libtorrent_recovered, libtorrent_message = self._attempt_libtorrent_session_recovery(
            failed_filepath,
            expected_crc,
            match,
            torrent_recovery
        )
        if libtorrent_recovered:
            return libtorrent_recovered, libtorrent_message

        if libtorrent_message:
            return False, libtorrent_message
        return False, 'Matched torrent metadata was found, but no CRC-valid file was recovered.'

    def _attempt_libtorrent_piece_recovery(
        self,
        failed_filepath: str,
        match: Dict[str, Any],
        torrent_recovery: Dict[str, Any]
    ) -> Tuple[bool, str]:
        """Recover a file by letting libtorrent redownload missing or corrupt pieces."""
        if not LIBTORRENT_AVAILABLE:
            return False, 'Matched torrent metadata was found, but libtorrent is not available.'

        failed_path = Path(failed_filepath)
        local_file_size = 0
        try:
            if failed_path.exists():
                local_file_size = failed_path.stat().st_size
        except OSError:
            local_file_size = 0

        expected_size = int(match.get('size', 0) or 0)
        repair_progress = None

        try:
            torrent_info = libtorrent.torrent_info(str(match['torrent_path']))
            session = libtorrent.session()
            matched_index, _ = self._find_matched_torrent_file_index(torrent_info, match)
            file_piece_spans = _get_libtorrent_file_piece_spans(torrent_info, matched_index, expected_size)
            baseline_valid_piece_indexes = self._scan_local_valid_piece_indexes(
                torrent_info,
                match,
                failed_path.parent,
                failed_path,
                matched_index,
                file_piece_spans,
            )
            baseline_file_ok_bytes = _get_libtorrent_file_ok_bytes(
                None,
                file_piece_spans,
                expected_size,
                baseline_valid_piece_indexes=baseline_valid_piece_indexes,
            ) or 0
            estimated_repair_bytes, missing_piece_count = _estimate_missing_piece_repair_bytes(
                torrent_info,
                file_piece_spans,
                baseline_valid_piece_indexes,
            )
            if expected_size > 0 and baseline_file_ok_bytes >= expected_size:
                verified, verify_status = self._verify_file_against_torrent_source(failed_filepath, match, torrent_recovery)
                if verified:
                    return True, 'File already fully backed by valid torrent pieces'
            try:
                _configure_libtorrent_session(session)
            except Exception:
                pass

            params = self._build_matched_torrent_params(torrent_info, match, failed_path.parent, failed_path)
            if baseline_valid_piece_indexes:
                piece_bitfield = _build_libtorrent_piece_bitfield(int(torrent_info.num_pieces()), baseline_valid_piece_indexes)
                if piece_bitfield:
                    try:
                        params.have_pieces = piece_bitfield
                    except Exception:
                        pass
                    try:
                        params.verified_pieces = piece_bitfield
                    except Exception:
                        pass

            handle = session.add_torrent(params)

            try:
                _enable_sequential_download(handle)
            except Exception:
                pass

            try:
                handle.resume()
            except Exception:
                pass

            try:
                _call_libtorrent_without_deprecation_warnings(handle.force_recheck)
            except Exception:
                pass

            self._configure_matched_torrent_file(handle, torrent_info, match, failed_path)

            timeout_seconds = int(torrent_recovery.get('timeout_seconds') or 120)
            deadline = time.time() + timeout_seconds
            progress_total = estimated_repair_bytes if estimated_repair_bytes > 0 else max(expected_size, 1)
            repair_progress = self._create_crc_progress_bar(failed_filepath, progress_total, label='Repair')
            last_status_message = 'Torrent repair timed out before the file reached a verified complete state.'
            next_verification_attempt_at = 0.0

            if expected_size > 0 and self.verbose >= 1:
                on_disk_ratio = min(local_file_size / expected_size, 1.0) if expected_size else 0.0
                baseline_file_ok_ratio = min(baseline_file_ok_bytes / expected_size, 1.0) if expected_size else 0.0
                self._log_progress_message(
                    f"Torrent repair baseline for {failed_path.name}: local-file="
                    f"{_format_byte_size(local_file_size)}/{_format_byte_size(expected_size)} ({on_disk_ratio:.2%}), "
                    f"file-ok={_format_byte_size(baseline_file_ok_bytes)}/{_format_byte_size(expected_size)} ({baseline_file_ok_ratio:.2%})",
                    1
                )
                if missing_piece_count > 0:
                    self._log_progress_message(
                        f"Estimated repair download: {_format_byte_size(estimated_repair_bytes)} across {missing_piece_count} missing torrent piece(s)",
                        1
                    )

            while time.time() < deadline:
                status = _get_libtorrent_status(handle)
                progress, completed_bytes, total_bytes, error_text = self._extract_torrent_status_progress(status, expected_size)
                error_text = error_text or _format_libtorrent_error(getattr(status, 'errc', None))

                file_ok_bytes = _get_libtorrent_file_ok_bytes(
                    status,
                    file_piece_spans,
                    expected_size,
                    baseline_valid_piece_indexes=baseline_valid_piece_indexes,
                )

                if repair_progress is not None:
                    progress_value = completed_bytes
                    progress_limit = progress_total
                    if estimated_repair_bytes > 0:
                        try:
                            payload_downloaded = int(getattr(status, 'total_payload_download', 0) or 0)
                        except Exception:
                            payload_downloaded = completed_bytes
                        progress_value = payload_downloaded
                    elif file_ok_bytes is not None:
                        progress_value = file_ok_bytes

                    self._update_crc_progress_bar(repair_progress, progress_value, progress_limit)
                    postfix_parts = []
                    if file_ok_bytes is not None and expected_size > 0:
                        postfix_parts.append(f"file-ok {file_ok_bytes / expected_size:.2%}")
                    try:
                        download_rate = int(getattr(status, 'download_rate', 0) or 0)
                    except Exception:
                        download_rate = 0
                    if download_rate > 0:
                        postfix_parts.append(f"{download_rate / (1024 * 1024):.2f} MiB/s")
                    try:
                        num_peers = int(getattr(status, 'num_peers', 0) or 0)
                    except Exception:
                        num_peers = 0
                    if num_peers > 0:
                        postfix_parts.append(f"peers {num_peers}")
                    if error_text:
                        postfix_parts.append(f"error {error_text}")
                    if postfix_parts:
                        repair_progress.set_postfix_str(' | '.join(postfix_parts))

                if error_text:
                    last_status_message = f'Torrent session error: {error_text}'
                    break

                verification_threshold_reached = (
                    (file_ok_bytes is not None and expected_size > 0 and file_ok_bytes >= expected_size)
                    or progress >= 0.999
                )
                if verification_threshold_reached and time.time() >= next_verification_attempt_at:
                    verified, verify_status = self._verify_file_against_torrent_source(failed_filepath, match, torrent_recovery)
                    if verified:
                        try:
                            session.remove_torrent(handle)
                        except Exception:
                            pass
                        return True, 'Recovered in place via torrent pieces'

                    last_status_message = (
                        f'Repair reached completion threshold, but post-repair verification returned {verify_status}; '
                        'continuing until timeout.'
                    )
                    next_verification_attempt_at = time.time() + 1.0

                if file_ok_bytes is not None and expected_size > 0:
                    last_status_message = (
                        f"Torrent repair progress: file-ok {file_ok_bytes / expected_size:.2%} "
                        f"({_format_byte_size(file_ok_bytes)}/{_format_byte_size(expected_size)})"
                    )
                else:
                    last_status_message = f'Torrent repair progress: {progress:.2%}'
                time.sleep(0.5)

            try:
                session.remove_torrent(handle)
            except Exception:
                pass
            return False, last_status_message
        except Exception as exc:
            self._log(f"libtorrent recovery failed for {failed_filepath}: {exc}", 1)
            return False, f'libtorrent recovery failed: {exc}'
        finally:
            if repair_progress is not None:
                repair_progress.close()

    def _attempt_libtorrent_session_recovery(
        self,
        failed_filepath: str,
        expected_crc: str,
        match: Dict[str, Any],
        torrent_recovery: Dict[str, Any]
    ) -> Tuple[bool, str]:
        """Best-effort torrent session recovery using libtorrent when local files are unavailable."""
        if not LIBTORRENT_AVAILABLE:
            return False, 'Matched torrent metadata was found, but libtorrent is not available.'

        failed_path = Path(failed_filepath)
        save_path = failed_path.parent

        try:
            torrent_info = libtorrent.torrent_info(str(match['torrent_path']))
            session = libtorrent.session()
            try:
                _configure_libtorrent_session(session)
            except Exception:
                pass

            handle = session.add_torrent(
                self._build_matched_torrent_params(torrent_info, match, save_path, failed_path)
            )

            try:
                _enable_sequential_download(handle)
            except Exception:
                pass

            try:
                handle.resume()
            except Exception:
                pass

            try:
                _call_libtorrent_without_deprecation_warnings(handle.force_recheck)
            except Exception:
                pass

            self._configure_matched_torrent_file(handle, torrent_info, match, failed_path)

            timeout_seconds = int(torrent_recovery.get('timeout_seconds') or 120)
            deadline = time.time() + timeout_seconds
            last_status_message = 'Matched torrent metadata was found, but the torrent session did not produce a CRC-valid file before timeout.'

            while time.time() < deadline:
                try:
                    is_valid, _, actual_crc = self._verify_file_crc(str(failed_path), expected_crc)
                except Exception:
                    is_valid = False
                    actual_crc = 'ERROR'

                if is_valid:
                    try:
                        session.remove_torrent(handle)
                    except Exception:
                        pass
                    return True, f"Recovered in place via torrent session (CRC {actual_crc})"

                try:
                    status = handle.status()
                    state_name = getattr(status, 'state', None)
                    progress = getattr(status, 'progress', 0.0)
                    last_status_message = (
                        f"Torrent session reached state {state_name} with progress {progress:.2%}, "
                        'but no CRC-valid file was recovered.'
                    )
                    if getattr(status, 'errc', None):
                        error_text = _format_libtorrent_error(status.errc)
                        if error_text:
                            last_status_message = f'Torrent session error: {error_text}'
                except Exception:
                    pass

                time.sleep(1.0)

            try:
                session.remove_torrent(handle)
            except Exception:
                pass
            return False, last_status_message
        except Exception as exc:
            self._log(f"libtorrent recovery failed for {failed_filepath}: {exc}", 1)
            return False, f'libtorrent recovery failed: {exc}'

    def _find_matched_torrent_file_index(self, torrent_info: Any, match: Dict[str, Any]) -> Tuple[Optional[int], List[int]]:
        """Locate the matched torrent file index and build file priorities.

        Keep any files that share torrent pieces with the matched file enabled at low
        priority, otherwise boundary pieces can appear incomplete even when the target
        file itself is the only one we intend to repair.
        """
        try:
            file_paths = self._get_torrent_file_paths_in_order(str(match['torrent_path']))
        except Exception:
            return None, []

        if not file_paths:
            return None, []

        target_relative_path = str(Path(match['relative_path']))
        matched_index = None
        for index, current_path in enumerate(file_paths):
            is_match = current_path == target_relative_path or os.path.basename(current_path) == match['display_name']
            if is_match and matched_index is None:
                matched_index = index

        if matched_index is None:
            return None, []

        piece_sharing_indexes = set()
        matched_size = int(match.get('size', 0) or 0)
        if matched_size > 0:
            try:
                file_piece_spans = _get_libtorrent_file_piece_spans(torrent_info, matched_index, matched_size)
                for piece_index, _ in file_piece_spans:
                    piece_size = int(torrent_info.piece_size(piece_index))
                    for file_slice in torrent_info.map_block(piece_index, 0, piece_size):
                        file_index = int(file_slice.file_index)
                        if 0 <= file_index < len(file_paths):
                            piece_sharing_indexes.add(file_index)
            except Exception:
                piece_sharing_indexes.clear()

        priorities = []
        for index in range(len(file_paths)):
            if index == matched_index:
                priorities.append(7)
            elif index in piece_sharing_indexes:
                priorities.append(1)
            else:
                priorities.append(0)

        return matched_index, priorities

    def _prioritize_matched_torrent_file(self, handle: Any, torrent_info: Any, match: Dict[str, Any]) -> None:
        """Bias libtorrent toward the matched file when file priorities are supported."""
        matched_index, priorities = self._find_matched_torrent_file_index(torrent_info, match)
        if matched_index is None:
            return

        try:
            handle.prioritize_files(priorities)
        except Exception:
            pass

    def _configure_matched_torrent_file(self, handle: Any, torrent_info: Any, match: Dict[str, Any], failed_path: Path) -> None:
        """Bias libtorrent toward the matched file and map it to the broken file in place."""
        matched_index, priorities = self._find_matched_torrent_file_index(torrent_info, match)
        if matched_index is None:
            return

        priorities, rename_targets = self._prepare_in_place_torrent_targets(
            torrent_info,
            match,
            failed_path.parent,
            failed_path,
            matched_index,
            priorities,
        )

        try:
            handle.prioritize_files(priorities)
        except Exception:
            pass

        for file_index, target_path in rename_targets.items():
            try:
                handle.rename_file(file_index, target_path)
            except Exception:
                pass

    def _prepare_in_place_torrent_targets(
        self,
        torrent_info: Any,
        match: Dict[str, Any],
        save_path: Path,
        failed_path: Path,
        matched_index: int,
        priorities: List[int],
    ) -> Tuple[List[int], Dict[int, str]]:
        """Map enabled torrent files onto real local paths and disable helper files with no in-place target."""
        adjusted_priorities = list(priorities)
        rename_targets: Dict[int, str] = {}

        try:
            file_paths = self._get_torrent_file_paths_in_order(str(match['torrent_path']))
        except Exception:
            file_paths = []

        torrent_name = str(match.get('torrent_name') or '')
        try:
            rename_targets[matched_index] = str(failed_path.resolve())
        except Exception:
            rename_targets[matched_index] = str(failed_path)

        for file_index, priority in enumerate(adjusted_priorities):
            if priority <= 0 or file_index == matched_index:
                continue

            relative_path = file_paths[file_index] if file_index < len(file_paths) else ''
            resolved_path = self._resolve_in_place_target_path(
                save_path,
                torrent_name,
                relative_path,
            )
            if resolved_path == failed_path:
                adjusted_priorities[file_index] = 0
                continue

            try:
                rename_targets[file_index] = str(resolved_path.resolve())
            except Exception:
                rename_targets[file_index] = str(resolved_path)

        return adjusted_priorities, rename_targets

    def _build_matched_torrent_params(
        self,
        torrent_info: Any,
        match: Dict[str, Any],
        save_path: Path,
        failed_path: Optional[Path] = None
    ) -> Any:
        """Create add_torrent_params seeded with matched-file priorities and optional rename mapping."""
        params = _create_add_torrent_params(torrent_info, save_path)
        matched_index, priorities = self._find_matched_torrent_file_index(torrent_info, match)
        rename_targets: Dict[int, str] = {}
        if matched_index is not None and failed_path is not None:
            priorities, rename_targets = self._prepare_in_place_torrent_targets(
                torrent_info,
                match,
                save_path,
                failed_path,
                matched_index,
                priorities,
            )
        _apply_match_to_add_torrent_params(params, priorities, rename_targets)
        return params

    def _run_torrent_recovery_for_crc_failures(
        self,
        crc_results: Dict[str, Dict],
        torrent_recovery: Optional[Dict[str, Optional[str]]] = None
    ) -> None:
        """Look up and attempt recovery for files that failed integrity validation."""
        if not torrent_recovery or not torrent_recovery.get('enabled'):
            return

        failed_files = {
            filepath: result
            for filepath, result in crc_results.items()
            if result.get('expected_crc') != 'N/A' and not result.get('is_valid', False)
        }

        if not failed_files:
            return

        if not torrent_recovery.get('auto_repair'):
            _safe_console_print(f"\n{self._color('Torrent recovery is ready to modify failed files.', Colors.YELLOW + Colors.BOLD)}")
            _safe_console_print(f"  Failed files queued: {self._color(str(len(failed_files)), Colors.WHITE)}")
            try:
                response = input('Proceed with torrent-based repair? [y/N]: ').strip().lower()
            except EOFError:
                response = ''

            if response not in {'y', 'yes'}:
                _safe_console_print(self._color('Torrent recovery skipped by user.', Colors.YELLOW))
                return

        _safe_console_print(f"\n{self._color('=== TORRENT RECOVERY REQUESTED ===', Colors.CYAN + Colors.BOLD)}")
        _safe_console_print(f"  Failed files queued: {self._color(str(len(failed_files)), Colors.WHITE)}")
        _safe_console_print(f"  Torrent metadata path: {self._color(str(torrent_recovery.get('torrent_files_path')), Colors.WHITE)}")

        recovered_count = 0
        matched_count = 0
        no_match_count = 0

        for failed_filepath, failed_result in failed_files.items():
            failed_filename = failed_result.get('filename') or os.path.basename(failed_filepath)
            best_match = failed_result.get('torrent_match')
            inspected_count = failed_result.get('inspected_torrent_count', 0)
            if not best_match:
                best_match, inspected_count = self._find_best_torrent_match(
                    failed_filepath,
                    failed_result,
                    torrent_recovery.get('torrent_files_path')
                )

            if not best_match:
                no_match_count += 1
                _safe_console_print(
                    f"  {self._color('[no-match]', Colors.YELLOW)} No torrent match found for "
                    f"{self._color(failed_filename, Colors.WHITE)} after inspecting "
                    f"{self._color(str(inspected_count), Colors.WHITE)} torrent files"
                )
                continue

            matched_count += 1
            score_color = Colors.GREEN if best_match['total_score'] >= 0.9 else Colors.YELLOW
            score_text = self._color(f"{best_match['total_score']:.2f}", score_color)
            _safe_console_print(
                f"  {self._color('[match]', Colors.CYAN)} Match for {self._color(failed_filename, Colors.WHITE)}: "
                f"{self._color(best_match['display_name'], Colors.CYAN)} in "
                f"{self._color(os.path.basename(best_match['torrent_path']), Colors.WHITE)} "
                f"(score {score_text})"
            )

            recovered, message = self._attempt_torrent_file_recovery(
                failed_filepath,
                failed_result,
                best_match,
                torrent_recovery
            )
            if recovered:
                recovered_count += 1
                _safe_console_print(f"    {self._color('[recovered]', Colors.GREEN)} {message}")
            else:
                _safe_console_print(f"    {self._color('[unresolved]', Colors.YELLOW)} {message}")

        _safe_console_print(f"\n{self._color('Torrent Recovery Summary:', Colors.CYAN)}")
        _safe_console_print(f"  Matched torrents: {self._color(str(matched_count), Colors.GREEN if matched_count else Colors.YELLOW)}")
        _safe_console_print(f"  Recovered files: {self._color(str(recovered_count), Colors.GREEN if recovered_count else Colors.YELLOW)}")
        _safe_console_print(f"  No torrent match: {self._color(str(no_match_count), Colors.YELLOW)}")
    
    def check_files_crc(self, files_or_groups, is_groups: bool = False, torrent_recovery: Optional[Dict[str, Any]] = None) -> Dict[str, Dict]:
        """
        Check CRC32 of files.
        
        Args:
            files_or_groups: List of file paths or dict of group data
            is_groups: If True, treats input as groups data from JSON
            
        Returns:
            Dict with CRC check results
        """
        results = {}
        files_to_check: List[Dict[str, str]] = []
        
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
            # Direct file list or file metadata records
            for file_entry in files_or_groups:
                if isinstance(file_entry, dict):
                    filepath = file_entry.get('filepath') or file_entry.get('dest_path') or file_entry.get('source_path')
                    if not filepath or not os.path.isfile(filepath):
                        continue

                    files_to_check.append({
                        'filepath': filepath,
                        'filename': file_entry.get('filename', os.path.basename(filepath)),
                        'group': file_entry.get('group') or file_entry.get('group_title', 'Standalone'),
                        'group_key': file_entry.get('group_key', 'standalone'),
                        'source_path': file_entry.get('source_path')
                    })
                    continue

                filepath = file_entry
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
        
        # Check file integrity with progress
        desc = "Checking file integrity"
        total_files = len(files_to_check)
        with tqdm(total=total_files, desc=desc, unit="file", disable=self.verbose == 0, position=0) as pbar:
            for index, file_info in enumerate(files_to_check, start=1):
                filepath = file_info['filepath']
                filename = file_info['filename']

                result = self._check_file_integrity(filepath, filename=filename, torrent_recovery=torrent_recovery)
                status = result['status']
                if status == "no_crc":
                    no_crc_count += 1
                elif result['is_valid']:
                    valid_count += 1
                else:
                    invalid_count += 1

                results[filepath] = {
                    'filename': filename,
                    'group': file_info['group'],
                    'group_key': file_info['group_key'],
                    **result
                }
                if file_info.get('source_path'):
                    results[filepath]['source_path'] = file_info['source_path']

                if self.verbose >= 1:
                    short_filename = filename[:32] + "..." if len(filename) > 35 else filename
                    pbar.set_postfix_str(f"{status}: {short_filename}")
                pbar.update(1)
                
                # Log issues
                if status == "invalid":
                    expected_crc = results[filepath]['expected_crc']
                    actual_crc = results[filepath]['actual_crc']
                    if results[filepath].get('verification_source') == 'torrent':
                        self._log_progress_message(f"TORRENT VERIFY FAILED: {filename} (Status: {actual_crc})", 1)
                    else:
                        self._log_progress_message(f"CRC MISMATCH: {filename} (Expected: {expected_crc}, Actual: {actual_crc})", 1)
                elif status == "no_crc" and self.verbose >= 2:
                    self._log_progress_message(f"No CRC in filename: {filename}", 2)
        
        # Print summary
        _safe_console_print(f"\n{self._color('Integrity Check Summary:', Colors.CYAN + Colors.BOLD)}")
        _safe_console_print(f"  Total files: {self._color(str(total_files), Colors.WHITE)}")
        _safe_console_print(f"  Valid: {self._color(str(valid_count), Colors.GREEN)}")
        _safe_console_print(f"  Invalid: {self._color(str(invalid_count), Colors.RED if invalid_count > 0 else Colors.GREEN)}")
        _safe_console_print(f"  No filename CRC or torrent match: {self._color(str(no_crc_count), Colors.YELLOW)}")
        
        if invalid_count > 0:
            _safe_console_print(f"\n{self._color(f'⚠️  {invalid_count} files failed integrity validation!', Colors.RED + Colors.BOLD)}")
        elif valid_count > 0:
            _safe_console_print(f"\n{self._color(f'✅ All {valid_count} checked files passed validation!', Colors.GREEN + Colors.BOLD)}")
        
        return results
    
    def archive_groups(self, selected_groups: List[str], destination_root: str, 
                      copy_files: bool = False, dry_run: bool = False, verify_crc: bool = False,
                      torrent_recovery: Optional[Dict[str, Optional[str]]] = None) -> Dict[str, str]:
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

        # Estimate sizes per-group and check destination available space
        group_sizes = {}
        total_bytes_needed = 0
        for group_key in selected_groups:
            group_data = self.groups.get(group_key)
            size_bytes = 0
            if group_data:
                for file_info in group_data.get('files', []):
                    source_path = file_info.get('filepath')
                    if source_path and os.path.exists(source_path):
                        try:
                            size_bytes += os.path.getsize(source_path)
                        except Exception:
                            pass
            group_sizes[group_key] = size_bytes
            total_bytes_needed += size_bytes

        def _human_size(n: int) -> str:
            for unit in ['B','KB','MB','GB','TB']:
                if n < 1024.0:
                    return f"{n:3.1f}{unit}"
                n /= 1024.0
            return f"{n:.1f}PB"

        # Determine an existing path to get disk usage (walk up until existing)
        existing_path = destination_root
        p = Path(destination_root)
        while not p.exists():
            if p.parent == p:
                break
            p = p.parent
        existing_path = str(p)

        try:
            disk_usage = shutil.disk_usage(existing_path)
            available_bytes = disk_usage.free
        except Exception:
            available_bytes = 0

        # Print per-group estimated sizes and destination info
        print(f"\n{self._color('Destination summary:', Colors.CYAN + Colors.BOLD)}")
        print(f"  Root: {self._color(destination_root, Colors.WHITE)}")
        print(f"  Available on target ({existing_path}): {self._color(_human_size(available_bytes), Colors.YELLOW)}")
        print(f"  Total needed: {self._color(_human_size(total_bytes_needed), Colors.MAGENTA)} for {self._color(str(total_files), Colors.WHITE)} files")
        for group_key in selected_groups:
            group_data = self.groups.get(group_key)
            folder_name = self.generate_folder_name(group_data) if group_data else 'Unknown'
            size = group_sizes.get(group_key, 0)
            print(f"    - {self._color(folder_name, Colors.CYAN)}: {self._color(_human_size(size), Colors.WHITE)}")

        # Warn if not enough space
        if total_bytes_needed > available_bytes:
            warn_msg = f"Not enough free space on target to archive all selected series ({_human_size(total_bytes_needed)} needed, {_human_size(available_bytes)} available)."
            _safe_console_print(f"\n{self._color('⚠️  WARNING:', Colors.RED + Colors.BOLD)} {self._color(warn_msg, Colors.RED)}")
            if dry_run:
                print(self._color('Dry-run: no changes will be made, but space is insufficient for a real run.', Colors.YELLOW))
            else:
                # Prompt user for confirmation before proceeding
                try:
                    resp = input('Proceed anyway? [y/N]: ').strip().lower()
                except Exception:
                    resp = 'n'
                if resp not in ('y', 'yes'):
                    print('Aborted by user.')
                    return results

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
            folder_emoji = get_emoji('folder') or "📁"
            _safe_console_print(f"\n{folder_emoji} {action_word} group: {self._color(group_title, Colors.CYAN + Colors.BOLD)}")
            self._log(f"   Destination: {self._color(folder_name, Colors.CYAN)}")
            
            # Process files
            files = group_data.get('files', [])
            success_count = 0
            error_count = 0
            newest_file_time = None
            
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
                        # Track newest file time even in dry run
                        if os.path.exists(source_path):
                            file_mtime = os.path.getmtime(source_path)
                            if newest_file_time is None or file_mtime > newest_file_time:
                                newest_file_time = file_mtime
                    else:
                        # Track newest file time before moving
                        if os.path.exists(source_path):
                            file_mtime = os.path.getmtime(source_path)
                            if newest_file_time is None or file_mtime > newest_file_time:
                                newest_file_time = file_mtime
                        
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
                                'group_title': group_title,
                                'group_key': group_key
                            })
                    
                    success = True
                    success_count += 1
                    
                except Exception as e:
                    error_msg = str(e)
                    error_count += 1
                
                # Notify progress reporter of file completion
                if self.progress_reporter:
                    self.progress_reporter.on_file_processed(filename, success, error_msg)
            
            # Set folder modified date to newest file date
            if not dry_run and newest_file_time is not None and os.path.exists(dest_folder):
                try:
                    os.utime(dest_folder, (newest_file_time, newest_file_time))
                    date_str = datetime.fromtimestamp(newest_file_time).strftime('%Y-%m-%d')
                    self._log(f"   📅 Folder date set to: {self._color(date_str, Colors.YELLOW)}", 2)
                except Exception as e:
                    self._log(f"   {self._color('⚠️', Colors.YELLOW)} Could not set folder date: {e}", 1)
            
            # Notify progress reporter of group completion
            if self.progress_reporter:
                self.progress_reporter.on_group_complete(group_title, success_count, error_count)
            else:
                # Fallback output if no progress reporter
                status_word = "Would process" if dry_run else "Processed"
                _safe_console_print(f"   {self._color('✅', Colors.GREEN)} {status_word} {self._color(str(success_count), Colors.GREEN)} files successfully")
                if error_count > 0:
                    _safe_console_print(f"   {self._color('❌', Colors.RED)} {error_count} files had errors")
            
            # Store folder path and newest file date
            results[group_key] = {
                'folder_path': dest_folder,
                'newest_file_date': datetime.fromtimestamp(newest_file_time).strftime('%Y-%m-%d') if newest_file_time else None
            }
        
        # Notify progress reporter of completion
        if self.progress_reporter:
            self.progress_reporter.on_complete(len(selected_groups))
        
        # Verify CRC after operations if requested
        if verify_crc:
            # Normal run: verify CRC on destination files we processed
            if not dry_run and processed_files:
                _safe_console_print(f"\n{self._color('=== CRC VERIFICATION ===', Colors.CYAN + Colors.BOLD)}")
                crc_results = self.check_files_crc([
                    {
                        'filepath': file_info['dest_path'],
                        'filename': file_info['filename'],
                        'group': file_info['group_title'],
                        'group_key': file_info.get('group_key', 'archived'),
                        'source_path': file_info['source_path']
                    }
                    for file_info in processed_files
                ], torrent_recovery=torrent_recovery)
                self._run_torrent_recovery_for_crc_failures(crc_results, torrent_recovery)
            # Dry-run: verify CRC on source files and report status
            elif dry_run:
                _safe_console_print(f"\n{self._color('=== DRY-RUN CRC CHECK (source files) ===', Colors.CYAN + Colors.BOLD)}")
                crc_results = {}
                files_to_check = []
                for group_key in selected_groups:
                    group_data = self.groups.get(group_key)
                    group_title = group_data.get('title') if group_data else group_key
                    for file_info in (group_data.get('files', []) if group_data else []):
                        source_path = file_info.get('filepath')
                        filename = file_info.get('filename', os.path.basename(source_path) if source_path else 'Unknown')
                        if source_path and os.path.exists(source_path):
                            files_to_check.append((source_path, filename, group_title))

                if not files_to_check:
                    _safe_console_print(self._color('No existing source files found to check CRC in dry-run.', Colors.YELLOW))
                else:
                    iterator = files_to_check
                    if TQDM_AVAILABLE:
                        iterator = tqdm(files_to_check, desc='Checking CRC (dry-run)', unit='file', disable=self.verbose == 0)

                    for source_path, filename, group_title in iterator:
                        if self.verbose >= 1 and TQDM_AVAILABLE:
                            try:
                                iterator.set_postfix_str(filename[:40] + '...' if len(filename) > 40 else filename)
                            except Exception:
                                pass
                        is_valid, expected_crc, actual_crc = self._verify_file_crc(source_path, torrent_recovery=torrent_recovery)
                        if expected_crc != 'N/A':
                            crc_results[source_path] = {
                                'filename': filename,
                                'group': group_title,
                                'source_path': source_path,
                                'is_valid': is_valid,
                                'expected_crc': expected_crc,
                                'actual_crc': actual_crc
                            }
                            if is_valid:
                                _safe_console_print(f"   {self._color('✅', Colors.GREEN)} {filename} (CRC OK)")
                            else:
                                _safe_console_print(f"   {self._color('⚠️  CRC MISMATCH:', Colors.RED)} {filename} (Expected: {expected_crc}, Actual: {actual_crc})")

                    # Summary
                    if crc_results:
                        total_checked = len(crc_results)
                        valid_count = sum(1 for r in crc_results.values() if r['is_valid'])
                        invalid_count = total_checked - valid_count
                        _safe_console_print(f"\n{self._color('Dry-run CRC Summary:', Colors.CYAN)}")
                        _safe_console_print(f"  Files checked: {self._color(str(total_checked), Colors.WHITE)}")
                        _safe_console_print(f"  Valid: {self._color(str(valid_count), Colors.GREEN)}")
                        _safe_console_print(f"  Invalid: {self._color(str(invalid_count), Colors.RED if invalid_count > 0 else Colors.GREEN)}")
        
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


def _add_torrent_recovery_arguments(parser) -> None:
    """Add shared torrent recovery arguments to a subcommand parser."""
    parser.add_argument('--recover-by-torrent', action='store_true',
                       help='Attempt torrent-based recovery for files that fail CRC validation')
    parser.add_argument('--auto-repair-by-torrent', action='store_true',
                       help='Repair failed files without prompting when torrent recovery is enabled')
    parser.add_argument('--torrent-files-path', metavar='DIR',
                       help='Directory containing .torrent files to search for recovery matches')
    parser.add_argument('--torrent-recovery-timeout', metavar='SECONDS', type=int, default=120,
                       help='Maximum time to wait for torrent-session recovery before giving up (default: 120)')


def _get_torrent_recovery_options(args, require_verify_crc: bool = False) -> Optional[Dict[str, Any]]:
    """Normalize and validate torrent recovery CLI options."""
    recover_by_torrent = bool(getattr(args, 'recover_by_torrent', False))
    auto_repair_by_torrent = bool(getattr(args, 'auto_repair_by_torrent', False))
    torrent_files_path = getattr(args, 'torrent_files_path', None)
    torrent_recovery_timeout = getattr(args, 'torrent_recovery_timeout', 120)

    if not recover_by_torrent and (auto_repair_by_torrent or torrent_files_path):
        raise ValueError(
            'Torrent recovery options require --recover-by-torrent.'
        )

    if not recover_by_torrent:
        return None

    if not LIBTORRENT_AVAILABLE:
        raise ValueError(
            '--recover-by-torrent requires the optional libtorrent package, but it is not available in the current Python environment.'
        )

    if require_verify_crc and not getattr(args, 'verify_crc', False):
        raise ValueError(
            '--recover-by-torrent requires --verify-crc when used with the archive command.'
        )

    if not torrent_files_path:
        raise ValueError(
            '--recover-by-torrent requires --torrent-files-path.'
        )

    if not Path(torrent_files_path).exists():
        raise ValueError(
            f"Torrent files path not found: {torrent_files_path}"
        )

    if torrent_recovery_timeout is None or torrent_recovery_timeout <= 0:
        raise ValueError(
            '--torrent-recovery-timeout must be a positive integer.'
        )

    return {
        'enabled': True,
        'auto_repair': auto_repair_by_torrent,
        'torrent_files_path': torrent_files_path,
        'timeout_seconds': torrent_recovery_timeout
    }


def _normalize_datetime(dt: datetime) -> datetime:
    """Convert timezone-aware datetimes to local naive datetimes for safe comparisons."""
    if dt.tzinfo is not None:
        return dt.astimezone().replace(tzinfo=None)
    return dt


def _parse_smart_datetime(value: str) -> Tuple[Optional[datetime], bool]:
    """Parse datetime text (ISO preferred) and return (datetime, is_date_only)."""
    text = (value or '').strip()
    if not text:
        return None, False

    lowered = text.lower()
    now = datetime.now()
    if lowered == 'now':
        return now, False
    if lowered == 'today':
        return datetime(now.year, now.month, now.day), True
    if lowered == 'yesterday':
        today = datetime(now.year, now.month, now.day)
        return today - timedelta(days=1), True
    if lowered == 'tomorrow':
        today = datetime(now.year, now.month, now.day)
        return today + timedelta(days=1), True

    is_date_only = bool(re.fullmatch(r'\d{4}-\d{2}-\d{2}', text))

    # ISO-first parsing, including trailing Z
    iso_candidate = text[:-1] + '+00:00' if text.endswith('Z') else text
    try:
        parsed = datetime.fromisoformat(iso_candidate)
        return _normalize_datetime(parsed), is_date_only
    except ValueError:
        pass

    # Common fallback formats
    formats = [
        '%Y/%m/%d',
        '%Y.%m.%d',
        '%Y%m%d',
        '%Y-%m-%d %H:%M',
        '%Y-%m-%d %H:%M:%S',
        '%Y/%m/%d %H:%M',
        '%Y/%m/%d %H:%M:%S',
        '%d.%m.%Y',
        '%d.%m.%Y %H:%M',
        '%d.%m.%Y %H:%M:%S'
    ]

    for fmt in formats:
        try:
            parsed = datetime.strptime(text, fmt)
            date_only = fmt in ('%Y/%m/%d', '%Y.%m.%d', '%Y%m%d', '%d.%m.%Y')
            return parsed, date_only
        except ValueError:
            continue

    # Unix timestamp fallback (seconds or milliseconds)
    if re.fullmatch(r'\d{10,13}', text):
        try:
            timestamp = int(text)
            if len(text) == 13:
                timestamp = timestamp / 1000
            return datetime.fromtimestamp(timestamp), False
        except (ValueError, OSError):
            pass

    return None, False


def _parse_numeric_expression(expression: str, argument_name: str) -> Tuple[str, int]:
    """Parse expressions like '<12' or '>=24' for integer filters."""
    expr = (expression or '').strip()
    match = re.match(r'^(<=|>=|<|>|==|=|!=)\s*(.+)$', expr)

    if match:
        operator = match.group(1)
        raw_value = match.group(2).strip()
    else:
        operator = '='
        raw_value = expr

    if not re.fullmatch(r'-?\d+', raw_value):
        raise ValueError(
            f"Invalid {argument_name} expression '{expression}'. "
            f"Use integer values like '<12', '>=24', or '=13'."
        )

    return operator, int(raw_value)


def _parse_numeric_conditions(expression: str, argument_name: str) -> List[Tuple[str, int]]:
    """Parse integer filters into one or more conditions."""
    expr = (expression or '').strip()
    if not expr:
        raise ValueError(f"{argument_name} cannot be empty")

    if '..' in expr:
        parts = expr.split('..')
        if len(parts) != 2:
            raise ValueError(
                f"Invalid {argument_name} range '{expression}'. "
                "Use format like '12..24'."
            )

        start_raw, end_raw = parts[0].strip(), parts[1].strip()
        if not start_raw or not end_raw:
            raise ValueError(
                f"Invalid {argument_name} range '{expression}'. "
                "Both start and end values are required."
            )

        start_condition = _parse_numeric_expression(f">={start_raw}", argument_name)
        end_condition = _parse_numeric_expression(f"<={end_raw}", argument_name)

        lower_bound = start_condition[1]
        upper_bound = end_condition[1]
        if lower_bound > upper_bound:
            raise ValueError(
                f"Invalid {argument_name} range '{expression}'. "
                "Range start must be less than or equal to range end."
            )

        return [start_condition, end_condition]

    if ',' in expr:
        parts = [part.strip() for part in expr.split(',') if part.strip()]
        if not parts:
            raise ValueError(f"{argument_name} cannot be empty")
        return [_parse_numeric_expression(part, argument_name) for part in parts]

    return [_parse_numeric_expression(expr, argument_name)]


def _matches_numeric_expression(value: int, operator: str, target_value: int) -> bool:
    """Evaluate a parsed numeric expression against an integer value."""
    if operator in ('=', '=='):
        return value == target_value
    if operator == '!=':
        return value != target_value
    if operator == '<':
        return value < target_value
    if operator == '<=':
        return value <= target_value
    if operator == '>':
        return value > target_value
    if operator == '>=':
        return value >= target_value

    return False


def _parse_modified_expression(expression: str) -> Tuple[str, datetime, bool]:
    """Parse expressions like '<2026-01-01' or '>=2026-01-01T12:00'."""
    expr = (expression or '').strip()
    match = re.match(r'^(<=|>=|<|>|==|=|!=)\s*(.+)$', expr)

    if match:
        operator = match.group(1)
        raw_value = match.group(2).strip()
    else:
        operator = '='
        raw_value = expr

    parsed_dt, is_date_only = _parse_smart_datetime(raw_value)
    if parsed_dt is None:
        raise ValueError(
            f"Invalid --modified expression '{expression}'. "
            "Use forms like '<2026-01-01', '>=2026-01-01T15:30', '=2026-01-01'."
        )

    return operator, parsed_dt, is_date_only


def _parse_modified_conditions(expression: str) -> List[Tuple[str, datetime, bool]]:
    """Parse modified filter into one or more conditions.

    Supported forms:
    - Single expression: "<2026-01-01"
    - Closed range: "2026-01-01..2026-01-31"
    - Multiple expressions (AND): ">=2026-01-01, <2026-02-01"
    """
    expr = (expression or '').strip()
    if not expr:
        raise ValueError("--modified cannot be empty")

    if '..' in expr:
        parts = expr.split('..')
        if len(parts) != 2:
            raise ValueError(
                f"Invalid --modified range '{expression}'. "
                "Use format like '2026-01-01..2026-01-31'."
            )

        start_raw, end_raw = parts[0].strip(), parts[1].strip()
        if not start_raw or not end_raw:
            raise ValueError(
                f"Invalid --modified range '{expression}'. "
                "Both start and end values are required."
            )

        start_condition = _parse_modified_expression(f">={start_raw}")
        end_condition = _parse_modified_expression(f"<={end_raw}")

        lower_bound = _normalize_datetime(start_condition[1])
        upper_bound = _normalize_datetime(end_condition[1])
        if lower_bound > upper_bound:
            raise ValueError(
                f"Invalid --modified range '{expression}'. "
                "Range start must be earlier than or equal to range end."
            )

        return [start_condition, end_condition]

    if ',' in expr:
        parts = [part.strip() for part in expr.split(',') if part.strip()]
        if not parts:
            raise ValueError("--modified cannot be empty")
        return [_parse_modified_expression(part) for part in parts]

    return [_parse_modified_expression(expr)]


def _get_group_modified_datetime(group_data: Dict) -> Optional[datetime]:
    """Extract a group's modified datetime (prefers group avg timestamp, then newest file mtime)."""
    group_metadata = group_data.get('group_metadata', {}) or {}
    avg_modified_time = group_metadata.get('avg_modified_time')
    if isinstance(avg_modified_time, (int, float)):
        try:
            return datetime.fromtimestamp(avg_modified_time)
        except (ValueError, OSError, OverflowError):
            pass

    newest_mtime = None
    for file_info in group_data.get('files', []):
        source_path = file_info.get('filepath') or file_info.get('file_path')
        if not source_path:
            continue
        if not os.path.exists(source_path):
            continue

        try:
            file_mtime = os.path.getmtime(source_path)
            if newest_mtime is None or file_mtime > newest_mtime:
                newest_mtime = file_mtime
        except OSError:
            continue

    if newest_mtime is None:
        return None
    return datetime.fromtimestamp(newest_mtime)


def _matches_modified_expression(group_data: Dict, operator: str, target_dt: datetime, is_date_only: bool) -> bool:
    """Evaluate a parsed modified-date expression against group data."""
    group_modified_dt = _get_group_modified_datetime(group_data)
    if group_modified_dt is None:
        return False

    actual_dt = _normalize_datetime(group_modified_dt)
    expected_dt = _normalize_datetime(target_dt)

    if is_date_only and operator in ('=', '==', '!='):
        is_equal = actual_dt.date() == expected_dt.date()
        return (not is_equal) if operator == '!=' else is_equal

    if operator in ('=', '=='):
        return actual_dt == expected_dt
    if operator == '!=':
        return actual_dt != expected_dt
    if operator == '<':
        return actual_dt < expected_dt
    if operator == '<=':
        return actual_dt <= expected_dt
    if operator == '>':
        return actual_dt > expected_dt
    if operator == '>=':
        return actual_dt >= expected_dt

    return False


def cmd_list(args):
    """Handle the list command."""
    use_colors = not getattr(args, 'no_color', False)
    archiver = SeriesArchiver(verbose=args.verbose, use_colors=use_colors)
    title_length = 70
    
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
        all_watch_statuses = {'watched', 'watched_partial', 'unwatched'}
        
        # Parse include/exclude patterns
        status_filters = args.status_filter.split()
        include_statuses = set()
        exclude_statuses = set()
        plain_statuses = set()
        include_watch_statuses = set()
        exclude_watch_statuses = set()
        plain_watch_statuses = set()
        
        for filter_item in status_filters:
            if filter_item.startswith('+'):
                status = filter_item[1:]
                if status in all_statuses:
                    include_statuses.add(status)
                elif status in all_watch_statuses:
                    include_watch_statuses.add(status)
            elif filter_item.startswith('-'):
                status = filter_item[1:]
                if status in all_statuses:
                    exclude_statuses.add(status)
                elif status in all_watch_statuses:
                    exclude_watch_statuses.add(status)
            elif filter_item in all_statuses:
                plain_statuses.add(filter_item)
            elif filter_item in all_watch_statuses:
                plain_watch_statuses.add(filter_item)
        
        # Determine final filter sets
        # Completion status filter
        if plain_statuses:
            final_statuses = plain_statuses
        elif include_statuses:
            final_statuses = include_statuses - exclude_statuses
        elif exclude_statuses:
            final_statuses = all_statuses - exclude_statuses
        else:
            final_statuses = all_statuses
        
        # Watch status filter
        if plain_watch_statuses:
            final_watch_statuses = plain_watch_statuses
        elif include_watch_statuses:
            final_watch_statuses = include_watch_statuses - exclude_watch_statuses
        elif exclude_watch_statuses:
            final_watch_statuses = all_watch_statuses - exclude_watch_statuses
        else:
            final_watch_statuses = all_watch_statuses
        
        # Apply filtering while preserving original indices
        filtered_indexed_groups = []
        for original_index, group_key, details in indexed_groups:
            group_data = details['data']
            
            # Check completion status
            completion_status_match = details['status'] in final_statuses
            
            # Check watch status
            watch_status = archiver._get_watch_status_classification(group_data)
            watch_status_match = watch_status in final_watch_statuses
            
            # Include if both filters match
            if completion_status_match and watch_status_match:
                filtered_indexed_groups.append((original_index, group_key, details))
        indexed_groups = filtered_indexed_groups

    # Filter by modified datetime if requested
    if hasattr(args, 'modified') and args.modified:
        try:
            modified_conditions = _parse_modified_conditions(args.modified)
        except ValueError as exc:
            print(f"Error: {exc}")
            return 1

        filtered_indexed_groups = []
        for original_index, group_key, details in indexed_groups:
            group_data = details.get('data', {})
            if all(
                _matches_modified_expression(group_data, op, modified_dt, is_date_only)
                for op, modified_dt, is_date_only in modified_conditions
            ):
                filtered_indexed_groups.append((original_index, group_key, details))
        indexed_groups = filtered_indexed_groups

    if hasattr(args, 'episodes_found') and args.episodes_found:
        try:
            episodes_found_conditions = _parse_numeric_conditions(args.episodes_found, '--episodes-found')
        except ValueError as exc:
            print(f"Error: {exc}")
            return 1

        filtered_indexed_groups = []
        for original_index, group_key, details in indexed_groups:
            episodes_found = int(details.get('episodes_found', 0) or 0)
            if all(
                _matches_numeric_expression(episodes_found, op, target_value)
                for op, target_value in episodes_found_conditions
            ):
                filtered_indexed_groups.append((original_index, group_key, details))
        indexed_groups = filtered_indexed_groups

    if hasattr(args, 'episodes_expected') and args.episodes_expected:
        try:
            episodes_expected_conditions = _parse_numeric_conditions(args.episodes_expected, '--episodes-expected')
        except ValueError as exc:
            print(f"Error: {exc}")
            return 1

        filtered_indexed_groups = []
        for original_index, group_key, details in indexed_groups:
            episodes_expected = int(details.get('episodes_expected', 0) or 0)
            if all(
                _matches_numeric_expression(episodes_expected, op, target_value)
                for op, target_value in episodes_expected_conditions
            ):
                filtered_indexed_groups.append((original_index, group_key, details))
        indexed_groups = filtered_indexed_groups
    
    # Sort alphabetically if requested while preserving original indices
    if hasattr(args, 'sort') and args.sort:
        indexed_groups.sort(key=lambda x: x[2]['title'].lower())  # Sort by title (case-insensitive)
    
    if not indexed_groups:
        print("No groups found matching the filter criteria.")
        return 0
    
    presenter = Presenter(use_colors=use_colors)

    print("Available series groups:")
    print("=" * (title_length+30))  # Consistent width

    for original_index, group_key, details in indexed_groups:
        group_data = details.get('data', {})

        # Build an analysis-like dict compatible with Presenter
        analysis = {
            'status': details.get('status'),
            'title': group_data.get('title', details.get('title')),
            'season': group_data.get('season'),
            'episodes_found': details.get('episodes_found', 0),
            'episodes_expected': details.get('episodes_expected', 0),
            'watch_status': group_data.get('watch_status', {}),
            'files': group_data.get('files', []),
            'missing_episodes': group_data.get('missing_episodes', []),
            'extra_episodes': group_data.get('extra_episodes', []),
            'group_metadata': group_data.get('group_metadata', {}),
            'myanimelist_watch_status': group_data.get('myanimelist_watch_status')
        }

        if args.verbose == 0:
            # Compact one-line summary
            print(f"{original_index:4d}.", end=" ")
            presenter.print_one_line_summary(analysis, title_length=title_length)
        else:
            # Verbose: print one-line summary then detailed info
            print(f"{original_index:4d}.", end=" ")
            presenter.print_one_line_summary(analysis, title_length=title_length)
            # Additional details
            watched_episodes = archiver._get_watched_episodes(group_data)
            missing_episodes = group_data.get('missing_episodes', [])
            extra_episodes = group_data.get('extra_episodes', [])

            print(f"    Episodes: {details['episodes_found']}/{details['episodes_expected']} ({details['status']})")
            if watched_episodes:
                print(f"    Watched: {archiver._format_episode_ranges(watched_episodes)}")
            if missing_episodes:
                print(f"    Missing: {archiver._format_episode_ranges(missing_episodes)}")
            if extra_episodes:
                print(f"    Extra: {archiver._format_episode_ranges(extra_episodes)}")
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
    
    use_colors = not getattr(args, 'no_color', False)
    archiver = SeriesArchiver(verbose=args.verbose, progress_reporter=progress_reporter, use_colors=use_colors)

    try:
        torrent_recovery = _get_torrent_recovery_options(args, require_verify_crc=True)
    except ValueError as exc:
        print(f"Error: {exc}")
        return 1
    
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
        verify_crc=getattr(args, 'verify_crc', False),
        torrent_recovery=torrent_recovery
    )
    
    if results:
        if args.dry_run:
            _safe_console_print(f"\n{archiver._color('ℹ️  Dry run completed', Colors.CYAN + Colors.BOLD)} - would archive {archiver._color(str(len(results)), Colors.MAGENTA)} series.")
            _safe_console_print("Use without --dry-run to actually perform the operation.")
        else:
            _safe_console_print(f"\n{archiver._color(f'✅ Successfully archived {len(results)} series!', Colors.GREEN + Colors.BOLD)}")
        
        # Show folder dates if available
        if not args.dry_run and any(isinstance(v, dict) and v.get('newest_file_date') for v in results.values()):
            _safe_console_print(f"\n{archiver._color('Folder dates set to newest file:', Colors.YELLOW)}")
            for group_key, result_info in results.items():
                if isinstance(result_info, dict) and result_info.get('newest_file_date'):
                    folder_name = os.path.basename(result_info['folder_path'])
                    _safe_console_print(f"  📁 {archiver._color(folder_name, Colors.CYAN)}: {archiver._color(result_info['newest_file_date'], Colors.YELLOW)}")
    else:
        _safe_console_print(f"{archiver._color('⚠️  No series were processed.', Colors.YELLOW)}")
    
    return 0


def cmd_check_crc(args):
    """Handle the check-crc command."""
    use_colors = not getattr(args, 'no_color', False)
    archiver = SeriesArchiver(verbose=args.verbose, use_colors=use_colors)

    try:
        torrent_recovery = _get_torrent_recovery_options(args)
    except ValueError as exc:
        print(f"Error: {exc}")
        return 1
    
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
        crc_results = archiver.check_files_crc(groups_to_check, is_groups=True, torrent_recovery=torrent_recovery)
        archiver._run_torrent_recovery_for_crc_failures(crc_results, torrent_recovery)
    
    elif args.files:
        # Check CRC for individual files
        valid_files = [f for f in args.files if os.path.isfile(f)]
        if not valid_files:
            print("No valid files provided for CRC checking.")
            return 1
        
        print(f"Checking CRC for {len(valid_files)} files...")
        crc_results = archiver.check_files_crc(valid_files, is_groups=False, torrent_recovery=torrent_recovery)
        archiver._run_torrent_recovery_for_crc_failures(crc_results, torrent_recovery)
    
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
    list_parser.add_argument('--input-json', required=True, metavar='FILE',
                            help='JSON file from series_completeness_checker.py')
    list_parser.add_argument('--status-filter', metavar='FILTERS',
                            help='Filter results by completion status and/or watch status. Use +status to include only specific statuses, '
                                 '-status to exclude specific statuses, or plain status names for exact match. '
                                 'Available completion statuses: complete, incomplete, complete_with_extras, no_episode_numbers, '
                                 'unknown_total_episodes, not_series, no_metadata, no_metadata_manager, unknown. '
                                 'Available watch statuses: watched, watched_partial, unwatched. '
                                 'Examples: "complete watched", "+complete +watched", "-unknown -unwatched", "watched -watched_partial"')
    list_parser.add_argument('--sort', action='store_true',
                            help='Sort series alphabetically by title')
    list_parser.add_argument('--modified', metavar='EXPR',
                           help='Filter by modified datetime. Supports single expressions like "<2026-01-01" or ">=2026-01-01T12:00", '
                               'closed ranges like "2026-01-01..2026-01-31", and combined conditions like ">=2026-01-01, <2026-02-01". '
                               'ISO format is preferred, but common date formats are also accepted.')
    list_parser.add_argument('--episodes-found', metavar='EXPR',
                            help='Filter by the number of episodes found in a group. Supports single expressions like "12" or ">=12", '
                                 'closed ranges like "12..24", and combined conditions like ">=12, <25".')
    list_parser.add_argument('--episodes-expected', metavar='EXPR',
                            help='Filter by the expected episode count from metadata. Supports single expressions like "12" or "<=24", '
                                 'closed ranges like "12..24", and combined conditions like ">=12, <25".')
    list_parser.add_argument('--no-color', action='store_true',
                            help='Disable color formatting in output')
    
    # Archive command
    archive_parser = subparsers.add_parser('archive', 
                                          help='Archive selected series groups')
    archive_parser.add_argument('--input-json', required=True, metavar='FILE',
                               help='JSON file from series_completeness_checker.py')
    archive_parser.add_argument('--destination', required=True, metavar='DIR',
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
    archive_parser.add_argument('--no-color', action='store_true',
                               help='Disable color formatting in output')
    _add_torrent_recovery_arguments(archive_parser)
    
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
    crc_parser.add_argument('--no-color', action='store_true',
                           help='Disable color formatting in output')
    _add_torrent_recovery_arguments(crc_parser)
    
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
