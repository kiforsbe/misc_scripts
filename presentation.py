import datetime
import os
import sys
import unicodedata
from typing import Any, Dict, List, Optional


class Colors:
    RESET = '\033[0m'
    BOLD = '\033[1m'
    DIM = '\033[2m'
    BLACK = '\033[30m'
    RED = '\033[31m'
    GREEN = '\033[32m'
    YELLOW = '\033[33m'
    BLUE = '\033[34m'
    MAGENTA = '\033[35m'
    CYAN = '\033[36m'
    WHITE = '\033[37m'
    BRIGHT_BLACK = '\033[90m'
    BRIGHT_RED = '\033[91m'
    BRIGHT_GREEN = '\033[92m'
    BRIGHT_YELLOW = '\033[93m'
    BRIGHT_BLUE = '\033[94m'
    BRIGHT_MAGENTA = '\033[95m'
    BRIGHT_CYAN = '\033[96m'
    BRIGHT_WHITE = '\033[97m'

    @staticmethod
    def strip(text: str) -> str:
        import re
        return re.sub(r'\033\[[0-9;]+m', '', text)


# Status emojis were merged into EMOJI_MAP below for a single source of truth.


class Presenter:
    """Utility to render one-line series summaries in CLI tools."""

    def __init__(self, use_colors: bool = True):
        self.use_colors = use_colors

    def _display_width(self, text: str) -> int:
        """Calculate terminal display width for text (ignores ANSI color codes)."""
        plain = Colors.strip(text)
        width = 0
        for char in plain:
            if char == '\ufe0f':
                continue
            if unicodedata.combining(char):
                continue
            width += 2 if unicodedata.east_asian_width(char) in ('W', 'F') else 1
        return width

    def _truncate_to_width(self, text: str, max_width: int) -> str:
        """Truncate text to a terminal display width with ellipsis when needed."""
        if max_width <= 0:
            return ''

        if self._display_width(text) <= max_width:
            return text

        ellipsis = '...'
        ellipsis_width = self._display_width(ellipsis)
        if max_width <= ellipsis_width:
            return ellipsis[:max_width]

        target_width = max_width - ellipsis_width
        result_chars = []
        current_width = 0
        for char in text:
            char_width = 0
            if char != '\ufe0f' and not unicodedata.combining(char):
                char_width = 2 if unicodedata.east_asian_width(char) in ('W', 'F') else 1
            if current_width + char_width > target_width:
                break
            result_chars.append(char)
            current_width += char_width

        return ''.join(result_chars) + ellipsis

    def _format_episode_ranges(self, episodes: List[int]) -> str:
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
        if start == end:
            ranges.append(str(start))
        else:
            ranges.append(f"{start}-{end}")
        return f"[{', '.join(ranges)}]"

    def print_one_line_summary(self, analysis: Dict[str, Any], show_metadata_fields: Optional[List[str]] = None, title_length: int = 60) -> None:
        status = analysis.get('status', 'unknown')
        title = analysis.get('title', 'Unknown')
        season = analysis.get('season')
        episodes_found = analysis.get('episodes_found', 0)
        episodes_expected = analysis.get('episodes_expected', 0)
        watch_status = analysis.get('watch_status', {}) or {}

        # Emoji and color
        status_emoji = get_emoji(status) or '❓'
        status_color = Colors.RESET
        if status == 'complete':
            status_color = Colors.GREEN
        elif status == 'incomplete':
            status_color = Colors.RED
        elif status == 'complete_with_extras':
            status_color = Colors.YELLOW
        else:
            status_color = Colors.BRIGHT_BLACK

        # Title with season (display-width aware for proper alignment)
        season_suffix_plain = f" S{season:02d}" if season else ""
        season_suffix_colored = f" {Colors.DIM}S{season:02d}{Colors.RESET}" if season else ""

        title_plain_display = f"{title}{season_suffix_plain}"
        if self._display_width(title_plain_display) > title_length:
            max_title_width = max(1, title_length - self._display_width(season_suffix_plain))
            truncated_title = self._truncate_to_width(title, max_title_width)
            title_str = f"{truncated_title}{season_suffix_colored}"
            plain_title = f"{truncated_title}{season_suffix_plain}"
        else:
            title_str = f"{title}{season_suffix_colored}"
            plain_title = title_plain_display

        # Extra info: watched/missing/extra
        extra_info = []
        if watch_status.get('watched_episodes', 0) > 0 and analysis.get('files'):
            watched_nums = []
            for f in analysis.get('files', []):
                if f.get('episode_watched'):
                    ep = f.get('episode')
                    if isinstance(ep, list):
                        watched_nums.extend(ep)
                    elif ep is not None:
                        watched_nums.append(ep)
            if watched_nums:
                watched_range = self._format_episode_ranges(sorted(set(watched_nums)))
                extra_info.append(f"Watched: {watched_range}")

        if analysis.get('missing_episodes'):
            extra_info.append(f"Missing: {self._format_episode_ranges(analysis['missing_episodes'])}")
        if analysis.get('extra_episodes'):
            extra_info.append(f"Extra: {self._format_episode_ranges(analysis['extra_episodes'])}")

        # Timestamp
        timestamp_str = ''
        gm = analysis.get('group_metadata', {}) or {}
        avg_modified_time = gm.get('avg_modified_time')
        if avg_modified_time:
            ts = datetime.datetime.fromtimestamp(avg_modified_time)
            timestamp_str = f" {Colors.DIM}| Modified: {ts.strftime('%Y-%m-%d %H:%M')}{Colors.RESET}"

        all_info = extra_info
        extra_info_str = f" | {', '.join(all_info)}" if all_info else ""

        episodes_expected_str = str(episodes_expected) if episodes_expected else '?'

        padding_needed = title_length - self._display_width(plain_title)
        padding_needed = max(0, padding_needed)

        line = f"{status_color}{status_emoji}{Colors.RESET} {Colors.BOLD}{title_str}{Colors.RESET}{' ' * padding_needed} {Colors.BRIGHT_BLUE}{episodes_found:>4}{Colors.RESET}/{Colors.BRIGHT_BLACK}{episodes_expected_str:<4}{Colors.RESET}{extra_info_str}{timestamp_str}"

        print(line)


EMOJI_MAP = {
    # Status-related
    'complete': '✅',
    'incomplete': '❌',
    'complete_with_extras': '⚠️',
    'no_episode_numbers': '❓',
    'unknown_total_episodes': '❓',
    'not_series': 'ℹ️',
    'movie': '🎬',
    'no_metadata': '❓',
    'no_metadata_manager': '❓',
    'unknown': '❓',

    # Generic icons
    'check': '✅',
    'cross': '❌',
    'warning': '⚠️',
    'folder': '📁',
    'file': '📄',
    'calendar': '📅',
    'package': '📦',
    'chart': '📊',
    'star': '⭐'
}


def get_emoji(name: str) -> str:
    """Return emoji by name.

    This will automatically prefer Unicode emoji when the environment
    appears to support them. Set the environment variable `NO_EMOJI=1`
    or `PRESENTATION_FORCE_ASCII=1` to force ASCII fallbacks.
    """
    return _get_emoji(name)


# ASCII fallbacks for environments that can't render emoji/UTF-8
ASCII_MAP = {
    'complete': '[OK]',
    'incomplete': '[X]',
    'complete_with_extras': '[!]',
    'no_episode_numbers': '[?]',
    'unknown_total_episodes': '[?]',
    'not_series': '[i]',
    'movie': '[MOV]',
    'no_metadata': '[?]',
    'no_metadata_manager': '[?]',
    'unknown': '[?]',

    'check': '[OK]',
    'cross': '[X]',
    'warning': '[!]',
    'folder': '[DIR]',
    'file': '[FILE]',
    'calendar': '[DATE]',
    'package': '[PKG]',
    'chart': '[CHT]',
    'star': '[*]'
}


def _detect_emoji_support() -> bool:
    """Return True if stdout encoding can encode emoji and env vars don't disable them."""
    # Allow user to force ASCII via env vars
    if os.environ.get('NO_EMOJI') in ('1', 'true', 'True'):
        return False
    if os.environ.get('PRESENTATION_FORCE_ASCII') in ('1', 'true', 'True'):
        return False

    enc = getattr(sys.stdout, 'encoding', None) or os.environ.get('PYTHONIOENCODING') or 'utf-8'
    try:
        "✅".encode(enc)
        return True
    except Exception:
        return False


# Module-level cached decision whether to prefer Unicode emoji
_PREFER_UNICODE_EMOJI = _detect_emoji_support()


def _get_emoji(name: str, prefer_unicode: Optional[bool] = None) -> str:
    """Internal emoji getter honoring environment and detection.

    If `prefer_unicode` is None the module-level detection is used.
    """
    key = name.lower()
    if prefer_unicode is None:
        prefer_unicode = _PREFER_UNICODE_EMOJI

    if prefer_unicode:
        return EMOJI_MAP.get(key) or ASCII_MAP.get(key, '')
    else:
        return ASCII_MAP.get(key) or EMOJI_MAP.get(key, '')


def color_text(text: str, color: str = '', use_colors: bool = True) -> str:
    """Wrap text with color codes when enabled."""
    if use_colors and color:
        return f"{color}{text}{Colors.RESET}"
    return text
