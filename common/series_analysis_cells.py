import datetime
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

from common.presentation import Colors, Format, Icons, TerminalText

STATUS_WIDTH = 6
EPISODES_WIDTH = 9
EPISODE_RANGE_WIDTH = 20  # shared by watched / missing / extra
MAL_STATUS_WIDTH = 13
MODIFIED_WIDTH = 10  # "YYYY-MM-DD" -- date only, no time-of-day
SIZE_WIDTH = 9

_STATUS_COLORS = {
    'complete': Colors.GREEN,
    'incomplete': Colors.RED,
    'complete_with_extras': Colors.YELLOW,
}

_MAL_STATUS_COLORS = {
    'watching': Colors.BRIGHT_BLUE,
    'completed': Colors.GREEN,
    'completed (season)': Colors.GREEN,
    'on-hold': Colors.YELLOW,
    'dropped': Colors.RED,
    'plan to watch': Colors.BRIGHT_BLACK,
}


@dataclass
class SeriesDisplayRow:
    # title/season and episodes_found/episodes_expected each stay two
    # separate fields rather than one combined value -- callers combine
    # each pair into one "Title" / "Episodes" column via a two-argument
    # formatter (an approved exception -- see each caller's column list),
    # but keeping them separate here means a future split into standalone
    # Title/Season or Found/Expected columns only touches the column list,
    # never this row shape or build_display_row().
    index: Optional[int]
    status: Optional[str]
    title: str
    season: Optional[int]
    episodes_found: int
    episodes_expected: int
    watched: List[int]
    missing: List[int]
    extra: List[int]
    mal_status: Optional[str]
    modified: Optional[float]
    size: Optional[int]


def build_display_row(analysis: Dict[str, Any], index: Optional[int] = None) -> SeriesDisplayRow:
    """The one place that reads the raw analysis dict's shape. Makes no
    display decisions (no color, no padding, no truncation) -- only answers
    what data belongs to this row."""
    watched_nums: List[int] = []
    for f in analysis.get('files', []):
        if f.get('episode_watched'):
            ep = f.get('episode')
            if isinstance(ep, list):
                watched_nums.extend(ep)
            elif ep is not None:
                watched_nums.append(ep)

    mal = analysis.get('myanimelist_watch_status')
    mal_status = mal.get('my_status') if isinstance(mal, dict) else None
    if isinstance(mal_status, str) and not mal_status.strip():
        mal_status = None

    return SeriesDisplayRow(
        index=index,
        status=analysis.get('status'),
        title=analysis.get('title', 'Unknown'),
        season=analysis.get('season'),
        episodes_found=analysis.get('episodes_found', 0),
        episodes_expected=analysis.get('episodes_expected', 0),
        watched=sorted(set(watched_nums)),
        missing=analysis.get('missing_episodes', []),
        extra=analysis.get('extra_episodes', []),
        mal_status=mal_status,
        modified=(analysis.get('group_metadata') or {}).get('avg_modified_time'),
        size=analysis.get('total_size_bytes'),
    )


def status_cell(status: Optional[str]) -> str:
    return Icons.get(status or 'unknown') or '❓'


def status_color(status: Optional[str]):
    return _STATUS_COLORS.get(status, Colors.BRIGHT_BLACK)


def title_cell(title: str, season: Optional[int], title_length: int, use_colors: bool) -> str:
    season_suffix_plain = f" S{season:02d}" if season else ""
    season_suffix_colored = f" {Colors.wrap(f'S{season:02d}', Colors.DIM, use_colors)}" if season else ""
    plain_display = f"{title}{season_suffix_plain}"
    if TerminalText.width(plain_display) > title_length:
        max_title_width = max(1, title_length - TerminalText.width(season_suffix_plain))
        truncated_title = TerminalText.truncate(title, max_title_width)
        title_str = f"{truncated_title}{season_suffix_colored}"
    else:
        title_str = f"{title}{season_suffix_colored}"
    return Colors.wrap(title_str, Colors.BOLD, use_colors)


def episodes_cell(found: int, expected: int, use_colors: bool) -> str:
    expected_str = str(expected) if expected else '?'
    found_part = Colors.wrap(f"{found:>4}", Colors.BRIGHT_BLUE, use_colors)
    expected_part = Colors.wrap(f"{expected_str:<4}", Colors.BRIGHT_BLACK, use_colors)
    return f"{found_part}/{expected_part}"


def episode_range_cell(episodes: Sequence[int], color: str, use_colors: bool) -> str:
    if not episodes:
        return ''
    # Table cells don't need the surrounding brackets Format.episode_ranges()
    # adds for its other (plain-text) callers -- the column itself is the
    # bracket.
    return Colors.wrap(Format.episode_ranges(episodes).strip('[]'), color, use_colors)


def mal_status_cell(mal_status: Optional[str]) -> str:
    return mal_status or ''


def mal_status_color(mal_status: Optional[str]) -> Optional[str]:
    if not mal_status:
        return None
    return _MAL_STATUS_COLORS.get(mal_status.strip().lower(), Colors.MAGENTA)


def modified_cell(modified_at: Optional[float]) -> str:
    if not modified_at:
        return ''
    ts = datetime.datetime.fromtimestamp(modified_at)
    return ts.strftime('%Y-%m-%d')


def size_cell(total_size_bytes: Optional[int]) -> str:
    if not total_size_bytes:
        return ''
    # Right-justify the number and left-justify the unit in fixed
    # sub-widths so digits line up regardless of unit length ("8.2 GB" vs
    # "300 B") -- right-aligning the whole string would align on the unit
    # glyph instead, since unit width varies row to row.
    number_str, unit_str = Format.size(total_size_bytes).split(' ', 1)
    return f"{number_str:>6} {unit_str:<2}"
