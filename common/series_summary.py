import datetime
from typing import Any, Dict, List

from common.presentation import Colors, Format, Icons, Table, TableColumn, TerminalText

_STATUS_COLORS = {
    'complete': Colors.GREEN,
    'incomplete': Colors.RED,
    'complete_with_extras': Colors.YELLOW,
}

# "[OK]" (4 chars) is the widest ASCII icon fallback used for these statuses; a real
# emoji is 1-2 cells. The "Status" header label (6 chars) requires width of 6 to fit.
_STATUS_WIDTH = 6


def _status_cell(analysis: Dict[str, Any]) -> str:
    return Icons.get(analysis.get('status', 'unknown')) or '❓'


def _status_color(analysis: Dict[str, Any]):
    return _STATUS_COLORS.get(analysis.get('status'), Colors.BRIGHT_BLACK)


def _title_cell(analysis: Dict[str, Any], title_length: int, use_colors: bool) -> str:
    title = analysis.get('title', 'Unknown')
    season = analysis.get('season')
    season_suffix_plain = f" S{season:02d}" if season else ""
    season_suffix_colored = f" {Colors.wrap(f'S{season:02d}', Colors.DIM, use_colors)}" if season else ""

    plain_display = f"{title}{season_suffix_plain}"
    if TerminalText.width(plain_display) > title_length:
        max_title_width = max(1, title_length - TerminalText.width(season_suffix_plain))
        truncated_title = TerminalText.truncate(title, max_title_width)
        title_str = f"{truncated_title}{season_suffix_colored}"
    else:
        title_str = f"{title}{season_suffix_colored}"
    # The formatter's own truncation above already guarantees this cell's visible
    # width never exceeds title_length, matching the column's configured width, so
    # Table's own (ANSI-safe) truncation pass downstream is always a no-op here.
    return Colors.wrap(title_str, Colors.BOLD, use_colors)


# "NNNN/NNNN" — always exactly 9 visible cells for any realistic episode count. This
# stays one combined column (not split into two) because "found/expected" is one datum.
_EPISODES_WIDTH = 9


def _episodes_cell(analysis: Dict[str, Any], use_colors: bool) -> str:
    found = analysis.get('episodes_found', 0)
    expected = analysis.get('episodes_expected', 0)
    expected_str = str(expected) if expected else '?'
    found_part = Colors.wrap(f"{found:>4}", Colors.BRIGHT_BLUE, use_colors)
    expected_part = Colors.wrap(f"{expected_str:<4}", Colors.BRIGHT_BLACK, use_colors)
    return f"{found_part}/{expected_part}"


# "YYYY-MM-DD HH:MM" — always exactly 16 visible cells.
_MODIFIED_WIDTH = 16


def _modified_cell(analysis: Dict[str, Any], use_colors: bool) -> str:
    gm = analysis.get('group_metadata', {}) or {}
    avg_modified_time = gm.get('avg_modified_time')
    if not avg_modified_time:
        return ''
    ts = datetime.datetime.fromtimestamp(avg_modified_time)
    # No "Modified: " prefix here — the column header already says "Modified".
    return Colors.wrap(ts.strftime('%Y-%m-%d %H:%M'), Colors.DIM, use_colors)


def _info_cell(analysis: Dict[str, Any], use_colors: bool) -> str:
    watch_status = analysis.get('watch_status', {}) or {}
    extra_info: List[str] = []

    if watch_status.get('watched_episodes', 0) > 0 and analysis.get('files'):
        watched_nums: List[int] = []
        for f in analysis.get('files', []):
            if f.get('episode_watched'):
                ep = f.get('episode')
                if isinstance(ep, list):
                    watched_nums.extend(ep)
                elif ep is not None:
                    watched_nums.append(ep)
        if watched_nums:
            extra_info.append(f"Watched: {Format.episode_ranges(sorted(set(watched_nums)))}")

    mal_watch_status = analysis.get('myanimelist_watch_status') or {}
    if isinstance(mal_watch_status, dict):
        my_status = mal_watch_status.get('my_status')
        if isinstance(my_status, str):
            normalized = my_status.strip().lower().replace('_', ' ').replace('-', ' ')
            if normalized == 'plan to watch':
                extra_info.append("Plan to Watch")

    if analysis.get('missing_episodes'):
        extra_info.append(f"Missing: {Format.episode_ranges(analysis['missing_episodes'])}")
    if analysis.get('extra_episodes'):
        extra_info.append(f"Extra: {Format.episode_ranges(analysis['extra_episodes'])}")

    total_size_bytes = analysis.get('total_size_bytes', 0)
    if total_size_bytes and total_size_bytes > 0:
        extra_info.append(f"Size: {Format.size(total_size_bytes)}")

    return ', '.join(extra_info)


# This is the LAST column — Table's 'plain' style never pads the last column, so an
# oversized width here costs nothing (no trailing whitespace) and just needs to be
# larger than any realistic info-text length so Table's own truncation pass never
# fires on genuinely long content.
_INFO_WIDTH = 2000


def build_series_summary_table(title_length: int, use_colors: bool) -> Table:
    """Build a Table that renders one series `analysis` dict per row: status icon,
    bold title (with dim season suffix), combined episode-count column, a modified
    timestamp column, and an info column (watched/plan-to-watch/missing/extra/size).
    Call `print(table.render_header())` once, then `print(table.render_row(analysis))`
    per series."""
    columns = [
        TableColumn(name='status', label='Status', width=_STATUS_WIDTH, formatter=_status_cell, color=_status_color),
        TableColumn(
            name='title',
            label='Title',
            width=title_length,
            formatter=lambda a: _title_cell(a, title_length, use_colors),
        ),
        TableColumn(name='episodes', label='Episodes', width=_EPISODES_WIDTH, formatter=lambda a: _episodes_cell(a, use_colors)),
        TableColumn(name='modified', label='Modified', width=_MODIFIED_WIDTH, formatter=lambda a: _modified_cell(a, use_colors)),
        TableColumn(name='info', label='Info', width=_INFO_WIDTH, formatter=lambda a: _info_cell(a, use_colors)),
    ]
    return Table(columns, style='plain', use_colors=use_colors)
