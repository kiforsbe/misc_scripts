import datetime
import inspect
import os
import re
import sys
import unicodedata
from dataclasses import dataclass
from typing import Any, Callable, List, Mapping, Optional, Sequence


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
        return re.sub(r'\033\[[0-9;]+m', '', text)

    @staticmethod
    def wrap(text: str, color: str = '', enabled: bool = True) -> str:
        """Wrap text with a color code when enabled; otherwise return it unchanged."""
        if enabled and color:
            return f"{color}{text}{Colors.RESET}"
        return text

    @staticmethod
    def should_use(force: Optional[bool] = None) -> bool:
        """Decide whether ANSI colors should be used for terminal output.

        Honors an explicit ``force`` override; otherwise disables color when
        stdout isn't a TTY, and on Windows additionally requires a terminal known
        to render ANSI codes (Windows Terminal, ANSICON, ConEmu, or a TERM var).
        """
        if force is not None:
            return force
        if not sys.stdout.isatty():
            return False
        if os.name != 'nt':
            return True
        return any(
            os.environ.get(name)
            for name in ('WT_SESSION', 'ANSICON', 'ConEmuANSI', 'TERM')
        )


class TerminalText:
    """ANSI-aware text measurement: width, truncation, and padding for terminal display."""

    @staticmethod
    def width(text: str) -> int:
        """Calculate terminal display width for text (ignores ANSI color codes).

        Accounts for East-Asian wide/fullwidth characters and emoji, which occupy
        two terminal cells, and skips variation selectors and combining marks,
        which occupy none.
        """
        plain = Colors.strip(text)
        width = 0
        for char in plain:
            if char == '️':
                continue
            if unicodedata.combining(char):
                continue
            width += 2 if unicodedata.east_asian_width(char) in ('W', 'F') else 1
        return width

    @staticmethod
    def truncate(text: str, max_width: int, ellipsis: str = '...') -> str:
        """Truncate text to a terminal display width, appending an ellipsis when cut.

        ``text`` must not contain ANSI color codes: truncation walks the raw
        characters to measure display width, so escape sequences would be
        miscounted (and could be cut mid-sequence). Apply color after truncating.
        """
        if max_width <= 0:
            return ''

        if TerminalText.width(text) <= max_width:
            return text

        ellipsis_width = TerminalText.width(ellipsis)
        if max_width <= ellipsis_width:
            return ellipsis[:max_width]

        target_width = max_width - ellipsis_width
        result_chars = []
        current_width = 0
        for char in text:
            char_width = 0
            if char != '️' and not unicodedata.combining(char):
                char_width = 2 if unicodedata.east_asian_width(char) in ('W', 'F') else 1
            if current_width + char_width > target_width:
                break
            result_chars.append(char)
            current_width += char_width

        return ''.join(result_chars) + ellipsis

    @staticmethod
    def pad(text: str, width: int, align: str = 'left') -> str:
        """Pad text to a terminal display width, ignoring ANSI color codes when measuring.

        Safe to use on text that already contains color codes, since padding only
        appends spaces rather than slicing the string.
        """
        padding = max(0, width - TerminalText.width(text))
        if align == 'right':
            return f"{' ' * padding}{text}"
        if align == 'center':
            left_padding = padding // 2
            right_padding = padding - left_padding
            return f"{' ' * left_padding}{text}{' ' * right_padding}"
        return f"{text}{' ' * padding}"


class Icons:
    """Named icon lookup with automatic emoji/ASCII fallback for the current terminal."""

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
        'star': '⭐',
        'image': '🖼️',
        'audio': '🎵',
        'video': '🎞️',
    }

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
        'star': '[*]',
        'image': '[IMG]',
        'audio': '[AUD]',
        'video': '[VID]',
    }

    FILE_TYPE_EXTENSIONS = {
        'image': {'.jpg', '.jpeg', '.png', '.gif', '.webp'},
        'audio': {'.mp3', '.flac', '.wav', '.m4a'},
        'video': {'.mp4', '.mkv', '.avi', '.mov'},
    }

    _prefer_unicode: Optional[bool] = None

    @classmethod
    def _supports_unicode(cls) -> bool:
        """Return True if stdout encoding can encode emoji and env vars don't disable them.

        Cached on first call; set ``NO_EMOJI=1`` or ``PRESENTATION_FORCE_ASCII=1``
        to force ASCII fallbacks.
        """
        if cls._prefer_unicode is not None:
            return cls._prefer_unicode

        if os.environ.get('NO_EMOJI') in ('1', 'true', 'True'):
            cls._prefer_unicode = False
        elif os.environ.get('PRESENTATION_FORCE_ASCII') in ('1', 'true', 'True'):
            cls._prefer_unicode = False
        else:
            enc = getattr(sys.stdout, 'encoding', None) or os.environ.get('PYTHONIOENCODING') or 'utf-8'
            try:
                "✅".encode(enc)
                cls._prefer_unicode = True
            except Exception:
                cls._prefer_unicode = False
        return cls._prefer_unicode

    @classmethod
    def get(cls, name: str, prefer_unicode: Optional[bool] = None) -> str:
        """Return the named icon, preferring Unicode emoji when the environment supports them."""
        key = name.lower()
        if prefer_unicode is None:
            prefer_unicode = cls._supports_unicode()

        if prefer_unicode:
            return cls.EMOJI_MAP.get(key) or cls.ASCII_MAP.get(key, '')
        return cls.ASCII_MAP.get(key) or cls.EMOJI_MAP.get(key, '')

    @classmethod
    def for_file(cls, entry_type: str, extension: Optional[str] = None, use_icons: bool = True) -> str:
        """Return a trailing icon (with a space) for a file-listing entry, by type/extension.

        ``entry_type`` follows the ``os.DirEntry``/``stat`` convention: ``'d'`` for
        directories, anything else for files. Returns '' when ``use_icons`` is
        False. Degrades to an ASCII fallback in terminals that can't render emoji.
        """
        if not use_icons:
            return ''
        if entry_type == 'd':
            return f"{cls.get('folder')} "
        for name, extensions in cls.FILE_TYPE_EXTENSIONS.items():
            if extension in extensions:
                return f"{cls.get(name)} "
        return f"{cls.get('file')} "


class Format:
    """Human-readable formatting for sizes, timestamps, and relative ages."""

    @staticmethod
    def size(size_bytes: Optional[float], human: bool = True) -> str:
        """Format a byte count as a human-readable string (e.g. ``1.2 GB``).

        Args:
            size_bytes: Size in bytes. ``None`` returns an empty string.
            human: When False, returns the raw byte count as a string instead.

        Returns:
            A compact human-readable size string (or the raw byte count), or ''
            when there is nothing to show.
        """
        if size_bytes is None:
            return ''
        if not human:
            return str(int(size_bytes))
        units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
        size = float(size_bytes)
        unit_index = 0
        while size >= 1024 and unit_index < len(units) - 1:
            size /= 1024
            unit_index += 1
        if unit_index == 0:
            return f"{int(size)} {units[unit_index]}"
        return f"{size:.1f} {units[unit_index]}"

    @staticmethod
    def timestamp(timestamp: Optional[float]) -> str:
        """Format a Unix timestamp as ``YYYY-MM-DD HH:MM:SS``, or ``-`` when absent."""
        if timestamp is None:
            return '-'
        return datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

    @staticmethod
    def age(timestamp: Optional[float], now: Optional[float] = None) -> str:
        """Format how long ago a Unix timestamp was, e.g. ``3h ago``."""
        if timestamp is None:
            return 'unknown'
        if now is None:
            now = datetime.datetime.now().timestamp()
        delta = max(0, int(now - timestamp))
        units = [
            (86400, 'd'),
            (3600, 'h'),
            (60, 'm'),
            (1, 's'),
        ]
        for step, suffix in units:
            if delta >= step:
                return f"{delta // step}{suffix} ago"
        return '0s ago'

    @staticmethod
    def episode_ranges(episodes: Sequence[int]) -> str:
        """Compact a list of episode numbers into bracketed range notation,
        e.g. ``[1-5, 8, 10-12]``. Returns '' for an empty list."""
        if not episodes:
            return ''
        sorted_episodes = sorted(episodes)
        ranges = []
        start = sorted_episodes[0]
        end = start
        for value in sorted_episodes[1:]:
            if value == end + 1:
                end = value
                continue
            ranges.append(str(start) if start == end else f"{start}-{end}")
            start = end = value
        ranges.append(str(start) if start == end else f"{start}-{end}")
        return f"[{', '.join(ranges)}]"


@dataclass(frozen=True)
class TableColumn:
    """Column spec for :class:`Table`.

    ``name`` keys into each row (mapping key or attribute) when no ``formatter``
    is given. ``label`` is the header text shown (defaults to ``name``). ``width``
    caps how wide the column may grow before truncating (defaults to the table's
    ``fallback_width``).

    ``formatter``, when given, is called with this column's own extracted
    value — ``row[name]`` (mapping) or ``getattr(row, name)`` (object) — and
    returns the cell's plain display text (no ANSI codes — colors are
    applied by the table after truncating, via ``color``). A formatter
    declared with a SECOND parameter also receives the whole row as that
    second argument, e.g. ``def cell(value, row): ...`` — for the rare case
    a cell's content genuinely depends on more than its own column (Table
    decides which form to call based on the formatter's own signature, via
    ``inspect.signature``, computed once at construction).

    THIS IS NOT THE DEFAULT WAY TO SOLVE A FORMATTING PROBLEM. Prefer
    combining whatever facts a column needs into one compound value (a
    small dataclass/NamedTuple works fine) at the point the row is built,
    and keep the formatter single-argument. Writing a two-argument
    formatter is a deliberate, visible-in-the-diff choice — treat each one
    as requiring its own sign-off, not something to reach for by default.

    ``color``, when given, follows the same rule: it receives this column's
    own extracted value (and, if declared with a second parameter, the same
    row), and returns an ANSI color code to wrap the cell in, or
    ``None``/``''`` for no color — independent of what ``formatter``
    returned. Both let a column's presentation be fully declarative, so
    callers never need to format or color individual cells themselves.
    """
    name: str
    label: Optional[str] = None
    width: Optional[int] = None
    align: str = 'left'  # 'left' | 'right'
    formatter: Optional[Callable[..., Any]] = None
    color: Optional[Callable[..., Optional[str]]] = None


class Table:
    """Renders rows of structured data into an aligned text table.

    Supports two rendering modes:

    - **Bulk** — ``table.render(rows)`` renders the header and every row in one
      call. Columns auto-shrink to fit the full dataset (capped at each
      column's configured ``width``, or ``fallback_width`` when unset).
    - **Incremental** — ``table.start()`` (or ``table.render_header()`` when a
      header should be printed) starts the table before any rows are known,
      followed by one ``table.render_row(row)`` call per row as they become
      available. Because later rows can't be previewed, column widths are
      fixed at start time from each column's configured ``width`` (or
      ``fallback_width``) rather than auto-justified to content.

    In both modes, callers always pass whole rows (a mapping or an object with
    matching attributes) — never pre-formatted or pre-truncated cell strings.
    Per-column presentation goes through ``TableColumn.formatter``/``color``.
    """

    def __init__(
        self,
        columns: Sequence[TableColumn],
        *,
        style: str = 'grid',
        fallback_width: int = 32,
        ellipsis: str = '...',
        use_colors: bool = True,
    ):
        if not columns:
            raise ValueError("Table requires at least one column.")
        self.columns = list(columns)
        self.style = style
        self.fallback_width = fallback_width
        self.ellipsis = ellipsis
        self.use_colors = use_colors
        self._widths: Optional[List[int]] = None
        # Whether each column's formatter/color wants the whole row (a
        # second parameter) rather than just its own extracted value.
        # Computed once here, not per row/cell -- inspect.signature() isn't
        # cheap enough to call for every row of a large table.
        self._formatter_wants_row = [Table._accepts_row(c.formatter) for c in self.columns]
        self._color_wants_row = [Table._accepts_row(c.color) for c in self.columns]

    @staticmethod
    def _accepts_row(func: Optional[Callable]) -> bool:
        if func is None:
            return False
        try:
            return len(inspect.signature(func).parameters) >= 2
        except (TypeError, ValueError):
            return False  # builtins etc. without an inspectable signature: treat as single-value

    @property
    def _labels(self) -> List[str]:
        return [c.label if c.label is not None else c.name for c in self.columns]

    @property
    def _alignments(self) -> List[str]:
        return [c.align for c in self.columns]

    @property
    def _max_widths(self) -> List[int]:
        """Each column's hard width cap.

        An explicit ``TableColumn.width`` is authoritative — even when it's
        narrower than the column's own label, since callers rely on it as a
        real cap (e.g. a CLI ``--column-width`` override squeezing a column
        to fit a narrow terminal). Only an *unset* ``width`` falls back to
        auto-sizing to fit the label (there's no caller intent to override in
        that case).
        """
        labels = self._labels
        return [
            c.width if c.width is not None else max(TerminalText.width(labels[i]), self.fallback_width)
            for i, c in enumerate(self.columns)
        ]

    @staticmethod
    def _extract(row: Any, name: str) -> Any:
        if isinstance(row, Mapping):
            return row.get(name)
        return getattr(row, name, None)

    def _cell_text(self, row: Any, raw_value: Any, column: TableColumn, wants_row: bool) -> str:
        if column.formatter is not None:
            value = column.formatter(raw_value, row) if wants_row else column.formatter(raw_value)
        else:
            value = raw_value
        return '' if value is None else str(value)

    def _row_cells(self, row: Any, max_widths: Sequence[int]) -> List[str]:
        cells = []
        for i, column in enumerate(self.columns):
            raw_value = Table._extract(row, column.name)
            text = TerminalText.truncate(
                self._cell_text(row, raw_value, column, self._formatter_wants_row[i]),
                max_widths[i],
                self.ellipsis,
            )
            if self.use_colors and column.color is not None:
                color = column.color(raw_value, row) if self._color_wants_row[i] else column.color(raw_value)
                if color:
                    text = Colors.wrap(text, color, True)
            cells.append(text)
        return cells

    def render(self, rows: Sequence[Any]) -> str:
        """Render the header and every row in one call, auto-justified to ``rows``."""
        max_widths = self._max_widths
        row_cells = [self._row_cells(row, max_widths) for row in rows]
        self._widths = Table._compute_widths(self._labels, max_widths, row_cells)
        lines = [self._render_header_lines()]
        lines.extend(self._render_row_line(cells) for cells in row_cells)
        return "\n".join(lines)

    def start(self) -> None:
        """Start incremental rendering: lock column widths from each column's
        configured ``width`` (or ``fallback_width``) since no rows are known yet.
        Follow with ``render_row()`` per row. Use this instead of
        ``render_header()`` when the table has no header to print (e.g. a
        repeating one-row-per-item listing)."""
        self._widths = Table._compute_widths(self._labels, self._max_widths, [])

    def render_header(self) -> str:
        """Start incremental rendering (see ``start()``) and return the header +
        separator lines. Follow with ``render_row()`` per row."""
        self.start()
        return self._render_header_lines()

    def render_row(self, row: Any) -> str:
        """Render a single row using the layout locked by ``start()``/``render_header()``."""
        if self._widths is None:
            raise RuntimeError("Table.render_row() requires start() or render_header() to be called first.")
        cells = self._row_cells(row, self._max_widths)
        return self._render_row_line(cells)

    def _render_header_lines(self) -> str:
        labels = [
            TerminalText.truncate(label, self._widths[i], self.ellipsis)
            for i, label in enumerate(self._labels)
        ]
        header = Table._format_row(labels, self._widths, self._alignments, style=self.style)
        if self.style == 'plain':
            return header
        separator = Table._format_separator(self._widths, self._alignments, style=self.style)
        return "\n".join([header, separator])

    def _render_row_line(self, cells: Sequence[str]) -> str:
        return Table._format_row(cells, self._widths, self._alignments, style=self.style)

    @staticmethod
    def _compute_widths(
        labels: Sequence[str],
        max_widths: Sequence[int],
        row_cells: Sequence[Sequence[str]],
    ) -> List[int]:
        """Compute each column's rendered width.

        With no rows to preview (the incremental-mode case), each column's width
        is simply its configured max width — there's nothing to auto-shrink to,
        and shrinking to the label's own width would silently truncate any row
        content wider than the label but still within the configured cap.

        With rows given (bulk mode), each column starts wide enough for its
        label, then grows to fit every (already-truncated) cell, capped at that
        column's max width.
        """
        if not row_cells:
            return list(max_widths)
        widths = [min(TerminalText.width(label), max_widths[i]) for i, label in enumerate(labels)]
        for cells in row_cells:
            for i, cell in enumerate(cells):
                widths[i] = min(max(widths[i], TerminalText.width(cell)), max_widths[i])
        return widths

    @staticmethod
    def _format_row(
        cells: Sequence[str],
        widths: Sequence[int],
        alignments: Sequence[str],
        *,
        style: str = 'grid',
    ) -> str:
        """Render one row of already-truncated cells as an aligned table line.

        ``style='grid'`` joins cells with " | ". ``style='markdown'`` additionally
        wraps the row in leading/trailing "|" (GitHub-flavored markdown syntax).
        ``style='plain'`` joins cells with a single space and uses no separator
        glyph at all (e.g. an ``ls -l``-style columnar listing). Its last cell is
        never padded — nothing follows it to align with, so padding it would only
        add trailing whitespace.
        """
        if style == 'plain':
            last = len(cells) - 1
            padded = [
                cell if i == last else TerminalText.pad(cell, widths[i], alignments[i])
                for i, cell in enumerate(cells)
            ]
            return " ".join(padded)
        padded = [TerminalText.pad(cell, widths[i], alignments[i]) for i, cell in enumerate(cells)]
        joined = " | ".join(padded)
        return f"| {joined} |" if style == 'markdown' else joined

    @staticmethod
    def _format_separator(
        widths: Sequence[int],
        alignments: Sequence[str],
        *,
        style: str = 'grid',
    ) -> str:
        """Render the header/body separator line for a table.

        ``style='grid'`` renders plain dashes joined with "-+-". ``style='markdown'``
        renders GitHub-flavored markdown alignment markers (``:---`` left,
        ``---:`` right), wrapped in leading/trailing "|".
        """
        if style == 'markdown':
            segments = []
            for width, align in zip(widths, alignments):
                segment_width = max(3, width)
                segments.append(f"{'-' * (segment_width - 1)}:" if align == 'right' else f":{'-' * (segment_width - 1)}")
            return f"| {' | '.join(segments)} |"
        return "-+-".join("-" * width for width in widths)
