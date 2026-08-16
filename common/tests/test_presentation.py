import datetime

from common.presentation import (
    Colors,
    Format,
    Icons,
    Table,
    TableColumn,
    TerminalText,
)


def test_terminal_text_width_ignores_ansi_codes():
    assert TerminalText.width(f"{Colors.CYAN}hi{Colors.RESET}") == 2


def test_terminal_text_width_counts_wide_chars_as_two_cells():
    assert TerminalText.width("a") == 1
    assert TerminalText.width("好") == 2
    assert TerminalText.width("📁") == 2


def test_terminal_text_truncate_appends_ellipsis_when_over_width():
    assert TerminalText.truncate("hello world", 8) == "hello..."
    assert TerminalText.truncate("hi", 8) == "hi"


def test_terminal_text_truncate_accounts_for_wide_chars():
    # "📁" is 2 cells wide; a width-4 budget leaves no room for the icon once
    # the 3-cell ellipsis is reserved, so nothing of the original text survives.
    assert TerminalText.truncate("📁name", 4) == "..."


def test_terminal_text_pad_left_and_right_align():
    assert TerminalText.pad("ab", 5, "left") == "ab   "
    assert TerminalText.pad("ab", 5, "right") == "   ab"


def test_terminal_text_pad_center_align_splits_padding():
    assert TerminalText.pad("ab", 6, "center") == "  ab  "
    assert TerminalText.pad("ab", 5, "center") == " ab  "


def test_terminal_text_pad_is_ansi_safe():
    colored = f"{Colors.CYAN}ab{Colors.RESET}"
    assert TerminalText.pad(colored, 5, "left") == f"{colored}   "


def test_colors_wrap_when_enabled():
    assert Colors.wrap("hi", Colors.CYAN, True) == f"{Colors.CYAN}hi{Colors.RESET}"
    assert Colors.wrap("hi", Colors.CYAN, False) == "hi"
    assert Colors.wrap("hi", "", True) == "hi"


def test_colors_should_use_honors_explicit_force():
    assert Colors.should_use(True) is True
    assert Colors.should_use(False) is False


def test_colors_should_use_disabled_when_not_a_tty(monkeypatch):
    monkeypatch.setattr("sys.stdout.isatty", lambda: False)
    assert Colors.should_use(None) is False


def test_table_render_grid_style_matches_plain_pipe_format():
    rows = [{"name": "alpha", "count": 3}, {"name": "beta", "count": 42}]
    columns = [
        TableColumn(name="name", width=10),
        TableColumn(name="count", align="right", width=10),
    ]
    result = Table(columns, style="grid").render(rows)
    assert result == (
        "name  | count\n"
        "------+------\n"
        "alpha |     3\n"
        "beta  |    42"
    )


def test_table_render_truncates_cells_beyond_column_width():
    rows = [{"name": "a very long value that overflows"}]
    columns = [TableColumn(name="name", width=10)]
    result = Table(columns, style="grid").render(rows)
    assert result == "name      \n----------\na very ..."


def test_table_render_missing_values_render_blank():
    rows = [{"name": "alpha"}]
    columns = [TableColumn(name="name", width=10), TableColumn(name="missing", width=5)]
    result = Table(columns, style="grid").render(rows)
    assert result == "name  | mi...\n------+------\nalpha |      "


def test_table_render_explicit_width_truncates_header_below_label_width():
    # An explicit width is a hard cap, even when narrower than the column's own
    # label -- callers (e.g. a CLI --column-width override) rely on this to
    # squeeze a column to fit, not on the header silently overriding them.
    rows = [{"relative_path": "very-long-folder-name/sample.txt"}]
    columns = [TableColumn(name="relative_path", label="Relative path", width=12)]
    result = Table(columns, style="grid").render(rows)
    assert result == "Relative ...\n------------\nvery-long..."


def test_table_incremental_header_truncates_below_label_width():
    columns = [TableColumn(name="relative_path", label="Relative path", width=12)]
    table = Table(columns, style="grid")
    header = table.render_header()
    assert header == "Relative ...\n------------"


def test_table_render_markdown_style_wraps_in_pipes():
    rows = [{"a": "a", "b": "b"}]
    columns = [TableColumn(name="a", width=3), TableColumn(name="b", width=3)]
    result = Table(columns, style="markdown").render(rows)
    assert result == "| a | b |\n| :-- | :-- |\n| a | b |"


def test_table_render_plain_style_has_no_separator_glyph_or_dash_line():
    rows = [{"status": "OK", "title": "Some Show"}, {"status": "X", "title": "Another Show"}]
    columns = [TableColumn(name="status", width=6), TableColumn(name="title", width=20)]
    result = Table(columns, style="plain").render(rows)
    # The last column (title) is never padded — nothing follows it to align with.
    assert result == (
        "status title\n"
        "OK     Some Show\n"
        "X      Another Show"
    )


def test_table_render_uses_column_formatter_and_color_override():
    rows = [{"name": "dir1", "is_dir": True}, {"name": "file1", "is_dir": False}]
    columns = [
        TableColumn(
            name="name",
            width=10,
            formatter=lambda row: row["name"].upper(),
            color=lambda row: Colors.CYAN if row["is_dir"] else None,
        )
    ]
    result = Table(columns, style="grid").render(rows)
    assert result == (
        "name \n"
        "-----\n"
        f"{Colors.CYAN}DIR1{Colors.RESET} \n"
        "FILE1"
    )


def test_table_render_color_override_is_skipped_when_colors_disabled():
    rows = [{"name": "dir1", "is_dir": True}]
    columns = [TableColumn(name="name", width=10, color=lambda row: Colors.CYAN)]
    result = Table(columns, style="grid", use_colors=False).render(rows)
    assert result == "name\n----\ndir1"


def test_table_incremental_render_header_then_render_row():
    columns = [
        TableColumn(name="name", width=6),
        TableColumn(name="count", align="right", width=5),
    ]
    table = Table(columns, style="markdown")
    header = table.render_header()
    # Widths are locked to each column's configured width (6, 5) at
    # render_header() time, not shrunk to fit the label text.
    assert header == "| name   | count |\n| :----- | ----: |"
    assert table.render_row({"name": "alpha", "count": 3}) == "| alpha  |     3 |"
    # A later row wider than the configured width truncates rather than growing
    # the column, since the column is already locked.
    assert table.render_row({"name": "much too long", "count": 4200}) == "| muc... |  4200 |"


def test_table_start_locks_widths_without_printing_a_header():
    columns = [TableColumn(name="name", width=6)]
    table = Table(columns, style="grid")
    table.start()
    assert table.render_row({"name": "abcdefgh"}) == "abc..."
    # Widths are locked to the column's configured width (6), not the label.
    assert table.render_row({"name": "x"}) == "x     "


def test_table_render_row_before_render_header_raises():
    table = Table([TableColumn(name="name")])
    try:
        table.render_row({"name": "x"})
        assert False, "expected RuntimeError"
    except RuntimeError:
        pass


def test_format_size_human_readable():
    assert Format.size(0) == "0 B"
    assert Format.size(1536) == "1.5 KB"
    assert Format.size(None) == ""


def test_format_size_non_human_returns_raw_bytes():
    assert Format.size(1536, human=False) == "1536"


def test_format_timestamp_matches_local_fromtimestamp():
    ts = 1_700_000_000.0
    expected = datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
    assert Format.timestamp(ts) == expected
    assert Format.timestamp(None) == "-"


def test_format_age_buckets_by_largest_unit():
    now = 10_000.0
    assert Format.age(now - 30, now=now) == "30s ago"
    assert Format.age(now - 90, now=now) == "1m ago"
    assert Format.age(now - 3700, now=now) == "1h ago"
    assert Format.age(None) == "unknown"


def test_icons_for_file_disabled_returns_empty_string():
    assert Icons.for_file("d", None, use_icons=False) == ""


def test_icons_for_file_classifies_by_extension():
    assert Icons.for_file("d", None) == f"{Icons.get('folder')} "
    assert Icons.for_file("f", ".png") == f"{Icons.get('image')} "
    assert Icons.for_file("f", ".mp3") == f"{Icons.get('audio')} "
    assert Icons.for_file("f", ".mkv") == f"{Icons.get('video')} "
    assert Icons.for_file("f", ".py") == f"{Icons.get('file')} "
    assert Icons.for_file("f", None) == f"{Icons.get('file')} "


def test_format_episode_ranges_compacts_consecutive_runs():
    assert Format.episode_ranges([1, 2, 3, 5, 8, 9, 10]) == "[1-3, 5, 8-10]"
    assert Format.episode_ranges([7]) == "[7]"
    assert Format.episode_ranges([]) == ""
