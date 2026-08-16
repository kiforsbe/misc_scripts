import datetime

from common.presentation import Colors
from common.series_analysis_cells import (
    build_display_row,
    episode_range_cell,
    episodes_cell,
    mal_status_cell,
    mal_status_color,
    modified_cell,
    size_cell,
    status_cell,
    status_color,
    title_cell,
)


def _analysis(**overrides):
    base = {
        "status": "complete",
        "title": "Some Show",
        "season": 1,
        "episodes_found": 12,
        "episodes_expected": 12,
        "files": [],
        "missing_episodes": [],
        "extra_episodes": [],
        "total_size_bytes": 0,
        "group_metadata": {},
        "myanimelist_watch_status": None,
    }
    base.update(overrides)
    return base


# --- build_display_row ---

def test_build_display_row_extracts_basic_fields():
    row = build_display_row(_analysis(), index=7)
    assert row.index == 7
    assert row.status == "complete"
    assert row.title == "Some Show"
    assert row.season == 1
    assert row.episodes_found == 12
    assert row.episodes_expected == 12


def test_build_display_row_defaults_index_to_none_when_omitted():
    row = build_display_row(_analysis())
    assert row.index is None


def test_build_display_row_extracts_watched_episodes_from_files():
    analysis = _analysis(files=[
        {"episode_watched": True, "episode": 1},
        {"episode_watched": True, "episode": [3, 4]},
        {"episode_watched": False, "episode": 2},
    ])
    row = build_display_row(analysis)
    assert row.watched == [1, 3, 4]


def test_build_display_row_watched_episodes_empty_when_none_watched():
    row = build_display_row(_analysis(files=[{"episode_watched": False, "episode": 1}]))
    assert row.watched == []


def test_build_display_row_extracts_missing_and_extra():
    row = build_display_row(_analysis(missing_episodes=[9, 10], extra_episodes=[13]))
    assert row.missing == [9, 10]
    assert row.extra == [13]


def test_build_display_row_extracts_mal_status_when_present():
    analysis = _analysis(myanimelist_watch_status={"my_status": "Dropped"})
    row = build_display_row(analysis)
    assert row.mal_status == "Dropped"


def test_build_display_row_mal_status_none_when_absent():
    row = build_display_row(_analysis(myanimelist_watch_status=None))
    assert row.mal_status is None


def test_build_display_row_mal_status_none_when_blank_string():
    analysis = _analysis(myanimelist_watch_status={"my_status": "   "})
    row = build_display_row(analysis)
    assert row.mal_status is None


def test_build_display_row_extracts_modified_from_group_metadata():
    row = build_display_row(_analysis(group_metadata={"avg_modified_time": 1700000000}))
    assert row.modified == 1700000000


def test_build_display_row_modified_none_when_group_metadata_missing():
    row = build_display_row(_analysis(group_metadata={}))
    assert row.modified is None


def test_build_display_row_extracts_total_size_bytes():
    row = build_display_row(_analysis(total_size_bytes=123456))
    assert row.size == 123456


def test_build_display_row_size_none_when_key_absent():
    analysis = _analysis()
    del analysis["total_size_bytes"]
    row = build_display_row(analysis)
    assert row.size is None


# --- cell formatters: plain values in, no fixture dicts ---

def test_status_cell_returns_an_icon_or_fallback():
    assert status_cell("complete")
    assert status_cell(None)  # unknown fallback, still non-empty


def test_status_color_maps_known_statuses():
    assert status_color("complete") == Colors.GREEN
    assert status_color("incomplete") == Colors.RED
    assert status_color("something_else") == Colors.BRIGHT_BLACK


def test_title_cell_appends_season_suffix():
    text = title_cell("Some Show", 1, title_length=40, use_colors=False)
    assert "Some Show" in text
    assert "S01" in text


def test_title_cell_no_season_suffix_when_season_none():
    text = title_cell("Some Show", None, title_length=40, use_colors=False)
    assert "S0" not in text


def test_title_cell_truncates_to_title_length():
    text = title_cell("A" * 50, None, title_length=10, use_colors=False)
    assert len(text) <= 10


def test_episodes_cell_shows_found_and_expected():
    text = episodes_cell(5, 12, use_colors=False)
    assert "5" in text and "12" in text


def test_episodes_cell_shows_question_mark_when_expected_unknown():
    text = episodes_cell(5, 0, use_colors=False)
    assert "?" in text


def test_episode_range_cell_empty_for_no_episodes():
    assert episode_range_cell([], Colors.RED, use_colors=False) == ''


def test_episode_range_cell_formats_ranges():
    assert episode_range_cell([9, 10, 11, 12], Colors.RED, use_colors=False) == "9-12"


def test_episode_range_cell_has_no_surrounding_brackets():
    text = episode_range_cell([1, 2, 3, 5], Colors.RED, use_colors=False)
    assert "[" not in text and "]" not in text


def test_mal_status_cell_empty_when_none():
    assert mal_status_cell(None) == ''


def test_mal_status_cell_shows_any_status_not_just_plan_to_watch():
    assert mal_status_cell("Dropped") == "Dropped"
    assert mal_status_cell("Watching") == "Watching"
    assert mal_status_cell("Plan to Watch") == "Plan to Watch"


def test_mal_status_color_maps_known_statuses():
    assert mal_status_color("Watching") == Colors.BRIGHT_BLUE
    assert mal_status_color("Completed") == Colors.GREEN
    assert mal_status_color("Completed (Season)") == Colors.GREEN
    assert mal_status_color("On-Hold") == Colors.YELLOW
    assert mal_status_color("Dropped") == Colors.RED
    assert mal_status_color("Plan to Watch") == Colors.BRIGHT_BLACK


def test_mal_status_color_is_case_insensitive():
    assert mal_status_color("watching") == Colors.BRIGHT_BLUE


def test_mal_status_color_falls_back_for_unknown_status():
    assert mal_status_color("Something Else") == Colors.MAGENTA


def test_mal_status_color_none_when_no_status():
    assert mal_status_color(None) is None


def test_modified_cell_is_date_only_no_time_of_day():
    ts = datetime.datetime(2026, 3, 5, 14, 30).timestamp()
    text = modified_cell(ts)
    assert text == "2026-03-05"


def test_modified_cell_empty_when_none():
    assert modified_cell(None) == ''


def test_size_cell_formats_human_readable():
    text = size_cell(8_200_000_000)
    assert "GB" in text


def test_size_cell_empty_when_zero_or_none():
    assert size_cell(0) == ''
    assert size_cell(None) == ''


def test_size_cell_right_justifies_number_and_left_justifies_unit():
    # Different unit widths ("B" vs "GB") must not shift where the digits
    # land -- the number field is a fixed width regardless of unit.
    small = size_cell(500)
    large = size_cell(3 * 1024 ** 3)  # 3.0 GB
    assert small == "   500 B "
    assert large == "   3.0 GB"
    assert len(small) == len(large)
