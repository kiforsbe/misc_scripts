from common.presentation import Colors
from common.series_summary import build_series_summary_table


def _analysis(**overrides):
    base = {
        "status": "complete",
        "title": "Some Show",
        "season": 1,
        "episodes_found": 12,
        "episodes_expected": 12,
        "watch_status": {},
        "files": [],
        "missing_episodes": [],
        "extra_episodes": [],
        "total_size_bytes": 0,
        "group_metadata": {},
        "myanimelist_watch_status": None,
    }
    base.update(overrides)
    return base


def test_build_series_summary_table_header_names_every_column():
    table = build_series_summary_table(title_length=20, use_colors=False)
    header = table.render_header()
    for label in ("Status", "Title", "Episodes", "Modified", "Info"):
        assert label in header


def test_build_series_summary_table_renders_one_row_per_series():
    table = build_series_summary_table(title_length=20, use_colors=False)
    table.render_header()
    line = table.render_row(_analysis())
    assert "Some Show" in line
    assert "S01" in line
    assert "12/12" in line


def test_build_series_summary_table_includes_missing_and_extra_episode_info():
    table = build_series_summary_table(title_length=20, use_colors=False)
    table.render_header()
    line = table.render_row(_analysis(status="incomplete", missing_episodes=[3, 4, 5]))
    assert "Missing: [3-5]" in line


def test_build_series_summary_table_respects_use_colors_false():
    table = build_series_summary_table(title_length=20, use_colors=False)
    table.render_header()
    line = table.render_row(_analysis())
    assert Colors.RESET not in line


def test_build_series_summary_table_modified_cell_has_no_redundant_label_prefix():
    # The "Modified" header already says what the column is; a per-row "Modified: "
    # prefix (the old Presenter's behavior) would just repeat it on every line.
    table = build_series_summary_table(title_length=20, use_colors=False)
    table.render_header()
    line = table.render_row(_analysis())
    assert "Modified" not in line
