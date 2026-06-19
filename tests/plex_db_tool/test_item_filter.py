import pytest
from datetime import datetime

from plex_db_tool.item_filter import (
    ComparisonOp,
    DateCondition,
    MetadataItemFilter,
    NumericCondition,
    StringSetCondition,
    normalize_datetime,
    normalize_path_key,
    parse_smart_datetime,
    safe_int,
)


def test_comparison_op_values():
    assert ComparisonOp.EQ.value == "="
    assert ComparisonOp.NEQ.value == "!="
    assert ComparisonOp.LT.value == "<"
    assert ComparisonOp.LTE.value == "<="
    assert ComparisonOp.GT.value == ">"
    assert ComparisonOp.GTE.value == ">="


def test_numeric_condition_frozen():
    cond = NumericCondition(op=ComparisonOp.GTE, value=5)
    assert cond.op == ComparisonOp.GTE
    assert cond.value == 5
    with pytest.raises(Exception):
        cond.value = 10  # type: ignore[misc]


def test_date_condition_date_only_defaults_false():
    cond = DateCondition(op=ComparisonOp.LT, value=datetime(2026, 1, 1))
    assert cond.date_only is False


def test_string_set_condition():
    cond = StringSetCondition(op=ComparisonOp.EQ, values=frozenset({"unwatched"}))
    assert "unwatched" in cond.values


def test_metadata_item_filter_defaults():
    f = MetadataItemFilter()
    assert f.watch_status is None
    assert f.mal_status is None
    assert f.seasons == []
    assert f.episodes == []
    assert f.modified == []
    assert f.aired == []


def test_metadata_item_filter_stub_matches_everything():
    f = MetadataItemFilter()
    assert f.matches({"episode_watched": True}, {}) is True
    assert f.matches({}, {}) is True


def test_safe_int_valid():
    assert safe_int(5) == 5
    assert safe_int("12") == 12


def test_safe_int_invalid():
    assert safe_int(None) is None
    assert safe_int("") is None
    assert safe_int("abc") is None


def test_normalize_path_key():
    assert normalize_path_key("C:\\Foo\\Bar.mkv") == "c:/foo/bar.mkv"
    assert normalize_path_key("/mnt/media/ep.mkv") == "/mnt/media/ep.mkv"


def test_normalize_datetime_strips_tz():
    from datetime import timezone, timedelta
    dt_utc = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    result = normalize_datetime(dt_utc)
    assert result.tzinfo is None


def test_parse_smart_datetime_iso_date_only():
    dt, date_only = parse_smart_datetime("2026-01-15")
    assert dt == datetime(2026, 1, 15)
    assert date_only is True


def test_parse_smart_datetime_iso_datetime():
    dt, date_only = parse_smart_datetime("2026-01-15T12:30")
    assert dt == datetime(2026, 1, 15, 12, 30)
    assert date_only is False


def test_parse_smart_datetime_empty():
    dt, date_only = parse_smart_datetime("")
    assert dt is None
    assert date_only is False


from plex_db_tool.item_filter import (
    classify_file_watch_status,
    is_episode_already_watched,
    is_watching_mal_status,
    matches_date_condition,
    matches_numeric_condition,
)


# --- classify_file_watch_status ---

def test_classify_watched_via_episode_watched_flag():
    assert classify_file_watch_status({"episode_watched": True}, {}) == "watched"


def test_classify_watched_via_plex_watched_flag():
    assert classify_file_watch_status({"plex_watch_status": {"watched": True}}, {}) == "watched"


def test_classify_watched_partial_via_view_offset():
    fi = {"plex_watch_status": {"watched": False, "view_offset": 5000}}
    assert classify_file_watch_status(fi, {}) == "watched_partial"


def test_classify_watched_via_file_mal():
    fi = {"episode": 3, "myanimelist_watch_status": {"my_watched_episodes": 5}}
    assert classify_file_watch_status(fi, {}) == "watched"


def test_classify_watched_via_group_mal():
    fi = {"episode": 2}
    gd = {"myanimelist_watch_status": {"my_watched_episodes": 5}}
    assert classify_file_watch_status(fi, gd) == "watched"


def test_classify_unwatched_no_watch_data():
    assert classify_file_watch_status({"episode": 3}, {}) == "unwatched"


def test_classify_unwatched_future_episode():
    fi = {"episode": 8, "myanimelist_watch_status": {"my_watched_episodes": 5}}
    assert classify_file_watch_status(fi, {}) == "unwatched"


# --- is_episode_already_watched ---

def test_is_already_watched_via_flag():
    assert is_episode_already_watched({"episode_watched": True}, {}) is True


def test_is_already_watched_via_view_offset():
    assert is_episode_already_watched({"plex_watch_status": {"view_offset": 100}}, {}) is True


def test_is_not_watched():
    assert is_episode_already_watched({"episode": 5}, {}) is False


# --- is_watching_mal_status ---

def test_is_watching_mal_status_watching():
    assert is_watching_mal_status({"my_status": "Watching"}) is True


def test_is_watching_mal_status_not_dict():
    assert is_watching_mal_status("watching") is False


def test_is_watching_mal_status_other_status():
    assert is_watching_mal_status({"my_status": "Completed"}) is False


# --- matches_numeric_condition ---

def test_matches_numeric_eq():
    assert matches_numeric_condition(5, NumericCondition(ComparisonOp.EQ, 5)) is True
    assert matches_numeric_condition(4, NumericCondition(ComparisonOp.EQ, 5)) is False


def test_matches_numeric_neq():
    assert matches_numeric_condition(4, NumericCondition(ComparisonOp.NEQ, 5)) is True
    assert matches_numeric_condition(5, NumericCondition(ComparisonOp.NEQ, 5)) is False


def test_matches_numeric_lt():
    assert matches_numeric_condition(4, NumericCondition(ComparisonOp.LT, 5)) is True
    assert matches_numeric_condition(5, NumericCondition(ComparisonOp.LT, 5)) is False


def test_matches_numeric_lte():
    assert matches_numeric_condition(5, NumericCondition(ComparisonOp.LTE, 5)) is True
    assert matches_numeric_condition(6, NumericCondition(ComparisonOp.LTE, 5)) is False


def test_matches_numeric_gt():
    assert matches_numeric_condition(6, NumericCondition(ComparisonOp.GT, 5)) is True
    assert matches_numeric_condition(5, NumericCondition(ComparisonOp.GT, 5)) is False


def test_matches_numeric_gte():
    assert matches_numeric_condition(5, NumericCondition(ComparisonOp.GTE, 5)) is True
    assert matches_numeric_condition(4, NumericCondition(ComparisonOp.GTE, 5)) is False


# --- matches_date_condition ---

def test_matches_date_gte():
    actual = datetime(2026, 6, 15)
    cond = DateCondition(op=ComparisonOp.GTE, value=datetime(2026, 1, 1))
    assert matches_date_condition(actual, cond) is True


def test_matches_date_lt():
    actual = datetime(2025, 12, 31)
    cond = DateCondition(op=ComparisonOp.LT, value=datetime(2026, 1, 1))
    assert matches_date_condition(actual, cond) is True


def test_matches_date_eq_date_only_same_day():
    actual = datetime(2026, 1, 15, 10, 30)
    cond = DateCondition(op=ComparisonOp.EQ, value=datetime(2026, 1, 15), date_only=True)
    assert matches_date_condition(actual, cond) is True


def test_matches_date_eq_date_only_different_day():
    actual = datetime(2026, 1, 16, 10, 30)
    cond = DateCondition(op=ComparisonOp.EQ, value=datetime(2026, 1, 15), date_only=True)
    assert matches_date_condition(actual, cond) is False


def test_matches_date_neq_date_only():
    actual = datetime(2026, 1, 15, 10, 30)
    cond = DateCondition(op=ComparisonOp.NEQ, value=datetime(2026, 1, 16), date_only=True)
    assert matches_date_condition(actual, cond) is True


# --- MetadataItemFilter.matches ---

def test_filter_no_criteria_passes_everything():
    f = MetadataItemFilter()
    assert f.matches({"episode_watched": True, "season": 99}, {}) is True
    assert f.matches({}, {}) is True


def test_filter_watch_status_eq_unwatched():
    f = MetadataItemFilter(
        watch_status=StringSetCondition(ComparisonOp.EQ, frozenset({"unwatched"}))
    )
    assert f.matches({"episode": 1}, {}) is True
    assert f.matches({"episode_watched": True}, {}) is False


def test_filter_watch_status_neq_watched():
    f = MetadataItemFilter(
        watch_status=StringSetCondition(ComparisonOp.NEQ, frozenset({"watched"}))
    )
    assert f.matches({"episode": 1}, {}) is True
    assert f.matches({"episode_watched": True}, {}) is False


def test_filter_watch_status_multi_value():
    f = MetadataItemFilter(
        watch_status=StringSetCondition(ComparisonOp.EQ, frozenset({"unwatched", "watched_partial"}))
    )
    assert f.matches({"episode": 1}, {}) is True
    assert f.matches({"plex_watch_status": {"watched": False, "view_offset": 100}}, {}) is True
    assert f.matches({"episode_watched": True}, {}) is False


def test_filter_mal_status_eq_watching():
    f = MetadataItemFilter(
        mal_status=StringSetCondition(ComparisonOp.EQ, frozenset({"watching"}))
    )
    fi_watching = {"myanimelist_watch_status": {"my_status": "Watching"}}
    fi_completed = {"myanimelist_watch_status": {"my_status": "Completed"}}
    assert f.matches(fi_watching, {}) is True
    assert f.matches(fi_completed, {}) is False


def test_filter_mal_status_missing_excluded():
    f = MetadataItemFilter(
        mal_status=StringSetCondition(ComparisonOp.EQ, frozenset({"watching"}))
    )
    assert f.matches({}, {}) is False


def test_filter_season_eq():
    f = MetadataItemFilter(seasons=[NumericCondition(ComparisonOp.EQ, 1)])
    assert f.matches({"season": 1}, {}) is True
    assert f.matches({"season": 2}, {}) is False
    assert f.matches({}, {}) is False  # missing season → excluded


def test_filter_episode_gte():
    f = MetadataItemFilter(episodes=[NumericCondition(ComparisonOp.GTE, 5)])
    assert f.matches({"episode": 5}, {}) is True
    assert f.matches({"episode": 4}, {}) is False


def test_filter_episode_list_any_passes_all_conditions():
    f = MetadataItemFilter(episodes=[NumericCondition(ComparisonOp.GTE, 5)])
    assert f.matches({"episode": [3, 6]}, {}) is True   # 6 >= 5
    assert f.matches({"episode": [1, 2]}, {}) is False  # neither >= 5


def test_filter_episode_missing_excluded():
    f = MetadataItemFilter(episodes=[NumericCondition(ComparisonOp.GTE, 1)])
    assert f.matches({}, {}) is False


def test_filter_modified_gte():
    import time
    now_ts = time.time()
    f = MetadataItemFilter(
        modified=[DateCondition(op=ComparisonOp.GTE, value=datetime.fromtimestamp(now_ts))]
    )
    assert f.matches({"modified_time": now_ts + 86400}, {}) is True
    assert f.matches({"modified_time": now_ts - 86400}, {}) is False
    assert f.matches({}, {}) is False  # missing → excluded


def test_filter_multiple_criteria_all_must_pass():
    f = MetadataItemFilter(
        watch_status=StringSetCondition(ComparisonOp.EQ, frozenset({"unwatched"})),
        seasons=[NumericCondition(ComparisonOp.EQ, 1)],
    )
    assert f.matches({"season": 1, "episode": 1}, {}) is True
    assert f.matches({"season": 2, "episode": 1}, {}) is False  # wrong season
    assert f.matches({"season": 1, "episode_watched": True}, {}) is False  # watched
