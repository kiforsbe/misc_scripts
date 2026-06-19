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
