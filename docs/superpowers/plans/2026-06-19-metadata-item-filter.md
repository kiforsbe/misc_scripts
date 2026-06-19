# Metadata Item Filter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a reusable `MetadataItemFilter` class to `plex_db_tool/item_filter.py` and wire a `--item-filter` CLI argument into `sync-metadata-playlists` to pre-filter which per-group files are included in each generated playlist.

**Architecture:** A new `plex_db_tool/item_filter.py` module defines typed condition types (`ComparisonOp`, `NumericCondition`, `DateCondition`, `StringSetCondition`), a `MetadataItemFilter` dataclass with a `matches()` method, and a `MetadataItemFilterParser` that parses expression strings. Pure helpers (`safe_int`, `normalize_path_key`, `normalize_datetime`, `parse_smart_datetime`) and watch-status helpers (`is_episode_already_watched`, `is_watching_mal_status`) are moved from `sync_metadata_playlists.py` into this module; the parsing helpers (`parse_numeric_conditions`, `parse_modified_conditions`, etc.) are also moved and updated to return typed condition objects. `filter_groups` in `sync_metadata_playlists.py` is updated to use the new types. The filter is threaded through `plan_group_playlists` → `resolve_group_metadata_item_ids`.

**Tech Stack:** Python 3.9+, `dataclasses`, `enum`, `re`, `pytest`

## Global Constraints

- Python 3.9+ — no walrus operator, no structural `match` statements
- No new third-party dependencies
- All new public symbols live in `plex_db_tool/item_filter.py`
- `ComparisonOp.EQ` and `ComparisonOp.NEQ` are the only valid operators for `watch_status` and `mal_status`
- Expression tokens are whitespace-separated; values may not contain spaces; date-time values must use `T` separator if a time component is included (e.g. `2026-01-01T12:00`)

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `plex_db_tool/item_filter.py` | **Create** | All filter types, helpers, watch-status logic, parsing utilities, parser class |
| `plex_db_tool/commands/sync_metadata_playlists.py` | **Modify** | Remove moved helpers, add imports from `item_filter`, add `--item-filter` arg, thread filter through call chain |
| `tests/__init__.py` | **Create** | Test package marker (empty) |
| `tests/plex_db_tool/__init__.py` | **Create** | Test subpackage marker (empty) |
| `tests/plex_db_tool/test_item_filter.py` | **Create** | All unit tests for `item_filter.py` |
| `pytest.ini` | **Create** | Pytest root config |

---

### Task 1: Test infrastructure + condition types + pure utility helpers

**Files:**
- Create: `pytest.ini`
- Create: `tests/__init__.py`
- Create: `tests/plex_db_tool/__init__.py`
- Create: `plex_db_tool/item_filter.py`
- Create: `tests/plex_db_tool/test_item_filter.py`

**Interfaces:**
- Produces:
  - `ComparisonOp` (Enum)
  - `NumericCondition(op: ComparisonOp, value: int)` (frozen dataclass)
  - `DateCondition(op: ComparisonOp, value: datetime, date_only: bool = False)` (frozen dataclass)
  - `StringSetCondition(op: ComparisonOp, values: frozenset)` (frozen dataclass)
  - `MetadataItemFilter(watch_status, mal_status, seasons, episodes, modified, aired)` — `matches()` stub returns `True`
  - `safe_int(value: Any) -> Optional[int]`
  - `normalize_path_key(path_value: str) -> str`
  - `normalize_datetime(value: datetime) -> datetime`
  - `parse_smart_datetime(value: str) -> Tuple[Optional[datetime], bool]`

- [ ] **Step 1: Write the failing tests**

Create `tests/plex_db_tool/test_item_filter.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

```
python -m pytest tests/plex_db_tool/test_item_filter.py -v
```
Expected: `ModuleNotFoundError: No module named 'plex_db_tool.item_filter'`

- [ ] **Step 3: Create supporting files**

`pytest.ini`:
```ini
[pytest]
testpaths = tests
pythonpath = .
```

`tests/__init__.py` — empty file.

`tests/plex_db_tool/__init__.py` — empty file.

- [ ] **Step 4: Create `plex_db_tool/item_filter.py`**

```python
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple


class ComparisonOp(Enum):
    EQ = "="
    NEQ = "!="
    LT = "<"
    LTE = "<="
    GT = ">"
    GTE = ">="


@dataclass(frozen=True)
class NumericCondition:
    op: ComparisonOp
    value: int


@dataclass(frozen=True)
class DateCondition:
    op: ComparisonOp
    value: datetime
    date_only: bool = False


@dataclass(frozen=True)
class StringSetCondition:
    op: ComparisonOp
    values: frozenset


@dataclass
class MetadataItemFilter:
    watch_status: Optional[StringSetCondition] = None
    mal_status: Optional[StringSetCondition] = None
    seasons: List[NumericCondition] = field(default_factory=list)
    episodes: List[NumericCondition] = field(default_factory=list)
    modified: List[DateCondition] = field(default_factory=list)
    aired: List[DateCondition] = field(default_factory=list)

    def matches(self, file_info: Dict[str, Any], group_data: Dict[str, Any]) -> bool:
        return True


def safe_int(value: Any) -> Optional[int]:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def normalize_path_key(path_value: str) -> str:
    return str(path_value).replace("\\", "/").casefold()


def normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is not None:
        return value.astimezone().replace(tzinfo=None)
    return value


def parse_smart_datetime(value: str) -> Tuple[Optional[datetime], bool]:
    text = (value or "").strip()
    if not text:
        return None, False

    lowered = text.lower()
    now = datetime.now()
    if lowered == "now":
        return now, False
    if lowered == "today":
        return datetime(now.year, now.month, now.day), True
    if lowered == "yesterday":
        today = datetime(now.year, now.month, now.day)
        return today - timedelta(days=1), True
    if lowered == "tomorrow":
        today = datetime(now.year, now.month, now.day)
        return today + timedelta(days=1), True

    is_date_only = bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", text))
    iso_candidate = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(iso_candidate)
        return normalize_datetime(parsed), is_date_only
    except ValueError:
        pass

    formats = [
        "%Y/%m/%d",
        "%Y.%m.%d",
        "%Y%m%d",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%Y/%m/%d %H:%M:%S",
        "%d.%m.%Y",
        "%d.%m.%Y %H:%M",
        "%d.%m.%Y %H:%M:%S",
    ]
    for fmt in formats:
        try:
            parsed = datetime.strptime(text, fmt)
            date_only = fmt in ("%Y/%m/%d", "%Y.%m.%d", "%Y%m%d", "%d.%m.%Y")
            return parsed, date_only
        except ValueError:
            continue

    if re.fullmatch(r"\d{10,13}", text):
        try:
            timestamp = int(text)
            if len(text) == 13:
                timestamp = timestamp / 1000
            return datetime.fromtimestamp(timestamp), False
        except (ValueError, OSError):
            pass

    return None, False
```

- [ ] **Step 5: Run tests to verify they pass**

```
python -m pytest tests/plex_db_tool/test_item_filter.py -v
```
Expected: All 14 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add pytest.ini tests/ plex_db_tool/item_filter.py
git commit -m "feat: add MetadataItemFilter condition types and test infrastructure"
```

---

### Task 2: Watch-status helpers and `MetadataItemFilter.matches()`

**Files:**
- Modify: `plex_db_tool/item_filter.py`
- Modify: `tests/plex_db_tool/test_item_filter.py`

**Interfaces:**
- Consumes: `ComparisonOp`, `NumericCondition`, `DateCondition`, `StringSetCondition`, `MetadataItemFilter`, `safe_int`, `normalize_datetime` from Task 1
- Produces:
  - `classify_file_watch_status(file_info: Dict[str, Any], group_data: Dict[str, Any]) -> str` — returns `"watched"`, `"watched_partial"`, or `"unwatched"`
  - `is_episode_already_watched(file_info: Dict[str, Any], group: Dict[str, Any]) -> bool`
  - `is_watching_mal_status(status: Any) -> bool`
  - `matches_numeric_condition(value: int, cond: NumericCondition) -> bool`
  - `matches_date_condition(actual: datetime, cond: DateCondition) -> bool`
  - `MetadataItemFilter.matches()` — full implementation

- [ ] **Step 1: Add failing tests**

Append to `tests/plex_db_tool/test_item_filter.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

```
python -m pytest tests/plex_db_tool/test_item_filter.py -v -k "classify or is_episode or is_watching or matches_numeric or matches_date or test_filter"
```
Expected: FAIL with `ImportError` on the new names.

- [ ] **Step 3: Implement helpers and full `matches()` in `plex_db_tool/item_filter.py`**

Replace the stub `matches()` method and append the new helpers. The complete file after this step:

```python
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple


class ComparisonOp(Enum):
    EQ = "="
    NEQ = "!="
    LT = "<"
    LTE = "<="
    GT = ">"
    GTE = ">="


@dataclass(frozen=True)
class NumericCondition:
    op: ComparisonOp
    value: int


@dataclass(frozen=True)
class DateCondition:
    op: ComparisonOp
    value: datetime
    date_only: bool = False


@dataclass(frozen=True)
class StringSetCondition:
    op: ComparisonOp
    values: frozenset


@dataclass
class MetadataItemFilter:
    watch_status: Optional[StringSetCondition] = None
    mal_status: Optional[StringSetCondition] = None
    seasons: List[NumericCondition] = field(default_factory=list)
    episodes: List[NumericCondition] = field(default_factory=list)
    modified: List[DateCondition] = field(default_factory=list)
    aired: List[DateCondition] = field(default_factory=list)

    def matches(self, file_info: Dict[str, Any], group_data: Dict[str, Any]) -> bool:
        if self.watch_status is not None:
            status = classify_file_watch_status(file_info, group_data)
            in_set = status in self.watch_status.values
            if self.watch_status.op == ComparisonOp.EQ and not in_set:
                return False
            if self.watch_status.op == ComparisonOp.NEQ and in_set:
                return False

        if self.mal_status is not None:
            mal = _get_mal_status_string(file_info, group_data)
            in_set = mal in self.mal_status.values
            if self.mal_status.op == ComparisonOp.EQ and not in_set:
                return False
            if self.mal_status.op == ComparisonOp.NEQ and in_set:
                return False

        if self.seasons:
            season = safe_int(file_info.get("season"))
            if season is None:
                return False
            if not all(matches_numeric_condition(season, cond) for cond in self.seasons):
                return False

        if self.episodes:
            ep_raw = file_info.get("episode")
            ep_numbers: List[int] = []
            if isinstance(ep_raw, list):
                ep_numbers = [v for v in (safe_int(x) for x in ep_raw) if v is not None]
            else:
                ep = safe_int(ep_raw)
                if ep is not None:
                    ep_numbers = [ep]
            if not ep_numbers:
                return False
            if not any(
                all(matches_numeric_condition(ep, cond) for cond in self.episodes)
                for ep in ep_numbers
            ):
                return False

        if self.modified:
            raw = file_info.get("modified_time")
            dt = _timestamp_to_datetime(raw)
            if dt is None:
                return False
            if not all(matches_date_condition(dt, cond) for cond in self.modified):
                return False

        if self.aired:
            raw = file_info.get("aired_at")
            dt = _value_to_datetime(raw)
            if dt is None:
                return False
            if not all(matches_date_condition(dt, cond) for cond in self.aired):
                return False

        return True


def safe_int(value: Any) -> Optional[int]:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def normalize_path_key(path_value: str) -> str:
    return str(path_value).replace("\\", "/").casefold()


def normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is not None:
        return value.astimezone().replace(tzinfo=None)
    return value


def parse_smart_datetime(value: str) -> Tuple[Optional[datetime], bool]:
    text = (value or "").strip()
    if not text:
        return None, False

    lowered = text.lower()
    now = datetime.now()
    if lowered == "now":
        return now, False
    if lowered == "today":
        return datetime(now.year, now.month, now.day), True
    if lowered == "yesterday":
        today = datetime(now.year, now.month, now.day)
        return today - timedelta(days=1), True
    if lowered == "tomorrow":
        today = datetime(now.year, now.month, now.day)
        return today + timedelta(days=1), True

    is_date_only = bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", text))
    iso_candidate = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(iso_candidate)
        return normalize_datetime(parsed), is_date_only
    except ValueError:
        pass

    formats = [
        "%Y/%m/%d",
        "%Y.%m.%d",
        "%Y%m%d",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%Y/%m/%d %H:%M:%S",
        "%d.%m.%Y",
        "%d.%m.%Y %H:%M",
        "%d.%m.%Y %H:%M:%S",
    ]
    for fmt in formats:
        try:
            parsed = datetime.strptime(text, fmt)
            date_only = fmt in ("%Y/%m/%d", "%Y.%m.%d", "%Y%m%d", "%d.%m.%Y")
            return parsed, date_only
        except ValueError:
            continue

    if re.fullmatch(r"\d{10,13}", text):
        try:
            timestamp = int(text)
            if len(text) == 13:
                timestamp = timestamp / 1000
            return datetime.fromtimestamp(timestamp), False
        except (ValueError, OSError):
            pass

    return None, False


def classify_file_watch_status(file_info: Dict[str, Any], group_data: Dict[str, Any]) -> str:
    if file_info.get("episode_watched"):
        return "watched"
    plex_status = file_info.get("plex_watch_status") or {}
    if plex_status.get("watched"):
        return "watched"
    if plex_status.get("view_offset", 0) > 0:
        return "watched_partial"
    mal_status = (
        file_info.get("myanimelist_watch_status")
        or group_data.get("myanimelist_watch_status")
    )
    if isinstance(mal_status, dict):
        watched_eps = safe_int(mal_status.get("my_watched_episodes"))
        episode = safe_int(file_info.get("episode"))
        if watched_eps is not None and episode is not None and episode <= watched_eps:
            return "watched"
    return "unwatched"


def is_episode_already_watched(file_info: Dict[str, Any], group: Dict[str, Any]) -> bool:
    if file_info.get("episode_watched"):
        return True
    plex_status = file_info.get("plex_watch_status") or {}
    if plex_status.get("watched") or plex_status.get("view_offset", 0) > 0:
        return True
    mal_status = (
        file_info.get("myanimelist_watch_status")
        or (group.get("group_data") or {}).get("myanimelist_watch_status")
    )
    if not isinstance(mal_status, dict):
        return False
    watched_eps = safe_int(mal_status.get("my_watched_episodes"))
    episode = safe_int(file_info.get("episode"))
    if watched_eps is None or episode is None:
        return False
    return episode <= watched_eps


def is_watching_mal_status(status: Any) -> bool:
    if not isinstance(status, dict):
        return False
    my_status = str(status.get("my_status") or "").strip().casefold()
    return my_status in {"watching", "watching (season)", "watching_season"}


def matches_numeric_condition(value: int, cond: NumericCondition) -> bool:
    op = cond.op
    if op == ComparisonOp.EQ:
        return value == cond.value
    if op == ComparisonOp.NEQ:
        return value != cond.value
    if op == ComparisonOp.LT:
        return value < cond.value
    if op == ComparisonOp.LTE:
        return value <= cond.value
    if op == ComparisonOp.GT:
        return value > cond.value
    if op == ComparisonOp.GTE:
        return value >= cond.value
    return False


def matches_date_condition(actual: datetime, cond: DateCondition) -> bool:
    norm_actual = normalize_datetime(actual)
    norm_target = normalize_datetime(cond.value)
    op = cond.op
    if cond.date_only and op in (ComparisonOp.EQ, ComparisonOp.NEQ):
        is_equal = norm_actual.date() == norm_target.date()
        return (not is_equal) if op == ComparisonOp.NEQ else is_equal
    if op == ComparisonOp.EQ:
        return norm_actual == norm_target
    if op == ComparisonOp.NEQ:
        return norm_actual != norm_target
    if op == ComparisonOp.LT:
        return norm_actual < norm_target
    if op == ComparisonOp.LTE:
        return norm_actual <= norm_target
    if op == ComparisonOp.GT:
        return norm_actual > norm_target
    if op == ComparisonOp.GTE:
        return norm_actual >= norm_target
    return False


def _get_mal_status_string(file_info: Dict[str, Any], group_data: Dict[str, Any]) -> str:
    mal = (
        file_info.get("myanimelist_watch_status")
        or group_data.get("myanimelist_watch_status")
    )
    if not isinstance(mal, dict):
        return ""
    return str(mal.get("my_status") or "").strip().casefold()


def _timestamp_to_datetime(value: Any) -> Optional[datetime]:
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value))
        except (ValueError, OSError, OverflowError):
            return None
    return None


def _value_to_datetime(value: Any) -> Optional[datetime]:
    if isinstance(value, (int, float)):
        return _timestamp_to_datetime(value)
    if isinstance(value, str) and value.strip():
        parsed, _ = parse_smart_datetime(value.strip())
        return parsed
    return None
```

- [ ] **Step 4: Run all tests to verify they pass**

```
python -m pytest tests/plex_db_tool/test_item_filter.py -v
```
Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add plex_db_tool/item_filter.py tests/plex_db_tool/test_item_filter.py
git commit -m "feat: implement classify_file_watch_status and MetadataItemFilter.matches"
```

---

### Task 3: Move and update parsing helpers; update `filter_groups`

**Files:**
- Modify: `plex_db_tool/item_filter.py`
- Modify: `plex_db_tool/commands/sync_metadata_playlists.py`
- Modify: `tests/plex_db_tool/test_item_filter.py`

**Interfaces:**
- Consumes: `ComparisonOp`, `NumericCondition`, `DateCondition`, `normalize_datetime`, `parse_smart_datetime` from Tasks 1–2
- Produces:
  - `_parse_comparison_op(op_str: str) -> ComparisonOp`
  - `parse_numeric_expression(expression: str, argument_name: str) -> NumericCondition`
  - `parse_numeric_conditions(expression: str, argument_name: str) -> List[NumericCondition]`
  - `parse_modified_expression(expression: str) -> DateCondition`
  - `parse_modified_conditions(expression: str) -> List[DateCondition]`
  - `matches_numeric_condition` and `matches_date_condition` already exist from Task 2

- [ ] **Step 1: Add failing tests**

Append to `tests/plex_db_tool/test_item_filter.py`:

```python
from plex_db_tool.item_filter import (
    parse_modified_conditions,
    parse_modified_expression,
    parse_numeric_conditions,
    parse_numeric_expression,
)


# --- parse_numeric_expression ---

def test_parse_numeric_expr_gte():
    cond = parse_numeric_expression(">=5", "episode")
    assert cond == NumericCondition(op=ComparisonOp.GTE, value=5)


def test_parse_numeric_expr_plain_equals():
    cond = parse_numeric_expression("12", "episode")
    assert cond == NumericCondition(op=ComparisonOp.EQ, value=12)


def test_parse_numeric_expr_invalid_value():
    with pytest.raises(ValueError, match="episode"):
        parse_numeric_expression("abc", "episode")


# --- parse_numeric_conditions ---

def test_parse_numeric_conditions_single():
    conds = parse_numeric_conditions(">=5", "episode")
    assert conds == [NumericCondition(ComparisonOp.GTE, 5)]


def test_parse_numeric_conditions_range():
    conds = parse_numeric_conditions("1..12", "episode")
    assert conds == [
        NumericCondition(ComparisonOp.GTE, 1),
        NumericCondition(ComparisonOp.LTE, 12),
    ]


def test_parse_numeric_conditions_comma():
    conds = parse_numeric_conditions(">=5,<=12", "episode")
    assert conds == [
        NumericCondition(ComparisonOp.GTE, 5),
        NumericCondition(ComparisonOp.LTE, 12),
    ]


def test_parse_numeric_conditions_invalid_range_order():
    with pytest.raises(ValueError):
        parse_numeric_conditions("12..1", "episode")


# --- parse_modified_expression ---

def test_parse_modified_expr_gte_date_only():
    cond = parse_modified_expression(">=2026-01-01")
    assert cond.op == ComparisonOp.GTE
    assert cond.value == datetime(2026, 1, 1)
    assert cond.date_only is True


def test_parse_modified_expr_lt_datetime():
    cond = parse_modified_expression("<2026-01-01T12:00")
    assert cond.op == ComparisonOp.LT
    assert cond.value == datetime(2026, 1, 1, 12, 0)
    assert cond.date_only is False


def test_parse_modified_expr_invalid():
    with pytest.raises(ValueError, match="--modified"):
        parse_modified_expression(">=not-a-date")


# --- parse_modified_conditions ---

def test_parse_modified_conditions_range():
    conds = parse_modified_conditions("2026-01-01..2026-06-30")
    assert len(conds) == 2
    assert conds[0].op == ComparisonOp.GTE
    assert conds[1].op == ComparisonOp.LTE


def test_parse_modified_conditions_invalid_range_order():
    with pytest.raises(ValueError):
        parse_modified_conditions("2026-06-30..2026-01-01")
```

- [ ] **Step 2: Run tests to verify they fail**

```
python -m pytest tests/plex_db_tool/test_item_filter.py -v -k "parse_numeric or parse_modified"
```
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Add parsing helpers to `plex_db_tool/item_filter.py`**

Append to the bottom of `plex_db_tool/item_filter.py`:

```python
_OP_MAP = {
    "=": ComparisonOp.EQ,
    "==": ComparisonOp.EQ,
    "!=": ComparisonOp.NEQ,
    "<": ComparisonOp.LT,
    "<=": ComparisonOp.LTE,
    ">": ComparisonOp.GT,
    ">=": ComparisonOp.GTE,
}


def _parse_comparison_op(op_str: str) -> ComparisonOp:
    result = _OP_MAP.get(op_str)
    if result is None:
        raise ValueError(f"Unknown operator '{op_str}'. Valid operators: {', '.join(_OP_MAP)}")
    return result


def parse_numeric_expression(expression: str, argument_name: str) -> NumericCondition:
    expr = (expression or "").strip()
    match = re.match(r"^(<=|>=|<|>|==|=|!=)\s*(.+)$", expr)
    if match:
        op_str = match.group(1)
        raw_value = match.group(2).strip()
    else:
        op_str = "="
        raw_value = expr
    if not re.fullmatch(r"-?\d+", raw_value):
        raise ValueError(
            f"Invalid {argument_name} expression '{expression}'. "
            f"Use integer values like '<12', '>=24', or '=13'."
        )
    return NumericCondition(op=_parse_comparison_op(op_str), value=int(raw_value))


def parse_numeric_conditions(expression: str, argument_name: str) -> List[NumericCondition]:
    expr = (expression or "").strip()
    if not expr:
        raise ValueError(f"{argument_name} cannot be empty")
    if ".." in expr:
        parts = expr.split("..")
        if len(parts) != 2 or not parts[0].strip() or not parts[1].strip():
            raise ValueError(
                f"Invalid {argument_name} range '{expression}'. Use format like '12..24'."
            )
        start = parse_numeric_expression(f">={parts[0].strip()}", argument_name)
        end = parse_numeric_expression(f"<={parts[1].strip()}", argument_name)
        if start.value > end.value:
            raise ValueError(
                f"Invalid {argument_name} range '{expression}'. "
                f"Range start must be less than or equal to range end."
            )
        return [start, end]
    if "," in expr:
        parts = [p.strip() for p in expr.split(",") if p.strip()]
        if not parts:
            raise ValueError(f"{argument_name} cannot be empty")
        return [parse_numeric_expression(p, argument_name) for p in parts]
    return [parse_numeric_expression(expr, argument_name)]


def parse_modified_expression(expression: str) -> DateCondition:
    expr = (expression or "").strip()
    match = re.match(r"^(<=|>=|<|>|==|=|!=)\s*(.+)$", expr)
    if match:
        op_str = match.group(1)
        raw_value = match.group(2).strip()
    else:
        op_str = "="
        raw_value = expr
    parsed_dt, is_date_only = parse_smart_datetime(raw_value)
    if parsed_dt is None:
        raise ValueError(
            f"Invalid --modified expression '{expression}'. "
            f"Use forms like '<2026-01-01' or '>=2026-01-01T15:30'."
        )
    return DateCondition(op=_parse_comparison_op(op_str), value=parsed_dt, date_only=is_date_only)


def parse_modified_conditions(expression: str) -> List[DateCondition]:
    expr = (expression or "").strip()
    if not expr:
        raise ValueError("--modified cannot be empty")
    if ".." in expr:
        parts = expr.split("..")
        if len(parts) != 2 or not parts[0].strip() or not parts[1].strip():
            raise ValueError(
                f"Invalid --modified range '{expression}'. "
                f"Use format like '2026-01-01..2026-01-31'."
            )
        start = parse_modified_expression(f">={parts[0].strip()}")
        end = parse_modified_expression(f"<={parts[1].strip()}")
        if normalize_datetime(start.value) > normalize_datetime(end.value):
            raise ValueError(
                f"Invalid --modified range '{expression}'. "
                f"Range start must be earlier than or equal to range end."
            )
        return [start, end]
    if "," in expr:
        parts = [p.strip() for p in expr.split(",") if p.strip()]
        if not parts:
            raise ValueError("--modified cannot be empty")
        return [parse_modified_expression(p) for p in parts]
    return [parse_modified_expression(expr)]
```

- [ ] **Step 4: Run parsing tests to verify they pass**

```
python -m pytest tests/plex_db_tool/test_item_filter.py -v -k "parse_numeric or parse_modified"
```
Expected: All parsing tests PASS.

- [ ] **Step 5: Update `sync_metadata_playlists.py` — remove moved functions, add imports, update `filter_groups`**

**5a.** Remove these functions from `sync_metadata_playlists.py` (they are now in `item_filter.py`):
- `safe_int` (lines 796–802)
- `normalize_datetime` (lines 939–942)
- `parse_smart_datetime` (lines 945–999)
- `parse_modified_expression` (lines 1002–1019)
- `parse_modified_conditions` (lines 1022–1040)
- `matches_modified_expression` (lines 1043–1061)
- `parse_numeric_expression` (lines 1064–1079)
- `parse_numeric_conditions` (lines 1082–1100)
- `matches_numeric_expression` (lines 1103–1116)
- `is_episode_already_watched` (lines 554–570)
- `is_watching_mal_status` (lines 547–551)
- `normalize_path_key` (lines 1814–1815)

**5b.** Replace the existing imports block at the top of `sync_metadata_playlists.py` (the `from ..` imports, lines 11–15) with:

```python
from ..cli_support import PlexCliSupport
from ..infrastructure import PlexDatabase, PlexDatabaseLocator, PlexEnvironment, PlexFilenameParser
from ..item_filter import (
    is_episode_already_watched,
    is_watching_mal_status,
    matches_date_condition,
    matches_numeric_condition,
    normalize_path_key,
    parse_modified_conditions,
    parse_numeric_conditions,
    safe_int,
)
from ..models import MediaRecord, PlannedMutation, PlexPlaylist, TableColumnSpec
from ..planners import PlexMatcher, PlexPlaylistPlanner
from ..reporting import PlexReportWriter
```

**5c.** Update the `filter_groups` function body in `sync_metadata_playlists.py`. Replace the three filtering blocks:

```python
    if modified:
        conditions = parse_modified_conditions(modified)
        filtered = [
            group for group in filtered
            if group["modified_at"] is not None
            and all(matches_date_condition(group["modified_at"], cond) for cond in conditions)
        ]
    if episodes_found:
        conditions = parse_numeric_conditions(episodes_found, "--episodes-found")
        filtered = [
            group for group in filtered
            if all(matches_numeric_condition(int(group["episodes_found"] or 0), cond) for cond in conditions)
        ]
    if episodes_expected:
        conditions = parse_numeric_conditions(episodes_expected, "--episodes-expected")
        filtered = [
            group for group in filtered
            if all(matches_numeric_condition(int(group["episodes_expected"] or 0), cond) for cond in conditions)
        ]
```

- [ ] **Step 6: Verify the full test suite still passes**

```
python -m pytest tests/ -v
```
Expected: All tests PASS with no import errors.

- [ ] **Step 7: Commit**

```bash
git add plex_db_tool/item_filter.py plex_db_tool/commands/sync_metadata_playlists.py tests/plex_db_tool/test_item_filter.py
git commit -m "feat: move and update parsing helpers to item_filter; update filter_groups to use typed conditions"
```

---

### Task 4: `MetadataItemFilterParser`

**Files:**
- Modify: `plex_db_tool/item_filter.py`
- Modify: `tests/plex_db_tool/test_item_filter.py`

**Interfaces:**
- Consumes: `MetadataItemFilter`, `StringSetCondition`, `ComparisonOp`, `_parse_comparison_op`, `parse_numeric_conditions`, `parse_numeric_expression`, `parse_modified_conditions`, `parse_modified_expression` from Tasks 1–3
- Produces: `MetadataItemFilterParser` class with classmethod `parse(expression: str) -> MetadataItemFilter`

- [ ] **Step 1: Add failing tests**

Append to `tests/plex_db_tool/test_item_filter.py`:

```python
from plex_db_tool.item_filter import MetadataItemFilterParser


# --- MetadataItemFilterParser ---

def test_parser_empty_expression_returns_no_filter():
    f = MetadataItemFilterParser.parse("")
    assert f == MetadataItemFilter()


def test_parser_watch_status_eq():
    f = MetadataItemFilterParser.parse("watch_status=unwatched")
    assert f.watch_status == StringSetCondition(ComparisonOp.EQ, frozenset({"unwatched"}))


def test_parser_watch_status_neq():
    f = MetadataItemFilterParser.parse("watch_status!=watched")
    assert f.watch_status == StringSetCondition(ComparisonOp.NEQ, frozenset({"watched"}))


def test_parser_watch_status_multi_value():
    f = MetadataItemFilterParser.parse("watch_status=unwatched,watched_partial")
    assert f.watch_status == StringSetCondition(
        ComparisonOp.EQ, frozenset({"unwatched", "watched_partial"})
    )


def test_parser_watch_status_invalid_value():
    with pytest.raises(ValueError, match="watch_status"):
        MetadataItemFilterParser.parse("watch_status=flying")


def test_parser_mal_status_single():
    f = MetadataItemFilterParser.parse("mal_status=watching")
    assert f.mal_status == StringSetCondition(ComparisonOp.EQ, frozenset({"watching"}))


def test_parser_mal_status_multi():
    f = MetadataItemFilterParser.parse("mal_status=watching,completed")
    assert f.mal_status == StringSetCondition(
        ComparisonOp.EQ, frozenset({"watching", "completed"})
    )


def test_parser_episode_gte():
    f = MetadataItemFilterParser.parse("episode>=5")
    assert f.episodes == [NumericCondition(ComparisonOp.GTE, 5)]


def test_parser_season_range():
    f = MetadataItemFilterParser.parse("season=1..2")
    assert f.seasons == [
        NumericCondition(ComparisonOp.GTE, 1),
        NumericCondition(ComparisonOp.LTE, 2),
    ]


def test_parser_modified_gte():
    f = MetadataItemFilterParser.parse("modified>=2026-01-01")
    assert len(f.modified) == 1
    assert f.modified[0].op == ComparisonOp.GTE
    assert f.modified[0].value == datetime(2026, 1, 1)
    assert f.modified[0].date_only is True


def test_parser_multiple_tokens_space_separated():
    f = MetadataItemFilterParser.parse("watch_status=unwatched episode>=5")
    assert f.watch_status is not None
    assert f.episodes == [NumericCondition(ComparisonOp.GTE, 5)]


def test_parser_multiple_episode_conditions_accumulate():
    f = MetadataItemFilterParser.parse("episode>=5 episode<=12")
    assert f.episodes == [
        NumericCondition(ComparisonOp.GTE, 5),
        NumericCondition(ComparisonOp.LTE, 12),
    ]


def test_parser_watch_status_later_token_replaces_earlier():
    f = MetadataItemFilterParser.parse("watch_status=unwatched watch_status=watched")
    assert f.watch_status == StringSetCondition(ComparisonOp.EQ, frozenset({"watched"}))


def test_parser_unknown_field_raises():
    with pytest.raises(ValueError, match="Unknown field"):
        MetadataItemFilterParser.parse("foo=bar")


def test_parser_no_valid_tokens_raises():
    with pytest.raises(ValueError, match="no valid"):
        MetadataItemFilterParser.parse("!@#$%")
```

- [ ] **Step 2: Run tests to verify they fail**

```
python -m pytest tests/plex_db_tool/test_item_filter.py -v -k "parser"
```
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Append `MetadataItemFilterParser` to `plex_db_tool/item_filter.py`**

```python
_WATCH_STATUS_VALUES = frozenset({"watched", "watched_partial", "unwatched"})
_ITEM_FILTER_FIELDS = frozenset({"watch_status", "mal_status", "season", "episode", "modified", "aired"})
_TOKEN_RE = re.compile(r"([a-z_]+)(<=|>=|!=|<|>|==|=)([^\s]+)")


class MetadataItemFilterParser:
    @classmethod
    def parse(cls, expression: str) -> MetadataItemFilter:
        expr = (expression or "").strip()
        if not expr:
            return MetadataItemFilter()

        tokens = _TOKEN_RE.findall(expr)
        if not tokens:
            raise ValueError(
                f"Invalid --item-filter expression '{expression}': no valid field=value tokens found. "
                f"Valid fields: {', '.join(sorted(_ITEM_FILTER_FIELDS))}"
            )

        result = MetadataItemFilter()
        for field_name, op_str, raw_value in tokens:
            if field_name not in _ITEM_FILTER_FIELDS:
                raise ValueError(
                    f"Unknown field '{field_name}' in --item-filter. "
                    f"Valid fields: {', '.join(sorted(_ITEM_FILTER_FIELDS))}"
                )
            op = _parse_comparison_op(op_str)

            if field_name == "watch_status":
                if op not in (ComparisonOp.EQ, ComparisonOp.NEQ):
                    raise ValueError(f"watch_status only supports = and != operators, got '{op_str}'")
                values = frozenset(v.strip().casefold() for v in raw_value.split(",") if v.strip())
                invalid = values - _WATCH_STATUS_VALUES
                if invalid:
                    raise ValueError(
                        f"Unknown watch_status value(s): {', '.join(sorted(invalid))}. "
                        f"Valid: {', '.join(sorted(_WATCH_STATUS_VALUES))}"
                    )
                result.watch_status = StringSetCondition(op=op, values=values)

            elif field_name == "mal_status":
                if op not in (ComparisonOp.EQ, ComparisonOp.NEQ):
                    raise ValueError(f"mal_status only supports = and != operators, got '{op_str}'")
                values = frozenset(v.strip().casefold() for v in raw_value.split(",") if v.strip())
                result.mal_status = StringSetCondition(op=op, values=values)

            elif field_name == "season":
                if ".." in raw_value:
                    result.seasons.extend(parse_numeric_conditions(raw_value, "season"))
                else:
                    result.seasons.append(parse_numeric_expression(f"{op_str}{raw_value}", "season"))

            elif field_name == "episode":
                if ".." in raw_value:
                    result.episodes.extend(parse_numeric_conditions(raw_value, "episode"))
                else:
                    result.episodes.append(parse_numeric_expression(f"{op_str}{raw_value}", "episode"))

            elif field_name == "modified":
                if ".." in raw_value:
                    result.modified.extend(parse_modified_conditions(raw_value))
                else:
                    result.modified.append(parse_modified_expression(f"{op_str}{raw_value}"))

            elif field_name == "aired":
                if ".." in raw_value:
                    result.aired.extend(parse_modified_conditions(raw_value))
                else:
                    result.aired.append(parse_modified_expression(f"{op_str}{raw_value}"))

        return result
```

- [ ] **Step 4: Run all tests to verify they pass**

```
python -m pytest tests/plex_db_tool/test_item_filter.py -v
```
Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add plex_db_tool/item_filter.py tests/plex_db_tool/test_item_filter.py
git commit -m "feat: add MetadataItemFilterParser for expression-based item filtering"
```

---

### Task 5: CLI integration

**Files:**
- Modify: `plex_db_tool/commands/sync_metadata_playlists.py`
- Modify: `tests/plex_db_tool/test_item_filter.py`

**Interfaces:**
- Consumes: `MetadataItemFilter`, `MetadataItemFilterParser` from Tasks 1–4
- Produces:
  - `--item-filter EXPR` CLI argument on `sync-metadata-playlists`
  - `resolve_group_metadata_item_ids(..., item_filter: Optional[MetadataItemFilter] = None)`
  - `plan_group_playlists(..., item_filter: Optional[MetadataItemFilter] = None)`

- [ ] **Step 1: Add failing integration tests**

Append to `tests/plex_db_tool/test_item_filter.py`:

```python
from plex_db_tool.commands.sync_metadata_playlists import resolve_group_metadata_item_ids
from plex_db_tool.planners import PlexMatcher


def _make_group(files):
    return {"files": files, "group_data": {}}


def test_resolve_no_filter_includes_all_files():
    group = _make_group([
        {"filename": "ep1.mkv", "episode": 1},
        {"filename": "ep2.mkv", "episode": 2, "episode_watched": True},
    ])
    _, unmatched = resolve_group_metadata_item_ids(group, {}, {}, PlexMatcher("balanced", 0.65))
    assert len(unmatched) == 2  # both attempted, both unmatched (no inventory)


def test_resolve_item_filter_excludes_watched():
    group = _make_group([
        {"filename": "ep1.mkv", "episode": 1},
        {"filename": "ep2.mkv", "episode": 2, "episode_watched": True},
    ])
    item_filter = MetadataItemFilter(
        watch_status=StringSetCondition(ComparisonOp.EQ, frozenset({"unwatched"}))
    )
    _, unmatched = resolve_group_metadata_item_ids(
        group, {}, {}, PlexMatcher("balanced", 0.65), item_filter
    )
    assert len(unmatched) == 1  # only ep1 passed the filter; ep2 was excluded before matching
    assert "ep1.mkv" in unmatched[0]


def test_resolve_item_filter_all_excluded_returns_empty():
    group = _make_group([
        {"filename": "ep1.mkv", "episode": 1, "episode_watched": True},
    ])
    item_filter = MetadataItemFilter(
        watch_status=StringSetCondition(ComparisonOp.EQ, frozenset({"unwatched"}))
    )
    matched, unmatched = resolve_group_metadata_item_ids(
        group, {}, {}, PlexMatcher("balanced", 0.65), item_filter
    )
    assert matched == []
    assert unmatched == []


def test_resolve_item_filter_season_range():
    group = _make_group([
        {"filename": "s1e1.mkv", "season": 1, "episode": 1},
        {"filename": "s2e1.mkv", "season": 2, "episode": 1},
    ])
    item_filter = MetadataItemFilter(seasons=[NumericCondition(ComparisonOp.EQ, 1)])
    _, unmatched = resolve_group_metadata_item_ids(
        group, {}, {}, PlexMatcher("balanced", 0.65), item_filter
    )
    assert len(unmatched) == 1
    assert "s1e1.mkv" in unmatched[0]
```

- [ ] **Step 2: Run tests to verify they fail**

```
python -m pytest tests/plex_db_tool/test_item_filter.py -v -k "resolve"
```
Expected: FAIL — `resolve_group_metadata_item_ids` does not accept `item_filter` parameter.

- [ ] **Step 3: Update `sync_metadata_playlists.py` — add `--item-filter` argument**

In `register()`, add after the `--virtual-playlist-watching` argument (around line 262):

```python
    parser.add_argument(
        "--item-filter",
        default=None,
        metavar="EXPR",
        help=(
            "Filter which individual files are included in each playlist. "
            "Space-separated field=value tokens. "
            "Fields: watch_status, mal_status, season, episode, modified, aired. "
            "Examples: 'watch_status=unwatched', 'mal_status=watching episode>=5', 'season=1..2'."
        ),
    )
```

- [ ] **Step 4: Update `sync_metadata_playlists.py` — parse filter in `run()` and add import**

Add to the imports at the top of `sync_metadata_playlists.py` (extend the existing `from ..item_filter import` block):

```python
from ..item_filter import (
    MetadataItemFilter,
    MetadataItemFilterParser,
    is_episode_already_watched,
    is_watching_mal_status,
    matches_date_condition,
    matches_numeric_condition,
    normalize_path_key,
    parse_modified_conditions,
    parse_numeric_conditions,
    safe_int,
)
```

In `run()`, after `groups = add_virtual_watching_playlist(...)` and before `if not groups:`, add:

```python
    item_filter: Optional[MetadataItemFilter] = None
    if args.item_filter:
        try:
            item_filter = MetadataItemFilterParser.parse(args.item_filter)
        except ValueError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            return 1
```

- [ ] **Step 5: Update `plan_group_playlists` to accept and thread `item_filter`**

Change the signature of `plan_group_playlists` (around line 1119):

```python
def plan_group_playlists(
    groups: Sequence[Dict[str, Any]],
    target_inventory: Sequence[MediaRecord],
    target_playlists: Sequence[PlexPlaylist],
    target_account_id: int,
    conflict_policy: str,
    include_empty_playlists: bool,
    include_earlier_episodes: bool,
    restore_removed_playlists: bool,
    restore_removed_playlist_items: bool,
    deleted_metadata_playlists: Sequence[PlexPlaylist],
    playlist_prefix: str,
    playlist_status_prefix: bool,
    playlist_suffix: str,
    playlist_status_suffix: bool,
    playlist_complete_suffix: str,
    item_filter: Optional[MetadataItemFilter] = None,
) -> Tuple[List[Dict[str, Any]], List[PlannedMutation]]:
```

In the call to `resolve_group_metadata_item_ids` inside `plan_group_playlists` (around line 1173), add the parameter:

```python
        matched_records, unmatched_files = resolve_group_metadata_item_ids(
            group, target_indexes, path_index, matcher, item_filter
        )
```

In `run()`, add `item_filter=item_filter` to the `plan_group_playlists(...)` call (around line 305):

```python
        plans, mutations = plan_group_playlists(
            groups,
            target_inventory,
            target_playlists,
            target_account_id,
            args.playlist_conflict_policy,
            args.include_empty_playlists,
            args.include_earlier_episodes,
            args.restore_removed_playlists,
            args.restore_removed_playlist_items,
            deleted_metadata_playlists,
            args.playlist_prefix,
            args.playlist_status_prefix,
            args.playlist_suffix,
            args.playlist_status_suffix,
            args.playlist_complete_suffix,
            item_filter=item_filter,
        )
```

- [ ] **Step 6: Update `resolve_group_metadata_item_ids` to pre-filter files**

Change the signature and add pre-filtering (around line 1818):

```python
def resolve_group_metadata_item_ids(
    group: Dict[str, Any],
    target_indexes: Dict[str, List[MediaRecord]],
    path_index: Dict[str, List[MediaRecord]],
    matcher: PlexMatcher,
    item_filter: Optional[MetadataItemFilter] = None,
) -> Tuple[List[MediaRecord], List[str]]:
    matched_records: List[MediaRecord] = []
    unmatched_files: List[str] = []
    seen_ids: Set[int] = set()

    files = group["files"]
    if item_filter is not None:
        group_data = group.get("group_data") or {}
        files = [f for f in files if item_filter.matches(f, group_data)]

    for file_info in files:
        ...  # rest of the loop body unchanged
```

- [ ] **Step 7: Run all tests to verify they pass**

```
python -m pytest tests/ -v
```
Expected: All tests PASS.

- [ ] **Step 8: Commit**

```bash
git add plex_db_tool/commands/sync_metadata_playlists.py tests/plex_db_tool/test_item_filter.py
git commit -m "feat: wire --item-filter CLI arg into sync-metadata-playlists"
```

---

## Self-Review

**Spec coverage:**
- ✅ Typed condition types (`ComparisonOp`, `NumericCondition`, `DateCondition`, `StringSetCondition`) — Task 1
- ✅ `MetadataItemFilter` dataclass with `matches()` — Tasks 1–2
- ✅ `classify_file_watch_status` (watch status: primary priority) — Task 2
- ✅ MAL status filtering — Task 2
- ✅ Season/episode filtering with numeric conditions — Task 2
- ✅ Modified/aired date filtering — Task 2
- ✅ Parsing helpers moved from `sync_metadata_playlists.py` with typed return values — Task 3
- ✅ `filter_groups` updated to use new types — Task 3
- ✅ `MetadataItemFilterParser` with expression syntax — Task 4
- ✅ `--item-filter` CLI argument — Task 5
- ✅ Pre-filtering in `resolve_group_metadata_item_ids` — Task 5
- ✅ Error handling: `ValueError` caught in `run()`, clean stderr message — Task 5
- ✅ `watch_status` later token replaces earlier (string set fields) — Task 4 parser
- ✅ `episode` conditions accumulate (list fields) — Task 4 parser
- ✅ Multi-episode file: any episode number satisfying all conditions passes — Task 2

**Placeholder scan:** None found.

**Type consistency:**
- `matches_date_condition(actual: datetime, cond: DateCondition)` used consistently in Tasks 2, 3, and the `filter_groups` replacement in Task 3
- `matches_numeric_condition(value: int, cond: NumericCondition)` used consistently in Tasks 2, 3
- `parse_numeric_conditions` returns `List[NumericCondition]` — matches Task 3 implementation and Task 4 parser usage
- `parse_modified_conditions` returns `List[DateCondition]` — matches Task 3 implementation and Task 4 parser usage
- `item_filter: Optional[MetadataItemFilter] = None` parameter name used consistently across `plan_group_playlists` and `resolve_group_metadata_item_ids`
