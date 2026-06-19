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
