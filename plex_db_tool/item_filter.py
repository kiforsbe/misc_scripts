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
