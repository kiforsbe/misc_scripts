"""
Where tracks might start and what they're called, from what the uploader
wrote: chapters, then a lenient timestamp parse of the description and of
comments (catching list formats yt-dlp's own chapter extraction misses).
Also finds the "open spans" - long stretches no hint covers - which the
track planner fills in from the audio.
"""

import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

# A stretch this long with no hint is a "very long portion without
# chapters" (a looped tail, an unlisted second half, ...).
LONG_SPAN_S = 12 * 60
# A timestamp list needs this many lines, so a stray "the drop at 2:30"
# isn't taken as a tracklist.
MIN_TIMESTAMP_LINES = 3
# A comment's tracklist starts this close to 0:00; a list of favourite
# moments ("3:12 when the bass drops") starts anywhere.
COMMENT_LIST_START_S = 60
# A hint from a later source this close to an existing one is the same hint.
_SAME_HINT_S = 1.0

_UNTITLED_CHAPTER = re.compile(r"^<Untitled Chapter \d+>$")
# "1:02:03", "12:34", "2:05", optionally bracketed; not part of a longer
# number run like "123:45" or "1:2:3:4".
_TIMESTAMP = re.compile(r"[\[(]?(?<![\d:])(?:(\d{1,2}):)?(\d{1,2}):(\d{2})(?![\d:])[\])]?")
_EDGE_SEPARATORS = re.compile(r"^[\s\-–—|:]+|[\s\-–—|:]+$")


@dataclass(frozen=True)
class Hint:
    time: float
    title: Optional[str]


def chapter_hints(chapters: Optional[Iterable[Dict[str, Any]]]) -> List[Hint]:
    """yt-dlp chapter dicts -> hints in time order. yt-dlp's
    "<Untitled Chapter N>" placeholder (or a blank title) becomes None;
    chapters sharing a start time keep only the first."""
    hints: List[Hint] = []
    for chapter in chapters or []:
        start = chapter.get("start_time")
        if start is None:
            continue
        title = (chapter.get("title") or "").strip()
        if not title or _UNTITLED_CHAPTER.match(title):
            title = None
        hints.append(Hint(float(start), title))
    hints.sort(key=lambda h: h.time)
    return [h for i, h in enumerate(hints) if i == 0 or h.time != hints[i - 1].time]


def _longest_increasing_run(entries: List[Tuple[int, Optional[str]]]) -> List[Tuple[int, Optional[str]]]:
    best: List[Tuple[int, Optional[str]]] = []
    current: List[Tuple[int, Optional[str]]] = []
    for entry in entries:
        if current and entry[0] <= current[-1][0]:
            current = []
        current.append(entry)
        if len(current) > len(best):
            best = list(current)
    return best


def parse_timestamp_lines(text: Optional[str], duration: float, allow_lengths: bool = True) -> List[Hint]:
    """
    Lenient tracklist parse: the first timestamp on each line, the rest of
    the line as the title. If allow_lengths, every value is >= 30 s and they
    add up to within 10 % of the duration, they're track lengths (a
    start-time list begins at 0:00, so never looks like that). Otherwise
    the longest run of increasing times are start times - a stray "see you
    at 2:30" line elsewhere doesn't spoil the list. Gives [] for fewer than
    MIN_TIMESTAMP_LINES lines or a start past the end.
    """
    entries: List[Tuple[int, Optional[str]]] = []
    for line in (text or "").splitlines():
        m = _TIMESTAMP.search(line)
        if not m:
            continue
        hours, minutes, seconds = m.groups()
        secs = int(hours or 0) * 3600 + int(minutes) * 60 + int(seconds)
        title = " ".join(f"{line[:m.start()]} {line[m.end():]}".split())
        title = _EDGE_SEPARATORS.sub("", title)
        entries.append((secs, title or None))
    if len(entries) < MIN_TIMESTAMP_LINES:
        return []

    values = [secs for secs, _ in entries]
    if (
        allow_lengths
        and duration
        and all(v >= 30 for v in values)
        and abs(sum(values) - duration) <= 0.1 * duration
    ):
        starts = [sum(values[:i]) for i in range(len(values))]
        return [Hint(float(start), title) for start, (_, title) in zip(starts, entries)]

    run = _longest_increasing_run(entries)
    if len(run) < MIN_TIMESTAMP_LINES or (duration and run[-1][0] >= duration):
        return []
    return [Hint(float(secs), title) for secs, title in run]


def best_comment_hints(comments: Optional[Iterable[Dict[str, Any]]], duration: float) -> List[Hint]:
    """Hints from the first comment that parses as a tracklist: pinned
    first, then the uploader's own, then by like count. A comment's list
    must be start times beginning within COMMENT_LIST_START_S of 0:00 -
    commenters post lists of favourite moments far more often than track
    lengths."""
    ranked = sorted(
        comments or [],
        key=lambda c: (
            not c.get("is_pinned"),
            not c.get("author_is_uploader"),
            -(c.get("like_count") or 0),
        ),
    )
    for comment in ranked:
        hints = parse_timestamp_lines(comment.get("text"), duration, allow_lengths=False)
        if hints and hints[0].time <= COMMENT_LIST_START_S:
            return hints
    return []


def open_spans(hints: Sequence[Hint], duration: float) -> List[Tuple[float, float]]:
    """(start, end) stretches longer than LONG_SPAN_S that no hint covers:
    before the first hint, or from one hint to the next (or the end). With
    no hints, the whole video."""
    if not hints:
        return [(0.0, duration)]
    spans = []
    if hints[0].time > LONG_SPAN_S:
        spans.append((0.0, hints[0].time))
    bounds = [h.time for h in hints] + [duration]
    for start, end in zip(bounds, bounds[1:]):
        if end - start > LONG_SPAN_S:
            spans.append((start, end))
    return spans


def in_spans(time: float, spans: Sequence[Tuple[float, float]]) -> bool:
    return any(start <= time < end for start, end in spans)


def gather_hints(
    chapters: Optional[Iterable[Dict[str, Any]]],
    description: Optional[str],
    duration: float,
    fetch_comments: Callable[[], List[Dict[str, Any]]],
) -> List[Hint]:
    """
    Chapters first; then, while open spans remain, description timestamps
    and then comment timestamps that fall inside them. fetch_comments is
    only called if open spans remain after the description.
    """
    hints = chapter_hints(chapters)
    sources = (
        lambda: parse_timestamp_lines(description, duration),
        lambda: best_comment_hints(fetch_comments(), duration),
    )
    for source in sources:
        spans = open_spans(hints, duration)
        if not spans:
            break
        extra = [
            h for h in source()
            if in_spans(h.time, spans)
            and all(abs(h.time - existing.time) > _SAME_HINT_S for existing in hints)
        ]
        hints = sorted(hints + extra, key=lambda h: h.time)
    return hints
