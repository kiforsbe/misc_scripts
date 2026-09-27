"""
Decides where a long audio download is cut into tracks and what each track
is called, combining hints (chapters/description/comments) with what the
audio analysis found (silence gaps, repeated songs). See
docs/superpowers/specs/2026-09-27-track-boundary-detection-design.md.
"""

import logging
import pathlib
import threading
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

import yt_dlp.utils

from .audio_analysis import (
    MIN_PIECE_S, AudioAnalysis, Piece, analyse_audio, dedupe_pieces, gap_cuts,
)
from .chapter_split import ChapterTrack, derive_album_info, orient_artist_title, parse_chapter_title
from .track_hints import Hint, gather_hints, in_spans, open_spans

logger = logging.getLogger(__name__)

# A hint this close to a gap is at that gap (the hint is accurate).
HINT_GAP_MATCH_S = 5.0
# When names can't be matched to tracks by count, a name attaches to a
# track starting within this distance of its timestamp.
NAME_SNAP_S = 90.0
# Trusting gaps over hints needs at least this many gaps outside open
# spans: one quiet stop inside a song is no evidence the hints are wrong.
MIN_GAP_EVIDENCE = 3

Spans = Sequence[Tuple[float, float]]


def trust_gaps(hints: Sequence[Hint], gaps: Sequence[float], spans: Spans) -> bool:
    """True when the audio has real gaps that the hints mostly miss (hint
    times are wrong, so gaps decide the cuts and hints only give names)."""
    times = [h.time for h in hints if h.time > 0]
    if not times:
        return False
    gaps_outside = [g for g in gaps if not in_spans(g, spans)]
    matched = sum(1 for t in times if any(abs(g - t) <= HINT_GAP_MATCH_S for g in gaps))
    return len(gaps_outside) >= max(MIN_GAP_EVIDENCE, len(times) / 2) and matched < len(times) / 2


def combine_cuts(fixed: Iterable[float], gaps: Iterable[float], duration: float) -> List[float]:
    """All `fixed` cuts, plus each gap cut at least MIN_PIECE_S from the
    cuts around it (and from the start/end)."""
    fixed = sorted(set(fixed))
    kept: List[float] = []
    for gap in sorted(gaps):
        prev_cut = max([c for c in fixed if c <= gap] + kept + [0.0])
        next_cut = min([c for c in fixed if c > gap] + [duration])
        if gap - prev_cut >= MIN_PIECE_S and next_cut - gap >= MIN_PIECE_S:
            kept.append(gap)
    return sorted(fixed + kept)


def build_pieces(
    hints: Sequence[Hint], gaps: Sequence[float], spans: Spans, duration: float
) -> Tuple[List[Piece], bool]:
    """
    Cuts the timeline into pieces. Returns (pieces, gaps_mode). In gaps
    mode the pieces are unnamed (names come later, from assign_names). In
    hints mode every hint is a cut (moved onto a gap within
    HINT_GAP_MATCH_S), plus gap cuts inside open spans; a piece starting at
    a hint takes its title, and is protected unless that hint labels an
    open span.
    """
    if trust_gaps(hints, gaps, spans):
        cuts = combine_cuts([], gaps, duration)
        return [Piece(s, e) for s, e in zip([0.0] + cuts, cuts + [duration])], True

    span_starts = {start for start, _ in spans}
    at_cut: Dict[float, Hint] = {}
    # A list whose first time is just after 0:00 (e.g. 0:05) starts at 0:00.
    first_time = min((h.time for h in hints), default=0.0)
    for hint in hints:
        at_start = hint.time <= 0 or (hint.time == first_time and hint.time < MIN_PIECE_S)
        cut = 0.0 if at_start else hint.time
        near = [g for g in gaps if abs(g - hint.time) <= HINT_GAP_MATCH_S]
        if cut > 0 and near:
            cut = min(near, key=lambda g: abs(g - hint.time))
        at_cut.setdefault(cut, hint)
    cuts = combine_cuts(
        [c for c in at_cut if 0 < c < duration],
        [g for g in gaps if in_spans(g, spans)],
        duration,
    )
    pieces = []
    for start, end in zip([0.0] + cuts, cuts + [duration]):
        hint = at_cut.get(start)
        pieces.append(Piece(
            start, end,
            title=hint.title if hint else None,
            protected=hint is not None and hint.time not in span_starts,
        ))
    return pieces, False


def assign_names(pieces: List[Piece], hints: Sequence[Hint], spans: Spans) -> None:
    """
    Gaps mode naming, after duplicate removal (modifies pieces in place).
    Song names (hints that don't label an open span) go to the pieces that
    don't start inside an open span: in order if the counts match (robust to
    drifting timestamps), else each to the nearest unnamed piece start
    within NAME_SNAP_S (keeping order), else to the unnamed piece containing
    its time. A span label names the first piece starting inside its span.
    """
    span_starts = {start for start, _ in spans}
    songs = [h for h in hints if h.time not in span_starts]
    candidates = [p for p in pieces if not in_spans(p.start, spans)]

    if songs and len(songs) == len(candidates):
        for piece, hint in zip(candidates, songs):
            piece.title = hint.title
    else:
        leftovers, next_free = [], 0
        for hint in songs:
            options = [
                j for j in range(next_free, len(candidates))
                if candidates[j].title is None
                and abs(candidates[j].start - hint.time) <= NAME_SNAP_S
            ]
            if options:
                best = min(options, key=lambda j: abs(candidates[j].start - hint.time))
                candidates[best].title = hint.title
                next_free = best + 1
            else:
                leftovers.append(hint)
        for hint in leftovers:
            for piece in pieces:
                if piece.title is None and piece.start <= hint.time < piece.end:
                    piece.title = hint.title
                    break

    for hint in hints:
        if hint.time not in span_starts:
            continue
        span_end = next(end for start, end in spans if start == hint.time)
        first = next((p for p in pieces if hint.time <= p.start < span_end), None)
        if first is not None and first.title is None:
            first.title = hint.title


def to_tracks(
    pieces: Sequence[Piece], video_title: Optional[str], channel: Optional[str], year: Optional[int]
) -> List[ChapterTrack]:
    """Numbers pieces 1..N and tags them; unnamed ones become "Track NN"."""
    album_artist, album = derive_album_info(video_title, channel)
    width = max(2, len(str(len(pieces))))
    names = orient_artist_title([
        parse_chapter_title(p.title, None) if p.title else (None, f"Track {n:0{width}d}")
        for n, p in enumerate(pieces, 1)
    ])
    return [
        ChapterTrack(
            number=n, total=len(pieces), start=p.start, end=p.end,
            artist=artist or album_artist, title=title,
            album=album, album_artist=album_artist, year=year,
        )
        for n, (p, (artist, title)) in enumerate(zip(pieces, names), 1)
    ]


def plan_tracks(
    hints: Sequence[Hint],
    gaps: Sequence[float],
    duration: float,
    video_title: Optional[str],
    channel: Optional[str],
    year: Optional[int],
    dedupe: Callable[[List[Piece]], List[Piece]] = lambda pieces: pieces,
) -> List[ChapterTrack]:
    """Pure planning: cuts, duplicate removal (via `dedupe`), names, tags."""
    spans = open_spans(hints, duration)
    pieces, gaps_mode = build_pieces(hints, gaps, spans, duration)
    pieces = dedupe(pieces)
    if gaps_mode:
        assign_names(pieces, hints, spans)
    return to_tracks(pieces, video_title, channel, year)


def detect_tracks(
    src: pathlib.Path,
    ffmpeg_path: str,
    *,
    chapters: Optional[Iterable[Dict[str, Any]]],
    description: Optional[str],
    expected_duration: Optional[float],
    video_title: Optional[str],
    channel: Optional[str],
    year: Optional[int],
    fetch_comments: Callable[[], List[Dict[str, Any]]],
    status_cb: Optional[Callable[[str], None]] = None,
    cancel_event: Optional[threading.Event] = None,
) -> List[ChapterTrack]:
    """
    Analyses the downloaded file and plans its tracks. If the analysis
    itself fails, falls back to hints only (chapters/description/comments)
    against expected_duration. Returns [] when there's nothing to split on.

    Raises:
        yt_dlp.utils.DownloadCancelled: If cancel_event is set.
    """
    def _status(message: str) -> None:
        if status_cb:
            status_cb(message)

    def _progress(decoded_s: float) -> None:
        if expected_duration:
            _status(f"Finding track boundaries ({min(99, int(decoded_s * 100 / expected_duration))}%)")

    def _comments() -> List[Dict[str, Any]]:
        _status("Fetching comments for track names")
        return fetch_comments()

    _status("Finding track boundaries")
    try:
        analysis = analyse_audio(src, ffmpeg_path, progress_cb=_progress, cancel_event=cancel_event)
    except yt_dlp.utils.DownloadCancelled:
        raise
    except (OSError, RuntimeError) as e:
        if not expected_duration:
            logger.warning(f"Audio analysis failed ({e}); not splitting.")
            return []
        logger.warning(f"Audio analysis failed ({e}); splitting on chapters/timestamps only.")
        analysis = AudioAnalysis(duration=float(expected_duration), silences=[], fingerprint=None)

    hints = gather_hints(chapters, description, analysis.duration, _comments)
    return plan_tracks(
        hints,
        gap_cuts(analysis.silences, analysis.duration),
        analysis.duration,
        video_title,
        channel,
        year,
        dedupe=lambda pieces: dedupe_pieces(pieces, analysis.fingerprint),
    )
