"""
What track splitting needs from the audio itself, from one ffmpeg pass over
a downloaded file: its true (decoded) duration, its silent stretches, and a
Chromaprint fingerprint. Plus the pure logic that turns silences into gap
cuts and finds repeated songs (looped mixes) by fingerprint.

Calibrated on two real looped playlist mixes; see
docs/superpowers/specs/2026-09-27-track-boundary-detection-design.md.
"""

import copy
import functools
import logging
import pathlib
import re
import subprocess
import tempfile
import threading
from dataclasses import dataclass
from typing import Callable, List, Optional, Sequence, Tuple

import yt_dlp.utils

logger = logging.getLogger(__name__)

# Real gaps between songs are digital silence; quiet passages inside songs
# reach -50..-74 dB but not -80 dB. After lossy encoding (YouTube's Opus
# or AAC) the shortest real gaps stay below -80 dB for only ~0.03 s.
# Calibrated on both example mixes in both formats: clean for d 0.02-0.03 s.
SILENCE_NOISE_DB = -80
SILENCE_MIN_S = 0.025
# Silences this close together are one gap (double silences ~1 s apart).
GAP_CLUSTER_S = 3.0
# Silence-derived pieces shorter than this are merged away.
MIN_PIECE_S = 30.0

# Chromaprint's default algorithm emits one 32-bit item per 1365 samples at
# 11025 Hz.
FP_ITEM_S = 1365 / 11025
# Ignore each piece's first/last seconds when comparing (cut jitter).
FP_EDGE_TRIM_S = 2.0
# How far two copies of a song may be misaligned between loop passes.
FP_MATCH_SLACK_S = 10.0
# Bit error rate below which two stretches are the same recording: repeats
# measured <= 0.08, different songs >= 0.29.
FP_DUP_MAX_BER = 0.2
# Too little overlap to call a match (~12 s).
_FP_MIN_OVERLAP_ITEMS = 100

_SILENCE_START = re.compile(r"silence_start:\s*(-?[\d.]+)")
_SILENCE_END = re.compile(r"silence_end:\s*(-?[\d.]+)")
_OUT_TIME_US = re.compile(r"^out_time_us=(\d+)\s*$")


@dataclass
class AudioAnalysis:
    duration: float
    silences: List[Tuple[float, float]]
    # Raw little-endian uint32 Chromaprint items; None when unavailable.
    fingerprint: Optional[bytes]


@dataclass
class Piece:
    """A candidate track: a time range, its name if known, and whether it
    came from a hint the uploader wrote (never dropped as a repeat)."""
    start: float
    end: float
    title: Optional[str] = None
    protected: bool = False


@functools.lru_cache(maxsize=None)
def has_chromaprint(ffmpeg_path: str) -> bool:
    """Whether this ffmpeg build has the chromaprint muxer."""
    try:
        out = subprocess.run(
            [ffmpeg_path, "-hide_banner", "-muxers"],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
            timeout=30,
        ).stdout
    except (OSError, subprocess.TimeoutExpired):
        return False
    return re.search(r"^\s*E\s+chromaprint\b", out or "", re.MULTILINE) is not None


def parse_silences(stderr_text: str, duration: float) -> List[Tuple[float, float]]:
    """(start, end) pairs from silencedetect's log; a silence still open at
    the end of the file ends at `duration`."""
    events = sorted(
        [(m.start(), "start", float(m.group(1))) for m in _SILENCE_START.finditer(stderr_text)]
        + [(m.start(), "end", float(m.group(1))) for m in _SILENCE_END.finditer(stderr_text)]
    )
    silences, open_start = [], None
    for _, kind, value in events:
        if kind == "start":
            open_start = max(0.0, value)
        elif open_start is not None:
            silences.append((open_start, value))
            open_start = None
    if open_start is not None:
        silences.append((open_start, duration))
    return silences


def analyse_audio(
    src: pathlib.Path,
    ffmpeg_path: str,
    progress_cb: Optional[Callable[[float], None]] = None,
    cancel_event: Optional[threading.Event] = None,
) -> AudioAnalysis:
    """
    Decodes src once: silencedetect for gaps, the chromaprint muxer for the
    fingerprint (skipped with a warning if this ffmpeg lacks it), and
    -progress for the decoded duration. progress_cb gets seconds decoded so far.

    Raises:
        yt_dlp.utils.DownloadCancelled: If cancel_event is set.
        RuntimeError: If ffmpeg fails.
    """
    with tempfile.TemporaryDirectory(prefix="ytdl_analyse_") as tmp:
        tmp_dir = pathlib.Path(tmp)
        fp_path = tmp_dir / "fingerprint.raw"
        cmd = [
            ffmpeg_path, "-hide_banner", "-nostats", "-progress", "pipe:1",
            "-i", str(src), "-vn",
            "-af", f"silencedetect=noise={SILENCE_NOISE_DB}dB:d={SILENCE_MIN_S}",
        ]
        if has_chromaprint(ffmpeg_path):
            cmd += ["-f", "chromaprint", "-fp_format", "raw", "-y", str(fp_path)]
        else:
            logger.warning("ffmpeg has no chromaprint muxer; repeated songs won't be detected.")
            cmd += ["-f", "null", "-"]

        # stderr (the silence log) goes to a file: it can outgrow a pipe
        # buffer on a long mix while we're busy reading progress on stdout.
        with open(tmp_dir / "stderr.log", "w+", encoding="utf-8", errors="replace") as err:
            proc = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=err,
                text=True, encoding="utf-8", errors="replace",
            )
            decoded_s = 0.0
            try:
                for line in proc.stdout:
                    if cancel_event is not None and cancel_event.is_set():
                        raise yt_dlp.utils.DownloadCancelled("Download cancelled by user request.")
                    m = _OUT_TIME_US.match(line)
                    if m:
                        decoded_s = int(m.group(1)) / 1_000_000
                        if progress_cb:
                            progress_cb(decoded_s)
                proc.wait()
            finally:
                if proc.poll() is None:
                    proc.kill()
                    proc.wait()
            err.seek(0)
            stderr_text = err.read()

        if proc.returncode != 0:
            lines = stderr_text.strip().splitlines()
            raise RuntimeError(
                f"ffmpeg audio analysis failed: {lines[-1] if lines else f'exit code {proc.returncode}'}"
            )
        silences = parse_silences(stderr_text, decoded_s)
        duration = max([decoded_s] + [end for _, end in silences])
        fingerprint = fp_path.read_bytes() if fp_path.exists() else None
        return AudioAnalysis(duration=duration, silences=silences, fingerprint=fingerprint or None)


def gap_cuts(silences: Sequence[Tuple[float, float]], duration: float) -> List[float]:
    """
    One cut time per gap between songs: silences within 1 s of either end
    are ignored, and silences within GAP_CLUSTER_S of each other count as
    one gap, cut at the middle of its longest silence. (The MIN_PIECE_S
    spacing is applied later, together with any hint cuts.)
    """
    clusters: List[List[Tuple[float, float]]] = []  # (midpoint, length)
    for start, end in sorted(silences):
        if start < 1.0 or end > duration - 1.0:
            continue
        mid, length = (start + end) / 2, end - start
        if clusters and mid - clusters[-1][-1][0] <= GAP_CLUSTER_S:
            clusters[-1].append((mid, length))
        else:
            clusters.append([(mid, length)])
    return [max(c, key=lambda x: x[1])[0] for c in clusters]


def _best_match(np, short, long, slack: int) -> Tuple[float, int]:
    """Lowest bit error rate of `short` laid over `long` at any item offset
    from -slack to len(long) - len(short) + slack, and that offset."""
    best = (1.0, 0)
    min_overlap = max(_FP_MIN_OVERLAP_ITEMS, len(short) // 2)
    for off in range(-slack, max(0, len(long) - len(short)) + slack + 1):
        a = short[max(0, -off):]
        b = long[max(0, off):]
        n = min(len(a), len(b))
        if n < min_overlap:
            continue
        ber = float(np.bitwise_count(a[:n] ^ b[:n]).sum()) / (32 * n)
        if ber < best[0]:
            best = (ber, off)
    return best


def dedupe_pieces(pieces: Sequence[Piece], fingerprint: Optional[bytes]) -> List[Piece]:
    """
    Drops pieces that repeat an earlier kept piece (looped mixes), comparing
    fingerprints. A piece no longer than a kept one is dropped if it matches
    anywhere inside it (repeats, shorter edits). A longer piece that
    contains a kept one (a gap missed in a later pass) has that part carved
    out, and what's left before/after (if >= MIN_PIECE_S) is checked again.
    A dropped part's name passes to its original if that has none.
    Protected pieces are always kept. Returns kept pieces in time order;
    the input is not modified.
    """
    pieces = [copy.copy(p) for p in pieces]
    if not fingerprint:
        return pieces
    try:
        import numpy as np
    except ImportError:
        logger.warning("numpy is not installed; repeated songs won't be removed.")
        return pieces
    if not hasattr(np, "bitwise_count"):
        logger.warning("numpy is older than 2.0; repeated songs won't be removed.")
        return pieces

    fp = np.frombuffer(fingerprint, dtype="<u4")
    slack = int(FP_MATCH_SLACK_S / FP_ITEM_S)

    def items(p: Piece):
        first = int((p.start + FP_EDGE_TRIM_S) / FP_ITEM_S)
        last = int((p.end - FP_EDGE_TRIM_S) / FP_ITEM_S)
        return fp[first:max(first, last)]

    kept: List[Piece] = []
    queue = list(pieces)
    while queue:
        piece = queue.pop(0)
        if piece.protected:
            kept.append(piece)
            continue
        mine = items(piece)
        for original in kept:
            theirs = items(original)
            if len(mine) <= len(theirs) + slack:
                ber, _ = _best_match(np, mine, theirs, slack)
                if ber < FP_DUP_MAX_BER:
                    original.title = original.title or piece.title
                    break
            else:
                ber, off = _best_match(np, theirs, mine, slack)
                if ber < FP_DUP_MAX_BER:
                    original.title = original.title or piece.title
                    match_start = max(piece.start, piece.start + off * FP_ITEM_S)
                    match_end = match_start + (original.end - original.start)
                    queue[0:0] = [
                        Piece(start, end)
                        for start, end in ((piece.start, match_start), (match_end, piece.end))
                        if end - start >= MIN_PIECE_S
                    ]
                    break
        else:
            kept.append(piece)
    return sorted(kept, key=lambda p: p.start)
