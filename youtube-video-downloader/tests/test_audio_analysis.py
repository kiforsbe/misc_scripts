"""
Tests for ytdl_helper.audio_analysis: parsing ffmpeg's silence log, turning
silences into gap cuts, the one-pass ffmpeg analysis (real ffmpeg on
generated audio), and fingerprint-based duplicate removal (synthetic
fingerprints, plus one real looped file).
"""

import pathlib
import shutil
import subprocess
import sys
import threading

import numpy as np
import pytest
import yt_dlp.utils

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from ytdl_helper import audio_analysis
from ytdl_helper.audio_analysis import (
    FP_ITEM_S,
    Piece,
    analyse_audio,
    dedupe_pieces,
    gap_cuts,
    has_chromaprint,
    parse_silences,
)

FFMPEG = shutil.which("ffmpeg")
needs_ffmpeg = pytest.mark.skipif(not FFMPEG, reason="ffmpeg not on PATH")
needs_chromaprint = pytest.mark.skipif(
    not (FFMPEG and has_chromaprint(FFMPEG)), reason="ffmpeg without the chromaprint muxer"
)


# --- parse_silences ---


def test_parse_silences_pairs_starts_and_ends():
    log = "\n".join([
        "[silencedetect @ 0x1] silence_start: 0",
        "[silencedetect @ 0x1] silence_end: 0.104 | silence_duration: 0.104",
        "size=N/A time=00:03:18.70 bitrate=N/A speed= 900x",
        "[silencedetect @ 0x1] silence_start: 198.7",
        "[silencedetect @ 0x1] silence_end: 199.77 | silence_duration: 1.07",
        "[silencedetect @ 0x1] silence_start: 299.5",
    ])
    assert parse_silences(log, duration=300.0) == [
        (0.0, 0.104), (198.7, 199.77), (299.5, 300.0),
    ]


def test_parse_silences_empty_log():
    assert parse_silences("", duration=300.0) == []


# --- gap_cuts ---


def test_gap_cuts_ignores_edges_and_merges_close_silences():
    silences = [(0.0, 0.1), (198.7, 199.77), (199.85, 200.0), (400.0, 400.4), (599.5, 600.0)]
    assert gap_cuts(silences, duration=600.0) == [
        pytest.approx(199.235),  # middle of the longer of the two close silences
        pytest.approx(400.2),
    ]


def test_gap_cuts_without_silences():
    assert gap_cuts([], duration=600.0) == []


# --- has_chromaprint ---


def test_has_chromaprint_is_false_for_a_missing_ffmpeg():
    assert has_chromaprint("C:/definitely/not/ffmpeg.exe") is False


# --- analyse_audio (real ffmpeg) ---


@needs_chromaprint
def test_analyse_audio_finds_duration_gaps_and_fingerprint(tmp_path, make_audio):
    src = make_audio(tmp_path / "abab.mp3", "A_B_A_B")
    progress = []

    analysis = analyse_audio(src, FFMPEG, progress_cb=progress.append)

    assert analysis.duration == pytest.approx(141.5, abs=0.2)
    assert gap_cuts(analysis.silences, analysis.duration) == [
        pytest.approx(35.25, abs=0.1),
        pytest.approx(70.75, abs=0.1),
        pytest.approx(106.25, abs=0.1),
    ]
    assert len(analysis.fingerprint) // 4 == pytest.approx(141.5 / FP_ITEM_S, rel=0.05)
    assert progress and progress[-1] == pytest.approx(analysis.duration, abs=0.2)


@needs_ffmpeg
def test_analyse_audio_without_chromaprint_still_finds_gaps(tmp_path, make_audio, monkeypatch):
    monkeypatch.setattr(audio_analysis, "has_chromaprint", lambda ffmpeg_path: False)
    src = make_audio(tmp_path / "ab.wav", "A_B")

    analysis = analyse_audio(src, FFMPEG)

    assert analysis.fingerprint is None
    assert gap_cuts(analysis.silences, analysis.duration) == [pytest.approx(35.25, abs=0.1)]


@needs_ffmpeg
def test_analyse_audio_stops_when_cancelled(tmp_path, make_audio):
    src = make_audio(tmp_path / "a.wav", "A")
    cancel_event = threading.Event()
    cancel_event.set()

    with pytest.raises(yt_dlp.utils.DownloadCancelled):
        analyse_audio(src, FFMPEG, cancel_event=cancel_event)


@needs_ffmpeg
def test_analyse_audio_raises_when_ffmpeg_fails(tmp_path):
    src = tmp_path / "garbage.mp3"
    src.write_bytes(b"garbage")

    with pytest.raises(RuntimeError, match="audio analysis failed"):
        analyse_audio(src, FFMPEG)


@needs_ffmpeg
def test_analyse_audio_cuts_at_digital_silence_not_at_quiet_passages(tmp_path):
    # Song A with a 0.3 s quiet passage (about -74 dB) in the middle, then a
    # 0.04 s digital-silence gap - after lossy encoding, real gaps between
    # songs can be that short - then song B. Only the gap is a cut.
    def noise(seconds, seed, amplitude):
        return f"anoisesrc=d={seconds}:c=pink:seed={seed}:a={amplitude}:r=22050"

    parts = [
        noise(35, 1, 0.5), noise(0.3, 7, 0.0002), noise(35, 1, 0.5),
        "anullsrc=r=22050:cl=mono,atrim=duration=0.04", noise(35, 2, 0.5),
    ]
    graph = ";".join(f"{p},aformat=sample_fmts=s16:channel_layouts=mono[s{i}]" for i, p in enumerate(parts))
    graph += ";" + "".join(f"[s{i}]" for i in range(len(parts))) + f"concat=n={len(parts)}:v=0:a=1[out]"
    src = tmp_path / "dip_and_gap.wav"
    subprocess.run([FFMPEG, "-v", "error", "-y", "-filter_complex", graph, "-map", "[out]", str(src)], check=True)

    analysis = analyse_audio(src, FFMPEG)

    assert [round(c, 1) for c in gap_cuts(analysis.silences, analysis.duration)] == [70.3]


# --- dedupe_pieces (synthetic fingerprints) ---

_RNG = np.random.default_rng(7)


def _song(seconds: float) -> np.ndarray:
    """A made-up song: random fingerprint items (different songs match ~50 %
    of bits, i.e. not at all)."""
    return _RNG.integers(0, 2**32, size=int(seconds / FP_ITEM_S), dtype=np.uint32)


def _reencoded(items: np.ndarray, flip_fraction: float = 0.05) -> np.ndarray:
    """The same song again, as a re-encoded copy: ~5 % of bits flipped."""
    flips = _RNG.random((len(items), 32)) < flip_fraction
    mask = (flips * (1 << np.arange(32, dtype=np.uint64))).sum(axis=1).astype(np.uint32)
    return items ^ mask


def _timeline(*parts):
    """(fingerprint bytes, [(start_s, end_s) of each part])."""
    spans, pos = [], 0
    for part in parts:
        spans.append((pos * FP_ITEM_S, (pos + len(part)) * FP_ITEM_S))
        pos += len(part)
    return np.concatenate(parts).astype("<u4").tobytes(), spans


def test_dedupe_drops_repeats_of_earlier_pieces():
    a, b = _song(200), _song(180)
    fp, spans = _timeline(a, b, _reencoded(a), _reencoded(b))

    kept = dedupe_pieces([Piece(*s) for s in spans], fp)

    assert [(p.start, p.end) for p in kept] == spans[:2]


def test_dedupe_drops_a_shorter_edit_of_an_earlier_song():
    a, b = _song(240), _song(200)
    fp, spans = _timeline(a, b, _reencoded(a[: int(220 / FP_ITEM_S)]))  # 20 s shorter

    kept = dedupe_pieces([Piece(*s) for s in spans], fp)

    assert [(p.start, p.end) for p in kept] == spans[:2]


def test_dedupe_carves_known_songs_out_of_a_longer_piece():
    a, b = _song(200), _song(180)
    fp, spans = _timeline(a, b, _reencoded(np.concatenate([a, b])))  # missed gap

    kept = dedupe_pieces([Piece(*s) for s in spans], fp)

    assert [(p.start, p.end) for p in kept] == spans[:2]


def test_dedupe_keeps_new_material_left_after_carving():
    a, b, c = _song(200), _song(180), _song(150)
    fp, spans = _timeline(a, b, np.concatenate([_reencoded(a), c]))

    kept = dedupe_pieces([Piece(*s) for s in spans], fp)

    assert len(kept) == 3
    assert kept[2].start == pytest.approx(spans[2][0] + 200, abs=0.5)
    assert kept[2].end == spans[2][1]


def test_dedupe_never_drops_protected_pieces():
    a = _song(200)
    fp, spans = _timeline(a, _reencoded(a))

    kept = dedupe_pieces([Piece(*s, protected=True) for s in spans], fp)

    assert len(kept) == 2


def test_dedupe_passes_a_dropped_repeats_name_to_an_unnamed_original():
    a, b = _song(200), _song(180)
    fp, spans = _timeline(a, b, _reencoded(a), _reencoded(b))
    pieces = [
        Piece(*spans[0]),
        Piece(*spans[1], title="Bee"),
        Piece(*spans[2], title="Ay"),
        Piece(*spans[3], title="Other"),
    ]

    kept = dedupe_pieces(pieces, fp)

    assert [p.title for p in kept] == ["Ay", "Bee"]
    assert pieces[0].title is None  # the input isn't modified


def test_dedupe_without_fingerprint_keeps_everything():
    pieces = [Piece(0.0, 100.0), Piece(100.0, 200.0)]
    assert dedupe_pieces(pieces, None) == pieces


def test_dedupe_without_numpy_keeps_everything(monkeypatch):
    a = _song(200)
    fp, spans = _timeline(a, _reencoded(a))
    monkeypatch.setitem(sys.modules, "numpy", None)  # makes "import numpy" fail

    assert len(dedupe_pieces([Piece(*s) for s in spans], fp)) == 2


def test_dedupe_with_numpy_older_than_2_keeps_everything(monkeypatch):
    a = _song(200)
    fp, spans = _timeline(a, _reencoded(a))
    monkeypatch.delattr(np, "bitwise_count")  # numpy < 2.0 doesn't have it

    assert len(dedupe_pieces([Piece(*s) for s in spans], fp)) == 2


# --- dedupe_pieces (real looped file) ---


@needs_chromaprint
def test_dedupe_on_a_real_looped_file(tmp_path, make_audio):
    src = make_audio(tmp_path / "abab.mp3", "A_B_A_B")
    analysis = analyse_audio(src, FFMPEG)
    cuts = gap_cuts(analysis.silences, analysis.duration)
    pieces = [Piece(s, e) for s, e in zip([0.0] + cuts, cuts + [analysis.duration])]

    kept = dedupe_pieces(pieces, analysis.fingerprint)

    assert [(round(p.start), round(p.end)) for p in kept] == [(0, 35), (35, 71)]
