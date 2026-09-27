"""
Tests for ytdl_helper.track_plan: choosing between hint cuts and gap cuts,
naming, tagging, and the detect_tracks orchestration.

The replay tests use the real chapter list and calibrated gap cuts of the
two example mixes from the design (docs/superpowers/specs/
2026-09-27-track-boundary-detection-design.md), with duplicate removal
faked as "drop everything after the first loop pass".
"""

import pathlib
import shutil
import sys
import threading

import pytest
import yt_dlp.utils

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from ytdl_helper.audio_analysis import Piece, has_chromaprint
from ytdl_helper.track_hints import Hint, chapter_hints, open_spans
from ytdl_helper.track_plan import (
    assign_names,
    build_pieces,
    combine_cuts,
    detect_tracks,
    plan_tracks,
    to_tracks,
    trust_gaps,
)

FFMPEG = shutil.which("ffmpeg")
needs_ffmpeg = pytest.mark.skipif(not FFMPEG, reason="ffmpeg not on PATH")
needs_chromaprint = pytest.mark.skipif(
    not (FFMPEG and has_chromaprint(FFMPEG)), reason="ffmpeg without the chromaprint muxer"
)

# Jf0DbTb3yog: 10 song chapters with drifting times, then a 3 h
# "Romantic Loop Starts" chapter; the same 10 songs repeat from 2300.9 s.
JF0_DURATION = 14439.8
JF0_CHAPTERS = [
    {"start_time": 0, "title": "Intro: Take Me Back Tonight"},
    {"start_time": 255, "title": "Photos in the Rain"},
    {"start_time": 480, "title": "Stay in My Arms"},
    {"start_time": 705, "title": "Back to Me"},
    {"start_time": 930, "title": "Take Me Back to the Coast"},
    {"start_time": 1120, "title": "Hold On / Flicker on the Glass"},
    {"start_time": 1335, "title": "Crossed That Line"},
    {"start_time": 1700, "title": "The Silver Key"},
    {"start_time": 2085, "title": "Feels Like Home"},
    {"start_time": 2320, "title": "Still Playing on My Radio"},
    {"start_time": 2560, "title": "Romantic Loop Starts"},
]
JF0_GAP_CUTS = [
    240.3, 492.9, 656.4, 906.1, 1124.8, 1340.3, 1569.9, 1820.6, 2024.9, 2300.9,
    2541.4, 2794.1, 2957.5, 3207.3, 3425.9, 3871.0, 4121.7, 4325.4, 4579.9, 4819.8,
]

# O3eXTfxJeqE: no chapters, no timestamps; 10 songs looped from 2229.2 s
# (4133.6 is a false cut inside a later pass's song).
O3_DURATION = 11148.4
O3_GAP_CUTS = [
    199.2, 417.5, 675.7, 876.4, 1127.5, 1351.3, 1575.0, 1819.9, 2020.2, 2229.2,
    2428.9, 2647.2, 2905.4, 3106.1, 3357.1, 3580.9, 3804.7, 4049.6, 4133.6, 4249.9,
]


def _first_pass_only(end):
    return lambda pieces: [p for p in pieces if p.start < end]


# --- trust_gaps ---


def test_trust_gaps_when_hints_miss_the_real_gaps():
    hints = chapter_hints(JF0_CHAPTERS)
    assert trust_gaps(hints, JF0_GAP_CUTS, open_spans(hints, JF0_DURATION)) is True


def test_trust_hints_when_hints_sit_on_gaps():
    hints = [Hint(0.0, "A"), Hint(200.0, "B"), Hint(400.0, "C"), Hint(600.0, "D")]
    assert trust_gaps(hints, [201.0, 399.0, 603.0, 650.0], []) is False


def test_trust_hints_when_there_are_few_gaps():
    hints = [Hint(float(t), str(t)) for t in range(0, 3000, 300)]  # a crossfaded mix
    assert trust_gaps(hints, [1234.0], []) is False


def test_trust_hints_without_hints():
    assert trust_gaps([], [100.0, 200.0], [(0.0, 300.0)]) is False


def test_plan_tracks_keeps_few_chapters_despite_a_single_pause():
    # Accurate chapters, crossfaded (gapless) boundaries, and one quiet stop
    # inside song B: a single gap is no evidence that the chapters are wrong.
    hints = [Hint(0.0, "A"), Hint(300.0, "B"), Hint(600.0, "C")]
    tracks = plan_tracks(hints, [450.0], 900.0, "Artist - Album", None, None)
    assert [(t.start, t.title) for t in tracks] == [(0.0, "A"), (300.0, "B"), (600.0, "C")]


# --- combine_cuts ---


def test_combine_cuts_keeps_fixed_cuts_and_spaced_out_gaps():
    assert combine_cuts([300.0], [20.0, 100.0, 110.0, 320.0, 500.0, 590.0], 600.0) == [
        100.0, 300.0, 500.0,
    ]


# --- build_pieces ---


def test_build_pieces_snaps_accurate_hints_onto_gaps():
    hints = [Hint(0.0, "A - One"), Hint(200.0, "A - Two"), Hint(400.0, "A - Three")]

    pieces, gaps_mode = build_pieces(hints, [202.0, 398.5], [], 600.0)

    assert gaps_mode is False
    assert pieces == [
        Piece(0.0, 202.0, "A - One", protected=True),
        Piece(202.0, 398.5, "A - Two", protected=True),
        Piece(398.5, 600.0, "A - Three", protected=True),
    ]


def test_build_pieces_ignores_pauses_inside_chaptered_songs():
    hints = [Hint(0.0, "One"), Hint(200.0, "Two"), Hint(400.0, "Three")]

    # 290.0 is a pause inside "Two", not a track change.
    pieces, gaps_mode = build_pieces(hints, [200.5, 290.0, 400.2], [], 600.0)

    assert gaps_mode is False
    assert [(p.start, p.title) for p in pieces] == [(0.0, "One"), (200.5, "Two"), (400.2, "Three")]


def test_build_pieces_cuts_an_open_span_at_its_gaps():
    hints = [Hint(0.0, "One"), Hint(300.0, "Two"), Hint(600.0, "Loop starts")]
    spans = open_spans(hints, 1800.0)  # 600..1800 has no hints

    pieces, gaps_mode = build_pieces(hints, [301.0, 850.0, 1100.0, 1350.0], spans, 1800.0)

    assert gaps_mode is False
    assert pieces == [
        Piece(0.0, 301.0, "One", protected=True),
        Piece(301.0, 600.0, "Two", protected=True),
        Piece(600.0, 850.0, "Loop starts", protected=False),  # a span label
        Piece(850.0, 1100.0),
        Piece(1100.0, 1350.0),
        Piece(1350.0, 1800.0),
    ]


def test_build_pieces_in_gaps_mode_cuts_at_gaps_only():
    hints = chapter_hints(JF0_CHAPTERS)

    pieces, gaps_mode = build_pieces(hints, JF0_GAP_CUTS, open_spans(hints, JF0_DURATION), JF0_DURATION)

    assert gaps_mode is True
    assert [p.start for p in pieces] == [0.0] + JF0_GAP_CUTS
    assert all(p.title is None and not p.protected for p in pieces)


# --- assign_names ---


def test_assign_names_snaps_to_nearby_starts_when_counts_differ():
    pieces = [Piece(0.0, 240.0), Piece(240.0, 500.0), Piece(500.0, 800.0), Piece(800.0, 1000.0)]
    hints = [Hint(0.0, "A"), Hint(250.0, "B"), Hint(700.0, "C")]

    assign_names(pieces, hints, [])

    # C is 200 s from 500 and 100 s from 800 - too far to snap - so it goes
    # to the piece its time falls in.
    assert [p.title for p in pieces] == ["A", "B", "C", None]


def test_assign_names_gives_a_span_label_to_the_first_piece_in_its_span():
    pieces = [Piece(0.0, 300.0), Piece(300.0, 900.0), Piece(900.0, 1500.0)]
    hints = [Hint(0.0, "One"), Hint(300.0, "Mix tail")]

    assign_names(pieces, hints, open_spans(hints, 1500.0))

    assert [p.title for p in pieces] == ["One", "Mix tail", None]


# --- to_tracks ---


def test_to_tracks_numbers_tags_and_names_unnamed_pieces():
    pieces = [Piece(0.0, 200.0, "Kai Mori - Neon"), Piece(200.0, 400.0)]

    tracks = to_tracks(pieces, video_title="Summer Mix", channel="DJ Sun", year=2024)

    assert [(t.number, t.total, t.start, t.end) for t in tracks] == [(1, 2, 0.0, 200.0), (2, 2, 200.0, 400.0)]
    assert [(t.artist, t.title) for t in tracks] == [("Kai Mori", "Neon"), ("DJ Sun", "Track 02")]
    assert all((t.album, t.album_artist, t.year) == ("Summer Mix", "DJ Sun", 2024) for t in tracks)


def test_to_tracks_swaps_title_artist_names_for_the_whole_album():
    pieces = [
        Piece(0.0, 200.0, "Song One - Luna Vale"),
        Piece(200.0, 400.0, "Song Two - Luna Vale"),
        Piece(400.0, 600.0),
    ]

    tracks = to_tracks(pieces, video_title="Summer Mix", channel="DJ Sun", year=None)

    assert [(t.artist, t.title) for t in tracks] == [
        ("Luna Vale", "Song One"), ("Luna Vale", "Song Two"), ("DJ Sun", "Track 03"),
    ]


# --- plan_tracks (replays of the example mixes) ---


def test_plan_tracks_names_a_drifting_chaptered_mix_in_order():
    tracks = plan_tracks(
        chapter_hints(JF0_CHAPTERS), JF0_GAP_CUTS, JF0_DURATION,
        video_title="Back to 1987 Mix", channel="Night Drive", year=None,
        dedupe=_first_pass_only(2300.0),
    )

    assert [t.title for t in tracks] == [c["title"] for c in JF0_CHAPTERS[:10]]
    assert [t.start for t in tracks] == [0.0] + JF0_GAP_CUTS[:9]
    assert tracks[-1].end == 2300.9
    assert all(t.artist == "Night Drive" for t in tracks)


def test_plan_tracks_numbers_an_unnamed_looped_mix():
    tracks = plan_tracks(
        [], O3_GAP_CUTS, O3_DURATION,
        video_title="LOST IN 1983", channel="Retro Channel", year=None,
        dedupe=_first_pass_only(2229.0),
    )

    assert [t.title for t in tracks] == [f"Track {n:02d}" for n in range(1, 11)]
    assert tracks[-1].end == 2229.2


def test_plan_tracks_keeps_a_long_final_song_whole():
    hints = [Hint(0.0, "Opener"), Hint(300.0, "Twenty Minute Epic")]

    tracks = plan_tracks(hints, [300.2], 1500.0, "Prog Album", "Band", None)

    assert [(t.title, t.start, t.end) for t in tracks] == [
        ("Opener", 0.0, 300.2), ("Twenty Minute Epic", 300.2, 1500.0),
    ]


def test_plan_tracks_single_piece_when_nothing_to_split_on():
    assert len(plan_tracks([], [], 900.0, "Song", "Artist", None)) == 1


def test_plan_tracks_starts_a_list_beginning_just_after_0_00_at_zero():
    # A description list starting at 0:05 must not make a 5 s "Track 01".
    hints = [Hint(5.0, "One"), Hint(180.0, "Two"), Hint(360.0, "Three")]
    tracks = plan_tracks(hints, [], 540.0, "Artist - Album", None, None)
    assert [(t.start, t.title) for t in tracks] == [(0.0, "One"), (180.0, "Two"), (360.0, "Three")]


# --- detect_tracks (real ffmpeg) ---


@needs_chromaprint
def test_detect_tracks_finds_the_unique_songs_of_a_looped_file(tmp_path, make_audio):
    src = make_audio(tmp_path / "abab.mp3", "A_B_A_B")
    messages = []

    tracks = detect_tracks(
        src, FFMPEG, chapters=None, description=None, expected_duration=141.5,
        video_title="Loop Mix", channel="DJ", year=None,
        fetch_comments=lambda: [], status_cb=messages.append,
    )

    assert [(t.title, round(t.start), round(t.end)) for t in tracks] == [
        ("Track 01", 0, 35), ("Track 02", 35, 71),
    ]
    assert messages[0] == "Finding track boundaries"
    assert "Fetching comments for track names" in messages


@needs_ffmpeg
def test_detect_tracks_falls_back_to_chapters_when_analysis_fails(tmp_path):
    src = tmp_path / "garbage.mp3"
    src.write_bytes(b"garbage")
    chapters = [{"start_time": 0, "title": "A"}, {"start_time": 60, "title": "B"}]

    tracks = detect_tracks(
        src, FFMPEG, chapters=chapters, description=None, expected_duration=120,
        video_title="Mix", channel="DJ", year=None, fetch_comments=lambda: [],
    )
    assert [(t.title, t.start, t.end) for t in tracks] == [("A", 0.0, 60.0), ("B", 60.0, 120.0)]

    no_duration = detect_tracks(
        src, FFMPEG, chapters=chapters, description=None, expected_duration=None,
        video_title="Mix", channel="DJ", year=None, fetch_comments=lambda: [],
    )
    assert no_duration == []


@needs_ffmpeg
def test_detect_tracks_stops_when_cancelled(tmp_path, make_audio):
    src = make_audio(tmp_path / "a.wav", "A")
    cancel_event = threading.Event()
    cancel_event.set()

    with pytest.raises(yt_dlp.utils.DownloadCancelled):
        detect_tracks(
            src, FFMPEG, chapters=None, description=None, expected_duration=35,
            video_title="Mix", channel="DJ", year=None, fetch_comments=lambda: [],
            cancel_event=cancel_event,
        )
