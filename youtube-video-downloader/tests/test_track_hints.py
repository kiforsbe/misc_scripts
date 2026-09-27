"""
Tests for ytdl_helper.track_hints: chapter hints, the lenient description/
comment tracklist parse, open spans, and the chapters -> description ->
comments chain. All pure - no ffmpeg, no network.
"""

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from ytdl_helper.track_hints import (
    Hint,
    best_comment_hints,
    chapter_hints,
    gather_hints,
    in_spans,
    open_spans,
    parse_timestamp_lines,
)


# --- chapter_hints ---


def test_chapter_hints_keep_titles_and_drop_placeholders():
    chapters = [
        {"start_time": 0.0, "end_time": 200.0, "title": "Intro"},
        {"start_time": 200.0, "end_time": 400.0, "title": "<Untitled Chapter 2>"},
        {"start_time": 400.0, "end_time": 600.0, "title": "  "},
    ]
    assert chapter_hints(chapters) == [Hint(0.0, "Intro"), Hint(200.0, None), Hint(400.0, None)]


def test_chapter_hints_are_sorted_and_keep_the_first_of_a_shared_start():
    chapters = [
        {"start_time": 300, "title": "B"},
        {"start_time": 0, "title": "A"},
        {"start_time": 300, "title": "B again"},
    ]
    assert chapter_hints(chapters) == [Hint(0.0, "A"), Hint(300.0, "B")]


def test_chapter_hints_empty():
    assert chapter_hints(None) == []
    assert chapter_hints([]) == []


# --- parse_timestamp_lines ---


def test_parse_timestamp_lines_reads_formats_yt_dlp_misses():
    text = "\n".join([
        "Tracklist:",
        "[00:00] Intro: Take Me Back Tonight",
        "01. 4:15 Photos in the Rain",
        "Stay in My Arms (8:00)",
        "11:45 - Luna Vale - Back to Me",
        "1:02:03 Finale",
    ])
    assert parse_timestamp_lines(text, duration=4000) == [
        Hint(0.0, "Intro: Take Me Back Tonight"),
        Hint(255.0, "01. Photos in the Rain"),  # parse_chapter_title drops "01." later
        Hint(480.0, "Stay in My Arms"),
        Hint(705.0, "Luna Vale - Back to Me"),
        Hint(3723.0, "Finale"),
    ]


def test_parse_timestamp_lines_ignores_a_stray_time_after_the_list():
    text = "0:00 One\n3:00 Two\n6:00 Three\nLive again at 2:30 on Friday"
    assert parse_timestamp_lines(text, duration=900) == [
        Hint(0.0, "One"), Hint(180.0, "Two"), Hint(360.0, "Three"),
    ]


def test_parse_timestamp_lines_takes_the_longest_list_when_it_restarts():
    text = "Side A\n0:00 A1\n3:00 A2\n6:00 A3\nSide B\n0:00 B1\n4:00 B2"
    assert [h.title for h in parse_timestamp_lines(text, duration=900)] == ["A1", "A2", "A3"]


def test_parse_timestamp_lines_accumulates_track_lengths():
    text = "1. Song A (3:00)\n2. Song B (4:00)\n3. Song C (2:30)\n4. Song D (3:30)"
    assert parse_timestamp_lines(text, duration=790) == [
        Hint(0.0, "1. Song A"), Hint(180.0, "2. Song B"),
        Hint(420.0, "3. Song C"), Hint(570.0, "4. Song D"),
    ]


def test_parse_timestamp_lines_prefers_lengths_even_when_they_increase():
    text = "Song A 3:00\nSong B 3:30\nSong C 4:00"
    assert [h.time for h in parse_timestamp_lines(text, duration=640)] == [0.0, 180.0, 390.0]


def test_parse_timestamp_lines_rejects_short_or_out_of_range_lists():
    assert parse_timestamp_lines("0:00 One\n3:00 Two", duration=900) == []
    assert parse_timestamp_lines("0:00 A\n5:00 B\n20:00 C", duration=600) == []
    assert parse_timestamp_lines(None, duration=600) == []
    assert parse_timestamp_lines("", duration=600) == []


def test_parse_timestamp_lines_skips_numbers_that_are_not_timestamps():
    text = "0:00 A\n1:00 B\n2:00 C\nRoom 123:45\nCall 1:2:3:4"
    assert [h.time for h in parse_timestamp_lines(text, duration=600)] == [0.0, 60.0, 120.0]


# --- best_comment_hints ---


def test_best_comment_hints_prefers_pinned_then_uploader_then_likes():
    comments = [
        {"text": "0:00 Liked\n3:00 L2\n6:00 L3", "like_count": 500},
        {"text": "0:00 Uploader\n3:00 U2\n6:00 U3", "author_is_uploader": True, "like_count": 1},
        {"text": "0:00 Pinned\n3:00 P2\n6:00 P3", "is_pinned": True, "like_count": 0},
    ]
    assert best_comment_hints(comments, 900)[0].title == "Pinned"
    assert best_comment_hints(comments[:2], 900)[0].title == "Uploader"
    assert best_comment_hints(comments[:1], 900)[0].title == "Liked"


def test_best_comment_hints_skips_comments_without_a_tracklist():
    comments = [
        {"text": "great mix, the drop at 3:00!", "is_pinned": True},
        {"text": "0:00 One\n3:00 Two\n6:00 Three", "like_count": 3},
    ]
    assert [h.time for h in best_comment_hints(comments, 900)] == [0.0, 180.0, 360.0]
    assert best_comment_hints([], 900) == []
    assert best_comment_hints(None, 900) == []


def test_best_comment_hints_ignores_lists_of_favourite_moments():
    # Not a tracklist: it doesn't start near 0:00 (and its times happen to
    # add up to about the video's length, so must not be read as lengths).
    comments = [{"text": "3:12 when the bass drops\n17:45 goosebumps\n42:10 this transition", "like_count": 900}]
    assert best_comment_hints(comments, 3600) == []


# --- open_spans / in_spans ---


def test_open_spans():
    hints = [Hint(0.0, "A"), Hint(300.0, "B"), Hint(600.0, "Loop")]
    assert open_spans(hints, 14440.0) == [(600.0, 14440.0)]
    assert open_spans([Hint(0.0, "A"), Hint(300.0, "B"), Hint(2560.0, "C")], 3000.0) == [(300.0, 2560.0)]
    assert open_spans([], 3600.0) == [(0.0, 3600.0)]
    assert open_spans([Hint(1000.0, "Late")], 1500.0) == [(0.0, 1000.0)]
    assert open_spans([Hint(0.0, "A"), Hint(700.0, "B")], 1400.0) == []


def test_in_spans():
    spans = [(100.0, 200.0)]
    assert in_spans(100.0, spans)
    assert in_spans(150.0, spans)
    assert not in_spans(200.0, spans)
    assert not in_spans(50.0, [])


# --- gather_hints ---


def _no_comments():
    raise AssertionError("comments must not be fetched")


def test_gather_hints_uses_chapters_alone_when_they_cover_everything():
    chapters = [{"start_time": 0, "title": "A"}, {"start_time": 300, "title": "B"}]
    hints = gather_hints(chapters, "0:00 X\n1:00 Y\n2:00 Z", 600, _no_comments)
    assert hints == [Hint(0.0, "A"), Hint(300.0, "B")]


def test_gather_hints_fills_an_open_span_from_the_description():
    chapters = [{"start_time": 0, "title": "A"}, {"start_time": 300, "title": "Long tail"}]
    description = "0:00 A\n5:00 Long tail\n15:00 C\n25:00 D"
    hints = gather_hints(chapters, description, 1800, _no_comments)
    assert hints == [Hint(0.0, "A"), Hint(300.0, "Long tail"), Hint(900.0, "C"), Hint(1500.0, "D")]


def test_gather_hints_uses_the_description_when_there_are_no_chapters():
    hints = gather_hints(None, "0:00 One\n4:00 Two\n8:00 Three", 900, _no_comments)
    assert [h.title for h in hints] == ["One", "Two", "Three"]


def test_gather_hints_fetches_comments_only_while_spans_remain():
    calls = []

    def fetch():
        calls.append(1)
        return [{"text": "0:00 One\n4:00 Two\n8:00 Three", "is_pinned": True}]

    hints = gather_hints([], "no timestamps here", 900, fetch)
    assert hints == [Hint(0.0, "One"), Hint(240.0, "Two"), Hint(480.0, "Three")]
    assert calls == [1]
