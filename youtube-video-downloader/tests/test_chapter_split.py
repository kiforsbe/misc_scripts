"""
Tests for ytdl_helper.chapter_split: turning a video's chapter markers
into separately-tagged audio tracks.

The pure helpers (chapter-title parsing, album info, track planning) are
tested directly. The ffmpeg-backed splitting is tested against a real,
tiny generated audio file (skipped if ffmpeg isn't on PATH) since whether
stream-copy splitting keeps tags/cover art intact is exactly the kind of
thing that can't be predicted from the command line alone.
"""

import json
import pathlib
import shutil
import subprocess
import sys
import threading
import zipfile

import pytest
import yt_dlp.utils

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from ytdl_helper.chapter_split import (
    ChapterTrack,
    build_track_plan,
    derive_album_info,
    pack_tracks_zip,
    parse_chapter_title,
    split_audio_by_chapters,
)


# --- parse_chapter_title ---


@pytest.mark.parametrize(
    "chapter_title, expected",
    [
        ("Daft Punk - One More Time", ("Daft Punk", "One More Time")),
        ("01. Daft Punk - One More Time", ("Daft Punk", "One More Time")),
        ("3) Daft Punk – Aerodynamic", ("Daft Punk", "Aerodynamic")),
        ("12:34 Daft Punk — Digital Love", ("Daft Punk", "Digital Love")),
        ("1:02:03 - Daft Punk - Harder", ("Daft Punk", "Harder")),
        ("[07] Daft Punk - Crescendolls", ("Daft Punk", "Crescendolls")),
        ("  Daft Punk  -  Nightvision  ", ("Daft Punk", "Nightvision")),
    ],
)
def test_parse_chapter_title_splits_artist_and_title(chapter_title, expected):
    assert parse_chapter_title(chapter_title, fallback_artist="Fallback") == expected


def test_parse_chapter_title_splits_on_first_separator_only():
    assert parse_chapter_title("Daft Punk - Too Long - Live", "Fallback") == (
        "Daft Punk",
        "Too Long - Live",
    )


@pytest.mark.parametrize(
    "chapter_title, expected_title",
    [
        ("One More Time", "One More Time"),
        ("02. One More Time", "One More Time"),
        # A leading number that isn't followed by track-number punctuation
        # is part of the song title, not a track number.
        ("99 Luftballons", "99 Luftballons"),
        # "by" inside a title is not an artist separator.
        ("Stand by Me", "Stand by Me"),
        # A hyphen without surrounding spaces is part of the title.
        ("Harder-Better-Faster", "Harder-Better-Faster"),
    ],
)
def test_parse_chapter_title_without_separator_uses_fallback_artist(
    chapter_title, expected_title
):
    assert parse_chapter_title(chapter_title, fallback_artist="Fallback") == (
        "Fallback",
        expected_title,
    )


def test_parse_chapter_title_keeps_original_when_only_a_number_remains():
    assert parse_chapter_title("01.", fallback_artist=None) == (None, "01.")


# --- derive_album_info ---


@pytest.mark.parametrize(
    "video_title, channel, expected",
    [
        ("Daft Punk - Discovery (Full Album)", "Some Uploader", ("Daft Punk", "Discovery")),
        ("Daft Punk - Discovery [Full Album]", "Some Uploader", ("Daft Punk", "Discovery")),
        ("Daft Punk - Discovery - Full Album", "Some Uploader", ("Daft Punk", "Discovery")),
        ("Daft Punk – Discovery", "Some Uploader", ("Daft Punk", "Discovery")),
        ("Best of 2020 Mix", "DJ Channel", ("DJ Channel", "Best of 2020 Mix")),
        ("Discovery (Full Album)", "Daft Punk - Topic", ("Daft Punk", "Discovery")),
        ("Best of 2020 Mix", None, (None, "Best of 2020 Mix")),
    ],
)
def test_derive_album_info(video_title, channel, expected):
    assert derive_album_info(video_title, channel) == expected


# --- build_track_plan ---


def _chapters():
    return [
        {"start_time": 0.0, "end_time": 2.0, "title": "01. Daft Punk - One More Time"},
        {"start_time": 2.0, "end_time": 3.0, "title": "Aerodynamic"},
        {"start_time": 3.0, "end_time": 5.0, "title": "Romanthony - Too Long"},
    ]


def test_build_track_plan_tags_each_chapter():
    tracks = build_track_plan(
        _chapters(),
        video_title="Daft Punk - Discovery (Full Album)",
        channel="Some Uploader",
        year=2001,
    )

    assert [(t.number, t.total) for t in tracks] == [(1, 3), (2, 3), (3, 3)]
    assert [(t.artist, t.title) for t in tracks] == [
        ("Daft Punk", "One More Time"),
        ("Daft Punk", "Aerodynamic"),  # no separator -> album artist
        ("Romanthony", "Too Long"),
    ]
    assert all(t.album == "Discovery" for t in tracks)
    assert all(t.album_artist == "Daft Punk" for t in tracks)
    assert all(t.year == 2001 for t in tracks)
    assert [(t.start, t.end) for t in tracks] == [(0.0, 2.0), (2.0, 3.0), (3.0, 5.0)]


def test_build_track_plan_skips_zero_length_chapters():
    chapters = _chapters()
    chapters.insert(1, {"start_time": 2.0, "end_time": 2.0, "title": "Empty"})

    tracks = build_track_plan(chapters, video_title="Mix", channel="DJ", year=None)

    assert [t.title for t in tracks] == ["One More Time", "Aerodynamic", "Too Long"]
    assert [(t.number, t.total) for t in tracks] == [(1, 3), (2, 3), (3, 3)]


def test_build_track_plan_empty_when_no_chapters():
    assert build_track_plan([], video_title="Mix", channel="DJ", year=None) == []
    assert build_track_plan(None, video_title="Mix", channel="DJ", year=None) == []


def test_track_filename_stem_is_numbered_and_filesystem_safe():
    track = ChapterTrack(
        number=3, total=12, start=0.0, end=1.0,
        artist="AC/DC", title="Back: In Black?",
        album="X", album_artist="AC/DC", year=None,
    )
    assert track.filename_stem == "03 - AC_DC - Back_ In Black_"


def test_track_filename_stem_without_artist():
    track = ChapterTrack(
        number=1, total=2, start=0.0, end=1.0,
        artist=None, title="Intro",
        album="X", album_artist=None, year=None,
    )
    assert track.filename_stem == "01 - Intro"


# --- split_audio_by_chapters / pack_tracks_zip (real ffmpeg) ---

FFMPEG = shutil.which("ffmpeg")
FFPROBE = shutil.which("ffprobe")
needs_ffmpeg = pytest.mark.skipif(
    not (FFMPEG and FFPROBE), reason="ffmpeg/ffprobe not on PATH"
)


def _make_source_audio(tmp_path: pathlib.Path, ext: str) -> pathlib.Path:
    """A 5s tone tagged like a real download (whole-video title/artist,
    a genre) with a cover image attached, so tests can check which tags
    each split track keeps or overrides."""
    cover = tmp_path / "cover.png"
    subprocess.run(
        [FFMPEG, "-v", "error", "-y", "-f", "lavfi", "-i", "color=c=red:s=32x32",
         "-frames:v", "1", str(cover)],
        check=True,
    )
    src = tmp_path / f"source{ext}"
    audio_codec = ["-c:a", "libmp3lame"] if ext == ".mp3" else ["-c:a", "aac"]
    subprocess.run(
        [FFMPEG, "-v", "error", "-y",
         "-f", "lavfi", "-i", "sine=frequency=440:duration=5",
         "-i", str(cover),
         "-map", "0:a", "-map", "1:v", *audio_codec, "-c:v", "png",
         "-disposition:v", "attached_pic",
         "-metadata", "title=Whole Video Title",
         "-metadata", "artist=Whole Video Artist",
         "-metadata", "genre=Electronic",
         str(src)],
        check=True,
    )
    return src


def _probe(path: pathlib.Path) -> dict:
    out = subprocess.run(
        [FFPROBE, "-v", "error", "-show_format", "-show_streams", "-of", "json", str(path)],
        capture_output=True, text=True, check=True,
    ).stdout
    return json.loads(out)


def _tags(probe: dict) -> dict:
    return {k.lower(): v for k, v in probe["format"].get("tags", {}).items()}


@needs_ffmpeg
@pytest.mark.parametrize("ext", [".mp3", ".m4a"])
def test_split_audio_by_chapters_writes_tagged_tracks(tmp_path, ext):
    src = _make_source_audio(tmp_path, ext)
    tracks = build_track_plan(
        _chapters(),
        video_title="Daft Punk - Discovery (Full Album)",
        channel="Some Uploader",
        year=2001,
    )
    out_dir = tmp_path / "tracks"

    paths = split_audio_by_chapters(src, tracks, out_dir, ffmpeg_path=FFMPEG)

    assert [p.name for p in paths] == [
        f"01 - Daft Punk - One More Time{ext}",
        f"02 - Daft Punk - Aerodynamic{ext}",
        f"03 - Romanthony - Too Long{ext}",
    ]
    first = _probe(paths[0])
    tags = _tags(first)
    assert tags["title"] == "One More Time"
    assert tags["artist"] == "Daft Punk"
    assert tags["album"] == "Discovery"
    assert tags["album_artist"] == "Daft Punk"
    assert tags["track"] == "1/3"
    assert tags["date"] == "2001"
    assert tags["genre"] == "Electronic"  # carried over from the full file
    assert abs(float(first["format"]["duration"]) - 2.0) < 0.2
    assert any(
        s.get("disposition", {}).get("attached_pic") == 1 for s in first["streams"]
    ), "cover art should be carried over"
    assert "chapters" not in first or not first.get("chapters")

    third = _probe(paths[2])
    assert _tags(third)["artist"] == "Romanthony"
    assert abs(float(third["format"]["duration"]) - 2.0) < 0.2


@needs_ffmpeg
def test_split_audio_by_chapters_stops_when_cancelled(tmp_path):
    src = _make_source_audio(tmp_path, ".mp3")
    tracks = build_track_plan(_chapters(), video_title="Mix", channel="DJ", year=None)
    cancel_event = threading.Event()
    cancel_event.set()

    with pytest.raises(yt_dlp.utils.DownloadCancelled):
        split_audio_by_chapters(
            src, tracks, tmp_path / "tracks", ffmpeg_path=FFMPEG, cancel_event=cancel_event
        )


def test_split_audio_by_chapters_raises_when_ffmpeg_fails(tmp_path):
    src = tmp_path / "not-really-audio.mp3"
    src.write_bytes(b"garbage")
    tracks = build_track_plan(_chapters(), video_title="Mix", channel="DJ", year=None)
    if not FFMPEG:
        pytest.skip("ffmpeg not on PATH")

    with pytest.raises(RuntimeError, match="One More Time"):
        split_audio_by_chapters(src, tracks, tmp_path / "tracks", ffmpeg_path=FFMPEG)


def test_pack_tracks_zip_stores_files_flat(tmp_path):
    a = tmp_path / "01 - A - One.mp3"
    b = tmp_path / "02 - B - Two.mp3"
    a.write_bytes(b"one")
    b.write_bytes(b"two")
    zip_path = tmp_path / "out" / "Album (chapters).zip"

    pack_tracks_zip([a, b], zip_path)

    with zipfile.ZipFile(zip_path) as zf:
        assert zf.namelist() == ["01 - A - One.mp3", "02 - B - Two.mp3"]
        assert zf.read("02 - B - Two.mp3") == b"two"
