"""
Tests for ytdl_helper.chapter_split: turning a video's chapter markers
into separately-tagged audio tracks.

The pure helpers (chapter-title parsing, artist/title orientation, album
info) are tested directly. The ffmpeg-backed splitting is tested against a real,
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
    derive_album_info,
    orient_artist_title,
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


# --- orient_artist_title ---


def test_orient_artist_title_swaps_when_the_title_part_repeats():
    names = [
        ("Photos in the Rain", "Luna Vale"),
        ("Stay in My Arms", "Luna Vale"),
        ("Back to Me", "luna vale"),
    ]
    assert orient_artist_title(names) == [
        ("Luna Vale", "Photos in the Rain"),
        ("Luna Vale", "Stay in My Arms"),
        ("luna vale", "Back to Me"),
    ]


def test_orient_artist_title_keeps_artist_title_albums():
    names = [("Daft Punk", "One More Time"), ("Daft Punk", "Aerodynamic"), ("Romanthony", "Too Long")]
    assert orient_artist_title(names) == names


def test_orient_artist_title_never_swaps_on_a_tie():
    names = [("A", "X"), ("A", "X")]
    assert orient_artist_title(names) == names


def test_orient_artist_title_leaves_unsplit_names_alone():
    names = [(None, "Intro"), ("Song One", "Luna Vale"), ("Song Two", "Luna Vale"), (None, "Track 04")]
    assert orient_artist_title(names) == [
        (None, "Intro"), ("Luna Vale", "Song One"), ("Luna Vale", "Song Two"), (None, "Track 04"),
    ]


def test_orient_artist_title_needs_a_real_repeat():
    names = [("Song One", "Luna Vale"), ("Song Two", "Kai Mori")]
    assert orient_artist_title(names) == names
    assert orient_artist_title([]) == []


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


# --- ChapterTrack ---


def _tracks():
    """Three tracks over the 5 s test tone: 0-2 s, 2-3 s, 3-5 s."""
    spec = [
        (0.0, 2.0, "Daft Punk", "One More Time"),
        (2.0, 3.0, "Daft Punk", "Aerodynamic"),
        (3.0, 5.0, "Romanthony", "Too Long"),
    ]
    return [
        ChapterTrack(
            number=n, total=len(spec), start=start, end=end, artist=artist, title=title,
            album="Discovery", album_artist="Daft Punk", year=2001,
        )
        for n, (start, end, artist, title) in enumerate(spec, start=1)
    ]


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
    tracks = _tracks()
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
    tracks = _tracks()
    cancel_event = threading.Event()
    cancel_event.set()

    with pytest.raises(yt_dlp.utils.DownloadCancelled):
        split_audio_by_chapters(
            src, tracks, tmp_path / "tracks", ffmpeg_path=FFMPEG, cancel_event=cancel_event
        )


def test_split_audio_by_chapters_raises_when_ffmpeg_fails(tmp_path):
    src = tmp_path / "not-really-audio.mp3"
    src.write_bytes(b"garbage")
    tracks = _tracks()
    if not FFMPEG:
        pytest.skip("ffmpeg not on PATH")

    with pytest.raises(RuntimeError, match="One More Time"):
        split_audio_by_chapters(src, tracks, tmp_path / "tracks", ffmpeg_path=FFMPEG)


@needs_ffmpeg
@pytest.mark.parametrize("ext", [".mp3", ".m4a"])
def test_split_audio_by_chapters_tags_each_track_with_its_own_genre(tmp_path, ext):
    src = _make_source_audio(tmp_path, ext)
    genres = iter(["Synthwave", None, "Italo-Disco"])
    classified = []

    def classify(path):
        classified.append(pathlib.Path(path).name)
        return next(genres)

    paths = split_audio_by_chapters(
        src, _tracks(), tmp_path / "tracks", ffmpeg_path=FFMPEG, classify_genre=classify
    )

    assert classified == [p.name for p in paths]
    # None keeps the full file's genre; re-tagging keeps the other tags.
    assert [_tags(_probe(p))["genre"] for p in paths] == ["Synthwave", "Electronic", "Italo-Disco"]
    assert _tags(_probe(paths[0]))["title"] == "One More Time"


@needs_ffmpeg
def test_split_audio_by_chapters_survives_a_failing_genre_classifier(tmp_path):
    src = _make_source_audio(tmp_path, ".mp3")

    def classify(path):
        raise RuntimeError("model exploded")

    paths = split_audio_by_chapters(
        src, _tracks(), tmp_path / "tracks", ffmpeg_path=FFMPEG, classify_genre=classify
    )

    assert len(paths) == 3
    assert _tags(_probe(paths[0]))["genre"] == "Electronic"


@needs_ffmpeg
def test_split_audio_by_chapters_reports_each_track(tmp_path):
    src = _make_source_audio(tmp_path, ".mp3")
    seen = []

    split_audio_by_chapters(
        src, _tracks(), tmp_path / "tracks", ffmpeg_path=FFMPEG, on_track=lambda t: seen.append(t.number)
    )

    assert seen == [1, 2, 3]


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
