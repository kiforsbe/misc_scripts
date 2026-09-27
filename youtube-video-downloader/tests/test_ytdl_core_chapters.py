"""
Tests for track splitting wired into ytdl_helper.core: fetch_info keeping
yt-dlp's chapter list, and download_item(split_chapters=True) turning an
audio download into a zip of per-track files (chapters, or silence gaps
when there are none).

yt_dlp.YoutubeDL is faked (no network) the same way test_ytdl_core_cancel.py
does it, but the split path uses real ffmpeg on small generated files,
since that's what actually finds and produces the tracks. Comment fetching
and per-track genre classification (network / an ML model) are stubbed.
"""

import asyncio
import pathlib
import shutil
import subprocess
import sys
import zipfile
from unittest.mock import patch

import pytest
import yt_dlp.utils

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from ytdl_helper import core as ytdl_core
from ytdl_helper.models import DownloadItem, FormatInfo
from ytdl_helper.utils import sanitize_filename

FFMPEG = shutil.which("ffmpeg")
needs_ffmpeg = pytest.mark.skipif(not FFMPEG, reason="ffmpeg not on PATH")

CHAPTERS = [
    {"start_time": 0.0, "end_time": 1.0, "title": "Daft Punk - One More Time"},
    {"start_time": 1.0, "end_time": 2.0, "title": "Daft Punk - Aerodynamic"},
    {"start_time": 2.0, "end_time": 3.0, "title": "Daft Punk - Digital Love"},
]

_REAL_TRACK_GENRE_CLASSIFIER = ytdl_core._track_genre_classifier
_REAL_FETCH_COMMENTS = ytdl_core._fetch_comments


@pytest.fixture(autouse=True)
def _no_network_or_ml(monkeypatch):
    """Split downloads may fetch comments (network) and classify each
    track's genre (an ML model); neither belongs in these tests."""
    monkeypatch.setattr(ytdl_core, "_fetch_comments", lambda url, use_cookies=False: [])
    monkeypatch.setattr(ytdl_core, "_track_genre_classifier", lambda: None)


def _make_audio_item(chapters) -> DownloadItem:
    item = DownloadItem(url="https://www.youtube.com/watch?v=test123")
    item.title = "Daft Punk - Discovery (Full Album)"
    item.artist = "Some Uploader"
    item.chapters = chapters
    item.audio_formats = [FormatInfo(format_id="140", ext="m4a", acodec="aac", abr=128.0)]
    item.selected_audio_format_id = "140"
    return item


def _make_fake_youtube_dl(download_impl):
    class _FakeYoutubeDL:
        def __init__(self, opts):
            self.opts = opts

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def download(self, urls):
            return download_impl(self.opts)

    return _FakeYoutubeDL


def _processed_path(opts, item) -> pathlib.Path:
    """Where download_item() expects yt-dlp + FFmpegExtractAudio's mp3."""
    temp_dir = pathlib.Path(opts["outtmpl"]).parent
    return temp_dir / f"{sanitize_filename(f'{item.artist} - {item.title}')}.mp3"


def _write_real_mp3(opts, item):
    """A real 3 s tone (no gaps), standing in for the processed download."""
    subprocess.run(
        [FFMPEG, "-v", "error", "-y", "-f", "lavfi", "-i", "sine=duration=3",
         "-c:a", "libmp3lame", str(_processed_path(opts, item))],
        check=True,
    )


def _write_layout_mp3(make_audio, layout):
    """A real mp3 with the conftest make_audio layout (e.g. "A_B")."""
    def _impl(opts, item):
        make_audio(_processed_path(opts, item), layout)
    return _impl


def _write_fake_file(ext):
    def _impl(opts, item):
        temp_dir = pathlib.Path(opts["outtmpl"]).parent
        out = temp_dir / f"{sanitize_filename(f'{item.artist} - {item.title}')}{ext}"
        out.write_bytes(b"fake media")
    return _impl


def _run_download(item, tmp_path, impl, **kwargs):
    with patch("yt_dlp.YoutubeDL", _make_fake_youtube_dl(lambda opts: impl(opts, item))):
        asyncio.run(ytdl_core.download_item(item, output_dir=tmp_path, **kwargs))


# --- fetch_info ---


def test_fetch_info_keeps_chapters():
    info = {
        "title": "Daft Punk - Discovery (Full Album)",
        "channel": "Some Uploader",
        "formats": [
            {"format_id": "140", "ext": "m4a", "acodec": "aac", "vcodec": "none", "abr": 128}
        ],
        "chapters": CHAPTERS,
    }

    class _FakeInfoYoutubeDL:
        def __init__(self, opts):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def extract_info(self, url, download=False):
            return info

    with patch("yt_dlp.YoutubeDL", _FakeInfoYoutubeDL):
        item = asyncio.run(ytdl_core.fetch_info("https://www.youtube.com/watch?v=test123"))

    assert item.chapters == CHAPTERS


def test_fetch_info_chapters_default_to_empty_list():
    info = {
        "title": "Song",
        "formats": [
            {"format_id": "140", "ext": "m4a", "acodec": "aac", "vcodec": "none", "abr": 128}
        ],
        "chapters": None,
    }

    class _FakeInfoYoutubeDL:
        def __init__(self, opts):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def extract_info(self, url, download=False):
            return info

    with patch("yt_dlp.YoutubeDL", _FakeInfoYoutubeDL):
        item = asyncio.run(ytdl_core.fetch_info("https://www.youtube.com/watch?v=test123"))

    assert item.chapters == []


# --- download_item(split_chapters=True) ---


@needs_ffmpeg
def test_split_chapters_produces_zip_of_tagged_tracks(tmp_path):
    item = _make_audio_item(CHAPTERS)

    _run_download(item, tmp_path, _write_real_mp3, target_format="mp3", split_chapters=True)

    assert item.status == "Complete"
    assert item.final_filepath.name == (
        "Some Uploader - Daft Punk - Discovery (Full Album) (tracks).zip"
    )
    with zipfile.ZipFile(item.final_filepath) as zf:
        assert zf.namelist() == [
            "01 - Daft Punk - One More Time.mp3",
            "02 - Daft Punk - Aerodynamic.mp3",
            "03 - Daft Punk - Digital Love.mp3",
        ]
    # Only the zip is delivered, not the full-length file as well.
    assert [p.name for p in tmp_path.iterdir()] == [item.final_filepath.name]


@needs_ffmpeg
def test_split_without_chapters_finds_tracks_from_silence(tmp_path, make_audio):
    item = _make_audio_item([])

    _run_download(
        item, tmp_path, _write_layout_mp3(make_audio, "A_B"), target_format="mp3", split_chapters=True
    )

    assert item.status == "Complete"
    with zipfile.ZipFile(item.final_filepath) as zf:
        assert zf.namelist() == ["01 - Daft Punk - Track 01.mp3", "02 - Daft Punk - Track 02.mp3"]


@needs_ffmpeg
def test_split_finds_tracks_in_the_original_download_not_the_transcode(tmp_path, make_audio):
    # Transcoding can blur a very short gap between songs (a 0.1 s silence in
    # a real mix didn't survive libmp3lame), so tracks are found in the
    # original download, which yt-dlp must keep, and cut from the transcode.
    item = _make_audio_item([])
    seen_opts = {}

    def impl(opts, item):
        seen_opts.update(opts)
        processed = _processed_path(opts, item)
        make_audio(processed.with_name(f"{item.title}.m4a"), "A_B")  # original: gap kept
        make_audio(processed, "AB")  # transcode: gap lost

    _run_download(item, tmp_path, impl, target_format="mp3", split_chapters=True)

    with zipfile.ZipFile(item.final_filepath) as zf:
        assert zf.namelist() == ["01 - Daft Punk - Track 01.mp3", "02 - Daft Punk - Track 02.mp3"]
    assert seen_opts["keepvideo"] is True


@needs_ffmpeg
def test_split_falls_back_to_single_file_with_fewer_than_two_tracks(tmp_path):
    item = _make_audio_item(CHAPTERS[:1])

    _run_download(item, tmp_path, _write_real_mp3, target_format="mp3", split_chapters=True)

    assert item.status == "Complete"
    assert item.final_filepath.name == "Some Uploader - Daft Punk - Discovery (Full Album).mp3"
    assert [p.name for p in tmp_path.iterdir()] == [item.final_filepath.name]


@needs_ffmpeg
def test_split_delivers_a_single_file_when_track_detection_crashes(tmp_path, monkeypatch):
    def crash(*args, **kwargs):
        raise ValueError("planner bug")

    monkeypatch.setattr(ytdl_core, "detect_tracks", crash)
    item = _make_audio_item(CHAPTERS)

    _run_download(item, tmp_path, _write_real_mp3, target_format="mp3", split_chapters=True)

    assert item.status == "Complete"
    assert item.final_filepath.name == "Some Uploader - Daft Punk - Discovery (Full Album).mp3"


@needs_ffmpeg
def test_split_classifies_each_tracks_genre_when_enabled(tmp_path, monkeypatch):
    classified = []

    def classify(path):
        classified.append(pathlib.Path(path).name)
        return "Synthwave"

    monkeypatch.setattr(ytdl_core, "_track_genre_classifier", lambda: classify)
    item = _make_audio_item(CHAPTERS)

    _run_download(item, tmp_path, _write_real_mp3, target_format="mp3", split_chapters=True)

    assert classified == [
        "01 - Daft Punk - One More Time.mp3",
        "02 - Daft Punk - Aerodynamic.mp3",
        "03 - Daft Punk - Digital Love.mp3",
    ]


@needs_ffmpeg
def test_split_reports_finding_and_splitting_progress(tmp_path):
    item = _make_audio_item(CHAPTERS)
    details = []

    _run_download(
        item, tmp_path, _write_real_mp3, target_format="mp3", split_chapters=True,
        status_callback=lambda it, status, detail: details.append(detail),
    )

    assert "Finding track boundaries" in details
    assert "Splitting into 3 tracks (1/3)" in details


def test_track_genre_classifier_follows_the_genre_setting(monkeypatch):
    monkeypatch.setattr(ytdl_core, "ENABLE_CUSTOM_GENRE_PP", True)
    monkeypatch.setattr(ytdl_core, "is_classifier_available", lambda: True)
    assert _REAL_TRACK_GENRE_CLASSIFIER() is ytdl_core.get_music_genre

    monkeypatch.setattr(ytdl_core, "is_classifier_available", lambda: False)
    assert _REAL_TRACK_GENRE_CLASSIFIER() is None

    monkeypatch.setattr(ytdl_core, "ENABLE_CUSTOM_GENRE_PP", False)
    monkeypatch.setattr(ytdl_core, "is_classifier_available", lambda: True)
    assert _REAL_TRACK_GENRE_CLASSIFIER() is None


def test_fetch_comments_asks_for_top_comments_without_replies():
    seen_opts = {}

    class _FakeYoutubeDL:
        def __init__(self, opts):
            seen_opts.update(opts)

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def extract_info(self, url, download=False):
            return {"comments": [{"text": "0:00 One"}]}

    with patch("yt_dlp.YoutubeDL", _FakeYoutubeDL):
        assert _REAL_FETCH_COMMENTS("https://www.youtube.com/watch?v=test123") == [{"text": "0:00 One"}]
    assert seen_opts["getcomments"] is True
    assert seen_opts["extractor_args"]["youtube"]["max_comments"] == ["30", "all", "0", "0"]


def test_fetch_comments_returns_empty_when_comments_are_unavailable():
    class _FailingYoutubeDL:
        def __init__(self, opts):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def extract_info(self, url, download=False):
            raise yt_dlp.utils.DownloadError("Comments are turned off")

    with patch("yt_dlp.YoutubeDL", _FailingYoutubeDL):
        assert _REAL_FETCH_COMMENTS("https://www.youtube.com/watch?v=test123") == []


def test_split_chapters_is_ignored_for_video_downloads(tmp_path):
    item = _make_audio_item(CHAPTERS)
    item.video_formats = [FormatInfo(format_id="137", ext="mp4", vcodec="avc1", height=1080)]
    item.selected_video_format_id = "137"

    with patch("ytdl_helper.core.check_ffmpeg", return_value="C:/fake/ffmpeg.exe"):
        _run_download(item, tmp_path, _write_fake_file(".mp4"), split_chapters=True)

    assert item.status == "Complete"
    assert item.final_filepath.suffix == ".mp4"


def test_split_chapters_off_by_default(tmp_path):
    item = _make_audio_item(CHAPTERS)

    with patch("ytdl_helper.core.check_ffmpeg", return_value="C:/fake/ffmpeg.exe"):
        _run_download(item, tmp_path, _write_fake_file(".mp3"), target_format="mp3")

    assert item.final_filepath.suffix == ".mp3"


@needs_ffmpeg
def test_existing_tracks_zip_is_reused_instead_of_redownloading(tmp_path):
    item = _make_audio_item(CHAPTERS)
    existing = tmp_path / "Some Uploader - Daft Punk - Discovery (Full Album) (tracks).zip"
    with zipfile.ZipFile(existing, "w") as zf:
        zf.writestr("01 - Daft Punk - One More Time.mp3", b"audio")

    def _must_not_download(opts, item):
        raise AssertionError("should have reused the existing zip")

    _run_download(item, tmp_path, _must_not_download, target_format="mp3", split_chapters=True)

    assert item.status == "Skipped"
    assert item.final_filepath == existing
