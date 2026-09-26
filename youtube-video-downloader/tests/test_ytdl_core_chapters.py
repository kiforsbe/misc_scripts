"""
Tests for chapter splitting wired into ytdl_helper.core: fetch_info keeping
yt-dlp's chapter list, and download_item(split_chapters=True) turning an
audio download into a zip of per-chapter tracks.

yt_dlp.YoutubeDL is faked (no network) the same way test_ytdl_core_cancel.py
does it, but the split path uses real ffmpeg on a tiny generated file,
since that's what actually produces the tracks.
"""

import asyncio
import pathlib
import shutil
import subprocess
import sys
import zipfile
from unittest.mock import patch

import pytest

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


def _write_real_mp3(opts, item):
    """Stands in for yt-dlp + FFmpegExtractAudio: a real 3s mp3 where
    download_item() expects to find the processed file."""
    temp_dir = pathlib.Path(opts["outtmpl"]).parent
    out = temp_dir / f"{sanitize_filename(f'{item.artist} - {item.title}')}.mp3"
    subprocess.run(
        [FFMPEG, "-v", "error", "-y", "-f", "lavfi", "-i", "sine=duration=3",
         "-c:a", "libmp3lame", str(out)],
        check=True,
    )


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
        "Some Uploader - Daft Punk - Discovery (Full Album) (chapters).zip"
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
def test_split_chapters_falls_back_to_single_file_with_fewer_than_two_chapters(tmp_path):
    item = _make_audio_item(CHAPTERS[:1])

    _run_download(item, tmp_path, _write_real_mp3, target_format="mp3", split_chapters=True)

    assert item.status == "Complete"
    assert item.final_filepath.suffix == ".mp3"


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
def test_existing_chapters_zip_is_reused_instead_of_redownloading(tmp_path):
    item = _make_audio_item(CHAPTERS)
    existing = tmp_path / "Some Uploader - Daft Punk - Discovery (Full Album) (chapters).zip"
    with zipfile.ZipFile(existing, "w") as zf:
        zf.writestr("01 - Daft Punk - One More Time.mp3", b"audio")

    def _must_not_download(opts, item):
        raise AssertionError("should have reused the existing zip")

    _run_download(item, tmp_path, _must_not_download, target_format="mp3", split_chapters=True)

    assert item.status == "Skipped"
    assert item.final_filepath == existing
