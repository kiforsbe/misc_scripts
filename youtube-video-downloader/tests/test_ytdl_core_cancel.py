"""
Tests for cancel_event support in ytdl_helper.core.download_item().

yt-dlp's documented mechanism for aborting a download from within a
progress hook is to raise yt_dlp.utils.DownloadCancelled from the hook.
These tests stand in for yt_dlp.YoutubeDL with a fake that just invokes
the registered progress hook directly (no network/ffmpeg involved), so
they exercise the real _progress_hook/cancel_event wiring in core.py
without depending on a real download.
"""

import asyncio
import pathlib
import sys
import threading
from unittest.mock import patch

import pytest
import yt_dlp.utils

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from ytdl_helper import core as ytdl_core
from ytdl_helper.models import DownloadItem, FormatInfo
from ytdl_helper.utils import sanitize_filename


def _make_audio_item() -> DownloadItem:
    item = DownloadItem(url="https://www.youtube.com/watch?v=test123")
    item.title = "Test Video"
    item.artist = "Test Artist"
    item.audio_formats = [
        FormatInfo(format_id="140", ext="m4a", acodec="aac", abr=128.0)
    ]
    item.selected_audio_format_id = "140"
    item.selected_video_format_id = None
    return item


def _make_fake_youtube_dl(download_impl):
    """Returns a class that can stand in for yt_dlp.YoutubeDL: a context
    manager whose download() just calls download_impl(opts)."""

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


def _cancelling_download_impl(opts):
    """Simulates yt-dlp's real behavior: the progress hook raising
    DownloadCancelled aborts the download and propagates out of download()."""
    hook = opts["progress_hooks"][0]
    hook({"status": "downloading", "downloaded_bytes": 100, "total_bytes": 1000})


def _succeeding_download_impl(opts):
    """Simulates a normal completed download: one progress tick, then the
    expected output file appears in the temp dir for download_item() to find."""
    hook = opts["progress_hooks"][0]
    hook({"status": "downloading", "downloaded_bytes": 500, "total_bytes": 1000})
    temp_dir = pathlib.Path(opts["outtmpl"]).parent
    safe_base = sanitize_filename("Test Artist - Test Video")
    (temp_dir / f"{safe_base}.m4a").write_bytes(b"fake audio data")


def test_download_item_cancels_when_cancel_event_is_already_set(tmp_path):
    item = _make_audio_item()
    cancel_event = threading.Event()
    cancel_event.set()  # cancelled before the download even starts

    with patch("ytdl_helper.core.check_ffmpeg", return_value="C:/fake/ffmpeg.exe"), patch(
        "yt_dlp.YoutubeDL", _make_fake_youtube_dl(_cancelling_download_impl)
    ):
        with pytest.raises(yt_dlp.utils.DownloadCancelled):
            asyncio.run(
                ytdl_core.download_item(
                    item,
                    output_dir=tmp_path,
                    cancel_event=cancel_event,
                )
            )

    assert item.status == "Cancelled"


def test_download_item_cancels_when_cancel_event_is_set_mid_download(tmp_path):
    """The hook checks cancel_event on every tick, so setting it from
    another thread mid-download must also abort - not just a pre-set flag."""
    item = _make_audio_item()
    cancel_event = threading.Event()

    def _download_then_get_cancelled(opts):
        hook = opts["progress_hooks"][0]
        # First tick: not cancelled yet.
        hook({"status": "downloading", "downloaded_bytes": 100, "total_bytes": 1000})
        # Something external cancels it between ticks.
        cancel_event.set()
        # Second tick: hook must now raise.
        hook({"status": "downloading", "downloaded_bytes": 200, "total_bytes": 1000})

    with patch("ytdl_helper.core.check_ffmpeg", return_value="C:/fake/ffmpeg.exe"), patch(
        "yt_dlp.YoutubeDL", _make_fake_youtube_dl(_download_then_get_cancelled)
    ):
        with pytest.raises(yt_dlp.utils.DownloadCancelled):
            asyncio.run(
                ytdl_core.download_item(
                    item,
                    output_dir=tmp_path,
                    cancel_event=cancel_event,
                )
            )

    assert item.status == "Cancelled"


def test_download_item_completes_normally_without_cancel_event(tmp_path):
    """Sanity check: passing no cancel_event (the default) doesn't change
    the normal success path."""
    item = _make_audio_item()

    with patch("ytdl_helper.core.check_ffmpeg", return_value="C:/fake/ffmpeg.exe"), patch(
        "yt_dlp.YoutubeDL", _make_fake_youtube_dl(_succeeding_download_impl)
    ):
        asyncio.run(
            ytdl_core.download_item(
                item,
                output_dir=tmp_path,
            )
        )

    assert item.status == "Complete"
    assert item.final_filepath is not None
    assert item.final_filepath.exists()


def test_download_item_completes_normally_when_cancel_event_never_set(tmp_path):
    """A cancel_event that's passed but never set must not interfere."""
    item = _make_audio_item()
    cancel_event = threading.Event()

    with patch("ytdl_helper.core.check_ffmpeg", return_value="C:/fake/ffmpeg.exe"), patch(
        "yt_dlp.YoutubeDL", _make_fake_youtube_dl(_succeeding_download_impl)
    ):
        asyncio.run(
            ytdl_core.download_item(
                item,
                output_dir=tmp_path,
                cancel_event=cancel_event,
            )
        )

    assert item.status == "Complete"
