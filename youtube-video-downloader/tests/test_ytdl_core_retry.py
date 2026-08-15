"""
Tests for the automatic retry of transient-looking download failures in
ytdl_helper.core.download_item() (e.g. a signed CDN URL rejected with a
403/429/5xx - these commonly succeed moments later on a fresh extraction,
which is what manually re-running the download was already doing).

Uses the same fake-YoutubeDL pattern as test_ytdl_core_cancel.py: a fake
download() implementation stands in for yt-dlp so these exercise the real
retry loop in core.py without any real network/ffmpeg involved.
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


def _write_success_output(opts):
    temp_dir = pathlib.Path(opts["outtmpl"]).parent
    safe_base = sanitize_filename("Test Artist - Test Video")
    (temp_dir / f"{safe_base}.m4a").write_bytes(b"fake audio data")


@pytest.fixture(autouse=True)
def _fast_retries(monkeypatch):
    """Keep retry delays effectively instant so these tests run fast."""
    monkeypatch.setattr(ytdl_core, "DOWNLOAD_RETRY_DELAY_SECONDS", 0.01)


def test_is_likely_transient_download_error_matches_common_transient_cases():
    transient_messages = [
        "ERROR: unable to download video data: HTTP Error 403: Forbidden",
        "HTTP Error 429: Too Many Requests",
        "HTTP Error 503: Service Unavailable",
        "urlopen error [Errno 110] Connection timed out",
        "Remote end closed connection without response",
    ]
    for msg in transient_messages:
        assert ytdl_core._is_likely_transient_download_error(
            yt_dlp.utils.DownloadError(msg)
        ), f"expected transient: {msg}"


def test_is_likely_transient_download_error_rejects_permanent_cases():
    permanent_messages = [
        "ERROR: Private video. Sign in if you've been invited to this video",
        "ERROR: This video is unavailable",
        "ERROR: Video unavailable. This video has been removed by the uploader",
    ]
    for msg in permanent_messages:
        assert not ytdl_core._is_likely_transient_download_error(
            yt_dlp.utils.DownloadError(msg)
        ), f"expected non-transient: {msg}"


def test_download_item_retries_transient_error_and_then_succeeds(tmp_path, monkeypatch):
    monkeypatch.setattr(ytdl_core, "DOWNLOAD_RETRY_ATTEMPTS", 3)
    calls = {"n": 0}

    def download_impl(opts):
        calls["n"] += 1
        if calls["n"] < 3:
            raise yt_dlp.utils.DownloadError(
                "ERROR: unable to download video data: HTTP Error 403: Forbidden"
            )
        _write_success_output(opts)

    item = _make_audio_item()
    with patch("ytdl_helper.core.check_ffmpeg", return_value="C:/fake/ffmpeg.exe"), patch(
        "yt_dlp.YoutubeDL", _make_fake_youtube_dl(download_impl)
    ):
        asyncio.run(ytdl_core.download_item(item, output_dir=tmp_path))

    assert calls["n"] == 3
    assert item.status == "Complete"
    assert item.final_filepath is not None and item.final_filepath.exists()


def test_download_item_gives_up_after_exhausting_retries(tmp_path, monkeypatch):
    monkeypatch.setattr(ytdl_core, "DOWNLOAD_RETRY_ATTEMPTS", 2)
    calls = {"n": 0}

    def download_impl(opts):
        calls["n"] += 1
        raise yt_dlp.utils.DownloadError(
            "ERROR: unable to download video data: HTTP Error 403: Forbidden"
        )

    item = _make_audio_item()
    with patch("ytdl_helper.core.check_ffmpeg", return_value="C:/fake/ffmpeg.exe"), patch(
        "yt_dlp.YoutubeDL", _make_fake_youtube_dl(download_impl)
    ):
        with pytest.raises(yt_dlp.utils.DownloadError):
            asyncio.run(ytdl_core.download_item(item, output_dir=tmp_path))

    assert calls["n"] == 3  # 1 initial attempt + 2 retries
    assert item.status == "Error"


def test_download_item_does_not_retry_non_transient_error(tmp_path, monkeypatch):
    monkeypatch.setattr(ytdl_core, "DOWNLOAD_RETRY_ATTEMPTS", 3)
    calls = {"n": 0}

    def download_impl(opts):
        calls["n"] += 1
        raise yt_dlp.utils.DownloadError("ERROR: Private video")

    item = _make_audio_item()
    with patch("ytdl_helper.core.check_ffmpeg", return_value="C:/fake/ffmpeg.exe"), patch(
        "yt_dlp.YoutubeDL", _make_fake_youtube_dl(download_impl)
    ):
        with pytest.raises(yt_dlp.utils.DownloadError):
            asyncio.run(ytdl_core.download_item(item, output_dir=tmp_path))

    assert calls["n"] == 1, "a non-transient error must not be retried"


def test_download_item_stops_retrying_once_cancelled_between_attempts(tmp_path, monkeypatch):
    monkeypatch.setattr(ytdl_core, "DOWNLOAD_RETRY_ATTEMPTS", 5)
    calls = {"n": 0}
    cancel_event = threading.Event()

    def download_impl(opts):
        calls["n"] += 1
        cancel_event.set()  # cancelled while this (failing) attempt was in flight
        raise yt_dlp.utils.DownloadError(
            "ERROR: unable to download video data: HTTP Error 403: Forbidden"
        )

    item = _make_audio_item()
    with patch("ytdl_helper.core.check_ffmpeg", return_value="C:/fake/ffmpeg.exe"), patch(
        "yt_dlp.YoutubeDL", _make_fake_youtube_dl(download_impl)
    ):
        with pytest.raises(yt_dlp.utils.DownloadError):
            asyncio.run(
                ytdl_core.download_item(item, output_dir=tmp_path, cancel_event=cancel_event)
            )

    assert calls["n"] == 1, "must not retry once cancellation has been requested"


def test_download_item_retry_disabled_via_zero_attempts(tmp_path, monkeypatch):
    monkeypatch.setattr(ytdl_core, "DOWNLOAD_RETRY_ATTEMPTS", 0)
    calls = {"n": 0}

    def download_impl(opts):
        calls["n"] += 1
        raise yt_dlp.utils.DownloadError(
            "ERROR: unable to download video data: HTTP Error 403: Forbidden"
        )

    item = _make_audio_item()
    with patch("ytdl_helper.core.check_ffmpeg", return_value="C:/fake/ffmpeg.exe"), patch(
        "yt_dlp.YoutubeDL", _make_fake_youtube_dl(download_impl)
    ):
        with pytest.raises(yt_dlp.utils.DownloadError):
            asyncio.run(ytdl_core.download_item(item, output_dir=tmp_path))

    assert calls["n"] == 1
