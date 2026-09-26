"""
Tests for the Flask service's split_chapters option: request parsing, the
dedup key (a split and an unsplit request for the same video are different
downloads), pass-through to ytdl_core.download_item, and /list_formats
exposing chapters so the userscript knows when to offer splitting.

ytdl_core.fetch_info/download_item are faked - their chapter behavior is
covered in test_ytdl_core_chapters.py.
"""

import asyncio
import importlib
import pathlib
import sys
import tempfile
import time

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from ytdl_helper.models import DownloadItem, FormatInfo

MODULE_NAME = "youtube-video-downloader.youtube-video-downloader-flask-ws"
URL = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
CHAPTERS = [
    {"start_time": 0.0, "end_time": 60.0, "title": "Daft Punk - One More Time"},
    {"start_time": 60.0, "end_time": 120.0, "title": "Daft Punk - Aerodynamic"},
]


@pytest.fixture()
def mod():
    m = importlib.import_module(MODULE_NAME)
    m.FFMPEG_PATH = "C:/fake/ffmpeg.exe"
    original = (
        m._process_download,
        m.ytdl_core.fetch_info,
        m.ytdl_core.download_item,
        m.ENABLE_CONSOLE_PROGRESS,
    )
    m.ENABLE_CONSOLE_PROGRESS = False
    m._active_downloads.clear()
    m._jobs.clear()

    yield m

    (
        m._process_download,
        m.ytdl_core.fetch_info,
        m.ytdl_core.download_item,
        m.ENABLE_CONSOLE_PROGRESS,
    ) = original
    m._active_downloads.clear()
    m._jobs.clear()


def _parse(m, data):
    with m.app.app_context():
        return m._parse_download_request(data)


@pytest.mark.parametrize("value", ["1", "true", "True", "on", "yes", True])
def test_split_chapters_truthy_values_are_parsed(mod, value):
    params, error = _parse(mod, {"url": URL, "split_chapters": value})
    assert error is None
    assert params["split_chapters"] is True


@pytest.mark.parametrize("value", [None, "", "0", "false", False])
def test_split_chapters_defaults_to_false(mod, value):
    data = {"url": URL}
    if value is not None:
        data["split_chapters"] = value
    params, error = _parse(mod, data)
    assert error is None
    assert params["split_chapters"] is False


def test_split_and_unsplit_requests_have_different_dedup_keys(mod):
    unsplit = mod._compute_dedup_key(URL, "140", None, "mp3", None, None)
    split = mod._compute_dedup_key(URL, "140", None, "mp3", None, None, split_chapters=True)
    assert unsplit != split


def test_download_start_passes_split_chapters_through(mod):
    received = {}
    result_file = pathlib.Path(tempfile.gettempdir()) / "ytdl_pytest_chapters.zip"
    result_file.write_bytes(b"zip")

    async def fake_process_download(*a, progress_hook=None, cancel_event=None, **kw):
        received.update(kw)
        return result_file

    mod._process_download = fake_process_download
    client = mod.app.test_client()

    resp = client.post(
        "/download/start",
        data={"url": URL, "audio_format_id": "140", "target_format": "mp3", "split_chapters": "1"},
    )
    job_id = resp.get_json()["job_id"]

    deadline = time.time() + 5
    while time.time() < deadline:
        if client.get(f"/download/status/{job_id}").get_json()["status"] == "complete":
            break
        time.sleep(0.05)

    assert received.get("split_chapters") is True


def test_process_download_forwards_split_chapters_and_reports_splitting(mod, tmp_path):
    final = tmp_path / "Artist - Mix (chapters).zip"
    final.write_bytes(b"zip")
    download_kwargs = {}

    async def fake_fetch_info(url):
        item = DownloadItem(url=url, title="Mix", artist="DJ")
        item.chapters = CHAPTERS
        item.audio_formats = [FormatInfo(format_id="140", ext="m4a", acodec="aac", abr=128.0)]
        return item

    async def fake_download_item(item, **kwargs):
        download_kwargs.update(kwargs)
        kwargs["status_callback"](item, "Processing", "Splitting into 2 chapter tracks")
        item.status = "Complete"
        item.final_filepath = final

    mod.ytdl_core.fetch_info = fake_fetch_info
    mod.ytdl_core.download_item = fake_download_item
    messages = []

    result = asyncio.run(
        mod._process_download(
            URL, "140", None, "mp3", None, None,
            progress_hook=lambda pct, msg=None: messages.append(msg),
            split_chapters=True,
        )
    )

    assert result == final
    assert download_kwargs["split_chapters"] is True
    assert "Mix - Splitting into 2 chapter tracks" in messages


def test_list_formats_includes_chapters(mod):
    async def fake_fetch_info(url):
        item = DownloadItem(url=url, title="Mix", artist="DJ")
        item.chapters = CHAPTERS
        return item

    mod.ytdl_core.fetch_info = fake_fetch_info

    resp = mod.app.test_client().get("/list_formats", query_string={"url": URL})

    assert resp.status_code == 200
    assert resp.get_json()["chapters"] == [
        {"title": "Daft Punk - One More Time", "start_time": 0.0, "end_time": 60.0},
        {"title": "Daft Punk - Aerodynamic", "start_time": 60.0, "end_time": 120.0},
    ]
