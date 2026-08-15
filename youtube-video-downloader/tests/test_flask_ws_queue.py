"""
Flask-level integration tests for the download concurrency queue
(YTDL_MAX_CONCURRENT_DOWNLOADS / mod.download_queue), wired into
_run_download_deduped in youtube-video-downloader-flask-ws.py.

These cover the queue's interaction with the rest of the service: the
default cap of 1, a configurable higher cap, cancelling a job while it's
still queued (not yet downloading), and the key invariant that a dedup
"waiter" never consumes a queue slot of its own - only the "owner" that
actually calls _process_download does. Pure DownloadQueue behavior (FIFO
ordering, position reporting) is covered in test_download_queue.py.

Note: a job's status flips to "downloading" (via on_downloading_started)
slightly *before* its fake _process_download body actually runs, so tests
that need to know the fake has actually started signal that explicitly
with a threading.Event rather than polling job status for it - the same
pattern the rest of this test suite uses.
"""

import importlib
import pathlib
import sys
import tempfile
import threading
import time

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from ytdl_helper.download_queue import DownloadQueue

MODULE_NAME = "youtube-video-downloader.youtube-video-downloader-flask-ws"


@pytest.fixture()
def mod():
    m = importlib.import_module(MODULE_NAME)

    m.ytdl_core.clean_youtube_url = lambda u: u
    m.FFMPEG_PATH = "C:/fake/ffmpeg.exe"

    original_process_download = m._process_download
    original_enable_console_progress = m.ENABLE_CONSOLE_PROGRESS
    original_download_queue = m.download_queue

    m._active_downloads.clear()
    m._jobs.clear()
    m.ENABLE_CONSOLE_PROGRESS = False

    yield m

    m._process_download = original_process_download
    m.ENABLE_CONSOLE_PROGRESS = original_enable_console_progress
    m.download_queue = original_download_queue
    m._active_downloads.clear()
    m._jobs.clear()


@pytest.fixture()
def client(mod):
    return mod.app.test_client()


@pytest.fixture()
def result_file():
    path = pathlib.Path(tempfile.gettempdir()) / "ytdl_pytest_queue_result.mp4"
    path.write_bytes(b"fake video bytes")
    yield path


def _poll_until(predicate, get_value, timeout=5.0, interval=0.05):
    deadline = time.time() + timeout
    value = None
    while time.time() < deadline:
        value = get_value()
        if predicate(value):
            return value
        time.sleep(interval)
    return value


def _status(client, job_id):
    return client.get(f"/download/status/{job_id}").get_json()


def test_default_cap_serializes_two_different_items(mod, client, result_file):
    """With the default cap of 1, a second job for a *different* item must
    stay queued until the first job's real download releases its slot."""
    started = threading.Event()
    release = threading.Event()
    count = {"n": 0}
    lock = threading.Lock()

    async def fake_process_download(*a, progress_hook=None, cancel_event=None, **kw):
        with lock:
            count["n"] += 1
        started.set()
        release.wait(timeout=3)
        return result_file

    mod._process_download = fake_process_download

    r1 = client.post("/download/start", json={"url": "https://example.com/q1"})
    job1 = r1.get_json()["job_id"]
    assert started.wait(timeout=2)

    r2 = client.post("/download/start", json={"url": "https://example.com/q2"})
    job2 = r2.get_json()["job_id"]

    time.sleep(0.2)
    assert count["n"] == 1, "second item's download started while cap=1 was already in use"
    assert _status(client, job2)["status"] == "queued"

    release.set()

    for job_id in (job1, job2):
        final = _poll_until(lambda d: d["status"] == "complete", lambda jid=job_id: _status(client, jid))
        assert final["status"] == "complete"

    assert count["n"] == 2


def test_configurable_cap_allows_two_different_items_concurrently(mod, client, result_file):
    mod.download_queue = DownloadQueue(max_concurrent=2)

    release = threading.Event()
    count = {"n": 0}
    lock = threading.Lock()

    async def fake_process_download(*a, progress_hook=None, cancel_event=None, **kw):
        with lock:
            count["n"] += 1
        release.wait(timeout=3)
        return result_file

    mod._process_download = fake_process_download

    r1 = client.post("/download/start", json={"url": "https://example.com/qc1"})
    job1 = r1.get_json()["job_id"]
    r2 = client.post("/download/start", json={"url": "https://example.com/qc2"})
    job2 = r2.get_json()["job_id"]

    _poll_until(lambda n: n == 2, lambda: count["n"], timeout=2)
    assert count["n"] == 2, "cap=2 should allow both different items to download at once"

    release.set()
    for job_id in (job1, job2):
        final = _poll_until(lambda d: d["status"] == "complete", lambda jid=job_id: _status(client, jid))
        assert final["status"] == "complete"


def test_cancel_while_queued_never_starts_its_download(mod, client, result_file):
    holder_started = threading.Event()
    holder_release = threading.Event()
    count = {"n": 0}
    lock = threading.Lock()

    async def fake_process_download(*a, progress_hook=None, cancel_event=None, **kw):
        with lock:
            count["n"] += 1
        holder_started.set()
        holder_release.wait(timeout=3)
        return result_file

    mod._process_download = fake_process_download

    r1 = client.post("/download/start", json={"url": "https://example.com/qcancel-holder"})
    job_holder = r1.get_json()["job_id"]
    assert holder_started.wait(timeout=2), "holder's download never started"

    r2 = client.post("/download/start", json={"url": "https://example.com/qcancel-queued"})
    job_queued = r2.get_json()["job_id"]
    assert _status(client, job_queued)["status"] == "queued"

    cancel_resp = client.post(f"/download/cancel/{job_queued}")
    assert cancel_resp.status_code == 200

    final = _poll_until(lambda d: d["status"] == "cancelled", lambda: _status(client, job_queued))
    assert final["status"] == "cancelled"
    # holder_started already proved the holder's own download ran; give the
    # (never-started) queued job a moment to prove it truly never runs too.
    time.sleep(0.2)
    assert count["n"] == 1, "a job cancelled while still queued must never start its download"

    holder_release.set()
    _poll_until(lambda d: d["status"] == "complete", lambda: _status(client, job_holder))


def test_queued_message_includes_title_hint_when_provided(mod, client, result_file):
    """A job queued behind another (cap=1) should show the client-supplied
    title alongside its queue position, not just the generic message -
    the server doesn't know the real title itself until _process_download
    fetches it, which only happens after a queue slot is claimed."""
    holder_started = threading.Event()
    holder_release = threading.Event()

    async def fake_process_download(*a, progress_hook=None, cancel_event=None, **kw):
        holder_started.set()
        holder_release.wait(timeout=3)
        return result_file

    mod._process_download = fake_process_download

    r1 = client.post("/download/start", json={"url": "https://example.com/qtitle-holder"})
    job_holder = r1.get_json()["job_id"]
    assert holder_started.wait(timeout=2)

    r2 = client.post(
        "/download/start",
        json={"url": "https://example.com/qtitle-queued", "title_hint": "My Cool Video"},
    )
    job_queued = r2.get_json()["job_id"]

    queued_status = _poll_until(
        lambda d: "position" in (d.get("message") or ""),
        lambda: _status(client, job_queued),
    )
    assert queued_status["message"].startswith("My Cool Video - "), queued_status
    assert "position" in queued_status["message"]

    holder_release.set()
    for job_id in (job_holder, job_queued):
        final = _poll_until(lambda d: d["status"] == "complete", lambda jid=job_id: _status(client, jid))
        assert final["status"] == "complete"


def test_queued_message_falls_back_to_generic_text_without_title_hint(mod, client, result_file):
    holder_started = threading.Event()
    holder_release = threading.Event()

    async def fake_process_download(*a, progress_hook=None, cancel_event=None, **kw):
        holder_started.set()
        holder_release.wait(timeout=3)
        return result_file

    mod._process_download = fake_process_download

    r1 = client.post("/download/start", json={"url": "https://example.com/qnotitle-holder"})
    job_holder = r1.get_json()["job_id"]
    assert holder_started.wait(timeout=2)

    r2 = client.post("/download/start", json={"url": "https://example.com/qnotitle-queued"})
    job_queued = r2.get_json()["job_id"]

    queued_status = _poll_until(
        lambda d: "position" in (d.get("message") or ""),
        lambda: _status(client, job_queued),
    )
    assert queued_status["message"].startswith("Queued for download (position"), queued_status

    holder_release.set()
    for job_id in (job_holder, job_queued):
        final = _poll_until(lambda d: d["status"] == "complete", lambda jid=job_id: _status(client, jid))
        assert final["status"] == "complete"


def test_dedup_waiter_does_not_consume_a_queue_slot(mod, client, result_file):
    """Regression coverage for the core queue/dedup interaction: with cap=2,
    an owner (item X) plus a dedup waiter for that same item X must not
    occupy both slots - the waiter shares the owner's slot rather than
    claiming one of its own, leaving the second slot free for an unrelated
    item Y to start immediately."""
    mod.download_queue = DownloadQueue(max_concurrent=2)

    x_started = threading.Event()
    y_started = threading.Event()
    release_x = threading.Event()
    release_y = threading.Event()
    count = {"x": 0, "y": 0}
    lock = threading.Lock()

    async def fake_process_download(url, *a, progress_hook=None, cancel_event=None, **kw):
        if "qdedup-x" in url:
            with lock:
                count["x"] += 1
            x_started.set()
            release_x.wait(timeout=3)
        else:
            with lock:
                count["y"] += 1
            y_started.set()
            release_y.wait(timeout=3)
        return result_file

    mod._process_download = fake_process_download

    r_owner = client.post(
        "/download/start",
        json={"url": "https://example.com/qdedup-x", "video_format_id": "137"},
    )
    job_owner = r_owner.get_json()["job_id"]
    assert x_started.wait(timeout=2)

    r_waiter = client.post(
        "/download/start",
        json={"url": "https://example.com/qdedup-x", "video_format_id": "137"},
    )
    job_waiter = r_waiter.get_json()["job_id"]

    r_other = client.post(
        "/download/start",
        json={"url": "https://example.com/qdedup-y", "video_format_id": "137"},
    )
    job_other = r_other.get_json()["job_id"]

    assert y_started.wait(timeout=2), (
        "unrelated item Y never got a slot - the dedup waiter for X must have "
        "incorrectly consumed the second slot"
    )
    assert count["y"] == 1
    assert count["x"] == 1  # still just the one real download for X

    release_x.set()
    release_y.set()

    for job_id in (job_owner, job_waiter, job_other):
        final = _poll_until(lambda d: d["status"] == "complete", lambda jid=job_id: _status(client, jid))
        assert final["status"] == "complete", f"job {job_id} did not complete: {final}"
