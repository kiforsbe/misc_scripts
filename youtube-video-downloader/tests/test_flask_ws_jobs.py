"""
Tests for the async job lifecycle in youtube-video-downloader-flask-ws.py:
/download/start, /download/status/<job_id>, /download/result/<job_id>,
/download/cancel/<job_id>, in-flight download dedup, and the heartbeat-loss
reaper.

These tests replace _process_download with fakes so they exercise the
Flask/job/dedup/cancellation plumbing without doing a real download - the
actual yt-dlp cancellation mechanism is covered separately in
test_ytdl_core_cancel.py.
"""

import asyncio
import importlib
import pathlib
import tempfile
import threading
import time

import pytest

MODULE_NAME = "youtube-video-downloader.youtube-video-downloader-flask-ws"


@pytest.fixture()
def mod():
    """Imports the Flask app module fresh-ish and resets its shared mutable
    state before/after each test so tests don't leak into each other."""
    m = importlib.import_module(MODULE_NAME)

    # Isolate from real network/ffmpeg/dedup-url-cleaning behavior.
    m.ytdl_core.clean_youtube_url = lambda u: u
    original_is_video_url = m.is_video_url
    m.is_video_url = lambda u: True
    m.FFMPEG_PATH = "C:/fake/ffmpeg.exe"

    original_process_download = m._process_download
    original_auto_cancel = m.AUTO_CANCEL_ON_HEARTBEAT_LOSS
    original_heartbeat_timeout = m.JOB_HEARTBEAT_TIMEOUT_SECONDS
    original_reaper_interval = m.JOB_REAPER_INTERVAL_SECONDS
    original_job_retention = m.JOB_RETENTION_SECONDS
    original_enable_console_progress = m.ENABLE_CONSOLE_PROGRESS
    original_wait_timeout = m.DOWNLOAD_WAIT_TIMEOUT_SECONDS

    m._active_downloads.clear()
    m._jobs.clear()
    # Real console progress spins up a Rich Live terminal renderer, which is
    # unnecessary and flaky under pytest; console-subscriber wiring itself
    # is covered separately (with _make_console_progress_subscriber faked
    # out) in test_flask_ws_progress.py.
    m.ENABLE_CONSOLE_PROGRESS = False

    yield m

    m._process_download = original_process_download
    m.is_video_url = original_is_video_url
    m.AUTO_CANCEL_ON_HEARTBEAT_LOSS = original_auto_cancel
    m.JOB_HEARTBEAT_TIMEOUT_SECONDS = original_heartbeat_timeout
    m.JOB_REAPER_INTERVAL_SECONDS = original_reaper_interval
    m.JOB_RETENTION_SECONDS = original_job_retention
    m.ENABLE_CONSOLE_PROGRESS = original_enable_console_progress
    m.DOWNLOAD_WAIT_TIMEOUT_SECONDS = original_wait_timeout
    m._active_downloads.clear()
    m._jobs.clear()


@pytest.fixture()
def client(mod):
    return mod.app.test_client()


@pytest.fixture()
def result_file():
    """A real file on disk to stand in for a completed download's output,
    since /download/result sends the file from disk."""
    path = pathlib.Path(tempfile.gettempdir()) / "ytdl_pytest_result.mp4"
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


# --- Happy path: start -> poll -> result ---


def test_download_start_returns_job_id(mod, client):
    async def fake_process_download(*a, **kw):
        return pathlib.Path("unused")

    mod._process_download = fake_process_download

    resp = client.post("/download/start", json={"url": "https://example.com/v1"})
    assert resp.status_code == 202
    data = resp.get_json()
    assert "job_id" in data and data["job_id"]


def test_download_start_missing_url_is_rejected(mod, client):
    resp = client.post("/download/start", json={})
    assert resp.status_code == 400


def test_download_start_requires_ffmpeg_when_needed(mod, client):
    mod.FFMPEG_PATH = None
    resp = client.post(
        "/download/start",
        json={"url": "https://example.com/v1", "target_format": "mp3"},
    )
    assert resp.status_code == 501


def test_job_lifecycle_reaches_complete_and_serves_result(mod, client, result_file):
    percents_seen = []

    async def fake_process_download(
        url, audio_format_id, video_format_id, target_format,
        target_audio_params, target_video_params, progress_hook=None, cancel_event=None,
        **kw,
    ):
        for pct in (20, 50, 90):
            if progress_hook:
                progress_hook(pct, f"at {pct}%")
                percents_seen.append(pct)
            time.sleep(0.05)
        return result_file

    mod._process_download = fake_process_download

    resp = client.post("/download/start", json={"url": "https://example.com/v1"})
    job_id = resp.get_json()["job_id"]

    final = _poll_until(
        lambda d: d["status"] == "complete",
        lambda: client.get(f"/download/status/{job_id}").get_json(),
    )
    assert final["status"] == "complete"
    assert final["ready"] is True
    assert final["percent"] == 100
    assert percents_seen  # live progress was actually observed during the run

    result_resp = client.get(f"/download/result/{job_id}")
    assert result_resp.status_code == 200


def test_job_status_unknown_job_id_is_404(client):
    assert client.get("/download/status/does-not-exist").status_code == 404


def test_job_result_unknown_job_id_is_404(client):
    assert client.get("/download/result/does-not-exist").status_code == 404


def test_job_result_before_complete_is_409(mod, client):
    started = threading.Event()
    release = threading.Event()

    async def fake_process_download(*a, progress_hook=None, cancel_event=None, **kw):
        started.set()
        while not release.is_set():
            time.sleep(0.02)
        return pathlib.Path("unused")

    mod._process_download = fake_process_download

    resp = client.post("/download/start", json={"url": "https://example.com/v1"})
    job_id = resp.get_json()["job_id"]
    assert started.wait(timeout=2)

    assert client.get(f"/download/result/{job_id}").status_code == 409

    release.set()  # let the background thread finish so it doesn't leak into other tests


def test_job_error_is_reported_via_status(mod, client):
    async def fake_process_download(*a, **kw):
        raise RuntimeError("boom")

    mod._process_download = fake_process_download

    resp = client.post("/download/start", json={"url": "https://example.com/v1"})
    job_id = resp.get_json()["job_id"]

    final = _poll_until(
        lambda d: d["status"] == "error",
        lambda: client.get(f"/download/status/{job_id}").get_json(),
    )
    assert final["status"] == "error"
    assert "boom" in final["error"]


def test_download_error_reports_clean_message_without_traceback_spam(mod, client, caplog):
    """A yt_dlp.utils.DownloadError is already logged with a clean message
    (no traceback) inside _process_download - _background_job_runner must
    not dump a second, redundant full stack trace for this already-
    diagnosed failure type, and the job's error message should have the
    "ERROR: " prefix stripped rather than being shown to the client raw."""
    async def fake_process_download(*a, **kw):
        raise mod.yt_dlp.utils.DownloadError(
            "ERROR: unable to download video data: HTTP Error 403: Forbidden"
        )

    mod._process_download = fake_process_download

    with caplog.at_level("ERROR"):
        resp = client.post("/download/start", json={"url": "https://example.com/v-403"})
        job_id = resp.get_json()["job_id"]

        final = _poll_until(
            lambda d: d["status"] == "error",
            lambda: client.get(f"/download/status/{job_id}").get_json(),
        )

    assert final["status"] == "error"
    assert final["error"] == "unable to download video data: HTTP Error 403: Forbidden"

    job_failed_records = [r for r in caplog.records if f"Job {job_id} failed" in r.message]
    assert job_failed_records, "expected a 'Job ... failed' log record"
    assert all(r.exc_info is None for r in job_failed_records), (
        "known/already-diagnosed error types must not dump a redundant traceback"
    )


# --- Explicit cancel ---


def test_explicit_cancel_stops_the_underlying_download(mod, client):
    cancel_seen = threading.Event()

    async def fake_process_download(*a, progress_hook=None, cancel_event=None, **kw):
        for _ in range(100):
            if cancel_event is not None and cancel_event.is_set():
                cancel_seen.set()
                raise asyncio.CancelledError("cancelled mid-flight")
            time.sleep(0.02)
        raise AssertionError("cancel_event was never observed")

    mod._process_download = fake_process_download

    resp = client.post("/download/start", json={"url": "https://example.com/v2"})
    job_id = resp.get_json()["job_id"]
    time.sleep(0.1)  # let it get into the loop

    cancel_resp = client.post(f"/download/cancel/{job_id}")
    assert cancel_resp.status_code == 200

    assert cancel_seen.wait(timeout=3), "underlying download was never actually cancelled"

    final = _poll_until(
        lambda d: d["status"] == "cancelled",
        lambda: client.get(f"/download/status/{job_id}").get_json(),
    )
    assert final["status"] == "cancelled"


def test_cancel_unknown_job_id_is_404(client):
    assert client.post("/download/cancel/does-not-exist").status_code == 404


def test_cancel_before_background_thread_registers_still_cancels(mod, client):
    """Covers the race where cancel arrives before the owner's job_id has
    even been added to the dedup entry's job_ids set: _run_download_deduped
    checks the job's cancel_requested flag and sets cancel_event immediately
    once it creates the entry, so the download still gets cancelled."""
    cancel_seen = threading.Event()

    async def fake_process_download(*a, progress_hook=None, cancel_event=None, **kw):
        for _ in range(300):
            if cancel_event is not None and cancel_event.is_set():
                cancel_seen.set()
                raise asyncio.CancelledError("cancelled")
            time.sleep(0.01)
        raise AssertionError("cancel_event was never observed")

    mod._process_download = fake_process_download

    resp = client.post("/download/start", json={"url": "https://example.com/v-race"})
    job_id = resp.get_json()["job_id"]
    # Cancel immediately, without waiting for the background thread to run at all.
    cancel_resp = client.post(f"/download/cancel/{job_id}")
    assert cancel_resp.status_code == 200

    assert cancel_seen.wait(timeout=3), "cancel-before-registration race was not handled"


# --- Dedup + cancel interaction: one waiter cancelling must not kill it for another ---


def test_cancelling_one_waiter_does_not_cancel_shared_download_for_another(mod, client, result_file):
    started = threading.Event()
    finish = threading.Event()
    cancel_seen = threading.Event()

    async def fake_process_download(*a, progress_hook=None, cancel_event=None, **kw):
        started.set()
        for _ in range(200):
            if cancel_event is not None and cancel_event.is_set():
                cancel_seen.set()
                raise asyncio.CancelledError("cancelled")
            if finish.is_set():
                return result_file
            time.sleep(0.02)
        raise AssertionError("never finished or cancelled")

    mod._process_download = fake_process_download

    r1 = client.post(
        "/download/start",
        json={"url": "https://example.com/v3", "video_format_id": "137"},
    )
    job_owner = r1.get_json()["job_id"]
    assert started.wait(timeout=2)

    r2 = client.post(
        "/download/start",
        json={"url": "https://example.com/v3", "video_format_id": "137"},
    )
    job_waiter = r2.get_json()["job_id"]
    time.sleep(0.1)

    cancel_resp = client.post(f"/download/cancel/{job_owner}")
    assert cancel_resp.status_code == 200
    time.sleep(0.3)
    assert not cancel_seen.is_set(), "download was cancelled even though a waiter still wanted it"

    owner_status = client.get(f"/download/status/{job_owner}").get_json()
    assert owner_status["status"] == "cancelled"

    finish.set()
    waiter_final = _poll_until(
        lambda d: d["status"] == "complete",
        lambda: client.get(f"/download/status/{job_waiter}").get_json(),
    )
    assert waiter_final["status"] == "complete"

    # The cancelled job's status must not flip back to complete just because
    # the shared download succeeded for the other job.
    owner_status_after = client.get(f"/download/status/{job_owner}").get_json()
    assert owner_status_after["status"] == "cancelled"


def test_dedup_only_runs_underlying_download_once_for_concurrent_identical_requests(mod, client, result_file):
    call_count = {"n": 0}
    lock = threading.Lock()

    async def fake_process_download(*a, progress_hook=None, cancel_event=None, **kw):
        with lock:
            call_count["n"] += 1
        time.sleep(0.2)
        return result_file

    mod._process_download = fake_process_download

    job_ids = []
    for _ in range(4):
        resp = client.post(
            "/download/start",
            json={"url": "https://example.com/v-dedup", "video_format_id": "137"},
        )
        job_ids.append(resp.get_json()["job_id"])

    for job_id in job_ids:
        _poll_until(
            lambda d: d["status"] == "complete",
            lambda jid=job_id: client.get(f"/download/status/{jid}").get_json(),
        )

    assert call_count["n"] == 1, f"expected exactly 1 real download, got {call_count['n']}"


def test_waiter_timeout_retries_via_dedup_instead_of_starting_a_duplicate_download(mod, client, result_file):
    """Regression test: a waiter that times out waiting for the owner's
    download used to fall back to calling _process_download directly,
    bypassing dedup registration entirely and starting a second, unmanaged,
    uncancellable download of the same item. It must instead retry through
    _run_download_deduped - repeatedly re-waiting on the still-running
    owner - and never launch a second concurrent _process_download call for
    the same item."""
    mod.DOWNLOAD_WAIT_TIMEOUT_SECONDS = 0.05

    call_count = {"n": 0}
    lock = threading.Lock()
    started = threading.Event()
    release = threading.Event()

    async def fake_process_download(*a, progress_hook=None, cancel_event=None, **kw):
        with lock:
            call_count["n"] += 1
        started.set()
        while not release.is_set():
            time.sleep(0.02)
        return result_file

    mod._process_download = fake_process_download

    r1 = client.post(
        "/download/start",
        json={"url": "https://example.com/v-timeout", "video_format_id": "137"},
    )
    job_owner = r1.get_json()["job_id"]
    assert started.wait(timeout=2)

    r2 = client.post(
        "/download/start",
        json={"url": "https://example.com/v-timeout", "video_format_id": "137"},
    )
    job_waiter = r2.get_json()["job_id"]

    # The owner's download runs far longer than the (tiny) wait timeout, so
    # the waiter is guaranteed to time out and retry - several times over -
    # while the owner is still in flight.
    time.sleep(0.3)
    assert call_count["n"] == 1, "waiter's timeout retry started a duplicate download"

    release.set()

    for job_id in (job_owner, job_waiter):
        final = _poll_until(
            lambda d: d["status"] == "complete",
            lambda jid=job_id: client.get(f"/download/status/{jid}").get_json(),
        )
        assert final["status"] == "complete", f"job {job_id} did not complete: {final}"

    assert call_count["n"] == 1, f"expected exactly 1 real download, got {call_count['n']}"


# --- Heartbeat-loss reaper ---


def test_heartbeat_loss_does_not_cancel_by_default(mod, client, result_file):
    mod.JOB_HEARTBEAT_TIMEOUT_SECONDS = 0.15
    mod.JOB_REAPER_INTERVAL_SECONDS = 0.05
    assert mod.AUTO_CANCEL_ON_HEARTBEAT_LOSS is False

    reaper = threading.Thread(target=mod._job_reaper_loop, daemon=True)
    reaper.start()

    cancel_seen = threading.Event()
    finish = threading.Event()

    async def fake_process_download(*a, progress_hook=None, cancel_event=None, **kw):
        for _ in range(200):
            if cancel_event is not None and cancel_event.is_set():
                cancel_seen.set()
                raise asyncio.CancelledError("cancelled")
            if finish.is_set():
                return result_file
            time.sleep(0.02)
        raise AssertionError("never finished or cancelled")

    mod._process_download = fake_process_download

    resp = client.post("/download/start", json={"url": "https://example.com/hb1"})
    job_id = resp.get_json()["job_id"]
    # Deliberately do not poll status - simulate a dropped client.
    time.sleep(0.6)

    assert not cancel_seen.is_set(), "heartbeat loss cancelled the download despite auto-cancel being off"
    with mod._jobs_lock:
        assert mod._jobs[job_id]["status"] == "dropped"

    finish.set()
    time.sleep(0.1)


def test_heartbeat_loss_cancels_when_auto_cancel_enabled(mod, client):
    mod.JOB_HEARTBEAT_TIMEOUT_SECONDS = 0.15
    mod.JOB_REAPER_INTERVAL_SECONDS = 0.05
    mod.AUTO_CANCEL_ON_HEARTBEAT_LOSS = True

    reaper = threading.Thread(target=mod._job_reaper_loop, daemon=True)
    reaper.start()

    cancel_seen = threading.Event()

    async def fake_process_download(*a, progress_hook=None, cancel_event=None, **kw):
        for _ in range(200):
            if cancel_event is not None and cancel_event.is_set():
                cancel_seen.set()
                raise asyncio.CancelledError("cancelled")
            time.sleep(0.02)
        raise AssertionError("never cancelled")

    mod._process_download = fake_process_download

    resp = client.post("/download/start", json={"url": "https://example.com/hb2"})
    # Deliberately do not poll status - simulate a dropped client.
    assert cancel_seen.wait(timeout=3), "auto-cancel is on but heartbeat loss did not cancel the download"


# --- Orphaned completed-file cleanup ---


def test_forgotten_completed_job_still_queues_its_file_for_deletion(mod, client, result_file):
    """A job whose client never calls /download/result (e.g. the tab was
    closed right after the download finished, well after heartbeat loss
    already marked it "dropped") must not leak its output file forever -
    once the reaper forgets the job (JOB_RETENTION_SECONDS after it
    actually completed), the file must still end up in the delete queue
    even though nobody ever fetched it."""
    mod.JOB_HEARTBEAT_TIMEOUT_SECONDS = 0.15
    mod.JOB_REAPER_INTERVAL_SECONDS = 0.05
    mod.JOB_RETENTION_SECONDS = 0.2

    reaper = threading.Thread(target=mod._job_reaper_loop, daemon=True)
    reaper.start()

    async def fake_process_download(*a, progress_hook=None, cancel_event=None, **kw):
        return result_file

    mod._process_download = fake_process_download

    resp = client.post("/download/start", json={"url": "https://example.com/leak1"})
    job_id = resp.get_json()["job_id"]

    final = _poll_until(
        lambda d: d["status"] == "complete",
        lambda: client.get(f"/download/status/{job_id}").get_json(),
    )
    assert final["status"] == "complete"
    # Deliberately never call /download/result - simulate an abandoned tab.

    deadline = time.time() + 3
    while time.time() < deadline and job_id in mod._jobs:
        time.sleep(0.05)
    assert job_id not in mod._jobs, "job was never forgotten by the reaper"

    with mod.queue_lock:
        queued_paths = [fi["path"] for fi in mod.delete_queue]
    assert str(result_file) in queued_paths, (
        "completed file from a forgotten-but-never-fetched job was never queued for deletion"
    )
