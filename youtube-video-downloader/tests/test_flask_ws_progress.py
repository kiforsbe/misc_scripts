"""
Regression coverage for the console progress display in
youtube-video-downloader-flask-ws.py.

Background: the server shows live download progress in its own terminal
via a shared rich.progress.Progress instance. The blocking /download route
originally built that Progress instance inline; when the background-job
/download/start path was added, it wired its own one-off progress_hook
that only updated the job dict and never attached a console subscriber -
so the terminal progress bar silently stopped appearing for every download
started via /download/start (which is what the userscript actually uses).

The fix centralizes progress wiring behind _build_progress_sink, which
every entry point must call to get a ProgressSink; whether the console
subscriber gets attached is controlled by a single flag
(ENABLE_CONSOLE_PROGRESS) and a single subscriber-factory function
(_make_console_progress_subscriber). These tests fake that factory out
(so no real terminal rendering happens under pytest) and assert both
entry points invoke it - directly regression-testing the bug above.
"""

import importlib
import pathlib
import re
import time

import pytest

MODULE_NAME = "youtube-video-downloader.youtube-video-downloader-flask-ws"
SOURCE_PATH = (
    pathlib.Path(__file__).resolve().parents[1]
    / "youtube-video-downloader-flask-ws.py"
)


@pytest.fixture()
def mod():
    m = importlib.import_module(MODULE_NAME)

    m.ytdl_core.clean_youtube_url = lambda u: u
    m.FFMPEG_PATH = "C:/fake/ffmpeg.exe"

    original_process_download = m._process_download
    original_enable_console_progress = m.ENABLE_CONSOLE_PROGRESS
    original_make_console_subscriber = m._make_console_progress_subscriber

    m._active_downloads.clear()
    m._jobs.clear()
    m.ENABLE_CONSOLE_PROGRESS = True

    yield m

    m._process_download = original_process_download
    m.ENABLE_CONSOLE_PROGRESS = original_enable_console_progress
    m._make_console_progress_subscriber = original_make_console_subscriber
    m._active_downloads.clear()
    m._jobs.clear()


@pytest.fixture()
def client(mod):
    return mod.app.test_client()


class _FakeConsoleTask:
    """Stands in for the (subscriber, remove) pair _make_console_progress_subscriber
    normally returns, without touching a real rich.progress.Progress/terminal."""

    def __init__(self, description):
        self.description = description
        self.updates = []
        self.removed = False


def _install_fake_console_subscriber(mod, created_tasks):
    def _fake_make_console_progress_subscriber(description):
        task = _FakeConsoleTask(description)
        created_tasks.append(task)

        def _subscriber(percent, message=None):
            task.updates.append((percent, message))

        def _remove():
            task.removed = True

        return _subscriber, _remove

    mod._make_console_progress_subscriber = _fake_make_console_progress_subscriber


def _poll_until(predicate, get_value, timeout=5.0, interval=0.05):
    deadline = time.time() + timeout
    value = None
    while time.time() < deadline:
        value = get_value()
        if predicate(value):
            return value
        time.sleep(interval)
    return value


def test_download_start_attaches_a_console_progress_task(mod, client, tmp_path):
    """This is the exact regression: a job started via /download/start must
    get a console subscriber, matching what the job-status dict sees."""
    created_tasks = []
    _install_fake_console_subscriber(mod, created_tasks)

    result_file = tmp_path / "result.mp4"
    result_file.write_bytes(b"fake")

    async def fake_process_download(*a, progress_hook=None, cancel_event=None, **kw):
        for pct in (20, 50, 90):
            if progress_hook:
                progress_hook(pct, f"at {pct}%")
        return result_file

    mod._process_download = fake_process_download

    resp = client.post("/download/start", json={"url": "https://example.com/console1"})
    job_id = resp.get_json()["job_id"]

    _poll_until(
        lambda d: d["status"] == "complete",
        lambda: client.get(f"/download/status/{job_id}").get_json(),
    )

    assert len(created_tasks) == 1, "expected exactly one console task for this job"
    task = created_tasks[0]
    assert (20, "at 20%") in task.updates
    assert (50, "at 50%") in task.updates
    assert (90, "at 90%") in task.updates
    assert task.removed, "console task must be released once the job finishes"


def test_download_start_removes_console_task_on_job_error(mod, client):
    """Cleanup must happen on failure too, not just success, or the shared
    console display would accumulate stuck tasks over time."""
    created_tasks = []
    _install_fake_console_subscriber(mod, created_tasks)

    async def fake_process_download(*a, **kw):
        raise RuntimeError("boom")

    mod._process_download = fake_process_download

    resp = client.post("/download/start", json={"url": "https://example.com/console2"})
    job_id = resp.get_json()["job_id"]

    _poll_until(
        lambda d: d["status"] == "error",
        lambda: client.get(f"/download/status/{job_id}").get_json(),
    )

    assert len(created_tasks) == 1
    assert created_tasks[0].removed


def test_legacy_download_route_also_attaches_a_console_progress_task(mod, client, tmp_path):
    """The older blocking /download route must keep working the same way
    now that it shares _build_progress_sink with /download/start."""
    created_tasks = []
    _install_fake_console_subscriber(mod, created_tasks)

    result_file = tmp_path / "result.mp4"
    result_file.write_bytes(b"fake")

    async def fake_process_download(*a, progress_hook=None, cancel_event=None, **kw):
        if progress_hook:
            progress_hook(50, "halfway")
        return result_file

    mod._process_download = fake_process_download

    resp = client.get("/download", query_string={"url": "https://example.com/console3"})
    assert resp.status_code == 200

    assert len(created_tasks) == 1
    assert (50, "halfway") in created_tasks[0].updates
    assert created_tasks[0].removed


def test_console_progress_disabled_by_default_flag_skips_the_subscriber(mod, client, tmp_path):
    """With ENABLE_CONSOLE_PROGRESS off (the default in most test runs, and
    an operator-facing escape hatch via YTDL_ENABLE_CONSOLE_PROGRESS=0),
    the console factory must not be invoked at all."""
    created_tasks = []
    _install_fake_console_subscriber(mod, created_tasks)
    mod.ENABLE_CONSOLE_PROGRESS = False

    result_file = tmp_path / "result.mp4"
    result_file.write_bytes(b"fake")

    async def fake_process_download(*a, progress_hook=None, cancel_event=None, **kw):
        return result_file

    mod._process_download = fake_process_download

    resp = client.post("/download/start", json={"url": "https://example.com/console4"})
    job_id = resp.get_json()["job_id"]
    _poll_until(
        lambda d: d["status"] == "complete",
        lambda: client.get(f"/download/status/{job_id}").get_json(),
    )

    assert created_tasks == []


def test_no_progress_construction_outside_the_shared_factory():
    """
    Structural guardrail: the only place allowed to construct a
    rich.progress.Progress(...) instance is _get_console_progress. This is
    what a new download entry point bypassing _build_progress_sink (the
    exact mistake that caused the original regression) would violate, so
    catch it cheaply here instead of relying on someone noticing a missing
    terminal UI again.
    """
    source = SOURCE_PATH.read_text(encoding="utf-8")
    construction_sites = [m.start() for m in re.finditer(r"\bProgress\(", source)]

    # Every literal "Progress(" call site must be the one inside
    # _get_console_progress; find that function's body and confirm all
    # matches fall within it.
    func_match = re.search(
        r"def _get_console_progress\(\).*?\n\n\n", source, re.DOTALL
    )
    assert func_match, "could not locate _get_console_progress in source"
    func_start, func_end = func_match.span()

    outside_sites = [pos for pos in construction_sites if not (func_start <= pos < func_end)]
    assert outside_sites == [], (
        "found Progress(...) construction outside _get_console_progress - "
        "route new download entry points through _build_progress_sink instead"
    )
