import os
import logging
import threading
import time
import asyncio
import pathlib
import sys
import uuid
import re
from urllib.parse import urlparse, parse_qs
from flask import Flask, request, send_file, jsonify, render_template_string
import yt_dlp  # For exceptions
import yt_dlp.utils  # For exceptions
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    TimeRemainingColumn,
)

# rich.progress is required; assume installed and available

# --- Add ytdl_helper to Python path ---
try:
    from .ytdl_helper import core as ytdl_core
    from .ytdl_helper import models as ytdl_models
    from .ytdl_helper.utils import check_ffmpeg
    from .ytdl_helper.progress_sink import ProgressSink
    from .ytdl_helper.download_queue import DownloadQueue
except ImportError:
    current_dir = pathlib.Path(__file__).parent
    ytdl_helper_path = current_dir / "ytdl_helper"
    if ytdl_helper_path.is_dir():
        sys.path.insert(0, str(current_dir))
        try:
            from ytdl_helper import core as ytdl_core
            from ytdl_helper import models as ytdl_models
            from ytdl_helper.utils import check_ffmpeg
            from ytdl_helper.progress_sink import ProgressSink
            from ytdl_helper.download_queue import DownloadQueue
        except ImportError as e:
            print(
                f"ERROR: Failed to import ytdl_helper from {current_dir}. Error: {e}",
                file=sys.stderr,
            )
            sys.exit(1)
    else:
        # If not found locally, try assuming it's installed as a package
        try:
            from ytdl_helper import core as ytdl_core
            from ytdl_helper import models as ytdl_models
            from ytdl_helper.utils import check_ffmpeg
            from ytdl_helper.progress_sink import ProgressSink
            from ytdl_helper.download_queue import DownloadQueue
        except ImportError:
            print(
                "ERROR: Could not find ytdl_helper locally or as an installed package.",
                file=sys.stderr,
            )
            print(
                "Ensure 'ytdl_helper' directory exists relative to the script or is installed.",
                file=sys.stderr,
            )
            sys.exit(1)
# --- End ytdl_helper import ---


# --- Flask App and Logging Setup ---
app = Flask(__name__)

# Allow Tampermonkey (https) to call local Flask (http) by adding CORS headers.
# Keep this permissive for local-only usage.
CORS_ALLOW_ORIGIN = "*"
CORS_ALLOW_HEADERS = "Content-Type, Authorization, X-Requested-With"
CORS_EXPOSE_HEADERS = "Content-Disposition, Content-Length, Content-Type"
CORS_ALLOW_METHODS = "GET, POST, OPTIONS"


@app.after_request
def add_cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = CORS_ALLOW_ORIGIN
    response.headers["Access-Control-Allow-Methods"] = CORS_ALLOW_METHODS
    response.headers["Access-Control-Allow-Headers"] = CORS_ALLOW_HEADERS
    response.headers["Access-Control-Expose-Headers"] = CORS_EXPOSE_HEADERS
    return response


def is_video_url(url: str) -> bool:
    """Return True if the URL points to an individual YouTube video (watch, youtu.be, embed)."""
    try:
        parsed = urlparse(url)
        host = (parsed.hostname or "").lower()
        if host.endswith("youtube.com") or host.endswith("youtube-nocookie.com"):
            qs = parse_qs(parsed.query)
            if qs.get("v"):
                return True
            if parsed.path.startswith("/embed/"):
                return True
            return False
        if host == "youtu.be":
            pid = parsed.path.lstrip("/")
            return bool(pid)
        return False
    except Exception:
        return False

# Logger will be configured in __main__
log = logging.getLogger(__name__)  # Use a specific logger for the app

# --- Temporary Directory Management ---
TEMP_DIR = ".temp"  # Use a distinct name
DELETE_DELAY = 2*60 # seconds until files are deleted (2 minutes)
os.makedirs(TEMP_DIR, exist_ok=True)
delete_queue = []
queue_lock = threading.Lock()  # For thread safety


def delayed_delete():
    """Periodically checks the queue and deletes old files."""
    log.info("Background deletion thread started.")
    while True:
        try:
            current_time = time.time()
            files_to_delete = []

            with queue_lock:
                # Iterate backwards to safely remove items while iterating
                for i in range(len(delete_queue) - 1, -1, -1):
                    file_info = delete_queue[i]
                    if current_time - file_info["time"] > DELETE_DELAY:
                        files_to_delete.append(file_info)
                        del delete_queue[i]  # Remove from queue

            # Perform deletions outside the lock
            for file_info in files_to_delete:
                try:
                    file_path = pathlib.Path(file_info["path"])
                    if file_path.exists():
                        os.remove(file_path)
                        log.info(f"Deleted temporary file: {file_path}")
                    else:
                        log.warning(
                            f"Attempted to delete non-existent file: {file_path}"
                        )
                except OSError as e:
                    log.error(f"Error deleting file {file_info['path']}: {e}")
                except Exception as e:
                    log.error(
                        f"Unexpected error during file deletion {file_info['path']}: {e}",
                        exc_info=True,
                    )

            # No progress-store cleanup required in this simplified server.

        except Exception as e:
            log.error(f"Error in delayed_delete loop: {e}", exc_info=True)
            # Avoid busy-looping on error
            time.sleep(60)
        finally:
            # Check every 60 seconds regardless of deletions
            time.sleep(60)


# --- In-Flight Download Deduplication ---
# Prevents a client retry (e.g. after its own request timed out while the
# server kept downloading) from kicking off a duplicate download of a video
# that is already being downloaded. Concurrent requests for the same item
# (same URL + same requested formats/target) wait for the in-progress
# download to finish and then reuse its result via the existing-file check
# in download_item(), instead of starting a second parallel download.
DOWNLOAD_WAIT_TIMEOUT_SECONDS = float(
    os.environ.get("YTDL_DOWNLOAD_WAIT_TIMEOUT_SECONDS", "1800")
)
_active_downloads: dict = {}
_active_downloads_lock = threading.Lock()


# --- Download Concurrency Queue ---
# Caps how many real downloads (i.e. _process_download calls) run at once,
# independent of the dedup system above. Dedup collapses duplicate requests
# for the same item into one; this queue limits how many distinct items may
# be downloading simultaneously. Only a dedup "owner" ever calls
# download_queue.acquire()/release() - a "waiter" shares the owner's
# already-acquired slot instead of claiming one of its own.
MAX_CONCURRENT_DOWNLOADS = int(os.environ.get("YTDL_MAX_CONCURRENT_DOWNLOADS", "1"))
download_queue = DownloadQueue(max_concurrent=MAX_CONCURRENT_DOWNLOADS)


# --- Async Job Tracking (for /download/start, /download/status, /download/cancel) ---
# A "job" is one client's request for a download; several jobs can point at the
# same in-flight download (see dedup above) when multiple clients ask for the
# same item at the same time. Each job tracks its own lifecycle (queued ->
# downloading -> complete/error/cancelled/dropped) plus a heartbeat timestamp
# refreshed by /download/status polls, so an abandoned job (client crashed,
# tab closed without a clean unload, network lost) can be detected even
# without an explicit cancel request. A job stays "queued" both before it's
# picked up at all and while its owner is waiting on a download_queue slot
# (see above) - it only becomes "downloading" once that slot is granted.
JOB_HEARTBEAT_TIMEOUT_SECONDS = float(
    os.environ.get("YTDL_JOB_HEARTBEAT_TIMEOUT_SECONDS", "15")
)
JOB_REAPER_INTERVAL_SECONDS = float(
    os.environ.get("YTDL_JOB_REAPER_INTERVAL_SECONDS", "5")
)
JOB_RETENTION_SECONDS = float(
    os.environ.get("YTDL_JOB_RETENTION_SECONDS", "600")
)
# Off by default: a lost heartbeat alone does not cancel the underlying
# download (it might resume polling, or another tab may still want the
# result). An explicit /download/cancel request always cancels regardless of
# this flag. Set YTDL_AUTO_CANCEL_ON_HEARTBEAT_LOSS=1 to also auto-cancel on
# heartbeat loss.
AUTO_CANCEL_ON_HEARTBEAT_LOSS = os.environ.get(
    "YTDL_AUTO_CANCEL_ON_HEARTBEAT_LOSS", ""
).strip().lower() in ("1", "true", "yes", "on")
_jobs: dict = {}
_jobs_lock = threading.Lock()


# --- Progress reporting fan-out ---
# Every download entry point (the blocking /download route and background
# /download/start jobs) reports progress through one ProgressSink built by
# _build_progress_sink, instead of each hand-rolling its own wiring - a
# background job runner previously used its own bespoke progress_hook that
# only updated the job dict and silently never attached the console display
# below, so the terminal progress bar stopped appearing for any download
# started via /download/start. Routing every entry point through this one
# factory means a new one can no longer "forget" a subscriber the same way.
ENABLE_CONSOLE_PROGRESS = os.environ.get(
    "YTDL_ENABLE_CONSOLE_PROGRESS", "1"
).strip().lower() in ("1", "true", "yes", "on")

_console_progress_lock = threading.Lock()
_console_progress: Progress | None = None
_console_progress_active_tasks = 0


def _get_console_progress() -> Progress:
    """
    Returns the single process-wide Progress/Live instance used by every
    download's console subscriber. Rich's Live renderer isn't safe to run
    multiple overlapping instances of in one terminal, so concurrent
    downloads share this one instance, each as its own task within it -
    this is the only place a Progress(...) instance should be constructed.
    """
    global _console_progress
    if _console_progress is None:
        _console_progress = Progress(
            SpinnerColumn(),
            TextColumn("{task.description}"),
            BarColumn(bar_width=None),
            TextColumn("{task.completed:>3.0f}%"),
            TimeRemainingColumn(),
        )
    return _console_progress


def _make_console_progress_subscriber(description: str):
    """
    Adds a task to the shared console Progress display for one download.
    Returns (subscriber, remove) - remove() must be called exactly once,
    when the download reaches a terminal state, to release the task and
    stop the shared renderer once nothing else is using it. Split out as
    its own function (rather than inlined into _build_progress_sink) so
    tests can monkeypatch it with a fake instead of exercising real
    terminal rendering.
    """
    global _console_progress_active_tasks
    with _console_progress_lock:
        progress = _get_console_progress()
        if _console_progress_active_tasks == 0:
            progress.start()
        task_id = progress.add_task(description, total=100)
        _console_progress_active_tasks += 1

    def _subscriber(percent, message=None):
        with _console_progress_lock:
            if message:
                progress.update(task_id, completed=percent, description=message)
            else:
                progress.update(task_id, completed=percent)

    def _remove():
        global _console_progress_active_tasks
        with _console_progress_lock:
            progress.remove_task(task_id)
            _console_progress_active_tasks = max(0, _console_progress_active_tasks - 1)
            if _console_progress_active_tasks == 0:
                progress.stop()

    return _subscriber, _remove


def _build_progress_sink(description: str, job_id: str | None = None):
    """
    Builds the ProgressSink shared by every download entry point: a
    job-dict subscriber (only if job_id is given), a console subscriber
    (unless disabled via YTDL_ENABLE_CONSOLE_PROGRESS=0), and a debug-log
    subscriber. Returns (sink, cleanup) - cleanup() must be called exactly
    once the download reaches a terminal state (success, error, or cancel).
    """
    sink = ProgressSink()
    cleanup_fns = []

    if job_id is not None:
        def _job_subscriber(percent, message=None):
            with _jobs_lock:
                job = _jobs.get(job_id)
                if job is not None:
                    job["percent"] = percent
                    if message:
                        job["message"] = message
                    job["last_seen"] = time.time()

        sink.add(_job_subscriber)

    if ENABLE_CONSOLE_PROGRESS:
        console_subscriber, remove_console_task = _make_console_progress_subscriber(description)
        sink.add(console_subscriber)
        cleanup_fns.append(remove_console_task)

    def _log_subscriber(percent, message=None):
        if message:
            log.debug(f"Progress {percent:.0f}% - {message}")
        else:
            log.debug(f"Progress {percent:.0f}%")

    sink.add(_log_subscriber)

    def cleanup():
        for fn in cleanup_fns:
            try:
                fn()
            except Exception:
                log.exception("Error cleaning up progress sink")

    return sink, cleanup


def _maybe_warm_up_genre_classifier():
    """
    If music style recognition is enabled, starts a background thread that
    eagerly loads its ML model so the first real download doesn't pay that
    (potentially minutes-long, first-run) cost inline. Returns the started
    thread, or None if genre recognition isn't enabled. Split out of the
    __main__ block (rather than inlined there) so it's directly callable
    from tests and reusable if another entry point needs the same warm-up.
    """
    if not ytdl_core.ENABLE_CUSTOM_GENRE_PP:
        return None
    thread = threading.Thread(
        target=ytdl_core.warm_up_genre_classifier_if_enabled,
        name="GenreClassifierWarmup",
        daemon=True,
    )
    thread.start()
    return thread


# --- Check for FFmpeg ---
FFMPEG_PATH = None
try:
    FFMPEG_PATH = check_ffmpeg()
    if not FFMPEG_PATH:
        log.warning(
            "FFmpeg executable not found or not configured. Downloads requiring merging or format conversion might fail."
        )
    else:
        log.info(f"FFmpeg found at: {FFMPEG_PATH}")
except Exception as e:
    log.error(f"Error checking for FFmpeg: {e}", exc_info=True)
    log.warning("Proceeding without FFmpeg check. Merging/conversion may fail.")


# --- Async Download Logic ---
async def _process_download(
    url: str,
    audio_format_id: str | None,
    video_format_id: str | None,
    target_format: str | None,
    target_audio_params: str | None,
    target_video_params: str | None,
    progress_hook=None,
    cancel_event: threading.Event | None = None,
) -> pathlib.Path:
    """
    Fetches info, selects formats, downloads the item into TEMP_DIR,
    and returns the final path.
    """
    temp_dir_path = pathlib.Path(TEMP_DIR)
    item: ytdl_models.DownloadItem | None = None  # Initialize item

    try:
        log.info(f"Fetching info for URL: {url}")
        item = await ytdl_core.fetch_info(url)
        log.info(f"Successfully fetched info for '{item.title}'")
        if progress_hook:
            progress_hook(20, f"{item.title} - info fetched")


        selected_audio_id = audio_format_id
        selected_video_id = video_format_id

        # --- Format Selection Logic ---
        if not selected_audio_id and not selected_video_id:
            log.info(
                "No specific format IDs provided, selecting best available by default."
            )
            # Default: Select best video and best audio if available
            if item.video_formats:
                best_video = item.video_formats[0]
                selected_video_id = best_video.format_id
                log.info(
                    f"Selected best video format: {selected_video_id} ({best_video.width}x{best_video.height}@{best_video.fps}fps, {best_video.ext}, vcodec:{best_video.vcodec}, acodec:{best_video.acodec})"
                )
            else:
                log.info("No video formats found.")

            if item.audio_formats:
                best_audio = item.audio_formats[0]
                selected_audio_id = best_audio.format_id
                log.info(
                    f"Selected best audio format: {selected_audio_id} ({best_audio.abr}k, {best_audio.ext}, acodec:{best_audio.acodec})"
                )
            else:
                log.info("No audio-only formats found.")

            # If we selected a video format that *already* has good audio,
            # we might not need a separate audio stream. yt-dlp handles this,
            # but we log what we initially selected.
            if selected_video_id and not selected_audio_id:
                log.info(
                    "Only video format selected (best available). It might contain audio."
                )
            elif not selected_video_id and selected_audio_id:
                log.info("Only audio format selected (best available).")
            elif not selected_video_id and not selected_audio_id:
                # This case should be rare if fetch_info succeeded but means no usable formats
                raise ValueError(
                    "No downloadable video or audio formats found for this URL."
                )

            # Report selected formats to progress hook so client sees this step
            if progress_hook:
                progress_hook(30, f"{item.title} - formats selected (V:{selected_video_id}, A:{selected_audio_id})")

        else:  # Specific format IDs were provided
            log.info(
                f"Using provided format IDs - Audio: {selected_audio_id}, Video: {selected_video_id}"
            )
            # Validate provided IDs exist in the fetched lists
            if selected_audio_id and not any(
                f.format_id == selected_audio_id for f in item.audio_formats
            ):
                valid_ids = [f.format_id for f in item.audio_formats]
                raise ValueError(
                    f"Provided audio_format_id '{selected_audio_id}' not found. Available audio-only: {valid_ids}"
                )
            if selected_video_id and not any(
                f.format_id == selected_video_id for f in item.video_formats
            ):
                valid_ids = [f.format_id for f in item.video_formats]
                raise ValueError(
                    f"Provided video_format_id '{selected_video_id}' not found. Available video: {valid_ids}"
                )

        # Assign selected formats to the item for download_item to use
        item.selected_audio_format_id = selected_audio_id
        item.selected_video_format_id = selected_video_id

        log.info(
            f"Starting download for '{item.title}' (V:{selected_video_id}, A:{selected_audio_id}, Target:{target_format}, AudioParams:{target_audio_params}, VideoParams:{target_video_params})"
        )

        # Define simple callbacks for logging within the async task
        def status_callback(cb_item, status, error):
            log_msg = f"Item '{cb_item.title}': Status -> {status}"
            if error:
                log_msg += f" (Error: {error})"
            log.info(log_msg)
            # Translate certain status strings into progress updates
            try:
                if progress_hook:
                    if status.lower() in ("downloading", "download"):
                        progress_hook(40, f"{cb_item.title} - downloading")
                    elif status.lower() in ("processing", "post-processing", "merging"):
                        progress_hook(88, f"{cb_item.title} - processing")
                    elif status.lower() in ("finished", "completed"):
                        progress_hook(95, f"{cb_item.title} - finished")
                    elif status.lower() in ("error",):
                        progress_hook(entry.get('percent', 0) if 'entry' in globals() else 0, f"{cb_item.title} - error: {error}")
            except Exception:
                pass

        # Define the progress callback function first
        def progress_callback(cb_item, progress_data):
            if progress_data["status"] == "downloading":
                percent = progress_data.get("percentage")
                if percent is not None:
                    if progress_hook:
                        progress_hook(
                            20 + (percent * 0.7),
                            f"{cb_item.title} - downloading {percent:.1f}%",
                        )
                    # Log progress every ~10%
                    # Use getattr to safely access the attribute, providing a default
                    last_logged = getattr(progress_callback, 'last_logged_percent', -10)
                    if percent >= last_logged + 10:
                        log.info(f"Item '{cb_item.title}': Downloading {percent:.1f}%")
                        # Update the attribute on the function object
                        progress_callback.last_logged_percent = percent
            elif progress_data["status"] == "finished":
                # Reset for next potential download stage (e.g., post-processing)
                # Set the attribute directly here
                progress_callback.last_logged_percent = -10
                log.info(
                    f"Item '{cb_item.title}': Download part finished, may start processing."
                )
                if progress_hook:
                    progress_hook(90, f"{cb_item.title} - processing")

        # Now initialize the attribute on the defined function object
        progress_callback.last_logged_percent = -10

        # --- Trigger the download ---
        if progress_hook:
            progress_hook(35, f"{item.title} - starting download")

        await ytdl_core.download_item(
            item,
            output_dir=temp_dir_path,  # Download directly into our temp dir
            target_format=target_format,
            target_audio_params=target_audio_params,
            target_video_params=target_video_params,
            status_callback=status_callback,
            progress_callback=progress_callback,
            cancel_event=cancel_event,
        )

        # --- Verify result ---
        # "Skipped" with a final_filepath that exists means download_item found the
        # target already on disk (e.g. a retry after this client's previous request
        # timed out while the server kept downloading) - that is success, not failure.
        if (
            item.status in ("Complete", "Skipped")
            and item.final_filepath
            and item.final_filepath.exists()
        ):
            if item.status == "Skipped":
                log.info(
                    f"Reusing already-downloaded file for '{item.title}': {item.final_filepath}"
                )
            else:
                log.info(
                    f"Download and processing complete. Final file: {item.final_filepath}"
                )
            return item.final_filepath
        else:
            # This case should ideally be handled by exceptions within download_item,
            # but catch it here just in case.
            error_msg = (
                item.error
                or "Download did not complete successfully, but no specific error recorded."
            )
            log.error(
                f"Download failed for '{item.title}'. Final Status: {item.status}. Error: {error_msg}"
            )
            # Use a more specific exception if possible based on status
            if item.status == "Cancelled":
                raise asyncio.CancelledError(error_msg)
            else:
                raise RuntimeError(f"Download failed: {error_msg}")

    except yt_dlp.utils.DownloadCancelled as e:
        log.info(f"Download cancelled for {url}: {e}")
        if item:
            item.status = "Cancelled"
            item.error = str(e)
        raise asyncio.CancelledError(str(e)) from e
    except (
        yt_dlp.utils.DownloadError,
        ValueError,
        FileNotFoundError,
        RuntimeError,
        asyncio.CancelledError,
    ) as e:
        # This is just re-raised to whichever caller invoked
        # _run_download_deduped (the /download route or
        # _background_job_runner), which each log the failure exactly once
        # at the point it's finally handled - log at DEBUG here so a
        # failure isn't logged multiple times at ERROR level.
        log.debug(f"Error processing download for {url}: {e}")
        if item:
            item.status = "Error"
            item.error = str(e)
        raise
    except Exception as e:
        # Same reasoning as above - the caller's own catch-all logs
        # unexpected exceptions (with a traceback) exactly once.
        log.debug(f"Unexpected error processing download for {url}: {e}", exc_info=True)
        if item:
            item.status = "Error"
            item.error = f"Unexpected: {str(e)}"
        raise RuntimeError(
            f"An unexpected server error occurred during download processing: {e}"
        )


def _run_download_deduped(
    url: str,
    audio_format_id: str | None,
    video_format_id: str | None,
    target_format: str | None,
    target_audio_params: str | None,
    target_video_params: str | None,
    progress_hook=None,
    job_id: str | None = None,
    on_downloading_started=None,
) -> pathlib.Path:
    """
    Runs _process_download for this request, but if an identical request
    (same URL and same requested formats/target) is already downloading on
    another thread, waits for it to finish and reuses its result instead of
    starting a duplicate download. Only the first ("owner") caller for a
    given dedup key actually invokes _process_download; every other caller
    that arrives while it is in flight blocks on the owner's completion and
    then returns (or re-raises) exactly what the owner produced.

    The owner must also claim a slot from the process-wide download_queue
    before it may actually call _process_download, so at most
    YTDL_MAX_CONCURRENT_DOWNLOADS real downloads run at once - a waiter
    never claims a slot of its own, since it isn't starting a new download.

    If job_id is given, it is registered against the underlying download so
    that a later /download/cancel request or a lost heartbeat can ask for
    cancellation (see _deregister_job) - the download is only actually
    cancelled once no registered job_id is left wanting the result.

    on_downloading_started, if given, is called once this call's underlying
    download is actually running: immediately for a waiter (it shares an
    already-in-progress or about-to-run download), or for the owner only
    after it has claimed a download_queue slot - which may be well after
    this function was entered if the queue was full.
    """
    dedup_key = (
        ytdl_core.clean_youtube_url(url),
        audio_format_id,
        video_format_id,
        target_format,
        target_audio_params,
        target_video_params,
    )

    cancel_requested_early = False
    if job_id is not None:
        with _jobs_lock:
            job = _jobs.get(job_id)
            if job is not None:
                cancel_requested_early = bool(job.get("cancel_requested"))

    with _active_downloads_lock:
        entry = _active_downloads.get(dedup_key)
        is_owner = entry is None
        if is_owner:
            entry = {
                "event": threading.Event(),
                "result": None,
                "error": None,
                "cancel_event": threading.Event(),
                "job_ids": set(),
                "percent": 0.0,
                "message": "",
            }
            _active_downloads[dedup_key] = entry
        if job_id is not None:
            if cancel_requested_early:
                # Cancelled before this job's download work even started
                # (e.g. the user clicked Cancel within milliseconds of
                # starting). Don't register it as interested; if it would
                # have been the sole owner, cancel immediately.
                if is_owner and not entry["job_ids"]:
                    entry["cancel_event"].set()
            else:
                entry["job_ids"].add(job_id)

    def _tracked_progress_hook(percent, message=None):
        with _active_downloads_lock:
            entry["percent"] = percent
            if message:
                entry["message"] = message
        if progress_hook:
            progress_hook(percent, message)

    if not is_owner:
        log.info(
            f"Download already in progress for {dedup_key}; "
            "waiting for it to finish instead of starting a duplicate."
        )
        if on_downloading_started:
            on_downloading_started()
        if progress_hook:
            progress_hook(10, "Waiting for an in-progress download of this item to finish")
        if entry["event"].wait(timeout=DOWNLOAD_WAIT_TIMEOUT_SECONDS):
            if entry["error"] is not None:
                raise entry["error"]
            return entry["result"]
        log.warning(
            f"Timed out waiting for in-progress download {dedup_key}; "
            "retrying dedup registration instead of starting an "
            "unmanaged duplicate download."
        )
        # Re-enter from the top rather than calling _process_download
        # directly here. A raw call would bypass the _active_downloads
        # entry entirely: no cancel_event (this retry could never be
        # cancelled), no job_id bookkeeping (leaving _deregister_job's
        # view of this job's dedup entry incoherent), and no protection
        # against running a second, duplicate yt-dlp download of the same
        # item concurrently with the still-running owner. Recursing goes
        # through the same registration path again, either becoming the
        # new owner (if the original just finished) or waiting again (if
        # it's still running) - both outcomes keep this call fully
        # participating in dedup/cancel/job tracking. job_id re-adds are
        # idempotent (entry["job_ids"] is a set), so this is safe even if
        # the original entry is still around.
        return _run_download_deduped(
            url,
            audio_format_id,
            video_format_id,
            target_format,
            target_audio_params,
            target_video_params,
            progress_hook=progress_hook,
            job_id=job_id,
            on_downloading_started=on_downloading_started,
        )

    ticket_id = uuid.uuid4().hex
    try:
        acquired = download_queue.acquire(
            ticket_id,
            cancel_event=entry["cancel_event"],
            on_waiting=lambda position: _tracked_progress_hook(
                0, f"Queued for download (position {position})"
            ),
        )
        if not acquired:
            raise asyncio.CancelledError("Cancelled while queued for a download slot")
        try:
            if on_downloading_started:
                on_downloading_started()
            result = asyncio.run(
                _process_download(
                    url,
                    audio_format_id,
                    video_format_id,
                    target_format,
                    target_audio_params,
                    target_video_params,
                    progress_hook=_tracked_progress_hook,
                    cancel_event=entry["cancel_event"],
                )
            )
            entry["result"] = result
            return result
        finally:
            download_queue.release(ticket_id)
    except Exception as e:
        entry["error"] = e
        raise
    finally:
        with _active_downloads_lock:
            _active_downloads.pop(dedup_key, None)
        entry["event"].set()


def _deregister_job(job_id: str, allow_cancel: bool) -> None:
    """
    Removes job_id from its dedup entry's set of interested jobs. If that
    was the last interested job and allow_cancel is True, cancels the
    underlying download. Safe to call even if the job or its entry no
    longer exist (e.g. the download already finished).
    """
    with _jobs_lock:
        job = _jobs.get(job_id)
        dedup_key = job.get("dedup_key") if job else None

    if dedup_key is None:
        return

    with _active_downloads_lock:
        entry = _active_downloads.get(dedup_key)
        if entry is None:
            return
        entry["job_ids"].discard(job_id)
        if allow_cancel and not entry["job_ids"]:
            entry["cancel_event"].set()


def _compute_dedup_key(
    url: str,
    audio_format_id: str | None,
    video_format_id: str | None,
    target_format: str | None,
    target_audio_params: str | None,
    target_video_params: str | None,
):
    return (
        ytdl_core.clean_youtube_url(url),
        audio_format_id,
        video_format_id,
        target_format,
        target_audio_params,
        target_video_params,
    )


def _job_reaper_loop():
    """
    Periodically sweeps the job registry: jobs whose client hasn't polled
    /download/status recently are treated as dropped (heartbeat lost) - this
    always cleans up their bookkeeping so they stop blocking other clients'
    explicit cancels, and additionally cancels the underlying download too
    if AUTO_CANCEL_ON_HEARTBEAT_LOSS is enabled. Old finished jobs are
    forgotten after JOB_RETENTION_SECONDS so _jobs doesn't grow unbounded.
    """
    log.info("Job reaper thread started.")
    terminal_statuses = ("complete", "error", "cancelled", "dropped")
    while True:
        try:
            now = time.time()
            to_drop = []
            to_forget = []
            with _jobs_lock:
                for job_id, job in _jobs.items():
                    if job["status"] not in terminal_statuses:
                        if now - job.get("last_seen", job["created"]) > JOB_HEARTBEAT_TIMEOUT_SECONDS:
                            to_drop.append(job_id)
                    elif now - job.get("finished_at", job["created"]) > JOB_RETENTION_SECONDS:
                        to_forget.append(job_id)

            for job_id in to_drop:
                log.info(f"Job {job_id} heartbeat lost; treating as dropped.")
                _deregister_job(job_id, allow_cancel=AUTO_CANCEL_ON_HEARTBEAT_LOSS)
                with _jobs_lock:
                    job = _jobs.get(job_id)
                    if job is not None and job["status"] not in terminal_statuses:
                        job["status"] = "dropped"
                        job["finished_at"] = now

            if to_forget:
                with _jobs_lock:
                    for job_id in to_forget:
                        _jobs.pop(job_id, None)
        except Exception as e:
            log.error(f"Error in job reaper loop: {e}", exc_info=True)
        time.sleep(JOB_REAPER_INTERVAL_SECONDS)


def _background_job_runner(
    job_id: str,
    url: str,
    audio_format_id: str | None,
    video_format_id: str | None,
    target_format: str | None,
    target_audio_params: str | None,
    target_video_params: str | None,
) -> None:
    """Runs a /download/start job in the background and records its outcome."""
    sink, cleanup_progress = _build_progress_sink(f"Job {job_id[:8]}", job_id=job_id)

    def _mark_downloading():
        # Fired by _run_download_deduped once this job's underlying download
        # is actually running - for an owner, that's only after it has
        # claimed a download_queue slot, so the job stays "queued" for as
        # long as it's waiting on a concurrency slot, not just at creation.
        with _jobs_lock:
            job = _jobs.get(job_id)
            if job is not None and job["status"] not in (
                "complete", "error", "cancelled", "dropped",
            ):
                job["status"] = "downloading"

    try:
        result_path = _run_download_deduped(
            url,
            audio_format_id,
            video_format_id,
            target_format,
            target_audio_params,
            target_video_params,
            progress_hook=sink,
            job_id=job_id,
            on_downloading_started=_mark_downloading,
        )
        with _jobs_lock:
            job = _jobs.get(job_id)
            # A job the client already explicitly cancelled (e.g. this job
            # was kept alive only because another waiter shared the same
            # download) stays "cancelled" from that client's point of view,
            # even though the shared download went on to succeed.
            if job is not None and job["status"] != "cancelled":
                job["status"] = "complete"
                job["percent"] = 100
                job["result_path"] = str(result_path)
                job["finished_at"] = time.time()
    except asyncio.CancelledError as e:
        with _jobs_lock:
            job = _jobs.get(job_id)
            if job is not None:
                job["status"] = "cancelled"
                job["error"] = str(e) or "Cancelled"
                job["finished_at"] = time.time()
    except (
        yt_dlp.utils.DownloadError,
        ValueError,
        FileNotFoundError,
        RuntimeError,
    ) as e:
        # _process_download/download_item already logged this with a clean,
        # single-line message and no traceback - dumping a second full
        # stack trace here for an already-diagnosed, expected failure type
        # just buries the useful log line under noise.
        log.error(f"Job {job_id} failed: {_clean_error_message(e)}")
        with _jobs_lock:
            job = _jobs.get(job_id)
            if job is not None and job["status"] != "cancelled":
                job["status"] = "error"
                job["error"] = _clean_error_message(e)
                job["finished_at"] = time.time()
    except Exception as e:
        # An exception type not already recognized/logged elsewhere - keep
        # the traceback here since this is the only place it'll be seen.
        log.error(f"Job {job_id} failed unexpectedly: {e}", exc_info=True)
        with _jobs_lock:
            job = _jobs.get(job_id)
            if job is not None and job["status"] != "cancelled":
                job["status"] = "error"
                job["error"] = str(e)
                job["finished_at"] = time.time()
    finally:
        cleanup_progress()


def _clean_error_message(e: Exception) -> str:
    """
    Returns a client-friendly message for an exception raised during
    download processing: yt_dlp.utils.DownloadError messages come prefixed
    with a redundant "ERROR: " tag (already implied by the fact this is an
    error), so that prefix is stripped while keeping the rest of the
    message (e.g. "HTTP Error 403: Forbidden") intact. Other exception
    types are returned as-is via str(e).
    """
    message = str(e)
    if isinstance(e, yt_dlp.utils.DownloadError):
        parts = message.split(":")
        if len(parts) > 1:
            stripped = ":".join(parts[1:]).strip()
            if stripped:
                return stripped
    return message


def _parse_download_request(data):
    """
    Parses/validates the parameters shared by /download and /download/start.
    Returns (params_dict, None) on success, or (None, (response, status_code))
    on failure - the caller can return that tuple directly from a Flask view.
    """
    if not data:
        log.warning("Download request received with no parameters.")
        return None, (jsonify({"error": "No parameters provided"}), 400)

    url = data.get("url")
    audio_format_id = data.get("audio_format_id") or None
    video_format_id = data.get("video_format_id") or None
    target_format = data.get("target_format") or None
    target_audio_params = data.get("target_audio_params") or None
    target_video_params = data.get("target_video_params") or None

    if not url:
        log.warning("Download request missing 'url' parameter.")
        return None, (jsonify({"error": "Missing 'url' parameter"}), 400)

    if not url.startswith(("http://", "https://")):
        log.warning(f"Download request with invalid URL format: {url}")
        return None, (jsonify({"error": "Invalid 'url' parameter format"}), 400)

    needs_ffmpeg = bool(target_format) or (
        audio_format_id and video_format_id and audio_format_id != video_format_id
    )
    if needs_ffmpeg and not FFMPEG_PATH:
        log.warning(
            f"Request requires FFmpeg (Target: {target_format}, A_ID: {audio_format_id}, V_ID: {video_format_id}) but it is not available."
        )
        return None, (
            jsonify(
                {
                    "error": "FFmpeg is required for this request (conversion or merging) but was not found or configured."
                }
            ),
            501,
        )

    return {
        "url": url,
        "audio_format_id": audio_format_id,
        "video_format_id": video_format_id,
        "target_format": target_format,
        "target_audio_params": target_audio_params,
        "target_video_params": target_video_params,
    }, None


# --- Flask Routes ---
@app.route("/", methods=["GET"])
def index():
    """Provides a simple HTML form to interact with the /download endpoint."""
    # Check FFmpeg status to display info on the form
    ffmpeg_status = (
        "Available" if FFMPEG_PATH else "Not Found (merging/conversion may fail)"
    )
    return render_template_string(
        f"""<!DOCTYPE html>
<html>
<head><title>YouTube Downloader Service</title>
<style>
    body {{ font-family: sans-serif; }}
    input[type=text] {{ width: 400px; margin-bottom: 5px; }}
    label {{ display: inline-block; width: 150px; }}
</style>
</head>
<body>
    <h1>YouTube Downloader Service</h1>
    <p>Enter a YouTube URL and optionally specify format IDs or a target container.</p>
    <form action="/download" method="get" target="_blank">
        <label for="url">YouTube URL:</label>
        <input type="text" id="url" name="url" size="60" required placeholder="https://www.youtube.com/watch?v=..."><br>

        <label for="audio_format_id">Audio Format ID:</label>
        <input type="text" id="audio_format_id" name="audio_format_id" placeholder="e.g., 251 (Opus@160k)"><br>

        <label for="video_format_id">Video Format ID:</label>
        <input type="text" id="video_format_id" name="video_format_id" placeholder="e.g., 137 (1080p MP4)"><br>

        <label for="target_format">Target Format:</label>
        <input type="text" id="target_format" name="target_format" placeholder="e.g., mp3, m4a, mp4, mkv"><br>

        <input type="submit" value="Download">
    </form>
    <hr>
    <h2>Notes:</h2>
    <ul>
        <li>Leave format IDs blank to get the best available quality (usually merged video+audio in mp4/mkv/webm).</li>
        <li>Specify <b>only</b> Audio ID for audio-only download (best if source is audio-only like Opus/M4A).</li>
        <li>Specify <b>only</b> Video ID for video download (it might already contain audio, or be video-only).</li>
        <li>Specify <b>both</b> Audio and Video ID if you want to force merging specific streams (requires FFmpeg).</li>
        <li>Use <b>Target Format</b> to convert the output (e.g., specify best audio ID and 'mp3' target; requires FFmpeg). Valid targets depend on FFmpeg capabilities (common: mp3, m4a, aac, ogg, opus, mp4, mkv, webm).</li>
        <li>FFmpeg Status: <b>{ffmpeg_status}</b></li>
        <li>Files are temporarily stored in <code>{TEMP_DIR}</code> and automatically deleted after {DELETE_DELAY // 60} minutes.</li>
    </ul>
    <p><a href="/list_formats" target="_blank">List Available Formats (Experimental)</a> - Enter URL below:</p>
    <form action="/list_formats" method="get" target="_blank">
        <label for="list_url">YouTube URL:</label>
        <input type="text" id="list_url" name="url" size="60" required placeholder="https://www.youtube.com/watch?v=..."><br>
        <input type="submit" value="List Formats">
    </form>
</body>
</html>
    """
    )


@app.route("/download", methods=["GET", "POST", "OPTIONS"])
def download():
    """Handles the download request, calls async processing, and sends the file."""
    if request.method == "OPTIONS":
        return ("", 204)
    if request.method == "POST":
        # Prefer JSON body for POST, fallback to form
        data = request.json if request.is_json else request.form
    else:  # GET
        data = request.args

    params, error_response = _parse_download_request(data)
    if error_response:
        return error_response

    url = params["url"]
    audio_format_id = params["audio_format_id"]
    video_format_id = params["video_format_id"]
    target_format = params["target_format"]
    target_audio_params = params["target_audio_params"]
    target_video_params = params["target_video_params"]

    progress_hook, cleanup_progress = _build_progress_sink("Preparing")

    final_filepath = None
    # Keep original blocking download route for compatibility, but recommend
    # using /download_start for background downloads. This route will still
    # perform the download synchronously and return the file (blocking).
    try:
        progress_hook(5, "Request validated")
        log.info(
            f"Processing download request for URL: {url} (A_ID: {audio_format_id}, V_ID: {video_format_id}, Target: {target_format}), TargetAudioParams: {target_audio_params}), TargetVideoParams: {target_video_params})"
        )
        progress_hook(10, "Download task started")
        final_filepath = _run_download_deduped(
            url,
            audio_format_id,
            video_format_id,
            target_format,
            target_audio_params,
            target_video_params,
            progress_hook=progress_hook,
        )

        if final_filepath and final_filepath.exists():
            log.info(f"File ready for sending: {final_filepath}")
            progress_hook(95, f"{final_filepath.name} - file prepared")

            # Add to delete queue *before* sending the file
            with queue_lock:
                delete_queue.append({"path": str(final_filepath), "time": time.time()})
                log.info(
                    f"Queued for deletion: {final_filepath} (Queue size: {len(delete_queue)})"
                )

            progress_hook(100, f"{final_filepath.name} - completed")
            return send_file(
                str(final_filepath),
                as_attachment=True,
                download_name=final_filepath.name,  # Use the actual filename generated
            )
        else:
            # This case should ideally be caught by exceptions within _process_download
            log.error(
                f"Download process completed but final file path is invalid or file does not exist: {final_filepath}"
            )
            return (
                jsonify(
                    {"error": "Download failed: Final file not found after processing."}
                ),
                500,
            )

    except (
        yt_dlp.utils.DownloadError,
        ValueError,
        FileNotFoundError,
        RuntimeError,
        asyncio.CancelledError,
    ) as e:
        # _process_download already logged the details with a clean
        # message; a warning here is enough context to correlate the URL.
        error_type = type(e).__name__
        error_detail = _clean_error_message(e)
        log.warning(f"Download failed for {url}. Error: {error_type}: {error_detail}")

        status_code = (
            400 if isinstance(e, ValueError) else 500
        )  # Bad request for value errors, server error otherwise
        if isinstance(e, FileNotFoundError):
            status_code = 404  # Or 500? Let's use 500 as it's likely a server-side processing issue.

        return jsonify({"error": f"{error_type}: {error_detail}"}), status_code

    except Exception as e:
        # Catch any unexpected errors during the request handling itself
        log.error(
            f"Unexpected error during download request for {url}: {e}", exc_info=True
        )
        return jsonify({"error": "An unexpected server error occurred."}), 500
    finally:
        cleanup_progress()
        # Ensure the file is queued for deletion even if send_file fails?
        # No, send_file failure means the client didn't get it, maybe don't delete yet?
        # The current logic queues *before* send_file, which seems reasonable.
        # If send_file raises an exception (e.g., client disconnects), the file remains queued.
        pass


@app.route("/download/start", methods=["POST", "OPTIONS"])
def download_start():
    """
    Starts a download in the background and returns a job_id immediately.
    Intended for clients (the userscript) that want live status/percent via
    /download/status/<job_id> instead of blocking on the whole download like
    /download does. Poll /download/status/<job_id>, then GET
    /download/result/<job_id> once status is "complete".
    """
    if request.method == "OPTIONS":
        return ("", 204)
    data = request.json if request.is_json else request.form

    params, error_response = _parse_download_request(data)
    if error_response:
        return error_response

    job_id = uuid.uuid4().hex
    dedup_key = _compute_dedup_key(
        params["url"],
        params["audio_format_id"],
        params["video_format_id"],
        params["target_format"],
        params["target_audio_params"],
        params["target_video_params"],
    )
    now = time.time()
    with _jobs_lock:
        _jobs[job_id] = {
            "dedup_key": dedup_key,
            "status": "queued",
            "percent": 0,
            "message": "Queued",
            "error": None,
            "result_path": None,
            "cancel_requested": False,
            "created": now,
            "last_seen": now,
            "finished_at": None,
        }

    thread = threading.Thread(
        target=_background_job_runner,
        args=(
            job_id,
            params["url"],
            params["audio_format_id"],
            params["video_format_id"],
            params["target_format"],
            params["target_audio_params"],
            params["target_video_params"],
        ),
        name=f"ytdl-job-{job_id[:8]}",
        daemon=True,
    )
    thread.start()

    return jsonify({"job_id": job_id}), 202


@app.route("/download/status/<job_id>", methods=["GET", "OPTIONS"])
def download_status(job_id):
    """Returns live status/percent for a job started via /download/start."""
    if request.method == "OPTIONS":
        return ("", 204)
    with _jobs_lock:
        job = _jobs.get(job_id)
        if job is None:
            return jsonify({"error": "Unknown or expired job_id"}), 404
        job["last_seen"] = time.time()
        status = job["status"]
        percent = job["percent"]
        message = job["message"]
        error = job["error"]

    return jsonify(
        {
            "job_id": job_id,
            "status": status,
            "percent": percent,
            "message": message,
            "error": error,
            "ready": status == "complete",
        }
    )


@app.route("/download/result/<job_id>", methods=["GET", "OPTIONS"])
def download_result(job_id):
    """Sends the completed file for a job started via /download/start."""
    if request.method == "OPTIONS":
        return ("", 204)
    with _jobs_lock:
        job = _jobs.get(job_id)
        if job is None:
            return jsonify({"error": "Unknown or expired job_id"}), 404
        if job["status"] != "complete":
            return (
                jsonify({"error": f"Job is not complete (status: {job['status']})"}),
                409,
            )
        result_path = job["result_path"]

    final_filepath = pathlib.Path(result_path)
    if not final_filepath.exists():
        log.error(f"Job {job_id} reports complete but file is missing: {final_filepath}")
        return jsonify({"error": "Result file not found on server."}), 500

    with queue_lock:
        already_queued = any(fi["path"] == str(final_filepath) for fi in delete_queue)
        if not already_queued:
            delete_queue.append({"path": str(final_filepath), "time": time.time()})
            log.info(
                f"Queued for deletion: {final_filepath} (Queue size: {len(delete_queue)})"
            )

    return send_file(
        str(final_filepath),
        as_attachment=True,
        download_name=final_filepath.name,
    )


@app.route("/download/cancel/<job_id>", methods=["POST", "OPTIONS"])
def download_cancel(job_id):
    """
    Cancels a job started via /download/start. Only actually stops the
    underlying yt-dlp download once no other job is still waiting on the
    same item (see _run_download_deduped/_deregister_job) - one tab
    cancelling doesn't kill a download another tab is still waiting for.
    """
    if request.method == "OPTIONS":
        return ("", 204)
    with _jobs_lock:
        job = _jobs.get(job_id)
        if job is None:
            return jsonify({"error": "Unknown or expired job_id"}), 404
        job["cancel_requested"] = True
        already_terminal = job["status"] in ("complete", "error", "cancelled", "dropped")

    if not already_terminal:
        _deregister_job(job_id, allow_cancel=True)
        with _jobs_lock:
            job = _jobs.get(job_id)
            if job is not None and job["status"] not in ("complete", "error", "cancelled", "dropped"):
                job["status"] = "cancelled"
                job["finished_at"] = time.time()

    return jsonify({"job_id": job_id, "status": "cancelled"})


@app.route("/list_formats", methods=["GET", "OPTIONS"])
def list_formats():
    """(Experimental) Fetches and lists available formats for a URL."""
    if request.method == "OPTIONS":
        return ("", 204)
    url = request.args.get("url")
    if not url:
        return jsonify({"error": "Missing 'url' parameter"}), 400
    if not url.startswith(("http://", "https://")):
        return jsonify({"error": "Invalid 'url' parameter format"}), 400

    # Require an individual video URL (watch?v=..., youtu.be, or /embed/)
    if not is_video_url(url):
        log.warning(f"list_formats called with non-video URL: {url}")
        return (
            jsonify({"error": "Only individual YouTube video URLs are supported by this endpoint."}),
            400,
        )

    try:
        log.info(f"Fetching formats for URL: {url}")
        # Run fetch_info asynchronously
        item = asyncio.run(ytdl_core.fetch_info(url))
        log.info(f"Format fetch successful for '{item.title}'")

        # Prepare data for JSON response
        response_data = {
            "title": item.title,
            "artist": item.artist,
            "duration": item.duration,
            "year": item.year,
            "url": item.url,
            "audio_formats": [f.to_dict() for f in item.audio_formats],
            "video_formats": [f.to_dict() for f in item.video_formats],
        }
        return jsonify(response_data)

    except (yt_dlp.utils.DownloadError, ValueError) as e:
        error_type = type(e).__name__
        error_detail = _clean_error_message(e)
        log.warning(f"Error fetching formats for {url}: {error_type}: {error_detail}")
        return jsonify({"error": f"{error_type}: {error_detail}"}), 500
    except Exception as e:
        log.error(f"Unexpected error fetching formats for {url}: {e}", exc_info=True)
        return jsonify({"error": f"{type(e).__name__}: {e}"}), 500


# --- Main Execution ---
if __name__ == "__main__":
    def setup_logging():
        """Configure logging: console ERROR by default, optional file logging."""
        log_level = os.environ.get("LOG_LEVEL", "ERROR").upper()
        resolved_log_level = getattr(logging, log_level, logging.ERROR)
        log_to_file = os.environ.get("LOG_TO_FILE", "").strip().lower() in (
            "1",
            "true",
            "yes",
            "on",
        )
        log_file_path = os.environ.get("LOG_FILE", "server.log")

        root_logger = logging.getLogger()
        root_logger.handlers.clear()
        root_logger.setLevel(resolved_log_level)

        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(resolved_log_level)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)

        # Route Flask/Werkzeug access logs to INFO and avoid their own handlers
        werkzeug_logger = logging.getLogger("werkzeug")
        werkzeug_logger.handlers.clear()
        werkzeug_logger.propagate = False
        werkzeug_logger.setLevel(resolved_log_level)
        werkzeug_logger.addHandler(logging.NullHandler())

        flask_logger = logging.getLogger("flask.app")
        flask_logger.handlers.clear()
        flask_logger.propagate = False
        flask_logger.addHandler(logging.NullHandler())

        if log_to_file:
            file_handler = logging.FileHandler(log_file_path, encoding="utf-8")
            file_handler.setLevel(resolved_log_level)
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)

            # Send Werkzeug/Flask access logs to file only
            werkzeug_logger.handlers.clear()
            werkzeug_logger.addHandler(file_handler)
            flask_logger.handlers.clear()
            flask_logger.addHandler(file_handler)

        # Send warnings through logging at WARNING level
        logging.captureWarnings(True)
        logging.getLogger("py.warnings").setLevel(logging.WARNING)

    setup_logging()

    # Log python version and executable location to debug
    log.debug(f"Starting server with Python {sys.version} at {sys.executable}")

    # Start the background deletion task in a separate thread
    delete_thread = threading.Thread(
        target=delayed_delete, name="FileDeletionThread", daemon=True
    )
    delete_thread.start()

    # Start the job heartbeat reaper for /download/start jobs
    reaper_thread = threading.Thread(
        target=_job_reaper_loop, name="JobReaperThread", daemon=True
    )
    reaper_thread.start()

    # If music style recognition is enabled, warm up its ML model now
    # rather than lazily during the first download that needs it.
    _maybe_warm_up_genre_classifier()

    # Run Flask app
    # Use host='0.0.0.0' to make it accessible on the local network
    # Use debug=False for anything resembling production/shared use
    log.info("Starting Flask server...")
    app.run(
        host="127.0.0.1", port=5000, debug=False, threaded=True
    )  # Set debug=True for development only; threaded=True allows /progress polling during long requests
