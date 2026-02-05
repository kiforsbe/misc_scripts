import os
import logging
import threading
import time
import asyncio
import pathlib
import sys
from flask import Flask, request, send_file, jsonify, render_template_string
import yt_dlp  # For exceptions
import yt_dlp.utils  # For exceptions

# --- Add ytdl_helper to Python path ---
# This assumes the script is run from the 'youtube-video-downloader' directory
# and 'ytdl_helper' is a subdirectory within it.
current_dir = pathlib.Path(__file__).parent
ytdl_helper_path = current_dir / "ytdl_helper"
if ytdl_helper_path.is_dir():
    sys.path.insert(0, str(current_dir))
    print(f"DEBUG: Added {current_dir} to sys.path")  # Debug print
    try:
        from ytdl_helper import core as ytdl_core
        from ytdl_helper import models as ytdl_models
        from ytdl_helper.utils import check_ffmpeg
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

        print("DEBUG: Imported ytdl_helper as installed package.")  # Debug print
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

        except Exception as e:
            log.error(f"Error in delayed_delete loop: {e}", exc_info=True)
            # Avoid busy-looping on error
            time.sleep(60)
        finally:
            # Check every 60 seconds regardless of deletions
            time.sleep(60)


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

        # Define the progress callback function first
        def progress_callback(cb_item, progress_data):
            if progress_data["status"] == "downloading":
                percent = progress_data.get("percentage")
                if percent is not None:
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

        # Now initialize the attribute on the defined function object
        progress_callback.last_logged_percent = -10

        # --- Trigger the download ---
        await ytdl_core.download_item(
            item,
            output_dir=temp_dir_path,  # Download directly into our temp dir
            target_format=target_format,
            target_audio_params=target_audio_params,
            target_video_params=target_video_params,
            status_callback=status_callback,
            progress_callback=progress_callback,
        )

        # --- Verify result ---
        if (
            item.status == "Complete"
            and item.final_filepath
            and item.final_filepath.exists()
        ):
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

    except (
        yt_dlp.utils.DownloadError,
        ValueError,
        FileNotFoundError,
        RuntimeError,
        asyncio.CancelledError,
    ) as e:
        log_msg = f"Error processing download for {url}: {e}"
        # Avoid logging full trace here if it's a known DownloadError type,
        # as ytdl_core likely logged it already. Log trace for unexpected ones.
        log.error(
            log_msg,
            exc_info=not isinstance(
                e,
                (
                    yt_dlp.utils.DownloadError,
                    ValueError,
                    FileNotFoundError,
                    asyncio.CancelledError,
                ),
            ),
        )
        # Ensure item status reflects error if item exists
        if item:
            item.status = "Error"
            item.error = str(e)
        # Re-raise the exception to be caught by the Flask route
        raise
    except Exception as e:
        # Catch any other unexpected exceptions
        log.error(f"Unexpected error processing download for {url}: {e}", exc_info=True)
        if item:
            item.status = "Error"
            item.error = f"Unexpected: {str(e)}"
        raise RuntimeError(
            f"An unexpected server error occurred during download processing: {e}"
        )


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

    if not data:
        log.warning("Download request received with no parameters.")
        return jsonify({"error": "No parameters provided"}), 400

    url = data.get("url")
    # Ensure None if empty string or missing, otherwise use the value
    audio_format_id = data.get("audio_format_id") or None
    video_format_id = data.get("video_format_id") or None
    target_format = data.get("target_format") or None
    target_audio_params = data.get("target_audio_params") or None
    target_video_params = data.get("target_video_params") or None

    if not url:
        log.warning("Download request missing 'url' parameter.")
        return jsonify({"error": "Missing 'url' parameter"}), 400

    # Basic URL validation (can be improved, e.g., regex for YouTube domains)
    if not url.startswith(("http://", "https://")):
        log.warning(f"Download request with invalid URL format: {url}")
        return jsonify({"error": "Invalid 'url' parameter format"}), 400

    # Check if FFmpeg is needed but unavailable
    needs_ffmpeg = bool(target_format) or (
        audio_format_id and video_format_id and audio_format_id != video_format_id
    )
    if needs_ffmpeg and not FFMPEG_PATH:
        log.warning(
            f"Request requires FFmpeg (Target: {target_format}, A_ID: {audio_format_id}, V_ID: {video_format_id}) but it is not available."
        )
        # Return an error immediately if FFmpeg is essential for the request
        return (
            jsonify(
                {
                    "error": f"FFmpeg is required for this request (conversion or merging) but was not found or configured."
                }
            ),
            501,
        )  # 501 Not Implemented

    final_filepath = None
    try:
        # Run the async download process using asyncio.run()
        # Note: This blocks the current Flask worker thread until completion.
        # For production, consider async Flask routes or a task queue (Celery).
        log.info(
            f"Processing download request for URL: {url} (A_ID: {audio_format_id}, V_ID: {video_format_id}, Target: {target_format}), TargetAudioParams: {target_audio_params}), TargetVideoParams: {target_video_params})"
        )
        final_filepath = asyncio.run(
            _process_download(url, audio_format_id, video_format_id, target_format, target_audio_params, target_video_params)
        )

        if final_filepath and final_filepath.exists():
            log.info(f"File ready for sending: {final_filepath}")

            # Add to delete queue *before* sending the file
            with queue_lock:
                delete_queue.append({"path": str(final_filepath), "time": time.time()})
                log.info(
                    f"Queued for deletion: {final_filepath} (Queue size: {len(delete_queue)})"
                )

            # Send the file back to the client
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
        # Handle errors raised from _process_download
        error_type = type(e).__name__
        error_detail = str(e)
        log.warning(
            f"Download failed for {url}. Error: {error_type}: {error_detail}"
        )  # Already logged details in _process_download

        # Sanitize yt-dlp error messages slightly for the client
        if isinstance(e, yt_dlp.utils.DownloadError):
            # Often contains verbose prefixes, try to get the core message
            parts = error_detail.split(":")
            if len(parts) > 1:
                error_detail = ":".join(
                    parts[1:]
                ).strip()  # Join back in case of colons in message
            if not error_detail:
                error_detail = str(e)  # Fallback if split fails

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
        # Ensure the file is queued for deletion even if send_file fails?
        # No, send_file failure means the client didn't get it, maybe don't delete yet?
        # The current logic queues *before* send_file, which seems reasonable.
        # If send_file raises an exception (e.g., client disconnects), the file remains queued.
        pass


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

    except (yt_dlp.utils.DownloadError, ValueError, Exception) as e:
        log.error(f"Error fetching formats for {url}: {e}", exc_info=True)
        error_type = type(e).__name__
        error_detail = str(e)
        if isinstance(e, yt_dlp.utils.DownloadError):
            parts = error_detail.split(":")
            if len(parts) > 1:
                error_detail = ":".join(parts[1:]).strip()
            if not error_detail:
                error_detail = str(e)
        return jsonify({"error": f"{error_type}: {error_detail}"}), 500


# --- Main Execution ---
if __name__ == "__main__":
    # Configure logging with force=True to ensure it takes effect
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        #stream=sys.stdout,
        force=True,  # Force reconfiguration if already configured
    )
    
    # Also ensure root logger is set to DEBUG
    #logging.getLogger().setLevel(logging.DEBUG)

    # Log what version of pythong this process was started with and what path to the python environment, log this to debug
    log.debug(f"Starting server with Python {sys.version} at {sys.executable}")

    # Start the background deletion task in a separate thread
    delete_thread = threading.Thread(
        target=delayed_delete, name="FileDeletionThread", daemon=True
    )
    delete_thread.start()

    # Run Flask app
    # Use host='0.0.0.0' to make it accessible on the local network
    # Use debug=False for anything resembling production/shared use
    log.info("Starting Flask server...")
    app.run(
        host="127.0.0.1", port=5000, debug=False
    )  # Set debug=True for development only
