import argparse
import json
import logging
import pathlib
import sys
from typing import List, Optional, Dict, Any

# Import tqdm for the progress bar
try:
    from tqdm import tqdm
except ImportError:
    print("Error: 'tqdm' library not found. Please install it (`pip install tqdm`)", file=sys.stderr)
    sys.exit(1)

# Assuming ytdl_helper is in the same parent directory or installed
try:
    from ytdl_helper import (
        DownloadItem,
        FormatInfo,
        fetch_info, # Still async, but we'll call it differently
        download_item, # Still async, but we'll call it differently
        check_ffmpeg,
        sanitize_filename, # Import sanitize_filename if needed directly
        __version__ as ytdl_helper_version
    )
except ImportError:
    # Add parent directory to path if running script directly
    script_dir = pathlib.Path(__file__).parent
    sys.path.insert(0, str(script_dir.parent))
    try:
        from ytdl_helper import (
            DownloadItem,
            FormatInfo,
            fetch_info,
            download_item,
            check_ffmpeg,
            sanitize_filename,
            __version__ as ytdl_helper_version
        )
    except ImportError:
        print("Error: Could not import the 'ytdl_helper' library.", file=sys.stderr)
        print("Ensure it's installed or located correctly relative to this script.", file=sys.stderr)
        sys.exit(1)

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s',
    stream=sys.stderr
)
logger = logging.getLogger("ytdl_cli")
# Silence tqdm's default output slightly by raising the logger level
logging.getLogger('tqdm').setLevel(logging.WARNING)
# logging.getLogger("ytdl_helper").setLevel(logging.DEBUG) # Uncomment for more details

# --- Helper Functions (format_info_to_dict, download_item_to_dict, find_closest_*, etc. remain the same) ---
# --- Keep these helpers as they are useful ---
def format_info_to_dict(fmt: FormatInfo) -> Dict[str, Any]:
    """Converts a FormatInfo object to a JSON-serializable dictionary."""
    return {
        "format_id": fmt.format_id,
        "ext": fmt.ext,
        "note": fmt.note,
        "vcodec": fmt.vcodec,
        "acodec": fmt.acodec,
        "height": fmt.height,
        "width": fmt.width,
        "fps": fmt.fps,
        "abr": fmt.abr,
        "vbr": fmt.vbr,
        "filesize": fmt.filesize,
        "filesize_approx": fmt.filesize_approx,
        "filesize_str": fmt.filesize_str,
        "filesize_approx_str": fmt.filesize_approx_str,
    }

def download_item_to_dict(item: DownloadItem) -> Dict[str, Any]:
    """Converts a DownloadItem object to a JSON-serializable dictionary."""
    return {
        "url": item.url,
        "title": item.title,
        "duration": item.duration,
        "artist": item.artist,
        "year": item.year,
        "status": item.status, # Include status in case of fetch errors
        "error": item.error,   # Include error message if any
        "audio_formats": [format_info_to_dict(f) for f in item.audio_formats],
        "video_formats": [format_info_to_dict(f) for f in item.video_formats],
    }

def find_closest_resolution(formats: List[FormatInfo], target_height: int) -> Optional[FormatInfo]:
    """Finds the video format closest to the target height."""
    if not formats: return None
    # Sort by height descending to prioritize higher quality
    sorted_formats = sorted([f for f in formats if f.height], key=lambda f: f.height or 0, reverse=True)
    if not sorted_formats: # No formats with height info
        return formats[0] if formats else None # Return best overall if no height

    if target_height <= 0: return sorted_formats[0] # Default to best if target is invalid

    best_match = min(sorted_formats, key=lambda f: abs(f.height - target_height))
    return best_match


def find_closest_bitrate(formats: List[FormatInfo], target_abr: float) -> Optional[FormatInfo]:
    """Finds the audio format closest to the target audio bitrate (ABR in kbps)."""
    if not formats:
        logger.warning("find_closest_bitrate called with no formats.")
        return None

    # Filter out formats without ABR and sort by ABR descending
    formats_with_abr = sorted(
        [f for f in formats if f.abr is not None and f.abr > 0], # Ensure abr is positive
        key=lambda f: f.abr,
        reverse=True
    )

    if not formats_with_abr:
        logger.warning("No formats with ABR found. Falling back to sorting by filesize.")
        # Fallback: sort by filesize (prefer exact, then approximate) descending
        fallback_sorted = sorted(
            formats,
            key=lambda f: (f.filesize if f.filesize is not None else 0,
                           f.filesize_approx if f.filesize_approx is not None else 0),
            reverse=True
        )
        if fallback_sorted:
            logger.info(f"Selected fallback format (no ABR): {fallback_sorted[0].format_id}")
            return fallback_sorted[0]
        else:
            # Should be impossible if 'formats' was not empty, but handle defensively
            logger.error("Fallback sorting failed, no formats available.")
            return None

    # If target_abr is invalid or not specified, return the best available (first in sorted list)
    if target_abr <= 0:
        logger.info(f"Target ABR <= 0, selecting best available: {formats_with_abr[0].format_id} ({formats_with_abr[0].abr}k)")
        return formats_with_abr[0]

    # Find the format with the minimum absolute difference from the target ABR
    # min() returns the first element in case of ties. Since the list is sorted descending,
    # this implicitly prefers the higher bitrate if the difference is the same.
    best_match = min(formats_with_abr, key=lambda f: abs(f.abr - target_abr))

    logger.info(f"Target ABR: {target_abr}k. Found closest match: {best_match.format_id} ({best_match.abr}k)")
    return best_match

# --- Synchronous Execution Wrapper ---
# We need a way to run the async functions from our sync code.
# asyncio.run() is the simplest way if we only need it temporarily.
import asyncio

def run_async_task(coro):
    """Runs a single async coroutine synchronously."""
    # This creates/gets an event loop just for running this one task
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # If called from within an already running loop (less likely here, but possible)
            # This approach might be problematic. For a simple CLI, creating a new loop is often fine.
            logger.warning("Attempting to run async task from a running loop. This might not work as expected.")
            # A more robust solution might involve threading or concurrent.futures
            # For simplicity, we'll stick with asyncio.run for now.
            # Alternative: Use threading to run the async part in a separate thread's loop.
            import threading
            result = None
            exc = None
            def run_in_thread():
                nonlocal result, exc
                try:
                    result = asyncio.run(coro)
                except Exception as e:
                    exc = e
            thread = threading.Thread(target=run_in_thread)
            thread.start()
            thread.join()
            if exc:
                raise exc
            return result
        else:
            return asyncio.run(coro)
    except RuntimeError:
         # If no current event loop exists, asyncio.run() will create one.
         return asyncio.run(coro)


# --- Progress Bar Handling ---
# Use a dictionary to store tqdm instances per download item URL
progress_bars: Dict[str, tqdm] = {}

def sync_status_callback(item: DownloadItem, status: str, error: Optional[str]):
    """Status callback for synchronous CLI."""
    # Close progress bar if the download ends (Complete, Error, Cancelled, Skipped)
    if item.url in progress_bars:
        if status in ["Complete", "Error", "Cancelled", "Skipped"]:
            # Update final description and close
            final_desc = f"'{item.title}'"
            if status == "Complete":
                progress_bars[item.url].set_description(f"{final_desc} - Completed", refresh=True)
                progress_bars[item.url].update(progress_bars[item.url].total - progress_bars[item.url].n) # Ensure it reaches 100%
            elif status == "Skipped":
                 progress_bars[item.url].set_description(f"{final_desc} - Skipped: {error}", refresh=True)
                 progress_bars[item.url].update(progress_bars[item.url].total - progress_bars[item.url].n) # Mark as 100% visually
            else:
                progress_bars[item.url].set_description(f"{final_desc} - {status}: {error}", refresh=True)
                progress_bars[item.url].colour = 'red' # Make errors red

            progress_bars[item.url].close()
            del progress_bars[item.url]
            # Print a newline after closing to avoid overlap with logs/next bar
            print(file=sys.stderr)
        elif status == "Processing":
             progress_bars[item.url].set_description(f"'{item.title}' - Processing...", refresh=True)
        # Don't log status changes like "Downloading" here, progress callback handles that.
        elif status != "Downloading":
             logger.info(f"'{item.title}': Status changed to {status}")

    elif error: # Log errors even if no progress bar was active
        logger.error(f"'{item.title}': {status} - {error}")
    elif status not in ["Downloading", "Starting"]: # Log other non-terminal statuses
        logger.info(f"'{item.title}': Status changed to {status}")


def sync_progress_callback(item: DownloadItem, progress_data: dict):
    """Progress callback using tqdm."""
    status = progress_data.get('status')
    if status == 'downloading':
        total_bytes = progress_data.get('total_bytes') or progress_data.get('total_bytes_estimate')
        downloaded_bytes = progress_data.get('downloaded_bytes', 0)
        speed = progress_data.get('speed') # Bytes/s
        eta_seconds = progress_data.get('eta') # Seconds

        # Create or get the tqdm instance for this item
        if item.url not in progress_bars:
            # Determine total for tqdm (use 100 if bytes unknown for percentage-based)
            tqdm_total = total_bytes if total_bytes else 100.0
            unit = 'B' if total_bytes else '%'
            unit_scale = True if total_bytes else False
            progress_bars[item.url] = tqdm(
                total=tqdm_total,
                desc=f"'{item.title}'",
                unit=unit,
                unit_scale=unit_scale,
                unit_divisor=1024,
                leave=True, # Leave the bar on screen when done
                ncols=100, # Adjust width as needed
                file=sys.stderr # Ensure it prints to stderr
            )

        pbar = progress_bars[item.url]

        # Update progress
        if total_bytes:
            # Update based on bytes downloaded
            update_amount = downloaded_bytes - pbar.n
            pbar.update(update_amount)
        else:
            # Update based on percentage if total bytes unknown
            percent = progress_data.get('percentage', 0)
            update_amount = percent - pbar.n
            pbar.update(update_amount)

        # Update postfix with speed and ETA
        postfix_dict = {}
        if speed is not None:
            postfix_dict['speed'] = f"{speed / 1024 / 1024:.2f} MiB/s"
        if eta_seconds is not None:
             postfix_dict['eta'] = f"{int(eta_seconds)}s"
        if postfix_dict:
             pbar.set_postfix(postfix_dict, refresh=True) # Use refresh=True less often if performance is an issue

    elif status == 'finished':
        # The 'finished' hook in yt-dlp often means one part is done (e.g., video download before merge)
        # The status callback handles the transition to "Processing" or "Complete"
        if item.url in progress_bars:
             # Ensure bar reaches 100% if it was byte-based
             pbar = progress_bars[item.url]
             if pbar.total and pbar.unit == 'B':
                  pbar.update(pbar.total - pbar.n)
             # Description change handled by status callback ("Processing")
             pass
    elif status == 'error':
        # Error handling is primarily done in the status callback
        if item.url in progress_bars:
            progress_bars[item.url].set_description(f"'{item.title}' - Error", refresh=True)
            progress_bars[item.url].colour = 'red'
            # Status callback will close the bar


# --- Synchronous Handlers ---

def handle_info_sync(args: argparse.Namespace):
    """Handles the 'info' subcommand synchronously."""
    logger.info(f"Fetching info for {len(args.urls)} URL(s)...")
    results = []

    for url in tqdm(args.urls, desc="Fetching Info", unit="url", file=sys.stderr):
        try:
            # Run the async fetch_info function synchronously
            item = run_async_task(fetch_info(url))
            if item:
                results.append(download_item_to_dict(item))
                logger.debug(f"Successfully fetched info for {url}")
            else:
                 # Should not happen if fetch_info raises exceptions on failure
                 logger.error(f"Fetching info for {url} returned None unexpectedly.")
                 results.append({"url": url, "title": None, "status": "Error", "error": "Unknown fetch error (returned None)", "audio_formats": [], "video_formats": []})
        except Exception as e:
            logger.error(f"Failed to fetch info for {url}: {e}")
            results.append({"url": url, "title": None, "status": "Error", "error": str(e), "audio_formats": [], "video_formats": []})

    # Output as JSON to stdout
    try:
        print(json.dumps(results, indent=2))
    except TypeError as e:
        logger.critical(f"Failed to serialize results to JSON: {e}")
        # Fallback: print basic info to stderr if JSON fails
        for item_dict in results:
             print(f"--- URL: {item_dict['url']} ---", file=sys.stderr)
             print(f"Title: {item_dict.get('title', 'N/A')}", file=sys.stderr)
             print(f"Status: {item_dict.get('status', 'N/A')}", file=sys.stderr)
             if item_dict.get('error'):
                  print(f"Error: {item_dict['error']}", file=sys.stderr)
        sys.exit(1)


def handle_download_sync(args: argparse.Namespace):
    """Handles the 'download' subcommand synchronously."""
    if not check_ffmpeg():
        logger.critical("FFmpeg not found. Please install it and add to PATH.")
        sys.exit(1)

    output_dir = pathlib.Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Output directory: {output_dir}")
    if args.format: # Log the chosen format
        logger.info(f"Target format: {args.format}")

    items_to_process: List[DownloadItem] = []
    fetch_errors = 0

    # 1. Fetch info sequentially
    logger.info(f"Fetching info for {len(args.urls)} URL(s)...")
    for url in tqdm(args.urls, desc="Fetching Info", unit="url", file=sys.stderr):
        try:
            item = run_async_task(fetch_info(url))
            if item:
                items_to_process.append(item)
            else:
                logger.error(f"Fetching info for {url} returned None unexpectedly.")
                fetch_errors += 1
        except Exception as e:
            logger.error(f"Failed to fetch info for {url}: {e}")
            fetch_errors += 1

    if not items_to_process:
        logger.error("No video information could be fetched. Cannot proceed.")
        sys.exit(1)

    # 2. Select formats
    logger.info("Selecting formats based on criteria...")
    items_to_download: List[DownloadItem] = []
    skipped_count = 0
    for item in items_to_process:
        logger.debug(f"Processing formats for: {item.title} ({item.url})")
        try:
            target_res = 0
            if args.resolution:
                try:
                    target_res = int(''.join(filter(str.isdigit, args.resolution)))
                except ValueError:
                    logger.warning(f"Invalid resolution format: '{args.resolution}'. Ignoring for '{item.title}'.")

            target_abr = 0.0
            if args.audio_bitrate:
                try:
                    target_abr = float(''.join(filter(str.isdigit, args.audio_bitrate)))
                except ValueError:
                    logger.warning(f"Invalid audio bitrate format: '{args.audio_bitrate}'. Ignoring for '{item.title}'.")

            # --- Format Selection Logic (same as before) ---
            selected_video_fmt: Optional[FormatInfo] = None
            selected_audio_fmt: Optional[FormatInfo] = None

            if args.audio_only:
                if not item.audio_formats:
                    logger.error(f"No audio-only formats found for '{item.title}'. Skipping.")
                    skipped_count += 1
                    continue
                selected_audio_fmt = find_closest_bitrate(item.audio_formats, target_abr)
                if not selected_audio_fmt:
                     logger.error(f"Could not select an audio format for '{item.title}'. Skipping.")
                     skipped_count += 1
                     continue
                item.selected_audio_format_id = selected_audio_fmt.format_id
                logger.info(f"Selected Audio for '{item.title}': [{selected_audio_fmt.format_id}] {selected_audio_fmt.abr}k")
            else: # Video + Audio Mode
                if not item.video_formats:
                    logger.error(f"No video formats found for '{item.title}'. Skipping.")
                    skipped_count += 1
                    continue
                selected_video_fmt = find_closest_resolution(item.video_formats, target_res)
                if not selected_video_fmt:
                    logger.error(f"Could not select a video format for '{item.title}'. Skipping.")
                    skipped_count += 1
                    continue
                item.selected_video_format_id = selected_video_fmt.format_id
                logger.info(f"Selected Video for '{item.title}': [{selected_video_fmt.format_id}] {selected_video_fmt.height}p")

                # Select Audio (either from video or separate stream)
                video_has_audio = selected_video_fmt.acodec and selected_video_fmt.acodec != 'none'
                video_audio_abr = selected_video_fmt.abr

                use_videos_audio = False
                if video_has_audio:
                    if target_abr <= 0: use_videos_audio = True
                    elif video_audio_abr is not None and abs(video_audio_abr - target_abr) <= target_abr * 0.2: # 20% tolerance
                        use_videos_audio = True

                if use_videos_audio:
                    selected_audio_fmt = selected_video_fmt
                    item.selected_audio_format_id = selected_video_fmt.format_id
                    logger.info(f"Using Audio from Video for '{item.title}': [{selected_audio_fmt.format_id}] {selected_audio_fmt.abr}k")
                else: # Need separate audio stream
                    if not item.audio_formats:
                         logger.warning(f"Video format [{selected_video_fmt.format_id}] for '{item.title}' lacks suitable audio, and no separate audio streams found. Audio may be missing.")
                         item.selected_audio_format_id = None # Explicitly None
                    else:
                         best_separate_audio = find_closest_bitrate(item.audio_formats, target_abr)
                         if not best_separate_audio:
                              logger.warning(f"Could not find suitable separate audio stream for '{item.title}'. Audio may be missing.")
                              item.selected_audio_format_id = None
                         else:
                              selected_audio_fmt = best_separate_audio
                              item.selected_audio_format_id = selected_audio_fmt.format_id
                              logger.info(f"Selected Separate Audio for '{item.title}': [{selected_audio_fmt.format_id}] {selected_audio_fmt.abr}k")

            # Add item to the list if formats were successfully selected
            if item.selected_audio_format_id or item.selected_video_format_id:
                items_to_download.append(item)
            else:
                 logger.error(f"Failed to select any valid format for '{item.title}'. Skipping download.")
                 skipped_count += 1

        except Exception as format_exc:
             logger.error(f"Error selecting format for '{item.title}': {format_exc}. Skipping.")
             skipped_count += 1


    # 3. Start downloads sequentially
    if not items_to_download:
        logger.warning("No items eligible for download after format selection.")
        if fetch_errors > 0 or skipped_count > 0:
             sys.exit(1) # Exit with error if items failed fetch or format selection
        else:
             sys.exit(0) # Exit cleanly if no items were provided or eligible

    logger.info(f"Starting download for {len(items_to_download)} item(s)...")
    success_count = 0
    error_count = 0

    # Use enumerate for sequential download numbering if desired
    for idx, item in enumerate(items_to_download):
        logger.info(f"--- Downloading item {idx + 1} of {len(items_to_download)}: '{item.title}' ---")
        try:
            # Run the async download_item function synchronously
            run_async_task(
                download_item(
                    item,
                    output_dir,
                    target_format=args.format,
                    progress_callback=sync_progress_callback,
                    status_callback=sync_status_callback
                )
            )
            # Check status after completion (status callback might have updated it)
            if item.status == "Complete" or item.status == "Skipped":
                 logger.info(f"Successfully processed: '{item.title}' (Status: {item.status})")
                 success_count += 1
            else:
                 # Should have been caught by exception, but double-check
                 logger.error(f"Download ended with unexpected status '{item.status}' for '{item.title}'")
                 error_count += 1

        except Exception as e:
            logger.error(f"Download failed for '{item.title}': {e}")
            error_count += 1
            # Ensure progress bar is cleaned up if an exception occurred outside the callbacks
            if item.url in progress_bars:
                progress_bars[item.url].set_description(f"'{item.title}' - Failed: {e}", refresh=True)
                progress_bars[item.url].colour = 'red'
                progress_bars[item.url].close()
                del progress_bars[item.url]
                print(file=sys.stderr) # Newline after closing

    logger.info(f"--- Download Summary ---")
    logger.info(f"Total Fetched: {len(items_to_process)}")
    logger.info(f"Format Selection Skipped: {skipped_count}")
    logger.info(f"Downloads Attempted: {len(items_to_download)}")
    logger.info(f"Succeeded/Skipped: {success_count}")
    logger.info(f"Failed: {error_count}")

    if error_count > 0 or fetch_errors > 0:
        sys.exit(1)


# --- Main Execution ---

def main():
    parser = argparse.ArgumentParser(
        description=f"YouTube Downloader CLI (Sync) (using ytdl_helper v{ytdl_helper_version})",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Get info for a video as JSON
  python youtube-video-downloader-cli.py info https://www.youtube.com/watch?v=dQw4w9WgXcQ

  # Download best available video+audio (defaults to MP4)
  python youtube-video-downloader-cli.py download https://www.youtube.com/watch?v=dQw4w9WgXcQ

  # Download as MKV
  python youtube-video-downloader-cli.py download https://www.youtube.com/watch?v=dQw4w9WgXcQ --format mkv

  # Download audio only (defaults to M4A) to a specific directory
  python youtube-video-downloader-cli.py download https://www.youtube.com/watch?v=dQw4w9WgXcQ -a -o ./music

  # Download audio only as MP3
  python youtube-video-downloader-cli.py download https://www.youtube.com/watch?v=dQw4w9WgXcQ -a --format mp3

  # Download 720p video (closest) with 128k audio (closest) into an MKV container
  python youtube-video-downloader-cli.py download https://www.youtube.com/watch?v=dQw4w9WgXcQ -r 720p -b 128k --format mkv
"""
    )

    # --- Add log level argument to the main parser ---
    parser.add_argument(
        '--log-level',
        default='WARN',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Set the logging level (default: INFO)'
    )

    subparsers = parser.add_subparsers(dest="command", required=True, help="Sub-command help")

    # --- Info Sub-parser ---
    parser_info = subparsers.add_parser("info", help="Fetch video metadata and available formats as JSON")
    parser_info.add_argument("urls", nargs='+', help="One or more YouTube video/playlist URLs")
    parser_info.set_defaults(func=handle_info_sync) # Use sync handler

    # --- Download Sub-parser ---
    parser_download = subparsers.add_parser("download", help="Download videos or audio")
    parser_download.add_argument("urls", nargs='+', help="One or more YouTube video/playlist URLs")
    parser_download.add_argument(
        "-o", "--output-dir", default=".",
        help="Directory to save downloaded files (default: current directory)"
    )
    parser_download.add_argument(
        "-a", "--audio-only", action="store_true",
        help="Download audio only. Use --format to specify type (default: m4a)."
    )
    parser_download.add_argument(
        "-r", "--resolution", metavar="HEIGHT",
        help="Desired video resolution height (e.g., 1080, 720p). Selects closest available. (Default: best)"
    )
    parser_download.add_argument(
        "-b", "--audio-bitrate", metavar="KBPS",
        help="Desired audio bitrate in kbps (e.g., 192, 128k). Selects closest available for format selection. (Default: best)"
    )
    parser_download.add_argument(
        "-f", "--format", metavar="EXT",
        help="Target container format (e.g., mp4, mkv, webm, mp3, m4a, ogg). Overrides defaults."
    )
    parser_download.set_defaults(func=handle_download_sync)

    args = parser.parse_args()

    # --- Configure logging based on args ---
    log_level_str = args.log_level.upper()
    log_level = getattr(logging, log_level_str, logging.WARN)

    # Reconfigure the root logger
    # Remove existing handlers if basicConfig was called before
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(
        level=log_level, # Set the level based on the argument
        format='%(levelname)s: %(message)s',
        stream=sys.stderr
    )
    # Re-apply tqdm silencing if needed, though setting root level might cover it
    logging.getLogger('tqdm').setLevel(logging.WARNING)
    # Optionally set ytdl_helper level based on main level
    logging.getLogger("ytdl_helper").setLevel(max(log_level, logging.INFO)) # Keep ytdl_helper at least INFO unless main level is DEBUG

    logger.info(f"Log level set to {log_level_str}")

    # Execute the appropriate handler function
    args.func(args)

if __name__ == "__main__":
    # No need for asyncio policy setup anymore
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Operation cancelled by user.")
        # Clean up any lingering progress bars on Ctrl+C
        for pbar in list(progress_bars.values()): # Iterate over a copy
             pbar.close()
        print(file=sys.stderr) # Newline after closing bars
        sys.exit(130) # Standard exit code for Ctrl+C
    except Exception as e:
         logger.critical(f"An unexpected error occurred: {e}", exc_info=True)
         # Clean up progress bars on other exceptions too
         for pbar in list(progress_bars.values()):
              pbar.close()
         print(file=sys.stderr)
         sys.exit(1)
