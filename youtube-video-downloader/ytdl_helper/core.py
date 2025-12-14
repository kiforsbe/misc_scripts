import asyncio
import functools
import logging
import pathlib
import shutil
import sys
import tempfile
from typing import Optional, Callable, Dict, Any

from flask import json
import yt_dlp
import yt_dlp.utils

from .models import DownloadItem, FormatInfo
from .utils import sanitize_filename, check_ffmpeg
from .ffmpeg_genre_pp import FFmpegGenrePP

# Set up module-level logger
logger = logging.getLogger(__name__)
# Configure logging if not already configured
# if not logger.hasHandlers():
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

# Register the custom postprocessor with yt-dlp
ENABLE_CUSTOM_GENRE_PP = False
try:
    # Test that our custom postprocessor can be imported and instantiated
    test_pp = FFmpegGenrePP()
    logging.debug("FFmpegGenrePP postprocessor is available")
    
    # Register the postprocessor in yt-dlp's registry
    from yt_dlp.globals import postprocessors
    postprocessors.value['FFmpegGenrePP'] = FFmpegGenrePP
    
    # Verify registration worked
    if 'FFmpegGenrePP' in postprocessors.value:
        logging.debug("Successfully registered FFmpegGenrePP in yt-dlp registry")
        ENABLE_CUSTOM_GENRE_PP = True
    else:
        logging.warning("Failed to register FFmpegGenrePP - not found in registry after registration")
        ENABLE_CUSTOM_GENRE_PP = False
        
except Exception as e:
    logging.warning(f"FFmpegGenrePP postprocessor unavailable: {e}")
    ENABLE_CUSTOM_GENRE_PP = False

# Define callback types for clarity
ProgressCallbackType = Callable[[DownloadItem, Dict[str, Any]], None]  # Progress callback
StatusCallbackType = Callable[[DownloadItem, str, Optional[str]], None]  # Status callback


def get_cookies_from_browser() -> Optional[tuple]:
    """
    Try to get cookies from available browsers.
    Returns a tuple for yt-dlp's cookiesfrombrowser option, or None if all fail.
    """
    browsers = ["chrome", "firefox", "edge", "chromium"]
    
    for browser in browsers:
        try:
            # Test if we can access the browser's cookies by creating a test YoutubeDL instance
            test_opts = {
                "quiet": True,
                "no_warnings": True,
                "cookiesfrombrowser": (browser,),
                "extract_flat": True,
            }
            with yt_dlp.YoutubeDL(test_opts) as ydl:
                # If this doesn't raise an exception, the browser is accessible
                logger.info(f"Successfully configured cookie extraction from {browser}")
                return (browser,)
        except Exception as e:
            logger.debug(f"Could not access {browser} cookies: {e}")
            continue
    
    logger.warning("Could not access cookies from any browser. Continuing without cookies.")
    return None


def clean_youtube_url(url: str) -> str:
    """
    Clean YouTube URL by removing playlist and other parameters that might cause auth issues.
    Keeps only the video ID parameter.
    """
    try:
        from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
        
        parsed = urlparse(url)
        query_params = parse_qs(parsed.query)
        
        # Only keep the 'v' parameter (video ID)
        cleaned_params = {}
        if 'v' in query_params:
            cleaned_params['v'] = query_params['v']
        
        # Reconstruct the URL with only the video ID
        new_query = urlencode(cleaned_params, doseq=True)
        cleaned_url = urlunparse((
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            new_query,
            parsed.fragment
        ))
        
        if cleaned_url != url:
            logger.info(f"Cleaned URL: {url} -> {cleaned_url}")
        
        return cleaned_url
    except Exception as e:
        logger.warning(f"Failed to clean URL: {e}, using original")
        return url


async def fetch_info(url: str, use_cookies: bool = False) -> DownloadItem:
    """
    Fetches metadata and available formats for a given URL.

    Args:
        url: The YouTube URL.

    Returns:
        A DownloadItem instance populated with information.

    Raises:
        yt_dlp.utils.DownloadError: If yt-dlp fails to extract info.
        Exception: For other unexpected errors during fetching.
    """
    # Clean the URL to remove playlist parameters that might cause auth issues
    cleaned_url = clean_youtube_url(url)
    
    item = DownloadItem(url=cleaned_url, status="Fetching")
    logger.info(f"Fetching info for URL: {cleaned_url}")

    try:
        # Use specific options for info fetching
        info_ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "extract_flat": False,  # Need full format info
            "skip_download": True,
            "verbose": False,
            "ignoreerrors": False,  # Raise error if info fetch fails for this URL
            "forcejson": True,
            "dump_single_json": True,  # Get info as JSON string
            # 'simulate': True, # Alternative to skip_download? Test needed.
        }
        
        if use_cookies:
            # Try to add browser cookies if available
            cookies_config = get_cookies_from_browser()
            if cookies_config:
                info_ydl_opts["cookiesfrombrowser"] = cookies_config

        loop = asyncio.get_event_loop()
        with yt_dlp.YoutubeDL(info_ydl_opts) as ydl:  # type: ignore
            # Run synchronously in executor as yt-dlp info extraction can block
            info_dict = await loop.run_in_executor(
                None, functools.partial(ydl.extract_info, cleaned_url, download=False)
            )

        if not info_dict:
            raise yt_dlp.utils.DownloadError(f"No information extracted for {cleaned_url}")

        logger.debug(f"Successfully fetched info for {cleaned_url}")
        item._raw_info = dict(info_dict)  # Store raw info as dict

        # Populate DownloadItem fields
        item.title = info_dict.get("title", "Unknown Title")
        item.duration = info_dict.get("duration")
        item.artist = (
            info_dict.get("channel") or info_dict.get("uploader") or "Unknown Artist"
        )
        item.description = info_dict.get("description") # Fetch the description
        logger.debug(f"Fetched description for '{item.title}': '{str(item.description)[:100]}...'") # Log fetched description

        upload_date_str = info_dict.get("upload_date")  # Format typically 'YYYYMMDD'
        if (
            upload_date_str
            and isinstance(upload_date_str, str)
            and len(upload_date_str) == 8
        ):
            try:
                item.year = int(upload_date_str[:4])
            except ValueError:
                logger.warning(
                    f"Could not parse year from upload_date: {upload_date_str}"
                )

        # Process formats
        formats_raw = info_dict.get("formats", [])
        if not formats_raw:
            logger.warning(
                f"No 'formats' array found for {url}. Trying 'requested_formats'."
            )
            formats_raw = info_dict.get(
                "requested_formats", []
            )  # Fallback for playlists?

        if not formats_raw:
            raise yt_dlp.utils.DownloadError(f"No downloadable formats found for {url}")

        all_formats = []
        for f_raw in formats_raw:
            # Pre-calculate filesize strings if yt-dlp provided them
            f_raw["filesize_str"] = f_raw.get("filesize") and yt_dlp.utils.format_bytes(
                f_raw["filesize"]
            )
            f_raw["filesize_approx_str"] = f_raw.get(
                "filesize_approx"
            ) and yt_dlp.utils.format_bytes(f_raw["filesize_approx"])

            fmt = FormatInfo(
                format_id=f_raw["format_id"],
                ext=f_raw.get("ext") or "unknown",
                note=f_raw.get("format_note"),
                vcodec=f_raw.get("vcodec"),
                acodec=f_raw.get("acodec"),
                height=f_raw.get("height"),
                width=f_raw.get("width"),
                fps=f_raw.get("fps"),
                abr=f_raw.get("abr"),
                vbr=f_raw.get("vbr"),
                filesize=f_raw.get("filesize"),
                filesize_approx=f_raw.get("filesize_approx"),
                filesize_str=f_raw.get("filesize_str"),
                filesize_approx_str=f_raw.get("filesize_approx_str"),
                raw_data=f_raw,
            )
            all_formats.append(fmt)

        # Separate and sort formats
        item.audio_formats = sorted(
            [
                f
                for f in all_formats
                if f.acodec
                and f.acodec != "none"
                and (not f.vcodec or f.vcodec == "none")
            ],
            key=lambda x: x.abr or 0,
            reverse=True,
        )
        item.video_formats = sorted(
            [f for f in all_formats if f.vcodec and f.vcodec != "none"],
            key=lambda x: (x.height or 0, x.fps or 0, x.vbr or 0.0),
            reverse=True,
        )

        logger.debug(
            f"Found {len(item.audio_formats)} audio-only and "
            f"{len(item.video_formats)} video formats for {url}"
        )

        item.status = "Pending"  # Ready for format selection or download
        return item

    except yt_dlp.utils.DownloadError as e:
        logger.error(
            f"yt-dlp DownloadError fetching info for {url}: {e}", exc_info=False
        )
        item.status = "Error"
        item.error = f"yt-dlp error: {e}"
        # Re-raise or return the item with error status? Re-raising is clearer.
        raise e
    except Exception as e:
        logger.error(f"Unexpected error fetching info for {url}: {e}", exc_info=True)
        item.status = "Error"
        item.error = f"Unexpected error: {e}"
        raise e  # Re-raise unexpected errors


async def download_item(
    item: DownloadItem,
    output_dir: pathlib.Path,
    target_format: Optional[str] = None,
    target_audio_params: Optional[str] = None,
    target_video_params: Optional[str] = None,
    progress_callback: Optional[ProgressCallbackType] = None,
    status_callback: Optional[StatusCallbackType] = None,
    use_cookies: bool = False,
) -> None:
    """
    Downloads the specified DownloadItem based on its selected formats,
    optionally converting to a target container format.

    Args:
        item: The DownloadItem to download (must have format IDs selected).
        output_dir: The directory where the final file should be saved.
        target_format: Desired output container format (e.g., 'mp4', 'mkv',
                       'webm', 'mp3', 'm4a', 'ogg'). If None, uses defaults
                       (mp4 for video, m4a for audio).
        progress_callback: Function called with download progress updates.
        status_callback: Function called when the overall status changes.

    Raises:
        ValueError: If required format selections are missing, FFmpeg is not found,
                    or target_format is invalid for the download type.
        yt_dlp.utils.DownloadError: If the download itself fails.
        FileNotFoundError: If the expected output file is not found after processing.
        Exception: For other unexpected errors during download.
    """
    # --- Pre-checks ---
    if not item.selected_audio_format_id and not item.selected_video_format_id:
        raise ValueError(
            f"Cannot start download for '{item.title}': No format selected."
        )

    ffmpeg_path = check_ffmpeg()
    if not ffmpeg_path:
        raise ValueError("FFmpeg is required for processing but was not found.")

    output_dir.mkdir(parents=True, exist_ok=True)  # Ensure output directory exists

    # --- Normalize target_format ---
    if target_format:
        target_format = target_format.lower().strip(
            "."
        )  # Ensure lowercase, no leading dot

    # --- Helper for status updates ---
    def _update_status(status: str, error: Optional[str] = None):
        item.status = status
        item.error = error
        if status_callback:
            try:
                status_callback(item, status, error)
            except Exception as cb_err:
                logger.error(f"Error in status callback: {cb_err}", exc_info=True)

    # --- Helper for progress updates ---
    def _progress_hook(d: Dict[str, Any]):
        # Update item's internal progress
        if d["status"] == "downloading":
            total = d.get("total_bytes") or d.get("total_bytes_estimate")
            if total and total > 0:
                item.progress = min(100.0, (d.get("downloaded_bytes", 0) / total) * 100)
            else:
                item.progress = 0  # Indicate indeterminate? Or use downloaded_bytes?
            # Don't change overall item status here, let the main flow handle it
        elif d["status"] == "finished":
            # 'finished' hook might fire before post-processing, don't set to 100% yet
            # Let the status callback handle the final "Complete" state.
            # We can set status to "Processing" here if needed, but the main flow does that too.
            pass
        elif d["status"] == "error":
            item.progress = 0.0

        # Call the external callback
        if progress_callback:
            try:
                # Pass a copy or relevant parts of 'd'
                progress_data = {
                    "status": d["status"],
                    "downloaded_bytes": d.get("downloaded_bytes"),
                    "total_bytes": d.get("total_bytes"),
                    "total_bytes_estimate": d.get("total_bytes_estimate"),
                    "percentage": item.progress,  # Use our calculated percentage
                    "speed": d.get("speed"),  # yt-dlp v2021.12.17+ uses 'speed'
                    "_speed_str": d.get("_speed_str"),  # Older versions might use this
                    "eta": d.get("eta"),  # yt-dlp v2021.12.17+ uses 'eta'
                    "_eta_str": d.get("_eta_str"),  # Older versions
                    "filename": d.get("filename"),
                    "info_dict": d.get("info_dict"),  # Be careful, can be large
                }
                progress_callback(item, progress_data)
            except Exception as cb_err:
                logger.error(f"Error in progress callback: {cb_err}", exc_info=True)

    # --- Prepare Download ---
    _update_status("Starting")
    item.progress = 0.0
    temp_download_path: Optional[pathlib.Path] = None  # Track the file within temp dir

    try:
        # Create a temporary directory *per download* for isolation
        # Prefix helps identify temp dirs if they aren't cleaned up properly
        safe_title_prefix = sanitize_filename(item.title or "untitled")[:20]
        with tempfile.TemporaryDirectory(
            prefix=f"ytdl_{safe_title_prefix}_"
        ) as temp_dir_str:
            temp_dir = pathlib.Path(temp_dir_str)
            logger.debug(f"Using temp directory: {temp_dir}")

            # --- Construct format string ---
            format_string = ""
            is_audio_only = False
            is_video_merge = False
            selected_audio_format: Optional[FormatInfo] = item.selected_audio_format
            selected_video_format: Optional[FormatInfo] = item.selected_video_format

            if item.selected_video_format_id and item.selected_audio_format_id:
                if item.selected_video_format_id == item.selected_audio_format_id:
                    format_string = item.selected_video_format_id  # Combined format
                else:
                    format_string = f"{item.selected_video_format_id}+{item.selected_audio_format_id}"
                    is_video_merge = True
            elif item.selected_video_format_id:
                format_string = (
                    item.selected_video_format_id
                )  # Video (might contain audio)
            elif item.selected_audio_format_id:
                format_string = item.selected_audio_format_id  # Audio only
                is_audio_only = True
            else:
                # Should be caught earlier, but defensive check
                raise ValueError("No format ID selected for download")

            logger.debug(f"Using format string: {format_string}")

            # --- Prepare yt-dlp options ---
            # Base filename in temp dir (yt-dlp adds extension)
            temp_out_tmpl = temp_dir / "%(title)s.%(ext)s"

            # Metadata for embedding
            metadata_dict = {
                "title": str(item.title) if item.title else None,
                "artist": str(item.artist) if item.artist else None,
                "date": str(item.year) if item.year else None,  # yt-dlp expects string date
                "description": json.dumps(str(item.description)) if item.description else None,
            }
            
            # Filter out None values
            metadata_dict = {k: v for k, v in metadata_dict.items() if v is not None}

            ydl_opts = {
                "format": format_string,
                "progress_hooks": [_progress_hook],
                "outtmpl": str(temp_out_tmpl),
                "windowsfilenames": sys.platform == "win32",  # Use OS-specific sanitization
                "quiet": True,
                "no_warnings": True,
                "verbose": False,
                "ignoreerrors": False,  # Fail on download errors
                "noprogress": True,  # Disable yt-dlp's console progress bar
                "ffmpeg_location": ffmpeg_path,
                "postprocessors": [],
                "writethumbnail": True,
                "metadata": metadata_dict,
            }
            
            if use_cookies:
                # Try to add browser cookies if available
                cookies_config = get_cookies_from_browser()
                if cookies_config:
                    ydl_opts["cookiesfrombrowser"] = cookies_config

            # --- Configure Postprocessors ---
            final_extension = ".?"  # The final desired extension
            temp_extension = ".?"  # The extension expected in temp dir after PP
            add_metadata_pp = {
                "key": "FFmpegMetadata",
                "add_metadata": True,
                "add_chapters": True,
            }
            add_genremetadata_pp = {
                "key": "FFmpegGenre",
                "when": "post_process"
            }
            embed_thumbnail_pp = {
                "key": "EmbedThumbnail",
                "already_have_thumbnail": False,
            }

            # Define valid formats
            valid_audio_formats = {"mp3", "m4a", "aac", "ogg", "vorbis", "opus", "webm"}
            valid_video_formats = {"mp4", "mkv", "webm"}

            # Determine default format if not specified
            if not target_format:
                target_format = "m4a" if is_audio_only else "mp4"
                logger.debug(
                    f"No target format specified, using default: '{target_format}'"
                )

            if is_audio_only:
                # --- Audio Only Download ---
                if target_format not in valid_audio_formats:
                    logger.warning(
                        f"Invalid or unsupported target audio format '{target_format}'. Falling back to 'm4a'. Valid: {valid_audio_formats}"
                    )
                    target_format = "m4a"

                # Map target format to preferredcodec and final extension
                preferred_codec = None
                if target_format == "mp3":
                    preferred_codec = "mp3"
                    final_extension = ".mp3"
                    temp_extension = ".mp3"  # FFmpeg usually outputs .mp3
                elif target_format in ("m4a", "aac"):
                    preferred_codec = "m4a"
                    final_extension = ".m4a"
                    temp_extension = ".m4a"  # FFmpeg usually outputs .m4a
                elif target_format in ("ogg", "vorbis"):
                    preferred_codec = "vorbis"
                    final_extension = ".ogg"
                    temp_extension = ".ogg"  # FFmpeg usually outputs .ogg
                elif target_format in ("webm", "opus"):
                    preferred_codec = "opus"
                    final_extension = ".webm"  # Final desired container
                    temp_extension = (
                        ".opus"  # FFmpegExtractAudio with opus codec outputs .opus
                    )

                if not preferred_codec:  # Should not happen with the fallback logic
                    raise ValueError(
                        f"Internal error: Could not determine preferred codec for target format '{target_format}'"
                    )

                # Determine target quality (bitrate)
                target_quality = "192"  # Default fallback quality
                if target_audio_params:
                    # Use the provided target audio parameters (e.g., "192k")
                    target_quality = target_audio_params.strip("kK")
                elif selected_audio_format and selected_audio_format.abr:
                    # Use the bitrate of the selected audio stream
                    target_quality = str(int(selected_audio_format.abr))
                    logger.info(
                        f"Audio download: Using source bitrate {target_quality}k for transcoding."
                    )
                else:
                    logger.warning(
                        f"Could not determine source bitrate for format {item.selected_audio_format_id}. Falling back to {target_quality}k."
                    )

                postprocessors_to_add = [
                    {
                        "key": "FFmpegExtractAudio",
                        "preferredcodec": preferred_codec,
                        "preferredquality": target_quality,
                    },
                    add_metadata_pp,  # Add metadata after conversion
                    embed_thumbnail_pp,  # Embed thumbnail after conversion
                ]
                
                # Add genre postprocessor only if enabled
                if ENABLE_CUSTOM_GENRE_PP:
                    logging.info("Adding custom genre postprocessor for audio download.")
                    postprocessors_to_add.append({
                        "key": "FFmpegGenre",
                        "when": "post_process"
                    })
                else:
                    logging.info("Custom genre postprocessor is disabled.")
                    
                ydl_opts["postprocessors"].extend(postprocessors_to_add)
                logger.info(
                    f"Audio download: Configured transcoding to {preferred_codec.upper()} (target container: {final_extension}) at {target_quality}k."
                )

            else:  # Video Download (is_video_merge or selected_video_format)
                # --- Video Download ---
                if target_format not in valid_video_formats:
                    logger.warning(
                        f"Invalid or unsupported target video format '{target_format}'. Falling back to 'mp4'. Valid: {valid_video_formats}"
                    )
                    target_format = "mp4"

                final_extension = f".{target_format}"
                temp_extension = final_extension

                # mp4 is handled by merge_output_format
                if target_format == "mp4":
                    # Remux only, no transcoding, do not add the merger postprocessor!!!
                    ydl_opts["merge_output_format"] = "mp4"
                else:
                    ydl_opts["postprocessors"].append({
                        "key": "FFmpegVideoRemuxer",
                        "preferedformat": target_format,  # Use the validated target format
                    })

                # Add metadata and thumbnail embedding
                # These should be after the remuxer to ensure they apply to the final file
                postprocessors_to_add = [
                    add_metadata_pp,
                    embed_thumbnail_pp,
                ]
                
                # Add genre postprocessor only if enabled
                if ENABLE_CUSTOM_GENRE_PP:
                    logging.info("Adding custom genre postprocessor for audio download.")
                    postprocessors_to_add.append({
                        "key": "FFmpegGenre",
                        "when": "post_process"
                    })
                else:
                    logging.info("Custom genre postprocessor is disabled.")

            # --- Determine Final Output Path ---
            artist_part = item.artist or "Unknown Artist"
            title_part = item.title or "Unknown Title"
            base_filename_raw = f"{artist_part} - {title_part}"
            safe_title_base = sanitize_filename(base_filename_raw)
            if final_extension == ".?":
                logger.error("Internal error: Final file extension was not determined.")
                final_extension = ".media"

            final_filename = f"{safe_title_base}{final_extension}"
            item.final_filepath = output_dir / final_filename

            # Check for existing final file *before* download
            if item.final_filepath.exists():
                logger.warning(
                    f"Output file already exists: '{item.final_filepath}'. Skipping."
                )
                _update_status("Skipped", "File already exists")
                item.progress = 100  # Mark as complete visually
                return  # Exit download process for this item

            # --- Execute Download ---
            _update_status("Downloading")
            logger.debug(f"Starting yt-dlp download with options: {ydl_opts}")
            loop = asyncio.get_event_loop()
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:  # type: ignore
                # Run download in executor
                await loop.run_in_executor(None, ydl.download, [item.url])

            # --- Post-Download Processing (Finding and Moving) ---
            _update_status("Processing")

            # Find the processed file
            # Use the *sanitized* base name for globbing, as yt-dlp uses the sanitized name
            # in the temp directory before post-processing might rename it.
            search_pattern = f"{safe_title_base}{temp_extension}"
            logger.debug(
                f"Searching for processed file in temp dir using pattern: '{search_pattern}'"
            )
            processed_files = list(temp_dir.glob(search_pattern))
            if not processed_files:
                # Fallback check: Use the pattern based on the *original* title template
                # This might catch cases where sanitization differs slightly or PP fails rename
                original_temp_pattern = (
                    sanitize_filename(item.title or "untitled") + temp_extension
                )
                processed_files = list(temp_dir.glob(original_temp_pattern))
                if processed_files:
                    logger.warning(
                        f"Could not find exact '{search_pattern}', but found matching original pattern: {processed_files[0]}. Using this."
                    )
                else:
                    # Last resort: Check for *any* file with the temp_extension
                    processed_files = list(temp_dir.glob(f"*{temp_extension}"))
                    if processed_files:
                        logger.warning(
                            f"Could not find exact name match, using first file with extension '{final_extension}': {processed_files[0]}"
                        )
                    else:
                        all_files = list(temp_dir.glob("*.*"))
                        logger.error(
                            f"Could not find expected file ('{search_pattern}' or fallbacks with ext '{temp_extension}') in temp dir '{temp_dir}'. Found: {all_files}"
                        )
                        raise FileNotFoundError(
                            f"Processed file ('*{temp_extension}') not found in temp dir."
                        )

            # Handle multiple matches (less likely now with specific extension)
            if len(processed_files) > 1:
                # Prefer the one matching the expected sanitized name if possible
                exact_match = [
                    f
                    for f in processed_files
                    if f.name == f"{safe_title_base}{temp_extension}"
                ]
                temp_download_path = (
                    exact_match[0] if exact_match else processed_files[0]
                )
                logger.warning(
                    f"Multiple files found matching '{temp_extension}', using: {temp_download_path}"
                )
            else:
                temp_download_path = processed_files[0]

            logger.debug(f"Processed file found: {temp_download_path}")

            # Move the final file (shutil.move handles the rename to final_filepath)
            logger.info(
                f"Moving '{temp_download_path.name}' to '{item.final_filepath}'"
            )
            shutil.move(str(temp_download_path), str(item.final_filepath))

            # --- Success ---
            _update_status("Complete")
            item.progress = 100  # Ensure progress hits 100% on success
            logger.info(
                f"Download successful for '{item.title}' -> '{item.final_filepath}'"
            )

        # Temp directory cleaned up automatically

    except FileNotFoundError as e:
        logger.error(f"Download failed for '{item.title}': {e}", exc_info=False)
        _update_status("Error", str(e))
        raise e
    except yt_dlp.utils.DownloadError as e:
        error_msg = str(e).split(":")[-1].strip()  # Get cleaner error message
        logger.error(
            f"yt-dlp DownloadError during download for '{item.title}': {error_msg}",
            exc_info=False,
        )
        _update_status("Error", f"yt-dlp: {error_msg}")
        raise e
    except asyncio.CancelledError:
        logger.warning(f"Download cancelled for '{item.title}'")
        _update_status("Cancelled", "User cancelled")
        raise  # Re-raise CancelledError so the caller knows
    except Exception as e:
        error_msg = str(e)
        logger.error(
            f"Unexpected download error for '{item.title}': {error_msg}", exc_info=True
        )
        _update_status("Error", f"Unexpected: {error_msg[:100]}")
        raise e
    finally:
        # Final status update is handled within the try/except blocks
        pass
