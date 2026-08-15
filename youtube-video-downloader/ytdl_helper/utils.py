import re
import os
import shutil
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def sanitize_filename(filename: str) -> str:
    """Sanitize filename for cross-platform compatibility, focusing on Windows.

    Always produces a single flat filename component - it strips every
    path separator rather than trying to sanitize-while-preserving a
    multi-segment path, since the latter is what let a crafted video
    title (e.g. starting with '\\') survive as a drive-relative path and
    escape the intended output directory when joined onto it.
    """
    if not filename:  # Handle empty input
        return "_untitled_"

    # Remove invalid characters (Windows), including both path separators.
    filename = re.sub(r'[<>:"/\\|?*]', "_", filename)
    # Remove all control characters (0-31) and DEL (127). Tab, newline, and
    # carriage return are invalid in filenames (notably on Windows, where they
    # trigger WinError 123), so they must be stripped too.
    filename = re.sub(r"[\x00-\x1F\x7F]", "", filename)

    filename = _sanitize_part(filename)
    filename = _limit_component_length(filename)

    # Ensure it's not empty after sanitization
    if not filename or filename.isspace():
        return "_sanitized_"

    return filename


def _sanitize_part(part: str) -> str:
    """Sanitizes a single path component (directory or filename)."""
    if not part:
        return ""
    # Strip leading/trailing dots and spaces
    part = part.strip(". ")
    # Replace reserved names (Windows) - case-insensitive
    reserved_names = {
        "CON",
        "PRN",
        "AUX",
        "NUL",
        "COM1",
        "COM2",
        "COM3",
        "COM4",
        "COM5",
        "COM6",
        "COM7",
        "COM8",
        "COM9",
        "LPT1",
        "LPT2",
        "LPT3",
        "LPT4",
        "LPT5",
        "LPT6",
        "LPT7",
        "LPT8",
        "LPT9",
    }
    if part.upper() in reserved_names:
        part = f"_{part}_"
    return part


def _limit_component_length(basename: str, max_len: int = 200) -> str:
    """Limits the length of a filename component (basename)."""
    if len(basename) > max_len:
        name, ext = os.path.splitext(basename)
        # Ensure max_len accounts for the extension and the dot
        allowed_name_len = max_len - len(ext) - (1 if ext else 0)
        if allowed_name_len < 1:  # Handle cases where extension is too long
            return basename[:max_len]  # Just truncate the whole thing
        base_name = name[:allowed_name_len] + ext
        logger.debug(
            f"Filename component '{basename}' truncated to '{base_name}' (max_len={max_len})"
        )
        return base_name
    return basename


def check_ffmpeg() -> Optional[str]:
    """Check if FFmpeg is available in the system PATH and return its path."""
    ffmpeg_path = shutil.which("ffmpeg")
    if not ffmpeg_path:
        logger.error("FFmpeg not found in system PATH.")
        return None
    logger.debug(f"FFmpeg found at: {ffmpeg_path}")
    return ffmpeg_path
