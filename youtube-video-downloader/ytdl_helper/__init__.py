import logging

# Configure library-specific logging
# Set default NullHandler to avoid "No handler found" warnings.
# The application using the library should configure the actual logging handlers.
logging.getLogger(__name__).addHandler(logging.NullHandler())

# Expose key components for easier import
from .models import DownloadItem, FormatInfo
from .core import fetch_info, download_item
from .utils import sanitize_filename, check_ffmpeg

__version__ = "0.1.0" # Example version

__all__ = [
    "DownloadItem",
    "FormatInfo",
    "fetch_info",
    "download_item",
    "sanitize_filename",
    "check_ffmpeg",
]
