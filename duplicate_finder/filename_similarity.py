"""Pure string-comparison helpers for duplicate-filename detection.

No filesystem access here — everything below takes and returns strings so it
can be unit tested in isolation from `similarity_engine.py`'s directory
scanning.
"""
from __future__ import annotations

import re
from pathlib import Path

from rapidfuzz import fuzz

_SEPARATOR_RE = re.compile(r"[._\-\s]+")

VIDEO_EXTENSIONS = {
    ".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv", ".webm",
    ".m4v", ".mpg", ".mpeg", ".ts", ".m2ts", ".3gp",
}
AUDIO_EXTENSIONS = {
    ".mp3", ".flac", ".wav", ".aac", ".m4a", ".ogg", ".wma", ".opus", ".alac",
}
IMAGE_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp", ".tiff", ".tif", ".heic", ".svg",
}
DOCUMENT_EXTENSIONS = {".pdf", ".doc", ".docx", ".txt", ".rtf", ".odt", ".md"}
ARCHIVE_EXTENSIONS = {".zip", ".rar", ".7z", ".tar", ".gz", ".bz2", ".cbr", ".cbz"}
SUBTITLE_EXTENSIONS = {".srt", ".ass", ".vtt", ".sub"}

_CATEGORY_BY_EXTENSION: dict[str, str] = {
    **{ext: "video" for ext in VIDEO_EXTENSIONS},
    **{ext: "audio" for ext in AUDIO_EXTENSIONS},
    **{ext: "image" for ext in IMAGE_EXTENSIONS},
    **{ext: "document" for ext in DOCUMENT_EXTENSIONS},
    **{ext: "archive" for ext in ARCHIVE_EXTENSIONS},
    **{ext: "subtitle" for ext in SUBTITLE_EXTENSIONS},
}


def normalize(name: str) -> str:
    stem = Path(name).stem
    collapsed = _SEPARATOR_RE.sub(" ", stem)
    return collapsed.strip().lower()


def extension_category(name: str) -> str:
    ext = Path(name).suffix.lower()
    return _CATEGORY_BY_EXTENSION.get(ext, "unknown")


def similarity(name_a: str, name_b: str) -> float:
    return fuzz.WRatio(normalize(name_a), normalize(name_b))


def categories_compatible(category_a: str, category_b: str) -> bool:
    if category_a == "unknown" or category_b == "unknown":
        return True
    return category_a == category_b


def matches_keyword_filters(name: str, include: list[str], exclude: list[str]) -> bool:
    lowered = name.lower()
    if any(keyword.lower() in lowered for keyword in exclude):
        return False
    if not include:
        return True
    return any(keyword.lower() in lowered for keyword in include)
