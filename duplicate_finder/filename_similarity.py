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
_TAG_RE = re.compile(r"[\(\[][^\)\]]*[\)\]]")

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


def _collapse_and_lower(text: str) -> str:
    collapsed = _SEPARATOR_RE.sub(" ", text)
    return collapsed.strip().lower()


def normalize(name: str) -> str:
    return _collapse_and_lower(Path(name).stem)


def core_title(name: str) -> str:
    """Normalized name with bracketed release tags -- (USA), [Rev 1], etc. --
    removed, leaving just the title. Two files are almost always the same
    release iff their core titles are identical; the tags are exactly what
    varies between region/language/revision copies of the same release."""
    stem = Path(name).stem
    without_tags = _TAG_RE.sub(" ", stem)
    return _collapse_and_lower(without_tags)


def core_similarity(name_a: str, name_b: str) -> float:
    return fuzz.WRatio(core_title(name_a), core_title(name_b))


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
