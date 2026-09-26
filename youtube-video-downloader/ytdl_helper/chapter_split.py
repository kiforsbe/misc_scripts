"""
Splits a downloaded audio file into one track per chapter marker, tagging
each track from its chapter title (e.g. a full-album upload or DJ mix whose
chapters are "01. Artist - Song").

Splitting is ffmpeg stream-copy (no re-encode), so it is fast and lossless;
tags on the full-length file (genre, description, cover art) carry over to
each track, with title/artist/album/track number overridden per chapter.
"""

import logging
import pathlib
import re
import subprocess
import threading
import zipfile
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import yt_dlp.utils

from .utils import sanitize_filename

logger = logging.getLogger(__name__)

# " - ", " – " or " — " between artist and title. Requires surrounding
# whitespace so hyphenated words ("Harder-Better") aren't split.
_ARTIST_TITLE_SEPARATOR = re.compile(r"\s+[-–—]\s+")

# Leading chapter numbering/timestamps that aren't part of the song name.
_LEADING_PREFIXES = (
    # "12:34 ", "1:02:03 - "
    re.compile(r"^(?:\d{1,2}:)?\d{1,2}:\d{2}\s*(?:[-–—|:.)]\s*)?"),
    # "[07] ", "(07) "
    re.compile(r"^[\[(]\d{1,3}[\])]\s*"),
    # "01. ", "3) " - punctuation required, so "99 Luftballons" is untouched
    re.compile(r"^\d{1,3}\s*[.)]\s+"),
    # "02 - " (two digits max, so a band like "311 - Amber" survives)
    re.compile(r"^\d{1,2}\s+[-–—:]\s+"),
)

_FULL_ALBUM_MARKERS = (
    re.compile(r"\s*[\(\[][^\)\]]*full\s+album[^\)\]]*[\)\]]", re.IGNORECASE),
    re.compile(r"\s*[-–—|]\s*full\s+album\s*$", re.IGNORECASE),
)

_TOPIC_CHANNEL_SUFFIX = re.compile(r"\s*-\s*Topic$", re.IGNORECASE)

# Containers where ffmpeg can stream-copy an attached cover image. Others
# (ogg/opus/webm) get audio only rather than failing the whole split.
_COVER_ART_EXTENSIONS = {".mp3", ".m4a", ".mp4"}


def parse_chapter_title(
    chapter_title: str, fallback_artist: Optional[str]
) -> Tuple[Optional[str], str]:
    """
    Returns (artist, title) for one chapter. "Artist - Title" is split on
    the first separator; anything else is taken as the title, with
    fallback_artist as the artist.
    """
    original = (chapter_title or "").strip()
    cleaned = original
    for prefix in _LEADING_PREFIXES:
        cleaned = prefix.sub("", cleaned, count=1).strip()
    if not cleaned:
        return fallback_artist, original

    parts = _ARTIST_TITLE_SEPARATOR.split(cleaned, maxsplit=1)
    if len(parts) == 2 and parts[0].strip() and parts[1].strip():
        return parts[0].strip(), parts[1].strip()
    return fallback_artist, cleaned


def derive_album_info(
    video_title: Optional[str], channel: Optional[str]
) -> Tuple[Optional[str], str]:
    """
    Returns (album_artist, album) for the whole video. Prefers an
    "Artist - Album (Full Album)" video title, since the uploading channel
    of an album upload often isn't the artist; otherwise falls back to the
    channel name (minus YouTube's auto-generated " - Topic" suffix).
    """
    original = (video_title or "").strip()
    cleaned = original
    for marker in _FULL_ALBUM_MARKERS:
        cleaned = marker.sub("", cleaned).strip()
    if not cleaned:
        cleaned = original

    parts = _ARTIST_TITLE_SEPARATOR.split(cleaned, maxsplit=1)
    if len(parts) == 2 and parts[0].strip() and parts[1].strip():
        return parts[0].strip(), parts[1].strip()

    album_artist = _TOPIC_CHANNEL_SUFFIX.sub("", channel).strip() if channel else None
    return album_artist or None, cleaned


@dataclass
class ChapterTrack:
    """One output track: a chapter's time range plus its tags."""
    number: int
    total: int
    start: float
    end: float
    artist: Optional[str]
    title: str
    album: str
    album_artist: Optional[str]
    year: Optional[int]

    @property
    def filename_stem(self) -> str:
        width = max(2, len(str(self.total)))
        name = f"{self.number:0{width}d} - "
        if self.artist:
            name += f"{self.artist} - "
        name += self.title
        return sanitize_filename(name)


def build_track_plan(
    chapters: Optional[Iterable[Dict[str, Any]]],
    video_title: Optional[str],
    channel: Optional[str],
    year: Optional[int],
) -> List[ChapterTrack]:
    """Maps yt-dlp chapter dicts (start_time/end_time/title) to tracks,
    dropping zero-length chapters."""
    album_artist, album = derive_album_info(video_title, channel)
    usable = [
        c for c in (chapters or [])
        if c.get("end_time") is not None
        and c.get("start_time") is not None
        and c["end_time"] > c["start_time"]
    ]

    tracks = []
    for number, chapter in enumerate(usable, start=1):
        artist, title = parse_chapter_title(
            chapter.get("title") or f"Track {number}", album_artist
        )
        tracks.append(
            ChapterTrack(
                number=number,
                total=len(usable),
                start=float(chapter["start_time"]),
                end=float(chapter["end_time"]),
                artist=artist,
                title=title,
                album=album,
                album_artist=album_artist,
                year=year,
            )
        )
    return tracks


def _split_command(
    ffmpeg_path: str, src: pathlib.Path, track: ChapterTrack, out_path: pathlib.Path
) -> List[str]:
    stream_maps = ["-map", "0:a"]
    if src.suffix.lower() in _COVER_ART_EXTENSIONS:
        stream_maps += ["-map", "0:v?"]

    tags = {
        "title": track.title,
        "artist": track.artist,
        "album": track.album,
        "album_artist": track.album_artist,
        "track": f"{track.number}/{track.total}",
        "date": str(track.year) if track.year else None,
    }
    tag_args = []
    for key, value in tags.items():
        if value:
            tag_args += ["-metadata", f"{key}={value}"]

    return [
        ffmpeg_path, "-hide_banner", "-loglevel", "error", "-y",
        "-ss", f"{track.start:.3f}",
        "-t", f"{track.end - track.start:.3f}",
        "-i", str(src),
        *stream_maps,
        "-map_metadata", "0",
        "-map_chapters", "-1",  # each track shouldn't carry the whole video's chapter list
        "-c", "copy",
        *tag_args,
        str(out_path),
    ]


def split_audio_by_chapters(
    src: pathlib.Path,
    tracks: List[ChapterTrack],
    out_dir: pathlib.Path,
    ffmpeg_path: str,
    cancel_event: Optional[threading.Event] = None,
) -> List[pathlib.Path]:
    """
    Writes one file per track into out_dir (same container as src) and
    returns their paths in track order. Checks cancel_event between tracks.

    Raises:
        yt_dlp.utils.DownloadCancelled: If cancel_event is set.
        RuntimeError: If ffmpeg fails on any track.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    ext = src.suffix.lower()
    paths = []
    for track in tracks:
        if cancel_event is not None and cancel_event.is_set():
            raise yt_dlp.utils.DownloadCancelled("Download cancelled by user request.")

        out_path = out_dir / f"{track.filename_stem}{ext}"
        logger.debug(f"Splitting chapter {track.number}/{track.total} -> '{out_path.name}'")
        result = subprocess.run(
            _split_command(ffmpeg_path, src, track, out_path),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        if result.returncode != 0:
            detail = (result.stderr or "").strip().splitlines()
            raise RuntimeError(
                f"ffmpeg failed to split chapter {track.number} '{track.title}': "
                f"{detail[-1] if detail else f'exit code {result.returncode}'}"
            )
        paths.append(out_path)
    return paths


def pack_tracks_zip(files: Iterable[pathlib.Path], zip_path: pathlib.Path) -> pathlib.Path:
    """Stores files flat (no directories) in a zip. Audio is already
    compressed, so entries are stored rather than deflated."""
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_STORED) as zf:
        for path in files:
            zf.write(path, arcname=path.name)
    return zip_path
