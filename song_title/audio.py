from __future__ import annotations

import math
import re
from pathlib import Path
from typing import Iterable

from .types import Chunk

AUDIO_EXTENSIONS = {'.mp3', '.flac', '.m4a', '.mp4', '.ogg', '.opus', '.wav', '.aac', '.aiff', '.aif', '.wma', '.m4b'}
_TRANSCRIPT_WORD = re.compile(r"[^\W_]+(?:['’][^\W_]+)*", re.UNICODE)


def chunk_ranges(duration: float, seconds: float, overlap: float) -> list[tuple[float, float]]:
    if not all(math.isfinite(x) for x in (duration, seconds, overlap)) or duration < 0 or seconds <= 0 or not 0 <= overlap < seconds:
        raise ValueError('Invalid duration or overlap')
    ranges = []
    start = 0.0
    while start < duration:
        end = min(start + seconds, duration)
        ranges.append((start, end))
        if end == duration:
            break
        start = end - overlap
    return ranges


def _remove_repeated_overlap(previous: str, current: str, overlap_seconds: float) -> str:
    previous_words = list(_TRANSCRIPT_WORD.finditer(previous))
    current_words = list(_TRANSCRIPT_WORD.finditer(current))
    if len(previous_words) < 3 or len(current_words) < 3:
        return current

    max_words = min(len(previous_words), len(current_words), max(3, math.ceil(overlap_seconds * 6)))
    previous_tokens = [word.group().casefold().replace('’', "'") for word in previous_words]
    current_tokens = [word.group().casefold().replace('’', "'") for word in current_words]
    for count in range(max_words, 2, -1):
        if previous_tokens[-count:] != current_tokens[:count]:
            continue
        # A match that consumes an entire chunk is ambiguous: it could be a
        # genuinely repeated chorus rather than duplicated overlap text.
        if count == len(previous_words) or count == len(current_words):
            return current
        remainder = current[current_words[count].start():]
        return remainder.lstrip(' \t\r\n,.;:!?—–-')
    return current


def assemble_transcript(chunks: list[Chunk]) -> str:
    nonempty = [chunk for chunk in chunks if chunk.text.strip()]
    if nonempty and all(chunk.aligned_words for chunk in nonempty):
        words = [word.text.strip() for chunk in nonempty for word in chunk.aligned_words if word.text.strip()]
        if words:
            text = ''
            attach = set(',.;:!?%)]}’')
            for word in words:
                if text and word[0] not in attach and not word.startswith("'"):
                    text += ' '
                text += word
            return text
    lines = []
    previous_chunk = None
    for chunk in chunks:
        text = ' '.join(chunk.text.split())
        if text and previous_chunk is not None:
            overlap_seconds = previous_chunk.end - chunk.start
            if overlap_seconds > 0:
                text = _remove_repeated_overlap(previous_chunk.text, text, overlap_seconds)
        if text:
            lines.append(text)
        if chunk.text.strip():
            previous_chunk = chunk
    return '\n'.join(lines)


def discover_inputs(paths: Iterable[Path], recursive: bool, excluded: Iterable[Path]) -> list[Path]:
    excluded = [p.resolve() for p in excluded]
    result = []
    seen = set()
    for value in paths:
        path = Path(value)
        if not path.exists():
            raise ValueError(f'Input does not exist: {path}')
        files = sorted(path.rglob('*') if recursive else path.iterdir()) if path.is_dir() else [path]
        for file in files:
            resolved = file.resolve()
            if not file.is_file() or file.suffix.lower() not in AUDIO_EXTENSIONS:
                continue
            if any(resolved == root or resolved.is_relative_to(root) for root in excluded):
                continue
            if '.song-title-' in file.name or resolved in seen:
                continue
            seen.add(resolved)
            result.append(resolved)
    return result
