from __future__ import annotations

import math
from pathlib import Path
from typing import Iterable

from .types import Chunk

AUDIO_EXTENSIONS = {'.mp3', '.flac', '.m4a', '.mp4', '.ogg', '.opus', '.wav', '.aac', '.aiff', '.aif', '.wma', '.m4b'}


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


def assemble_transcript(chunks: list[Chunk]) -> str:
    # Chunk offsets cannot locate individual words inside the shared audio.
    # Keep possible overlap duplicates rather than delete real repeated lyrics.
    return '\n'.join(' '.join(chunk.text.split()) for chunk in chunks if chunk.text.strip())


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
