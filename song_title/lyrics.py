from __future__ import annotations

import os
import re
import tempfile
from pathlib import Path

from .types import Chunk

_SENTENCE_END = re.compile(r'(?<=[.!?])\s+')
_WORD = re.compile(r'\S+')


def format_lyrics(chunks: list[Chunk], line_width: int = 48, stanza_lines: int = 4) -> str:
    """Reflow ASR text into readable lyric lines without changing its words."""
    text = ' '.join(' '.join(chunk.text.split()) for chunk in chunks if chunk.text.strip())
    if not text:
        return ''
    phrases = [phrase.strip() for phrase in _SENTENCE_END.split(text) if phrase.strip()]
    lines: list[str] = []
    for phrase in phrases:
        current = ''
        for word in _WORD.findall(phrase):
            candidate = f'{current} {word}'.strip()
            if current and len(candidate) > line_width:
                lines.append(current)
                current = word
            else:
                current = candidate
        if current:
            lines.append(current)
    stanzas = ['\n'.join(lines[i:i + stanza_lines]) for i in range(0, len(lines), stanza_lines)]
    return '\n\n'.join(stanzas)


def lyrics_file_path(audio_path: Path) -> Path:
    path = Path(audio_path)
    return path.with_name(path.stem + '.lyrics.txt')


def write_lyrics_file(audio_path: Path, lyrics: str, *, overwrite: bool = False) -> Path | None:
    """Atomically create an adjacent plain-text lyrics file; preserve it by default."""
    destination = lyrics_file_path(audio_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=f'.{destination.name}.', suffix='.tmp', dir=destination.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8', newline='\n') as stream:
            stream.write(lyrics.rstrip() + '\n')
        if overwrite:
            os.replace(temporary, destination)
        else:
            try:
                os.link(temporary, destination)
            except FileExistsError:
                return None
        return destination
    finally:
        temporary.unlink(missing_ok=True)
