from __future__ import annotations

import os
import re
import tempfile
from difflib import SequenceMatcher
import textwrap
from pathlib import Path

from .types import Chunk, TimedWord

_WORD = re.compile(r"[^\W_]+(?:['’][^\W_]+)*", re.UNICODE)
_SENTENCE_END = re.compile(r'(?<=[.!?])\s+')
_TIMED_LINE_MAX_CHARS = 42
_TIMED_LINE_MAX_WORDS = 8
_TIMED_LINE_MAX_SECONDS = 6.0


def format_lyrics(source: list[Chunk] | str, line_width: int = 42, stanza_lines: int = 4) -> str:
    """Use simple wrapping only as a last-resort fallback when Ollama formatting fails."""
    if isinstance(source, str):
        text = ' '.join(source.split())
    else:
        text = ' '.join(' '.join(chunk.text.split()) for chunk in source if chunk.text.strip())
    if not text:
        return ''
    phrases = [phrase.strip() for phrase in _SENTENCE_END.split(text) if phrase.strip()]
    lines: list[str] = []
    for phrase in phrases:
        lines.extend(textwrap.wrap(phrase, width=line_width, break_long_words=False,
                                  break_on_hyphens=False) or [phrase])
    stanzas = ['\n'.join(lines[index:index + stanza_lines])
               for index in range(0, len(lines), stanza_lines)]
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


def _word_key(value: str) -> str:
    return value.casefold().replace('’', "'")


def map_timed_words_to_formatted_lyrics(original: str, formatted: str,
                                       words: list[TimedWord]) -> list[TimedWord]:
    """Carry ASR timings onto a lightly corrected Ollama lyric transcript."""
    source_tokens = list(_WORD.finditer(original))
    formatted_tokens = list(_WORD.finditer(formatted))
    if len(source_tokens) != len(words):
        raise ValueError('Aligned word count does not match the source transcript')
    source = [_word_key(token.group()) for token in source_tokens]
    target = [_word_key(token.group()) for token in formatted_tokens]
    matcher = SequenceMatcher(None, source, target, autojunk=False)
    if (not source or not target or matcher.ratio() < .80
            or not .88 <= len(target) / len(source) <= 1.12):
        raise ValueError('Ollama lyric corrections differ too much to preserve word timings')
    mapped: dict[int, TimedWord] = {}
    for operation, source_start, source_end, target_start, target_end in matcher.get_opcodes():
        if operation == 'equal':
            for source_index, target_index in zip(range(source_start, source_end),
                                                  range(target_start, target_end)):
                mapped[target_index] = TimedWord(formatted_tokens[target_index].group(),
                                                 words[source_index].start, words[source_index].end)
        elif operation == 'replace' and source_end > source_start:
            source_count = source_end - source_start
            target_count = target_end - target_start
            for offset, target_index in enumerate(range(target_start, target_end)):
                source_index = source_start + min(source_count - 1, offset * source_count // target_count)
                mapped[target_index] = TimedWord(formatted_tokens[target_index].group(),
                                                 words[source_index].start, words[source_index].end)
        elif operation == 'insert':
            if source_start < len(words):
                start = words[source_start].start
            elif words:
                start = words[-1].end
            else:
                raise ValueError('Cannot time inserted words without source timings')
            for target_index in range(target_start, target_end):
                mapped[target_index] = TimedWord(formatted_tokens[target_index].group(), start, start)
    if len(mapped) != len(formatted_tokens):
        raise ValueError('Could not map every formatted lyric word to source timing')
    return [mapped[index] for index in range(len(formatted_tokens))]


def _timed_lines(formatted_lyrics: str, words: list[TimedWord]):
    """Pair lyric phrases with timings, splitting long formatter lines into short cues."""
    word_index = 0
    result = []
    for line in formatted_lyrics.splitlines():
        tokens = list(_WORD.finditer(line))
        if not tokens:
            continue
        line_words = []
        for token in tokens:
            if word_index >= len(words) or _word_key(token.group()) != _word_key(words[word_index].text):
                raise ValueError('Formatted lyrics do not match aligned transcript words in order')
            line_words.append(words[word_index])
            word_index += 1
        group_start = 0
        for index in range(1, len(tokens)):
            current_text = line[tokens[group_start].start():tokens[index].end()]
            gap = line[tokens[index - 1].end():tokens[index].start()]
            elapsed = line_words[index].start - line_words[group_start].start
            sentence_end = bool(re.search(r'[.!?]["\'’)]*$', gap))
            pause = line_words[index].start - line_words[index - 1].end > 1.2
            if (len(current_text) > _TIMED_LINE_MAX_CHARS
                    or index - group_start >= _TIMED_LINE_MAX_WORDS
                    or elapsed > _TIMED_LINE_MAX_SECONDS
                    or sentence_end or pause):
                first = tokens[group_start]
                result.append((line[first.start():tokens[index].start()].strip(),
                               line_words[group_start].start, line_words[index - 1].end))
                group_start = index
        first = tokens[group_start]
        result.append((line[first.start():].strip(),
                       line_words[group_start].start, line_words[-1].end))
    if word_index != len(words) or not result:
        raise ValueError('Formatted lyrics do not contain every aligned transcript word')
    return result


def _lrc_time(seconds: float) -> str:
    centiseconds = max(0, round(seconds * 100))
    minutes, remainder = divmod(centiseconds, 6000)
    return f'[{minutes:02d}:{remainder // 100:02d}.{remainder % 100:02d}]'


def format_lrc(formatted_lyrics: str, words: list[TimedWord]) -> str:
    lines = _timed_lines(formatted_lyrics, words)
    return '\n'.join(f'{_lrc_time(start)}{text}' for text, start, _ in lines) + '\n'


def _subtitle_time(seconds: float, decimal_separator: str) -> str:
    milliseconds = max(0, round(seconds * 1000))
    hours, remainder = divmod(milliseconds, 3_600_000)
    minutes, remainder = divmod(remainder, 60_000)
    whole_seconds, millis = divmod(remainder, 1000)
    return f'{hours:02d}:{minutes:02d}:{whole_seconds:02d}{decimal_separator}{millis:03d}'


def format_srt(formatted_lyrics: str, words: list[TimedWord]) -> str:
    cues = []
    for number, (text, start, end) in enumerate(_timed_lines(formatted_lyrics, words), 1):
        end = max(end, start + 0.1)
        cues.append(f'{number}\n{_subtitle_time(start, ",")} --> {_subtitle_time(end, ",")}\n{text}')
    return '\n\n'.join(cues) + '\n'


def format_vtt(formatted_lyrics: str, words: list[TimedWord]) -> str:
    cues = ['WEBVTT', '']
    for text, start, end in _timed_lines(formatted_lyrics, words):
        end = max(end, start + 0.1)
        cues.extend((f'{_subtitle_time(start, ".")} --> {_subtitle_time(end, ".")}', text, ''))
    return '\n'.join(cues)


def timed_lyrics_file_path(audio_path: Path) -> Path:
    path = Path(audio_path)
    return path.with_name(path.stem + '.lyrics.lrc')


def write_timed_lyrics_file(audio_path: Path, lyrics: str, *, overwrite: bool = False) -> Path | None:
    destination = timed_lyrics_file_path(audio_path)
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
