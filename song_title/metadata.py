from __future__ import annotations

import copy
import hashlib
import math
import os
import re
import shutil
import tempfile
from pathlib import Path

WRITABLE = {'.mp3', '.flac', '.m4a', '.mp4', '.ogg', '.opus'}
_GENERIC_TRACK_TITLE = re.compile(r'track[\s._-]*\d+', re.IGNORECASE)


def has_meaningful_title(value) -> bool:
    title = str(value or '').strip()
    return bool(title) and _GENERIC_TRACK_TITLE.fullmatch(title) is None


def fingerprint(path: Path) -> str:
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def read_metadata(path: Path) -> dict:
    from mutagen import File
    from mutagen.id3 import ID3
    result = {'filename': path.name}
    try:
        if path.suffix.lower() == '.mp3':
            tags = ID3(path)
            mapping = {'title':'TIT2','artist':'TPE1','albumartist':'TPE2','album':'TALB','genre':'TCON','tracknumber':'TRCK','discnumber':'TPOS','date':'TDRC'}
            for name, key in mapping.items():
                if key in tags:
                    result[name] = str(tags[key])
            for name, key in [('comments','COMM'), ('lyrics','USLT')]:
                values = tags.getall(key)
                if values:
                    result[name] = '\n'.join(str(v.text) for v in values)
            audio = File(path)
        else:
            audio = File(path)
            if audio:
                mapping = ({'title':'\xa9nam','artist':'\xa9ART','albumartist':'aART','album':'\xa9alb','genre':'\xa9gen','comments':'\xa9cmt','lyrics':'\xa9lyr','tracknumber':'trkn','discnumber':'disk','date':'\xa9day'}
                           if path.suffix.lower() in {'.m4a','.mp4','.m4b'} else
                           {'title':'title','artist':'artist','albumartist':'albumartist','album':'album','genre':'genre','comments':'comment','lyrics':'lyrics','tracknumber':'tracknumber','discnumber':'discnumber','date':'date'})
                for name, key in mapping.items():
                    if audio.tags and key in audio.tags:
                        values = audio.tags[key]
                        if path.suffix.lower() in {'.m4a', '.mp4', '.m4b'} and name in {'tracknumber', 'discnumber'}:
                            values = [f'{value[0]}/{value[1]}' if isinstance(value, (tuple, list)) and len(value) > 1 else str(value[0] if isinstance(value, (tuple, list)) else value) for value in values]
                        result[name] = '\n'.join(str(v) for v in values)
        if audio and audio.info:
            result['duration'] = float(audio.info.length)
    except Exception as exc:
        # Many inputs are decodable even when their tags are absent or invalid.
        result['metadata_warning'] = str(exc)
    return result


def can_write(path: Path) -> bool:
    return path.suffix.lower() in WRITABLE


def _load_tags(path):
    from mutagen import File
    from mutagen.id3 import ID3, ID3NoHeaderError
    if path.suffix.lower() == '.mp3':
        try:
            return ID3(path)
        except ID3NoHeaderError:
            return ID3()
    audio = File(path)
    if audio is None:
        raise ValueError(f'Unsupported audio: {path}')
    if audio.tags is None:
        audio.add_tags()
    return audio


def _other_tags(audio, key):
    from mutagen.id3 import ID3
    tags = audio if isinstance(audio, ID3) else audio.tags
    return copy.deepcopy({k: v for k, v in tags.items() if k != key})


def _verify_copy(path, title, original_other, title_key):
    from mutagen.id3 import ID3
    audio = _load_tags(path)
    tags = audio if isinstance(audio, ID3) else audio.tags
    actual = str(tags.get(title_key, '')) if isinstance(audio, ID3) else (tags.get(title_key) or [''])[0]
    if actual != title or _other_tags(audio, title_key) != original_other:
        raise ValueError('Title verification or unrelated-tag preservation failed')


def _backup_original(path: Path) -> Path:
    number = 0
    while True:
        suffix = '.bak' if number == 0 else f'.bak.{number}'
        backup = path.with_name(path.name + suffix)
        try:
            with backup.open('xb') as stream, path.open('rb') as source:
                shutil.copyfileobj(source, stream)
            shutil.copystat(path, backup)
            return backup
        except FileExistsError:
            number += 1


def update_metadata(path: Path, expected_digest: str, *, title: str | None = None,
                    lyrics: str | None = None, synchronized_lyrics=None,
                    make_backup: bool = False) -> Path | None:
    """Update tags through a verified copy; optionally retain the original as .bak."""
    from mutagen.id3 import ID3, SYLT, TIT2, USLT
    path = Path(path).resolve()
    if title is None and lyrics is None and synchronized_lyrics is None:
        raise ValueError('At least one metadata field must be updated')
    if synchronized_lyrics is not None and path.suffix.lower() != '.mp3':
        raise ValueError('Synchronized lyric metadata is only supported for MP3 ID3 tags; use an LRC sidecar')
    if title is not None:
        title = title.strip()
        if not title or len(title) > 250 or any(ord(c) < 32 for c in title):
            raise ValueError('Title must be 1–250 characters without control characters')
    if not can_write(path):
        raise ValueError(f'Title writing not supported for {path.suffix}')
    if fingerprint(path) != expected_digest:
        raise ValueError('Source changed since analysis; analyze it again')
    audio = _load_tags(path)
    is_mp3 = isinstance(audio, ID3)
    is_mp4 = path.suffix.lower() in {'.m4a', '.mp4', '.m4b'}
    title_key = 'TIT2' if is_mp3 else ('\xa9nam' if is_mp4 else 'title')
    lyrics_key = 'USLT' if is_mp3 else ('\xa9lyr' if is_mp4 else 'lyrics')
    ignored = {key for value, key in ((title, title_key), (lyrics, lyrics_key)) if value is not None}
    if synchronized_lyrics is not None:
        ignored.add('SYLT')
    tags = audio if is_mp3 else audio.tags
    other = copy.deepcopy({key: value for key, value in tags.items() if key not in ignored})
    original_version = audio.version[1] if isinstance(audio, ID3) else None
    backup = _backup_original(path) if make_backup else None
    fd, name = tempfile.mkstemp(prefix=f'.{path.stem}.song-title-', suffix=path.suffix, dir=path.parent)
    os.close(fd)
    temporary = Path(name)
    try:
        shutil.copy2(path, temporary)
        audio = _load_tags(temporary)
        if isinstance(audio, ID3):
            if title is not None:
                audio.setall('TIT2', [TIT2(encoding=3, text=[title])])
            if lyrics is not None:
                audio.setall('USLT', [USLT(encoding=3, lang='eng', desc='', text=lyrics)])
            if synchronized_lyrics is not None:
                timed_entries = [(word.text, max(0, round(word.start * 1000)))
                                 for word in synchronized_lyrics if word.text.strip()]
                if any(not math.isfinite(word.start) or not math.isfinite(word.end)
                       or word.start < 0 or word.end < word.start
                       for word in synchronized_lyrics):
                    raise ValueError('Synchronized lyrics contain invalid timestamps')
                if not timed_entries or any(current[1] < previous[1]
                                            for previous, current in zip(timed_entries, timed_entries[1:])):
                    raise ValueError('Synchronized lyrics must contain ordered word timestamps')
                audio.setall('SYLT', [SYLT(encoding=3, lang='eng', format=2, type=1,
                                           desc='Lyrics', text=timed_entries)])
            audio.save(temporary, v2_version=3 if original_version == 3 else 4)
        else:
            if title is not None:
                audio[title_key] = [title]
            if lyrics is not None:
                audio[lyrics_key] = [lyrics]
            audio.save()
        saved = _load_tags(temporary)
        saved_tags = saved if isinstance(saved, ID3) else saved.tags
        actual_other = {key: value for key, value in saved_tags.items() if key not in ignored}
        if actual_other != other:
            raise ValueError('Unrelated-tag preservation failed')
        if title is not None and lyrics is None and synchronized_lyrics is None:
            _verify_copy(temporary, title, other, title_key)
        if title is not None:
            actual_title = str(saved_tags.get(title_key, '')) if is_mp3 else str((saved_tags.get(title_key) or [''])[0])
            if actual_title != title:
                raise ValueError('Title verification failed')
        if lyrics is not None:
            actual_lyrics = str(saved_tags.get(lyrics_key, '')) if is_mp3 else str((saved_tags.get(lyrics_key) or [''])[0])
            if is_mp3:
                actual_lyrics = str(saved_tags.getall('USLT')[0].text) if saved_tags.getall('USLT') else ''
            if actual_lyrics != lyrics:
                raise ValueError('Lyrics verification failed')
        if synchronized_lyrics is not None:
            frames = saved_tags.getall('SYLT')
            actual_timed = [(text, int(timestamp)) for text, timestamp in frames[0].text] if frames else []
            expected_timed = [(word.text, max(0, round(word.start * 1000)))
                              for word in synchronized_lyrics if word.text.strip()]
            if actual_timed != expected_timed:
                raise ValueError('Synchronized lyrics verification failed')
        if fingerprint(path) != expected_digest:
            raise ValueError('Source changed during save; original preserved')
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return backup


def write_title(path: Path, title: str, expected_digest: str) -> Path:
    """Backward-compatible title writer that always retains the original copy."""
    backup = update_metadata(path, expected_digest, title=title, make_backup=True)
    assert backup is not None
    return backup
