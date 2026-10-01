from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import unicodedata
from pathlib import Path

from .titles import shorten_filename_fields, valid_filename_shortening
from .types import Settings

DEFAULT_FILENAME_TEMPLATE = '%album% - %artist% - $num(%tracknumber%,2) - %title%'
DEFAULT_MAX_FILENAME_LENGTH = 120
_FIELD = re.compile(r'%([A-Za-z0-9_]+)%')
_NUM = re.compile(r'\$num\(\s*(%[A-Za-z0-9_]+%)\s*,\s*(\d+)\s*\)')
_INVALID_COMPONENT = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
_SAFE_FILENAME_PUNCTUATION = frozenset(" _-.()[]'&,!+")


def _sanitize_component(value: str) -> str:
    value = _INVALID_COMPONENT.sub('', value)
    value = ''.join(char for char in value
                    if char.isalnum() or unicodedata.category(char).startswith('M') or char in _SAFE_FILENAME_PUNCTUATION)
    return value.strip(' .')


def _has_malformed_fields(value: str) -> bool:
    """Reject stray percent signs while allowing both delimiters of each %field%."""
    cursor = 0
    for match in _FIELD.finditer(value):
        if '%' in value[cursor:match.start()]:
            return True
        cursor = match.end()
    return '%' in value[cursor:]


def render_filename_template(template: str, metadata: dict, extension: str) -> Path:
    """Render the common MusicBrainz Picard titleformat subset used for filenames."""
    values = {str(key).casefold(): str(value).strip() for key, value in metadata.items() if value is not None}
    values['_extension'] = extension.lstrip('.')

    def value_for(field: str) -> str:
        value = values.get(field.casefold(), '')
        return _sanitize_component(value)

    def number(match: re.Match) -> str:
        field, width = match.group(1)[1:-1].casefold(), int(match.group(2))
        raw = values.get(field, '')
        found = re.match(r'\s*(\d+)', raw)
        return found.group(1).zfill(min(width, 12)) if found else ''

    rendered = _NUM.sub(number, template)
    if '$' in rendered or _has_malformed_fields(rendered):
        raise ValueError('Unsupported or malformed filename-template expression; use Picard-style %field% tags and $num(%tracknumber%,2)')
    unknown = {field.casefold() for field in _FIELD.findall(rendered)} - set(values)
    if unknown:
        raise ValueError('Unknown filename-template field(s): ' + ', '.join(sorted(unknown)))
    rendered = _FIELD.sub(lambda match: value_for(match.group(1)), rendered)
    rendered = rendered.replace('\\', '/')
    parts = []
    for part in rendered.split('/'):
        dash_parts = re.split(r'\s+-\s+', part)
        if any(not segment.strip() for segment in dash_parts):
            part = ' - '.join(segment.strip() for segment in dash_parts if segment.strip())
        part = _sanitize_component(part)
        part = re.sub(r'^[-_ ]+', '', part)
        if part in {'', '.', '..'}:
            raise ValueError('Filename template produced an empty or unsafe path component')
        if re.fullmatch(r'(?i)(con|prn|aux|nul|com[1-9]|lpt[1-9])(?:\..*)?', part):
            part = '_' + part
        parts.append(part)
    if not parts:
        raise ValueError('Filename template produced an empty path')
    relative = Path(*parts)
    if relative.is_absolute() or any(part == '..' for part in relative.parts):
        raise ValueError('Filename template must produce a relative path inside the source folder')
    return relative


class FilenameTooLongError(RuntimeError):
    pass


def _utf16_length(value: str) -> int:
    return len(value.encode('utf-16-le')) // 2


def _filename_cache_key(field: str, original: str, settings: Settings, max_length: int) -> str:
    identity = json.dumps([settings.ollama_host.rstrip('/'), settings.ollama_model, max_length, field, original], ensure_ascii=False)
    return hashlib.sha256(identity.encode('utf-8')).hexdigest()


def _read_filename_cache(path: Path) -> dict[str, str]:
    try:
        data = json.loads(path.read_text(encoding='utf-8'))
        if data.get('schema') == 1 and isinstance(data.get('entries'), dict):
            return {key: value for key, value in data['entries'].items() if isinstance(key, str) and isinstance(value, str)}
    except (OSError, json.JSONDecodeError, AttributeError):
        pass
    return {}


def _write_filename_cache(path: Path, entries: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(dir=path.parent, suffix='.tmp')
    try:
        with os.fdopen(descriptor, 'w', encoding='utf-8') as stream:
            json.dump({'schema': 1, 'entries': entries}, stream, ensure_ascii=False, indent=2)
        os.replace(temporary_name, path)
    finally:
        Path(temporary_name).unlink(missing_ok=True)


def _render_output_path(source: Path, metadata: dict, template: str) -> Path:
    source = Path(source).resolve()
    relative = render_filename_template(template, metadata, source.suffix)
    target = source.parent / relative
    if target.suffix.casefold() != source.suffix.casefold():
        target = target.with_name(target.name + source.suffix)
    # Final gate after field interpolation, template cleanup, and extension handling.
    safe_name = _sanitize_component(target.name)
    if not safe_name:
        raise ValueError('Filename template produced an empty filename after sanitization')
    target = target.with_name(safe_name)
    return target


def output_path(source: Path, metadata: dict, template: str, *, settings: Settings | None = None,
                max_length: int = DEFAULT_MAX_FILENAME_LENGTH) -> Path:
    """Render a short filename, using Ollama to shorten template fields when needed."""
    if max_length < 1:
        raise ValueError('Maximum filename length must be positive')
    target_length = max_length - min(20, max_length // 6)
    working = dict(metadata)
    number_fields = {match.group(1)[1:-1].casefold() for match in _NUM.finditer(template)}
    fields = list(dict.fromkeys(field.casefold() for field in _FIELD.findall(template)
                                if field.casefold() not in number_fields and field.casefold() != '_extension'))
    metadata_values = {str(key).casefold(): str(value).strip() for key, value in working.items() if value is not None}
    original_values = dict(metadata_values)
    cache_path = settings.cache_dir / 'filename-shortening.json' if settings is not None else None
    cache = _read_filename_cache(cache_path) if cache_path is not None else {}
    cache_keys = {}
    if settings is not None:
        for field in fields:
            original = original_values.get(field)
            if original:
                key = _filename_cache_key(field, original, settings, max_length)
                cache_keys[field] = key
                cached = cache.get(key)
                if cached:
                    if valid_filename_shortening(original, cached) and _utf16_length(cached) < _utf16_length(original):
                        working[field] = cached
                    else:
                        cache.pop(key, None)

    unproductive_fields = set()
    for _attempt in range(6):
        target = _render_output_path(source, working, template)
        if _utf16_length(target.name) <= target_length:
            return target
        if settings is None:
            if _utf16_length(target.name) <= max_length:
                return target
            raise FilenameTooLongError(f'filename is {_utf16_length(target.name)} characters; maximum is {max_length}')
        untried = [field for field in fields if metadata_values.get(field) and field not in unproductive_fields]
        ranked_fields = sorted(untried, key=lambda field: _utf16_length(str(working.get(field, metadata_values.get(field, '')))), reverse=True)
        overage = _utf16_length(target.name) - max_length
        shorten = {}
        selected_length = 0
        for field in ranked_fields:
            value = str(working.get(field, metadata_values.get(field, ''))).strip()
            if value:
                shorten[field] = value
                selected_length += _utf16_length(_sanitize_component(value))
                if selected_length >= overage:
                    break
        if not shorten:
            if _utf16_length(target.name) <= max_length:
                return target
            raise FilenameTooLongError(f'filename is too long ({_utf16_length(target.name)} > {max_length}) and has no shorten-able metadata fields')
        try:
            replacements = shorten_filename_fields(shorten, target.name, target_length, settings)
        except RuntimeError as exc:
            if _utf16_length(target.name) <= max_length:
                return target
            raise FilenameTooLongError(f'could not shorten filename below {max_length} characters: {exc}') from exc
        changed = False
        changed_fields = set()
        for field, value in replacements.items():
            safe_value = _sanitize_component(value)
            if safe_value and _utf16_length(safe_value) < _utf16_length(shorten[field]):
                working[field] = safe_value
                metadata_values[field] = safe_value
                changed = True
                changed_fields.add(field)
                key = cache_keys.get(field)
                if key is not None:
                    cache[key] = safe_value
        if changed and cache_path is not None:
            try:
                _write_filename_cache(cache_path, cache)
            except OSError:
                pass
        if not changed:
            unproductive_fields.update(shorten)
        else:
            unproductive_fields.update(set(shorten) - changed_fields)
            unproductive_fields.difference_update(changed_fields)
    target = _render_output_path(source, working, template)
    if _utf16_length(target.name) <= max_length:
        return target
    raise FilenameTooLongError(f'filename remains too long ({_utf16_length(target.name)} > {max_length}) after shortening')
