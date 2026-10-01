from __future__ import annotations

import re
from pathlib import Path

DEFAULT_FILENAME_TEMPLATE = '%album% - %artist% - $num(%tracknumber%,2) - %title%'
_FIELD = re.compile(r'%([A-Za-z0-9_]+)%')
_NUM = re.compile(r'\$num\(\s*(%[A-Za-z0-9_]+%)\s*,\s*(\d+)\s*\)')
_INVALID_COMPONENT = re.compile(r'[<>:"/\\|?*\x00-\x1f]')


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
        return _INVALID_COMPONENT.sub('_', value).strip(' .')

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
        part = _INVALID_COMPONENT.sub('_', part).strip(' .')
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


def output_path(source: Path, metadata: dict, template: str) -> Path:
    source = Path(source).resolve()
    relative = render_filename_template(template, metadata, source.suffix)
    target = source.parent / relative
    if target.suffix.casefold() != source.suffix.casefold():
        target = target.with_name(target.name + source.suffix)
    return target
