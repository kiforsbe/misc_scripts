from __future__ import annotations

import json
import os
import re
import tempfile
import unicodedata
from difflib import SequenceMatcher
from pathlib import Path


def normalize_title(title: str) -> str:
    normalized = unicodedata.normalize('NFKC', title).casefold()
    return ' '.join(re.findall(r'\w+', normalized))


def similar_title(title: str, previous_titles: list[str], threshold: float = 0.9) -> str | None:
    normalized = normalize_title(title)
    if not normalized:
        return None
    for previous in previous_titles:
        previous_normalized = normalize_title(previous)
        if normalized == previous_normalized or SequenceMatcher(None, normalized, previous_normalized).ratio() >= threshold:
            return previous
    return None


def load_title_history(path: Path) -> list[str]:
    try:
        data = json.loads(path.read_text(encoding='utf-8'))
        if data.get('schema') == 1 and isinstance(data.get('titles'), list):
            return list(dict.fromkeys(title for title in data['titles'] if isinstance(title, str) and normalize_title(title)))
    except (OSError, json.JSONDecodeError, AttributeError):
        pass
    return []


def save_title_history(path: Path, titles: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(dir=path.parent, suffix='.tmp')
    try:
        with os.fdopen(descriptor, 'w', encoding='utf-8') as stream:
            json.dump({'schema': 1, 'titles': titles}, stream, ensure_ascii=False, indent=2)
        os.replace(temporary_name, path)
    finally:
        Path(temporary_name).unlink(missing_ok=True)
