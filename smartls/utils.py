from __future__ import annotations

import os
import stat
from pathlib import Path


def format_permissions(mode: int | None) -> tuple[str | None, str | None]:
    if mode is None or os.name == "nt":
        return None, None
    return oct(stat.S_IMODE(mode)), stat.filemode(mode)


def display_path(path: Path, root: Path, absolute: bool = False, is_dir: bool = False) -> str:
    if absolute:
        text = str(path)
    else:
        try:
            relative = path.relative_to(root)
            text = path.name if str(relative) == "." else str(relative)
        except ValueError:
            text = str(path)
    if is_dir and not text.endswith(("/", "\\")):
        return f"{text}/"
    return text