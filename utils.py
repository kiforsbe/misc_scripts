from __future__ import annotations

import os
import stat
import sys
from datetime import datetime
from pathlib import Path


class Colors:
    RESET = "\033[0m"
    DIM = "\033[2m"
    BLUE = "\033[34m"
    CYAN = "\033[36m"
    GREEN = "\033[32m"
    MAGENTA = "\033[35m"
    RED = "\033[31m"
    YELLOW = "\033[33m"


def should_use_color(force: bool | None = None) -> bool:
    if force is not None:
        return force
    if not sys.stdout.isatty():
        return False
    if os.name != "nt":
        return True
    return any(
        os.environ.get(name)
        for name in ("WT_SESSION", "ANSICON", "ConEmuANSI", "TERM")
    )


def colorize(text: str, color: str, enabled: bool) -> str:
    if not enabled:
        return text
    return f"{color}{text}{Colors.RESET}"


def format_size(size_bytes: int, human: bool = True) -> str:
    if not human:
        return str(size_bytes)
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    value = float(size_bytes)
    unit_index = 0
    while value >= 1024 and unit_index < len(units) - 1:
        value /= 1024.0
        unit_index += 1
    if unit_index == 0:
        return f"{int(value)} {units[unit_index]}"
    return f"{value:.1f} {units[unit_index]}"


def format_timestamp(timestamp: float | None) -> str:
    if timestamp is None:
        return "-"
    return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")


def format_age(timestamp: float | None, now: float | None = None) -> str:
    if timestamp is None:
        return "unknown"
    if now is None:
        now = datetime.now().timestamp()
    delta = max(0, int(now - timestamp))
    units = [
        (86400, "d"),
        (3600, "h"),
        (60, "m"),
        (1, "s"),
    ]
    for step, suffix in units:
        if delta >= step:
            return f"{delta // step}{suffix} ago"
    return "0s ago"


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


def icon_for_entry(entry_type: str, extension: str | None, use_icons: bool) -> str:
    if not use_icons:
        return ""
    if entry_type == "d":
        return "📁 "
    if extension in {".py", ".js", ".ts", ".json", ".csv", ".md"}:
        return "📄 "
    if extension in {".jpg", ".jpeg", ".png", ".gif", ".webp"}:
        return "🖼️ "
    if extension in {".mp3", ".flac", ".wav", ".m4a"}:
        return "🎵 "
    if extension in {".mp4", ".mkv", ".avi", ".mov"}:
        return "🎞️ "
    return "📄 "