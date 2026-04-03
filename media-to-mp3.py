import argparse
import glob
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable, Optional

progress_bar_cls: Any
try:
    from tqdm import tqdm as progress_bar_cls
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

    class SimpleProgressBar:
        def __init__(self, total=None, desc=None, unit=None, disable=False, leave=True):
            self.total = float(total or 0)
            self.desc = desc or "Progress"
            self.disable = disable
            self.current = 0.0
            self._last_percent = -1
            if not self.disable:
                print(f"{self.desc}: 0%")

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            if not self.disable:
                print(f"{self.desc}: 100%")

        def update(self, amount):
            if self.disable:
                return

            self.current += amount
            if self.total <= 0:
                return

            percent = min(100, int((self.current / self.total) * 100))
            if percent != self._last_percent and (percent == 100 or percent % 5 == 0):
                self._last_percent = percent
                print(f"{self.desc}: {percent}%")

    progress_bar_cls = SimpleProgressBar


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert one or more media files to MP3 using the first audio track."
    )
    parser.add_argument(
        "files",
        nargs="+",
        help="One or more input media files or wildcard patterns to convert."
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite an existing output .mp3 file."
    )
    return parser.parse_args()


def check_dependencies() -> None:
    missing = [name for name in ("ffmpeg", "ffprobe") if shutil.which(name) is None]
    if missing:
        print(
            f"Error: Missing required tool(s): {', '.join(missing)}. Install FFmpeg and ensure both commands are on PATH.",
            file=sys.stderr,
        )
        raise SystemExit(1)


def iter_input_files(raw_paths: Iterable[str]) -> list[Path]:
    files: list[Path] = []
    missing: list[str] = []
    seen: set[str] = set()

    def add_path(path: Path) -> None:
        normalized = os.path.normcase(str(path.resolve()))
        if normalized in seen:
            return
        seen.add(normalized)

        if not path.is_file():
            print(f"Skipping non-file path: {path}", file=sys.stderr)
            return

        files.append(path)

    for raw_path in raw_paths:
        expanded_raw_path = str(Path(raw_path).expanduser())
        if glob.has_magic(expanded_raw_path):
            matches = sorted(Path(match) for match in glob.glob(expanded_raw_path, recursive=True))
            if not matches:
                missing.append(raw_path)
                continue

            for match in matches:
                add_path(match)
            continue

        path = Path(expanded_raw_path)
        if not path.exists():
            missing.append(raw_path)
            continue

        add_path(path)

    if missing:
        for raw_path in missing:
            print(f"Input file not found: {raw_path}", file=sys.stderr)
        raise SystemExit(1)

    if not files:
        print("Error: No valid input files were provided.", file=sys.stderr)
        raise SystemExit(1)

    return files


def get_duration_seconds(input_file: Path) -> Optional[float]:
    command = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "json",
        str(input_file),
    ]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        return None

    try:
        payload = json.loads(result.stdout)
        duration = payload.get("format", {}).get("duration")
        if duration is None:
            return None
        return max(0.0, float(duration))
    except (ValueError, json.JSONDecodeError, TypeError):
        return None


def format_ffmpeg_error(stderr: str) -> str:
    lines = [line.strip() for line in stderr.splitlines() if line.strip()]
    if not lines:
        return "ffmpeg failed without an error message"
    return lines[-1]


def convert_file(input_file: Path, force: bool = False) -> bool:
    output_file = input_file.with_suffix(".mp3")
    if output_file == input_file:
        print(f"Skipping {input_file.name}: input is already an .mp3 file")
        return True

    if output_file.exists() and not force:
        print(f"Skipping {input_file.name}: {output_file.name} already exists (use --force to overwrite)")
        return True

    duration_seconds = get_duration_seconds(input_file)
    progress_total = duration_seconds if duration_seconds and duration_seconds > 0 else None
    overwrite_flag = "-y" if force else "-n"
    command = [
        "ffmpeg",
        overwrite_flag,
        "-hide_banner",
        "-loglevel",
        "error",
        "-nostdin",
        "-i",
        str(input_file),
        "-map",
        "0:a:0",
        "-vn",
        "-progress",
        "pipe:1",
        str(output_file),
    ]

    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )

    last_position = 0.0
    progress_desc = f"Converting {input_file.name}"
    progress_kwargs = {
        "desc": progress_desc,
        "unit": "s",
        "disable": False,
        "leave": True,
    }
    if progress_total is not None:
        progress_kwargs["total"] = progress_total

    with progress_bar_cls(**progress_kwargs) as progress_bar:
        assert process.stdout is not None
        for line in process.stdout:
            key, _, value = line.strip().partition("=")
            if key not in {"out_time_ms", "out_time_us", "progress"}:
                continue

            if key == "progress" and value == "end":
                if progress_total is not None and last_position < progress_total:
                    progress_bar.update(progress_total - last_position)
                continue

            if key in {"out_time_ms", "out_time_us"}:
                try:
                    current_position = int(value) / 1_000_000
                except ValueError:
                    continue

                if progress_total is None:
                    continue

                bounded_position = min(progress_total, max(0.0, current_position))
                increment = bounded_position - last_position
                if increment > 0:
                    progress_bar.update(increment)
                    last_position = bounded_position

    stderr_output = ""
    if process.stderr is not None:
        stderr_output = process.stderr.read()

    return_code = process.wait()
    if return_code != 0:
        print(f"Failed: {input_file} -> {format_ffmpeg_error(stderr_output)}", file=sys.stderr)
        if output_file.exists() and force:
            try:
                output_file.unlink()
            except OSError:
                pass
        return False

    print(f"Created: {output_file}")
    return True


def main() -> int:
    args = parse_args()
    check_dependencies()
    input_files = iter_input_files(args.files)

    failures = 0
    for input_file in input_files:
        if not convert_file(input_file, force=args.force):
            failures += 1

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())