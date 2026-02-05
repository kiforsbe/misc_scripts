import argparse
from importlib import metadata as importlib_metadata
import re
import sys

from packaging.version import InvalidVersion, Version, parse as parse_version


ANSI_RESET = "\x1b[0m"
ANSI_RED = "\x1b[31m"
ANSI_YELLOW = "\x1b[33m"
ANSI_GREEN = "\x1b[32m"


def read_input_text(arg_text: str | None) -> str:
    if arg_text:
        return arg_text
    if not sys.stdin.isatty():
        return sys.stdin.read()
    raise SystemExit(
        "No input provided. Pipe text into stdin or pass a string argument."
    )


def split_local(version: str) -> tuple[str, str | None]:
    try:
        parsed = Version(version)
    except InvalidVersion:
        if "+" in version:
            base, local = version.split("+", 1)
            return base, local
        return version, None
    return parsed.public, parsed.local


def colorize(text: str, color: str, enable: bool) -> str:
    if not enable:
        return text
    return f"{color}{text}{ANSI_RESET}"


def extract_pairs(text: str) -> list[tuple[str, str]]:
    match = re.search(r"Would install\s+(.+)", text)
    if match:
        candidate = match.group(1)
    else:
        candidate = text

    pairs: list[tuple[str, str]] = []

    collecting_matches = re.findall(
        r"Collecting\s+([A-Za-z0-9_.-]+)==([^\s]+)", candidate
    )
    if collecting_matches:
        pairs.extend(collecting_matches)
        return pairs

    for pkg, ver in re.findall(
        r"\b([A-Za-z0-9_.-]+)-(\d[0-9A-Za-z.+-]*)\b", candidate
    ):
        if any(suffix in ver for suffix in (".whl", ".tar", ".zip", ".metadata")):
            continue
        pairs.append((pkg, ver))

    return pairs


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare proposed package versions against installed ones."
    )
    parser.add_argument(
        "text",
        nargs="?",
        help="Text containing package-version pairs (e.g., pip output).",
    )
    parser.add_argument(
        "--no-color",
        action="store_true",
        help="Disable ANSI color output.",
    )
    args = parser.parse_args()

    text = read_input_text(args.text)
    use_color = sys.stdout.isatty() and not args.no_color

    # Extract package-version pairs
    pairs = extract_pairs(text)

    print(f"{'Package':25} {'Current':15} {'Proposed':15}")
    print("-" * 60)

    for pkg, new_ver in pairs:
        try:
            current_ver = importlib_metadata.version(pkg)
        except importlib_metadata.PackageNotFoundError:
            current_ver = "Not installed"
        display_current = current_ver
        display_new = new_ver

        if current_ver != "Not installed":
            base_current, local_current = split_local(current_ver)
            base_new, local_new = split_local(new_ver)

            if parse_version(base_new) < parse_version(base_current):
                display_new = colorize(display_new, ANSI_RED, use_color)
            elif base_new == base_current and local_new != local_current:
                display_new = colorize(display_new, ANSI_YELLOW, use_color)
            elif parse_version(base_new) > parse_version(base_current):
                display_new = colorize(display_new, ANSI_GREEN, use_color)

        print(f"{pkg:25} {display_current:15} {display_new:15}")


if __name__ == "__main__":
    main()
