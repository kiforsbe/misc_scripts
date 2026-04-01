from __future__ import annotations

import argparse
import csv
import datetime
import fnmatch
import hashlib
import json
import mimetypes
import os
import re
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Callable, Iterable, Sequence, cast

from utils import (
    Colors,
    colorize,
    display_path,
    format_age,
    format_permissions,
    format_size,
    format_timestamp,
    icon_for_entry,
    should_use_color,
)

try:
    import grp
    import pwd
except ImportError:
    grp = None
    pwd = None


NUMERIC_TYPE = int | float
SECONDS_PER_UNIT = {
    "s": 1,
    "m": 60,
    "h": 3600,
    "d": 86400,
    "w": 604800,
}
SIZE_UNITS = {
    "B": 1,
    "KB": 1024,
    "MB": 1024**2,
    "GB": 1024**3,
    "TB": 1024**4,
}


@dataclass(slots=True)
class ScanError:
    path: Path
    message: str


@dataclass(slots=True)
class Entry:
    path: Path
    name: str
    entry_type: str
    depth: int
    parent: Path | None
    size_bytes: int
    raw_size_bytes: int
    created_ts: float | None
    modified_ts: float | None
    accessed_ts: float | None
    permissions_octal: str | None
    permissions_text: str | None
    owner: str | None
    group: str | None
    is_symlink: bool
    symlink_target: str | None
    mime_type: str | None
    hash_md5: str | None = None
    hash_sha256: str | None = None
    direct_files: int = 0
    direct_dirs: int = 0
    direct_children: int = 0
    recursive_files: int = 0
    deepest_nesting: int = 0
    is_empty: bool = False
    is_sparse: bool = False
    children: list[Path] = field(default_factory=list)

    def to_dict(self, root: Path, absolute: bool = False) -> dict[str, object]:
        return {
            "name": self.name,
            "path": display_path(self.path, root, absolute=absolute, is_dir=self.entry_type == "d"),
            "absolute_path": str(self.path),
            "type": self.entry_type,
            "extension": self.path.suffix.lower() or None,
            "depth": self.depth,
            "size_bytes": self.size_bytes,
            "raw_size_bytes": self.raw_size_bytes,
            "created_ts": self.created_ts,
            "modified_ts": self.modified_ts,
            "accessed_ts": self.accessed_ts,
            "created": format_timestamp(self.created_ts),
            "modified": format_timestamp(self.modified_ts),
            "accessed": format_timestamp(self.accessed_ts),
            "permissions_octal": self.permissions_octal,
            "permissions_text": self.permissions_text,
            "owner": self.owner,
            "group": self.group,
            "is_symlink": self.is_symlink,
            "symlink_target": self.symlink_target,
            "mime_type": self.mime_type,
            "hash_md5": self.hash_md5,
            "hash_sha256": self.hash_sha256,
            "direct_files": self.direct_files,
            "direct_dirs": self.direct_dirs,
            "direct_children": self.direct_children,
            "recursive_files": self.recursive_files,
            "deepest_nesting": self.deepest_nesting,
            "is_empty": self.is_empty,
            "is_sparse": self.is_sparse,
        }


@dataclass(slots=True)
class ScanResult:
    root: Path
    entries: dict[Path, Entry]
    errors: list[ScanError]


@dataclass(slots=True)
class SummaryStats:
    folders_scanned: int
    folders_matched: int
    files_listed: int
    total_size_bytes: int
    avg_files_per_folder: float
    emptiest_folder: str | None
    largest_file: str | None


ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*m")
NAME_COLUMN_MAX_WIDTH = 64
SMARTLS_DIRECTORY_COLOR = Colors.CYAN
CONSOLE_COLUMN_SPECS: dict[str, dict[str, object]] = {
    "type": {"header": "Type", "align": "left", "max_width": 9},
    "size": {"header": "Size", "align": "right", "max_width": 12},
    "modified": {"header": "Modified", "align": "left", "max_width": 19},
    "created": {"header": "Created", "align": "left", "max_width": 19},
    "accessed": {"header": "Accessed", "align": "left", "max_width": 19},
    "children": {"header": "Children", "align": "right", "max_width": 8},
    "recursive_files": {"header": "Recursive files", "align": "right", "max_width": 15},
    "mime": {"header": "Mime", "align": "left", "max_width": 24},
    "extension": {"header": "Extension", "align": "left", "max_width": 12},
    "relative_path": {"header": "Relative path", "align": "left", "max_width": 44},
    "full_path": {"header": "Full path", "align": "left", "max_width": 56},
    "owner": {"header": "Owner", "align": "left", "max_width": 16},
    "group": {"header": "Group", "align": "left", "max_width": 16},
    "permissions": {"header": "Permissions", "align": "left", "max_width": 12},
}
CONSOLE_COLUMN_ALIASES = {
    "recursivefiles": "recursive_files",
    "relativepath": "relative_path",
    "fullpath": "full_path",
    "path": "relative_path",
}


class SmartLSJSONEncoder(json.JSONEncoder):
    def default(self, obj: object) -> object:
        if isinstance(obj, Path):
            return str(obj)
        if isinstance(obj, Entry):
            return asdict(obj)
        if isinstance(obj, ScanError):
            return asdict(obj)
        return super().default(obj)


def parse_numeric_value(token: str) -> NUMERIC_TYPE:
    if re.fullmatch(r"-?\d+", token):
        return int(token)
    return float(token)


def parse_numeric_expr(expr: str) -> Callable[[NUMERIC_TYPE], bool]:
    expr = expr.strip()
    approx_match = re.fullmatch(r"~\s*(-?\d+(?:\.\d+)?)\s*(?:±|\+/-)\s*(\d+(?:\.\d+)?)", expr)
    if approx_match:
        center = float(approx_match.group(1))
        delta = float(approx_match.group(2))
        return lambda value: center - delta <= value <= center + delta

    modulo_match = re.fullmatch(r"%\s*(\d+)", expr)
    if modulo_match:
        divisor = int(modulo_match.group(1))
        if divisor == 0:
            raise ValueError("Modulo divisor cannot be zero")
        return lambda value: value % divisor == 0

    range_match = re.fullmatch(r"\s*(-?\d+(?:\.\d+)?)\s*\.\.\s*(-?\d+(?:\.\d+)?)\s*", expr)
    if range_match:
        lower = parse_numeric_value(range_match.group(1))
        upper = parse_numeric_value(range_match.group(2))
        if lower > upper:
            raise ValueError("Range lower bound cannot exceed upper bound")
        return lambda value: lower <= value <= upper

    if "," in expr:
        values = {parse_numeric_value(part.strip()) for part in expr.split(",") if part.strip()}
        if not values:
            raise ValueError("Empty enumeration expression")
        return lambda value: value in values

    comparison_match = re.fullmatch(r"(<=|>=|!=|=|<|>)\s*(-?\d+(?:\.\d+)?)", expr)
    if comparison_match:
        operator = comparison_match.group(1)
        threshold = parse_numeric_value(comparison_match.group(2))
        if operator == "=":
            return lambda value: value == threshold
        if operator == "!=":
            return lambda value: value != threshold
        if operator == ">":
            return lambda value: value > threshold
        if operator == ">=":
            return lambda value: value >= threshold
        if operator == "<":
            return lambda value: value < threshold
        return lambda value: value <= threshold

    if re.fullmatch(r"-?\d+(?:\.\d+)?", expr):
        exact = parse_numeric_value(expr)
        return lambda value: value == exact

    raise ValueError(f"Unsupported numeric expression: {expr}")


def normalize_size_token(token: str) -> int:
    match = re.fullmatch(r"(-?\d+(?:\.\d+)?)(B|KB|MB|GB|TB)?", token.strip(), re.IGNORECASE)
    if not match:
        raise ValueError(f"Invalid size token: {token}")
    value = float(match.group(1))
    unit = (match.group(2) or "B").upper()
    return int(value * SIZE_UNITS[unit])


def normalize_duration_token(token: str) -> float:
    match = re.fullmatch(r"(-?\d+(?:\.\d+)?)([smhdw])", token.strip(), re.IGNORECASE)
    if not match:
        raise ValueError(f"Invalid time token: {token}")
    value = float(match.group(1))
    unit = match.group(2).lower()
    return value * SECONDS_PER_UNIT[unit]


def _transform_numeric_expr(expr: str, value_transform: Callable[[str], NUMERIC_TYPE]) -> str:
    approx_match = re.fullmatch(r"~\s*([^±]+?)\s*(?:±|\+/-)\s*(.+)", expr)
    if approx_match:
        left = value_transform(approx_match.group(1).strip())
        right = value_transform(approx_match.group(2).strip())
        return f"~{left}±{right}"

    modulo_match = re.fullmatch(r"%\s*(.+)", expr)
    if modulo_match:
        value = value_transform(modulo_match.group(1).strip())
        if int(value) != value:
            raise ValueError("Modulo expressions require integer values")
        return f"%{int(value)}"

    range_match = re.fullmatch(r"(.+?)\.\.(.+)", expr)
    if range_match:
        return f"{value_transform(range_match.group(1).strip())}..{value_transform(range_match.group(2).strip())}"

    comparison_match = re.fullmatch(r"(<=|>=|!=|=|<|>)(.+)", expr)
    if comparison_match:
        return f"{comparison_match.group(1)}{value_transform(comparison_match.group(2).strip())}"

    if "," in expr:
        return ",".join(str(value_transform(part.strip())) for part in expr.split(","))

    return str(value_transform(expr.strip()))


def parse_size_expr(expr: str) -> Callable[[NUMERIC_TYPE], bool]:
    return parse_numeric_expr(_transform_numeric_expr(expr, normalize_size_token))


def parse_time_expr(expr: str) -> Callable[[NUMERIC_TYPE], bool]:
    return parse_numeric_expr(_transform_numeric_expr(expr, normalize_duration_token))


class Filter:
    def apply(self, entry: Entry) -> bool:
        raise NotImplementedError


class PredicateFilter(Filter):
    def __init__(self, predicate: Callable[[Entry], bool]):
        self.predicate = predicate

    def apply(self, entry: Entry) -> bool:
        return self.predicate(entry)


class NotFilter(Filter):
    def __init__(self, inner: Filter):
        self.inner = inner

    def apply(self, entry: Entry) -> bool:
        return not self.inner.apply(entry)


class FilterFactory:
    FILTER_ARITY = {
        "--count": 1,
        "--files": 1,
        "--dirs": 1,
        "--size": 1,
        "--mtime": 1,
        "--ctime": 1,
        "--name": 1,
        "--ext": 1,
        "--depth-filter": 1,
        "--type": 1,
        "--empty": 0,
        "--sparse": 0,
        "--or": 0,
        "--not": 0,
    }

    NON_FILTER_ARITY = {
        "--depth": 1,
        "--sort": 1,
        "--limit": 1,
        "--group-by": 1,
        "--columns": 1,
        "--color": 0,
        "--no-color": 0,
        "--icons": 0,
        "--long": 0,
        "--short": 0,
        "--relative": 0,
        "--absolute": 0,
        "--human": 0,
        "--bytes": 0,
        "--flat": 0,
        "--json": 0,
        "--csv": 0,
        "--stats": 0,
        "--skip-errors": 0,
        "--show-errors": 0,
    }

    @classmethod
    def from_argv(cls, argv: Sequence[str]) -> list[list[Filter]]:
        groups: list[list[Filter]] = [[]]
        negate_next = False
        index = 0

        while index < len(argv):
            token = argv[index]
            if token == "--or":
                groups.append([])
            elif token == "--not":
                negate_next = not negate_next
            elif token in cls.FILTER_ARITY:
                arity = cls.FILTER_ARITY[token]
                values = list(argv[index + 1:index + 1 + arity])
                if len(values) != arity:
                    raise ValueError(f"Missing value for {token}")
                current = cls.build(token, values)
                if negate_next:
                    current = NotFilter(current)
                    negate_next = False
                groups[-1].append(current)
                index += arity
            elif token == "--hash":
                if index + 1 < len(argv) and not argv[index + 1].startswith("-"):
                    index += 1
            elif token in cls.NON_FILTER_ARITY:
                index += cls.NON_FILTER_ARITY[token]
            index += 1

        if negate_next:
            raise ValueError("--not must be followed by a filter")

        return [group for group in groups if group]

    @staticmethod
    def build(flag: str, values: Sequence[str]) -> Filter:
        value = values[0] if values else None
        if flag == "--count":
            matcher = parse_numeric_expr(_require_filter_value(flag, value))
            return PredicateFilter(lambda entry: entry.entry_type == "d" and matcher(entry.direct_children))
        if flag == "--files":
            raw_value = _require_filter_value(flag, value)
            matcher = parse_numeric_expr(raw_value)
            return PredicateFilter(lambda entry: entry.entry_type == "d" and matcher(entry.recursive_files))
        if flag == "--dirs":
            matcher = parse_numeric_expr(_require_filter_value(flag, value))
            return PredicateFilter(lambda entry: entry.entry_type == "d" and matcher(entry.direct_dirs))
        if flag == "--size":
            matcher = parse_size_expr(_require_filter_value(flag, value))
            return PredicateFilter(lambda entry: matcher(entry.size_bytes))
        if flag == "--mtime":
            matcher = parse_time_expr(_require_filter_value(flag, value))
            return PredicateFilter(lambda entry: matcher(age_seconds(entry.modified_ts)))
        if flag == "--ctime":
            matcher = parse_time_expr(_require_filter_value(flag, value))
            return PredicateFilter(lambda entry: matcher(age_seconds(entry.created_ts)))
        if flag == "--name":
            pattern = _require_filter_value(flag, value)
            return PredicateFilter(lambda entry: fnmatch.fnmatch(entry.name, pattern))
        if flag == "--ext":
            allowed = {normalize_extension(part) for part in _require_filter_value(flag, value).split(",") if part.strip()}
            return PredicateFilter(lambda entry: entry.entry_type == "f" and normalize_extension(entry.path.suffix) in allowed)
        if flag == "--depth-filter":
            matcher = parse_numeric_expr(_require_filter_value(flag, value))
            return PredicateFilter(lambda entry: matcher(entry.depth))
        if flag == "--type":
            wanted = _require_filter_value(flag, value).lower()
            return PredicateFilter(lambda entry: entry.entry_type == wanted)
        if flag == "--empty":
            return PredicateFilter(lambda entry: entry.entry_type == "d" and entry.recursive_files == 0)
        if flag == "--sparse":
            return PredicateFilter(lambda entry: entry.entry_type == "d" and entry.recursive_files <= 3)
        raise ValueError(f"Unknown filter flag: {flag}")


def _require_filter_value(flag: str, value: str | None) -> str:
    if value is None:
        raise ValueError(f"Missing value for {flag}")
    return value


def normalize_extension(extension: str) -> str:
    if not extension:
        return ""
    return extension.lower() if extension.startswith(".") else f".{extension.lower()}"


def age_seconds(timestamp: float | None) -> float:
    if timestamp is None:
        return float("inf")
    return max(0.0, time_now() - timestamp)


def time_now() -> float:
    return time.time()


class DirectoryScanner:
    def __init__(self, root: Path, max_depth: int | None, hash_mode: str | None):
        self.root = root
        self.max_depth = max_depth
        self.hash_mode = hash_mode
        self.entries: dict[Path, Entry] = {}
        self.errors: list[ScanError] = []

    def scan(self) -> ScanResult:
        self._scan_path(self.root, 0, None)
        return ScanResult(root=self.root, entries=self.entries, errors=self.errors)

    def _scan_path(self, path: Path, depth: int, parent: Path | None) -> Entry | None:
        try:
            stat_result = path.lstat()
        except OSError as exc:
            self.errors.append(ScanError(path=path, message=str(exc)))
            return None

        is_symlink = path.is_symlink()
        symlink_target = None
        if is_symlink:
            try:
                symlink_target = os.readlink(path)
            except OSError:
                symlink_target = None

        is_dir = path.is_dir() and not is_symlink
        entry_type = "d" if is_dir else "f"
        permissions_octal, permissions_text = format_permissions(stat_result.st_mode)
        owner, group_name = self._owner_group(stat_result)
        entry = Entry(
            path=path,
            name=path.name or str(path),
            entry_type=entry_type,
            depth=depth,
            parent=parent,
            size_bytes=stat_result.st_size if entry_type == "f" else 0,
            raw_size_bytes=stat_result.st_size,
            created_ts=getattr(stat_result, "st_ctime", None),
            modified_ts=getattr(stat_result, "st_mtime", None),
            accessed_ts=getattr(stat_result, "st_atime", None),
            permissions_octal=permissions_octal,
            permissions_text=permissions_text,
            owner=owner,
            group=group_name,
            is_symlink=is_symlink,
            symlink_target=symlink_target,
            mime_type=mimetypes.guess_type(path.name)[0],
        )
        self.entries[path] = entry

        if entry.entry_type == "f":
            self._maybe_hash(entry)
            return entry

        if self.max_depth is None or depth < self.max_depth:
            try:
                children = sorted(path.iterdir(), key=lambda child: child.name.lower())
            except OSError as exc:
                self.errors.append(ScanError(path=path, message=str(exc)))
                children = []
        else:
            children = []

        for child in children:
            child_entry = self._scan_path(child, depth + 1, path)
            if child_entry is not None:
                entry.children.append(child_entry.path)

        self._finalize_directory(entry)
        return entry

    def _finalize_directory(self, entry: Entry) -> None:
        child_entries = [self.entries[path] for path in entry.children if path in self.entries]
        entry.direct_files = sum(1 for child in child_entries if child.entry_type == "f")
        entry.direct_dirs = sum(1 for child in child_entries if child.entry_type == "d")
        entry.direct_children = entry.direct_files + entry.direct_dirs
        entry.recursive_files = sum(
            1 if child.entry_type == "f" else child.recursive_files
            for child in child_entries
        )
        entry.size_bytes = sum(child.size_bytes for child in child_entries)
        entry.deepest_nesting = max(
            ((child.deepest_nesting + 1) for child in child_entries if child.entry_type == "d"),
            default=0,
        )
        entry.is_empty = entry.direct_children == 0
        entry.is_sparse = entry.direct_files <= 3

    def _owner_group(self, stat_result: os.stat_result) -> tuple[str | None, str | None]:
        if os.name == "nt" or pwd is None or grp is None:
            return None, None
        try:
            owner = pwd.getpwuid(stat_result.st_uid).pw_name
        except KeyError:
            owner = str(stat_result.st_uid)
        try:
            group_name = grp.getgrgid(stat_result.st_gid).gr_name
        except KeyError:
            group_name = str(stat_result.st_gid)
        return owner, group_name

    def _maybe_hash(self, entry: Entry) -> None:
        if self.hash_mode is None:
            return
        md5_hash = hashlib.md5() if self.hash_mode in {"md5", "both"} else None
        sha256_hash = hashlib.sha256() if self.hash_mode in {"sha256", "both"} else None
        try:
            with entry.path.open("rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    if md5_hash is not None:
                        md5_hash.update(chunk)
                    if sha256_hash is not None:
                        sha256_hash.update(chunk)
        except OSError as exc:
            self.errors.append(ScanError(path=entry.path, message=str(exc)))
            return
        entry.hash_md5 = md5_hash.hexdigest() if md5_hash is not None else None
        entry.hash_sha256 = sha256_hash.hexdigest() if sha256_hash is not None else None


class SmartLSArgumentParser:
    def __init__(self) -> None:
        self.parser = self._build()

    def _build(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(
            description="Smart directory explorer with composable filesystem filters.",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog=(
                "Examples:\n"
                "  %(prog)s --type d --files =0 --long\n"
                "  %(prog)s --type d --files 1..3 --sort -size --stats\n"
                "  %(prog)s ./src --ext py,js --not --name '*test*' --long\n"
                "  %(prog)s --type f --size >=50MB --mtime <7d --flat\n"
                "  %(prog)s --type f --flat --columns type,size,modified,relative-path\n"
            ),
        )
        parser.add_argument("root", nargs="?", default=Path.cwd(), type=Path, help="Root path to scan")
        parser.add_argument("--depth", type=int, default=None, metavar="N", help="Maximum traversal depth")
        parser.add_argument("--count", metavar="EXPR", help="Filter directories by direct child count")
        parser.add_argument("--files", metavar="EXPR", help="Filter directories by recursive file count")
        parser.add_argument("--dirs", metavar="EXPR", help="Filter directories by direct subdirectory count")
        parser.add_argument("--size", metavar="EXPR", help="Filter by total size")
        parser.add_argument("--mtime", metavar="EXPR", help="Filter by age since modification time")
        parser.add_argument("--ctime", metavar="EXPR", help="Filter by age since creation time")
        parser.add_argument("--name", metavar="GLOB", help="Filter by name glob")
        parser.add_argument("--ext", metavar="LIST", help="Filter files by comma-separated extensions")
        parser.add_argument("--depth-filter", metavar="EXPR", help="Filter by entry depth")
        parser.add_argument("--empty", action="store_true", help="Shorthand for --files =0")
        parser.add_argument("--sparse", action="store_true", help="Shorthand for --files <=3 using recursive file count")
        parser.add_argument("--type", choices=["f", "d"], help="Show only files or directories")
        parser.add_argument("--or", action="store_true", dest="or_flag", help="OR-combine adjacent filter groups")
        parser.add_argument("--not", action="store_true", dest="not_flag", help="Negate the next filter")

        output_group = parser.add_mutually_exclusive_group()
        output_group.add_argument("--flat", action="store_true", help="Render a flat list")
        output_group.add_argument("--json", action="store_true", help="Emit JSON output")
        output_group.add_argument("--csv", action="store_true", help="Emit CSV output")

        verbosity_group = parser.add_mutually_exclusive_group()
        verbosity_group.add_argument("--color", action="store_true", dest="use_color", help="Force ANSI color output")
        verbosity_group.add_argument("--no-color", action="store_false", dest="use_color", help="Disable ANSI color output")
        parser.set_defaults(use_color=None)

        detail_group = parser.add_mutually_exclusive_group()
        detail_group.add_argument("--long", action="store_true", help="Show extended metadata")
        detail_group.add_argument("--short", action="store_true", help="Show names only")

        path_group = parser.add_mutually_exclusive_group()
        path_group.add_argument("--relative", action="store_true", dest="relative_paths", help="Show paths relative to the root")
        path_group.add_argument("--absolute", action="store_false", dest="relative_paths", help="Show absolute paths")
        parser.set_defaults(relative_paths=True)

        size_group = parser.add_mutually_exclusive_group()
        size_group.add_argument("--human", action="store_true", dest="human_sizes", help="Show human-readable sizes")
        size_group.add_argument("--bytes", action="store_false", dest="human_sizes", help="Show raw byte counts")
        parser.set_defaults(human_sizes=True)

        parser.add_argument("--icons", action="store_true", help="Show icons for entries")
        parser.add_argument("--stats", action="store_true", help="Print summary statistics")
        parser.add_argument("--sort", default="name", metavar="KEY", help="Sort by name, size, mtime, files, count, or depth")
        parser.add_argument("--limit", type=int, metavar="N", help="Limit results after filtering and sorting")
        parser.add_argument("--group-by", choices=["d", "ext", "mtime-day"], help="Group flat output by depth, extension, or modification day")
        parser.add_argument("--hash", nargs="?", const="both", choices=["md5", "sha256", "both"], help="Compute file hashes")
        parser.add_argument(
            "--columns",
            metavar="LIST",
            help=(
                "Render console output as a table with comma-separated metadata columns: "
                "type,size,modified,created,accessed,children,recursive_files,mime,extension,relative_path,full_path,owner,group,permissions"
            ),
        )
        parser.add_argument("--export-html", metavar="FILE", type=Path, help="Write a self-contained HTML report")
        parser.add_argument("--skip-errors", action="store_true", default=True, help="Continue past filesystem errors")
        parser.add_argument("--show-errors", action="store_true", help="Print collected scan errors to stderr")
        return parser

    def parse(self, argv: Sequence[str] | None = None) -> argparse.Namespace:
        args = self.parser.parse_args(argv)
        if args.depth is not None and args.depth < 0:
            self.parser.error("--depth must be >= 0")
        if args.limit is not None and args.limit <= 0:
            self.parser.error("--limit must be > 0")
        if args.group_by and not args.flat:
            self.parser.error("--group-by requires --flat")
        if args.columns and (args.json or args.csv):
            self.parser.error("--columns is only supported for console tree or flat output")
        if args.columns:
            try:
                args.columns, args.column_widths = parse_console_columns(args.columns)
            except ValueError as exc:
                self.parser.error(str(exc))
        else:
            args.columns = []
            args.column_widths = {}
        if not args.root.exists():
            self.parser.error(f"Root path does not exist: {args.root}")
        return args


def normalize_cli_argv(argv: Sequence[str]) -> list[str]:
    normalized: list[str] = []
    sort_keys = {"-name", "-size", "-mtime", "-files", "-count", "-depth"}
    index = 0
    while index < len(argv):
        token = argv[index]
        if token == "--sort" and index + 1 < len(argv) and argv[index + 1] in sort_keys:
            normalized.append(f"--sort={argv[index + 1]}")
            index += 2
            continue
        normalized.append(token)
        index += 1
    return normalized


def normalize_console_path(text: str) -> str:
    return text.replace("\\", "/")


def strip_ansi(text: str) -> str:
    return ANSI_ESCAPE_RE.sub("", text)


def visible_text_width(text: str) -> int:
    return len(strip_ansi(text))


def pad_console_cell(text: str, width: int, align: str) -> str:
    padding = max(0, width - visible_text_width(text))
    if align == "right":
        return f"{' ' * padding}{text}"
    return f"{text}{' ' * padding}"


def truncate_console_text(text: str, max_width: int) -> str:
    if max_width <= 0:
        return ""
    if len(text) <= max_width:
        return text
    if max_width <= 3:
        return text[:max_width]
    return f"{text[:max_width - 3]}..."


def parse_console_columns(value: str) -> tuple[list[str], dict[str, int]]:
    columns: list[str] = []
    seen: set[str] = set()
    widths: dict[str, int] = {}
    for index, part in enumerate(value.split(",")):
        item = part.strip()
        if not item:
            continue
        width_override: int | None = None
        raw_key = item
        if ":" in item:
            raw_key, width_text = item.rsplit(":", 1)
            width_text = width_text.strip()
            if not re.fullmatch(r"\d+", width_text):
                raise ValueError(f"Invalid width in column spec '{item}'. Use column:n, for example type:12")
            width_override = int(width_text)
            if width_override <= 0:
                raise ValueError(f"Column width must be > 0 in '{item}'")
        key = raw_key.strip().lower().replace("-", "_")
        if not key:
            raise ValueError(f"Missing column name in '{item}'")
        if key == "name":
            if index != 0:
                raise ValueError("The name column may only be specified in the first position")
            if width_override is not None:
                widths["name"] = width_override
            continue
        normalized = CONSOLE_COLUMN_ALIASES.get(key, key)
        if normalized not in CONSOLE_COLUMN_SPECS:
            allowed = ", ".join(sorted(CONSOLE_COLUMN_SPECS))
            raise ValueError(f"Unsupported column '{raw_key.strip()}'. Choose from: {allowed}")
        if normalized in seen:
            if width_override is not None:
                widths[normalized] = width_override
            continue
        seen.add(normalized)
        columns.append(normalized)
        if width_override is not None:
            widths[normalized] = width_override
    if not columns:
        raise ValueError("--columns requires at least one column")
    return columns, widths


def matches_filters(entry: Entry, groups: Sequence[Sequence[Filter]]) -> bool:
    if not groups:
        return True
    return any(all(filter_obj.apply(entry) for filter_obj in group) for group in groups)


def sort_entries(entries: Iterable[Entry], sort_key: str) -> list[Entry]:
    reverse = sort_key.startswith("-")
    key_name = sort_key[1:] if reverse else sort_key

    def key(entry: Entry) -> tuple[object, str]:
        mapping = {
            "name": entry.name.lower(),
            "size": entry.size_bytes,
            "mtime": entry.modified_ts or 0,
            "files": entry.direct_files if entry.entry_type == "d" else -1,
            "count": entry.direct_children if entry.entry_type == "d" else -1,
            "depth": entry.depth,
        }
        if key_name not in mapping:
            raise ValueError(f"Unsupported sort key: {key_name}")
        return mapping[key_name], entry.name.lower()

    return sorted(entries, key=key, reverse=reverse)


def compute_visible_tree(entries: dict[Path, Entry], matched_entries: Sequence[Entry]) -> set[Path]:
    visible: set[Path] = set()
    for entry in matched_entries:
        current: Path | None = entry.path
        while current is not None and current in entries:
            visible.add(current)
            current = entries[current].parent
    return visible


def group_entries(entries: Sequence[Entry], group_by: str | None) -> list[tuple[str, list[Entry]]]:
    if group_by is None:
        return [("", list(entries))]

    buckets: dict[str, list[Entry]] = {}
    for entry in entries:
        if group_by == "d":
            key = f"Depth {entry.depth}"
        elif group_by == "ext":
            key = entry.path.suffix.lower() or "[no extension]"
        else:
            key = format_timestamp(entry.modified_ts).split(" ")[0]
        buckets.setdefault(key, []).append(entry)
    return [(key, buckets[key]) for key in sorted(buckets)]


def build_summary_stats(scan_result: ScanResult, matched_entries: Sequence[Entry], args: argparse.Namespace) -> SummaryStats:
    matched_dirs = [entry for entry in matched_entries if entry.entry_type == "d"]
    matched_files = [entry for entry in matched_entries if entry.entry_type == "f"]
    scanned_dirs = sum(1 for entry in scan_result.entries.values() if entry.entry_type == "d")
    total_size = sum(entry.size_bytes for entry in matched_files)
    avg_files = (sum(entry.direct_files for entry in matched_dirs) / len(matched_dirs)) if matched_dirs else 0.0
    emptiest = min(matched_dirs, key=lambda entry: (entry.direct_files, entry.name.lower()), default=None)
    largest = max(matched_files, key=lambda entry: entry.size_bytes, default=None)
    return SummaryStats(
        folders_scanned=scanned_dirs,
        folders_matched=len(matched_dirs),
        files_listed=len(matched_files),
        total_size_bytes=total_size,
        avg_files_per_folder=avg_files,
        emptiest_folder=(display_path(emptiest.path, scan_result.root, absolute=not args.relative_paths, is_dir=True) if emptiest else None),
        largest_file=(display_path(largest.path, scan_result.root, absolute=not args.relative_paths) if largest else None),
    )


def load_template_asset(file_name: str) -> str:
    asset_path = Path(__file__).with_name(file_name)
    return asset_path.read_text(encoding="utf-8")


def build_webapp_payload(scan_result: ScanResult, matched_entries: Sequence[Entry], args: argparse.Namespace) -> dict[str, object]:
    visible_paths = compute_visible_tree(scan_result.entries, matched_entries) if matched_entries else {scan_result.root}
    matched_paths = {entry.path for entry in matched_entries}
    visible_directories = [
        entry for entry in scan_result.entries.values()
        if entry.entry_type == "d" and entry.path in visible_paths
    ]
    visible_directories = sort_entries(visible_directories, "depth")
    summary = build_summary_stats(scan_result, matched_entries, args)
    entries_payload: list[dict[str, object]] = []
    for entry in matched_entries:
        item = entry.to_dict(scan_result.root, absolute=not args.relative_paths)
        item["matched"] = True
        item["parent_path"] = (
            display_path(entry.parent, scan_result.root, absolute=not args.relative_paths, is_dir=True)
            if entry.parent is not None and entry.parent in scan_result.entries
            else None
        )
        item["name_path"] = display_path(entry.path, scan_result.root, absolute=not args.relative_paths, is_dir=entry.entry_type == "d")
        entries_payload.append(item)

    directories_payload: list[dict[str, object]] = []
    for entry in visible_directories:
        item = entry.to_dict(scan_result.root, absolute=not args.relative_paths)
        item["matched"] = entry.path in matched_paths
        item["path"] = display_path(entry.path, scan_result.root, absolute=not args.relative_paths, is_dir=True)
        item["name_path"] = item["path"]
        item["parent_path"] = (
            display_path(entry.parent, scan_result.root, absolute=not args.relative_paths, is_dir=True)
            if entry.parent is not None and entry.parent in scan_result.entries
            else None
        )
        directories_payload.append(item)

    return {
        "meta": {
            "title": "smartls web report",
            "root_path": str(scan_result.root),
            "generated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "generated_at_ts": time_now(),
            "sort": args.sort,
            "group_by": args.group_by,
            "relative_paths": args.relative_paths,
            "human_sizes": args.human_sizes,
        },
        "summary": {
            "folders_scanned": summary.folders_scanned,
            "folders_matched": summary.folders_matched,
            "files_listed": summary.files_listed,
            "total_size_bytes": summary.total_size_bytes,
            "avg_files_per_folder": round(summary.avg_files_per_folder, 2),
            "emptiest_folder": summary.emptiest_folder,
            "largest_file": summary.largest_file,
        },
        "entries": entries_payload,
        "directories": directories_payload,
        "errors": [{"path": str(error.path), "message": error.message} for error in scan_result.errors],
    }


def render_webapp_html(payload: dict[str, object]) -> str:
    template = load_template_asset("smartls_webapp_template.html")
    css = load_template_asset("smartls_webapp_template.css")
    script = load_template_asset("smartls_webapp_template.js")
    serialized = json.dumps(payload, ensure_ascii=False).replace("</", "<\\/")
    return (
        template
        .replace("/*SMARTLS_CSS*/", css)
        .replace("/*SMARTLS_JSON*/", serialized)
        .replace("/*SMARTLS_JS*/", script)
    )


def export_webapp_report(scan_result: ScanResult, matched_entries: Sequence[Entry], args: argparse.Namespace) -> Path:
    output_path = args.export_html.expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    html = render_webapp_html(build_webapp_payload(scan_result, matched_entries, args))
    output_path.write_text(html, encoding="utf-8")
    return output_path


def make_entry_line(entry: Entry, root: Path, args: argparse.Namespace, include_path: bool = True) -> str:
    absolute = not args.relative_paths
    label = display_path(entry.path, root, absolute=absolute, is_dir=entry.entry_type == "d") if include_path else entry.name
    icon = icon_for_entry(entry.entry_type, entry.path.suffix.lower(), args.icons)
    use_color = should_use_color(args.use_color)

    if args.short:
        text = f"{icon}{label}"
        if entry.entry_type == "d":
            return colorize(text, SMARTLS_DIRECTORY_COLOR, use_color)
        return text

    if entry.entry_type == "d":
        parts = [f"{entry.direct_files} files", f"{entry.direct_dirs} dirs", format_size(entry.size_bytes, args.human_sizes)]
        if entry.is_empty:
            parts.append("empty")
        elif entry.is_sparse:
            parts.append("sparse")
        parts.append(f"modified {format_age(entry.modified_ts)}")
        if args.long:
            parts.extend([
                f"recursive_files {entry.recursive_files}",
                f"deepest {entry.deepest_nesting}",
                f"created {format_timestamp(entry.created_ts)}",
            ])
            if entry.permissions_text:
                parts.append(f"perm {entry.permissions_text}")
        text = f"{icon}{label}  [{' | '.join(parts)}]"
        return colorize(text, SMARTLS_DIRECTORY_COLOR, use_color)

    parts = [format_size(entry.size_bytes, args.human_sizes)]
    if entry.path.suffix:
        parts.append(entry.path.suffix.lower())
    parts.append(f"modified {format_age(entry.modified_ts)}")
    if args.long:
        parts.extend([
            f"mime {entry.mime_type or '-'}",
            f"created {format_timestamp(entry.created_ts)}",
            f"accessed {format_timestamp(entry.accessed_ts)}",
        ])
        if entry.permissions_text:
            parts.append(f"perm {entry.permissions_text}")
        if entry.is_symlink and entry.symlink_target:
            parts.append(f"symlink -> {entry.symlink_target}")
        if entry.hash_md5:
            parts.append(f"md5 {entry.hash_md5}")
        if entry.hash_sha256:
            parts.append(f"sha256 {entry.hash_sha256}")
    return f"{icon}{label}  [{' | '.join(parts)}]"


class OutputRenderer:
    def __init__(self, scan_result: ScanResult, args: argparse.Namespace):
        self.scan_result = scan_result
        self.args = args
        self.use_color = should_use_color(args.use_color)

    def render(self, matched_entries: Sequence[Entry]) -> None:
        if self.args.json:
            self._render_json(matched_entries)
        elif self.args.csv:
            self._render_csv(matched_entries)
        elif self.args.columns:
            if self.args.flat:
                self._render_flat_table(matched_entries)
            else:
                self._render_tree_table(matched_entries)
        elif self.args.flat:
            self._render_flat(matched_entries)
        else:
            self._render_tree(matched_entries)

        if self.args.export_html:
            output_path = export_webapp_report(self.scan_result, matched_entries, self.args)
            print(f"Exported web report to {output_path}", file=sys.stderr)

        if self.args.stats:
            self._render_stats(matched_entries)
        if self.args.show_errors and self.scan_result.errors:
            self._render_errors()

    def _render_tree(self, matched_entries: Sequence[Entry]) -> None:
        visible = compute_visible_tree(self.scan_result.entries, matched_entries)
        if not visible:
            return
        self._render_tree_node(self.scan_result.root, visible, 0)

    def _render_tree_node(self, path: Path, visible: set[Path], level: int) -> None:
        if path not in visible:
            return
        entry = self.scan_result.entries[path]
        indent = "  " * level
        print(f"{indent}{make_entry_line(entry, self.scan_result.root, self.args)}")
        children = [self.scan_result.entries[child_path] for child_path in entry.children if child_path in visible]
        for child in sort_entries(children, self.args.sort):
            self._render_tree_node(child.path, visible, level + 1)

    def _render_tree_table(self, matched_entries: Sequence[Entry]) -> None:
        visible = compute_visible_tree(self.scan_result.entries, matched_entries)
        if not visible:
            return
        rows: list[tuple[int, Entry]] = []

        def visit(path: Path, level: int) -> None:
            if path not in visible:
                return
            entry = self.scan_result.entries[path]
            rows.append((level, entry))
            children = [self.scan_result.entries[child_path] for child_path in entry.children if child_path in visible]
            for child in sort_entries(children, self.args.sort):
                visit(child.path, level + 1)

        visit(self.scan_result.root, 0)
        self._print_table_rows(rows)

    def _render_flat(self, matched_entries: Sequence[Entry]) -> None:
        for group_name, group_entries_list in group_entries(matched_entries, self.args.group_by):
            if group_name:
                print(group_name)
            for entry in group_entries_list:
                print(make_entry_line(entry, self.scan_result.root, self.args))

    def _render_flat_table(self, matched_entries: Sequence[Entry]) -> None:
        grouped = group_entries(matched_entries, self.args.group_by)
        for group_name, group_entries_list in grouped:
            if group_name:
                print(group_name)
            self._print_table_rows([(0, entry) for entry in group_entries_list])

    def _print_table_rows(self, rows: Sequence[tuple[int, Entry]]) -> None:
        if not rows:
            return
        headers = ["Name", *(str(CONSOLE_COLUMN_SPECS[key]["header"]) for key in self.args.columns)]
        alignments = ["left", *(str(CONSOLE_COLUMN_SPECS[key]["align"]) for key in self.args.columns)]
        max_widths = [
            self.args.column_widths.get("name", NAME_COLUMN_MAX_WIDTH),
            *(
                self.args.column_widths.get(key, cast(int, CONSOLE_COLUMN_SPECS[key]["max_width"]))
                for key in self.args.columns
            ),
        ]
        row_models = [
            {
                "entry": entry,
                "cells": [self._format_name_cell(entry, level), *(self._format_column_cell(entry, key) for key in self.args.columns)],
            }
            for level, entry in rows
        ]
        widths = [min(visible_text_width(header), max_widths[index]) for index, header in enumerate(headers)]
        for row in row_models:
            for index, cell in enumerate(row["cells"]):
                widths[index] = min(max(widths[index], len(cell)), max_widths[index])

        print(self._render_table_line(headers, widths, alignments))
        print(self._render_table_separator(widths, alignments))
        for row in row_models:
            formatted_cells: list[str] = []
            for index, cell in enumerate(row["cells"]):
                fitted = pad_console_cell(truncate_console_text(cell, widths[index]), widths[index], alignments[index])
                if index == 0 and row["entry"].entry_type == "d":
                    fitted = colorize(fitted, SMARTLS_DIRECTORY_COLOR, self.use_color)
                formatted_cells.append(fitted)
            print(self._render_table_line(formatted_cells, widths, alignments, preformatted=True))

    def _render_table_line(
        self,
        values: Sequence[str],
        widths: Sequence[int],
        alignments: Sequence[str],
        *,
        preformatted: bool = False,
    ) -> str:
        cells = []
        for index, value in enumerate(values):
            if preformatted:
                cells.append(value)
            else:
                fitted = pad_console_cell(truncate_console_text(value, widths[index]), widths[index], alignments[index])
                cells.append(fitted)
        return f"| {' | '.join(cells)} |"

    def _render_table_separator(self, widths: Sequence[int], alignments: Sequence[str]) -> str:
        segments: list[str] = []
        for width, alignment in zip(widths, alignments):
            segment_width = max(3, width)
            if alignment == "right":
                segment = f"{'-' * (segment_width - 1)}:"
            else:
                segment = f":{'-' * (segment_width - 1)}"
            segments.append(segment)
        return f"| {' | '.join(segments)} |"

    def _format_name_cell(self, entry: Entry, level: int) -> str:
        indent = "" if self.args.flat else "  " * level
        icon = icon_for_entry(entry.entry_type, entry.path.suffix.lower(), self.args.icons)
        label = entry.name or normalize_console_path(str(entry.path))
        return f"{indent}{icon}{label}"

    def _format_column_cell(self, entry: Entry, key: str) -> str:
        if key == "type":
            return "Directory" if entry.entry_type == "d" else "File"
        if key == "size":
            return format_size(entry.size_bytes, self.args.human_sizes)
        if key == "modified":
            return format_timestamp(entry.modified_ts)
        if key == "created":
            return format_timestamp(entry.created_ts)
        if key == "accessed":
            return format_timestamp(entry.accessed_ts)
        if key == "children":
            return str(entry.direct_children)
        if key == "recursive_files":
            return str(entry.recursive_files)
        if key == "mime":
            return entry.mime_type or "-"
        if key == "extension":
            return entry.path.suffix.lower() or "-"
        if key == "relative_path":
            return normalize_console_path(display_path(entry.path, self.scan_result.root, absolute=False, is_dir=entry.entry_type == "d"))
        if key == "full_path":
            return normalize_console_path(str(entry.path))
        if key == "owner":
            return entry.owner or "-"
        if key == "group":
            return entry.group or "-"
        if key == "permissions":
            return entry.permissions_text or entry.permissions_octal or "-"
        raise ValueError(f"Unsupported console column: {key}")

    def _render_json(self, matched_entries: Sequence[Entry]) -> None:
        payload = [entry.to_dict(self.scan_result.root, absolute=not self.args.relative_paths) for entry in matched_entries]
        print(json.dumps(payload, indent=2, cls=SmartLSJSONEncoder))

    def _render_csv(self, matched_entries: Sequence[Entry]) -> None:
        fieldnames = [
            "name",
            "path",
            "absolute_path",
            "type",
            "extension",
            "depth",
            "size_bytes",
            "created",
            "modified",
            "accessed",
            "permissions_octal",
            "permissions_text",
            "owner",
            "group",
            "is_symlink",
            "symlink_target",
            "mime_type",
            "hash_md5",
            "hash_sha256",
            "direct_files",
            "direct_dirs",
            "direct_children",
            "recursive_files",
            "deepest_nesting",
            "is_empty",
            "is_sparse",
        ]
        writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
        writer.writeheader()
        for entry in matched_entries:
            writer.writerow(entry.to_dict(self.scan_result.root, absolute=True))

    def _render_stats(self, matched_entries: Sequence[Entry]) -> None:
        summary = build_summary_stats(self.scan_result, matched_entries, self.args)

        print("── Summary ─────────────────────────────")
        print(f"  Folders scanned : {summary.folders_scanned}")
        print(f"  Folders matched : {summary.folders_matched}")
        print(f"  Files listed    : {summary.files_listed}")
        print(f"  Total size      : {format_size(summary.total_size_bytes, self.args.human_sizes)}")
        print(f"  Avg files/folder: {summary.avg_files_per_folder:.1f}")
        print(f"  Emptiest folder : {summary.emptiest_folder or '-'}")
        print(f"  Largest file    : {summary.largest_file or '-'}")

    def _render_errors(self) -> None:
        for error in self.scan_result.errors:
            print(f"smartls: {error.path}: {error.message}", file=sys.stderr)


def collect_matches(scan_result: ScanResult, groups: Sequence[Sequence[Filter]], args: argparse.Namespace) -> list[Entry]:
    matched = [entry for entry in scan_result.entries.values() if matches_filters(entry, groups)]
    matched = sort_entries(matched, args.sort)
    if args.limit is not None:
        matched = matched[:args.limit]
    return matched


def run(argv: Sequence[str] | None = None) -> int:
    raw_argv = normalize_cli_argv(list(sys.argv[1:] if argv is None else argv))
    parser = SmartLSArgumentParser()
    args = parser.parse(raw_argv)
    try:
        filter_groups = FilterFactory.from_argv(raw_argv)
    except ValueError as exc:
        parser.parser.error(str(exc))

    scanner = DirectoryScanner(root=args.root.resolve(), max_depth=args.depth, hash_mode=args.hash)
    scan_result = scanner.scan()
    matched_entries = collect_matches(scan_result, filter_groups, args)
    OutputRenderer(scan_result, args).render(matched_entries)
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()