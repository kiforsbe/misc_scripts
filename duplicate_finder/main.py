"""CLI entrypoint for duplicate_finder."""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Callable

from .review_ui import ScanParams, run_review
from .similarity_engine import DuplicateGroup, find_duplicate_groups, scan


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="duplicate_finder",
        description="Find likely-duplicate files by filename similarity and review them interactively.",
    )
    parser.add_argument("root", type=Path, help="Folder to scan")
    parser.add_argument(
        "--no-recursive",
        dest="recursive",
        action="store_false",
        default=True,
        help="Only scan the top level of root (default: recursive)",
    )
    parser.add_argument(
        "--name-threshold", type=float, default=85.0,
        help="Minimum name similarity, 0-100 (default: 85)",
    )
    parser.add_argument(
        "--size-tolerance-percent", type=float, default=None,
        help="Require file sizes within this percent of each other (default: disabled)",
    )
    parser.add_argument(
        "--min-group-size", type=int, default=2,
        help="Minimum files to count as a duplicate group (default: 2)",
    )
    parser.add_argument(
        "--include-keyword", action="append", default=[],
        help="Only consider files matching at least one (repeatable)",
    )
    parser.add_argument(
        "--exclude-keyword", action="append", default=[],
        help="Never consider files matching any of these (repeatable)",
    )
    parser.add_argument(
        "--output-dir", type=Path, default=None,
        help="Where discarded files are moved (default: <root>/_duplicates)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print groups and exit; no UI, no filesystem changes",
    )
    return parser


def _make_rescan(root: Path) -> Callable[[ScanParams], list[DuplicateGroup]]:
    def rescan(params: ScanParams) -> list[DuplicateGroup]:
        files = scan(root, params.recursive, params.include_keywords, params.exclude_keywords, params.output_dir)
        return find_duplicate_groups(files, params.name_threshold, params.size_tolerance_percent, params.min_group_size)

    return rescan


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    root: Path = args.root
    if not root.is_dir():
        parser.error(f"{root} is not a directory")

    if args.min_group_size < 2:
        parser.error("--min-group-size must be at least 2 (a duplicate group needs at least two files)")

    output_dir = args.output_dir if args.output_dir is not None else root / "_duplicates"

    params = ScanParams(
        recursive=args.recursive,
        name_threshold=args.name_threshold,
        size_tolerance_percent=args.size_tolerance_percent,
        min_group_size=args.min_group_size,
        include_keywords=list(args.include_keyword),
        exclude_keywords=list(args.exclude_keyword),
        output_dir=output_dir,
    )

    files = scan(root, params.recursive, params.include_keywords, params.exclude_keywords, params.output_dir)
    groups = find_duplicate_groups(files, params.name_threshold, params.size_tolerance_percent, params.min_group_size)

    if not groups:
        print("No likely duplicates found.")
        return 0

    if args.dry_run:
        for group in groups:
            print(f"{group.label} ({len(group.files)} files):")
            for file in group.files:
                print(f"  {file.path}  ({file.size} bytes)")
        return 0

    app = run_review(root, groups, params, _make_rescan(root))

    print(f"Moved {app.moved_count} file(s) to {output_dir}.")
    if app.move_errors:
        print("Errors:")
        for error in app.move_errors:
            print(f"  {error}")
    return 0
