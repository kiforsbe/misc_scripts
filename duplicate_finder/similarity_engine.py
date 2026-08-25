"""Directory scanning and duplicate-group clustering."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from . import filename_similarity as fs


@dataclass(frozen=True)
class FileRecord:
    path: Path
    name: str
    size: int
    category: str


@dataclass
class DuplicateGroup:
    label: str
    files: list[FileRecord]


def scan(
    root: Path,
    recursive: bool,
    include_keywords: list[str],
    exclude_keywords: list[str],
    output_dir: Path,
) -> list[FileRecord]:
    root = Path(root)
    resolved_output_dir = Path(output_dir).resolve()
    iterator = root.rglob("*") if recursive else root.iterdir()

    records: list[FileRecord] = []
    for path in iterator:
        if not path.is_file():
            continue
        resolved = path.resolve()
        if resolved == resolved_output_dir or resolved_output_dir in resolved.parents:
            continue
        if not fs.matches_keyword_filters(path.name, include_keywords, exclude_keywords):
            continue
        records.append(
            FileRecord(
                path=path,
                name=path.name,
                size=path.stat().st_size,
                category=fs.extension_category(path.name),
            )
        )
    return records


class _UnionFind:
    def __init__(self, items):
        self._parent = {item: item for item in items}

    def find(self, item):
        root = item
        while self._parent[root] != root:
            root = self._parent[root]
        while self._parent[item] != root:
            self._parent[item], item = root, self._parent[item]
        return root

    def union(self, a, b) -> None:
        root_a, root_b = self.find(a), self.find(b)
        if root_a != root_b:
            self._parent[root_b] = root_a


def _size_within_tolerance(size_a: int, size_b: int, tolerance_percent: float) -> bool:
    largest = max(size_a, size_b)
    if largest == 0:
        return size_a == size_b
    return abs(size_a - size_b) / largest * 100 <= tolerance_percent


def find_duplicate_groups(
    files: list[FileRecord],
    name_threshold: float,
    size_tolerance_percent: float | None,
    min_group_size: int = 2,
) -> list[DuplicateGroup]:
    if min_group_size < 2:
        raise ValueError("min_group_size must be at least 2 (a duplicate group needs at least two files)")

    buckets: dict[str, list[int]] = {}
    unknown_indices: list[int] = []
    for index, record in enumerate(files):
        if record.category == "unknown":
            unknown_indices.append(index)
        else:
            buckets.setdefault(record.category, []).append(index)

    union_find = _UnionFind(range(len(files)))

    def compare_and_union(candidate_indices: list[int]) -> None:
        for a in range(len(candidate_indices)):
            for b in range(a + 1, len(candidate_indices)):
                i, j = candidate_indices[a], candidate_indices[b]
                record_a, record_b = files[i], files[j]
                if not fs.categories_compatible(record_a.category, record_b.category):
                    continue
                if fs.core_similarity(record_a.name, record_b.name) < name_threshold:
                    continue
                if size_tolerance_percent is not None and not _size_within_tolerance(
                    record_a.size, record_b.size, size_tolerance_percent
                ):
                    continue
                union_find.union(i, j)

    for category_indices in buckets.values():
        compare_and_union(category_indices + unknown_indices)
    if not buckets:
        compare_and_union(unknown_indices)

    grouped: dict[int, list[int]] = {}
    for index in range(len(files)):
        grouped.setdefault(union_find.find(index), []).append(index)

    groups: list[DuplicateGroup] = []
    for member_indices in grouped.values():
        if len(member_indices) < min_group_size:
            continue
        members = [files[i] for i in member_indices]
        groups.append(DuplicateGroup(label=_medoid_label(members), files=members))
    return groups


def _medoid_label(members: list[FileRecord]) -> str:
    best_index = 0
    best_score = -1.0
    for i, member in enumerate(members):
        total = sum(
            fs.similarity(member.name, other.name)
            for j, other in enumerate(members)
            if i != j
        )
        average = total / (len(members) - 1)
        if average > best_score:
            best_score = average
            best_index = i
    return fs.normalize(members[best_index].name)
