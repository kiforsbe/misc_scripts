from __future__ import annotations

from pathlib import Path

from duplicate_finder.similarity_engine import (
    FileRecord,
    find_duplicate_groups,
    scan,
)


def _write(root: Path, relative_path: str, size_bytes: int = 10) -> Path:
    file_path = root / relative_path
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_bytes(b"x" * size_bytes)
    return file_path


def _record(name: str, size: int = 1000, category: str = "video") -> FileRecord:
    return FileRecord(path=Path(name), name=name, size=size, category=category)


def test_scan_recursive_finds_nested_files(tmp_path):
    _write(tmp_path, "movie.mp4")
    _write(tmp_path, "sub/movie_copy.mp4")

    records = scan(tmp_path, recursive=True, include_keywords=[], exclude_keywords=[], output_dir=tmp_path / "_duplicates")

    assert {r.name for r in records} == {"movie.mp4", "movie_copy.mp4"}


def test_scan_non_recursive_skips_subfolders(tmp_path):
    _write(tmp_path, "movie.mp4")
    _write(tmp_path, "sub/movie_copy.mp4")

    records = scan(tmp_path, recursive=False, include_keywords=[], exclude_keywords=[], output_dir=tmp_path / "_duplicates")

    assert {r.name for r in records} == {"movie.mp4"}


def test_scan_excludes_output_dir_contents(tmp_path):
    output_dir = tmp_path / "_duplicates"
    _write(tmp_path, "movie.mp4")
    _write(tmp_path, "_duplicates/old_copy.mp4")

    records = scan(tmp_path, recursive=True, include_keywords=[], exclude_keywords=[], output_dir=output_dir)

    assert {r.name for r in records} == {"movie.mp4"}


def test_scan_applies_include_and_exclude_keywords(tmp_path):
    _write(tmp_path, "trailer_movie.mp4")
    _write(tmp_path, "sample_movie.mp4")
    _write(tmp_path, "other.mp4")

    records = scan(
        tmp_path,
        recursive=True,
        include_keywords=["movie"],
        exclude_keywords=["sample"],
        output_dir=tmp_path / "_duplicates",
    )

    assert {r.name for r in records} == {"trailer_movie.mp4"}


def test_find_duplicate_groups_clusters_similar_names_above_threshold():
    files = [_record("My Movie.mp4"), _record("my_movie.mkv"), _record("unrelated.mp4")]

    groups = find_duplicate_groups(files, name_threshold=80, size_tolerance_percent=None)

    assert len(groups) == 1
    assert {f.name for f in groups[0].files} == {"My Movie.mp4", "my_movie.mkv"}


def test_find_duplicate_groups_respects_category_compatibility():
    files = [_record("title.mp4", category="video"), _record("title.mp3", category="audio")]

    groups = find_duplicate_groups(files, name_threshold=50, size_tolerance_percent=None)

    assert groups == []


def test_find_duplicate_groups_unknown_category_is_permissive():
    files = [_record("title.mp4", category="video"), _record("title.xyz", category="unknown")]

    groups = find_duplicate_groups(files, name_threshold=50, size_tolerance_percent=None)

    assert len(groups) == 1


def test_find_duplicate_groups_applies_size_tolerance():
    files = [_record("clip.mp4", size=1000), _record("clip_copy.mp4", size=5000)]

    groups = find_duplicate_groups(files, name_threshold=50, size_tolerance_percent=10)

    assert groups == []


def test_find_duplicate_groups_size_tolerance_disabled_ignores_size():
    files = [_record("clip.mp4", size=1000), _record("clip_copy.mp4", size=5000)]

    groups = find_duplicate_groups(files, name_threshold=50, size_tolerance_percent=None)

    assert len(groups) == 1


def test_find_duplicate_groups_drops_groups_below_min_size():
    files = [_record("solo_file.mp4")]

    groups = find_duplicate_groups(files, name_threshold=50, size_tolerance_percent=None)

    assert groups == []


def test_find_duplicate_groups_label_uses_normalized_best_representative_name():
    files = [
        _record("Movie Title.mp4"),
        _record("movie_title.mkv"),
        _record("Movie Title Extended Cut.avi"),
    ]

    groups = find_duplicate_groups(files, name_threshold=40, size_tolerance_percent=None)

    assert len(groups) == 1
    assert groups[0].label == "movie title"


def test_keyword_filter_shrinking_group_to_one_member_is_not_a_duplicate_group(tmp_path):
    _write(tmp_path, "movie.mp4")
    _write(tmp_path, "movie_copy.mp4")
    _write(tmp_path, "movie_sample.mp4")

    records = scan(
        tmp_path,
        recursive=True,
        include_keywords=[],
        exclude_keywords=["sample", "copy"],
        output_dir=tmp_path / "_duplicates",
    )
    groups = find_duplicate_groups(records, name_threshold=50, size_tolerance_percent=None)

    assert groups == []
