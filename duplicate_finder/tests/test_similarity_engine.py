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


def test_find_duplicate_groups_rejects_min_group_size_less_than_2():
    import pytest
    files = [_record("solo_file.mp4")]

    with pytest.raises(ValueError, match="min_group_size must be at least 2"):
        find_duplicate_groups(files, name_threshold=50, size_tolerance_percent=None, min_group_size=1)

    with pytest.raises(ValueError, match="min_group_size must be at least 2"):
        find_duplicate_groups(files, name_threshold=50, size_tolerance_percent=None, min_group_size=0)


# Real-world regression cases, modeled on (but not copied from) a large
# No-Intro-style ROM set: "Title (Region) [(Language)] [(Rev N)]" naming.
# Names below are invented, not real game titles. Duplicate detection now
# strips bracketed tags and requires the remaining core title to match at
# name_threshold=100 (exact) -- the CLI's new default. No threshold below
# 100 can separate Category B/C from genuine duplicates: coincidental word
# overlap and single-character series numbering both fuzzy-match in the
# high 80s/90s, in the same range as true region/language variants.


def test_find_duplicate_groups_catches_true_region_duplicate_even_when_short_title_dilutes_full_name_score():
    """Category A (false negative): a short title plus a region tag makes the
    FULL-NAME fuzzy score dip below the old default threshold, even though
    these are unambiguously the same release. e.g. real "101 Shark Pets
    (Europe)" vs "(USA)" scored 83.72 -- below the old 85 threshold. Stripping
    the tag and comparing core titles fixes this: "otter pets" == "otter
    pets" is an exact match regardless of threshold."""
    files = [_record("Otter Pets (Europe).zip", category="archive"), _record("Otter Pets (USA).zip", category="archive")]

    groups = find_duplicate_groups(files, name_threshold=100, size_tolerance_percent=None)

    assert len(groups) == 1
    assert {f.name for f in groups[0].files} == {"Otter Pets (Europe).zip", "Otter Pets (USA).zip"}


def test_find_duplicate_groups_does_not_merge_unrelated_titles_that_share_words_and_region_tag():
    """Category B (false positive): two different games that happen to share
    words and formatting score above the old threshold on the full name even
    though they are not duplicates. e.g. real "AiRace (USA)" vs "Airport
    Mania - Non-Stop Flights (USA)" scored 85.50. Their core titles ("sky
    racer" vs "sky runner airport dash") are still similar but not identical,
    so requiring an exact core-title match correctly keeps them apart."""
    files = [
        _record("Sky Racer (USA).zip", category="archive"),
        _record("Sky Runner - Airport Dash (USA).zip", category="archive"),
    ]

    groups = find_duplicate_groups(files, name_threshold=100, size_tolerance_percent=None)

    assert groups == []


def test_find_duplicate_groups_does_not_merge_numbered_series_entries():
    """Category C (false positive): sequential entries in a numbered series
    differ by a single character, so fuzzy matching scores them almost as
    high as a genuine region-variant duplicate -- no fuzzy threshold fixes
    this, since raising it just as easily excludes real duplicates. e.g. real
    "Anonymous Notes 1/2/3 - From The Abyss (USA)" scored 96+ against each
    other and still incorrectly grouped even at name_threshold=92. Requiring
    an EXACT core-title match ("shadow realm 1 rising" != "shadow realm 2
    rising") is the only threshold-independent fix."""
    files = [
        _record("Shadow Realm 1 - Rising (USA).zip", category="archive"),
        _record("Shadow Realm 2 - Rising (USA).zip", category="archive"),
        _record("Shadow Realm 3 - Rising (USA).zip", category="archive"),
    ]

    groups = find_duplicate_groups(files, name_threshold=100, size_tolerance_percent=None)

    assert groups == []


def test_find_duplicate_groups_does_not_chain_unrelated_titles_into_one_giant_group():
    """Category D (catastrophic chaining at scale): five unrelated titles,
    each with one genuine region-variant pair, must resolve into five
    correctly-sized 2-file groups -- not get transitively chained together
    into a single oversized group via union-find, the way the real 1070-file
    ROM folder collapsed into one 1070-file "group" at the old default."""
    files = [
        _record("Puzzle Farmyard Friends (Europe).zip", category="archive"),
        _record("Puzzle Farmyard Friends (USA).zip", category="archive"),
        _record("Sky Racer (Europe).zip", category="archive"),
        _record("Sky Racer (USA).zip", category="archive"),
        _record("Sky Runner - Airport Dash (Europe).zip", category="archive"),
        _record("Sky Runner - Airport Dash (USA).zip", category="archive"),
        _record("Turbo Trail Bowling (Europe).zip", category="archive"),
        _record("Turbo Trail Bowling (USA).zip", category="archive"),
        _record("Turbo Trail Mini Golf (Europe).zip", category="archive"),
        _record("Turbo Trail Mini Golf (USA).zip", category="archive"),
    ]

    groups = find_duplicate_groups(files, name_threshold=100, size_tolerance_percent=None)

    assert len(groups) == 5
    for group in groups:
        assert len(group.files) == 2
