from __future__ import annotations

from duplicate_finder.filename_similarity import (
    categories_compatible,
    extension_category,
    matches_keyword_filters,
    normalize,
    similarity,
)


def test_normalize_strips_extension_and_separators():
    assert normalize("My.Movie_Name-2024.mp4") == "my movie name 2024"


def test_normalize_collapses_repeated_separators_and_case():
    assert normalize("FOO___bar---baz.txt") == "foo bar baz"


def test_normalize_already_clean_name():
    assert normalize("clean name.txt") == "clean name"


def test_extension_category_known_types():
    assert extension_category("movie.mp4") == "video"
    assert extension_category("song.flac") == "audio"
    assert extension_category("photo.PNG") == "image"
    assert extension_category("book.pdf") == "document"
    assert extension_category("archive.cbz") == "archive"
    assert extension_category("subs.srt") == "subtitle"


def test_extension_category_unknown_extension():
    assert extension_category("data.xyz123") == "unknown"


def test_similarity_identical_names_scores_100():
    assert similarity("movie.mp4", "movie.mp4") == 100.0


def test_similarity_unrelated_names_scores_low():
    assert similarity("movie.mp4", "completely different title.mkv") < 50.0


def test_similarity_ignores_separator_style_differences():
    assert similarity("My Movie 2024.mp4", "my_movie_2024.mkv") > 90.0


def test_categories_compatible_same_category():
    assert categories_compatible("video", "video") is True


def test_categories_compatible_different_known_categories():
    assert categories_compatible("video", "audio") is False


def test_categories_compatible_permissive_when_either_unknown():
    assert categories_compatible("unknown", "video") is True
    assert categories_compatible("video", "unknown") is True
    assert categories_compatible("unknown", "unknown") is True


def test_matches_keyword_filters_no_filters_matches_everything():
    assert matches_keyword_filters("anything.mp4", [], []) is True


def test_matches_keyword_filters_exclude_blocks_match():
    assert matches_keyword_filters("sample_clip.mp4", [], ["sample"]) is False


def test_matches_keyword_filters_include_requires_match():
    assert matches_keyword_filters("trailer.mp4", ["trailer"], []) is True
    assert matches_keyword_filters("movie.mp4", ["trailer"], []) is False


def test_matches_keyword_filters_exclude_wins_over_include():
    assert matches_keyword_filters("trailer_sample.mp4", ["trailer"], ["sample"]) is False


def test_matches_keyword_filters_case_insensitive():
    assert matches_keyword_filters("SAMPLE.mp4", [], ["sample"]) is False
