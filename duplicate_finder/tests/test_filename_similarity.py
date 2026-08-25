from __future__ import annotations

from duplicate_finder.filename_similarity import (
    categories_compatible,
    core_similarity,
    core_title,
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


def test_core_title_strips_single_bracketed_tag():
    assert core_title("Otter Pets (USA).zip") == "otter pets"


def test_core_title_strips_multiple_bracketed_tag_groups():
    assert core_title("Otter Pets (USA) (En,Fr,De) (Rev 1).zip") == "otter pets"


def test_core_title_strips_square_bracket_tags_too():
    assert core_title("Otter Pets [Rev 1].zip") == "otter pets"


def test_core_title_no_tags_present():
    assert core_title("Otter Pets.zip") == "otter pets"


def test_core_title_normalizes_separators_and_case_after_stripping():
    assert core_title("Otter_Pets-Deluxe.EUROPE.zip") == "otter pets deluxe europe"


def test_core_title_does_not_double_stem_names_with_internal_ellipsis():
    """A real observed filename pattern: internal "..." punctuation in the
    title must not be mistaken for a second extension by Path.stem. core_title
    stems the real filename exactly once, before tag-stripping, so this stays
    intact."""
    assert core_title("3,2,1...Words Up! (Europe).zip") == "3,2,1 words up!"
    assert core_title("3,2,1...Words Up! (USA).zip") == "3,2,1 words up!"


def test_core_similarity_identical_core_titles_scores_100_despite_differing_tags():
    assert core_similarity("Otter Pets (Europe).zip", "Otter Pets (USA).zip") == 100.0


def test_core_similarity_numbered_series_entries_score_below_exact_match():
    """Sequential series entries differ by a single character in the core
    title, so they still score high on fuzzy similarity -- but not 100 --
    which is why duplicate detection requires an exact core-title match
    rather than a fuzzy threshold to tell them apart from real duplicates."""
    score = core_similarity("Shadow Realm 1 - Rising (USA).zip", "Shadow Realm 2 - Rising (USA).zip")
    assert 90.0 < score < 100.0
