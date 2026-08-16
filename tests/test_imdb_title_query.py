from imdb_title_query import format_table


def test_format_table_renders_grid_style_with_stringified_values():
    rows = [{"tconst": "tt0111161", "primaryTitle": "The Shawshank Redemption"}]
    result = format_table(rows, ["tconst", "primaryTitle"], max_width=12)
    assert result == (
        "tconst    | primaryTitle\n"
        "----------+-------------\n"
        "tt0111161 | The Shawsha…"
    )


def test_format_table_handles_missing_and_none_values():
    rows = [{"tconst": "tt1"}]
    result = format_table(rows, ["tconst", "genres"], max_width=10)
    assert result == "tconst | genres\n-------+-------\ntt1    |       "
