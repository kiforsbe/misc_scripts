from netflix_title_parser import parse_netflix_title


def assert_parse(raw_title, expected):
    parsed = parse_netflix_title(raw_title)
    for key, expected_value in expected.items():
        actual_value = getattr(parsed, key)
        assert actual_value == expected_value, (
            f"{raw_title!r}: expected {key}={expected_value!r}, got {actual_value!r}"
        )


def run_tests():
    assert_parse(
        "No Time to Die",
        {
            "title": "No Time to Die",
            "media_kind": "movie",
            "is_explicit_series": False,
        },
    )
    assert_parse(
        "Mission: Cross",
        {
            "title": "Mission: Cross",
            "media_kind": "movie",
            "is_explicit_series": False,
        },
    )
    assert_parse(
        "Anne Rice's Mayfair Witches: Season 1: The Witching Hour",
        {
            "title": "Anne Rice's Mayfair Witches",
            "media_kind": "series",
            "season": 1,
            "season_title": "Season 1",
            "episode": None,
            "episode_title": "The Witching Hour",
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "The King's Avatar: Episode 1",
        {
            "title": "The King's Avatar",
            "media_kind": "series",
            "season": None,
            "episode": 1,
            "episode_title": None,
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "Call of the Night: 3Rd Night: A Lot Came Out",
        {
            "title": "Call of the Night",
            "media_kind": "series",
            "season": None,
            "episode": 3,
            "episode_title": "A Lot Came Out",
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "Monster: CHAPTER 1",
        {
            "title": "Monster",
            "media_kind": "series",
            "season": None,
            "episode": 1,
            "episode_title": None,
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "American Primeval: Limited Series: Episode 1",
        {
            "title": "American Primeval",
            "media_kind": "series",
            "season": 1,
            "season_title": "Limited Series",
            "episode": 1,
            "episode_title": None,
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "Trollhunters: Tales of Arcadia: Part 1: Roaming Fees May Apply",
        {
            "title": "Trollhunters: Tales of Arcadia",
            "media_kind": "series",
            "season": 1,
            "season_title": "Part 1",
            "episode": None,
            "episode_title": "Roaming Fees May Apply",
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "SAKAMOTO DAYS: Each One's Mission",
        {
            "title": "SAKAMOTO DAYS: Each One's Mission",
            "media_kind": "movie",
            "is_explicit_series": False,
        },
    )


if __name__ == "__main__":
    run_tests()
    print("All netflix_title_parser tests passed.")