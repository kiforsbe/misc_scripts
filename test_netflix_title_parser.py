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
            "title": "Mission",
            "media_kind": "movie",
            "episode_title": "Cross",
            "is_explicit_series": False,
            "has_implicit_split": True,
        },
    )
    assert_parse(
        "A.I.C.O.: Awakening",
        {
            "title": "A.I.C.O.",
            "media_kind": "series",
            "season": None,
            "episode": None,
            "episode_title": "Awakening",
            "is_explicit_series": False,
            "has_implicit_split": True,
        },
    )
    assert_parse(
        "A.I.C.O.: Devotion",
        {
            "title": "A.I.C.O.",
            "media_kind": "series",
            "season": None,
            "episode": None,
            "episode_title": "Devotion",
            "is_explicit_series": False,
            "has_implicit_split": True,
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
        "Black Lightning: Season 2: The Book of Consequences: Chapter One: The Rise of the Green Light Babies",
        {
            "title": "Black Lightning",
            "media_kind": "series",
            "season": 2,
            "season_title": "Season 2: The Book of Consequences",
            "episode": None,
            "episode_title": "Chapter One: The Rise of the Green Light Babies",
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "A Series of Unfortunate Events: Season 1: The Bad Beginning: Part One",
        {
            "title": "A Series of Unfortunate Events",
            "media_kind": "series",
            "season": 1,
            "season_title": "Season 1: The Bad Beginning",
            "episode": None,
            "episode_title": "The Bad Beginning: Part One",
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "Carmen Sandiego: Season 1: Becoming Carmen Sandiego: Part I",
        {
            "title": "Carmen Sandiego",
            "media_kind": "series",
            "season": 1,
            "season_title": "Season 1",
            "episode": None,
            "episode_title": "Becoming Carmen Sandiego: Part I",
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "Case Closed: Zero's Tea Time: TIME.1",
        {
            "title": "Case Closed: Zero's Tea Time",
            "media_kind": "series",
            "season": None,
            "episode": None,
            "episode_title": "TIME.1",
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "Cyberpunk: Edgerunners: Humanity",
        {
            "title": "Cyberpunk: Edgerunners",
            "media_kind": "movie",
            "episode_title": "Humanity",
            "is_explicit_series": False,
            "has_implicit_split": True,
        },
    )
    assert_parse(
        "Cyberpunk: Edgerunners: Girl on Fire",
        {
            "title": "Cyberpunk: Edgerunners",
            "media_kind": "movie",
            "episode_title": "Girl on Fire",
            "is_explicit_series": False,
            "has_implicit_split": True,
        },
    )
    assert_parse(
        "Cyberpunk: Edgerunners: My Moon My Man",
        {
            "title": "Cyberpunk: Edgerunners",
            "media_kind": "movie",
            "episode_title": "My Moon My Man",
            "is_explicit_series": False,
            "has_implicit_split": True,
        },
    )
    assert_parse(
        "Daybreak: Josh vs. the Apocalypse: Part 1",
        {
            "title": "Daybreak",
            "media_kind": "movie",
            "episode_title": "Josh vs. the Apocalypse: Part 1",
            "is_explicit_series": False,
            "has_implicit_split": True,
        },
    )
    assert_parse(
        "Demon Slayer: Kimetsu no Yaiba: Tanjiro Kamado, Unwavering Resolve Arc: Cruelty",
        {
            "title": "Demon Slayer: Kimetsu no Yaiba",
            "media_kind": "series",
            "season": None,
            "season_title": "Tanjiro Kamado, Unwavering Resolve Arc",
            "episode": None,
            "episode_title": "Cruelty",
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "Demon Slayer: Kimetsu no Yaiba: Tanjiro Kamado, Unwavering Resolve Arc: Swordsman Accompanying a Demon",
        {
            "title": "Demon Slayer: Kimetsu no Yaiba",
            "media_kind": "series",
            "season": None,
            "season_title": "Tanjiro Kamado, Unwavering Resolve Arc",
            "episode": None,
            "episode_title": "Swordsman Accompanying a Demon",
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "Chilling Adventures of Sabrina: Part 4: Chapter Thirty-Four: The Returned",
        {
            "title": "Chilling Adventures of Sabrina",
            "media_kind": "series",
            "season": 4,
            "season_title": "Part 4",
            "episode": None,
            "episode_title": "Chapter Thirty-Four: The Returned",
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "Chilling Adventures of Sabrina: Part 1: Chapter Thirteen: The Passion of Sabrina Spellman",
        {
            "title": "Chilling Adventures of Sabrina",
            "media_kind": "series",
            "season": 1,
            "season_title": "Part 1",
            "episode": None,
            "episode_title": "Chapter Thirteen: The Passion of Sabrina Spellman",
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "Chilling Adventures of Sabrina: Part 1: Chapter One: October Country",
        {
            "title": "Chilling Adventures of Sabrina",
            "media_kind": "series",
            "season": 1,
            "season_title": "Part 1",
            "episode": None,
            "episode_title": "Chapter One: October Country",
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "DOTA: Dragon's Blood: Book 1: The Knight, Death and the Devil",
        {
            "title": "DOTA: Dragon's Blood",
            "media_kind": "series",
            "season": 1,
            "season_title": "Book 1",
            "episode": None,
            "episode_title": "The Knight, Death and the Devil",
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "DOTA: Dragon's Blood: Book 3: Consider Phlebas",
        {
            "title": "DOTA: Dragon's Blood",
            "media_kind": "series",
            "season": 3,
            "season_title": "Book 3",
            "episode": None,
            "episode_title": "Consider Phlebas",
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "DOTA: Dragon's Blood: Book 3: Summons of the Ideal",
        {
            "title": "DOTA: Dragon's Blood",
            "media_kind": "series",
            "season": 3,
            "season_title": "Book 3",
            "episode": None,
            "episode_title": "Summons of the Ideal",
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "Stranger Things: Chapter Seven: The Bathtub",
        {
            "title": "Stranger Things",
            "media_kind": "series",
            "season": None,
            "episode": None,
            "episode_title": "Chapter Seven: The Bathtub",
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "Stranger Things: Chapter Four: The Body",
        {
            "title": "Stranger Things",
            "media_kind": "series",
            "season": None,
            "episode": None,
            "episode_title": "Chapter Four: The Body",
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "Stranger Things: Stranger Things 3: Chapter Three: The Case of the Missing Lifeguard",
        {
            "title": "Stranger Things",
            "media_kind": "series",
            "season": None,
            "season_title": "Stranger Things 3",
            "episode": None,
            "episode_title": "Chapter Three: The Case of the Missing Lifeguard",
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "Stranger Things: Stranger Things 4: Chapter One: The Hellfire Club",
        {
            "title": "Stranger Things",
            "media_kind": "series",
            "season": None,
            "season_title": "Stranger Things 4",
            "episode": None,
            "episode_title": "Chapter One: The Hellfire Club",
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "Great Pretender: Season 1: Case 1_4: Los Angeles Connection",
        {
            "title": "Great Pretender",
            "media_kind": "series",
            "season": 1,
            "season_title": "Season 1",
            "episode": None,
            "episode_title": "Case 1_4: Los Angeles Connection",
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "Great Pretender: Season 1: Case 3_1: Snow of London",
        {
            "title": "Great Pretender",
            "media_kind": "series",
            "season": 1,
            "season_title": "Season 1",
            "episode": None,
            "episode_title": "Case 3_1: Snow of London",
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "Great Pretender: Season 2: Case 4_6: Wizard of Far East",
        {
            "title": "Great Pretender",
            "media_kind": "series",
            "season": 2,
            "season_title": "Season 2",
            "episode": None,
            "episode_title": "Case 4_6: Wizard of Far East",
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "Raising Dion: Season 1: ISSUE #101: How Do You Raise a Superhero?",
        {
            "title": "Raising Dion",
            "media_kind": "series",
            "season": 1,
            "season_title": "Season 1",
            "episode": None,
            "episode_title": "ISSUE #101: How Do You Raise a Superhero?",
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "Raising Dion: Season 1: ISSUE #104: Welcome to BIONA. Hope You Survive the Experience",
        {
            "title": "Raising Dion",
            "media_kind": "series",
            "season": 1,
            "season_title": "Season 1",
            "episode": None,
            "episode_title": "ISSUE #104: Welcome to BIONA. Hope You Survive the Experience",
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "Shadowhunters: The Mortal Instruments: Season 3: What Lies Beneath",
        {
            "title": "Shadowhunters: The Mortal Instruments",
            "media_kind": "series",
            "season": 3,
            "season_title": "Season 3",
            "episode": None,
            "episode_title": "What Lies Beneath",
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "Shadowhunters: The Mortal Instruments: Season 3B: A Kiss from a Rose",
        {
            "title": "Shadowhunters: The Mortal Instruments",
            "media_kind": "series",
            "season": 3,
            "season_title": "Season 3B",
            "episode": None,
            "episode_title": "A Kiss from a Rose",
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "The Last Kids on Earth: Book 1: The Last Kids on Earth",
        {
            "title": "The Last Kids on Earth",
            "media_kind": "series",
            "season": 1,
            "season_title": "Book 1",
            "episode": None,
            "episode_title": "The Last Kids on Earth",
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "The Last Kids on Earth: Book 2: Follow That Butler",
        {
            "title": "The Last Kids on Earth",
            "media_kind": "series",
            "season": 2,
            "season_title": "Book 2",
            "episode": None,
            "episode_title": "Follow That Butler",
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "The Last Kids on Earth: Book 2: Stay on Target",
        {
            "title": "The Last Kids on Earth",
            "media_kind": "series",
            "season": 2,
            "season_title": "Book 2",
            "episode": None,
            "episode_title": "Stay on Target",
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "The OA: Part I: Chapter 2: New Colossus",
        {
            "title": "The OA",
            "media_kind": "series",
            "season": 1,
            "season_title": "Part I",
            "episode": 2,
            "episode_title": "New Colossus",
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "The OA: Part I: Chapter 5: Paradise",
        {
            "title": "The OA",
            "media_kind": "series",
            "season": 1,
            "season_title": "Part I",
            "episode": 5,
            "episode_title": "Paradise",
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "The OA: Part II: Chapter 1: Angel of Death",
        {
            "title": "The OA",
            "media_kind": "series",
            "season": 2,
            "season_title": "Part II",
            "episode": 1,
            "episode_title": "Angel of Death",
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "The OA: Part II: Chapter 3: Magic Mirror",
        {
            "title": "The OA",
            "media_kind": "series",
            "season": 2,
            "season_title": "Part II",
            "episode": 3,
            "episode_title": "Magic Mirror",
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "The Order: Season 1: Undeclared: Part 1",
        {
            "title": "The Order",
            "media_kind": "series",
            "season": 1,
            "season_title": "Season 1",
            "episode": None,
            "episode_title": "Undeclared: Part 1",
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "The Order: Season 1: Undeclared: Part 2",
        {
            "title": "The Order",
            "media_kind": "series",
            "season": 1,
            "season_title": "Season 1",
            "episode": None,
            "episode_title": "Undeclared: Part 2",
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "The Order: Season 2: Fear Itself, Part 1",
        {
            "title": "The Order",
            "media_kind": "series",
            "season": 2,
            "season_title": "Season 2",
            "episode": None,
            "episode_title": "Fear Itself, Part 1",
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "The Order: Season 2: Fear Itself, Part 2",
        {
            "title": "The Order",
            "media_kind": "series",
            "season": 2,
            "season_title": "Season 2",
            "episode": None,
            "episode_title": "Fear Itself, Part 2",
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "Warrior Nun: Season 1: Matthew 7:13",
        {
            "title": "Warrior Nun",
            "media_kind": "series",
            "season": 1,
            "season_title": "Season 1",
            "episode": None,
            "episode_title": "Matthew 7:13",
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "Warrior Nun: Season 1: Proverbs 14:1",
        {
            "title": "Warrior Nun",
            "media_kind": "series",
            "season": 1,
            "season_title": "Season 1",
            "episode": None,
            "episode_title": "Proverbs 14:1",
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "Warrior Nun: Season 2: Jeremiah 29:13",
        {
            "title": "Warrior Nun",
            "media_kind": "series",
            "season": 2,
            "season_title": "Season 2",
            "episode": None,
            "episode_title": "Jeremiah 29:13",
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "A.P. Bio: Teacher Jail",
        {
            "title": "A.P. Bio",
            "media_kind": "movie",
            "episode": None,
            "episode_title": "Teacher Jail",
            "is_explicit_series": False,
            "has_implicit_split": True,
        },
    )
    assert_parse(
        "A.P. Bio: Pilot: Catfish",
        {
            "title": "A.P. Bio",
            "media_kind": "movie",
            "episode": None,
            "episode_title": "Pilot: Catfish",
            "is_explicit_series": False,
            "has_implicit_split": True,
        },
    )
    assert_parse(
        "B: The Beginning: Season 1: Episode 1",
        {
            "title": "B: The Beginning",
            "media_kind": "series",
            "season": 1,
            "season_title": "Season 1",
            "episode": 1,
            "episode_title": None,
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "B: The Beginning: Season 2 Succession: Episode 1",
        {
            "title": "B: The Beginning",
            "media_kind": "series",
            "season": 2,
            "season_title": "Season 2",
            "episode": 1,
            "episode_title": None,
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "Borgen - Power & Glory: The Lesser of Two Evils",
        {
            "title": "Borgen - Power & Glory",
            "media_kind": "movie",
            "episode": None,
            "episode_title": "The Lesser of Two Evils",
            "is_explicit_series": False,
            "has_implicit_split": True,
        },
    )
    assert_parse(
        "Borgen - Power & Glory: The Minister Doesn’t Wish to Comment",
        {
            "title": "Borgen - Power & Glory",
            "media_kind": "movie",
            "episode": None,
            "episode_title": "The Minister Doesn’t Wish to Comment",
            "is_explicit_series": False,
            "has_implicit_split": True,
        },
    )
    assert_parse(
        "Borgen - Power & Glory: Inuit Nunaat: Land of the People",
        {
            "title": "Borgen - Power & Glory",
            "media_kind": "movie",
            "episode": None,
            "episode_title": "Inuit Nunaat: Land of the People",
            "is_explicit_series": False,
            "has_implicit_split": True,
        },
    )
    assert_parse(
        "Tokyo Ghoul: Tokyo Ghoul:re: member: Fragments",
        {
            "title": "Tokyo Ghoul: re",
            "media_kind": "movie",
            "episode": None,
            "episode_title": "member: Fragments",
            "is_explicit_series": False,
            "has_implicit_split": True,
        },
    )
    assert_parse(
        "Tokyo Ghoul: Tokyo Ghoul:re: mind: Days of Recollections",
        {
            "title": "Tokyo Ghoul: re",
            "media_kind": "movie",
            "episode": None,
            "episode_title": "mind: Days of Recollections",
            "is_explicit_series": False,
            "has_implicit_split": True,
        },
    )
    assert_parse(
        "Tokyo Ghoul: Tokyo Ghoul:re: Morse: Remembrances",
        {
            "title": "Tokyo Ghoul: re",
            "media_kind": "movie",
            "episode": None,
            "episode_title": "Morse: Remembrances",
            "is_explicit_series": False,
            "has_implicit_split": True,
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
        "Missing: The Other Side: Missing: The Other Side 2: Episode 1",
        {
            "title": "Missing: The Other Side: Missing: The Other Side 2",
            "media_kind": "series",
            "season": None,
            "episode": 1,
            "episode_title": None,
            "is_explicit_series": True,
        },
    )
    assert_parse(
        "SAKAMOTO DAYS: Each One's Mission",
        {
            "title": "SAKAMOTO DAYS",
            "media_kind": "movie",
            "episode_title": "Each One's Mission",
            "is_explicit_series": False,
            "has_implicit_split": True,
        },
    )


if __name__ == "__main__":
    run_tests()
    print("All netflix_title_parser tests passed.")