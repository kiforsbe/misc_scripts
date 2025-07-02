import re
import json
import os
# Use the new wrapper
from guessit_wrapper import guessit_wrapper


def color_text(text, color):
    # ANSI color codes for Windows terminal compatibility
    colors = {
        'red': '\033[91m',
        'green': '\033[92m',
        'yellow': '\033[93m',
        'reset': '\033[0m',
    }
    return f"{colors.get(color, '')}{text}{colors['reset']}"


def test_guessit():
    options_path = os.path.join(os.path.dirname(__file__), 'guessit-options.json')
    with open(options_path, 'r', encoding='utf-8') as f:
        options = json.load(f)

    filenames = [
        {
            "filename": "[SubsPlease] Ao no Exorcist - Shimane Illuminati-hen - 03 (720p) [5183EC6A].mkv",
            "expected": {
                "title": "Ao no Exorcist - Shimane Illuminati-hen",
                "episode_title": None,
                "episode": 3,
            },
        },
        {
            "filename": "[SubsPlease] Bartender - Kami no Glass - 06 (720p) [56965B1F].mkv",
            "expected": {
                "title": "Bartender - Kami no Glass",
                "episode_title": None,
                "episode": 6,
            },
        },
        {
            "filename": "[SubsPlease] Haite Kudasai, Takamine-san - 08 (720p) [AC71FD4C].mkv",
            "expected": {
                "title": "Haite Kudasai, Takamine-san",
                "episode": 8
            },
        },
        {
            "filename": "[SubsPlease] Knights of the Zodiac - Saint Seiya S2 Part 2 - 10 (720p) [8383D3A9].mkv",
            "expected": {
                "title": "Knights of the Zodiac - Saint Seiya",
                "season": 2,
                "episode": 10,
                "episode_title": None,
            },
        },
        {
            "filename": "[SubsPlease] Log Horizon S3 - 05v2 (720p) [FE4351B6].mkv",
            "expected": {
                "title": "Log Horizon",
                "season": 3,
                "episode": 5,
                "episode_title": None,
            },
        },
        {
            "filename": "[SubsPlease] Nige Jouzu no Wakagimi - 09.5 (720p) [47A461D7].mkv",
            "expected": {
                "title": "Nige Jouzu no Wakagimi",
                "episode": 9.5,
                "episode_title": None,
            },
        },
        {
            "filename": "[SubsPlease] Left-Hand Layup - 06 (720p) [AD908E4E].mkv",
            "expected": {
                "title": "Left-Hand Layup",
                "episode": 6
            },
        },
        {
            "filename": "[SubsPlease] Touken Ranbu - Hanamaru - Tsuki no Maki (720p) [F4C20F97].mkv",
            "expected": {
                "title": "Touken Ranbu - Hanamaru - Tsuki no Maki",
                "episode_title": None,
                "alternative_title": None,
                "type": "movie"
            },
        },
        {
            "filename": "[SubsPlease] Hokkyoku Hyakkaten no Concierge-san (720p) [5B953354].mkv",
            "expected": {
                "title": "Hokkyoku Hyakkaten no Concierge-san",
                "type": "movie"
            },
        },
        {
            "filename": "[SubsPlease] Great Pretender - Razbliuto (720p) [98ADC4AE].mkv",
            "expected": {
                "title": "Great Pretender - Razbliuto",
                "type": "movie"
            },
        },
        {
            "filename": "[SubsPlease] Tensei shitara Slime Datta Ken Movie - Guren no Kizuna-hen (1080p) [CE6D653A].mkv",
            "expected": {
                "title": "Tensei shitara Slime Datta Ken Movie - Guren no Kizuna-hen",
                "type": "movie"
            },
        },
        {
            "filename": "[SubsPlease] Link Click S2 - 08v2 (720p) [18F845C7].mkv",
            "expected": {
                "title": "Link Click",
                "season": 2,
                "episode": 8,
                "episode_title": None,
            },
        },
        {
            "filename": "[SubsPlease] 2.5-jigen no Ririsa - 13 (720p) [65A15059].mkv",
            "expected": {
                "title": "2.5-jigen no Ririsa",
                "episode": 13,
            },
        },
        {
            "filename": "[Judas] Date A Live - S03E06.mkv",
            "expected": {
                "title": "Date A Live",
                "season": 3,
                "episode": 6,
            },
        },
        {
            "filename": "[Erai-raws] Girls Band Cry - 07 [720p][Multiple Subtitle][7AE16A73].mkv",
            "expected": {
                "title": "Girls Band Cry",
                "episode": 7,
            },
        },
        {
            "filename": "[Erai-raws] Shoukoku no Altair - 12 [1080p AVC-YUV444P10][E-AC3].mkv",
            "expected": {
                "title": "Shoukoku no Altair",
                "episode": 12,
            },
        },
        {
            "filename": "[Reaktor] The Boy and the Beast [1080p][x265][10-bit][Dual-Audio].mkv",
            "expected": {
                "title": "The Boy and the Beast",
                "type": "movie"
            },
        },
        {
            "filename": "[SubsPlease] Tensei Kizoku, Kantei Skill de Nariagaru - 23 (720p) [E2EA4A90].mkv",
            "expected": {
                "title": "Tensei Kizoku, Kantei Skill de Nariagaru",
                "episode": 23,
            },
        },
        {
            "filename": "[Judas] Sword Art Online - Movie 01 (Ordinal Scale) [BD 1080p][HEVC x265 10bit][Dual-Audio][Eng-Subs].mkv",
            "expected": {
                "title": "Sword Art Online - Movie 01 (Ordinal Scale)",
                "type": "movie"
            },
        },
        {
            "filename": "[SubsPlease] Detective Conan - 1142 (720p) [FCFE40AA].mkv",
            "expected": {
                "title": "Detective Conan",
                "episode": 1142,
            },
        },
        {
            "filename": "[SubsPlease] VTuber Nandaga Haishin Kiri Wasuretara Densetsu ni Natteta - 09 (720p) [EFE18972].mkv",
            "expected": {
                "title": "VTuber Nandaga Haishin Kiri Wasuretara Densetsu ni Natteta",
                "episode": 9
            },
        },
        {
            "filename": "[-__-'] Alderamin on the Sky - 07 [BD 1080p] [A328BCC9].mkv",
            "expected": {
                "title": "Alderamin on the Sky",
                "episode": 7
            },
        },
        {
            "filename": "[HorribleSubs] Itai no wa Iya nano de Bougyoryoku ni Kyokufuri Shitai to Omoimasu - 08 [720p].mkv",
            "expected": {
                "title": "Itai no wa Iya nano de Bougyoryoku ni Kyokufuri Shitai to Omoimasu",
                "episode": 8
            },
        },
        {
            "filename": "[SubsPlease] The Prince of Tennis II - U-17 World Cup Semifinal - 08 (720p) [0A753A4F].mkv",
            "expected": {
                "title": "The Prince of Tennis II - U-17 World Cup Semifinal",
                "episode": 8
            },
        },
        {
            "filename": "[Anime Time] Gekijouban Sword Art Online Progressive - Kuraki Yuuyami no Scherzo (2022) [BD] [1080p][HEVC 10bit x265][AAC][Eng Sub].mkv",
            "expected": {
                "title": "Gekijouban Sword Art Online Progressive - Kuraki Yuuyami no Scherzo",
                "type": "movie"
            },
        },
        {
            "filename": "star.trek.lower.decks.s05e06.1080p.web.h264-successfulcrab[EZTVx.to].mkv",
            "expected": {
                "title": "star trek lower decks",
                "season": 5,
                "episode": 6
            },
        },
        {
            "filename": "The.Legend.of.Korra.S01E06.And.the.Winner.Is.1080p.BluRay.DDP.5.1.H.265.-EDGE2020.mkv",
            "expected": {
                "title": "The Legend of Korra",
                "season": 1,
                "episode": 6,
                "episode_title": "And the Winner Is",
            },
        },
        {
            "filename": "Alex Rider S03E06 Target 1080p AMZN WEB-DL DDP5 1 H 264-FLUX.mkv",
            "expected": {
                "title": "Alex Rider",
                "season": 3,
                "episode": 6,
                "episode_title": "Target",
            },
        },
        {
            "filename": "[SubsPlease] Ore wa Seikan Kokka no Akutoku Ryoushu - 02v2 (720p) [93E85391].mkv",
            "expected": {
                "title": "Ore wa Seikan Kokka no Akutoku Ryoushu",
                "episode": 2
            },
        },
        {
            "filename": "[SubsPlease] Ore wa Seikan Kokka no Akutoku Ryoushu - 05 (720p) [E3527CF0].mkv",
            "expected": {
                "title": "Ore wa Seikan Kokka no Akutoku Ryoushu",
                "episode": 5
            },
        },
        {
            "filename": "[SubsPlease] Kuroshitsuji - Kishuku Gakkou-hen - 01v2 (720p) [119B837E].mkv",
            "expected": {
                "title": "Kuroshitsuji - Kishuku Gakkou-hen",
                "episode": 1
            },
        },
        {
            "filename": "[SubsPlease] Kuroshitsuji - Kishuku Gakkou-hen - 02 (720p) [0C6E5DB6].mkv",
            "expected": {
                "title": "Kuroshitsuji - Kishuku Gakkou-hen",
                "episode": 2
            },
        },
        {
            "filename": "[SubsPlease] Kuroshitsuji - Midori no Majo-hen - 11v2 (720p) [361B736C].mkv",
            "expected": {
                "title": "Kuroshitsuji - Midori no Majo-hen",
                "episode": 11
            },
        },
        {
            "filename": "[SubsPlease] Kuroshitsuji - Midori no Majo-hen - 12 (720p) [DD783C07].mkv",
            "expected": {
                "title": "Kuroshitsuji - Midori no Majo-hen",
                "episode": 12
            },
        },
        {
            "filename": "[404] Ai Tenchi Muyo! - 1x07 - Tenchi Encounter [BD 1080p x264 FLAC][22B0E3B4].mkv",
            "expected": {
                "title": "Ai Tenchi Muyo!",
                "season": 1,
                "episode": 7,
                "episode_title": "Tenchi Encounter",
            },
        },
        {
            "filename": "[Judas] Akame ga Kill! - 09.mkv",
            "expected": {
                "title": "Akame ga Kill!",
                "episode": 9
            },
        },
        {
            "filename": "Hai to Gensou no Grimgar S01E05-Crying Doesn't Mean You're Weak. Enduring Doesn't Mean You're Strong [D4027016].mkv",
            "expected": {
                "title": "Hai to Gensou no Grimgar",
                "season": 1,
                "episode": 5,
                "episode_title": "Crying Doesn't Mean You're Weak. Enduring Doesn't Mean You're Strong",
            },
        },
        {
            "filename": "[Judas] Date A Live - S01E06.mkv",
            "expected": {
                "title": "Date A Live",
                "season": 1,
                "episode": 6
            },
        },
        {
            "filename": "[Judas] Date A Live - S01E13 - OVA.mkv",
            "expected": {
                "title": "Date A Live",
                "season": 1,
                "episode": 13,
                "episode_title": "OVA"
            },
        },
        {
            "filename": "[Judas] Date A Live S01 NCED - 01.mkv",
            "expected": {
                "title": "Date A Live",
                "season": 1,
                "episode": 1,
                "episode_title": "NCED",
                "type": "extra"
            },
        },
        {
            "filename": "[Judas] Date A Live S01 NCOP - 01.mkv",
            "expected": {
                "title": "Date A Live",
                "season": 1,
                "episode": 1,
                "episode_title": "NCOP",
                "type": "extra"
            },
        },
        {
            "filename": "[Judas] Date A Live - The Movie - Mayuri Judgement.mkv",
            "expected": {
                "title": "Date A Live - The Movie - Mayuri Judgement",
                "type": "movie"
            }
        },
        {
            "filename": "[Chihiro] In the Land of Leadale NCED [Blu-ray 1080p Hi10P FLAC][15C5F20E].mkv",
            "expected": {
                "title": "In the Land of Leadale",
                "episode_title": "NCED",
                "type": "extra"
            },
        },
        {
            "filename": "[Chihiro] In the Land of Leadale NCOP [Blu-ray 1080p Hi10P FLAC][9E7D276B].mkv",
            "expected": {
                "title": "In the Land of Leadale",
                "episode_title": "NCOP",
                "type": "extra"
            },
        },
        {
            "filename": "[Chihiro] In the Land of Leadale Preview [Blu-ray 1080p Hi10P FLAC][FD430E33].mkv",
            "expected": {
                "title": "In the Land of Leadale",
                "episode_title": "Preview",
                "type": "extra"
            },
        },
        {
            "filename": "[EngelGroup&mastress] Mahou Shoujo Pretty Sammy 18 [DVD 448p x264 AC3][3BC9A702].mkv",
            "expected": {
                "title": "Mahou Shoujo Pretty Sammy",
                "episode": 18,
            },
        },
        {
            "filename": "[EngelGroup&mastress] Mahou Shoujo Pretty Sammy 26 [DVD 448p x264 AC3][2832A92F].mkv",
            "expected": {
                "title": "Mahou Shoujo Pretty Sammy",
                "episode": 26,
            },
        },
        {
            "filename": "[Kosaka] Noragami - 01 - A Housecat, a Stray God, and a Tail [78F3D1D2].mkv",
            "expected": {
                "title": "Noragami",
                "episode": 1,
                "episode_title": "A Housecat, a Stray God, and a Tail"
            },
        },
        {
            "filename": "[Kosaka] Noragami - 07 - Uncertainty & Destiny [E67C6CA4].mkv",
            "expected": {
                "title": "Noragami",
                "episode": 7,
                "episode_title": "Uncertainty & Destiny"
            },
        },
        {
            "filename": "[Kosaka] Noragami Aragoto - 05 - Divine Acclamation, Imprecation [C8C75050].mkv",
            "expected": {
                "title": "Noragami Aragoto",
                "episode": 5,
                "episode_title": "Divine Acclamation, Imprecation"
            },
        },
        {
            "filename": "[SubsPlease] Arknights - Reimei Zensou - 01v2 (720p) [740E2F58].mkv",
            "expected": {
                "title": "Arknights - Reimei Zensou",
                "episode": 1,
                "type": "episode",
            }
        },
        {
            "filename": "[SubsPlease] Arknights - Reimei Zensou - 08v2 (720p) [7BC3AB4B].mkv",
            "expected": {
                "title": "Arknights - Reimei Zensou",
                "episode": 8,
                "type": "episode",
                "version": 2,
            }
        },
        {
            "filename": "[SubsPlease] Arknights - Reimei Zensou - 04 (720p) [5FE07317].mkv",
            "expected": {
                "title": "Arknights - Reimei Zensou",
                "episode": 4,
                "type": "episode",
            }
        },
        {
            "filename": "[SubsPlease] Arknights - Reimei Zensou - 07 (720p) [4D98D8A9].mkv",
            "expected": {
                "title": "Arknights - Reimei Zensou",
                "episode": 7,
                "type": "episode",
            }
        }
    ]

    pass_count = 0
    fail_count = 0
    for entry in filenames:
        filename = entry["filename"]
        expected = entry["expected"]
        result = guessit_wrapper(filename, options=options)

        all_match = True
        mismatch_lines = []
        for key, value in expected.items():
            actual = result.get(key)
            if actual != value:
                mark = color_text('✘', 'red')
                mismatch_lines.append(f"  {mark} {key}: expected = {repr(value)} | actual = {repr(actual)}")
                all_match = False
        extra_keys = set(result.keys()) - set(expected.keys())
        if all_match:
            pass_count += 1
        else:
            fail_count += 1
            print(f"{'='*10}")
            print(f"Filename: {filename}")
            for line in mismatch_lines:
                print(line)
            if extra_keys:
                key_value_pairs = zip(
                    sorted(extra_keys),
                    [result[key] for key in sorted(extra_keys)]
                )
                print(color_text(f"  [Info] Extra fields in result: {dict(key_value_pairs)}", 'yellow'))
    print(color_text(f"{pass_count} / {len(filenames)} cases passed.", 'green' if fail_count == 0 else 'yellow'))
    if fail_count == 0:
        print(color_text("All test cases passed!", 'green'))
    else:
        print(color_text(f"{fail_count} test case(s) failed.", 'red'))

if __name__ == "__main__":
    test_guessit()
