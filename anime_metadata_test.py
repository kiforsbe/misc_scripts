import os
import argparse
from guessit_wrapper import guessit_wrapper

provider = None

try:
    import sys
    # Load this library from subfolder video-optimizer-v2
    sys.path.append(os.path.join(os.path.dirname(__file__), 'video-optimizer-v2'))
    from anime_metadata import AnimeDataProvider
    from metadata_provider import TitleInfo, EpisodeInfo, MatchResult

    provider = AnimeDataProvider()
except ImportError:
    print("Warning: metadata_provider not found. Enhanced metadata features will be disabled.")

def color_text(text, color):
    colors = {
        'red': '\033[91m',
        'green': '\033[92m',
        'yellow': '\033[93m',
        'reset': '\033[0m',
    }
    return f"{colors.get(color, '')}{text}{colors['reset']}"

def test_anime_metadata(verbosity=0):
    # Test cases adapted from test_guessit.py
    test_cases = [
        {
            "filename": "[SubsPlease] Ao no Exorcist - Shimane Illuminati-hen - 03 (720p) [5183EC6A].mkv",
            "expected": {
                "title": "Ao no Exorcist - Shimane Illuminati-hen",
                "episode": 3,
            },
        },
        {
            "filename": "[SubsPlease] Bartender - Kami no Glass - 06 (720p) [56965B1F].mkv",
            "expected": {
                "title": "Bartender - Kami no Glass",
                "episode": 6,
            },
        },
        {
            "filename": "[SubsPlease] Haite Kudasai, Takamine-san - 08 (720p) [AC71FD4C].mkv",
            "expected": {
                "title": "Haite Kudasai, Takamine-san",
                "episode": 8,
            },
        },
        {
            "filename": "[SubsPlease] Knights of the Zodiac - Saint Seiya S2 Part 2 - 10 (720p) [8383D3A9].mkv",
            "expected": {
                "title": "Knights of the Zodiac - Saint Seiya",
                "season": 2,
                "episode": 10,
            },
        },
        {
            "filename": "[SubsPlease] Log Horizon S3 - 05v2 (720p) [FE4351B6].mkv",
            "expected": {
                "title": "Log Horizon",
                "season": 3,
                "episode": 5,
                "version": 2,
            },
        },
        {
            "filename": "[SubsPlease] Nige Jouzu no Wakagimi - 09.5 (720p) [47A461D7].mkv",
            "expected": {
                "title": "Nige Jouzu no Wakagimi",
                "episode": 9.5,
            },
        },
        {
            "filename": "[SubsPlease] Left-Hand Layup - 06 (720p) [AD908E4E].mkv",
            "expected": {
                "title": "Left-Hand Layup",
                "episode": 6,
            },
        },
        {
            "filename": "[SubsPlease] Touken Ranbu - Hanamaru - Tsuki no Maki (720p) [F4C20F97].mkv",
            "expected": {
                "title": "Touken Ranbu - Hanamaru - Tsuki no Maki",
            },
        },
        {
            "filename": "[SubsPlease] Hokkyoku Hyakkaten no Concierge-san (720p) [5B953354].mkv",
            "expected": {
                "title": "Hokkyoku Hyakkaten no Concierge-san",
            },
        },
        {
            "filename": "[SubsPlease] Great Pretender - Razbliuto (720p) [98ADC4AE].mkv",
            "expected": {
                "title": "Great Pretender - Razbliuto",
            },
        },
        {
            "filename": "[SubsPlease] Tensei shitara Slime Datta Ken Movie - Guren no Kizuna-hen (1080p) [CE6D653A].mkv",
            "expected": {
                "title": "Tensei shitara Slime Datta Ken Movie - Guren no Kizuna-hen",
            },
        },
        {
            "filename": "[SubsPlease] Link Click S2 - 08v2 (720p) [18F845C7].mkv",
            "expected": {
                "title": "Link Click",
                "season": 2,
                "episode": 8,
                "version": 2,
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
            },
        },
        {
            "filename": "[SubsPlease] Tensei Kizoku, Kantei Skill de Nariagaru - 23 (720p) [E2EA4A90].mkv",
            "expected": {
                "title": "Tensei Kizoku, Kantei Skill de Nariagaru",
                "season": 2,
                "episode": 11,
            },
        },
        {
            "filename": "[Judas] Sword Art Online - Movie 01 (Ordinal Scale) [BD 1080p][HEVC x265 10bit][Dual-Audio][Eng-Subs].mkv",
            "expected": {
                "title": "Sword Art Online - Movie 01 (Ordinal Scale)",
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
                "episode": 9,
            },
        },
        {
            "filename": "[-__-'] Alderamin on the Sky - 07 [BD 1080p] [A328BCC9].mkv",
            "expected": {
                "title": "Alderamin on the Sky",
                "episode": 7,
            },
        },
        {
            "filename": "[HorribleSubs] Itai no wa Iya nano de Bougyoryoku ni Kyokufuri Shitai to Omoimasu - 08 [720p].mkv",
            "expected": {
                "title": "Itai no wa Iya nano de Bougyoryoku ni Kyokufuri Shitai to Omoimasu",
                "episode": 8,
            },
        },
        {
            "filename": "[SubsPlease] The Prince of Tennis II - U-17 World Cup Semifinal - 08 (720p) [0A753A4F].mkv",
            "expected": {
                "title": "The Prince of Tennis II - U-17 World Cup Semifinal",
                "episode": 8,
            },
        },
        {
            "filename": "[Anime Time] Gekijouban Sword Art Online Progressive - Kuraki Yuuyami no Scherzo (2022) [BD] [1080p][HEVC 10bit x265][AAC][Eng Sub].mkv",
            "expected": {
                "title": "Gekijouban Sword Art Online Progressive - Kuraki Yuuyami no Scherzo",
            },
        },
        {
            "filename": "[SubsPlease] Ore wa Seikan Kokka no Akutoku Ryoushu - 02v2 (720p) [93E85391].mkv",
            "expected": {
                "title": "Ore wa Seikan Kokka no Akutoku Ryoushu",
                "episode": 2,
                "version": 2,
            },
        },
        {
            "filename": "[SubsPlease] Ore wa Seikan Kokka no Akutoku Ryoushu - 05 (720p) [E3527CF0].mkv",
            "expected": {
                "title": "Ore wa Seikan Kokka no Akutoku Ryoushu",
                "episode": 5,
            },
        },
        {
            "filename": "[SubsPlease] Kuroshitsuji - Kishuku Gakkou-hen - 01v2 (720p) [119B837E].mkv",
            "expected": {
                "title": "Kuroshitsuji - Kishuku Gakkou-hen",
                "episode": 1,
                "version": 2,
            },
        },
        {
            "filename": "[SubsPlease] Kuroshitsuji - Kishuku Gakkou-hen - 02 (720p) [0C6E5DB6].mkv",
            "expected": {
                "title": "Kuroshitsuji - Kishuku Gakkou-hen",
                "episode": 2,
            },
        },
        {
            "filename": "[SubsPlease] Kuroshitsuji - Midori no Majo-hen - 11v2 (720p) [361B736C].mkv",
            "expected": {
                "title": "Kuroshitsuji - Midori no Majo-hen",
                "episode": 11,
                "version": 2,
            },
        },
        {
            "filename": "[SubsPlease] Kuroshitsuji - Midori no Majo-hen - 12 (720p) [DD783C07].mkv",
            "expected": {
                "title": "Kuroshitsuji - Midori no Majo-hen",
                "episode": 12,
            },
        },
        {
            "filename": "[SubsPlease] Kaijuu 8-gou - 23 (720p) [0F5118FB].mkv",
            "expected": {
                "title": "Kaijuu 8-gou",
                "season": 2,
                "episode": 11,
            },
        },
        {
            "filename": "[SubsPlease] Shangri-La Frontier - 12 (720p) [D4C8F1E3].mkv",
            "expected": {
                "title": "Shangri-La Frontier",
                "season": 1,
                "episode": 12,
            }
        },
        {
            "filename": "[SubsPlease] Shangri-La Frontier - 27 (720p) [D4C8F1E3].mkv",
            "expected": {
                "title": "Shangri-La Frontier",
                "season": 2,
                "episode": 2,
            }
        },
        {
            "filename": "[SubsPlease] Sorairo Utility - 01 (720p) [0C457B5F].mkv",
            "expected": {
                "title": "Sorairo Utility (TV)",
                "season": 1,
                "episode": 1,
                "mal_id": 58066,
            }
        },
        {
            "filename": "[SubsPlease] Sorairo Utility - 02 (720p) [A96408EB].mkv",
            "expected": {
                "title": "Sorairo Utility (TV)",
                "season": 1,
                "episode": 2,
                "mal_id": 58066,
            }
        },
        {
            "filename": "[SubsPlease] Jidou Hanbaiki ni Umarekawatta Ore wa Meikyuu wo Samayou S2 - 01 (720p) [C3DD45C9].mkv",
            "expected": {
                "title": "Jidou Hanbaiki ni Umarekawatta Ore wa Meikyuu wo Samayou",
                "season": 2,
                "episode": 1,
                "mal_id": 56700,
            }
        },
        {
            "filename": "[SubsPlease] Gorilla no Kami - 01 (720p) [121AD8F1].mkv",
            "expected": {
                "title": "Gorilla no Kami",
                "season": 1,
                "episode": 1,
                "mal_id": 59935,
            }
        },
        {
            "filename": "[SubsPlease] Gorilla no Kami - 03 (720p) [34CF04F3].mkv",
            "expected": {
                "title": "Gorilla no Kami",
                "season": 1,
                "episode": 3,
                "mal_id": 59935,
            }
        },
        {
            "filename": "[Erai-raws] Your Forma - 01v2 [720p ADN WEB-DL AVC AAC][MultiSub][13AE09D6].mkv",
            "expected": {
                "title": "Your Forma",
                "season": 1,
                "episode": 1,
                "version": 2,
                "mal_id": 55995,
                "release_group": "Erai-raws",
            }
        },
        {
            "filename": "[SubsPlease] Yuusha Party wo Oidasareta Kiyoubinbou - 03 (720p) [65BCA59E].mkv",
            "expected": {
                "title": "Yuusha Party wo Oidasareta Kiyoubinbou",
                "season": 1,
                "episode": 3,
                "mal_id": 61128,
            }
        }
    ]

    pass_count = 0
    fail_count = 0
    failed_tests = []
    
    for entry in test_cases:
        filename = entry["filename"]
        expected = entry["expected"]
        # --- Use guessit_wrapper to extract episode number ---
        guess = guessit_wrapper(filename)
        title = guess.get("title")
        season_num = guess.get("season", None)
        if season_num:
            title = f"{title} Season {season_num}"
        episode_num = guess.get("episode")
        
        # Use the filename as the title to search in anime_metadata
        title_result = None
        if provider:
            title_result = provider.find_title(title, year=guess.get("year"))
        
        # If we found the anime, use it to calculate proper season/episode
        if title_result and provider:
            episode_result = provider.get_episode_info(title_result.info.id, season_num, episode_num)
            if episode_result:
                # Update guess with calculated season/episode
                guess['season'] = episode_result.season
                guess['episode'] = episode_result.episode
            
            # Extract MAL ID from sources if available
            if title_result.info.sources:
                for source in title_result.info.sources:
                    if "myanimelist.net/anime/" in source:
                        try:
                            mal_id = int(source.rstrip('/').split('/')[-1])
                            guess['mal_id'] = mal_id
                            break
                        except (ValueError, IndexError):
                            pass
        
        # Prepare output data
        output_lines = []
        output_lines.append(f"File: {filename}")
        
        if title_result is None:
            output_lines.append(f"{color_text('✘ No match found in anime_metadata', 'red')}")
        else:
            info: TitleInfo = title_result.info
            output_lines.append(f"{info.title}")

            episode_result = None
            if provider:
                episode_result = provider.get_episode_info(info.id, season_num, episode_num)
            if episode_result is None:
                output_lines.append(f"{info.year or '----':<6}")
            else:
                epinfo: EpisodeInfo = episode_result
                output_lines.append(f"{info.year or '----':<6} {epinfo.episode or '---':>3} / {info.total_episodes or '---':>3} [{info.sources[0]}]")
        
        actual = guess

        # Start matching to expected values
        all_match = True
        mismatch_lines = []
        for key, value in expected.items():
            actual_value = actual.get(key)
            if actual_value != value:
                mark = color_text('✘', 'red')
                mismatch_lines.append(f"  {mark} {key}: expected = {repr(value)} | actual = {repr(actual_value)}")
                all_match = False
        extra_keys = set(actual.keys()) - set(expected.keys())
        
        if all_match:
            pass_count += 1
            # At verbosity level 2, show all tests including passes
            if verbosity >= 2:
                for line in output_lines:
                    print(line)
                # At verbosity level 3+, show full comparison for passed tests
                if verbosity >= 3:
                    print(f"{'='*10}")
                    for key, value in expected.items():
                        actual_value = actual.get(key)
                        mark = color_text('✓', 'green')
                        print(f"  {mark} {key}: expected = {repr(value)} | actual = {repr(actual_value)}")
                    if extra_keys:
                        key_value_pairs = zip(
                            sorted(extra_keys),
                            [actual[key] for key in sorted(extra_keys)]
                        )
                        print(color_text(f"  [Info] Extra fields in result: {dict(key_value_pairs)}", 'yellow'))
                else:
                    print(color_text("  ✓ Test passed", 'green'))
        else:
            fail_count += 1
            failed_tests.append({
                'filename': filename,
                'output_lines': output_lines,
                'mismatch_lines': mismatch_lines,
                'extra_keys': extra_keys,
                'actual': actual
            })
            
            # At verbosity level 2, show failures inline
            if verbosity >= 2:
                for line in output_lines:
                    print(line)
                print(f"{'='*10}")
                for line in mismatch_lines:
                    print(line)
                if extra_keys:
                    key_value_pairs = zip(
                        sorted(extra_keys),
                        [actual[key] for key in sorted(extra_keys)]
                    )
                    print(color_text(f"  [Info] Extra fields in result: {dict(key_value_pairs)}", 'yellow'))
    
    # At verbosity level 1, show failed tests
    if verbosity == 1 and failed_tests:
        print("\nFailed tests:")
        print("=" * 50)
        for test in failed_tests:
            for line in test['output_lines']:
                print(line)
            print(f"{'='*10}")
            for line in test['mismatch_lines']:
                print(line)
            if test['extra_keys']:
                key_value_pairs = zip(
                    sorted(test['extra_keys']),
                    [test['actual'][key] for key in sorted(test['extra_keys'])]
                )
                print(color_text(f"  [Info] Extra fields in result: {dict(key_value_pairs)}", 'yellow'))
            print()
    
    # Show summary at all verbosity levels
    if verbosity >= 1 or fail_count > 0:
        print()
    print(color_text(f"{pass_count} / {len(test_cases)} cases passed.", 'green' if fail_count == 0 else 'yellow'))
    if fail_count == 0:
        print(color_text("All test cases passed!", 'green'))
    else:
        print(color_text(f"{fail_count} test case(s) failed.", 'red'))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Test anime metadata extraction')
    parser.add_argument('-v', '--verbosity', action='count', default=0,
                        help='Increase verbosity level (use -v for failures only, -vv for all tests, -vvv for full comparison)')
    args = parser.parse_args()
    
    test_anime_metadata(verbosity=args.verbosity)
