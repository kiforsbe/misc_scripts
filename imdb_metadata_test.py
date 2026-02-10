import os
import time
import argparse
from guessit_wrapper import guessit_wrapper

provider = None

try:
    import sys
    # Load this library from subfolder video-optimizer-v2
    sys.path.append(os.path.join(os.path.dirname(__file__), 'video-optimizer-v2'))
    from imdb_metadata import IMDbDataProvider
    from metadata_provider import TitleInfo, EpisodeInfo, MatchResult

    provider = IMDbDataProvider()
except ImportError:
    print("Warning: metadata_provider not found. Enhanced metadata features will be disabled.")

def color_text(text, color):
    colors = {
        'red': '\033[91m',
        'green': '\033[92m',
        'yellow': '\033[93m',
        'blue': '\033[94m',
        'cyan': '\033[96m',
        'reset': '\033[0m',
    }
    return f"{colors.get(color, '')}{text}{colors['reset']}"

def print_test_failure(filename, mismatch_lines, extra_keys, actual):
    """Print detailed failure information for a test case."""
    print(f"\n{'='*80}")
    print(color_text(f"✘ FAIL | {filename}", 'red'))
    print(f"{'='*80}")
    for line in mismatch_lines:
        print(line)
    if extra_keys:
        key_value_pairs = {key: actual[key] for key in sorted(extra_keys)}
        print(color_text(f"  [Info] Extra fields in result: {key_value_pairs}", 'yellow'))

def print_test_pass(filename, actual):
    """Print simple pass message for a test case including timing."""
    timings = []
    if actual.get('search_time_ms') is not None:
        timings.append(f"search={actual.get('search_time_ms')}ms")
    if actual.get('episode_search_time_ms') is not None:
        timings.append(f"episode={actual.get('episode_search_time_ms')}ms")
    timing_str = (' | ' + ', '.join(timings)) if timings else ''
    print(color_text(f"✓ {filename}{timing_str}", 'green'))

def print_test_full(filename, all_match, expected, actual, extra_keys):
    """Print full detailed comparison for a test case."""
    status_mark = color_text('✓ PASS', 'green') if all_match else color_text('✘ FAIL', 'red')
    print(f"\n{'='*80}")
    print(f"{status_mark} | {filename}")
    print(f"{'='*80}")
    
    # Show expected values
    print(color_text("\nExpected:", 'cyan'))
    for key, value in sorted(expected.items()):
        print(f"  {key}: {repr(value)}")
    # Timings
    if actual.get('search_time_ms') is not None or actual.get('episode_search_time_ms') is not None:
        print(color_text("\nTimings:", 'blue'))
        if actual.get('search_time_ms') is not None:
            print(f"  title lookup: {actual.get('search_time_ms')} ms")
        if actual.get('episode_search_time_ms') is not None:
            print(f"  episode lookup: {actual.get('episode_search_time_ms')} ms")
    
    # Show actual values for expected keys
    print(color_text("\nActual (expected keys):", 'cyan'))
    for key in sorted(expected.keys()):
        actual_value = actual.get(key)
        matches = expected[key] == actual_value
        mark = color_text('✓', 'green') if matches else color_text('✘', 'red')
        print(f"  {mark} {key}: {repr(actual_value)}")
    
    # Show extra fields if any
    if extra_keys:
        print(color_text("\nExtra fields in result:", 'yellow'))
        for key in sorted(extra_keys):
            print(f"  {key}: {repr(actual[key])}")

def test_imdb_metadata(verbosity=1):
    # A mix of TV shows and movies, with expected titles as in IMDb
    test_cases = [
        {
            "filename": "Breaking.Bad.S05E14.720p.HDTV.x264-IMMERSE.mkv",
            "expected": {"title": "Breaking Bad", "season": 5, "episode": 14, "start_year": 2008, "end_year": 2013, "status": "Ended", "total_episodes": 62, "total_seasons": 5},
        },
        {
            "filename": "Game.of.Thrones.S08E03.1080p.WEB.H264-MEMENTO.mkv",
            "expected": {"title": "Game of Thrones", "season": 8, "episode": 3, "start_year": 2011, "end_year": 2019, "status": "Ended", "total_episodes": 73, "total_seasons": 8},
        },
        {
            "filename": "The.Office.US.S02E01.720p.BluRay.x264-SiNNERS.mkv",
            "expected": {"title": "The Office", "season": 2, "episode": 1, "start_year": 2005, "end_year": 2013, "status": "Ended", "total_episodes": 191, "total_seasons": 9},
        },
        {
            "filename": "Friends.S10E17E18.720p.BluRay.x264-SiNNERS.mkv",
            "expected": {"title": "Friends", "season": 10, "episode": [17, 18], "start_year": 1994, "end_year": 2004, "status": "Ended", "total_episodes": 235, "total_seasons": 10},
        },
        {
            "filename": "The.Wire.S03E12.720p.HDTV.x264-CTU.mkv",
            "expected": {"title": "The Wire", "season": 3, "episode": 12, "start_year": 2002, "end_year": 2008, "status": "Ended", "total_episodes": 60, "total_seasons": 5},
        },
        {
            "filename": "Better.Call.Saul.S06E13.1080p.WEB.H264-CAKES.mkv",
            "expected": {"title": "Better Call Saul", "season": 6, "episode": 13, "start_year": 2015, "end_year": 2022, "status": "Ended", "total_episodes": 63, "total_seasons": 6},
        },
        {
            "filename": "The.Sopranos.S06E21.720p.BluRay.x264-REWARD.mkv",
            "expected": {"title": "The Sopranos", "season": 6, "episode": 21, "start_year": 1999, "end_year": 2007, "status": "Ended", "total_episodes": 86, "total_seasons": 6},
        },
        {
            "filename": "Lost.S04E01.720p.HDTV.x264-CTU.mkv",
            "expected": {"title": "Lost", "season": 4, "episode": 1, "start_year": 2004, "end_year": 2010, "status": "Ended", "total_episodes": 121, "total_seasons": 6},
        },
        {
            "filename": "The.Mandalorian.S02E08.2160p.DSNP.WEB-DL.DDP5.1.Atmos.HDR.HEVC-TOMMY.mkv",
            "expected": {"title": "The Mandalorian", "season": 2, "episode": 8, "start_year": 2019, "end_year": None, "status": "Continuing", "total_episodes": 25, "total_seasons": 4},
        },
        {
            "filename": "Stranger.Things.S04E09.1080p.NF.WEB-DL.DDP5.1.Atmos.x264-NTb.mkv",
            "expected": {"title": "Stranger Things", "season": 4, "episode": 9, "start_year": 2016, "end_year": 2025, "status": "Ended", "total_episodes": 42, "total_seasons": 5},
        },
        {
            "filename": "The.Matrix.1999.1080p.BluRay.x264.YIFY.mp4",
            "expected": {"title": "The Matrix", "year": 1999, "start_year": 1999, "end_year": None, "status": None, "total_episodes": None, "total_seasons": None},
        },
        {
            "filename": "Inception.2010.1080p.BluRay.x264.YIFY.mp4",
            "expected": {"title": "Inception", "year": 2010, "start_year": 2010, "end_year": None, "status": None, "total_episodes": None, "total_seasons": None},
        },
        {
            "filename": "The.Godfather.1972.1080p.BluRay.x264.YIFY.mp4",
            "expected": {"title": "The Godfather", "year": 1972},
        },
        {
            "filename": "Pulp.Fiction.1994.1080p.BluRay.x264.YIFY.mp4",
            "expected": {"title": "Pulp Fiction", "year": 1994},
        },
        {
            "filename": "The.Dark.Knight.2008.1080p.BluRay.x264.YIFY.mp4",
            "expected": {"title": "The Dark Knight", "year": 2008},
        },
        {
            "filename": "Forrest.Gump.1994.1080p.BluRay.x264.YIFY.mp4",
            "expected": {"title": "Forrest Gump", "year": 1994},
        },
        {
            "filename": "The.Shawshank.Redemption.1994.1080p.BluRay.x264.YIFY.mp4",
            "expected": {"title": "The Shawshank Redemption", "year": 1994},
        },
        {
            "filename": "Interstellar.2014.1080p.BluRay.x264.YIFY.mp4",
            "expected": {"title": "Interstellar", "year": 2014},
        },
        {
            "filename": "Fight.Club.1999.1080p.BluRay.x264.YIFY.mp4",
            "expected": {"title": "Fight Club", "year": 1999},
        },
        {
            "filename": "Se7en.1995.1080p.BluRay.x264.YIFY.mp4",
            "expected": {"title": "Se7en", "year": 1995},
        },
        {
            "filename": "The.Lord.of.the.Rings.The.Fellowship.of.the.Ring.2001.1080p.BluRay.x264.YIFY.mp4",
            "expected": {"title": "The Lord of the Rings: The Fellowship of the Ring", "year": 2001},
        },
        {
            "filename": "The.Lord.of.the.Rings.The.Two.Towers.2002.1080p.BluRay.x264.YIFY.mp4",
            "expected": {"title": "The Lord of the Rings: The Two Towers", "year": 2002},
        },
        {
            "filename": "The.Lord.of.the.Rings.The.Return.of.the.King.2003.1080p.BluRay.x264.YIFY.mp4",
            "expected": {"title": "The Lord of the Rings: The Return of the King", "year": 2003},
        },
        {
            "filename": "The.Avengers.2012.1080p.BluRay.x264.YIFY.mp4",
            "expected": {"title": "The Avengers", "year": 2012},
        },
        {
            "filename": "Avengers.Endgame.2019.1080p.BluRay.x264.YIFY.mp4",
            "expected": {"title": "Avengers: Endgame", "year": 2019},
        },
        {
            "filename": "Avatar.2009.1080p.BluRay.x264.YIFY.mp4",
            "expected": {"title": "Avatar", "year": 2009},
        },
        {
            "filename": "Titanic.1997.1080p.BluRay.x264.YIFY.mp4",
            "expected": {"title": "Titanic", "year": 1997},
        },
        {
            "filename": "Joker.2019.1080p.BluRay.x264.YIFY.mp4",
            "expected": {"title": "Joker", "year": 2019},
        },
        {
            "filename": "Black.Mirror.S05E01.1080p.WEB.H264-MEMENTO.mkv",
            "expected": {"title": "Black Mirror", "season": 5, "episode": 1, "start_year": 2011, "end_year": None, "status": "Continuing", "total_episodes": 34, "total_seasons": 8},
        },
        {
            "filename": "Sherlock.S04E03.1080p.BluRay.x264-SHORTBREHD.mkv",
            "expected": {"title": "Sherlock", "season": 4, "episode": 3, "start_year": 2010, "end_year": 2017, "status": "Ended", "total_episodes": 12, "total_seasons": 4},
        },
        {
            "filename": "House.of.Cards.S06E08.1080p.NF.WEB-DL.DDP5.1.x264-NTb.mkv",
            "expected": {"title": "House of Cards", "season": 6, "episode": 8, "start_year": 2013, "end_year": 2018, "status": "Ended", "total_episodes": 73, "total_seasons": 6},
        },
        {
            "filename": "True.Detective.S03E08.1080p.AMZN.WEB-DL.DDP5.1.H.264-NTb.mkv",
            "expected": {"title": "True Detective", "season": 3, "episode": 8, "start_year": 2014, "end_year": None, "status": "Continuing", "total_episodes": 31, "total_seasons": 5},
        },
        {
            "filename": "Chernobyl.S01E05.1080p.BluRay.x264-ROVERS.mkv",
            "expected": {"title": "Chernobyl", "season": 1, "episode": 5, "start_year": 2019, "end_year": 2019, "status": "Ended", "total_episodes": 5, "total_seasons": 1},
        },
        {
            "filename": "The.Crown.S04E10.1080p.NF.WEB-DL.DDP5.1.Atmos.x264-NTb.mkv",
            "expected": {"title": "The Crown", "season": 4, "episode": 10, "start_year": 2016, "end_year": 2023, "status": "Ended", "total_episodes": 60, "total_seasons": 6},
        },
        {
            "filename": "Westworld.S03E08.1080p.AMZN.WEB-DL.DDP5.1.H.264-NTb.mkv",
            "expected": {"title": "Westworld", "season": 3, "episode": 8, "start_year": 2016, "end_year": 2022, "status": "Ended", "total_episodes": 36, "total_seasons": 4},
        },
        {
            "filename": "The.Queen's.Gambit.S01E07.1080p.NF.WEB-DL.DDP5.1.Atmos.x264-NTb.mkv",
            "expected": {"title": "The Queen's Gambit", "season": 1, "episode": 7, "start_year": 2020, "end_year": 2020, "status": "Ended", "total_episodes": 7, "total_seasons": 1},
        },
        {
            "filename": "Mindhunter.S02E09.1080p.NF.WEB-DL.DDP5.1.x264-NTb.mkv",
            "expected": {"title": "Mindhunter", "season": 2, "episode": 9, "start_year": 2017, "end_year": 2019, "status": "Ended", "total_episodes": 19, "total_seasons": 2},
        },
        {
            "filename": "Better.Call.Saul.S01E01.720p.HDTV.x264-KILLERS.mkv",
            "expected": {"title": "Better Call Saul", "season": 1, "episode": 1, "start_year": 2015, "end_year": 2022, "status": "Ended", "total_episodes": 63, "total_seasons": 6},
        },
        {
            "filename": "The.Witcher.S02E08.1080p.NF.WEB-DL.DDP5.1.Atmos.x264-NTb.mkv",
            "expected": {"title": "The Witcher", "season": 2, "episode": 8, "start_year": 2019, "end_year": None, "status": "Continuing", "total_episodes": 33, "total_seasons": 5},
        },
        {
            "filename": "Dexter.New.Blood.S01E10.1080p.AMZN.WEB-DL.DDP5.1.H.264-NTb.mkv",
            "expected": {"title": "Dexter: New Blood", "season": 1, "episode": 10, "start_year": 2021, "end_year": 2022, "status": "Ended", "total_episodes": 10, "total_seasons": 1},
        },
        {
            "filename": "The.Boys.S03E08.1080p.AMZN.WEB-DL.DDP5.1.H.264-NTb.mkv",
            "expected": {"title": "The Boys", "season": 3, "episode": 8, "start_year": 2019, "end_year": None, "status": "Continuing", "total_episodes": 40, "total_seasons": 5},
        },
        {
            "filename": "Squid.Game.S01E09.1080p.NF.WEB-DL.DDP5.1.Atmos.x264-NTb.mkv",
            "expected": {"title": "Squid Game", "season": 1, "episode": 9, "start_year": 2021, "end_year": 2025, "status": "Ended", "total_episodes": 22, "total_seasons": 3},
        },
        {
            "filename": "Money.Heist.S05E10.1080p.NF.WEB-DL.DDP5.1.Atmos.x264-NTb.mkv",
            "expected": {"title": "Money Heist", "season": 5, "episode": 10, "start_year": 2017, "end_year": 2021, "status": "Ended", "total_episodes": 41, "total_seasons": 5},
        },
        {
            "filename": "The.Handmaid's.Tale.S04E10.1080p.HULU.WEB-DL.DDP5.1.H.264-NTb.mkv",
            "expected": {"title": "The Handmaid's Tale", "season": 4, "episode": 10, "start_year": 2017, "end_year": 2025, "status": "Ended", "total_episodes": 66, "total_seasons": 6},
        },
        {
            "filename": "Ozark.S04E14.1080p.NF.WEB-DL.DDP5.1.Atmos.x264-NTb.mkv",
            "expected": {"title": "Ozark", "season": 4, "episode": 14, "start_year": 2017, "end_year": 2022, "status": "Ended", "total_episodes": 44, "total_seasons": 4},
        },
        {
            "filename": "Succession.S03E09.1080p.HMAX.WEB-DL.DDP5.1.H.264-NTb.mkv",
            "expected": {"title": "Succession", "season": 3, "episode": 9, "start_year": 2018, "end_year": 2023, "status": "Ended", "total_episodes": 39, "total_seasons": 4},
        },
        {
            "filename": "The.Expanse.S06E06.1080p.AMZN.WEB-DL.DDP5.1.H.264-NTb.mkv",
            "expected": {"title": "The Expanse", "season": 6, "episode": 6, "start_year": 2015, "end_year": 2022, "status": "Ended", "total_episodes": 62, "total_seasons": 6},
        },
        {
            "filename": "The.Walking.Dead.S11E24.1080p.AMZN.WEB-DL.DDP5.1.H.264-NTb.mkv",
            "expected": {"title": "The Walking Dead", "season": 11, "episode": 24, "start_year": 2010, "end_year": 2022, "status": "Ended", "total_episodes": 177, "total_seasons": 11},
        },
        {
            "filename": "House.S08E22.720p.BluRay.x264-REWARD.mkv",
            "expected": {"title": "House", "season": 8, "episode": 22},
        },
        {
            "filename": "Prison.Break.S05E09.720p.HDTV.x264-KILLERS.mkv",
            "expected": {"title": "Prison Break", "season": 5, "episode": 9, "start_year": 2005, "end_year": 2017, "status": "Ended", "total_episodes": 90, "total_seasons": 5},
        },
        {
            "filename": "Homeland.S08E12.1080p.AMZN.WEB-DL.DDP5.1.H.264-NTb.mkv",
            "expected": {"title": "Homeland", "season": 8, "episode": 12},
        },
        {
            "filename": "Narcos.S03E10.1080p.NF.WEB-DL.DDP5.1.x264-NTb.mkv",
            "expected": {"title": "Narcos", "season": 3, "episode": 10},
        },
        {
            "filename": "Peaky.Blinders.S06E06.1080p.BBC.WEB-DL.AAC2.0.H.264-NTb.mkv",
            "expected": {"title": "Peaky Blinders", "season": 6, "episode": 6, "start_year": 2013, "end_year": 2022, "status": "Ended", "total_episodes": 36, "total_seasons": 6},
        },
        {
            "filename": "SurrealEstate S03E08 1080p x265-ELiTE[EZTVx.to].mkv",
            "expected": {"title": "SurrealEstate", "season": 3, "episode": 8},
        }
    ]

    pass_count = 0
    fail_count = 0
    
    for entry in test_cases:
        filename = entry["filename"]
        expected = entry["expected"]
        guess = guessit_wrapper(filename)
        actual = guess

        # Determine if this is a TV episode or a movie
        is_episode = "season" in expected and "episode" in expected

        # Get title metadata (timed)
        title_result = None
        search_start = time.time()
        try:
            title_result = provider.find_title(guess.get("title"), year=guess.get("year", None))
        finally:
            search_time_ms = int((time.time() - search_start) * 1000)
            actual['search_time_ms'] = search_time_ms

        # Fill in actual with title metadata
        if title_result is not None and isinstance(title_result.info, TitleInfo):
            actual['title'] = title_result.info.title
            actual['start_year'] = title_result.info.start_year
            actual['end_year'] = title_result.info.end_year
            actual['votes'] = title_result.info.votes
            actual['genres'] = title_result.info.genres
            actual['tags'] = title_result.info.tags
            actual['status'] = title_result.info.status
            actual['total_episodes'] = title_result.info.total_episodes
            actual['total_seasons'] = title_result.info.total_seasons
            actual['sources'] = title_result.info.sources
            actual['plot'] = title_result.info.plot

        # Query IMDb metadata appropriately
        if is_episode and provider is not None:
            if title_result is not None and hasattr(provider, "get_episode_info"):
                guess_episode_num = guess.get("episode", None)
                # Handle cases where episode is a list (e.g., multi-episode files), only take first episode in list
                ep_nums = guess_episode_num if isinstance(guess_episode_num, list) else [guess_episode_num]
                ep_num = ep_nums[0] if ep_nums else None
                # Time episode lookup separately
                ep_result = None
                ep_start = time.time()
                try:
                    ep_result = provider.get_episode_info(title_result.info.id, season=guess.get("season", None), episode=ep_num)
                finally:
                    actual['episode_search_time_ms'] = int((time.time() - ep_start) * 1000)

                # Fill in actuals based on the IMDB metadata
                actual['episode_title'] = ep_result.title if ep_result else None
                actual['episode_air_date'] = ep_result.air_date if ep_result else None
                actual['episode_plot'] = ep_result.plot if ep_result else None
                actual['episode_rating'] = ep_result.rating if ep_result else None
                actual['episode_votes'] = ep_result.votes if ep_result else None
                actual['episode_year'] = ep_result.year if ep_result else None

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
        
        # Update counts
        if all_match:
            pass_count += 1
        else:
            fail_count += 1
        
        # Display results immediately based on verbosity level
        if verbosity >= 3:
            # Level 3: Show full details for all tests
            print_test_full(filename, all_match, expected, actual, extra_keys)
        elif verbosity >= 2:
            # Level 2: Show failures + passes (one line per pass)
            if not all_match:
                print_test_failure(filename, mismatch_lines, extra_keys, actual)
            else:
                print_test_pass(filename, actual)
        elif verbosity >= 1:
            # Level 1: Show failures only
            if not all_match:
                print_test_failure(filename, mismatch_lines, extra_keys, actual)
    
    # Level 0+: Always show summary
    print(f"\n{'='*80}")
    print(color_text(f"SUMMARY: {pass_count} / {len(test_cases)} cases passed.", 'green' if fail_count == 0 else 'yellow'))
    if fail_count == 0:
        print(color_text("All test cases passed! ✓", 'green'))
    else:
        print(color_text(f"{fail_count} test case(s) failed.", 'red'))
    print(f"{'='*80}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Test IMDb metadata extraction',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Verbosity levels:
  (none)  - Only show summary
  -v      - Show summary + failures (default)
  -vv     - Show summary + failures + passes (one line per pass)
  -vvv    - Show full comparison for all tests (expected vs actual metadata)
        '''
    )
    parser.add_argument(
        '-v', '--verbose',
        action='count',
        default=1,
        help='Increase verbosity (use -v, -vv, -vvv, etc.)'
    )
    args = parser.parse_args()
    
    # Cap verbosity at level 3
    verbosity = min(args.verbose, 3)
    
    test_imdb_metadata(verbosity=verbosity)
