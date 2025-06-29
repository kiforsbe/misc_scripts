import re
import guessit

def guessit_wrapper(filename, options=None):
    # Generalize to match any release group in brackets
    group_prefix = r"^\[(?P<group>[^\]]+)\] "
    # Movie title (greedy parenthesis, applied first)
    movie_title_pattern = re.compile(r"^(\[[^\]]+\] )?(?P<title>.+? - Movie \d+ \(.+?\))(?= |\[|$)")
    # Patterns for various cases
    # SxxExx pattern (e.g., S03E06)
    sxxexx_pattern = re.compile(group_prefix + r"(?P<title>.+?) - S(?P<season>\d{2})E(?P<episode>\d{2})(?=\.| |\[|$)")
    # Season pattern (Sx Part y)
    season_pattern = re.compile(group_prefix + r"(?P<title>.+?) S(?P<season>\d+)(?: Part (?P<part>\d+))? - (?P<episode>\d+(?:\.\d+)?)(?:[vV]\d+)? ")
    # Triple-part title (only if third part contains at least one non-digit)
    triple_part_pattern = re.compile(group_prefix + r"(?P<part1>.+?) - (?P<part2>.+?) - (?P<part3>.+?)(?=\(|\[|\d|$)")
    # Title - part - episode (non-greedy title, only if part is not all digits)
    parts_episode_pattern = re.compile(group_prefix + r"(?P<title>.+?) - (?P<part>(?!\d+$).+?) - (?P<episode>\d+(?:\.\d+)?)(?= |\(|\[|\)|\]|$)")
    # Accept episode numbers with optional v-version (e.g. 02v2, 11.5v2)
    episode_pattern = re.compile(group_prefix + r"(?P<title>.+?) - (?P<episode>\d+(?:\.\d+)?(?:v\d+)?)(?= |\(|\[|\)|\]|$)")
    # Two-part title (only if second part contains at least one non-digit)
    two_part_pattern = re.compile(group_prefix + r"(?P<part1>.+?) - (?P<part2>.+?)(?=\(|\[|$)")

    # Movie title (apply first)
    m = movie_title_pattern.match(filename)
    if m:
        result = guessit.guessit(filename, options=options)
        result['title'] = m.group('title').strip()
        return result

    # SxxExx pattern (e.g., S03E06)
    m = sxxexx_pattern.match(filename)
    if m:
        result = guessit.guessit(filename, options=options)
        result['title'] = m.group('title').strip()
        result['season'] = int(m.group('season'))
        result['episode'] = int(m.group('episode'))
        result['episode_title'] = None
        result.pop('alternative_title', None)
        return result

    # S/season pattern
    m = season_pattern.match(filename)
    if m:
        result = guessit.guessit(filename, options=options)
        base_title = m.group('title').strip()
        result['title'] = base_title
        result['season'] = int(m.group('season'))
        result['episode'] = float(m.group('episode')) if '.' in m.group('episode') else int(m.group('episode'))
        result['episode_title'] = None
        result.pop('alternative_title', None)
        return result

    # Triple-part title (only if third part contains at least one non-digit)
    m = triple_part_pattern.match(filename)
    if m:
        # If part3 is all digits or '0', skip this pattern
        if not m.group('part3').strip().isdigit() and m.group('part3').strip() != '0':
            result = guessit.guessit(filename, options=options)
            result['title'] = f"{m.group('part1').strip()} - {m.group('part2').strip()} - {m.group('part3').strip()}"
            result['episode_title'] = None
            result['alternative_title'] = None
            return result

    # Title - part - episode (only if part is not all digits)
    m = parts_episode_pattern.match(filename)
    if m:
        result = guessit.guessit(filename, options=options)
        result['title'] = f"{m.group('title').strip()} - {m.group('part').strip()}"
        result['episode'] = float(m.group('episode')) if '.' in m.group('episode') else int(m.group('episode'))
        result['episode_title'] = None
        result.pop('alternative_title', None)
        return result

    # Accept episode numbers with optional v-version (e.g. 02v2, 11.5v2)
    m = episode_pattern.match(filename)
    if m:
        ep = m.group('episode')
        ep_num = re.match(r"(\d+(?:\.\d+)?)(?:v\d+)?", ep)
        if ep_num:
            result = guessit.guessit(filename, options=options)
            result['title'] = m.group('title').strip()
            result['episode'] = float(ep_num.group(1)) if '.' in ep_num.group(1) else int(ep_num.group(1))
            result['episode_title'] = None
            result.pop('alternative_title', None)
            return result

    # Two-part title (only if not matching episode pattern and second part is not all digits)
    m = two_part_pattern.match(filename)
    if m:
        # If part2 is all digits or '0', skip this pattern
        if not m.group('part2').strip().isdigit() and m.group('part2').strip() != '0':
            result = guessit.guessit(filename, options=options)
            result['title'] = f"{m.group('part1').strip()} - {m.group('part2').strip()}"
            result['episode_title'] = None
            result['alternative_title'] = None
            return result
    # fallback to normal guessit, but forcibly split trailing episode from title if present
    result = guessit.guessit(filename, options=options)
    # If title ends with ' - <number>' and episode matches, split it
    title = result.get('title', '')
    episode = result.get('episode')
    m = re.match(r'^(.*) - (\d+(?:\.\d+)?(?:v\d+)?)$', title)
    if m and episode is not None:
        ep_num = re.match(r"(\d+(?:\.\d+)?)(?:v\d+)?", m.group(2))
        if ep_num and (str(episode) == ep_num.group(1) or float(episode) == float(ep_num.group(1))):
            result['title'] = m.group(1)
    return result
