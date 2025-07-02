import re
import guessit

def guessit_wrapper(filename, options=None):
    # Generalize to match any release group in brackets
    group_prefix = r"^\[(?P<group>[^\]]+)\] "
    # Movie title (greedy parenthesis, applied first)
    movie_title_pattern = re.compile(r"^(\[[^\]]+\] )?(?P<title>.+? - Movie \d+ \(.+?\))(?= |\[|$)")

    # Combined patterns, ordered from most specific to most general
    patterns = [
        # [Group] Title - 1x07 - EpisodeTitle [..]
        (
            re.compile(group_prefix + r"(?P<title>.+?) - (?P<season>\d+)x(?P<episode>\d+)\s*-\s*(?P<episode_title>[^\[\(\]]+)"),
            lambda m: {
                "title": m.group('title').strip(),
                "season": int(m.group('season')),
                "episode": int(m.group('episode')),
                "episode_title": m.group('episode_title').strip(),
            }
        ),
        # [Group] Title S2 Part 2 - 10 (720p) [..]
        (
            re.compile(group_prefix + r"(?P<title>.+?) S(?P<season>\d+)(?: Part (?P<part>\d+))? - (?P<episode>\d+(?:\.\d+)?)(?:[vV]\d+)? "),
            lambda m: {
                "title": m.group('title').strip(),
                "season": int(m.group('season')),
                "episode": float(m.group('episode')) if '.' in m.group('episode') else int(m.group('episode')),
                "episode_title": None,
            }
        ),
        # [Group] Title - S03E06 or [Group] Title - S01E13 - OVA (with optional episode_title)
        (
            re.compile(group_prefix + r"(?P<title>.+?) - S(?P<season>\d{2})E(?P<episode>\d{2})(?: - (?P<episode_title>[^\[\(]+))?(?=\.| |\[|$)"),
            lambda m: {
                "title": m.group('title').strip(),
                "season": int(m.group('season')),
                "episode": int(m.group('episode')),
                # Strip known extensions from episode_title if present
                "episode_title": re.sub(r'\.(mkv|mp4|avi|mov|wmv|flv|ts)$', '', m.group('episode_title').strip(" .-")) if m.group('episode_title') else None,
            }
        ),
        # [Group] Title - Part - Episode (where Part is not all digits)
        (
            re.compile(group_prefix + r"(?P<title>.+?) - (?P<part>(?!\d+$).+?) - (?P<episode>\d+(?:\.\d+)?)(?= |\(|\[|\)|\]|$)"),
            lambda m: {
                "title": f"{m.group('title').strip()} - {m.group('part').strip()}",
                "episode": float(m.group('episode')) if '.' in m.group('episode') else int(m.group('episode')),
                "episode_title": None,
            }
        ),
        # [Group] Title - 09.<ext> (season 1, episode N, trailing episode in title is stripped)
        (
            re.compile(group_prefix + r"(?P<title>.+?) - (?P<episode>\d+(?:\.\d+)?)(?:v\d+)?\.[a-zA-Z0-9]+$"),
            lambda m: {
                "title": re.sub(r'\s*-\s*\d+(?:\.\d+)?$', '', m.group('title').strip()),
                "season": 1,
                "episode": float(m.group('episode')) if '.' in m.group('episode') else int(m.group('episode')),
                "episode_title": None,
            }
        ),
        # [Group] Title - 09[...] (season 1, episode N, no trailing episode in title)
        (
            re.compile(group_prefix + r"(?P<title>.+?) - (?P<episode>\d+(?:\.\d+)?)(?:v\d+)?(?= |\(|\[|\)|\]|$)"),
            lambda m: {
                "title": m.group('title').strip(),
                "season": 1,
                "episode": float(m.group('episode')) if '.' in m.group('episode') else int(m.group('episode')),
                "episode_title": None,
            }
        ),
        # [Group] Title <epnum> [ ... ] (season 1, episode N, trailing episode in title is stripped)
        (
            re.compile(group_prefix + r"(?P<title>.+?) (?P<episode>\d+)(?= \[)"),
            lambda m: {
                "title": m.group('title').strip(),
                "season": 1,
                "episode": int(m.group('episode')),
            }
        ),
        # [Group] Title - Part2 - Part3 (triple part, only if part3 is not all digits)
        (
            re.compile(group_prefix + r"(?P<part1>.+?) - (?P<part2>.+?) - (?P<part3>.+?)(?=\(|\[|\d|$)"),
            lambda m: {
                "title": f"{m.group('part1').strip()} - {m.group('part2').strip()} - {m.group('part3').strip()}" if not m.group('part3').strip().isdigit() and m.group('part3').strip() != '0' else None,
                "episode_title": None,
                "alternative_title": None,
            }
        ),
        # [Group] Title - Part2 (two part, only if part2 is not all digits)
        (
            re.compile(group_prefix + r"(?P<part1>.+?) - (?P<part2>.+?)(?=\(|\[|$)"),
            lambda m: {
                "title": f"{m.group('part1').strip()} - {m.group('part2').strip()}" if not m.group('part2').strip().isdigit() and m.group('part2').strip() != '0' else None,
                "episode_title": None,
                "alternative_title": None,
            }
        ),
        # SxxEyy[-. ]episode_title (no group, allow punctuation in episode_title, stop at scene/tech info)
        (
            re.compile(
                r"^(?P<title>.+?) S(?P<season>\d{2})E(?P<episode>\d{2})[-\. ]+(?P<episode_title>[^-\[\(\]0-9\.][^-\[\(\]]*)"
            ),
            lambda m: {
                "title": m.group('title').strip(),
                "season": int(m.group('season')),
                "episode": int(m.group('episode')),
                "episode_title": re.split(r"\s+\d+|(?=\s+(?:AMZN|WEB|BluRay|BD|HDTV|DDP|AAC|H\.?264|HEVC|x265|FLUX|-\w+|\[))", m.group('episode_title').strip(" .-"))[0].strip(" .-"),
            }
        ),
        # [Group] Title - NCOP/NCED/Preview (specials, e.g. [Group] Title Preview [..])
        (
            re.compile(group_prefix + r"(?P<title>.+?) (?P<episode_title>NCOP|NCED|Preview|Movie|Promo)(?= \[|$)"),
            lambda m: {
                "title": m.group('title').strip(),
                "episode_title": m.group('episode_title').strip(),
            }
        ),
    ]

    # Movie title (apply first)
    m = movie_title_pattern.match(filename)
    if m:
        result = guessit.guessit(filename, options=options)
        result['title'] = m.group('title').strip()
        return result

    # Try all patterns in order
    for pat, handler in patterns:
        m = pat.match(filename)
        if m:
            fields = handler(m)
            if fields.get("title") is None:
                continue  # skip if triple/two-part pattern is not valid for this match
            result = guessit.guessit(filename, options=options)
            result.update(fields)
            result.pop('alternative_title', None)
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

