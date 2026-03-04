import re
import guessit

GROUP_PREFIX = r"^\[(?P<group>[^\]]+)\] "
EXT_PATTERN = r"(?:\.(?:mkv|mp4|avi|mov|wmv|flv|ts))?"

GROUP_PREFIX_RE = re.compile(GROUP_PREFIX)
TITLE_WITH_TRAILING_EPISODE_RE = re.compile(r'^(.*) - (\d+(?:\.\d+)?(?:v\d+)?)$')
EPISODE_NUMBER_RE = re.compile(r"(\d+(?:\.\d+)?)(?:v\d+)?")
MOVIE_EXT_RE = re.compile(r'\.(mkv|mp4|avi|mov|wmv|flv|ts)$')
EPISODE_TITLE_STOP_RE = re.compile(r"\s+\d+|(?=\s+(?:AMZN|WEB|BluRay|BD|HDTV|DDP|AAC|H\.?264|HEVC|x265|FLUX|-\w+|\[))")
VERSION_RE = re.compile(r"\b\d+(?:\.\d+)?[vV](\d+)\b")
LOCALE_TAG_RE = re.compile(r"\s*\((?:jp|jpn|eng|english|sub|subs|dub|multi|raw)\)\s*", re.IGNORECASE)

PATTERNS = (
    (
        re.compile(
            r"^(\[[^\]]+\] )?(?P<title>(?:.+? - Movie \d+ \([^)]+\))|(?:.+? - The Movie(?: - [^\[\]]+)?))"
            r"(?: ?\[.*?\])*"
            r"(?:\.(?:mkv|mp4|avi|mov|wmv|flv|ts))?$"
        ),
        lambda m: {
            "title": MOVIE_EXT_RE.sub('', m.group('title').strip()),
            "type": "movie",
        }
    ),
    (
        re.compile(
            GROUP_PREFIX + r"(?P<title>.+?) S(?P<season>\d{2}) (?P<episode_title>(NCOP|NCED|Preview|Promo)) - (?P<episode>\d+)" + EXT_PATTERN + r"(?: ?\[.*\])?$"
        ),
        lambda m: {
            "title": m.group('title').strip(),
            "season": int(m.group('season')),
            "episode": int(m.group('episode')),
            "episode_title": m.group('episode_title').strip(),
            "type": "extra",
        }
    ),
    (
        re.compile(
            GROUP_PREFIX + r"(?P<title>.+?) - (?P<episode>\d+) - (?P<episode_title>[^\[\]]+?)(?: ?\[.*\])?" + EXT_PATTERN + r"$"
        ),
        lambda m: {
            "title": m.group('title').strip(),
            "episode": int(m.group('episode')),
            "episode_title": m.group('episode_title').strip(),
        }
    ),
    (
        re.compile(GROUP_PREFIX + r"(?P<title>.+?) - (?P<season>\d+)x(?P<episode>\d+)\s*-\s*(?P<episode_title>[^\[\(\]]+)" + EXT_PATTERN),
        lambda m: {
            "title": m.group('title').strip(),
            "season": int(m.group('season')),
            "episode": int(m.group('episode')),
            "episode_title": m.group('episode_title').strip(),
        }
    ),
    (
        re.compile(GROUP_PREFIX + r"(?P<title>.+?) S(?P<season>\d+)(?: Part (?P<part>\d+))? - (?P<episode>\d+(?:\.\d+)?)(?:[vV]\d+)? " + EXT_PATTERN),
        lambda m: {
            "title": m.group('title').strip(),
            "season": int(m.group('season')),
            "episode": float(m.group('episode')) if '.' in m.group('episode') else int(m.group('episode')),
            "episode_title": None,
        }
    ),
    (
        re.compile(GROUP_PREFIX + r"(?P<title>.+?) - S(?P<season>\d{2})E(?P<episode>\d{2})(?: - (?P<episode_title>[^\[\(]+))?(?=\.| |\[|$)" + EXT_PATTERN),
        lambda m: {
            "title": m.group('title').strip(),
            "season": int(m.group('season')),
            "episode": int(m.group('episode')),
            "episode_title": MOVIE_EXT_RE.sub('', m.group('episode_title').strip(" .-")) if m.group('episode_title') else None,
        }
    ),
    (
        re.compile(GROUP_PREFIX + r"(?P<title>.+?) - (?P<part>(?!\d+$).+?) - (?P<episode>\d+(?:\.\d+)?)(?= |\(|\[|\)|\]|$)" + EXT_PATTERN),
        lambda m: {
            "title": f"{m.group('title').strip()} - {m.group('part').strip()}",
            "episode": float(m.group('episode')) if '.' in m.group('episode') else int(m.group('episode')),
            "episode_title": None,
        }
    ),
    (
        re.compile(GROUP_PREFIX + r"(?P<title>.+?) - (?P<episode>\d+(?:\.\d+)?)(?:v\d+)?" + EXT_PATTERN + r"$"),
        lambda m: {
            "title": re.sub(r'\s*-\s*\d+(?:\.\d+)?$', '', m.group('title').strip()),
            "episode": float(m.group('episode')) if '.' in m.group('episode') else int(m.group('episode')),
            "episode_title": None,
        }
    ),
    (
        re.compile(GROUP_PREFIX + r"(?P<title>.+?) - (?P<episode>\d+(?:\.\d+)?)(?:v\d+)?(?= |\(|\[|\)|\]|$)" + EXT_PATTERN),
        lambda m: {
            "title": m.group('title').strip(),
            "episode": float(m.group('episode')) if '.' in m.group('episode') else int(m.group('episode')),
            "episode_title": None,
        }
    ),
    (
        re.compile(GROUP_PREFIX + r"(?P<title>.+?) (?P<episode>\d+)(?= \[)" + EXT_PATTERN),
        lambda m: {
            "title": m.group('title').strip(),
            "episode": int(m.group('episode')),
        }
    ),
    (
        re.compile(GROUP_PREFIX + r"(?P<part1>.+?) - (?P<part2>.+?) - (?P<part3>.+?)(?=\(|\[|\d|$)" + EXT_PATTERN),
        lambda m: {
            "title": f"{m.group('part1').strip()} - {m.group('part2').strip()} - {m.group('part3').strip()}" if not m.group('part3').strip().isdigit() and m.group('part3').strip() != '0' else None,
            "episode_title": None,
            "alternative_title": None,
            "type": "movie",
        }
    ),
    (
        re.compile(GROUP_PREFIX + r"(?P<part1>.+?) - (?P<part2>.+?)(?=\(|\[|$)" + EXT_PATTERN),
        lambda m: {
            "title": f"{m.group('part1').strip()} - {m.group('part2').strip()}" if not m.group('part2').strip().isdigit() and m.group('part2').strip() != '0' else None,
            "episode_title": None,
            "alternative_title": None,
            "type": "movie",
        }
    ),
    (
        re.compile(
            r"^(?P<title>.+?) S(?P<season>\d{2})E(?P<episode>\d{2})[-\. ]+(?P<episode_title>[^-\[\(\]0-9\.][^-\[\(\]]*)" + EXT_PATTERN
        ),
        lambda m: {
            "title": m.group('title').strip(),
            "season": int(m.group('season')),
            "episode": int(m.group('episode')),
            "episode_title": EPISODE_TITLE_STOP_RE.split(m.group('episode_title').strip(" .-"))[0].strip(" .-"),
        }
    ),
    (
        re.compile(GROUP_PREFIX + r"(?P<title>.+?) (?P<episode_title>NCOP|NCED|Preview|Promo)(?= \[|$)" + EXT_PATTERN),
        lambda m: {
            "title": m.group('title').strip(),
            "episode_title": m.group('episode_title').strip(),
            "type": "extra",
        }
    ),
    (
        re.compile(
            GROUP_PREFIX + r"(?P<title>.+?) \((?P<screen_size>\d{3,4}p)\)(?: ?\[.*?\])?" + EXT_PATTERN + r"$"
        ),
        lambda m: {
            "title": m.group('title').strip(),
            "type": "movie",
        }
    ),
)
def _infer_version(filename):
    match = VERSION_RE.search(filename)
    return int(match.group(1)) if match else None


def _build_fast_result(fields, release_group, filename):
    result = dict(fields)
    result.pop('alternative_title', None)

    if 'title' in result and result['title']:
        result['title'] = _normalize_title(result['title'])

    if release_group:
        result['release_group'] = release_group

    if 'version' not in result:
        version = _infer_version(filename)
        if version is not None:
            result['version'] = version

    if 'type' not in result and 'episode' in result:
        result['type'] = 'episode'

    return result


def _normalize_title(title):
    # Remove language/track tags that should not be part of canonical anime title lookup.
    cleaned = LOCALE_TAG_RE.sub(' ', title)
    return re.sub(r'\s+', ' ', cleaned).strip()

def guessit_wrapper(filename, options=None):
    # Extract release group from the beginning if present
    release_group = None
    group_match = GROUP_PREFIX_RE.match(filename)
    if group_match:
        release_group = group_match.group('group')

    # Try all patterns in order
    for pat, handler in PATTERNS:
        m = pat.match(filename)
        if m:
            fields = handler(m)
            if fields.get("title") is None:
                continue  # skip if triple/two-part pattern is not valid for this match
            if options is None:
                return _build_fast_result(fields, release_group, filename)
            result = guessit.guessit(filename, options=options)
            result.update(fields)
            result.pop('alternative_title', None)
            if 'title' in result and result['title']:
                result['title'] = _normalize_title(result['title'])
            # Preserve release_group from beginning of filename if captured
            if release_group:
                result['release_group'] = release_group
            return result

    # fallback to normal guessit, but forcibly split trailing episode from title if present
    result = guessit.guessit(filename, options=options)
    # If title ends with ' - <number>' and episode matches, split it
    title = result.get('title', '')
    episode = result.get('episode')
    m = TITLE_WITH_TRAILING_EPISODE_RE.match(title)
    if m and episode is not None:
        ep_num = EPISODE_NUMBER_RE.match(m.group(2))
        if ep_num and (str(episode) == ep_num.group(1) or float(episode) == float(ep_num.group(1))):
            result['title'] = m.group(1)

    if 'title' in result and result['title']:
        result['title'] = _normalize_title(result['title'])

    # Preserve release_group from beginning of filename if captured
    if release_group:
        result['release_group'] = release_group

    return result

