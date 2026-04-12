import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Iterable, Optional, Tuple


SEASON_TOKEN_RULES = (
    r"season\s+(?P<number>\d+)",
    r"series\s+(?P<number>\d+)",
    r"part\s+(?P<number>\d+)",
)

SEASON_NUMBER_PATTERNS = tuple(
    re.compile(rf"^{rule}$", re.IGNORECASE) for rule in SEASON_TOKEN_RULES
)

LIMITED_SERIES_RE = re.compile(r"^limited\s+series$", re.IGNORECASE)

EPISODE_TOKEN_RULES = (
    r"episode\s+(?P<number>\d+)",
    r"chapter\s+(?P<number>\d+)",
    r"(?P<number>\d+)(?:st|nd|rd|th)\s+[^:]+",
)

EPISODE_NUMBER_PATTERNS = tuple(
    re.compile(rf"^{rule}$", re.IGNORECASE) for rule in EPISODE_TOKEN_RULES
)

SEASON_SPLIT_TOKEN_REGEX = r"(?:season\s+\d+|series\s+\d+|part\s+\d+|limited\s+series)"
EPISODE_SPLIT_TOKEN_REGEX = r"(?:episode\s+\d+|chapter\s+\d+|\d+(?:st|nd|rd|th)\s+[^:]+)"
EPISODE_SUFFIX_REGEX = rf"(?P<episode_token>{EPISODE_SPLIT_TOKEN_REGEX})(?:\s*:\s*(?P<episode_title>.+))?"

LOOKUP_TITLE_MATCH_THRESHOLD = 60.0

@dataclass(frozen=True)
class ParsedNetflixTitle:
    raw_title: str
    title: str
    media_kind: str
    season: Optional[int] = None
    season_title: Optional[str] = None
    episode: Optional[int] = None
    episode_title: Optional[str] = None
    is_explicit_series: bool = False
    has_implicit_split: bool = False


@dataclass(frozen=True)
class TitleSplitRule:
    kind: str
    pattern: re.Pattern


TITLE_SPLIT_RULES = (
    TitleSplitRule(
        kind="season",
        pattern=re.compile(
            rf"^(?P<title>.+?)\s*:\s*(?P<season_title>{SEASON_SPLIT_TOKEN_REGEX})(?:\s*:\s*(?P<remainder>.+))?$",
            re.IGNORECASE,
        ),
    ),
    TitleSplitRule(
        kind="episode",
        pattern=re.compile(
            rf"^(?P<title>.+?)\s*:\s*{EPISODE_SUFFIX_REGEX}$",
            re.IGNORECASE,
        ),
    ),
    TitleSplitRule(
        kind="implicit_episode",
        pattern=re.compile(
            r"^(?P<title>[^:]+?)\s*:\s*(?P<episode_title>[^:]+)$",
        ),
    ),
)

EPISODE_REMAINDER_PATTERN = re.compile(rf"^{EPISODE_SUFFIX_REGEX}$", re.IGNORECASE)


def _clean_token(token: str) -> str:
    return re.sub(r"\s+", " ", token.strip())


def _parse_season_number(token: str) -> Optional[int]:
    cleaned = _clean_token(token)
    for pattern in SEASON_NUMBER_PATTERNS:
        match = pattern.match(cleaned)
        if match:
            return int(match.group("number"))
    if LIMITED_SERIES_RE.match(cleaned):
        return 1
    return None


def _parse_episode_number(token: str) -> Optional[int]:
    cleaned = _clean_token(token)
    for pattern in EPISODE_NUMBER_PATTERNS:
        match = pattern.match(cleaned)
        if match:
            return int(match.group("number"))
    return None


def _normalize_lookup_text(text: Optional[str]) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^\w\s]", " ", _clean_token(text or "").casefold())).strip()


def _score_lookup_title_match(source_title: str, candidate_title: str) -> Optional[float]:
    normalized_source_title = _normalize_lookup_text(source_title)
    normalized_candidate_title = _normalize_lookup_text(candidate_title)
    if not normalized_source_title or not normalized_candidate_title:
        return None

    score = SequenceMatcher(None, normalized_source_title, normalized_candidate_title).ratio() * 100.0
    if normalized_source_title == normalized_candidate_title:
        score += 100.0
    elif normalized_candidate_title in normalized_source_title or normalized_source_title in normalized_candidate_title:
        score += 75.0

    source_tokens = set(normalized_source_title.split())
    candidate_tokens = set(normalized_candidate_title.split())
    if source_tokens and candidate_tokens:
        overlap = source_tokens & candidate_tokens
        if overlap:
            score += 25.0 * (len(overlap) / len(candidate_tokens))

    return score


def adapt_lookup_titles(title: Optional[str], known_titles: Optional[Iterable[str]] = None) -> Tuple[str, ...]:
    cleaned = _clean_token(title or "")
    if not cleaned:
        return ()

    candidates = [cleaned]
    scored_candidates = []
    for known_title in known_titles or ():
        cleaned_known_title = _clean_token(known_title)
        score = _score_lookup_title_match(cleaned, cleaned_known_title)
        if score is None or score < LOOKUP_TITLE_MATCH_THRESHOLD:
            continue

        scored_candidates.append((score, cleaned_known_title))

    for _, candidate in sorted(scored_candidates, key=lambda item: (-item[0], item[1].casefold())):
        candidates.append(candidate)

    return tuple(dict.fromkeys(candidate for candidate in candidates if candidate))


def parse_netflix_title(raw_title: str) -> ParsedNetflixTitle:
    cleaned_title = _clean_token(raw_title)
    if not cleaned_title:
        return ParsedNetflixTitle(raw_title=raw_title, title="", media_kind="movie")

    for rule in TITLE_SPLIT_RULES:
        match = rule.pattern.match(cleaned_title)
        if not match:
            continue

        series_title = _clean_token(match.group("title") or cleaned_title) or cleaned_title
        if rule.kind == "season":
            season_title = _clean_token(match.group("season_title") or "") or None
            season_number = _parse_season_number(season_title or "")
            remainder = _clean_token(match.group("remainder") or "")

            episode_number = None
            episode_title = None
            if remainder:
                episode_match = EPISODE_REMAINDER_PATTERN.match(remainder)
                if episode_match:
                    episode_number = _parse_episode_number(episode_match.group("episode_token"))
                    episode_title = _clean_token(episode_match.group("episode_title") or "") or None
                else:
                    episode_title = remainder

            return ParsedNetflixTitle(
                raw_title=raw_title,
                title=series_title,
                media_kind="series",
                season=season_number,
                season_title=season_title,
                episode=episode_number,
                episode_title=episode_title,
                is_explicit_series=True,
            )

        if rule.kind == "implicit_episode":
            episode_title = _clean_token(match.group("episode_title") or "") or None
            return ParsedNetflixTitle(
                raw_title=raw_title,
                title=series_title,
                media_kind="movie",
                season=None,
                episode=None,
                episode_title=episode_title,
                is_explicit_series=False,
                has_implicit_split=True,
            )

        episode_number = _parse_episode_number(match.group("episode_token"))
        episode_title = _clean_token(match.group("episode_title") or "") or None
        return ParsedNetflixTitle(
            raw_title=raw_title,
            title=series_title,
            media_kind="series",
            season=None,
            episode=episode_number,
            episode_title=episode_title,
            is_explicit_series=True,
        )

    return ParsedNetflixTitle(
        raw_title=raw_title,
        title=cleaned_title,
        media_kind="movie",
        is_explicit_series=False,
    )