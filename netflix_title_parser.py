import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Iterable, Optional, Tuple


SEASON_NUMBER_PATTERNS = (
    re.compile(r"^season\s+(?P<number>\d+)$", re.IGNORECASE),
    re.compile(r"^series\s+(?P<number>\d+)$", re.IGNORECASE),
    re.compile(r"^part\s+(?P<number>\d+)$", re.IGNORECASE),
)

LIMITED_SERIES_RE = re.compile(r"^limited\s+series$", re.IGNORECASE)

EPISODE_NUMBER_PATTERNS = (
    re.compile(r"^episode\s+(?P<number>\d+)$", re.IGNORECASE),
    re.compile(r"^chapter\s+(?P<number>\d+)$", re.IGNORECASE),
    re.compile(r"^(?P<number>\d+)(?:st|nd|rd|th)\s+.+$", re.IGNORECASE),
)

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

    tokens = [_clean_token(token) for token in cleaned_title.split(":")]
    tokens = [token for token in tokens if token]

    for index in range(1, len(tokens)):
        season_number = _parse_season_number(tokens[index])
        if season_number is None:
            continue

        series_title = ": ".join(tokens[:index]).strip()
        trailing_tokens = tokens[index + 1 :]

        episode_number = None
        if trailing_tokens:
            episode_number = _parse_episode_number(trailing_tokens[0])
            if episode_number is not None:
                trailing_tokens = trailing_tokens[1:]

        episode_title = ": ".join(trailing_tokens).strip() or None
        return ParsedNetflixTitle(
            raw_title=raw_title,
            title=series_title or cleaned_title,
            media_kind="series",
            season=season_number,
            season_title=tokens[index],
            episode=episode_number,
            episode_title=episode_title,
            is_explicit_series=True,
        )

    for index in range(1, len(tokens)):
        episode_number = _parse_episode_number(tokens[index])
        if episode_number is None:
            continue

        series_title = ": ".join(tokens[:index]).strip()
        episode_title = ": ".join(tokens[index + 1 :]).strip() or None
        return ParsedNetflixTitle(
            raw_title=raw_title,
            title=series_title or cleaned_title,
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