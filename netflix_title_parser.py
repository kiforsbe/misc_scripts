import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Iterable, Optional, Tuple


SEASON_TOKEN_RULES = (
    r"season\s+(?P<number>\d+)(?:[a-z])?",
    r"series\s+(?P<number>\d+)(?:[a-z])?",
    r"part\s+(?P<number>\d+)(?:[a-z])?",
)

SEASON_NUMBER_PATTERNS = tuple(
    re.compile(rf"^{rule}$", re.IGNORECASE) for rule in SEASON_TOKEN_RULES
)

LIMITED_SERIES_RE = re.compile(r"^limited\s+series$", re.IGNORECASE)
PART_ROMAN_SEASON_RE = re.compile(
    r"^part\s+(?P<roman>i|ii|iii|iv|v|vi|vii|viii|ix|x)$",
    re.IGNORECASE,
)
BOOK_NUMBER_RE = re.compile(r"^book\s+(?P<number>\d+)$", re.IGNORECASE)
TEXTUAL_PART_RE = re.compile(
    r"^part\s+(?:one|two|three|four|five|six|seven|eight|nine|ten|i|ii|iii|iv|v|vi|vii|viii|ix|x)$",
    re.IGNORECASE,
)
NUMERIC_OR_ROMAN_PART_RE = re.compile(
    r"^part\s+(?:\d+|i|ii|iii|iv|v|vi|vii|viii|ix|x)$",
    re.IGNORECASE,
)
TEXTUAL_CHAPTER_RE = re.compile(r"^chapter\s+[^:]+$", re.IGNORECASE)
TEXTUAL_CASE_RE = re.compile(r"^case\s+[^:]+$", re.IGNORECASE)
TEXTUAL_ISSUE_RE = re.compile(r"^issue\s*#?\s*[^:]+$", re.IGNORECASE)
SCRIPTURE_REFERENCE_HEAD_RE = re.compile(
    r"^(?:[1-3]\s+)?[a-z]+(?:\s+[a-z]+)*\s+\d+$",
    re.IGNORECASE,
)
SCRIPTURE_REFERENCE_TAIL_RE = re.compile(r"^\d+(?:[-–]\d+)?$", re.IGNORECASE)

EPISODE_TOKEN_RULES = (
    r"episode\s+(?P<number>\d+)",
    r"chapter\s+(?P<number>\d+)",
    r"(?P<number>\d+)(?:st|nd|rd|th)\s+[^:]+",
)

EPISODE_NUMBER_PATTERNS = tuple(
    re.compile(rf"^{rule}$", re.IGNORECASE) for rule in EPISODE_TOKEN_RULES
)

SEASON_SPLIT_TOKEN_REGEX = r"(?:season\s+\d+(?:[a-z])?|series\s+\d+(?:[a-z])?|part\s+(?:\d+(?:[a-z])?|i|ii|iii|iv|v|vi|vii|viii|ix|x)|limited\s+series)"
EPISODE_SPLIT_TOKEN_REGEX = r"(?:episode\s+\d+|chapter\s+\d+|\d+(?:st|nd|rd|th)\s+[^:]+)"
EPISODE_SUFFIX_REGEX = rf"(?P<episode_token>{EPISODE_SPLIT_TOKEN_REGEX})(?:\s*:\s*(?P<episode_title>.+))?"
SEASON_SUBTITLE_REGEX = rf"(?:\s*:\s*(?P<season_subtitle>(?!\s*(?:{EPISODE_SPLIT_TOKEN_REGEX})(?:\s*:|$))[^:]+)(?=\s*:))?"

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
        kind="implicit_vs_part_episode",
        pattern=re.compile(
            r"^(?P<title>[^:]+)\s*:\s*(?P<episode_title>[^:]*vs\.[^:]*:\s*part\s+\d+)$",
            re.IGNORECASE,
        ),
    ),
    TitleSplitRule(
        kind="implicit_pilot_episode",
        pattern=re.compile(
            r"^(?P<title>[^:]+)\s*:\s*(?P<episode_title>pilot\s*:\s*.+)$",
            re.IGNORECASE,
        ),
    ),
    TitleSplitRule(
        kind="implicit_dash_subtitle_episode",
        pattern=re.compile(
            r"^(?P<title>[^:]+\s-\s[^:]+)\s*:\s*(?P<episode_title>[^:]+:\s*.+)$",
            re.IGNORECASE,
        ),
    ),
    TitleSplitRule(
        kind="named_season",
        pattern=re.compile(
            r"^(?P<title>.+?)\s*:\s*(?P<season_token>season\s+\d+(?:[a-z])?)\s+[^:]+\s*:\s*(?P<remainder>.+)$",
            re.IGNORECASE,
        ),
    ),
    TitleSplitRule(
        kind="season",
        pattern=re.compile(
            rf"^(?P<title>.+?)\s*:\s*(?P<season_token>{SEASON_SPLIT_TOKEN_REGEX}){SEASON_SUBTITLE_REGEX}(?:\s*:\s*(?P<remainder>.+))?$",
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
        kind="episode_title_token",
        pattern=re.compile(
            r"^(?P<title>.+?)\s*:\s*(?P<episode_title>time\.\d+)$",
            re.IGNORECASE,
        ),
    ),
    TitleSplitRule(
        kind="repeated_numbered_season",
        pattern=re.compile(
            r"^(?P<title>.+?)\s*:\s*(?P<season_title>(?P=title)\s+\d+)\s*:\s*(?P<episode_title>chapter\s+[^:]+:\s*.+)$",
            re.IGNORECASE,
        ),
    ),
    TitleSplitRule(
        kind="book_season",
        pattern=re.compile(
            r"^(?P<title>[^:]+)\s*:\s*(?P<season_title>book\s+\d+)\s*:\s*(?P<episode_title>.+)$",
            re.IGNORECASE,
        ),
    ),
    TitleSplitRule(
        kind="named_book_season",
        pattern=re.compile(
            r"^(?P<title>.+?:.+?)\s*:\s*(?P<season_title>book\s+\d+)\s*:\s*(?P<episode_title>.+)$",
            re.IGNORECASE,
        ),
    ),
    TitleSplitRule(
        kind="chapter_title_episode",
        pattern=re.compile(
            r"^(?P<title>.+?)\s*:\s*(?P<episode_title>chapter\s+[^:]+:\s*.+)$",
            re.IGNORECASE,
        ),
    ),
    TitleSplitRule(
        kind="named_arc_season",
        pattern=re.compile(
            r"^(?P<title>.+)\s*:\s*(?P<season_title>[^:]*\barc)\s*:\s*(?P<episode_title>.+)$",
            re.IGNORECASE,
        ),
    ),
    TitleSplitRule(
        kind="implicit_nested_episode",
        pattern=re.compile(
            r"^(?P<title>.+?:.+?)\s*:\s*(?P<episode_title>[^:]+)$",
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
    roman_match = PART_ROMAN_SEASON_RE.match(cleaned)
    if roman_match:
        roman_value = roman_match.group("roman").upper()
        roman_numbers = {
            "I": 1,
            "II": 2,
            "III": 3,
            "IV": 4,
            "V": 5,
            "VI": 6,
            "VII": 7,
            "VIII": 8,
            "IX": 9,
            "X": 10,
        }
        return roman_numbers.get(roman_value)
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
        if rule.kind in {"season", "named_season"}:
            season_token = _clean_token(match.group("season_token") or "")
            season_subtitle = ""
            if rule.kind == "season":
                season_subtitle = _clean_token(match.group("season_subtitle") or "")
            season_title = season_token
            if season_subtitle:
                season_title = _clean_token(f"{season_token}: {season_subtitle}")
            season_title = season_title or None
            season_number = _parse_season_number(season_token)
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

                if season_subtitle and episode_title and (
                    TEXTUAL_CHAPTER_RE.match(season_subtitle)
                    or TEXTUAL_CASE_RE.match(season_subtitle)
                    or TEXTUAL_ISSUE_RE.match(season_subtitle)
                ):
                    season_title = season_token or None
                    episode_title = _clean_token(f"{season_subtitle}: {episode_title}")

                if season_subtitle and episode_title and (
                    SCRIPTURE_REFERENCE_HEAD_RE.match(season_subtitle)
                    and SCRIPTURE_REFERENCE_TAIL_RE.match(episode_title)
                ):
                    season_title = season_token or None
                    episode_title = _clean_token(f"{season_subtitle}:{episode_title}")

                if season_subtitle and episode_title and (
                    TEXTUAL_PART_RE.match(episode_title)
                    or NUMERIC_OR_ROMAN_PART_RE.match(episode_title)
                ):
                    if (
                        NUMERIC_OR_ROMAN_PART_RE.match(episode_title)
                        or _normalize_lookup_text(series_title) in _normalize_lookup_text(season_subtitle)
                    ):
                        season_title = season_token or None
                    episode_title = _clean_token(f"{season_subtitle}: {episode_title}")

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

        if rule.kind in {
            "implicit_vs_part_episode",
            "implicit_pilot_episode",
            "implicit_dash_subtitle_episode",
        }:
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

        if rule.kind == "episode_title_token":
            episode_title = _clean_token(match.group("episode_title") or "") or None
            return ParsedNetflixTitle(
                raw_title=raw_title,
                title=series_title,
                media_kind="series",
                season=None,
                episode=None,
                episode_title=episode_title,
                is_explicit_series=True,
            )

        if rule.kind == "repeated_numbered_season":
            season_title = _clean_token(match.group("season_title") or "") or None
            episode_title = _clean_token(match.group("episode_title") or "") or None
            return ParsedNetflixTitle(
                raw_title=raw_title,
                title=series_title,
                media_kind="series",
                season=None,
                season_title=season_title,
                episode=None,
                episode_title=episode_title,
                is_explicit_series=True,
            )

        if rule.kind == "book_season":
            season_title = _clean_token(match.group("season_title") or "") or None
            book_match = BOOK_NUMBER_RE.match(season_title or "")
            season_number = int(book_match.group("number")) if book_match else None
            episode_title = _clean_token(match.group("episode_title") or "") or None
            return ParsedNetflixTitle(
                raw_title=raw_title,
                title=series_title,
                media_kind="series",
                season=season_number,
                season_title=season_title,
                episode=None,
                episode_title=episode_title,
                is_explicit_series=True,
            )

        if rule.kind == "named_book_season":
            season_title = _clean_token(match.group("season_title") or "") or None
            book_match = BOOK_NUMBER_RE.match(season_title or "")
            season_number = int(book_match.group("number")) if book_match else None
            episode_title = _clean_token(match.group("episode_title") or "") or None
            return ParsedNetflixTitle(
                raw_title=raw_title,
                title=series_title,
                media_kind="series",
                season=season_number,
                season_title=season_title,
                episode=None,
                episode_title=episode_title,
                is_explicit_series=True,
            )

        if rule.kind == "chapter_title_episode":
            episode_title = _clean_token(match.group("episode_title") or "") or None
            return ParsedNetflixTitle(
                raw_title=raw_title,
                title=series_title,
                media_kind="series",
                season=None,
                episode=None,
                episode_title=episode_title,
                is_explicit_series=True,
            )

        if rule.kind == "named_arc_season":
            season_title = _clean_token(match.group("season_title") or "") or None
            episode_title = _clean_token(match.group("episode_title") or "") or None
            return ParsedNetflixTitle(
                raw_title=raw_title,
                title=series_title,
                media_kind="series",
                season=None,
                season_title=season_title,
                episode=None,
                episode_title=episode_title,
                is_explicit_series=True,
            )

        if rule.kind == "implicit_vs_part_episode":
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

        if rule.kind == "implicit_nested_episode":
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