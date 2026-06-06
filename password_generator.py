"""Generate strong, easy-to-remember passwords.

Features:
- Configurable length (default 8)
- Caps mode: no, only, mixed
- Optional digits and symbols
- Optionally exclude visually ambiguous characters (0/O, 1/l/I, etc.)
- Enforce at least 2 characters from each enabled subset
"""
from __future__ import annotations

import argparse
import secrets
import string
import sys
from typing import List, Sequence
from pathlib import Path
import urllib.request
import urllib.error


AMBIGUOUS = set("0OolI1")

# Curated symbol set commonly accepted by sites
SYMBOLS = list("!@#$%&*()-_+=<>?.")

# Small built-in wordlist for quick diceware usage
BUILTIN_WORDLIST = [
    "apple",
    "banana",
    "cherry",
    "delta",
    "echo",
    "foxtrot",
    "golf",
    "hotel",
    "india",
    "juliet",
    "kangaroo",
    "lemon",
    "mango",
    "nectar",
    "orange",
]


def _filter_ambiguous(chars: Sequence[str], exclude_ambiguous: bool) -> List[str]:
    if not exclude_ambiguous:
        return list(chars)
    return [c for c in chars if c not in AMBIGUOUS]


def _enforce_required_subsets_on_chars(password_chars: List[str], required_subsets: List[List[str]]) -> None:
    """Replace random positions in password_chars with characters from each required subset.

    Modifies the list in-place.
    """
    if not required_subsets:
        return
    length = len(password_chars)
    indices = list(range(length))
    rand = secrets.SystemRandom()
    rand.shuffle(indices)
    pos = 0
    MIN_PER_SUBSET = 2
    for subset in required_subsets:
        if not subset:
            raise ValueError("A required character subset is empty after ambiguous filtering")
        for _ in range(MIN_PER_SUBSET):
            if pos < len(indices):
                idx = indices[pos]
                password_chars[idx] = secrets.choice(subset)
                pos += 1
            else:
                # No free positions left; append instead
                password_chars.append(secrets.choice(subset))


def get_eff_wordlist_path(wordlists_dir: Path | None = None, download_if_missing: bool = True) -> Path:
    """Return local path to the EFF large wordlist, downloading it into `wordlists/` if missing.

    Raises OSError or URLError when download fails.
    """
    if wordlists_dir is None:
        wordlists_dir = Path(__file__).parent / "wordlists"
    wordlists_dir.mkdir(parents=True, exist_ok=True)
    eff_path = wordlists_dir / "eff_large_wordlist.txt"
    if not eff_path.exists():
        if not download_if_missing:
            raise FileNotFoundError(f"EFF wordlist not found at: {eff_path}")
        EFF_URL = "https://www.eff.org/files/2016/07/18/eff_large_wordlist.txt"
        with urllib.request.urlopen(EFF_URL, timeout=30) as resp:
            data = resp.read()
        eff_path.write_bytes(data)
    return eff_path



def generate_password(
    length: int = 8,
    caps: str = "mixed",
    include_digits: bool = True,
    include_symbols: bool = False,
    exclude_ambiguous: bool = True,
    mode: str = "random",
    dice_words: int = 4,
    wordlist_path: str | None = None,
    pronounceable_syllables: int = 0,
) -> str:
    """Generate a password meeting the requested composition rules.

    caps: one of 'no', 'only', 'mixed'
    The function requires at least 2 characters from each enabled subset:
      - lowercase (when caps != 'only')
      - uppercase (when caps != 'no')
      - digits (when include_digits)
      - symbols (when include_symbols)
    """
    if length < 1:
        raise ValueError("length must be >= 1")

    caps = caps.lower()
    if caps not in {"no", "only", "mixed"}:
        raise ValueError("caps must be one of: no, only, mixed")

    lower_chars = _filter_ambiguous(string.ascii_lowercase, exclude_ambiguous)
    upper_chars = _filter_ambiguous(string.ascii_uppercase, exclude_ambiguous)
    digit_chars = _filter_ambiguous(string.digits, exclude_ambiguous)
    symbol_chars = _filter_ambiguous(SYMBOLS, exclude_ambiguous)

    required_subsets: List[List[str]] = []

    # Determine which subsets are allowed and which are required (2 chars each)
    allow_lower = caps != "only"
    allow_upper = caps != "no"
    if allow_lower:
        required_subsets.append(lower_chars)
    if allow_upper:
        required_subsets.append(upper_chars)
    if include_digits:
        required_subsets.append(digit_chars)
    if include_symbols:
        required_subsets.append(symbol_chars)

    mode = (mode or "random").lower()
    if mode not in {"random", "pronounceable", "diceware"}:
        raise ValueError("mode must be one of: random, pronounceable, diceware")

    # Each enabled subset must contribute at least 2 characters
    MIN_PER_SUBSET = 2
    total_required = MIN_PER_SUBSET * len(required_subsets)
    if mode != "diceware" and length < total_required:
        raise ValueError(
            f"length too small for composition requirements: need at least {total_required} chars"
        )

    # Build pool of allowed chars for filling the rest
    pool: List[str] = []
    if allow_lower:
        pool.extend(lower_chars)
    if allow_upper:
        pool.extend(upper_chars)
    if include_digits:
        pool.extend(digit_chars)
    if include_symbols:
        pool.extend(symbol_chars)

    if not pool:
        raise ValueError("No character sets enabled")
    # Mode-specific generation
    if mode == "random":
        password_chars: List[str] = []
        # Pick required characters
        for subset in required_subsets:
            if not subset:
                raise ValueError("A required character subset is empty after ambiguous filtering")
            for _ in range(MIN_PER_SUBSET):
                password_chars.append(secrets.choice(subset))

        remaining = length - len(password_chars)
        for _ in range(remaining):
            password_chars.append(secrets.choice(pool))

        # Securely shuffle using SystemRandom
        rand = secrets.SystemRandom()
        rand.shuffle(password_chars)
        return "".join(password_chars)

    if mode == "pronounceable":
        # Syllable-based pronounceable generator. If pronounceable_syllables>0,
        # build that many syllables; otherwise fall back to CV alternation up to length.
        vowels = _filter_ambiguous("aeiou", exclude_ambiguous)
        consonants = [c for c in _filter_ambiguous("bcdfghjklmnpqrstvwxyz", exclude_ambiguous)]
        if not vowels or not consonants:
            raise ValueError("Not enough letters left after ambiguous filtering for pronounceable mode")

        def make_syllable() -> str:
            patterns = ["CV", "CVC", "VC", "CVV"]
            pattern = secrets.choice(patterns)
            s = []
            for ch in pattern:
                if ch == "C":
                    s.append(secrets.choice(consonants))
                else:
                    s.append(secrets.choice(vowels))
            return "".join(s)

        if pronounceable_syllables and pronounceable_syllables > 0:
            parts: List[str] = [make_syllable() for _ in range(pronounceable_syllables)]
            base_str = "".join(parts)
        else:
            # Alternate consonant/vowel until length reached
            out: List[str] = []
            use_consonant = secrets.choice([True, False])
            while len("".join(out)) < length:
                if use_consonant:
                    out.append(secrets.choice(consonants))
                else:
                    out.append(secrets.choice(vowels))
                use_consonant = not use_consonant
            base_str = "".join(out)

        # Trim or pad to desired length
        if len(base_str) > length:
            base = list(base_str[:length])
        else:
            base = list(base_str)
            # pad with lowercase letters if needed
            while len(base) < length:
                base.append(secrets.choice(lower_chars))

        # Enforce required subsets by replacing random positions
        _enforce_required_subsets_on_chars(base, required_subsets)

        rand = secrets.SystemRandom()
        rand.shuffle(base)
        return "".join(base)

    # diceware
    if mode == "diceware":
        # Support a special shortcut 'eff' to use the recommended EFF wordlist.
        if not wordlist_path:
            raise ValueError("diceware mode requires --wordlist <path> (or 'eff') to a newline-separated wordlist")

        # Support a small builtin list for quick use
        if wordlist_path == "builtin":
            words = BUILTIN_WORDLIST
        else:
            if wordlist_path == "eff":
                try:
                    eff_path = get_eff_wordlist_path()
                except Exception as exc:  # propagate as ValueError for caller
                    raise ValueError(f"Unable to fetch EFF wordlist: {exc}") from exc
                wordlist_path = str(eff_path)

            try:
                with open(wordlist_path, "r", encoding="utf-8") as fh:
                    words: List[str] = []
                    for line in fh:
                        # EFF wordlist lines are formatted as: "11111\tword" (dice-roll code + tab/space + word)
                        # Accept either the builtin/simple wordlist format (one word per line)
                        # or the EFF format; split on whitespace and take the last token as the word.
                        raw = line.strip()
                        if not raw or raw.startswith("#"):
                            continue
                        parts = raw.split()
                        if not parts:
                            continue
                        w = parts[-1]
                        words.append(w)
            except OSError as exc:
                raise ValueError(f"Unable to read wordlist: {exc}") from exc
        if not words:
            raise ValueError("Wordlist is empty")

        chosen = [secrets.choice(words) for _ in range(max(1, dice_words))]
        base_str = "-".join(chosen)
        chars = list(base_str)

        # Enforce required subsets by appending required characters if necessary
        # (diceware focuses on word memorability; appending keeps words intact)
        extra_chars: List[str] = []
        for subset in required_subsets:
            if not subset:
                raise ValueError("A required character subset is empty after ambiguous filtering")
            for _ in range(MIN_PER_SUBSET):
                extra_chars.append(secrets.choice(subset))

        if extra_chars:
            # insert extra chars at random positions
            for ch in extra_chars:
                insert_at = secrets.randbelow(len(chars) + 1)
                chars.insert(insert_at, ch)

        return "".join(chars)

    # Fallback (should not reach)
    raise RuntimeError("unsupported mode")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate strong, easy-to-remember passwords")
    parser.add_argument("-l", "--length", type=int, default=8, help="Password length (default: 8)")
    parser.add_argument(
        "--caps",
        choices=["no", "only", "mixed"],
        default="mixed",
        help="Caps mode: no (lowercase only), only (uppercase only), mixed (both)",
    )
    parser.add_argument(
        "--mode",
        choices=["random", "pronounceable", "diceware"],
        default="random",
        help="Generation mode: random (default), pronounceable, or diceware",
    )
    parser.add_argument(
        "--pronounceable-syllables",
        type=int,
        default=0,
        help="Number of syllables to use in pronounceable mode (default: 0 - auto by length)",
    )
    parser.add_argument("--no-digits", action="store_true", help="Disable digits in the password")
    parser.add_argument("--symbols", action="store_true", help="Enable symbols in the password")
    parser.add_argument(
        "--allow-ambiguous",
        action="store_true",
        help="Allow ambiguous characters like 0/O and 1/l (disabled by default)",
    )
    parser.add_argument(
        "--wordlist",
        help="Path to newline-separated wordlist for diceware mode (or 'eff' or 'builtin')",
    )
    parser.add_argument(
        "--install-wordlist",
        action="store_true",
        help="Download and install the recommended EFF diceware wordlist to ./wordlists/eff_large_wordlist.txt",
    )
    parser.add_argument(
        "--dice-words",
        type=int,
        default=4,
        help="Number of words to use in diceware mode (default: 4)",
    )
    parser.add_argument("-n", "--count", type=int, default=1, help="Number of passwords to generate")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    # Handle install-wordlist preflight
    if args.install_wordlist:
        try:
            eff_path = get_eff_wordlist_path()
            print(f"Installed EFF wordlist to: {eff_path}")
        except Exception as exc:
            print(f"Failed to download/install EFF wordlist: {exc}", file=sys.stderr)
            sys.exit(2)
        return
    try:
        for _ in range(max(1, args.count)):
            pwd = generate_password(
                length=args.length,
                caps=args.caps,
                include_digits=not args.no_digits,
                include_symbols=args.symbols,
                exclude_ambiguous=not args.allow_ambiguous,
                mode=args.mode,
                dice_words=args.dice_words,
                wordlist_path=args.wordlist,
                pronounceable_syllables=args.pronounceable_syllables,
            )
            print(pwd)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
