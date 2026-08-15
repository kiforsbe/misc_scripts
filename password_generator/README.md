# password_generator

A small CLI for generating strong, easy-to-remember passwords.

## Features
- Secure random generation using the `secrets` module
- Modes: `random`, `pronounceable` (syllable-based), and `diceware` (wordlist)
- Options to exclude ambiguous characters (0/O, 1/l, etc.), include/exclude digits and symbols
- Enforces at least two characters from each enabled subset (upper/lower/digits/symbols)
- Built-in small wordlist plus support for the EFF large diceware list (downloadable)

## Usage examples
```bash
# Random 12-char password (mixed case, digits)
python -m password_generator --length 12

# Pronounceable password using 4 syllables
python -m password_generator --mode pronounceable --pronounceable-syllables 4 --length 12

# Diceware using bundled builtin wordlist (3 words)
python -m password_generator --mode diceware --wordlist builtin --dice-words 3

# Install the recommended EFF wordlist for diceware mode
python -m password_generator --install-wordlist
```

Can also be run directly: `python password_generator/password_generator.py ...`

Downloaded wordlists are stored in `wordlists/` (ignored by git).
