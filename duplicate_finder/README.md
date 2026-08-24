# duplicate_finder

A CLI to find likely-duplicate files by filename similarity (not content),
and interactively choose which copy to keep, in a DOS-styled full-screen
terminal UI.

## Features
- Recursive (or top-level-only) directory scanning
- Filename similarity scoring via `rapidfuzz`, with a configurable threshold
- Extension-category matching (video/audio/image/document/archive/subtitle) —
  cross-format matches like `movie.mp4` vs `movie.mkv` are allowed, but
  `movie.mp4` vs `movie.mp3` never are; unrecognized extensions are always
  included permissively rather than silently skipped
- Optional file-size tolerance filter (`--size-tolerance-percent`)
- Include/exclude keyword filters
- Interactive review: a tree of duplicate groups, per-file keep/discard
  toggling (`Enter`), bulk per-group actions (`k` keep all, `d` discard all),
  and a settings dialog (`F2`) that can rescan in place with new parameters
- Never deletes files — discarded files are moved into a review folder
  (`_duplicates/` by default), mirroring their original relative path

## Usage
```bash
python -m duplicate_finder <root> [options]

  --no-recursive              only scan the top level of <root> (default: recursive)
  --name-threshold FLOAT      0-100, default 85
  --size-tolerance-percent F  optional, default None (disabled)
  --min-group-size INT        default 2
  --include-keyword KEYWORD   repeatable; only files matching >=1 are considered
  --exclude-keyword KEYWORD   repeatable; files matching any are never considered
  --output-dir PATH           default "<root>/_duplicates"
  --dry-run                   print groups and exit; no UI, no filesystem changes
```

### Interactive controls
- Arrow keys — navigate the tree
- `Enter` — toggle keep/discard on the focused file
- `k` — mark every file in the group under the cursor as "keep"
- `d` — mark every file in the group under the cursor as "discard"
- `F2` — open the settings dialog (rescan with new parameters; discards
  unsaved keep/discard choices)
- `Ctrl+S` — commit: move every "discard" file in every reviewed group to
  the output directory, after a confirmation screen
- `q` — quit without making any filesystem changes

## Requires
- rapidfuzz
- textual
- pytest, pytest-asyncio (for running the test suite)
