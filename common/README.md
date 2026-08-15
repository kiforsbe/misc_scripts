# common

Shared helper modules used by multiple scripts in this repository. Consuming scripts add the repo root to `sys.path` and import via `common.<module>`.

## Modules

### file_grouper.py
A script that organizes files in a directory by grouping them based on their filenames using intelligent pattern matching. It identifies files that belong together (such as episodes of a TV series, parts of a multi-part archive, or related documents) and creates subdirectories to organize them logically.

The script uses advanced string matching algorithms to detect patterns in filenames, handle various naming conventions, and group related files while preserving the original file structure. It's particularly useful for organizing large collections of media files, software downloads, or document archives.

Integrates with **MyAnimeList** (via [metadatacommon](../metadatacommon/README.md)) as the primary source for anime information, providing enhanced metadata and series validation. Supports public MyAnimeList lists and exported lists for tracking watch status and completion data.

#### Features
- Intelligent filename pattern detection and grouping
- Support for various naming conventions (TV shows, movies, archives, documents)
- MyAnimeList integration for anime metadata and series validation
- Watch status tracking via public or exported MyAnimeList lists
- Configurable grouping sensitivity and pattern matching
- Recursive directory processing with configurable depth
- Detailed logging and progress reporting

#### Requires
- rapidfuzz
- pathlib

### presentation.py
Shared console presentation helpers (colors, emoji, and a `Presenter` class) used across several CLI tools for consistent terminal output formatting.

### netflix_title_parser.py
Parses Netflix viewing-history title strings into structured components (series/movie title, season, episode title) used by `netflix_watch_status.py`.

### video_thumbnail_generator.py
Generates static and animated (WEBP) video thumbnails via ffmpeg, with batch processing and progress tracking. Used by `file_metadata_scanner.py`, `latest_episodes_viewer.py`, `series_completeness_checker.py`, and `mini-dlna-server`.

## Requires
Consuming tools declare `common`'s runtime dependencies in their own requirements.txt.
