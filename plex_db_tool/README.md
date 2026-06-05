# Plex DB Tool

A command-line utility designed to manage, inspect, and migrate data within Plex SQLite databases. This tool is particularly useful for users who need to move watch history or playlists between different Plex installations or perform deep inspections of their library structure.

---

## User Instructions

### Overview
The `plex_db_tool` provides a suite of commands to interact with your Plex database files directly. It allows you to view account and library information, manage playlists, and—most importantly—transfer watch history and playlist metadata between different databases.

### Details
The tool supports the following primary commands:

- **`list-accounts`**: Displays all accounts associated with the provided Plex database. It retrieves account IDs, names, and default audio/subtitle language settings.
- **`list-libraries`**: Lists all library sections (e.g., Movies, TV Shows) within the database, including section types, agents, scanners, and public visibility status.
- **`list-playlists`**: Enumerates all playlists in the database. 
    - Supports scoping to specific libraries using `--library`.
    - Can include empty playlists via `--include-empty-playlists`.
    - Outputs can be formatted as `table`, `json`, or `csv` for both console and file reports.
- **`remove-playlists`**: Deletes specific playlists from the database by ID or exact name. 
    - Includes a dry-run mode (default) and an `--apply` flag to commit changes.
    - Supports library scoping to identify which "empty" playlists to include in the removal list.
- **`sync-metadata-playlists`**: Creates or updates Plex playlists from grouped metadata JSON files (e.g., outputs from `series_completeness_checker.py`). It maps source items to target playlists based on provided group keys.
- **`transfer-watch-status`**: Migrates watch history (view counts, last viewed timestamps) between two databases using exact basename matching. 
    - Features multiple match modes (`strict`, `balanced`, `loose`) and a confidence threshold.
    - Handles conflicts via `merge`, `overwrite`, or `skip`.
- **`transfer-playlists`**: Migrates entire playlists between two databases using filename-based item matching.
    - Supports conflict policies: `unique`, `merge`, `replace`, `skip`.
    - Uses a confidence-based duplicate resolution system for items sharing the same basename.

**Basic Usage:**
```bash
python -m plex_db_tool.main <command> [args]
```
*Note: Replace `<command>` with any of the commands listed above.*


---

## Developer Maintenance

### Overview
`plex_db_tool` is built as a modular Python CLI application. It uses a registry pattern for command handling, making it easy to extend by adding new modules to the `commands/` directory. The project emphasizes type safety and clear data modeling using Python's `dataclasses`.

### Details

#### Project Structure & Architecture
- **`main.py`**: The entry point of the application. It uses `argparse` to define subcommands and a registry pattern (via `COMMAND_MODULES`) to dispatch execution to specific command handlers.
- **`models.py`**: Defines the core data domain using Python `dataclasses`. 
    - `PlexSchema`: Maps database tables to internal objects and provides helper properties for identifying key columns like `size` or `duration`.
    - `MediaRecord`: The primary object representing a media item, including metadata, file paths, and a `ParsedIdentity` (title/season/episode).
    - `WatchHistory`: Represents the watch state of a specific media item.
    - `MatchCandidate`/`MatchResult`: Structures used by the matching engine to score and report on how items from different databases correlate.
- **`infrastructure.py`**: Handles low-level system interactions:
    - `PlexDatabase`: A wrapper around `sqlite3` providing high-level methods like `list_accounts()`, `build_media_inventory()`, and `list_playlists()`.
    - `PlexFilenameParser`: Provides normalization logic for titles and basenames, and integrates with `guessit_wrapper` to extract structured identity from filenames.
    - `PlexDatabaseLocator`: Utility to resolve various path formats (folders vs. direct DB files) into valid SQLite paths.
- **`planners.py`**: Contains the "intelligence" of the tool:
    - `PlexMatcher`: Implements a weighted scoring system for matching media items across databases based on basename, file size, duration, year, and parsed identity (season/episode).
    - `PlexPlaylistPlanner`: Orchestrates complex multi-step logic for playlist migration.
- **`reporting.py`**: A unified reporting engine that handles output to the console or files in multiple formats (`table`, `json`, `csv`). It uses a column specification system to ensure consistent formatting across different commands.
- **`commands/`**: Each file is a self-contained command module. They follow a standard pattern:
    1. A `register()` function to add the command and its arguments to the parser.
    2. A `run()` function that orchestrates the database interaction, planning, and reporting for that specific action.

#### API & Development Workflow
- **Command Registration**: New commands are added by creating a file in `commands/` and ensuring it is imported into `COMMAND_MODULES` in `main.py`.
- **Data Flow**: 
    1. `main.py` parses args $\rightarrow$ 2. Command module calls `infrastructure` to fetch raw data $\rightarrow$ 3. Data is mapped to `models` $\rightarrow$ 4. `planners` process the models (e.g., matching) $\rightarrow$ 5. `reporting` outputs the results.
- **Environment**: Use a virtual environment as specified in `requirements.txt`.
- **Type Checking**: The project uses extensive type hints; new code should adhere to these for consistency and IDE support.

