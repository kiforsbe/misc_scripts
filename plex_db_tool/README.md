# plex_db_tool

A CLI for transferring Plex watch history and playlists between Plex library databases, matching items by exact filename basename only. The user provides source and target locations, and the tool discovers `com.plexapp.plugins.library.db` by exact filename under those locations, validates that each file is a usable Plex SQLite database, and then performs watch-history or playlist operations without relying on media paths.

The main entrypoint is the root shim `plex_db_tool.py`, which forwards into the `plex_db_tool` package. You can also run the package directly with `python -m plex_db_tool`. The older root script `plex_watch_status_transfer.py` is still available as a compatibility alias.

---

## Features
- `transfer-watch-status`, `transfer-playlists`, `sync-metadata-playlists`, `list-playlists`, `remove-playlists`, `list-libraries`, and `list-accounts` subcommands for transfer and inspection workflows
- `recover-database` for rebuilding malformed Plex library databases, plus a file-backed `Date Added` audit/repair mode that checks drift against file timestamps and can repair flagged rows in place after making a backup
- Accepts source and target locations instead of requiring the full DB filename path
- Locates `com.plexapp.plugins.library.db` by exact filename and verifies the schema before continuing
- Exact basename matching only; no partial filename matching
- Optional source and target library section filters by library name
- Separate source and target account ids for account-scoped read and write operations
- Interactive mode when required transfer inputs are omitted, including library, account, and playlist selection from the discovered source and target DB contents
- Smart interactive defaults for shared named accounts and existing CLI-provided values
- Dry-run by default with table, CSV, or JSON console output and JSON, CSV, or table report output
- Configurable table columns with compact labels, right-aligned numeric columns, truncation, and optional `column:width` overrides
- Dry-run filters for `all`, `warnings`, and `errors`
- Conservative merge behavior for existing target history with optional overwrite or skip policies
- Planned mutations are only created when the target actually needs to change; in-sync and target-ahead rows are left untouched unless conflict policy explicitly allows overwrite behavior
- Guarded write path that inspects the target `metadata_item_views` schema before inserting history rows
- Updates Plex account-scoped watch state in both `metadata_item_views` and `metadata_item_settings` when supported by the target schema
- Playlist listing supports library scoping, optional inclusion of empty playlists, and shows playlist ownership via `account_id` when available
- `remove-playlists` deletes specific playlists by ID or exact name, dry-run by default, with library scoping to identify which "empty" playlists to include
- Playlist transfer uses the same filename-based matching strategy as watch transfer for playlist members
- Playlist transfer supports selecting specific playlists by id or exact name and conflict handling via `unique`, `merge`, `replace`, or `skip`
- Metadata-playlist sync creates or updates one Plex playlist per selected JSON group from grouped metadata exports such as `series_completeness_checker.py`
- Metadata-playlist sync supports the same `--status-filter`, `--modified`, `--episodes-found`, `--episodes-expected`, and `--sort` group filters used by `series_archiver.py`
- Metadata-playlist sync supports `--playlist-prefix` and `--playlist-suffix` so generated playlists can be namespaced without changing the source JSON
- Metadata-playlist sync supports `--playlist-status-prefix` to derive prefixes automatically from each group's status, such as `[Incomplete]` or `[Complete]`
- Metadata-playlist sync supports `--playlist-status-suffix` to derive suffixes automatically from each group's status instead of putting the status at the front
- Metadata-playlist sync also supports `--playlist-complete-suffix` so playlists can be labeled differently when all expected episodes are available for the season
- Metadata-playlist sync supports JSON, CSV, and table console output plus matching JSON, CSV, or table report files
- Metadata-playlist sync tracks previously generated playlists by the stored source `group_key`, so changing or removing a prefix/suffix updates the same playlist instead of creating a duplicate when the group identity still matches
- Empty playlists are excluded by default for both `list-playlists` and `transfer-playlists` unless `--include-empty-playlists` is used
- Playlist transfer requires an explicit `--target-account-id` in non-interactive mode so created or updated target playlists are assigned to the intended Plex account
- Apply mode blocks until Plex Media Server is no longer running
- `recover-database` rebuilds a malformed Plex SQLite library database into a clean copy, and supports a read-only `--check-episode-date-added` audit mode plus `--apply-episode-date-added-fix --in-place` repair

## Usage Examples
```bash
# Show top-level help through the main root shim
python plex_db_tool.py --help

# Preview transfer results without writing changes
python plex_db_tool.py transfer-watch-status --source-path "C:\Users\you\AppData\Local\Plex Media Server" --target-path "D:\Backup\Plex Media Server" --source-account-id 1 --target-account-id 1 --report transfer-report.json

# Restrict transfer to named libraries and apply changes
python plex_db_tool.py transfer-watch-status --source-path "C:\Users\you\AppData\Local\Plex Media Server" --target-path "D:\Backup\Plex Media Server" --source-library TV --target-library TV --source-account-id 1 --target-account-id 1 --apply

# Show a compact console table during dry-run
python plex_db_tool.py transfer-watch-status --source-path "C:\Users\you\AppData\Local\Plex Media Server" --target-path "D:\Backup\Plex Media Server" --source-account-id 1 --target-account-id 1 --console-format table --columns status,dry_run_status,source_watch_count,target_watch_count,source_filename,target_filename

# Export a CSV review report
python plex_db_tool.py transfer-watch-status --source-path "C:\Users\you\AppData\Local\Plex Media Server" --target-path "D:\Backup\Plex Media Server" --source-account-id 1 --target-account-id 1 --report transfer-report.csv

# Inspect available libraries and accounts before transferring
python plex_db_tool.py list-libraries --path "C:\Users\you\AppData\Local\Plex Media Server"
python plex_db_tool.py list-accounts --path "C:\Users\you\AppData\Local\Plex Media Server"

# List playlists in a DB and show owner account ids when available
python plex_db_tool.py list-playlists --path "C:\Users\you\AppData\Local\Plex Media Server" --library TV --console-format table

# Preview transferring selected playlists into a target account
python plex_db_tool.py transfer-playlists --source-path "C:\Users\you\AppData\Local\Plex Media Server" --target-path "D:\Backup\Plex Media Server" --source-library TV --target-library TV --target-account-id 1 --playlist "Favorites" --playlist-conflict-policy merge

# Build or refresh one Plex playlist per filtered JSON group
python plex_db_tool.py sync-metadata-playlists --input-json ".\series-results.json" --target-path "C:\Users\you\AppData\Local\Plex Media Server" --target-library "TV Shows" --target-account-id 1 --status-filter "+incomplete +watched_partial" --episodes-found ">=2" --sort

# Namespace generated playlist names and write a JSON review report
python plex_db_tool.py sync-metadata-playlists --input-json ".\series-results.json" --playlist-prefix "[Incomplete] " --playlist-suffix " [Review]" --console-format json --report .\sync-playlists.json

# Automatically prefix playlist names from each group's status
python plex_db_tool.py sync-metadata-playlists --input-json ".\series-results.json" --playlist-status-prefix

# Automatically suffix playlist names from each group's status
python plex_db_tool.py sync-metadata-playlists --input-json ".\series-results.json" --playlist-status-suffix

# Append an extra suffix only when a season is complete
python plex_db_tool.py sync-metadata-playlists --input-json ".\series-results.json" --playlist-prefix "[Anime] " --playlist-complete-suffix " [Complete]"

# Combine every selected episodes_expected=1 group into one playlist
python plex_db_tool.py sync-metadata-playlists --input-json ".\series-results.json" --one-of-one-playlist "[A] One Of One"

# Export a compact table report with custom columns
python plex_db_tool.py sync-metadata-playlists --input-json ".\series-results.json" --console-format table --columns target_playlist,status,matched_item_count,unmatched_item_count,notes --report .\sync-playlists.txt --report-format table

# Audit episode Date Added drift against file timestamps without modifying the DB
python plex_db_tool.py recover-database --path "C:\Users\you\AppData\Local\Plex Media Server" --check-episode-date-added --episode-date-added-max-drift-hours 24

# Back up the source DB and repair flagged Date Added rows in place
python plex_db_tool.py recover-database --path "C:\Users\you\AppData\Local\Plex Media Server" --in-place --apply-episode-date-added-fix --episode-date-added-max-drift-hours 24

# Omit required transfer values to use the interactive workflow
python plex_db_tool.py transfer-watch-status

# Or run the package directly
python -m plex_db_tool --help

# Or use the older compatibility script name
python plex_watch_status_transfer.py --help
```

## Notes
- In apply mode the script waits until Plex Media Server has fully stopped before it starts writing.
- Dry-run mode can be used while Plex is still running, but copied DBs are still safer.
- If the provided location contains multiple matching DB files, the script refuses to guess and asks for a narrower path.
- If multiple target records share the same basename, the tool uses secondary metadata to decide whether the match is safe enough to apply.
- If you omit the subcommand entirely, the CLI defaults to `transfer-watch-status`.
- Interactive transfer mode first performs a dry-run, shows only rows with planned mutations by default, and then asks whether to apply the changes.
- By default, rows that are already in sync or where the target is already ahead do not produce planned mutations.
- Depending on the Plex schema version, source and target account ids may be required for account-scoped reads and writes.
- Playlist discovery can read both legacy/custom playlist rows and metadata-backed Plex playlists.
- `transfer-playlists` excludes empty playlists by default and prints a notice explaining how to include them.
- `transfer-playlists` requires `--target-account-id` in non-interactive mode; interactive mode can prompt for it.
- `sync-metadata-playlists` defaults `--playlist-conflict-policy` to `replace`, so rerunning it refreshes existing playlists from the current JSON selection.
- `sync-metadata-playlists` also removes previously synced playlists when their stored `group_key` is no longer present in the current metadata selection for the chosen target account and library scope.
- `sync-metadata-playlists` uses the standard `%LOCALAPPDATA%\Plex Media Server` folder as the target when `--target-path` is omitted.
- `sync-metadata-playlists` requires `--target-library` and `--target-account-id` in non-interactive mode; if you omit them in an interactive terminal session, the command prompts you to choose them.
- `sync-metadata-playlists` table output supports `--columns` with `column` or `column:width` entries; mandatory columns are `status` and `target_playlist`.
- `--one-of-one-playlist` collapses all selected groups whose original metadata reports `episodes_expected=1` into one synthetic playlist, which is useful when movie-like metadata should still be synced as a single mixed collection.
- If a playlist was previously created with a prefix or suffix, later syncs can rename it to the new configured name as long as the stored metadata `group_key` still identifies the same JSON group.
- `--playlist-status-prefix` turns a group's stored status value into a prefix automatically, so `incomplete` becomes `[Incomplete]` and `complete_with_extras` becomes `[Complete With Extras]`.
- `--playlist-status-suffix` does the same at the end of the playlist name instead of the beginning.
- Prefix options are mutually exclusive with each other, and suffix options are mutually exclusive with each other, so each sync run can use at most one prefix mode and one suffix mode.
- `--playlist-complete-suffix` is applied when `episodes_found >= episodes_expected` for groups with a known expected count; if no expected count is present, `complete` and `complete_with_extras` statuses are treated as complete.

---

## Developer Maintenance

### Overview
`plex_db_tool` is built as a modular Python CLI application. It uses a registry pattern for command handling, making it easy to extend by adding new modules to the `commands/` directory. The project emphasizes type safety and clear data modeling using Python's `dataclasses`.

### Project Structure & Architecture
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

### API & Development Workflow
- **Command Registration**: New commands are added by creating a file in `commands/` and ensuring it is imported into `COMMAND_MODULES` in `main.py`.
- **Data Flow**:
    1. `main.py` parses args → 2. Command module calls `infrastructure` to fetch raw data → 3. Data is mapped to `models` → 4. `planners` process the models (e.g., matching) → 5. `reporting` outputs the results.
- **Environment**: Use a virtual environment as specified in `requirements.txt`.
- **Type Checking**: The project uses extensive type hints; new code should adhere to these for consistency and IDE support.
