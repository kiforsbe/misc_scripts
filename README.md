# misc_scripts
Miscellaneous scripts to automate common tasks.

## Table of Contents
- [User Scripts](#user-scripts): Tampermonkey userscripts that enhance supported sites with faster actions and metadata helpers.
  - [plex-playlist-watch-status.user.js](#plex-playlist-watch-statususerjs): Shows Plex playlist watch status with simple triangle indicators.
  - [myanimelist-addtolist-improved.user-script.js](#myanimelist-addtolist-improveduser-scriptjs): Adds quick MyAnimeList watch-status dropdowns directly to anime pages.
- [Libraries](#libraries): Reusable helper modules shared by multiple scripts in this repository.
  - [common](#common): Shared file grouping, presentation, thumbnail-generation, browser-launching, and filename-parsing helpers.
  - [metadatacommon](#metadatacommon): Shared metadata provider package (anime, IMDb, Plex, MyAnimeList) used across several tools.
- [Projects](#projects): Larger multi-file tools with dedicated packages, helpers, tests, or service components.
  - [plex_db_tool](#plex_db_tool): Package-backed Plex database transfer and playlist sync CLI with root shim.
  - [video-optimizer-v2](#video-optimizer-v2): Multi-file video transcoder using shared metadata providers, with tests.
  - [youtube-video-downloader](#youtube-video-downloader): Bundle of CLI, TUI, web, userscript, and helper download tools.
  - [mini-dlna-server](#mini-dlna-server): Experimental DLNA server project with multiple networking and service modules.
- [Scripts](#scripts): Standalone utilities for media, metadata, downloads, reports, and local tooling.
  - Media Conversion & Transcription
    - [cbr_to_cbz_converter.py](#cbr_to_cbz_converterpy): Converts CBR archives to CBZ files using parallel in-memory processing.
    - [media-to-mp3.py](#media-to-mp3py): Converts media files to MP3 using each file's first audio track.
    - [srt_to_transcript.py](#srt_to_transcriptpy): Extracts plain-text transcripts from SRT subtitle files.
    - [transcribe_to_srt.py](#transcribe_to_srtpy): Transcribes media files into SRT subtitles using SubsAI models.
    - [transcribe_audio.py](#transcribe_audiopy): Transcribes audio with diarization and exports SRT, VTT, JSON, and TXT.
    - [insanely-fast-whisper.py](#insanely-fast-whisperpy): Minimal Whisper transcription script for fast speech-to-text generation.
    - [mp4-to-mp3-converter-with-origin.py](#mp4-to-mp3-converter-with-originpy): Converts MP4 files to tagged MP3s with thumbnail and source metadata.
    - [merge-audio-files-to-one-output.py](#merge-audio-files-to-one-outputpy): Interactive tool for merging multiple audio files into one output.
    - [get_music_genre.py](#get_music_genrepy): Classifies music genre from audio or video files.
  - Metadata, Cataloging & Reports
    - [compare_package_versions.py](#compare_package_versionspy): Compares proposed package versions against installed ones, highlighting upgrades and downgrades.
    - [imdb_title_query.py](#imdb_title_querypy): Queries IMDb TSV datasets with filtering, column selection, and downloads.
    - [metadata_cache_manager.py](#metadata_cache_managerpy): CLI to inspect and control TTLs for the metadatacommon provider caches.
    - [smartls.py](#smartlspy): Metadata-aware directory explorer with rich filtering, sorting, and report exports.
    - [series_info_tool.py](#series_info_toolpy): Groups video files and fetches series metadata with MyAnimeList integration.
    - [netflix_watch_status.py](#netflix_watch_statuspy): Builds Netflix viewing history reports with metadata and standalone HTML output.
    - [file_metadata_scanner.py](#file_metadata_scannerpy): Scans files for metadata and exports CSV, JSON, and HTML bundles.
    - [gog_galaxy_exporter.py](#gog_galaxy_exporterpy): Exports GOG Galaxy library data to CSV, JSON, and Excel.
    - [gog_csv_to_html.py](#gog_csv_to_htmlpy): Builds an interactive HTML game library viewer from GOG exports.
    - [file_grouper.py](#file_grouperpy): Groups related files by filename patterns and series metadata.
    - [series_completeness_checker.py](#series_completeness_checkerpy): Analyzes series collections for missing episodes, gaps, and completeness.
    - [series_archiver.py](#series_archiverpy): Archives series into organized folders with optional integrity checks.
    - [series_bundler.py](#series_bundlerpy): Bundles episodes into standardized archive folders using parsed filename metadata.
    - [latest_episodes_viewer.py](#latest_episodes_viewerpy): Generates an HTML page listing the latest episodes in a collection.
  - Local Tooling & Automation
    - [ollama_tool_agent.py](#ollama_tool_agentpy): Approval-gated local Ollama tool agent with streamed reasoning, persistent task state, and file tools.
    - [local_test_server_manager.py](#local_test_server_managerpy): Lists and cleanly stops locally running test HTTP servers (npx, http.server, vite, etc.).
  - Web Services, Networking & Downloads
    - [simple_scraper_proxy.py](#simple_scraper_proxypy): Fetches pages with YAML selectors and returns scraped RSS feeds.
    - [m3u8-to-mp4-flask-webservice.py](#m3u8-to-mp4-flask-webservicepy): Flask service that inspects and converts M3U8 streams to MP4.
    - [m3u8-to-mp4-flask-webservice-simple.py](#m3u8-to-mp4-flask-webservice-simplepy): Simplified Flask service that streams M3U8 inputs directly into MP4 files.
    - [udio-flask-webservice.py](#udio-flask-webservicepy-udio-download_ext-buttonuserjs): Downloads songs with embedded metadata, cover art, and optional enrichment.
    - [rss-feed-downloader.py](#rss-feed-downloaderpy): Downloads selected RSS enclosures with terminal-based selection and progress.
    - [simple_http_proxy.py](#simple_http_proxypy): Serves remote resources through simple URL-based local proxying.
    - [socks5_http_tunneler.py](#socks5_http_tunnelerpy): Local HTTP tunnel that forwards upstream traffic through SOCKS5.
    - [radio_station_checker.py](#radio_station_checkerpy): Checks radio stream availability with a high-performance terminal interface.
    - [serve_local.py](#serve_localpy): Serves local files or folders with optional live-reload and file proxy.
  - File, Clipboard & Document Utilities
    - [clipboard-monitor.py](#clipboard-monitorpy): Monitors clipboard changes and appends captured content to CSV.
    - [file-renamer-script.py](#file-renamer-scriptpy): Builds CSV-based rename mappings from clipboard IDs before applying changes.
    - [md_to_docx.py](#md_to_docxpy): Converts Markdown documents into formatted DOCX files.
- [Bash Scripts](#bash-scripts): Shell utilities for platform-specific maintenance tasks.
  - [macos-uninstall-python3.10.sh](#macos-uninstall-python310sh): Safely removes python.org Python 3.10 installations from macOS.
- [Experimental](#experimental): Early-stage ideas and prototypes that are not yet stable.
  - [lyrics-timing-generator.py](#lyrics-timing-generatorpy): Experimental timed-lyrics generator built from transcription and LLM formatting.

## User Scripts
These user scripts enhance the webservice functionality by integrating download buttons directly into the respective web interfaces. They automatically capture song metadata and cover art, then send this information to the webservice for processing, making the download process seamless and efficient. They have been tested with Tampermonkey on Chrome.
- `udio-download_ext-button.user.js`: Adds a "Download with metadata" button to Udio song pages
- `riffusion-download_ext-button.user.js`: Adds a "Download with metadata" button to Riffusion song pages

### plex-playlist-watch-status.user.js
A Tampermonkey script that adds simple triangle indicators to Plex playlist items, showing their watch status (watched/unwatched) based on metadata from the Plex API.
It fetches the watch status of each item in a Plex playlist and displays a triangle icon next to each item, indicating whether it has been watched or not. The script is designed to enhance the user experience by providing quick visual feedback on the watch status of playlist items.

#### Usage
1. Install Tampermonkey or a similar userscript manager in your browser.
2. Import the script into Tampermonkey.
3. Make sure your local IP addresses are whitelisted in the script.
4. Navigate to your Plex playlist page, open a playlist and see the watch status indicators appear next to each item thumbnail.

#### Requires
- Tampermonkey or a similar userscript manager

### myanimelist-addtolist-improved.user-script.js
A Tampermonkey script that augments every watch-status button on MyAnimeList season and anime pages with a quick-status chevron dropdown, allowing you to set or change the watch status of any title without leaving the page.

#### Features
- Adds a seamless chevron trigger to the right of every `.btn-anime-watch-status` button, styled to look like a native extension of the host button (matching background, border, height, and border-radius)
- Dropdown lists all five statuses: Plan to Watch, Watching, On Hold, Completed, Dropped
- Applies status changes in-page via `fetch` (same-origin form POST) — no page navigation required
- Automatically detects whether the title is being added or edited by following MAL's server-side redirect
- Reloads the page after a successful save so button state reflects the new status
- Disables invalid statuses (Watching, Completed, On Hold, Dropped) for not-yet-aired titles, keeping only Plan to Watch enabled
- Toast notifications for success and error feedback
- Keyboard navigation (Escape, Arrow Up/Down) and full ARIA support
- Observes dynamic content and SPA navigation to augment newly added buttons automatically
- `DEBUG` constant at the top of the script to toggle verbose console logging on/off

#### Matches
- `https://myanimelist.net/anime/season/*`
- `https://myanimelist.net/anime/*`

## Libraries

### common
Shared helper modules used by multiple scripts in this repository: `file_grouper.py`, `presentation.py`, `netflix_title_parser.py`, `video_thumbnail_generator.py`, `browser_utils.py`, and `guessit_wrapper.py`.

See [common/README.md](common/README.md) for module details and requirements.

### metadatacommon
Shared metadata provider package (anime, IMDb, Plex, MyAnimeList) used by `video-optimizer-v2`, `common/file_grouper.py`, and several other tools to look up show/movie metadata and MyAnimeList watch status. Also bundles the standalone `metadata_cache_manager.py` and `validate_mal_xml.py` utility scripts.

See [metadatacommon/README.md](metadatacommon/README.md) for the module list, utility scripts, and requirements.

## Projects
Larger tools in this repository that have their own subfolders, packages, helpers, or tests.

### plex_db_tool
A CLI for transferring Plex watch history and playlists between Plex library databases, matching items by exact filename basename only. Supports watch-history and playlist transfer, metadata-driven playlist sync, and database recovery/repair. The main entrypoint is the root shim `plex_db_tool.py` (forwards into the `plex_db_tool` package); `python -m plex_db_tool` also works.

See [plex_db_tool/README.md](plex_db_tool/README.md) for the full command reference, usage examples, and architecture notes.

### video-optimizer-v2
A tool for quick and easy video optimization: supply a list of videos on the command line or drag and drop them onto the script for choices about subtitle/audio defaults and target quality/resolution. Made for transcoding legacy TV-show media, and looks up metadata from anime databases and IMDb via the shared [metadatacommon](#metadatacommon) providers.

See [video-optimizer-v2/README.md](video-optimizer-v2/README.md) for usage and requirements.

### youtube-video-downloader
A collection of YouTube download scripts using the `ytdl_helper` library: a CLI, a TUI, a Flask web service with companion userscript, and integration with `common/music_style_classifier.py` for classifying downloaded audio.

See [youtube-video-downloader/README.md](youtube-video-downloader/README.md) for each component's usage and requirements.

### mini-dlna-server
A DLNA/UPnP media server targeting Samsung TVs (2022+) and Windows 11 hosts, with automatic thumbnail generation, hot-reload config, and SSDP discovery. Still under active development.

See [mini-dlna-server/README.md](mini-dlna-server/README.md) for configuration, supported formats, and architecture.

## Scripts

### cbr_to_cbz_converter.py
A script to convert Comic Book RAR (CBR) files to Comic Book ZIP (CBZ) files using in-memory processing. It does this to avoid first extracting the files onto the drive and then recompressing them, generating additional IO operations. Recursively scans directories for CBR files and converts them in parallel with progress tracking.

#### Features
- In-memory processing to minimize disk I/O
- Recursive directory scanning for CBR files
- Multi-threaded parallel conversion for faster processing
- Progress bars with tqdm for visual feedback
- Automatic CBZ duplicate detection (skips existing files)
- Optional deletion of original CBR files after successful conversion
- Dual extraction method support:
  - **libarchive-c** (preferred, faster, requires an installation of libarchive library)
  - **rarfile** (fallback if libarchive unavailable, requires unrar tool installed)
- Detailed logging with configurable verbosity levels
- Comprehensive conversion statistics and error reporting
- Thread-safe statistics tracking
- Automatic cleanup of partial files on failure

#### Usage Examples
```bash
# Convert all CBR files in a directory (deletes originals by default)
python cbr_to_cbz_converter.py /path/to/comics

# Keep original CBR files after conversion
python cbr_to_cbz_converter.py /path/to/comics --keep-original

# Use parallel processing with 4 workers
python cbr_to_cbz_converter.py /path/to/comics -j 4

# Verbose output (INFO level)
python cbr_to_cbz_converter.py /path/to/comics -v

# Very verbose output (DEBUG level)
python cbr_to_cbz_converter.py /path/to/comics -vv

# Quiet mode (errors only)
python cbr_to_cbz_converter.py /path/to/comics --quiet

# Combine options
python cbr_to_cbz_converter.py /path/to/comics --keep-original -j 4 -v
```

#### Requires
- tqdm
- libarchive-c (recommended) or rarfile (fallback)
  - libarchive-c requires libarchive DLL installed on system
  - rarfile requires UnRAR tool installed and on PATH

### password_generator.py
A small CLI for generating strong, easy-to-remember passwords, with `random`, `pronounceable`, and `diceware` (wordlist) modes.

See [password_generator/README.md](password_generator/README.md) for usage and features.


### compare_package_versions.py
Compares proposed package versions against installed versions. Useful for inspecting pip output or a list of package-version pairs and highlighting downgrades, upgrades, and local version changes.

#### Usage Examples
```bash
# Compare from pip output
pip install -r requirements.txt | python compare_package_versions.py

# Compare from a string argument
python compare_package_versions.py "requests==2.31.0 urllib3==2.2.1"

# Disable ANSI colors
python compare_package_versions.py --no-color "requests==2.31.0"
```

#### Requires
- packaging

### ollama_tool_agent.py
A local Ollama-based task agent that can inspect files and execute approved tool actions step by step. It streams visible thinking to the console, requires explicit user approval before each tool execution, preserves recent turn context across turns, and keeps the session open for follow-up requests.

#### Features
- Streams user-visible thinking and response progress in the console
- Uses structured JSON responses with a single next action per turn
- Requires explicit approval before any tool call is executed
- Persists recent thinking, plans, answers, guidance, tool inputs, and tool outputs as task state
- Includes built-in tools for `list_files`, `crc32`, and batch `rename_files`
- Supports follow-up guidance so the agent can continue the same task instead of restarting from scratch
- Exposes CLI options for model name, host, read timeout, max steps, working directory, and requested context size

#### Usage Examples
```bash
# Start an interactive session
python ollama_tool_agent.py

# Run a single task from the command line
python ollama_tool_agent.py "List the files in C:\Media and tell me how many there are"

# Override the model and requested context size
python ollama_tool_agent.py --model gemma4:e2b --num-ctx 65536 "Compute CRC32 for files in C:\Downloads\Season 1"
```

#### Requires
- ollama
- httpx
- colorama
- tqdm

### local_test_server_manager.py
A Windows-only utility that finds locally running test HTTP servers (`npx serve`/`http-server`, `python -m http.server`, `vite preview`, `php -S`, etc.) and lets you stop them cleanly without touching unrelated local processes. Detects servers by matching process command lines against a known pattern list, groups wrapper and child processes (e.g. `npx` and the `node` process it spawns) into a single instance, and shows the port(s) each one is listening on.

#### Features
- Lists detected test servers with id, PID, port(s), and command line via a `rich` table
- Stops one, several, or all detected servers by id, port, or `all`
- No-argument interactive mode with an `inquirer` checkbox menu for picking which servers to stop
- Graceful shutdown first (`taskkill /T`), escalating to a forced whole-tree kill only if the process is still alive after a short wait
- Only ever acts on processes whose command line matched a known test-server pattern; never lists or touches arbitrary local servers

#### Usage Examples
```bash
# List detected test servers
python local_test_server_manager.py list

# Stop a specific instance by id or port
python local_test_server_manager.py kill 1
python local_test_server_manager.py kill 8080

# Stop everything detected, skipping the confirmation prompt
python local_test_server_manager.py kill all -y

# No arguments: interactive checkbox menu
python local_test_server_manager.py
```

#### Requires
Windows 10 or Windows 11 (uses PowerShell and `taskkill`/`tasklist`).
- rich
- inquirer

### simple_scraper_proxy.py
A standalone scraping proxy that fetches an upstream HTML page, extracts feed data using a local YAML selector template, and returns an RSS 2.0 feed.

See [simple_scraper_proxy/README.md](simple_scraper_proxy/README.md) for usage and requirements.

### imdb_title_query.py
A simple CLI for querying IMDb `title.*.tsv.gz` datasets using the declared dataset schemas, substring search, basic filters, and plain-text table output. It can read a specific file, scan a directory for the matching dataset, or use the same default cache folder as the `metadatacommon` IMDb provider.

#### Features
- Declared schemas for `title.basics`, `title.akas`, `title.crew`, `title.episode`, `title.principals`, and `title.ratings`
- Simple filtering with operators such as `=`, `!=`, `~`, `!~`, `>`, `>=`, `<`, and `<=`
- Selectable output columns and optional query restriction to specific search columns
- Reads `.tsv.gz` files directly without extracting them first
- Defaults to `%USERPROFILE%\.video_metadata_cache\imdb` when no path is provided
- Prompts before downloading a missing dataset interactively, with `--download` and `--force-download` options for unattended or refresh workflows

#### Usage Examples
```bash
# Query a specific IMDb dataset file
python imdb_title_query.py C:\path\to\title.basics.tsv.gz --query matrix --where titleType=movie

# Use the shared IMDb cache folder and auto-download if missing
python imdb_title_query.py --dataset title.ratings --download --where numVotes>=100000

# Force re-download of a cached dataset into a custom cache directory
python imdb_title_query.py --dataset title.akas --force-download --cache-dir D:\imdb-cache --query ghost

# Show only selected columns
python imdb_title_query.py --dataset title.basics --columns tconst,primaryTitle,startYear --query alien

# Print the declared schema for a dataset
python imdb_title_query.py --show-schema title.episode
```

#### Requires
- No external dependencies required (uses only Python standard libraries)

### metadata_cache_manager.py
CLI helper for the `metadatacommon` providers to inspect and control cache TTLs (`status`, `refresh`, `invalidate`, `set-expiry`).

See [metadatacommon/README.md](metadatacommon/README.md#metadata_cache_managerpy) for usage and requirements.

### media-to-mp3.py
Converts one or more media files to `.mp3` in the same folder, always using the first audio track from each input. Shows a per-file conversion progress bar and keeps FFmpeg's default MP3 encoding settings.

#### Usage Examples
```bash
# Convert one file
python media-to-mp3.py video.mkv

# Convert multiple files
python media-to-mp3.py file1.mkv file2.mp4 file3.webm

# Convert using wildcard patterns (quoted so the script expands them itself)
python media-to-mp3.py "*.mkv" "subfolder/**/*.mp4"

# Overwrite existing MP3 outputs
python media-to-mp3.py --force file1.mkv file2.mp4
```

#### Requires
- FFmpeg (`ffmpeg` and `ffprobe` on PATH)
- tqdm (optional, for a richer progress bar)

### smartls.py
A smart directory explorer for querying files and folders with composable filters, metadata-aware sorting, and multiple output modes (tree, flat list, JSON, CSV, self-contained HTML report).

See [smartls/README.md](smartls/README.md) for the full filter syntax, usage examples, and notes.

### series_info_tool.py
A comprehensive tool to extract and display series information for video files, with MyAnimeList integration. Groups video files by series title, retrieves metadata from anime and movie databases, and provides convenient ways to access online information. Designed for Windows shell:sendto and drag-drop operations.

#### Features
- Groups video files by series title using FileGrouper
- Retrieves metadata from MyAnimeList, IMDb, and other sources
- Extracts and displays comprehensive series information including:
  - Basic metadata (Type, Year, Status, Rating, Episodes, Seasons)
  - MyAnimeList information (Score, Rank, Studios, Genres, Themes)
  - IMDb information (Rating, Votes, Metascore)
  - Watch status from MyAnimeList XML exports
- Multiple output formats:
  - **default**: Simple text output
  - **aligned**: Right-aligned labels for better readability
  - **color**: ANSI colored output
  - **json**: Machine-readable JSON format
- URL operations:
  - Display MyAnimeList URLs
  - Copy URLs to clipboard (Windows)
  - Open URLs in browser with window mode control
- Browser window modes via common/browser_utils.py:
  - **default**: New tab/window
  - **popup**: Chromeless, half-width, centered
  - **maximized**: Full screen
- Configurable logging levels including DEBUG2 for regex debugging
- Extended metadata mode for verbose output

#### Usage Examples
```bash
# Display information about files
series_info_tool.py file1.mkv file2.mkv

# Copy MyAnimeList URLs to clipboard
series_info_tool.py --copy file1.mkv file2.mkv

# Open URLs in browser (new tab)
series_info_tool.py --open file1.mkv

# Open in popup mode (chromeless, half-width, centered)
series_info_tool.py --open=popup file1.mkv

# Open maximized
series_info_tool.py --open=maximized file1.mkv

# Use with MyAnimeList XML for watch status
series_info_tool.py --mal-xml animelist.xml file1.mkv

# Different output formats
series_info_tool.py --format aligned file1.mkv
series_info_tool.py --format color file1.mkv
series_info_tool.py --format json file1.mkv

# Extended metadata with all sources and full tags
series_info_tool.py --extended-metadata file1.mkv

# Debug mode with regex debugging
series_info_tool.py --log-level DEBUG2 file1.mkv
```

#### Requires
- common/file_grouper.py
- common/browser_utils.py

### netflix_watch_status.py
Reads a Netflix viewing history CSV, classifies entries as movies or series episodes, resolves metadata from IMDb and anime providers when available, and generates both console and standalone HTML watch-status reports.

See [netflix_watch_status/README.md](netflix_watch_status/README.md) for features and usage examples.

### srt_to_transcript.py
Saves contents of the specified `.srt` files to a plain text transcripts.

#### Requires
- srt

### transcribe_to_srt.py
Transcribes the specified media files such as `.mkv` to `.srt` subtitles.
Defaults to model `WhisperX` and language `English` (`"en"`) for transcription.
The model should automatically download and install when the script is run.

#### Requires
- SubsAI (<https://github.com/abdeladim-s/subsai>)
  - Model: m-bain/whisperX

#### Recommended
Torch with CUDA support is highly recommended if you have a CUDA capable machine. For SubsAI with `torch-2.0.1` requirement, install `torch-2.0.1+cu118` per instruction <https://pytorch.org/get-started/previous-versions/#v201> instead of default one in SubsAI "`requirements.txt`" file.

### transcribe_audio.py
Object-oriented CLI to transcribe meeting audio with faster-whisper, WhisperX 3.7.6 alignment, lightweight WhisperX VAD segmentation, and ECAPA-TDNN speaker clustering. Produces SRT, VTT, JSON, and TXT outputs with optional SRT speaker tags. Designed for Python 3.11 and 3.13 and optimized for CPU.

#### Usage Examples
```bash
# Transcribe with default outputs (SRT, JSON, TXT)
python transcribe_audio.py meeting.wav

# Add WebVTT output
python transcribe_audio.py meeting.wav --outputs srt vtt json txt

# Disable speaker tags in SRT output
python transcribe_audio.py meeting.wav --no-srt-speaker-tags
```

#### Requires
- faster-whisper
- whisperx==3.7.6
- torch
- torchaudio
- speechbrain
- scikit-learn
- numpy

### insanely-fast-whisper.py
Minimalistic script to generate transcription using Whisper.

#### Requires
- click
- torch
- transformers

### mp4-to-mp3-converter-with-origin.py
Converts mp4 files to mp3 files. I use this to easily convert my Udio songs to `.mp3`s for my iPhone.

The converted MP3-files include:
- the audio from the video file
- a thumbnail based on the first frame of the video
- the following metadata:
  - **Title & Artist** (based on the filename, "`Artist - Title.mp4`")
    - Defaults to "`Udio`" if nothing else is specified
  - **Comments:** `Refferer` and `HostUrl` based on the Windows 10/11 metadata stored with the file when downloaded

#### Requires
Windows 10 or Windows 11.
- moviepy
- eyeD3

### clipboard-monitor.py
Monitors the clipboard for changes and appends the contents to "`clipboard.csv`" file. It plays a sound when a change is detected and saved to the file.

#### Requires
Windows 10 or Windows 11.
- pywin32 (provides the win32clipboard module)

### file-renamer-script.py
Takes a "`clipboard.csv`" file as input and uses the first column as a file_id that it tries to find in the files in the same folder as the script. If it finds the file, it is added to the list, with the proposed filename in the second column.

It then outputs the full list of proposed changes as file "`rename_mappings.csv`", so the user can verify the changes.

If the user approves them, the user simply responds "y", or "yes" or presses enter to confirm that they want to rename the files in the folder with the proposed filenames.

#### Requires
- No external dependencies required (uses only Python standard libraries)

### file_metadata_scanner.py
A comprehensive tool for extracting metadata from files and folders, with optional extended metadata (audio/video via ffmpeg, image dimensions, comic archive contents), thumbnail generation, and export to CSV, JSON, and an interactive HTML webapp.

See [file_metadata_scanner/README.md](file_metadata_scanner/README.md) for features, usage examples, and requirements.

### m3u8-to-mp4-flask-webservice.py
A flask web service that takes a m3u8 file as input and converts it into an MP4 file.

The webservice exposes the following interfaces:

| Interface | Methods | Functions | Parameters |
| --- | --- | --- | --- |
| stream_info | GET | stream_info | url |
| convert | POST & GET | convert_m3u8_to_mp4 | url, alt_url, title, video_id, description |

#### Endpoints

##### stream_info
Returns a JSON structure describing the overall metadata of the base stream, and the contained audio and video streams.

| Parameter | Description |
| --- | --- |
| url | The URL of the m3u8 file to be queried. |

##### convert
Converts an input m3u8 file into a MP4 file. The input is sent as a multipart/form-data request, with the key "file" and the value being the m3u8 file to be converted.

| Parameter | Description |
| --- | --- |
| url | The URL of the m3u8 file to be converted. |
| alt_url | Can be used to provide the web page where the video was located for example. |
| title | The title of the video. |
| video_id | A unique ID that is used to identify the video. |
| description | A description of the video. |

#### Requires
- flask
- requests
- m3u8
- ollama
- ffmpeg (the executable, installed separately and on PATH)

### m3u8-to-mp4-flask-webservice-simple.py
A flask web service that takes a m3u8 file as input and converts it into an MP4 file.

The webservice exposes the following interfaces:

| Interface | Methods | Functions | Parameters |
| --- | --- | --- | --- |
| get | POST & GET | convert_m3u8_to_mp4 | url, filename |

#### Endpoints

##### get
Streams a m3u8 to save it as a mp4, real-time saving only, so will take as long as the stream itself is.

| Parameter | Description |
| --- | --- |
| url | The URL of the m3u8 file to be converted. |
| filename | Target filename (optional). |

#### Requires
- flask
- requests
- m3u8
- ffmpeg (the executable, installed separately and on PATH)

### merge-audio-files-to-one-output.py
Simple merge a bunch of audio files into one single output file. Just drag all the input files onto the script and it will be output in the same folder as the first file with the name "`combined_output.<ext>`". The script will ask what format, bitrate etc the output shall get.

#### Requires
- pydub
- inquirer
- tqdm

### udio-flask-webservice.py (udio-download_ext-button.user.js)
A flask web service that adds metadata including cover art to song files downloaded from Udio or Riffusion. Comes with user scripts (e.g. Tampermonkey) that add a "Download with metadata" button to the song pages, calling the webservice.

See [udio-flask-webservice/README.md](udio-flask-webservice/README.md) for the API reference and requirements.

### rss-feed-downloader.py
A script to parse RSS feeds and download enclosures (e.g., audio, video, or other files) with a console-based GUI for selection and progress tracking.

#### Features
- Parses RSS feeds from local files or URLs.
- Extracts enclosures and allows users to select files for download.
- Downloads files with progress tracking and optional HTTP Basic Authentication.
- Saves downloaded files to a specified directory and generates a mapping file in JSON format.

#### Requires
- windows-curses (Windows only; the stdlib `curses` module doesn't work on Windows without it)

### get_music_genre.py
A script that takes as imput an audio/video file to classify the music style of the file. It uses a pre-trained model to classify the music style and outputs the result.
It is a command line tool; the underlying classifier (get_music_genre(file_path: str, track_index: int = None) -> str | None, warm_up()) lives in [common/music_style_classifier.py](common/music_style_classifier.py) as a library, reused by `udio-flask-webservice`, `youtube-video-downloader`, and `set_music_genre.py`.

#### Requires
- librosa
- torch
- safetensors
- transformers
- ffmpeg (the executable, installed separately and on PATH; ffmpeg-python is the pip package)

### md_to_docx.py
Converts Markdown files to Microsoft Word DOCX format, handling headings, lists, tables, inline formatting, and blockquotes with proper Word styling.

See [md_to_docx/README.md](md_to_docx/README.md) for features, usage examples, and requirements.

### gog_galaxy_exporter.py
A script that exports game library data from GOG Galaxy 2.0 database to CSV, JSON, and Excel formats. It extracts comprehensive game information including titles, platforms, playtime, purchase dates, ratings, features, and enhanced metadata from the GamePieces system.

The script automatically locates the GOG Galaxy database, processes game data with proper title extraction (especially for non-GOG platforms like Amazon, Steam, Epic), and exports the data in multiple formats for analysis and backup purposes.

#### Features
- Exports to CSV, JSON, and Excel (.xlsx) formats with professional table formatting
- Extracts comprehensive game metadata including:
  - Game titles, descriptions, and platform information
  - Image URLs (background, square icon, vertical cover)
- Supports all platforms integrated with GOG Galaxy (GOG, Steam, Epic, Xbox, Amazon, etc.)
- Automatic database discovery with read-only access for safety
- Professional Excel export with formatted tables and auto-adjusted columns
- Game data consolidation to merge duplicate entries across platforms
- Command-line interface with flexible export format selection

#### Usage (Examples)
```bash
# Export to both JSON and CSV (default)
python gog_galaxy_exporter.py

# Export to Excel only
python gog_galaxy_exporter.py xlsx

# Export to all formats
python gog_galaxy_exporter.py all

# Export to CSV only
python gog_galaxy_exporter.py csv
```

#### Requires
- openpyxl (optional, for Excel export functionality)

### gog_csv_to_html.py
Converts GOG Galaxy CSV export files into a modern, interactive HTML game library viewer, with rich media integration and optional AI-powered game analysis (axis scoring, clustering, similarity recommendations via Ollama).

See [gog_csv_to_html/README.md](gog_csv_to_html/README.md) for features, usage examples, AI setup, and requirements.

### file_grouper.py
Organizes files in a directory by grouping them based on filename pattern matching (e.g. TV episodes, multi-part archives), with MyAnimeList integration for anime metadata and watch-status tracking. Part of the [common](common/README.md) module.

See [common/README.md](common/README.md#file_grouperpy) for features and requirements.

### series_completeness_checker.py
Analyzes TV series collections to identify missing episodes, season gaps, and incomplete series, using MyAnimeList as the primary source for anime metadata and episode counts.

See [series_completeness_checker/README.md](series_completeness_checker/README.md) for features, usage examples, and requirements.

### series_archiver.py
A script that archives anime series files based on series completeness checker output. It organizes files into structured folders with standardized naming patterns and provides both command-line interface and programmatic access for integration with other tools.

The script processes JSON output from series_completeness_checker.py and allows users to select specific series groups for archiving. It creates organized folder structures following the pattern `[release_group] show_name (start_ep-last_ep) (resolution)` and can either copy or move files to the destination. It also includes a standalone integrity-check workflow for validating archived or source files.

Enhanced with **MyAnimeList** integration as the primary source for anime information, providing accurate series metadata, episode counts, and watch status data. Supports public MyAnimeList lists and exported lists for comprehensive watch status tracking during the archiving process.

#### Features
- Dual Interface: Command-line tool and importable Python class for programmatic use
- MyAnimeList Integration: Primary source for anime metadata and series validation
- Watch Status Support: Integration with public or exported MyAnimeList lists
- Intelligent Organization: Creates standardized folder names based on series metadata
- Flexible Selection: Archive specific series or all series with simple selection syntax
- Safe Operations: Dry-run mode to preview changes before execution
- Integrity Validation: `check-crc` command and archive-time `--verify-crc` support for validating files by filename CRC or torrent-backed piece verification
- Torrent-Backed Recovery: Optional in-situ torrent-piece repair workflow for damaged files without filename CRCs when matching `.torrent` metadata is available
- Shared Progress Reporting: Tqdm-aware status output for file operations, integrity checks, torrent verification, and torrent repair
- Comprehensive Logging: Multiple verbosity levels for detailed operation tracking
- File Operations: Support for both copying and moving files with progress feedback
- Error Handling: Robust error reporting and validation of source files and destinations

#### Usage (Examples)
```bash
# Get help for specific commands
python series_archiver.py list --help
python series_archiver.py archive --help
python series_archiver.py check-crc --help

# List available series (basic)
python series_archiver.py list --input-json files.json

# List series with detailed information
python series_archiver.py -v list --input-json files.json

# Archive specific series (move files)
python series_archiver.py archive --input-json files.json --destination /dest/path --select "1,3,5"

# Archive all series (copy instead of move)
python series_archiver.py archive --input-json files.json --destination --select "all" --copy

# Preview what would happen (dry run)
python series_archiver.py archive --input-json files.json --destination --select "1,2" --dry-run

# Verify files after archiving and enable torrent-backed repair for failures
python series_archiver.py -v archive files.json /dest/path --select "all" --verify-crc --recover-by-torrent --torrent-files-path /path/to/torrents

# Check integrity for groups from JSON
python series_archiver.py check-crc --input-json files.json --select "185"

# Check integrity for individual files
python series_archiver.py check-crc --files episode01.mkv episode02.mkv

# Check integrity and allow torrent-backed repair after the scan completes
python series_archiver.py -v check-crc --input-json files.json --select "185" --recover-by-torrent --torrent-files-path /path/to/torrents

# Skip the repair confirmation prompt
python series_archiver.py check-crc --input-json files.json --select "185" --recover-by-torrent --auto-repair-by-torrent --torrent-files-path /path/to/torrents
```

#### Programmatic Usage
```python
from series_archiver import SeriesArchiver

# Initialize archiver with verbosity
archiver = SeriesArchiver(verbose=1)

# Load series data
archiver.load_data('files.json')

# List available groups
groups = archiver.list_groups(show_details=True)

# Archive selected groups
results = archiver.archive_groups(
    selected_groups=['group1', 'group2'], 
    destination_root='/dest/path',
    copy_files=True,
    dry_run=True,
    verify_crc=True,
    torrent_recovery={
        'enabled': True,
        'auto_repair': False,
        'torrent_files_path': '/path/to/torrents',
        'timeout_seconds': 120,
    },
)

# Or run a standalone integrity check
crc_results = archiver.check_files_crc(['/path/to/file1.mkv', '/path/to/file2.mkv'])
```

#### Commands
- **list/ls**: Display available series groups with episode counts and completion status
- **archive**: Archive selected series groups to organized destination folders
- **check-crc/crc**: Validate files by filename CRC when present, otherwise by matching torrent piece data when torrent recovery metadata is enabled

#### Options
- **-v, --verbose**: Increase verbosity level (use -v, -vv, or -vvv for different detail levels)
- **archive --select**: Specify which series to archive using comma-separated numbers or `all`
- **archive --copy**: Copy files instead of moving them (preserves originals)
- **archive --dry-run**: Show what would be done without actually performing file operations
- **archive --verify-crc**: Validate processed destination files after archiving
- **check-crc --input-json / --files**: Choose whether integrity checks operate on grouped JSON input or explicit file paths
- **check-crc --select**: Restrict JSON-backed integrity checks to selected group numbers or `all`
- **--recover-by-torrent**: Enable torrent-backed verification/recovery flows
- **--auto-repair-by-torrent**: Skip the confirmation prompt before modifying failed files
- **--torrent-files-path DIR**: Directory containing `.torrent` files used for matching and repair
- **--torrent-recovery-timeout SECONDS**: Maximum time to wait for torrent-backed verification or repair

#### Notes
- `--recover-by-torrent` requires `--torrent-files-path` and the optional `libtorrent` package.
- On the `archive` command, `--recover-by-torrent` also requires `--verify-crc`.
- Torrent-backed checks follow a two-step flow: first the scan validates integrity, then repair is offered only after the scan completes.
- Torrent-backed verification and repair are piece-based and can reuse already valid local torrent pieces during in-situ repair.

#### Requires
- tqdm (for progress bars)

#### Optional Dependencies
- libtorrent (required for `--recover-by-torrent`)
- bencodepy (optional torrent metadata parser fallback)

### series_bundler.py
A script that groups series files and creates organized folder structures for archiving. It analyzes video files using guessit to extract metadata, groups them by series, release group, and resolution, then creates standardized folder names following the pattern `[Release Group] Series Name (YYYY) (xx-yy) (Resolution)`.

The script is designed to bundle anime/TV series episodes into organized folders suitable for long-term archiving. It handles various filename patterns, supports both drag-and-drop and command-line usage, and can process decimal episode numbers (like episode 12.5) and series with years in the title.

#### Features
- **Intelligent Metadata Extraction**: Uses guessit to parse filenames and extract series information
- **Flexible Episode Handling**: Supports regular episodes, decimal episodes (12.5), and handles year misclassification
- **Dual Interface**: Interactive drag-and-drop mode and full command-line interface
- **Smart Grouping**: Groups files by series, release group, resolution, and season
- **Standardized Naming**: Creates consistent folder names for archival organization
- **Preview Mode**: Dry-run functionality to preview changes before execution
- **Progress Tracking**: Visual progress bars and detailed logging options
- **File Operations**: Support for both copying and moving files with error handling

#### Usage (Examples)
```bash
# Drag-and-drop mode (interactive) - just drag files onto the script
python series_bundler.py file1.mkv file2.mkv file3.mkv

# Analyze files in current directory (preview only)
python series_bundler.py . --summary-only

# Bundle files to destination (dry run preview)
python series_bundler.py /path/to/series -d /path/to/archive --dry-run

# Actually move files to organized folders
python series_bundler.py /path/to/series -d /path/to/archive

# Copy files instead of moving them
python series_bundler.py /path/to/series -d /path/to/archive --copy

# Process files recursively with verbose output
python series_bundler.py /path/to/library -d /path/to/archive --recursive -vv

# Disable interactive mode for scripting
python series_bundler.py *.mkv -d /dest --no-interactive
```

#### Requires
- guessit
- tqdm

### simple_http_proxy.py
A simple script that allows you to fetch a remote file or resource by appending the full remote URL to the end of your request to the app, e.g.:
``` bash
http://<host-ip>:8080/<remote-url>
```
This is **not** a real HTTP proxy, but a tool to fetch a specific file or similar resource via another URL, making it accessible to local computers on your network.

#### Requires
- No external dependencies required (uses only Python standard libraries)

### socks5_http_tunneler.py
A one-file local HTTP tunneler that accepts a target URL via query string and forwards upstream traffic through a configured SOCKS5 proxy. It rewrites redirected and embedded upstream URLs into local proxy paths so navigation continues through the local endpoint while upstream requests still look native to the target host.

#### Features
- Accepts entry URLs in the form `http://localhost:8080/?url=http://example.com`
- Forwards both HTTP and HTTPS upstream requests through a user-supplied SOCKS5 proxy
- Rewrites `Location` headers, common HTML URL attributes, CSS `url(...)` references, `srcset`, and meta refresh redirects into local proxy paths
- Injects a small client-side shim so browser-side `fetch`, `XMLHttpRequest`, `history`, `window.open`, and form submissions continue using the local tunnel
- Supports verbose `--debug` logging for request resolution, upstream headers, response metadata, and rewrite previews
- Supports strict upstream TLS verification by default, optional private CA trust with `--ca-bundle`, or explicit bypass with `--insecure`

#### Usage Examples
```bash
# Start the tunnel through a SOCKS5 proxy
python socks5_http_tunneler.py --socks5 127.0.0.1:1080

# Enable debug logging
python socks5_http_tunneler.py --socks5 socks5://127.0.0.1:1080 --debug

# Trust a private proxy root CA bundle for upstream HTTPS
python socks5_http_tunneler.py --socks5 socks5://127.0.0.1:1080 --ca-bundle C:\path\to\proxy-root-ca.pem

# Disable upstream HTTPS verification entirely
python socks5_http_tunneler.py --socks5 socks5://127.0.0.1:1080 --insecure
```

#### Requires
- requests with SOCKS support: `pip install "requests[socks]"`

### radio_station_checker.py
A multi-threaded Python script that checks the availability of radio stations from SII, PLS, and M3U playlist files. It provides a real-time terminal interface with virtualized rendering for performance, displaying station status, response times, and metadata in an organized table format.

The script features intelligent performance optimizations, only updating the display when necessary, and includes full user interaction capabilities with keyboard navigation and graceful shutdown handling. It's designed to efficiently monitor large collections of radio stations while providing a smooth, responsive user experience.

#### Features
- **Multi-format Support**: Handles Truck Simulator SII, PLS, and M3U playlist formats
- **High-Performance Virtualized Rendering**: Smart display updates only when station status changes or user interactions occur
- **Multi-threaded Station Checking**: Concurrent HTTP requests with configurable thread pool for optimal performance
- **Interactive Terminal Interface**: Rich terminal UI with real-time status updates and progress tracking

#### Requires
- rich
- requests
- concurrent.futures (standard library)

### latest_episodes_viewer.py
Generates a simple HTML page listing the latest episodes from a collection of TV series, based on metadata extracted via guessit and custom metadata providers.

See [latest_episodes_viewer/README.md](latest_episodes_viewer/README.md) for features and requirements.

### serve_local.py
Serve a file or folder as a simple local webhost with optional live-reload and a small file-proxy helper.

#### Features
- Serve a single file as the site index or serve a whole folder (directory listing when no `index.html`).
- Sandbox served paths to the configured web root; blocks requests outside that root.
- Optional forced-index `--live` mode with Server-Sent-Events (SSE) live-reload for the chosen index file.
- `/file-proxy` endpoint to allow pages served over HTTP to fetch local `file:///` resources (restricted to webroot and the user's home on Windows).
- Windows GUI helper to pick a file/folder when no path is provided (Tkinter-based).

#### Usage
```bash
# Drag-and-drop a file or folder onto the script (Windows)
python serve_local.py "C:\path\to\file_or_folder" [-p PORT] [--live]
```

#### Key functions & classes
- `get_local_ip()` — returns a likely LAN IP address for advertising the server on the local network.
- `CustomHandler` — subclass of `SimpleHTTPRequestHandler` that implements forced index handling, SSE `/__watch`, `/file-proxy`, richer directory listings, and sandbox enforcement.
- `_watch_file_for_changes(path)` — background watcher that notifies connected SSE clients when the forced index file changes.
- `choose_path_interactive()` — Windows file/folder chooser (uses Tkinter) when no path argument is provided.
- `main()` — CLI entrypoint that configures logging, determines web root, optionally starts the watcher, and runs a threaded HTTP server.

## Bash Scripts

### macos-uninstall-python3.10.sh
Safe uninstall script for Python 3.10 installed via the official python.org macOS package installer. It performs sanity checks, lists matching framework/app/symlink targets, asks for confirmation, removes matching files, and forgets matching pkgutil receipts.

#### Usage
```bash
# Make executable
chmod +x macos-uninstall-python3.10.sh

# Run
./macos-uninstall-python3.10.sh
```

## Experimental

### lyrics-timing-generator.py
Intended to generate timed lyrics for audio files (.lrc). Uses whisper library to generate timed lyrics, and ollama and an llm to structure them.

But it is not good. Really not good. It's a start, but not quite there yet. Need to restart from a known base to generate the timed subtitles which is a known working thing, and then convert that to lyrics, using an llm to format them.

#### Requires
- demucs
- librosa
- mutagen
- numpy
- requests
- soundfile
- torch
- tqdm
- whisperx

