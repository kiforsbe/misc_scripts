# misc_scripts
Miscellaneous scripts to automate common tasks.

## User Scripts
These user scripts enhance the webservice functionality by integrating download buttons directly into the respective web interfaces. They automatically capture song metadata and cover art, then send this information to the webservice for processing, making the download process seamless and efficient. They have been tested with Tampermonkey on Chrome.
- `udio-download_ext-button.user.js`: Adds a "Download with metadata" button to Udio song pages
- `riffusion-download_ext-button.user.js`: Adds a "Download with metadata" button to Riffusion song pages

### plex-playlist-watch-status.user.js
A Tampermonkey script that adds simple triangle indicators to Plex playlist items, showing their watch status (watched/unwatched) based on metadata from the Plex API.
It fetches the watch status of each item in a Plex playlist and displays a triangle icon next to each item, indicating whether it has been watched or not. The script is designed to enhance the user experience by providing quick visual feedback on the watch status of playlist items.

## Libraries

### browser_utils.py
A cross-platform browser launcher library with support for different window modes. Provides functionality to open URLs in the default browser with control over window size, position, and behavior. Used by other scripts to provide consistent browser interaction across platforms.

#### Features
- Automatic detection of default browser on Windows, macOS, and Linux
- Multiple window modes:
  - **Default**: Opens in new tab or reuses existing window
  - **New Window**: Opens in a new browser window
  - **Popup**: Chromeless window, half screen width, centered
  - **Maximized**: Full screen window
- Custom window size and position support
- Platform-specific optimizations for Chrome, Firefox, Edge, Safari
- Screen dimension detection for popup positioning

#### API
- `BrowserLauncher` class with methods:
  - `get_screen_dimensions()`: Detect primary screen size
  - `get_browser_command()`: Find default browser executable
  - `open_urls(urls, new_window, popup, maximized, window_size, window_position)`: Open URLs with specified mode
- Convenience function: `open_urls_in_browser(urls, ...)`

#### Requires
- Platform-specific: winreg (Windows), ctypes (Windows)

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
- Browser window modes via browser_utils:
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
- file_grouper (local module)
- browser_utils (local module)

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
- speechbrain
- scikit-learn
- numpy

### insanely-fast-whisper.py
Minimalistic script to generate transcription using Whisper.

#### Requires
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
- eyed3

### clipboard-monitor.py
Monitors the clipboard for changes and appends the contents to "`clipboard.csv`" file. It plays a sound when a change is detected and saved to the file.

#### Requires
Windows 10 or Windows 11.
- winsound
- win32clipboard

### file-renamer-script.py
Takes a "`clipboard.csv`" file as input and uses the first column as a file_id that it tries to find in the files in the same folder as the script. If it finds the file, it is added to the list, with the proposed filename in the second column.

It then outputs the full list of proposed changes as file "`rename_mappings.csv`", so the user can verify the changes.

If the user approves them, the user simply responds "y", or "yes" or presses enter to confirm that they want to rename the files in the folder with the proposed filenames.

#### Requires
- csv
- logging
- traceback

### file_metadata_scanner.py
A comprehensive tool for extracting metadata from files and folders with support for various file types. Scans directories recursively or non-recursively, extracts basic file information (size, timestamps, attributes) and optional extended metadata (audio/video properties via ffmpeg, image dimensions, comic archive contents), and exports results to CSV, JSON, and an interactive HTML webapp. Supports thumbnail generation for video files and provides flexible filtering options.

#### Features
- Recursive and non-recursive directory scanning with progress tracking
- Basic metadata extraction:
  - File/directory name, type, size (bytes and human-readable)
  - Timestamps (created, modified, accessed)
  - File attributes (hidden, readonly, system)
  - File extensions
- Extended metadata extraction (optional):
  - **Video/Audio**: Duration, bitrate, codec, resolution, frame rate, audio channels, sample rate
  - **Images**: Dimensions, format, color mode, DPI
  - **Comic Archives (CBR/CBZ)**: Page count, image formats, dimensions
- Thumbnail generation for video files using `video_thumbnail_generator`:
  - Static thumbnails (3x3 grid of frames)
  - Animated WEBM thumbnails
  - Configurable minimum duration filter
  - Batch processing with progress tracking
- Flexible filtering and exclusion:
  - Filter by file extensions (e.g., only .mp4, .mkv)
  - Exclude specific paths or directories
- Multiple export formats:
  - **CSV**: Tabular data for spreadsheet analysis
  - **JSON**: Structured data for programmatic use
  - **HTML Webapp**: Standalone interactive file explorer with search, filtering, and thumbnail viewing
- Customizable export location for metadata bundles
- Regenerate webapp from existing metadata without rescanning
- CBR processing can be skipped to improve performance (RAR extraction is slow)

#### Usage Examples
```bash
# Basic scan of current directory (exports to ./metadata/)
python file_metadata_scanner.py .

# Recursive scan with custom export location
python file_metadata_scanner.py /path/to/folder -r --export-bundle /output/location

# Scan only video files with extended metadata and thumbnails
python file_metadata_scanner.py /path/to/videos -r -e mp4,mkv,avi --extended --thumbnails

# Exclude specific paths (node_modules, cache directories, etc.)
python file_metadata_scanner.py /path/to/folder -r --exclude node_modules,__pycache__,.git

# Full scan with all features and custom export location
python file_metadata_scanner.py /path/to/media -r --extended --thumbnails --export-bundle C:\MyMetadata

# Skip slow CBR processing, only process CBZ comic archives
python file_metadata_scanner.py /path/to/comics -r --extended --skip-cbr

# Set minimum video duration for thumbnail generation (e.g., 10 minutes)
python file_metadata_scanner.py /path/to/videos -r --thumbnails --min-duration 600

# Regenerate webapp from existing metadata bundle
python file_metadata_scanner.py --regenerate-bundle /path/to/bundle

# Regenerate webapp with missing thumbnails
python file_metadata_scanner.py --regenerate-bundle /path/to/bundle --thumbnails

# Verbose logging for troubleshooting
python file_metadata_scanner.py /path/to/folder -r --extended --log-level DEBUG
```

#### Requires
- tqdm (for progress bars)
- Pillow (for image metadata extraction)
- video_thumbnail_generator (local module, for thumbnail generation)
- ffmpeg and ffprobe (system binaries, for extended video/audio metadata)
- libarchive-c or rarfile (for CBR comic archive extraction)
  - libarchive-c (preferred): Requires libarchive DLL
  - rarfile (fallback): Requires UnRAR tool on PATH

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

### merge-audio-files-to-one-output.py
Simple merge a bunch of audio files into one single output file. Just drag all the input files onto the script and it will be output in the same folder as the first file with the name "`combined_output.<ext>`". The script will ask what format, bitrate etc the output shall get.

#### Requires
- pydub
- inquirer
- tqdm

### udio-flask-webservice.py (udio-download_ext-button.user.js)
A flask web service that adds metadata including cover art to your song files downloaded from Udio. It comes with a user script (e.g. Tampermonkey) that simplifies this process by adding a new button to the song pages "Download with metadata" that calls the webservice.
This webservice now also supports Riffusion and .m4a audio files.

The webservice exposes the following interfaces:

| Interface | Methods | Functions | Parameters |
| --- | --- | --- | --- |
| /api/download_ext | POST & GET | download_ext | mp3_url, image_url, title, artist, album, genre, year, cannonical, lyrics |

#### /api/download_ext
Downloads the specified `.mp3` file and adds the provided metadata to it.

| Parameter | Tag | Description |
| --- | --- | --- |
| mp3_url | ***Not used*** | The URL of the `.mp3` file to be converted. |
| image_url | Images (Cover) | The URL of the cover art in .jpg format to use. |
| title | Title | The title of the track. |
| artist | Artist | Artist name(s) and/or alias(es). |
| album | Album | The title of the album. |
| genre | Genre | The genre of the track. |
| year | Year | Year of release. |
| cannonical | WWWAUDIOFILE | The source url of the track where it can be found permanently. |

#### Requires
- flask
- requests
- python-magic-bin
- audio_metadata
- bidict
- importlib-resources
- moviepy
- eyed3
- ffmpeg
- pillow

#### Optional Dependencies
- music_style_classifier.py
  - librosa
  - tensorflow
  - numpy
  - transformers

### video-optimizer-v2
A script that allows for quick and easy optimization of videos. Just supply a list of videos on the command line or drag and drop them onto the script. You get a list of choices based on the contents of the videos such as which subtitles to make default, and which audio to make default along with target quality and resolution.

It is made specifically for transcoding for example tv-shows from your legacy media in a quick and simple way. Just drag a whole season onto the script and easily convert it for use on your phone.

It now also supports lookup of meta data from common anime databases and imdb. It will automatically download the metadata and add it to the video.

Check out branch mediaoptimizer_v1 for the old version.

#### Matadata Providers
##### metadata_cache_manager.py
CLI helper for the video-optimizer providers to inspect and control cache TTLs. Supports `status`, `refresh`, `invalidate`, and `set-expiry` (accepts long form like "3 days" or short form like `2m7d`). TTL is persisted per provider (IMDb, Anime) and `invalidate` forces TTL to 0 so cache refreshes on next access. Optional `--no-color` disables colored output.

##### metadata_provider.py
Base metadata provider class that defines the interface for metadata retrieval services. This abstract class provides a common structure for implementing different metadata sources, handling search functionality, and managing metadata formatting for video files.

##### anime_provider.py
Implements metadata retrieval from anime databases such as AniList and MyAnimeList. Provides specialized handling for anime series metadata including episode information, season data, air dates, and anime-specific details like studio information and Japanese titles.

##### imdb_provider.py
Implements metadata retrieval from the Internet Movie Database (IMDb). Handles both movies and TV series metadata including cast information, directors, release dates, ratings, and plot summaries. Integrates with IMDb's API or web scraping for comprehensive movie and TV show information.

##### plex_metadata.py
Implements metadata retrieval from Plex Media Server. Connects to a local or remote Plex server to fetch metadata for movies and TV shows stored in the Plex library. Retrieves details such as titles, descriptions, genres, ratings, and artwork associated with the media files.

#### Requires
Use the video-optimizer-v2/requirements.txt file to install the requirements.
- ffmpeg-python
- requests
- pandas
- tqdm
- rapidfuzz
- inquirer
- mutagen

### rss-feed-downloader.py
A script to parse RSS feeds and download enclosures (e.g., audio, video, or other files) with a console-based GUI for selection and progress tracking.

#### Features
- Parses RSS feeds from local files or URLs.
- Extracts enclosures and allows users to select files for download.
- Downloads files with progress tracking and optional HTTP Basic Authentication.
- Saves downloaded files to a specified directory and generates a mapping file in JSON format.

#### Requires
- curses
- urllib
- json

### youtube-video-downloader
A collection of youtube download scripts using the `ytdl_helper` library.
It includes a command-line interface and a text-based user interface (TUI) for downloading YouTube videos and audio. It also includes a Flask web service for downloading YouTube videos and audio via a web interface, and a user script for adding a download button to YouTube pages.
Now integrates with `music_style_classifier.py` to classify the music style of downloaded audio files.

#### ytdl_helper library
This library provides functionalities for downloading YouTube videos and audio efficiently. Users can fetch video information (metadata, available formats) and download content directly. The library supports various output formats (e.g., mp4, mp3) and allows users to specify desired resolution, audio bitrate, and target directory.
It is used by both the command-line and TUI scripts.

#### youtube-video-downloader-cli.py
A command-line tool for downloading YouTube videos and audio using the `ytdl_helper` library. It allows fetching video information (metadata, available formats) as JSON or downloading content directly. Users can specify desired resolution, audio bitrate, output format (e.g., mp4, mp3), and target directory via command-line arguments. Download progress is displayed using `tqdm` progress bars.

##### Usage (Examples)
```bash
# Get video info as JSON
python youtube-video-downloader-cli.py info "VIDEO_URL"

# Download best available video+audio (defaults to mp4)
python youtube-video-downloader-cli.py download "VIDEO_URL"

# Download audio only as mp3 to a specific directory
python youtube-video-downloader-cli.py download "VIDEO_URL" -a --format mp3 -o ./downloads

# Download 720p video (closest) with 192k audio (closest) as mkv
python youtube-video-downloader-cli.py download "VIDEO_URL" -r 720p -b 192k -f mkv
```
##### Requires
- ytdl_helper (and its dependencies, likely yt-dlp)
- tqdm
- ffmpeg (must be installed and in the system PATH)
- music_style_classifier.py

#### youtube-video-downloader-gui.py
A Text-based User Interface (TUI) built with urwid for downloading YouTube videos. It takes video URLs as command-line arguments, fetches their information asynchronously using ytdl_helper, and displays them in an interactive list. Users can select items, choose specific video and audio formats via a detailed dialog, and initiate downloads. The TUI shows status updates and progress bars for each item. Batch pre-selection of best audio or video is possible via command-line flags (--audio-only, --video).

##### Features
- Interactive TUI powered by urwid.
- Handles multiple URLs provided via command line.
- Displays video title, duration, status, and progress.
- Item selection using +/- keys.
- Detailed format selection dialog (Enter key) allowing choice of:
- Mode (Video+Audio, Video Only, Audio Only).
- Specific video streams (resolution, codec, etc.).
- Specific audio streams (bitrate, codec, etc.).
- Initiates downloads for selected items (d key).
- Real-time status and progress updates.
- Batch mode flags (--audio-only, --video) for quick downloads.
- Logs activity to logs/youtube_downloader.log.

##### Requires
- ytdl_helper (and its dependencies, likely yt-dlp)
- urwid
- ffmpeg (must be installed and in the system PATH)
- music_style_classifier.py
- **Note! (Windows specific):** ctypes (standard library, used for console setup)

#### youtube-video-downloader-flask-ws.py & youtube-video-downloader-user-script.js
A Flask web service that allows downloading YouTube videos and audio via a web interface. It accepts video URLs via POST requests, fetches metadata, and downloads the content. The service supports various output formats (e.g., mp4, mp3) and allows users to specify desired resolution, audio bitrate, and target directory. It returns download progress and status updates in JSON format. To use it, run the Flask web service and send POST requests with the video URL and desired parameters. The web service can be accessed via a user script (e.g., Tampermonkey) that adds a button to download videos directly from YouTube pages.
The user script can be installed in a browser extension like Tampermonkey, which allows users to add custom scripts to web pages. The script adds a button to YouTube video pages, enabling users to download videos directly from the page.

##### Usage (Examples)
```bash
# Start the Flask web service
python youtube-video-downloader-flask-ws.py

# Send a POST request to download a video
curl -X POST -H "Content-Type: application/json" -d '{"url": "VIDEO_URL", "format": "mp4", "resolution": "720p", "audio_bitrate": "192k", "output_dir": "./downloads"}' http://localhost:5000/download
```

##### Features
- Accepts video URLs via POST requests.
- Fetches metadata and downloads content in various formats.
- Provides download progress and status updates in JSON format.
- Supports output formats (e.g., mp4, mp3) and allows users to specify desired resolution, audio bitrate, and target directory.
- Returns download progress and status updates in JSON format.
- Logs activity to logs/youtube_downloader.log.

##### Requires
- Flask
- ytdl_helper (and its dependencies, likely yt-dlp)
- ffmpeg (must be installed and in the system PATH)
- music_style_classifier.py

### music_style_classifier.py
A script that takes as imput an audio/video file to classify the music style of the file. It uses a pre-trained model to classify the music style and outputs the result.
It is intended to be used as a command line tool, but it can also be used as a library (get_music_genre(file_path: str, track_index: int = None) -> str | None:).

#### Requires
- librosa
- tensorflow
- numpy
- ffmpeg
- transformers

### md_to_docx.py
A script that converts Markdown files to Microsoft Word DOCX format. It processes Markdown content by first converting it to HTML using mistletoe, then parsing the HTML with BeautifulSoup to create properly formatted Word documents. The converter handles various Markdown elements including headings, paragraphs, lists (ordered and unordered with nesting), tables, bold/italic text, code blocks, links, and blockquotes. Tables are automatically formatted with proper styling and column widths.

The script can be used from the command line by specifying an input Markdown file and optionally an output DOCX file. If no output filename is provided, it will generate one based on the input filename and avoid overwriting existing files.

#### Usage (Examples)
```bash
# Convert README.md to README.docx
python md_to_docx.py README.md

# Convert with specific output filename
python md_to_docx.py input.md output.docx
```

#### Features
- Converts Markdown to properly formatted Word documents
- Handles headings (H1-H9), paragraphs, lists, and tables
- Supports inline formatting (bold, italic, code)
- Processes nested lists with appropriate indentation
- Formats tables with automatic column sizing and styling
- Generates debug HTML file for troubleshooting
- Automatic output filename generation to avoid overwriting

#### Requires
- mistletoe
- beautifulsoup4
- python-docx

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
A Python script that converts GOG Galaxy CSV export files into a modern, interactive HTML game library viewer. It creates a responsive web application with game browsing, filtering, search functionality, rich media integration, and advanced AI-powered game analysis including clustering visualization and similarity recommendations.

The script automatically fetches additional media content from online sources and caches it locally for improved performance. It provides a professional game library interface similar to modern gaming platforms, with detailed game information, ratings, playtime tracking, visual elements, and intelligent game analysis features.

#### Features
- **Modern Interactive Interface**: Responsive React-based web application with dual-pane layout
- **Rich Media Integration**:
  - Automatic YouTube trailer and gameplay video embedding
  - Game screenshot galleries with carousel navigation and modal view
  - Cover art and background images from game metadata
- **Advanced Filtering & Search**:
  - Real-time search across titles, descriptions, genres, and tags
  - Filter by played/unplayed games and recently played titles
  - Platform-based filtering and sorting options
- **Game Information Display**:
  - Comprehensive game details including playtime, ratings, release dates
  - Developer/publisher information and genre classifications
  - Platform badges and compatibility information
  - Purchase and last played date tracking
- **AI-Powered Game Analysis** (Enhanced):
  - **14-axis game scoring system** using Ollama and deepseek-r1 model for analyzing:
    - Core mechanics complexity and count
    - Player agency and world impact
    - Narrative density and integration
    - Scope, pacing, and replayability
    - Technical execution and aesthetics
  - **Interactive cluster visualization** using t-SNE and K-means clustering
  - **Game similarity recommendations** based on comprehensive vector analysis
  - **Visual axis comparison** in compact grid format for quick game assessment
- **Machine Learning Features**:
  - **MiniLM text embeddings** for semantic game similarity analysis
  - **Hybrid vector space** combining structured axis data with semantic embeddings
  - **Real-time clustering** with meaningful cluster naming and analysis
  - **Intelligent game recommendations** using euclidean distance in high-dimensional space
- **Performance Optimizations**:
  - Local SQLite database for media content and axis scoring caching
  - React virtualization for smooth scrolling of large game libraries
  - Lazy loading of images and content
  - Standardized vector preprocessing for improved clustering results
- **Professional Presentation**:
  - Modern gradient backgrounds and card-based layouts
  - Star rating displays and playtime formatting
  - Responsive design for desktop and mobile viewing
  - Bootstrap-based styling with custom enhancements
- **Game Analysis Integration** (Optional):
  - AI-powered game axis scoring using Ollama and deepseek-r1 model
  - 14-axis game comparison system for analyzing game mechanics, narrative, and design
  - Cached scoring results for improved performance on repeated runs

#### Usage (Examples)
```bash
# Convert CSV to HTML with full AI analysis (recommended)
python gog_csv_to_html.py gog_export.csv

# Convert with custom output filename
python gog_csv_to_html.py gog_export.csv -o my_game_library.html

# Skip media fetching for faster processing (disables AI features)
python gog_csv_to_html.py gog_export.csv --no-media

# Disable media caching
python gog_csv_to_html.py gog_export.csv --no-cache

# Open result in browser automatically
python gog_csv_to_html.py gog_export.csv --open

# Show cache statistics including AI analysis data
python gog_csv_to_html.py --cache-stats

# Use custom Ollama host for AI analysis
python gog_csv_to_html.py gog_export.csv --ollama-host http://192.168.1.100:11434
```

#### AI Analysis Features
The script now includes sophisticated AI-powered game analysis:

- **Axis Scoring**: Each game is analyzed across 14 dimensions using the deepseek-r1 model
- **Cluster Analysis**: Games are automatically grouped using machine learning clustering algorithms
- **Similarity Engine**: Find games similar to your favorites using hybrid semantic + structured analysis
- **Visual Analytics**: Interactive t-SNE plots show your game library's structure and patterns

#### Template Dependency
- **gog_csv_to_html_template.html**: Jinja2 template file containing the HTML structure, CSS styling, and React-based JavaScript application with ML clustering features. Must be present in the same directory as the Python script.
- **gog_csv_to_html_template.css**: CSS styling file with responsive design and clustering modal styles.

#### Requires
- requests
- beautifulsoup4
- jinja2
- **ollama** (for AI-powered game axis scoring - requires deepseek-r1 model)
- **pydantic** (for structured output validation with Ollama)

##### Optional Dependencies
- **Image Processing**: gzip, zlib, brotli (for handling compressed web responses)
- **Web Browser Integration**: webbrowser (standard library, for --open flag)
- **AI Game Analysis**: Ollama server with deepseek-r1 model (for axis scoring and clustering features)

#### Setup for AI Features
To enable the full AI analysis capabilities:

1. **Install Ollama**: Download from [ollama.ai](https://ollama.ai)
2. **Install deepseek-r1 model**: `ollama pull deepseek-r1`
3. **Start Ollama server**: `ollama serve`
4. **Run with AI features**: `python gog_csv_to_html.py your_games.csv`

The script will automatically detect Ollama availability and enable advanced features when the server and model are accessible.

### file_grouper.py
A script that organizes files in a directory by grouping them based on their filenames using intelligent pattern matching. It identifies files that belong together (such as episodes of a TV series, parts of a multi-part archive, or related documents) and creates subdirectories to organize them logically.

The script uses advanced string matching algorithms to detect patterns in filenames, handle various naming conventions, and group related files while preserving the original file structure. It's particularly useful for organizing large collections of media files, software downloads, or document archives.

Now integrates with **MyAnimeList** as the primary source for anime information, providing enhanced metadata and series validation. Supports public MyAnimeList lists and exported lists for tracking watch status and completion data.

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

### series_completeness_checker.py
A script that analyzes TV series collections to identify missing episodes, gaps in seasons, and incomplete series. It scans directory structures and filenames to build a comprehensive view of your media library, highlighting what episodes or seasons might be missing from your collection.

The script supports various TV series naming conventions and provides detailed reports on series completeness, making it easy to identify and fill gaps in your media collection. It can also suggest potential naming inconsistencies and provide recommendations for organizing your series.

Now features **MyAnimeList** as the primary source for anime information, providing accurate episode counts, season data, and series metadata. Supports integration with public MyAnimeList lists and exported lists to track watch status and completion progress.

#### Features
- Comprehensive TV series analysis and gap detection
- Support for multiple naming conventions and formats
- MyAnimeList integration for accurate anime metadata and episode validation
- Watch status integration with public or exported MyAnimeList lists
- Season and episode numbering validation
- Missing episode identification with detailed reporting
- Series metadata integration for enhanced accuracy
- Export results to various formats (JSON, HTML reports)
- Integration with metadata providers for series validation
- Batch processing of multiple series directories

#### Usage (Examples)
```bash
# Check completeness of series in current directory
python series_completeness_checker.py

# Generate JSON report
python series_completeness_checker.py /path/to/series --export series.json

# Generate HTML webapp
python series_completeness_checker.py /path/to/series --webapp-export series.html
```

#### Requires
- rapidfuzz
- requests
- pandas
- pathlib

### series_archiver.py
A script that archives anime series files based on series completeness checker output. It organizes files into structured folders with standardized naming patterns and provides both command-line interface and programmatic access for integration with other tools.

The script processes JSON output from series_completeness_checker.py and allows users to select specific series groups for archiving. It creates organized folder structures following the pattern `[release_group] show_name (start_ep-last_ep) (resolution)` and can either copy or move files to the destination.

Enhanced with **MyAnimeList** integration as the primary source for anime information, providing accurate series metadata, episode counts, and watch status data. Supports public MyAnimeList lists and exported lists for comprehensive watch status tracking during the archiving process.

#### Features
- Dual Interface: Command-line tool and importable Python class for programmatic use
- MyAnimeList Integration: Primary source for anime metadata and series validation
- Watch Status Support: Integration with public or exported MyAnimeList lists
- Intelligent Organization: Creates standardized folder names based on series metadata
- Flexible Selection: Archive specific series or all series with simple selection syntax
- Safe Operations: Dry-run mode to preview changes before execution
- Comprehensive Logging: Multiple verbosity levels for detailed operation tracking
- File Operations: Support for both copying and moving files with progress feedback
- Error Handling: Robust error reporting and validation of source files and destinations

#### Usage (Examples)
```bash
# Get help for specific commands
python series_archiver.py list --help
python series_archiver.py archive --help

# List available series (basic)
python series_archiver.py list files.json

# List series with detailed information
python series_archiver.py -v list files.json
python series_archiver.py -vv list files.json

# Archive specific series (move files)
python series_archiver.py archive files.json /dest/path --select "1,3,5"

# Archive all series (copy instead of move)
python series_archiver.py archive files.json /dest/path --select "all" --copy

# Preview what would happen (dry run)
python series_archiver.py archive files.json /dest/path --select "1,2" --dry-run

# Verbose archiving with copy and dry run
python series_archiver.py -vv archive files.json /dest/path --select "all" --copy --dry-run
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
    dry_run=True
)
```

#### Commands
- **list/ls**: Display available series groups with episode counts and completion status
- **archive**: Archive selected series groups to organized destination folders

#### Options
- **-v, --verbose**: Increase verbosity level (use -v, -vv, or -vvv for different detail levels)
- **--select**: Specify which series to archive using comma-separated numbers or "all"
- **--copy**: Copy files instead of moving them (preserves originals)
- **--dry-run**: Show what would be done without actually performing file operations

#### Requires
- pathlib
- shutil
- json

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

#### Usage
1. Install Tampermonkey or a similar userscript manager in your browser.
2. Import the script into Tampermonkey.
3. Make sure your local IP addresses are whitelisted in the script.
4. Navigate to your Plex playlist page, open a playlist and see the watch status indicators appear next to each item thumbnail.

#### Requires
- Tampermonkey or a similar userscript manager

### latest_episodes_viewer.py
A script that generates a simple HTML page listing the latest episodes from a collection of TV series. It scans a specified directory for video files, extracts metadata using guessit and some custom metadata providers, and creates an organized list of the most recent episodes based on their air dates. The generated HTML page includes links to the episodes, making it easy to access and view the latest content.

#### Features
- Scans a specified directory for video files
- Extracts metadata using guessit and custom metadata providers
- Generates an organized HTML page listing the latest episodes
- Includes links to the episodes for easy access

#### Requires
- guessit
- requests
- tqdm

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

## Experimental

### lyrics-timing-generator.py
Intended to generate timed lyrics for audio files (.lrc). Uses whisper library to generate timed lyrics, and ollama and an llm to structure them.

But it is not good. Really not good. It's a start, but not quite there yet. Need to restart from a known base to generate the timed subtitles which is a known working thing, and then convert that to lyrics, using an llm to format them.

#### Requires
- pydub
- mutagen
- numpy
- librosa
- tensorflow
- spleeter
- soundfile
- whisper
- tqdm
- dataclasses

### mini-dlna-server.py
A script that runs a local dlna server instance on the computer, taking as input a command line argument pointing out the folder to serve to clients.

It is intended if I get time to do it, to stabilize it to properly handle conenctions, work better with Windows 11, and transcode media to the client using ffmpeg.

**Note!** Currently it is extremely unstable and mostly doesn't work. If anyone wants to refactor it and fix some of the remaining issues that would be cool. :)

#### Requires
- mutagen
