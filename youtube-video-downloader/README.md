# youtube-video-downloader

A collection of youtube download scripts using the `ytdl_helper` library.
It includes a command-line interface and a text-based user interface (TUI) for downloading YouTube videos and audio. It also includes a Flask web service for downloading YouTube videos and audio via a web interface, and a user script for adding a download button to YouTube pages.
Now integrates with `common/music_style_classifier.py` to classify the music style of downloaded audio files.

Each script can also be run as a module from the repo root. `python -m youtube-video-downloader` defaults to the CLI; run the GUI or Flask webservice with their explicit module path (e.g. `python -m youtube-video-downloader.youtube-video-downloader-gui`).

## Project Files

### ytdl_helper library
This library provides functionalities for downloading YouTube videos and audio efficiently. Users can fetch video information (metadata, available formats) and download content directly. The library supports various output formats (e.g., mp4, mp3) and allows users to specify desired resolution, audio bitrate, and target directory.
It is used by both the command-line and TUI scripts.

### youtube-video-downloader-cli.py
A command-line tool for downloading YouTube videos and audio using the `ytdl_helper` library. It allows fetching video information (metadata, available formats) as JSON or downloading content directly. Users can specify desired resolution, audio bitrate, output format (e.g., mp4, mp3), and target directory via command-line arguments. Download progress is displayed using `tqdm` progress bars.

#### Usage (Examples)
```bash
# Get video info as JSON (python -m youtube-video-downloader is the __main__ default, i.e. the CLI)
python -m youtube-video-downloader info "VIDEO_URL"

# Download best available video+audio (defaults to mp4)
python -m youtube-video-downloader download "VIDEO_URL"

# Download audio only as mp3 to a specific directory
python -m youtube-video-downloader download "VIDEO_URL" -a --format mp3 -o ./downloads

# Download 720p video (closest) with 192k audio (closest) as mkv
python -m youtube-video-downloader download "VIDEO_URL" -r 720p -b 192k -f mkv
```

Can also be run directly: `python youtube-video-downloader/youtube-video-downloader-cli.py ...`

#### Requires
- ytdl_helper (and its dependencies, likely yt-dlp)
- tqdm
- ffmpeg (must be installed and in the system PATH)
- common/music_style_classifier.py

### youtube-video-downloader-gui.py
A Text-based User Interface (TUI) built with urwid for downloading YouTube videos. It takes video URLs as command-line arguments, fetches their information asynchronously using ytdl_helper, and displays them in an interactive list. Users can select items, choose specific video and audio formats via a detailed dialog, and initiate downloads. The TUI shows status updates and progress bars for each item. Batch pre-selection of best audio or video is possible via command-line flags (--audio-only, --video).

#### Usage (Examples)
```bash
python -m youtube-video-downloader.youtube-video-downloader-gui "VIDEO_URL_1" "VIDEO_URL_2"
```

Can also be run directly: `python youtube-video-downloader/youtube-video-downloader-gui.py ...`

#### Features
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

#### Requires
- ytdl_helper (and its dependencies, likely yt-dlp)
- urwid
- ffmpeg (must be installed and in the system PATH)
- common/music_style_classifier.py
- **Note! (Windows specific):** ctypes (standard library, used for console setup)

### youtube-video-downloader-flask-ws.py & youtube-video-downloader.user-script.js
A Flask web service that allows downloading YouTube videos and audio via a web interface. It accepts video URLs via POST requests, fetches metadata, and downloads the content. The service supports various output formats (e.g., mp4, mp3) and allows users to specify desired resolution, audio bitrate, and target directory. It returns download progress and status updates in JSON format. To use it, run the Flask web service and send POST requests with the video URL and desired parameters. The web service can be accessed via a user script (e.g., Tampermonkey) that adds a button to download videos directly from YouTube pages.
The user script can be installed in a browser extension like Tampermonkey, which allows users to add custom scripts to web pages. The script adds a button to YouTube video pages, enabling users to download videos directly from the page.

#### Usage (Examples)
```bash
# Start the Flask web service
python -m youtube-video-downloader.youtube-video-downloader-flask-ws

# Send a POST request to download a video
curl -X POST -H "Content-Type: application/json" -d '{"url": "VIDEO_URL", "format": "mp4", "resolution": "720p", "audio_bitrate": "192k", "output_dir": "./downloads"}' http://localhost:5000/download
```

Can also be run directly: `python youtube-video-downloader/youtube-video-downloader-flask-ws.py`

#### Features
- Accepts video URLs via POST requests.
- Fetches metadata and downloads content in various formats.
- Provides download progress and status updates in JSON format.
- Supports output formats (e.g., mp4, mp3) and allows users to specify desired resolution, audio bitrate, and target directory.
- Returns download progress and status updates in JSON format.
- Logs activity to logs/youtube_downloader.log.

#### Requires
- Flask
- ytdl_helper (and its dependencies, likely yt-dlp)
- ffmpeg (must be installed and in the system PATH)
- common/music_style_classifier.py
