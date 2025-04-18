# misc_scripts
Miscellaneous scripts to automate common tasks.

## srt_to_transcript.py
Saves contents of the specified `.srt` files to a plain text transcripts.

### Requires
- srt

## transcribe_to_srt.py
Transcribes the specified media files such as `.mkv` to `.srt` subtitles.
Defaults to model `WhisperX` and language `English` (`"en"`) for transcription.
The model should automatically download and install when the script is run.

### Requires
- SubsAI (https://github.com/abdeladim-s/subsai)
  - Model: m-bain/whisperX

### Recommended
Torch with CUDA support is highly recommended if you have a CUDA capable machine. For SubsAI with `torch-2.0.1` requirement, install `torch-2.0.1+cu118` per instruction https://pytorch.org/get-started/previous-versions/#v201 instead of default one in SubsAI "`requirements.txt`" file.

## insanely-fast-whisper.py
Minimalistic script to generate transcription using Whisper.

### Requires
- torch
- transformers

## mp4-to-mp3-converter-with-origin.py
Converts mp4 files to mp3 files. I use this to easily convert my Udio songs to `.mp3`s for my iPhone.

The converted MP3-files include:
- the audio from the video file
- a thumbnail based on the first frame of the video
- the following metadata:
  - **Title & Artist** (based on the filename, "`Artist - Title.mp4`")
    - Defaults to "`Udio`" if nothing else is specified
  - **Comments:** `Refferer` and `HostUrl` based on the Windows 10/11 metadata stored with the file when downloaded

### Requires
Windows 10 or Windows 11.
- moviepy
- eyed3

## clipboard-monitor.py
Monitors the clipboard for changes and appends the contents to "`clipboard.csv`" file. It plays a sound when a change is detected and saved to the file.

### Requires
Windows 10 or Windows 11.
- winsound
- win32clipboard

## file-renamer-script.py
Takes a "`clipboard.csv`" file as input and uses the first column as a file_id that it tries to find in the files in the same folder as the script. If it finds the file, it is added to the list, with the proposed filename in the second column.

It then outputs the full list of proposed changes as file "`rename_mappings.csv`", so the user can verify the changes.

If the user approves them, the user simply responds "y", or "yes" or presses enter to confirm that they want to rename the files in the folder with the proposed filenames.

### Requires
- csv
- logging
- traceback

## m3u8-to-mp4-flask-webservice.py
A flask web service that takes a m3u8 file as input and converts it into an MP4 file.

The webservice exposes the following interfaces:
| Interface | Methods | Functions | Parameters |
| --- | --- | --- | --- |
| stream_info | GET | stream_info | url |
| convert  | POST & GET | convert_m3u8_to_mp4 | url, alt_url, title, video_id, description |

### stream_info
Returns a JSON structure describing the overall metadata of the base stream, and the contained audio and video streams.

| Parameter | Description |
| --- | --- |
| url | The URL of the m3u8 file to be queried. |

### convert
Converts an input m3u8 file into a MP4 file. The input is sent as a multipart/form-data request, with the key "file" and the value being the m3u8 file to be converted.

| Parameter | Description |
| --- | --- |
| url | The URL of the m3u8 file to be converted. |
| alt_url | Can be used to provide the web page where the video was located for example. |
| title | The title of the video. |
| video_id | A unique ID that is used to identify the video. |
| description | A description of the video. |

### Requires
- flask
- requests
- m3u8
- ollama

## m3u8-to-mp4-flask-webservice-simple.py
A flask web service that takes a m3u8 file as input and converts it into an MP4 file.

The webservice exposes the following interfaces:
| Interface | Methods | Functions | Parameters |
| --- | --- | --- | --- |
| get | POST & GET | convert_m3u8_to_mp4 | url, filename |

### get
Streams a m3u8 to save it as a mp4, real-time saving only, so will take as long as the stream itself is.

| Parameter | Description |
| --- | --- |
| url | The URL of the m3u8 file to be converted. |
| filename | Target filename (optional). |

### Requires
- flask
- requests
- m3u8

## merge-audio-files-to-one-output.py
Simple merge a bunch of audio files into one single output file. Just drag all the input files onto the script and it will be output in the same folder as the first file with the name "`combined_output.<ext>`". The script will ask what format, bitrate etc the output shall get.

### Requires
- pydub
- inquirer
- tqdm

## udio-flask-webservice.py (udio-download_ext-button.user.js)
A flask web service that adds metadata including cover art to your song files downloaded from Udio. It comes with a user script (e.g. Tampermonkey) that simplifies this process by adding a new button to the song pages "Download with metadata" that calls the webservice.

The webservice exposes the following interfaces:
| Interface | Methods | Functions | Parameters |
| --- | --- | --- | --- |
| /api/download_ext | POST & GET | download_ext | mp3_url, image_url, title, artist, album, genre, year, cannonical, lyrics |

### /api/download_ext
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
| lyrics | UNSYNCEDLYRICS | Lyrics to add to the track. |

### Requires
- flask
- eyed3
- magic (python-magic-bin on windows)

## video-optimizer-v2
A script that allows for quick and easy optimization of videos. Just supply a list of videos on the command line or drag and drop them onto the script. You get a list of choices based on the contents of the videos such as which subtitles to make default, and which audio to make default along with target quality and resolution.

It is made specifically for transcoding for example tv-shows from your legacy media in a quick and simple way. Just drag a whole season onto the script and easily convert it for use on your phone.

It now also supports lookup of meta data from common anime databases and imdb. It will automatically download the metadata and add it to the video.

Check out branch mediaoptimizer_v1 for the old version.

### Requires
Use the video-optimizer-v2/requirements.txt file to install the requirements.
- ffmpeg-python
- requests
- pandas
- tqdm
- rapidfuzz
- inquirer
- mutagen

## rss-feed-downloader.py
A script to parse RSS feeds and download enclosures (e.g., audio, video, or other files) with a console-based GUI for selection and progress tracking.

### Features
- Parses RSS feeds from local files or URLs.
- Extracts enclosures and allows users to select files for download.
- Downloads files with progress tracking and optional HTTP Basic Authentication.
- Saves downloaded files to a specified directory and generates a mapping file in JSON format.

### Requires
- curses
- urllib
- json

## youtube-video-downloader-cli.py
A command-line tool for downloading YouTube videos and audio using the `ytdl_helper` library. It allows fetching video information (metadata, available formats) as JSON or downloading content directly. Users can specify desired resolution, audio bitrate, output format (e.g., mp4, mp3), and target directory via command-line arguments. Download progress is displayed using `tqdm` progress bars.

### Usage (Examples)
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
### Requires
- ytdl_helper (and its dependencies, likely yt-dlp)
- tqdm
- ffmpeg (must be installed and in the system PATH)

## youtube-video-downloader-gui.py
A Text-based User Interface (TUI) built with urwid for downloading YouTube videos. It takes video URLs as command-line arguments, fetches their information asynchronously using ytdl_helper, and displays them in an interactive list. Users can select items, choose specific video and audio formats via a detailed dialog, and initiate downloads. The TUI shows status updates and progress bars for each item. Batch pre-selection of best audio or video is possible via command-line flags (--audio-only, --video).

### Features
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

### Requires
- ytdl_helper (and its dependencies, likely yt-dlp)
- urwid
- ffmpeg (must be installed and in the system PATH)
- **Note! (Windows specific):** ctypes (standard library, used for console setup)

# Experimental

## lyrics-timing-generator.py
Intended to generate timed lyrics for audio files (.lrc). Uses whisper library to generate timed lyrics, and ollama and an llm to structure them.

But it is not good. Really not good. It's a start, but not quite there yet. Need to restart from a known base to generate the timed subtitles which is a known working thing, and then convert that to lyrics, using an llm to format them.

### Requires
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

## mini-dlna-server.py
A script that runs a local dlna server instance on the computer, taking as input a command line argument pointing out the folder to serve to clients.

It is intended if I get time to do it, to stabilize it to properly handle conenctions, work better with Windows 11, and transcode media to the client using ffmpeg.

**Note!** Currently it is extremely unstable and mostly doesn't work. If anyone wants to refactor it and fix some of the remaining issues that would be cool. :)

### Requires
- mutagen
