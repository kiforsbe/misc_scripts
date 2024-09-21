# misc_scripts
Miscellaneous scripts to automate common tasks.

## srt_to_transcript.py
Saves contents of the specified `.srt` files to a plain text transcripts.

### Requires
- srt

## transcribe_to_srt.py
Transcribes the specified media files such as `.mkv` to `.srt` subtitles.
Defaults to model `WhisperX` and language `English` (`"en"`) for transcription.

### Requires
- SubsAI

## insanely-fast-whisper.py
Minimalistic script to generate transcription using Whisper.

### Requires
- torch
- transformers

## mp4-to-mp3-converter-with-origin.py
Converts mp4 files to mp3 files. I use this to easily convert my Udio songs to .mp3s for my iPhone.

The converted MP3-files include:
- the audio from the video
- a thumbnail based on the first frame of the video
- the following metadata:
  - **Title & Artist** (based on the filename, "Artist - Title.mp4)
    - Defaults to "Udio" if nothing else is specified
  - **Comments:** Refferer and HostUrl based on the Windows 10/11 metadata stored with the file when downloaded

### Requires
Windows 10 or Windows 11.
- moviepy
- eyed3

## clipboard-monitor.py
Monitors the clipboard for changes and appends the contents to "clipboard.csv" file. It plays a sound when a change is detected and saved to the file.

### Requires
Windows 10 or Windows 11.
- winsound
- win32clipboard

## file-renamer-script.py
Takes a "clipboard.csv" file as input and uses the first column as a file_id that it tries to find in the files in the same folder as the script. If it finds the file, it is added to the list, with the proposed filename in the second column.

It then outputs the full list of proposed changes as file "rename_mappings.csv", so the user can verify the changes.

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
| convert  | POST & GET | convert_m3u8_to_mp4 | url, alt-url, title, video_id, description |

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
| alt-url | Can be used to provide the web page where the video was located for example. |
| title | The title of the video. |
| video_id | A unique ID that is used to identify the video. |
| description | A description of the video. |

### Requires
- uuid
- flask
- requests
- m3u8
- urllib
- ollama