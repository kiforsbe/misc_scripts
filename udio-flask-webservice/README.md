# udio-flask-webservice (udio-download_ext-button.user.js)

A flask web service that adds metadata including cover art to your song files downloaded from Udio. It comes with a user script (e.g. Tampermonkey) that simplifies this process by adding a new button to the song pages "Download with metadata" that calls the webservice.
This webservice now also supports Riffusion and .m4a audio files.

The webservice exposes the following interfaces:

| Interface | Methods | Functions | Parameters |
| --- | --- | --- | --- |
| /api/download_ext | POST & GET | download_ext | mp3_url, image_url, title, artist, album, genre, year, cannonical, lyrics |

## /api/download_ext
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

## Requires
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

## Optional Dependencies
- music_style_classifier.py
  - librosa
  - tensorflow
  - numpy
  - transformers

## User Scripts
- `udio-download_ext-button.user.js`: Adds a "Download with metadata" button to Udio song pages
- `riffusion-download_ext-button.user.js`: Adds a "Download with metadata" button to Riffusion song pages

These user scripts enhance the webservice functionality by integrating download buttons directly into the respective web interfaces. They automatically capture song metadata and cover art, then send this information to the webservice for processing, making the download process seamless and efficient. They have been tested with Tampermonkey on Chrome.
