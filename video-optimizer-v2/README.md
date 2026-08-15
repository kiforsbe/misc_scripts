# video-optimizer-v2

A script that allows for quick and easy optimization of videos. Just supply a list of videos on the command line or drag and drop them onto the script. You get a list of choices based on the contents of the videos such as which subtitles to make default, and which audio to make default along with target quality and resolution.

It is made specifically for transcoding for example tv-shows from your legacy media in a quick and simple way. Just drag a whole season onto the script and easily convert it for use on your phone.

It now also supports lookup of meta data from common anime databases and imdb via the shared [metadatacommon](../metadatacommon/README.md) providers. It will automatically download the metadata and add it to the video.

Check out branch mediaoptimizer_v1 for the old version.

## Usage
```bash
python video-optimizer-v2/video-optimizer-v2.py video1.mkv video2.mkv
```

## Requires
Use the video-optimizer-v2/requirements.txt file to install the requirements.
- ffmpeg-python
- requests
- pandas
- tqdm
- rapidfuzz
- inquirer
- mutagen
