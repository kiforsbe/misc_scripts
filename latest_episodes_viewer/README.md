# latest_episodes_viewer

A script that generates a simple HTML page listing the latest episodes from a collection of TV series. It scans a specified directory for video files, extracts metadata using guessit and some custom metadata providers, and creates an organized list of the most recent episodes based on their air dates. The generated HTML page includes links to the episodes, making it easy to access and view the latest content.

## Features
- Scans a specified directory for video files
- Extracts metadata using guessit and custom metadata providers
- Generates an organized HTML page listing the latest episodes
- Includes links to the episodes for easy access

## Usage Examples
```bash
latest_episodes_viewer.py /path/to/episodes --max-episodes 50 --recursive
latest_episodes_viewer.py /path/to/episodes --exclude-paths /path/to/episodes/trash
latest_episodes_viewer.py /path/to/episodes --include-patterns "*.mkv" "*.mp4" --export episodes.html
latest_episodes_viewer.py /path/to/episodes --myanimelist-xml /path/to/animelist.xml --export episodes.html
latest_episodes_viewer.py /path/to/episodes --verbose 3 --max-episodes 200 --export latest.html
```

## Requires
- guessit
- requests
- tqdm
