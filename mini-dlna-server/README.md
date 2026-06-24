# mini-dlna-server

A lightweight DLNA/UPnP media server written in Python, targeting Samsung TVs (2022+) and Windows 11 hosts.

## Features

- DLNA 1.5 / UPnP 1.0 compliant
- Samsung TV compatibility (Q-series and later) — client profiling, `sec:dcmInfo`, Samsung namespaces
- Automatic JPEG thumbnail generation for video files via ffmpeg
- On-the-fly thumbnail caching (disk-backed)
- HTTP range requests and time-based seeking (DLNA `TimeSeekRange.dlna.org`)
- Hot-reload of `config.json` — no restart needed to add/remove media paths
- Playlist support (flat lists, nested folders, dict or array format)
- Multiple shared paths exposed as separate DLNA containers
- SSDP device discovery with multicast and M-SEARCH response
- Coexists with the Windows 11 built-in UPnP/SSDP service

## Requirements

- Python 3.11+
- **ffmpeg** must be on `PATH` (used for thumbnail generation)
- Python packages (see `requirements.txt`):

```
pip install -r requirements.txt
```

| Package | Purpose |
|---------|---------|
| `mutagen` | Audio/video metadata (duration, tags) |
| `Pillow` | Image thumbnail generation and resizing |
| `psutil` | Resource monitoring |
| `netifaces` | Multi-interface network detection |
| `whoosh` | Full-text search (content directory search) |
| `ffmpeg-python` | ffmpeg bindings (ffmpeg binary also required) |

## Configuration

Create a `config.json` file (default location: same directory as the script).

### Minimal config

```json
{
  "shared_paths": [
    "C:/Users/you/Videos",
    "D:/Media/Movies"
  ]
}
```

### With playlists (dict format)

```json
{
  "shared_paths": ["C:/Media"],
  "playlists": {
    "Favourites": [
      "C:/Media/Movies/film1.mp4",
      "C:/Media/Movies/film2.mkv"
    ],
    "Music Mix": [
      "C:/Media/Music/track1.mp3",
      "C:/Media/Music/track2.flac"
    ]
  }
}
```

### With nested playlist folders (dict format)

```json
{
  "shared_paths": ["C:/Media"],
  "playlists": {
    "Movies": {
      "Action": [
        "C:/Media/Movies/action1.mp4"
      ],
      "Drama": [
        "C:/Media/Movies/drama1.mkv"
      ]
    }
  }
}
```

### With playlists (array format)

```json
{
  "shared_paths": ["C:/Media"],
  "playlists": [
    {
      "name": "Weekend Watch",
      "files": [
        "C:/Media/Movies/film1.mp4",
        "C:/Media/Movies/film2.mp4"
      ]
    }
  ]
}
```

Playlist files must be inside one of the `shared_paths`. Entries outside shared paths or with unsupported extensions are skipped with a warning in the log.

## Running

```bash
python mini-dlna-server.py
# or with a custom config path:
python mini-dlna-server.py --config /path/to/config.json
```

The server binds to the local IP on port 8201 (incrementing to 8299 if in use) and starts SSDP discovery. The console prints the server address on startup:

```
Server identity: Python Media Server [143022] (HOSTNAME) at http://192.168.1.100:8201
```

Your TV should discover the server automatically. If it does not appear within 30 seconds, trigger a network device scan from the TV's settings.

## Supported Media Formats

| Type | Extensions |
|------|-----------|
| Video | `.mp4`, `.m4v`, `.mkv`, `.avi`, `.mov` |
| Audio | `.mp3`, `.flac`, `.wav`, `.m4a`, `.aac`, `.ogg` |
| Image | `.jpg`, `.jpeg`, `.png`, `.gif`, `.webp` |

## Thumbnails

Thumbnails are generated on first request using ffmpeg and cached to `cache/thumbnails/` next to the script. The static thumbnail is extracted at 20% into the video duration. Generation happens on the first browse request for a directory — expect a short delay for large folders on cold start.

Thumbnail cache is keyed by filename (SHA-256 hash), so moving files between folders will regenerate thumbnails.

## Logs

| File | Content |
|------|---------|
| `logs/dlna_server_debug.log` | Full DEBUG output (SOAP, DIDL, browse params) |
| `logs/dlna_server.log` | WARNING and above |

Console output is WARNING and above only.

## Hot-reload

Editing `config.json` while the server is running triggers an automatic reload on the next client request. Added/removed shared paths take effect immediately. Connected clients receive a `ContentDirectory` event notification.

## Architecture

```
mini-dlna-server.py     — HTTP server, SOAP dispatch, media streaming
contentdirectoryhandler.py — Browse/Search, DIDL-Lite generation
ssdpserver.py           — SSDP multicast listener and announcer
resourcemonitor.py      — CPU/memory/network tracking
network_utils.py        — Local IP detection
../video_thumbnail_generator.py — ffmpeg thumbnail generation (shared module)
```

## Windows 11 Notes

The Windows Discovery Service (WDS) occupies UDP port 1900. The SSDP server handles this gracefully — it binds in shared mode and logs a warning if the exclusive bind fails. DLNA functionality is not affected.

Firewall rules may block multicast on port 1900 or the HTTP port (8201). If the TV cannot discover the server, add inbound rules for both ports.
