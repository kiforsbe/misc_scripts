# file_metadata_scanner

A comprehensive tool for extracting metadata from files and folders with support for various file types. Scans directories recursively or non-recursively, extracts basic file information (size, timestamps, attributes) and optional extended metadata (audio/video properties via ffmpeg, image dimensions, comic archive contents), and exports results to CSV, JSON, and an interactive HTML webapp. Supports thumbnail generation for video files and provides flexible filtering options.

## Features
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

## Usage Examples
```bash
# Basic scan of current directory (exports to ./metadata/)
python file_metadata_scanner/file_metadata_scanner.py .

# Recursive scan with custom export location
python file_metadata_scanner/file_metadata_scanner.py /path/to/folder -r --export-bundle /output/location

# Scan only video files with extended metadata and thumbnails
python file_metadata_scanner/file_metadata_scanner.py /path/to/videos -r -e mp4,mkv,avi --extended --thumbnails

# Exclude specific paths (node_modules, cache directories, etc.)
python file_metadata_scanner/file_metadata_scanner.py /path/to/folder -r --exclude node_modules,__pycache__,.git

# Full scan with all features and custom export location
python file_metadata_scanner/file_metadata_scanner.py /path/to/media -r --extended --thumbnails --export-bundle C:\MyMetadata

# Skip slow CBR processing, only process CBZ comic archives
python file_metadata_scanner/file_metadata_scanner.py /path/to/comics -r --extended --skip-cbr

# Set minimum video duration for thumbnail generation (e.g., 10 minutes)
python file_metadata_scanner/file_metadata_scanner.py /path/to/videos -r --thumbnails --min-duration 600

# Regenerate webapp from existing metadata bundle
python file_metadata_scanner/file_metadata_scanner.py --regenerate-bundle /path/to/bundle

# Regenerate webapp with missing thumbnails
python file_metadata_scanner/file_metadata_scanner.py --regenerate-bundle /path/to/bundle --thumbnails

# Verbose logging for troubleshooting
python file_metadata_scanner/file_metadata_scanner.py /path/to/folder -r --extended --log-level DEBUG
```

## Requires
- tqdm (for progress bars)
- Pillow (for image metadata extraction)
- video_thumbnail_generator (local module, for thumbnail generation)
- ffmpeg and ffprobe (system binaries, for extended video/audio metadata)
- libarchive-c or rarfile (for CBR comic archive extraction)
  - libarchive-c (preferred): Requires libarchive DLL
  - rarfile (fallback): Requires UnRAR tool on PATH
