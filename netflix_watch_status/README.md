# netflix_watch_status

Reads a Netflix viewing history CSV, classifies entries as movies or series episodes, resolves metadata from IMDb and anime providers when available, and generates both console and standalone HTML watch-status reports. It also produces a CSV template of titles that still need IMDb-oriented override mappings.

## Features
- Parses Netflix viewing-history CSV exports with fallback CSV encodings and basic mojibake repair
- Uses Netflix-specific title parsing plus optional metadata lookups to improve movie/series classification and resolve season, episode, year, runtime, genres, rating, vote count, and source IDs
- Multiple output modes:
  - **default**: Text summary of unique movies and series
  - **json**: Machine-readable export of the summary and resolved entries
  - **table**: Treegrid-style console table grouped by title, season, and episode
  - **webapp**: Self-contained HTML report with filtering, selection details, and artwork thumbnails
- Table and webapp rows can show watched progress such as watched/total episodes when provider episode lists are available, including synthetic unwatched episode rows in the grouped view
- Supports CSV-based episode title overrides via `netflix_episode_title_overrides.csv` and `--episode-title-overrides`, including canonical title, year, source ID, and episode-title remapping
- Automatically exports an `*_unmapped_imdb_titles.csv` helper file to make missing override mappings easier to review and fill in
- Webapp export caches thumbnails locally and prefers direct IMDb-backed poster URLs when available

## Usage Examples
```bash
python netflix_watch_status/netflix_watch_status.py path/to/NetflixViewingHistory.csv
python netflix_watch_status/netflix_watch_status.py path/to/NetflixViewingHistory.csv --json
python netflix_watch_status/netflix_watch_status.py path/to/NetflixViewingHistory.csv --table
python netflix_watch_status/netflix_watch_status.py path/to/NetflixViewingHistory.csv --table --columns title,year,episode,views,average_rating
python netflix_watch_status/netflix_watch_status.py path/to/NetflixViewingHistory.csv --webapp-export netflix-watch-status.html
python netflix_watch_status/netflix_watch_status.py path/to/NetflixViewingHistory.csv --episode-title-overrides my_overrides.csv
python netflix_watch_status/netflix_watch_status.py path/to/NetflixViewingHistory.csv --no-metadata
```
