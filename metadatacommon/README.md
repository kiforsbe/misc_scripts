# metadatacommon

Shared metadata provider package used by `video-optimizer-v2`, `common/file_grouper.py`, and several other tools (`netflix_watch_status`, `series_completeness_checker`, `latest_episodes_viewer`, `series_bundler.py`) to look up show/movie metadata and MyAnimeList watch status.

## Modules
- `metadata_provider.py` — Base metadata provider class (`BaseMetadataProvider`, `MetadataManager`, `TitleInfo`, `EpisodeInfo`, `MatchResult`) that defines the interface for metadata retrieval services and manages metadata formatting for video files.
- `anime_metadata.py` — Metadata retrieval from anime databases such as AniList and MyAnimeList, including episode information, season data, air dates, and anime-specific details like studio information and Japanese titles.
- `imdb_metadata.py` — Metadata retrieval from the Internet Movie Database (IMDb), including cast information, directors, release dates, ratings, and plot summaries.
- `plex_metadata.py` — Metadata retrieval from a local or remote Plex Media Server, including titles, descriptions, genres, ratings, and artwork.
- `myanimelist_watch_status.py` — Reads MyAnimeList watch-status data for tagging and completeness workflows.

## Utility Scripts

### metadata_cache_manager.py
CLI helper for the `metadatacommon` providers to inspect and control cache TTLs. Supports `status`, `refresh`, `invalidate`, and `set-expiry` (accepts long form like "3 days" or short form like `2m7d`). TTL is persisted per provider (IMDb, Anime) and `invalidate` forces TTL to 0 so cache refreshes on next access. Optional `--no-color` disables colored output.

This script lives at the repo root (`metadata_cache_manager.py`) rather than inside this package, since it's a standalone consumer of these providers rather than part of the shared library.

```bash
python metadata_cache_manager.py --provider imdb status
python metadata_cache_manager.py --provider all refresh
python metadata_cache_manager.py --provider anime set-expiry "7 days"
```

#### Requires
- colorama (optional, for colored output)

### validate_mal_xml.py
Validates a MyAnimeList XML export (plain or gzipped) against the bundled `myanimelist.xsd` schema, and prints a short summary (anime entry count, user total) on success.

```bash
# Validate a plain or gzipped MyAnimeList export
python metadatacommon/validate_mal_xml.py animelist.xml
python metadatacommon/validate_mal_xml.py animelist.xml.gz

# Validate against a custom schema
python metadatacommon/validate_mal_xml.py animelist.xml --xsd custom_schema.xsd
```

#### Requires
- lxml

## Requires
Install `metadatacommon`'s runtime dependencies with `pip install -r metadatacommon/requirements.txt`: `requests`, `tqdm`, `rapidfuzz`, `zstandard`, `lxml`. `guessit` is also listed -- it's only needed to run `metadatacommon`'s own test suite (via `common/guessit_wrapper.py`), not by the providers themselves.
