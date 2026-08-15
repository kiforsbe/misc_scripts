# series_completeness_checker

A script that analyzes TV series collections to identify missing episodes, gaps in seasons, and incomplete series. It scans directory structures and filenames to build a comprehensive view of your media library, highlighting what episodes or seasons might be missing from your collection.

The script supports various TV series naming conventions and provides detailed reports on series completeness, making it easy to identify and fill gaps in your media collection. It can also suggest potential naming inconsistencies and provide recommendations for organizing your series.

Features **MyAnimeList** (via [metadatacommon](../metadatacommon/README.md)) as the primary source for anime information, providing accurate episode counts, season data, and series metadata. Supports integration with public MyAnimeList lists and exported lists to track watch status and completion progress.

## Features
- Comprehensive TV series analysis and gap detection
- Support for multiple naming conventions and formats
- MyAnimeList integration for accurate anime metadata and episode validation
- Watch status integration with public or exported MyAnimeList lists
- Season and episode numbering validation
- Missing episode identification with detailed reporting
- Series metadata integration for enhanced accuracy
- Export results to various formats (JSON, HTML reports)
- Integration with metadata providers for series validation
- Batch processing of multiple series directories

## Usage (Examples)
```bash
# Check completeness of series in current directory
python series_completeness_checker/series_completeness_checker.py

# Generate JSON report
python series_completeness_checker/series_completeness_checker.py /path/to/series --export series.json

# Generate HTML webapp
python series_completeness_checker/series_completeness_checker.py /path/to/series --webapp-export series.html
```

For the full CLI reference (filters, refresh operations, thumbnails), run:
```bash
python series_completeness_checker/series_completeness_checker.py --help
```

## Requires
- rapidfuzz
- requests
- pandas
- pathlib
