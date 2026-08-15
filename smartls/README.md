# smartls

A smart directory explorer for querying files and folders with composable filters, metadata-aware sorting, and multiple output modes. It scans a filesystem tree once, aggregates directory metadata, and can render the filtered result set as a tree, flat list, JSON, or CSV.

## Features
- Recursive traversal with optional depth limiting
- Composable filters for direct child counts, recursive file counts, direct directory counts, sizes, ages, names, extensions, entry type, and depth
- Numeric expression syntax for exact matches, inequalities, ranges, enumerations, modulo checks, and approximate values
- Directory metadata including direct file and directory counts, recursive file counts, total descendant size, empty and sparse flags, and deepest nesting
- File metadata including size, timestamps, MIME type, symlink details, and optional MD5 or SHA256 hashing
- Output modes for tree view, flat list, formatted console tables, JSON, CSV, self-contained HTML web reports, and summary statistics
- Sorting, limiting, grouping, relative or absolute paths, human-readable or raw byte sizes, optional icons, and ANSI color control

## Usage Examples
```bash
# All directories with no files anywhere below them
python smartls/smartls.py --type d --files =0 --long

# Directories with 1 to 3 recursive files, sorted by total size descending
python smartls/smartls.py --type d --files 1..3 --sort -size --stats

# Large files modified within the last week
python smartls/smartls.py --type f --size >=50MB --mtime <7d --flat

# Python and JavaScript files excluding test names
python smartls/smartls.py ./src --ext py,js --not --name "*test*" --long

# Export matching directories to JSON
python smartls/smartls.py --type d --files =0 --json

# Render console output in aligned columns like the web report
python smartls/smartls.py --type f --flat --columns type,size,modified,relative-path --bytes

# Export a self-contained HTML report
python smartls/smartls.py --type f --size >=10MB --export-html smartls-report.html
```

## Notes
- `--or` separates filter groups and `--not` negates only the next filter
- In tree mode, matching descendants keep their ancestor path visible for context
- `--columns` takes a comma-separated list of metadata columns: `type`, `size`, `modified`, `created`, `accessed`, `children`, `recursive_files`, `mime`, `extension`, `relative_path`, `full_path`, `owner`, `group`, `permissions`
- The HTML export is self-contained and can be opened directly in a browser without external assets
- Presets, config files, and parallel traversal are not included in this implementation
