from __future__ import annotations

import argparse
import csv
import gzip
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence
from urllib.error import URLError
from urllib.request import urlopen


@dataclass(frozen=True)
class ColumnDef:
    name: str
    kind: str = "text"


@dataclass(frozen=True)
class DatasetSchema:
    name: str
    columns: tuple[ColumnDef, ...]

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns)

    @property
    def column_map(self) -> Dict[str, ColumnDef]:
        return {column.name: column for column in self.columns}

    @property
    def default_search_columns(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns if column.kind == "text")


SCHEMAS: Dict[str, DatasetSchema] = {
    "title.akas": DatasetSchema(
        name="title.akas",
        columns=(
            ColumnDef("titleId"),
            ColumnDef("ordering", "int"),
            ColumnDef("title"),
            ColumnDef("region"),
            ColumnDef("language"),
            ColumnDef("types"),
            ColumnDef("attributes"),
            ColumnDef("isOriginalTitle", "bool"),
        ),
    ),
    "title.basics": DatasetSchema(
        name="title.basics",
        columns=(
            ColumnDef("tconst"),
            ColumnDef("titleType"),
            ColumnDef("primaryTitle"),
            ColumnDef("originalTitle"),
            ColumnDef("isAdult", "bool"),
            ColumnDef("startYear", "int"),
            ColumnDef("endYear", "int"),
            ColumnDef("runtimeMinutes", "int"),
            ColumnDef("genres"),
        ),
    ),
    "title.crew": DatasetSchema(
        name="title.crew",
        columns=(
            ColumnDef("tconst"),
            ColumnDef("directors"),
            ColumnDef("writers"),
        ),
    ),
    "title.episode": DatasetSchema(
        name="title.episode",
        columns=(
            ColumnDef("tconst"),
            ColumnDef("parentTconst"),
            ColumnDef("seasonNumber", "int"),
            ColumnDef("episodeNumber", "int"),
        ),
    ),
    "title.principals": DatasetSchema(
        name="title.principals",
        columns=(
            ColumnDef("tconst"),
            ColumnDef("ordering", "int"),
            ColumnDef("nconst"),
            ColumnDef("category"),
            ColumnDef("job"),
            ColumnDef("characters"),
        ),
    ),
    "title.ratings": DatasetSchema(
        name="title.ratings",
        columns=(
            ColumnDef("tconst"),
            ColumnDef("averageRating", "float"),
            ColumnDef("numVotes", "int"),
        ),
    ),
}

DATASET_URLS: Dict[str, str] = {
    "title.basics": "https://datasets.imdbws.com/title.basics.tsv.gz",
    "title.episode": "https://datasets.imdbws.com/title.episode.tsv.gz",
    "title.ratings": "https://datasets.imdbws.com/title.ratings.tsv.gz",
    "title.akas": "https://datasets.imdbws.com/title.akas.tsv.gz",
    "title.crew": "https://datasets.imdbws.com/title.crew.tsv.gz",
    "title.principals": "https://datasets.imdbws.com/title.principals.tsv.gz",
}

DEFAULT_CACHE_DIR = Path.home() / ".video_metadata_cache" / "imdb"

FILTER_OPERATORS = (">=", "<=", "!=", "=", "!~", "~", ">", "<")


@dataclass(frozen=True)
class CompiledFilter:
    expression: str
    column: str
    operator: str
    matcher: Callable[[Dict[str, Any]], bool]


@dataclass(frozen=True)
class QueryResult:
    rows: List[Dict[str, Any]]
    matched_count: int


PromptFunction = Callable[[DatasetSchema, Path, bool], bool]
DownloadFunction = Callable[[DatasetSchema, Path], Path]


def set_csv_field_size_limit() -> None:
    limit = sys.maxsize
    while True:
        try:
            csv.field_size_limit(limit)
            return
        except OverflowError:
            limit //= 10
            if limit < 1:
                raise RuntimeError("Unable to configure CSV field size limit.")


def parse_dataset_name(input_path: Path) -> Optional[str]:
    name = input_path.name
    for suffix in (".tsv.gz", ".tsv"):
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return None


def get_default_cache_dir() -> Path:
    return Path(os.path.expanduser("~")) / ".video_metadata_cache" / "imdb"


def parse_output_columns(schema: DatasetSchema, columns_text: Optional[str]) -> List[str]:
    if not columns_text:
        return list(schema.column_names)

    selected = [item.strip() for item in columns_text.split(",") if item.strip()]
    if not selected:
        raise ValueError("No output columns were provided.")

    invalid = [column for column in selected if column not in schema.column_map]
    if invalid:
        raise ValueError(f"Unknown output column(s): {', '.join(invalid)}")

    return selected


def parse_search_columns(schema: DatasetSchema, columns: Sequence[str]) -> List[str]:
    if not columns:
        return list(schema.default_search_columns)

    selected: List[str] = []
    for chunk in columns:
        for item in chunk.split(","):
            column = item.strip()
            if not column:
                continue
            if column not in schema.column_map:
                raise ValueError(f"Unknown search column: {column}")
            selected.append(column)

    if not selected:
        raise ValueError("No search columns were provided.")

    return selected


def parse_scalar(raw_value: str, kind: str) -> Any:
    if raw_value in ("", r"\N"):
        return None
    if kind == "int":
        return int(raw_value)
    if kind == "float":
        return float(raw_value)
    if kind == "bool":
        return raw_value == "1"
    return raw_value


def stringify_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "1" if value else "0"
    return str(value)


def build_filter(schema: DatasetSchema, expression: str, ignore_case: bool = True) -> CompiledFilter:
    column_name: Optional[str] = None
    operator_text: Optional[str] = None
    value_text: Optional[str] = None

    for candidate in FILTER_OPERATORS:
        if candidate in expression:
            left, right = expression.split(candidate, 1)
            column_name = left.strip()
            operator_text = candidate
            value_text = right.strip()
            break

    if not column_name or operator_text is None or value_text is None:
        raise ValueError(
            f"Invalid filter '{expression}'. Use column=value, column~text, or numeric comparisons like numVotes>=1000."
        )

    column = schema.column_map.get(column_name)
    if column is None:
        raise ValueError(f"Unknown filter column: {column_name}")

    if operator_text in {">", ">=", "<", "<="} and column.kind not in {"int", "float"}:
        raise ValueError(f"Operator '{operator_text}' requires a numeric column, got '{column_name}'.")

    expected = parse_scalar(value_text, column.kind)
    if expected is None and operator_text in {">", ">=", "<", "<="}:
        raise ValueError(f"Filter '{expression}' compares against an empty value.")

    def normalize_text(value: Any) -> str:
        text = stringify_value(value)
        return text.casefold() if ignore_case else text

    def matcher(row: Dict[str, Any]) -> bool:
        actual = row.get(column_name)

        if operator_text == "=":
            if column.kind == "text":
                return normalize_text(actual) == normalize_text(expected)
            return actual == expected
        if operator_text == "!=":
            if column.kind == "text":
                return normalize_text(actual) != normalize_text(expected)
            return actual != expected
        if operator_text == "~":
            return normalize_text(expected) in normalize_text(actual)
        if operator_text == "!~":
            return normalize_text(expected) not in normalize_text(actual)
        if actual is None:
            return False
        if operator_text == ">":
            return actual > expected
        if operator_text == ">=":
            return actual >= expected
        if operator_text == "<":
            return actual < expected
        return actual <= expected

    return CompiledFilter(expression=expression, column=column_name, operator=operator_text, matcher=matcher)


def iter_tsv_rows(file_path: Path, schema: DatasetSchema) -> Iterable[Dict[str, Any]]:
    set_csv_field_size_limit()
    opener = gzip.open if file_path.suffix == ".gz" else open
    with opener(file_path, "rt", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        header = tuple(reader.fieldnames or ())
        missing = [column for column in schema.column_names if column not in header]
        if missing:
            raise ValueError(
                f"File '{file_path}' does not match schema '{schema.name}'. Missing columns: {', '.join(missing)}"
            )

        for raw_row in reader:
            parsed_row: Dict[str, Any] = {}
            for column in schema.columns:
                parsed_row[column.name] = parse_scalar(raw_row.get(column.name, ""), column.kind)
            yield parsed_row


def row_matches_search(row: Dict[str, Any], query: Optional[str], search_columns: Sequence[str], ignore_case: bool) -> bool:
    if not query:
        return True

    needle = query.casefold() if ignore_case else query
    for column in search_columns:
        haystack = stringify_value(row.get(column))
        if ignore_case:
            haystack = haystack.casefold()
        if needle in haystack:
            return True
    return False


def query_rows(
    file_path: Path,
    schema: DatasetSchema,
    compiled_filters: Sequence[CompiledFilter],
    query: Optional[str],
    search_columns: Sequence[str],
    limit: int,
    ignore_case: bool = True,
) -> QueryResult:
    rows: List[Dict[str, Any]] = []
    matched_count = 0

    for row in iter_tsv_rows(file_path, schema):
        if not row_matches_search(row, query, search_columns, ignore_case):
            continue
        if any(not compiled.matcher(row) for compiled in compiled_filters):
            continue

        matched_count += 1
        if len(rows) < limit:
            rows.append(row)

    return QueryResult(rows=rows, matched_count=matched_count)


def truncate_text(text: str, width: int) -> str:
    if width < 1:
        return ""
    if len(text) <= width:
        return text
    if width == 1:
        return text[:1]
    return text[: width - 1] + "…"


def format_table(rows: Sequence[Dict[str, Any]], columns: Sequence[str], max_width: int) -> str:
    widths: Dict[str, int] = {column: min(len(column), max_width) for column in columns}
    rendered_rows: List[List[str]] = []

    for row in rows:
        rendered_row: List[str] = []
        for column in columns:
            cell = truncate_text(stringify_value(row.get(column)), max_width)
            widths[column] = min(max(widths[column], len(cell)), max_width)
            rendered_row.append(cell)
        rendered_rows.append(rendered_row)

    def render_line(values: Sequence[str]) -> str:
        return " | ".join(value.ljust(widths[column]) for column, value in zip(columns, values))

    header = render_line(columns)
    separator = "-+-".join("-" * widths[column] for column in columns)
    body = [render_line(row) for row in rendered_rows]
    return "\n".join([header, separator, *body])


def prompt_for_download(dataset: DatasetSchema, destination: Path, force_download: bool) -> bool:
    action = "Re-download" if force_download else "Download"
    prompt = f"{action} {dataset.name} to {destination}? [y/N]: "
    try:
        response = input(prompt).strip().lower()
    except EOFError:
        return False
    return response in {"y", "yes"}


def download_dataset(dataset: DatasetSchema, destination: Path) -> Path:
    source_url = DATASET_URLS.get(dataset.name)
    if not source_url:
        raise ValueError(f"No download URL is defined for dataset '{dataset.name}'.")

    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        with urlopen(source_url) as response, destination.open("wb") as handle:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                handle.write(chunk)
    except URLError as exc:
        raise FileNotFoundError(f"Failed to download {dataset.name}: {exc}") from exc

    return destination


def find_existing_dataset_file(base_dir: Path, dataset: DatasetSchema) -> Optional[Path]:
    for candidate_name in (f"{dataset.name}.tsv.gz", f"{dataset.name}.tsv"):
        candidate = base_dir / candidate_name
        if candidate.exists():
            return candidate
    return None


def ensure_dataset_available(
    dataset: DatasetSchema,
    destination: Path,
    download_missing: bool,
    force_download: bool,
    prompt_download: PromptFunction = prompt_for_download,
    download_file: DownloadFunction = download_dataset,
    interactive: Optional[bool] = None,
) -> Path:
    if destination.exists() and not force_download:
        return destination

    if not force_download and not download_missing:
        if interactive is None:
            interactive = sys.stdin.isatty()
        if not interactive:
            raise FileNotFoundError(
                f"Dataset file not found: {destination}. Run with --download to fetch it automatically."
            )
        if not prompt_download(dataset, destination, force_download):
            raise FileNotFoundError(f"Dataset file not found: {destination}")

    return download_file(dataset, destination)


def resolve_dataset(args: argparse.Namespace) -> DatasetSchema:
    if args.dataset:
        return SCHEMAS[args.dataset]

    inferred = parse_dataset_name(Path(args.path))
    if inferred and inferred in SCHEMAS:
        return SCHEMAS[inferred]

    available = ", ".join(sorted(SCHEMAS))
    raise ValueError(f"Could not infer dataset from path. Use --dataset. Available datasets: {available}")


def resolve_input_path(
    raw_path: Optional[str],
    dataset: DatasetSchema,
    cache_dir: Path,
    download_missing: bool = False,
    force_download: bool = False,
    prompt_download: PromptFunction = prompt_for_download,
    download_file: DownloadFunction = download_dataset,
) -> Path:
    if raw_path:
        path = Path(raw_path)
        if path.exists() and path.is_file():
            if force_download:
                return ensure_dataset_available(
                    dataset,
                    path,
                    download_missing=download_missing,
                    force_download=True,
                    prompt_download=prompt_download,
                    download_file=download_file,
                )
            return path

        if path.exists() and path.is_dir():
            existing = find_existing_dataset_file(path, dataset)
            if existing is not None and not force_download:
                return existing
            target = path / f"{dataset.name}.tsv.gz"
            return ensure_dataset_available(
                dataset,
                target,
                download_missing=download_missing,
                force_download=force_download,
                prompt_download=prompt_download,
                download_file=download_file,
            )

        if path.suffix in {".gz", ".tsv"}:
            return ensure_dataset_available(
                dataset,
                path,
                download_missing=download_missing,
                force_download=force_download,
                prompt_download=prompt_download,
                download_file=download_file,
            )

        existing = find_existing_dataset_file(path, dataset)
        if existing is not None and not force_download:
            return existing
        target = path / f"{dataset.name}.tsv.gz"
        return ensure_dataset_available(
            dataset,
            target,
            download_missing=download_missing,
            force_download=force_download,
            prompt_download=prompt_download,
            download_file=download_file,
        )

    existing = find_existing_dataset_file(cache_dir, dataset)
    if existing is not None and not force_download:
        return existing

    target = cache_dir / f"{dataset.name}.tsv.gz"
    return ensure_dataset_available(
        dataset,
        target,
        download_missing=download_missing,
        force_download=force_download,
        prompt_download=prompt_download,
        download_file=download_file,
    )


def print_schema(schema: DatasetSchema) -> None:
    print(f"Dataset: {schema.name}")
    print("Columns:")
    for column in schema.columns:
        print(f"  - {column.name} ({column.kind})")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Query IMDb title.* TSV or TSV.GZ files with simple search/filtering and tabular CLI output."
    )
    parser.add_argument(
        "path",
        nargs="?",
        help="Path to a title.*.tsv.gz file or a directory containing IMDb title datasets. Defaults to the IMDb cache directory when omitted.",
    )
    parser.add_argument("--dataset", choices=sorted(SCHEMAS), help="Dataset schema to use. Inferred from the filename when omitted.")
    parser.add_argument(
        "--cache-dir",
        default=str(DEFAULT_CACHE_DIR),
        help="Directory used to locate or download cached IMDb dataset files. Defaults to the same cache folder as the IMDb provider.",
    )
    parser.add_argument("--download", action="store_true", help="Download the dataset automatically when it is missing.")
    parser.add_argument("--force-download", action="store_true", help="Re-download the dataset even if a cached copy already exists.")
    parser.add_argument("--columns", help="Comma-separated output columns. Defaults to all columns in the schema.")
    parser.add_argument(
        "--where",
        action="append",
        default=[],
        help="Filter expression. Examples: titleType=movie, primaryTitle~matrix, numVotes>=100000, isAdult=0",
    )
    parser.add_argument("--query", help="Case-insensitive substring search across text columns by default.")
    parser.add_argument(
        "--search-column",
        action="append",
        default=[],
        help="Restrict --query to these columns. Can be repeated or passed as a comma-separated list.",
    )
    parser.add_argument("--limit", type=int, default=25, help="Maximum number of matching rows to display. Default: 25")
    parser.add_argument(
        "--max-width",
        type=int,
        default=40,
        help="Maximum displayed width for each table cell before truncation. Default: 40",
    )
    parser.add_argument("--case-sensitive", action="store_true", help="Use case-sensitive matching for --query and text filters.")
    parser.add_argument("--list-datasets", action="store_true", help="List supported title.* dataset schemas and exit.")
    parser.add_argument("--show-schema", choices=sorted(SCHEMAS), help="Print the declared columns for a dataset and exit.")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.list_datasets:
        for dataset_name in sorted(SCHEMAS):
            print(dataset_name)
        return 0

    if args.show_schema:
        print_schema(SCHEMAS[args.show_schema])
        return 0

    if args.limit < 1:
        parser.error("--limit must be >= 1")
    if args.max_width < 1:
        parser.error("--max-width must be >= 1")
    if args.force_download and not args.dataset and not args.path:
        parser.error("--force-download without a path requires --dataset so the dataset can be downloaded")

    ignore_case = not args.case_sensitive
    cache_dir = Path(args.cache_dir).expanduser()

    try:
        schema = resolve_dataset(args)
        file_path = resolve_input_path(
            args.path,
            schema,
            cache_dir=cache_dir,
            download_missing=args.download,
            force_download=args.force_download,
        )
        columns = parse_output_columns(schema, args.columns)
        search_columns = parse_search_columns(schema, args.search_column)
        compiled_filters = [build_filter(schema, expression, ignore_case=ignore_case) for expression in args.where]
        result = query_rows(
            file_path=file_path,
            schema=schema,
            compiled_filters=compiled_filters,
            query=args.query,
            search_columns=search_columns,
            limit=args.limit,
            ignore_case=ignore_case,
        )
    except (ValueError, FileNotFoundError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2

    if result.rows:
        print(format_table(result.rows, columns, args.max_width))
    else:
        print("No rows matched.")

    shown = min(len(result.rows), args.limit)
    suffix = " (truncated)" if result.matched_count > shown else ""
    print(f"\nMatched rows: {result.matched_count}; displayed: {shown}{suffix}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())