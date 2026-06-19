# Metadata Item Filter — Design Spec
Date: 2026-06-19

## Problem

`sync-metadata-playlists` currently filters which *groups* (playlists) get synced, but has no way to filter which individual *files/episodes* within a group are included in each generated playlist. Users need to be able to say "only include unwatched episodes" or "only episodes whose MAL status is Watching" without writing code.

## Goal

- Add per-item filtering to `sync-metadata-playlists` via a new `--item-filter` CLI argument.
- Back it with a reusable `MetadataItemFilter` class in `plex_db_tool/item_filter.py`, importable by other scripts in `misc_scripts`.

---

## Module: `plex_db_tool/item_filter.py`

### Condition types

```python
class ComparisonOp(Enum):
    EQ = "="
    NEQ = "!="
    LT = "<"
    LTE = "<="
    GT = ">"
    GTE = ">="

@dataclass(frozen=True)
class NumericCondition:
    op: ComparisonOp
    value: int

@dataclass(frozen=True)
class DateCondition:
    op: ComparisonOp
    value: datetime
    date_only: bool = False   # drives same-calendar-day equality for date-only inputs

@dataclass(frozen=True)
class StringSetCondition:
    op: ComparisonOp          # EQ = value must be in set, NEQ = must not be in set
    values: frozenset[str]
```

### `MetadataItemFilter` dataclass

```python
@dataclass
class MetadataItemFilter:
    watch_status: Optional[StringSetCondition] = None
    mal_status: Optional[StringSetCondition] = None
    seasons: List[NumericCondition] = field(default_factory=list)
    episodes: List[NumericCondition] = field(default_factory=list)
    modified: List[DateCondition] = field(default_factory=list)
    aired: List[DateCondition] = field(default_factory=list)
```

`None` / empty list means the criterion is inactive (passes everything).

### `matches(file_info, group_data) -> bool`

Returns `True` only when all active criteria pass. Evaluation order:

1. **watch_status** — calls `classify_file_watch_status(file_info, group_data)` which checks (in order): `episode_watched`, `plex_watch_status.watched / view_offset > 0`, then MAL `my_watched_episodes` vs `episode` number. Yields `"watched"`, `"watched_partial"`, or `"unwatched"`. Tests the result against the `StringSetCondition`.
2. **mal_status** — reads `file_info["myanimelist_watch_status"]["my_status"]`, falls back to `group_data["myanimelist_watch_status"]["my_status"]`. Normalised to lowercase/stripped before comparison.
3. **seasons** — reads `file_info["season"]` as int; if missing/None the condition fails (item excluded).
4. **episodes** — reads `file_info["episode"]` as int; list values use the maximum episode number. If missing/None and conditions are present, item is excluded.
5. **modified** — reads `file_info["modified_time"]` (unix timestamp → datetime).
6. **aired** — reads `file_info["aired_at"]` (ISO string or unix timestamp → datetime).

### `MetadataItemFilterParser`

A class with a single classmethod `parse(expression: str) -> MetadataItemFilter`.

**Expression syntax** — space- or comma-separated `field op value` tokens:

| Token example | Meaning |
|---|---|
| `watch_status=unwatched` | watch status must be "unwatched" |
| `watch_status!=watched` | watch status must not be "watched" |
| `mal_status=watching,completed` | MAL status must be in {watching, completed} |
| `episode>=5` | episode number ≥ 5 |
| `season=1..2` | season between 1 and 2 (expands to `>=1, <=2`) |
| `modified>=2026-01-01` | file modified on or after 2026-01-01 |
| `aired<2026-06-01` | aired before 2026-06-01 |

Rules:
- Multiple tokens build one `MetadataItemFilter`.
- A later token for the same field overwrites the earlier one (no silent AND-stacking on the same field).
- Range shorthand `start..end` on numeric/date fields expands to `[GTE(start), LTE(end)]`, consistent with the existing `parse_numeric_conditions` / `parse_modified_conditions` patterns.
- Unknown field names raise a `ValueError` with a clear message listing valid fields.

---

## Helpers moved from `sync_metadata_playlists.py` → `item_filter.py`

The following helpers are needed by `item_filter.py` and are moved there. `sync_metadata_playlists.py` imports them back from `item_filter`:

- `is_episode_already_watched(file_info, group)`
- `is_watching_mal_status(status)`
- `safe_int(value)`
- `normalize_path_key(path_value)`
- `parse_numeric_conditions(expression, argument_name)`
- `parse_numeric_expression(expression, argument_name)`
- `parse_modified_conditions(expression)`
- `parse_modified_expression(expression)`
- `parse_smart_datetime(value)`
- `normalize_datetime(value)`
- `matches_numeric_expression(value, operator, target_value)`
- `matches_modified_expression(actual, operator, target_value, is_date_only)`

`build_episode_identity_key` and `sort_episode_files_for_playlist` stay in `sync_metadata_playlists.py` as they are not needed by the filter.

---

## Integration in `sync_metadata_playlists.py`

### New CLI argument

```
--item-filter EXPR
```

Parsed once at startup by `MetadataItemFilterParser.parse(args.item_filter)` into a `MetadataItemFilter`. `None` (not provided) means no item-level filtering.

### Changed call sites

`resolve_group_metadata_item_ids` gains an optional parameter:

```python
def resolve_group_metadata_item_ids(
    group, target_indexes, path_index, matcher,
    item_filter: Optional[MetadataItemFilter] = None,
) -> Tuple[List[MediaRecord], List[str]]:
    files = group["files"]
    if item_filter is not None:
        group_data = group.get("group_data") or {}
        files = [f for f in files if item_filter.matches(f, group_data)]
    ...
```

`plan_group_playlists` accepts and threads through `item_filter`. `run()` parses it from `args.item_filter` and passes it in.

---

## Error handling

- `MetadataItemFilterParser.parse` raises `ValueError` with a descriptive message for unknown fields, bad operators, unparseable values, or invalid ranges.
- `run()` catches the `ValueError` and exits with a user-readable error (no stack trace).
- A `None` field value in `file_info` when a numeric/date condition is active causes `matches()` to return `False` (item is excluded, not an error).

---

## Out of scope

- No changes to other commands (`transfer_playlists`, `transfer_watch_status`, etc.) — they can adopt `MetadataItemFilter` in future PRs.
- No support for combining two `--item-filter` flags; a single expression string is sufficient.
- No `aired_at` field normalisation beyond what the existing `parse_smart_datetime` already supports.
