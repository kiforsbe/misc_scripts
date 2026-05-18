from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class TableColumn:
    name: str
    declared_type: str
    not_null: bool
    default_value: Optional[str]
    primary_key_position: int


@dataclass
class PlexSchema:
    media_parts_columns: Dict[str, TableColumn]
    metadata_item_views_columns: Dict[str, TableColumn]
    metadata_item_settings_columns: Dict[str, TableColumn]
    custom_channels_columns: Dict[str, TableColumn]
    play_queues_columns: Dict[str, TableColumn]
    play_queue_items_columns: Dict[str, TableColumn]
    play_queue_generators_columns: Dict[str, TableColumn]

    @property
    def size_column(self) -> Optional[str]:
        for candidate in ("size", "file_size", "total_size"):
            if candidate in self.media_parts_columns:
                return candidate
        return None

    @property
    def duration_column(self) -> Optional[str]:
        return "duration" if "duration" in self.media_parts_columns else None

    @property
    def supports_playlists(self) -> bool:
        return bool(
            self.play_queues_columns
            and (
                self.play_queue_generators_columns
                or (self.custom_channels_columns and self.play_queue_items_columns)
            )
        )


@dataclass
class ParsedIdentity:
    title_key: Optional[str]
    season: Optional[int]
    episode: Optional[int]


@dataclass
class MediaRecord:
    metadata_item_id: int
    guid: str
    metadata_type: Optional[int]
    title: Optional[str]
    year: Optional[int]
    item_index: Optional[int]
    originally_available_at: Optional[int]
    file_path: str
    library_section_id: Optional[int]
    library_section_name: Optional[str]
    basename: str
    basename_key: str
    file_size: Optional[int]
    duration: Optional[int]
    parent_title: Optional[str]
    parent_index: Optional[int]
    parent_guid: Optional[str]
    grandparent_title: Optional[str]
    grandparent_guid: Optional[str]
    parsed_identity: ParsedIdentity


@dataclass
class WatchHistory:
    guid: str
    watch_count: int
    last_viewed_at: Optional[int]
    row_ids: List[int]


@dataclass
class MatchCandidate:
    target: MediaRecord
    confidence: float
    reason: str


@dataclass
class MatchResult:
    source: MediaRecord
    source_history: WatchHistory
    status: str
    confidence: float
    reason: str
    target: Optional[MediaRecord]
    target_history: Optional[WatchHistory]
    notes: List[str]
    dry_run_status: str = "unknown"
    account_status: str = "not_applicable"
    library_status: str = "not_requested"


@dataclass
class PlannedMutation:
    action: str
    target_guid: str
    details: Dict[str, Any]


@dataclass(frozen=True)
class TableColumnSpec:
    name: str
    width: Optional[int] = None


@dataclass(frozen=True)
class PlexLibrarySection:
    id: int
    name: str
    section_type: Optional[int]
    agent: Optional[str]
    scanner: Optional[str]
    language: Optional[str]
    public: Optional[int]


@dataclass(frozen=True)
class PlexAccount:
    id: int
    name: str
    default_audio_language: Optional[str]
    default_subtitle_language: Optional[str]
    auto_select_audio: Optional[int]
    auto_select_subtitle: Optional[int]


@dataclass(frozen=True)
class PlexPlaylistItem:
    play_queue_item_id: Optional[int]
    metadata_item_id: Optional[int]
    order_value: float
    media: Optional[MediaRecord]
    title: Optional[str]
    library_section_name: Optional[str]
    in_scope: bool
    notes: List[str]

    @property
    def display_label(self) -> str:
        if self.media and self.media.basename:
            return self.media.basename
        if self.title:
            return self.title
        if self.metadata_item_id is not None:
            return f"metadata:{self.metadata_item_id}"
        return "unknown item"


@dataclass(frozen=True)
class PlexPlaylist:
    id: int
    name: str
    description: Optional[str]
    added_at: Optional[int]
    play_queue_id: Optional[int]
    account_id: Optional[int]
    items: List[PlexPlaylistItem]
    storage_model: str = "custom"

    @property
    def scoped_items(self) -> List[PlexPlaylistItem]:
        return [item for item in self.items if item.in_scope]

    @property
    def is_empty_in_scope(self) -> bool:
        return not self.scoped_items


@dataclass(frozen=True)
class PlaylistMatchResult:
    source_item: PlexPlaylistItem
    status: str
    target: Optional[MediaRecord]
    confidence: float
    reason: str
    notes: List[str]


@dataclass
class PlaylistTransferPlan:
    source_playlist: PlexPlaylist
    target_playlist_name: Optional[str]
    action: str
    status: str
    source_item_count: int
    matched_items: List[PlaylistMatchResult]
    transfer_items: List[MediaRecord]
    unmatched_items: List[PlaylistMatchResult]
    existing_target_playlist: Optional[PlexPlaylist]
    notes: List[str]

    @property
    def matched_item_count(self) -> int:
        return len(self.matched_items)

    @property
    def transfer_item_count(self) -> int:
        return len(self.transfer_items)

    @property
    def unmatched_item_count(self) -> int:
        return len(self.unmatched_items)