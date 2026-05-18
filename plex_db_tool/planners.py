from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from .infrastructure import PlexFilenameParser
from .models import (
    MatchCandidate,
    MatchResult,
    MediaRecord,
    PlannedMutation,
    PlaylistMatchResult,
    PlaylistTransferPlan,
    PlexPlaylist,
    PlexPlaylistItem,
    PlexSchema,
    WatchHistory,
)


class PlexMatcher:
    MATCH_THRESHOLDS = {
        "strict": 0.8,
        "balanced": 0.65,
        "loose": 0.5,
    }

    def __init__(self, match_mode: str, min_confidence: float) -> None:
        self.match_mode = match_mode
        self.min_confidence = min_confidence

    @staticmethod
    def index_target_inventory(target_inventory: Iterable[MediaRecord]) -> Dict[str, List[MediaRecord]]:
        by_basename: Dict[str, List[MediaRecord]] = defaultdict(list)
        for record in target_inventory:
            by_basename[record.basename_key].append(record)
        return by_basename

    def select_best_candidate(
        self,
        source: MediaRecord,
        candidates: Sequence[MediaRecord],
    ) -> Optional[MatchCandidate]:
        if not candidates:
            return None

        scored: List[MatchCandidate] = []
        source_title_key = PlexFilenameParser.normalize_title(source.title) or source.parsed_identity.title_key
        for candidate in candidates:
            confidence = 0.0
            reasons: List[str] = []

            if candidate.basename_key == source.basename_key:
                confidence += 0.55
                reasons.append("basename")

            if source.file_size is not None and candidate.file_size == source.file_size:
                confidence += 0.2
                reasons.append("file_size")

            if source.duration is not None and candidate.duration == source.duration:
                confidence += 0.1
                reasons.append("duration")

            candidate_title_key = PlexFilenameParser.normalize_title(candidate.title) or candidate.parsed_identity.title_key
            if source_title_key and candidate_title_key and source_title_key == candidate_title_key:
                confidence += 0.1
                reasons.append("title")

            if source.year is not None and candidate.year == source.year:
                confidence += 0.05
                reasons.append("year")

            if source.parsed_identity.season is not None and candidate.parsed_identity.season == source.parsed_identity.season:
                confidence += 0.08
                reasons.append("season")

            if source.parsed_identity.episode is not None and candidate.parsed_identity.episode == source.parsed_identity.episode:
                confidence += 0.12
                reasons.append("episode")

            scored.append(MatchCandidate(candidate, min(confidence, 1.0), "+".join(reasons) or "weak"))

        scored.sort(key=lambda item: item.confidence, reverse=True)
        top = scored[0]
        if len(scored) > 1 and abs(scored[0].confidence - scored[1].confidence) < 0.05:
            return None

        if top.confidence < self.MATCH_THRESHOLDS[self.match_mode]:
            return None
        return top

    def find_match(
        self,
        source: MediaRecord,
        target_indexes: Dict[str, List[MediaRecord]],
    ) -> Tuple[Optional[MediaRecord], float, str, List[str]]:
        notes: List[str] = []
        basename_candidates = list(target_indexes.get(source.basename_key, []))
        if not basename_candidates:
            return None, 0.0, "basename_exact_missing", notes

        candidate = self.select_best_candidate(source, basename_candidates)
        if candidate and candidate.confidence >= self.min_confidence:
            return candidate.target, candidate.confidence, candidate.reason, notes

        notes.append(f"basename candidates: {len(basename_candidates)}")
        return None, 0.0, "basename_exact_ambiguous", notes

    def collect_matches(
        self,
        source_inventory: Sequence[MediaRecord],
        source_history: Dict[str, WatchHistory],
        target_inventory: Sequence[MediaRecord],
        target_history: Dict[str, WatchHistory],
    ) -> List[MatchResult]:
        target_indexes = self.index_target_inventory(target_inventory)
        results: List[MatchResult] = []

        for source in source_inventory:
            history = source_history.get(source.guid)
            if not history or history.watch_count <= 0:
                continue

            target, confidence, reason, notes = self.find_match(source, target_indexes)
            if not target:
                results.append(
                    MatchResult(
                        source=source,
                        source_history=history,
                        status="unmatched",
                        confidence=confidence,
                        reason=reason,
                        target=None,
                        target_history=None,
                        notes=notes,
                        dry_run_status="unmatched",
                    )
                )
                continue

            results.append(
                MatchResult(
                    source=source,
                    source_history=history,
                    status="matched",
                    confidence=confidence,
                    reason=reason,
                    target=target,
                    target_history=target_history.get(target.guid, WatchHistory(target.guid, 0, None, [])),
                    notes=notes,
                    dry_run_status="matched",
                )
            )
        return results


class PlexMutationPlanner:
    def __init__(self, schema: PlexSchema, account_id: Optional[int], conflict_policy: str) -> None:
        self.schema = schema
        self.account_id = account_id
        self.conflict_policy = conflict_policy

    def supports_item_settings(self) -> bool:
        return bool(self.schema.metadata_item_settings_columns)

    def build_item_settings_values(
        self,
        guid: str,
        view_count: int,
        last_viewed_at: Optional[int],
    ) -> Optional[Dict[str, Any]]:
        if not self.supports_item_settings():
            return None

        timestamp = int(datetime.now().timestamp())
        return {
            "account_id": self.account_id,
            "guid": guid,
            "view_count": view_count,
            "last_viewed_at": last_viewed_at,
            "created_at": timestamp,
            "updated_at": timestamp,
            "changed_at": timestamp,
        }

    def build_view_refresh_values(
        self,
        target: MediaRecord,
        viewed_at: Optional[int],
    ) -> Dict[str, Any]:
        return {
            "account_id": self.account_id,
            "guid": target.guid,
            "metadata_type": target.metadata_type,
            "library_section_id": target.library_section_id,
            "grandparent_title": target.grandparent_title,
            "parent_index": target.parent_index,
            "parent_title": target.parent_title,
            "index": target.item_index,
            "title": target.title,
            "viewed_at": viewed_at,
            "grandparent_guid": target.grandparent_guid,
            "originally_available_at": target.originally_available_at,
            "view_type": 0,
        }

    def requires_account_id(self) -> bool:
        column = self.schema.metadata_item_views_columns.get("account_id")
        return bool(column and column.not_null and column.default_value is None)

    def resolve_insert_defaults(self, target: MediaRecord, viewed_at: int) -> Tuple[Optional[Dict[str, Any]], str, Optional[str]]:
        values: Dict[str, Any] = {}
        columns = self.schema.metadata_item_views_columns
        account_status = "not_needed"
        if "guid" in columns:
            values["guid"] = target.guid
        if "viewed_at" in columns:
            values["viewed_at"] = viewed_at
        if "account_id" in columns:
            if self.account_id is None and columns["account_id"].not_null and columns["account_id"].default_value is None:
                return None, "missing_required", "Target metadata_item_views requires account_id; pass --account-id."
            if self.account_id is not None:
                values["account_id"] = self.account_id
                account_status = "provided"
        if "parent_guid" in columns and target.parent_guid is not None:
            values["parent_guid"] = target.parent_guid
        if "grandparent_guid" in columns and target.grandparent_guid is not None:
            values["grandparent_guid"] = target.grandparent_guid

        unsupported_required = []
        for column in columns.values():
            if column.primary_key_position:
                continue
            if column.name in values:
                continue
            if column.not_null and column.default_value is None:
                unsupported_required.append(column.name)
        if unsupported_required:
            return None, account_status, (
                "Target metadata_item_views has unsupported required columns: " + ", ".join(sorted(unsupported_required))
            )
        return values, account_status, None

    def plan_mutations(self, matches: Sequence[MatchResult]) -> List[PlannedMutation]:
        mutations: List[PlannedMutation] = []
        for match in matches:
            if match.status != "matched" or not match.target or not match.target_history:
                continue

            source_history = match.source_history
            target_history = match.target_history
            delta = source_history.watch_count - target_history.watch_count
            resulting_watch_count = target_history.watch_count
            resulting_last_viewed_at = target_history.last_viewed_at
            source_last_viewed_at = source_history.last_viewed_at or 0
            target_last_viewed_at = target_history.last_viewed_at or 0
            target_count_ahead = target_history.watch_count > source_history.watch_count
            target_timestamp_ahead = (
                target_history.watch_count == source_history.watch_count
                and target_last_viewed_at > source_last_viewed_at
            )
            needs_target_change = False
            match.account_status = "not_needed"
            match.dry_run_status = "in_sync"

            if target_count_ahead or target_timestamp_ahead:
                match.dry_run_status = "target_ahead"

            if self.conflict_policy == "skip" and target_history.watch_count > 0:
                if delta > 0 or (
                    source_history.last_viewed_at
                    and source_history.watch_count == target_history.watch_count
                    and source_last_viewed_at != target_last_viewed_at
                ):
                    match.dry_run_status = "skipped_conflict"
                continue

            if self.conflict_policy == "overwrite" and target_count_ahead:
                viewed_at = source_history.last_viewed_at or int(datetime.now().timestamp())
                insert_values, account_status, error_message = self.resolve_insert_defaults(match.target, viewed_at)
                match.account_status = account_status
                if error_message:
                    match.dry_run_status = "missing_required_account" if account_status == "missing_required" else "blocked_required_columns"
                    match.notes.append(error_message)
                    continue
                mutations.append(
                    PlannedMutation(
                        action="replace_views",
                        target_guid=match.target.guid,
                        details={
                            "row_ids": list(target_history.row_ids),
                            "count": source_history.watch_count,
                            "values": insert_values,
                            "source_watch_count": source_history.watch_count,
                            "target_watch_count": target_history.watch_count,
                        },
                    )
                )
                resulting_watch_count = source_history.watch_count
                resulting_last_viewed_at = viewed_at
                match.dry_run_status = "ready_overwrite"
                needs_target_change = True
            elif self.conflict_policy == "overwrite" and target_timestamp_ahead and target_history.row_ids:
                mutations.append(
                    PlannedMutation(
                        action="update_latest_view",
                        target_guid=match.target.guid,
                        details={
                            "row_id": target_history.row_ids[-1],
                            "viewed_at": source_history.last_viewed_at,
                        },
                    )
                )
                resulting_watch_count = target_history.watch_count
                resulting_last_viewed_at = source_history.last_viewed_at
                match.dry_run_status = "ready_overwrite"
                needs_target_change = True
            elif delta > 0:
                viewed_at = source_history.last_viewed_at or int(datetime.now().timestamp())
                insert_values, account_status, error_message = self.resolve_insert_defaults(match.target, viewed_at)
                match.account_status = account_status
                if error_message:
                    match.dry_run_status = "missing_required_account" if account_status == "missing_required" else "blocked_required_columns"
                    match.notes.append(error_message)
                    continue
                mutations.append(
                    PlannedMutation(
                        action="insert_views",
                        target_guid=match.target.guid,
                        details={
                            "count": delta,
                            "values": insert_values,
                            "source_watch_count": source_history.watch_count,
                            "target_watch_count": target_history.watch_count,
                        },
                    )
                )
                resulting_watch_count = source_history.watch_count
                resulting_last_viewed_at = viewed_at
                match.dry_run_status = "ready_insert"
                needs_target_change = True
            elif (
                source_history.last_viewed_at
                and target_history.watch_count > 0
                and source_history.watch_count == target_history.watch_count
                and (target_history.last_viewed_at or 0) < source_history.last_viewed_at
                and target_history.row_ids
            ):
                mutations.append(
                    PlannedMutation(
                        action="update_latest_view",
                        target_guid=match.target.guid,
                        details={
                            "row_id": target_history.row_ids[-1],
                            "viewed_at": source_history.last_viewed_at,
                        },
                    )
                )
                resulting_watch_count = target_history.watch_count
                resulting_last_viewed_at = source_history.last_viewed_at
                match.dry_run_status = "ready_update"
                needs_target_change = True

            if not needs_target_change:
                continue

            settings_values = self.build_item_settings_values(
                match.target.guid,
                resulting_watch_count,
                resulting_last_viewed_at,
            )
            if settings_values is not None:
                mutations.append(
                    PlannedMutation(
                        action="upsert_settings",
                        target_guid=match.target.guid,
                        details=settings_values,
                    )
                )
            mutations.append(
                PlannedMutation(
                    action="refresh_view_rows",
                    target_guid=match.target.guid,
                    details=self.build_view_refresh_values(match.target, resulting_last_viewed_at),
                )
            )
        return mutations


class PlexPlaylistPlanner:
    def __init__(
        self,
        matcher: PlexMatcher,
        conflict_policy: str,
        include_empty_playlists: bool,
    ) -> None:
        self.matcher = matcher
        self.conflict_policy = conflict_policy
        self.include_empty_playlists = include_empty_playlists

    @staticmethod
    def resolve_unique_name(existing_names: Sequence[str], desired_name: str) -> str:
        existing_keys = {name.casefold() for name in existing_names}
        base_name = desired_name
        if base_name.casefold() not in existing_keys:
            return base_name

        first_candidate = f"{base_name} (Imported)"
        if first_candidate.casefold() not in existing_keys:
            return first_candidate

        suffix = 2
        while True:
            candidate = f"{base_name} (Imported {suffix})"
            if candidate.casefold() not in existing_keys:
                return candidate
            suffix += 1

    @staticmethod
    def choose_existing_playlist(
        playlists: Sequence[PlexPlaylist],
        name: str,
        target_account_id: Optional[int],
    ) -> Tuple[Optional[PlexPlaylist], List[PlexPlaylist]]:
        candidates = [playlist for playlist in playlists if playlist.name.casefold() == name.casefold()]
        if not candidates:
            return None, []

        if target_account_id is not None:
            exact_account_matches = [
                playlist
                for playlist in candidates
                if playlist.account_id == target_account_id
            ]
            if exact_account_matches:
                preferred_pool = exact_account_matches
            else:
                stale_candidates = [playlist for playlist in candidates if playlist.account_id is None]
                return None, stale_candidates
        else:
            preferred_pool = candidates

        selected = max(
            preferred_pool,
            key=lambda playlist: (
                playlist.added_at or 0,
                playlist.play_queue_id or 0,
                playlist.id,
            ),
        )
        stale_candidates = [playlist for playlist in candidates if playlist.id != selected.id and playlist.account_id is None]
        return selected, stale_candidates

    def build_item_match(
        self,
        source_item: PlexPlaylistItem,
        target_indexes: Dict[str, List[MediaRecord]],
        full_indexes: Dict[str, List[MediaRecord]],
        has_target_library_filter: bool,
    ) -> PlaylistMatchResult:
        notes = list(source_item.notes)
        if source_item.media is None:
            return PlaylistMatchResult(
                source_item=source_item,
                status="unmatched",
                target=None,
                confidence=0.0,
                reason="source_media_missing",
                notes=notes,
            )

        target, confidence, reason, match_notes = self.matcher.find_match(source_item.media, target_indexes)
        notes.extend(match_notes)
        if target is not None:
            return PlaylistMatchResult(
                source_item=source_item,
                status="matched",
                target=target,
                confidence=confidence,
                reason=reason,
                notes=notes,
            )

        if has_target_library_filter:
            full_candidates = list(full_indexes.get(source_item.media.basename_key, []))
            full_match = self.matcher.select_best_candidate(source_item.media, full_candidates) if full_candidates else None
            if full_match is not None:
                notes.append("matching target exists outside the selected target library")
                if full_match.target.library_section_name:
                    notes.append(f"matching target library: {full_match.target.library_section_name}")

        return PlaylistMatchResult(
            source_item=source_item,
            status="unmatched",
            target=None,
            confidence=confidence,
            reason=reason,
            notes=notes,
        )

    def plan_transfers(
        self,
        source_playlists: Sequence[PlexPlaylist],
        target_playlists: Sequence[PlexPlaylist],
        target_inventory: Sequence[MediaRecord],
        target_inventory_all: Sequence[MediaRecord],
        target_account_id: Optional[int] = None,
        has_target_library_filter: bool = False,
    ) -> Tuple[List[PlaylistTransferPlan], List[PlannedMutation]]:
        target_indexes = self.matcher.index_target_inventory(target_inventory)
        full_indexes = self.matcher.index_target_inventory(target_inventory_all)
        reserved_names = [playlist.name for playlist in target_playlists]

        plans: List[PlaylistTransferPlan] = []
        mutations: List[PlannedMutation] = []

        for source_playlist in source_playlists:
            scoped_items = source_playlist.scoped_items
            notes: List[str] = []
            if not self.include_empty_playlists and not scoped_items:
                notes.append("empty in the selected source library scope; excluded by default")
                plans.append(
                    PlaylistTransferPlan(
                        source_playlist=source_playlist,
                        target_playlist_name=None,
                        action="skip_empty",
                        status="skipped_empty",
                        source_item_count=0,
                        matched_items=[],
                        transfer_items=[],
                        unmatched_items=[],
                        existing_target_playlist=None,
                        notes=notes,
                    )
                )
                continue

            matched_items: List[PlaylistMatchResult] = []
            unmatched_items: List[PlaylistMatchResult] = []
            for source_item in scoped_items:
                match_result = self.build_item_match(
                    source_item,
                    target_indexes,
                    full_indexes,
                    has_target_library_filter,
                )
                if match_result.status == "matched":
                    matched_items.append(match_result)
                else:
                    unmatched_items.append(match_result)

            existing_target_playlist, stale_target_playlists = self.choose_existing_playlist(
                target_playlists,
                source_playlist.name,
                target_account_id,
            )
            transfer_items = [item.target for item in matched_items if item.target is not None]
            action = "create_new"
            status = "ready_create"
            target_playlist_name = source_playlist.name

            if not transfer_items:
                notes.append("no playlist items matched in the selected target library scope")
                plans.append(
                    PlaylistTransferPlan(
                        source_playlist=source_playlist,
                        target_playlist_name=None,
                        action="skip_unmatched",
                        status="no_transferable_items",
                        source_item_count=len(scoped_items),
                        matched_items=matched_items,
                        transfer_items=[],
                        unmatched_items=unmatched_items,
                        existing_target_playlist=existing_target_playlist,
                        notes=notes,
                    )
                )
                continue

            if stale_target_playlists:
                notes.append(
                    f"will remove {len(stale_target_playlists)} stale null-account target playlist(s)"
                )
                for stale_playlist in stale_target_playlists:
                    mutations.append(
                        PlannedMutation(
                            action="delete_playlist",
                            target_guid=f"playlist:{stale_playlist.name}:{stale_playlist.id}",
                            details={
                                "playlist_id": stale_playlist.id,
                                "play_queue_id": stale_playlist.play_queue_id,
                                "storage_model": stale_playlist.storage_model,
                            },
                        )
                    )

            if existing_target_playlist is None:
                mutations.append(
                    PlannedMutation(
                        action="create_playlist",
                        target_guid=f"playlist:{source_playlist.name}",
                        details={
                            "name": target_playlist_name,
                            "description": source_playlist.description,
                            "added_at": source_playlist.added_at,
                            "account_id": target_account_id,
                            "metadata_type": transfer_items[0].metadata_type if transfer_items else None,
                            "metadata_item_ids": [item.metadata_item_id for item in transfer_items],
                            "storage_model": "metadata",
                        },
                    )
                )
            elif self.conflict_policy == "skip":
                action = "skip_existing"
                status = "skipped_conflict"
                notes.append("target playlist already exists; conflict policy is skip")
                transfer_items = []
            elif self.conflict_policy == "unique":
                action = "create_unique"
                target_playlist_name = self.resolve_unique_name(reserved_names, source_playlist.name)
                reserved_names.append(target_playlist_name)
                mutations.append(
                    PlannedMutation(
                        action="create_playlist",
                        target_guid=f"playlist:{target_playlist_name}",
                        details={
                            "name": target_playlist_name,
                            "description": source_playlist.description,
                            "added_at": source_playlist.added_at,
                            "account_id": target_account_id,
                            "metadata_type": transfer_items[0].metadata_type if transfer_items else None,
                            "metadata_item_ids": [item.metadata_item_id for item in transfer_items],
                            "storage_model": "metadata",
                        },
                    )
                )
            elif self.conflict_policy == "replace":
                action = "replace_existing"
                status = "ready_replace"
                if existing_target_playlist.storage_model == "custom" and existing_target_playlist.play_queue_id is None:
                    status = "blocked_missing_target_queue"
                    notes.append("target playlist exists but has no play queue row")
                    transfer_items = []
                else:
                    mutations.append(
                        PlannedMutation(
                            action="replace_playlist_items",
                            target_guid=f"playlist:{existing_target_playlist.name}",
                            details={
                                "play_queue_id": existing_target_playlist.play_queue_id,
                                "playlist_id": existing_target_playlist.id,
                                "storage_model": existing_target_playlist.storage_model,
                                "added_at": source_playlist.added_at,
                                "metadata_item_ids": [item.metadata_item_id for item in transfer_items],
                            },
                        )
                    )
            else:
                action = "merge_existing"
                status = "ready_merge"
                if existing_target_playlist.storage_model == "custom" and existing_target_playlist.play_queue_id is None:
                    status = "blocked_missing_target_queue"
                    notes.append("target playlist exists but has no play queue row")
                    transfer_items = []
                else:
                    existing_target_ids = {
                        item.media.metadata_item_id
                        for item in existing_target_playlist.items
                        if item.media is not None
                    }
                    unique_transfer_items = [
                        item
                        for item in transfer_items
                        if item.metadata_item_id not in existing_target_ids
                    ]
                    duplicate_count = len(transfer_items) - len(unique_transfer_items)
                    if duplicate_count:
                        notes.append(f"{duplicate_count} matched items already exist in the target playlist")
                    transfer_items = unique_transfer_items
                    if transfer_items:
                        mutations.append(
                            PlannedMutation(
                                action="merge_playlist_items",
                                target_guid=f"playlist:{existing_target_playlist.name}",
                                details={
                                    "play_queue_id": existing_target_playlist.play_queue_id,
                                    "playlist_id": existing_target_playlist.id,
                                    "storage_model": existing_target_playlist.storage_model,
                                    "metadata_item_ids": [item.metadata_item_id for item in transfer_items],
                                },
                            )
                        )
                    else:
                        status = "in_sync"

            plans.append(
                PlaylistTransferPlan(
                    source_playlist=source_playlist,
                    target_playlist_name=target_playlist_name,
                    action=action,
                    status=status,
                    source_item_count=len(scoped_items),
                    matched_items=matched_items,
                    transfer_items=transfer_items,
                    unmatched_items=unmatched_items,
                    existing_target_playlist=existing_target_playlist,
                    notes=notes,
                )
            )

        return plans, mutations