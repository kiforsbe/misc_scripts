import argparse
import binascii
import hashlib
import importlib
import os
import re
import shutil
import sys
import time
import uuid
import warnings
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    tqdm = None
    TQDM_AVAILABLE = False

LIBTORRENT_IMPORT_ERROR: Optional[Exception] = None
try:
    libtorrent = importlib.import_module('libtorrent')
    LIBTORRENT_AVAILABLE = True
except ImportError as exc:
    libtorrent = None
    LIBTORRENT_AVAILABLE = False
    LIBTORRENT_IMPORT_ERROR = exc

try:
    bencodepy = importlib.import_module('bencodepy')
    BENCODEPY_AVAILABLE = True
except ImportError:
    bencodepy = None
    BENCODEPY_AVAILABLE = False


def _safe_console_print(text: str = "") -> None:
    """Print text with a best-effort encoding fallback for Windows consoles."""
    encoding = getattr(sys.stdout, 'encoding', None) or 'utf-8'
    try:
        print(text)
    except UnicodeEncodeError:
        safe_text = text.encode(encoding, errors='replace').decode(encoding, errors='replace')
        print(safe_text)


def _call_libtorrent_without_deprecation_warnings(callback, *args, **kwargs):
    """Call libtorrent APIs while suppressing deprecation warning noise."""
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', DeprecationWarning)
        return callback(*args, **kwargs)


def _configure_libtorrent_session(session: Any) -> None:
    """Apply session settings through the current libtorrent API."""
    if hasattr(session, 'apply_settings'):
        session.apply_settings({
            'listen_interfaces': '0.0.0.0:6881,[::]:6881'
        })


def _create_add_torrent_params(torrent_info: Any, save_path: Path) -> Any:
    """Create add_torrent_params using the current libtorrent API."""
    params = libtorrent.add_torrent_params()
    params.ti = torrent_info
    params.save_path = str(save_path)
    if hasattr(libtorrent, 'torrent_flags') and hasattr(params, 'flags'):
        params.flags |= libtorrent.torrent_flags.sequential_download
    return params


def _apply_match_to_add_torrent_params(
    params: Any,
    priorities: List[int],
    rename_targets: Optional[Dict[int, str]] = None,
) -> None:
    """Preconfigure matched-file priorities and rename mapping before add_torrent()."""
    if hasattr(libtorrent, 'torrent_flags') and hasattr(params, 'flags'):
        params.flags |= libtorrent.torrent_flags.default_dont_download

    if priorities:
        params.file_priorities = priorities

    if rename_targets:
        for file_index, target_path in rename_targets.items():
            params.renamed_files[file_index] = target_path


def _enable_sequential_download(handle: Any) -> None:
    """Enable sequential download when supported by the current libtorrent build."""
    if hasattr(handle, 'set_flags') and hasattr(libtorrent, 'torrent_flags'):
        handle.set_flags(libtorrent.torrent_flags.sequential_download)
        return
    raise RuntimeError('Current libtorrent flags API is unavailable; sequential download cannot be enabled.')


def _prioritize_missing_torrent_pieces(handle: Any, piece_indexes: Set[int]) -> None:
    """Force libtorrent to request specific unresolved pieces for focused repair."""
    for piece_index in sorted(piece_indexes):
        try:
            handle.piece_priority(piece_index, 7)
        except Exception:
            pass

        try:
            handle.set_piece_deadline(piece_index, 0)
        except Exception:
            pass


def _format_libtorrent_error(error_value: Any) -> str:
    """Render a libtorrent error_code as readable text."""
    if not error_value:
        return ''

    try:
        error_message = error_value.message()
    except Exception:
        error_message = str(error_value)

    try:
        error_code = error_value.value()
    except Exception:
        error_code = None

    if error_code == 0:
        return ''
    if not error_message or error_message in {'No error', 'The operation completed successfully'}:
        return ''
    if error_code is None:
        return error_message
    return f"{error_message} ({error_code})"


def _get_libtorrent_status(handle: Any) -> Any:
    """Fetch torrent status with piece bitfields when the binding supports them."""
    if hasattr(libtorrent, 'status_flags_t'):
        try:
            status_flags = libtorrent.status_flags_t.query_pieces | libtorrent.status_flags_t.query_verified_pieces
            return handle.status(status_flags)
        except Exception:
            pass
    return handle.status()


def _get_libtorrent_file_piece_spans(
    torrent_info: Any,
    matched_index: Optional[int],
    matched_file_size: int,
) -> List[Tuple[int, int]]:
    """Return the overlapping byte count for each torrent piece covering the matched file."""
    if torrent_info is None or matched_index is None or matched_file_size <= 0:
        return []

    try:
        first_mapping = torrent_info.map_file(matched_index, 0, 1)
        last_mapping = torrent_info.map_file(matched_index, matched_file_size - 1, 1)
        piece_length = int(torrent_info.piece_length())
    except Exception:
        return []

    file_start = int(first_mapping.piece) * piece_length + int(first_mapping.start)
    file_end = file_start + matched_file_size
    first_piece = int(first_mapping.piece)
    last_piece = int(last_mapping.piece)

    spans = []
    for piece_index in range(first_piece, last_piece + 1):
        piece_start = piece_index * piece_length
        piece_end = piece_start + int(torrent_info.piece_size(piece_index))
        overlap_start = max(file_start, piece_start)
        overlap_end = min(file_end, piece_end)
        if overlap_end > overlap_start:
            spans.append((piece_index, overlap_end - overlap_start))

    return spans


def _get_libtorrent_valid_piece_indexes(status: Any) -> Set[int]:
    """Return the set of piece indexes libtorrent currently considers valid."""
    if status is None:
        return set()

    valid_piece_indexes = set()
    for attribute_name in ('pieces', 'verified_pieces'):
        try:
            piece_flags = getattr(status, attribute_name, None) or []
        except Exception:
            piece_flags = []

        for piece_index, is_valid in enumerate(piece_flags):
            if is_valid:
                valid_piece_indexes.add(piece_index)

    return valid_piece_indexes


def _build_libtorrent_piece_bitfield(total_piece_count: int, valid_piece_indexes: Set[int]) -> List[bool]:
    """Build a libtorrent-compatible piece bitfield from verified piece indexes."""
    if total_piece_count <= 0:
        return []

    piece_bitfield = [False] * total_piece_count
    for piece_index in valid_piece_indexes:
        if 0 <= piece_index < total_piece_count:
            piece_bitfield[piece_index] = True

    return piece_bitfield


def _estimate_missing_piece_repair_bytes(
    torrent_info: Any,
    file_piece_spans: List[Tuple[int, int]],
    baseline_valid_piece_indexes: Set[int],
) -> Tuple[int, int]:
    """Estimate repair download bytes by counting whole missing torrent pieces."""
    missing_piece_indexes = [piece_index for piece_index, _ in file_piece_spans if piece_index not in baseline_valid_piece_indexes]
    if not missing_piece_indexes:
        return 0, 0

    estimated_bytes = 0
    for piece_index in missing_piece_indexes:
        try:
            estimated_bytes += int(torrent_info.piece_size(piece_index))
        except Exception:
            pass

    return estimated_bytes, len(missing_piece_indexes)


def _get_libtorrent_file_ok_bytes(
    status: Any,
    file_piece_spans: List[Tuple[int, int]],
    matched_file_size: int,
    baseline_valid_piece_indexes: Optional[Set[int]] = None,
) -> Optional[int]:
    """Estimate how many bytes of the matched file are currently backed by valid pieces."""
    if matched_file_size <= 0 or not file_piece_spans:
        return None

    valid_piece_indexes = _get_libtorrent_valid_piece_indexes(status)
    if baseline_valid_piece_indexes:
        valid_piece_indexes.update(baseline_valid_piece_indexes)

    valid_bytes = 0
    for piece_index, overlap_bytes in file_piece_spans:
        if piece_index in valid_piece_indexes:
            valid_bytes += overlap_bytes

    return min(valid_bytes, matched_file_size)


def _format_byte_size(size_bytes: int) -> str:
    """Return a compact human-readable byte size string."""
    size_value = float(size_bytes)
    for unit in ('B', 'KiB', 'MiB', 'GiB', 'TiB'):
        if size_value < 1024.0 or unit == 'TiB':
            return f"{size_value:.2f} {unit}"
        size_value /= 1024.0


class TorrentFileCheckRepair:
    """Standalone torrent-backed verifier and repair tool."""

    def __init__(self, torrent_dir: str, verbose: int = 0):
        self.torrent_dir = str(Path(torrent_dir).resolve())
        self.verbose = verbose

    def _log(self, message: str, level: int = 1) -> None:
        if self.verbose >= level:
            _safe_console_print(message)

    def _create_progress_bar(self, filepath: str, total_bytes: Optional[int] = None, label: str = 'CRC'):
        if not (TQDM_AVAILABLE and self.verbose >= 1):
            return None

        display_name = os.path.basename(filepath)
        if len(display_name) > 36:
            display_name = display_name[:33] + '...'

        return tqdm(
            total=total_bytes,
            desc=f"{label} {display_name}",
            unit='B',
            unit_scale=True,
            unit_divisor=1024,
            leave=False,
            disable=False,
        )

    def _update_progress_bar(self, progress_bar, completed_bytes: int, total_bytes: Optional[int] = None) -> None:
        if progress_bar is None:
            return

        if total_bytes and (progress_bar.total is None or progress_bar.total != total_bytes):
            progress_bar.total = total_bytes

        if progress_bar.total is not None:
            completed_bytes = min(completed_bytes, int(progress_bar.total))

        progress_bar.n = max(0, completed_bytes)
        progress_bar.refresh()

    def _extract_crc_from_filename(self, filename: str) -> Optional[str]:
        match = re.search(r'\[([A-Fa-f0-9]{8})\]', filename)
        return match.group(1).upper() if match else None

    def _calculate_file_crc32(self, filepath: str, show_progress: bool = False) -> str:
        crc = 0
        file_progress = None
        try:
            if show_progress:
                file_progress = self._create_progress_bar(filepath, os.path.getsize(filepath), label='CRC')

            with open(filepath, 'rb') as source_file:
                while True:
                    chunk = source_file.read(8192)
                    if not chunk:
                        break
                    crc = binascii.crc32(chunk, crc)
                    if file_progress is not None:
                        file_progress.update(len(chunk))
        finally:
            if file_progress is not None:
                file_progress.close()

        return f"{crc & 0xffffffff:08X}"

    def _verify_file_crc(self, filepath: str, expected_crc: Optional[str] = None) -> Tuple[bool, str, str]:
        filename = os.path.basename(filepath)
        if expected_crc is None:
            expected_crc = self._extract_crc_from_filename(filename)

        if expected_crc is None:
            return False, 'N/A', 'N/A'

        try:
            actual_crc = self._calculate_file_crc32(filepath, show_progress=True)
            return expected_crc.upper() == actual_crc.upper(), expected_crc.upper(), actual_crc.upper()
        except Exception as exc:
            self._log(f"Error calculating CRC for {filename}: {exc}", 1)
            return False, expected_crc.upper(), 'ERROR'

    def _normalize_match_name(self, value: str) -> str:
        name = os.path.basename(value or '')
        name = re.sub(r'\.[^.]+$', '', name)
        name = re.sub(r'\[[A-Fa-f0-9]{8}\]', ' ', name)
        name = re.sub(r'[._\-\[\]\(\)]+', ' ', name)
        name = re.sub(r'\s+', ' ', name).strip().lower()
        return name

    def _filename_similarity(self, left: str, right: str) -> float:
        left_name = self._normalize_match_name(left)
        right_name = self._normalize_match_name(right)

        if not left_name or not right_name:
            return 0.0
        if left_name == right_name:
            return 1.0
        if left_name in right_name or right_name in left_name:
            return 0.95
        return SequenceMatcher(None, left_name, right_name).ratio()

    def _bdecode(self, payload: bytes) -> Any:
        if BENCODEPY_AVAILABLE:
            return bencodepy.decode(payload)

        def _decode_at(index: int) -> Tuple[Any, int]:
            token = payload[index:index + 1]
            if token == b'i':
                end = payload.index(b'e', index)
                return int(payload[index + 1:end]), end + 1
            if token == b'l':
                index += 1
                items = []
                while payload[index:index + 1] != b'e':
                    item, index = _decode_at(index)
                    items.append(item)
                return items, index + 1
            if token == b'd':
                index += 1
                items = {}
                while payload[index:index + 1] != b'e':
                    key, index = _decode_at(index)
                    value, index = _decode_at(index)
                    items[key] = value
                return items, index + 1
            if token.isdigit():
                colon_index = payload.index(b':', index)
                length = int(payload[index:colon_index])
                start = colon_index + 1
                end = start + length
                return payload[start:end], end
            raise ValueError(f'Invalid bencode token at index {index}')

        decoded, end_index = _decode_at(0)
        if end_index != len(payload):
            raise ValueError('Unexpected trailing data in torrent payload')
        return decoded

    def _decode_torrent_text(self, value: Any) -> str:
        if isinstance(value, bytes):
            return value.decode('utf-8', errors='replace')
        return str(value)

    def _read_torrent_metadata(self, torrent_path: str) -> Optional[Dict[str, Any]]:
        try:
            with open(torrent_path, 'rb') as torrent_file:
                raw_metadata = self._bdecode(torrent_file.read())
        except Exception as exc:
            self._log(f"Could not parse torrent '{torrent_path}': {exc}", 2)
            return None

        if not isinstance(raw_metadata, dict):
            return None

        info = raw_metadata.get(b'info')
        if not isinstance(info, dict):
            return None

        torrent_name = self._decode_torrent_text(info.get(b'name', Path(torrent_path).stem))
        files = []
        if b'files' in info and isinstance(info[b'files'], list):
            for file_entry in info[b'files']:
                if not isinstance(file_entry, dict):
                    continue
                path_parts = file_entry.get(b'path') or file_entry.get(b'path.utf-8')
                if not isinstance(path_parts, list):
                    continue
                relative_parts = [self._decode_torrent_text(part) for part in path_parts]
                relative_path = os.path.join(*relative_parts) if relative_parts else ''
                if not relative_path:
                    continue
                files.append({
                    'relative_path': relative_path,
                    'display_name': os.path.basename(relative_path),
                    'size': file_entry.get(b'length'),
                })
        else:
            files.append({
                'relative_path': torrent_name,
                'display_name': os.path.basename(torrent_name),
                'size': info.get(b'length'),
            })

        return {
            'torrent_path': torrent_path,
            'torrent_name': torrent_name,
            'files': files,
        }

    def _collect_candidate_torrent_files(self, failed_filepath: str) -> List[str]:
        candidates: List[str] = []
        seen = set()

        local_folder = Path(failed_filepath).parent
        if local_folder.exists():
            for torrent_path in sorted(local_folder.glob('*.torrent')):
                resolved = str(torrent_path.resolve())
                if resolved not in seen:
                    candidates.append(resolved)
                    seen.add(resolved)

        torrent_root = Path(self.torrent_dir)
        if torrent_root.exists():
            for torrent_path in sorted(torrent_root.rglob('*.torrent')):
                resolved = str(torrent_path.resolve())
                if resolved not in seen:
                    candidates.append(resolved)
                    seen.add(resolved)

        return candidates

    def _score_torrent_candidate(self, failed_filename: str, torrent_path: str) -> float:
        return self._filename_similarity(failed_filename, Path(torrent_path).stem)

    def _find_best_torrent_match(self, failed_filepath: str, filename: Optional[str] = None) -> Tuple[Optional[Dict[str, Any]], int]:
        failed_filename = filename or os.path.basename(failed_filepath)
        candidates = self._collect_candidate_torrent_files(failed_filepath)
        if not candidates:
            return None, 0

        ranked_candidates = sorted(
            candidates,
            key=lambda candidate: (
                1 if Path(candidate).parent == Path(failed_filepath).parent else 0,
                self._score_torrent_candidate(failed_filename, candidate),
            ),
            reverse=True,
        )

        likely_suspects = []
        remaining_candidates = []
        for index, candidate in enumerate(ranked_candidates):
            candidate_score = self._score_torrent_candidate(failed_filename, candidate)
            if index < 10 or candidate_score >= 0.45 or Path(candidate).parent == Path(failed_filepath).parent:
                likely_suspects.append(candidate)
            else:
                remaining_candidates.append(candidate)

        search_order = likely_suspects + remaining_candidates
        best_match = None
        inspected_count = 0

        for torrent_path in search_order:
            metadata = self._read_torrent_metadata(torrent_path)
            inspected_count += 1
            if not metadata:
                continue

            torrent_name_score = self._score_torrent_candidate(failed_filename, torrent_path)
            for file_entry in metadata['files']:
                file_score = self._filename_similarity(failed_filename, file_entry['display_name'])
                total_score = max(file_score, torrent_name_score * 0.85)
                if file_score >= 0.995:
                    total_score = 1.0

                candidate_match = {
                    'torrent_path': metadata['torrent_path'],
                    'torrent_name': metadata['torrent_name'],
                    'relative_path': file_entry['relative_path'],
                    'display_name': file_entry['display_name'],
                    'size': file_entry.get('size'),
                    'torrent_name_score': torrent_name_score,
                    'file_score': file_score,
                    'total_score': total_score,
                }
                if best_match is None or candidate_match['total_score'] > best_match['total_score']:
                    best_match = candidate_match

            if best_match and best_match['total_score'] >= 0.99:
                break

        if best_match and best_match['total_score'] >= 0.55:
            return best_match, inspected_count
        return None, inspected_count

    def _get_torrent_file_paths_in_order(self, torrent_path: str) -> List[str]:
        metadata = self._read_torrent_metadata(torrent_path)
        if not metadata:
            return []

        return [
            str(file_entry.get('relative_path', ''))
            for file_entry in metadata.get('files', [])
            if file_entry.get('relative_path')
        ]

    def _resolve_session_candidate_paths(self, root: Path, match: Dict[str, Any]) -> List[Path]:
        relative_path = Path(match['relative_path'])
        torrent_name = Path(match['torrent_name'])

        candidates = []
        seen = set()
        for candidate in (
            root / relative_path,
            root / torrent_name / relative_path,
            root / torrent_name.name / relative_path,
            root / relative_path.name,
        ):
            resolved_key = str(candidate)
            if resolved_key not in seen:
                candidates.append(candidate)
                seen.add(resolved_key)

        return candidates

    def _resolve_session_file_path(
        self,
        root: Path,
        torrent_name: str,
        relative_path: str,
        preferred_path: Optional[Path] = None,
        extra_roots: Optional[List[Path]] = None,
    ) -> Optional[Path]:
        if preferred_path is not None and preferred_path.exists():
            return preferred_path

        candidate_match = {
            'relative_path': relative_path,
            'torrent_name': torrent_name,
        }
        search_roots = [root]
        if extra_roots:
            search_roots.extend(extra_roots)

        for search_root in search_roots:
            for candidate in self._resolve_session_candidate_paths(search_root, candidate_match):
                if candidate.exists():
                    return candidate

        return None

    def _resolve_in_place_target_path(
        self,
        root: Path,
        torrent_name: str,
        relative_path: str,
        preferred_path: Optional[Path] = None,
    ) -> Path:
        existing_path = self._resolve_session_file_path(
            root,
            torrent_name,
            relative_path,
            preferred_path=preferred_path,
        )
        if existing_path is not None:
            return existing_path

        if preferred_path is not None:
            return preferred_path

        return root / Path(relative_path).name

    def _find_matched_torrent_file_index(self, torrent_info: Any, match: Dict[str, Any]) -> Tuple[Optional[int], List[int]]:
        try:
            file_paths = self._get_torrent_file_paths_in_order(str(match['torrent_path']))
        except Exception:
            return None, []

        if not file_paths:
            return None, []

        target_relative_path = str(Path(match['relative_path']))
        matched_index = None
        for index, current_path in enumerate(file_paths):
            is_match = current_path == target_relative_path or os.path.basename(current_path) == match['display_name']
            if is_match and matched_index is None:
                matched_index = index

        if matched_index is None:
            return None, []

        piece_sharing_indexes = set()
        matched_size = int(match.get('size', 0) or 0)
        if matched_size > 0:
            try:
                file_piece_spans = _get_libtorrent_file_piece_spans(torrent_info, matched_index, matched_size)
                for piece_index, _ in file_piece_spans:
                    piece_size = int(torrent_info.piece_size(piece_index))
                    for file_slice in torrent_info.map_block(piece_index, 0, piece_size):
                        file_index = int(file_slice.file_index)
                        if 0 <= file_index < len(file_paths):
                            piece_sharing_indexes.add(file_index)
            except Exception:
                piece_sharing_indexes.clear()

        priorities = []
        for index in range(len(file_paths)):
            if index == matched_index:
                priorities.append(7)
            elif index in piece_sharing_indexes:
                priorities.append(7)
            else:
                priorities.append(0)

        return matched_index, priorities

    def _prepare_in_place_torrent_targets(
        self,
        torrent_info: Any,
        match: Dict[str, Any],
        save_path: Path,
        failed_path: Path,
        matched_index: int,
        priorities: List[int],
        scratch_root: Optional[Path] = None,
    ) -> Tuple[List[int], Dict[int, str]]:
        adjusted_priorities = list(priorities)
        rename_targets: Dict[int, str] = {}

        try:
            file_paths = self._get_torrent_file_paths_in_order(str(match['torrent_path']))
        except Exception:
            file_paths = []

        torrent_name = str(match.get('torrent_name') or '')
        local_target_paths = self._collect_local_torrent_target_paths(
            save_path,
            torrent_name,
            file_paths,
            failed_path,
            matched_index,
        )

        for file_index, target_path in local_target_paths.items():
            try:
                rename_targets[file_index] = str(target_path.resolve())
            except Exception:
                rename_targets[file_index] = str(target_path)

        for file_index, priority in enumerate(adjusted_priorities):
            if priority <= 0 or file_index == matched_index:
                continue

            resolved_path = local_target_paths.get(file_index)
            if resolved_path is None:
                relative_path = file_paths[file_index] if file_index < len(file_paths) else ''
                resolved_path = self._resolve_in_place_target_path(save_path, torrent_name, relative_path)
            if resolved_path == failed_path:
                adjusted_priorities[file_index] = 0
                continue

            try:
                rename_targets[file_index] = str(resolved_path.resolve())
            except Exception:
                rename_targets[file_index] = str(resolved_path)

        for file_index in local_target_paths:
            if file_index != matched_index and 0 <= file_index < len(adjusted_priorities):
                adjusted_priorities[file_index] = max(adjusted_priorities[file_index], 1)

        if scratch_root is not None:
            for file_index, priority in enumerate(adjusted_priorities):
                if priority <= 0 or file_index == matched_index:
                    continue

                relative_path = file_paths[file_index] if file_index < len(file_paths) else f'file_{file_index}'
                scratch_target = scratch_root / Path(relative_path)
                try:
                    scratch_target.parent.mkdir(parents=True, exist_ok=True)
                except Exception:
                    pass
                rename_targets[file_index] = str(scratch_target)

        return adjusted_priorities, rename_targets

    def _collect_local_torrent_target_paths(
        self,
        save_path: Path,
        torrent_name: str,
        file_paths: List[str],
        failed_path: Path,
        matched_index: int,
    ) -> Dict[int, Path]:
        target_paths: Dict[int, Path] = {matched_index: failed_path}

        for file_index, relative_path in enumerate(file_paths):
            if file_index == matched_index:
                continue

            resolved_path = self._resolve_session_file_path(save_path, torrent_name, relative_path)
            if resolved_path is None or not resolved_path.exists() or not resolved_path.is_file():
                continue

            if resolved_path == failed_path:
                continue

            target_paths[file_index] = resolved_path

        return target_paths

    def _build_matched_torrent_params(
        self,
        torrent_info: Any,
        match: Dict[str, Any],
        save_path: Path,
        failed_path: Optional[Path] = None,
        scratch_root: Optional[Path] = None,
    ) -> Any:
        params = _create_add_torrent_params(torrent_info, save_path)
        matched_index, priorities = self._find_matched_torrent_file_index(torrent_info, match)
        rename_targets: Dict[int, str] = {}
        if matched_index is not None and failed_path is not None:
            priorities, rename_targets = self._prepare_in_place_torrent_targets(
                torrent_info,
                match,
                save_path,
                failed_path,
                matched_index,
                priorities,
                scratch_root=scratch_root,
            )
        _apply_match_to_add_torrent_params(params, priorities, rename_targets)
        return params

    def _configure_matched_torrent_file(
        self,
        handle: Any,
        torrent_info: Any,
        match: Dict[str, Any],
        failed_path: Path,
        scratch_root: Optional[Path] = None,
    ) -> None:
        matched_index, priorities = self._find_matched_torrent_file_index(torrent_info, match)
        if matched_index is None:
            return

        priorities, rename_targets = self._prepare_in_place_torrent_targets(
            torrent_info,
            match,
            failed_path.parent,
            failed_path,
            matched_index,
            priorities,
            scratch_root=scratch_root,
        )

        try:
            handle.prioritize_files(priorities)
        except Exception:
            pass

        for file_index, target_path in rename_targets.items():
            try:
                handle.rename_file(file_index, target_path)
            except Exception:
                pass

    def _scan_local_valid_piece_indexes(
        self,
        torrent_info: Any,
        match: Dict[str, Any],
        save_path: Path,
        failed_path: Path,
        matched_index: Optional[int],
        file_piece_spans: List[Tuple[int, int]],
        extra_roots: Optional[List[Path]] = None,
    ) -> Set[int]:
        if matched_index is None or not file_piece_spans:
            return set()

        try:
            torrent_file_paths = self._get_torrent_file_paths_in_order(str(match['torrent_path']))
        except Exception:
            return set()

        if not torrent_file_paths:
            return set()

        valid_piece_indexes = set()
        torrent_name = str(match.get('torrent_name', ''))
        for piece_index, _ in file_piece_spans:
            try:
                piece_size = int(torrent_info.piece_size(piece_index))
                file_slices = torrent_info.map_block(piece_index, 0, piece_size)
                expected_hash = bytes(torrent_info.hash_for_piece(piece_index))
            except Exception:
                continue

            piece_chunks = []
            piece_complete = True
            for file_slice in file_slices:
                try:
                    file_index = int(file_slice.file_index)
                    slice_offset = int(file_slice.offset)
                    slice_size = int(file_slice.size)
                except Exception:
                    piece_complete = False
                    break

                if file_index < 0 or file_index >= len(torrent_file_paths):
                    piece_complete = False
                    break

                preferred_path = failed_path if file_index == matched_index else None
                resolved_path = self._resolve_session_file_path(
                    save_path,
                    torrent_name,
                    torrent_file_paths[file_index],
                    preferred_path=preferred_path,
                    extra_roots=extra_roots,
                )
                if resolved_path is None:
                    piece_complete = False
                    break

                try:
                    with open(resolved_path, 'rb') as source_file:
                        source_file.seek(slice_offset)
                        chunk = source_file.read(slice_size)
                except OSError:
                    piece_complete = False
                    break

                if len(chunk) != slice_size:
                    piece_complete = False
                    break

                piece_chunks.append(chunk)

            if piece_complete and hashlib.sha1(b''.join(piece_chunks)).digest() == expected_hash:
                valid_piece_indexes.add(piece_index)

        return valid_piece_indexes

    def _extract_torrent_status_progress(self, status: Any, fallback_total: Optional[int] = None) -> Tuple[float, int, Optional[int], Optional[str]]:
        progress = 0.0
        completed_bytes = 0
        total_bytes = fallback_total
        error_text = None

        try:
            progress = float(getattr(status, 'progress', 0.0) or 0.0)
        except Exception:
            progress = 0.0

        try:
            wanted_done = getattr(status, 'total_wanted_done', None)
            wanted_total = getattr(status, 'total_wanted', None)
            if isinstance(wanted_total, (int, float)) and wanted_total > 0:
                total_bytes = int(wanted_total)
            if isinstance(wanted_done, (int, float)) and wanted_done >= 0:
                completed_bytes = int(wanted_done)
            elif total_bytes:
                completed_bytes = int(progress * total_bytes)
        except Exception:
            if total_bytes:
                completed_bytes = int(progress * total_bytes)

        try:
            errc = getattr(status, 'errc', None)
            if errc:
                error_text = _format_libtorrent_error(errc) or None
        except Exception:
            pass

        return progress, completed_bytes, total_bytes, error_text

    def _verify_file_against_torrent_source(
        self,
        filepath: str,
        match: Dict[str, Any],
        extra_roots: Optional[List[Path]] = None,
    ) -> Tuple[bool, str]:
        if not LIBTORRENT_AVAILABLE:
            return False, 'LIBTORRENT_UNAVAILABLE'

        failed_path = Path(filepath)
        expected_size = int(match.get('size', 0) or 0)
        verify_progress = None

        try:
            torrent_info = libtorrent.torrent_info(str(match['torrent_path']))
            matched_index, _ = self._find_matched_torrent_file_index(torrent_info, match)
            file_piece_spans = _get_libtorrent_file_piece_spans(torrent_info, matched_index, expected_size)
            if matched_index is None or not file_piece_spans or expected_size <= 0:
                return False, 'TORRENT_METADATA_INCOMPLETE'

            verify_progress = self._create_progress_bar(filepath, expected_size, label='Verify')
            verified_overlap_bytes = 0

            for piece_number, (piece_index, overlap_bytes) in enumerate(file_piece_spans, start=1):
                single_piece_valid = self._scan_local_valid_piece_indexes(
                    torrent_info,
                    match,
                    failed_path.parent,
                    failed_path,
                    matched_index,
                    [(piece_index, overlap_bytes)],
                    extra_roots=extra_roots,
                )
                if piece_index in single_piece_valid:
                    verified_overlap_bytes += overlap_bytes

                if verify_progress is not None:
                    completed_overlap = sum(span for _, span in file_piece_spans[:piece_number])
                    self._update_progress_bar(verify_progress, min(expected_size, completed_overlap), expected_size)
                    verify_progress.set_postfix_str(f"file-ok {verified_overlap_bytes / expected_size:.2%}")

            if verified_overlap_bytes >= expected_size:
                return True, 'TORRENT_OK'
            return False, 'TORRENT_DAMAGED'
        except Exception as exc:
            self._log(f"Torrent-backed verification failed for {filepath}: {exc}", 1)
            return False, 'ERROR'
        finally:
            if verify_progress is not None:
                verify_progress.close()

    def _check_file_integrity(self, filepath: str) -> Dict[str, Any]:
        filename = os.path.basename(filepath)
        result = {
            'filepath': filepath,
            'filename': filename,
            'status': 'unverified',
            'verification_source': 'none',
            'is_valid': False,
            'expected_crc': 'N/A',
            'actual_crc': 'N/A',
        }

        expected_crc = self._extract_crc_from_filename(filename)
        if expected_crc is not None:
            is_valid, expected_crc, actual_crc = self._verify_file_crc(filepath, expected_crc=expected_crc)
            result.update({
                'status': 'valid' if is_valid else 'invalid',
                'verification_source': 'filename_crc',
                'is_valid': is_valid,
                'expected_crc': expected_crc,
                'actual_crc': actual_crc,
            })
            if is_valid or not LIBTORRENT_AVAILABLE:
                return result

        best_match, inspected_count = self._find_best_torrent_match(filepath, filename=filename)
        if not best_match:
            if expected_crc is not None:
                result['status'] = 'invalid'
            result['inspected_torrent_count'] = inspected_count
            return result

        result['torrent_match'] = best_match
        result['inspected_torrent_count'] = inspected_count

        if expected_crc is not None:
            return result

        if not LIBTORRENT_AVAILABLE:
            result['status'] = 'unverified'
            result['actual_crc'] = 'LIBTORRENT_UNAVAILABLE'
            return result

        is_valid, actual_status = self._verify_file_against_torrent_source(filepath, best_match)
        result.update({
            'status': 'valid' if is_valid else 'invalid',
            'verification_source': 'torrent',
            'is_valid': is_valid,
            'expected_crc': result['expected_crc'] if result['expected_crc'] != 'N/A' else 'TORRENT',
            'actual_crc': actual_status,
        })
        return result

    def _attempt_libtorrent_session_recovery(
        self,
        failed_filepath: str,
        expected_crc: str,
        match: Dict[str, Any],
        timeout_seconds: int,
    ) -> Tuple[bool, str]:
        if not LIBTORRENT_AVAILABLE:
            return False, 'libtorrent is not available.'

        failed_path = Path(failed_filepath)
        save_path = failed_path.parent
        repair_scratch_root = None
        try:
            torrent_info = libtorrent.torrent_info(str(match['torrent_path']))
            session = libtorrent.session()
            try:
                _configure_libtorrent_session(session)
            except Exception:
                pass

            repair_scratch_root = self._create_repair_scratch_root(failed_path)

            handle = session.add_torrent(
                self._build_matched_torrent_params(
                    torrent_info,
                    match,
                    save_path,
                    failed_path,
                    scratch_root=repair_scratch_root,
                )
            )
            try:
                _enable_sequential_download(handle)
            except Exception:
                pass
            try:
                handle.resume()
            except Exception:
                pass
            try:
                _call_libtorrent_without_deprecation_warnings(handle.force_recheck)
            except Exception:
                pass

            self._configure_matched_torrent_file(
                handle,
                torrent_info,
                match,
                failed_path,
                scratch_root=repair_scratch_root,
            )
            deadline = time.time() + timeout_seconds
            last_status_message = 'Torrent session timed out before CRC matched.'

            while time.time() < deadline:
                try:
                    is_valid, _, actual_crc = self._verify_file_crc(str(failed_path), expected_crc)
                except Exception:
                    is_valid = False
                    actual_crc = 'ERROR'

                if is_valid:
                    try:
                        session.remove_torrent(handle)
                    except Exception:
                        pass
                    return True, f"Recovered in place via torrent session (CRC {actual_crc})"

                try:
                    status = handle.status()
                    progress = getattr(status, 'progress', 0.0)
                    last_status_message = f"Torrent session progress: {progress:.2%}"
                    if getattr(status, 'errc', None):
                        error_text = _format_libtorrent_error(status.errc)
                        if error_text:
                            last_status_message = f"Torrent session error: {error_text}"
                except Exception:
                    pass

                time.sleep(1.0)

            try:
                session.remove_torrent(handle)
            except Exception:
                pass
            return False, last_status_message
        except Exception as exc:
            self._log(f"libtorrent recovery failed for {failed_filepath}: {exc}", 1)
            return False, f"libtorrent recovery failed: {exc}"
        finally:
            self._cleanup_repair_scratch_root(repair_scratch_root)

    def _attempt_libtorrent_piece_recovery(
        self,
        failed_filepath: str,
        match: Dict[str, Any],
        timeout_seconds: int,
    ) -> Tuple[bool, str]:
        if not LIBTORRENT_AVAILABLE:
            return False, 'libtorrent is not available.'

        failed_path = Path(failed_filepath)
        expected_size = int(match.get('size', 0) or 0)
        local_file_size = failed_path.stat().st_size if failed_path.exists() else 0
        repair_progress = None
        repair_scratch_root = None

        try:
            torrent_info = libtorrent.torrent_info(str(match['torrent_path']))
            session = libtorrent.session()
            matched_index, _ = self._find_matched_torrent_file_index(torrent_info, match)
            file_piece_spans = _get_libtorrent_file_piece_spans(torrent_info, matched_index, expected_size)
            baseline_valid_piece_indexes = self._scan_local_valid_piece_indexes(
                torrent_info,
                match,
                failed_path.parent,
                failed_path,
                matched_index,
                file_piece_spans,
            )
            baseline_file_ok_bytes = _get_libtorrent_file_ok_bytes(
                None,
                file_piece_spans,
                expected_size,
                baseline_valid_piece_indexes=baseline_valid_piece_indexes,
            ) or 0
            estimated_repair_bytes, missing_piece_count = _estimate_missing_piece_repair_bytes(
                torrent_info,
                file_piece_spans,
                baseline_valid_piece_indexes,
            )
            missing_piece_indexes = {
                piece_index
                for piece_index, _ in file_piece_spans
                if piece_index not in baseline_valid_piece_indexes
            }

            if expected_size > 0 and baseline_file_ok_bytes >= expected_size:
                verified, _ = self._verify_file_against_torrent_source(failed_filepath, match)
                if verified:
                    return True, 'File already fully backed by valid torrent pieces'

            try:
                _configure_libtorrent_session(session)
            except Exception:
                pass

            repair_scratch_root = self._create_repair_scratch_root(failed_path)
            params = self._build_matched_torrent_params(
                torrent_info,
                match,
                failed_path.parent,
                failed_path,
                scratch_root=repair_scratch_root,
            )
            if baseline_valid_piece_indexes:
                piece_bitfield = _build_libtorrent_piece_bitfield(int(torrent_info.num_pieces()), baseline_valid_piece_indexes)
                if piece_bitfield:
                    try:
                        params.have_pieces = piece_bitfield
                    except Exception:
                        pass
                    try:
                        params.verified_pieces = piece_bitfield
                    except Exception:
                        pass

            handle = session.add_torrent(params)
            try:
                _enable_sequential_download(handle)
            except Exception:
                pass
            try:
                handle.resume()
            except Exception:
                pass
            try:
                _call_libtorrent_without_deprecation_warnings(handle.force_recheck)
            except Exception:
                pass

            self._configure_matched_torrent_file(
                handle,
                torrent_info,
                match,
                failed_path,
                scratch_root=repair_scratch_root,
            )
            if missing_piece_indexes:
                _prioritize_missing_torrent_pieces(handle, missing_piece_indexes)
            deadline = time.time() + timeout_seconds
            progress_total = estimated_repair_bytes if estimated_repair_bytes > 0 else max(expected_size, 1)
            repair_progress = self._create_progress_bar(failed_filepath, progress_total, label='Repair')
            last_status_message = 'Torrent repair timed out before the file reached a verified complete state.'
            next_verification_attempt_at = 0.0
            next_disk_piece_check_at = 0.0

            if expected_size > 0 and self.verbose >= 1:
                self._log(
                    f"Repair baseline for {failed_path.name}: local-file={_format_byte_size(local_file_size)}/{_format_byte_size(expected_size)}, "
                    f"file-ok={_format_byte_size(baseline_file_ok_bytes)}/{_format_byte_size(expected_size)}",
                    1,
                )
                if missing_piece_count > 0:
                    self._log(
                        f"Estimated repair download: {_format_byte_size(estimated_repair_bytes)} across {missing_piece_count} missing torrent piece(s)",
                        1,
                    )

            while time.time() < deadline:
                status = _get_libtorrent_status(handle)
                progress, completed_bytes, _, error_text = self._extract_torrent_status_progress(status, expected_size)
                error_text = error_text or _format_libtorrent_error(getattr(status, 'errc', None))
                try:
                    payload_downloaded = int(getattr(status, 'total_payload_download', 0) or 0)
                except Exception:
                    payload_downloaded = completed_bytes

                file_ok_bytes = _get_libtorrent_file_ok_bytes(
                    status,
                    file_piece_spans,
                    expected_size,
                    baseline_valid_piece_indexes=baseline_valid_piece_indexes,
                )

                if repair_progress is not None:
                    progress_value = completed_bytes
                    if estimated_repair_bytes > 0:
                        progress_value = payload_downloaded
                    elif file_ok_bytes is not None:
                        progress_value = file_ok_bytes

                    self._update_progress_bar(repair_progress, progress_value, progress_total)
                    postfix_parts = []
                    if file_ok_bytes is not None and expected_size > 0:
                        postfix_parts.append(f"file-ok {file_ok_bytes / expected_size:.2%}")
                    try:
                        download_rate = int(getattr(status, 'download_rate', 0) or 0)
                    except Exception:
                        download_rate = 0
                    if download_rate > 0:
                        postfix_parts.append(f"{download_rate / (1024 * 1024):.2f} MiB/s")
                    try:
                        num_peers = int(getattr(status, 'num_peers', 0) or 0)
                    except Exception:
                        num_peers = 0
                    if num_peers > 0:
                        postfix_parts.append(f"peers {num_peers}")
                    if error_text:
                        postfix_parts.append(f"error {error_text}")
                    if postfix_parts:
                        repair_progress.set_postfix_str(' | '.join(postfix_parts))

                if error_text:
                    last_status_message = f"Torrent session error: {error_text}"
                    break

                should_recheck_disk = False
                if estimated_repair_bytes > 0 and payload_downloaded >= estimated_repair_bytes:
                    should_recheck_disk = True
                elif file_ok_bytes is not None and baseline_file_ok_bytes is not None and file_ok_bytes <= baseline_file_ok_bytes:
                    should_recheck_disk = time.time() >= next_disk_piece_check_at

                if should_recheck_disk:
                    rescanned_valid_piece_indexes = self._scan_local_valid_piece_indexes(
                        torrent_info,
                        match,
                        failed_path.parent,
                        failed_path,
                        matched_index,
                        file_piece_spans,
                        extra_roots=[repair_scratch_root] if repair_scratch_root is not None else None,
                    )
                    if rescanned_valid_piece_indexes:
                        baseline_valid_piece_indexes = rescanned_valid_piece_indexes
                        file_ok_bytes = _get_libtorrent_file_ok_bytes(
                            None,
                            file_piece_spans,
                            expected_size,
                            baseline_valid_piece_indexes=baseline_valid_piece_indexes,
                        )
                    next_disk_piece_check_at = time.time() + 1.0

                verification_threshold_reached = (
                    (file_ok_bytes is not None and expected_size > 0 and file_ok_bytes >= expected_size)
                    or progress >= 0.999
                )
                if verification_threshold_reached and time.time() >= next_verification_attempt_at:
                    verified, verify_status = self._verify_file_against_torrent_source(
                        failed_filepath,
                        match,
                        extra_roots=[repair_scratch_root] if repair_scratch_root is not None else None,
                    )
                    if verified:
                        try:
                            session.remove_torrent(handle)
                        except Exception:
                            pass
                        return True, 'Recovered in place via torrent pieces'

                    last_status_message = f"Repair reached completion threshold, but verification returned {verify_status}."
                    next_verification_attempt_at = time.time() + 1.0

                if file_ok_bytes is not None and expected_size > 0:
                    last_status_message = (
                        f"Torrent repair progress: file-ok {file_ok_bytes / expected_size:.2%} "
                        f"({_format_byte_size(file_ok_bytes)}/{_format_byte_size(expected_size)})"
                    )
                else:
                    last_status_message = f"Torrent repair progress: {progress:.2%}"

                time.sleep(0.5)

            try:
                session.remove_torrent(handle)
            except Exception:
                pass
            return False, last_status_message
        except Exception as exc:
            self._log(f"libtorrent recovery failed for {failed_filepath}: {exc}", 1)
            return False, f"libtorrent recovery failed: {exc}"
        finally:
            if repair_progress is not None:
                repair_progress.close()
            self._cleanup_repair_scratch_root(repair_scratch_root)

    def _create_repair_scratch_root(self, failed_path: Path) -> Path:
        """Create a temporary workspace for non-target boundary files during repair."""
        scratch_root = failed_path.parent / '.torrent_repair_parts' / f"{failed_path.stem}_{uuid.uuid4().hex[:8]}"
        scratch_root.mkdir(parents=True, exist_ok=True)
        return scratch_root

    def _cleanup_repair_scratch_root(self, scratch_root: Optional[Path]) -> None:
        """Remove the temporary repair scratch workspace."""
        if scratch_root is None:
            return

        try:
            if scratch_root.exists() and scratch_root.parent.name == '.torrent_repair_parts':
                shutil.rmtree(scratch_root, ignore_errors=True)
                try:
                    scratch_root.parent.rmdir()
                except OSError:
                    pass
        except Exception as exc:
            self._log(f"Could not clean temporary torrent repair path {scratch_root}: {exc}", 2)

    def repair_file(self, result: Dict[str, Any], timeout_seconds: int) -> Tuple[bool, str]:
        filepath = result['filepath']
        match = result.get('torrent_match')
        if not match:
            return False, 'No matching torrent metadata was found.'

        if result.get('verification_source') == 'torrent':
            return self._attempt_libtorrent_piece_recovery(filepath, match, timeout_seconds)

        expected_crc = result.get('expected_crc')
        if not expected_crc or expected_crc == 'N/A':
            return False, 'Expected CRC is not available for repair.'

        return self._attempt_libtorrent_session_recovery(filepath, expected_crc, match, timeout_seconds)


def _expand_input_paths(paths: Iterable[str]) -> List[str]:
    selected_files: List[str] = []
    seen = set()
    for raw_path in paths:
        path = Path(raw_path)
        if not path.exists():
            _safe_console_print(f"Warning: path not found: {raw_path}")
            continue

        if path.is_file():
            resolved = str(path.resolve())
            if resolved not in seen and path.suffix.lower() != '.torrent':
                selected_files.append(resolved)
                seen.add(resolved)
            continue

        for candidate in sorted(path.rglob('*')):
            if not candidate.is_file() or candidate.suffix.lower() == '.torrent':
                continue
            resolved = str(candidate.resolve())
            if resolved not in seen:
                selected_files.append(resolved)
                seen.add(resolved)

    return selected_files


def _print_libtorrent_notice() -> None:
    if LIBTORRENT_AVAILABLE:
        return

    _safe_console_print('libtorrent is not available. Torrent-backed verification and repair will be unavailable for files without CRC in the filename.')
    if LIBTORRENT_IMPORT_ERROR is not None:
        _safe_console_print(f"libtorrent import error: {LIBTORRENT_IMPORT_ERROR}")


def _print_result(result: Dict[str, Any]) -> None:
    filename = result['filename']
    status = result['status']
    source = result.get('verification_source', 'none')
    match = result.get('torrent_match')

    if status == 'valid':
        _safe_console_print(f"[ok] {filename} via {source}")
    elif status == 'invalid':
        _safe_console_print(f"[bad] {filename} via {source}")
    else:
        _safe_console_print(f"[skip] {filename} ({status})")

    if result.get('expected_crc') not in {None, 'N/A'}:
        _safe_console_print(f"  expected: {result['expected_crc']}")
    if result.get('actual_crc') not in {None, 'N/A'}:
        _safe_console_print(f"  actual:   {result['actual_crc']}")
    if match:
        _safe_console_print(
            f"  torrent:  {os.path.basename(match['torrent_path'])} -> {match['display_name']} (score {match['total_score']:.2f})"
        )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Verify files against CRCs in filenames and/or an archive of .torrent files, with optional in-place repair.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('paths', nargs='+', metavar='PATH', help='Files or folders to check')
    parser.add_argument('--torrent-dir', required=True, metavar='DIR', help='Directory containing archived .torrent files')
    parser.add_argument('--repair', action='store_true', help='Attempt in-place repair for files that fail validation')
    parser.add_argument('--yes', action='store_true', help='Do not prompt before starting repair')
    parser.add_argument('--timeout', type=int, default=120, metavar='SECONDS', help='Maximum seconds to wait for each repair attempt')
    parser.add_argument('-v', '--verbose', action='count', default=0, help='Increase verbosity')
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    torrent_dir = Path(args.torrent_dir)
    if not torrent_dir.exists() or not torrent_dir.is_dir():
        _safe_console_print(f"Error: torrent directory not found: {args.torrent_dir}")
        return 1

    if args.timeout <= 0:
        _safe_console_print('Error: --timeout must be a positive integer.')
        return 1

    selected_files = _expand_input_paths(args.paths)
    if not selected_files:
        _safe_console_print('No files found to check.')
        return 1

    _print_libtorrent_notice()
    checker = TorrentFileCheckRepair(str(torrent_dir), verbose=args.verbose)

    if TQDM_AVAILABLE:
        iterator = tqdm(selected_files, desc='Checking files', unit='file', disable=args.verbose == 0)
    else:
        iterator = selected_files

    results: List[Dict[str, Any]] = []
    for filepath in iterator:
        result = checker._check_file_integrity(filepath)
        results.append(result)
        if TQDM_AVAILABLE and args.verbose >= 1 and hasattr(iterator, 'set_postfix_str'):
            try:
                iterator.set_postfix_str(os.path.basename(filepath)[:40])
            except Exception:
                pass
        _print_result(result)

    invalid_results = [result for result in results if result.get('status') == 'invalid']
    valid_count = sum(1 for result in results if result.get('is_valid', False))
    invalid_count = sum(1 for result in results if result.get('status') == 'invalid')
    unverified_count = sum(1 for result in results if result.get('status') == 'unverified')

    _safe_console_print('')
    _safe_console_print('Summary:')
    _safe_console_print(f"  total files: {len(results)}")
    _safe_console_print(f"  valid:       {valid_count}")
    _safe_console_print(f"  invalid:     {invalid_count}")
    _safe_console_print(f"  unverified:  {unverified_count}")

    if not args.repair or not invalid_results:
        return 0 if invalid_count == 0 else 2

    if not LIBTORRENT_AVAILABLE:
        _safe_console_print('Repair requested, but libtorrent is not available.')
        return 2

    if not args.yes:
        try:
            response = input(f"Proceed with repair for {len(invalid_results)} failed file(s)? [y/N]: ").strip().lower()
        except EOFError:
            response = ''
        if response not in {'y', 'yes'}:
            _safe_console_print('Repair skipped.')
            return 2 if invalid_count > 0 else 0

    recovered_count = 0
    for result in invalid_results:
        _safe_console_print(f"Repairing {result['filename']}...")
        recovered, message = checker.repair_file(result, timeout_seconds=args.timeout)
        if recovered:
            recovered_count += 1
            _safe_console_print(f"  [recovered] {message}")
        else:
            _safe_console_print(f"  [unresolved] {message}")

    _safe_console_print('')
    _safe_console_print('Repair summary:')
    _safe_console_print(f"  attempted:   {len(invalid_results)}")
    _safe_console_print(f"  recovered:   {recovered_count}")
    _safe_console_print(f"  unresolved:  {len(invalid_results) - recovered_count}")

    return 0 if recovered_count == len(invalid_results) else 2


if __name__ == '__main__':
    sys.exit(main())