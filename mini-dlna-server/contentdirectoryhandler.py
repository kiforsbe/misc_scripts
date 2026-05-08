import datetime
import logging
import os
from urllib.parse import quote, unquote
from xml.sax.saxutils import escape
from xml.etree import ElementTree
from xml.etree.ElementTree import Element, SubElement, tostring

from mutagen import File


VIDEO_EXTENSIONS = {
    '.mp4': 'video/mp4',
    '.m4v': 'video/mp4',
    '.mkv': 'video/x-matroska',
    '.avi': 'video/x-msvideo',
    '.mov': 'video/quicktime',
}

AUDIO_EXTENSIONS = {
    '.mp3': 'audio/mpeg',
    '.flac': 'audio/flac',
    '.wav': 'audio/wav',
    '.m4a': 'audio/mp4',
    '.aac': 'audio/aac',
    '.ogg': 'audio/ogg',
}

IMAGE_EXTENSIONS = {
    '.jpg': 'image/jpeg',
    '.jpeg': 'image/jpeg',
    '.png': 'image/png',
    '.gif': 'image/gif',
    '.webp': 'image/webp',
}

ALL_EXTENSIONS = {**VIDEO_EXTENSIONS, **AUDIO_EXTENSIONS, **IMAGE_EXTENSIONS}


class ContentDirectoryHandler:
    """Handles ContentDirectory browse requests and DIDL-Lite generation."""

    CONTENT_DIRECTORY_NS = 'urn:schemas-upnp-org:service:ContentDirectory:1'

    def __init__(self, request_handler):
        self.request_handler = request_handler
        self.http_server = request_handler.server
        self.logger = logging.getLogger('DLNAServer')
        self.soap_handler = request_handler.soap_handler
        self.error_handler = request_handler.error_handler

    def _client_profile(self):
        getter = getattr(self.request_handler, 'get_client_profile', None)
        if getter is None:
            return {'is_likely_samsung': False}
        return getter()

    def _is_likely_samsung_client(self):
        return bool(self._client_profile().get('is_likely_samsung'))

    def handle_control(self, post_data):
        try:
            action = self._get_soap_action()
            if action == 'Browse':
                self._handle_browse(post_data)
                return
            if action == 'GetSearchCapabilities':
                self._send_simple_response('GetSearchCapabilities', '<SearchCaps>dc:title,upnp:class</SearchCaps>')
                return
            if action == 'GetSortCapabilities':
                self._send_simple_response('GetSortCapabilities', '<SortCaps>dc:title,dc:date</SortCaps>')
                return
            if action == 'GetSystemUpdateID':
                self._send_simple_response('GetSystemUpdateID', '<Id>1</Id>')
                return
            raise ValueError(f'Unsupported ContentDirectory action: {action or "unknown"}')
        except Exception as exc:
            self.error_handler.handle_request_error(self.request_handler, exc, 400)

    def _get_soap_action(self):
        soap_action = self.request_handler.headers.get('SOAPACTION', '').strip('"')
        if '#' in soap_action:
            return soap_action.rsplit('#', 1)[1]
        return soap_action

    def _handle_browse(self, post_data):
        params = self._parse_browse_request(post_data)
        self.logger.info(
            "Browse request object_id=%s flag=%s start=%s count=%s",
            params['object_id'],
            params['browse_flag'],
            params['starting_index'],
            params['requested_count'],
        )
        didl, number_returned, total_matches = self.generate_browse_didl(
            params['object_id'],
            params['browse_flag'],
            params['starting_index'],
            params['requested_count'],
        )
        body = (
            f'<Result>{escape(didl)}</Result>'
            f'<NumberReturned>{number_returned}</NumberReturned>'
            f'<TotalMatches>{total_matches}</TotalMatches>'
            '<UpdateID>1</UpdateID>'
        )
        self.logger.info(
            "Browse response object_id=%s returned=%s total=%s",
            params['object_id'],
            number_returned,
            total_matches,
        )
        if params['browse_flag'] == 'BrowseMetadata' and params['object_id'] != '0':
            self.logger.info('Browse metadata DIDL for %s: %s', params['object_id'], didl)
        self.soap_handler.send_soap_response(body, 'Browse', self.CONTENT_DIRECTORY_NS)

    def _send_simple_response(self, action_name, body):
        self.soap_handler.send_soap_response(body, action_name, self.CONTENT_DIRECTORY_NS)

    def _parse_browse_request(self, soap_body):
        try:
            root = ElementTree.fromstring(soap_body)
            namespaces = {
                's': 'http://schemas.xmlsoap.org/soap/envelope/',
                'u': self.CONTENT_DIRECTORY_NS,
            }
            browse = (
                root.find('.//u:Browse', namespaces)
                or root.find('.//Browse')
                or root.find(f'.//{{{self.CONTENT_DIRECTORY_NS}}}Browse')
            )
            if browse is None:
                raise ValueError('Browse action not found in SOAP request')
            return {
                'object_id': self._get_child_text(browse, 'ObjectID', '0'),
                'browse_flag': self._get_child_text(browse, 'BrowseFlag', 'BrowseDirectChildren'),
                'starting_index': int(self._get_child_text(browse, 'StartingIndex', '0')),
                'requested_count': int(self._get_child_text(browse, 'RequestedCount', '0')),
            }
        except Exception as exc:
            raise ValueError(f'Invalid Browse request: {exc}') from exc

    def _get_child_text(self, parent, child_name, default=''):
        child = parent.find(child_name)
        if child is None:
            child = parent.find(f'./{child_name}')
        if child is None or child.text is None:
            return default
        return child.text

    def generate_browse_didl(self, object_id, browse_flag, starting_index, requested_count):
        root = self._create_didl_root()
        if object_id == '0':
            if browse_flag == 'BrowseMetadata':
                total_children = self._add_root_metadata(root)
                self._append_root_children(root, starting_index, requested_count)
                number_returned = 1 + min(total_children, requested_count or total_children)
                total_matches = 1 + total_children
                return self.encode_didl(root), number_returned, total_matches
            return self._browse_root_children(root, starting_index, requested_count)

        resolved = self._resolve_object_id(object_id)
        if resolved is None:
            self.logger.warning('Browse requested unresolved object_id=%s', object_id)
            return self.encode_didl(root), 0, 0

        if browse_flag == 'BrowseMetadata':
            return self._browse_metadata(root, resolved)

        return self._browse_resolved_children(root, object_id, resolved, starting_index, requested_count)

    def _browse_metadata(self, root, resolved):
        kind = resolved['kind']
        if kind in {'share', 'merged', 'playlists-root', 'playlist'}:
            self._add_virtual_container(
                root,
                resolved['object_id'],
                resolved['parent_id'],
                resolved['title'],
                resolved['child_count'],
            )
            return self.encode_didl(root), 1, 1
        if kind == 'entry':
            self._add_context_entry(
                root,
                resolved['source'],
                resolved['folder_index'],
                resolved['abs_path'],
                self._parent_id_for_entry(resolved),
            )
            return self.encode_didl(root), 1, 1
        if kind == 'playlist-item':
            self._add_item(
                root,
                resolved['folder_index'],
                resolved['abs_path'],
                resolved['parent_id'],
                object_id=resolved['object_id'],
            )
            return self.encode_didl(root), 1, 1
        return self.encode_didl(root), 0, 0

    def _browse_resolved_children(self, root, object_id, resolved, starting_index, requested_count):
        kind = resolved['kind']
        if kind in {'share', 'merged'}:
            return self._browse_context_children(root, resolved, starting_index, requested_count)
        if kind == 'playlists-root':
            return self._browse_playlist_containers(root, resolved, starting_index, requested_count)
        if kind == 'playlist':
            return self._browse_playlist_items(root, resolved, starting_index, requested_count)
        if kind != 'entry' or not os.path.isdir(resolved['abs_path']):
            return self.encode_didl(root), 0, 0
        return self._browse_directory_children(
            root,
            resolved['source'],
            resolved['folder_index'],
            resolved['abs_path'],
            object_id,
            starting_index,
            requested_count,
        )

    def _create_didl_root(self):
        return Element(
            'DIDL-Lite',
            {
                'xmlns': 'urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/',
                'xmlns:dc': 'http://purl.org/dc/elements/1.1/',
                'xmlns:upnp': 'urn:schemas-upnp-org:metadata-1-0/upnp/',
                'xmlns:dlna': 'urn:schemas-dlna-org:metadata-1-0/',
                'xmlns:sec': 'http://www.sec.co.kr/dlna',
            },
        )

    def _add_root_metadata(self, root):
        total_children = len(self._collect_root_entries())

        self.logger.info('Root metadata reports %s virtual children', total_children)

        container = SubElement(
            root,
            'container',
            {
                'id': '0',
                'parentID': '-1',
                'restricted': 'false',
                'searchable': 'true',
                'childCount': str(total_children),
                'dlna:dlnaManaged': '00000004',
            },
        )
        SubElement(container, 'dc:title').text = 'Root'
        SubElement(container, 'upnp:class').text = 'object.container'
        SubElement(container, 'upnp:storageUsed').text = '-1'
        SubElement(container, 'sec:containerType').text = 'DLNA'

        self.logger.info('Root metadata DIDL: %s', self.encode_didl(root))
        return total_children

    def _collect_root_entries(self):
        entries = []
        for index, shared_folder in enumerate(self.http_server.media_folders):
            entries.append(
                {
                    'kind': 'share',
                    'object_id': self._share_container_id(index),
                    'parent_id': '0',
                    'title': self._share_title(shared_folder, index),
                    'child_count': self._count_visible_children(shared_folder),
                }
            )
        entries.append(
            {
                'kind': 'merged',
                'object_id': self._merged_container_id(),
                'parent_id': '0',
                'title': 'All Shared Paths',
                'child_count': len(self._collect_context_entries({'source': 'merged'})),
            }
        )
        if self._playlist_definitions():
            entries.append(
                {
                    'kind': 'playlists-root',
                    'object_id': self._playlists_root_id(),
                    'parent_id': '0',
                    'title': 'Playlists',
                    'child_count': len(self._playlist_definitions()),
                }
            )
        return entries

    def _append_root_children(self, root, starting_index, requested_count):
        entries = self._collect_root_entries()
        selected = self._slice_entries(entries, starting_index, requested_count)
        self.logger.info(
            'Root metadata compatibility append total=%s selected=%s preview=%s',
            len(entries),
            len(selected),
            [entry['title'] for entry in selected[:5]],
        )
        for entry in selected:
            self._add_virtual_container(root, entry['object_id'], '0', entry['title'], entry['child_count'])

    def _browse_root_children(self, root, starting_index, requested_count):
        entries = self._collect_root_entries()
        total = len(entries)
        selected = self._slice_entries(entries, starting_index, requested_count)
        self.logger.info(
            'Root children total=%s selected=%s preview=%s',
            total,
            len(selected),
            [entry['title'] for entry in selected[:5]],
        )
        for entry in selected:
            self._add_virtual_container(root, entry['object_id'], '0', entry['title'], entry['child_count'])
        return self.encode_didl(root), len(selected), total

    def _browse_context_children(self, root, resolved, starting_index, requested_count):
        entries = self._collect_context_entries(resolved)
        total = len(entries)
        selected = self._slice_entries(entries, starting_index, requested_count)
        self.logger.info(
            'Virtual children parent=%s total=%s selected=%s preview=%s',
            resolved['object_id'],
            total,
            len(selected),
            [os.path.basename(entry['abs_path']) for entry in selected[:5]],
        )
        for entry in selected:
            self._add_context_entry(
                root,
                resolved['source'],
                entry['folder_index'],
                entry['abs_path'],
                resolved['object_id'],
            )
        return self.encode_didl(root), len(selected), total

    def _browse_playlist_containers(self, root, resolved, starting_index, requested_count):
        entries = self._playlist_definitions()
        total = len(entries)
        selected = self._slice_entries(entries, starting_index, requested_count)
        self.logger.info(
            'Playlist containers parent=%s total=%s selected=%s preview=%s',
            resolved['object_id'],
            total,
            len(selected),
            [entry['name'] for entry in selected[:5]],
        )
        for playlist_index, playlist in enumerate(selected, start=starting_index):
            self._add_virtual_container(
                root,
                self._playlist_container_id(playlist_index),
                resolved['object_id'],
                playlist['name'],
                len(playlist['items']),
            )
        return self.encode_didl(root), len(selected), total

    def _browse_playlist_items(self, root, resolved, starting_index, requested_count):
        entries = resolved['items']
        total = len(entries)
        selected = self._slice_entries(entries, starting_index, requested_count)
        self.logger.info(
            'Playlist items parent=%s total=%s selected=%s preview=%s',
            resolved['object_id'],
            total,
            len(selected),
            [os.path.basename(entry['abs_path']) for entry in selected[:5]],
        )
        for offset, entry in enumerate(selected, start=starting_index):
            self._add_item(
                root,
                entry['folder_index'],
                entry['abs_path'],
                resolved['object_id'],
                object_id=self._playlist_item_id(resolved['playlist_index'], offset),
            )
        return self.encode_didl(root), len(selected), total

    def _browse_directory_children(self, root, source, folder_index, abs_path, parent_id, starting_index, requested_count):
        entries = []
        try:
            for entry in os.scandir(abs_path):
                if self._is_visible_entry(entry.path, entry.is_dir()):
                    entries.append({'folder_index': folder_index, 'abs_path': entry.path})
        except OSError as exc:
            self.logger.error(f'Error scanning directory {abs_path}: {exc}')
            return self.encode_didl(root), 0, 0

        entries = self._sort_default_entries(entries)
        selected = self._slice_entries(entries, starting_index, requested_count)
        self.logger.info(
            'Directory children path=%s total=%s selected=%s preview=%s',
            abs_path,
            len(entries),
            len(selected),
            [os.path.basename(entry['abs_path']) for entry in selected[:5]],
        )
        for entry in selected:
            self._add_context_entry(root, source, entry['folder_index'], entry['abs_path'], parent_id)
        return self.encode_didl(root), len(selected), len(entries)

    def _add_entry(self, root, folder_index, abs_path, parent_id, object_id=None):
        if os.path.isdir(abs_path):
            self._add_container(root, folder_index, abs_path, parent_id, object_id=object_id)
        else:
            self._add_item(root, folder_index, abs_path, parent_id, object_id=object_id)

    def _add_context_entry(self, root, source, folder_index, abs_path, parent_id):
        self._add_entry(
            root,
            folder_index,
            abs_path,
            parent_id,
            object_id=self._context_object_id(source, folder_index, abs_path),
        )

    def _add_virtual_container(self, root, object_id, parent_id, title, child_count):
        container = SubElement(
            root,
            'container',
            {
                'id': object_id,
                'parentID': parent_id,
                'restricted': '1',
                'searchable': '1',
                'childCount': str(child_count),
            },
        )
        SubElement(container, 'dc:title').text = title
        SubElement(container, 'upnp:class').text = 'object.container.storageFolder'

    def _add_container(self, root, folder_index, abs_path, parent_id, object_id=None):
        container = SubElement(
            root,
            'container',
            {
                'id': object_id or self._object_id_for_path(folder_index, abs_path),
                'parentID': parent_id,
                'restricted': '1',
                'searchable': '1',
                'childCount': str(self._count_visible_children(abs_path)),
            },
        )
        SubElement(container, 'dc:title').text = os.path.basename(abs_path)
        SubElement(container, 'upnp:class').text = 'object.container.storageFolder'

    def _add_item(self, root, folder_index, abs_path, parent_id, object_id=None):
        ext = os.path.splitext(abs_path)[1].lower()
        mime_type = ALL_EXTENSIONS.get(ext)
        if not mime_type:
            return

        is_samsung = self._is_likely_samsung_client()
        is_video = ext in VIDEO_EXTENSIONS

        item = SubElement(
            root,
            'item',
            {
                'id': object_id or self._object_id_for_path(folder_index, abs_path),
                'parentID': parent_id,
                'restricted': '1',
                **({'dlna:dlnaManaged': '00000004'} if is_samsung else {}),
            },
        )
        SubElement(item, 'dc:title').text = os.path.basename(abs_path)
        SubElement(item, 'upnp:class').text = self._upnp_class_for_extension(ext)

        res = SubElement(item, 'res')
        res.text = self._media_url(folder_index, abs_path)
        res.set('size', str(os.path.getsize(abs_path)))
        res.set('protocolInfo', self._protocol_info(ext, mime_type))

        duration = self.get_media_duration(abs_path)
        if duration:
            res.set('duration', duration)

        resolution = self.get_image_resolution(abs_path)
        if resolution:
            res.set('resolution', resolution)

        if ext in IMAGE_EXTENSIONS or ext in VIDEO_EXTENSIONS:
            thumbnail_info = self._thumbnail_info(folder_index, abs_path)
            thumbnail_url = thumbnail_info['url']
            if is_samsung and is_video:
                thumbnail_res = SubElement(item, 'res')
                thumbnail_res.text = thumbnail_url
                thumbnail_res.set('protocolInfo', 'http-get:*:image/jpeg:*')
                if thumbnail_info['size'] is not None:
                    thumbnail_res.set('size', str(thumbnail_info['size']))
                if thumbnail_info['resolution'] is not None:
                    thumbnail_res.set('resolution', thumbnail_info['resolution'])

                album_art = SubElement(item, 'upnp:albumArtURI')
                album_art.text = thumbnail_url

                sec_dcm_info = SubElement(item, 'sec:dcmInfo')
                sec_dcm_info.text = 'thumbnail'
            else:
                thumbnail_res = SubElement(item, 'res')
                thumbnail_res.text = thumbnail_url
                thumbnail_res.set('protocolInfo', self._thumbnail_protocol_info())
                thumbnail_res.set('dlna:profileID', 'JPEG_TN')
                if thumbnail_info['size'] is not None:
                    thumbnail_res.set('size', str(thumbnail_info['size']))
                if thumbnail_info['resolution'] is not None:
                    thumbnail_res.set('resolution', thumbnail_info['resolution'])

                album_art = SubElement(item, 'upnp:albumArtURI')
                album_art.set('dlna:profileID', 'JPEG_TN')
                album_art.text = thumbnail_url

                icon = SubElement(item, 'upnp:icon')
                icon.text = thumbnail_url

                if is_samsung:
                    sec_dcm_info = SubElement(item, 'sec:dcmInfo')
                    sec_dcm_info.text = 'thumbnail'

        self._add_audio_metadata(abs_path, item)

        try:
            modified = datetime.datetime.fromtimestamp(os.path.getmtime(abs_path))
            SubElement(item, 'dc:date').text = modified.isoformat()
        except OSError:
            pass

    def _add_audio_metadata(self, abs_path, item):
        ext = os.path.splitext(abs_path)[1].lower()
        if ext not in AUDIO_EXTENSIONS:
            return
        try:
            audio = File(abs_path)
            tags = getattr(audio, 'tags', None)
            if not tags:
                return
            artist = self._tag_value(tags, 'artist')
            album = self._tag_value(tags, 'album')
            genre = self._tag_value(tags, 'genre')
            if artist:
                SubElement(item, 'upnp:artist').text = artist
            if album:
                SubElement(item, 'upnp:album').text = album
            if genre:
                SubElement(item, 'upnp:genre').text = genre
        except Exception as exc:
            self.logger.debug(f'Error reading metadata for {abs_path}: {exc}')

    def _tag_value(self, tags, key):
        value = tags.get(key)
        if isinstance(value, (list, tuple)):
            return str(value[0]) if value else ''
        return str(value or '')

    def _folder_object_id(self, folder_index):
        return f'folder-{folder_index}'

    def _share_container_id(self, folder_index):
        return f'share-{folder_index}'

    def _merged_container_id(self):
        return 'merged'

    def _playlists_root_id(self):
        return 'playlists'

    def _playlist_container_id(self, playlist_index):
        return f'playlist-{playlist_index}'

    def _playlist_item_id(self, playlist_index, item_index):
        return f'{self._playlist_container_id(playlist_index)}/item/{item_index}'

    def _share_title(self, shared_folder, folder_index):
        normalized = os.path.normpath(shared_folder)
        name = os.path.basename(normalized)
        return name or f'Shared Path {folder_index + 1}'

    def _context_object_id(self, source, folder_index, abs_path):
        shared_root = self.http_server.media_folders[folder_index]
        relative_path = os.path.relpath(abs_path, shared_root).replace(os.sep, '/')
        if source == 'merged':
            return f'{self._merged_container_id()}/path/{folder_index}/{quote(relative_path, safe="/")}'
        return f'{self._share_container_id(folder_index)}/path/{quote(relative_path, safe="/")}'

    def _object_id_for_path(self, folder_index, abs_path):
        shared_root = self.http_server.media_folders[folder_index]
        relative_path = os.path.relpath(abs_path, shared_root).replace(os.sep, '/')
        if relative_path == '.':
            return self._folder_object_id(folder_index)
        return f'{self._folder_object_id(folder_index)}/{quote(relative_path, safe="/")}'

    def _resolve_object_id(self, object_id):
        if object_id == self._merged_container_id():
            return {
                'kind': 'merged',
                'source': 'merged',
                'object_id': object_id,
                'parent_id': '0',
                'title': 'All Shared Paths',
                'child_count': len(self._collect_context_entries({'source': 'merged'})),
            }

        if object_id == self._playlists_root_id():
            return {
                'kind': 'playlists-root',
                'object_id': object_id,
                'parent_id': '0',
                'title': 'Playlists',
                'child_count': len(self._playlist_definitions()),
            }

        if object_id.startswith('share-') and '/path/' not in object_id:
            try:
                folder_index = int(object_id.split('-', 1)[1])
            except ValueError:
                return None
            if not self._is_valid_folder_index(folder_index):
                return None
            return {
                'kind': 'share',
                'source': 'share',
                'object_id': object_id,
                'parent_id': '0',
                'folder_index': folder_index,
                'title': self._share_title(self.http_server.media_folders[folder_index], folder_index),
                'child_count': self._count_visible_children(self.http_server.media_folders[folder_index]),
            }

        playlist = self._resolve_playlist_object_id(object_id)
        if playlist is not None:
            return playlist

        context = self._resolve_context_object_id(object_id)
        if context is not None:
            return context

        legacy = self._resolve_legacy_object_id(object_id)
        if legacy is not None:
            return legacy
        return None

    def _resolve_playlist_object_id(self, object_id):
        if not object_id.startswith('playlist-'):
            return None
        parts = object_id.split('/')
        try:
            playlist_index = int(parts[0].split('-', 1)[1])
        except ValueError:
            return None
        playlists = self._playlist_definitions()
        if playlist_index < 0 or playlist_index >= len(playlists):
            return None
        playlist = playlists[playlist_index]
        if len(parts) == 1:
            return {
                'kind': 'playlist',
                'object_id': object_id,
                'parent_id': self._playlists_root_id(),
                'playlist_index': playlist_index,
                'title': playlist['name'],
                'child_count': len(playlist['items']),
                'items': playlist['items'],
            }
        if len(parts) == 3 and parts[1] == 'item':
            try:
                item_index = int(parts[2])
            except ValueError:
                return None
            if item_index < 0 or item_index >= len(playlist['items']):
                return None
            entry = playlist['items'][item_index]
            return {
                'kind': 'playlist-item',
                'object_id': object_id,
                'parent_id': self._playlist_container_id(playlist_index),
                'folder_index': entry['folder_index'],
                'abs_path': entry['abs_path'],
            }
        return None

    def _resolve_context_object_id(self, object_id):
        parts = object_id.split('/')
        if len(parts) < 3 or parts[1] != 'path':
            return None

        source_token = parts[0]
        if source_token == self._merged_container_id():
            source = 'merged'
            if len(parts) < 4:
                return None
            try:
                folder_index = int(parts[2])
            except ValueError:
                return None
            if not self._is_valid_folder_index(folder_index):
                return None
            encoded_relative = '/'.join(parts[3:])
        elif source_token.startswith('share-'):
            source = 'share'
            try:
                folder_index = int(source_token.split('-', 1)[1])
            except ValueError:
                return None
            if not self._is_valid_folder_index(folder_index):
                return None
            encoded_relative = '/'.join(parts[2:])
        else:
            return None

        if not encoded_relative:
            return None

        abs_path = self._resolve_relative_path(folder_index, encoded_relative, object_id)
        if abs_path is None:
            return None
        return {
            'kind': 'entry',
            'object_id': object_id,
            'source': source,
            'folder_index': folder_index,
            'abs_path': abs_path,
        }

    def _resolve_legacy_object_id(self, object_id):
        if not object_id.startswith('folder-'):
            return None
        folder_token, _, encoded_relative = object_id.partition('/')
        try:
            folder_index = int(folder_token.split('-', 1)[1])
        except (IndexError, ValueError):
            return None
        if not self._is_valid_folder_index(folder_index):
            return None
        if not encoded_relative:
            return {
                'kind': 'share',
                'object_id': self._share_container_id(folder_index),
                'source': 'share',
                'parent_id': '0',
                'folder_index': folder_index,
                'title': self._share_title(self.http_server.media_folders[folder_index], folder_index),
                'child_count': self._count_visible_children(self.http_server.media_folders[folder_index]),
            }
        abs_path = self._resolve_relative_path(folder_index, encoded_relative, object_id)
        if abs_path is None:
            return None
        return {
            'kind': 'entry',
            'object_id': self._context_object_id('share', folder_index, abs_path),
            'source': 'share',
            'folder_index': folder_index,
            'abs_path': abs_path,
        }

    def _resolve_relative_path(self, folder_index, encoded_relative, object_id):
        shared_root = self.http_server.media_folders[folder_index]
        relative_path = os.path.normpath(unquote(encoded_relative))
        abs_path = os.path.abspath(os.path.join(shared_root, relative_path))
        shared_root_abs = os.path.abspath(shared_root)
        if os.path.commonpath([shared_root_abs, abs_path]) != shared_root_abs:
            self.logger.warning('Rejected object_id=%s because path escaped share root', object_id)
            return None
        if not os.path.exists(abs_path):
            self.logger.warning('Rejected object_id=%s because path does not exist: %s', object_id, abs_path)
            return None
        return abs_path

    def _is_valid_folder_index(self, folder_index):
        return 0 <= folder_index < len(self.http_server.media_folders)

    def _playlist_definitions(self):
        return getattr(self.http_server, 'playlists', [])

    def _collect_context_entries(self, resolved):
        entries = []
        if resolved['source'] == 'merged':
            for folder_index, shared_folder in enumerate(self.http_server.media_folders):
                try:
                    for entry in os.scandir(shared_folder):
                        if self._is_visible_entry(entry.path, entry.is_dir()):
                            entries.append({'folder_index': folder_index, 'abs_path': entry.path})
                except OSError as exc:
                    self.logger.error(f'Error scanning root folder {shared_folder}: {exc}')
        else:
            shared_folder = self.http_server.media_folders[resolved['folder_index']]
            try:
                for entry in os.scandir(shared_folder):
                    if self._is_visible_entry(entry.path, entry.is_dir()):
                        entries.append({'folder_index': resolved['folder_index'], 'abs_path': entry.path})
            except OSError as exc:
                self.logger.error(f'Error scanning root folder {shared_folder}: {exc}')
        return self._sort_default_entries(entries)

    def _sort_default_entries(self, entries):
        return sorted(
            entries,
            key=lambda entry: (
                not os.path.isdir(entry['abs_path']),
                os.path.basename(entry['abs_path']).lower(),
            ),
        )

    def _parent_id_for_entry(self, resolved):
        shared_root = os.path.abspath(self.http_server.media_folders[resolved['folder_index']])
        abs_path = os.path.abspath(resolved['abs_path'])
        root_parent_id = self._merged_container_id() if resolved['source'] == 'merged' else self._share_container_id(resolved['folder_index'])
        parent_path = os.path.dirname(abs_path)
        if parent_path == shared_root:
            return root_parent_id
        return self._context_object_id(resolved['source'], resolved['folder_index'], parent_path)

    def _count_visible_children(self, directory):
        count = 0
        try:
            for entry in os.scandir(directory):
                if self._is_visible_entry(entry.path, entry.is_dir()):
                    count += 1
        except OSError as exc:
            self.logger.error(f'Error counting children in {directory}: {exc}')
        return count

    def _is_visible_entry(self, path, is_dir=None):
        if is_dir is None:
            is_dir = os.path.isdir(path)
        if is_dir:
            return True
        return os.path.splitext(path)[1].lower() in ALL_EXTENSIONS

    def _slice_entries(self, entries, starting_index, requested_count):
        if starting_index < 0:
            starting_index = 0
        if requested_count <= 0:
            return entries[starting_index:]
        return entries[starting_index:starting_index + requested_count]

    def _media_url(self, folder_index, abs_path):
        shared_root = self.http_server.media_folders[folder_index]
        relative_path = os.path.relpath(abs_path, shared_root).replace(os.sep, '/')
        quoted_path = quote(relative_path, safe='/')
        return f'http://{self.http_server.local_ip}:{self.http_server.server_port}/media/{folder_index}/{quoted_path}'

    def _thumbnail_url(self, folder_index, abs_path):
        shared_root = self.http_server.media_folders[folder_index]
        relative_path = os.path.relpath(abs_path, shared_root).replace(os.sep, '/')
        quoted_path = quote(relative_path, safe='/')
        return f'http://{self.http_server.local_ip}:{self.http_server.server_port}/thumbnails/{folder_index}/{quoted_path}.jpg'

    def _thumbnail_info(self, folder_index, abs_path):
        info = {
            'url': self._thumbnail_url(folder_index, abs_path),
            'size': None,
            'resolution': None,
        }
        generator = getattr(self.http_server, 'thumbnail_generator', None)
        if generator is None:
            return info
        try:
            thumbnail_path, _ = generator.ensure_static_thumbnail(abs_path, output_extension='jpg', verbose=0)
            if not thumbnail_path or not os.path.exists(thumbnail_path):
                return info
            info['size'] = os.path.getsize(thumbnail_path)
            try:
                from PIL import Image

                with Image.open(thumbnail_path) as image:
                    info['resolution'] = f'{image.width}x{image.height}'
            except Exception as exc:
                self.logger.debug('Could not inspect thumbnail dimensions for %s: %s', abs_path, exc)
        except Exception as exc:
            self.logger.debug('Could not prepare thumbnail metadata for %s: %s', abs_path, exc)
        return info

    def _upnp_class_for_extension(self, ext):
        if ext in VIDEO_EXTENSIONS:
            if self._is_likely_samsung_client():
                return 'object.item.videoItem'
            return 'object.item.videoItem.movie'
        if ext in AUDIO_EXTENSIONS:
            return 'object.item.audioItem.musicTrack'
        if ext in IMAGE_EXTENSIONS:
            return 'object.item.imageItem.photo'
        return 'object.item'

    def _protocol_info(self, ext, mime_type):
        profiles = {
            '.mp4': 'AVC_MP4_BL_CIF15_AAC',
            '.m4v': 'AVC_MP4_BL_CIF15_AAC',
            '.mp3': 'MP3',
            '.flac': 'FLAC',
            '.jpg': 'JPEG_LRG',
            '.jpeg': 'JPEG_LRG',
            '.png': 'PNG_LRG',
        }
        profile = profiles.get(ext)
        if not profile:
            return f'http-get:*:{mime_type}:*'
        return (
            f'http-get:*:{mime_type}:'
            f'DLNA.ORG_PN={profile};DLNA.ORG_OP=01;DLNA.ORG_CI=0;'
            'DLNA.ORG_FLAGS=01700000000000000000000000000000'
        )

    def _thumbnail_protocol_info(self):
        return (
            'http-get:*:image/jpeg:'
            'DLNA.ORG_PN=JPEG_TN;DLNA.ORG_OP=01;DLNA.ORG_CI=0;'
            'DLNA.ORG_FLAGS=00f00000000000000000000000000000'
        )

    def get_media_duration(self, file_path):
        try:
            media = File(file_path)
            if media and getattr(media, 'info', None) and hasattr(media.info, 'length'):
                duration_seconds = int(media.info.length)
                hours = duration_seconds // 3600
                minutes = (duration_seconds % 3600) // 60
                seconds = duration_seconds % 60
                return f'{hours}:{minutes:02}:{seconds:02}.000'
        except Exception as exc:
            self.logger.debug(f'Could not get duration for {file_path}: {exc}')
        return None

    def get_image_resolution(self, file_path):
        ext = os.path.splitext(file_path)[1].lower()
        if ext not in IMAGE_EXTENSIONS:
            return None
        try:
            from PIL import Image

            Image.MAX_IMAGE_PIXELS = None
            with Image.open(file_path) as image:
                width, height = image.size
                return f'{width}x{height}'
        except ImportError:
            return None
        except Exception as exc:
            self.logger.debug(f'Could not get resolution for {file_path}: {exc}')
            return None

    def encode_didl(self, root_element):
        return tostring(root_element, encoding='unicode')