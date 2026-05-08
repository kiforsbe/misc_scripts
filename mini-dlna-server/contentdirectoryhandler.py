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

        folder_index, abs_path = resolved
        if browse_flag == 'BrowseMetadata':
            self._add_entry(root, folder_index, abs_path, self._parent_id_for_path(folder_index, abs_path))
            return self.encode_didl(root), 1, 1

        if not os.path.isdir(abs_path):
            return self.encode_didl(root), 0, 0

        return self._browse_directory_children(root, folder_index, abs_path, object_id, starting_index, requested_count)

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
        total_children = 0
        for shared_folder in self.http_server.media_folders:
            total_children += self._count_visible_children(shared_folder)

        self.logger.info('Root metadata reports %s visible children', total_children)

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
            try:
                for entry in os.scandir(shared_folder):
                    if self._is_visible_entry(entry.path, entry.is_dir()):
                        entries.append((index, entry.path))
            except OSError as exc:
                self.logger.error(f'Error scanning root folder {shared_folder}: {exc}')
        entries.sort(key=lambda item: os.path.basename(item[1]).lower())
        return entries

    def _append_root_children(self, root, starting_index, requested_count):
        entries = self._collect_root_entries()
        selected = self._slice_entries(entries, starting_index, requested_count)
        self.logger.info(
            'Root metadata compatibility append total=%s selected=%s preview=%s',
            len(entries),
            len(selected),
            [os.path.basename(path) for _, path in selected[:5]],
        )
        for folder_index, entry_path in selected:
            self._add_entry(root, folder_index, entry_path, '0')

    def _browse_root_children(self, root, starting_index, requested_count):
        entries = self._collect_root_entries()
        total = len(entries)
        selected = self._slice_entries(entries, starting_index, requested_count)
        self.logger.info(
            'Root children total=%s selected=%s preview=%s',
            total,
            len(selected),
            [os.path.basename(path) for _, path in selected[:5]],
        )
        for folder_index, entry_path in selected:
            self._add_entry(root, folder_index, entry_path, '0')
        return self.encode_didl(root), len(selected), total

    def _browse_directory_children(self, root, folder_index, abs_path, parent_id, starting_index, requested_count):
        entries = []
        try:
            for entry in sorted(os.scandir(abs_path), key=lambda item: item.name.lower()):
                if self._is_visible_entry(entry.path, entry.is_dir()):
                    entries.append(entry.path)
        except OSError as exc:
            self.logger.error(f'Error scanning directory {abs_path}: {exc}')
            return self.encode_didl(root), 0, 0

        selected = self._slice_entries(entries, starting_index, requested_count)
        self.logger.info(
            'Directory children path=%s total=%s selected=%s preview=%s',
            abs_path,
            len(entries),
            len(selected),
            [os.path.basename(path) for path in selected[:5]],
        )
        for entry_path in selected:
            self._add_entry(root, folder_index, entry_path, parent_id)
        return self.encode_didl(root), len(selected), len(entries)

    def _add_entry(self, root, folder_index, abs_path, parent_id):
        if os.path.isdir(abs_path):
            self._add_container(root, folder_index, abs_path, parent_id)
        else:
            self._add_item(root, folder_index, abs_path, parent_id)

    def _add_container(self, root, folder_index, abs_path, parent_id):
        container = SubElement(
            root,
            'container',
            {
                'id': self._object_id_for_path(folder_index, abs_path),
                'parentID': parent_id,
                'restricted': '1',
                'searchable': '1',
                'childCount': str(self._count_visible_children(abs_path)),
            },
        )
        SubElement(container, 'dc:title').text = os.path.basename(abs_path)
        SubElement(container, 'upnp:class').text = 'object.container.storageFolder'

    def _add_item(self, root, folder_index, abs_path, parent_id):
        ext = os.path.splitext(abs_path)[1].lower()
        mime_type = ALL_EXTENSIONS.get(ext)
        if not mime_type:
            return

        item = SubElement(
            root,
            'item',
            {
                'id': self._object_id_for_path(folder_index, abs_path),
                'parentID': parent_id,
                'restricted': '1',
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
            album_art = SubElement(item, 'upnp:albumArtURI')
            album_art.set('dlna:profileID', 'JPEG_TN')
            album_art.text = f'{self._media_url(folder_index, abs_path)}?thumbnail=true'

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

    def _object_id_for_path(self, folder_index, abs_path):
        shared_root = self.http_server.media_folders[folder_index]
        relative_path = os.path.relpath(abs_path, shared_root).replace(os.sep, '/')
        if relative_path == '.':
            return self._folder_object_id(folder_index)
        return f'{self._folder_object_id(folder_index)}/{quote(relative_path, safe="/")}'

    def _resolve_object_id(self, object_id):
        if not object_id.startswith('folder-'):
            return None
        folder_token, _, encoded_relative = object_id.partition('/')
        try:
            folder_index = int(folder_token.split('-', 1)[1])
        except (IndexError, ValueError):
            return None
        if folder_index < 0 or folder_index >= len(self.http_server.media_folders):
            return None
        shared_root = self.http_server.media_folders[folder_index]
        if not encoded_relative:
            return folder_index, shared_root
        relative_path = os.path.normpath(unquote(encoded_relative))
        abs_path = os.path.abspath(os.path.join(shared_root, relative_path))
        shared_root_abs = os.path.abspath(shared_root)
        if os.path.commonpath([shared_root_abs, abs_path]) != shared_root_abs:
            self.logger.warning('Rejected object_id=%s because path escaped share root', object_id)
            return None
        if not os.path.exists(abs_path):
            self.logger.warning('Rejected object_id=%s because path does not exist: %s', object_id, abs_path)
            return None
        return folder_index, abs_path

    def _parent_id_for_path(self, folder_index, abs_path):
        shared_root = os.path.abspath(self.http_server.media_folders[folder_index])
        abs_path = os.path.abspath(abs_path)
        if abs_path == shared_root:
            return '0'
        parent_path = os.path.dirname(abs_path)
        if parent_path == shared_root:
            return '0'
        return self._object_id_for_path(folder_index, parent_path)

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
        return f'http://{self.http_server.local_ip}:{self.http_server.server_port}/media/{quoted_path}'

    def _upnp_class_for_extension(self, ext):
        if ext in VIDEO_EXTENSIONS:
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