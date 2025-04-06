import datetime
import logging
import os
from mutagen import File
from urllib.parse import unquote, quote, urlparse, parse_qs
from xml.etree import ElementTree
from xml.etree.ElementTree import Element, SubElement, tostring, fromstring

# Supported extensions
VIDEO_EXTENSIONS = {'.mp4': 'video/mp4', '.mkv': 'video/x-matroska', '.avi': 'video/x-msvideo'}
AUDIO_EXTENSIONS = {'.mp3': 'audio/mpeg', '.flac': 'audio/flac', '.wav': 'audio/wav'}
IMAGE_EXTENSIONS = {'.jpg': 'image/jpeg', '.png': 'image/png', '.gif': 'image/gif'}

class DLNAXMLGenerator:
    @staticmethod
    def create_didl_container(id, parent_id, title, child_count):
        """Create a DIDL-Lite container element"""
        container = Element('container', {
            'id': id,
            'parentID': parent_id,
            'restricted': '1',
            'searchable': '1',
            'childCount': str(child_count)
        })
        SubElement(container, 'dc:title').text = title
        SubElement(container, 'upnp:class').text = 'object.container.storageFolder'
        return container

    @staticmethod
    def create_didl_item(id, parent_id, title, resource_url, mime_type, size):
        """Create a DIDL-Lite item element"""
        item = Element('item', {
            'id': id,
            'parentID': parent_id,
            'restricted': '1'
        })
        SubElement(item, 'dc:title').text = title
        res = SubElement(item, 'res')
        res.text = resource_url
        res.set('protocolInfo', f'http-get:*:{mime_type}:*')
        res.set('size', str(size))
        return item

class DLNARequestParser:
    @staticmethod
    def parse_browse_request(soap_body):
        """Parse Browse action request parameters with improved error handling"""
        try:
            root = ElementTree.fromstring(soap_body)
            namespaces = {
                's': 'http://schemas.xmlsoap.org/soap/envelope/',
                'u': 'urn:schemas-upnp-org:service:ContentDirectory:1'
            }
            
            # Try both with and without namespace
            browse = (root.find('.//u:Browse', namespaces) or 
                    root.find('.//Browse') or 
                    root.find(".//{urn:schemas-upnp-org:service:ContentDirectory:1}Browse"))
            
            if browse is None:
                raise ValueError("Browse action not found in SOAP request")

            # Safe extraction of parameters with defaults
            return {
                'object_id': (browse.find('ObjectID') or browse.find('./ObjectID')).text or '0',
                'browse_flag': (browse.find('BrowseFlag') or browse.find('./BrowseFlag')).text or 'BrowseDirectChildren',
                'filter': (browse.find('Filter') or browse.find('./Filter')).text or '*',
                'starting_index': int((browse.find('StartingIndex') or browse.find('./StartingIndex')).text or '0'),
                'requested_count': int((browse.find('RequestedCount') or browse.find('./RequestedCount')).text or '0'),
                'sort_criteria': (browse.find('SortCriteria') or browse.find('./SortCriteria')).text or ''
            }
        except (AttributeError, ValueError) as e:
            raise ValueError(f"Invalid Browse request: {str(e)}")
        except Exception as e:
            raise ValueError(f"Error parsing Browse request: {str(e)}")

class ContentDirectoryHandler:
    """Handles all Content Directory service requests"""
    def __init__(self, dlna_server):
        self.server = dlna_server
        self.logger = logging.getLogger(__name__)
        self.soap_handler = dlna_server.soap_handler
        self.error_handler = dlna_server.error_handler
        self.request_parser = dlna_server.request_parser
        self.xml_generator = dlna_server.xml_generator

    def handle_control(self, post_data):
        """Handle POST requests to /ContentDirectory/control"""
        try:
            self.logger.info(f"Received ContentDirectory control request (length: {len(post_data)})")
            self.logger.debug(f"SOAP Request Body: {post_data.decode('utf-8', errors='ignore')}")

            # Parse the SOAP request robustly
            try:
                # Extract browse parameters
                params = self.request_parser.parse_browse_request(post_data)
                
                self.logger.info(f"Browse Request: ObjectID='{params['object_id']}', BrowseFlag='{params['browse_flag']}', "
                                f"StartIndex={params['starting_index']}, Count={params['requested_count']}, "
                                f"Filter='{params['filter']}', Sort='{params['sort_criteria']}'")

                # Generate browse response
                result_didl, number_returned, total_matches = self.generate_browse_didl(
                    params['object_id'], 
                    params['browse_flag'],
                    params['starting_index'], 
                    params['requested_count'],
                    params['filter'],
                    params['sort_criteria']
                )
                
                # Send SOAP response using handler
                response_content = f'''
                    <Result><![CDATA[{result_didl}]]></Result>
                    <NumberReturned>{number_returned}</NumberReturned>
                    <TotalMatches>{total_matches}</TotalMatches>
                    <UpdateID>1</UpdateID>'''
                
                self.soap_handler.send_soap_response(
                    response_content,
                    'Browse',
                    'urn:schemas-upnp-org:service:ContentDirectory:1'
                )
                
                self.logger.info(f"Sent BrowseResponse for ObjectID '{params['object_id']}' ({number_returned}/{total_matches} items)")

            except ValueError as ve:
                self.error_handler.handle_request_error(self.server, ve, 400)

        except Exception as e:
            self.error_handler.handle_request_error(self.server, e)

    def generate_browse_didl(self, object_id, browse_flag, starting_index, requested_count, filter_str, sort_criteria):
        """Generates the DIDL-Lite XML string for Samsung TV compatibility"""
        self.logger.debug(f"Generating DIDL - ObjectID: {object_id}, BrowseFlag: {browse_flag}, StartIndex: {starting_index}, Count: {requested_count}")
        
        root = Element('DIDL-Lite', {
            'xmlns': 'urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/',
            'xmlns:dc': 'http://purl.org/dc/elements/1.1/',
            'xmlns:upnp': 'urn:schemas-upnp-org:metadata-1-0/upnp/',
            'xmlns:dlna': 'urn:schemas-dlna-org:metadata-1-0/',
            'xmlns:sec': 'http://www.sec.co.kr/dlna'
        })

        try:
            if object_id == '0':  # Root container
                if browse_flag == 'BrowseMetadata':
                    return self._handle_root_metadata(root)
                elif browse_flag == 'BrowseDirectChildren':
                    return self._handle_root_children(root, starting_index, requested_count)
            else:  # Non-root items
                if browse_flag == 'BrowseMetadata':
                    return self._handle_item_metadata(root, object_id)
                elif browse_flag == 'BrowseDirectChildren':
                    return self._handle_item_children(root, object_id, starting_index, requested_count)

            return self.encode_didl(root), 0, 0

        except Exception as e:
            self.logger.error(f"Error in generate_browse_didl: {e}", exc_info=True)
            return self.encode_didl(root), 0, 0

    def _handle_root_metadata(self, root):
        """Handle BrowseMetadata for root container"""
        # Count valid media files and folders
        total_children = 0
        for shared_folder in self.server.media_folders:
            with os.scandir(shared_folder) as entries:
                for entry in entries:
                    if entry.is_dir() or (entry.is_file() and os.path.splitext(entry.name)[1].lower() in 
                        {**VIDEO_EXTENSIONS, **AUDIO_EXTENSIONS, **IMAGE_EXTENSIONS}):
                        total_children += 1
        
        container = SubElement(root, 'container', {
            'id': '0',
            'parentID': '-1',
            'restricted': 'false',
            'searchable': 'true',
            'childCount': str(total_children),
            'dlna:dlnaManaged': '00000004'
        })
        
        SubElement(container, 'dc:title').text = "Root"
        SubElement(container, 'upnp:class').text = 'object.container'
        SubElement(container, 'upnp:storageUsed').text = '-1'
        SubElement(container, 'sec:deviceID').text = str(DEVICE_UUID)
        SubElement(container, 'sec:containerType').text = 'DLNA'
        
        result = self.encode_didl(root)
        self.logger.debug(f"Root BrowseMetadata response - Children: {total_children}, DIDL: {result}")
        return result, total_children, total_children

    def _handle_root_children(self, root, starting_index, requested_count):
        """Handle BrowseDirectChildren for root container"""
        total_matched = 0
        items_added = 0

        for shared_folder in self.server.media_folders:
            try:
                entries = list(os.scandir(shared_folder))
                entries.sort(key=lambda x: x.name.lower())
                
                for entry in entries:
                    if total_matched >= starting_index:
                        if requested_count > 0 and items_added >= requested_count:
                            break

                        if entry.is_dir():
                            self.add_container_to_didl(root, entry.path, entry.name, '0')
                            items_added += 1
                        elif entry.is_file():
                            ext = os.path.splitext(entry.name)[1].lower()
                            if ext in VIDEO_EXTENSIONS or ext in AUDIO_EXTENSIONS or ext in IMAGE_EXTENSIONS:
                                self.add_item_to_didl(root, entry.path, entry.name, '0')
                                items_added += 1
                    
                    if entry.is_dir() or (entry.is_file() and 
                        os.path.splitext(entry.name)[1].lower() in 
                        {**VIDEO_EXTENSIONS, **AUDIO_EXTENSIONS, **IMAGE_EXTENSIONS}):
                        total_matched += 1

            except OSError as e:
                self.logger.error(f"Error scanning directory {shared_folder}: {e}")
                continue

        result = self.encode_didl(root)
        self.logger.debug(f"Root BrowseDirectChildren response - Added: {items_added}, Total: {total_matched}")
        return result, items_added, total_matched

    def _handle_item_metadata(self, root, object_id):
        """Handle BrowseMetadata for non-root items"""
        actual_path = self._find_actual_path(object_id)
        if actual_path and os.path.exists(actual_path):
            if os.path.isdir(actual_path):
                return self._handle_directory_metadata(root, actual_path, object_id)
            else:
                return self._handle_file_metadata(root, actual_path, object_id)
        return self.encode_didl(root), 0, 0

    def _handle_item_children(self, root, object_id, starting_index, requested_count):
        """Handle BrowseDirectChildren for non-root items"""
        dir_path = self._find_directory_path(object_id)
        if dir_path:
            try:
                entries = list(os.scandir(dir_path))
                entries.sort(key=lambda x: x.name.lower())
                total_matched = 0
                items_added = 0

                for entry in entries:
                    if total_matched >= starting_index:
                        if requested_count > 0 and items_added >= requested_count:
                            break

                        if entry.is_dir():
                            self.add_container_to_didl(root, entry.path, entry.name, object_id)
                            items_added += 1
                        elif entry.is_file():
                            ext = os.path.splitext(entry.name)[1].lower()
                            if ext in VIDEO_EXTENSIONS or ext in AUDIO_EXTENSIONS or ext in IMAGE_EXTENSIONS:
                                self.add_item_to_didl(root, entry.path, entry.name, object_id)
                                items_added += 1

                    if entry.is_dir() or (entry.is_file() and 
                        os.path.splitext(entry.name)[1].lower() in 
                        {**VIDEO_EXTENSIONS, **AUDIO_EXTENSIONS, **IMAGE_EXTENSIONS}):
                        total_matched += 1

                result = self.encode_didl(root)
                self.logger.debug(f"Directory BrowseDirectChildren response - Added: {items_added}, Total: {total_matched}")
                return result, items_added, total_matched

            except OSError as e:
                self.logger.error(f"Error scanning directory {dir_path}: {e}")

        return self.encode_didl(root), 0, 0

    def generate_browse_didl(self, object_id, browse_flag, starting_index, requested_count, filter_str, sort_criteria):
        """Generates the DIDL-Lite XML string for Samsung TV compatibility"""
        self.logger.debug(f"Generating DIDL - ObjectID: {object_id}, BrowseFlag: {browse_flag}, StartIndex: {starting_index}, Count: {requested_count}")
        
        root = Element('DIDL-Lite', {
            'xmlns': 'urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/',
            'xmlns:dc': 'http://purl.org/dc/elements/1.1/',
            'xmlns:upnp': 'urn:schemas-upnp-org:metadata-1-0/upnp/',
            'xmlns:dlna': 'urn:schemas-dlna-org:metadata-1-0/',  # Fixed namespace
            'xmlns:sec': 'http://www.sec.co.kr/dlna'
        })

        try:
            if object_id == '0':  # Root container
                if browse_flag == 'BrowseMetadata':
                    self.logger.debug("Processing BrowseMetadata for root container")
                    # Count valid media files and folders
                    total_children = 0
                    for shared_folder in self.server.media_folders:
                        with os.scandir(shared_folder) as entries:
                            for entry in entries:
                                if entry.is_dir() or (entry.is_file() and os.path.splitext(entry.name)[1].lower() in 
                                    {**VIDEO_EXTENSIONS, **AUDIO_EXTENSIONS, **IMAGE_EXTENSIONS}):
                                    total_children += 1
                    
                    # Updated root container attributes
                    container = SubElement(root, 'container', {
                        'id': '0',
                        'parentID': '-1',  # Changed from -1 to 0
                        'restricted': 'false',
                        'searchable': 'true',
                        'childCount': str(total_children),
                        'dlna:dlnaManaged': '00000004'  # Add DLNA managed flag
                    })
                    
                    # Add required elements for root container
                    SubElement(container, 'dc:title').text = "Root"
                    SubElement(container, 'upnp:class').text = 'object.container'
                    SubElement(container, 'upnp:storageUsed').text = '-1'
                    SubElement(container, 'sec:deviceID').text = str(DEVICE_UUID)  # Add Samsung device ID
                    SubElement(container, 'sec:containerType').text = 'DLNA'  # Add Samsung container type
                    
                    result = self.encode_didl(root)
                    self.logger.debug(f"Root BrowseMetadata response - Children: {total_children}, DIDL: {result}")
                    return result, total_children, total_children

                elif browse_flag == 'BrowseDirectChildren':
                    self.logger.debug("Processing BrowseDirectChildren for root container")
                    total_matched = 0
                    items_added = 0

                    for shared_folder in self.server.media_folders:
                        try:
                            entries = list(os.scandir(shared_folder))
                            entries.sort(key=lambda x: x.name.lower())  # Sort entries alphabetically
                            
                            for entry in entries:
                                if total_matched >= starting_index:
                                    if requested_count > 0 and items_added >= requested_count:
                                        break

                                    if entry.is_dir():
                                        self.add_container_to_didl(root, entry.path, entry.name, '0')
                                        items_added += 1
                                    elif entry.is_file():
                                        ext = os.path.splitext(entry.name)[1].lower()
                                        if ext in VIDEO_EXTENSIONS or ext in AUDIO_EXTENSIONS or ext in IMAGE_EXTENSIONS:
                                            self.add_item_to_didl(root, entry.path, entry.name, '0')
                                            items_added += 1
                                
                                if entry.is_dir() or (entry.is_file() and 
                                    os.path.splitext(entry.name)[1].lower() in 
                                    {**VIDEO_EXTENSIONS, **AUDIO_EXTENSIONS, **IMAGE_EXTENSIONS}):
                                    total_matched += 1

                        except OSError as e:
                            self.logger.error(f"Error scanning directory {shared_folder}: {e}")
                            continue

                    result = self.encode_didl(root)
                    self.logger.debug(f"Root BrowseDirectChildren response - Added: {items_added}, Total: {total_matched}")
                    return result, items_added, total_matched

            else:  # Non-root items
                if browse_flag == 'BrowseMetadata':
                    self.logger.debug(f"Processing BrowseMetadata for object: {object_id}")
                    # Find the actual path from object_id
                    actual_path = None
                    for shared_folder in self.server.media_folders:
                        potential_path = os.path.join(shared_folder, unquote(object_id))
                        if os.path.exists(potential_path):
                            actual_path = potential_path
                            self.logger.debug(f"Found matching path: {actual_path}")
                            break

                    if actual_path and os.path.exists(actual_path):
                        if os.path.isdir(actual_path):
                            self.logger.debug(f"Processing directory metadata: {actual_path}")
                            child_count = 0
                            try:
                                with os.scandir(actual_path) as entries:
                                    for entry in entries:
                                        if entry.is_dir() or (entry.is_file() and os.path.splitext(entry.name)[1].lower() in 
                                            {**VIDEO_EXTENSIONS, **AUDIO_EXTENSIONS, **IMAGE_EXTENSIONS}):
                                            child_count += 1
                                            self.logger.debug(f"Found valid child: {entry.name} ({child_count})")
                            except OSError as e:
                                self.logger.error(f"Error counting children in {actual_path}: {e}")

                            container = SubElement(root, 'container', {
                                'id': object_id,
                                'parentID': os.path.dirname(object_id) or '0',
                                'restricted': '1',
                                'searchable': '1',
                                'childCount': str(child_count)
                            })
                            SubElement(container, 'dc:title').text = os.path.basename(actual_path)
                            SubElement(container, 'upnp:class').text = 'object.container.storageFolder'
                            
                            result = self.encode_didl(root)
                            self.logger.debug(f"Directory BrowseMetadata response - Path: {actual_path}, Children: {child_count}")
                            return result, 1, 1

                        else:  # File metadata
                            self.logger.debug(f"Processing file metadata: {actual_path}")
                            self.add_item_to_didl(root, actual_path, os.path.basename(actual_path), 
                                                os.path.dirname(object_id) or '0')
                            result = self.encode_didl(root)
                            self.logger.debug(f"File BrowseMetadata response - Path: {actual_path}")
                            return result, 1, 1

                    else:
                        self.logger.error(f"Path not found for object_id: {object_id}")
                        return self.encode_didl(root), 0, 0

                elif browse_flag == 'BrowseDirectChildren':
                    self.logger.debug(f"Processing BrowseDirectChildren for object: {object_id}")
                    # Find directory path
                    dir_path = None
                    for shared_folder in self.server.media_folders:
                        potential_path = os.path.join(shared_folder, unquote(object_id))
                        if os.path.exists(potential_path) and os.path.isdir(potential_path):
                            dir_path = potential_path
                            break

                    if dir_path:
                        try:
                            entries = list(os.scandir(dir_path))
                            entries.sort(key=lambda x: x.name.lower())  # Sort entries alphabetically
                            total_matched = 0
                            items_added = 0

                            for entry in entries:
                                if total_matched >= starting_index:
                                    if requested_count > 0 and items_added >= requested_count:
                                        break

                                    if entry.is_dir():
                                        self.add_container_to_didl(root, entry.path, entry.name, object_id)
                                        items_added += 1
                                    elif entry.is_file():
                                        ext = os.path.splitext(entry.name)[1].lower()
                                        if ext in VIDEO_EXTENSIONS or ext in AUDIO_EXTENSIONS or ext in IMAGE_EXTENSIONS:
                                            self.add_item_to_didl(root, entry.path, entry.name, object_id)
                                            items_added += 1

                                if entry.is_dir() or (entry.is_file() and 
                                    os.path.splitext(entry.name)[1].lower() in 
                                    {**VIDEO_EXTENSIONS, **AUDIO_EXTENSIONS, **IMAGE_EXTENSIONS}):
                                    total_matched += 1

                            result = self.encode_didl(root)
                            self.logger.debug(f"Directory BrowseDirectChildren response - Added: {items_added}, Total: {total_matched}")
                            return result, items_added, total_matched

                        except OSError as e:
                            self.logger.error(f"Error scanning directory {dir_path}: {e}")
                            return self.encode_didl(root), 0, 0

                    else:
                        self.logger.error(f"Directory not found for object_id: {object_id}")
                        return self.encode_didl(root), 0, 0

            return self.encode_didl(root), 0, 0

        except Exception as e:
            self.logger.error(f"Error in generate_browse_didl: {e}", exc_info=True)
            return self.encode_didl(root), 0, 0

    def encode_didl(self, root_element):
        """Encodes the ElementTree DIDL-Lite to a string suitable for SOAP response."""
        # Convert to string and escape XML special characters for embedding in SOAP
        xml_string = tostring(root_element, encoding='unicode')
        # Basic escaping for embedding in XML. More robust escaping might be needed.
        return f"{xml_string}" #.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

    def find_shared_folder_root(self, abs_path):
        """Finds which shared folder an absolute path belongs to."""
        abs_path = os.path.abspath(abs_path)
        for shared_folder in self.server.media_folders:
            shared_folder_abs = os.path.abspath(shared_folder)
            if os.path.commonpath([shared_folder_abs, abs_path]) == shared_folder_abs:
                return shared_folder_abs
        return None # Path not found within any shared folder

    def add_container_to_didl(self, root, path, title, parent_id):
        """Add a container (directory) to the DIDL-Lite XML"""
        try:
            # For root-level container (shared folder)
            if parent_id == '0':
                container_id = quote(os.path.basename(path))
            else:
                shared_root = self.find_shared_folder_root(path)
                if not shared_root:
                    self.logger.warning(f"Cannot determine relative path for container: {path}")
                    return

                relative_path = os.path.relpath(path, shared_root)
                if relative_path == '.':
                    container_id = quote(os.path.basename(path))
                else:
                    container_id = quote(relative_path.replace('\\', '/'))

            # Calculate child count
            child_count = 0
            try:
                with os.scandir(path) as entries:
                    for entry in entries:
                        if entry.is_dir():
                            child_count += 1
                        elif entry.is_file():
                            ext = os.path.splitext(entry.name)[1].lower()
                            if ext in VIDEO_EXTENSIONS or ext in AUDIO_EXTENSIONS or ext in IMAGE_EXTENSIONS:
                                child_count += 1
            except OSError as e:
                self.logger.error(f"Error counting children for {path}: {e}")
                child_count = 0

            container = SubElement(root, 'container', {
                'id': container_id,
                'parentID': parent_id,
                'restricted': '1',
                'searchable': '1',
                'childCount': str(child_count)
            })

            SubElement(container, 'dc:title').text = title
            SubElement(container, 'upnp:class').text = 'object.container.storageFolder'

            try:
                mod_time = datetime.fromtimestamp(os.path.getmtime(path))
                SubElement(container, 'dc:date').text = mod_time.isoformat()
            except OSError:
                pass

            self.logger.debug(f"Added container: id='{container_id}', parentID='{parent_id}', title='{title}', childCount={child_count}")

        except Exception as e:
            self.logger.error(f"Error adding container to DIDL for path {path}: {e}", exc_info=True)

    def add_item_to_didl(self, root, path, title, parent_id, next_id=None):
        """Add an item to the DIDL-Lite XML with Samsung TV compatibility"""
        try:
            shared_root = self.find_shared_folder_root(path)
            if not shared_root:
                self.logger.warning(f"Cannot determine relative path for item: {path}")
                return

            relative_path = os.path.relpath(path, shared_root)
            item_id = quote(relative_path.replace('\\', '/'))
            ext = os.path.splitext(path)[1].lower()

            mime_type = (VIDEO_EXTENSIONS.get(ext) or 
                        AUDIO_EXTENSIONS.get(ext) or 
                        IMAGE_EXTENSIONS.get(ext))
            if not mime_type:
                self.logger.debug(f"Skipping item with unknown type: {path}")
                return

            # Determine DLNA profile and class
            dlna_profile, protocol_info = self.get_dlna_profile(ext, mime_type)
            upnp_class = ('object.item.videoItem.movie' if ext in VIDEO_EXTENSIONS else
                        'object.item.audioItem.musicTrack' if ext in AUDIO_EXTENSIONS else
                        'object.item.imageItem.photo' if ext in IMAGE_EXTENSIONS else
                        'object.item')

            # Create item element with required attributes
            item = SubElement(root, 'item', {
                'id': item_id,
                'parentID': parent_id,
                'restricted': '1',
                'dlna:dlnaManaged': '00000001'  # Samsung TV compatibility
            })

            # Add basic metadata
            SubElement(item, 'dc:title').text = title
            SubElement(item, 'upnp:class').text = upnp_class

            # Add resource element with full metadata
            res = SubElement(item, 'res')
            try:
                file_size = os.path.getsize(path)
                url = f'http://{self.server.server_address[0]}:{self.server.server_address[1]}/{quote(relative_path)}'
                res.text = url
                res.set('size', str(file_size))
                res.set('protocolInfo', protocol_info)

                # Add media-specific metadata
                if ext in VIDEO_EXTENSIONS:
                    duration = self.get_media_duration_seconds(path)
                    if duration:
                        res.set('duration', str(datetime.timedelta(seconds=int(duration))))
                        res.set('sampleRate', '48000')  # Common video sample rate
                        res.set('nrAudioChannels', '2')  # Stereo audio
                    # Add video thumbnail
                    thumb = SubElement(item, 'upnp:albumArtURI')
                    thumb.set('dlna:profileID', 'JPEG_TN')
                    thumb.set('xmlns:dlna', 'urn:schemas-dlna-org:metadata-1-0')
                    thumb.text = f'{url}?thumbnail=true'
                    # Add Samsung-specific video metadata
                    SubElement(item, 'sec:CaptionInfo').text = 'No'
                    SubElement(item, 'sec:CaptionInfoEx').text = 'No'
                    SubElement(item, 'sec:dcmInfo').text = 'No'

                elif ext in AUDIO_EXTENSIONS:
                    duration = self.get_media_duration_seconds(path)
                    if duration:
                        res.set('duration', str(datetime.timedelta(seconds=int(duration))))
                    # Add audio metadata from file
                    try:
                        audio = File(path)
                        if audio and hasattr(audio, 'tags'):
                            tags = audio.tags
                            if hasattr(tags, 'get'):  # Handle both dict-like and object interfaces
                                artist = str(tags.get('artist', [''])[0]) if isinstance(tags.get('artist', ['']), (list, tuple)) else str(tags.get('artist', ''))
                                album = str(tags.get('album', [''])[0]) if isinstance(tags.get('album', ['']), (list, tuple)) else str(tags.get('album', ''))
                                genre = str(tags.get('genre', [''])[0]) if isinstance(tags.get('genre', ['']), (list, tuple)) else str(tags.get('genre', ''))
                                if artist:
                                    SubElement(item, 'upnp:artist').text = artist
                                if album:
                                    SubElement(item, 'upnp:album').text = album
                                if genre:
                                    SubElement(item, 'upnp:genre').text = genre
                    except Exception as e:
                        self.logger.debug(f"Error reading audio metadata: {e}")

                elif ext in IMAGE_EXTENSIONS:
                    # Add image resolution if available
                    resolution = self.get_image_resolution(path)
                    if resolution:
                        res.set('resolution', resolution)
                    # Add thumbnail for images
                    thumb = SubElement(item, 'upnp:albumArtURI')
                    thumb.set('dlna:profileID', 'JPEG_TN')
                    thumb.set('xmlns:dlna', 'urn:schemas-dlna-org:metadata-1-0')
                    thumb.text = f'{url}?thumbnail=true'

                # Add modification date
                try:
                    mod_time = datetime.fromtimestamp(os.path.getmtime(path))
                    SubElement(item, 'dc:date').text = mod_time.isoformat()
                except OSError:
                    pass

            except Exception as e:
                self.logger.error(f"Error adding resource element for {path}: {e}")

        except Exception as e:
            self.logger.error(f"Error adding item to DIDL-Lite for {path}: {e}", exc_info=True)

    def get_media_duration(self, file_path):
        """Get media duration using mutagen (works for audio/some video)"""
        try:
            media = File(file_path)
            if media and media.info and hasattr(media.info, 'length') and media.info.length > 0:
                duration_sec = int(media.info.length)
                hours = duration_sec // 3600
                minutes = (duration_sec % 3600) // 60
                seconds = duration_sec % 60
                # Format as H:MM:SS.ms (UPnP standard) - add .000 for milliseconds
                return f"{hours}:{minutes:02}:{seconds:02}.000"
        except Exception as e:
            self.logger.debug(f"Could not get duration for {file_path}: {e}")
        return None

    def get_image_resolution(self, file_path):
        """Get image resolution using Pillow"""
        try:
            from PIL import Image
            # Suppress DecompressionBomb warning if images are large
            Image.MAX_IMAGE_PIXELS = None
            with Image.open(file_path) as img:
                width, height = img.size
                return f"{width}x{height}"
        except ImportError:
            self.logger.debug("Pillow not installed, cannot get image resolution.")
        except Exception as e:
            self.logger.warning(f"Could not get resolution for image {file_path}: {e}")
        return None

    def get_mime_and_upnp_class(self, filename):
        """Determine MIME type and UPnP class based on file extension"""
        ext = os.path.splitext(filename)[1].lower()
        # Use single dictionary for mapping
        EXTENSION_MAP = {
            **{ext: (mime, 'object.item.videoItem.Movie') 
                for ext, mime in VIDEO_EXTENSIONS.items()},
            **{ext: (mime, 'object.item.audioItem.musicTrack') 
                for ext, mime in AUDIO_EXTENSIONS.items()},
            **{ext: (mime, 'object.item.imageItem.photo') 
                for ext, mime in IMAGE_EXTENSIONS.items()}
        }
        
        return EXTENSION_MAP.get(ext, ('application/octet-stream', 'object.item'))

    def add_audio_metadata(self, file_path, item_element):
        """Add audio-specific metadata"""
        try:
            audio = File(file_path)
            if audio:
                if hasattr(audio, 'tags'):
                    tags = audio.tags
                    if 'artist' in tags:
                        SubElement(item_element, 'upnp:artist').text = str(tags['artist'][0])
                    if 'album' in tags:
                        SubElement(item_element, 'upnp:album').text = str(tags['album'][0])
                    if 'genre' in tags:
                        SubElement(item_element, 'upnp:genre').text = str(tags['genre'][0])
        except Exception as e:
            self.logger.warning(f"Error reading audio metadata: {str(e)}")

    def add_video_metadata(self, file_path, item_element):
        """Add video-specific metadata"""
        # Add basic video metadata
        SubElement(item_element, 'upnp:genre').text = "Unknown"
        SubElement(item_element, 'dc:publisher').text = "Unknown"
        
        # You could expand this using a video metadata library like ffmpeg-python
        # to extract resolution, duration, etc.

    def add_image_metadata(self, file_path, item_element):
        """Add image-specific metadata"""
        try:
            from PIL import Image
            with Image.open(file_path) as img:
                width, height = img.size
                SubElement(item_element, 'upnp:resolution').text = f"{width}x{height}"
        except Exception as e:
            self.logger.warning(f"Error reading image metadata: {e}")
