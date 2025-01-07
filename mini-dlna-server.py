import errno
import os
import sys
import socket
import logging
import time
import uuid
import threading
from pathlib import Path
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import unquote, quote
from logging.handlers import RotatingFileHandler
from xml.etree.ElementTree import Element, SubElement, tostring, fromstring
from datetime import datetime
from mutagen import File

# DLNA/UPnP Constants
DEVICE_UUID = uuid.uuid5(uuid.NAMESPACE_DNS, socket.gethostname())
DEVICE_NAME = f"Python Media Server ({socket.gethostname()})"
SSDP_ADDR = '239.255.255.250'
SSDP_PORT = 1900

# Supported extensions
VIDEO_EXTENSIONS = {'.mp4': 'video/mp4', '.mkv': 'video/x-matroska', '.avi': 'video/x-msvideo'}
AUDIO_EXTENSIONS = {'.mp3': 'audio/mpeg', '.flac': 'audio/flac', '.wav': 'audio/wav'}
IMAGE_EXTENSIONS = {'.jpg': 'image/jpeg', '.png': 'image/png', '.gif': 'image/gif'}

def setup_logging():
    """Set up logging configuration with both file and console handlers"""
    logger = logging.getLogger('DLNAServer')
    logger.setLevel(logging.DEBUG)

    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)

    file_handler = RotatingFileHandler(
        log_dir / 'dlna_server.log',
        maxBytes=5*1024*1024,
        backupCount=5
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    )

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(
        logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    )

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

class NetworkErrorHandler:
    """Handles network-related errors and implements retry logic"""
    def __init__(self, logger, max_retries=3, retry_delay=1):
        self.logger = logger
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        
    def with_retry(self, func, *args, **kwargs):
        """Execute a function with retry logic"""
        last_exception = None
        for attempt in range(self.max_retries):
            try:
                return func(*args, **kwargs)
            except socket.error as e:
                last_exception = e
                self.logger.warning(f"Network operation failed (attempt {attempt + 1}/{self.max_retries}): {str(e)}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
        raise last_exception

class SSDPServer:
    """SSDP server that coexists with Windows 11 UPnP"""
    def __init__(self, http_server_address):
        self.logger = logging.getLogger('DLNAServer')
        self.http_server_address = http_server_address
        self.running = False
        self.discovery_count = 0
        self.announcement_interval = 5
        self.error_handler = NetworkErrorHandler(self.logger)
        self.socket = None
        self.announce_socket = None

    def send_alive_notification(self):
        """Send SSDP presence notification with enhanced Samsung compatibility"""
        services = [
            'upnp:rootdevice',
            'urn:schemas-upnp-org:device:MediaServer:1',
            'urn:schemas-upnp-org:service:ContentDirectory:1',
            'urn:schemas-upnp-org:service:ConnectionManager:1'
        ]
        
        location = f'http://{self.http_server_address[0]}:{self.http_server_address[1]}/description.xml'
        
        for service in services:
            try:
                notify_msg = '\r\n'.join([
                    'NOTIFY * HTTP/1.1',
                    f'HOST: {SSDP_ADDR}:{SSDP_PORT}',
                    'CACHE-CONTROL: max-age=1800',
                    f'LOCATION: {location}',
                    'NT: ' + service,
                    'NTS: ssdp:alive',
                    'SERVER: Windows/10.0 UPnP/1.0 Python-DLNA/1.0',  # Changed server string
                    f'USN: uuid:{DEVICE_UUID}::{service}',
                    'BOOTID.UPNP.ORG: 1',
                    'CONFIGID.UPNP.ORG: 1',
                    'SEARCHPORT.UPNP.ORG: 1900',
                    # Samsung-specific headers
                    'X-DLNADOC: DMS-1.50',
                    'X-DLNACAP: av-upload,time-seek,connection-stalling,range',  # Added range
                    'Content-Length: 0',
                    '',
                    ''
                ])
                
                # Send notification on all interfaces
                interfaces = self.get_all_interfaces()
                for interface in interfaces:
                    try:
                        # Bind to specific interface
                        self.announce_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
                        self.announce_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                        self.announce_socket.bind((interface, 0))
                        
                        # Set TTL to 4 for better network traversal
                        self.announce_socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 4)
                        
                        # Send the notification
                        self.error_handler.with_retry(
                            lambda: self.announce_socket.sendto(notify_msg.encode(), (SSDP_ADDR, SSDP_PORT))
                        )
                        self.logger.info(f"Sent alive notification for service ['{service}'] on interface ['{interface}']")
                            
                    except Exception as e:
                        self.logger.warning(f"Failed to send notification on interface {interface}: {str(e)}")
                    finally:
                        try:
                            self.announce_socket.close()
                        except:
                            pass
                        
            except Exception as e:
                self.logger.error(f"Failed to send notification for service {service}: {str(e)}")
                continue

    def periodic_announce(self):
        """Periodically send presence announcements"""
        initial_interval = 1  # Start with more frequent announcements
        max_interval = 5    # Maximum interval between announcements
        current_interval = initial_interval
        
        while self.running:
            try:
                self.error_handler.with_retry(self.send_alive_notification)
                
                # Sleep with interruption check
                for _ in range(int(current_interval)):
                    if not self.running:
                        break
                    time.sleep(1)
                
                # Gradually increase interval up to max_interval
                if current_interval < max_interval:
                    current_interval = min(current_interval * 1.5, max_interval)
                    
            except Exception as e:
                self.logger.error(f"Error in periodic announce: {str(e)}")
                time.sleep(1)  # Prevent tight loop on error

    def get_all_interfaces(self):
        """Get all IPv4 addresses of all network interfaces with error handling"""
        addresses = []
        try:
            # Try using netifaces library if available
            try:
                import netifaces
                for interface in netifaces.interfaces():
                    try:
                        addrs = netifaces.ifaddresses(interface)
                        if netifaces.AF_INET in addrs:
                            for addr in addrs[netifaces.AF_INET]:
                                ip = addr['addr']
                                if not ip.startswith('127.'):
                                    addresses.append(ip)
                    except Exception as e:
                        self.logger.debug(f"Error getting addresses for interface {interface}: {str(e)}")
            except ImportError:
                # Fallback to socket method
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                try:
                    # Doesn't actually connect but gets local interface IP
                    s.connect(('8.8.8.8', 80))
                    addresses.append(s.getsockname()[0])
                finally:
                    s.close()

            # If all methods fail, add localhost
            if not addresses:
                addresses.append('127.0.0.1')
                
            self.logger.info(f"Found network interfaces: {addresses}")
            return addresses

        except Exception as e:
            self.logger.error(f"Error getting network interfaces: {str(e)}")
            return ['127.0.0.1']  # Return localhost as fallback

    def initialize_sockets(self):
        """Initialize network sockets with Windows-specific handling"""
        try:
            # Create main multicast socket with specific options for Windows
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            
            # Set multicast TTL
            self.socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 4)
            
            # Allow multiple bindings for Windows
            if hasattr(socket, 'SO_REUSEPORT'):  # Not available on Windows
                self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
                
            # Create separate socket for sending with specific options
            self.announce_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            self.announce_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.announce_socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 4)
            
            # Bind to all interfaces
            try:
                self.socket.bind(('', SSDP_PORT))
            except socket.error as e:
                if e.errno == errno.EADDRINUSE:
                    # Port in use is expected on Windows - we can still send/receive
                    self.logger.info("SSDP port already in use (Windows UPnP). Continuing in shared mode.")
                    return True
                else:
                    raise
            
            # Set multicast interface for the announce socket
            interfaces = self.get_all_interfaces()
            if interfaces:
                self.announce_socket.setsockopt(
                    socket.IPPROTO_IP,
                    socket.IP_MULTICAST_IF,
                    socket.inet_aton(interfaces[0])
                )
            
            self.logger.info(f"SSDP server initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize SSDP sockets: {str(e)}")
            self.cleanup_sockets()
            return False

    def cleanup_sockets(self):
        """Clean up network sockets"""
        try:
            if self.socket:
                self.socket.close()
            if self.announce_socket:
                self.announce_socket.close()
        except Exception as e:
            self.logger.error(f"Error during socket cleanup: {str(e)}")

    def join_multicast_group(self):
        """Join multicast group with Windows-specific handling"""
        successful_joins = 0
        interfaces = self.get_all_interfaces()
        
        for addr in interfaces:
            try:
                # Windows-specific: Join group on specific interface
                mreq = socket.inet_aton(SSDP_ADDR) + socket.inet_aton(addr)
                self.socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
                
                # Set multicast interface
                self.socket.setsockopt(
                    socket.IPPROTO_IP,
                    socket.IP_MULTICAST_IF,
                    socket.inet_aton(addr)
                )
                
                self.logger.info(f"Successfully joined multicast group on interface: {addr}")
                successful_joins += 1
            except Exception as e:
                self.logger.warning(f"Failed to join multicast group on interface {addr}: {str(e)}")
                
        if successful_joins == 0:
            self.logger.error("Failed to join multicast group on any interface")
            return False
        return True

    def safe_send(self, data, addr):
        """Send data with error handling"""
        try:
            return self.error_handler.with_retry(
                lambda: self.announce_socket.sendto(data, addr)
            )
        except Exception as e:
            self.logger.error(f"Failed to send data to {addr}: {str(e)}")
            return None

    def handle_request(self, data, addr):
        """Handle incoming SSDP requests with error handling"""
        try:
            request = data.decode().split('\r\n')
            self.logger.info(f"Received SSDP request from {addr[0]}:{addr[1]}")
            self.logger.debug(f"Full request: {request}")
            
            if 'M-SEARCH * HTTP/1.1' in request[0]:
                try:
                    # Parse headers
                    headers = {}
                    for line in request:
                        if ': ' in line:
                            key, value = line.split(': ', 1)
                            headers[key.upper()] = value
                    
                    self.logger.debug(f"M-SEARCH headers: {headers}")
                    
                    # Check various search targets
                    st = headers.get('ST', '')
                    search_targets = [
                        'ssdp:all',
                        'upnp:rootdevice',
                        f'uuid:{DEVICE_UUID}',
                        'urn:schemas-upnp-org:device:MediaServer:1',
                        'urn:schemas-upnp-org:service:ContentDirectory:1'
                    ]
                    
                    if st in search_targets:
                        self.logger.info(f"Matching search target: {st}")
                        self.send_discovery_response(addr, st)
                    else:
                        self.logger.debug(f"Ignoring search request with ST: {st}")
                        
                except Exception as e:
                    self.logger.error(f"Error processing M-SEARCH request: {str(e)}")
                    
        except UnicodeDecodeError as e:
            self.logger.warning(f"Received malformed SSDP request from {addr[0]}:{addr[1]}: {str(e)}")
        except Exception as e:
            self.logger.error(f"Error handling SSDP request from {addr[0]}:{addr[1]}: {str(e)}")

    def send_discovery_response(self, addr, st):
        """Send SSDP discovery response with error handling"""
        try:
            self.discovery_count += 1
            self.logger.info(f"Sending discovery response to {addr[0]}:{addr[1]} (Total responses: {self.discovery_count})")

            response = '\r\n'.join([
                'HTTP/1.1 200 OK',
                'CACHE-CONTROL: max-age=1800',
                'DATE: ' + time.strftime('%a, %d %b %Y %H:%M:%S GMT', time.gmtime()),
                'EXT:',
                f'LOCATION: http://{self.http_server_address[0]}:{self.http_server_address[1]}/description.xml',
                'SERVER: Windows/10.0 UPnP/1.0 Python-DLNA/1.0',
                f'ST: {st}',
                f'USN: uuid:{DEVICE_UUID}::{st}',
                'BOOTID.UPNP.ORG: 1',
                'CONFIGID.UPNP.ORG: 1',
                'X-DLNADOC: DMS-1.50',
                '',
                ''
            ])

            try:
                self.error_handler.with_retry(
                    lambda: self.socket.sendto(response.encode(), addr)
                )
                self.logger.debug(f"Discovery response sent successfully to {addr[0]}:{addr[1]}")
            except Exception as e:
                raise Exception(f"Failed to send discovery response: {str(e)}")

        except Exception as e:
            self.logger.error(f"Error preparing/sending discovery response to {addr[0]}:{addr[1]}: {str(e)}")

    def start(self):
        """Start SSDP server with error handling"""
        if not self.initialize_sockets():
            self.logger.error("Failed to initialize SSDP server")
            return
            
        if not self.join_multicast_group():
            self.logger.error("Failed to join multicast group")
            self.cleanup_sockets()
            return
            
        self.running = True
        self.logger.info(f"SSDP server started on {SSDP_ADDR}:{SSDP_PORT}")
        
        # Start announcement thread with error handling
        self.announcement_thread = threading.Thread(target=self.periodic_announce)
        self.announcement_thread.daemon = True
        self.announcement_thread.start()
        
        try:
            self.send_alive_notification()  # Send initial announcement
        except Exception as e:
            self.logger.error(f"Failed to send initial announcement: {str(e)}")
        
        while self.running:
            try:
                data, addr = self.socket.recvfrom(1024)
                self.handle_request(data, addr)
            except socket.timeout:
                continue
            except socket.error as e:
                if self.running:
                    self.logger.error(f"Socket error during receive: {str(e)}")
                    # Try to recover from socket error
                    if not self.recover_from_error():
                        break
            except Exception as e:
                if self.running:
                    self.logger.error(f"Unexpected error in SSDP server: {str(e)}")
                    if not self.recover_from_error():
                        break

        self.cleanup_sockets()

    def recover_from_error(self):
        """Attempt to recover from network errors"""
        try:
            self.logger.info("Attempting to recover from network error...")
            self.cleanup_sockets()
            time.sleep(1)  # Wait before attempting recovery
            
            if self.initialize_sockets() and self.join_multicast_group():
                self.logger.info("Successfully recovered from network error")
                return True
            else:
                self.logger.error("Failed to recover from network error")
                return False
        except Exception as e:
            self.logger.error(f"Error during recovery attempt: {str(e)}")
            return False

class DLNAServer(BaseHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        self.logger = logging.getLogger('DLNAServer')
        self.protocol_version = 'HTTP/1.1'
        self.timeout = 60  # Set timeout to 60 seconds
        super().__init__(*args, **kwargs)
    
    def do_GET(self):
        """Handle GET requests with improved error handling"""
        try:
            # Parse the path properly
            from urllib.parse import urlparse
            parsed_path = urlparse(self.path)
            clean_path = parsed_path.path  # This removes any http:// prefixes
            
            if clean_path == '/description.xml':
                self.send_device_description()
            elif clean_path == '/ContentDirectory.xml':
                self.send_content_directory()
            elif clean_path == '/ConnectionManager.xml':
                self.send_connection_manager()
            elif clean_path == '/AVTransport.xml':
                self.send_av_transport()
            else:
                # Remove leading slash and handle media file requests
                path = clean_path[1:]
                if path:
                    self.serve_media_file(path)
                else:
                    self.send_error(404, "File not found")
        except ConnectionAbortedError:
            self.logger.warning("Client connection was aborted")
            return
        except Exception as e:
            self.logger.error(f"Error handling GET request: {str(e)}")
            try:
                self.send_error(500, "Internal server error")
            except:
                # If we can't send the error response, just log it
                self.logger.error("Could not send error response to client")
                return

    def do_POST(self):
        """Handle POST requests"""
        try:
            if self.path == '/ContentDirectory/control':
                self.handle_content_directory_control()
            else:
                self.send_error(501, "Unsupported method ('POST')")
        except Exception as e:
            self.logger.error(f"Error handling POST request: {str(e)}")
            
            try:
                self.send_error(500, "Internal server error")
            except Exception as e:
                self.logger.error(f"Could not return error response to client: {str(e)}")
                return

    def handle_content_directory_control(self):
        """Handle POST requests to /ContentDirectory/control with improved XML handling"""
        try:
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            self.logger.info(f"Received POST data: {post_data}")

            # Parse the SOAP request
            envelope = fromstring(post_data)
            body = envelope.find('{http://schemas.xmlsoap.org/soap/envelope/}Body')
            browse = body.find('{urn:schemas-upnp-org:service:ContentDirectory:1}Browse')

            if browse is not None:
                object_id = browse.find('ObjectID').text
                browse_flag = browse.find('BrowseFlag').text
                filter = browse.find('Filter').text
                starting_index = int(browse.find('StartingIndex').text)
                requested_count = int(browse.find('RequestedCount').text)
                sort_criteria = browse.find('SortCriteria').text

                # Handle root container specially
                if object_id == '0' or object_id == '':
                    result = self.handle_root_container(browse_flag, starting_index, requested_count)
                else:
                    # Handle regular browse requests
                    result = self.handle_browse_request(object_id, browse_flag, starting_index, requested_count)

                # Create SOAP response
                soap_response = f'''<?xml version="1.0" encoding="utf-8"?>
                <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">
                    <s:Body>
                        <u:BrowseResponse xmlns:u="urn:schemas-upnp-org:service:ContentDirectory:1">
                            <Result>{result['Result']}</Result>
                            <NumberReturned>{result['NumberReturned']}</NumberReturned>
                            <TotalMatches>{result['TotalMatches']}</TotalMatches>
                            <UpdateID>1</UpdateID>
                        </u:BrowseResponse>
                    </s:Body>
                </s:Envelope>'''

                self.send_response(200)
                self.send_header('Content-Type', 'text/xml; charset="utf-8"')
                self.send_header('Ext', '')
                self.send_header('Server', 'Windows/1.0 UPnP/1.0 MiniDLNA/1.0')
                self.send_header('Content-Length', len(soap_response))
                self.end_headers()
                self.wfile.write(soap_response.encode('utf-8'))
            else:
                self.send_error(400, "Invalid SOAP request")

        except Exception as e:
            self.logger.error(f"Error handling content directory control: {str(e)}")
            self.send_error(500, "Internal server error")

    def handle_root_container(self, browse_flag, starting_index, requested_count):
        """Handle browsing of the root container"""
        try:
            root = Element('DIDL-Lite', {
                'xmlns': 'urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/',
                'xmlns:dc': 'http://purl.org/dc/elements/1.1/',
                'xmlns:upnp': 'urn:schemas-upnp-org:metadata-1-0/upnp/',
                'xmlns:dlna': 'urn:schemas-dlna-org:metadata-1-0'
            })

            if browse_flag == 'BrowseMetadata':
                # Return root container metadata
                container = SubElement(root, 'container', {
                    'id': '0',
                    'parentID': '-1',
                    'restricted': '1',
                    'searchable': '1'
                })
                SubElement(container, 'dc:title').text = 'Root'
                SubElement(container, 'upnp:class').text = 'object.container'
                SubElement(container, 'upnp:storageUsed').text = '-1'
                
                return {
                    'Result': tostring(root, encoding='unicode'),
                    'NumberReturned': 1,
                    'TotalMatches': 1
                }
                
            elif browse_flag == 'BrowseDirectChildren':
                # List contents of the media folder
                items = sorted(os.listdir(self.server.media_folder))
                total_matches = len(items)
                
                # Apply pagination
                items = items[starting_index:starting_index + requested_count if requested_count > 0 else None]
                
                for item in items:
                    item_path = os.path.join(self.server.media_folder, item)
                    if os.path.isdir(item_path):
                        self.add_container_to_didl(root, item_path, item, '0')
                    else:
                        self.add_item_to_didl(root, item_path, item, '0')
                
                return {
                    'Result': tostring(root, encoding='unicode'),
                    'NumberReturned': len(items),
                    'TotalMatches': total_matches
                }
                
        except Exception as e:
            self.logger.error(f"Error handling root container: {str(e)}")
            raise

    def handle_browse_request(self, object_id, browse_flag, starting_index, requested_count):
        """Handle browsing of non-root containers and items"""
        try:
            root = Element('DIDL-Lite', {
                'xmlns': 'urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/',
                'xmlns:dc': 'http://purl.org/dc/elements/1.1/',
                'xmlns:upnp': 'urn:schemas-upnp-org:metadata-1-0/upnp/',
                'xmlns:dlna': 'urn:schemas-dlna-org:metadata-1-0'
            })

            path = os.path.join(self.server.media_folder, unquote(object_id))
            
            if not os.path.exists(path):
                raise ValueError(f"Path does not exist: {path}")
                
            if browse_flag == 'BrowseMetadata':
                # Return metadata for the specific item/container
                if os.path.isdir(path):
                    self.add_container_to_didl(root, path, os.path.basename(path), os.path.dirname(object_id))
                else:
                    self.add_item_to_didl(root, path, os.path.basename(path), os.path.dirname(object_id))
                    
                return {
                    'Result': tostring(root, encoding='unicode'),
                    'NumberReturned': 1,
                    'TotalMatches': 1
                }
                
            elif browse_flag == 'BrowseDirectChildren' and os.path.isdir(path):
                # List contents of the directory
                items = sorted(os.listdir(path))
                total_matches = len(items)
                
                # Apply pagination
                items = items[starting_index:starting_index + requested_count if requested_count > 0 else None]
                
                for item in items:
                    item_path = os.path.join(path, item)
                    if os.path.isdir(item_path):
                        self.add_container_to_didl(root, item_path, item, object_id)
                    else:
                        self.add_item_to_didl(root, item_path, item, object_id)
                
                return {
                    'Result': tostring(root, encoding='unicode'),
                    'NumberReturned': len(items),
                    'TotalMatches': total_matches
                }
                
        except Exception as e:
            self.logger.error(f"Error handling browse request: {str(e)}")
            raise

    def add_container_to_didl(self, root, path, title, parent_id):
        """Add a container (directory) to the DIDL-Lite XML"""
        try:
            container = SubElement(root, 'container', {
                'id': quote(os.path.relpath(path, self.server.media_folder)),
                'parentID': quote(parent_id),
                'restricted': '1',
                'searchable': '1',
                'childCount': str(len(os.listdir(path)))
            })
            
            SubElement(container, 'dc:title').text = title
            SubElement(container, 'upnp:class').text = 'object.container.storageFolder'
            
            # Add creation date
            creation_time = datetime.fromtimestamp(os.path.getctime(path))
            SubElement(container, 'dc:date').text = creation_time.isoformat()
            
        except Exception as e:
            self.logger.error(f"Error adding container to DIDL: {str(e)}")
            raise

    def add_item_to_didl(self, root, path, title, parent_id):
        """Add an item (file) to the DIDL-Lite XML with improved metadata"""
        try:
            mime_type, upnp_class = self.get_mime_and_upnp_class(title)
            relative_path = os.path.relpath(path, self.server.media_folder)
            
            item = SubElement(root, 'item', {
                'id': quote(relative_path),
                'parentID': quote(parent_id),
                'restricted': '1'
            })
            
            SubElement(item, 'dc:title').text = title
            SubElement(item, 'upnp:class').text = upnp_class
            
            # Add resource element
            res = SubElement(item, 'res')
            file_size = os.path.getsize(path)
            url = f'http://{self.server.server_address[0]}:{self.server.server_address[1]}/{quote(relative_path)}'
            res.text = url
            
            # Add protocol info with DLNA parameters
            protocol_info = f'http-get:*:{mime_type}:'
            protocol_info += 'DLNA.ORG_OP=01;' # Allow range requests
            protocol_info += 'DLNA.ORG_CI=0;'   # No conversion
            protocol_info += 'DLNA.ORG_FLAGS=01700000000000000000000000000000' # Standard DLNA flags
            
            res.set('protocolInfo', protocol_info)
            res.set('size', str(file_size))
            
            # Add media-specific metadata
            if upnp_class.startswith('object.item.audioItem'):
                self.add_audio_metadata(path, item)
            elif upnp_class.startswith('object.item.videoItem'):
                self.add_video_metadata(path, item)
            elif upnp_class.startswith('object.item.imageItem'):
                self.add_image_metadata(path, item)
            
            # Add creation date
            creation_time = datetime.fromtimestamp(os.path.getctime(path))
            SubElement(item, 'dc:date').text = creation_time.isoformat()
            
        except Exception as e:
            self.logger.error(f"Error adding item to DIDL: {str(e)}")
            raise

    def get_metadata_for_object(self, object_id):
        try:
            path = os.path.abspath(object_id)
            if not os.path.exists(path):
                return self.create_default_metadata(object_id)

            if os.path.isdir(path):
                return self.get_directory_metadata(path)
            else:
                return self.get_file_metadata(path)
        except Exception as e:
            self.logger.error(f"Error handling content directory control: {str(e)}")
            self.send_error(500, "Internal server error")

    def get_file_metadata(self, path):
        metadata = {
            'id': path,
            'parentID': os.path.dirname(path),
            'title': os.path.basename(path),
            'date': datetime.fromtimestamp(os.path.getmtime(path)).isoformat(),
            'size': str(os.path.getsize(path)),
            'class': 'object.item'
        }
        
        if path.lower().endswith(('.mp3', '.flac', '.m4a', '.wma')):
            try:
                audio = File(path)
                if audio:
                    metadata.update({
                        'artist': audio.get('artist', ['Unknown'])[0],
                        'album': audio.get('album', ['Unknown'])[0],
                        'genre': audio.get('genre', ['Unknown'])[0],
                        'class': 'object.item.audioItem.musicTrack'
                    })
            except:
                pass
        
        return metadata

    def get_directory_metadata(self, path):
        return {
            'id': path,
            'parentID': os.path.dirname(path),
            'title': os.path.basename(path),
            'date': datetime.fromtimestamp(os.path.getmtime(path)).isoformat(),
            'class': 'object.container.storageFolder'
        }

    def create_default_metadata(self, object_id):
        return {
            'id': object_id,
            'parentID': '0',
            'title': 'Unknown',
            'date': datetime.now().isoformat(),
            'class': 'object.item',
            'artist': 'Unknown',
            'album': 'Unknown',
            'genre':  'Unknown'
        }

    def create_browse_metadata_response(self, metadata):
        # Implement the logic to create a response for BrowseMetadata
        response = f"""
        <DIDL-Lite xmlns:dc="http://purl.org/dc/elements/1.1/"
                xmlns:upnp="urn:schemas-upnp-org:metadata-1-0/upnp/"
                xmlns="urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/">
            <item id="{metadata['id']}" parentID="0" restricted="1">
                <dc:title>{metadata['title']}</dc:title>
                <upnp:artist>{metadata['artist']}</upnp:artist>
                <upnp:album>{metadata['album']}</upnp:album>
                <upnp:genre>{metadata['genre']}</upnp:genre>
                <dc:date>{metadata['date']}</dc:date>
            </item>
        </DIDL-Lite>
        """
        return response

    def generate_browse_response(self, object_id, starting_index, requested_count):
        """Generate a DIDL-Lite XML response for the Browse request"""
        root = Element('DIDL-Lite', {
            'xmlns': 'urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/',
            'xmlns:dc': 'http://purl.org/dc/elements/1.1/',
            'xmlns:upnp': 'urn:schemas-upnp-org:metadata-1-0/upnp/',
            'xmlns:dlna': 'urn:schemas-dlna-org:metadata-1-0'
        })

        media_path = os.path.abspath(os.path.join(self.server.media_folder, object_id))
        items = []
        total_matches = 0
        
        try:
            if os.path.isdir(media_path):
                items = sorted(os.listdir(media_path))
                total_matches = len(items)
                items = items[starting_index:starting_index + requested_count]
                
                for item in items:
                    item_path = os.path.join(media_path, item)
                    item_id = os.path.relpath(item_path, self.server.media_folder)
                    
                    if os.path.isdir(item_path):
                        container = SubElement(root, 'container', {
                            'id': quote(item_id),
                            'parentID': quote(object_id),
                            'restricted': '1',
                            'searchable': '1',
                            'childCount': str(len(os.listdir(item_path)))
                        })
                        SubElement(container, 'dc:title').text = item
                        SubElement(container, 'upnp:class').text = 'object.container.storageFolder'
                        
                    else:
                        # File handling
                        item_element = SubElement(root, 'item', {
                            'id': quote(item_id),
                            'parentID': quote(object_id),
                            'restricted': '1'
                        })
                        
                        SubElement(item_element, 'dc:title').text = item
                        
                        # Determine media type and set appropriate class
                        mime_type, upnp_class = self.get_mime_and_upnp_class(item)
                        SubElement(item_element, 'upnp:class').text = upnp_class
                        
                        # Add resource element with proper DLNA attributes
                        res = SubElement(item_element, 'res')
                        file_size = os.path.getsize(item_path)
                        url = f'http://{self.server.server_address[0]}:{self.server.server_address[1]}/{quote(item_id)}'
                        res.text = url
                        res.set('protocolInfo', f'http-get:*:{mime_type}:DLNA.ORG_OP=01;DLNA.ORG_CI=0;')
                        res.set('size', str(file_size))
                        
                        # Add media-specific metadata
                        if upnp_class.startswith('object.item.audioItem'):
                            self.add_audio_metadata(item_path, item_element)
                        elif upnp_class.startswith('object.item.videoItem'):
                            self.add_video_metadata(item_path, item_element)
                        elif upnp_class.startswith('object.item.imageItem'):
                            self.add_image_metadata(item_path, item_element)

        except Exception as e:
            self.logger.error(f"Error generating browse response: {str(e)}")
            raise

        response = f'''<?xml version="1.0" encoding="utf-8"?>
        <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">
            <s:Body>
                <u:BrowseResponse xmlns:u="urn:schemas-upnp-org:service:ContentDirectory:1">
                    <Result>{tostring(root, encoding='unicode')}</Result>
                    <NumberReturned>{len(items)}</NumberReturned>
                    <TotalMatches>{total_matches}</TotalMatches>
                    <UpdateID>1</UpdateID>
                </u:BrowseResponse>
            </s:Body>
        </s:Envelope>'''
        
        return response
    
    def get_mime_and_upnp_class(self, filename):
        """Determine MIME type and UPnP class based on file extension"""
        ext = os.path.splitext(filename)[1].lower()
        
        # Video files
        if ext in ['.mp4', '.mkv', '.avi', '.mov']:
            return 'video/mp4', 'object.item.videoItem'
        
        # Audio files
        elif ext in ['.mp3', '.flac', '.wav', '.m4a']:
            return 'audio/mpeg', 'object.item.audioItem.musicTrack'
        
        # Image files
        elif ext in ['.jpg', '.jpeg', '.png', '.gif']:
            return 'image/jpeg', 'object.item.imageItem.photo'
        
        # Default
        return 'application/octet-stream', 'object.item'

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
            self.logger.warning(f"Error reading image metadata: {str(e)}")

    def send_device_description(self):
        """Send DLNA device description XML with Samsung compatibility"""
        try:
            # Create the device description XML
            root = Element('root', {
                'xmlns': 'urn:schemas-upnp-org:device-1-0',
                'xmlns:dlna': 'urn:schemas-dlna-org:device-1-0',
                'xmlns:sec': 'http://www.sec.co.kr/dlna'  # Added Samsung namespace
            })
            
            # Add specVersion
            spec_version = SubElement(root, 'specVersion')
            SubElement(spec_version, 'major').text = '1'
            SubElement(spec_version, 'minor').text = '0'
            
            # Add device information
            device = SubElement(root, 'device')
            SubElement(device, 'deviceType').text = 'urn:schemas-upnp-org:device:MediaServer:1'
            SubElement(device, 'friendlyName').text = DEVICE_NAME
            SubElement(device, 'manufacturer').text = 'Python DLNA'
            SubElement(device, 'manufacturerURL').text = 'http://example.com'
            SubElement(device, 'modelDescription').text = 'Python DLNA Media Server'
            SubElement(device, 'modelName').text = 'Python DLNA'
            SubElement(device, 'modelNumber').text = '1.0'
            SubElement(device, 'modelURL').text = 'http://example.com'
            SubElement(device, 'serialNumber').text = '1'
            SubElement(device, 'UDN').text = f'uuid:{DEVICE_UUID}'
            
            # Add Samsung-specific elements
            SubElement(device, 'sec:ProductCap').text = 'smi,DCM10,getMediaInfo.sec,getCaptionInfo.sec'
            SubElement(device, 'sec:X_ProductCap').text = 'smi,DCM10,getMediaInfo.sec,getCaptionInfo.sec'
            
            # Add dlna:X_DLNADOC
            SubElement(device, 'dlna:X_DLNADOC').text = 'DMS-1.50'
            
            # Add service list
            service_list = SubElement(device, 'serviceList')
            
            # Content Directory service
            service1 = SubElement(service_list, 'service')
            SubElement(service1, 'serviceType').text = 'urn:schemas-upnp-org:service:ContentDirectory:1'
            SubElement(service1, 'serviceId').text = 'urn:upnp-org:serviceId:ContentDirectory'
            SubElement(service1, 'SCPDURL').text = '/ContentDirectory.xml'
            SubElement(service1, 'controlURL').text = '/ContentDirectory/control'
            SubElement(service1, 'eventSubURL').text = '/ContentDirectory/event'
            
            # Connection Manager service
            service2 = SubElement(service_list, 'service')
            SubElement(service2, 'serviceType').text = 'urn:schemas-upnp-org:service:ConnectionManager:1'
            SubElement(service2, 'serviceId').text = 'urn:upnp-org:serviceId:ConnectionManager'
            SubElement(service2, 'SCPDURL').text = '/ConnectionManager.xml'
            SubElement(service2, 'controlURL').text = '/ConnectionManager/control'
            SubElement(service2, 'eventSubURL').text = '/ConnectionManager/event'
            
            # AVTransport service
            service3 = SubElement(service_list, 'service')
            SubElement(service3, 'serviceType').text = 'urn:schemas-upnp-org:service:AVTransport:1'
            SubElement(service3, 'serviceId').text = 'urn:upnp-org:serviceId:AVTransport'
            SubElement(service3, 'SCPDURL').text = '/AVTransport.xml'
            SubElement(service3, 'controlURL').text = '/AVTransport/control'
            SubElement(service3, 'eventSubURL').text = '/AVTransport/event'
            
            # Convert to string
            xml_string = '<?xml version="1.0" encoding="utf-8"?>\n' + tostring(root, encoding='unicode')
            
            # Send response
            self.send_response(200)
            self.send_header('Content-Type', 'text/xml; charset="utf-8"')
            self.send_header('Content-Length', str(len(xml_string)))
            self.send_header('Server', 'Python DLNA/1.0 UPnP/1.0')  # Added server header
            self.end_headers()
            self.wfile.write(xml_string.encode())
            
        except Exception as e:
            self.logger.error(f"Error sending device description: {str(e)}")
            self.send_error(500, "Internal server error")

    def send_content_directory(self):
        """Send DLNA Content Directory XML"""
        try:
            # Create the content directory XML
            root = Element('root', {
                'xmlns': 'urn:schemas-upnp-org:service-1-0',
                'xmlns:dlna': 'urn:schemas-dlna-org:service-1-0'
            })
            
            # Add specVersion
            spec_version = SubElement(root, 'specVersion')
            SubElement(spec_version, 'major').text = '1'
            SubElement(spec_version, 'minor').text = '0'
            
            # Add service information
            service = SubElement(root, 'service')
            SubElement(service, 'serviceType').text = 'urn:schemas-upnp-org:service:ContentDirectory:1'
            SubElement(service, 'serviceId').text = 'urn:upnp-org:serviceId:ContentDirectory'
            
            # Convert XML to string
            xml_str = tostring(root, encoding='utf-8', method='xml')
            
            # Send response
            self.send_response(200)
            self.send_header('Content-Type', 'application/xml')
            self.send_header('Content-Length', len(xml_str))
            self.end_headers()
            self.wfile.write(xml_str)
        except Exception as e:
            self.logger.error(f"Error sending content directory: {str(e)}")
            self.send_error(500, "Internal server error")

    def send_connection_manager(self):
        """Send DLNA Connection Manager XML"""
        try:
            # Create the connection manager XML
            root = Element('root', {
                'xmlns': 'urn:schemas-upnp-org:service-1-0',
                'xmlns:dlna': 'urn:schemas-dlna-org:service-1-0'
            })
            
            # Add specVersion
            spec_version = SubElement(root, 'specVersion')
            SubElement(spec_version, 'major').text = '1'
            SubElement(spec_version, 'minor').text = '0'
            
            # Add service information
            service = SubElement(root, 'service')
            SubElement(service, 'serviceType').text = 'urn:schemas-upnp-org:service:ConnectionManager:1'
            SubElement(service, 'serviceId').text = 'urn:upnp-org:serviceId:ConnectionManager'
            
            # Convert XML to string
            xml_str = tostring(root, encoding='utf-8', method='xml')
            
            # Send response
            self.send_response(200)
            self.send_header('Content-Type', 'application/xml')
            self.send_header('Content-Length', len(xml_str))
            self.end_headers()
            self.wfile.write(xml_str)
        except Exception as e:
            self.logger.error(f"Error sending connection manager: {str(e)}")
            self.send_error(500, "Internal server error")

    def send_av_transport(self):
        """Send AVTransport service description"""
        av_transport_xml = """<?xml version="1.0" encoding="utf-8"?>
<scpd xmlns="urn:schemas-upnp-org:service-1-0">
    <specVersion>
        <major>1</major>
        <minor>0</minor>
    </specVersion>
    <actionList>
        <action>
            <name>SetAVTransportURI</name>
            <argumentList>
                <argument>
                    <name>InstanceID</name>
                    <direction>in</direction>
                    <relatedStateVariable>A_ARG_TYPE_InstanceID</relatedStateVariable>
                </argument>
                <argument>
                    <name>CurrentURI</name>
                    <direction>in</direction>
                    <relatedStateVariable>AVTransportURI</relatedStateVariable>
                </argument>
            </argumentList>
        </action>
        <action>
            <name>Play</name>
            <argumentList>
                <argument>
                    <name>InstanceID</name>
                    <direction>in</direction>
                    <relatedStateVariable>A_ARG_TYPE_InstanceID</relatedStateVariable>
                </argument>
                <argument>
                    <name>Speed</name>
                    <direction>in</direction>
                    <relatedStateVariable>TransportPlaySpeed</relatedStateVariable>
                </argument>
            </argumentList>
        </action>
    </actionList>
    <serviceStateTable>
        <stateVariable sendEvents="no">
            <name>A_ARG_TYPE_InstanceID</name>
            <dataType>ui4</dataType>
        </stateVariable>
        <stateVariable sendEvents="no">
            <name>AVTransportURI</name>
            <dataType>string</dataType>
        </stateVariable>
        <stateVariable sendEvents="no">
            <name>TransportPlaySpeed</name>
            <dataType>string</dataType>
            <defaultValue>1</defaultValue>
        </stateVariable>
    </serviceStateTable>
</scpd>"""
        self.send_response(200)
        self.send_header('Content-Type', 'text/xml; charset="utf-8"')
        self.send_header('Content-Length', len(av_transport_xml))
        self.end_headers()
        self.wfile.write(av_transport_xml.encode('utf-8'))

    def send_file_with_error_handling(self, file_path):
        """Send file with proper error handling"""
        try:
            with open(file_path, 'rb') as f:
                while True:
                    chunk = f.read(64 * 1024)
                    if not chunk:
                        break
                    try:
                        self.wfile.write(chunk)
                    except (ConnectionError, socket.error) as e:
                        self.logger.error(f"Connection error while sending file: {str(e)}")
                        return False
                    except Exception as e:
                        self.logger.error(f"Error while sending file: {str(e)}")
                        return False
            return True
        except IOError as e:
            self.logger.error(f"IO error while reading file {file_path}: {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error while sending file {file_path}: {str(e)}")
            return False

    def serve_media_file(self, path):
        """Serve a media file with enhanced error handling"""
        try:
            decoded_path = unquote(path)
            
            # Clean the path of any URL components
            decoded_path = decoded_path.split('?')[0]  # Remove query parameters
            decoded_path = decoded_path.replace('http://', '')  # Remove protocol
            decoded_path = decoded_path.split('/', 1)[-1] if '/' in decoded_path else decoded_path  # Remove domain
            
            file_path = os.path.abspath(os.path.join(self.server.media_folder, decoded_path))
            
            # Security check - ensure the path is within media_folder
            if not file_path.startswith(os.path.abspath(self.server.media_folder)):
                self.logger.warning(f"Attempted access to file outside media folder: {file_path}")
                self.send_error(403, "Access denied")
                return
                
            if not os.path.exists(file_path) or not os.path.isfile(file_path):
                self.logger.warning(f"File not found: {file_path}")
                self.send_error(404, "File not found")
                return

            try:
                file_size = os.path.getsize(file_path)
            except OSError as e:
                self.logger.error(f"Error getting file size: {str(e)}")
                self.send_error(500, "Internal server error")
                return

            content_type = self.determine_content_type(path)
            
            try:
                self.send_response(200)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(file_size))
                self.send_header("Connection", "keep-alive")
                self.send_header("Accept-Ranges", "bytes")
                self.send_header("transferMode.dlna.org", "Streaming")
                self.send_header("contentFeatures.dlna.org", "DLNA.ORG_OP=01;DLNA.ORG_CI=0")
                self.end_headers()
            except ConnectionAbortedError:
                self.logger.warning("Client connection was aborted while sending headers")
                return
            except Exception as e:
                self.logger.error(f"Error sending headers: {str(e)}")
                return

            if not self.send_file_with_error_handling(file_path):
                try:
                    self.send_error(500, "Error sending file")
                except:
                    self.logger.error("Could not send error response to client")
                    return

        except ConnectionAbortedError:
            self.logger.warning(f"Client connection was aborted while serving file {path}")
            return
        except Exception as e:
            self.logger.error(f"Error serving file {path}: {str(e)}")
            try:
                self.send_error(500, "Internal server error")
            except:
                self.logger.error("Could not send error response to client")
                return
        
    def determine_content_type(self, path):
        """Determine content type with error handling"""
        try:
            if path.lower().endswith(('.mp4', '.mkv', '.avi')):
                return "video/mp4"
            elif path.lower().endswith(('.mp3', '.wav')):
                return "audio/mpeg"
            return "application/octet-stream"
        except Exception as e:
            self.logger.error(f"Error determining content type: {str(e)}")
            return "application/octet-stream"

def get_local_ip():
    """Get the local IP address"""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"
    finally:
        s.close()

def start_server(media_folder):
    """Start the DLNA media server with Windows compatibility"""
    logger = setup_logging()

    if not os.path.exists(media_folder):
        logger.error(f"Media folder does not exist: {media_folder}")
        sys.exit(1)

    local_ip = get_local_ip()
    
    # Try ports until we find an available one
    base_port = 8201  # Start at 8201 to avoid common DLNA ports
    max_port = 8299
    
    server = None
    port = base_port
    
    while port <= max_port:
        try:
            server = HTTPServer((local_ip, port), DLNAServer)
            break
        except socket.error:
            logger.debug(f"Port {port} in use, trying next port")
            port += 1
    
    if server is None:
        logger.error(f"Could not find available port between {base_port} and {max_port}")
        sys.exit(1)

    server.media_folder = media_folder
    
    # Start SSDP server in a separate thread
    ssdp_server = SSDPServer((local_ip, port))
    ssdp_thread = threading.Thread(target=ssdp_server.start)
    ssdp_thread.daemon = True
    ssdp_thread.start()

    try:
        logger.info(f"DLNA server started at http://{local_ip}:{port}")
        logger.info(f"Serving media from: {media_folder}")
        logger.info("Press Ctrl+C to stop the server")
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("\nShutting down server...")
        ssdp_server.running = False
        ssdp_thread.join(timeout=1)
        server.server_close()
        sys.exit(0)
    except Exception as e:
        logger.error(f"Critical error: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python dlna_server.py <media_folder>")
        sys.exit(1)

    media_folder = os.path.abspath(sys.argv[1])
    start_server(media_folder)
