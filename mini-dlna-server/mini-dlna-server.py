import os
import sys
import socket
import logging
import time
import uuid
import threading
from urllib.parse import unquote, quote, urlparse, parse_qs
from pathlib import Path
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from logging.handlers import RotatingFileHandler
from xml.etree import ElementTree
from xml.etree.ElementTree import Element, SubElement, tostring, fromstring
import json
import argparse
import re
from mutagen import File
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from video_thumbnail_generator import VideoThumbnailGenerator
from network_utils import NetworkUtils
from ssdpserver import SSDPServer
from resourcemonitor import ResourceMonitor
from contentdirectoryhandler import (
    ALL_EXTENSIONS,
    AUDIO_EXTENSIONS,
    IMAGE_EXTENSIONS,
    VIDEO_EXTENSIONS,
    ContentDirectoryHandler,
)

# DLNA/UPnP Constants
DEVICE_UUID = uuid.uuid5(uuid.NAMESPACE_DNS, socket.gethostname())


def build_device_name(instance_id):
    return f"Python Media Server [{instance_id}] ({socket.gethostname()})"


def load_config_file(config_path):
    try:
        with open(config_path, 'r') as config_file:
            return json.load(config_file)
    except FileNotFoundError:
        print(f"Configuration file not found: {config_path}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error parsing configuration file: {e}")
        sys.exit(1)

def setup_logging():
    """Set up logging configuration with both file and console handlers"""
    logger = logging.getLogger('DLNAServer')
    logger.setLevel(logging.DEBUG)  # Set root logger to DEBUG

    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)

    # Detailed debug log file
    debug_handler = RotatingFileHandler(
        log_dir / 'dlna_server_debug.log',
        maxBytes=5*1024*1024,
        backupCount=5
    )
    debug_handler.setLevel(logging.DEBUG)
    debug_handler.setFormatter(
        logging.Formatter('%(asctime)s - %(levelname)s - [%(threadName)s] %(message)s')
    )

    # Regular log file with warnings and above
    file_handler = RotatingFileHandler(
        log_dir / 'dlna_server.log',
        maxBytes=5*1024*1024,
        backupCount=5
    )
    file_handler.setLevel(logging.WARNING)
    file_handler.setFormatter(
        logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    )

    # Console with warnings and errors only
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.WARNING)
    console_handler.setFormatter(
        logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    )

    logger.addHandler(debug_handler)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    # Add request tracking metrics
    logger.request_count = 0
    logger.last_request_time = time.time()
    
    return logger

# Update DLNAServer to use resource monitoring
class SOAPResponseHandler:
    def __init__(self, http_handler):
        self.handler = http_handler
        self.logger = logging.getLogger('DLNAServer')

    def send_soap_response(self, body_content, action_name, service_type):
        """Send a SOAP response with common headers and formatting"""
        soap_response = f'''<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">
    <s:Body>
        <u:{action_name}Response xmlns:u="{service_type}">
            {body_content}
        </u:{action_name}Response>
    </s:Body>
</s:Envelope>'''

        try:
            self.handler.send_response(200)
            self.handler.send_header('Content-Type', 'text/xml; charset="utf-8"')
            self.handler.send_header('Ext', '')
            self.handler.send_header('Server', 'Windows/10.0 UPnP/1.0 Python-DLNA/1.0')
            response_bytes = soap_response.encode('utf-8')
            self.handler.send_header('Content-Length', str(len(response_bytes)))
            self.handler.end_headers()
            self.handler.wfile.write(response_bytes)
        except Exception as e:
            self.logger.error(f"Error sending SOAP response: {e}")
            if not self.handler.headers_sent:
                self.handler.send_error(500, "Internal server error")

class DLNAErrorHandler:
    def __init__(self, logger):
        self.logger = logger

    def handle_request_error(self, handler, error, status_code=500):
        """Handle errors during request processing"""
        self.logger.error(f"Error processing request: {error}")
        if not handler.headers_sent:
            handler.send_error(status_code, str(error))

    def handle_network_error(self, error, retry_count=3):
        """Handle network-related errors with retry logic"""
        for attempt in range(retry_count):
            try:
                yield attempt
            except Exception as e:
                if attempt == retry_count - 1:
                    raise e
                self.logger.warning(f"Network error (attempt {attempt + 1}/{retry_count}): {e}")
                time.sleep(1)

class DLNAServer(BaseHTTPRequestHandler):
    SAMSUNG_HEADER_PATTERNS = (
        'samsung',
        'tizen',
        'sec_hhp',
        'sec-tv',
        'smart-tv',
        'allshare',
    )

    def __init__(self, request, client_address, server):
        # Initialize logger first
        self.logger = logging.getLogger('DLNAServer')
        self.server = server
        self.protocol_version = 'HTTP/1.1'
        self.timeout = 60  # Set timeout to 60 seconds
        self.headers_sent = False  # Track if headers have been sent
        
        # Initialize resource monitor before calling parent
        if hasattr(server, 'resource_monitor'):
            self.resource_monitor = server.resource_monitor
        else:
            self.resource_monitor = None
            self.logger.warning("Resource monitor not available on server instance")

        # Initialize the new components
        self.soap_handler = SOAPResponseHandler(self)
        self.error_handler = DLNAErrorHandler(self.logger)
        self.content_handler = ContentDirectoryHandler(self)
        if not hasattr(server, 'thumbnail_generator'):
            server.thumbnail_generator = VideoThumbnailGenerator(
                thumbnail_dir=str(Path(__file__).resolve().parent / 'cache' / 'thumbnails'),
                max_height=320,
                max_width=320,
                min_duration=300.0,
            )
        self.thumbnail_generator = server.thumbnail_generator
        if not hasattr(server, 'client_profiles'):
            server.client_profiles = {}

        # Call parent constructor last
        super().__init__(request, client_address, server)

    # Set up logging
    logger = logging.getLogger('dlna_server')
    
    def log_message(self, format, *args):
        """Override the default logging to use our logger"""
        self.logger.info("%s - - %s" % (self.address_string(), format % args))
    
    def log_error(self, format, *args):
        """Override error logging to use our logger"""
        self.logger.error("%s - - %s" % (self.address_string(), format % args))

    def _request_header_fingerprint(self):
        parts = []
        for header_name in ('User-Agent', 'X-AV-Client-Info', 'FriendlyName', 'Server'):
            header_value = self.headers.get(header_name)
            if header_value:
                parts.append(f'{header_name}={header_value}')
        return ' | '.join(parts) if parts else 'no identifying headers'

    def _detect_client_profile(self):
        client_ip = self.client_address[0]
        existing_profile = self.server.client_profiles.get(client_ip)
        fingerprint = self._request_header_fingerprint()
        fingerprint_lc = fingerprint.lower()
        is_likely_samsung = any(pattern in fingerprint_lc for pattern in self.SAMSUNG_HEADER_PATTERNS)
        if existing_profile and existing_profile.get('is_likely_samsung') and not is_likely_samsung:
            # Keep the stronger prior classification when later requests omit identifying headers.
            is_likely_samsung = True
            if fingerprint == 'no identifying headers':
                fingerprint = existing_profile.get('fingerprint', fingerprint)
        profile = {
            'client_ip': client_ip,
            'is_likely_samsung': is_likely_samsung,
            'fingerprint': fingerprint,
        }
        if existing_profile != profile:
            self.server.client_profiles[client_ip] = profile
            if is_likely_samsung:
                self.logger.info('Detected likely Samsung client ip=%s headers=%s', client_ip, fingerprint)
            else:
                self.logger.debug('Detected non-Samsung client ip=%s headers=%s', client_ip, fingerprint)
        return self.server.client_profiles[client_ip]

    def get_client_profile(self):
        return self.server.client_profiles.get(
            self.client_address[0],
            {
                'client_ip': self.client_address[0],
                'is_likely_samsung': False,
                'fingerprint': 'unknown',
            },
        )

    def ensure_current_config(self):
        try:
            refresh_server_config(self.server)
        except Exception as exc:
            self.logger.warning('Config refresh check failed: %s', exc)

    def send_response(self, *args, **kwargs):
        """Override to track headers sent state"""
        super().send_response(*args, **kwargs)
        self.headers_sent = True

    def send_error(self, *args, **kwargs):
        """Override to handle socket errors when sending error responses"""
        try:
            super().send_error(*args, **kwargs)
        except (socket.error, ConnectionError) as e:
            self.logger.debug(f"Socket error while sending error response: {str(e)}")
        except Exception as e:
            self.logger.debug(f"Error while sending error response: {str(e)}")
        finally:
            self.headers_sent = True

    def handle_one_request(self):
        """Override to add better error handling for socket operations"""
        try:
            return super().handle_one_request()
        except (socket.error, ConnectionError) as e:
            # Don't log common client disconnection errors at error level
            if isinstance(e, ConnectionAbortedError) or \
                getattr(e, 'winerror', None) in (10053, 10054):  # Connection aborted/reset
                self.logger.debug(f"Client connection closed: {str(e)}")
            else:
                self.logger.error(f"Socket error during request: {str(e)}")
            try:
                self.close_connection = True
            except Exception:
                pass
        except Exception as e:
            self.logger.error(f"Error handling request: {str(e)}", exc_info=True)
            try:
                self.close_connection = True
            except Exception:
                pass
    
    def send_media_file(self, file_path, content_type):
        try:
            file_size = os.path.getsize(file_path)
            
            # Handle range requests
            start_byte = 0
            end_byte = file_size - 1
            content_length = file_size
            
            range_header = self.headers.get('Range')
            if range_header:
                try:
                    ranges = range_header.replace('bytes=', '').split('-')
                    start_byte = int(ranges[0]) if ranges[0] else 0
                    if len(ranges) > 1 and ranges[1]:
                        end_byte = min(int(ranges[1]), file_size - 1)
                    content_length = end_byte - start_byte + 1
                except (ValueError, IndexError):
                    self.send_error(416, "Requested range not satisfiable")
                    return
                
                self.send_response(206)
                self.send_header('Content-Range', f'bytes {start_byte}-{end_byte}/{file_size}')
            else:
                self.send_response(200)
            
            duration = self.get_media_duration(file_path)
            if duration:
                self.send_header('X-Content-Duration', duration)
                
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(content_length))
            self.send_header('Accept-Ranges', 'bytes')
            self.send_header('Connection', 'keep-alive')
            self.send_header('transferMode.dlna.org', 'Streaming')
            self.send_header('contentFeatures.dlna.org', 'DLNA.ORG_OP=01;DLNA.ORG_CI=0;DLNA.ORG_FLAGS=01500000000000000000000000000000')
            self.end_headers()

            with open(file_path, 'rb') as f:
                if start_byte > 0:
                    f.seek(start_byte)
                    
                remaining = content_length
                while remaining > 0:
                    chunk_size = min(64 * 1024, remaining)  # 64KB chunks
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                        
                    try:
                        self.wfile.write(chunk)
                        self.logger.debug(f"Sent {len(chunk)} bytes for {os.path.basename(file_path)}")
                        remaining -= len(chunk)
                    except (socket.error, ConnectionError) as e:
                        self.logger.warning(f"Connection error while streaming: {e}")
                        break

                if remaining == 0:
                    self.logger.info(f"Successfully streamed {content_length} bytes for {os.path.basename(file_path)}")

        except Exception as e:
            self.logger.error(f"Error sending media file: {e}")
            if not self.headers_sent:
                self.send_error(500, "Error sending media file")

    def _parse_range_header(self, range_header, file_size):
        """Parse HTTP range header"""
        try:
            range_match = re.match(r'bytes=(\d+)-(\d*)', range_header)
            if range_match:
                start = int(range_match.group(1))
                end = int(range_match.group(2)) if range_match.group(2) else file_size - 1
                return max(0, start), min(file_size - 1, end)
            return 0, file_size - 1
        except Exception:
            return 0, file_size - 1

    def get_file_path(self, media_path):
        """Resolve a /media/<folder_index>/<relative_path> request to a shared file."""
        normalized = media_path.lstrip('/')
        folder_token, _, relative_path = normalized.partition('/')
        if not relative_path:
            self.logger.info("Media path missing relative component: %s", media_path)
            return None

        try:
            folder_index = int(folder_token)
        except ValueError:
            self.logger.info("Media path missing valid folder index: %s", media_path)
            return None

        if folder_index < 0 or folder_index >= len(self.server.media_folders):
            self.logger.info("Media path folder index out of range: %s", media_path)
            return None

        shared_root = os.path.abspath(self.server.media_folders[folder_index])
        candidate = os.path.abspath(os.path.join(shared_root, relative_path))
        if os.path.commonpath([shared_root, candidate]) != shared_root:
            self.logger.warning("Rejected media path outside share root: %s -> %s", media_path, candidate)
            return None
        if not os.path.isfile(candidate):
            self.logger.info("Media path resolved to missing file: %s -> %s", media_path, candidate)
            return None
        self.logger.info("Resolved media path %s -> %s", media_path, candidate)
        return candidate

    def do_GET(self):
        """Handle GET requests with proper logging"""
        try:
            self._detect_client_profile()
            self.ensure_current_config()
            parsed_path = urlparse(self.path)
            clean_path = parsed_path.path
            query = parse_qs(parsed_path.query)

            if clean_path == '/description.xml':
                self.send_device_description()
            elif clean_path == '/ContentDirectory.xml':
                self.send_content_directory()
            elif clean_path == '/ConnectionManager.xml':
                self.send_connection_manager()
            elif clean_path.startswith('/thumbnails/'):
                thumbnail_path = unquote(clean_path[len('/thumbnails/'):])
                if thumbnail_path.lower().endswith('.jpg'):
                    thumbnail_path = thumbnail_path[:-4]
                absolute_path = self.get_file_path(thumbnail_path)
                if absolute_path is None:
                    self.send_error(404, 'File not found')
                    return

                extension = os.path.splitext(absolute_path)[1].lower()
                if extension not in VIDEO_EXTENSIONS and extension not in IMAGE_EXTENSIONS:
                    self.send_error(415, 'Unsupported thumbnail media type')
                    return
                self.handle_thumbnail_request(absolute_path, is_video=extension in VIDEO_EXTENSIONS)
                return
            elif clean_path.startswith('/media/'):
                media_path = unquote(clean_path[len('/media/'):])
                absolute_path = self.get_file_path(media_path)
                if absolute_path is None:
                    self.send_error(404, 'File not found')
                    return

                if query.get('thumbnail', ['false'])[0].lower() == 'true':
                    extension = os.path.splitext(absolute_path)[1].lower()
                    if extension not in VIDEO_EXTENSIONS and extension not in IMAGE_EXTENSIONS:
                        self.send_error(415, 'Unsupported thumbnail media type')
                        return
                    self.handle_thumbnail_request(absolute_path, is_video=extension in VIDEO_EXTENSIONS)
                    return

                ext = os.path.splitext(absolute_path)[1].lower()
                content_type = VIDEO_EXTENSIONS.get(ext) or AUDIO_EXTENSIONS.get(ext) or IMAGE_EXTENSIONS.get(ext)
                if not content_type:
                    self.send_error(415, 'Unsupported media type')
                    return
                self.send_media_file(absolute_path, content_type)
            else:
                self.send_response(404)
                self.end_headers()
                
        except Exception as e:
            self.logger.error(f"Error handling GET request for {self.path}: {str(e)}")
            self.send_response(500)
            self.end_headers()

    def do_HEAD(self):
        """Handle HEAD requests by performing the same logic as GET but without sending the body"""
        try:
            self._detect_client_profile()
            self.ensure_current_config()
            parsed_path = urlparse(self.path)
            clean_path = parsed_path.path
            query = parse_qs(parsed_path.query)

            if clean_path.startswith('/thumbnails/'):
                thumbnail_path = unquote(clean_path[len('/thumbnails/'):])
                if thumbnail_path.lower().endswith('.jpg'):
                    thumbnail_path = thumbnail_path[:-4]
                abs_path = self.get_file_path(thumbnail_path)
                if abs_path is None:
                    self.send_error(404, 'File not found')
                    return

                ext = os.path.splitext(abs_path)[1].lower()
                if ext not in VIDEO_EXTENSIONS and ext not in IMAGE_EXTENSIONS:
                    self.send_error(415, 'Unsupported thumbnail media type')
                    return
                self.send_thumbnail_headers(abs_path, is_video=ext in VIDEO_EXTENSIONS)
                return

            if clean_path.startswith('/media/'):
                media_path = unquote(clean_path[len('/media/'):])
                abs_path = self.get_file_path(media_path)
                
                if abs_path:
                    ext = os.path.splitext(abs_path)[1].lower()
                    if query.get('thumbnail', ['false'])[0].lower() == 'true':
                        if ext not in VIDEO_EXTENSIONS and ext not in IMAGE_EXTENSIONS:
                            self.send_error(415, 'Unsupported thumbnail media type')
                            return
                        self.send_thumbnail_headers(abs_path, is_video=ext in VIDEO_EXTENSIONS)
                        return
                    content_type = VIDEO_EXTENSIONS.get(ext) or AUDIO_EXTENSIONS.get(ext) or IMAGE_EXTENSIONS.get(ext)
                    
                    if content_type:
                        self.send_response(200)
                        self.send_header('Content-Type', content_type)
                        self.send_header('Content-Length', str(os.path.getsize(abs_path)))
                        self.send_header('transferMode.dlna.org', 'Streaming')
                        self.send_header('contentFeatures.dlna.org', 'DLNA.ORG_OP=01;DLNA.ORG_CI=0')
                        self.end_headers()
                        return

            if clean_path == '/description.xml':
                self.send_response(200)
                self.send_header('Content-Type', 'text/xml; charset="utf-8"')
                self.end_headers()
            elif clean_path in ['/ContentDirectory.xml', '/ConnectionManager.xml']:
                self.send_response(200)
                self.send_header('Content-Type', 'text/xml; charset="utf-8"')
                self.end_headers()
            else:
                self.send_error(404, "File not found")
                
        except Exception as e:
            self.logger.error(f"Error handling HEAD request: {e}")
            self.send_error(500, f"Internal server error: {str(e)}")

    def do_POST(self):
        """Handle POST requests, particularly for ContentDirectory control"""
        self._detect_client_profile()
        self.ensure_current_config()
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        
        if self.path == '/ContentDirectory/control':
            try:
                self.content_handler.handle_control(post_data)
            except Exception as e:
                self.error_handler.handle_request_error(self, e)
        elif self.path == '/ConnectionManager/control':
            try:
                self.handle_connection_manager_control()
            except Exception as e:
                self.error_handler.handle_request_error(self, e)
        else:
            self.send_error(404)

    def handle_connection_manager_control(self):
        action = self.headers.get('SOAPACTION', '').strip('"')
        if '#' in action:
            action = action.rsplit('#', 1)[1]

        if action == 'GetProtocolInfo':
            source = ','.join([
                'http-get:*:audio/mpeg:DLNA.ORG_PN=MP3',
                'http-get:*:audio/flac:*',
                'http-get:*:audio/wav:*',
                'http-get:*:video/mp4:DLNA.ORG_PN=AVC_MP4_BL_CIF15_AAC',
                'http-get:*:video/x-matroska:*',
                'http-get:*:video/x-msvideo:*',
                'http-get:*:image/jpeg:DLNA.ORG_PN=JPEG_LRG',
                'http-get:*:image/png:DLNA.ORG_PN=PNG_LRG',
                'http-get:*:image/gif:*',
            ])
            body = f'<Source>{source}</Source><Sink></Sink>'
            self.soap_handler.send_soap_response(body, 'GetProtocolInfo', 'urn:schemas-upnp-org:service:ConnectionManager:1')
            return

        if action == 'GetCurrentConnectionIDs':
            self.soap_handler.send_soap_response(
                '<ConnectionIDs>0</ConnectionIDs>',
                'GetCurrentConnectionIDs',
                'urn:schemas-upnp-org:service:ConnectionManager:1',
            )
            return

        if action == 'GetCurrentConnectionInfo':
            body = (
                '<RcsID>-1</RcsID>'
                '<AVTransportID>-1</AVTransportID>'
                '<ProtocolInfo></ProtocolInfo>'
                '<PeerConnectionManager></PeerConnectionManager>'
                '<PeerConnectionID>-1</PeerConnectionID>'
                '<Direction>Output</Direction>'
                '<Status>OK</Status>'
            )
            self.soap_handler.send_soap_response(
                body,
                'GetCurrentConnectionInfo',
                'urn:schemas-upnp-org:service:ConnectionManager:1',
            )
            return

        raise ValueError(f'Unsupported ConnectionManager action: {action or "unknown"}')

    def send_media_file(self, file_path, content_type):
        """Stream media file with proper DLNA support and range handling"""
        file_size = os.path.getsize(file_path)
        duration = self.get_media_duration_seconds(file_path)

        # Handle range requests
        start_byte = 0
        end_byte = file_size - 1
        
        if 'Range' in self.headers:
            try:
                range_header = self.headers['Range'].replace('bytes=', '').split('-')
                start_byte = int(range_header[0]) if range_header[0] else 0
                end_byte = int(range_header[1]) if len(range_header) > 1 and range_header[1] else file_size - 1
            except Exception as e:
                self.logger.warning(f"Range parsing error: {e}")

        # Handle time-based seeking
        start_time, end_time = self.handle_time_seek_request()
        if start_time is not None:
            # Convert time to bytes (approximate)
            bytes_per_second = file_size / duration if duration else 0
            start_byte = int(start_time * bytes_per_second)
            if end_time:
                end_byte = int(end_time * bytes_per_second)

        content_length = end_byte - start_byte + 1
        
        # Send headers
        self.send_response(206 if start_byte > 0 or end_byte < file_size - 1 else 200)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(content_length))
        self.send_header('Accept-Ranges', 'bytes')
        self.send_header('Content-Range', f'bytes {start_byte}-{end_byte}/{file_size}')
        
        # DLNA specific headers
        self.send_header('TransferMode.DLNA.ORG', 'Streaming')
        self.send_header('contentFeatures.dlna.org', 
                        'DLNA.ORG_OP=01;DLNA.ORG_CI=0;DLNA.ORG_FLAGS=01700000000000000000000000000000')
        self.send_header('Connection', 'keep-alive')
        
        if duration:
            self.send_header('X-Content-Duration', str(duration))
            self.send_header('TimeSeekRange.dlna.org', f'npt=0.0-{duration}')
        
        self.end_headers()

        # Stream the file
        with open(file_path, 'rb') as f:
            f.seek(start_byte)
            remaining = content_length
            chunk_size = min(102400, remaining)  # 100KB chunks
            
            while remaining > 0:
                if self.close_connection:
                    break
                    
                chunk = f.read(min(chunk_size, remaining))
                if not chunk:
                    break
                    
                try:
                    self.wfile.write(chunk)
                    remaining -= len(chunk)
                except (ConnectionError, socket.error) as e:
                    self.logger.warning(f"Connection error while streaming: {e}")
                    break

    def _generate_search_didl(self, results):
        """Generate DIDL-Lite XML for search results"""
        root = Element('DIDL-Lite', {
            'xmlns': 'urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/',
            'xmlns:dc': 'http://purl.org/dc/elements/1.1/',
            'xmlns:upnp': 'urn:schemas-upnp-org:metadata-1-0/upnp/'
        })
        
        for path, score, title in results:
            self.add_item_to_didl(root, path, title, '0')
            
        return self.encode_didl(root)

    def _send_search_response(self, didl, number_returned, total_matches):
        """Send SOAP response for search results"""
        response = f'''<?xml version="1.0"?>
        <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
            <s:Body>
                <u:SearchResponse xmlns:u="urn:schemas-upnp-org:service:ContentDirectory:1">
                    <Result>{didl}</Result>
                    <NumberReturned>{number_returned}</NumberReturned>
                    <TotalMatches>{total_matches}</TotalMatches>
                    <UpdateID>1</UpdateID>
                </u:SearchResponse>
            </s:Body>
        </s:Envelope>'''
        
        self.send_response(200)
        self.send_header('Content-Type', 'text/xml; charset="utf-8"')
        response_bytes = response.encode('utf-8')
        self.send_header('Content-Length', str(len(response_bytes)))
        self.end_headers()
        self.wfile.write(response_bytes)

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
            friendly_name = getattr(self.server, 'device_name', build_device_name('unknown'))
            SubElement(device, 'friendlyName').text = friendly_name
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
        """Send the ContentDirectory SCPD document."""
        try:
            content_directory_xml = '''<?xml version="1.0" encoding="utf-8"?>
<scpd xmlns="urn:schemas-upnp-org:service-1-0">
    <specVersion>
        <major>1</major>
        <minor>0</minor>
    </specVersion>
    <actionList>
        <action>
            <name>Browse</name>
            <argumentList>
                <argument><name>ObjectID</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_ObjectID</relatedStateVariable></argument>
                <argument><name>BrowseFlag</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_BrowseFlag</relatedStateVariable></argument>
                <argument><name>Filter</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_Filter</relatedStateVariable></argument>
                <argument><name>StartingIndex</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_Index</relatedStateVariable></argument>
                <argument><name>RequestedCount</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_Count</relatedStateVariable></argument>
                <argument><name>SortCriteria</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_SortCriteria</relatedStateVariable></argument>
                <argument><name>Result</name><direction>out</direction><relatedStateVariable>A_ARG_TYPE_Result</relatedStateVariable></argument>
                <argument><name>NumberReturned</name><direction>out</direction><relatedStateVariable>A_ARG_TYPE_Count</relatedStateVariable></argument>
                <argument><name>TotalMatches</name><direction>out</direction><relatedStateVariable>A_ARG_TYPE_Count</relatedStateVariable></argument>
                <argument><name>UpdateID</name><direction>out</direction><relatedStateVariable>SystemUpdateID</relatedStateVariable></argument>
            </argumentList>
        </action>
        <action>
            <name>GetSearchCapabilities</name>
            <argumentList>
                <argument><name>SearchCaps</name><direction>out</direction><relatedStateVariable>SearchCapabilities</relatedStateVariable></argument>
            </argumentList>
        </action>
        <action>
            <name>GetSortCapabilities</name>
            <argumentList>
                <argument><name>SortCaps</name><direction>out</direction><relatedStateVariable>SortCapabilities</relatedStateVariable></argument>
            </argumentList>
        </action>
        <action>
            <name>GetSystemUpdateID</name>
            <argumentList>
                <argument><name>Id</name><direction>out</direction><relatedStateVariable>SystemUpdateID</relatedStateVariable></argument>
            </argumentList>
        </action>
    </actionList>
    <serviceStateTable>
        <stateVariable sendEvents="no"><name>A_ARG_TYPE_ObjectID</name><dataType>string</dataType></stateVariable>
        <stateVariable sendEvents="no"><name>A_ARG_TYPE_BrowseFlag</name><dataType>string</dataType></stateVariable>
        <stateVariable sendEvents="no"><name>A_ARG_TYPE_Filter</name><dataType>string</dataType></stateVariable>
        <stateVariable sendEvents="no"><name>A_ARG_TYPE_Index</name><dataType>ui4</dataType></stateVariable>
        <stateVariable sendEvents="no"><name>A_ARG_TYPE_Count</name><dataType>ui4</dataType></stateVariable>
        <stateVariable sendEvents="no"><name>A_ARG_TYPE_SortCriteria</name><dataType>string</dataType></stateVariable>
        <stateVariable sendEvents="no"><name>A_ARG_TYPE_Result</name><dataType>string</dataType></stateVariable>
        <stateVariable sendEvents="no"><name>SystemUpdateID</name><dataType>ui4</dataType><defaultValue>1</defaultValue></stateVariable>
        <stateVariable sendEvents="no"><name>SortCapabilities</name><dataType>string</dataType><defaultValue>dc:title,dc:date</defaultValue></stateVariable>
        <stateVariable sendEvents="no"><name>SearchCapabilities</name><dataType>string</dataType><defaultValue>dc:title,upnp:class</defaultValue></stateVariable>
    </serviceStateTable>
</scpd>'''

            self.send_response(200)
            self.send_header('Content-Type', 'text/xml; charset="utf-8"')
            response_bytes = content_directory_xml.encode('utf-8')
            self.send_header('Content-Length', str(len(response_bytes)))
            self.end_headers()
            self.wfile.write(response_bytes)
            self.logger.debug("Sent Content Directory service description")

        except Exception as e:
            self.logger.error(f"Error sending Content Directory description: {str(e)}")
            if not self.headers_sent:
                self.send_error(500, "Internal server error")

    def send_connection_manager(self):
        """Send the ConnectionManager SCPD document."""
        try:
            connection_manager_xml = '''<?xml version="1.0" encoding="utf-8"?>
<scpd xmlns="urn:schemas-upnp-org:service-1-0">
    <specVersion>
        <major>1</major>
        <minor>0</minor>
    </specVersion>
    <actionList>
        <action>
            <name>GetProtocolInfo</name>
            <argumentList>
                <argument><name>Source</name><direction>out</direction><relatedStateVariable>SourceProtocolInfo</relatedStateVariable></argument>
                <argument><name>Sink</name><direction>out</direction><relatedStateVariable>SinkProtocolInfo</relatedStateVariable></argument>
            </argumentList>
        </action>
        <action>
            <name>GetCurrentConnectionIDs</name>
            <argumentList>
                <argument><name>ConnectionIDs</name><direction>out</direction><relatedStateVariable>CurrentConnectionIDs</relatedStateVariable></argument>
            </argumentList>
        </action>
        <action>
            <name>GetCurrentConnectionInfo</name>
            <argumentList>
                <argument><name>ConnectionID</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_ConnectionID</relatedStateVariable></argument>
                <argument><name>RcsID</name><direction>out</direction><relatedStateVariable>A_ARG_TYPE_RcsID</relatedStateVariable></argument>
                <argument><name>AVTransportID</name><direction>out</direction><relatedStateVariable>A_ARG_TYPE_AVTransportID</relatedStateVariable></argument>
                <argument><name>ProtocolInfo</name><direction>out</direction><relatedStateVariable>A_ARG_TYPE_ProtocolInfo</relatedStateVariable></argument>
                <argument><name>PeerConnectionManager</name><direction>out</direction><relatedStateVariable>A_ARG_TYPE_ConnectionManager</relatedStateVariable></argument>
                <argument><name>PeerConnectionID</name><direction>out</direction><relatedStateVariable>A_ARG_TYPE_ConnectionID</relatedStateVariable></argument>
                <argument><name>Direction</name><direction>out</direction><relatedStateVariable>A_ARG_TYPE_Direction</relatedStateVariable></argument>
                <argument><name>Status</name><direction>out</direction><relatedStateVariable>A_ARG_TYPE_ConnectionStatus</relatedStateVariable></argument>
            </argumentList>
        </action>
    </actionList>
    <serviceStateTable>
        <stateVariable sendEvents="no"><name>SourceProtocolInfo</name><dataType>string</dataType></stateVariable>
        <stateVariable sendEvents="no"><name>SinkProtocolInfo</name><dataType>string</dataType></stateVariable>
        <stateVariable sendEvents="no"><name>CurrentConnectionIDs</name><dataType>string</dataType></stateVariable>
        <stateVariable sendEvents="no"><name>A_ARG_TYPE_ConnectionID</name><dataType>i4</dataType></stateVariable>
        <stateVariable sendEvents="no"><name>A_ARG_TYPE_RcsID</name><dataType>i4</dataType></stateVariable>
        <stateVariable sendEvents="no"><name>A_ARG_TYPE_AVTransportID</name><dataType>i4</dataType></stateVariable>
        <stateVariable sendEvents="no"><name>A_ARG_TYPE_ProtocolInfo</name><dataType>string</dataType></stateVariable>
        <stateVariable sendEvents="no"><name>A_ARG_TYPE_ConnectionManager</name><dataType>string</dataType></stateVariable>
        <stateVariable sendEvents="no"><name>A_ARG_TYPE_Direction</name><dataType>string</dataType></stateVariable>
        <stateVariable sendEvents="no"><name>A_ARG_TYPE_ConnectionStatus</name><dataType>string</dataType></stateVariable>
    </serviceStateTable>
</scpd>'''

            self.send_response(200)
            self.send_header('Content-Type', 'text/xml; charset="utf-8"')
            response_bytes = connection_manager_xml.encode('utf-8')
            self.send_header('Content-Length', str(len(response_bytes)))
            self.end_headers()
            self.wfile.write(response_bytes)

        except Exception as e:
            self.logger.error(f"Error sending connection manager: {str(e)}")
            if not self.headers_sent:
                self.send_error(500, "Internal server error")

    def send_av_transport(self):
        """Send DLNA AV Transport XML"""
        try:
            # Create SOAP envelope with AV transport info
            av_transport_xml = '''<?xml version="1.0" encoding="utf-8" standalone="yes"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">
  <s:Body>
    <u:GetTransportInfoResponse xmlns:u="urn:schemas-upnp-org:service:AVTransport:1">
      <CurrentTransportState>STOPPED</CurrentTransportState>
      <CurrentTransportStatus>OK</CurrentTransportStatus>
      <CurrentSpeed>1</CurrentSpeed>
    </u:GetTransportInfoResponse>
  </s:Body>
</s:Envelope>'''

            self.send_response(200)
            self.send_header('Content-Type', 'text/xml; charset="utf-8"')
            self.send_header('Ext', '') # Required by UPnP spec
            self.send_header('Server', 'Windows/10.0 UPnP/1.0 Python-DLNA/1.0')
            response_bytes = av_transport_xml.encode('utf-8')
            self.send_header('Content-Length', str(len(response_bytes)))
            self.end_headers()
            self.wfile.write(response_bytes)

        except Exception as e:
            self.logger.error(f"Error sending AV transport info: {str(e)}")
            if not self.headers_sent:
                self.send_error(500, "Internal server error")

    def send_file_with_error_handling(self, file_path):
        """Send file with improved buffering, timeouts and connection handling"""
        try:
            # Set socket timeout for streaming operations
            self.connection.settimeout(30.0)  # 30 second timeout for network operations
            
            # Use larger buffer for network operations
            buffer_size = 256 * 1024  # 256KB buffer
            total_sent = 0
            last_activity = time.time()
            
            with open(file_path, 'rb') as f:
                while True:
                    try:
                        chunk = f.read(buffer_size)
                        if not chunk:
                            break
                            
                        self.wfile.write(chunk)
                        total_sent += len(chunk)
                        
                        # Update activity timestamp
                        last_activity = time.time()
                        
                        # Track network usage if resource monitor is available
                        if self.resource_monitor:
                            self.resource_monitor.track_network(bytes_sent=len(chunk))
                                
                    except socket.timeout:
                        # Check if connection is still alive
                        if time.time() - last_activity > 60:  # 1 minute without activity
                            self.logger.warning("Connection timed out due to inactivity")
                            return False
                    except (socket.error, ConnectionError) as e:
                        self.logger.warning(f"Connection error while streaming: {e}")
                        return False
                        
                self.logger.debug(f"Successfully sent {total_sent} bytes")
                return True
                
        except IOError as e:
            self.logger.error(f"IO error reading {file_path}: {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error sending {file_path}: {str(e)}")
            return False
        finally:
            try:
                # Reset socket timeout to default
                self.connection.settimeout(None)
            except:
                pass

    def serve_media_file(self, file_path):
        try:
            for shared_folder in self.server.media_folders:
                potential_path = os.path.abspath(os.path.join(shared_folder, file_path))
                shared_folder_abs = os.path.abspath(shared_folder)
                if os.path.commonpath([shared_folder_abs, potential_path]) == shared_folder_abs:
                    if os.path.exists(potential_path) and os.path.isfile(potential_path):
                        ext = os.path.splitext(potential_path)[1].lower()
                        content_type = (VIDEO_EXTENSIONS.get(ext) or 
                                      AUDIO_EXTENSIONS.get(ext) or 
                                      IMAGE_EXTENSIONS.get(ext))
                        if content_type:
                            self.send_media_file(potential_path, content_type)
                            return
                        break

            self.send_error(404, "File not found")

        except Exception as e:
            self.logger.error(f"Error serving media file: {e}")
            if not self.headers_sent:
                self.send_error(500, "Error serving media file")

    def send_thumbnail_headers(self, file_path, is_video=False):
        thumbnail_path, cache_hit = self.thumbnail_generator.ensure_static_thumbnail(
            file_path,
            output_extension='jpg',
            verbose=0,
        )
        if not thumbnail_path:
            raise FileNotFoundError(f'No thumbnail generated for {file_path}')
        if self.resource_monitor:
            self.resource_monitor.track_cache('thumbnail', hit=cache_hit)
        self.send_response(200)
        self.send_header('Content-Type', 'image/jpeg')
        self.send_header('Content-Length', str(os.path.getsize(thumbnail_path)))
        self.send_header('Cache-Control', 'max-age=86400')
        self.send_header('Last-Modified', self.date_time_string(os.path.getmtime(thumbnail_path)))
        self.end_headers()

    def handle_thumbnail_request(self, file_path, is_video=False):
        """Generate and serve thumbnails for videos and images with disk caching"""
        try:
            thumbnail_path, cache_hit = self.thumbnail_generator.ensure_static_thumbnail(
                file_path,
                output_extension='jpg',
                verbose=0,
            )
            if not thumbnail_path:
                raise FileNotFoundError(f'No thumbnail generated for {file_path}')
            thumb_data = Path(thumbnail_path).read_bytes()
            if self.resource_monitor:
                self.resource_monitor.track_cache('thumbnail', hit=cache_hit)
            self.send_response(200)
            self.send_header('Content-Type', 'image/jpeg')
            self.send_header('Content-Length', str(len(thumb_data)))
            self.send_header('Cache-Control', 'max-age=86400')
            self.send_header('Last-Modified', self.date_time_string(os.path.getmtime(thumbnail_path)))
            self.end_headers()
            self.wfile.write(thumb_data)
            
        except Exception as e:
            self.logger.error(f"Error handling thumbnail: {e}")
            if not self.headers_sent:
                self.send_error(500, "Could not generate thumbnail")

    def get_dlna_profile(self, ext, mime_type):
        """Get DLNA profile and protocol info based on file type"""
        DLNA_PROFILES = {
            '.mp4': ('AVC_MP4_HP_HD_AAC', 'video', '01700000000000000000000000000000'),
            '.mkv': ('MKV', 'video', '01700000000000000000000000000000'),
            '.mp3': ('MP3', 'audio', '01500000000000000000000000000000'),
            '.flac': ('FLAC', 'audio', '01500000000000000000000000000000'),
            '.jpg': ('JPEG_LRG', 'image', '00f00000000000000000000000000000'),
            '.jpeg': ('JPEG_LRG', 'image', '00f00000000000000000000000000000'),
            '.png': ('PNG_LRG', 'image', '00f00000000000000000000000000000')
        }
        
        if ext not in DLNA_PROFILES:
            return None, None
            
        profile, media_type, flags = DLNA_PROFILES[ext]
        protocol_info = (f'http-get:*:{mime_type}:DLNA.ORG_PN={profile};'
                        f'DLNA.ORG_OP=11;DLNA.ORG_CI=0;DLNA.ORG_FLAGS={flags}')
        
        return profile, protocol_info

    def serve_descriptor_file(self, file_path):
        """Serve UPnP/DLNA XML descriptor files with enhanced error handling"""
        try:
            if not hasattr(self, 'descriptor_stats'):
                self.__class__.descriptor_stats = {
                    'total_requests': 0,
                    'errors': 0,
                    'last_error': None
                }
            
            self.descriptor_stats['total_requests'] += 1
            
            if file_path == '/description.xml':
                self.send_device_description()
            elif file_path == '/ContentDirectory.xml':
                self.send_content_directory()
            elif file_path == '/ConnectionManager.xml':
                self.send_connection_manager()
            elif file_path == '/AVTransport.xml':
                self.send_av_transport()
            else:
                self.descriptor_stats['errors'] += 1
                self.descriptor_stats['last_error'] = f"Unknown descriptor: {file_path}"
                self.send_error(404, "Unknown descriptor file")
                return

            # Log descriptor service stats periodically
            if self.descriptor_stats['total_requests'] % 10 == 0:
                error_rate = (self.descriptor_stats['errors'] / self.descriptor_stats['total_requests']) * 100
                self.logger.info(f"XML Descriptor Stats - Requests: {self.descriptor_stats['total_requests']}, "
                                f"Error Rate: {error_rate:.1f}%, Last Error: {self.descriptor_stats['last_error']}")

        except Exception as e:
            self.descriptor_stats['errors'] += 1
            self.descriptor_stats['last_error'] = str(e)
            self.logger.error(f"Error serving descriptor file {file_path}: {e}", exc_info=True)
            if not self.headers_sent:
                self.send_error(500, f"Internal server error serving {file_path}")

    def get_media_duration_seconds(self, file_path):
        """Get media duration in seconds"""
        try:
            media = File(file_path)
            if media and hasattr(media.info, 'length'):
                return int(media.info.length)
        except Exception as e:
            self.logger.debug(f"Could not get duration for {file_path}: {e}")
        return None

    def handle_time_seek_request(self):
        """Handle DLNA time-based seek requests"""
        try:
            timeseek_header = self.headers.get('TimeSeekRange.dlna.org')
            if timeseek_header:
                # Format: npt=<start_time>-<end_time>
                times = timeseek_header.split('=')[1].split('-')
                start_time = float(times[0])
                end_time = float(times[1]) if len(times) > 1 and times[1] else None
                return start_time, end_time
        except Exception as e:
            self.logger.debug(f"Error parsing time seek request: {e}")
        return None, None

def load_config():
    parser = argparse.ArgumentParser(description="Mini DLNA Server")
    parser.add_argument('--config', type=str, help='Path to configuration JSON file', default='config.json')
    args = parser.parse_args()

    config_path = args.config
    return load_config_file(config_path), config_path


def normalize_playlists(raw_playlists, media_folders, logger):
    def playlist_specs():
        if raw_playlists is None:
            return []
        if isinstance(raw_playlists, dict):
            return [
                {'name': str(name), 'files': files}
                for name, files in raw_playlists.items()
            ]
        if isinstance(raw_playlists, list):
            specs = []
            for index, entry in enumerate(raw_playlists, start=1):
                if isinstance(entry, dict):
                    specs.append(
                        {
                            'name': str(entry.get('name') or f'Playlist {index}'),
                            'files': entry.get('files', []),
                        }
                    )
                else:
                    specs.append({'name': f'Playlist {index}', 'files': entry})
            return specs
        logger.warning('Ignoring invalid playlists config because it is neither a list nor an object')
        return []

    def resolve_shared_file(file_path, playlist_name):
        if not isinstance(file_path, str):
            logger.warning('Skipping non-string playlist entry in %s: %r', playlist_name, file_path)
            return None

        abs_path = os.path.abspath(file_path)
        if not os.path.exists(abs_path):
            logger.warning('Skipping missing playlist file in %s: %s', playlist_name, abs_path)
            return None
        if not os.path.isfile(abs_path):
            logger.warning('Skipping non-file playlist entry in %s: %s', playlist_name, abs_path)
            return None
        if os.path.splitext(abs_path)[1].lower() not in ALL_EXTENSIONS:
            logger.warning('Skipping unsupported playlist file in %s: %s', playlist_name, abs_path)
            return None

        for folder_index, shared_folder in enumerate(media_folders):
            shared_root = os.path.abspath(shared_folder)
            try:
                if os.path.commonpath([shared_root, abs_path]) == shared_root:
                    return {'folder_index': folder_index, 'abs_path': abs_path}
            except ValueError:
                continue

        logger.warning('Playlist file is outside shared folders in %s: %s', playlist_name, abs_path)
        return None

    playlists = []
    for spec in playlist_specs():
        files = spec.get('files', [])
        if not isinstance(files, list):
            logger.warning('Ignoring playlist %s because files is not a list', spec['name'])
            continue

        items = []
        for file_path in files:
            resolved = resolve_shared_file(file_path, spec['name'])
            if resolved is not None:
                items.append(resolved)

        playlists.append({'name': spec['name'], 'items': items})

    return playlists


def build_runtime_state(config, logger):
    media_folders = config.get('shared_paths', [])
    if not media_folders:
        raise ValueError('No shared paths specified in configuration.')

    for folder in media_folders:
        if not os.path.exists(folder):
            raise ValueError(f'Media folder does not exist: {folder}')

    playlists = normalize_playlists(config.get('playlists', []), media_folders, logger)
    return media_folders, playlists


def apply_runtime_state(server, media_folders, playlists):
    server.media_folders = media_folders
    server.playlists = playlists


def refresh_server_config(server, force=False):
    config_path = getattr(server, 'config_path', None)
    if not config_path:
        return False

    config_lock = getattr(server, 'config_lock', None)
    if config_lock is None:
        return False

    logger = logging.getLogger('DLNAServer')
    with config_lock:
        try:
            stat_result = os.stat(config_path)
        except OSError as exc:
            logger.warning('Config reload skipped because config path is unreadable: %s (%s)', config_path, exc)
            return False

        current_signature = (stat_result.st_mtime_ns, stat_result.st_size)
        previous_signature = getattr(server, 'config_signature', None)
        if not force and current_signature == previous_signature:
            return False

        try:
            config = load_config_file(config_path)
            media_folders, playlists = build_runtime_state(config, logger)
        except Exception as exc:
            logger.warning('Config reload failed, keeping previous configuration: %s', exc)
            return False

        apply_runtime_state(server, media_folders, playlists)
        server.config_signature = current_signature
        server.system_update_id = getattr(server, 'system_update_id', 1) + (0 if previous_signature is None else 1)
        logger.info(
            'Configuration %s from %s: shared_paths=%s playlists=%s system_update_id=%s',
            'loaded' if previous_signature is None else 'reloaded',
            config_path,
            len(media_folders),
            len(playlists),
            server.system_update_id,
        )
        if previous_signature is not None:
            logger.info(
                'Config changed but no ContentDirectory event subscribers are implemented; clients must detect updates via browsing or GetSystemUpdateID.'
            )
        return True

def start_server(config):
    """Start the DLNA media server with Windows compatibility"""
    logger = setup_logging()
    instance_id = time.strftime('%H%M%S')
    device_name = build_device_name(instance_id)

    try:
        media_folders, playlists = build_runtime_state(config, logger)
    except ValueError as exc:
        logger.error(str(exc))
        sys.exit(1)

    local_ip = NetworkUtils.get_local_ip()
    
    # Try ports until we find an available one
    base_port = 8201  # Start at 8201 to avoid common DLNA ports
    max_port = 8299
    
    server = None
    port = base_port
    
    while port <= max_port:
        try:
            server = ThreadingHTTPServer((local_ip, port), DLNAServer)
            break
        except socket.error:
            logger.debug(f"Port {port} in use, trying next port")
            port += 1
    
    if server is None:
        logger.error(f"Could not find available port between {base_port} and {max_port}")
        sys.exit(1)

    server.config_lock = threading.Lock()
    server.config_path = os.path.abspath(config.get('_config_path', 'config.json'))
    server.config_signature = None
    server.system_update_id = 1
    apply_runtime_state(server, media_folders, playlists)
    server.local_ip = local_ip
    server.device_name = device_name
    server.resource_monitor = ResourceMonitor()  # Initialize resource monitor
    refresh_server_config(server, force=True)
    
    # Start SSDP server in a separate thread
    ssdp_server = SSDPServer((local_ip, port), DEVICE_UUID, device_name)
    ssdp_thread = threading.Thread(target=ssdp_server.start, name="SSDPServerThread")
    ssdp_thread.daemon = True
    ssdp_thread.start()

    shutdown_requested = False

    def cleanup_server():
        nonlocal shutdown_requested
        if shutdown_requested:
            return
        shutdown_requested = True
        logger.info("Shutting down server...")
        ssdp_server.stop()
        ssdp_thread.join(timeout=3.0)
        server.server_close()
        logger.info("HTTP server closed.")

    try:
        print(f"Server identity: {device_name} at http://{local_ip}:{port}", flush=True)
        logger.info(f"DLNA server started at http://{local_ip}:{port}")
        logger.info(f"Device name: {device_name}")
        logger.info(f"Serving media from: {', '.join(server.media_folders)}") # Access via server instance
        logger.info("Press Ctrl+C to stop the server")
        server.serve_forever()
    except KeyboardInterrupt:
        cleanup_server()
        sys.exit(0)
    except Exception as e:
        logger.critical(f"Critical error in main server loop: {e}", exc_info=True)
        cleanup_server()
        sys.exit(1)

if __name__ == "__main__":
    config, config_path = load_config()
    config['_config_path'] = config_path
    start_server(config)
