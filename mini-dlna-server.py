import errno
import os
import sys
import socket
import logging
import time
import uuid
import threading
import subprocess
import shutil
from urllib.parse import unquote, quote, urlparse, parse_qs
from pathlib import Path
from http.server import HTTPServer, BaseHTTPRequestHandler
from logging.handlers import RotatingFileHandler
from xml.etree import ElementTree
from xml.etree.ElementTree import Element, SubElement, tostring, fromstring
from datetime import datetime
from mutagen import File
import json
import argparse
import re
import random
from network_utils import NetworkUtils  # Add this import

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

    # Regular log file with INFO and above
    file_handler = RotatingFileHandler(
        log_dir / 'dlna_server.log',
        maxBytes=5*1024*1024,
        backupCount=5
    )
    file_handler.setLevel(logging.INFO)
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
        self.last_announcement_time = None
        self.announcement_count = 0
        self.metrics = {
            'announcements': [],
            'filtered_msgs': 0,
            'intervals': [],
            'last_interval': None,
            'backoff_violations': 0,
            'performance': {
                'total_messages': 0,
                'response_times': [],
                'error_count': 0,
            }
        }
        self.metrics['last_check'] = time.time()  # Initialize last_check
        self.known_clients = set()  # Track unique clients

    def get_local_ip(self):
        """Get the local IP address used for SSDP communication"""
        return NetworkUtils.get_local_ip(self.http_server_address[0])

    def validate_announcement_timing(self, current_time):
        """Validate announcement timing follows exponential backoff"""
        if self.last_announcement_time:
            interval = current_time - self.last_announcement_time
            self.metrics['intervals'].append(interval)
            
            # Keep last 10 intervals for analysis
            if len(self.metrics['intervals']) > 10:
                self.metrics['intervals'].pop(0)
            
            # Check if interval is increasing properly with more lenient threshold
            if self.metrics['last_interval']:
                expected_interval = min(self.metrics['last_interval'] * 1.5, 1800)  # Changed from 2x to 1.5x
                if interval < expected_interval * 0.8:  # Allow 20% variance instead of 10%
                    self.metrics['backoff_violations'] += 1
                    self.logger.warning(
                        f"Backoff violation: Interval {interval:.1f}s, "
                        f"Expected {expected_interval:.1f}s"
                    )
            
            self.metrics['last_interval'] = interval
            
            # Log comprehensive metrics every 10 announcements
            if len(self.metrics['intervals']) >= 10:
                self.log_metrics()

    def log_metrics(self):
        """Log comprehensive performance metrics"""
        now = time.time()
        metrics = self.metrics
        
        # Calculate time-based stats
        elapsed = now - metrics['last_check']
        msg_rate = metrics['performance']['total_messages'] / elapsed if elapsed > 0 else 0
        avg_interval = sum(metrics['intervals']) / len(metrics['intervals'])
        
        # Log detailed stats
        self.logger.info(
            f"SSDP Stats - Avg Interval: {avg_interval:.1f}s, "
            f"Violations: {metrics['backoff_violations']}, "
            f"Filtered: {metrics['filtered_msgs']}, "
            f"Msg Rate: {msg_rate:.2f}/s, "
            f"Unique Clients: {len(self.known_clients)}"
        )
        
        # Reset counters
        metrics['performance']['total_messages'] = 0
        metrics['performance']['response_times'] = []
        metrics['last_check'] = now

    def send_notification(self, nts_type):
        """Send SSDP notification (alive or byebye) with enhanced Samsung compatibility"""
        services = [
            'upnp:rootdevice',
            f'uuid:{DEVICE_UUID}',
            'urn:schemas-upnp-org:device:MediaServer:1',
            'urn:schemas-upnp-org:service:ContentDirectory:1',
            'urn:schemas-upnp-org:service:ConnectionManager:1',
            'urn:schemas-upnp-org:service:AVTransport:1'
        ]

        location = f'http://{self.http_server_address[0]}:{self.http_server_address[1]}/description.xml'

        for service in services:
            usn = f'uuid:{DEVICE_UUID}'
            if service != f'uuid:{DEVICE_UUID}':
                usn += f'::{service}'

            try:
                notify_msg = '\r\n'.join([
                    'NOTIFY * HTTP/1.1',
                    f'HOST: {SSDP_ADDR}:{SSDP_PORT}',
                    'CACHE-CONTROL: max-age=1800',
                    f'LOCATION: {location}',
                    f'NT: {service}',
                    f'NTS: {nts_type}',
                    'SERVER: Windows/10.0 UPnP/1.0 Python-DLNA/1.0',
                    f'USN: {usn}',
                    'BOOTID.UPNP.ORG: 1',
                    'CONFIGID.UPNP.ORG: 1',
                    'DEVICEID.SES.COM: 1',  # Samsung specific
                    'X-DLNADOC: DMS-1.50',  # DLNA version for Samsung
                    'X-DLNACAP: av-upload,image-upload,audio-upload',
                    '',
                    ''
                ])

                interfaces = self.get_all_interfaces()
                for interface_ip in interfaces:
                    try:
                        self.socket.sendto(notify_msg.encode('utf-8'), (SSDP_ADDR, SSDP_PORT))
                        self.logger.info(f"Sent {nts_type} notification for service [{service}] via interface [{interface_ip}]")
                    except Exception as e:
                        self.logger.warning(f"Failed to send {nts_type} on interface {interface_ip}: {e}")

            except Exception as e:
                self.logger.error(f"Failed to send {nts_type} notification for service {service}: {str(e)}")
                continue

    def send_alive_notification(self):
        self.announcement_count += 1
        self.send_notification('ssdp:alive')
        
        # Log metrics every 10 announcements
        if self.announcement_count % 10 == 0:
            avg_interval = sum(self.metrics['announcements']) / len(self.metrics['announcements']) if self.metrics['announcements'] else 0
            self.logger.info(f"SSDP Metrics - Filtered: {self.metrics['filtered_msgs']}, Avg Interval: {avg_interval:.1f}s")

    def send_byebye_notification(self):
        self.send_notification('ssdp:byebye')

    def periodic_announce(self):
        """Periodically send presence announcements with optimized timing"""
        try:
            # Initial burst for quick discovery (2 announcements, 500ms apart)
            for _ in range(2):
                self.send_alive_notification()
                time.sleep(0.5)

            # Use exponential backoff for announcements
            announcement_interval = 60  # Start with 1 minute
            max_interval = 1800  # Max 30 minutes
            last_announcement = time.time()

            while self.running:
                current_time = time.time()
                if current_time - last_announcement >= announcement_interval:
                    self.send_alive_notification()
                    last_announcement = current_time
                    # Increase interval up to max
                    announcement_interval = min(announcement_interval * 2, max_interval)

                # Sleep in smaller chunks (5 seconds) to allow clean shutdown
                for _ in range(10):  # 10 * 0.5s = 5s chunks
                    if not self.running:
                        break
                    time.sleep(0.5)

        except Exception as e:
            self.logger.error(f"Error in periodic announce: {str(e)}")
            if self.running:
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
            # Create main multicast listening socket
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            # Allow reuse of the address, crucial for multicast and coexistence
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            
            # Set optimal buffer sizes
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 262144)  # 256KB receive buffer
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 262144)  # 256KB send buffer
            
            # Set socket timeouts
            self.socket.settimeout(1.0)  # 1 second timeout for receiving
            
            # Bind to the SSDP port on all available interfaces
            try:
                self.socket.bind(('', SSDP_PORT))
                self.logger.info(f"Successfully bound listening socket to ('', {SSDP_PORT})")
            except socket.error as e:
                if e.errno == errno.WSAEADDRINUSE:
                    self.logger.warning(f"SSDP port {SSDP_PORT} is already in use (Windows Discovery Service). Attempting shared mode.")
                else:
                    self.logger.error(f"Failed to bind listening socket: {e}")
                    self.cleanup_sockets()
                    return False

            self.logger.info(f"SSDP listening socket initialized.")
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
        """Join multicast group on all interfaces"""
        joined_any = False
        interfaces = self.get_all_interfaces()
        if not interfaces:
            self.logger.error("No network interfaces found to join multicast group.")
            return False

        for addr in interfaces:
            try:
                # Construct the multicast request structure
                mreq = socket.inet_aton(SSDP_ADDR) + socket.inet_aton(addr)
                # Join the multicast group for the listening socket
                self.socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
                self.logger.info(f"Successfully joined SSDP multicast group on interface: {addr}")
                joined_any = True
            except socket.error as e:
                # Common errors: WSAENOBUFS (no buffer space), EADDRNOTAVAIL (bad interface IP)
                self.logger.warning(f"Failed to join multicast group on interface {addr}: {e}")
            except Exception as e:
                 self.logger.warning(f"Unexpected error joining multicast group on interface {addr}: {e}")

        if not joined_any:
             self.logger.error("Failed to join multicast group on ANY interface.")
             return False

        self.logger.info("Successfully joined multicast group on at least one interface.")
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
        """Handle incoming SSDP requests with enhanced filtering and metrics"""
        try:
            start_time = time.time()
            self.metrics['performance']['total_messages'] += 1
            
            # Track unique clients
            self.known_clients.add(addr[0])
            
            # Enhanced source filtering - only do this once
            if addr[0] == self.get_local_ip():
                self.metrics['filtered_msgs'] += 1
                self.logger.debug(f"Filtered own SSDP message from {addr[0]}")
                return
            
            # Track timing metrics
            current_time = time.time()
            if self.last_announcement_time:
                interval = current_time - self.last_announcement_time
                self.metrics['announcements'].append(interval)
                # Keep only last 100 measurements
                if len(self.metrics['announcements']) > 100:
                    self.metrics['announcements'].pop(0)
            
            self.last_announcement_time = current_time
            self.validate_announcement_timing(current_time)

            request_line, *header_lines = data.decode('utf-8', errors='ignore').split('\r\n')
            self.logger.info(f"Received SSDP request from {addr[0]}:{addr[1]}: {request_line}")
            # self.logger.debug(f"Full request data: {data!r}") # Log raw data if needed

            if 'M-SEARCH * HTTP/1.1' in request_line:
                headers = {}
                for line in header_lines:
                    if ': ' in line:
                        key, value = line.split(': ', 1)
                        headers[key.upper()] = value.strip()

                st = headers.get('ST', '')
                man = headers.get('MAN', '') # Mandatory header for discovery

                if not st or man != '"ssdp:discover"':
                    self.logger.debug(f"Ignoring M-SEARCH from {addr}: Missing ST or invalid MAN header. ST='{st}', MAN='{man}'")
                    return

                self.logger.debug(f"M-SEARCH from {addr} for ST: {st}")

                # Check if the search target matches our services
                matching_services = [
                    'upnp:rootdevice',
                    f'uuid:{DEVICE_UUID}',
                    'urn:schemas-upnp-org:device:MediaServer:1',
                    'urn:schemas-upnp-org:service:ContentDirectory:1',
                    'urn:schemas-upnp-org:service:ConnectionManager:1',
                    'urn:schemas-upnp-org:service:AVTransport:1'
                ]

                respond = False
                if st == 'ssdp:all':
                    respond = True
                    response_st = 'upnp:rootdevice' # Respond with root device for ssdp:all
                elif st in matching_services:
                    respond = True
                    response_st = st # Respond with the specific ST requested

                if respond:
                    # Add a small random delay to avoid flooding (RFC requirement)
                    time.sleep(random.uniform(0.1, 0.5))
                    self.send_discovery_response(addr, response_st)
                else:
                    self.logger.debug(f"Ignoring M-SEARCH from {addr}: ST '{st}' does not match our services.")

            # Record response time
            response_time = time.time() - start_time
            self.metrics['performance']['response_times'].append(response_time)

        except UnicodeDecodeError as e:
            self.logger.warning(f"Received malformed SSDP request (UnicodeDecodeError) from {addr[0]}:{addr[1]}: {e}")
        except Exception as e:
            self.metrics['performance']['error_count'] += 1
            self.logger.error(f"Error handling SSDP request from {addr[0]}:{addr[1]}: {e}", exc_info=True)

    def send_discovery_response(self, addr, st):
        """Send SSDP discovery response with optimized timing"""
        try:
            self.discovery_count += 1
            self.logger.info(f"Sending discovery response for ST '{st}' to {addr[0]}:{addr[1]} (Count: {self.discovery_count})")

            location = f'http://{self.http_server_address[0]}:{self.http_server_address[1]}/description.xml'
            usn = f'uuid:{DEVICE_UUID}'
            if st != 'upnp:rootdevice' and not st.startswith('uuid:'):
                 usn += f'::{st}'

            # Enhanced response with more detailed headers
            response = '\r\n'.join([
                'HTTP/1.1 200 OK',
                'CACHE-CONTROL: max-age=1800',
                'DATE: ' + time.strftime('%a, %d %b %Y %H:%M:%S GMT', time.gmtime()),
                'EXT:', # Required header
                f'LOCATION: {location}',
                'SERVER: Windows/10 UPnP/1.0 Python-DLNA/1.0',
                f'ST: {st}',
                f'USN: {usn}',
                'BOOTID.UPNP.ORG: 1',
                'CONFIGID.UPNP.ORG: 1',
                'X-DLNADOC: DMS-1.50',
                'X-DLNACAP: av-upload,image-upload,audio-upload',
                '', # Empty line required by HTTP
                ''
            ])

            # Minimal delay based on discovery count to prevent flooding
            delay = min(0.1, 0.02 * self.discovery_count)  # Max 100ms delay
            time.sleep(delay)

            # Send response immediately for the first few discoveries
            if self.discovery_count <= 3:
                self.error_handler.with_retry(
                    lambda: self.socket.sendto(response.encode('utf-8'), addr)
                )
                self.logger.debug(f"Initial discovery response sent immediately to {addr[0]}:{addr[1]}")
            else:
                # For subsequent discoveries, use a small random delay
                time.sleep(random.uniform(0.01, 0.1))
                self.error_handler.with_retry(
                    lambda: self.socket.sendto(response.encode('utf-8'), addr)
                )
                self.logger.debug(f"Discovery response sent with delay to {addr[0]}:{addr[1]}")

        except socket.error as sock_err:
             self.logger.error(f"Socket error sending discovery response to {addr[0]}:{addr[1]}: {sock_err}")
        except Exception as e:
            self.logger.error(f"Error preparing/sending discovery response to {addr[0]}:{addr[1]}: {e}", exc_info=True)

    def start(self):
        """Start SSDP server with error handling"""
        if not self.initialize_sockets():
            self.logger.error("Failed to initialize SSDP server sockets.")
            return

        if not self.join_multicast_group():
            self.logger.error("Failed to join SSDP multicast group.")
            self.cleanup_sockets()
            return

        self.running = True
        self.logger.info(f"SSDP server started, listening on port {SSDP_PORT}")

        # Start announcement thread
        self.announcement_thread = threading.Thread(target=self.periodic_announce, name="SSDPeriodicAnnounce")
        self.announcement_thread.daemon = True
        self.announcement_thread.start()

        # Send initial alive notification *after* starting listener and announcer
        try:
            # Give a slight delay for network stack to settle
            time.sleep(1)
            self.send_alive_notification()
        except Exception as e:
            self.logger.error(f"Failed to send initial alive announcement: {e}")

        while self.running:
            try:
                # Receive data using the main listening socket
                data, addr = self.socket.recvfrom(2048) # Increased buffer size
                if data:
                    # Handle request in a separate thread to avoid blocking listener?
                    # For now, handle directly. If performance becomes an issue, consider threading.
                    self.handle_request(data, addr)
            except socket.timeout:
                # Timeout is expected, just loop again to check self.running
                continue
            except socket.error as e:
                # Handle specific socket errors if needed
                if self.running: # Avoid logging errors during shutdown
                    # WSAECONNRESET might occur if client disconnects abruptly
                    if e.errno == errno.WSAECONNRESET:
                         self.logger.warning(f"Socket connection reset by peer: {e}")
                    else:
                         self.logger.error(f"Socket error during receive: {e}")
                    # Consider if recovery is needed/possible here
                    time.sleep(0.1) # Small delay before retrying receive
            except Exception as e:
                if self.running:
                    self.logger.error(f"Unexpected error in SSDP receive loop: {e}", exc_info=True)
                    time.sleep(0.1) # Small delay

        # Shutdown sequence
        self.logger.info("SSDP server stopping...")
        try:
            self.send_byebye_notification() # Send byebye before closing sockets
            time.sleep(1) # Allow time for byebye to propagate
        except Exception as e:
            self.logger.error(f"Failed to send byebye notification: {e}")
        finally:
            self.cleanup_sockets()
            self.logger.info("SSDP server stopped.")

class AVTransportService:
    """Handles media transport controls and playlist management"""
    def __init__(self):
        self.state = {
            'TransportState': 'STOPPED',  # PLAYING, PAUSED_PLAYBACK, STOPPED
            'CurrentURI': '',
            'CurrentTrack': 0,
            'NumberOfTracks': 0,
            'PlaybackSpeed': '1',
            'RelativeTimePosition': '00:00:00',
            'AbsoluteTimePosition': '00:00:00'
        }
        self.playlist = []
        self.current_media = None
        self._last_update = time.time()
        self.subscribers = set()

    def set_transport_uri(self, uri):
        """Set the URI of the media to be played"""
        self.state['CurrentURI'] = uri
        self.state['TransportState'] = 'STOPPED'
        self._notify_state_change()
        return True

    def play(self, speed='1'):
        """Start or resume playback"""
        if self.state['CurrentURI']:
            self.state['TransportState'] = 'PLAYING'
            self.state['PlaybackSpeed'] = speed
            self._notify_state_change()
            return True
        return False

    def pause(self):
        """Pause playback"""
        if self.state['TransportState'] == 'PLAYING':
            self.state['TransportState'] = 'PAUSED_PLAYBACK'
            self._notify_state_change()
            return True
        return False

    def stop(self):
        """Stop playback"""
        self.state['TransportState'] = 'STOPPED'
        self.state['RelativeTimePosition'] = '00:00:00'
        self._notify_state_change()
        return True

    def seek(self, target):
        """Seek to specific position"""
        if self.current_media:
            # Parse target format (time or track number)
            if ':' in target:  # Time format
                hours, minutes, seconds = map(int, target.split(':'))
                position_seconds = hours * 3600 + minutes * 60 + seconds
                self.state['RelativeTimePosition'] = target
                self._notify_state_change()
                return True
        return False

    def _notify_state_change(self):
        """Notify subscribers of state changes"""
        event_data = {
            'TransportState': self.state['TransportState'],
            'CurrentTrack': self.state['CurrentTrack'],
            'RelativeTime': self.state['RelativeTimePosition']
        }
        for subscriber in self.subscribers:
            try:
                self._send_event(subscriber, event_data)
            except Exception as e:
                self.subscribers.remove(subscriber)
                logging.error(f"Failed to notify subscriber {subscriber}: {e}")

    def _send_event(self, subscriber, data):
        """Send event notification to subscriber"""
        # Implementation will use HTTP NOTIFY
        pass

class ContentDirectorySearch:
    """Handles indexed search functionality for the Content Directory Service"""
    def __init__(self, media_folders):
        self.logger = logging.getLogger('DLNAServer')
        self.media_folders = media_folders
        self.index = {}
        self.metadata_cache = {}
        self._build_index()

    def _build_index(self):
        """Build search index from media folders"""
        try:
            from whoosh.fields import Schema, TEXT, ID, STORED
            from whoosh.analysis import StandardAnalyzer
            from whoosh.index import create_in, exists_in, open_dir
            import os.path
            
            # Create schema for media indexing
            self.schema = Schema(
                path=ID(stored=True),
                filename=TEXT(stored=True, analyzer=StandardAnalyzer()),
                title=TEXT(stored=True),
                artist=TEXT(stored=True),
                album=TEXT(stored=True),
                genre=TEXT(stored=True),
                type=TEXT(stored=True)
            )
            
            # Create or open index
            index_dir = os.path.join(os.path.dirname(__file__), 'search_index')
            if not os.path.exists(index_dir):
                os.makedirs(index_dir)
                
            if exists_in(index_dir):
                self.ix = open_dir(index_dir)
            else:
                self.ix = create_in(index_dir, self.schema)
            
            # Index all media files
            writer = self.ix.writer()
            
            for folder in self.media_folders:
                for root, _, files in os.walk(folder):
                    for file in files:
                        try:
                            ext = os.path.splitext(file)[1].lower()
                            if ext in VIDEO_EXTENSIONS or ext in AUDIO_EXTENSIONS or ext in IMAGE_EXTENSIONS:
                                full_path = os.path.join(root, file)
                                metadata = self._extract_metadata(full_path)
                                
                                # Store in index
                                writer.add_document(
                                    path=full_path,
                                    filename=file,
                                    title=metadata.get('title', file),
                                    artist=metadata.get('artist', ''),
                                    album=metadata.get('album', ''),
                                    genre=metadata.get('genre', ''),
                                    type=self._get_media_type(ext)
                                )
                                
                                # Cache metadata
                                self.metadata_cache[full_path] = metadata
                                
                        except Exception as e:
                            self.logger.warning(f"Error indexing {file}: {e}")
                            
            writer.commit()
            self.logger.info("Search index built successfully")
            
        except ImportError:
            self.logger.warning("Whoosh not installed, falling back to basic search")
            self._build_basic_index()

    def _build_basic_index(self):
        """Build a simple in-memory index when Whoosh is not available"""
        for folder in self.media_folders:
            for root, _, files in os.walk(folder):
                for file in files:
                    ext = os.path.splitext(file)[1].lower()
                    if ext in VIDEO_EXTENSIONS or ext in AUDIO_EXTENSIONS or ext in IMAGE_EXTENSIONS:
                        full_path = os.path.join(root, file)
                        self.index[full_path] = {
                            'filename': file,
                            'type': self._get_media_type(ext),
                            'metadata': self._extract_metadata(full_path)
                        }

    def search(self, query, media_type=None, limit=50):
        """Search for media items matching query"""
        try:
            from whoosh.qparser import MultifieldParser
            from whoosh.query import Term
            
            with self.ix.searcher() as searcher:
                query_fields = ['filename', 'title', 'artist', 'album', 'genre']
                parser = MultifieldParser(query_fields, self.schema)
                q = parser.parse(query)
                
                # Add media type filter if specified
                if media_type:
                    q = q & Term('type', media_type)
                
                results = searcher.search(q, limit=limit)
                return [(hit['path'], hit.score, hit['title']) for hit in results]
                
        except ImportError:
            return self._basic_search(query, media_type, limit)

    def _basic_search(self, query, media_type=None, limit=50):
        """Basic search implementation without Whoosh"""
        query = query.lower()
        results = []
        
        for path, info in self.index.items():
            if media_type and info['type'] != media_type:
                continue
                
            score = 0
            metadata = info['metadata']
            
            # Check filename
            if query in info['filename'].lower():
                score += 1
                
            # Check metadata
            for field in ['title', 'artist', 'album', 'genre']:
                if field in metadata and query in metadata[field].lower():
                    score += 1
                    
            if score > 0:
                results.append((path, score, metadata.get('title', info['filename'])))
                
        # Sort by score and limit results
        results.sort(key=lambda x: x[1], reverse=True)
        return results[:limit]

    def _extract_metadata(self, file_path):
        """Extract metadata from media file"""
        try:
            from mutagen import File
            metadata = {}
            media_file = File(file_path)
            
            if media_file is not None:
                if hasattr(media_file, 'tags'):
                    tags = media_file.tags
                    if tags:
                        metadata['title'] = str(tags.get('title', [''])[0])
                        metadata['artist'] = str(tags.get('artist', [''])[0])
                        metadata['album'] = str(tags.get('album', [''])[0])
                        metadata['genre'] = str(tags.get('genre', [''])[0])
                        
                if hasattr(media_file.info, 'length'):
                    metadata['duration'] = int(media_file.info.length)
                    
            return metadata
            
        except Exception as e:
            self.logger.debug(f"Error extracting metadata from {file_path}: {e}")
            return {}

    def _get_media_type(self, ext):
        """Get media type from file extension"""
        if ext in VIDEO_EXTENSIONS:
            return 'video'
        elif ext in AUDIO_EXTENSIONS:
            return 'audio'
        elif ext in IMAGE_EXTENSIONS:
            return 'image'
        return 'unknown'

class ResourceMonitor:
    """Monitors system resources used by the DLNA server"""
    def __init__(self, logger):
        self.logger = logger
        self.metrics = {
            'cpu_usage': [],
            'memory_usage': [],
            'network_stats': {
                'bytes_sent': 0,
                'bytes_received': 0,
                'connections': 0
            },
            'active_streams': 0,
            'cache_stats': {
                'thumbnail_hits': 0,
                'thumbnail_misses': 0,
                'metadata_hits': 0,
                'metadata_misses': 0
            }
        }
        self.last_cpu_check = time.time()
        self.last_metric_log = time.time()
        self._start_monitoring()

    def _start_monitoring(self):
        """Start resource monitoring in background thread with optimized sampling"""
        try:
            import psutil
            self.process = psutil.Process()
            
            def monitor_loop():
                while True:
                    try:
                        current_time = time.time()
                        
                        # Only sample CPU every 5 seconds to reduce overhead
                        if current_time - self.last_cpu_check >= 5:
                            cpu_percent = self.process.cpu_percent(interval=0.1)  # Reduced interval
                            self.metrics['cpu_usage'].append(cpu_percent)
                            if len(self.metrics['cpu_usage']) > 12:  # Keep last minute
                                self.metrics['cpu_usage'].pop(0)
                            self.last_cpu_check = current_time

                        # Sample memory usage
                        memory_info = self.process.memory_info()
                        memory_mb = memory_info.rss / (1024 * 1024)
                        self.metrics['memory_usage'].append(memory_mb)
                        if len(self.metrics['memory_usage']) > 12:
                            self.metrics['memory_usage'].pop(0)
                        
                        # Log metrics every minute
                        if current_time - self.last_metric_log >= 60:
                            avg_cpu = sum(self.metrics['cpu_usage']) / len(self.metrics['cpu_usage']) if self.metrics['cpu_usage'] else 0
                            avg_mem = sum(self.metrics['memory_usage']) / len(self.metrics['memory_usage']) if self.metrics['memory_usage'] else 0
                            
                            self.logger.info(
                                f"Resource Usage - CPU: {avg_cpu:.1f}%, Memory: {avg_mem:.1f}MB, "
                                f"Streams: {self.metrics['active_streams']}, "
                                f"Cache Hits: {self.metrics['cache_stats']['thumbnail_hits'] + self.metrics['cache_stats']['metadata_hits']}"
                            )
                            self.last_metric_log = current_time
                        
                        time.sleep(5)  # Reduced polling frequency
                        
                    except Exception as e:
                        self.logger.error(f"Error in resource monitoring: {e}")
                        time.sleep(5)

            threading.Thread(target=monitor_loop, daemon=True, name="ResourceMonitor").start()
            self.logger.info("Resource monitoring started")
            
        except ImportError:
            self.logger.warning("psutil not installed, resource monitoring disabled")

    def track_stream(self, started=True):
        """Track active media streams"""
        if started:
            self.metrics['active_streams'] += 1
        else:
            self.metrics['active_streams'] = max(0, self.metrics['active_streams'] - 1)

    def track_network(self, bytes_sent=0, bytes_received=0):
        """Track network usage"""
        self.metrics['network_stats']['bytes_sent'] += bytes_sent
        self.metrics['network_stats']['bytes_received'] += bytes_received

    def track_cache(self, cache_type, hit=True):
        """Track cache hits/misses"""
        if hit:
            self.metrics['cache_stats'][f'{cache_type}_hits'] += 1
        else:
            self.metrics['cache_stats'][f'{cache_type}_misses'] += 1

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
    # Add thumbnail cache as class variable
    thumbnail_cache = {}
    thumbnail_cache_size = 100  # Maximum number of cached thumbnails

    def __init__(self, request, client_address, server):
        # Initialize logger first
        self.logger = logging.getLogger('DLNAServer')
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
        self.xml_generator = DLNAXMLGenerator()
        self.request_parser = DLNARequestParser()
        self.error_handler = DLNAErrorHandler(self.logger)

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

    def do_GET(self):
        """Handle GET requests with proper logging"""
        try:
            if self.path == '/description.xml':
                self.send_device_description()
            elif self.path == '/ContentDirectory.xml':
                self.send_content_directory()
            elif self.path == '/ConnectionManager.xml':
                self.send_connection_manager()
            elif self.path == '/AVTransport.xml':
                self.send_av_transport()
            elif self.path.startswith('/media/'):
                # Strip /media/ from path and serve the file
                file_path = unquote(self.path[7:])
                self.serve_media_file(file_path)
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
            # Parse the path properly
            parsed_path = urlparse(self.path)
            clean_path = parsed_path.path  # This removes any http:// prefixes

            # Content type and size check for media files
            if not clean_path.endswith('.xml'):
                file_path = unquote(clean_path)
                abs_path = self.get_file_path(file_path)
                
                if abs_path:
                    ext = os.path.splitext(abs_path)[1].lower()
                    content_type = VIDEO_EXTENSIONS.get(ext) or AUDIO_EXTENSIONS.get(ext) or IMAGE_EXTENSIONS.get(ext)
                    
                    if content_type:
                        self.send_response(200)
                        self.send_header('Content-Type', content_type)
                        self.send_header('Content-Length', str(os.path.getsize(abs_path)))
                        self.send_header('transferMode.dlna.org', 'Streaming')
                        self.send_header('contentFeatures.dlna.org', 'DLNA.ORG_OP=01;DLNA.ORG_CI=0')
                        self.end_headers()
                        return

            # Handle descriptor files
            if clean_path == '/description.xml':
                self.send_response(200)
                self.send_header('Content-Type', 'text/xml; charset="utf-8"')
                self.end_headers()
            elif clean_path in ['/ContentDirectory.xml', '/ConnectionManager.xml', '/AVTransport.xml']:
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
        if self.path == '/ContentDirectory/control':
            self.handle_content_directory_control()
        else:
            self.send_error(404)

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

    def handle_content_directory_control(self):
        """Handle POST requests to /ContentDirectory/control with improved XML handling and browsing logic"""
        try:
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            self.logger.info(f"Received ContentDirectory control request (length: {content_length})")
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
                    <Result>{result_didl}</Result>
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
                self.error_handler.handle_request_error(self, ve, 400)

        except Exception as e:
            self.error_handler.handle_request_error(self, e)

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
                        'parentID': '0',  # Changed from -1 to 0
                        'restricted': '1',
                        'searchable': '1',
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
                    return result, 1, 1

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
        """Send DLNA Content Directory XML with Samsung compatibility"""
        try:
            content_directory_xml = '''<?xml version="1.0" encoding="utf-8"?>
<scpd xmlns="urn:schemas-upnp-org:service-1-0">
    <specVersion>
        <major>1</major>
        <minor>0</minor>
    </specVersion>
    <actionList>
        <!-- ...existing actions... -->
    </actionList>
    <serviceStateTable>
        <!-- ...existing state variables... -->
        <stateVariable sendEvents="no">
            <name>SortCapabilities</name>
            <dataType>string</dataType>
            <defaultValue>dc:title,dc:date,upnp:class</defaultValue>
        </stateVariable>
        <stateVariable sendEvents="no">
            <name>SearchCapabilities</name>
            <dataType>string</dataType>
            <defaultValue>dc:title,dc:creator,upnp:class</defaultValue>
        </stateVariable>
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
        """Send DLNA Connection Manager XML"""
        try:
            # Create SOAP envelope with connection manager info
            connection_manager_xml = '''<?xml version="1.0" encoding="utf-8" standalone="yes"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">
  <s:Body>
    <u:GetProtocolInfoResponse xmlns:u="urn:schemas-upnp-org:service:ConnectionManager:1">
      <Source>http-get:*:image/jpeg:DLNA.ORG_PN=JPEG_LRG,http-get:*:audio/mpeg:DLNA.ORG_PN=MP3,http-get:*:video/mp4:DLNA.ORG_PN=AVC_MP4_HP_HD_AAC</Source>
      <Sink></Sink>
      <CurrentConnectionIDs>0</CurrentConnectionIDs>
    </u:GetProtocolInfoResponse>
  </s:Body>
</s:Envelope>'''

            self.send_response(200)
            self.send_header('Content-Type', 'text/xml; charset="utf-8"')
            self.send_header('Ext', '') # Required by UPnP spec
            self.send_header('Server', 'Windows/10.0 UPnP/1.0 Python-DLNA/1.0')
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

    def handle_thumbnail_request(self, file_path, is_video=False):
        """Generate and serve thumbnails for videos and images with caching"""
        try:
            # Check cache first
            cache_key = f"{file_path}_{is_video}"
            cached_thumb = self.thumbnail_cache.get(cache_key)
            
            if cached_thumb:
                self.send_response(200)
                self.send_header('Content-Type', 'image/jpeg')
                self.send_header('Content-Length', str(len(cached_thumb)))
                self.send_header('Cache-Control', 'max-age=3600')  # Cache for 1 hour
                self.end_headers()
                self.wfile.write(cached_thumb)
                return

            from PIL import Image
            import io
            
            if is_video:
                try:
                    import ffmpeg
                    out, _ = (
                        ffmpeg
                        .input(file_path, ss="00:00:01")
                        .filter('scale', 320, -1)
                        .output('pipe:', vframes=1, format='image2', vcodec='mjpeg')
                        .run(capture_stdout=True, capture_stderr=True)
                    )
                    image = Image.open(io.BytesIO(out))
                except Exception as e:
                    self.logger.error(f"Error generating video thumbnail: {e}")
                    self.send_error(500, "Could not generate thumbnail")
                    return
            else:
                image = Image.open(file_path)
            
            image.thumbnail((320, 320), Image.Resampling.LANCZOS)
            
            if image.mode in ('RGBA', 'LA'):
                background = Image.new('RGB', image.size, (255, 255, 255))
                background.paste(image, mask=image.split()[-1])
                image = background
            elif image.mode != 'RGB':
                image = image.convert('RGB')
                
            thumb_io = io.BytesIO()
            image.save(thumb_io, 'JPEG', quality=85)
            thumb_data = thumb_io.getvalue()
            
            # Cache the thumbnail
            if len(self.thumbnail_cache) >= self.thumbnail_cache_size:
                # Remove oldest item if cache is full
                self.thumbnail_cache.pop(next(iter(self.thumbnail_cache)))
            self.thumbnail_cache[cache_key] = thumb_data
            
            self.send_response(200)
            self.send_header('Content-Type', 'image/jpeg')
            self.send_header('Content-Length', str(len(thumb_data)))
            self.send_header('Cache-Control', 'max-age=3600')
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
    try:
        with open(config_path, 'r') as config_file:
            config = json.load(config_file)
            return config
    except FileNotFoundError:
        print(f"Configuration file not found: {config_path}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error parsing configuration file: {e}")
        sys.exit(1)

# Add file indexing and compatibility checks

def index_files(media_folders):
    supported_extensions = VIDEO_EXTENSIONS.keys() | AUDIO_EXTENSIONS.keys() | IMAGE_EXTENSIONS.keys()
    indexed_files = {}
    log_file = Path('logs/non_compatible_files.log')
    log_file.parent.mkdir(exist_ok=True, parents=True)

    with log_file.open('w') as log:
        for folder in media_folders:
            for root, _, files in os.walk(folder):
                relative_root = os.path.relpath(root, folder)
                indexed_files[relative_root] = []
                for file in files:
                    ext = os.path.splitext(file)[1].lower()
                    if ext in supported_extensions:
                        indexed_files[relative_root].append(file)
                    else:
                        log.write(f"Unsupported file: {os.path.join(root, file)}\n")

    return indexed_files

# Update start_server to include file indexing

def start_server(config):
    """Start the DLNA media server with Windows compatibility"""
    logger = setup_logging()

    media_folders = config.get('shared_paths', [])
    if not media_folders:
        logger.error("No shared paths specified in configuration.")
        sys.exit(1)

    for folder in media_folders:
        if not os.path.exists(folder):
            logger.error(f"Media folder does not exist: {folder}")
            sys.exit(1)

    indexed_files = index_files(media_folders)
    logger.info(f"Indexed files: {indexed_files}")

    local_ip = NetworkUtils.get_local_ip()
    
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

    server.media_folders = media_folders
    server.indexed_files = indexed_files
    server.av_transport = AVTransportService()  # Initialize AVTransport service
    server.content_search = ContentDirectorySearch(media_folders)  # Initialize Content Directory Search
    server.resource_monitor = ResourceMonitor(logger)  # Initialize resource monitor
    
    # Start SSDP server in a separate thread
    ssdp_server = SSDPServer((local_ip, port)) # Pass logger if needed
    ssdp_thread = threading.Thread(target=ssdp_server.start, name="SSDPServerThread")
    ssdp_thread.daemon = True
    ssdp_thread.start()

    try:
        logger.info(f"DLNA server started at http://{local_ip}:{port}")
        logger.info(f"Serving media from: {', '.join(server.media_folders)}") # Access via server instance
        logger.info("Press Ctrl+C to stop the server")
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("\nShutting down server...")
        # Signal SSDP server to stop and send byebye
        ssdp_server.running = False
        # Wait briefly for SSDP thread to potentially send byebye and clean up
        ssdp_thread.join(timeout=2.0) # Increased timeout slightly
        # Close HTTP server
        server.server_close()
        logger.info("HTTP server closed.")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"Critical error in main server loop: {e}", exc_info=True)
        # Attempt graceful shutdown even on critical error
        ssdp_server.running = False
        ssdp_thread.join(timeout=2.0)
        server.server_close()
        sys.exit(1)

if __name__ == "__main__":
    config = load_config()
    start_server(config)
