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
import json
import argparse
import re
import random

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

    def send_notification(self, nts_type):
        """Send SSDP notification (alive or byebye) with enhanced Samsung compatibility"""
        services = [
            'upnp:rootdevice',
            f'uuid:{DEVICE_UUID}', # Add device UDN itself
            'urn:schemas-upnp-org:device:MediaServer:1',
            'urn:schemas-upnp-org:service:ContentDirectory:1',
            'urn:schemas-upnp-org:service:ConnectionManager:1',
            'urn:schemas-upnp-org:service:AVTransport:1' # Added AVTransport
        ]

        location = f'http://{self.http_server_address[0]}:{self.http_server_address[1]}/description.xml'

        for service in services:
            usn = f'uuid:{DEVICE_UUID}'
            if service != 'upnp:rootdevice' and not service.startswith('uuid:'):
                 usn += f'::{service}' # Correct USN format

            try:
                notify_msg = '\r\n'.join([
                    'NOTIFY * HTTP/1.1',
                    f'HOST: {SSDP_ADDR}:{SSDP_PORT}',
                    'CACHE-CONTROL: max-age=1800' if nts_type == 'ssdp:alive' else '', # Only for alive
                    f'LOCATION: {location}',
                    f'NT: {service}',
                    f'NTS: {nts_type}',
                    'SERVER: Windows/10 UPnP/1.0 Python-DLNA/1.0', # Simplified Server header
                    f'USN: {usn}',
                    'BOOTID.UPNP.ORG: 1', # Keep BootID
                    'CONFIGID.UPNP.ORG: 1', # Keep ConfigID
                    # Samsung-specific headers (optional but potentially helpful)
                    'X-DLNADOC: DMS-1.50',
                    'X-DLNACAP: av-upload,image-upload,audio-upload', # Capabilities
                    'Content-Length: 0',
                    '',
                    ''
                ]).strip() # Remove empty lines if CACHE-CONTROL is absent

                # Send notification using the announce socket bound to a specific interface
                interfaces = self.get_all_interfaces()
                for interface_ip in interfaces:
                    try:
                        # Create a temporary socket for sending on this interface
                        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP) as send_sock:
                            send_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                            # Try binding to the interface IP - might fail if port is in use, but that's okay for sending
                            try:
                                send_sock.bind((interface_ip, 0)) # Bind to ephemeral port
                            except socket.error as bind_err:
                                self.logger.debug(f"Could not bind send socket to {interface_ip}: {bind_err}")
                                # Continue anyway, OS might route correctly

                            # Set TTL
                            send_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 4)
                            # Set outgoing interface
                            send_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(interface_ip))

                            self.error_handler.with_retry(
                                lambda: send_sock.sendto(notify_msg.encode('utf-8'), (SSDP_ADDR, SSDP_PORT))
                            )
                            self.logger.info(f"Sent {nts_type} notification for service ['{service}'] via interface ['{interface_ip}']")

                    except socket.error as sock_err:
                         # Log specific socket errors during sending
                         self.logger.warning(f"Socket error sending {nts_type} on {interface_ip} for {service}: {sock_err}")
                    except Exception as e:
                        self.logger.warning(f"Failed to send {nts_type} notification on interface {interface_ip} for {service}: {str(e)}")

            except Exception as e:
                self.logger.error(f"Failed to prepare/send {nts_type} notification for service {service}: {str(e)}")
                continue

    def send_alive_notification(self):
        self.send_notification('ssdp:alive')

    def send_byebye_notification(self):
        self.send_notification('ssdp:byebye')

    def periodic_announce(self):
        """Periodically send presence announcements with optimized timing"""
        initial_interval = 0.5  # Start with more frequent announcements (was 1)
        max_interval = 2      # Maximum interval between announcements (was 5)
        burst_count = 3       # Number of initial rapid announcements
        
        # Initial burst of announcements
        for _ in range(burst_count):
            try:
                self.error_handler.with_retry(self.send_alive_notification)
                time.sleep(0.25)  # Short delay between initial announcements
            except Exception as e:
                self.logger.error(f"Error in initial announce burst: {str(e)}")

        current_interval = initial_interval
        while self.running:
            try:
                self.error_handler.with_retry(self.send_alive_notification)
                
                # Sleep with interruption check
                for _ in range(int(current_interval * 4)):  # *4 because we use 0.25s sleep chunks
                    if not self.running:
                        break
                    time.sleep(0.25)  # Use shorter sleep intervals to allow faster shutdown
                
                # Gradually increase interval up to max_interval
                if current_interval < max_interval:
                    current_interval = min(current_interval * 1.2, max_interval)
                    
            except Exception as e:
                self.logger.error(f"Error in periodic announce: {str(e)}")
                time.sleep(0.25)  # Prevent tight loop on error

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

            # Bind to the SSDP port on all available interfaces
            try:
                self.socket.bind(('', SSDP_PORT))
                self.logger.info(f"Successfully bound listening socket to ('', {SSDP_PORT})")
            except socket.error as e:
                # EADDRINUSE (10048 on Windows) is expected if another service (like Windows Discovery) is running
                if e.errno == errno.WSAEADDRINUSE or e.errno == errno.EADDRINUSE:
                    self.logger.warning(f"SSDP port {SSDP_PORT} is already in use (Likely Windows Discovery Service). Will attempt to listen in shared mode.")
                    # We can still often receive multicast packets even if binding fails with WSAEADDRINUSE on Windows when SO_REUSEADDR is set.
                else:
                    self.logger.error(f"Failed to bind listening socket to ('', {SSDP_PORT}): {e}")
                    self.cleanup_sockets()
                    return False

            # Set socket timeout for receiving
            self.socket.settimeout(1.0) # Timeout allows checking self.running periodically

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
        """Handle incoming SSDP requests with error handling"""
        try:
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

        except UnicodeDecodeError as e:
            self.logger.warning(f"Received malformed SSDP request (UnicodeDecodeError) from {addr[0]}:{addr[1]}: {e}")
        except Exception as e:
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

class DLNAServer(BaseHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        self.logger = logging.getLogger('DLNAServer')
        self.protocol_version = 'HTTP/1.1'
        self.timeout = 60  # Set timeout to 60 seconds
        self.headers_sent = False  # Track if headers have been sent
        super().__init__(*args, **kwargs)

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

    def do_HEAD(self):
        """Handle HEAD requests by performing the same logic as GET but without sending the body"""
        try:
            # Parse the path properly
            from urllib.parse import urlparse
            parsed_path = urlparse(self.path)
            clean_path = parsed_path.path  # This removes any http:// prefixes

            if clean_path == '/description.xml':
                self.send_response(200)
                self.send_header('Content-Type', 'text/xml; charset="utf-8"')
                self.send_header('Server', 'Python DLNA/1.0 UPnP/1.0')
                self.end_headers()
            elif clean_path == '/ContentDirectory.xml':
                self.send_response(200)
                self.send_header('Content-Type', 'application/xml')
                self.end_headers()
            elif clean_path == '/ConnectionManager.xml':
                self.send_response(200)
                self.send_header('Content-Type', 'application/xml')
                self.end_headers()
            elif clean_path == '/AVTransport.xml':
                self.send_response(200)
                self.send_header('Content-Type', 'text/xml; charset="utf-8"')
                self.end_headers()
            else:
                # For media files, we'll set up all the same headers we would for GET
                path = clean_path[1:]
                if path:
                    # Find the file path
                    abs_path = None
                    for shared_folder in self.server.media_folders:
                        potential_path = os.path.abspath(os.path.join(shared_folder, unquote(path)))
                        shared_folder_abs = os.path.abspath(shared_folder)
                        if os.path.commonpath([shared_folder_abs, potential_path]) == shared_folder_abs:
                            if os.path.exists(potential_path) and os.path.isfile(potential_path):
                                abs_path = potential_path
                                break

                    if abs_path is None:
                        self.send_error(404, "File not found")
                        return

                    # Check file type and set appropriate headers
                    ext = os.path.splitext(abs_path)[1].lower()
                    content_type = VIDEO_EXTENSIONS.get(ext) or AUDIO_EXTENSIONS.get(ext) or IMAGE_EXTENSIONS.get(ext)
                    if not content_type:
                        self.send_error(415, "Unsupported media type")
                        return

                    try:
                        file_size = os.path.getsize(abs_path)
                        self.send_response(200)
                        self.send_header("Content-Type", content_type)
                        self.send_header("Accept-Ranges", "bytes")
                        self.send_header("Content-Length", str(file_size))
                        self.send_header("Connection", "keep-alive")
                        # DLNA headers
                        self.send_header("transferMode.dlna.org", "Streaming")
                        self.send_header("contentFeatures.dlna.org", "DLNA.ORG_OP=01;DLNA.ORG_CI=0;DLNA.ORG_FLAGS=01700000000000000000000000000000")
                        self.end_headers()
                    except Exception as e:
                        self.logger.error(f"Error getting file info for HEAD request: {str(e)}")
                        self.send_error(500, "Internal server error")
                else:
                    self.send_error(404, "File not found")
        except Exception as e:
            self.logger.error(f"Error handling HEAD request: {str(e)}")
            self.send_error(500, "Internal server error")

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
        """Handle POST requests to /ContentDirectory/control with improved XML handling and browsing logic"""
        try:
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            self.logger.info(f"Received ContentDirectory control request (length: {content_length})")
            self.logger.debug(f"SOAP Request Body: {post_data.decode('utf-8', errors='ignore')}")

            # Parse the SOAP request robustly
            try:
                envelope = fromstring(post_data)
                # Namespace handling might be needed depending on client request format
                ns = {
                    's': 'http://schemas.xmlsoap.org/soap/envelope/',
                    'u': 'urn:schemas-upnp-org:service:ContentDirectory:1'
                }
                body = envelope.find('s:Body', ns)
                if body is None: # Try without namespace if first fails
                     body = envelope.find('{http://schemas.xmlsoap.org/soap/envelope/}Body')

                if body is None:
                     self.logger.error("Could not find SOAP Body in request")
                     self.send_error(400, "Invalid SOAP request: Missing Body")
                     return

                browse = body.find('u:Browse', ns)
                if browse is None: # Try without namespace
                     browse = body.find('{urn:schemas-upnp-org:service:ContentDirectory:1}Browse')

                if browse is None:
                     self.logger.error("Could not find Browse action in SOAP Body")
                     self.send_error(400, "Invalid SOAP request: Missing Browse action")
                     return

            except Exception as xml_err:
                self.logger.error(f"Error parsing SOAP XML: {xml_err}")
                self.send_error(400, "Invalid SOAP XML")
                return

            # Extract parameters safely
            object_id_elem = browse.find('ObjectID')
            browse_flag_elem = browse.find('BrowseFlag')
            # Optional parameters with defaults
            filter_elem = browse.find('Filter')
            starting_index_elem = browse.find('StartingIndex')
            requested_count_elem = browse.find('RequestedCount')
            sort_criteria_elem = browse.find('SortCriteria')

            object_id = object_id_elem.text if object_id_elem is not None else '0' # Default to root
            browse_flag = browse_flag_elem.text if browse_flag_elem is not None else 'BrowseDirectChildren'
            filter_str = filter_elem.text if filter_elem is not None else '*'
            starting_index = int(starting_index_elem.text) if starting_index_elem is not None and starting_index_elem.text.isdigit() else 0
            requested_count = int(requested_count_elem.text) if requested_count_elem is not None and requested_count_elem.text.isdigit() else 0 # 0 means all
            sort_criteria = sort_criteria_elem.text if sort_criteria_elem is not None else ''

            self.logger.info(f"Browse Request: ObjectID='{object_id}', BrowseFlag='{browse_flag}', StartIndex={starting_index}, Count={requested_count}, Filter='{filter_str}', Sort='{sort_criteria}'")

            # --- Browsing Logic ---
            result_didl, number_returned, total_matches = self.generate_browse_didl(
                object_id, browse_flag, starting_index, requested_count, filter_str, sort_criteria
            )
            # --- End Browsing Logic ---


            # Create SOAP response
            soap_response_xml = f'''<?xml version="1.0" encoding="utf-8"?>
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">
                <s:Body>
                    <u:BrowseResponse xmlns:u="urn:schemas-upnp-org:service:ContentDirectory:1">
                        <Result>{result_didl}</Result>
                        <NumberReturned>{number_returned}</NumberReturned>
                        <TotalMatches>{total_matches}</TotalMatches>
                        <UpdateID>1</UpdateID>
                    </u:BrowseResponse>
                </s:Body>
            </s:Envelope>'''

            self.send_response(200)
            self.send_header('Content-Type', 'text/xml; charset="utf-8"')
            self.send_header('Ext', '') # Required by UPnP spec for SOAP responses
            self.send_header('Server', 'Windows/10 UPnP/1.0 Python-DLNA/1.0')
            # Calculate content length based on bytes
            response_bytes = soap_response_xml.encode('utf-8')
            self.send_header('Content-Length', str(len(response_bytes)))
            self.end_headers()
            self.wfile.write(response_bytes)
            self.logger.info(f"Sent BrowseResponse for ObjectID '{object_id}' ({number_returned}/{total_matches} items)")

        except Exception as e:
            self.logger.error(f"Error handling content directory control: {e}", exc_info=True)
            # Avoid sending error if headers already sent
            if not self.headers_sent:
                 try:
                      self.send_error(500, "Internal server error processing Browse request")
                 except Exception as send_err:
                      self.logger.error(f"Could not send error response to client: {send_err}")

    def generate_browse_didl(self, object_id, browse_flag, starting_index, requested_count, filter_str, sort_criteria):
        """Generates the DIDL-Lite XML string and counts based on browse parameters."""
        root = Element('DIDL-Lite', {
            'xmlns': 'urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/',
            'xmlns:dc': 'http://purl.org/dc/elements/1.1/',
            'xmlns:upnp': 'urn:schemas-upnp-org:metadata-1-0/upnp/',
            'xmlns:dlna': 'urn:schemas-dlna-org:metadata-1-0'
        })

        items_list = []  # List to hold (path, is_directory) tuples

        # --- Determine Path and Parent ---
        if object_id == '0':
            if browse_flag == 'BrowseMetadata':
                # For root metadata, just add root container
                container = SubElement(root, 'container', {
                    'id': '0',
                    'parentID': '-1',
                    'restricted': '1',
                    'searchable': '1',
                    'childCount': str(len(self.server.media_folders))
                })
                SubElement(container, 'dc:title').text = "Root"
                SubElement(container, 'upnp:class').text = 'object.container'
                return self.encode_didl(root), 1, 1
            else:  # BrowseDirectChildren
                try:
                    if self.server.media_folders:
                        shared_folder = self.server.media_folders[0]  # Use first shared folder
                        entries = os.scandir(shared_folder)
                        for entry in entries:
                            if entry.is_file():
                                ext = os.path.splitext(entry.name)[1].lower()
                                if ext in VIDEO_EXTENSIONS or ext in AUDIO_EXTENSIONS or ext in IMAGE_EXTENSIONS:
                                    items_list.append((entry.path, False))
                                    self.logger.debug(f"Added file to list: {entry.path}")
                            elif entry.is_dir():
                                items_list.append((entry.path, True))
                                self.logger.debug(f"Added directory to list: {entry.path}")
                except OSError as e:
                    self.logger.error(f"Error listing directory {shared_folder}: {e}")
                    return self.encode_didl(root), 0, 0

        # --- Sort items (directories first, then by name) ---
        items_list.sort(key=lambda x: (not x[1], os.path.basename(x[0]).lower()))

        # --- Pagination ---
        total_matches = len(items_list)
        if requested_count == 0:  # 0 means all items
            paged_items = items_list[starting_index:]
        else:
            paged_items = items_list[starting_index:starting_index + requested_count]

        number_returned = len(paged_items)

        # --- Generate DIDL for paged items ---
        parent_id = '-1' if object_id == '0' else object_id
        for item_path, is_directory in paged_items:
            if is_directory:
                self.add_container_to_didl(root, item_path, os.path.basename(item_path), object_id)
            else:
                self.add_item_to_didl(root, item_path, os.path.basename(item_path), object_id)

        return self.encode_didl(root), number_returned, total_matches

    def encode_didl(self, root_element):
        """Encodes the ElementTree DIDL-Lite to a string suitable for SOAP response."""
        # Convert to string and escape XML special characters for embedding in SOAP
        xml_string = tostring(root_element, encoding='unicode')
        # Basic escaping for embedding in XML. More robust escaping might be needed.
        return xml_string.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

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

    def add_item_to_didl(self, root, path, title, parent_id):
        """Add an item (file) to the DIDL-Lite XML with improved metadata"""
        try:
            shared_root = self.find_shared_folder_root(path)
            if not shared_root:
                 self.logger.warning(f"Cannot determine relative path for item: {path}")
                 return # Skip item

            relative_path = os.path.relpath(path, shared_root)
            item_id = quote(relative_path.replace('\\', '/')) # Use forward slashes

            mime_type, upnp_class = self.get_mime_and_upnp_class(title)
            if upnp_class == 'object.item': # Skip if type couldn't be determined or isn't media
                 self.logger.debug(f"Skipping item with unknown/non-media type: {path}")
                 return

            item = SubElement(root, 'item', {
                'id': item_id,
                'parentID': parent_id,
                'restricted': '1'
            })

            SubElement(item, 'dc:title').text = title
            SubElement(item, 'upnp:class').text = upnp_class

            # Add resource element for main content
            res = SubElement(item, 'res')
            try:
                file_size = os.path.getsize(path)
            except OSError:
                file_size = 0

            # Main content URL
            url_path_part = quote(relative_path.replace('\\', '/'))
            url = f'http://{self.server.server_address[0]}:{self.server.server_address[1]}/{url_path_part}'
            res.text = url

            # Add protocol info with DLNA parameters
            protocol_info = f'http-get:*:{mime_type}:DLNA.ORG_OP=01;DLNA.ORG_CI=0;DLNA.ORG_FLAGS=01700000000000000000000000000000'
            res.set('protocolInfo', protocol_info)
            res.set('size', str(file_size))

            # Add media-specific metadata and thumbnails
            if upnp_class.startswith('object.item.audioItem'):
                self.add_audio_metadata(path, item)
                duration = self.get_media_duration(path)
                if duration:
                    res.set('duration', duration)
                # Try to get album art for audio files
                try:
                    audio = File(path)
                    if hasattr(audio, 'tags') and hasattr(audio.tags, 'getall'):
                        apic = audio.tags.getall('APIC') if hasattr(audio.tags, 'getall') else []
                        if apic:
                            # Add albumArtURI for embedded artwork
                            art_uri = SubElement(item, 'upnp:albumArtURI')
                            art_uri.set('dlna:profileID', 'JPEG_TN')
                            art_uri.text = f'{url}?albumArt=true'
                except Exception as e:
                    self.logger.debug(f"Could not extract album art: {e}")

            elif upnp_class.startswith('object.item.videoItem'):
                self.add_video_metadata(path, item)
                duration = self.get_media_duration(path)
                if duration:
                    res.set('duration', duration)
                # Add thumbnail resource for video
                thumbnail_res = SubElement(item, 'upnp:albumArtURI')
                thumbnail_res.set('dlna:profileID', 'JPEG_TN')
                thumbnail_res.text = f'{url}?thumbnail=true'

            elif upnp_class.startswith('object.item.imageItem'):
                self.add_image_metadata(path, item)
                resolution = self.get_image_resolution(path)
                if resolution:
                    res.set('resolution', resolution)
                # For images, add thumbnail version
                thumbnail_res = SubElement(item, 'upnp:albumArtURI')
                thumbnail_res.set('dlna:profileID', 'JPEG_TN')
                thumbnail_res.text = f'{url}?thumbnail=true'

            # Add modification date
            try:
                mod_time = datetime.fromtimestamp(os.path.getmtime(path))
                SubElement(item, 'dc:date').text = mod_time.isoformat()
            except OSError:
                pass

        except Exception as e:
            self.logger.error(f"Error adding item to DIDL for path {path}: {e}", exc_info=True)

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
        """Serve a media file with enhanced error handling and range request support."""
        try:
            # Decode the path component from the URL *once*
            decoded_path_segment = unquote(path)

            # Find the absolute path based on the decoded segment relative to shared folders
            abs_path = None
            shared_root_found = None
            for shared_folder in self.server.media_folders:
                potential_path = os.path.abspath(os.path.join(shared_folder, decoded_path_segment))
                # Security check
                shared_folder_abs = os.path.abspath(shared_folder)
                if os.path.commonpath([shared_folder_abs, potential_path]) == shared_folder_abs:
                     if os.path.exists(potential_path) and os.path.isfile(potential_path):
                          abs_path = potential_path
                          shared_root_found = shared_folder_abs
                          break

            if abs_path is None:
                self.logger.warning(f"Media file not found or invalid: requested='{path}', decoded='{decoded_path_segment}'")
                self.send_error(404, "File not found")
                return

            # Check if file type is supported (redundant check, but safe)
            ext = os.path.splitext(abs_path)[1].lower()
            content_type = VIDEO_EXTENSIONS.get(ext) or AUDIO_EXTENSIONS.get(ext) or IMAGE_EXTENSIONS.get(ext)
            if not content_type:
                self.logger.warning(f"Attempt to serve unsupported file type: {abs_path}")
                self.send_error(415, "Unsupported media type")
                return

            try:
                file_size = os.path.getsize(abs_path)
            except OSError as e:
                self.logger.error(f"Error getting file size for {abs_path}: {e}")
                self.send_error(500, "Internal server error")
                return

            # --- Range Request Handling ---
            range_header = self.headers.get('Range')
            start_byte = 0
            end_byte = file_size - 1
            is_range_request = False

            if range_header:
                self.logger.info(f"Range request received: {range_header}")
                range_match = re.match(r'bytes=(\d+)-(\d*)', range_header)
                if range_match:
                    start_byte = int(range_match.group(1))
                    end_byte_str = range_match.group(2)
                    if end_byte_str:
                        end_byte = int(end_byte_str)
                    # Ensure range is valid
                    if start_byte >= file_size or start_byte > end_byte:
                        self.logger.warning(f"Invalid range requested: {range_header}, size={file_size}")
                        self.send_response(416) # Range Not Satisfiable
                        self.send_header("Content-Range", f"bytes */{file_size}")
                        self.end_headers()
                        return
                    is_range_request = True
                    self.logger.info(f"Serving range: bytes {start_byte}-{end_byte}/{file_size}")
                else:
                    self.logger.warning(f"Malformed range header: {range_header}")
                    # Proceed with full file if range is malformed? Or send error?
                    # For simplicity, serve full file if range is invalid format.

            # --- Send Headers ---
            try:
                status_code = 206 if is_range_request else 200
                self.send_response(status_code)
                self.send_header("Content-Type", content_type)
                self.send_header("Connection", "keep-alive")
                self.send_header("Accept-Ranges", "bytes") # Indicate range support
                # DLNA headers
                self.send_header("transferMode.dlna.org", "Streaming")
                self.send_header("contentFeatures.dlna.org", "DLNA.ORG_OP=01;DLNA.ORG_CI=0") # OP=01 means range supported

                if is_range_request:
                    content_length = end_byte - start_byte + 1
                    self.send_header("Content-Length", str(content_length))
                    self.send_header("Content-Range", f"bytes {start_byte}-{end_byte}/{file_size}")
                else:
                    self.send_header("Content-Length", str(file_size))

                self.end_headers()
            except ConnectionAbortedError:
                self.logger.warning(f"Client connection aborted while sending headers for {abs_path}")
                return
            except Exception as e:
                self.logger.error(f"Error sending headers for {abs_path}: {e}")
                return # Cannot continue if headers failed

            # --- Send File Content ---
            try:
                with open(abs_path, 'rb') as f:
                    if is_range_request:
                        f.seek(start_byte)
                        bytes_to_send = end_byte - start_byte + 1
                    else:
                        bytes_to_send = file_size

                    sent_bytes = 0
                    chunk_size = 64 * 1024 # 64KB chunks
                    while bytes_to_send > 0:
                        read_size = min(chunk_size, bytes_to_send)
                        chunk = f.read(read_size)
                        if not chunk:
                            self.logger.warning(f"Unexpected EOF reading {abs_path} at offset {f.tell()}")
                            break # Unexpected end of file
                        try:
                            self.wfile.write(chunk)
                            sent_bytes += len(chunk)
                            bytes_to_send -= len(chunk)
                        except (ConnectionError, socket.error) as conn_err:
                            self.logger.warning(f"Connection error sending file {abs_path}: {conn_err}")
                            return # Stop sending if connection breaks
            except IOError as io_err:
                self.logger.error(f"IOError reading file {abs_path}: {io_err}")
                # Don't try to send error if headers already sent
            except Exception as e:
                 self.logger.error(f"Unexpected error sending file content for {abs_path}: {e}")
                 # Don't try to send error if headers already sent

            self.logger.debug(f"Finished sending {sent_bytes} bytes for {abs_path}")

        except ConnectionAbortedError:
            self.logger.warning(f"Client connection aborted while serving file {path}")
        except Exception as e:
            self.logger.error(f"Unhandled error serving file {path}: {e}", exc_info=True)
            if not self.headers_sent:
                try:
                    self.send_error(500, "Internal server error serving file")
                except Exception as send_err:
                    self.logger.error(f"Could not send error response to client: {send_err}")

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

    server.media_folders = media_folders
    server.indexed_files = indexed_files
    
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
