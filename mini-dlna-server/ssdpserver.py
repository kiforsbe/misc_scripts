import errno
import logging
import random
import socket
import threading
import time
from network_utils import NetworkUtils

SSDP_ADDR = '239.255.255.250'
SSDP_PORT = 1900

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
    def __init__(self, http_server_address, DEVICE_UUID, DEVICE_NAME):
        self.logger = logging.getLogger(__name__)
        self.http_server_address = http_server_address
        self.DEVICE_UUID = DEVICE_UUID
        self.DEVICE_NAME = DEVICE_NAME
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
            f'uuid:{self.DEVICE_UUID}',
            'urn:schemas-upnp-org:device:MediaServer:1',
            'urn:schemas-upnp-org:service:ContentDirectory:1',
            'urn:schemas-upnp-org:service:ConnectionManager:1',
            'urn:schemas-upnp-org:service:AVTransport:1'
        ]

        location = f'http://{self.http_server_address[0]}:{self.http_server_address[1]}/description.xml'

        for service in services:
            usn = f'uuid:{self.DEVICE_UUID}'
            if service != f'uuid:{self.DEVICE_UUID}':
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
                    f'uuid:{self.DEVICE_UUID}',
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
            usn = f'uuid:{self.DEVICE_UUID}'
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
