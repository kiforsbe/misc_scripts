import socket
import logging

class NetworkUtils:
    """Utility class for network operations"""
    _cached_local_ip = None
    logger = logging.getLogger(__name__)

    @classmethod
    def get_local_ip(cls, preferred_ip=None):
        """
        Get the local IP address with fallback options
        Args:
            preferred_ip: Optional preferred IP to use (e.g. from server config)
        Returns:
            str: Local IP address
        """
        # Return cached IP if available
        if cls._cached_local_ip:
            return cls._cached_local_ip

        # Try preferred IP first
        if preferred_ip and preferred_ip != '0.0.0.0':
            cls._cached_local_ip = preferred_ip
            return cls._cached_local_ip

        try:
            # Try connecting to public DNS to determine local IP
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                s.connect(('8.8.8.8', 80))
                cls._cached_local_ip = s.getsockname()[0]
                return cls._cached_local_ip
            finally:
                s.close()
        except Exception as e:
            cls.logger.warning(f"Error getting local IP via socket: {e}")
            
            try:
                # Fallback: Get hostname resolution
                cls._cached_local_ip = socket.gethostbyname(socket.gethostname())
                if not cls._cached_local_ip.startswith('127.'):
                    return cls._cached_local_ip
            except Exception as e:
                cls.logger.warning(f"Error getting local IP via hostname: {e}")

        # Final fallback
        cls._cached_local_ip = '127.0.0.1'
        return cls._cached_local_ip

    @classmethod
    def reset_cache(cls):
        """Reset the cached IP address"""
        cls._cached_local_ip = None
