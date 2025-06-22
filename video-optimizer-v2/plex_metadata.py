import sqlite3
import os
import logging
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, List
from datetime import datetime
import threading
import time
import weakref

@dataclass
class PlexWatchStatus:
    """Watch status information from Plex database"""
    file_path: str
    watched: bool = False
    watch_count: int = 0
    last_watched: Optional[datetime] = None
    view_offset: int = 0  # Resume position in milliseconds
    duration: Optional[int] = None  # Total duration in milliseconds
    progress_percent: float = 0.0  # Calculated progress percentage
    plex_title: Optional[str] = None
    plex_year: Optional[int] = None
    library_section: Optional[str] = None

class PlexConnectionPool:
    """Connection pool for Plex database with automatic cleanup"""
    
    def __init__(self, db_path: str, pool_size: int = 5, connection_timeout: float = 30.0):
        self.db_path = db_path
        self.pool_size = pool_size
        self.connection_timeout = connection_timeout
        self._pool = []
        self._pool_lock = threading.Lock()
        self._cleanup_thread = None
        self._shutdown = False
        self._last_cleanup = time.time()
        
        # Use weakref to track all pool instances for cleanup
        PlexConnectionPool._instances.add(self)
    
    # Class-level tracking of instances for cleanup
    _instances = weakref.WeakSet()
    
    def get_connection(self) -> sqlite3.Connection:
        """Get a connection from the pool or create a new one"""
        with self._pool_lock:
            # Try to get an existing connection
            while self._pool:
                conn_info = self._pool.pop(0)
                conn, last_used = conn_info
                
                # Check if connection is still valid
                try:
                    conn.execute("SELECT 1").fetchone()
                    # Update last used time
                    conn_info = (conn, time.time())
                    return conn
                except (sqlite3.Error, sqlite3.OperationalError):
                    # Connection is dead, close it
                    try:
                        conn.close()
                    except:
                        pass
            
            # No valid connections in pool, create new one
            return self._create_connection()
    
    def return_connection(self, conn: sqlite3.Connection):
        """Return a connection to the pool"""
        if self._shutdown:
            try:
                conn.close()
            except:
                pass
            return
        
        with self._pool_lock:
            # Only keep up to pool_size connections
            if len(self._pool) < self.pool_size:
                try:
                    # Test connection is still valid
                    conn.execute("SELECT 1").fetchone()
                    self._pool.append((conn, time.time()))
                    
                    # Start cleanup thread if needed
                    self._ensure_cleanup_thread()
                except (sqlite3.Error, sqlite3.OperationalError):
                    # Connection is dead, close it
                    try:
                        conn.close()
                    except:
                        pass
            else:
                # Pool is full, close this connection
                try:
                    conn.close()
                except:
                    pass
    
    def _create_connection(self) -> sqlite3.Connection:
        """Create a new database connection"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        # Set pragmas for better performance and safety
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA mmap_size=268435456")  # 256MB
        return conn
    
    def _ensure_cleanup_thread(self):
        """Ensure cleanup thread is running"""
        if self._cleanup_thread is None or not self._cleanup_thread.is_alive():
            self._cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
            self._cleanup_thread.start()
    
    def _cleanup_loop(self):
        """Background thread to clean up old connections"""
        while not self._shutdown:
            try:
                time.sleep(5)  # Check every 5 seconds
                current_time = time.time()
                
                with self._pool_lock:
                    # Remove connections that have been idle too long
                    active_connections = []
                    for conn, last_used in self._pool:
                        if current_time - last_used < self.connection_timeout:
                            active_connections.append((conn, last_used))
                        else:
                            try:
                                conn.close()
                            except:
                                pass
                    
                    self._pool = active_connections
                    
                    # If pool is empty, thread can exit
                    if not self._pool:
                        break
                        
            except Exception as e:
                logging.warning(f"Error in Plex connection cleanup: {e}")
    
    def close_all(self):
        """Close all connections and shutdown pool"""
        self._shutdown = True
        
        with self._pool_lock:
            for conn, _ in self._pool:
                try:
                    conn.close()
                except:
                    pass
            self._pool.clear()
        
        if self._cleanup_thread and self._cleanup_thread.is_alive():
            self._cleanup_thread.join(timeout=1.0)
    
    def __del__(self):
        """Cleanup when pool is destroyed"""
        self.close_all()
    
    @classmethod
    def cleanup_all_instances(cls):
        """Class method to cleanup all pool instances"""
        for instance in list(cls._instances):
            try:
                instance.close_all()
            except:
                pass

class PlexMetadataProvider:
    """Provider for querying Plex database for watch status"""
    
    def __init__(self, plex_data_dir: Optional[str] = None, pool_size: int = 3, connection_timeout: float = 30.0):
        """
        Initialize Plex metadata provider
        
        Args:
            plex_data_dir: Path to Plex data directory. If None, will try to auto-detect.
            pool_size: Maximum number of connections to keep in pool
            connection_timeout: Seconds to keep idle connections open
        """
        self.plex_data_dir = plex_data_dir or self._find_plex_data_dir()
        self.db_path = None
        self.connection_pool = None
        
        if self.plex_data_dir:
            self.db_path = os.path.join(self.plex_data_dir, "Plug-in Support", "Databases", "com.plexapp.plugins.library.db")
            if os.path.exists(self.db_path):
                self.connection_pool = PlexConnectionPool(self.db_path, pool_size, connection_timeout)
    
    def _find_plex_data_dir(self) -> Optional[str]:
        """Try to auto-detect Plex data directory"""
        common_paths = [
            os.path.expandvars(r"%LOCALAPPDATA%\Plex Media Server"),  # Windows
            os.path.expanduser("~/Library/Application Support/Plex Media Server"),  # macOS
            os.path.expanduser("~/.config/plex"),  # Linux
            "/var/lib/plexmediaserver/Library/Application Support/Plex Media Server",  # Linux system install
        ]
        
        for path in common_paths:
            if os.path.exists(path):
                return path
        
        logging.warning("Could not auto-detect Plex data directory")
        return None
    
    def is_available(self) -> bool:
        """Check if Plex database is accessible"""
        if not self.db_path:
            logging.error("Plex database path is not set")
            return False
        
        return os.path.exists(self.db_path) and self.connection_pool is not None
    
    def get_watch_status(self, file_path: str) -> Optional[PlexWatchStatus]:
        """
        Get watch status for a specific file path
        
        Args:
            file_path: Full path to the media file
            
        Returns:
            PlexWatchStatus object or None if not found
        """
        if not self.is_available() or not self.connection_pool:
            return None
        
        conn = None
        try:
            # Normalize path for comparison
            normalized_path = Path(file_path).resolve().as_posix()
            
            conn = self.connection_pool.get_connection()
            cursor = conn.cursor()
            
            # Updated query to use correct Plex database schema
            # First get the basic media information
            media_query = """
            SELECT 
                md.id as metadata_item_id,
                md.guid,
                md.title,
                md.year,
                mp.duration,
                mp.file as file_path,
                ls.name as library_section
            FROM metadata_items md
            JOIN media_items mi ON md.id = mi.metadata_item_id
            JOIN media_parts mp ON mi.id = mp.media_item_id
            LEFT JOIN library_sections ls ON md.library_section_id = ls.id
            WHERE mp.file LIKE ?
               OR mp.file = ?
            """
            
            cursor.execute(media_query, (f"%{normalized_path}%", file_path))
            media_result = cursor.fetchone()
            
            if media_result:
                # Get view information from metadata_item_views
                view_query = """
                SELECT 
                    COUNT(*) as view_count,
                    MAX(viewed_at) as last_viewed_at
                FROM metadata_item_views
                WHERE guid = ?
                """
                
                cursor.execute(view_query, (media_result['guid'],))
                view_result = cursor.fetchone()
                
                view_count = view_result['view_count'] if view_result else 0
                last_viewed_at = view_result['last_viewed_at'] if view_result else None
                
                # Convert timestamps
                last_watched = None
                if last_viewed_at:
                    last_watched = datetime.fromtimestamp(last_viewed_at)
                
                # Note: view_offset (resume position) is not available in this schema
                # It might be stored elsewhere or not tracked in this version
                
                return PlexWatchStatus(
                    file_path=file_path,
                    watched=bool(view_count > 0),
                    watch_count=view_count,
                    last_watched=last_watched,
                    view_offset=0,  # Not available in this schema
                    duration=media_result['duration'],
                    progress_percent=0.0,  # Cannot calculate without view_offset
                    plex_title=media_result['title'],
                    plex_year=media_result['year'],
                    library_section=media_result['library_section']
                )
                
        except Exception as e:
            logging.error(f"Error querying Plex database for {file_path}: {str(e)}")
        finally:
            if conn and self.connection_pool:
                self.connection_pool.return_connection(conn)
        
        return None
    
    def get_watch_status_by_title(self, title: str, year: Optional[int] = None) -> List[PlexWatchStatus]:
        """
        Get watch status for all files matching a title (useful for TV shows)
        
        Args:
            title: Title to search for
            year: Optional year filter
            
        Returns:
            List of PlexWatchStatus objects
        """
        if not self.is_available() or not self.connection_pool:
            return []
        
        results = []
        conn = None
        try:
            conn = self.connection_pool.get_connection()
            cursor = conn.cursor()
            
            # Updated query to use correct Plex database schema
            media_query = """
            SELECT 
                md.id as metadata_item_id,
                md.guid,
                md.title,
                md.year,
                mp.duration,
                mp.file as file_path,
                ls.name as library_section
            FROM metadata_items md
            JOIN media_items mi ON md.id = mi.metadata_item_id
            JOIN media_parts mp ON mi.id = mp.media_item_id
            LEFT JOIN library_sections ls ON md.library_section_id = ls.id
            WHERE md.title LIKE ?
            """
            
            params = [f"%{title}%"]
            if year:
                media_query += " AND md.year = ?"
                params.append(str(year))
            
            cursor.execute(media_query, params)
            
            for media_row in cursor.fetchall():
                # Get view information for this specific item
                view_query = """
                SELECT 
                    COUNT(*) as view_count,
                    MAX(viewed_at) as last_viewed_at
                FROM metadata_item_views
                WHERE guid = ?
                """
                
                cursor.execute(view_query, (media_row['guid'],))
                view_result = cursor.fetchone()
                
                view_count = view_result['view_count'] if view_result else 0
                last_viewed_at = view_result['last_viewed_at'] if view_result else None
                
                last_watched = None
                if last_viewed_at:
                    last_watched = datetime.fromtimestamp(last_viewed_at)
                
                results.append(PlexWatchStatus(
                    file_path=media_row['file_path'],
                    watched=bool(view_count > 0),
                    watch_count=view_count,
                    last_watched=last_watched,
                    view_offset=0,  # Not available in this schema
                    duration=media_row['duration'],
                    progress_percent=0.0,  # Cannot calculate without view_offset
                    plex_title=media_row['title'],
                    plex_year=media_row['year'],
                    library_section=media_row['library_section']
                ))
                
        except Exception as e:
            logging.error(f"Error querying Plex database for title {title}: {str(e)}")
        finally:
            if conn and self.connection_pool:
                self.connection_pool.return_connection(conn)
        
        return results
    
    def close(self):
        """Close connection pool and cleanup resources"""
        if self.connection_pool:
            self.connection_pool.close_all()
            self.connection_pool = None
    
    def __del__(self):
        """Cleanup when provider is destroyed"""
        self.close()

# Module-level cleanup function
def cleanup_all_plex_connections():
    """Cleanup all Plex connection pools"""
    PlexConnectionPool.cleanup_all_instances()

# Register cleanup function to be called on module exit
import atexit
atexit.register(cleanup_all_plex_connections)
