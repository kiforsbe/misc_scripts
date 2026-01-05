"""
File Metadata Scanner

A comprehensive tool for extracting metadata from files and folders.
Supports basic metadata (size, timestamps, attributes) and extended metadata
(audio/video properties via ffmpeg, image properties, etc.).

Usage:
    python file_metadata_scanner.py <path> [options]
"""

import os
import sys
import json
import csv
import argparse
import subprocess
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Set, Any
from dataclasses import dataclass, asdict, field
from abc import ABC, abstractmethod

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    # Fallback: no-op progress bar
    class tqdm:
        def __init__(self, iterable=None, desc="", unit="", total=None, disable=False, leave=True):
            self.iterable = iterable
            self.desc = desc
            self.n = 0
            self.total = total
            
        def __iter__(self):
            return iter(self.iterable) if self.iterable else iter([])
        
        def __enter__(self):
            return self
        
        def __exit__(self, *args):
            pass
        
        def update(self, n=1):
            self.n += n
        
        def set_description(self, desc):
            self.desc = desc

try:
    from video_thumbnail_generator import VideoThumbnailGenerator
    THUMBNAIL_GENERATOR_AVAILABLE = True
except ImportError:
    THUMBNAIL_GENERATOR_AVAILABLE = False
    VideoThumbnailGenerator = None


def setup_logging(log_level: str, log_file: Optional[str] = None) -> None:
    """Configure logging with both console and file handlers.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional path to log file. If None, defaults to file_metadata_scanner.log
    """
    # Convert string level to logging constant
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {log_level}')
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Get root logger
    logger = logging.getLogger()
    logger.setLevel(numeric_level)
    
    # Remove existing handlers
    logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler
    if log_file is None:
        log_file = 'file_metadata_scanner.log'
    file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    file_handler.setLevel(numeric_level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    logging.info(f"Logging initialized at {log_level} level")


@dataclass
class FileMetadata:
    """Data class for file metadata."""
    path: str
    name: str
    type: str  # 'file' or 'directory'
    size: int
    size_human: str
    created_time: str
    modified_time: str
    accessed_time: str
    extension: str = ""
    is_hidden: bool = False
    is_readonly: bool = False
    is_system: bool = False
    extended_metadata: Dict[str, Any] = field(default_factory=dict)
    static_thumbnail: str = ""
    animated_thumbnail: str = ""


class ExtendedMetadataExtractor:
    """
    Base class for extracting extended metadata from files.
    Provides a common API with file-type-specific implementations.
    """
    
    # Define file type groups
    VIDEO_EXTENSIONS = {'.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv', '.webm', '.m4v', '.mpg', '.mpeg'}
    AUDIO_EXTENSIONS = {'.mp3', '.wav', '.flac', '.aac', '.ogg', '.m4a', '.wma', '.opus', '.ape'}
    IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp', '.svg', '.ico'}
    DOCUMENT_EXTENSIONS = {'.pdf', '.doc', '.docx', '.txt', '.rtf', '.odt'}
    COMIC_EXTENSIONS = {'.cbr', '.cbz'}
    
    def __init__(self, skip_cbr: bool = False):
        self.logger = logging.getLogger(__name__)
        self.skip_cbr = skip_cbr
        self._ffmpeg_available = self._check_ffmpeg()
        self._ffprobe_available = self._check_ffprobe()
    
    def _check_ffmpeg(self) -> bool:
        """Check if ffmpeg is available."""
        try:
            subprocess.run(['ffmpeg', '-version'], 
                         capture_output=True, 
                         check=True,
                         creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == 'win32' else 0)
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            return False
    
    def _check_ffprobe(self) -> bool:
        """Check if ffprobe is available."""
        try:
            subprocess.run(['ffprobe', '-version'], 
                         capture_output=True, 
                         check=True,
                         creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == 'win32' else 0)
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            return False
    
    def extract(self, file_path: str) -> Dict[str, Any]:
        """
        Extract extended metadata based on file type.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Dictionary containing extended metadata
        """
        ext = Path(file_path).suffix.lower()
        
        if ext in self.VIDEO_EXTENSIONS or ext in self.AUDIO_EXTENSIONS:
            return self._extract_media_metadata(file_path)
        elif ext in self.IMAGE_EXTENSIONS:
            return self._extract_image_metadata(file_path)
        elif ext in self.DOCUMENT_EXTENSIONS:
            return self._extract_document_metadata(file_path)
        elif ext in self.COMIC_EXTENSIONS:
            return self._extract_comic_metadata(file_path)
        else:
            return {}
    
    def _extract_media_metadata(self, file_path: str) -> Dict[str, Any]:
        """Extract metadata from audio/video files using ffprobe."""
        if not self._ffprobe_available:
            self.logger.warning("ffprobe not available for media metadata extraction")
            return {'error': 'ffprobe not available'}
        
        try:
            cmd = [
                'ffprobe',
                '-v', 'quiet',
                '-print_format', 'json',
                '-show_format',
                '-show_streams',
                file_path
            ]
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == 'win32' else 0
            )
            
            data = json.loads(result.stdout)
            metadata = {}
            
            # Extract format information
            if 'format' in data:
                fmt = data['format']
                metadata['duration'] = float(fmt.get('duration', 0))
                metadata['duration_human'] = self._format_duration(metadata['duration'])
                metadata['bitrate'] = int(fmt.get('bit_rate', 0))
                metadata['format_name'] = fmt.get('format_name', '')
                
                # Extract tags (title, artist, album, etc.)
                if 'tags' in fmt:
                    metadata['tags'] = fmt['tags']
            
            # Extract stream information
            if 'streams' in data:
                video_streams = [s for s in data['streams'] if s.get('codec_type') == 'video']
                audio_streams = [s for s in data['streams'] if s.get('codec_type') == 'audio']
                
                if video_streams:
                    v = video_streams[0]
                    metadata['video_codec'] = v.get('codec_name', '')
                    metadata['video_width'] = v.get('width', 0)
                    metadata['video_height'] = v.get('height', 0)
                    metadata['video_fps'] = self._parse_fps(v.get('r_frame_rate', '0/1'))
                    metadata['video_bitrate'] = int(v.get('bit_rate', 0))
                
                if audio_streams:
                    a = audio_streams[0]
                    metadata['audio_codec'] = a.get('codec_name', '')
                    metadata['audio_sample_rate'] = int(a.get('sample_rate', 0))
                    metadata['audio_channels'] = a.get('channels', 0)
                    metadata['audio_bitrate'] = int(a.get('bit_rate', 0))
            
            return metadata
            
        except subprocess.CalledProcessError as e:
            self.logger.error(f"ffprobe failed for {Path(file_path).name}: {e.stderr if hasattr(e, 'stderr') else str(e)}", exc_info=True)
            return {'error': str(e)}
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse ffprobe output for {Path(file_path).name}: {str(e)}", exc_info=True)
            return {'error': f'JSON decode error: {str(e)}'}
        except Exception as e:
            self.logger.error(f"Error extracting media metadata for {Path(file_path).name}: {str(e)}", exc_info=True)
            return {'error': str(e)}
    
    def _extract_image_metadata(self, file_path: str) -> Dict[str, Any]:
        """Extract metadata from image files."""
        try:
            # Try using PIL if available
            try:
                from PIL import Image
                with Image.open(file_path) as img:
                    metadata = {
                        'width': img.width,
                        'height': img.height,
                        'format': img.format,
                        'mode': img.mode
                    }
                    
                    # Extract EXIF data if available
                    if hasattr(img, '_getexif') and img._getexif():
                        metadata['has_exif'] = True
                    
                    return metadata
            except ImportError:
                return {'error': 'PIL/Pillow not available'}
                
        except Exception as e:
            return {'error': str(e)}
    
    def _extract_document_metadata(self, file_path: str) -> Dict[str, Any]:
        """Extract metadata from document files."""
        # Placeholder for document metadata extraction
        # Could be extended with libraries like pypdf2, python-docx, etc.
        return {}
    
    def _extract_comic_metadata(self, file_path: str) -> Dict[str, Any]:
        """Extract metadata from comic book archive files (CBR/CBZ)."""
        import zipfile
        import xml.etree.ElementTree as ET
        
        ext = Path(file_path).suffix.lower()
        metadata = {}
        
        try:
            # Handle CBZ (ZIP) archives
            if ext == '.cbz':
                with zipfile.ZipFile(file_path, 'r') as archive:
                    # Get namelist once and cache it
                    all_files = archive.namelist()
                    
                    # Get list of image files
                    image_files = [f for f in all_files
                                 if f.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'))
                                 and not f.startswith('__MACOSX/')]
                    image_files.sort()
                    
                    metadata['page_count'] = len(image_files)
                    metadata['format'] = 'CBZ'
                    
                    # Try to read ComicInfo.xml if present
                    if 'ComicInfo.xml' in all_files:
                        try:
                            with archive.open('ComicInfo.xml') as xml_file:
                                tree = ET.parse(xml_file)
                                root = tree.getroot()
                                
                                # Extract common comic metadata
                                for field in ['Title', 'Series', 'Number', 'Volume', 'Writer', 
                                            'Penciller', 'Publisher', 'Year', 'PageCount']:
                                    elem = root.find(field)
                                    if elem is not None and elem.text:
                                        metadata[field.lower()] = elem.text
                        except Exception:
                            pass
                    
                    # Store first page name for thumbnail generation
                    if image_files:
                        metadata['first_page'] = image_files[0]
            
            # Handle CBR (RAR) archives
            elif ext == '.cbr':
                if self.skip_cbr:
                    self.logger.debug(f"Skipping CBR file (--skip-cbr enabled): {Path(file_path).name}")
                    metadata['format'] = 'CBR'
                    metadata['page_count'] = 0
                    metadata['error'] = 'CBR processing skipped (--skip-cbr flag)'
                    return metadata
                
                # Try libarchive first (fast, native library)
                libarchive_failed = False
                try:
                    import libarchive
                    self.logger.debug(f"Using libarchive for CBR archive: {Path(file_path).name}")
                    
                    image_files = []
                    comicinfo_data = None
                    
                    try:
                        with libarchive.file_reader(file_path) as archive:
                            for entry in archive:
                                try:
                                    filename = entry.pathname
                                    
                                    # Skip if pathname is None or not a string
                                    if not filename or not isinstance(filename, str):
                                        continue
                                    
                                    # Check for ComicInfo.xml
                                    if filename == 'ComicInfo.xml':
                                        # Read the XML data
                                        comicinfo_data = b''.join(entry.get_blocks())
                                    
                                    # Check for image files
                                    elif (filename.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'))
                                          and not filename.startswith('__MACOSX/')):
                                        image_files.append(filename)
                                except Exception as e:
                                    self.logger.debug(f"Error processing archive entry: {e}")
                                    continue
                    except Exception as e:
                        self.logger.warning(f"libarchive failed for {Path(file_path).name}: {e}, falling back to rarfile")
                        libarchive_failed = True
                    
                    if not libarchive_failed:
                        image_files.sort()
                        metadata['page_count'] = len(image_files)
                        metadata['format'] = 'CBR'
                        
                        # Parse ComicInfo.xml if found
                        if comicinfo_data:
                            try:
                                import xml.etree.ElementTree as ET
                                root = ET.fromstring(comicinfo_data)
                                for field in ['Title', 'Series', 'Number', 'Volume', 'Writer', 
                                            'Penciller', 'Publisher', 'Year', 'PageCount']:
                                    elem = root.find(field)
                                    if elem is not None and elem.text:
                                        metadata[field.lower()] = elem.text
                            except Exception:
                                pass
                        
                        # Store first page name for thumbnail generation
                        if image_files:
                            metadata['first_page'] = image_files[0]
                        
                        self.logger.debug(f"Finished CBR archive (libarchive): {Path(file_path).name}")
                    
                except (ImportError, OSError, TypeError) as e:
                    # ImportError: libarchive-c not installed
                    # OSError: libarchive DLL not found or invalid path
                    # TypeError: libarchive path is None during import (DLL not installed)
                    if isinstance(e, TypeError):
                        self.logger.debug(f"libarchive-c is installed but libarchive DLL is missing. Install libarchive DLL or use 'pip install rarfile' for CBR support. Falling back to rarfile.")
                    elif isinstance(e, OSError):
                        self.logger.debug(f"libarchive DLL initialization failed: {e}. The DLL path may be incorrect or dependencies are missing. Falling back to rarfile.")
                    else:
                        self.logger.debug(f"libarchive import/initialization failed: {e}. Falling back to rarfile.")
                    libarchive_failed = True
                
                # Fallback to rarfile if libarchive failed or not available
                if libarchive_failed:
                    try:
                        import rarfile
                        self.logger.debug(f"Using rarfile (slow) for CBR archive: {Path(file_path).name}")
                        with rarfile.RarFile(file_path, 'r') as archive:
                            # Get namelist once and cache it (subprocess call - slow)
                            all_files = archive.namelist()
                            
                            # Get list of image files
                            image_files = [f for f in all_files
                                         if f.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'))
                                         and not f.startswith('__MACOSX/')]
                            image_files.sort()
                            
                            metadata['page_count'] = len(image_files)
                            metadata['format'] = 'CBR'
                            
                            # Skip ComicInfo.xml extraction to avoid additional subprocess overhead
                            
                            # Store first page name for thumbnail generation
                            if image_files:
                                metadata['first_page'] = image_files[0]
                        self.logger.debug(f"Finished CBR archive (rarfile): {Path(file_path).name}")
                    except ImportError:
                        self.logger.warning(f"Neither libarchive nor rarfile library available for CBR extraction: {Path(file_path).name}")
                        metadata['error'] = 'No RAR library available for CBR extraction (install libarchive-c or rarfile)'
                        metadata['page_count'] = 0
                        metadata['format'] = 'CBR'
            
            return metadata
            
        except Exception as e:
            self.logger.error(f"Error extracting comic metadata for {Path(file_path).name}: {str(e)}", exc_info=True)
            return {'error': str(e)}
    
    @staticmethod
    def _format_duration(seconds: float) -> str:
        """Format duration in seconds to human-readable format."""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        
        if hours > 0:
            return f"{hours:02d}:{minutes:02d}:{secs:02d}"
        else:
            return f"{minutes:02d}:{secs:02d}"
    
    @staticmethod
    def _parse_fps(fps_str: str) -> float:
        """Parse FPS from fraction string (e.g., '30000/1001')."""
        try:
            if '/' in fps_str:
                num, den = fps_str.split('/')
                return round(float(num) / float(den), 2)
            return float(fps_str)
        except (ValueError, ZeroDivisionError):
            return 0.0


class WebappGenerator:
    """
    Generator for standalone HTML webapps from file metadata.
    Handles template loading and HTML generation independently of metadata source.
    """
    
    @staticmethod
    def generate_html(metadata_list: List[Dict[str, Any]], output_path: Path) -> bool:
        """
        Generate webapp HTML from metadata list.
        
        Args:
            metadata_list: List of metadata dictionaries
            output_path: Path where the HTML file will be written
            
        Returns:
            True if successful, False otherwise
        """
        logger = logging.getLogger(__name__)
        logger.info(f"Generating webapp HTML with {len(metadata_list)} items")
        
        # Get script directory to load templates
        script_dir = Path(__file__).parent
        
        try:
            # Load templates
            logger.debug("Loading template files")
            html_template = (script_dir / 'file_metadata_scanner_template.html').read_text(encoding='utf-8')
            css_template = (script_dir / 'file_metadata_scanner_template.css').read_text(encoding='utf-8')
            js_template = (script_dir / 'file_metadata_scanner_template.js').read_text(encoding='utf-8')
            
            # Prepare metadata JSON
            metadata_json = json.dumps(metadata_list, ensure_ascii=False)
            
            # Replace placeholders
            html_content = html_template.replace('/*CSS_PLACEHOLDER*/', css_template)
            html_content = html_content.replace('/*JS_PLACEHOLDER*/', js_template)
            html_content = html_content.replace('/*JSON_PLACEHOLDER*/', metadata_json)
            
            # Write output
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            logger.info(f"Webapp generated successfully: {output_path}")
            return True
            
        except FileNotFoundError as e:
            logger.error(f"Template file not found: {e}")
            print(f"Error: Template file not found. Make sure template files are in the script directory.", file=sys.stderr)
            print(f"Details: {e}", file=sys.stderr)
            return False
        except Exception as e:
            logger.error(f"Error generating webapp: {e}")
            print(f"Error generating webapp: {e}", file=sys.stderr)
            return False


class FileMetadataScanner:
    """
    Main class for scanning directories and extracting file metadata.
    """
    
    def __init__(self, 
                 root_path: str,
                 recursive: bool = True,
                 exclude_paths: Optional[List[str]] = None,
                 file_extensions: Optional[Set[str]] = None,
                 extract_extended: bool = False,
                 metadata_root: Optional[str] = None,
                 thumbnails_enabled: bool = False,
                 min_duration: float = 300.0,
                 skip_cbr: bool = False):
        """
        Initialize the scanner.
        
        Args:
            root_path: Root directory to scan
            recursive: Whether to scan recursively
            exclude_paths: List of paths to exclude
            file_extensions: Set of file extensions to filter (e.g., {'.mp4', '.txt'})
            extract_extended: Whether to extract extended metadata
            metadata_root: Root directory for metadata files (CSV, JSON, thumbnails)
            thumbnails_enabled: Whether to generate thumbnails for video files
            min_duration: Minimum video duration in seconds for thumbnail generation
            skip_cbr: Whether to skip CBR (RAR) comic archive processing
        """
        self.logger = logging.getLogger(__name__)
        self.root_path = Path(root_path).resolve()
        self.recursive = recursive
        self.exclude_paths = set(exclude_paths or [])
        self.file_extensions = set(ext.lower() if ext.startswith('.') else f'.{ext.lower()}' 
                                   for ext in (file_extensions or []))
        self.extract_extended = extract_extended
        self.thumbnails_enabled = thumbnails_enabled
        self.min_duration = min_duration
        self.skip_cbr = skip_cbr
        
        self.logger.info(f"Scanner initialized: path={root_path}, recursive={recursive}, extended={extract_extended}, thumbnails={thumbnails_enabled}, min_duration={min_duration}s, skip_cbr={skip_cbr}")
        
        # Set metadata root directory
        if metadata_root:
            self.metadata_root = Path(metadata_root).resolve()
        else:
            # Create default bundle name based on the scanned path
            if self._is_drive_root(self.root_path):
                # For drive roots (C:\, D:\, etc.), use drive label
                drive_label = self._get_drive_label(self.root_path)
                bundle_name = f"{drive_label}_metadata"
                # Place bundle in the drive root
                self.metadata_root = self.root_path / bundle_name
            else:
                # For regular folders, create bundle as sibling with _metadata suffix
                parent = self.root_path.parent
                folder_name = self.root_path.name
                bundle_name = f"{folder_name}_metadata"
                self.metadata_root = parent / bundle_name
        
        # Create metadata root directory
        self.metadata_root.mkdir(parents=True, exist_ok=True)
        
        self.metadata_extractor = ExtendedMetadataExtractor(skip_cbr=skip_cbr) if extract_extended else None
        self.results: List[FileMetadata] = []
        self.video_files: List[Path] = []  # Track video files for batch thumbnail generation
        
        # Initialize thumbnail generator if requested
        self.thumbnail_generator = None
        if thumbnails_enabled:
            if THUMBNAIL_GENERATOR_AVAILABLE and VideoThumbnailGenerator:
                thumbnail_dir = self.metadata_root / 'thumbnails'
                self.thumbnail_generator = VideoThumbnailGenerator(
                    thumbnail_dir=str(thumbnail_dir),
                    max_height=480,
                    min_duration=min_duration,
                    skip_cbr=skip_cbr
                )
            else:
                print("Warning: Video thumbnail generation requested but video_thumbnail_generator.py not available", 
                      file=sys.stderr)
        
        # Normalize exclude paths
        self.exclude_paths = {Path(p).resolve() for p in self.exclude_paths}
    
    def _get_drive_label(self, path: Path) -> str:
        """Get the volume label for a drive on Windows."""
        if sys.platform == 'win32':
            try:
                import ctypes
                drive_letter = path.drive  # e.g., 'C:'
                
                # Prepare buffers for the volume information
                volume_name_buffer = ctypes.create_unicode_buffer(1024)
                file_system_name_buffer = ctypes.create_unicode_buffer(1024)
                
                # Call GetVolumeInformationW
                result = ctypes.windll.kernel32.GetVolumeInformationW(
                    ctypes.c_wchar_p(drive_letter + '\\'),
                    volume_name_buffer,
                    ctypes.sizeof(volume_name_buffer),
                    None, None, None,
                    file_system_name_buffer,
                    ctypes.sizeof(file_system_name_buffer)
                )
                
                if result:
                    volume_label = volume_name_buffer.value
                    if volume_label:
                        # Sanitize the label for use in folder names
                        return volume_label.replace(':', '').replace('\\', '').replace('/', '')
                
                # Fallback to drive letter if no label found
                return drive_letter.rstrip(':')
            except Exception:
                # Fallback to drive letter on any error
                return path.drive.rstrip(':')
        else:
            # On Unix-like systems, no volume labels typically
            return 'root'
    
    def _is_drive_root(self, path: Path) -> bool:
        """Check if path is a drive root (e.g., C:\, D:\) on Windows."""
        if sys.platform == 'win32':
            # Check if path is just a drive letter (C:\, D:\, etc.)
            return path.parent == path
        else:
            # On Unix-like systems, check if it's the root directory
            return str(path) == '/'
    
    def _is_excluded(self, path: Path) -> bool:
        """Check if path should be excluded."""
        resolved = path.resolve()
        
        # Check if path is in exclude list or is a child of an excluded path
        for exclude_path in self.exclude_paths:
            try:
                resolved.relative_to(exclude_path)
                return True
            except ValueError:
                continue
        
        return False
    
    def _should_include_file(self, file_path: Path) -> bool:
        """Check if file should be included based on extension filter."""
        if not self.file_extensions:
            return True
        return file_path.suffix.lower() in self.file_extensions
    
    def _get_directory_size(self, dir_path: Path) -> int:
        """
        Recursively calculate directory size.
        
        Args:
            dir_path: Directory path
            
        Returns:
            Total size in bytes
        """
        total_size = 0
        
        try:
            for item in dir_path.iterdir():
                if self._is_excluded(item):
                    continue
                    
                if item.is_file():
                    try:
                        total_size += item.stat().st_size
                    except (OSError, PermissionError):
                        pass
                elif item.is_dir():
                    total_size += self._get_directory_size(item)
        except (OSError, PermissionError):
            pass
        
        return total_size
    
    def _format_size(self, size_bytes: int) -> str:
        """Format size in bytes to human-readable format."""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.2f} PB"
    
    def _get_file_attributes(self, path: Path) -> Dict[str, bool]:
        """Get file attributes (hidden, readonly, system)."""
        attrs = {
            'is_hidden': False,
            'is_readonly': False,
            'is_system': False
        }
        
        try:
            if sys.platform == 'win32':
                import stat
                st = path.stat()
                
                # Check if hidden (name starts with .)
                attrs['is_hidden'] = path.name.startswith('.')
                
                # Check if readonly
                attrs['is_readonly'] = not (st.st_mode & stat.S_IWRITE)
                
                # On Windows, check FILE_ATTRIBUTE_HIDDEN and FILE_ATTRIBUTE_SYSTEM
                try:
                    import ctypes
                    FILE_ATTRIBUTE_HIDDEN = 0x2
                    FILE_ATTRIBUTE_SYSTEM = 0x4
                    
                    file_attrs = ctypes.windll.kernel32.GetFileAttributesW(str(path))
                    if file_attrs != -1:
                        attrs['is_hidden'] = bool(file_attrs & FILE_ATTRIBUTE_HIDDEN)
                        attrs['is_system'] = bool(file_attrs & FILE_ATTRIBUTE_SYSTEM)
                except:
                    pass
            else:
                # Unix-like systems
                attrs['is_hidden'] = path.name.startswith('.')
                st = path.stat()
                attrs['is_readonly'] = not (st.st_mode & 0o200)
        except (OSError, PermissionError):
            pass
        
        return attrs
    
    def _extract_metadata(self, path: Path) -> FileMetadata:
        """
        Extract metadata for a file or directory.
        
        Args:
            path: Path to file or directory
            
        Returns:
            FileMetadata object
        """
        try:
            stat_info = path.stat()
            is_dir = path.is_dir()
            
            # Calculate size
            if is_dir:
                size = self._get_directory_size(path)
            else:
                size = stat_info.st_size
            
            # Get timestamps
            created = datetime.fromtimestamp(stat_info.st_ctime).isoformat()
            modified = datetime.fromtimestamp(stat_info.st_mtime).isoformat()
            accessed = datetime.fromtimestamp(stat_info.st_atime).isoformat()
            
            # Get attributes
            attrs = self._get_file_attributes(path)
            
            # Create metadata object
            metadata = FileMetadata(
                path=str(path),
                name=path.name,
                type='directory' if is_dir else 'file',
                size=size,
                size_human=self._format_size(size),
                created_time=created,
                modified_time=modified,
                accessed_time=accessed,
                extension=path.suffix.lower() if not is_dir else '',
                **attrs
            )
            
            # Extract extended metadata for files
            if not is_dir and self.extract_extended and self.metadata_extractor:
                metadata.extended_metadata = self.metadata_extractor.extract(str(path))
            
            # Collect video files and comic files for batch thumbnail generation later
            if not is_dir and self.thumbnail_generator:
                ext = path.suffix.lower()
                if ext in ExtendedMetadataExtractor.VIDEO_EXTENSIONS:
                    self.video_files.append(path)
                elif ext in ExtendedMetadataExtractor.COMIC_EXTENSIONS:
                    self.video_files.append(path)  # Reuse video_files list for all thumbnail generation
            
            return metadata
            
        except (OSError, PermissionError) as e:
            # Return minimal metadata on error
            return FileMetadata(
                path=str(path),
                name=path.name,
                type='unknown',
                size=0,
                size_human='0 B',
                created_time='',
                modified_time='',
                accessed_time='',
                extension=path.suffix.lower() if path.suffix else '',
                extended_metadata={'error': str(e)}
            )
    
    def scan(self, show_progress: bool = True) -> List[FileMetadata]:
        """
        Scan the directory and collect metadata.
        
        Args:
            show_progress: Whether to show progress bars
            
        Returns:
            List of FileMetadata objects
        """
        self.logger.info(f"Starting scan of {self.root_path}")
        self.results = []
        self.video_files = []
        
        if not self.root_path.exists():
            self.logger.error(f"Path does not exist: {self.root_path}")
            print(f"Error: Path '{self.root_path}' does not exist.", file=sys.stderr)
            return self.results
        
        # Add root directory metadata
        if self.root_path.is_dir():
            self.results.append(self._extract_metadata(self.root_path))
        
        # Scan directory with progress indication
        self.logger.info(f"Scanning files (recursive={self.recursive})...")
        print(f"Scanning files...")
        if self.recursive:
            self._scan_recursive(self.root_path, show_progress=show_progress)
        else:
            self._scan_non_recursive(self.root_path, show_progress=show_progress)
        
        self.logger.info(f"Scan complete: found {len(self.results)} items, {len(self.video_files)} video/comic files")
        return self.results
    
    def generate_thumbnails(self, show_progress: bool = True, force_regenerate: bool = False) -> bool:
        """
        Generate thumbnails for video files found during scan.
        
        Args:
            show_progress: Whether to show progress bars
            force_regenerate: Force regeneration even if thumbnails exist
            
        Returns:
            True if thumbnails were generated successfully, False otherwise
        """
        if not self.thumbnail_generator:
            self.logger.warning("Thumbnail generator not initialized")
            print("Warning: Thumbnail generator not initialized", file=sys.stderr)
            return False
        
        if not self.video_files:
            self.logger.info("No video files found for thumbnail generation")
            if show_progress:
                print("No video files found for thumbnail generation")
            return True
        
        self.logger.info(f"Generating thumbnails for {len(self.video_files)} video files")
        print(f"\nGenerating thumbnails for {len(self.video_files)} video files...")
        thumbnail_results = self.thumbnail_generator.generate_thumbnails_for_videos(
            self.video_files,
            verbose=1,
            force_regenerate=force_regenerate,
            show_progress=show_progress
        )
        
        self.logger.info(f"Thumbnail generation complete: {len(thumbnail_results)} results")
        
        # Map thumbnail results back to file metadata
        thumbnail_map = {r['video']: r for r in thumbnail_results}
        for item in self.results:
            if item.path in thumbnail_map:
                thumb_data = thumbnail_map[item.path]
                # Store only filename, not full path
                static_path = thumb_data.get('static_thumbnail')
                animated_path = thumb_data.get('animated_thumbnail')
                item.static_thumbnail = Path(static_path).name if static_path else ''
                item.animated_thumbnail = Path(animated_path).name if animated_path else ''
        
        return True
    
    def _scan_recursive(self, dir_path: Path, show_progress: bool = True, _pbar: Optional[tqdm] = None):
        """Recursively scan directory with progress indication."""
        # Create progress bar only at the top level
        is_top_level = _pbar is None
        if is_top_level and show_progress:
            _pbar = tqdm(desc="Scanning", unit="files", leave=False)
        
        try:
            items = sorted(dir_path.iterdir())
            for item in items:
                if self._is_excluded(item):
                    continue
                
                if item.is_dir():
                    self.results.append(self._extract_metadata(item))
                    if _pbar is not None:
                        _pbar.update(1)
                    self._scan_recursive(item, show_progress=show_progress, _pbar=_pbar)
                elif item.is_file():
                    if self._should_include_file(item):
                        self.results.append(self._extract_metadata(item))
                        if _pbar is not None:
                            _pbar.update(1)
        except (OSError, PermissionError) as e:
            print(f"Warning: Cannot access '{dir_path}': {e}", file=sys.stderr)
        finally:
            # Close progress bar only at the top level
            if is_top_level and _pbar is not None:
                _pbar.close()
    
    def _scan_non_recursive(self, dir_path: Path, show_progress: bool = True):
        """Scan directory non-recursively with progress indication."""
        try:
            items = list(sorted(dir_path.iterdir()))
            iterator = tqdm(items, desc="Scanning", unit="files", leave=False) if show_progress else items
            
            for item in iterator:
                if self._is_excluded(item):
                    continue
                
                if item.is_dir():
                    self.results.append(self._extract_metadata(item))
                elif item.is_file():
                    if self._should_include_file(item):
                        self.results.append(self._extract_metadata(item))
        except (OSError, PermissionError) as e:
            print(f"Warning: Cannot access '{dir_path}': {e}", file=sys.stderr)
    
    def export_to_csv(self, output_filename: str):
        """
        Export metadata to CSV file in metadata root directory.
        
        Args:
            output_filename: Filename for output CSV file (will be placed in metadata_root)
        """
        if not self.results:
            self.logger.warning("No results to export to CSV")
            print("No results to export.", file=sys.stderr)
            return
        
        # Ensure output is in metadata root
        output_path = self.metadata_root / output_filename
        
        self.logger.info(f"Exporting {len(self.results)} items to CSV: {output_path}")
        
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            # Define basic fields
            fieldnames = [
                'path', 'name', 'type', 'size', 'size_human',
                'created_time', 'modified_time', 'accessed_time',
                'extension', 'is_hidden', 'is_readonly', 'is_system',
                'static_thumbnail', 'animated_thumbnail'
            ]
            
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for item in self.results:
                row = asdict(item)
                # Remove extended_metadata from CSV (too complex)
                row.pop('extended_metadata', None)
                writer.writerow(row)
        
        self.logger.info(f"CSV export completed: {output_path}")
        print(f"CSV exported to: {output_path}")
    
    def export_to_json(self, output_filename: str):
        """
        Export metadata to JSON file in metadata root directory.
        
        Args:
            output_filename: Filename for output JSON file (will be placed in metadata_root)
        """
        if not self.results:
            self.logger.warning("No results to export to JSON")
            print("No results to export.", file=sys.stderr)
            return
        
        # Ensure output is in metadata root
        output_path = self.metadata_root / output_filename
        
        self.logger.info(f"Exporting {len(self.results)} items to JSON: {output_path}")
        
        data = [asdict(item) for item in self.results]
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"JSON export completed: {output_path}")
        print(f"JSON exported to: {output_path}")
    
    def export_to_webapp(self, output_filename: str):
        """
        Export metadata to a standalone HTML webapp in metadata root directory.
        
        Args:
            output_filename: Filename for output HTML file (will be placed in metadata_root)
        """
        if not self.results:
            self.logger.warning("No results to export to webapp")
            print("No results to export.", file=sys.stderr)
            return
        
        # Ensure output is in metadata root
        output_path = self.metadata_root / output_filename
        
        self.logger.info(f"Generating webapp with {len(self.results)} items: {output_path}")
        
        # Convert results to dictionary list
        data = [asdict(item) for item in self.results]
        
        # Generate webapp
        if WebappGenerator.generate_html(data, output_path):
            self.logger.info(f"Webapp generation completed: {output_path}")
            print(f"Webapp exported to: {output_path}")
    
    @staticmethod
    def regenerate_webapp_from_bundle(bundle_path: str, generate_thumbnails: bool = False,
                                      min_duration: float = 300.0, skip_cbr: bool = False) -> bool:
        """
        Regenerate the webapp HTML file from existing JSON metadata in a bundle.
        
        Args:
            bundle_path: Path to the metadata bundle directory
            generate_thumbnails: Whether to generate missing thumbnails for videos
            min_duration: Minimum video duration in seconds for thumbnail generation
            skip_cbr: Whether to skip CBR (RAR) comic archive processing
            
        Returns:
            True if successful, False otherwise
        """
        logger = logging.getLogger(__name__)
        bundle_dir = Path(bundle_path).resolve()
        
        logger.info(f"Regenerating webapp from bundle: {bundle_dir}")
        
        if not bundle_dir.exists() or not bundle_dir.is_dir():
            logger.error(f"Bundle directory does not exist: {bundle_path}")
            print(f"Error: Bundle directory '{bundle_path}' does not exist.", file=sys.stderr)
            return False
        
        # Find the latest JSON file in the bundle
        json_files = list(bundle_dir.glob('*_metadata_*.json'))
        
        if not json_files:
            logger.error(f"No metadata JSON files found in bundle: {bundle_path}")
            print(f"Error: No metadata JSON files found in '{bundle_path}'.", file=sys.stderr)
            return False
        
        # Get the most recent JSON file
        latest_json = max(json_files, key=lambda p: p.stat().st_mtime)
        logger.info(f"Loading metadata from: {latest_json}")
        print(f"Loading metadata from: {latest_json}")
        
        try:
            # Load metadata from JSON
            with open(latest_json, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
            
            print(f"Loaded {len(metadata)} items")
            
            # Generate missing thumbnails if requested
            if generate_thumbnails:
                if not THUMBNAIL_GENERATOR_AVAILABLE or not VideoThumbnailGenerator:
                    logger.warning("Video thumbnail generation requested but video_thumbnail_generator.py not available")
                    print("Warning: Video thumbnail generation requested but video_thumbnail_generator.py not available", file=sys.stderr)
                else:
                    # Find videos and comics without thumbnails
                    videos_needing_thumbnails = []
                    for item in metadata:
                        if (item.get('type') == 'file' and 
                            (item.get('extension', '').lower() in [ext for ext in ExtendedMetadataExtractor.VIDEO_EXTENSIONS] or
                             item.get('extension', '').lower() in [ext for ext in ExtendedMetadataExtractor.COMIC_EXTENSIONS]) and
                            not item.get('static_thumbnail') and 
                            not item.get('animated_thumbnail')):
                            videos_needing_thumbnails.append(Path(item['path']))
                    
                    if videos_needing_thumbnails:
                        logger.info(f"Generating thumbnails for {len(videos_needing_thumbnails)} videos")
                        print(f"\nGenerating thumbnails for {len(videos_needing_thumbnails)} videos...")
                        
                        # Initialize thumbnail generator
                        thumbnail_dir = bundle_dir / 'thumbnails'
                        thumbnail_generator = VideoThumbnailGenerator(
                            thumbnail_dir=str(thumbnail_dir),
                            max_height=480,
                            min_duration=min_duration,
                            skip_cbr=skip_cbr
                        )
                        
                        # Generate thumbnails
                        thumbnail_results = thumbnail_generator.generate_thumbnails_for_videos(
                            videos_needing_thumbnails,
                            verbose=1,
                            force_regenerate=False,
                            show_progress=True
                        )
                        
                        logger.info(f"Thumbnail generation complete: {len(thumbnail_results)} results")
                        
                        # Update metadata with thumbnail information
                        thumbnail_map = {r['video']: r for r in thumbnail_results}
                        for item in metadata:
                            if item.get('path') in thumbnail_map:
                                thumb_data = thumbnail_map[item['path']]
                                static_path = thumb_data.get('static_thumbnail')
                                animated_path = thumb_data.get('animated_thumbnail')
                                item['static_thumbnail'] = Path(static_path).name if static_path else ''
                                item['animated_thumbnail'] = Path(animated_path).name if animated_path else ''
                        
                        # Save updated metadata back to JSON
                        logger.info("Updating metadata file with thumbnail information")
                        print("\nUpdating metadata file with thumbnail information...")
                        with open(latest_json, 'w', encoding='utf-8') as f:
                            json.dump(metadata, f, indent=2, ensure_ascii=False)
                        logger.info(f"Metadata updated: {latest_json}")
                        print(f"Metadata updated: {latest_json}")
                    else:
                        logger.info("All videos already have thumbnails")
                        print("All videos already have thumbnails")
            
            # Generate output filename
            bundle_name = bundle_dir.name
            webapp_filename = f'{bundle_name}_explorer.html'
            output_path = bundle_dir / webapp_filename
            
            # Generate webapp using WebappGenerator
            if WebappGenerator.generate_html(metadata, output_path):
                print(f"Webapp regenerated: {output_path}")
                return True
            else:
                return False
            
        except json.JSONDecodeError as e:
            print(f"Error: Invalid JSON in metadata file.", file=sys.stderr)
            print(f"Details: {e}", file=sys.stderr)
            return False
        except Exception as e:
            print(f"Error regenerating webapp: {e}", file=sys.stderr)
            return False


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Extract metadata from files and folders',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic scan of current directory (exports to ./metadata/)
  python file_metadata_scanner.py .

  # Recursive scan with custom export location
  python file_metadata_scanner.py /path/to/folder -r --export-bundle /output/location

  # Scan only video files with extended metadata and thumbnails
  python file_metadata_scanner.py /path/to/videos -r -e mp4,mkv,avi --extended --thumbnails

  # Exclude specific paths
  python file_metadata_scanner.py /path/to/folder -r --exclude node_modules,__pycache__,.git

  # Full scan with all features and custom export location
  python file_metadata_scanner.py /path/to/media -r --extended --thumbnails --export-bundle C:\\MyMetadata

  # Regenerate webapp from existing bundle
  python file_metadata_scanner.py --regenerate-bundle /path/to/bundle
        """
    )
    
    # Create mutually exclusive group for scan vs regenerate
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument('path', nargs='?', help='Path to scan')
    mode_group.add_argument('--regenerate-bundle', type=str, dest='regenerate_bundle',
                           help='Regenerate webapp from existing metadata bundle directory')
    
    parser.add_argument('-r', '--recursive', action='store_true',
                       help='Scan recursively (default: non-recursive)')
    parser.add_argument('-e', '--extensions', type=str,
                       help='Comma-separated list of file extensions to include (e.g., mp4,txt,jpg)')
    parser.add_argument('--exclude', type=str,
                       help='Comma-separated list of paths to exclude')
    parser.add_argument('--extended', action='store_true',
                       help='Extract extended metadata (audio/video info, etc.)')
    parser.add_argument('--thumbnails', action='store_true',
                       help='Generate thumbnails for video files (requires ffmpeg). In scan mode, generates thumbnails during scan. In regenerate mode, generates missing thumbnails.')
    parser.add_argument('--skip-cbr', action='store_true',
                       help='Skip CBR (RAR) comic archives during metadata extraction (CBR processing is slow due to subprocess overhead). CBZ files will still be processed.')
    parser.add_argument('--export-bundle', type=str, dest='export_bundle',
                       help='Directory path where CSV, JSON, and thumbnails will be exported (default: <path>/metadata)')
    parser.add_argument('--min-duration', type=float, default=300.0,
                       help='Minimum video duration in seconds for thumbnail generation (default: 300 = 5 minutes). Set to 0 to generate for all videos.')
    
    # Logging arguments
    parser.add_argument('--log-level', type=str, default='ERROR',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                       help='Set the logging level (default: ERROR)')
    parser.add_argument('--log-file', type=str, default=None,
                       help='Path to log file (default: file_metadata_scanner.log)')
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level, args.log_file)
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 80)
    logger.info("File Metadata Scanner started")
    logger.info(f"Arguments: {vars(args)}")
    
    # Handle regenerate mode
    if args.regenerate_bundle:
        success = FileMetadataScanner.regenerate_webapp_from_bundle(
            args.regenerate_bundle, 
            generate_thumbnails=args.thumbnails,
            min_duration=args.min_duration,
            skip_cbr=args.skip_cbr
        )
        logger.info("Webapp regeneration completed")
        logger.info("=" * 80)
        sys.exit(0 if success else 1)
    
    # Validate path argument for scan mode
    if not args.path:
        logger.error("No path specified for scan")
        parser.error('path is required when not using --regenerate-bundle')
    
    # Parse extensions
    extensions = None
    if args.extensions:
        extensions = {ext.strip() for ext in args.extensions.split(',')}
    
    # Parse exclude paths
    exclude_paths = None
    if args.exclude:
        exclude_paths = [p.strip() for p in args.exclude.split(',')]
    
    # Create scanner
    scanner = FileMetadataScanner(
        root_path=args.path,
        recursive=args.recursive,
        exclude_paths=exclude_paths,
        file_extensions=extensions,
        extract_extended=args.extended,
        metadata_root=args.export_bundle,
        thumbnails_enabled=args.thumbnails,
        min_duration=args.min_duration,
        skip_cbr=args.skip_cbr
    )
    
    # Scan for files
    results = scanner.scan()
    print(f"Found {len(results)} items")
    logger.info(f"Scan completed: {len(results)} items found")
    
    # Export initial results (without thumbnails)
    base_name = scanner.metadata_root.name or 'metadata'
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = f'{base_name}_metadata_{timestamp}.csv'
    json_filename = f'{base_name}_metadata_{timestamp}.json'
    webapp_filename = f'{base_name}_explorer.html'
    
    print("\nSaving metadata...")
    logger.info("Exporting metadata to CSV and JSON")
    scanner.export_to_csv(csv_filename)
    scanner.export_to_json(json_filename)
    
    # Generate thumbnails if requested
    if args.thumbnails:
        if scanner.generate_thumbnails():
            # Re-export with thumbnail information
            print("\nUpdating metadata with thumbnail information...")
            logger.info("Updating metadata with thumbnail information")
            scanner.export_to_csv(csv_filename)
            scanner.export_to_json(json_filename)
    
    # Generate webapp
    scanner.export_to_webapp(webapp_filename)
    
    logger.info("File Metadata Scanner completed successfully")
    logger.info("=" * 80)


if __name__ == '__main__':
    main()
