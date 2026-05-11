"""
Video Thumbnail Generator Module

Generates static and animated WebP thumbnails for video files with caching support.
Provides both episode-based and file-based interfaces for generating thumbnails.

Features:
- Static thumbnails (20% into video)
- Animated thumbnails (19 frames, 5-95% of duration, 2 fps)
- Automatic caching and cache checking
- Error handling for problematic frames
- Index JSON generation
- Configurable output directory and quality settings
"""

import os
import json
import hashlib
import subprocess
import tempfile
import logging
import time
from io import BytesIO
from typing import List, Dict, Any, Optional

from PIL import Image

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    # Fallback progress indicator
    class tqdm:
        def __init__(self, iterable, desc="", unit="", disable=False):
            self.iterable = iterable
            self.desc = desc
            
        def __iter__(self):
            return iter(self.iterable)
        
        def __enter__(self):
            return self
        
        def __exit__(self, *args):
            pass


class VideoThumbnailGenerator:
    """Generates and manages video thumbnails with caching support."""

    IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'}
    
    def __init__(self, thumbnail_dir: Optional[str] = None, max_height: int = 480,
                 min_duration: float = 300.0, skip_cbr: bool = False,
                 max_width: Optional[int] = None):
        """
        Initialize the thumbnail generator.
        
        Args:
            thumbnail_dir: Directory to store thumbnails (default: ~/.video_thumbnail_cache)
            max_height: Maximum height for thumbnails in pixels
            min_duration: Minimum video duration in seconds to generate thumbnails (default: 300 = 5 minutes)
            skip_cbr: Whether to skip CBR (RAR) comic archive processing
            max_width: Optional maximum thumbnail width in pixels
        """
        self.logger = logging.getLogger(__name__)
        if thumbnail_dir is None:
            self.thumbnail_dir = os.path.expanduser("~/.video_thumbnail_cache")
        else:
            self.thumbnail_dir = os.path.expanduser(thumbnail_dir)
            
        self.max_height = max_height
        self.max_width = max_width
        self.min_duration = min_duration
        self.skip_cbr = skip_cbr
        os.makedirs(self.thumbnail_dir, exist_ok=True)
        self.index_path = os.path.join(self.thumbnail_dir, "thumbnail_index.json")
        self._thumbnail_index_cache: Optional[List[Dict[str, Any]]] = None

    def _get_video_filename(self, video_path: str) -> str:
        return os.path.basename(os.path.normpath(str(video_path)))

    def _get_video_cache_metadata(self, video_path: str) -> Dict[str, Any]:
        filename = self._get_video_filename(video_path)
        return {
            "video_filename": filename,
            "cache_key": hashlib.sha256(filename.encode("utf-8")).hexdigest(),
        }

    def _build_thumbnail_entry(
        self,
        video_path: str,
        static_thumbnail: Optional[str],
        animated_thumbnail: Optional[str],
    ) -> Dict[str, Any]:
        entry = {
            "video": str(video_path),
            "static_thumbnail": static_thumbnail,
            "animated_thumbnail": animated_thumbnail,
        }

        try:
            entry.update(self._get_video_cache_metadata(video_path))
        except OSError:
            entry["video_filename"] = self._get_video_filename(video_path)

        return entry

    def _resolve_thumbnail_index_path(self, thumbnail_path: Optional[str]) -> Optional[str]:
        if not thumbnail_path:
            return None

        normalized_path = os.path.normpath(str(thumbnail_path))
        if os.path.isabs(normalized_path) and os.path.exists(normalized_path):
            return normalized_path

        cache_local_path = os.path.join(self.thumbnail_dir, os.path.basename(normalized_path))
        if os.path.exists(cache_local_path):
            return cache_local_path

        if os.path.isabs(normalized_path):
            return normalized_path

        return cache_local_path

    def _load_thumbnail_index_cache(self) -> List[Dict[str, Any]]:
        if self._thumbnail_index_cache is not None:
            return self._thumbnail_index_cache

        try:
            if os.path.exists(self.index_path):
                with open(self.index_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        self._thumbnail_index_cache = data
                        return data
        except Exception as e:
            print(f"Warning: Could not load thumbnail index from {self.index_path}: {e}")

        self._thumbnail_index_cache = []
        return self._thumbnail_index_cache

    def _get_cached_thumbnail_status(self, static_thumb: str, animated_thumb: str) -> tuple[bool, bool]:
        return os.path.exists(static_thumb), os.path.exists(animated_thumb)

    def _materialize_cached_thumbnails_for_entry(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        entry_filename = entry.get("video_filename") or entry.get("video", "")
        if not entry_filename:
            return dict(entry)

        static_thumb, animated_thumb = self._get_thumbnail_paths(entry_filename)

        materialized_entry = dict(entry)
        materialized_entry["video_filename"] = self._get_video_filename(entry_filename)
        materialized_entry["static_thumbnail"] = static_thumb if os.path.exists(static_thumb) else None
        materialized_entry["animated_thumbnail"] = animated_thumb if os.path.exists(animated_thumb) else None
        return materialized_entry

    def _thumbnail_index_entry_key(self, entry: Dict[str, Any]) -> str:
        return entry.get("video_filename") or self._get_video_filename(entry.get("video", ""))

    def _merge_serialized_thumbnail_index(
        self,
        serialized_index: List[Dict[str, Any]],
        existing_index: Optional[List[Dict[str, Any]]] = None,
    ) -> List[Dict[str, Any]]:
        merged_by_key: Dict[str, Dict[str, Any]] = {}
        ordered_keys: List[str] = []

        for source_index in (existing_index or [], serialized_index):
            for entry in source_index:
                entry_key = self._thumbnail_index_entry_key(entry)
                if not entry_key:
                    continue
                if entry_key not in merged_by_key:
                    ordered_keys.append(entry_key)
                merged_by_key[entry_key] = entry

        return [merged_by_key[entry_key] for entry_key in ordered_keys]

    def _serialize_thumbnail_index_entry(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        video_filename = entry.get("video_filename") or self._get_video_filename(entry.get("video", ""))
        static_thumbnail = entry.get("static_thumbnail")
        animated_thumbnail = entry.get("animated_thumbnail")
        return {
            "video": video_filename,
            "static_thumbnail": os.path.basename(static_thumbnail) if static_thumbnail else None,
            "animated_thumbnail": os.path.basename(animated_thumbnail) if animated_thumbnail else None,
        }
    
    def _thumbnail_cache_key(self, video_path: str) -> str:
        """Build a cache key from the full filename only."""
        return self._get_video_cache_metadata(video_path)["cache_key"]

    def _is_image_file(self, file_path: str) -> bool:
        return os.path.splitext(str(file_path))[1].lower() in self.IMAGE_EXTENSIONS

    def _get_thumbnail_paths(self, video_path: str, static_extension: str = 'webp',
                             animated_extension: str = 'webp') -> tuple[str, str]:
        """Get the static and animated thumbnail paths for a video file."""
        h = self._thumbnail_cache_key(video_path)
        static_thumb = os.path.join(self.thumbnail_dir, f"{h}_static.{static_extension.lstrip('.')}")
        animated_thumb = os.path.join(self.thumbnail_dir, f"{h}_video.{animated_extension.lstrip('.')}")
        return static_thumb, animated_thumb

    def _scale_filter(self) -> str:
        if self.max_width is not None:
            return f"scale={self.max_width}:{self.max_height}:force_original_aspect_ratio=decrease"
        return f"scale=-2:{self.max_height}"

    def _resize_image(self, image: Image.Image) -> Image.Image:
        working_image = image.copy()

        if self.max_width is not None:
            working_image.thumbnail((self.max_width, self.max_height), Image.Resampling.LANCZOS)
        else:
            width, height = working_image.size
            if height > self.max_height:
                new_height = self.max_height
                new_width = int(width * (new_height / height))
                working_image = working_image.resize((new_width, new_height), Image.Resampling.LANCZOS)

        if working_image.mode in ('RGBA', 'LA'):
            background = Image.new('RGB', working_image.size, (255, 255, 255))
            background.paste(working_image, mask=working_image.split()[-1])
            return background
        if working_image.mode != 'RGB':
            return working_image.convert('RGB')
        return working_image

    def _save_static_image(self, image: Image.Image, output_path: str) -> None:
        output_ext = os.path.splitext(output_path)[1].lower()
        resized = self._resize_image(image)
        if output_ext in ('.jpg', '.jpeg'):
            resized.save(output_path, 'JPEG', quality=85)
        else:
            resized.save(output_path, 'WEBP', quality=75)

    def _generate_image_thumbnail(self, image_path: str, static_thumb_path: str,
                                  verbose: int = 1) -> bool:
        try:
            with Image.open(image_path) as image:
                self._save_static_image(image, static_thumb_path)
            return True
        except Exception as e:
            if verbose >= 2:
                print(f"Failed to generate image thumbnail for {image_path}: {e}")
            return False
    
    def _get_video_duration(self, video_path: str, verbose: int = 1) -> Optional[float]:
        """Get video duration in seconds using ffprobe."""
        try:
            cmd = [
                "ffprobe", "-v", "error", "-show_entries", "format=duration", 
                "-of", "default=noprint_wrappers=1:nokey=1", video_path
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            duration = float(result.stdout.strip())
            return duration if duration > 0 else None
        except Exception as e:
            if verbose >= 2:
                print(f"Could not get duration for {video_path}: {e}")
            return None
    
    def _generate_static_thumbnail(self, video_path: str, static_thumb_path: str, 
                                    duration: float, verbose: int = 1) -> bool:
        """Generate a static thumbnail at 20% into the video."""
        static_time = duration * 0.2
        static_ext = os.path.splitext(static_thumb_path)[1].lower()
        static_cmd = [
            "ffmpeg", "-y", "-ss", str(static_time), "-i", video_path,
            "-vframes", "1", "-vf", self._scale_filter(),
        ]
        if static_ext in ('.jpg', '.jpeg'):
            static_cmd.extend(["-f", "image2", "-q:v", "3", static_thumb_path])
        else:
            static_cmd.extend(["-f", "webp", "-quality", "75", static_thumb_path])
        try:
            subprocess.run(static_cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            return True
        except subprocess.CalledProcessError as e:
            if verbose >= 2:
                error_msg = e.stderr.decode(errors='ignore') if e.stderr else str(e)
                print(f"Failed to generate static thumbnail for {video_path}: {e}\\nffmpeg stderr:\\n{error_msg}")
            return False
    
    def _generate_animated_thumbnail(self, video_path: str, animated_thumb_path: str, 
                                    duration: float, verbose: int = 1) -> bool:
        """Generate an animated thumbnail with frames from 5-95% of the video."""
        start_time = time.time()
        frame_times = [duration * (i / 100) for i in range(5, 100, 5)]
        
        with tempfile.TemporaryDirectory() as tmpdir:
            frame_files = []
            
            # Extract individual frames with optimized ffmpeg flags
            for idx, t in enumerate(frame_times):
                frame_file = os.path.join(tmpdir, f"frame_{idx:02d}.webp")
                frame_cmd = [
                    "ffmpeg", "-y",
                    #"-hwaccel", "auto", # Hardware acceleration does not seem to improve time here
                    "-noaccurate_seek",
                    "-ss", str(t),
                    "-i", video_path,
                    "-vframes", "1",
                    "-vf", self._scale_filter(),
                    "-f", "webp",
                    "-quality", "75",
                    frame_file
                ]
                try:
                    subprocess.run(frame_cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    if os.path.exists(frame_file) and os.path.getsize(frame_file) > 0:
                        frame_files.append(frame_file)
                    elif verbose >= 2:
                        print(f"Frame {idx} at {t:.2f}s could not be extracted (empty file, skipping).")
                except subprocess.CalledProcessError as e:
                    if verbose >= 2:
                        error_msg = e.stderr.decode(errors='ignore') if e.stderr else str(e)
                        print(f"Failed to extract frame {idx} for {video_path}: {e}\\nffmpeg stderr:\\n{error_msg}")
            
            # Combine frames into animated WebP with optimized settings
            if frame_files:
                anim_cmd = [
                    "ffmpeg", "-y",
                    #"-hwaccel", "auto", # Hardware acceleration does not seem to improve time here
                    "-framerate", "2",
                    "-i", os.path.join(tmpdir, "frame_%02d.webp"),
                    "-vf", self._scale_filter(),
                    "-loop", "0", 
                    "-quality", "75",
                    "-f", "webp",
                    animated_thumb_path
                ]
                try:
                    subprocess.run(anim_cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    elapsed = time.time() - start_time
                    if verbose >= 3:
                        print(f"DEBUG2: Generated animated thumbnail for {os.path.basename(video_path)} in {elapsed:.2f}s")
                    return True
                except subprocess.CalledProcessError as e:
                    if verbose >= 2:
                        error_msg = e.stderr.decode(errors='ignore') if e.stderr else str(e)
                        print(f"Failed to generate animated thumbnail for {video_path}: {e}\\nffmpeg stderr:\\n{error_msg}")
            
            elapsed = time.time() - start_time
            if verbose >= 3:
                print(f"DEBUG2: Failed to generate animated thumbnail for {os.path.basename(video_path)} after {elapsed:.2f}s")
            return False
    
    def _generate_comic_thumbnail(self, comic_path: str, verbose: int = 1,
                                  force_regenerate: bool = False) -> Dict[str, Any]:
        """Generate thumbnail for comic book archive (CBR/CBZ)."""
        import zipfile
        
        static_thumb, animated_thumb = self._get_thumbnail_paths(comic_path)
        static_exists = os.path.exists(static_thumb)
        
        # Return existing thumbnail if it exists and we're not forcing regeneration
        if not force_regenerate and static_exists:
            return self._build_thumbnail_entry(comic_path, static_thumb, None)
        
        try:
            # Try to use PIL for image processing
            try:
                from PIL import Image
            except ImportError:
                self.logger.warning(f"PIL/Pillow not available for comic thumbnail generation: {comic_path}")
                return self._build_thumbnail_entry(comic_path, None, None)
            
            ext = comic_path.lower()
            image_data = None
            
            # Handle CBZ (ZIP) archives
            if ext.endswith('.cbz'):
                with zipfile.ZipFile(comic_path, 'r') as archive:
                    # Get list of image files
                    image_files = [f for f in archive.namelist() 
                                 if f.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'))
                                 and not f.startswith('__MACOSX/')]
                    
                    if not image_files:
                        if verbose >= 1:
                            print(f"No images found in comic archive: {comic_path}")
                        return self._build_thumbnail_entry(comic_path, None, None)
                    
                    # Sort to get first page
                    image_files.sort()
                    
                    # Read first image
                    with archive.open(image_files[0]) as img_file:
                        image_data = img_file.read()
            
            # Handle CBR (RAR) archives
            elif ext.endswith('.cbr'):
                # Try libarchive first (fast, native library)
                try:
                    import libarchive
                    
                    image_files = []
                    
                    # First pass: collect image filenames
                    try:
                        with libarchive.file_reader(comic_path) as archive:
                            for entry in archive:
                                try:
                                    filename = entry.pathname
                                    
                                    # Skip if pathname is None or not a string
                                    if not filename or not isinstance(filename, str):
                                        continue
                                    
                                    if (filename.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'))
                                        and not filename.startswith('__MACOSX/')):
                                        image_files.append(filename)
                                except Exception:
                                    continue
                    except Exception as e:
                        self.logger.error(f"Error reading CBR archive with libarchive: {e}")
                        raise  # Re-raise to trigger fallback to rarfile
                    
                    if not image_files:
                        self.logger.warning(f"No images found in comic archive: {comic_path}")
                        return self._build_thumbnail_entry(comic_path, None, None)
                    
                    # Sort to get first page
                    image_files.sort()
                    first_image = image_files[0]
                    
                    # Second pass: extract first image data
                    with libarchive.file_reader(comic_path) as archive:
                        for entry in archive:
                            if entry.pathname == first_image:
                                image_data = b''.join(entry.get_blocks())
                                break
                    
                except ImportError:
                    # Fallback to rarfile
                    try:
                        import rarfile
                        with rarfile.RarFile(comic_path, 'r') as archive:
                            # Get list of image files
                            image_files = [f for f in archive.namelist() 
                                         if f.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'))
                                         and not f.startswith('__MACOSX/')]
                            
                            if not image_files:
                                self.logger.warning(f"No images found in comic archive: {comic_path}")
                                return self._build_thumbnail_entry(comic_path, None, None)
                            
                            # Sort to get first page
                            image_files.sort()
                            
                            # Read first image
                            with archive.open(image_files[0]) as img_file:
                                image_data = img_file.read()
                    except ImportError:
                        self.logger.warning(f"No RAR library available for CBR extraction: {comic_path}")
                        return self._build_thumbnail_entry(comic_path, None, None)
            
            if image_data:
                with Image.open(BytesIO(image_data)) as img:
                    self._save_static_image(img, static_thumb)
                
                if verbose >= 2:
                    print(f"Generated comic thumbnail: {static_thumb}")
                return self._build_thumbnail_entry(comic_path, static_thumb, None)
            
        except Exception as e:
            if verbose >= 1:
                print(f"Failed to generate comic thumbnail for {comic_path}: {e}")
            return self._build_thumbnail_entry(comic_path, None, None)
        
        return self._build_thumbnail_entry(comic_path, None, None)
    
    def generate_thumbnail_for_video(self, video_path: str, verbose: int = 1, 
                                    force_regenerate: bool = False) -> Dict[str, Any]:
        """
        Generate thumbnails for a single video or comic file.
        
        Args:
            video_path: Path to the video or comic file
            verbose: Verbosity level (0=silent, 1=errors, 2=detailed)
            force_regenerate: Force regeneration even if thumbnails exist
            
        Returns:
            Dictionary with video path and thumbnail paths (None if generation failed)
        """
        video_path_str = str(video_path)
        
        # Check if this is a comic file
        if video_path_str.lower().endswith(('.cbr', '.cbz')):
            # Skip CBR files if skip_cbr is enabled
            if self.skip_cbr and video_path_str.lower().endswith('.cbr'):
                self.logger.debug(f"Skipping CBR thumbnail generation (--skip-cbr enabled): {os.path.basename(video_path_str)}")
                return self._build_thumbnail_entry(video_path_str, None, None)
            return self._generate_comic_thumbnail(video_path_str, verbose, force_regenerate)
        
        static_thumb, animated_thumb = self._get_thumbnail_paths(video_path_str)
        
        static_exists, animated_exists = self._get_cached_thumbnail_status(static_thumb, animated_thumb)
        
        # Return existing thumbnails if they exist and we're not forcing regeneration
        if not force_regenerate and static_exists and animated_exists:
            return self._build_thumbnail_entry(video_path_str, static_thumb, animated_thumb)
        
        # Get video duration
        duration = self._get_video_duration(video_path_str, verbose)
        if duration is None:
            if verbose >= 1:
                print(f"Invalid or missing duration for {video_path_str}")
            return self._build_thumbnail_entry(video_path_str, None, None)
        
        # Generate static thumbnail if needed (always generate regardless of duration)
        static_success = static_exists
        if not static_exists or force_regenerate:
            static_success = self._generate_static_thumbnail(video_path_str, static_thumb, duration, verbose)
        
        # Generate animated thumbnail only for videos longer than minimum duration
        animated_success = animated_exists
        if duration >= self.min_duration:
            if not animated_exists or force_regenerate:
                animated_success = self._generate_animated_thumbnail(video_path_str, animated_thumb, duration, verbose)
        else:
            # Skip animated thumbnail for short videos
            if verbose >= 2:
                print(f"Skipping animated thumbnail for {video_path_str}: duration {duration:.1f}s is less than minimum {self.min_duration:.1f}s")
            animated_success = False
        
        return self._build_thumbnail_entry(
            video_path_str,
            static_thumb if static_success else None,
            animated_thumb if animated_success else None,
        )

    def ensure_static_thumbnail(self, video_path: str, output_extension: str = 'webp',
                                verbose: int = 1, force_regenerate: bool = False) -> tuple[Optional[str], bool]:
        """Ensure a cached static thumbnail exists in the requested format."""
        video_path_str = str(video_path)
        static_thumb, _ = self._get_thumbnail_paths(
            video_path_str,
            static_extension=output_extension,
            animated_extension='webp',
        )

        if not force_regenerate and os.path.exists(static_thumb):
            return static_thumb, True

        if self._is_image_file(video_path_str):
            success = self._generate_image_thumbnail(video_path_str, static_thumb, verbose)
            return (static_thumb if success else None), False

        if video_path_str.lower().endswith(('.cbr', '.cbz')) and output_extension.lower() == 'webp':
            result = self._generate_comic_thumbnail(video_path_str, verbose, force_regenerate)
            static_result = result.get('static_thumbnail')
            return (static_result if static_result else None), False

        duration = self._get_video_duration(video_path_str, verbose)
        if duration is None:
            if verbose >= 1:
                print(f"Invalid or missing duration for {video_path_str}")
            return None, False

        success = self._generate_static_thumbnail(video_path_str, static_thumb, duration, verbose)
        return (static_thumb if success else None), False
    
    def generate_thumbnails_for_videos(self, video_paths, 
                                     verbose: int = 1, force_regenerate: bool = False,
                                     show_progress: bool = True) -> List[Dict[str, Any]]:
        """
        Generate thumbnails for multiple video files.
        
        Args:
            video_paths: List of video file paths
            verbose: Verbosity level (0=silent, 1=errors, 2=detailed)
            force_regenerate: Force regeneration even if thumbnails exist
            show_progress: Show progress bar
            
        Returns:
            List of dictionaries with video paths and thumbnail paths
        """
        # First pass: check which videos need thumbnail generation
        videos_needing_generation = []
        existing_thumbnails = {}
        
        if verbose >= 1:
            print(f"Checking existing thumbnails for {len(video_paths)} videos...")
        
        for video_path in video_paths:
            video_path_str = str(video_path)
            static_thumb, animated_thumb = self._get_thumbnail_paths(video_path_str)
            static_exists, animated_exists = self._get_cached_thumbnail_status(static_thumb, animated_thumb)
            
            if force_regenerate or not (static_exists and animated_exists):
                videos_needing_generation.append(video_path_str)
            else:
                # Store existing thumbnails
                existing_thumbnails[video_path_str] = self._build_thumbnail_entry(
                    video_path_str,
                    static_thumb,
                    animated_thumb,
                )
        
        # Report what we found
        if verbose >= 1:
            existing_count = len(existing_thumbnails)
            generation_count = len(videos_needing_generation)
            if existing_count > 0:
                print(f"Found {existing_count} videos with existing thumbnails")
            if generation_count > 0:
                print(f"Generating thumbnails for {generation_count} videos...")
            elif generation_count == 0:
                print("All thumbnails already exist")
        
        # Second pass: generate thumbnails only for videos that need them
        generated_thumbnails = {}
        if videos_needing_generation:
            progress_iter = tqdm(videos_needing_generation, desc="Generating thumbnails", unit="file", 
                               disable=not show_progress or verbose < 1) if show_progress else videos_needing_generation
            
            for video_path in progress_iter:
                result = self.generate_thumbnail_for_video(video_path, verbose, force_regenerate)
                generated_thumbnails[video_path] = result
        
        # Combine results: maintain original order and include all videos
        thumbnail_index = []
        for video_path in video_paths:
            video_path_str = str(video_path)
            if video_path_str in existing_thumbnails:
                thumbnail_index.append(existing_thumbnails[video_path_str])
            elif video_path_str in generated_thumbnails:
                thumbnail_index.append(generated_thumbnails[video_path_str])
            else:
                # Fallback: should not happen, but handle gracefully
                thumbnail_index.append(self._build_thumbnail_entry(video_path_str, None, None))

        self.save_thumbnail_index(thumbnail_index, verbose=0)
        
        return thumbnail_index
    
    def load_thumbnail_index(self, index_path: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Load existing thumbnail index from JSON file.
        
        Args:
            index_path: Path to index JSON file (default: thumbnail_dir/thumbnail_index.json)
            
        Returns:
            List of thumbnail entries, empty list if file doesn't exist
        """
        if index_path is None:
            index_path = self.index_path
        
        try:
            if os.path.exists(index_path):
                with open(index_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    normalized_entries = []
                    for entry in data:
                        normalized_entry = self._materialize_cached_thumbnails_for_entry(entry)
                        normalized_entry["static_thumbnail"] = self._resolve_thumbnail_index_path(normalized_entry.get("static_thumbnail"))
                        normalized_entry["animated_thumbnail"] = self._resolve_thumbnail_index_path(normalized_entry.get("animated_thumbnail"))
                        normalized_entries.append(normalized_entry)
                    self._thumbnail_index_cache = data if isinstance(data, list) else []
                    return normalized_entries
        except Exception as e:
            print(f"Warning: Could not load thumbnail index from {index_path}: {e}")
        
        return []
    
    def save_thumbnail_index(self, thumbnail_index: List[Dict[str, Any]], 
                            index_path: Optional[str] = None, verbose: int = 1) -> None:
        """
        Save thumbnail index to JSON file.
        
        Args:
            thumbnail_index: List of thumbnail entries to save
            index_path: Path to index JSON file (default: thumbnail_dir/thumbnail_index.json)
            verbose: Verbosity level for logging
        """
        if index_path is None:
            index_path = self.index_path
        
        try:
            serialized_index = [self._serialize_thumbnail_index_entry(entry) for entry in thumbnail_index]
            existing_serialized_index: List[Dict[str, Any]] = []
            if index_path == self.index_path:
                existing_serialized_index = self._load_thumbnail_index_cache()
            elif os.path.exists(index_path):
                with open(index_path, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
                    if isinstance(existing_data, list):
                        existing_serialized_index = existing_data

            merged_index = self._merge_serialized_thumbnail_index(serialized_index, existing_serialized_index)
            with open(index_path, 'w', encoding='utf-8') as f:
                json.dump(merged_index, f, indent=2, ensure_ascii=False)
            if index_path == self.index_path:
                self._thumbnail_index_cache = merged_index
            if verbose >= 1:
                print(f"Thumbnail index written to {index_path}")
        except Exception as e:
            print(f"Error: Could not save thumbnail index to {index_path}: {e}")
    
    def get_thumbnail_for_video(self, video_path: str) -> Dict[str, Any]:
        """
        Get thumbnail paths for a video without generating them.
        
        Args:
            video_path: Path to the video file
            
        Returns:
            Dictionary with video path and existing thumbnail paths (None if not found)
        """
        video_path_str = str(video_path)
        static_thumb, animated_thumb = self._get_thumbnail_paths(video_path_str)
        static_exists, animated_exists = self._get_cached_thumbnail_status(static_thumb, animated_thumb)
        
        return self._build_thumbnail_entry(
            video_path_str,
            static_thumb if static_exists else None,
            animated_thumb if animated_exists else None,
        )


