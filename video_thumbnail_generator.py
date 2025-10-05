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
from pathlib import Path
from typing import List, Dict, Any, Optional, Union

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
    
    def __init__(self, thumbnail_dir: Optional[str] = None, max_height: int = 480):
        """
        Initialize the thumbnail generator.
        
        Args:
            thumbnail_dir: Directory to store thumbnails (default: ~/.video_thumbnail_cache)
            max_height: Maximum height for thumbnails in pixels
        """
        if thumbnail_dir is None:
            self.thumbnail_dir = os.path.expanduser("~/.video_thumbnail_cache")
        else:
            self.thumbnail_dir = os.path.expanduser(thumbnail_dir)
            
        self.max_height = max_height
        os.makedirs(self.thumbnail_dir, exist_ok=True)
    
    def _get_thumbnail_paths(self, video_path: str) -> tuple[str, str]:
        """Get the static and animated thumbnail paths for a video file."""
        h = hashlib.sha256(video_path.encode("utf-8")).hexdigest()
        static_thumb = os.path.join(self.thumbnail_dir, f"{h}_static.webp")
        animated_thumb = os.path.join(self.thumbnail_dir, f"{h}_video.webp")
        return static_thumb, animated_thumb
    
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
        static_cmd = [
            "ffmpeg", "-y", "-ss", str(static_time), "-i", video_path,
            "-vframes", "1", "-vf", f"scale=-2:{self.max_height}", "-f", "webp", static_thumb_path
        ]
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
        frame_times = [duration * (i / 100) for i in range(5, 100, 5)]
        
        with tempfile.TemporaryDirectory() as tmpdir:
            frame_files = []
            
            # Extract individual frames
            for idx, t in enumerate(frame_times):
                frame_file = os.path.join(tmpdir, f"frame_{idx:02d}.webp")
                frame_cmd = [
                    "ffmpeg", "-y", "-noaccurate_seek", "-ss", str(t), "-i", video_path,
                    "-vframes", "1", "-vf", f"scale=-2:{self.max_height}", "-f", "webp", frame_file
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
            
            # Combine frames into animated WebP
            if frame_files:
                anim_cmd = [
                    "ffmpeg", "-y", "-framerate", "2", "-i", os.path.join(tmpdir, "frame_%02d.webp"),
                    "-vf", f"scale=-2:{self.max_height}", "-loop", "0", "-f", "webp", animated_thumb_path
                ]
                try:
                    subprocess.run(anim_cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    return True
                except subprocess.CalledProcessError as e:
                    if verbose >= 2:
                        error_msg = e.stderr.decode(errors='ignore') if e.stderr else str(e)
                        print(f"Failed to generate animated thumbnail for {video_path}: {e}\\nffmpeg stderr:\\n{error_msg}")
            
            return False
    
    def generate_thumbnail_for_video(self, video_path: str, verbose: int = 1, 
                                   force_regenerate: bool = False) -> Dict[str, Any]:
        """
        Generate thumbnails for a single video file.
        
        Args:
            video_path: Path to the video file
            verbose: Verbosity level (0=silent, 1=errors, 2=detailed)
            force_regenerate: Force regeneration even if thumbnails exist
            
        Returns:
            Dictionary with video path and thumbnail paths (None if generation failed)
        """
        video_path_str = str(video_path)
        static_thumb, animated_thumb = self._get_thumbnail_paths(video_path_str)
        
        static_exists = os.path.exists(static_thumb)
        animated_exists = os.path.exists(animated_thumb)
        
        # Return existing thumbnails if they exist and we're not forcing regeneration
        if not force_regenerate and static_exists and animated_exists:
            return {
                "video": video_path_str,
                "static_thumbnail": static_thumb,
                "animated_thumbnail": animated_thumb
            }
        
        # Get video duration
        duration = self._get_video_duration(video_path_str, verbose)
        if duration is None:
            if verbose >= 1:
                print(f"Invalid or missing duration for {video_path_str}")
            return {
                "video": video_path_str,
                "static_thumbnail": None,
                "animated_thumbnail": None
            }
        
        # Generate static thumbnail if needed
        static_success = static_exists
        if not static_exists or force_regenerate:
            static_success = self._generate_static_thumbnail(video_path_str, static_thumb, duration, verbose)
        
        # Generate animated thumbnail if needed
        animated_success = animated_exists
        if not animated_exists or force_regenerate:
            animated_success = self._generate_animated_thumbnail(video_path_str, animated_thumb, duration, verbose)
        
        return {
            "video": video_path_str,
            "static_thumbnail": static_thumb if static_success else None,
            "animated_thumbnail": animated_thumb if animated_success else None
        }
    
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
            
            static_exists = os.path.exists(static_thumb)
            animated_exists = os.path.exists(animated_thumb)
            
            if force_regenerate or not (static_exists and animated_exists):
                videos_needing_generation.append(video_path_str)
            else:
                # Store existing thumbnails
                existing_thumbnails[video_path_str] = {
                    "video": video_path_str,
                    "static_thumbnail": static_thumb,
                    "animated_thumbnail": animated_thumb
                }
        
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
                thumbnail_index.append({
                    "video": video_path_str,
                    "static_thumbnail": None,
                    "animated_thumbnail": None
                })
        
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
            index_path = os.path.join(self.thumbnail_dir, "thumbnail_index.json")
        
        try:
            if os.path.exists(index_path):
                with open(index_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
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
            index_path = os.path.join(self.thumbnail_dir, "thumbnail_index.json")
        
        try:
            with open(index_path, 'w', encoding='utf-8') as f:
                json.dump(thumbnail_index, f, indent=2, ensure_ascii=False)
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
        
        return {
            "video": video_path_str,
            "static_thumbnail": static_thumb if os.path.exists(static_thumb) else None,
            "animated_thumbnail": animated_thumb if os.path.exists(animated_thumb) else None
        }


