import pathlib
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Any

@dataclass
class FormatInfo:
    """Represents details about a specific media format."""
    format_id: str
    ext: str
    note: Optional[str] = None
    vcodec: Optional[str] = None
    acodec: Optional[str] = None
    height: Optional[int] = None
    width: Optional[int] = None
    fps: Optional[int] = None
    abr: Optional[float] = None # Audio Bitrate (kbps)
    vbr: Optional[float] = None # Video Bitrate (kbps)
    filesize: Optional[int] = None # Bytes
    filesize_approx: Optional[int] = None # Bytes
    filesize_str: Optional[str] = None # Pre-formatted string from yt-dlp
    filesize_approx_str: Optional[str] = None # Pre-formatted string from yt-dlp
    # Store the raw dictionary for potential future use or debugging
    raw_data: Dict[str, Any] = field(default_factory=dict, repr=False)

    def __str__(self) -> str:
        """Provides a user-friendly string representation."""
        details = []
        if self.note: details.append(self.note)
        elif self.height: details.append(f"{self.height}p")
        if self.fps: details.append(f"{self.fps}fps")
        if self.vcodec and self.vcodec != 'none': details.append(self.vcodec)
        if self.acodec and self.acodec != 'none': details.append(self.acodec)
        if self.abr: details.append(f"{self.abr:.0f}k")
        elif self.vbr: details.append(f"{self.vbr:.0f}k")
        if self.filesize_str: details.append(self.filesize_str)
        elif self.filesize_approx_str: details.append(f"~{self.filesize_approx_str}")

        details_str = ' / '.join(filter(None, details))
        return f"[{self.format_id}] {self.ext} - {details_str}"

    def to_dict(self) -> Dict[str, Any]:
        """Converts the FormatInfo object to a dictionary suitable for JSON serialization."""
        # Use dataclasses.asdict to get most fields, excluding raw_data by default
        # if we keep repr=False for it. Or manually build if more control is needed.

        # Manual approach for explicit control and filtering None:
        data = {
            "format_id": self.format_id,
            "ext": self.ext,
            "note": self.note,
            "vcodec": self.vcodec,
            "acodec": self.acodec,
            "height": self.height,
            "width": self.width,
            "fps": self.fps,
            "abr": self.abr,
            "vbr": self.vbr,
            "filesize": self.filesize,
            "filesize_approx": self.filesize_approx,
            "filesize_str": self.filesize_str,
            "filesize_approx_str": self.filesize_approx_str,
            # Add other fields if needed, but avoid 'raw_data' unless necessary
        }
        # Filter out keys where the value is None for cleaner JSON
        return {k: v for k, v in data.items() if v is not None}

@dataclass
class DownloadItem:
    """Represents a video/audio item to be downloaded."""
    url: str
    # --- Metadata (populated after fetching info) ---
    title: Optional[str] = None
    duration: Optional[int] = None # Seconds
    artist: Optional[str] = None
    year: Optional[int] = None
    description: Optional[str] = None
    # --- Format Information (populated after fetching info) ---
    # Store lists of FormatInfo objects
    audio_formats: List[FormatInfo] = field(default_factory=list)
    video_formats: List[FormatInfo] = field(default_factory=list) # Includes combined formats
    # --- Download State ---
    status: str = "Queued" # e.g., Queued, Fetching, Pending, Downloading, Processing, Complete, Error, Cancelled, Skipped
    progress: float = 0.0 # Percentage (0.0 to 100.0) or bytes downloaded if total unknown? Let's stick to percentage.
    error: Optional[str] = None
    # --- User Selection ---
    selected_audio_format_id: Optional[str] = None
    selected_video_format_id: Optional[str] = None
    is_selected: bool = False
    # --- Output ---
    # The final path is determined during download based on output_dir and sanitized title
    final_filepath: Optional[pathlib.Path] = None
    # Store a reference to the original raw info dict if needed later
    _raw_info: Dict[str, Any] = field(default_factory=dict, repr=False)

    # --- Helper properties for selected format details (read-only) ---
    @property
    def selected_audio_format(self) -> Optional[FormatInfo]:
        if not self.selected_audio_format_id: return None
        # Search in both audio-only and video formats (for combined)
        for f in self.audio_formats + self.video_formats:
            if f.format_id == self.selected_audio_format_id:
                return f
        return None

    @property
    def selected_video_format(self) -> Optional[FormatInfo]:
        if not self.selected_video_format_id: return None
        for f in self.video_formats:
            if f.format_id == self.selected_video_format_id:
                return f
        return None

    @property
    def selected_audio_details(self) -> str:
        fmt = self.selected_audio_format
        if not fmt: return ""
        # Special case: audio is from the selected video format
        if self.selected_video_format_id == self.selected_audio_format_id:
            return "(from video)"
        return f"{fmt.ext} {fmt.acodec} {fmt.abr or 0:.0f}k"

    @property
    def selected_video_details(self) -> str:
        fmt = self.selected_video_format
        if not fmt: return ""
        return f"{fmt.ext} {fmt.height or '?'}p {fmt.vcodec or '?'}"

