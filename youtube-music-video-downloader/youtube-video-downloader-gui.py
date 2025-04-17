import os
import sys
import argparse
import asyncio
import urwid
import yt_dlp
import logging
import pathlib
import re
import locale
import shutil
import tempfile
import atexit
from datetime import timedelta
from logging.handlers import RotatingFileHandler
from moviepy.editor import VideoFileClip # Consider lazy import if startup time is critical
import eyed3 # Consider lazy import
from typing import List, Dict, Optional
import functools

# Store original terminal settings
original_cp = None
original_output_cp = None
original_stdout_encoding = None

def store_terminal_state():
    """Store original terminal settings"""
    global original_cp, original_output_cp, original_stdout_encoding
    if sys.platform == 'win32':
        try:
            import ctypes
            kernel32 = ctypes.windll.kernel32
            original_cp = kernel32.GetConsoleCP()
            original_output_cp = kernel32.GetConsoleOutputCP()
            original_stdout_encoding = sys.stdout.encoding
            logger.debug(f"Stored original terminal state: CP={original_cp}, Output CP={original_output_cp}, Encoding={original_stdout_encoding}")
        except Exception as e:
            logger.warning(f"Could not store terminal state: {e}")

def restore_terminal_state():
    """Restore original terminal settings"""
    if sys.platform == 'win32' and (original_cp is not None or original_output_cp is not None):
        try:
            import ctypes
            kernel32 = ctypes.windll.kernel32
            if original_cp is not None:
                kernel32.SetConsoleCP(original_cp)
            if original_output_cp is not None:
                kernel32.SetConsoleOutputCP(original_output_cp)
            # Check if stdout exists and needs reconfiguration
            if hasattr(sys, 'stdout') and sys.stdout and original_stdout_encoding is not None and original_stdout_encoding != sys.stdout.encoding:
                 try:
                     sys.stdout.reconfigure(encoding=original_stdout_encoding)
                     logger.debug("Restored original terminal state (stdout encoding)")
                 except Exception as e:
                     logger.warning(f"Could not restore stdout encoding: {e}")
            else:
                 logger.debug("Restored original terminal state (CP/OutputCP only)")

        except Exception as e:
            logger.warning(f"Could not restore terminal state: {e}")

def sanitize_filename(filename: str) -> str:
    """Sanitize filename for Windows compatibility"""
    # Remove invalid characters
    filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    # Remove control characters (0-31) except tab (9), newline (10), carriage return (13)
    filename = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F]', '', filename)
    # Replace leading/trailing spaces/periods in components
    parts = filename.split(os.path.sep)
    sanitized_parts = []
    for part in parts:
        # Strip leading/trailing dots and spaces
        part = part.strip('. ')
        # Replace reserved names (Windows) - case-insensitive
        reserved_names = {'CON', 'PRN', 'AUX', 'NUL', 'COM1', 'COM2', 'COM3', 'COM4', 'COM5', 'COM6', 'COM7', 'COM8', 'COM9', 'LPT1', 'LPT2', 'LPT3', 'LPT4', 'LPT5', 'LPT6', 'LPT7', 'LPT8', 'LPT9'}
        if part.upper() in reserved_names:
            part = f"_{part}_"
        sanitized_parts.append(part)
    filename = os.path.sep.join(sanitized_parts)

    # Limit length (considering path length limits on Windows)
    # This is complex, so we'll limit the filename part for simplicity
    if len(os.path.basename(filename)) > 200: # Be conservative
        name, ext = os.path.splitext(os.path.basename(filename))
        base_name = name[:200-len(ext)] + ext
        filename = os.path.join(os.path.dirname(filename), base_name) if os.path.dirname(filename) else base_name

    # Ensure it's not empty
    if not filename:
        return "_untitled_"

    return filename

def check_ffmpeg():
    """Check if FFmpeg is available in the system"""
    if not shutil.which('ffmpeg'):
        logger.error("FFmpeg not found. Please install FFmpeg and add it to your system PATH.")
        print("\nError: FFmpeg is required but not found in your system PATH.")
        print("Please install FFmpeg from https://ffmpeg.org/download.html")
        print("and ensure the directory containing ffmpeg.exe is added to your PATH environment variable.")
        sys.exit(1)
    logger.debug(f"FFmpeg found at: {shutil.which('ffmpeg')}")


def setup_windows_console():
    """Setup Windows console for proper encoding"""
    if sys.platform == 'win32':
        try:
            # Store original state before modifying
            store_terminal_state()

            # Set console to UTF-8 mode
            import ctypes
            kernel32 = ctypes.windll.kernel32
            CP_UTF8 = 65001
            set_cp_success = kernel32.SetConsoleCP(CP_UTF8)
            set_output_cp_success = kernel32.SetConsoleOutputCP(CP_UTF8)

            if not set_cp_success or not set_output_cp_success:
                 logger.warning(f"Failed to set console CP to UTF-8. SetConsoleCP returned {set_cp_success}, SetConsoleOutputCP returned {set_output_cp_success}")
                 # Optionally, attempt to restore immediately if setting failed
                 # restore_terminal_state()
                 # return # Or raise an error?

            # Set Python's console encoding
            # Check if stdout is connected to a terminal/console
            if sys.stdout and sys.stdout.isatty() and sys.stdout.encoding != 'utf-8':
                try:
                    sys.stdout.reconfigure(encoding='utf-8')
                    logger.debug("Reconfigured sys.stdout encoding to utf-8")
                except Exception as e:
                    logger.warning(f"Could not reconfigure sys.stdout encoding: {e}")
            elif not (sys.stdout and sys.stdout.isatty()):
                 logger.debug("sys.stdout is not a tty, skipping encoding reconfiguration.")


            # Register cleanup function
            atexit.register(restore_terminal_state)

            logger.debug("Windows console configured for UTF-8")
        except ImportError:
             logger.warning("Could not import ctypes. Console encoding setup skipped.")
        except Exception as e:
            logger.warning(f"Could not set console encoding: {e}")

# Setup logging
def setup_logging():
    log_dir = pathlib.Path(__file__).parent / 'logs'
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / 'youtube_downloader.log'

    # Create formatters
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    )
    # Use a simpler format for console to avoid noise during TUI operation
    console_formatter = logging.Formatter('%(levelname)s: %(message)s')

    # Setup file handler with rotation
    file_handler = RotatingFileHandler(
        log_file, maxBytes=10*1024*1024, backupCount=5, encoding='utf-8'
    )
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(logging.DEBUG) # Log everything to file

    # Setup console handler
    console_handler = logging.StreamHandler(sys.stderr) # Log to stderr to avoid interfering with urwid stdout
    console_handler.setFormatter(console_formatter)
    # Set console level higher by default, can be overridden by args if needed
    console_handler.setLevel(logging.INFO) # Show INFO and above on console

    # Setup root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG) # Process all messages
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    # Silence noisy libraries if needed
    logging.getLogger('urwid').setLevel(logging.WARNING)
    logging.getLogger('eyed3').setLevel(logging.WARNING)
    logging.getLogger('moviepy').setLevel(logging.WARNING)


    return root_logger

logger = setup_logging()

class DownloadItem:
    def __init__(self, url: str, title: str = "", duration: int = 0):
        self.url = url
        self.title = title
        self.duration = duration
        self.progress = 0.0 # Use float for progress
        self.status = "Pending"
        self.download_type = None  # 'audio', 'video', or None
        self.format_info = {'audio': [], 'video': []} # Initialize to avoid potential errors
        self.error = None
        self.widget: Optional[urwid.Widget] = None # Reference to the widget for updates

class DownloaderTUI:
    palette = [
        ('header', 'white', 'dark blue'),
        ('footer', 'white', 'dark blue'),
        ('body', 'default', 'default'), # Default body style
        ('focus', 'black', 'light gray'), # Style for the focused item in ListBox
        ('progress_bar', 'white', 'dark blue'),
        ('progress_normal', 'black', 'light gray'),
        ('progress_complete', 'white', 'dark green'),
        ('progress_error', 'white', 'dark red'),
        ('status_pending', 'yellow', ''),
        ('status_downloading', 'light blue', ''),
        ('status_processing', 'dark cyan', ''),
        ('status_complete', 'dark green', ''),
        ('status_error', 'dark red', ''),
        ('error_text', 'dark red', ''),
        ('title_text', 'white', 'default'),
        ('duration_text', 'dark gray', 'default'),
        ('button', 'white', 'dark blue'),
        ('button_focus', 'black', 'light gray'),
        ('dialog_border', 'black', 'white'),
        ('dialog_body', 'black', 'light gray'),
        ('dialog_shadow', 'white', 'black'),
    ]

    def __init__(self):
        self.downloads: List[DownloadItem] = []
        # self.current_item = 0 # No longer needed, ListBox handles focus
        self.ydl_opts = {
            'quiet': True,
            'no_warnings': True,
            'extract_flat': True, # Keep true for initial fast fetching if needed, override later
            'skip_download': True, # Ensure we only fetch info by default
            'verbose': False, # Keep yt-dlp chatter low
            'ignoreerrors': True, # Don't let one URL failure stop info fetching for others
        }
        self.listbox_walker = urwid.SimpleListWalker([])
        self.loop: Optional[urwid.MainLoop] = None # Initialize loop reference
        self.frame: Optional[urwid.Frame] = None # Initialize frame reference
        self.current_download_tasks = {} # Track running download tasks {url: asyncio.Task}
        self.setup_display()


    def setup_display(self):
        # Header
        self.header = urwid.AttrMap(
            urwid.Text('YouTube Downloader - (↑↓ Navigate) (Enter Select/Download) (q Quit)'),
            'header'
        )

        # Main content area - Use AttrMap for focus styling
        self.listbox = urwid.ListBox(self.listbox_walker)

        # Footer
        self.footer_text = urwid.Text('')
        self.footer = urwid.AttrMap(self.footer_text, 'footer')

        # Layout
        self.frame = urwid.Frame(
            urwid.AttrMap(self.listbox, 'body', focus_map={'focus': 'focus'}), # Apply focus map
            header=self.header,
            footer=self.footer
        )
        self.update_footer()

    def create_download_widget(self, item: DownloadItem) -> urwid.Widget:
        # Title and duration
        title_widget = urwid.Text(('title_text', item.title or "Loading..."))
        duration_str = str(timedelta(seconds=int(item.duration))) if item.duration else "--:--:--"
        duration_widget = urwid.Text(('duration_text', f" ({duration_str})"), align='right')
        header_cols = urwid.Columns([title_widget, ('pack', duration_widget)], dividechars=1)

        # Status and progress
        status_map = {
            "Pending": "status_pending",
            "Downloading": "status_downloading",
            "Processing": "status_processing",
            "Complete": "status_complete",
            "Error": "status_error",
        }
        status_style = status_map.get(item.status, 'error_text') # Default to error if unknown status
        status_text = f"Status: {item.status}"
        if item.download_type:
            status_text += f" | Type: {item.download_type}"
        if item.error:
             status_text += f" | Error: {item.error[:50]}..." # Show truncated error

        status_widget = urwid.AttrMap(urwid.Text(status_text), status_style)

        # Progress bar
        progress_style = 'progress_normal'
        if item.status == "Complete":
            progress_style = 'progress_complete'
        elif item.status == "Error":
            progress_style = 'progress_error'

        progress_bar = urwid.ProgressBar(progress_style, 'progress_bar', # Use different attr for the bar itself
                                         current=item.progress, done=100)

        # Combine into a Pile, wrap with padding and selectable marker
        pile = urwid.Pile([
            header_cols,
            status_widget,
            progress_bar,
        ])
        # Add padding and make it selectable
        widget = urwid.Padding(pile, left=1, right=1)
        # Store widget reference in item for direct updates
        item.widget = widget # Store the outer widget
        return widget


    def update_widget_for_item(self, item: DownloadItem):
        """Updates the display components of a specific item's widget."""
        if not item.widget or not hasattr(item.widget, 'original_widget'):
             logger.warning(f"Cannot update widget for {item.title}: Widget not found or not structured as expected.")
             return

        pile = item.widget.original_widget # Access the Pile inside Padding

        # Update Header (Title/Duration)
        header_cols = pile.contents[0][0] # Get the Columns widget
        title_widget = header_cols.contents[0][0] # Get the Text widget for title
        duration_widget = header_cols.contents[1][0] # Get the Text widget for duration
        title_widget.set_text(('title_text', item.title or "Loading..."))
        duration_str = str(timedelta(seconds=int(item.duration))) if item.duration else "--:--:--"
        duration_widget.set_text(('duration_text', f" ({duration_str})"))

        # Update Status
        status_map = {
            "Pending": "status_pending",
            "Downloading": "status_downloading",
            "Processing": "status_processing",
            "Complete": "status_complete",
            "Error": "status_error",
        }
        status_style = status_map.get(item.status, 'error_text')
        status_text = f"Status: {item.status}"
        if item.download_type:
            status_text += f" | Type: {item.download_type}"
        if item.error:
             status_text += f" | Error: {item.error[:50]}..."

        status_widget = pile.contents[1][0] # Get the AttrMap widget for status
        status_widget.attr_map = {None: status_style}
        status_widget.original_widget.set_text(status_text) # Update text in the underlying Text widget

        # Update Progress Bar
        progress_bar = pile.contents[2][0] # Get the ProgressBar widget
        progress_style = 'progress_normal'
        bar_style = 'progress_bar'
        if item.status == "Complete":
            progress_style = 'progress_complete'
        elif item.status == "Error":
            progress_style = 'progress_error'
            item.progress = 0 # Reset progress visually on error

        progress_bar.set_completion(item.progress)
        progress_bar.normal = progress_style
        progress_bar.complete = bar_style # Keep the bar style consistent

        # Urwid doesn't automatically redraw on widget content change, signal the loop
        if self.loop:
            self.loop.draw_screen()


    def refresh_display(self):
        """Rebuilds the entire listbox - less efficient but simpler."""
        # This is less efficient than updating widgets in place,
        # but simpler to implement initially. Consider update_widget_for_item
        # for better performance with many items.
        new_widgets = []
        for item in self.downloads:
            # Ensure widget exists or create it
            if not item.widget:
                 item.widget = self.create_download_widget(item)
            else:
                 # If widget exists, update its contents before adding
                 self.update_widget_for_item(item) # Update internal state first
            new_widgets.append(item.widget)

        # Check if focus needs adjustment
        current_focus = self.listbox.focus_position
        max_focus = len(new_widgets) - 1

        self.listbox_walker[:] = new_widgets

        # Restore focus if it was valid, otherwise reset
        if max_focus >= 0:
             self.listbox.focus_position = min(current_focus, max_focus)
        else:
             self.listbox.focus_position = 0 # Or handle empty list case

        self.update_footer()
        if self.loop:
            self.loop.draw_screen() # Ensure redraw after bulk update

    def update_footer(self):
         total = len(self.downloads)
         complete = sum(1 for item in self.downloads if item.status == "Complete")
         downloading = sum(1 for item in self.downloads if item.status in ["Downloading", "Processing"])
         errors = sum(1 for item in self.downloads if item.status == "Error")
         pending = total - complete - downloading - errors

         status_str = f"Total: {total} | ✓: {complete} | ↓: {downloading} | !: {errors} | ?: {pending}"
         self.footer_text.set_text(status_str)


    def handle_input(self, key):
        if key in ('q', 'Q'):
            # Check for active downloads before quitting
            active_downloads = [t for t in self.current_download_tasks.values() if not t.done()]
            if active_downloads:
                 self.show_confirmation_dialog(
                      "Downloads in progress. Quit anyway?",
                      self._confirm_quit
                 )
            else:
                 raise urwid.ExitMainLoop()
            return True # Indicate key was handled

        # Let ListBox handle up/down navigation by default
        # We handle 'enter' specifically
        elif key == 'enter':
            focused_widget, position = self.listbox.get_focus()
            if position < len(self.downloads):
                item = self.downloads[position]
                # If already downloading/complete/error, maybe show details or retry?
                # For now, only show format selection if pending
                if item.status == "Pending":
                    self.show_format_selection(item)
                elif item.status == "Error":
                     # Option to retry? For now, just log maybe.
                     logger.info(f"Enter pressed on item with error: {item.title} - {item.error}")
                     self.show_message_dialog(f"Error for {item.title}:\n{item.error}", title="Download Error")
                else:
                     logger.debug(f"Enter pressed on item with status {item.status}, no action taken.")

            return True # Indicate key was handled

        # Return the key if not handled here, allowing default ListBox processing
        return key

    def _confirm_quit(self, button):
         if button.label.lower() == "yes":
              # Cancel running tasks before exiting
              for task in self.current_download_tasks.values():
                   if not task.done():
                        task.cancel()
              raise urwid.ExitMainLoop()
         else:
              # Close the dialog
              self.loop.widget = self.frame


    async def fetch_video_info(self, url: str) -> Optional[DownloadItem]:
        # Find if item already exists (e.g., from previous run or duplicate URL)
        existing_item = next((item for item in self.downloads if item.url == url), None)
        item = existing_item or DownloadItem(url=url)
        if not existing_item:
             # Add placeholder to list immediately for UI feedback
             item.widget = self.create_download_widget(item)
             self.listbox_walker.append(item.widget)
             self.downloads.append(item)
             self.update_footer()
             if self.loop: self.loop.draw_screen() # Update UI

        item.status = "Fetching Info..."
        item.title = url # Show URL while fetching
        self.update_widget_for_item(item)

        try:
            logger.info(f"Fetching info for URL: {url}")
            # Use specific options for info fetching
            info_ydl_opts = {
                'quiet': True,
                'no_warnings': True,
                'extract_flat': False, # Need full format info
                'skip_download': True,
                'verbose': False,
                'ignoreerrors': True, # Continue if one URL fails
                'forcejson': True, # Try to force JSON output
                'dump_single_json': True, # Get JSON directly if possible
            }

            # Run yt-dlp in executor
            loop = asyncio.get_event_loop()
            with yt_dlp.YoutubeDL(info_ydl_opts) as ydl:
                # extract_info can block, run in executor
                info = await loop.run_in_executor(
                    None, functools.partial(ydl.extract_info, url, download=False)
                )

            if not info:
                logger.warning(f"No info extracted for {url}")
                item.status = "Error"
                item.error = "Failed to extract video info"
                self.update_widget_for_item(item)
                self.update_footer()
                return None

            logger.debug(f"Successfully fetched info for {url}")
            item.title = info.get('title', 'Unknown Title')
            item.duration = info.get('duration', 0)

            # Get available formats with safe codec checking
            formats = info.get('formats', [])
            if not formats:
                 logger.warning(f"No formats list found for {url}. Info dump: {info}")
                 # Fallback: try getting formats from requested_formats if available
                 formats = info.get('requested_formats', [])
                 if not formats:
                      logger.error(f"No formats or requested_formats found for {url}")
                      item.status = "Error"
                      item.error = "No downloadable formats found"
                      self.update_widget_for_item(item)
                      self.update_footer()
                      return None # Cannot proceed without formats

            # Filter and store formats
            item.format_info['audio'] = [f for f in formats if f.get('acodec', 'none') != 'none' and f.get('vcodec', 'none') == 'none']
            item.format_info['video'] = [f for f in formats if f.get('vcodec', 'none') != 'none'] # Include video-only and video+audio

            # Sort formats (example: by bitrate for audio, resolution for video)
            # Handle potential None values in sorting keys
            item.format_info['audio'].sort(key=lambda x: x.get('abr', 0) or 0, reverse=True) # Add 'or 0' for safety
            item.format_info['video'].sort(
                key=lambda x: (
                    x.get('height', 0) or 0, # Ensure height is a number
                    x.get('fps', 0) or 0,    # Ensure fps is a number
                    x.get('vbr', 0) or 0.0   # Ensure vbr is a number (use 0.0 for float consistency if needed)
                ),
                reverse=True
            )

            logger.debug(f"Found {len(item.format_info['audio'])} audio-only formats and "
                       f"{len(item.format_info['video'])} video formats for {url}")

            item.status = "Pending" # Ready for format selection or auto-download
            self.update_widget_for_item(item)
            self.update_footer()
            return item

        except yt_dlp.utils.DownloadError as e:
             logger.error(f"yt-dlp DownloadError fetching info for {url}: {str(e)}", exc_info=False) # Keep log cleaner
             item.status = "Error"
             item.error = f"yt-dlp error: {e}"
             self.update_widget_for_item(item)
             self.update_footer()
             return None
        except Exception as e:
            # Log the full traceback for unexpected errors
            logger.error(f"Unexpected error fetching info for {url}: {str(e)}", exc_info=True)
            item.status = "Error"
            item.error = f"Unexpected error: {e}"
            self.update_widget_for_item(item)
            self.update_footer()
            return None

    def download_progress_hook(self, d, item: DownloadItem):
        """Progress hook specifically bound to an item."""
        try:
            if d['status'] == 'downloading':
                downloaded = d.get('downloaded_bytes', 0)
                total = d.get('total_bytes') or d.get('total_bytes_estimate') # Use estimate if available

                if total and total > 0:
                    progress = (downloaded / total) * 100
                    item.progress = min(progress, 100.0) # Cap at 100
                    item.status = "Downloading"
                else:
                    # If total size is unknown, maybe show bytes downloaded?
                    item.status = f"Downloading ({d.get('_speed_str', '...')})" # Show speed if available
                    item.progress = 0 # Or use a spinner/indeterminate state

                # Update widget less frequently to avoid overwhelming urwid
                # Throttling can be added here if needed (e.g., update only every 0.5s)
                self.update_widget_for_item(item)
                self.update_footer()

            elif d['status'] == 'finished':
                logger.debug(f"Download hook: 'finished' status for {item.title}. File: {d.get('filename')}")
                item.progress = 100
                item.status = "Processing" # Indicate post-processing might occur
                self.update_widget_for_item(item)
                self.update_footer()

            elif d['status'] == 'error':
                logger.error(f"Download hook: 'error' status for {item.title}")
                item.status = "Error"
                item.error = "Download failed during transfer"
                item.progress = 0
                self.update_widget_for_item(item)
                self.update_footer()

        except Exception as e:
            logger.error(f"Error in download progress hook for {item.title}: {str(e)}", exc_info=True)
            # Avoid crashing the hook itself
            item.status = "Error"
            item.error = "Hook error"
            self.update_widget_for_item(item)
            self.update_footer()


    async def start_download(self, item: DownloadItem):
        if not item.download_type:
             logger.error(f"Cannot start download for {item.title}: Download type not set.")
             item.status = "Error"
             item.error = "Download type missing"
             self.update_widget_for_item(item)
             self.update_footer()
             return

        logger.info(f"Starting download for '{item.title}' ({item.download_type})")
        item.status = "Starting..."
        item.progress = 0
        item.error = None
        self.update_widget_for_item(item)
        self.update_footer()

        # Ensure this task is tracked
        task = asyncio.current_task()
        if task:
             self.current_download_tasks[item.url] = task

        temp_file_path = None # To store the path of the initially downloaded file

        try:
            # Create a temporary directory *per download* for isolation
            with tempfile.TemporaryDirectory(prefix=f"ytdl_{sanitize_filename(item.title)[:20]}_") as temp_dir_str:
                temp_dir = pathlib.Path(temp_dir_str)
                logger.debug(f"Using temp directory: {temp_dir}")

                # Sanitize the output filename (base name)
                safe_title_base = sanitize_filename(item.title)

                # Define base yt-dlp options
                ydl_opts = {
                    'progress_hooks': [functools.partial(self.download_progress_hook, item=item)],
                    # Use a generic template in temp dir, we'll rename later
                    'outtmpl': str(temp_dir / '%(title)s.%(ext)s'),
                    'windowsfilenames': sys.platform == 'win32',
                    'quiet': True,
                    'no_warnings': True,
                    'verbose': False,
                    'ignoreerrors': False, # Fail on download error for this item
                    'noprogress': True, # Disable yt-dlp's console progress bar
                    'postprocessor_args': {}, # Initialize dict
                    'ffmpeg_location': shutil.which('ffmpeg'), # Explicitly provide path
                }

                # --- Format Selection ---
                # Choose best format based on type, preferring specific codecs if desired
                if item.download_type == 'audio':
                    # Prefer opus or m4a if available, otherwise best audio
                    ydl_opts['format'] = 'bestaudio[ext=opus]/bestaudio[ext=m4a]/bestaudio/best'
                    ydl_opts['postprocessors'] = [{
                        'key': 'FFmpegExtractAudio',
                        'preferredcodec': 'mp3', # Convert to MP3
                        'preferredquality': '192', # Target quality
                    }]
                    # Add metadata args for audio
                    ydl_opts['postprocessor_args'].setdefault('FFmpegExtractAudio', [])
                    ydl_opts['postprocessor_args']['FFmpegExtractAudio'].extend([
                        '-metadata', f'title={item.title}',
                        # Add more metadata if available (artist, album etc.)
                        # '-metadata', f'artist={info.get("artist", "")}',
                    ])
                    # Embed thumbnail using yt-dlp's built-in feature if possible
                    ydl_opts['writethumbnail'] = True
                    ydl_opts['postprocessors'].append({
                         'key': 'EmbedThumbnail',
                         'already_have_thumbnail': False, # Let yt-dlp download it
                    })

                    final_extension = ".mp3"

                elif item.download_type == 'video':
                    # Prefer mp4 container, h264 codec if possible
                    # Select best video format that has audio, or best video and best audio separately
                    ydl_opts['format'] = 'bestvideo[ext=mp4][vcodec^=avc]+bestaudio[ext=m4a]/bestvideo+bestaudio/best'
                    # Remux to MP4 if necessary (e.g., if downloaded as mkv)
                    ydl_opts['postprocessors'] = [{
                        'key': 'FFmpegVideoConvertor',
                        'preferedformat': 'mp4', # Ensure output is mp4
                    }]
                    final_extension = ".mp4"
                else:
                     raise ValueError(f"Invalid download_type: {item.download_type}")

                final_filename_base = f"{safe_title_base}{final_extension}"
                final_output_path = pathlib.Path.cwd() / final_filename_base # Download to current dir

                # Check for existing final file BEFORE download
                if final_output_path.exists():
                     logger.warning(f"Output file already exists: '{final_output_path}'. Skipping download.")
                     item.status = "Skipped"
                     item.error = "File already exists"
                     item.progress = 100 # Mark as complete visually
                     self.update_widget_for_item(item)
                     self.update_footer()
                     return # Exit download process for this item

                logger.debug(f"Starting yt-dlp download with options: {ydl_opts}")
                loop = asyncio.get_event_loop()
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    await loop.run_in_executor(
                        None, ydl.download, [item.url])

                # --- Post-Download Processing ---
                item.status = "Processing"
                self.update_widget_for_item(item)

                # Find the processed file in the temp directory
                processed_files = list(temp_dir.glob(f'*{final_extension}'))
                if not processed_files:
                     # Maybe the postprocessor failed silently or created a different extension?
                     all_files = list(temp_dir.glob('*.*'))
                     logger.error(f"Could not find expected '{final_extension}' file in temp dir '{temp_dir}'. Found: {all_files}")
                     raise FileNotFoundError(f"Processed file with extension {final_extension} not found in temp dir.")

                temp_file_path = processed_files[0]
                logger.debug(f"Processed file found: {temp_file_path}")

                # Move the final file from temp dir to destination
                logger.info(f"Moving '{temp_file_path.name}' to '{final_output_path}'")
                shutil.move(str(temp_file_path), str(final_output_path))

                # --- Optional: Further Metadata/Cover Art (if yt-dlp didn't handle it) ---
                # Example using eyed3 for MP3 if needed (yt-dlp usually handles this now)
                # if item.download_type == 'audio' and final_extension == '.mp3':
                #     try:
                #         logger.debug(f"Verifying metadata for {final_output_path}")
                #         audiofile = eyed3.load(str(final_output_path))
                #         if audiofile and audiofile.tag:
                #             if not audiofile.tag.title:
                #                 audiofile.tag.title = item.title
                #                 logger.debug("Set title metadata via eyed3")
                #             # Add more checks/settings if yt-dlp missed something
                #             audiofile.tag.save(version=eyed3.id3.ID3_V2_3, encoding='utf-8') # Specify version and encoding
                #         else:
                #              logger.warning(f"Could not load audio tag with eyed3 for {final_output_path}")
                #     except Exception as meta_err:
                #         logger.error(f"Error during final metadata check/set for {final_output_path}: {meta_err}", exc_info=True)


            # If we reach here, the download and move were successful
            item.status = "Complete"
            item.progress = 100
            logger.info(f"Download successful for '{item.title}' -> '{final_output_path}'")

        except FileNotFoundError as e:
             logger.error(f"Download failed for '{item.title}': {e}", exc_info=False)
             item.status = "Error"
             item.error = str(e)
             item.progress = 0
        except yt_dlp.utils.DownloadError as e:
            error_msg = str(e).split(':')[-1].strip() # Get cleaner error message
            logger.error(f"yt-dlp DownloadError during download for '{item.title}': {error_msg}", exc_info=False)
            item.status = "Error"
            item.error = f"yt-dlp: {error_msg}"
            item.progress = 0
        except asyncio.CancelledError:
             logger.warning(f"Download cancelled for '{item.title}'")
             item.status = "Cancelled"
             item.error = "User cancelled"
             item.progress = 0
             # Optionally clean up temp file if it exists and we know its path
             if temp_file_path and temp_file_path.exists():
                  try:
                       temp_file_path.unlink()
                       logger.debug(f"Cleaned up temp file on cancellation: {temp_file_path}")
                  except OSError as unlink_err:
                       logger.warning(f"Could not remove temp file on cancellation: {unlink_err}")
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Unexpected download failed for '{item.title}': {error_msg}", exc_info=True)
            item.status = "Error"
            item.error = f"Unexpected: {error_msg[:100]}" # Truncate long errors
            item.progress = 0

        finally:
            # Final UI update for the item
            self.update_widget_for_item(item)
            self.update_footer()
            # Remove task from tracking once done or failed
            if item.url in self.current_download_tasks:
                del self.current_download_tasks[item.url]


    def show_format_selection(self, item: DownloadItem):
        logger.debug(f"Showing format selection for {item.title}")

        body = [
            urwid.Text(f"Select format for:\n{item.title}", align='center'),
            urwid.Divider('-'),
        ]

        # Add buttons for choices
        audio_button = urwid.AttrMap(urwid.Button("Audio Only (MP3)", on_press=self.format_selected, user_data=('audio', item)), 'button', focus_map='button_focus')
        video_button = urwid.AttrMap(urwid.Button("Video + Audio (MP4)", on_press=self.format_selected, user_data=('video', item)), 'button', focus_map='button_focus')
        cancel_button = urwid.AttrMap(urwid.Button("Cancel", on_press=self.close_dialog), 'button', focus_map='button_focus')

        body.extend([
            urwid.Padding(audio_button, align='center', width=('relative', 80)),
            urwid.Padding(video_button, align='center', width=('relative', 80)),
            urwid.Divider(),
            urwid.Padding(cancel_button, align='center', width=('relative', 50)),
        ])

        list_box = urwid.ListBox(urwid.SimpleFocusListWalker(body))
        dialog = urwid.LineBox(urwid.Padding(list_box, top=1, bottom=1), title="Format Selection")
        dialog = urwid.AttrMap(dialog, 'dialog_border')

        # Overlay the dialog
        overlay = urwid.Overlay(
            dialog,
            self.frame,
            align='center', width=('relative', 60),
            valign='middle', height=('relative', 50),
            min_width=40, min_height=10
        )
        # Add a shadow effect
        shadow_overlay = urwid.DropShadow(overlay)
        self.loop.widget = shadow_overlay


    def format_selected(self, button, data):
        download_type, item = data
        item.download_type = download_type
        logger.info(f"Selected format '{download_type}' for '{item.title}'")
        self.close_dialog(button) # Close the dialog first
        # Update the item's display immediately before starting download
        self.update_widget_for_item(item)
        # Schedule the download to start
        asyncio.create_task(self.start_download(item))

    def close_dialog(self, button):
        """Restores the main frame view."""
        if self.loop and self.frame:
            self.loop.widget = self.frame
        else:
             logger.error("Cannot close dialog: loop or frame not initialized.")

    def show_confirmation_dialog(self, message: str, callback):
         """Shows a Yes/No confirmation dialog."""
         body = [
              urwid.Text(message, align='center'),
              urwid.Divider(),
              urwid.Columns([
                   ('weight', 1, urwid.Padding(urwid.AttrMap(urwid.Button("Yes", on_press=callback), 'button', focus_map='button_focus'), align='center', width=('relative', 80))),
                   ('weight', 1, urwid.Padding(urwid.AttrMap(urwid.Button("No", on_press=self.close_dialog), 'button', focus_map='button_focus'), align='center', width=('relative', 80))),
              ], dividechars=2)
         ]
         dialog = urwid.LineBox(urwid.Pile(body), title="Confirm")
         dialog = urwid.AttrMap(dialog, 'dialog_border')
         overlay = urwid.Overlay(dialog, self.frame, align='center', width=('relative', 50), valign='middle', height='pack', min_width=30)
         shadow_overlay = urwid.DropShadow(overlay)
         self.loop.widget = shadow_overlay

    def show_message_dialog(self, message: str, title: str = "Message"):
         """Shows a simple message dialog with an OK button."""
         body = [
              urwid.Text(message), # Allow multi-line messages
              urwid.Divider(),
              urwid.Padding(urwid.AttrMap(urwid.Button("OK", on_press=self.close_dialog), 'button', focus_map='button_focus'), align='center', width=('relative', 50))
         ]
         dialog = urwid.LineBox(urwid.Pile(body), title=title)
         dialog = urwid.AttrMap(dialog, 'dialog_border')
         overlay = urwid.Overlay(dialog, self.frame, align='center', width=('relative', 70), valign='middle', height='pack', min_width=40)
         shadow_overlay = urwid.DropShadow(overlay)
         self.loop.widget = shadow_overlay


# --- Main Execution Logic ---

async def setup_application(args) -> urwid.MainLoop:
    """Sets up the TUI, fetches initial data, and returns the configured MainLoop."""
    # Setup Windows console encoding (should happen early)
    setup_windows_console()

    # Check for FFmpeg
    check_ffmpeg()

    logger.info("Starting YouTube Downloader Setup")

    if not args.urls:
        logger.error("No URLs provided")
        # Use stderr for critical startup errors before TUI
        print("Error: No YouTube URLs provided.", file=sys.stderr)
        sys.exit(1)

    logger.info(f"Processing {len(args.urls)} URLs")
    mode = "Interactive"
    if args.audio_only:
        mode = "Audio only (batch)"
    elif args.video:
        mode = "Video + Audio (batch)"
    logger.info(f"Mode: {mode}")

    tui = DownloaderTUI()

    # Fetch info for all URLs concurrently
    fetch_tasks = [tui.fetch_video_info(url) for url in args.urls]
    results = await asyncio.gather(*fetch_tasks) # Results will contain DownloadItem or None

    # Filter out None results (errors during fetch) and assign download type if batch mode
    valid_items = []
    for i, item in enumerate(results):
        if item:
            if args.audio_only:
                item.download_type = 'audio'
            elif args.video:
                item.download_type = 'video'
            # Ensure item is in the list if it wasn't added during fetch (e.g., if fetch was instant)
            if item not in tui.downloads:
                 tui.downloads.append(item)
                 # If widget wasn't created during fetch, create it now
                 if not item.widget:
                      item.widget = tui.create_download_widget(item)
                 tui.listbox_walker.append(item.widget) # Add widget to list walker
            valid_items.append(item)
        else:
             logger.warning(f"Failed to fetch info for URL: {args.urls[i]}")
             # Placeholder for failed items might already be in tui.downloads/listbox_walker
             # Ensure its status reflects the error if it exists
             failed_item = next((d for d in tui.downloads if d.url == args.urls[i]), None)
             if failed_item and failed_item.status != "Error":
                  failed_item.status = "Error"
                  failed_item.error = "Info fetch failed"
                  tui.update_widget_for_item(failed_item)


    if not valid_items and not args.audio_only and not args.video:
        # Only exit if interactive and *no* items could be fetched
        logger.error("No valid URLs could be processed.")
        print("Error: None of the provided URLs could be processed.", file=sys.stderr)
        sys.exit(1)
    elif not valid_items and (args.audio_only or args.video):
         logger.warning("No valid URLs to process in batch mode.")
         # Don't exit, allow TUI to show errors if it starts

    # Refresh display after all fetches are done
    tui.refresh_display() # Rebuilds list with final fetched info

    # Initialize the Urwid MainLoop
    logger.debug("Initializing UI MainLoop")
    # Get the current asyncio loop to pass to Urwid
    current_asyncio_loop = asyncio.get_running_loop()
    event_loop = urwid.AsyncioEventLoop(loop=current_asyncio_loop)

    tui.loop = urwid.MainLoop(
        tui.frame,
        tui.palette,
        unhandled_input=tui.handle_input,
        event_loop=event_loop
        # pop_ups=True # Enable built-in pop-up handling if needed later
    )

    # Start downloads automatically for non-interactive mode AFTER loop is created
    if args.audio_only or args.video:
        logger.info("Starting batch downloads")
        for item in valid_items: # Only attempt to download valid items
            if item.download_type: # Ensure type is set
                logger.debug(f"Creating download task for: {item.title}")
                # Create task but don't await here, let the loop run them
                asyncio.create_task(tui.start_download(item))
            else:
                 logger.warning(f"Skipping batch download for {item.title}: download_type not set.")


    logger.debug("Setup complete, returning Urwid MainLoop")
    return tui.loop # Return the configured loop


if __name__ == "__main__":
    # --- Argument Parsing ---
    parser = argparse.ArgumentParser(description='YouTube Music/Video Downloader (TUI)')
    parser.add_argument('urls', nargs='*', help='YouTube URLs to download')
    parser.add_argument('--audio-only', action='store_true', help='Download audio only for all URLs (batch mode)')
    parser.add_argument('--video', action='store_true', help='Download video with audio for all URLs (batch mode)')
    # Add verbosity flag?
    # parser.add_argument('-v', '--verbose', action='count', default=0, help='Increase verbosity (console logging)')
    args = parser.parse_args()

    # --- Platform Specific Setup ---
    # Set asyncio policy *before* getting/creating the event loop
    if sys.platform == "win32":
        try:
            # Use print as logger might not be fully configured or working correctly yet
            print("DEBUG: Setting asyncio event loop policy to WindowsSelectorEventLoopPolicy (pre-run)")
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        except Exception as policy_err:
             print(f"ERROR: Failed to set asyncio event loop policy: {policy_err}", file=sys.stderr)
             # Decide if this is fatal
             # sys.exit(1)


    # --- Main Application Execution ---
    main_loop = None
    exit_code = 0
    try:
        # Get the main asyncio event loop
        loop = asyncio.get_event_loop()

        # Run the setup coroutine to configure everything and get the Urwid loop
        main_loop = loop.run_until_complete(setup_application(args))

        # Start the Urwid event loop (which uses the underlying asyncio loop)
        logger.debug("Starting Urwid main loop...")
        main_loop.run() # This blocks until urwid exits

    except KeyboardInterrupt:
        logger.info("Program interrupted by user")
        print("\nExiting...")
    except SystemExit as e:
         exit_code = e.code # Capture exit code from sys.exit() calls
         logger.warning(f"Program exited with code {exit_code}")
    except Exception as e:
        logger.critical(f"Unexpected error in main execution: {str(e)}", exc_info=True)
        print(f"\nCRITICAL ERROR: {e}", file=sys.stderr)
        print("Please check the log file in the 'logs' directory for details.", file=sys.stderr)
        exit_code = 1 # Indicate failure
    finally:
        # Cleanup happens via atexit (restore_terminal_state)
        logger.info("Program finished")
        # Avoid input() as it blocks and might interfere with terminal restoration
        # print("\nPress Enter to exit...")
        # input()
        sys.exit(exit_code) # Exit with the determined code