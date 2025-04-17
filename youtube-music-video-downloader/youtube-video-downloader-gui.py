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
    console_handler.setLevel(logging.ERROR) # Show INFO and above on console

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

class SelectablePadding(urwid.Padding):
    """A Padding widget that is always selectable."""
    def selectable(self) -> bool:
        return True

class DownloadItem:
    def __init__(self, url: str, title: str = "", duration: int = 0):
        self.url = url
        self.title = title
        self.duration = duration
        self.progress = 0.0
        self.status = "Pending"
        self.download_type: Optional[str] = None # Hint for batch mode or simple selection
        self.format_info = {'audio': [], 'video': []}
        self.error = None
        self.widget: Optional[urwid.Widget] = None
        self.is_selected: bool = False
        # --- Add fields for selected format IDs ---
        self.selected_audio_format_id: Optional[str] = None
        self.selected_video_format_id: Optional[str] = None
        # --- Store format details for display ---
        self.selected_audio_details: str = ""
        self.selected_video_details: str = ""

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
            # --- Update header text for new keybinds ---
            urwid.Text('YouTube Downloader - (↑↓ Navigate) (+/- Select) (Enter Format) (d Download Selected) (q Quit)'),
            'header'
        )

        # Main content area - Use AttrMap for focus styling
        self.listbox = urwid.ListBox(self.listbox_walker) # Keep the ListBox as is

        # Footer
        self.footer_text = urwid.Text('')
        self.footer = urwid.AttrMap(self.footer_text, 'footer')

        # Layout
        self.frame = urwid.Frame(
            # --- Use the ListBox directly as the body ---
            self.listbox,
            # --- Remove the AttrMap wrapper that was here ---
            # urwid.AttrMap(self.listbox, 'body', focus_map={'focus': 'focus'}),
            header=self.header,
            footer=self.footer
        )
        self.update_footer()


    def create_download_widget(self, item: DownloadItem) -> urwid.Widget:
        # --- (pile, status, progress setup remains the same) ---
        selection_marker = f"[{'x' if item.is_selected else ' '}] "
        title_text = f"{selection_marker}{item.title or 'Loading...'}"
        title_widget = urwid.Text(('title_text', title_text))
        duration_str = str(timedelta(seconds=int(item.duration))) if item.duration else "--:--:--"
        duration_widget = urwid.Text(('duration_text', f" ({duration_str})"), align='right')
        header_cols = urwid.Columns([title_widget, ('pack', duration_widget)], dividechars=1)

        status_map = {
            "Pending": "status_pending", "Fetching Info...": "status_pending",
            "Starting...": "status_downloading", "Downloading": "status_downloading",
            "Processing": "status_processing", "Complete": "status_complete",
            "Error": "status_error", "Cancelled": "status_error",
            "Skipped": "status_pending",
        }
        status_style = status_map.get(item.status, 'error_text')
        status_text = f"Status: {item.status}"
        if item.download_type: status_text += f" | Type: {item.download_type}"
        if item.error: status_text += f" | Error: {item.error[:50]}..."
        status_widget = urwid.AttrMap(urwid.Text(status_text), status_style)

        progress_style = 'progress_normal'
        bar_style = 'progress_bar'
        if item.status == "Complete" or item.status == "Skipped": progress_style = 'progress_complete'
        elif item.status == "Error" or item.status == "Cancelled": progress_style = 'progress_error'
        progress_bar = urwid.ProgressBar(progress_style, bar_style, current=item.progress, done=100)

        pile = urwid.Pile([
            header_cols,
            status_widget,
            progress_bar,
        ])

        # --- Use SelectablePadding instead of urwid.Padding ---
        padded_widget = SelectablePadding(pile, left=1, right=1)

        # --- Wrap Padding with AttrMap for focus ---
        widget = urwid.AttrMap(padded_widget, None, focus_map='focus')

        item.widget = widget # Store the outer AttrMap widget
        return widget
    
    def update_widget_for_item(self, item: DownloadItem):
        """Updates the display components of a specific item's widget."""
        # --- Adjust access path back to: AttrMap -> Padding -> Pile ---
        if not item.widget or not hasattr(item.widget, 'original_widget'):
             # Check if it's an AttrMap
             if not isinstance(item.widget, urwid.AttrMap):
                  logger.warning(f"Cannot update widget for {item.title}: Widget is not an AttrMap as expected.")
                  return
             logger.warning(f"Cannot update widget for {item.title}: AttrMap widget missing original_widget.")
             return
        try:
            # item.widget is the AttrMap
            padded_widget = item.widget.original_widget # This is the Padding
            pile = padded_widget.original_widget # This is the Pile
        except AttributeError:
             logger.warning(f"Cannot update widget for {item.title}: Widget structure incorrect (AttrMap->Padding->Pile expected).")
             return

        # --- The rest of the update logic remains the same ---
        header_cols = pile.contents[0][0]
        title_widget = header_cols.contents[0][0]
        duration_widget = header_cols.contents[1][0]

        selection_marker = f"[{'x' if item.is_selected else ' '}] "
        title_text = f"{selection_marker}{item.title or 'Loading...'}"
        title_widget.set_text(('title_text', title_text))
        duration_str = str(timedelta(seconds=int(item.duration))) if item.duration else "--:--:--"
        duration_widget.set_text(('duration_text', f" ({duration_str})"))

        status_map = {
            "Pending": "status_pending", "Fetching Info...": "status_pending",
            "Starting...": "status_downloading", "Downloading": "status_downloading",
            "Processing": "status_processing", "Complete": "status_complete",
            "Error": "status_error", "Cancelled": "status_error",
            "Skipped": "status_pending",
        }
        status_style = status_map.get(item.status, 'error_text')
        status_text = f"Status: {item.status}"

        # --- Display selected format details ---
        selected_formats_str = ""
        if item.selected_video_format_id:
            selected_formats_str += f" | V: {item.selected_video_details}"
        if item.selected_audio_format_id:
             # Only show audio if it's different from video or if no video selected
             # (Assumes combined formats might store audio ID separately if needed)
            selected_formats_str += f" | A: {item.selected_audio_details}"

        status_text += selected_formats_str
        # --- End format display ---

        if item.error:
             status_text += f" | Error: {item.error[:40]}..."
             
        status_widget = pile.contents[1][0]
        status_widget.attr_map = {None: status_style}
        status_widget.original_widget.set_text(status_text)

        progress_bar = pile.contents[2][0]
        progress_style = 'progress_normal'
        bar_style = 'progress_bar'
        current_progress = item.progress
        if item.status == "Complete" or item.status == "Skipped":
            progress_style = 'progress_complete'
            current_progress = 100
        elif item.status == "Error" or item.status == "Cancelled":
            progress_style = 'progress_error'
            current_progress = 0
        progress_bar.set_completion(current_progress)
        progress_bar.normal = progress_style
        progress_bar.complete = bar_style

    def update_footer(self):
         total = len(self.downloads)
         selected = sum(1 for item in self.downloads if item.is_selected) # Count selected
         complete = sum(1 for item in self.downloads if item.status == "Complete")
         downloading = sum(1 for item in self.downloads if item.status in ["Downloading", "Processing", "Starting..."])
         errors = sum(1 for item in self.downloads if item.status in ["Error", "Cancelled"])
         pending = total - complete - downloading - errors

         # --- Add selected count to footer ---
         status_str = f"Total: {total} | Sel: {selected} | ✓: {complete} | ↓: {downloading} | !: {errors} | ?: {pending}"
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

        # --- Handle +/- for selection ---
        elif key in ('+', '-'):
            focused_widget, position = self.listbox.get_focus()
            if position < len(self.downloads):
                item = self.downloads[position]
                # Toggle selection only if not actively downloading/processing
                if item.status not in ["Downloading", "Processing", "Starting..."]:
                    item.is_selected = not item.is_selected
                    logger.debug(f"Toggled selection for '{item.title}' to {item.is_selected}")
                    self._schedule_ui_update(item)
                    self.update_footer()
                else:
                    logger.debug(f"Cannot toggle selection for '{item.title}' while status is {item.status}")
            return True # Indicate key was handled

        # --- Handle 'Enter' for format selection (on focused item) ---
        elif key == 'enter':
            focused_widget, position = self.listbox.get_focus()
            if position < len(self.downloads):
                item = self.downloads[position]
                # Show format selection if info is fetched (Pending or Error/Cancelled/Skipped to allow re-selection)
                if item.status not in ["Queued", "Fetching Info...", "Downloading", "Processing", "Starting..."]:
                    if not item.format_info['audio'] and not item.format_info['video']:
                         self.show_message_dialog(f"No format information available for {item.title}.\nStatus: {item.status}\nError: {item.error}", title="Format Info Missing")
                    else:
                         self.show_detailed_format_selection(item) # <-- Call new dialog
                elif item.status in ["Downloading", "Processing", "Starting..."]:
                     self.show_message_dialog(f"Download in progress for {item.title}...", title="In Progress")
                else: # Queued or Fetching Info
                     self.show_message_dialog(f"Still fetching info for {item.title}...", title="Loading")
            return True # Indicate key was handled

        # --- Handle 'd' for downloading selected items ---
        elif key in ('d', 'D'):
            selected_items = [item for item in self.downloads if item.is_selected]
            items_to_download = []
            missing_selection = [] # Renamed from missing_format

            if not selected_items:
                self.show_message_dialog("No items selected. Use '+' or '-' to select items first.", title="Nothing Selected")
                return True

            for item in selected_items:
                if item.status in ["Downloading", "Processing", "Starting...", "Complete", "Skipped", "Queued", "Fetching Info..."]:
                    logger.warning(f"Skipping download trigger for '{item.title}': Status is '{item.status}'")
                    continue

                # --- Check if *any* format ID is selected ---
                if not item.selected_audio_format_id and not item.selected_video_format_id:
                    missing_selection.append(item.title)
                else:
                    # Reset error/cancelled status before retrying
                    if item.status in ["Error", "Cancelled"]:
                         item.error = None
                         item.status = "Pending" # Reset status
                         self._schedule_ui_update(item)
                    items_to_download.append(item)

            if missing_selection:
                missing_titles = "\n - ".join(missing_selection)
                # --- Update message ---
                self.show_message_dialog(f"Cannot start download. Format not selected for:\n - {missing_titles}\n\nUse Enter on each item to select formats first.", title="Format Not Selected")
                return True # Indicate key was handled

            if not items_to_download:
                 self.show_message_dialog("No eligible items to download among selected.", title="Download Info")
                 return True

            # Start downloads directly
            logger.info(f"Starting download for {len(items_to_download)} selected items.")
            for item in items_to_download:
                logger.debug(f"Creating download task for selected item: {item.title}")
                asyncio.create_task(self.start_download(item))

            self.update_footer() # Update footer after potentially changing selection
            return True # Indicate key was handled


        # Return the key if not handled here, allowing default ListBox processing (like PgUp/PgDown)
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


    async def fetch_video_info(self, url: str, args: argparse.Namespace) -> Optional[DownloadItem]: # Add args parameter
        # Find the existing placeholder item
        item = next((i for i in self.downloads if i.url == url), None)
        if not item:
             # This shouldn't happen with the new flow, but handle defensively
             logger.error(f"Could not find placeholder item for URL {url} during fetch.")
             return None

        # Update status to indicate fetching
        item.status = "Fetching Info..."
        # Keep URL as title temporarily
        self._schedule_ui_update(item)

        try:
            logger.info(f"Fetching info for URL: {url}")
            # Use specific options for info fetching
            info_ydl_opts = {
                'quiet': True, 'no_warnings': True, 'extract_flat': False,
                'skip_download': True, 'verbose': False, 'ignoreerrors': True,
                'forcejson': True, 'dump_single_json': True,
            }

            # Run yt-dlp in executor
            loop = asyncio.get_event_loop()
            with yt_dlp.YoutubeDL(info_ydl_opts) as ydl:
                info = await loop.run_in_executor(
                    None, functools.partial(ydl.extract_info, url, download=False)
                )

            if not info:
                logger.warning(f"No info extracted for {url}")
                item.status = "Error"
                item.error = "Failed to extract video info"
                self._schedule_ui_update(item)
                self.update_footer()
                return None

            logger.debug(f"Successfully fetched info for {url}")
            item.title = info.get('title', 'Unknown Title')
            item.duration = info.get('duration', 0)
            item.status = "Pending"
            item.error = None

            # Get available formats
            formats = info.get('formats', []) or info.get('requested_formats', [])
            if not formats:
                 logger.error(f"No formats or requested_formats found for {url}")
                 item.status = "Error"
                 item.error = "No downloadable formats found"
                 self._schedule_ui_update(item)
                 self.update_footer()
                 return None

            # Filter and sort formats
            item.format_info['audio'] = sorted(
                [f for f in formats if f.get('acodec', 'none') != 'none' and f.get('vcodec', 'none') == 'none'],
                key=lambda x: x.get('abr', 0) or 0, reverse=True
            )
            item.format_info['video'] = sorted(
                [f for f in formats if f.get('vcodec', 'none') != 'none'],
                key=lambda x: (x.get('height', 0) or 0, x.get('fps', 0) or 0, x.get('vbr', 0) or 0.0),
                reverse=True
            )

            logger.debug(f"Found {len(item.format_info['audio'])} audio-only formats and "
                       f"{len(item.format_info['video'])} video formats for {url}")

            # --- Auto-select best formats (optional) ---
            best_video = item.format_info['video'][0] if item.format_info['video'] else None
            best_audio = item.format_info['audio'][0] if item.format_info['audio'] else None

            # Reset previous selections before auto-selecting
            item.selected_video_format_id = None
            item.selected_audio_format_id = None
            item.selected_video_details = ""
            item.selected_audio_details = ""

            # Determine initial selection based on batch args or default to best video+audio
            preselect_audio = args.audio_only # From command line args (need access or pass it)
            preselect_video = args.video     # From command line args

            if preselect_audio and best_audio:
                 item.selected_audio_format_id = best_audio['format_id']
                 item.selected_audio_details = f"{best_audio.get('ext')} {best_audio.get('acodec')} {best_audio.get('abr', 0)}k"
                 logger.debug(f"Pre-selected best audio for {item.title}: {item.selected_audio_format_id}")
            elif preselect_video and best_video:
                 item.selected_video_format_id = best_video['format_id']
                 item.selected_video_details = f"{best_video.get('ext')} {best_video.get('height')}p {best_video.get('vcodec')}"
                 # If best video has audio, pre-select it; otherwise, pre-select best separate audio
                 if best_video.get('acodec', 'none') != 'none':
                      item.selected_audio_format_id = best_video['format_id']
                      item.selected_audio_details = "(from video)"
                 elif best_audio:
                      item.selected_audio_format_id = best_audio['format_id']
                      item.selected_audio_details = f"{best_audio.get('ext')} {best_audio.get('acodec')} {best_audio.get('abr', 0)}k"
                 logger.debug(f"Pre-selected best video (and maybe audio) for {item.title}: V:{item.selected_video_format_id} A:{item.selected_audio_format_id}")
            elif best_video: # Default interactive mode: pre-select best video + best audio
                 item.selected_video_format_id = best_video['format_id']
                 item.selected_video_details = f"{best_video.get('ext')} {best_video.get('height')}p {best_video.get('vcodec')}"
                 if best_video.get('acodec', 'none') != 'none':
                      item.selected_audio_format_id = best_video['format_id']
                      item.selected_audio_details = "(from video)"
                 elif best_audio:
                      item.selected_audio_format_id = best_audio['format_id']
                      item.selected_audio_details = f"{best_audio.get('ext')} {best_audio.get('acodec')} {best_audio.get('abr', 0)}k"
                 logger.debug(f"Pre-selected default best video/audio for {item.title}: V:{item.selected_video_format_id} A:{item.selected_audio_format_id}")
            elif best_audio: # Fallback if only audio exists
                 item.selected_audio_format_id = best_audio['format_id']
                 item.selected_audio_details = f"{best_audio.get('ext')} {best_audio.get('acodec')} {best_audio.get('abr', 0)}k"
                 logger.debug(f"Pre-selected fallback best audio for {item.title}: {item.selected_audio_format_id}")
            # --- End auto-select ---

            item.status = "Pending"
            item.error = None

            self._schedule_ui_update(item)
            self.update_footer()

            # --- Trigger automatic download if in batch mode AND formats were pre-selected ---
            # Check if *required* formats for the batch mode were successfully pre-selected
            batch_ready = False
            if args.audio_only and item.selected_audio_format_id:
                 batch_ready = True
            elif args.video and item.selected_video_format_id: # Video might implicitly include audio
                 batch_ready = True

            if batch_ready and item.status == "Pending":
                 logger.info(f"Auto-starting batch download for '{item.title}' (Pre-selected)")
                 asyncio.create_task(self.start_download(item))

            return item
            # --- End of batch trigger ---

            return item # Return the updated item

        except yt_dlp.utils.DownloadError as e:
             logger.error(f"yt-dlp DownloadError fetching info for {url}: {str(e)}", exc_info=False)
             item.status = "Error"
             item.error = f"yt-dlp error: {e}"
             self._schedule_ui_update(item)
             self.update_footer()
             return None
        except Exception as e:
            logger.error(f"Unexpected error fetching info for {url}: {str(e)}", exc_info=True)
            item.status = "Error"
            item.error = f"Unexpected error: {e}"
            self._schedule_ui_update(item)
            self.update_footer()
            return None
        
    def download_progress_hook(self, d, item: DownloadItem):
        """Progress hook specifically bound to an item."""
        try:
            needs_update = False
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
                needs_update = True

            elif d['status'] == 'finished':
                logger.debug(f"Download hook: 'finished' status for {item.title}. File: {d.get('filename')}")
                item.progress = 100
                item.status = "Processing" # Indicate post-processing might occur
                needs_update = True

            elif d['status'] == 'error':
                logger.error(f"Download hook: 'error' status for {item.title}")
                item.status = "Error"
                item.error = "Download failed during transfer"
                item.progress = 0
                needs_update = True

            if needs_update:
                # --- Use schedule method ---
                self._schedule_ui_update(item)
                # Footer update can likely stay outside the schedule call
                # as it doesn't directly touch complex screen drawing
                self.update_footer()

        except Exception as e:
            logger.error(f"Error in download progress hook for {item.title}: {str(e)}", exc_info=True)
            # Avoid crashing the hook itself
            item.status = "Error"
            item.error = "Hook error"
            self._schedule_ui_update(item)
            self.update_footer()


    async def start_download(self, item: DownloadItem):
        if not item.selected_audio_format_id and not item.selected_video_format_id:
             logger.error(f"Cannot start download for {item.title}: No format selected.")
             item.status = "Error"
             item.error = "No format selected"
             self._schedule_ui_update(item)
             self.update_footer()
             return

        # Determine intended download type based on selection
        download_intent = "Unknown"
        if item.selected_audio_format_id and not item.selected_video_format_id:
             download_intent = "Audio Only"
        elif item.selected_video_format_id and not item.selected_audio_format_id:
             download_intent = "Video Only (or Video+Audio combined)"
        elif item.selected_video_format_id and item.selected_audio_format_id:
             # Check if they are the same ID (combined format)
             if item.selected_video_format_id == item.selected_audio_format_id:
                  download_intent = "Video+Audio (Combined Format)"
             else:
                  download_intent = "Video+Audio (Merged)"

        logger.info(f"Starting download for '{item.title}' ({download_intent})")
        
        item.status = "Starting..."
        item.progress = 0
        item.error = None
        self._schedule_ui_update(item)
        self.update_footer()

        # Ensure this task is tracked
        task = asyncio.current_task()
        if task: self.current_download_tasks[item.url] = task
        temp_file_path = None

        try:
            # Create a temporary directory *per download* for isolation
            with tempfile.TemporaryDirectory(prefix=f"ytdl_{sanitize_filename(item.title)[:20]}_") as temp_dir_str:
                temp_dir = pathlib.Path(temp_dir_str)
                logger.debug(f"Using temp directory: {temp_dir}")

                # Sanitize the output filename (base name)
                safe_title_base = sanitize_filename(item.title)

                # --- Construct format string from selected IDs ---
                format_string = ""
                if item.selected_video_format_id and item.selected_audio_format_id:
                    if item.selected_video_format_id == item.selected_audio_format_id:
                        format_string = item.selected_video_format_id # Combined format
                    else:
                        format_string = f"{item.selected_video_format_id}+{item.selected_audio_format_id}" # Separate streams
                elif item.selected_video_format_id:
                    format_string = item.selected_video_format_id # Video only (might contain audio)
                elif item.selected_audio_format_id:
                    format_string = item.selected_audio_format_id # Audio only
                else:
                    # This case is already handled at the start, but defensive check
                    raise ValueError("No format ID selected for download")

                logger.debug(f"Using format string: {format_string}")
                
                # Define base yt-dlp options
                ydl_opts = {
                    'format': format_string, # Use the constructed format string
                    'progress_hooks': [functools.partial(self.download_progress_hook, item=item)],
                    'outtmpl': str(temp_dir / '%(title)s.%(ext)s'),
                    'windowsfilenames': sys.platform == 'win32',
                    'quiet': True, 'no_warnings': True, 'verbose': False,
                    'ignoreerrors': False, 'noprogress': True,
                    'postprocessor_args': {},
                    'ffmpeg_location': shutil.which('ffmpeg'),
                    'postprocessors': [], # Start with empty list
                }

                # --- Adjust postprocessors based on selection ---
                final_extension = ".?" # Determine based on download
                is_audio_download = item.selected_audio_format_id and not item.selected_video_format_id
                is_video_download = bool(item.selected_video_format_id)

                if is_audio_download:
                    # Audio only download - convert to MP3
                    ydl_opts['postprocessors'].append({
                        'key': 'FFmpegExtractAudio',
                        'preferredcodec': 'mp3',
                        'preferredquality': '192',
                    })
                    # Add metadata args
                    ydl_opts['postprocessor_args'].setdefault('FFmpegExtractAudio', [])
                    ydl_opts['postprocessor_args']['FFmpegExtractAudio'].extend([
                        '-metadata', f'title={item.title}',
                    ])
                    # Embed thumbnail
                    ydl_opts['writethumbnail'] = True
                    ydl_opts['postprocessors'].append({
                         'key': 'EmbedThumbnail', 'already_have_thumbnail': False,
                    })
                    final_extension = ".mp3"
                elif is_video_download:
                    # Video download - ensure MP4 container
                    ydl_opts['postprocessors'].append({
                        'key': 'FFmpegVideoConvertor',
                        'preferedformat': 'mp4',
                    })
                    final_extension = ".mp4"
                    # Optionally embed thumbnail for video too?
                    # ydl_opts['writethumbnail'] = True
                    # ydl_opts['postprocessors'].append({'key': 'EmbedThumbnail', 'already_have_thumbnail': False})


                # --- Final path and download execution ---

                final_filename_base = f"{safe_title_base}{final_extension}"
                final_output_path = pathlib.Path.cwd() / final_filename_base # Download to current dir

                # Check for existing final file BEFORE download
                if final_output_path.exists():
                     logger.warning(f"Output file already exists: '{final_output_path}'. Skipping download.")
                     item.status = "Skipped"
                     item.error = "File already exists"
                     item.progress = 100 # Mark as complete visually
                     self._schedule_ui_update(item)
                     self.update_footer()
                     return # Exit download process for this item

                logger.debug(f"Starting yt-dlp download with options: {ydl_opts}")
                loop = asyncio.get_event_loop()
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    await loop.run_in_executor(None, ydl.download, [item.url])

                # --- Post-Download Processing ---
                item.status = "Processing"
                self._schedule_ui_update(item)

                # Find the processed file (use final_extension)
                processed_files = list(temp_dir.glob(f'*{final_extension}'))
                if not processed_files:
                     # Try finding based on original extension if postprocessor failed? More complex.
                     all_files = list(temp_dir.glob('*.*'))
                     logger.error(f"Could not find expected '{final_extension}' file in temp dir '{temp_dir}'. Found: {all_files}")
                     raise FileNotFoundError(f"Processed file with extension {final_extension} not found.")

                temp_file_path = processed_files[0]
                logger.debug(f"Processed file found: {temp_file_path}")

                # Move the final file from temp dir to destination
                logger.info(f"Moving '{temp_file_path.name}' to '{final_output_path}'")
                shutil.move(str(temp_file_path), str(final_output_path))

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
            self._schedule_ui_update(item)
            self.update_footer()
            # Remove task from tracking once done or failed
            if item.url in self.current_download_tasks:
                del self.current_download_tasks[item.url]

    def format_selected(self, button, data):
        download_type, item = data
        item.download_type = download_type
        logger.info(f"Selected format '{download_type}' for '{item.title}'")
        self.close_dialog(button) # Close the dialog first
        # Update the item's display immediately before starting download
        self._schedule_ui_update(item)
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
         self.loop.widget = overlay

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
         self.loop.widget = overlay

    def _schedule_ui_update(self, item: DownloadItem):
        """Schedules widget update and screen redraw on the main event loop."""
        if not self.loop or not hasattr(self.loop, 'event_loop'):
            logger.error("Cannot schedule UI update: Main loop or event_loop not available.")
            return

        # --- Access the underlying asyncio loop ---
        urwid_event_loop = self.loop.event_loop
        if not hasattr(urwid_event_loop, '_loop'):
             logger.error("Cannot schedule UI update: Urwid event loop does not have '_loop' attribute.")
             return

        asyncio_loop = urwid_event_loop._loop
        # --- Use the asyncio loop's call_soon ---
        asyncio_loop.call_soon(self._do_update_and_draw, item)

    def _do_update_and_draw(self, item: DownloadItem):
        """Helper function called by call_soon to update widget and draw screen."""
        try:
            self.update_widget_for_item(item)
            if self.loop:
                self.loop.draw_screen() # Now draw_screen is called from the main loop context
        except Exception as e:
             # Log errors happening during the scheduled update
             logger.error(f"Error during scheduled UI update for {item.title}: {e}", exc_info=True)

    def _format_dict_to_str(self, f: Dict) -> str:
        """Helper to create a display string for a format dictionary."""
        details = []
        # Prioritize format_note if available
        if f.get('format_note'):
            details.append(f['format_note'])
        # Add resolution if it's a video format
        elif f.get('height'):
             details.append(f"{f['height']}p")

        # Add FPS if available
        if f.get('fps'):
            details.append(f"{f['fps']}fps")

        # Add video codec if available and not 'none'
        vcodec = f.get('vcodec')
        if vcodec and vcodec != 'none':
            details.append(vcodec)

        # Add audio codec if available and not 'none'
        acodec = f.get('acodec')
        if acodec and acodec != 'none':
            details.append(acodec)

        # Add bitrate (prefer audio bitrate, fallback to video bitrate)
        if f.get('abr'):
            details.append(f"{f['abr']:.0f}k") # Format as integer kbps
        elif f.get('vbr'):
            details.append(f"{f['vbr']:.0f}k") # Format as integer kbps

        # --- Use pre-formatted filesize strings ---
        if f.get('filesize_str'):
            details.append(f['filesize_str'])
        elif f.get('filesize_approx_str'):
             details.append(f"~{f['filesize_approx_str']}")
        # --- Remove incorrect calc_width usage ---
        # if f.get('filesize'): details.append(urwid.util.calc_width(f['filesize'], 0)) # Incorrect usage
        # elif f.get('filesize_approx'): details.append(f"~{urwid.util.calc_width(f['filesize_approx'], 0)}") # Incorrect usage

        ext = f.get('ext', '?')
        # Construct the final string, ensuring format_id exists
        format_id = f.get('format_id', 'N/A')
        details_str = ' / '.join(map(str, filter(None, details))) # Filter out None values before joining
        return f"[{format_id}] {ext} - {details_str}"


    def show_detailed_format_selection(self, item: DownloadItem):
        logger.debug(f"Showing detailed format selection for {item.title}")

        # --- Radio Button Groups ---
        video_group: List[urwid.RadioButton] = []
        audio_group: List[urwid.RadioButton] = []

        # --- Populate Video Formats ---
        video_widgets = [urwid.Text(("bold", "Video Formats:"))]
        
        # Option for video-only download (no separate audio merge)
        video_only_selected = (item.selected_video_format_id is not None and 
                            item.selected_audio_format_id is None)
        video_only_rb = urwid.RadioButton(video_group, "(Video Only - No Audio Merge)",
                                        state=video_only_selected)
        video_widgets.append(video_only_rb)
        
        for i, f in enumerate(item.format_info.get('video', [])):
            label = self._format_dict_to_str(f)
            is_selected = (item.selected_video_format_id == f['format_id'])
            rb = urwid.RadioButton(video_group, label, state=is_selected)
            video_widgets.append(rb)
        
        if not item.format_info.get('video'):
            video_widgets.append(urwid.Text(" (None available)"))

        # --- Populate Audio Formats ---
        audio_widgets = [urwid.Text(("bold", "Audio Formats (for merging or audio-only):"))]
        
        # Option for audio-only download (no video)
        audio_only_selected = (item.selected_audio_format_id is not None and 
                            item.selected_video_format_id is None)
        audio_only_rb = urwid.RadioButton(audio_group, "(Audio Only - No Video)",
                                        state=audio_only_selected)
        audio_widgets.append(audio_only_rb)
        
        for i, f in enumerate(item.format_info.get('audio', [])):
            label = self._format_dict_to_str(f)
            is_selected = (item.selected_audio_format_id == f['format_id'])
            rb = urwid.RadioButton(audio_group, label, state=is_selected)
            audio_widgets.append(rb)
        
        if not item.format_info.get('audio'):
            audio_widgets.append(urwid.Text(" (None available)"))

        # --- Dialog Layout ---
        # Create button widgets
        confirm_button = urwid.Button("Confirm Selection", 
                                    on_press=self._confirm_format_selection,
                                    user_data=(item, video_group, audio_group))
        cancel_button = urwid.Button("Cancel", on_press=self.close_dialog)
        
        # Apply styling to buttons
        confirm_button = urwid.AttrMap(confirm_button, 'button', focus_map='button_focus')
        cancel_button = urwid.AttrMap(cancel_button, 'button', focus_map='button_focus')

        # Create fixed-height ListBoxes with scrollbars if needed
        video_list = urwid.BoxAdapter(
            urwid.ListBox(urwid.SimpleListWalker(video_widgets)), 
            height=min(10, len(video_widgets))
        )
        
        audio_list = urwid.BoxAdapter(
            urwid.ListBox(urwid.SimpleListWalker(audio_widgets)), 
            height=min(10, len(audio_widgets))
        )

        # Place lists in LineBoxes with titles
        video_box = urwid.LineBox(video_list, title="Video")
        audio_box = urwid.LineBox(audio_list, title="Audio")

        # Create columns with video and audio sections
        body_columns = urwid.Columns([
            ('weight', 1, video_box),
            ('weight', 1, audio_box)
        ], dividechars=2)

        # Title section with item name
        title_text = urwid.Text(f"Select formats for:\n{item.title}", align='center')
        
        # Button row
        button_row = urwid.Columns([
            ('pack', confirm_button),
            ('pack', cancel_button),
        ], dividechars=4)

        # Build the dialog pile with explicit sizing
        dialog_pile = urwid.Pile([
            ('pack', title_text),
            ('pack', urwid.Divider('-')),
            ('weight', 1, body_columns),
            ('pack', urwid.Divider('-')),
            ('pack', button_row)
        ])

        # Create a padded container for the dialog
        padded_dialog = urwid.Padding(dialog_pile, left=1, right=1)
        
        # Wrap dialog content in LineBox and AttrMap
        dialog = urwid.LineBox(padded_dialog, title="Detailed Format Selection")
        dialog = urwid.AttrMap(dialog, 'dialog_border')

        # Create overlay with fixed size
        overlay = urwid.Overlay(
            dialog,
            self.frame,
            align='center', 
            width=('relative', 80),
            valign='middle', 
            height=('relative', 80),  # Use relative height instead of 'pack'
            min_width=60, 
            min_height=20
        )
        
        # Set as the main widget
        self.loop.widget = overlay

    def _confirm_format_selection(self, button, user_data):
        """Callback when 'Confirm' is pressed in the detailed format dialog."""
        item, video_group, audio_group = user_data
        
        # Find selected radio buttons
        selected_video_rb = next((rb for rb in video_group if rb.state), None)
        selected_audio_rb = next((rb for rb in audio_group if rb.state), None)
        
        # Reset selections before applying new ones
        item.selected_video_format_id = None
        item.selected_audio_format_id = None
        item.selected_video_details = ""
        item.selected_audio_details = ""
        
        # --- Process video selection ---
        video_format = None
        is_video_only = False
        
        if selected_video_rb:
            # Check if this is the "Video Only" option (first in the group)
            is_video_only = (video_group.index(selected_video_rb) == 0)
            
            # Get format data safely - use getattr to avoid AttributeError
            format_data = getattr(selected_video_rb, 'user_data', None)
            if format_data:
                video_format = format_data
            # If we don't have format data but a regular video option is selected,
            # try to get it from the format_info dictionary
            elif not is_video_only:
                try:
                    # Calculate index in format_info (skip "Video Only" option)
                    video_index = video_group.index(selected_video_rb) - 1
                    if 0 <= video_index < len(item.format_info.get('video', [])):
                        video_format = item.format_info['video'][video_index]
                except (ValueError, IndexError):
                    logger.warning("Failed to retrieve video format information.")
        
        # --- Process audio selection ---
        audio_format = None
        is_audio_only = False
        
        if selected_audio_rb:
            # Check if this is the "Audio Only" option (first in the group)
            is_audio_only = (audio_group.index(selected_audio_rb) == 0)
            
            # Get format data safely - use getattr to avoid AttributeError
            format_data = getattr(selected_audio_rb, 'user_data', None)
            if format_data:
                audio_format = format_data
            # If we don't have format data but a regular audio option is selected,
            # try to get it from the format_info dictionary
            elif not is_audio_only:
                try:
                    # Calculate index in format_info (skip "Audio Only" option)
                    audio_index = audio_group.index(selected_audio_rb) - 1
                    if 0 <= audio_index < len(item.format_info.get('audio', [])):
                        audio_format = item.format_info['audio'][audio_index]
                except (ValueError, IndexError):
                    logger.warning("Failed to retrieve audio format information.")
        
        # --- Apply selections based on modes and available formats ---
        
        # Audio-only mode
        if is_audio_only and audio_format:
            item.selected_audio_format_id = audio_format['format_id']
            item.selected_audio_details = f"{audio_format.get('ext', '?')} {audio_format.get('acodec', '?')} {audio_format.get('abr', 0)}k"
            logger.info(f"Format confirmed for '{item.title}': Audio Only - {item.selected_audio_format_id}")
        
        # Video-only mode
        elif is_video_only and video_format:
            item.selected_video_format_id = video_format['format_id']
            item.selected_video_details = f"{video_format.get('ext', '?')} {video_format.get('height', '?')}p {video_format.get('vcodec', '?')}"
            
            # Check if this video format also has audio
            if video_format.get('acodec', 'none') != 'none':
                item.selected_audio_format_id = video_format['format_id']
                item.selected_audio_details = "(from video)"
            
            logger.info(f"Format confirmed for '{item.title}': Video Only - {item.selected_video_format_id}")
        
        # Regular mode (video + audio, potentially for merging)
        elif video_format and audio_format:
            item.selected_video_format_id = video_format['format_id']
            item.selected_video_details = f"{video_format.get('ext', '?')} {video_format.get('height', '?')}p {video_format.get('vcodec', '?')}"
            item.selected_audio_format_id = audio_format['format_id']
            item.selected_audio_details = f"{audio_format.get('ext', '?')} {audio_format.get('acodec', '?')} {audio_format.get('abr', 0)}k"
            logger.info(f"Format confirmed for '{item.title}': Video+Audio Merge - V:{item.selected_video_format_id} + A:{item.selected_audio_format_id}")
        
        # Only video format selected (may include audio)
        elif video_format:
            item.selected_video_format_id = video_format['format_id']
            item.selected_video_details = f"{video_format.get('ext', '?')} {video_format.get('height', '?')}p {video_format.get('vcodec', '?')}"
            
            # Check if this video format also has audio
            if video_format.get('acodec', 'none') != 'none':
                item.selected_audio_format_id = video_format['format_id']
                item.selected_audio_details = "(from video)"
            
            logger.info(f"Format confirmed for '{item.title}': Video (maybe w/ audio) - {item.selected_video_format_id}")
        
        # Only audio format selected
        elif audio_format:
            item.selected_audio_format_id = audio_format['format_id']
            item.selected_audio_details = f"{audio_format.get('ext', '?')} {audio_format.get('acodec', '?')} {audio_format.get('abr', 0)}k"
            logger.info(f"Format confirmed for '{item.title}': Audio Only - {item.selected_audio_format_id}")
        
        # No valid selection
        else:
            logger.warning(f"No valid format selection confirmed for '{item.title}'")
        
        self.close_dialog(button)
        self._schedule_ui_update(item)  # Update the main list item display

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

    # --- Create placeholders immediately ---
    logger.debug("Creating placeholder items and widgets")
    for url in args.urls:
        # Create placeholder item
        item = DownloadItem(url=url, title=url) # Show URL as initial title
        item.status = "Queued" # Initial status before fetching starts

        # Set download type if in batch mode
        if args.audio_only:
            item.download_type = 'audio'
        elif args.video:
            item.download_type = 'video'

        # Create widget for the placeholder
        item.widget = tui.create_download_widget(item)

        # Add to TUI state
        tui.downloads.append(item)
        tui.listbox_walker.append(item.widget)

    tui.update_footer() # Update footer with initial count

    # --- Initialize the Urwid MainLoop ---
    logger.debug("Initializing UI MainLoop")
    current_asyncio_loop = asyncio.get_running_loop()
    event_loop = urwid.AsyncioEventLoop(loop=current_asyncio_loop)

    tui.loop = urwid.MainLoop(
        tui.frame,
        tui.palette,
        unhandled_input=tui.handle_input,
        event_loop=event_loop
    )

    # --- Start fetching info in the background AFTER loop is created ---
    logger.info("Starting background tasks to fetch video info")
    for item in tui.downloads:
        logger.debug(f"Creating info fetch task for: {item.url}")
        # --- Pass args to the fetch function ---
        asyncio.create_task(tui.fetch_video_info(item.url, args)) # Pass args here

    # --- Batch downloads are now triggered within fetch_video_info ---
    # (Remove the explicit batch download loop from here)

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