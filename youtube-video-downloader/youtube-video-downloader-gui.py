import os
import sys
import argparse
import asyncio
import urwid
import logging
import pathlib
import atexit
from datetime import timedelta
from logging.handlers import RotatingFileHandler
from typing import Any, List, Dict, Optional

try:
    from ytdl_helper import (
        DownloadItem,
        FormatInfo,
        fetch_info,
        download_item,
        check_ffmpeg,  # Keep check_ffmpeg for initial check if desired
        __version__ as ytdl_helper_version,
    )
except ImportError:
    # Add parent directory to path if running script directly
    script_dir = pathlib.Path(__file__).parent
    sys.path.insert(0, str(script_dir.parent))
    try:
        from ytdl_helper import (
            DownloadItem,
            FormatInfo,
            fetch_info,
            download_item,
            check_ffmpeg,
            __version__ as ytdl_helper_version,
        )
    except ImportError:
        print("Error: Could not import the 'ytdl_helper' library.", file=sys.stderr)
        print(
            "Ensure it's installed or located correctly relative to this script.",
            file=sys.stderr,
        )
        sys.exit(1)

# Store original terminal settings
original_cp = None
original_output_cp = None
original_stdout_encoding = None


def store_terminal_state():
    """Store original terminal settings"""
    global original_cp, original_output_cp, original_stdout_encoding
    if sys.platform == "win32":
        try:
            import ctypes

            kernel32 = ctypes.windll.kernel32
            original_cp = kernel32.GetConsoleCP()
            original_output_cp = kernel32.GetConsoleOutputCP()
            original_stdout_encoding = sys.stdout.encoding
            logger.debug(
                f"Stored original terminal state: CP={original_cp}, Output CP={original_output_cp}, Encoding={original_stdout_encoding}"
            )
        except Exception as e:
            logger.warning(f"Could not store terminal state: {e}")


def restore_terminal_state():
    """Restore original terminal settings"""
    if sys.platform == "win32" and (
        original_cp is not None or original_output_cp is not None
    ):
        try:
            import ctypes

            kernel32 = ctypes.windll.kernel32
            if original_cp is not None:
                kernel32.SetConsoleCP(original_cp)
            if original_output_cp is not None:
                kernel32.SetConsoleOutputCP(original_output_cp)
            # Check if stdout exists and needs reconfiguration
            if (
                hasattr(sys, "stdout")
                and sys.stdout
                and original_stdout_encoding is not None
                and original_stdout_encoding != sys.stdout.encoding
            ):
                try:
                    sys.stdout.reconfigure(encoding=original_stdout_encoding)
                    logger.debug("Restored original terminal state (stdout encoding)")
                except Exception as e:
                    logger.warning(f"Could not restore stdout encoding: {e}")
            else:
                logger.debug("Restored original terminal state (CP/OutputCP only)")

        except Exception as e:
            logger.warning(f"Could not restore terminal state: {e}")


def setup_windows_console():
    """Setup Windows console for proper encoding"""
    if sys.platform == "win32":
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
                logger.warning(
                    f"Failed to set console CP to UTF-8. SetConsoleCP returned {set_cp_success}, SetConsoleOutputCP returned {set_output_cp_success}"
                )
                # Optionally, attempt to restore immediately if setting failed
                # restore_terminal_state()
                # return # Or raise an error?

            # Set Python's console encoding
            # Check if stdout is connected to a terminal/console
            if sys.stdout and sys.stdout.isatty() and sys.stdout.encoding != "utf-8":
                try:
                    sys.stdout.reconfigure(encoding="utf-8")
                    logger.debug("Reconfigured sys.stdout encoding to utf-8")
                except Exception as e:
                    logger.warning(f"Could not reconfigure sys.stdout encoding: {e}")
            elif not (sys.stdout and sys.stdout.isatty()):
                logger.debug(
                    "sys.stdout is not a tty, skipping encoding reconfiguration."
                )

            # Register cleanup function
            atexit.register(restore_terminal_state)

            logger.debug("Windows console configured for UTF-8")
        except ImportError:
            logger.warning("Could not import ctypes. Console encoding setup skipped.")
        except Exception as e:
            logger.warning(f"Could not set console encoding: {e}")


# Setup logging
def setup_logging():
    log_dir = pathlib.Path(__file__).parent / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / "youtube_downloader.log"

    # Create formatters
    file_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s"
    )
    # Use a simpler format for console to avoid noise during TUI operation
    console_formatter = logging.Formatter("%(levelname)s: %(message)s")

    # Setup file handler with rotation
    file_handler = RotatingFileHandler(
        log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(logging.DEBUG)

    # Setup console handler
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(logging.ERROR)

    # Setup root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    # Silence noisy libraries if needed
    logging.getLogger("urwid").setLevel(logging.WARNING)
    logging.getLogger("ytdl_helper").setLevel(logging.INFO)

    return root_logger


logger = setup_logging()


class SelectablePadding(urwid.Padding):
    """A Padding widget that is always selectable."""

    def selectable(self) -> bool:
        return True

class DownloaderTUI:
    palette = [
        ("header", "white", "dark blue"),
        ("footer", "white", "dark blue"),
        ("body", "default", "default"),
        ("focus", "black", "light gray"),
        ("progress_bar", "white", "dark blue"),
        ("progress_normal", "black", "light gray"),
        ("progress_complete", "white", "dark green"),
        ("progress_error", "white", "dark red"),
        ("status_pending", "yellow", ""),
        ("status_downloading", "light blue", ""),
        ("status_processing", "dark cyan", ""),
        ("status_complete", "dark green", ""),
        ("status_error", "dark red", ""),
        ("error_text", "dark red", ""),
        ("title_text", "white", "default"),
        ("duration_text", "dark gray", "default"),
        ("button", "white", "dark blue"),
        ("button_focus", "black", "light gray"),
        ("dialog_border", "black", "white"),
        ("dialog_body", "black", "light gray"),
        ("dialog_shadow", "white", "black"),
    ]

    def __init__(self, args: argparse.Namespace):  # Pass args to __init__
        self.args = args  # Store args for later use (e.g., output dir)
        self.loop_running = False
        self.downloads: List[DownloadItem] = []  # Now uses library's DownloadItem
        self.ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "extract_flat": True,  # Keep true for initial fast fetching if needed, override later
            "skip_download": True,  # Ensure we only fetch info by default
            "verbose": False,  # Keep yt-dlp chatter low
            "ignoreerrors": True,  # Don't let one URL failure stop info fetching for others
        }
        self.listbox_walker = urwid.SimpleListWalker([])
        self.loop: Optional[urwid.MainLoop] = None
        self.frame: Optional[urwid.Frame] = None
        self.current_download_tasks = (
            {}
        )  # Track running download tasks {url: asyncio.Task}
        self.output_dir = pathlib.Path(
            args.output_dir
        ).resolve()  # Get output dir from args
        self.output_dir.mkdir(parents=True, exist_ok=True)  # Ensure it exists
        logger.info(f"GUI Output directory: {self.output_dir}")
        self.setup_display()

    def setup_display(self):
        self.header = urwid.AttrMap(
            urwid.Text(
                "YouTube Downloader - (↑↓ Navigate) (+/- Select) (Enter Format) (d Download Selected) (q Quit)"
            ),
            "header",
        )

        # Main content area - Use AttrMap for focus styling
        self.listbox = urwid.ListBox(self.listbox_walker)  # Keep the ListBox as is

        # Footer
        self.footer_text = urwid.Text("")
        self.footer = urwid.AttrMap(self.footer_text, "footer")

        # Layout
        self.frame = urwid.Frame(
            # --- Use the ListBox directly as the body ---
            self.listbox,
            # --- Remove the AttrMap wrapper that was here ---
            # urwid.AttrMap(self.listbox, 'body', focus_map={'focus': 'focus'}),
            header=self.header,
            footer=self.footer,
        )
        self.update_footer()

    def create_download_widget(self, item: DownloadItem) -> urwid.Widget:
        # --- (pile, status, progress setup remains the same) ---
        selection_marker = f"[{'x' if item.is_selected else ' '}] "
        title_text = f"{selection_marker}{item.title or 'Loading...'}"
        title_widget = urwid.Text(("title_text", title_text))
        duration_str = (
            str(timedelta(seconds=int(item.duration))) if item.duration else "--:--:--"
        )
        duration_widget = urwid.Text(
            ("duration_text", f" ({duration_str})"), align="right"
        )
        header_cols = urwid.Columns(
            [title_widget, ("pack", duration_widget)], dividechars=1
        )

        status_map = {
            "Pending": "status_pending",
            "Fetching Info...": "status_pending",
            "Starting...": "status_downloading",
            "Downloading": "status_downloading",
            "Processing": "status_processing",
            "Complete": "status_complete",
            "Error": "status_error",
            "Cancelled": "status_error",
            "Skipped": "status_pending",
        }
        status_style = status_map.get(item.status, "error_text")
        status_text = f"Status: {item.status}"
        selected_formats_str = ""
        if item.selected_video_format_id:
            selected_formats_str += f" | V: {item.selected_video_details}"
        if item.selected_audio_format_id:
            selected_formats_str += f" | A: {item.selected_audio_details}"
        status_text += selected_formats_str
        if item.error:
            status_text += f" | Error: {item.error[:50]}..."
        status_widget = urwid.AttrMap(urwid.Text(status_text), status_style)

        progress_style = "progress_normal"
        bar_style = "progress_bar"
        if item.status == "Complete" or item.status == "Skipped":
            progress_style = "progress_complete"
        elif item.status == "Error" or item.status == "Cancelled":
            progress_style = "progress_error"
        progress_bar = urwid.ProgressBar(
            progress_style, bar_style, current=item.progress, done=100
        )

        pile = urwid.Pile(
            [
                header_cols,
                status_widget,
                progress_bar,
            ]
        )

        # --- Use SelectablePadding instead of urwid.Padding ---
        padded_widget = SelectablePadding(pile, left=1, right=1)

        # --- Wrap Padding with AttrMap for focus ---
        widget = urwid.AttrMap(padded_widget, None, focus_map="focus")

        item.widget = widget  # Store the outer AttrMap widget
        return widget

    def update_widget_for_item(self, item: DownloadItem):
        """Updates the display components of a specific item's widget."""
        if (
            not item.widget
            or not isinstance(item.widget, urwid.AttrMap)
            or not hasattr(item.widget, "original_widget")
        ):
            logger.warning(
                f"Cannot update widget for {item.title}: Widget structure invalid or missing."
            )
            return
        try:
            # item.widget is the AttrMap
            padded_widget = item.widget.original_widget  # This is the Padding
            pile = padded_widget.original_widget  # This is the Pile
        except AttributeError:
            logger.warning(
                f"Cannot update widget for {item.title}: Widget structure incorrect (AttrMap->Padding->Pile expected)."
            )
            return

        # Update Title/Duration
        header_cols = pile.contents[0][0]
        title_widget = header_cols.contents[0][0]
        duration_widget = header_cols.contents[1][0]
        selection_marker = f"[{'x' if item.is_selected else ' '}] "
        title_text = f"{selection_marker}{item.title or 'Loading...'}"
        title_widget.set_text(("title_text", title_text))
        duration_str = (
            str(timedelta(seconds=int(item.duration))) if item.duration else "--:--:--"
        )
        duration_widget.set_text(("duration_text", f" ({duration_str})"))

        status_map = {
            "Pending": "status_pending",
            "Fetching Info...": "status_pending",
            "Starting...": "status_downloading",
            "Downloading": "status_downloading",
            "Processing": "status_processing",
            "Complete": "status_complete",
            "Error": "status_error",
            "Cancelled": "status_error",
            "Skipped": "status_pending",
        }
        status_style = status_map.get(item.status, "error_text")
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

        # Update Progress Bar
        progress_bar = pile.contents[2][0]
        progress_style = "progress_normal"
        bar_style = "progress_bar"
        current_progress = item.progress
        if item.status == "Complete" or item.status == "Skipped":
            progress_style = "progress_complete"
            current_progress = 100
        elif item.status == "Error" or item.status == "Cancelled":
            progress_style = "progress_error"
            current_progress = 0
        progress_bar.set_completion(current_progress)
        progress_bar.normal = progress_style
        progress_bar.complete = bar_style

    def update_footer(self):
        total = len(self.downloads)
        selected = sum(1 for item in self.downloads if item.is_selected)
        complete = sum(1 for item in self.downloads if item.status == "Complete")
        downloading = sum(
            1
            for item in self.downloads
            if item.status in ["Downloading", "Processing", "Starting"]
        )
        errors = sum(
            1 for item in self.downloads if item.status in ["Error", "Cancelled"]
        )
        # Calculate pending more accurately
        pending = (
            total
            - complete
            - downloading
            - errors
            - sum(1 for item in self.downloads if item.status == "Skipped")
        )

        # --- Add selected count to footer ---
        status_str = f"Total: {total} | Sel: {selected} | ✓: {complete} | ↓: {downloading} | !: {errors} | ?: {pending}"
        self.footer_text.set_text(status_str)

    def handle_input(self, key):
        if key in ("q", "Q"):
            # Check for active downloads before quitting
            active_downloads = [
                t for t in self.current_download_tasks.values() if not t.done()
            ]
            if active_downloads:
                self.show_confirmation_dialog(
                    "Downloads in progress. Quit anyway?", self._confirm_quit
                )
            else:
                raise urwid.ExitMainLoop()
            return True  # Indicate key was handled

        # --- Handle +/- for selection ---
        elif key in ("+", "-"):
            focused_widget, position = self.listbox.get_focus()
            if position < len(self.downloads):
                item = self.downloads[position]
                # Toggle selection only if not actively downloading/processing
                if item.status not in ["Downloading", "Processing", "Starting..."]:
                    item.is_selected = not item.is_selected
                    logger.debug(
                        f"Toggled selection for '{item.title}' to {item.is_selected}"
                    )
                    self._schedule_ui_update(item)
                    self.update_footer()
                else:
                    logger.debug(
                        f"Cannot toggle selection for '{item.title}' while status is {item.status}"
                    )
            return True  # Indicate key was handled

        # --- Handle 'Enter' for format selection (on focused item) ---
        elif key == "enter":
            focused_widget, position = self.listbox.get_focus()
            if position < len(self.downloads):
                item = self.downloads[position]
                # Show format selection if info is fetched (Pending or Error/Cancelled/Skipped to allow re-selection)
                if item.status not in [
                    "Queued",
                    "Fetching Info...",
                    "Downloading",
                    "Processing",
                    "Starting...",
                ]:
                    if not item.audio_formats and not item.video_formats:
                        self.show_message_dialog(
                            f"No format information available for {item.title}.\nStatus: {item.status}\nError: {item.error}",
                            title="Format Info Missing",
                        )
                    else:
                        self.show_detailed_format_selection(item)  # <-- Call new dialog
                elif item.status in ["Downloading", "Processing", "Starting..."]:
                    self.show_message_dialog(
                        f"Download in progress for {item.title}...", title="In Progress"
                    )
                else:  # Queued or Fetching Info
                    self.show_message_dialog(
                        f"Still fetching info for {item.title}...", title="Loading"
                    )
            return True  # Indicate key was handled

        # --- Handle 'd' for downloading selected items ---
        elif key in ("d", "D"):
            selected_items = [item for item in self.downloads if item.is_selected]
            items_to_download = []
            missing_selection = []  # Renamed from missing_format

            if not selected_items:
                self.show_message_dialog(
                    "No items selected. Use '+' or '-' to select items first.",
                    title="Nothing Selected",
                )
                return True

            for item in selected_items:
                if item.status in [
                    "Downloading",
                    "Processing",
                    "Starting...",
                    "Complete",
                    "Skipped",
                    "Queued",
                    "Fetching Info...",
                ]:
                    logger.warning(
                        f"Skipping download trigger for '{item.title}': Status is '{item.status}'"
                    )
                    continue

                # --- Check if *any* format ID is selected ---
                if (
                    not item.selected_audio_format_id
                    and not item.selected_video_format_id
                ):
                    missing_selection.append(item.title)
                else:
                    # Reset error/cancelled status before retrying
                    if item.status in ["Error", "Cancelled"]:
                        item.error = None
                        item.status = "Pending"  # Reset status
                        self._schedule_ui_update(item)
                    items_to_download.append(item)

            if missing_selection:
                missing_titles = "\n - ".join(missing_selection)
                # --- Update message ---
                self.show_message_dialog(
                    f"Cannot start download. Format not selected for:\n - {missing_titles}\n\nUse Enter on each item to select formats first.",
                    title="Format Not Selected",
                )
                return True  # Indicate key was handled

            if not items_to_download:
                self.show_message_dialog(
                    "No eligible items to download among selected.",
                    title="Download Info",
                )
                return True

            # --- Call library download function ---
            logger.info(
                f"Starting download for {len(items_to_download)} selected items."
            )
            for item in items_to_download:
                logger.debug(f"Creating download task for selected item: {item.title}")
                # Create task to call library's download_item
                task = asyncio.create_task(
                    download_item(
                        item=item,
                        output_dir=self.output_dir,  # Use stored output dir
                        target_format=None,  # GUI doesn't support selecting this yet
                        progress_callback=self.library_progress_hook,  # Use adapted callback
                        status_callback=self.library_status_hook,  # Use adapted callback
                    )
                )
                self.current_download_tasks[item.url] = task  # Track the task

            self.update_footer()
            return True

        return key

    def _confirm_quit(self, button):
        if button.label.lower() == "yes":
            for task in self.current_download_tasks.values():
                if not task.done():
                    task.cancel()
            raise urwid.ExitMainLoop()
        else:
            self.close_dialog(button)

    def library_progress_hook(self, item: DownloadItem, progress_data: Dict[str, Any]):
        """Callback adapter for library's progress updates."""
        # The library calculates item.progress internally based on bytes
        # We just need to trigger a UI update.
        # We can optionally use more data from progress_data if needed later.
        try:
            # Status update (like "Downloading") is handled by status hook now
            # Only trigger UI redraw based on progress change
            self._schedule_ui_update(item)
            self.update_footer()  # Footer update is likely fine here
        except Exception as e:
            logger.error(
                f"Error in library progress hook for {item.title}: {str(e)}",
                exc_info=True,
            )
            item.status = "Error"  # Mark item as error if hook fails
            item.error = "Progress Hook error"
            self._schedule_ui_update(item)
            self.update_footer()

    def library_status_hook(
        self, item: DownloadItem, status: str, error: Optional[str]
    ):
        """Callback adapter for library's status updates."""
        # The library updates item.status and item.error directly.
        # We just need to trigger a UI update and handle task cleanup.
        try:
            logger.debug(
                f"Library status hook for '{item.title}': Status={status}, Error={error}"
            )
            self._schedule_ui_update(item)
            self.update_footer()

            # Clean up task tracking if download finished/failed/cancelled
            if status in ["Complete", "Error", "Cancelled", "Skipped"]:
                if item.url in self.current_download_tasks:
                    logger.debug(
                        f"Removing task tracking for '{item.title}' (Status: {status})"
                    )
                    del self.current_download_tasks[item.url]
                else:
                    # This might happen if the task finished very quickly or was never tracked properly
                    logger.warning(
                        f"Attempted to remove task tracking for '{item.title}', but URL not found in tracked tasks."
                    )

        except Exception as e:
            logger.error(
                f"Error in library status hook for {item.title}: {str(e)}",
                exc_info=True,
            )
            # Avoid crashing the hook itself
            # The library should have already set the item's status/error
            self._schedule_ui_update(item)  # Still try to update UI
            self.update_footer()

    def close_dialog(self, button):
        """Restores the main frame view."""
        if self.loop and self.frame:
            self.loop.widget = self.frame
        else:
            logger.error("Cannot close dialog: loop or frame not initialized.")

    def show_confirmation_dialog(self, message: str, callback):
        """Shows a Yes/No confirmation dialog."""
        body = [
            urwid.Text(message, align="center"),
            urwid.Divider(),
            urwid.Columns(
                [
                    (
                        "weight",
                        1,
                        urwid.Padding(
                            urwid.AttrMap(
                                urwid.Button("Yes", on_press=callback),
                                "button",
                                focus_map="button_focus",
                            ),
                            align="center",
                            width=("relative", 80),
                        ),
                    ),
                    (
                        "weight",
                        1,
                        urwid.Padding(
                            urwid.AttrMap(
                                urwid.Button("No", on_press=self.close_dialog),
                                "button",
                                focus_map="button_focus",
                            ),
                            align="center",
                            width=("relative", 80),
                        ),
                    ),
                ],
                dividechars=2,
            ),
        ]
        dialog = urwid.LineBox(urwid.Pile(body), title="Confirm")
        dialog = urwid.AttrMap(dialog, "dialog_border")
        overlay = urwid.Overlay(
            dialog,
            self.frame,
            align="center",
            width=("relative", 50),
            valign="middle",
            height="pack",
            min_width=30,
        )
        self.loop.widget = overlay

    def show_message_dialog(self, message: str, title: str = "Message"):
        """Shows a simple message dialog with an OK button."""
        body = [
            urwid.Text(message),  # Allow multi-line messages
            urwid.Divider(),
            urwid.Padding(
                urwid.AttrMap(
                    urwid.Button("OK", on_press=self.close_dialog),
                    "button",
                    focus_map="button_focus",
                ),
                align="center",
                width=("relative", 50),
            ),
        ]
        dialog = urwid.LineBox(urwid.Pile(body), title=title)
        dialog = urwid.AttrMap(dialog, "dialog_border")
        overlay = urwid.Overlay(
            dialog,
            self.frame,
            align="center",
            width=("relative", 70),
            valign="middle",
            height="pack",
            min_width=40,
        )
        self.loop.widget = overlay

    def _schedule_ui_update(self, item: DownloadItem):
        """Schedules widget update and screen redraw on the main event loop."""
        if not self.loop or not hasattr(self.loop, "event_loop"):
            logger.error(
                "Cannot schedule UI update: Main loop or event_loop not available."
            )
            return

        # --- Access the underlying asyncio loop ---
        urwid_event_loop = self.loop.event_loop
        if not hasattr(urwid_event_loop, "_loop"):
            logger.error(
                "Cannot schedule UI update: Urwid event loop does not have '_loop' attribute."
            )
            return

        asyncio_loop = urwid_event_loop._loop
        # Ensure we don't schedule updates for items without widgets yet
        if item.widget:
            asyncio_loop.call_soon(self._do_update_and_draw, item)
        else:
            logger.warning(
                f"Skipping UI update schedule for '{item.title}': Widget not yet created."
            )

    def _do_update_and_draw(self, item: DownloadItem):
        """Helper function called by call_soon to update widget."""
        try:
            self.update_widget_for_item(item)
            if self.loop and self.loop_running:
                self.loop.draw_screen()
        except Exception as e:
            # Log errors happening during the scheduled update
            logger.error(
                f"Error during scheduled UI update for {item.title}: {e}", exc_info=True
            )

    def _format_info_to_str(self, f: FormatInfo) -> str:
        """Helper to create a display string for a FormatInfo object."""
        # Use the __str__ method defined in the library's FormatInfo class
        return str(f)

    def show_detailed_format_selection(self, item: DownloadItem):
        logger.debug(f"Showing detailed format selection for {item.title}")

        # --- Mode Selection ---
        mode_group = []

        # Determine current mode
        current_mode = "combined"
        if (
            item.selected_video_format_id is not None
            and item.selected_audio_format_id is None
        ):
            current_mode = "video_only"
        elif (
            item.selected_audio_format_id is not None
            and item.selected_video_format_id is None
        ):
            current_mode = "audio_only"
        elif (
            item.selected_video_format_id is not None
            and item.selected_audio_format_id is not None
            and item.selected_video_format_id == item.selected_audio_format_id
        ):
            # If IDs match, default mode could still be combined, but selection reflects it
            pass  # Keep combined as default visual

        # Create mode selection widgets
        mode_widgets = []
        modes = [
            ("combined", "Video + Audio (Combined/Merged)"),
            ("video_only", "Video Only (No Audio Merge)"),
            ("audio_only", "Audio Only (No Video)"),
        ]

        for mode_value, mode_label in modes:
            rb = urwid.RadioButton(
                mode_group, mode_label, state=(current_mode == mode_value)
            )
            mode_widgets.append(rb)

        # --- Video Format Selection (using FormatInfo) ---
        video_group = []
        video_widgets = []
        # Use item.video_formats (list of FormatInfo)
        for i, f_info in enumerate(item.video_formats):
            label = self._format_info_to_str(f_info)  # Use updated helper
            is_selected = item.selected_video_format_id == f_info.format_id
            rb = urwid.RadioButton(video_group, label, state=is_selected)
            video_widgets.append(rb)
        if not item.video_formats:
            video_widgets.append(urwid.Text(" (None available)"))

        # --- Audio Format Selection (using FormatInfo) ---
        audio_group = []
        audio_widgets = []
        # Use item.audio_formats (list of FormatInfo)
        for i, f_info in enumerate(item.audio_formats):
            label = self._format_info_to_str(f_info)  # Use updated helper
            is_selected = item.selected_audio_format_id == f_info.format_id
            rb = urwid.RadioButton(audio_group, label, state=is_selected)
            audio_widgets.append(rb)
        if not item.audio_formats:
            audio_widgets.append(urwid.Text(" (None available)"))

        # --- Dialog Layout ---
        # Create button widgets
        confirm_button = urwid.Button(
            "Confirm Selection",
            on_press=self._confirm_format_selection,
            user_data=(item, mode_group, video_group, audio_group),
        )
        cancel_button = urwid.Button("Cancel", on_press=self.close_dialog)

        # Apply styling to buttons
        confirm_button = urwid.AttrMap(
            confirm_button, "button", focus_map="button_focus"
        )
        cancel_button = urwid.AttrMap(cancel_button, "button", focus_map="button_focus")

        # Create scrollable listboxes
        mode_list = urwid.ListBox(urwid.SimpleListWalker(mode_widgets))
        video_list = urwid.ListBox(urwid.SimpleListWalker(video_widgets))
        audio_list = urwid.ListBox(urwid.SimpleListWalker(audio_widgets))

        # Set conservative fixed heights
        mode_box = urwid.BoxAdapter(
            mode_list, height=3
        )  # Just enough for 3 radio buttons

        # Place lists in LineBoxes with titles
        mode_box_framed = urwid.LineBox(mode_box, title="Mode")
        video_box_framed = urwid.LineBox(video_list, title="Video")
        audio_box_framed = urwid.LineBox(audio_list, title="Audio")

        # Title text
        title_text = urwid.Text(f"Select formats for:\n{item.title}", align="center")

        # Button row
        button_row = urwid.Columns(
            [
                ("pack", confirm_button),
                ("pack", cancel_button),
            ],
            dividechars=4,
        )

        # This widget itself will be given weighted height in the Pile
        body_columns = urwid.Columns(
            [("weight", 1, video_box_framed), ("weight", 1, audio_box_framed)],
            dividechars=1,
        )

        # Use Pile for layout with explicit sizing
        dialog_pile = urwid.Pile(
            [
                ("pack", title_text),  # Auto-size for title
                ("pack", urwid.Divider("-")),  # Single line divider
                ("pack", mode_box_framed),  # Fixed size mode box
                ("weight", 1, body_columns),  # Let this expand vertically
                ("pack", urwid.Divider("-")),  # Single line divider
                ("pack", button_row),  # Auto-size button row
            ]
        )

        # Create a padded container for the dialog
        padded_dialog = urwid.Padding(dialog_pile, left=1, right=1)

        # Wrap dialog content in LineBox and AttrMap
        dialog = urwid.LineBox(padded_dialog, title="Format Selection")
        dialog = urwid.AttrMap(dialog, "dialog_border")

        # Create overlay with fixed size
        overlay = urwid.Overlay(
            dialog,
            self.frame,
            align="center",
            width=("relative", 70),  # Make width slightly smaller
            valign="middle",
            height=("relative", 70),  # Make height slightly smaller
            min_width=50,
            min_height=15,  # Reduce minimum height
        )

        # Set as the main widget
        self.loop.widget = overlay

    def _confirm_format_selection(self, button, user_data):
        """Callback when 'Confirm' is pressed in the detailed format dialog."""
        item, mode_group, video_group, audio_group = user_data

        # Find selected radio buttons
        selected_mode_rb = next((rb for rb in mode_group if rb.state), None)
        selected_video_rb = next((rb for rb in video_group if rb.state), None)
        selected_audio_rb = next((rb for rb in audio_group if rb.state), None)

        # Determine selected mode
        selected_mode = "combined"
        if selected_mode_rb:
            mode_index = mode_group.index(selected_mode_rb)
            if mode_index == 1:
                selected_mode = "video_only"
            elif mode_index == 2:
                selected_mode = "audio_only"

        # --- Get selected FormatInfo objects ---
        video_format_info: Optional[FormatInfo] = None
        if selected_video_rb and item.video_formats:
            try:
                video_index = video_group.index(selected_video_rb)
                if 0 <= video_index < len(item.video_formats):
                    video_format_info = item.video_formats[video_index]
            except (ValueError, IndexError):
                logger.warning("Failed to get video format info")

        audio_format_info: Optional[FormatInfo] = None
        if selected_audio_rb and item.audio_formats:
            try:
                audio_index = audio_group.index(selected_audio_rb)
                if 0 <= audio_index < len(item.audio_formats):
                    audio_format_info = item.audio_formats[audio_index]
            except (ValueError, IndexError):
                logger.warning("Failed to get audio format info")

        # Reset selections before applying new ones
        item.selected_video_format_id = None
        item.selected_audio_format_id = None

        # Apply selections based on mode and selected formats
        if selected_mode == "audio_only" and audio_format_info:
            item.selected_audio_format_id = audio_format_info.format_id
            logger.info(
                f"Format confirmed for '{item.title}': Audio Only - {item.selected_audio_format_id}"
            )

        elif selected_mode == "video_only" and video_format_info:
            item.selected_video_format_id = video_format_info.format_id
            # Check if video includes audio
            if video_format_info.acodec and video_format_info.acodec != "none":
                item.selected_audio_format_id = (
                    video_format_info.format_id
                )  # Use same ID
            logger.info(
                f"Format confirmed for '{item.title}': Video Only - V:{item.selected_video_format_id} A:{item.selected_audio_format_id}"
            )

        elif selected_mode == "combined":
            if video_format_info:
                item.selected_video_format_id = video_format_info.format_id
            if audio_format_info:
                item.selected_audio_format_id = audio_format_info.format_id
            # If video selected but no separate audio, check if video has audio
            elif (
                video_format_info
                and video_format_info.acodec
                and video_format_info.acodec != "none"
            ):
                item.selected_audio_format_id = video_format_info.format_id

            logger.info(
                f"Format confirmed for '{item.title}': Combined/Merge - V:{item.selected_video_format_id} A:{item.selected_audio_format_id}"
            )

        if not item.selected_video_format_id and not item.selected_audio_format_id:
            logger.warning(f"No valid format selection confirmed for '{item.title}'")

        self.close_dialog(button)
        self._schedule_ui_update(item)


# --- Main Execution Logic ---


async def setup_application(args) -> urwid.MainLoop:
    """Sets up the TUI, fetches initial data, and returns the configured MainLoop."""
    # Setup Windows console encoding (should happen early)
    setup_windows_console()

    # Initial FFmpeg check (optional, library checks again before download)
    if not check_ffmpeg():
        print("\nError: FFmpeg is required but not found.", file=sys.stderr)
        # Optionally provide more detailed instructions here
        sys.exit(1)

    logger.info("Starting YouTube Downloader Setup")
    if not args.urls:
        logger.error("No URLs provided")
        print("Error: No YouTube URLs provided.", file=sys.stderr)
        sys.exit(1)

    logger.info(f"Processing {len(args.urls)} URLs")
    mode = "Interactive"
    if args.audio_only:
        mode = "Audio only (batch)"
    elif args.video:
        mode = "Video + Audio (batch)"
    logger.info(f"Mode: {mode}")

    tui = DownloaderTUI(args)
    tui.loop = urwid.MainLoop(...)

    # --- Create placeholders using library's DownloadItem ---
    logger.debug("Creating placeholder items and widgets")
    for url in args.urls:
        # Create placeholder item using library class
        item = DownloadItem(url=url, title=url, status="Queued")

        # Create widget for the placeholder
        item.widget = tui.create_download_widget(item)

        tui.downloads.append(item)
        tui.listbox_walker.append(item.widget)

    tui.update_footer()

    # --- Initialize the Urwid MainLoop ---
    logger.debug("Initializing UI MainLoop")
    current_asyncio_loop = asyncio.get_running_loop()
    event_loop = urwid.AsyncioEventLoop(loop=current_asyncio_loop)

    tui.loop = urwid.MainLoop(
        tui.frame, tui.palette, unhandled_input=tui.handle_input, event_loop=event_loop
    )

    # --- Start fetching info using library function ---
    logger.info("Starting background tasks to fetch video info")
    fetch_tasks = []
    for item in tui.downloads:
        logger.debug(f"Creating info fetch task for: {item.url}")
        # Call library's fetch_info
        fetch_tasks.append(asyncio.create_task(fetch_info(item.url)))

    # --- Process fetch results ---
    # Wait for all fetch tasks to complete
    results = await asyncio.gather(*fetch_tasks, return_exceptions=True)

    batch_items_to_download = []
    for i, result in enumerate(results):
        original_item = tui.downloads[i]  # Get the placeholder item
        if isinstance(result, Exception):
            logger.error(f"Failed to fetch info for {original_item.url}: {result}")
            original_item.status = "Error"
            original_item.error = f"Fetch failed: {result}"
            tui._schedule_ui_update(original_item)  # Update UI for the error
        elif isinstance(result, DownloadItem):
            logger.debug(f"Successfully fetched info for {result.url}")
            # Update the placeholder item with fetched data
            # This assumes the library fetch_info returns a fully populated DownloadItem
            # We need to merge the fetched data into the existing item that has the widget
            original_item.title = result.title
            original_item.duration = result.duration
            original_item.artist = result.artist
            original_item.year = result.year
            original_item.audio_formats = result.audio_formats
            original_item.video_formats = result.video_formats
            original_item._raw_info = result._raw_info
            original_item.status = "Pending"  # Set status to Pending
            original_item.error = None

            # --- Auto-select best formats (similar logic as before, but on library item) ---
            best_video = (
                original_item.video_formats[0] if original_item.video_formats else None
            )
            best_audio = (
                original_item.audio_formats[0] if original_item.audio_formats else None
            )

            original_item.selected_video_format_id = None
            original_item.selected_audio_format_id = None

            preselect_audio = args.audio_only
            preselect_video = args.video

            if preselect_audio and best_audio:
                original_item.selected_audio_format_id = best_audio.format_id
                logger.debug(
                    f"Pre-selected best audio for {original_item.title}: {original_item.selected_audio_format_id}"
                )
            elif preselect_video and best_video:
                original_item.selected_video_format_id = best_video.format_id
                if best_video.acodec and best_video.acodec != "none":
                    original_item.selected_audio_format_id = best_video.format_id
                elif best_audio:
                    original_item.selected_audio_format_id = best_audio.format_id
                logger.debug(
                    f"Pre-selected best video (and maybe audio) for {original_item.title}: V:{original_item.selected_video_format_id} A:{original_item.selected_audio_format_id}"
                )
            elif best_video:  # Default interactive mode
                original_item.selected_video_format_id = best_video.format_id
                if best_video.acodec and best_video.acodec != "none":
                    original_item.selected_audio_format_id = best_video.format_id
                elif best_audio:
                    original_item.selected_audio_format_id = best_audio.format_id
                logger.debug(
                    f"Pre-selected default best video/audio for {original_item.title}: V:{original_item.selected_video_format_id} A:{original_item.selected_audio_format_id}"
                )
            elif best_audio:  # Fallback if only audio exists
                original_item.selected_audio_format_id = best_audio.format_id
                logger.debug(
                    f"Pre-selected fallback best audio for {original_item.title}: {original_item.selected_audio_format_id}"
                )

            tui._schedule_ui_update(original_item)  # Update UI with fetched info

            # --- Check if ready for batch download ---
            batch_ready = False
            if args.audio_only and original_item.selected_audio_format_id:
                batch_ready = True
            elif (
                args.video and original_item.selected_video_format_id
            ):  # Video selection is sufficient
                batch_ready = True
            # Add other batch conditions if needed

            if batch_ready and original_item.status == "Pending":
                logger.info(
                    f"Queueing batch download for '{original_item.title}' (Pre-selected)"
                )
                batch_items_to_download.append(original_item)
        else:
            # Should not happen if fetch_info returns DownloadItem or raises Exception
            logger.error(
                f"Unexpected result type from fetch_info for {original_item.url}: {type(result)}"
            )
            original_item.status = "Error"
            original_item.error = "Unexpected fetch result"
            tui._schedule_ui_update(original_item)

    tui.update_footer()  # Update footer after processing all fetches

    # --- Start batch downloads (if any) ---
    if batch_items_to_download:
        logger.info(f"Starting {len(batch_items_to_download)} batch downloads...")
        for item in batch_items_to_download:
            logger.debug(f"Creating batch download task for: {item.title}")
            task = asyncio.create_task(
                download_item(
                    item=item,
                    output_dir=tui.output_dir,
                    target_format=None,  # Use library default or add GUI option later
                    progress_callback=tui.library_progress_hook,
                    status_callback=tui.library_status_hook,
                )
            )
            tui.current_download_tasks[item.url] = task

    logger.debug("Setup complete, returning Urwid MainLoop")
    return tui.loop, tui


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=f"YouTube Downloader TUI (using ytdl_helper v{ytdl_helper_version})"
    )
    parser.add_argument("urls", nargs="*", help="YouTube URLs to download")
    parser.add_argument(
        "--audio-only",
        action="store_true",
        help="Select best audio only for all URLs (batch mode)",
    )
    parser.add_argument(
        "--video",
        action="store_true",
        help="Select best video (+audio) for all URLs (batch mode)",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        default=".",
        help="Directory to save downloaded files (default: current directory)",
    )
    args = parser.parse_args()

    if sys.platform == "win32":
        try:
            print(
                "DEBUG: Setting asyncio event loop policy to WindowsSelectorEventLoopPolicy (pre-run)"
            )
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        except Exception as policy_err:
            print(
                f"ERROR: Failed to set asyncio event loop policy: {policy_err}",
                file=sys.stderr,
            )

    main_loop = None
    tui_instance = None # Keep track of the TUI instance
    exit_code = 0
    try:
        loop = asyncio.get_event_loop()
        main_loop, tui_instance = loop.run_until_complete(setup_application(args))
        logger.debug("Starting Urwid main loop...")
        if tui_instance:
            tui_instance.loop_running = True # Set the flag HERE
        else:
            logger.error("Could not get TUI instance to set loop_running flag.")

        main_loop.run()
    except KeyboardInterrupt:
        logger.info("Program interrupted by user")
        print("\nExiting...")
    except SystemExit as e:
        exit_code = e.code if e.code is not None else 0
        logger.warning(f"Program exited with code {exit_code}")
    except Exception as e:
        logger.critical(f"Unexpected error in main execution: {str(e)}", exc_info=True)
        print(f"\nCRITICAL ERROR: {e}", file=sys.stderr)
        print(
            "Please check the log file in the 'logs' directory for details.",
            file=sys.stderr,
        )
        exit_code = 1
    finally:
        logger.info("Program finished")
        sys.exit(exit_code)
