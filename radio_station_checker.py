import argparse
import re
import requests
import time
from pathlib import Path
from typing import List, Dict, Type, Optional
import logging
import signal
import sys
import threading
from rich.console import Console
import concurrent.futures
from rich.live import Live
from rich.table import Table
from rich.panel import Panel

# --- Format Handler Registry ---

class FormatHandlerRegistry:
    _handlers: Dict[str, 'BaseFormatHandler'] = {}

    @classmethod
    def register(cls, handler_cls):
        for ext in handler_cls.extensions():
            cls._handlers[ext] = handler_cls
        return handler_cls

    @classmethod
    def get_handler(cls, ext):
        ext = ext.lower()
        if ext in cls._handlers:
            return cls._handlers[ext]
        raise ValueError(f"No handler registered for extension: {ext}")

    @classmethod
    def detect_handler(cls, filepath):
        ext = Path(filepath).suffix.lower()
        if ext in cls._handlers:
            return cls._handlers[ext]
        # fallback: try to guess by content
        with open(filepath, encoding="utf-8") as f:
            first = f.readline()
            for handler in cls._handlers.values():
                if handler.sniff(first):
                    return handler
        raise ValueError("Unknown format for file: " + filepath)

    @classmethod
    def all_formats(cls):
        return list(cls._handlers.keys())

# --- Base Format Handler ---

class BaseFormatHandler:
    @staticmethod
    def extensions() -> List[str]:
        raise NotImplementedError

    @staticmethod
    def sniff(first_line: str) -> bool:
        raise NotImplementedError

    @staticmethod
    def load(filepath: str):
        """Return (stations, metadata) tuple."""
        raise NotImplementedError

    @staticmethod
    def save(filepath: str, stations: List[Dict], metadata=None):
        raise NotImplementedError

# --- SII Handler ---

@FormatHandlerRegistry.register
class SiiFormatHandler(BaseFormatHandler):
    @staticmethod
    def extensions():
        return [".sii"]

    @staticmethod
    def sniff(first_line):
        return first_line.startswith("SiiNunit")

    @staticmethod
    def load(filepath):
        stations = []
        metadata = {}
        with open(filepath, encoding="utf-8") as f:
            for line in f:
                m = re.match(r'\s*live_stream_def\s*:\s*([^\s{]+)\s*{', line)
                if m:
                    metadata["identity"] = m.group(1)
                m = re.match(r'\s*stream_data\[\d+\]:\s*"([^"]+)"', line)
                if m:
                    parts = m.group(1).split('|')
                    url = parts[0].strip()
                    meta = parts[1:]
                    stations.append({'url': url, 'meta': meta, 'raw': m.group(1)})
        return stations, metadata

    @staticmethod
    def save(filepath, stations, metadata=None):
        identity = (metadata or {}).get("identity") or "_nameless.0"
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(f"SiiNunit\n{{\nlive_stream_def : {identity} {{\n stream_data: {len(stations)}\n")
            for i, s in enumerate(stations):
                f.write(f' stream_data[{i}]: "{s["raw"]}"\n')
            f.write("}\n}\n")

# --- PLS Handler ---

@FormatHandlerRegistry.register
class PlsFormatHandler(BaseFormatHandler):
    @staticmethod
    def extensions():
        return [".pls"]

    @staticmethod
    def sniff(first_line):
        return first_line.startswith("[playlist]")

    @staticmethod
    def load(filepath):
        stations = []
        # No metadata for PLS
        with open(filepath, encoding="utf-8") as f:
            for line in f:
                if line.lower().startswith("file"):
                    url = line.split('=',1)[1].strip()
                    stations.append({'url': url, 'meta': [], 'raw': url})
        return stations, {}

    @staticmethod
    def save(filepath, stations, metadata=None):
        with open(filepath, "w", encoding="utf-8") as f:
            f.write("[playlist]\n")
            for i, s in enumerate(stations, 1):
                f.write(f"File{i}={s['url']}\n")
            f.write(f"NumberOfEntries={len(stations)}\n")

# --- M3U Handler ---

@FormatHandlerRegistry.register
class M3uFormatHandler(BaseFormatHandler):
    @staticmethod
    def extensions():
        return [".m3u"]

    @staticmethod
    def sniff(first_line):
        return first_line.startswith("#EXTM3U")

    @staticmethod
    def load(filepath):
        stations = []
        # No metadata for M3U
        with open(filepath, encoding="utf-8") as f:
            meta = []
            for line in f:
                line = line.strip()
                if not line or line.startswith('#EXTM3U'):
                    continue
                if line.startswith('#EXTINF'):
                    meta = [line]
                elif not line.startswith('#'):
                    stations.append({'url': line, 'meta': meta, 'raw': line})
                    meta = []
        return stations, {}

    @staticmethod
    def save(filepath, stations, metadata=None):
        with open(filepath, "w", encoding="utf-8") as f:
            f.write("#EXTM3U\n")
            for s in stations:
                for m in s['meta']:
                    f.write(f"{m}\n")
                f.write(f"{s['url']}\n")

# --- Station Checking ---

def check_station(url, retries=2, timeout=3, backoff_factor=1.2):
    import requests
    import socket
    current_timeout = timeout
    for attempt in range(retries + 1):
        try:
            resp = requests.get(url, stream=True, timeout=current_timeout, headers={"Icy-MetaData": "1"})
            if resp.status_code == 200:
                return True
        except requests.exceptions.RequestException as e:
            logging.debug(f"  [retry {attempt+1}] Error: {e}")
            return False
        except socket.gaierror as e:
            logging.debug(f"  [retry {attempt+1}] Name resolution error: {e}")
            return False
        time.sleep(current_timeout)
        current_timeout *= backoff_factor
    return False

def check_station_threadsafe(args):
    # Helper for thread pool: (url, retries, timeout, backoff)
    url, retries, timeout, backoff = args
    return check_station(
        url,
        retries=retries,
        timeout=timeout,
        backoff_factor=backoff
    )

# --- Logging Setup ---

def setup_logging(logfile=None, loglevel=logging.INFO):
    handlers = []
    formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    handlers.append(console)
    if logfile:
        file_handler = logging.FileHandler(logfile, encoding="utf-8")
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)
    logging.basicConfig(level=loglevel, handlers=handlers)

# --- URL Sanitization ---

def sanitize_url(url: str) -> str:
    # Remove whitespace, newlines, and trailing semicolons or slashes
    url = url.strip()
    url = url.replace('\n', '').replace('\r', '')
    url = url.replace(r"\n", '')
    # Remove all trailing slashes and semicolons (not just one)
    while url.endswith('/') or url.endswith(';'):
        url = url[:-1]
    return url

def render_station_table_fast(stations, results, current_idx, window=None, manual_scroll_offset=0):
    """Fast virtualized rendering - only visible stations, no Progress overhead."""
    total = len(stations)
    if window is None:
        window = 10  # Default fallback, caller should set this
    
    # Apply manual scroll offset for user navigation
    display_idx = max(0, min(total - 1, current_idx + manual_scroll_offset))
    
    half_window = window // 2
    start = max(0, display_idx - half_window)
    end = min(total, start + window)
    start = max(0, end - window)  # ensure window size if near end

    table = Table(show_header=True, header_style="bold magenta", box=None, show_lines=False, expand=True)
    table.add_column("Status", width=6, no_wrap=True)
    table.add_column("Station", overflow="fold")
    table.add_column("Progress", width=16, no_wrap=True)
    table.add_column("Result", width=6, justify="right", no_wrap=True)

    def simple_bar(color, filled=True):
        # Simple static bar representation
        if filled:
            return f"[{color}]{'█' * 12}[/{color}]"
        else:
            return f"[grey62]{'░' * 12}[/grey62]"

    for i in range(start, end):
        s = stations[i]
        station_name = f"[{i+1}/{total}] {s['url']}"
        
        # Highlight the current focus item
        if i == display_idx:
            station_name = f"[bold yellow]> {station_name}[/bold yellow]"
        
        if results[i] is None:  # In progress or pending
            status = "[blue]●[/blue]"
            percent = "[blue] ...[/blue]"
            bar = simple_bar("blue", False)
        elif results[i]:  # Success
            status = "[green]●[/green]"
            percent = "[green] OK[/green]"
            bar = simple_bar("green", True)
        else:  # Failed
            status = "[red]●[/red]"
            percent = "[red]NOK[/red]"
            bar = simple_bar("red", True)
            
        table.add_row(status, station_name, bar, percent)
    
    if total > window:
        scroll_info = f"[dim]Showing {start+1}-{end} of {total} | Use ↑↓ or mouse wheel to scroll | Ctrl+C to exit[/dim]"
        return Panel(table, title=scroll_info, padding=(0,1))
    else:
        return table

# --- Main Logic ---

def main():
    parser = argparse.ArgumentParser(description="Check online radio stations from playlist or SII files.")
    parser.add_argument("input", nargs="?", default=None, help="Input file (.pls, .m3u, .sii)")
    parser.add_argument("-o", "--output", help="Output file for live stations")
    parser.add_argument("-f", "--format", choices=[f[1:] for f in FormatHandlerRegistry.all_formats()], help="Output format")
    parser.add_argument("--retries", type=int, default=2, help="Retries per station")
    parser.add_argument("--timeout", type=int, default=3, help="Timeout per request (seconds)")
    parser.add_argument("--backoff", type=float, default=1.2, help="Backoff multiplier for each retry")
    parser.add_argument("--logfile", help="Log to file (in addition to console)")
    parser.add_argument("--loglevel", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help="Logging level")
    parser.add_argument("--sanitize", default=True, action="store_true", help="Sanitize URLs before checking")
    args = parser.parse_args()

    setup_logging(args.logfile, getattr(logging, args.loglevel.upper()))

    if not args.input:
        parser.error("the following arguments are required: input")

    # Detect input handler
    input_handler_cls = FormatHandlerRegistry.detect_handler(args.input)
    stations, input_metadata = input_handler_cls.load(args.input)
    logging.info(f"Loaded {len(stations)} stations from {args.input}")

    # Optionally sanitize URLs after load, before processing
    if args.sanitize:
        for s in stations:
            orig_url = s['url']
            sanitized = sanitize_url(orig_url)
            s['url'] = sanitized
            # Always update the url part in 'raw' if present
            if 'raw' in s:
                parts = s['raw'].split('|', 1)
                if len(parts) > 1:
                    s['raw'] = sanitized + '|' + parts[1]
                else:
                    s['raw'] = sanitized

    live_stations = []
    total = len(stations)
    console = Console()
    results: List[Optional[bool]] = [None] * total
    
    # Graceful shutdown handling
    shutdown_event = threading.Event()
    def signal_handler(signum, frame):
        console.print("\n[yellow]Received interrupt signal. Shutting down gracefully...[/yellow]")
        shutdown_event.set()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # User scroll control
    manual_scroll_offset = 0
    
    def handle_input():
        """Handle keyboard input for scrolling in a separate thread."""
        nonlocal manual_scroll_offset
        try:
            if sys.platform == "win32":
                import msvcrt
            else:
                import termios
                import tty
            
            while not shutdown_event.is_set():
                if sys.platform == "win32" and msvcrt.kbhit():
                    key = msvcrt.getch()
                    if key == b'\xe0':  # Arrow key prefix on Windows
                        key = msvcrt.getch()
                        if key == b'H':  # Up arrow
                            manual_scroll_offset = max(manual_scroll_offset - 1, -total//2)
                        elif key == b'P':  # Down arrow
                            manual_scroll_offset = min(manual_scroll_offset + 1, total//2)
                elif sys.platform != "win32":
                    # Unix-like systems - simplified input handling
                    pass
                time.sleep(0.1)
        except:
            pass  # Input handling is optional, don't crash on errors
    
    # Start input handler thread
    input_thread = threading.Thread(target=handle_input, daemon=True)
    input_thread.start()
    
    # Tracking for smart updates
    last_visible_range = None
    last_visible_results = None
    last_window_size = None
    last_scroll_offset = None
    completed_count = 0
    
    def get_current_window_size():
        """Get current window size based on console height."""
        return max(5, console.size.height - 8)
    
    def should_update_display(current_idx):
        """Only update display if visible window, its results, console size, or scroll changed."""
        nonlocal last_visible_range, last_visible_results, last_window_size, last_scroll_offset
        
        current_window = get_current_window_size()
        display_idx = max(0, min(total - 1, current_idx + manual_scroll_offset))
        half_window = current_window // 2
        start = max(0, display_idx - half_window)
        end = min(total, start + current_window)
        start = max(0, end - current_window)
        
        current_range = (start, end)
        current_visible_results = tuple(results[start:end])
        
        # Update if window changed, console resized, scroll changed, or visible results changed
        if (last_visible_range != current_range or 
            last_visible_results != current_visible_results or
            last_window_size != current_window or
            last_scroll_offset != manual_scroll_offset):
            last_visible_range = current_range
            last_visible_results = current_visible_results
            last_window_size = current_window
            last_scroll_offset = manual_scroll_offset
            return True
        return False

    # Simple live display without Progress overhead
    with Live(render_station_table_fast(stations, results, 0, window=get_current_window_size(), manual_scroll_offset=manual_scroll_offset), 
              console=console, refresh_per_second=4, transient=True) as live:
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, total)) as executor:
            future_to_idx = {
                executor.submit(check_station_threadsafe, 
                              (s['url'], args.retries, args.timeout, args.backoff)): i
                for i, s in enumerate(stations)
            }
            
            for future in concurrent.futures.as_completed(future_to_idx):
                if shutdown_event.is_set():
                    console.print("[yellow]Cancelling remaining tasks...[/yellow]")
                    # Cancel remaining futures
                    for f in future_to_idx:
                        if not f.done():
                            f.cancel()
                    break
                    
                idx = future_to_idx[future]
                try:
                    ok = future.result()
                    results[idx] = ok
                    if ok:
                        live_stations.append(stations[idx])
                except Exception:
                    results[idx] = False
                
                completed_count += 1
                
                # Find current focus (first in-progress item, or last completed)
                in_progress = [i for i, r in enumerate(results) if r is None]
                current_idx = in_progress[0] if in_progress else idx
                
                # Only update display if necessary
                if should_update_display(current_idx):
                    live.update(render_station_table_fast(stations, results, current_idx, get_current_window_size(), manual_scroll_offset))
                
                # Always update on completion milestones (every 10% or so)
                elif completed_count % max(1, total // 10) == 0:
                    live.update(render_station_table_fast(stations, results, current_idx, get_current_window_size(), manual_scroll_offset))
        
        # Final update
        if not shutdown_event.is_set():
            live.update(render_station_table_fast(stations, results, len(stations)-1, get_current_window_size(), manual_scroll_offset))

    if shutdown_event.is_set():
        summary = f"[yellow]Interrupted: {len(live_stations)} out of {completed_count}/{len(stations)} checked stations are LIVE.[/yellow]"
    else:
        summary = f"{len(live_stations)} out of {len(stations)} stations are LIVE."
    
    console.print(f"\n{summary}")
    logging.info(summary)

    # Determine output path if not specified
    output_path = args.output
    if not output_path:
        p = Path(args.input)
        output_path = str(p.with_name(p.stem + ".checked" + p.suffix))

    # Select output handler
    if args.format:
        out_ext = "." + args.format.lower()
        handler_cls = FormatHandlerRegistry.get_handler(out_ext)
    else:
        handler_cls = FormatHandlerRegistry.detect_handler(output_path)

    # Save, passing metadata if needed
    handler_cls.save(output_path, live_stations, metadata=input_metadata)
    msg = f"Saved live stations to {output_path} ({handler_cls.extensions()[0][1:]})"
    console.print(msg)
    logging.info(msg)

if __name__ == "__main__":
    main()
