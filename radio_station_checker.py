import argparse
import re
import requests
import time
from pathlib import Path
from typing import List, Dict, Type, Optional
import logging
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, ProgressColumn
from rich.console import Console
import concurrent.futures
from rich.live import Live
from rich.table import Table

class StatusColumn(ProgressColumn):
    def render(self, task):
        # Show OK/NOK in green/red at 100%, else show percent in blue
        if task.finished:
            if "[green]" in task.fields["station_status"]:
                return "[green]{:>4}[/green]".format("OK")
            elif "[red]" in task.fields["station_status"]:
                return "[red]{:>4}[/red]".format("NOK")
        return f"[blue]{int(task.percentage):>3d}%[/blue]"

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

def check_station(url, retries=2, timeout=3, backoff_factor=1.2, progress=None, task_id=None):
    import requests
    import socket
    current_timeout = timeout
    for attempt in range(retries + 1):
        if progress and task_id is not None:
            progress.update(task_id, advance=1)
        try:
            resp = requests.get(url, stream=True, timeout=current_timeout, headers={"Icy-MetaData": "1"})
            if resp.status_code == 200:
                if progress and task_id is not None:
                    progress.update(
                        task_id,
                        completed=retries+1,
                        station_status=f"[green]●[/green]",
                        bar_style="green"
                    )
                return True
        except requests.exceptions.RequestException as e:
            logging.debug(f"  [retry {attempt+1}] Error: {e}")
            if progress and task_id is not None:
                progress.update(
                    task_id,
                    completed=retries+1,
                    station_status=f"[red]●[/red]",
                    bar_style="red"
                )
            return False
        except socket.gaierror as e:
            logging.debug(f"  [retry {attempt+1}] Name resolution error: {e}")
            if progress and task_id is not None:
                progress.update(
                    task_id,
                    completed=retries+1,
                    station_status=f"[red]●[/red]",
                    bar_style="red"
                )
            return False
        time.sleep(current_timeout)
        current_timeout *= backoff_factor
    if progress and task_id is not None:
        progress.update(
            task_id,
            completed=retries+1,
            station_status=f"[red]●[/red]",
            bar_style="red"
        )
    return False

def check_station_threadsafe(args):
    # Helper for thread pool: (url, retries, timeout, backoff, progress, task_id)
    url, retries, timeout, backoff, progress, task_id = args
    return check_station(
        url,
        retries=retries,
        timeout=timeout,
        backoff_factor=backoff,
        progress=progress,
        task_id=task_id
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

def render_station_table(stations, station_tasks, progress, current_idx, window=None):
    """Render a table of stations, showing only a window around the current station."""
    total = len(stations)
    if window is None:
        from rich.console import Console
        console = Console()
        window = max(5, console.size.height - 8)
    half_window = window // 2
    start = max(0, current_idx - half_window)
    end = min(total, start + window)
    start = max(0, end - window)  # ensure window size if near end

    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Status", width=6)
    table.add_column("Station", overflow="fold")
    table.add_column("Progress", width=16)
    table.add_column("Result", width=6, justify="right")

    for i in range(start, end):
        s = stations[i]
        task_id = station_tasks[i]
        task = progress.tasks[task_id]
        status = task.fields["station_status"]
        name = task.fields["station_name"]
        percent = f"{int(task.percentage):>3d}%" if not task.finished else (
            "[green]OK[/green]" if "[green]" in status else "[red]NOK[/red]"
        )
        bar = BarColumn(
            bar_width=12,
            complete_style=task.fields.get("bar_style", "blue"),
            finished_style="green" if "[green]" in status else "red" if "[red]" in status else "blue",
            style="grey62",
        ).render(task)
        table.add_row(status, name, bar, percent)
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
    with Progress(
        TextColumn("{task.fields[station_status]} {task.fields[station_name]}", justify="left"),
        BarColumn(
            bar_width=30,
            complete_style="blue",
            finished_style="green",
            pulse_style="blue",
            style="grey62",
        ),
        StatusColumn(),
        "•",
        TimeElapsedColumn(),
        console=console,
        transient=True,  # Use transient so Live can control the screen
        expand=True,
    ) as progress:
        station_tasks = []
        for i, s in enumerate(stations):
            station_name = f"[{i+1}/{total}] {s['url']}"
            task_id = progress.add_task(
                "",
                total=args.retries + 1,
                station_name=station_name,
                station_status="[grey62]●[/grey62]",
                bar_style="blue"
            )
            station_tasks.append(task_id)

        # Prepare arguments for thread pool
        thread_args = [
            (s['url'], args.retries, args.timeout, args.backoff, progress, station_tasks[i])
            for i, s in enumerate(stations)
        ]

        # Use Live to control the visible window of stations
        with Live(render_station_table(stations, station_tasks, progress, 0, window=None), console=console, refresh_per_second=10, transient=True) as live:
            results: List[Optional[bool]] = [None] * len(stations)
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(stations))) as executor:
                future_to_idx = {
                    executor.submit(check_station_threadsafe, thread_args[i]): i
                    for i in range(len(stations))
                }
                completed = set()
                for future in concurrent.futures.as_completed(future_to_idx):
                    idx = future_to_idx[future]
                    try:
                        ok = future.result()
                        results[idx] = ok
                        if ok:
                            progress.update(station_tasks[idx], bar_style="green", finished_style="green", complete_style="green", station_status="[green]●[/green]")
                        else:
                            progress.update(station_tasks[idx], bar_style="red", finished_style="red", complete_style="red", station_status="[red]●[/red]")
                        if ok:
                            live_stations.append(stations[idx])
                    except Exception as e:
                        progress.update(station_tasks[idx], bar_style="red", finished_style="red", complete_style="red", station_status="[red]●[/red]")
                        results[idx] = False
                    completed.add(idx)
                    # Only update the screen if the current station is within the visible window
                    in_progress = [i for i, t in enumerate(progress.tasks) if not t.finished]
                    current_idx = in_progress[0] if in_progress else idx
                    # Only update if current_idx is within the visible window
                    window = console.size.height - 8
                    half_window = window // 2
                    start = max(0, current_idx - half_window)
                    end = min(len(stations), start + window)
                    if start <= idx < end:
                        live.update(render_station_table(stations, station_tasks, progress, current_idx, window=window))
            # Final update to show all finished (just the last window)
            live.update(render_station_table(stations, station_tasks, progress, len(stations)-1, window=console.size.height - 8))

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
