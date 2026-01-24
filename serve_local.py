"""Serve a file or folder as a simple local webhost.

Usage:
  - Drag-and-drop a file or folder onto this script in Windows Explorer, or
  - Run: python serve_local.py "C:\path\to\file_or_folder" [-p PORT]

If a file is provided it will be used as the index page (root). If a folder
is provided and it contains an `index.html`, that will be used. If no
`index.html` is present, a directory listing with extension/size/mtime will
be shown. The served paths are sandboxed to the provided root.
"""
from __future__ import annotations

import argparse
import html
import io
import logging
import os
import socket
import sys
import time
import urllib.parse
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler
import mimetypes
import socketserver
import shutil
import threading


def get_local_ip() -> str:
    """Return a likely LAN IP address for the current machine."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"


# Simple global registry for Server-Sent-Events (SSE) watchers
SSE_WATCHERS: list[SimpleHTTPRequestHandler] = []
SSE_WATCHERS_LOCK = threading.Lock()
SSE_WATCHER_STOP = threading.Event()


def _notify_watchers(message: bytes) -> None:
    logger = logging.getLogger('serve_local')
    to_remove = []
    # Informative log for operators when live-reload is emitted
    try:
        with SSE_WATCHERS_LOCK:
            count = len(SSE_WATCHERS)
    except Exception:
        count = 0
    if count:
        logger.info('Live-reload: broadcasting message to %d client(s)', count)
    with SSE_WATCHERS_LOCK:
        for h in list(SSE_WATCHERS):
            try:
                h.wfile.write(message)
                h.wfile.flush()
            except Exception:
                # mark for removal; client likely disconnected
                to_remove.append(h)
        for h in to_remove:
            try:
                SSE_WATCHERS.remove(h)
            except ValueError:
                pass
            try:
                h._watcher_stopped = True
            except Exception:
                pass


def _watch_file_for_changes(path: str) -> None:
    logger = logging.getLogger('serve_local')
    try:
        last = os.path.getmtime(path)
    except Exception:
        last = None
    while not SSE_WATCHER_STOP.is_set():
        try:
            if not os.path.exists(path):
                # file may be temporarily absent; wait
                SSE_WATCHER_STOP.wait(1)
                continue
            mtime = os.path.getmtime(path)
            if last is None:
                last = mtime
            elif mtime != last:
                last = mtime
                logger.info('Detected change in watched index %s; notifying clients', path)
                _notify_watchers(b"data: reload\n\n")
        except Exception:
            logger.exception('Error watching file %s', path)
        SSE_WATCHER_STOP.wait(1)


class CustomHandler(SimpleHTTPRequestHandler):
    """HTTP handler that can force an index file and render a richer listing.

    This uses the `directory` passed to the handler to sandbox served paths.
    """

    def __init__(self, *args, directory: str | None = None, index_file: str | None = None, **kwargs):
        self._forced_index = index_file
        # live reload enabled flag (injected via handler_factory)
        self._live = kwargs.pop('live', False) if kwargs is not None else False
        super().__init__(*args, directory=directory, **kwargs)

    def do_GET(self):
        # Provide a small proxy endpoint for client pages served over HTTP to
        # request local files (which would otherwise be requested via the
        # file:/// scheme by the browser and not sent to this server).
        # Example: /file-proxy?target=file:///C:/Users/This%20User/…
        parsed = urllib.parse.urlsplit(self.path)
        if parsed.path == '/file-proxy':
            logger = logging.getLogger('serve_local')
            qs = urllib.parse.parse_qs(parsed.query)
            target = qs.get('target', [''])[0]
            if not target:
                self.send_error(HTTPStatus.BAD_REQUEST, 'missing target')
                return

            # Normalize file: URI to filesystem path
            candidate = target
            for prefix in ('file:///', 'file://', 'file:/', 'file:'):
                if candidate.startswith(prefix):
                    candidate = candidate[len(prefix):]
                    break

            # On Windows a leading /C:/ may be present; remove leading slash
            if os.name == 'nt' and candidate.startswith('/') and len(candidate) > 2 and candidate[1].isalpha() and candidate[2] == ':':
                candidate = candidate.lstrip('/')

            abs_path = os.path.abspath(candidate)
            home_dir = os.path.abspath(os.path.expanduser('~'))
            webroot = os.path.abspath(self.directory) if self.directory else os.path.abspath(os.getcwd())

            # Allow if file is under webroot or under user's home directory (Windows)
            allowed = False
            try:
                if os.path.commonpath([abs_path, webroot]) == webroot:
                    allowed = True
                elif os.name == 'nt' and os.path.commonpath([abs_path, home_dir]) == home_dir:
                    allowed = True
            except Exception:
                allowed = False

            if not allowed:
                logger.warning('Denied file-proxy request outside allowed roots: %s', abs_path)
                self.send_error(HTTPStatus.FORBIDDEN, 'not allowed')
                return

            # Log sandbox escape if serving from home_dir
            if os.name == 'nt' and os.path.commonpath([abs_path, home_dir]) == home_dir and os.path.commonpath([abs_path, webroot]) != webroot:
                logger.debug('Approved sandbox escape: %s', abs_path)

            if os.path.isdir(abs_path):
                self.send_error(HTTPStatus.NOT_FOUND, 'not a file')
                return

            try:
                ctype = mimetypes.guess_type(abs_path)[0] or 'application/octet-stream'
                fs = os.path.getsize(abs_path)
                self.send_response(HTTPStatus.OK)
                self.send_header('Content-type', ctype)
                self.send_header('Content-Length', str(fs))
                self.end_headers()
                try:
                    with open(abs_path, 'rb') as f:
                        shutil.copyfileobj(f, self.wfile)
                    return
                except OSError as e:
                    # Client likely disconnected while we were streaming.
                    logger.debug('Client disconnected while streaming %s: %s', abs_path, e)
                    return
                except Exception:
                    logger.exception('Error streaming file-proxy target %s', abs_path)
                    try:
                        self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR, 'error reading file')
                    except Exception:
                        logger.debug('Could not send error response; client may have disconnected')
                    return
            except Exception:
                logger.exception('Error preparing file-proxy response for %s', abs_path)
                try:
                    self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR, 'error preparing response')
                except Exception:
                    logger.debug('Could not send error response; client may have disconnected')
                return
        # Server-Sent Events endpoint for live reload notifications (only when enabled)
        if parsed.path == '/__watch':
            if not getattr(self, '_live', False):
                self.send_error(HTTPStatus.NOT_FOUND, 'not found')
                return
            logger = logging.getLogger('serve_local')
            # Send SSE headers and register this handler as a watcher
            self.send_response(HTTPStatus.OK)
            self.send_header('Content-Type', 'text/event-stream')
            self.send_header('Cache-Control', 'no-cache')
            self.send_header('Connection', 'keep-alive')
            self.end_headers()
                # Register
            try:
                with SSE_WATCHERS_LOCK:
                    SSE_WATCHERS.append(self)
                logger.info('Live-reload: client connected from %s', self.client_address[0])
                # initial comment to establish connection
                try:
                    self.wfile.write(b": connected\n\n")
                    self.wfile.flush()
                except Exception:
                    pass
                # Block here until removed by notifier or server shutdown
                while not getattr(self, '_watcher_stopped', False) and not SSE_WATCHER_STOP.is_set():
                    try:
                        # Sleep briefly; notifier will write events into wfile
                        time.sleep(0.5)
                    except Exception:
                        break
            finally:
                with SSE_WATCHERS_LOCK:
                    if self in SSE_WATCHERS:
                        try:
                            SSE_WATCHERS.remove(self)
                        except ValueError:
                            pass
                logger.info('Live-reload: client disconnected from %s', self.client_address[0])
            return

        # Force a single file to act as index for '/'
        # Treat requests where the URL path is '/' (including queries like '/?...')
        # as requests for the index when a forced index is configured.
        try:
            parsed = urllib.parse.urlsplit(self.path)
            request_path = parsed.path or "/"
        except Exception:
            request_path = self.path

        if request_path in ("/", "/index.html") and self._forced_index:
            # Rewrite path to the basename of forced index so SimpleHTTPRequestHandler
            # serves the file from the directory sandbox. Keep the original request
            # in logs if needed.
            base = os.path.basename(self._forced_index)
            self._original_request_path = self.path
            self.path = "/" + urllib.parse.quote(base)

        return super().do_GET()

    def send_response(self, code: int, message: str | None = None) -> None:
        # Log responses at DEBUG level before sending
        logging.getLogger("serve_local").debug("Responding to %s %s with code %s", getattr(self, 'command', '?'), getattr(self, 'path', '?'), code)
        return super().send_response(code, message)

    def log_request(self, code='-', size='-'):
        # Log every request and its response details at DEBUG level
        logger = logging.getLogger("serve_local")
        try:
            cmd = getattr(self, 'command', '-')
            path = getattr(self, 'path', '-')
            ver = getattr(self, 'request_version', '-')
            client = self.client_address[0]
        except Exception:
            cmd = path = ver = client = '-'
        logger.debug("Request from %s: %s %s %s -> response=%s size=%s", client, cmd, path, ver, code, size)

    def log_message(self, format: str, *args) -> None:
        # Route base handler logs through the logging module
        logging.getLogger("serve_local").info("%s - - %s", self.client_address[0], format % args)

    def send_head(self):
        """Serve a file, injecting a small live-reload client when serving the forced index."""
        # Defer to base behavior for directories and non-file responses
        path = self.translate_path(self.path)
        if os.path.isdir(path):
            return super().send_head()

        ctype = self.guess_type(path)
        try:
            with open(path, 'rb') as f:
                content = f.read()
        except OSError:
            self.send_error(HTTPStatus.NOT_FOUND, "File not found")
            return None

        # If this request is for the forced index file and live reload is enabled, inject SSE client script
        try:
            forced = self._forced_index
        except Exception:
            forced = None

        inject_script = b''
        if forced and getattr(self, '_live', False):
            try:
                forced_basename = os.path.basename(forced)
                requested = os.path.abspath(path)
                forced_path = os.path.abspath(os.path.join(self.directory or os.getcwd(), forced_basename))
                if requested == forced_path:
                    inject_script = b"\n<script>/* live-reload */(function(){try{if(typeof EventSource!=='undefined'){var s=new EventSource('/__watch');s.addEventListener('message',function(e){if(e.data&&e.data.trim()==='reload'){location.reload(true);}});s.addEventListener('error',function(){});} }catch(e){} })();</script>\n"
            except Exception:
                pass

        if inject_script:
            try:
                logging.getLogger('serve_local').info('Injecting live-reload script into served index %s', forced_basename)
            except Exception:
                pass
            body = content + inject_script
            self.send_response(HTTPStatus.OK)
            self.send_header('Content-type', ctype)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            return io.BytesIO(body)

        # No injection; send file as-is
        self.send_response(HTTPStatus.OK)
        self.send_header('Content-type', ctype)
        self.send_header('Content-Length', str(len(content)))
        self.end_headers()
        return io.BytesIO(content)

    def translate_path(self, path: str) -> str:
        """Translate a /-separated PATH to the local filename.
        Enforce that the resolved filesystem path is inside the configured
        web root directory. Requests for file:/// URIs are not sent to the
        server and are handled via the `/file-proxy` endpoint instead.
        """
        logger = logging.getLogger("serve_local")
        # Unquote URL and let base implementation produce a filesystem path
        upath = urllib.parse.unquote(path)
        # Default behavior: translate relative to self.directory
        localpath = super().translate_path(path)
        try:
            # Ensure the resolved path is inside the configured web root directory
            webroot = os.path.abspath(self.directory) if self.directory else os.path.abspath(os.getcwd())
            target = os.path.abspath(localpath)
            if os.path.commonpath([target, webroot]) != webroot:
                logger.warning("Blocked path outside web root: requested=%s resolved=%s webroot=%s", path, target, webroot)
                return os.path.join(webroot, ".forbidden")
        except Exception:
            logger.exception("Error enforcing web root sandbox for path %s", path)
        return localpath

    def list_directory(self, path):
        try:
            names = os.listdir(path)
        except OSError:
            self.send_error(HTTPStatus.NOT_FOUND, "No permission to list directory")
            return None
        names.sort(key=lambda a: a.lower())
        r = []
        displaypath = html.escape(urllib.parse.unquote(self.path))
        r.append(f"<html><head><meta charset=\"utf-8\"><title>Directory listing for {displaypath}</title></head>")
        r.append(f"<body><h2>Directory listing for {displaypath}</h2>")
        r.append("<table border=1 cellpadding=6><tr><th>Name</th><th>Ext</th><th>Size</th><th>Modified</th></tr>")
        for name in names:
            fullname = os.path.join(path, name)
            display_name = name + ("/" if os.path.isdir(fullname) else "")
            href = urllib.parse.quote(name)
            ext = os.path.splitext(name)[1].lower() or ""
            if os.path.isdir(fullname):
                size = "-"
                mtime = "-"
            else:
                try:
                    size = f"{os.path.getsize(fullname):,}"
                except OSError:
                    size = "?"
                try:
                    mtime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(os.path.getmtime(fullname)))
                except OSError:
                    mtime = "?"
            r.append(f"<tr><td><a href=\"{href}\">{html.escape(display_name)}</a></td><td>{html.escape(ext)}</td><td align=\"right\">{size}</td><td>{html.escape(mtime)}</td></tr>")
        r.append("</table></body></html>")
        encoded = "\n".join(r).encode("utf-8", "surrogateescape")
        f = io.BytesIO()
        f.write(encoded)
        f.seek(0)
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        return f


def choose_path_interactive() -> str | None:
    """Open a GUI dialog on Windows if available to let the user pick a file/folder."""
    try:
        import tkinter as tk
        from tkinter import filedialog

        root = tk.Tk()
        root.withdraw()
        # Ask for either file or folder: try file first, if cancelled ask folder
        p = filedialog.askopenfilename(title="Select file to serve (Cancel to choose folder)")
        if p:
            return p
        p = filedialog.askdirectory(title="Select folder to serve")
        if p:
            return p
    except Exception:
        return None
    return None


def main(argv=None):
    argv = argv or sys.argv[1:]
    parser = argparse.ArgumentParser(description="Serve a file or folder on the local network")
    parser.add_argument("path", nargs="?", help="File or folder to serve")
    parser.add_argument("-p", "--port", type=int, default=8000, help="Port to listen on (default 8000)")
    parser.add_argument("-l", "--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help="Logging level (default INFO)")
    parser.add_argument("--live", action="store_true", help="Enable live-reload (SSE) for forced index file")
    parser.add_argument("--qrcode", action="store_true", help=("Print a camera-scannable QR for the server URL in the terminal. Requires qrcode-terminal package."))
    args = parser.parse_args(argv)

    # Configure logging early
    numeric_level = getattr(logging, args.log_level.upper(), logging.INFO)
    logging.basicConfig(level=numeric_level, format="%(asctime)s %(levelname)s: %(message)s")
    logger = logging.getLogger("serve_local")

    path = args.path
    if not path:
        picked = choose_path_interactive()
        if not picked:
            print("No path provided. Usage: python serve_local.py <file_or_folder>")
            return 2
        path = picked

    path = os.path.abspath(path)
    if not os.path.exists(path):
        logger.error("Path does not exist: %s", path)
        return 2
    is_windows = os.name == "nt"
    home_dir = os.path.expanduser("~")

    if os.path.isfile(path):
        root_dir = os.path.dirname(path)
        forced_index = path
        logger.info("Serving file %s as index from root %s", path, root_dir)
        # If on Windows and the file is under the user's home, do NOT expand the web root.
        # Instead, allow serving of explicit `file:///` URIs that point into the user's
        # home directory; such escapes will be logged when they occur.
        try:
            if is_windows and os.path.commonpath([os.path.abspath(path), home_dir]) == os.path.abspath(home_dir):
                logger.info("Provided file is inside user's home directory; /file-proxy?target= requests under '%s' will be allowed and logged (no web-root expansion).", home_dir)
        except Exception:
            pass
    else:
        root_dir = path
        # If folder has index.html, let default handler serve it. No forced index needed.
        index_path = os.path.join(root_dir, "index.html")
        forced_index = None
        if os.path.exists(index_path) and os.path.isfile(index_path):
            logger.info("Found index.html in %s; it will be used as the default page.", root_dir)
        else:
            logger.info("No index.html in %s; a directory listing will be shown.", root_dir)
        # If on Windows and the folder is under the user's home, do NOT expand web root.
        # We will allow explicit `file:///` requests into the user's home and log them.
        try:
            if is_windows and os.path.commonpath([os.path.abspath(root_dir), home_dir]) == os.path.abspath(home_dir):
                logger.info("Provided folder is inside user's home directory; file:/// requests under '%s' will be allowed and logged (no web-root expansion).", home_dir)
        except Exception:
            pass

    port = args.port
    host_ip = get_local_ip()

    class ThreadedTCPServer(socketserver.ThreadingTCPServer):
        allow_reuse_address = True
        # Make per-request handler threads daemon so they don't block process exit
        daemon_threads = True
        def handle_error(self, request, client_address):
            # Suppress noisy tracebacks for common client-abort errors during shutdown
            import sys
            logger = logging.getLogger('serve_local')
            exc_type, exc_value, exc_tb = sys.exc_info()
            if isinstance(exc_value, (ConnectionAbortedError, BrokenPipeError, ConnectionResetError)):
                logger.debug('Ignored client disconnect from %s: %s', client_address, exc_value)
            else:
                logger.exception('Unhandled exception processing request from %s', client_address)

    handler_factory = lambda *hargs, **hkwargs: CustomHandler(*hargs, directory=root_dir, index_file=forced_index, live=args.live, **hkwargs)
    # If a forced index file was provided, start a background watcher to notify SSE clients
    if args.live and forced_index and os.path.isfile(forced_index):
        watcher_thread = threading.Thread(target=_watch_file_for_changes, args=(forced_index,), daemon=True)
        watcher_thread.start()

    with ThreadedTCPServer(("", port), handler_factory) as httpd:
        sa = httpd.socket.getsockname()
        url = f"http://{host_ip}:{sa[1]}/"
        logger.info("Serving HTTP on 0.0.0.0 port %s (%s)", sa[1], url)
        # Optionally generate a QR code for the served URL (terminal-only)
        if args.qrcode:
            try:
                import qrcode_terminal
                qrcode_terminal.draw(url)
            except ImportError:
                logger.warning("'qrcode-terminal' not installed; run 'pip install qrcode-terminal' to print QR in terminal")

        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            logger.info("Shutting down server")
            # First signal watcher thread and notify any connected SSE clients
            SSE_WATCHER_STOP.set()
            try:
                _notify_watchers(b"data: shutdown\n\n")
            except Exception:
                pass
            # Then shutdown the HTTP server (will stop serve_forever)
            try:
                httpd.shutdown()
            except Exception:
                logger.debug('Error during httpd.shutdown()', exc_info=True)
        finally:
            # Ensure watcher thread is signaled and clients are notified on exit
            SSE_WATCHER_STOP.set()
            try:
                _notify_watchers(b"data: shutdown\n\n")
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
