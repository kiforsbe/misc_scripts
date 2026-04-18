import base64
import http.server
import importlib
import select
import socket
import socketserver
import threading
import time
import urllib.error
import urllib.request
import urllib.parse
import sys

try:
    cloudscraper = importlib.import_module('cloudscraper')
except ImportError:
    cloudscraper = None

try:
    playwright_sync_api = importlib.import_module('playwright.sync_api')
except ImportError:
    playwright_sync_api = None


DEFAULT_SCHEME = 'https'
DEFAULT_USER_AGENT = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/135.0.0.0 Safari/537.36'
)
DEFAULT_TIMEOUT = 30
TUNNEL_TIMEOUT = 60
BROWSER_FETCH_WAIT = 1.0
BROWSER_FETCH_CHANNELS = ('msedge', 'chrome')
FORWARDED_REQUEST_HEADERS = {
    'Accept',
    'Accept-Language',
    'Cache-Control',
    'Cookie',
    'If-Modified-Since',
    'If-None-Match',
    'Pragma',
}
HOP_BY_HOP_HEADERS = {
    'connection',
    'keep-alive',
    'proxy-authenticate',
    'proxy-authorization',
    'proxy-connection',
    'te',
    'trailers',
    'transfer-encoding',
    'upgrade',
}
BROWSER_FORWARDABLE_HEADERS = {
    'Accept',
    'Accept-Language',
    'Cache-Control',
    'Pragma',
}


SCRAPER = (
    cloudscraper.create_scraper(
        browser={
            'browser': 'chrome',
            'platform': 'windows',
            'mobile': False,
        }
    )
    if cloudscraper is not None
    else None
)


def normalize_remote_url(raw_path):
    parsed_request = urllib.parse.urlsplit(raw_path)
    request_path = urllib.parse.unquote(parsed_request.path)

    if request_path in {'', '/'}:
        return None

    if request_path == '/favicon.ico':
        return ''

    remote_url = request_path.lstrip('/')
    if parsed_request.query:
        remote_url = f'{remote_url}?{parsed_request.query}'

    if remote_url.startswith(('http://', 'https://')):
        return remote_url

    if '://' in remote_url:
        return None

    parsed_remote = urllib.parse.urlsplit(f'{DEFAULT_SCHEME}://{remote_url}')
    if not parsed_remote.netloc:
        return None

    return urllib.parse.urlunsplit(parsed_remote)


def resolve_remote_url(path, headers):
    if path.startswith(('http://', 'https://')):
        return path

    if path.startswith(('/http://', '/https://')):
        return normalize_remote_url(path)

    if path == '/favicon.ico':
        return ''

    host = headers.get('Host')
    if host:
        request_target = path if path.startswith('/') else f'/{path}'
        return f'http://{host}{request_target}'

    return normalize_remote_url(path)


def build_upstream_headers(client_headers, remote_url):
    parsed_remote = urllib.parse.urlsplit(remote_url)
    headers = {
        'User-Agent': client_headers.get('User-Agent', DEFAULT_USER_AGENT),
        'Accept': client_headers.get('Accept', '*/*'),
        'Accept-Language': client_headers.get('Accept-Language', 'en-US,en;q=0.9'),
        'Referer': client_headers.get(
            'Referer',
            urllib.parse.urlunsplit((parsed_remote.scheme, parsed_remote.netloc, '/', '', '')),
        ),
    }

    for header_name, header_value in client_headers.items():
        if header_name.lower() in HOP_BY_HOP_HEADERS:
            continue
        if header_name.lower() == 'host':
            continue
        if header_name in headers:
            continue
        headers[header_name] = header_value

    headers['Host'] = parsed_remote.netloc

    return headers


def send_response_headers(handler, headers):
    for key, value in headers:
        if key.lower() != 'transfer-encoding':
            handler.send_header(key, value)


def stream_chunks(write_chunk, read_chunk):
    while True:
        chunk = read_chunk(8192)
        if not chunk:
            break
        write_chunk(chunk)


class BrowserFetchSession:
    def __init__(self):
        self._lock = threading.Lock()
        self._playwright = None
        self._browser = None
        self._context = None
        self._launch_label = None

    @property
    def available(self):
        return playwright_sync_api is not None

    def _ensure_context(self):
        module = playwright_sync_api
        if module is None:
            raise RuntimeError('playwright is not installed')

        if self._context is not None:
            return self._context

        sync_playwright = module.sync_playwright
        error_messages = []
        self._playwright = sync_playwright().start()

        for channel in BROWSER_FETCH_CHANNELS:
            try:
                self._browser = self._playwright.chromium.launch(channel=channel, headless=True)
                self._launch_label = channel
                break
            except Exception as exc:
                error_messages.append(f'{channel}: {exc}')

        if self._browser is None:
            try:
                self._browser = self._playwright.chromium.launch(headless=True)
                self._launch_label = 'chromium'
            except Exception as exc:
                error_messages.append(f'chromium: {exc}')
                self._playwright.stop()
                self._playwright = None
                raise RuntimeError('; '.join(error_messages)) from exc

        self._context = self._browser.new_context(user_agent=DEFAULT_USER_AGENT)
        self._context.set_default_timeout(DEFAULT_TIMEOUT * 1000)
        return self._context

    def _fetch_via_page(self, page, remote_url, client_headers, method):
        forward_headers = {
            key: value
            for key, value in client_headers.items()
            if key in BROWSER_FORWARDABLE_HEADERS
        }
        page.set_extra_http_headers(forward_headers)
        page.goto(remote_url, wait_until='domcontentloaded', timeout=DEFAULT_TIMEOUT * 1000)

        deadline = time.monotonic() + DEFAULT_TIMEOUT
        last_result = None
        while time.monotonic() < deadline:
            if method == 'HEAD':
                last_result = page.evaluate(
                    """async (requestInfo) => {
                        const response = await fetch(requestInfo.url, {
                            method: 'HEAD',
                            credentials: 'include',
                            redirect: 'follow',
                            headers: requestInfo.headers,
                        });
                        return {
                            status: response.status,
                            statusText: response.statusText,
                            headers: Array.from(response.headers.entries()),
                            bodyBase64: '',
                        };
                    }""",
                    {'url': remote_url, 'headers': forward_headers},
                )
            else:
                last_result = page.evaluate(
                    """async (requestInfo) => {
                        const response = await fetch(requestInfo.url, {
                            method: 'GET',
                            credentials: 'include',
                            redirect: 'follow',
                            headers: requestInfo.headers,
                        });
                        const arrayBuffer = await response.arrayBuffer();
                        const bytes = new Uint8Array(arrayBuffer);
                        let binary = '';
                        const chunkSize = 0x8000;
                        for (let index = 0; index < bytes.length; index += chunkSize) {
                            binary += String.fromCharCode(...bytes.subarray(index, index + chunkSize));
                        }
                        return {
                            status: response.status,
                            statusText: response.statusText,
                            headers: Array.from(response.headers.entries()),
                            bodyBase64: btoa(binary),
                        };
                    }""",
                    {'url': remote_url, 'headers': forward_headers},
                )

            body_text = base64.b64decode(last_result['bodyBase64']).decode('utf-8', errors='ignore') if last_result['bodyBase64'] else ''
            if not self._looks_like_challenge(last_result['status'], body_text):
                return last_result
            page.wait_for_timeout(BROWSER_FETCH_WAIT * 1000)

        return last_result

    @staticmethod
    def _looks_like_challenge(status_code, body_text):
        lowered_body = body_text.lower()
        return status_code == 403 and (
            'just a moment' in lowered_body
            or 'cf-mitigated' in lowered_body
            or '/cdn-cgi/challenge-platform/' in lowered_body
            or 'cloudflare' in lowered_body
        )

    def fetch(self, remote_url, client_headers, method):
        with self._lock:
            context = self._ensure_context()
            page = context.new_page()
            try:
                result = self._fetch_via_page(page, remote_url, client_headers, method)
            finally:
                page.close()

        if result is None:
            raise RuntimeError('browser fetch did not return a response')

        body = base64.b64decode(result['bodyBase64']) if result['bodyBase64'] else b''
        return result['status'], result['headers'], body


BROWSER_FETCHER = BrowserFetchSession()


class SimpleProxyHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def _read_request_body(self):
        content_length = self.headers.get('Content-Length')
        if not content_length:
            return None
        return self.rfile.read(int(content_length))

    def _relay_http_request(self):
        remote_url = resolve_remote_url(self.path, self.headers)
        is_reverse_proxy_path = self.path.startswith(('/http://', '/https://'))
        if remote_url == '':
            self.send_response(204)
            self.end_headers()
            return True

        if remote_url is None:
            self.send_error(400, "Invalid remote URL")
            return True

        try:
            if is_reverse_proxy_path and BROWSER_FETCHER.available and self.command in {'GET', 'HEAD'}:
                status_code, response_headers, response_body = BROWSER_FETCHER.fetch(
                    remote_url,
                    self.headers,
                    self.command,
                )
                self.send_response(status_code)
                send_response_headers(self, response_headers)
                self.end_headers()
                if self.command != 'HEAD' and response_body:
                    self.wfile.write(response_body)
                return True

            upstream_headers = build_upstream_headers(self.headers, remote_url)
            request_body = self._read_request_body()
            scraper = SCRAPER

            if scraper is not None and self.command in {'GET', 'HEAD'} and self.path.startswith('/'):
                resp = scraper.request(
                    self.command,
                    remote_url,
                    headers=upstream_headers,
                    data=request_body,
                    stream=True,
                    timeout=DEFAULT_TIMEOUT,
                    allow_redirects=False,
                )
                try:
                    self.send_response(resp.status_code)
                    send_response_headers(self, resp.headers.items())
                    self.end_headers()
                    if self.command != 'HEAD':
                        for chunk in resp.iter_content(chunk_size=8192):
                            if chunk:
                                self.wfile.write(chunk)
                finally:
                    resp.close()
                return True

            request = urllib.request.Request(
                remote_url,
                data=request_body,
                headers=upstream_headers,
                method=self.command,
            )
            with urllib.request.urlopen(request, timeout=DEFAULT_TIMEOUT) as resp:
                self.send_response(resp.status)
                send_response_headers(self, resp.getheaders())
                self.end_headers()
                if self.command != 'HEAD':
                    stream_chunks(self.wfile.write, resp.read)
        except urllib.error.HTTPError as e:
            self.send_response(e.code)
            send_response_headers(self, e.headers.items())
            self.end_headers()
            if self.command != 'HEAD':
                stream_chunks(self.wfile.write, e.read)
        except Exception as e:
            self.send_error(502, f"Proxy error: {e}")
        return True

    def _tunnel_data(self, upstream_socket):
        sockets = [self.connection, upstream_socket]
        while True:
            readable, _, exceptional = select.select(sockets, [], sockets, TUNNEL_TIMEOUT)
            if exceptional or not readable:
                break

            for ready_socket in readable:
                other_socket = upstream_socket if ready_socket is self.connection else self.connection
                chunk = ready_socket.recv(8192)
                if not chunk:
                    return
                other_socket.sendall(chunk)

    def do_CONNECT(self):
        host, separator, port_text = self.path.partition(':')
        if not separator:
            self.send_error(400, 'CONNECT target must be host:port')
            return

        try:
            port = int(port_text)
        except ValueError:
            self.send_error(400, 'Invalid CONNECT port')
            return

        try:
            with socket.create_connection((host, port), timeout=DEFAULT_TIMEOUT) as upstream_socket:
                upstream_socket.settimeout(TUNNEL_TIMEOUT)
                self.send_response(200, 'Connection Established')
                self.end_headers()
                self.connection.settimeout(TUNNEL_TIMEOUT)
                self._tunnel_data(upstream_socket)
        except Exception as e:
            self.send_error(502, f'CONNECT error: {e}')

    def do_GET(self):
        self._relay_http_request()

    def do_HEAD(self):
        self._relay_http_request()

    def do_POST(self):
        self._relay_http_request()

    def do_PUT(self):
        self._relay_http_request()

    def do_DELETE(self):
        self._relay_http_request()

    def do_OPTIONS(self):
        self._relay_http_request()

    def do_PATCH(self):
        self._relay_http_request()

    def log_message(self, format, *args):
        # Print to stderr for logging
        sys.stderr.write("%s - - [%s] %s\n" %
                         (self.client_address[0],
                          self.log_date_time_string(),
                          format%args))

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description=(
            'Simple forward HTTP proxy with HTTPS CONNECT tunneling. '
            'Legacy reverse-proxy paths like /https://example.com/ also still work.'
        )
    )
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind (default: 0.0.0.0)')
    parser.add_argument('--port', type=int, default=8080, help='Port to listen on (default: 8080)')
    args = parser.parse_args()

    with socketserver.ThreadingTCPServer((args.host, args.port), SimpleProxyHandler) as httpd:
        print(f"Serving HTTP proxy on {args.host}:{args.port}")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nShutting down proxy.")
