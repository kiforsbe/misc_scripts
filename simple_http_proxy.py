import http.server
import importlib
import select
import socket
import socketserver
import urllib.error
import urllib.request
import urllib.parse
import sys

try:
    cloudscraper = importlib.import_module('cloudscraper')
except ImportError:
    cloudscraper = None


DEFAULT_SCHEME = 'https'
DEFAULT_USER_AGENT = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/135.0.0.0 Safari/537.36'
)
DEFAULT_TIMEOUT = 30
TUNNEL_TIMEOUT = 60
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


class SimpleProxyHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def _read_request_body(self):
        content_length = self.headers.get('Content-Length')
        if not content_length:
            return None
        return self.rfile.read(int(content_length))

    def _relay_http_request(self):
        remote_url = resolve_remote_url(self.path, self.headers)
        if remote_url == '':
            self.send_response(204)
            self.end_headers()
            return True

        if remote_url is None:
            self.send_error(400, "Invalid remote URL")
            return True

        try:
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
