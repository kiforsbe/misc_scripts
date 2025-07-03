import http.server
import socketserver
import urllib.request
import urllib.parse
import sys

class SimpleProxyHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        # Remove leading slash and unquote the URL
        remote_url = urllib.parse.unquote(self.path.lstrip('/'))
        if not (remote_url.startswith('http://') or remote_url.startswith('https://')):
            self.send_error(400, "Invalid remote URL")
            return
        try:
            with urllib.request.urlopen(remote_url) as resp:
                self.send_response(resp.status)
                # Copy headers
                for key, value in resp.getheaders():
                    # Avoid sending Transfer-Encoding: chunked to client
                    if key.lower() != 'transfer-encoding':
                        self.send_header(key, value)
                self.end_headers()
                # Stream the response
                while True:
                    chunk = resp.read(8192)
                    if not chunk:
                        break
                    self.wfile.write(chunk)
        except Exception as e:
            self.send_error(502, f"Proxy error: {e}")

    def log_message(self, format, *args):
        # Print to stderr for logging
        sys.stderr.write("%s - - [%s] %s\n" %
                         (self.client_address[0],
                          self.log_date_time_string(),
                          format%args))

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="Simple HTTP proxy for local network. Usage: http://<host>:<port>/<remote_url>")
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind (default: 0.0.0.0)')
    parser.add_argument('--port', type=int, default=8080, help='Port to listen on (default: 8080)')
    args = parser.parse_args()

    with socketserver.ThreadingTCPServer((args.host, args.port), SimpleProxyHandler) as httpd:
        print(f"Serving HTTP proxy on {args.host}:{args.port}")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nShutting down proxy.")
