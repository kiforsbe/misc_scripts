import argparse
import html
import http.server
from pathlib import Path
import random
import re
import socketserver
import socket
import sys
import threading
import urllib.parse
from typing import Optional

try:
    import requests
except ImportError as exc:  # pragma: no cover - import guard
    raise SystemExit(
        "This script requires requests with SOCKS support. Install it with: pip install \"requests[socks]\""
    ) from exc


HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
}

STRIP_RESPONSE_HEADERS = {
    "alt-svc",
    "clear-site-data",
    "content-security-policy",
    "content-security-policy-report-only",
    "content-encoding",
    "content-length",
    "set-cookie",
    "strict-transport-security",
    "transfer-encoding",
    "x-frame-options",
}

HTML_CONTENT_TYPES = (
    "text/html",
    "application/xhtml+xml",
)

CSS_CONTENT_TYPES = (
    "text/css",
)

FEED_XML_CONTENT_TYPES = (
    "application/atom+xml",
    "application/rss+xml",
    "application/xml",
    "text/xml",
)

JAVASCRIPT_CONTENT_TYPES = (
    "application/javascript",
    "application/x-javascript",
    "text/javascript",
)

MAX_DEBUG_BODY_PREVIEW = 240
DEFAULT_PROXY_ROTATION_SECONDS = 10.0 * 60.0
ROTATION_INTERVAL_PATTERN = re.compile(
    r"^(?:(?P<minutes>\d+(?:\.\d+)?)m)?(?:(?P<seconds>\d+(?:\.\d+)?)s)?$",
    re.IGNORECASE,
)

REWRITE_ATTR_PATTERN = re.compile(
    r"(?P<name>href|src|action|poster|formaction|manifest)\s*=\s*(?P<quote>['\"])(?P<value>.*?)(?P=quote)",
    re.IGNORECASE | re.DOTALL,
)
REWRITE_SRCSET_PATTERN = re.compile(
    r"(?P<name>srcset)\s*=\s*(?P<quote>['\"])(?P<value>.*?)(?P=quote)",
    re.IGNORECASE | re.DOTALL,
)
REWRITE_CSS_URL_PATTERN = re.compile(
    r"url\(\s*(?P<quote>['\"]?)(?P<value>.*?)(?P=quote)\s*\)",
    re.IGNORECASE | re.DOTALL,
)
REWRITE_META_REFRESH_PATTERN = re.compile(
    r"(?P<prefix><meta\b[^>]*http-equiv\s*=\s*['\"]?refresh['\"]?[^>]*content\s*=\s*['\"])(?P<value>.*?)(?P<suffix>['\"])",
    re.IGNORECASE | re.DOTALL,
)

FEED_URL_TEXT_TAGS = {
    "comments",
    "docs",
    "icon",
    "id",
    "link",
    "logo",
    "uri",
    "url",
}

FEED_URL_ATTRIBUTES = {
    "href",
    "src",
    "url",
}

REWRITE_FEED_ATTR_PATTERN = re.compile(
    r"(?P<name>(?:[a-zA-Z_][\w.-]*:)?(?:href|src|url))\s*=\s*(?P<quote>['\"])(?P<value>.*?)(?P=quote)",
    re.IGNORECASE | re.DOTALL,
)
REWRITE_FEED_TEXT_PATTERN = re.compile(
    r"(?P<open><(?P<tag>(?:[a-zA-Z_][\w.-]*:)?(?:comments|docs|icon|id|link|logo|uri|url))\b[^>]*>)(?P<value>[^<]*?)(?P<close></(?P=tag)\s*>)",
    re.IGNORECASE | re.DOTALL,
)


def normalize_socks_proxy(value: str) -> str:
    # Force a canonical SOCKS URL form so requests gets one consistent proxy string.
    raw_value = value.strip()
    if not raw_value:
        raise ValueError("SOCKS5 proxy must not be empty")

    if "://" not in raw_value:
        raw_value = f"socks5h://{raw_value}"

    parsed = urllib.parse.urlsplit(raw_value)
    if parsed.scheme not in {"socks5", "socks5h"}:
        raise ValueError("SOCKS proxy must use socks5:// or socks5h://")
    if not parsed.hostname or parsed.port is None:
        raise ValueError("SOCKS proxy must include a host and port")

    if parsed.scheme == "socks5":
        parsed = parsed._replace(scheme="socks5h")

    return urllib.parse.urlunsplit(parsed)


def load_socks_proxy_candidates(file_path: Path) -> list[str]:
    try:
        entries = file_path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise SystemExit(f"Unable to read SOCKS proxy list file {file_path}: {exc}") from exc

    proxies: list[str] = []
    for line_number, raw_line in enumerate(entries, start=1):
        candidate = raw_line.strip()
        if not candidate or candidate.startswith("#"):
            continue
        try:
            proxies.append(normalize_socks_proxy(candidate))
        except ValueError as exc:
            raise SystemExit(f"Invalid SOCKS proxy on line {line_number} in {file_path}: {exc}") from exc

    if not proxies:
        raise SystemExit(f"SOCKS proxy list file is empty: {file_path}")

    return proxies


def probe_socks_proxy(socks_proxy: str, timeout: float) -> Optional[str]:
    parsed = urllib.parse.urlsplit(socks_proxy)
    if not parsed.hostname or parsed.port is None:
        return "missing host or port"

    username = urllib.parse.unquote(parsed.username) if parsed.username else None
    password = urllib.parse.unquote(parsed.password) if parsed.password else None
    methods = [0x00]
    if username is not None:
        methods.append(0x02)

    try:
        with socket.create_connection((parsed.hostname, parsed.port), timeout=timeout) as sock:
            sock.settimeout(timeout)
            # Perform the initial SOCKS5 negotiation so dead listeners and auth mismatches fail early.
            sock.sendall(bytes([0x05, len(methods), *methods]))
            response = sock.recv(2)
            if len(response) != 2 or response[0] != 0x05:
                return "invalid SOCKS5 greeting response"
            if response[1] == 0xFF:
                return "SOCKS5 server rejected available authentication methods"
            if response[1] == 0x02:
                if username is None or password is None:
                    return "SOCKS5 server requires username/password authentication"
                username_bytes = username.encode("utf-8")
                password_bytes = password.encode("utf-8")
                if len(username_bytes) > 255 or len(password_bytes) > 255:
                    return "SOCKS5 username/password is too long"
                auth_request = bytes([0x01, len(username_bytes)]) + username_bytes + bytes([len(password_bytes)]) + password_bytes
                sock.sendall(auth_request)
                auth_response = sock.recv(2)
                if len(auth_response) != 2 or auth_response[1] != 0x00:
                    return "SOCKS5 username/password authentication failed"
            elif response[1] != 0x00:
                return f"unsupported SOCKS5 authentication method selected: {response[1]}"
            return None
    except OSError as exc:
        return str(exc)


def choose_live_socks_proxy(socks_proxies: list[str], timeout: float, debug_enabled: bool = False) -> tuple[str, list[tuple[str, str]]]:
    remaining = list(socks_proxies)
    failures: list[tuple[str, str]] = []

    # Sample without replacement so startup picks a random live server but still tries the whole pool if needed.
    while remaining:
        index = random.randrange(len(remaining))
        candidate = remaining.pop(index)
        debug_log(debug_enabled, f"Probing SOCKS proxy candidate: {candidate}")
        failure_reason = probe_socks_proxy(candidate, timeout)
        if failure_reason is None:
            debug_log(debug_enabled, f"Selected live SOCKS proxy candidate: {candidate}")
            return candidate, failures
        failures.append((candidate, failure_reason))
        debug_log(debug_enabled, f"Rejected SOCKS proxy candidate {candidate}: {failure_reason}")

    raise SystemExit("No working SOCKS5 proxies found in the provided list")


def choose_rotated_socks_proxy(
    socks_proxies: list[str],
    timeout: float,
    debug_enabled: bool = False,
    current_proxy: Optional[str] = None,
) -> tuple[str, list[tuple[str, str]]]:
    candidates = list(socks_proxies)
    if current_proxy and len(candidates) > 1:
        candidates = [candidate for candidate in candidates if candidate != current_proxy]
    return choose_live_socks_proxy(candidates, timeout, debug_enabled)


def is_http_url(value: str) -> bool:
    return value.startswith("http://") or value.startswith("https://")


def debug_log(enabled: bool, message: str) -> None:
    if enabled:
        sys.stderr.write(f"[DEBUG] {message}\n")


def truncate_for_debug(value: str, limit: int = MAX_DEBUG_BODY_PREVIEW) -> str:
    compact = re.sub(r"\s+", " ", value)
    if len(compact) <= limit:
        return compact
    return compact[:limit] + "..."


def decode_text(payload: bytes, content_type: str, response: requests.Response) -> tuple[str, str]:
    encoding = response.encoding or "utf-8"
    if "charset=" in content_type.lower():
        try:
            encoding = content_type.lower().split("charset=", 1)[1].split(";", 1)[0].strip().strip('"') or encoding
        except Exception:
            encoding = response.encoding or "utf-8"
    try:
        return payload.decode(encoding, errors="replace"), encoding
    except LookupError:
        return payload.decode("utf-8", errors="replace"), "utf-8"


def parse_rotation_interval(value: str) -> float:
    raw_value = value.strip().lower()
    if not raw_value:
        raise argparse.ArgumentTypeError("rotation interval must not be empty")

    if raw_value[-1].isdigit():
        raw_value = f"{raw_value}m"

    match = ROTATION_INTERVAL_PATTERN.fullmatch(raw_value)
    if not match or not match.group(0):
        raise argparse.ArgumentTypeError(
            "rotation interval must look like 5, 5m, 45s, or 5m34s"
        )

    minutes = float(match.group("minutes") or 0.0)
    seconds = float(match.group("seconds") or 0.0)
    total_seconds = minutes * 60.0 + seconds
    if total_seconds <= 0:
        raise argparse.ArgumentTypeError("rotation interval must be greater than 0")
    return total_seconds


def format_rotation_interval(total_seconds: float) -> str:
    minutes = int(total_seconds // 60)
    seconds = total_seconds - minutes * 60
    parts: list[str] = []
    if minutes:
        parts.append(f"{minutes}m")
    if seconds or not parts:
        if seconds.is_integer():
            parts.append(f"{int(seconds)}s")
        else:
            parts.append(f"{seconds:g}s")
    return "".join(parts)


class ProxyApplication:
    def __init__(
        self,
        socks_proxy: str,
        timeout: float,
        debug_enabled: bool = False,
        verify_tls: bool | str = True,
        socks_proxy_candidates: Optional[list[str]] = None,
        rotation_interval_seconds: Optional[float] = None,
    ) -> None:
        self.socks_proxy = normalize_socks_proxy(socks_proxy)
        self.timeout = timeout
        self.debug_enabled = debug_enabled
        self.verify_tls = verify_tls
        self._socks_proxy_candidates = list(socks_proxy_candidates or [])
        self._rotation_interval_seconds = rotation_interval_seconds
        self._sessions: dict[str, requests.Session] = {}
        self._lock = threading.Lock()
        self._rotation_stop_event = threading.Event()
        self._rotation_thread: Optional[threading.Thread] = None

    def _close_sessions_locked(self) -> None:
        for session in self._sessions.values():
            session.close()
        self._sessions.clear()

    def rotate_socks_proxy(self) -> tuple[str, list[tuple[str, str]]]:
        current_proxy = self.socks_proxy
        next_proxy, failures = choose_rotated_socks_proxy(
            self._socks_proxy_candidates,
            self.timeout,
            self.debug_enabled,
            current_proxy=current_proxy,
        )
        with self._lock:
            self.socks_proxy = next_proxy
            self._close_sessions_locked()
        debug_log(
            self.debug_enabled,
            f"Rotated SOCKS proxy from {current_proxy} to {next_proxy}; cleared cached upstream sessions",
        )
        return next_proxy, failures

    def start_proxy_rotation(self) -> None:
        if not self._socks_proxy_candidates or self._rotation_interval_seconds is None:
            return
        if len(self._socks_proxy_candidates) < 2:
            debug_log(self.debug_enabled, "Skipping proxy rotation because the proxy list has fewer than 2 entries")
            return
        if self._rotation_thread is not None:
            return

        def run_rotation_loop() -> None:
            interval_seconds = self._rotation_interval_seconds
            assert interval_seconds is not None
            while not self._rotation_stop_event.wait(interval_seconds):
                try:
                    next_proxy, failures = self.rotate_socks_proxy()
                    if failures:
                        failed_summary = "; ".join(f"{proxy} ({reason})" for proxy, reason in failures)
                        debug_log(
                            self.debug_enabled,
                            f"Rotation switched to {next_proxy} after rejecting candidates: {failed_summary}",
                        )
                except SystemExit as exc:
                    debug_log(self.debug_enabled, f"Proxy rotation skipped because no working replacement proxy was found: {exc}")

        self._rotation_thread = threading.Thread(target=run_rotation_loop, name="socks-proxy-rotation", daemon=True)
        self._rotation_thread.start()
        debug_log(
            self.debug_enabled,
            f"Started proxy rotation thread interval_seconds={self._rotation_interval_seconds}",
        )

    def stop_proxy_rotation(self) -> None:
        self._rotation_stop_event.set()
        if self._rotation_thread is not None:
            self._rotation_thread.join(timeout=1.0)
            self._rotation_thread = None

    def get_session(self, client_key: str) -> requests.Session:
        with self._lock:
            session = self._sessions.get(client_key)
            if session is None:
                session = requests.Session()
                session.trust_env = False
                # Keep one upstream session per client IP so cookies and connection reuse stay isolated.
                session.proxies = {
                    "http": self.socks_proxy,
                    "https": self.socks_proxy,
                }
                session.verify = self.verify_tls
                self._sessions[client_key] = session
                debug_log(
                    self.debug_enabled,
                    f"Created upstream session for client={client_key} proxies={session.proxies} verify_tls={session.verify}",
                )
            return session

    def build_local_proxy_path(self, upstream_url: str) -> str:
        parsed = urllib.parse.urlsplit(upstream_url)
        path = parsed.path or "/"
        quoted_netloc = urllib.parse.quote(parsed.netloc, safe=":[]")
        quoted_path = urllib.parse.quote(path, safe="/%:@!$&'()*+,;=-._~")
        local_url = f"/proxy/{parsed.scheme}/{quoted_netloc}{quoted_path}"
        if parsed.query:
            local_url = f"{local_url}?{parsed.query}"
        if parsed.fragment:
            local_url = f"{local_url}#{parsed.fragment}"
        return local_url

    def build_local_proxy_url(self, upstream_url: str, local_origin: Optional[str] = None) -> str:
        local_path = self.build_local_proxy_path(upstream_url)
        if local_origin:
            return urllib.parse.urljoin(local_origin.rstrip("/") + "/", local_path.lstrip("/"))
        return local_path

    def local_to_upstream_url(self, local_url: str) -> Optional[str]:
        parsed = urllib.parse.urlsplit(local_url)
        query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)

        if parsed.path == "/":
            candidate = query.get("url", [""])[0].strip()
            return candidate if is_http_url(candidate) else None

        if not parsed.path.startswith("/proxy/"):
            return None

        parts = parsed.path.split("/", 4)
        if len(parts) < 4:
            return None

        scheme = parts[2]
        netloc = urllib.parse.unquote(parts[3])
        tail = "/"
        if len(parts) == 5 and parts[4]:
            tail = "/" + parts[4]

        upstream_url = urllib.parse.urlunsplit((scheme, netloc, tail, parsed.query, parsed.fragment))
        return upstream_url if is_http_url(upstream_url) else None

    def resolve_upstream_url(self, handler: "SocksTunnelHandler") -> tuple[Optional[str], bool]:
        parsed = urllib.parse.urlsplit(handler.path)
        query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        debug_log(
            self.debug_enabled,
            f"Resolving request method={handler.command} path={handler.path!r} referer={handler.headers.get('Referer')!r} origin={handler.headers.get('Origin')!r}",
        )

        if parsed.path == "/" and query.get("url"):
            # The entry URL is only a bootstrap format; normal browsing should move onto canonical /proxy/... paths.
            candidate = query["url"][0].strip()
            if is_http_url(candidate):
                debug_log(self.debug_enabled, f"Resolved target from query parameter: {candidate}")
                return candidate, True
            debug_log(self.debug_enabled, f"Rejected non-http query target: {candidate!r}")
            return None, False

        direct = self.local_to_upstream_url(handler.path)
        if direct:
            debug_log(self.debug_enabled, f"Resolved canonical proxied path to upstream URL: {direct}")
            return direct, False

        referer = handler.headers.get("Referer")
        if referer:
            upstream_referer = self.local_to_upstream_url(referer)
            if upstream_referer:
                # Relative asset requests rely on the last proxied page to recover their upstream base URL.
                relative_request = urllib.parse.urlunsplit(("", "", parsed.path or "/", parsed.query, ""))
                resolved = urllib.parse.urljoin(upstream_referer, relative_request)
                debug_log(self.debug_enabled, f"Resolved request relative to Referer: {resolved}")
                return resolved, False

        origin = handler.headers.get("Origin")
        if origin:
            upstream_origin = self.local_to_upstream_url(origin)
            if upstream_origin:
                relative_request = urllib.parse.urlunsplit(("", "", parsed.path or "/", parsed.query, ""))
                resolved = urllib.parse.urljoin(upstream_origin, relative_request)
                debug_log(self.debug_enabled, f"Resolved request relative to Origin: {resolved}")
                return resolved, False

        debug_log(self.debug_enabled, "Unable to resolve request to an upstream URL")
        return None, False

    def rewrite_embedded_url(self, value: str, base_url: str, local_origin: Optional[str] = None) -> str:
        candidate = html.unescape(value.strip())
        if not candidate or candidate.startswith(("#", "data:", "javascript:", "mailto:", "tel:", "about:")):
            return value

        # Preserve absolute form only when the source was already absolute; relative links stay local-path based.
        preserve_absolute = False
        if candidate.startswith("//"):
            absolute = urllib.parse.urlsplit(base_url).scheme + ":" + candidate
        elif is_http_url(candidate):
            absolute = candidate
            preserve_absolute = True
        elif candidate.startswith("/"):
            absolute = urllib.parse.urljoin(base_url, candidate)
        else:
            return value

        if preserve_absolute:
            return self.build_local_proxy_url(absolute, local_origin)
        return self.build_local_proxy_path(absolute)

    def rewrite_srcset(self, value: str, base_url: str, local_origin: Optional[str] = None) -> str:
        rewritten_entries: list[str] = []
        for part in value.split(","):
            segment = part.strip()
            if not segment:
                continue
            pieces = segment.split()
            rewritten_url = self.rewrite_embedded_url(pieces[0], base_url, local_origin)
            if len(pieces) > 1:
                rewritten_entries.append(" ".join([rewritten_url] + pieces[1:]))
            else:
                rewritten_entries.append(rewritten_url)
        return ", ".join(rewritten_entries)

    def rewrite_css_urls(self, text: str, base_url: str, local_origin: Optional[str] = None) -> str:
        def replace(match: re.Match[str]) -> str:
            value = match.group("value")
            rewritten = self.rewrite_embedded_url(value, base_url, local_origin)
            quote = match.group("quote")
            return f"url({quote}{rewritten}{quote})"

        return REWRITE_CSS_URL_PATTERN.sub(replace, text)

    def rewrite_html(self, text: str, base_url: str, local_origin: Optional[str] = None) -> str:
        def replace_attr(match: re.Match[str]) -> str:
            name = match.group("name")
            quote = match.group("quote")
            value = match.group("value")
            rewritten = self.rewrite_embedded_url(value, base_url, local_origin)
            return f"{name}={quote}{rewritten}{quote}"

        def replace_srcset(match: re.Match[str]) -> str:
            name = match.group("name")
            quote = match.group("quote")
            value = match.group("value")
            rewritten = self.rewrite_srcset(value, base_url, local_origin)
            return f"{name}={quote}{rewritten}{quote}"

        def replace_meta_refresh(match: re.Match[str]) -> str:
            value = match.group("value")
            parts = re.split(r"(;\s*url=)", value, maxsplit=1, flags=re.IGNORECASE)
            if len(parts) == 3:
                rewritten = self.rewrite_embedded_url(parts[2], base_url, local_origin)
                return f"{match.group('prefix')}{parts[0]}{parts[1]}{rewritten}{match.group('suffix')}"
            return match.group(0)

        rewritten = REWRITE_ATTR_PATTERN.sub(replace_attr, text)
        rewritten = REWRITE_SRCSET_PATTERN.sub(replace_srcset, rewritten)
        rewritten = self.rewrite_css_urls(rewritten, base_url, local_origin)
        rewritten = REWRITE_META_REFRESH_PATTERN.sub(replace_meta_refresh, rewritten)
        return self.inject_runtime_shim(rewritten)

    def looks_like_feed_xml(self, text: str) -> bool:
        lowered = text[:4096].lower()
        return "<rss" in lowered or "<feed" in lowered or "<rdf:rdf" in lowered

    def rewrite_feed_xml(self, text: str, base_url: str, local_origin: Optional[str] = None) -> str:
        # Use text substitutions instead of XML reserialization so prefixes, namespace aliases, and formatting survive unchanged.
        def replace_attr(match: re.Match[str]) -> str:
            value = match.group("value")
            rewritten = self.rewrite_embedded_url(value, base_url, local_origin)
            if rewritten == value:
                return match.group(0)
            return f"{match.group('name')}={match.group('quote')}{rewritten}{match.group('quote')}"

        def replace_text(match: re.Match[str]) -> str:
            value = match.group("value")
            stripped_value = value.strip()
            if not stripped_value:
                return match.group(0)
            rewritten = self.rewrite_embedded_url(stripped_value, base_url, local_origin)
            if rewritten == stripped_value:
                return match.group(0)
            leading = value[: len(value) - len(value.lstrip())]
            trailing = value[len(value.rstrip()) :]
            return f"{match.group('open')}{leading}{rewritten}{trailing}{match.group('close')}"

        rewritten = REWRITE_FEED_ATTR_PATTERN.sub(replace_attr, text)
        rewritten = REWRITE_FEED_TEXT_PATTERN.sub(replace_text, rewritten)
        return rewritten

    def inject_runtime_shim(self, html_text: str) -> str:
        # Runtime interception covers client-side fetch/XHR/navigation APIs that static HTML rewriting cannot see.
        shim = (
            "<script>"
            "(function(){"
            "const localOrigin=window.location.origin;"
            "function toProxy(input){"
            "if(typeof input!==\"string\"||!input){return input;}"
            "if(input.startsWith(\"#\")||input.startsWith(\"data:\")||input.startsWith(\"javascript:\")||input.startsWith(\"mailto:\")||input.startsWith(\"tel:\")){return input;}"
            "try{"
            "const url=new URL(input,window.location.href);"
            "if(!/^https?:$/.test(url.protocol)){return input;}"
            "if(url.origin===localOrigin){return input;}"
            "return \"/proxy/\"+url.protocol.slice(0,-1)+\"/\"+url.host+url.pathname+url.search+url.hash;"
            "}catch(_error){return input;}"
            "}"
            "const nativeFetch=window.fetch;"
            "if(nativeFetch){window.fetch=function(resource,init){"
            "if(typeof resource===\"string\"){resource=toProxy(resource);}"
            "else if(resource instanceof Request){resource=new Request(toProxy(resource.url),resource);}"
            "return nativeFetch.call(this,resource,init);"
            "};}"
            "const nativeOpen=XMLHttpRequest.prototype.open;"
            "XMLHttpRequest.prototype.open=function(method,url){arguments[1]=toProxy(url);return nativeOpen.apply(this,arguments);};"
            "const nativePushState=history.pushState;"
            "history.pushState=function(state,title,url){if(typeof url===\"string\"){arguments[2]=toProxy(url);}return nativePushState.apply(this,arguments);};"
            "const nativeReplaceState=history.replaceState;"
            "history.replaceState=function(state,title,url){if(typeof url===\"string\"){arguments[2]=toProxy(url);}return nativeReplaceState.apply(this,arguments);};"
            "const nativeOpenWindow=window.open;"
            "window.open=function(url){if(typeof url===\"string\"){arguments[0]=toProxy(url);}return nativeOpenWindow.apply(this,arguments);};"
            "document.addEventListener(\"click\",function(event){"
            "const link=event.target&&event.target.closest?event.target.closest(\"a[href]\"):null;"
            "if(!link){return;}"
            "const href=link.getAttribute(\"href\");"
            "const rewritten=toProxy(href);"
            "if(rewritten!==href){link.setAttribute(\"href\",rewritten);}"
            "},true);"
            "document.addEventListener(\"submit\",function(event){"
            "const form=event.target;"
            "if(!(form instanceof HTMLFormElement)){return;}"
            "const action=form.getAttribute(\"action\");"
            "if(!action){return;}"
            "const rewritten=toProxy(action);"
            "if(rewritten!==action){form.setAttribute(\"action\",rewritten);}"
            "},true);"
            "})();"
            "</script>"
        )

        head_close_index = html_text.lower().find("</head>")
        if head_close_index >= 0:
            return html_text[:head_close_index] + shim + html_text[head_close_index:]
        body_open_match = re.search(r"<body\b[^>]*>", html_text, flags=re.IGNORECASE)
        if body_open_match:
            insert_at = body_open_match.end()
            return html_text[:insert_at] + shim + html_text[insert_at:]
        return shim + html_text

    def rewrite_text_response(self, text: str, content_type: str, base_url: str, local_origin: Optional[str] = None) -> str:
        lowered = content_type.lower()
        if any(content_type_name in lowered for content_type_name in HTML_CONTENT_TYPES):
            debug_log(self.debug_enabled, f"Rewriting HTML response for {base_url} content_type={content_type}")
            return self.rewrite_html(text, base_url, local_origin)
        if any(content_type_name in lowered for content_type_name in CSS_CONTENT_TYPES):
            debug_log(self.debug_enabled, f"Rewriting CSS response for {base_url} content_type={content_type}")
            return self.rewrite_css_urls(text, base_url, local_origin)
        if any(content_type_name in lowered for content_type_name in FEED_XML_CONTENT_TYPES) and self.looks_like_feed_xml(text):
            debug_log(self.debug_enabled, f"Rewriting feed XML response for {base_url} content_type={content_type}")
            return self.rewrite_feed_xml(text, base_url, local_origin)
        debug_log(self.debug_enabled, f"Leaving response body unchanged for {base_url} content_type={content_type}")
        return text

    def rewrite_header_url(self, value: str, base_url: str, local_origin: Optional[str] = None) -> str:
        candidate = value.strip()
        if not candidate:
            return value
        preserve_absolute = is_http_url(candidate)
        if not preserve_absolute:
            candidate = urllib.parse.urljoin(base_url, candidate)
        if preserve_absolute:
            return self.build_local_proxy_url(candidate, local_origin)
        return self.build_local_proxy_path(candidate)

    def rewrite_refresh_header(self, value: str, base_url: str, local_origin: Optional[str] = None) -> str:
        parts = re.split(r"(;\s*url=)", value, maxsplit=1, flags=re.IGNORECASE)
        if len(parts) != 3:
            return value
        return f"{parts[0]}{parts[1]}{self.rewrite_header_url(parts[2], base_url, local_origin)}"


class SocksTunnelHandler(http.server.BaseHTTPRequestHandler):
    server_version = "socks5_http_tunneler/1.0"

    @property
    def app(self) -> ProxyApplication:
        return self.server.app  # type: ignore[attr-defined]

    def do_GET(self) -> None:
        self.handle_proxy_request()

    def do_HEAD(self) -> None:
        self.handle_proxy_request()

    def do_POST(self) -> None:
        self.handle_proxy_request()

    def do_PUT(self) -> None:
        self.handle_proxy_request()

    def do_PATCH(self) -> None:
        self.handle_proxy_request()

    def do_DELETE(self) -> None:
        self.handle_proxy_request()

    def do_OPTIONS(self) -> None:
        self.handle_proxy_request()

    def handle_proxy_request(self) -> None:
        target_url, should_redirect = self.app.resolve_upstream_url(self)
        if not target_url:
            self.send_error(400, "Missing or invalid target URL. Use /?url=http://example.com")
            return

        debug_log(
            self.app.debug_enabled,
            f"Handling client request method={self.command} target={target_url} redirect_to_canonical={should_redirect}",
        )

        if should_redirect and self.command in {"GET", "HEAD"}:
            location = self.app.build_local_proxy_path(target_url)
            debug_log(self.app.debug_enabled, f"Redirecting client to canonical local path: {location}")
            self.send_response(302)
            self.send_header("Location", location)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        request_body = self.read_request_body()
        upstream_headers = self.build_upstream_headers(target_url)
        client_key = self.client_address[0]
        session = self.app.get_session(client_key)

        debug_log(
            self.app.debug_enabled,
            f"Forwarding upstream request method={self.command} url={target_url} body_bytes={len(request_body)} header_count={len(upstream_headers)}",
        )
        if self.app.debug_enabled:
            debug_log(self.app.debug_enabled, f"Upstream request headers: {upstream_headers}")

        try:
            upstream_response = session.request(
                method=self.command,
                url=target_url,
                headers=upstream_headers,
                data=request_body,
                allow_redirects=False,
                timeout=self.app.timeout,
            )
        except requests.exceptions.SSLError as exc:
            debug_log(self.app.debug_enabled, f"Upstream TLS verification failed: {exc!r}")
            self.send_error(
                502,
                "Upstream TLS certificate verification failed. Supply --ca-bundle <pem> for a trusted private CA, or use --insecure if you trust the target path or SOCKS proxy.",
            )
            return
        except requests.RequestException as exc:
            debug_log(self.app.debug_enabled, f"Upstream request failed: {exc!r}")
            self.send_error(502, f"Upstream proxy error: {exc}")
            return

        debug_log(
            self.app.debug_enabled,
            f"Received upstream response status={upstream_response.status_code} reason={upstream_response.reason!r} final_url={upstream_response.url} content_type={upstream_response.headers.get('Content-Type', '')!r} body_bytes={len(upstream_response.content)}",
        )

        payload, content_type, encoding = self.prepare_response_payload(upstream_response, target_url)
        debug_log(
            self.app.debug_enabled,
            f"Prepared downstream payload content_type={content_type!r} encoding={encoding!r} body_bytes={len(payload)}",
        )
        self.send_response(upstream_response.status_code)
        self.copy_response_headers(upstream_response, target_url, content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()

        if self.command != "HEAD":
            self.wfile.write(payload)

    def build_upstream_headers(self, target_url: str) -> dict[str, str]:
        upstream_headers: dict[str, str] = {}
        target_parts = urllib.parse.urlsplit(target_url)
        upstream_origin = urllib.parse.urlunsplit((target_parts.scheme, target_parts.netloc, "", "", ""))
        for name, value in self.headers.items():
            lowered = name.lower()
            if lowered in HOP_BY_HOP_HEADERS or lowered in {"host", "content-length", "cookie", "accept-encoding"}:
                continue
            if lowered == "referer":
                # Referer needs to point back to the true upstream page instead of the local proxy URL.
                rewritten = self.app.local_to_upstream_url(value)
                if rewritten:
                    upstream_headers[name] = rewritten
                continue
            if lowered == "origin":
                # CORS-sensitive endpoints expect the upstream origin, not the local tunnel origin.
                upstream_headers[name] = upstream_origin
                continue
            upstream_headers[name] = value

        upstream_headers.setdefault(
            "User-Agent",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
        )
        upstream_headers.setdefault("Accept-Encoding", "gzip, deflate")
        return upstream_headers

    def get_local_origin(self) -> Optional[str]:
        host = self.headers.get("Host", "").strip()
        if not host:
            return None
        return f"http://{host}"

    def prepare_response_payload(self, response: requests.Response, base_url: str) -> tuple[bytes, str, str]:
        content_type = response.headers.get("Content-Type", "application/octet-stream")
        payload = response.content
        text, encoding = decode_text(payload, content_type, response)
        rewritten_text = self.app.rewrite_text_response(text, content_type, base_url, self.get_local_origin())
        if rewritten_text is text:
            if self.app.debug_enabled and payload:
                debug_log(self.app.debug_enabled, f"Response preview: {truncate_for_debug(text)}")
            return payload, content_type, encoding
        if self.app.debug_enabled:
            debug_log(self.app.debug_enabled, f"Rewritten response preview: {truncate_for_debug(rewritten_text)}")
        return rewritten_text.encode(encoding, errors="replace"), content_type, encoding

    def copy_response_headers(self, response: requests.Response, base_url: str, content_type: str) -> None:
        local_origin = self.get_local_origin()
        rewritten_header_names: list[str] = []
        for name, value in response.headers.items():
            lowered = name.lower()
            if lowered in HOP_BY_HOP_HEADERS or lowered in STRIP_RESPONSE_HEADERS:
                continue
            if lowered == "content-type":
                continue
            if lowered == "location":
                self.send_header(name, self.app.rewrite_header_url(value, base_url, local_origin))
                rewritten_header_names.append(name)
                continue
            if lowered == "refresh":
                self.send_header(name, self.app.rewrite_refresh_header(value, base_url, local_origin))
                rewritten_header_names.append(name)
                continue
            self.send_header(name, value)
        if self.app.debug_enabled:
            debug_log(
                self.app.debug_enabled,
                f"Copied downstream headers total={len(response.headers)} rewritten={rewritten_header_names}",
            )
        self.send_header("Content-Type", content_type)

    def read_request_body(self) -> bytes:
        content_length = self.headers.get("Content-Length")
        if not content_length:
            return b""
        try:
            body_length = int(content_length)
        except ValueError:
            return b""
        return self.rfile.read(body_length)

    def log_message(self, format: str, *args) -> None:
        sys.stderr.write(
            "%s - - [%s] %s\n"
            % (self.client_address[0], self.log_date_time_string(), format % args)
        )


class ThreadedHTTPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, server_address: tuple[str, int], handler_class: type[SocksTunnelHandler], app: ProxyApplication) -> None:
        super().__init__(server_address, handler_class)
        self.app = app


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Expose a local HTTP tunneler that forwards upstream traffic through a SOCKS5 proxy. "
            "Entry URL format: http://localhost:8080/?url=http://example.com"
        )
    )
    socks_group = parser.add_mutually_exclusive_group(required=True)
    socks_group.add_argument("--socks5", help="SOCKS5 proxy to use, for example 127.0.0.1:1080 or socks5h://127.0.0.1:1080")
    socks_group.add_argument("--socks5-file", help="Text file with one SOCKS5 proxy per line; one live entry is selected at random at startup")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind locally (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8080, help="Local port to listen on (default: 8080)")
    parser.add_argument("--timeout", type=float, default=30.0, help="Upstream request timeout in seconds (default: 30)")
    parser.add_argument(
        "--rotation-interval",
        type=parse_rotation_interval,
        help="Time between random proxy rotations when using --socks5-file, for example 5, 5m, 45s, or 5m34s (default: 10m)",
    )
    parser.add_argument("--debug", action="store_true", help="Print verbose request, rewrite, and upstream response diagnostics")
    tls_group = parser.add_mutually_exclusive_group()
    tls_group.add_argument(
        "--insecure",
        action="store_true",
        help="Disable upstream HTTPS certificate verification. Use only when you trust the SOCKS proxy and upstream path.",
    )
    tls_group.add_argument(
        "--ca-bundle",
        help="Path to a PEM CA bundle to trust for upstream HTTPS verification, for example a private proxy root CA.",
    )
    return parser


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()
    if args.socks5 and args.rotation_interval is not None:
        raise SystemExit("--rotation-interval can only be used with --socks5-file")

    verify_tls: bool | str = True
    if args.insecure:
        verify_tls = False
    elif args.ca_bundle:
        ca_bundle_path = Path(args.ca_bundle).expanduser().resolve()
        if not ca_bundle_path.is_file():
            raise SystemExit(f"CA bundle file not found: {ca_bundle_path}")
        verify_tls = str(ca_bundle_path)

    failed_socks_proxies: list[tuple[str, str]] = []
    socks5_candidates: list[str] = []
    rotation_interval_seconds: Optional[float] = None
    if args.socks5_file:
        # Resolve one working SOCKS endpoint at startup, then rotate across the list on a timer.
        socks5_file_path = Path(args.socks5_file).expanduser().resolve()
        if not socks5_file_path.is_file():
            raise SystemExit(f"SOCKS proxy list file not found: {socks5_file_path}")
        socks5_candidates = load_socks_proxy_candidates(socks5_file_path)
        selected_socks5, failed_socks_proxies = choose_live_socks_proxy(socks5_candidates, args.timeout, args.debug)
        rotation_interval_seconds = args.rotation_interval or DEFAULT_PROXY_ROTATION_SECONDS
    else:
        selected_socks5 = args.socks5

    app = ProxyApplication(
        selected_socks5,
        args.timeout,
        debug_enabled=args.debug,
        verify_tls=verify_tls,
        socks_proxy_candidates=socks5_candidates,
        rotation_interval_seconds=rotation_interval_seconds,
    )

    with ThreadedHTTPServer((args.host, args.port), SocksTunnelHandler, app) as server:
        app.start_proxy_rotation()
        print(f"Serving SOCKS5 HTTP tunneler on http://{args.host}:{args.port}")
        print(f"SOCKS5 upstream: {app.socks_proxy}")
        if args.socks5_file:
            print(f"SOCKS5 proxy list: {socks5_file_path}")
            if failed_socks_proxies:
                print("SOCKS5 proxies that failed startup probing:")
                for failed_proxy, reason in failed_socks_proxies:
                    print(f"  - {failed_proxy} ({reason})")
            print(f"Chosen SOCKS5 proxy for this session: {app.socks_proxy}")
            assert rotation_interval_seconds is not None
            print(f"SOCKS5 proxy rotation interval: {format_rotation_interval(rotation_interval_seconds)}")
        print(f"Open: http://{args.host}:{args.port}/?url=http://google.com")
        if args.insecure:
            print("Upstream TLS verification disabled")
        elif args.ca_bundle:
            print(f"Using upstream CA bundle: {app.verify_tls}")
        if args.debug:
            print("Debug logging enabled")
            debug_log(
                app.debug_enabled,
                f"Startup configuration host={args.host} port={args.port} timeout={args.timeout} verify_tls={app.verify_tls}",
            )
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print("\nShutting down tunneler.")
            server.shutdown()
        finally:
            app.stop_proxy_rotation()
            server.server_close()


if __name__ == "__main__":
    main()