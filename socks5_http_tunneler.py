import argparse
import csv
from dataclasses import dataclass
from datetime import datetime
import html
import http.server
from pathlib import Path
import random
import re
import socketserver
import socket
import sys
import threading
import time
import urllib.parse
from typing import Optional
import warnings

try:
    import requests
except ImportError as exc:  # pragma: no cover - import guard
    raise SystemExit(
        "This script requires requests with SOCKS support. Install it with: pip install \"requests[socks]\""
    ) from exc

try:
    from tqdm import tqdm
except ImportError as exc:  # pragma: no cover - import guard
    raise SystemExit(
        f"This script requires tqdm for proxy testing progress bars. Install it with: {sys.executable} -m pip install tqdm"
    ) from exc

from urllib3.exceptions import InsecureRequestWarning


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
DEFAULT_PROXY_ROTATION_SECONDS = 60.0 * 60.0
DEFAULT_CLIENT_IDLE_TIMEOUT_SECONDS = 5.0 * 60.0
DEFAULT_PROXY_BLACKLIST_FILE = Path.home() / ".socks5-proxy-blacklist.csv"
DEFAULT_PROXY_WHITELIST_FILE = Path.home() / ".socks5-proxy-whitelist.csv"
PROXY_PROGRESS_REFRESH_SECONDS = 0.1
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


def load_socks_proxy_blacklist(file_path: Path) -> dict[str, tuple[str, str]]:
    try:
        with file_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames != ["proxy", "failure_reason", "failed_at"]:
                raise SystemExit(
                    f"Invalid SOCKS proxy blacklist CSV header in {file_path}; expected: proxy,failure_reason,failed_at"
                )

            blacklisted: dict[str, tuple[str, str]] = {}
            for line_number, row in enumerate(reader, start=2):
                candidate = (row.get("proxy") or "").strip()
                failure_reason = (row.get("failure_reason") or "").strip()
                failed_at = (row.get("failed_at") or "").strip()
                if not candidate:
                    continue
                try:
                    blacklisted[normalize_socks_proxy(candidate)] = (failure_reason, failed_at)
                except ValueError as exc:
                    raise SystemExit(f"Invalid SOCKS proxy on line {line_number} in blacklist {file_path}: {exc}") from exc
            return blacklisted
    except FileNotFoundError:
        return {}
    except OSError as exc:
        raise SystemExit(f"Unable to read SOCKS proxy blacklist file {file_path}: {exc}") from exc


def load_socks_proxy_whitelist(file_path: Path) -> dict[str, tuple[int, str]]:
    try:
        with file_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames != ["proxy", "success_count", "last_succeeded_at"]:
                raise SystemExit(
                    f"Invalid SOCKS proxy whitelist CSV header in {file_path}; expected: proxy,success_count,last_succeeded_at"
                )

            whitelisted: dict[str, tuple[int, str]] = {}
            for line_number, row in enumerate(reader, start=2):
                candidate = (row.get("proxy") or "").strip()
                success_count_text = (row.get("success_count") or "").strip()
                last_succeeded_at = (row.get("last_succeeded_at") or "").strip()
                if not candidate:
                    continue
                try:
                    success_count = int(success_count_text)
                    if success_count < 1:
                        raise ValueError("success_count must be >= 1")
                    whitelisted[normalize_socks_proxy(candidate)] = (success_count, last_succeeded_at)
                except ValueError as exc:
                    raise SystemExit(f"Invalid SOCKS proxy whitelist entry on line {line_number} in {file_path}: {exc}") from exc
            return whitelisted
    except FileNotFoundError:
        return {}
    except OSError as exc:
        raise SystemExit(f"Unable to read SOCKS proxy whitelist file {file_path}: {exc}") from exc


def filter_blacklisted_socks_proxies(
    socks_proxies: list[str],
    blacklisted_socks_proxies: dict[str, tuple[str, str]],
) -> tuple[list[str], list[str]]:
    allowed: list[str] = []
    removed: list[str] = []
    for socks_proxy in socks_proxies:
        if socks_proxy in blacklisted_socks_proxies:
            removed.append(socks_proxy)
            continue
        allowed.append(socks_proxy)
    return allowed, removed


def append_socks_proxy_blacklist_entries(
    file_path: Path,
    failures: list[tuple[str, str]],
    known_blacklist: dict[str, tuple[str, str]],
) -> list[str]:
    timestamp = datetime.now().astimezone().isoformat(timespec="seconds")
    new_entries = [(socks_proxy, failure_reason, timestamp) for socks_proxy, failure_reason in failures if socks_proxy not in known_blacklist]
    if not new_entries:
        return []

    try:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_exists = file_path.exists()
        with file_path.open("a", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            if not file_exists:
                writer.writerow(["proxy", "failure_reason", "failed_at"])
            for socks_proxy, failure_reason, failed_at in new_entries:
                writer.writerow([socks_proxy, failure_reason, failed_at])
    except OSError as exc:
        raise SystemExit(f"Unable to update SOCKS proxy blacklist file {file_path}: {exc}") from exc

    for socks_proxy, failure_reason, failed_at in new_entries:
        known_blacklist[socks_proxy] = (failure_reason, failed_at)
    return [socks_proxy for socks_proxy, _failure_reason, _failed_at in new_entries]


def append_socks_proxy_whitelist_entries(
    file_path: Path,
    successes: list[str],
    known_whitelist: dict[str, tuple[int, str]],
) -> list[str]:
    timestamp = datetime.now().astimezone().isoformat(timespec="seconds")
    updated_entries: list[tuple[str, int, str]] = []
    for socks_proxy in successes:
        previous_count, _previous_timestamp = known_whitelist.get(socks_proxy, (0, ""))
        known_whitelist[socks_proxy] = (previous_count + 1, timestamp)
        updated_entries.append((socks_proxy, previous_count + 1, timestamp))

    if not updated_entries:
        return []

    try:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with file_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["proxy", "success_count", "last_succeeded_at"])
            for socks_proxy in sorted(known_whitelist):
                success_count, last_succeeded_at = known_whitelist[socks_proxy]
                writer.writerow([socks_proxy, success_count, last_succeeded_at])
    except OSError as exc:
        raise SystemExit(f"Unable to update SOCKS proxy whitelist file {file_path}: {exc}") from exc

    return [socks_proxy for socks_proxy, _success_count, _succeeded_at in updated_entries]


def progress_log(message: str) -> None:
    sys.stderr.write(f"{message}\n")


def format_proxy_progress_label(socks_proxy: str, max_length: int = 56) -> str:
    if len(socks_proxy) <= max_length:
        return socks_proxy
    return socks_proxy[: max_length - 3] + "..."


def probe_socks_proxy_with_progress(
    socks_proxy: str,
    timeout: float,
    attempt_number: int,
    total_candidates: int,
) -> Optional[str]:
    result: dict[str, object] = {}
    completed = threading.Event()
    timer_total = max(timeout, PROXY_PROGRESS_REFRESH_SECONDS)
    timer_label = format_proxy_progress_label(socks_proxy)

    def worker() -> None:
        try:
            result["failure_reason"] = probe_socks_proxy(socks_proxy, timeout)
        except BaseException as exc:  # pragma: no cover - should not happen, but preserve failures from the worker thread.
            result["exception"] = exc
        finally:
            completed.set()

    worker_thread = threading.Thread(target=worker, name="socks-proxy-probe", daemon=True)
    worker_thread.start()
    started_at = time.monotonic()
    last_elapsed = 0.0

    with tqdm(
        total=timer_total,
        desc=f"  Proxy {attempt_number}/{total_candidates}",
        unit="s",
        leave=False,
        dynamic_ncols=True,
        file=sys.stderr,
        bar_format="{desc}: |{bar}| {n:.1f}/{total:.1f}s [{elapsed}<{remaining}] {postfix}",
    ) as timer_progress:
        timer_progress.set_postfix_str(timer_label)
        while not completed.wait(PROXY_PROGRESS_REFRESH_SECONDS):
            elapsed = min(time.monotonic() - started_at, timer_total)
            increment = elapsed - last_elapsed
            if increment > 0:
                timer_progress.update(increment)
                last_elapsed = elapsed

        worker_thread.join()
        elapsed = min(time.monotonic() - started_at, timer_total)
        increment = elapsed - last_elapsed
        if increment > 0:
            timer_progress.update(increment)

    if "exception" in result:
        raise result["exception"]  # type: ignore[misc]
    return result.get("failure_reason")  # type: ignore[return-value]


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


def choose_live_socks_proxy(
    socks_proxies: list[str],
    timeout: float,
    debug_enabled: bool = False,
    blacklist_file_path: Optional[Path] = None,
    blacklisted_socks_proxies: Optional[dict[str, tuple[str, str]]] = None,
    whitelist_file_path: Optional[Path] = None,
    whitelisted_socks_proxies: Optional[dict[str, tuple[int, str]]] = None,
) -> tuple[str, list[tuple[str, str]]]:
    remaining = list(socks_proxies)
    random.shuffle(remaining)
    failures: list[tuple[str, str]] = []
    total_candidates = len(remaining)
    known_blacklist = blacklisted_socks_proxies if blacklisted_socks_proxies is not None else {}
    known_whitelist = whitelisted_socks_proxies if whitelisted_socks_proxies is not None else {}

    # Shuffle once so probe order is explicitly random without replacement.
    with tqdm(
        total=total_candidates,
        desc="Testing proxies",
        unit="proxy",
        dynamic_ncols=True,
        file=sys.stderr,
        bar_format="{desc}: |{bar}| {n_fmt}/{total_fmt} tested [{elapsed}<{remaining}] {postfix}",
    ) as total_progress:
        total_progress.set_postfix_str("failed=0")
        while remaining:
            candidate = remaining.pop()
            attempted = total_candidates - len(remaining)
            total_progress.set_postfix_str(f"failed={len(failures)} current={attempted}/{total_candidates}")
            debug_log(debug_enabled, f"Probing SOCKS proxy candidate: {candidate}")
            failure_reason = probe_socks_proxy_with_progress(candidate, timeout, attempted, total_candidates)
            total_progress.update(1)
            if failure_reason is None:
                if whitelist_file_path is not None:
                    append_socks_proxy_whitelist_entries(
                        whitelist_file_path,
                        [candidate],
                        known_whitelist,
                    )
                tqdm.write(f"SOCKS proxy probe succeeded: {candidate}", file=sys.stderr)
                debug_log(debug_enabled, f"Selected live SOCKS proxy candidate: {candidate}")
                total_progress.set_postfix_str(f"failed={len(failures)}")
                return candidate, failures
            failures.append((candidate, failure_reason))
            total_progress.set_postfix_str(f"failed={len(failures)}")
            tqdm.write(f"SOCKS proxy probe failed: {candidate} ({failure_reason})", file=sys.stderr)
            if blacklist_file_path is not None:
                append_socks_proxy_blacklist_entries(blacklist_file_path, [(candidate, failure_reason)], known_blacklist)
            debug_log(debug_enabled, f"Rejected SOCKS proxy candidate {candidate}: {failure_reason}")

    raise SystemExit("No working SOCKS5 proxies found in the provided list")


def choose_rotated_socks_proxy(
    socks_proxies: list[str],
    timeout: float,
    debug_enabled: bool = False,
    current_proxy: Optional[str] = None,
    blacklist_file_path: Optional[Path] = None,
    blacklisted_socks_proxies: Optional[dict[str, tuple[str, str]]] = None,
    whitelist_file_path: Optional[Path] = None,
    whitelisted_socks_proxies: Optional[dict[str, tuple[int, str]]] = None,
) -> tuple[str, list[tuple[str, str]]]:
    candidates = list(socks_proxies)
    if current_proxy and len(candidates) > 1:
        candidates = [candidate for candidate in candidates if candidate != current_proxy]
    return choose_live_socks_proxy(
        candidates,
        timeout,
        debug_enabled,
        blacklist_file_path=blacklist_file_path,
        blacklisted_socks_proxies=blacklisted_socks_proxies,
        whitelist_file_path=whitelist_file_path,
        whitelisted_socks_proxies=whitelisted_socks_proxies,
    )


def is_http_url(value: str) -> bool:
    return value.startswith("http://") or value.startswith("https://")


def debug_log(enabled: bool, message: str) -> None:
    if enabled:
        sys.stderr.write(f"[DEBUG] {message}\n")


def configure_runtime_noise(debug_enabled: bool, verify_tls: bool | str) -> None:
    if debug_enabled or verify_tls is not False:
        return
    warnings.filterwarnings("ignore", category=InsecureRequestWarning)


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

    if raw_value in {"0", "off", "disabled"}:
        return 0.0

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
    if total_seconds < 0:
        raise argparse.ArgumentTypeError("rotation interval must be 0 or greater")
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


def format_optional_interval(total_seconds: Optional[float]) -> str:
    if total_seconds is None or total_seconds <= 0:
        return "disabled"
    return format_rotation_interval(total_seconds)


@dataclass
class CachedUpstreamSession:
    session: requests.Session
    proxy_url: str
    last_activity: float
    in_use: int = 0


class ProxyApplication:
    def __init__(
        self,
        socks_proxy: str,
        timeout: float,
        debug_enabled: bool = False,
        verify_tls: bool | str = True,
        socks_proxy_candidates: Optional[list[str]] = None,
        rotation_interval_seconds: Optional[float] = None,
        client_idle_timeout_seconds: float = DEFAULT_CLIENT_IDLE_TIMEOUT_SECONDS,
        proxy_blacklist_file_path: Optional[Path] = None,
        blacklisted_socks_proxies: Optional[dict[str, tuple[str, str]]] = None,
        proxy_whitelist_file_path: Optional[Path] = None,
        whitelisted_socks_proxies: Optional[dict[str, tuple[int, str]]] = None,
    ) -> None:
        self.socks_proxy = normalize_socks_proxy(socks_proxy)
        self.timeout = timeout
        self.debug_enabled = debug_enabled
        self.verify_tls = verify_tls
        self._socks_proxy_candidates = list(socks_proxy_candidates or [])
        self._rotation_interval_seconds = rotation_interval_seconds if rotation_interval_seconds and rotation_interval_seconds > 0 else None
        self._client_idle_timeout_seconds = (
            client_idle_timeout_seconds if client_idle_timeout_seconds > 0 else None
        )
        self._proxy_blacklist_file_path = proxy_blacklist_file_path
        self._blacklisted_socks_proxies = blacklisted_socks_proxies if blacklisted_socks_proxies is not None else {}
        self._proxy_whitelist_file_path = proxy_whitelist_file_path
        self._whitelisted_socks_proxies = whitelisted_socks_proxies if whitelisted_socks_proxies is not None else {}
        self._sessions: dict[str, CachedUpstreamSession] = {}
        self._lock = threading.Lock()
        self._background_stop_event = threading.Event()
        self._rotation_thread: Optional[threading.Thread] = None
        self._idle_session_reaper_thread: Optional[threading.Thread] = None

    def _close_cached_session_locked(self, client_key: str, cached_session: CachedUpstreamSession, reason: str) -> None:
        cached_session.session.close()
        self._sessions.pop(client_key, None)
        debug_log(
            self.debug_enabled,
            f"Closed upstream session for client={client_key} proxy={cached_session.proxy_url} reason={reason}",
        )

    def _close_sessions_locked(self) -> None:
        current_items = list(self._sessions.items())
        for client_key, cached_session in current_items:
            if cached_session.in_use > 0:
                continue
            self._close_cached_session_locked(client_key, cached_session, "proxy rotation")

    def _close_idle_sessions_locked(self, now: float) -> None:
        if self._client_idle_timeout_seconds is None:
            return
        current_items = list(self._sessions.items())
        for client_key, cached_session in current_items:
            if cached_session.in_use > 0:
                continue
            idle_for = now - cached_session.last_activity
            if idle_for < self._client_idle_timeout_seconds:
                continue
            self._close_cached_session_locked(
                client_key,
                cached_session,
                f"client idle timeout after {format_rotation_interval(idle_for)}",
            )

    def _create_session_locked(self, client_key: str) -> CachedUpstreamSession:
        session = requests.Session()
        session.trust_env = False
        session.proxies = {
            "http": self.socks_proxy,
            "https": self.socks_proxy,
        }
        session.verify = self.verify_tls
        cached_session = CachedUpstreamSession(
            session=session,
            proxy_url=self.socks_proxy,
            last_activity=time.monotonic(),
        )
        self._sessions[client_key] = cached_session
        debug_log(
            self.debug_enabled,
            f"Created upstream session for client={client_key} proxies={session.proxies} verify_tls={session.verify}",
        )
        return cached_session

    def rotate_socks_proxy(self) -> tuple[str, list[tuple[str, str]]]:
        current_proxy = self.socks_proxy
        next_proxy, failures = choose_rotated_socks_proxy(
            self._socks_proxy_candidates,
            self.timeout,
            self.debug_enabled,
            current_proxy=current_proxy,
            blacklist_file_path=self._proxy_blacklist_file_path,
            blacklisted_socks_proxies=self._blacklisted_socks_proxies,
            whitelist_file_path=self._proxy_whitelist_file_path,
            whitelisted_socks_proxies=self._whitelisted_socks_proxies,
        )
        with self._lock:
            if failures:
                failed_proxies = {proxy for proxy, _reason in failures}
                self._socks_proxy_candidates = [
                    candidate for candidate in self._socks_proxy_candidates if candidate not in failed_proxies
                ]
            self.socks_proxy = next_proxy
            self._close_sessions_locked()
        debug_log(
            self.debug_enabled,
            f"Rotated SOCKS proxy from {current_proxy} to {next_proxy}; idle cached upstream sessions were closed and future sessions stay lazy",
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
            while not self._background_stop_event.wait(interval_seconds):
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

    def start_idle_session_reaper(self) -> None:
        if self._client_idle_timeout_seconds is None:
            return
        if self._idle_session_reaper_thread is not None:
            return

        def run_idle_session_reaper() -> None:
            timeout_seconds = self._client_idle_timeout_seconds
            assert timeout_seconds is not None
            wait_seconds = min(timeout_seconds, 1.0)
            while not self._background_stop_event.wait(wait_seconds):
                with self._lock:
                    self._close_idle_sessions_locked(time.monotonic())

        self._idle_session_reaper_thread = threading.Thread(
            target=run_idle_session_reaper,
            name="upstream-session-idle-reaper",
            daemon=True,
        )
        self._idle_session_reaper_thread.start()
        debug_log(
            self.debug_enabled,
            f"Started idle session reaper timeout_seconds={self._client_idle_timeout_seconds}",
        )

    def stop_proxy_rotation(self) -> None:
        self._background_stop_event.set()
        if self._rotation_thread is not None:
            self._rotation_thread.join(timeout=1.0)
            self._rotation_thread = None
        if self._idle_session_reaper_thread is not None:
            self._idle_session_reaper_thread.join(timeout=1.0)
            self._idle_session_reaper_thread = None

    def acquire_session(self, client_key: str) -> requests.Session:
        with self._lock:
            cached_session = self._sessions.get(client_key)
            if cached_session is not None and cached_session.proxy_url != self.socks_proxy and cached_session.in_use == 0:
                self._close_cached_session_locked(client_key, cached_session, "stale proxy assignment")
                cached_session = None
            if cached_session is None:
                cached_session = self._create_session_locked(client_key)
            cached_session.in_use += 1
            cached_session.last_activity = time.monotonic()
            return cached_session.session

    def release_session(self, client_key: str) -> None:
        with self._lock:
            cached_session = self._sessions.get(client_key)
            if cached_session is None:
                return
            if cached_session.in_use > 0:
                cached_session.in_use -= 1
            cached_session.last_activity = time.monotonic()
            if cached_session.proxy_url != self.socks_proxy and cached_session.in_use == 0:
                self._close_cached_session_locked(client_key, cached_session, "proxy rotated while session was active")

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
        else:
            absolute = urllib.parse.urljoin(base_url, candidate)

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
        return self.inject_runtime_shim(rewritten, base_url)

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

    def inject_runtime_shim(self, html_text: str, base_url: str) -> str:
        # Runtime interception covers client-side fetch/XHR/navigation APIs that static HTML rewriting cannot see.
        encoded_upstream_base_url = html.escape(base_url, quote=True)
        shim = (
            "<script>"
            "(function(){"
            "const localOrigin=window.location.origin;"
            f"const upstreamBaseUrl=\"{encoded_upstream_base_url}\";"
            "function toProxy(input){"
            "if(typeof input!==\"string\"||!input){return input;}"
            "if(input.startsWith(\"#\")||input.startsWith(\"data:\")||input.startsWith(\"javascript:\")||input.startsWith(\"mailto:\")||input.startsWith(\"tel:\")){return input;}"
            "try{"
            "const url=new URL(input,window.location.href);"
            "if(!/^https?:$/.test(url.protocol)){return input;}"
            "if(url.origin===localOrigin&&url.pathname.startsWith(\"/proxy/\")){return url.pathname+url.search+url.hash;}"
            "const upstreamUrl=url.origin===localOrigin?new URL(input,upstreamBaseUrl):url;"
            "return \"/proxy/\"+upstreamUrl.protocol.slice(0,-1)+\"/\"+upstreamUrl.host+upstreamUrl.pathname+upstreamUrl.search+upstreamUrl.hash;"
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

    DOWNSTREAM_DISCONNECT_EXCEPTIONS = (
        BrokenPipeError,
        ConnectionAbortedError,
        ConnectionResetError,
    )

    @property
    def app(self) -> ProxyApplication:
        return self.server.app  # type: ignore[attr-defined]

    def try_send_error(self, code: int, message: str) -> bool:
        try:
            self.send_error(code, message)
            return True
        except self.DOWNSTREAM_DISCONNECT_EXCEPTIONS as exc:
            debug_log(
                self.app.debug_enabled,
                f"Client disconnected while sending error response code={code}: {exc!r}",
            )
            return False

    def try_send_downstream_response(
        self,
        status_code: int,
        upstream_response: requests.Response,
        target_url: str,
        payload: bytes,
        content_type: str,
    ) -> bool:
        try:
            self.send_response(status_code)
            self.copy_response_headers(upstream_response, target_url, content_type)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()

            if self.command != "HEAD":
                self.wfile.write(payload)
            return True
        except self.DOWNSTREAM_DISCONNECT_EXCEPTIONS as exc:
            debug_log(
                self.app.debug_enabled,
                f"Client disconnected while sending upstream response status={status_code}: {exc!r}",
            )
            return False

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
            self.try_send_error(400, "Missing or invalid target URL. Use /?url=http://example.com")
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
        session = self.app.acquire_session(client_key)

        try:
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
                self.try_send_error(
                    502,
                    "Upstream TLS certificate verification failed. Supply --ca-bundle <pem> for a trusted private CA, or use --insecure if you trust the target path or SOCKS proxy.",
                )
                return
            except requests.RequestException as exc:
                debug_log(self.app.debug_enabled, f"Upstream request failed: {exc!r}")
                self.try_send_error(502, f"Upstream proxy error: {exc}")
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
            self.try_send_downstream_response(
                upstream_response.status_code,
                upstream_response,
                target_url,
                payload,
                content_type,
            )
        finally:
            self.app.release_session(client_key)

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
        app = getattr(self.server, "app", None)
        if app is None or not app.debug_enabled:
            return
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
        help="Time between random proxy rotations when using --socks5-file, for example 5, 5m, 45s, or 5m34s; use 0, off, or disabled to disable (default: 60m)",
    )
    parser.add_argument(
        "--client-idle-timeout",
        type=parse_rotation_interval,
        default=DEFAULT_CLIENT_IDLE_TIMEOUT_SECONDS,
        help="Close an upstream proxy session after this much client inactivity, for example 30s, 2m, or 2m30s; use 0, off, or disabled to disable (default: 5m)",
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
    socks5_blacklist_file_path = DEFAULT_PROXY_BLACKLIST_FILE
    socks5_whitelist_file_path = DEFAULT_PROXY_WHITELIST_FILE
    blacklisted_socks_proxies: dict[str, tuple[str, str]] = {}
    whitelisted_socks_proxies: dict[str, tuple[int, str]] = {}
    if args.socks5_file:
        # Resolve one working SOCKS endpoint at startup, then rotate across the list on a timer.
        socks5_file_path = Path(args.socks5_file).expanduser().resolve()
        if not socks5_file_path.is_file():
            raise SystemExit(f"SOCKS proxy list file not found: {socks5_file_path}")
        socks5_candidates = load_socks_proxy_candidates(socks5_file_path)
        blacklisted_socks_proxies = load_socks_proxy_blacklist(socks5_blacklist_file_path)
        whitelisted_socks_proxies = load_socks_proxy_whitelist(socks5_whitelist_file_path)
        socks5_candidates, removed_blacklisted_proxies = filter_blacklisted_socks_proxies(socks5_candidates, blacklisted_socks_proxies)
        if removed_blacklisted_proxies:
            print(f"Removed {len(removed_blacklisted_proxies)} blacklisted SOCKS5 proxies from the startup pool")
            print(f"SOCKS5 proxy blacklist: {socks5_blacklist_file_path}")
        if not socks5_candidates:
            raise SystemExit(
                f"All SOCKS5 proxies from {socks5_file_path} are blacklisted in {socks5_blacklist_file_path}"
            )
        selected_socks5, failed_socks_proxies = choose_live_socks_proxy(
            socks5_candidates,
            args.timeout,
            args.debug,
            blacklist_file_path=socks5_blacklist_file_path,
            blacklisted_socks_proxies=blacklisted_socks_proxies,
            whitelist_file_path=socks5_whitelist_file_path,
            whitelisted_socks_proxies=whitelisted_socks_proxies,
        )
        if failed_socks_proxies:
            failed_startup_proxies = {proxy for proxy, _reason in failed_socks_proxies}
            socks5_candidates = [candidate for candidate in socks5_candidates if candidate not in failed_startup_proxies]
        rotation_interval_seconds = (
            DEFAULT_PROXY_ROTATION_SECONDS if args.rotation_interval is None else args.rotation_interval
        )
    else:
        selected_socks5 = args.socks5

    app = ProxyApplication(
        selected_socks5,
        args.timeout,
        debug_enabled=args.debug,
        verify_tls=verify_tls,
        socks_proxy_candidates=socks5_candidates,
        rotation_interval_seconds=rotation_interval_seconds,
        client_idle_timeout_seconds=args.client_idle_timeout,
        proxy_blacklist_file_path=socks5_blacklist_file_path if args.socks5_file else None,
        blacklisted_socks_proxies=blacklisted_socks_proxies,
        proxy_whitelist_file_path=socks5_whitelist_file_path if args.socks5_file else None,
        whitelisted_socks_proxies=whitelisted_socks_proxies,
    )
    configure_runtime_noise(args.debug, verify_tls)

    with ThreadedHTTPServer((args.host, args.port), SocksTunnelHandler, app) as server:
        app.start_proxy_rotation()
        app.start_idle_session_reaper()
        print(f"Serving SOCKS5 HTTP tunneler on http://{args.host}:{args.port}")
        print(f"SOCKS5 upstream: {app.socks_proxy}")
        print(f"Client idle timeout: {format_optional_interval(app._client_idle_timeout_seconds)}")
        if args.socks5_file:
            print(f"SOCKS5 proxy list: {socks5_file_path}")
            print(f"SOCKS5 proxy blacklist: {socks5_blacklist_file_path}")
            print(f"SOCKS5 proxy whitelist: {socks5_whitelist_file_path}")
            if failed_socks_proxies:
                print("SOCKS5 proxies that failed startup probing:")
                for failed_proxy, reason in failed_socks_proxies:
                    print(f"  - {failed_proxy} ({reason})")
            print(f"Chosen SOCKS5 proxy for this session: {app.socks_proxy}")
            print(f"SOCKS5 proxy rotation interval: {format_optional_interval(app._rotation_interval_seconds)}")
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