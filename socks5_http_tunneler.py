from abc import ABC, abstractmethod
import argparse
from collections.abc import Iterable
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
from typing import ClassVar, Optional
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
DEFAULT_PROXY_BLACKLIST_FILE = Path.home() / ".proxy-blacklist.csv"
DEFAULT_PROXY_WHITELIST_FILE = Path.home() / ".proxy-whitelist.csv"
PROXY_PROGRESS_REFRESH_SECONDS = 0.1
# Fixed, low-weight target used to verify that proxies without a cheap no-op handshake
# (HTTP, HTTPS, SOCKS4) actually forward traffic end-to-end.
PROXY_LIVENESS_TEST_URL = "https://example.com/"

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


class Debug:
    """Namespaces the script's verbose stderr diagnostics behind a single gate."""

    @staticmethod
    def log(enabled: bool, message: str) -> None:
        if enabled:
            sys.stderr.write(f"[DEBUG] {message}\n")


class RotationInterval:
    """Parsing and formatting for human-friendly interval strings like '5m34s'."""

    _PATTERN = re.compile(
        r"^(?:(?P<minutes>\d+(?:\.\d+)?)m)?(?:(?P<seconds>\d+(?:\.\d+)?)s)?$",
        re.IGNORECASE,
    )

    @classmethod
    def parse(cls, value: str) -> float:
        raw_value = value.strip().lower()
        if not raw_value:
            raise argparse.ArgumentTypeError("rotation interval must not be empty")

        if raw_value in {"0", "off", "disabled"}:
            return 0.0

        if raw_value[-1].isdigit():
            raw_value = f"{raw_value}m"

        match = cls._PATTERN.fullmatch(raw_value)
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

    @staticmethod
    def format(total_seconds: float) -> str:
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

    @classmethod
    def format_optional(cls, total_seconds: Optional[float]) -> str:
        if total_seconds is None or total_seconds <= 0:
            return "disabled"
        return cls.format(total_seconds)


class ProxyBackend(ABC):
    """Scheme-specific proxy behavior: URL canonicalization and liveness probing.

    One instance handles every scheme in `schemes`. The base class also acts as
    the registry/factory for its subclasses, so callers never branch on scheme
    themselves: `ProxyBackend.normalize(...)` and `ProxyBackend.probe_url(...)`
    are the only entry points callers need.
    """

    schemes: ClassVar[frozenset[str]]
    canonical_scheme: ClassVar[str]

    _registry: ClassVar[Optional[dict[str, "ProxyBackend"]]] = None

    def canonicalize(self, parsed: urllib.parse.SplitResult) -> urllib.parse.SplitResult:
        if parsed.scheme == self.canonical_scheme:
            return parsed
        return parsed._replace(scheme=self.canonical_scheme)

    @abstractmethod
    def probe(self, proxy_url: str, timeout: float) -> Optional[str]:
        """Return None if the proxy is reachable and functioning, else a failure reason."""
        raise NotImplementedError

    @classmethod
    def registry(cls) -> dict[str, "ProxyBackend"]:
        if cls._registry is None:
            registry: dict[str, ProxyBackend] = {}
            for backend in (HttpProxyBackend(), HttpsProxyBackend(), Socks4ProxyBackend(), Socks5ProxyBackend()):
                for scheme in backend.schemes:
                    registry[scheme] = backend
            cls._registry = registry
        return cls._registry

    @classmethod
    def for_scheme(cls, scheme: str) -> Optional["ProxyBackend"]:
        return cls.registry().get(scheme)

    @classmethod
    def accepted_schemes(cls) -> list[str]:
        return sorted(cls.registry())

    @classmethod
    def normalize(cls, value: str) -> str:
        # Force a canonical proxy URL form so requests gets one consistent proxy string.
        raw_value = value.strip()
        if not raw_value:
            raise ValueError("proxy must not be empty")

        if "://" not in raw_value:
            raw_value = f"socks5h://{raw_value}"

        parsed = urllib.parse.urlsplit(raw_value)
        backend = cls.for_scheme(parsed.scheme)
        if backend is None:
            raise ValueError("proxy must use one of: " + ", ".join(cls.accepted_schemes()))
        if not parsed.hostname or parsed.port is None:
            raise ValueError("proxy must include a host and port")

        return urllib.parse.urlunsplit(backend.canonicalize(parsed))

    @classmethod
    def probe_url(cls, proxy_url: str, timeout: float) -> Optional[str]:
        parsed = urllib.parse.urlsplit(proxy_url)
        if not parsed.hostname or parsed.port is None:
            return "missing host or port"
        backend = cls.for_scheme(parsed.scheme)
        if backend is None:
            return f"unsupported proxy scheme: {parsed.scheme}"
        return backend.probe(proxy_url, timeout)


class TestRequestProxyBackend(ProxyBackend):
    """Shared probe for backends with no cheap no-op handshake: issue a real request."""

    def probe(self, proxy_url: str, timeout: float) -> Optional[str]:
        try:
            with requests.Session() as session:
                session.trust_env = False
                session.head(
                    PROXY_LIVENESS_TEST_URL,
                    proxies={"http": proxy_url, "https": proxy_url},
                    timeout=timeout,
                    verify=True,
                    allow_redirects=False,
                )
            return None
        except requests.RequestException as exc:
            return str(exc)


class HttpProxyBackend(TestRequestProxyBackend):
    schemes = frozenset({"http"})
    canonical_scheme = "http"


class HttpsProxyBackend(TestRequestProxyBackend):
    schemes = frozenset({"https"})
    canonical_scheme = "https"


class Socks4ProxyBackend(TestRequestProxyBackend):
    schemes = frozenset({"socks4"})
    canonical_scheme = "socks4"


class Socks5ProxyBackend(ProxyBackend):
    schemes = frozenset({"socks5", "socks5h"})
    canonical_scheme = "socks5h"

    def probe(self, proxy_url: str, timeout: float) -> Optional[str]:
        parsed = urllib.parse.urlsplit(proxy_url)
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


class ProxyPool:
    """A rotating set of proxy candidates plus their blacklist/whitelist history.

    Owns everything needed to pick a live proxy and to persist the outcome of
    every probe: the in-memory candidate list, the blacklist/whitelist dicts,
    and the CSV files backing them.
    """

    def __init__(
        self,
        candidates: list[str],
        file_path: Path,
        blacklist_file_path: Path,
        whitelist_file_path: Path,
        blacklisted_proxies: Optional[dict[str, tuple[str, str]]] = None,
        whitelisted_proxies: Optional[dict[str, tuple[int, str]]] = None,
    ) -> None:
        self._candidates = list(candidates)
        self._file_path = file_path
        self._blacklist_file_path = blacklist_file_path
        self._whitelist_file_path = whitelist_file_path
        self._blacklisted = blacklisted_proxies if blacklisted_proxies is not None else {}
        self._whitelisted = whitelisted_proxies if whitelisted_proxies is not None else {}

    @property
    def file_path(self) -> Path:
        return self._file_path

    @property
    def blacklist_file_path(self) -> Path:
        return self._blacklist_file_path

    @property
    def whitelist_file_path(self) -> Path:
        return self._whitelist_file_path

    @classmethod
    def load(cls, candidates_pattern: str, blacklist_file_path: Path, whitelist_file_path: Path) -> "ProxyPool":
        # Resolve one working proxy endpoint at startup, then rotate across the pool on a timer.
        file_path = cls._resolve_list_file(candidates_pattern)
        candidates = cls._load_candidates(file_path)
        blacklisted = cls._load_blacklist(blacklist_file_path)
        whitelisted = cls._load_whitelist(whitelist_file_path)
        candidates, removed = cls._filter_blacklisted(candidates, blacklisted)
        if removed:
            print(f"Removed {len(removed)} blacklisted proxies from the proxy pool")
            print(f"Proxy blacklist: {blacklist_file_path}")
        if not candidates:
            raise SystemExit(f"All proxies from {file_path} are blacklisted in {blacklist_file_path}")
        return cls(candidates, file_path, blacklist_file_path, whitelist_file_path, blacklisted, whitelisted)

    @staticmethod
    def _resolve_list_file(pattern: str) -> Path:
        # Support wildcards and pick the latest modified file.
        expanded_path = Path(pattern).expanduser()

        if "*" in pattern or "?" in pattern:
            parent = expanded_path.parent
            glob_pattern = expanded_path.name
            if not parent.exists():
                raise SystemExit(f"Directory not found: {parent}")
            matches = list(parent.glob(glob_pattern))
            if not matches:
                raise SystemExit(f"No files found matching pattern: {pattern}")
            matches.sort(key=lambda p: p.stat().st_mtime, reverse=True)
            return matches[0]

        resolved_path = expanded_path.resolve()
        if not resolved_path.is_file():
            raise SystemExit(f"Proxy list file not found: {resolved_path}")
        return resolved_path

    @staticmethod
    def _load_candidates(file_path: Path) -> list[str]:
        try:
            entries = file_path.read_text(encoding="utf-8").splitlines()
        except OSError as exc:
            raise SystemExit(f"Unable to read proxy list file {file_path}: {exc}") from exc

        proxies: list[str] = []
        for line_number, raw_line in enumerate(entries, start=1):
            candidate = raw_line.strip()
            if not candidate or candidate.startswith("#"):
                continue
            try:
                proxies.append(ProxyBackend.normalize(candidate))
            except ValueError as exc:
                raise SystemExit(f"Invalid proxy on line {line_number} in {file_path}: {exc}") from exc

        if not proxies:
            raise SystemExit(f"Proxy list file is empty: {file_path}")

        return proxies

    @staticmethod
    def _read_csv_rows(file_path: Path, expected_header: list[str], purpose: str) -> list[dict[str, Optional[str]]]:
        try:
            with file_path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                if reader.fieldnames != expected_header:
                    raise SystemExit(
                        f"Invalid proxy {purpose} CSV header in {file_path}; expected: {','.join(expected_header)}"
                    )
                return list(reader)
        except FileNotFoundError:
            return []
        except OSError as exc:
            raise SystemExit(f"Unable to read proxy {purpose} file {file_path}: {exc}") from exc

    @classmethod
    def _load_blacklist(cls, file_path: Path) -> dict[str, tuple[str, str]]:
        blacklisted: dict[str, tuple[str, str]] = {}
        rows = cls._read_csv_rows(file_path, ["proxy", "failure_reason", "failed_at"], "blacklist")
        for line_number, row in enumerate(rows, start=2):
            candidate = (row.get("proxy") or "").strip()
            failure_reason = (row.get("failure_reason") or "").strip()
            failed_at = (row.get("failed_at") or "").strip()
            if not candidate:
                continue
            try:
                blacklisted[ProxyBackend.normalize(candidate)] = (failure_reason, failed_at)
            except ValueError as exc:
                raise SystemExit(f"Invalid proxy on line {line_number} in blacklist {file_path}: {exc}") from exc
        return blacklisted

    @classmethod
    def _load_whitelist(cls, file_path: Path) -> dict[str, tuple[int, str]]:
        whitelisted: dict[str, tuple[int, str]] = {}
        rows = cls._read_csv_rows(file_path, ["proxy", "success_count", "last_succeeded_at"], "whitelist")
        for line_number, row in enumerate(rows, start=2):
            candidate = (row.get("proxy") or "").strip()
            success_count_text = (row.get("success_count") or "").strip()
            last_succeeded_at = (row.get("last_succeeded_at") or "").strip()
            if not candidate:
                continue
            try:
                success_count = int(success_count_text)
                if success_count < 1:
                    raise ValueError("success_count must be >= 1")
                whitelisted[ProxyBackend.normalize(candidate)] = (success_count, last_succeeded_at)
            except ValueError as exc:
                raise SystemExit(f"Invalid proxy whitelist entry on line {line_number} in {file_path}: {exc}") from exc
        return whitelisted

    @staticmethod
    def _filter_blacklisted(
        proxies: list[str],
        blacklisted_proxies: dict[str, tuple[str, str]],
    ) -> tuple[list[str], list[str]]:
        allowed: list[str] = []
        removed: list[str] = []
        for proxy in proxies:
            if proxy in blacklisted_proxies:
                removed.append(proxy)
                continue
            allowed.append(proxy)
        return allowed, removed

    def __len__(self) -> int:
        return len(self._candidates)

    def discard(self, proxies: Iterable[str]) -> None:
        discard_set = set(proxies)
        if not discard_set:
            return
        self._candidates = [candidate for candidate in self._candidates if candidate not in discard_set]

    def choose_live(self, timeout: float, debug_enabled: bool = False) -> tuple[str, list[tuple[str, str]]]:
        return self._choose(list(self._candidates), timeout, debug_enabled)

    def choose_rotated(
        self,
        current_proxy: Optional[str],
        timeout: float,
        debug_enabled: bool = False,
    ) -> tuple[str, list[tuple[str, str]]]:
        candidates = list(self._candidates)
        if current_proxy and len(candidates) > 1:
            candidates = [candidate for candidate in candidates if candidate != current_proxy]
        return self._choose(candidates, timeout, debug_enabled)

    def _record_success(self, proxy: str) -> None:
        timestamp = datetime.now().astimezone().isoformat(timespec="seconds")
        previous_count, _previous_timestamp = self._whitelisted.get(proxy, (0, ""))
        self._whitelisted[proxy] = (previous_count + 1, timestamp)
        try:
            self._whitelist_file_path.parent.mkdir(parents=True, exist_ok=True)
            with self._whitelist_file_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["proxy", "success_count", "last_succeeded_at"])
                for whitelisted_proxy in sorted(self._whitelisted):
                    success_count, last_succeeded_at = self._whitelisted[whitelisted_proxy]
                    writer.writerow([whitelisted_proxy, success_count, last_succeeded_at])
        except OSError as exc:
            raise SystemExit(f"Unable to update proxy whitelist file {self._whitelist_file_path}: {exc}") from exc

    def _record_failure(self, proxy: str, reason: str) -> None:
        if proxy in self._blacklisted:
            return
        timestamp = datetime.now().astimezone().isoformat(timespec="seconds")
        self._blacklisted[proxy] = (reason, timestamp)
        try:
            self._blacklist_file_path.parent.mkdir(parents=True, exist_ok=True)
            file_exists = self._blacklist_file_path.exists()
            with self._blacklist_file_path.open("a", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                if not file_exists:
                    writer.writerow(["proxy", "failure_reason", "failed_at"])
                writer.writerow([proxy, reason, timestamp])
        except OSError as exc:
            raise SystemExit(f"Unable to update proxy blacklist file {self._blacklist_file_path}: {exc}") from exc

    @staticmethod
    def _format_progress_label(proxy_url: str, max_length: int = 56) -> str:
        if len(proxy_url) <= max_length:
            return proxy_url
        return proxy_url[: max_length - 3] + "..."

    @staticmethod
    def _probe_with_progress(proxy_url: str, timeout: float, attempt_number: int, total_candidates: int) -> Optional[str]:
        result: dict[str, object] = {}
        completed = threading.Event()
        timer_total = max(timeout, PROXY_PROGRESS_REFRESH_SECONDS)
        timer_label = ProxyPool._format_progress_label(proxy_url)

        def worker() -> None:
            try:
                result["failure_reason"] = ProxyBackend.probe_url(proxy_url, timeout)
            except BaseException as exc:  # pragma: no cover - should not happen, but preserve failures from the worker thread.
                result["exception"] = exc
            finally:
                completed.set()

        worker_thread = threading.Thread(target=worker, name="proxy-probe", daemon=True)
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

    def _choose(self, candidates: list[str], timeout: float, debug_enabled: bool) -> tuple[str, list[tuple[str, str]]]:
        # Shuffle once so probe order is explicitly random without replacement.
        remaining = list(candidates)
        random.shuffle(remaining)
        failures: list[tuple[str, str]] = []
        total_candidates = len(remaining)

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
                Debug.log(debug_enabled, f"Probing proxy candidate: {candidate}")
                failure_reason = self._probe_with_progress(candidate, timeout, attempted, total_candidates)
                total_progress.update(1)
                if failure_reason is None:
                    self._record_success(candidate)
                    tqdm.write(f"Proxy probe succeeded: {candidate}", file=sys.stderr)
                    Debug.log(debug_enabled, f"Selected live proxy candidate: {candidate}")
                    total_progress.set_postfix_str(f"failed={len(failures)}")
                    if failures:
                        self.discard(proxy for proxy, _reason in failures)
                    return candidate, failures
                failures.append((candidate, failure_reason))
                total_progress.set_postfix_str(f"failed={len(failures)}")
                tqdm.write(f"Proxy probe failed: {candidate} ({failure_reason})", file=sys.stderr)
                self._record_failure(candidate, failure_reason)
                Debug.log(debug_enabled, f"Rejected proxy candidate {candidate}: {failure_reason}")

        raise SystemExit("No working proxies found in the provided list")


@dataclass
class CachedUpstreamSession:
    session: requests.Session
    proxy_url: str
    last_activity: float
    in_use: int = 0


class ProxyApplication:
    def __init__(
        self,
        proxy: str,
        timeout: float,
        debug_enabled: bool = False,
        verify_tls: bool | str = True,
        proxy_pool: Optional[ProxyPool] = None,
        rotation_interval_seconds: Optional[float] = None,
        client_idle_timeout_seconds: float = DEFAULT_CLIENT_IDLE_TIMEOUT_SECONDS,
    ) -> None:
        self.proxy = ProxyBackend.normalize(proxy)
        self.timeout = timeout
        self.debug_enabled = debug_enabled
        self.verify_tls = verify_tls
        self._pool = proxy_pool
        self._rotation_interval_seconds = rotation_interval_seconds if rotation_interval_seconds and rotation_interval_seconds > 0 else None
        self._client_idle_timeout_seconds = (
            client_idle_timeout_seconds if client_idle_timeout_seconds > 0 else None
        )
        self._sessions: dict[str, CachedUpstreamSession] = {}
        self._lock = threading.Lock()
        self._background_stop_event = threading.Event()
        self._rotation_thread: Optional[threading.Thread] = None
        self._idle_session_reaper_thread: Optional[threading.Thread] = None

    @staticmethod
    def is_http_url(value: str) -> bool:
        return value.startswith("http://") or value.startswith("https://")

    @staticmethod
    def build_argument_parser() -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(
            description=(
                "Expose a local HTTP tunneler that forwards upstream traffic through an HTTP, HTTPS, SOCKS4, or SOCKS5 proxy. "
                "Entry URL format: http://localhost:8080/?url=http://example.com"
            )
        )
        proxy_group = parser.add_mutually_exclusive_group(required=True)
        proxy_group.add_argument(
            "--proxy",
            help=(
                "Upstream proxy to use, for example 127.0.0.1:1080 (defaults to socks5h://) or an explicit "
                "http://, https://, socks4://, socks5://, or socks5h:// URL"
            ),
        )
        proxy_group.add_argument(
            "--proxy-file",
            help="Text file with one proxy per line (same URL forms as --proxy); one live entry is selected at random at startup",
        )
        parser.add_argument("--host", default="127.0.0.1", help="Host to bind locally (default: 127.0.0.1)")
        parser.add_argument("--port", type=int, default=8080, help="Local port to listen on (default: 8080)")
        parser.add_argument("--timeout", type=float, default=5.0, help="Upstream request timeout in seconds (default: 5)")
        parser.add_argument(
            "--rotation-interval",
            type=RotationInterval.parse,
            help="Time between random proxy rotations when using --proxy-file, for example 5, 5m, 45s, or 5m34s; use 0, off, or disabled to disable (default: 60m)",
        )
        parser.add_argument(
            "--client-idle-timeout",
            type=RotationInterval.parse,
            default=DEFAULT_CLIENT_IDLE_TIMEOUT_SECONDS,
            help="Close an upstream proxy session after this much client inactivity, for example 30s, 2m, or 2m30s; use 0, off, or disabled to disable (default: 5m)",
        )
        parser.add_argument("--debug", action="store_true", help="Print verbose request, rewrite, and upstream response diagnostics")
        tls_group = parser.add_mutually_exclusive_group()
        tls_group.add_argument(
            "--insecure",
            action="store_true",
            help="Disable upstream HTTPS certificate verification. Use only when you trust the proxy and upstream path.",
        )
        tls_group.add_argument(
            "--ca-bundle",
            help="Path to a PEM CA bundle to trust for upstream HTTPS verification, for example a private proxy root CA.",
        )
        return parser

    @property
    def rotation_interval_seconds(self) -> Optional[float]:
        return self._rotation_interval_seconds

    @property
    def client_idle_timeout_seconds(self) -> Optional[float]:
        return self._client_idle_timeout_seconds

    def _close_cached_session_locked(self, client_key: str, cached_session: CachedUpstreamSession, reason: str) -> None:
        cached_session.session.close()
        self._sessions.pop(client_key, None)
        Debug.log(
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
                f"client idle timeout after {RotationInterval.format(idle_for)}",
            )

    def _create_session_locked(self, client_key: str) -> CachedUpstreamSession:
        session = requests.Session()
        session.trust_env = False
        session.proxies = {
            "http": self.proxy,
            "https": self.proxy,
        }
        session.verify = self.verify_tls
        cached_session = CachedUpstreamSession(
            session=session,
            proxy_url=self.proxy,
            last_activity=time.monotonic(),
        )
        self._sessions[client_key] = cached_session
        Debug.log(
            self.debug_enabled,
            f"Created upstream session for client={client_key} proxies={session.proxies} verify_tls={session.verify}",
        )
        return cached_session

    def rotate_proxy(self) -> tuple[str, list[tuple[str, str]]]:
        assert self._pool is not None, "rotate_proxy requires a proxy pool"
        current_proxy = self.proxy
        next_proxy, failures = self._pool.choose_rotated(current_proxy, self.timeout, self.debug_enabled)
        with self._lock:
            self.proxy = next_proxy
            self._close_sessions_locked()
        Debug.log(
            self.debug_enabled,
            f"Rotated proxy from {current_proxy} to {next_proxy}; idle cached upstream sessions were closed and future sessions stay lazy",
        )
        return next_proxy, failures

    def start_proxy_rotation(self) -> None:
        if self._pool is None or self._rotation_interval_seconds is None:
            return
        if len(self._pool) < 2:
            Debug.log(self.debug_enabled, "Skipping proxy rotation because the proxy list has fewer than 2 entries")
            return
        if self._rotation_thread is not None:
            return

        def run_rotation_loop() -> None:
            interval_seconds = self._rotation_interval_seconds
            assert interval_seconds is not None
            while not self._background_stop_event.wait(interval_seconds):
                try:
                    next_proxy, failures = self.rotate_proxy()
                    if failures:
                        failed_summary = "; ".join(f"{proxy} ({reason})" for proxy, reason in failures)
                        Debug.log(
                            self.debug_enabled,
                            f"Rotation switched to {next_proxy} after rejecting candidates: {failed_summary}",
                        )
                except SystemExit as exc:
                    Debug.log(self.debug_enabled, f"Proxy rotation skipped because no working replacement proxy was found: {exc}")

        self._rotation_thread = threading.Thread(target=run_rotation_loop, name="proxy-rotation", daemon=True)
        self._rotation_thread.start()
        Debug.log(
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
        Debug.log(
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
            if cached_session is not None and cached_session.proxy_url != self.proxy and cached_session.in_use == 0:
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
            if cached_session.proxy_url != self.proxy and cached_session.in_use == 0:
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
            return candidate if self.is_http_url(candidate) else None

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
        return upstream_url if self.is_http_url(upstream_url) else None

    def resolve_upstream_url(self, handler: "ProxyTunnelHandler") -> tuple[Optional[str], bool]:
        parsed = urllib.parse.urlsplit(handler.path)
        query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        Debug.log(
            self.debug_enabled,
            f"Resolving request method={handler.command} path={handler.path!r} referer={handler.headers.get('Referer')!r} origin={handler.headers.get('Origin')!r}",
        )

        if parsed.path == "/" and query.get("url"):
            # The entry URL is only a bootstrap format; normal browsing should move onto canonical /proxy/... paths.
            candidate = query["url"][0].strip()
            if self.is_http_url(candidate):
                Debug.log(self.debug_enabled, f"Resolved target from query parameter: {candidate}")
                return candidate, True
            Debug.log(self.debug_enabled, f"Rejected non-http query target: {candidate!r}")
            return None, False

        direct = self.local_to_upstream_url(handler.path)
        if direct:
            Debug.log(self.debug_enabled, f"Resolved canonical proxied path to upstream URL: {direct}")
            return direct, False

        referer = handler.headers.get("Referer")
        if referer:
            upstream_referer = self.local_to_upstream_url(referer)
            if upstream_referer:
                # Relative asset requests rely on the last proxied page to recover their upstream base URL.
                relative_request = urllib.parse.urlunsplit(("", "", parsed.path or "/", parsed.query, ""))
                resolved = urllib.parse.urljoin(upstream_referer, relative_request)
                Debug.log(self.debug_enabled, f"Resolved request relative to Referer: {resolved}")
                return resolved, False

        origin = handler.headers.get("Origin")
        if origin:
            upstream_origin = self.local_to_upstream_url(origin)
            if upstream_origin:
                relative_request = urllib.parse.urlunsplit(("", "", parsed.path or "/", parsed.query, ""))
                resolved = urllib.parse.urljoin(upstream_origin, relative_request)
                Debug.log(self.debug_enabled, f"Resolved request relative to Origin: {resolved}")
                return resolved, False

        Debug.log(self.debug_enabled, "Unable to resolve request to an upstream URL")
        return None, False

    def rewrite_embedded_url(self, value: str, base_url: str, local_origin: Optional[str] = None) -> str:
        candidate = html.unescape(value.strip())
        if not candidate or candidate.startswith(("#", "data:", "javascript:", "mailto:", "tel:", "about:")):
            return value

        # Preserve absolute form only when the source was already absolute; relative links stay local-path based.
        preserve_absolute = False
        if candidate.startswith("//"):
            absolute = urllib.parse.urlsplit(base_url).scheme + ":" + candidate
        elif self.is_http_url(candidate):
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
            Debug.log(self.debug_enabled, f"Rewriting HTML response for {base_url} content_type={content_type}")
            return self.rewrite_html(text, base_url, local_origin)
        if any(content_type_name in lowered for content_type_name in CSS_CONTENT_TYPES):
            Debug.log(self.debug_enabled, f"Rewriting CSS response for {base_url} content_type={content_type}")
            return self.rewrite_css_urls(text, base_url, local_origin)
        if any(content_type_name in lowered for content_type_name in FEED_XML_CONTENT_TYPES) and self.looks_like_feed_xml(text):
            Debug.log(self.debug_enabled, f"Rewriting feed XML response for {base_url} content_type={content_type}")
            return self.rewrite_feed_xml(text, base_url, local_origin)
        Debug.log(self.debug_enabled, f"Leaving response body unchanged for {base_url} content_type={content_type}")
        return text

    def rewrite_header_url(self, value: str, base_url: str, local_origin: Optional[str] = None) -> str:
        candidate = value.strip()
        if not candidate:
            return value
        preserve_absolute = self.is_http_url(candidate)
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


class ProxyTunnelHandler(http.server.BaseHTTPRequestHandler):
    server_version = "socks5_http_tunneler/1.0"

    DOWNSTREAM_DISCONNECT_EXCEPTIONS = (
        BrokenPipeError,
        ConnectionAbortedError,
        ConnectionResetError,
    )

    @property
    def app(self) -> ProxyApplication:
        return self.server.app  # type: ignore[attr-defined]

    @staticmethod
    def _decode_text(payload: bytes, content_type: str, response: requests.Response) -> tuple[str, str]:
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

    @staticmethod
    def _truncate_for_debug(value: str, limit: int = MAX_DEBUG_BODY_PREVIEW) -> str:
        compact = re.sub(r"\s+", " ", value)
        if len(compact) <= limit:
            return compact
        return compact[:limit] + "..."

    def try_send_error(self, code: int, message: str) -> bool:
        try:
            self.send_error(code, message)
            return True
        except self.DOWNSTREAM_DISCONNECT_EXCEPTIONS as exc:
            Debug.log(
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
            Debug.log(
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

        Debug.log(
            self.app.debug_enabled,
            f"Handling client request method={self.command} target={target_url} redirect_to_canonical={should_redirect}",
        )

        if should_redirect and self.command in {"GET", "HEAD"}:
            location = self.app.build_local_proxy_path(target_url)
            Debug.log(self.app.debug_enabled, f"Redirecting client to canonical local path: {location}")
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
            Debug.log(
                self.app.debug_enabled,
                f"Forwarding upstream request method={self.command} url={target_url} body_bytes={len(request_body)} header_count={len(upstream_headers)}",
            )
            if self.app.debug_enabled:
                Debug.log(self.app.debug_enabled, f"Upstream request headers: {upstream_headers}")

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
                Debug.log(self.app.debug_enabled, f"Upstream TLS verification failed: {exc!r}")
                self.try_send_error(
                    502,
                    "Upstream TLS certificate verification failed. Supply --ca-bundle <pem> for a trusted private CA, or use --insecure if you trust the target path or proxy.",
                )
                return
            except requests.RequestException as exc:
                Debug.log(self.app.debug_enabled, f"Upstream request failed: {exc!r}")
                self.try_send_error(502, f"Upstream proxy error: {exc}")
                return

            Debug.log(
                self.app.debug_enabled,
                f"Received upstream response status={upstream_response.status_code} reason={upstream_response.reason!r} final_url={upstream_response.url} content_type={upstream_response.headers.get('Content-Type', '')!r} body_bytes={len(upstream_response.content)}",
            )

            payload, content_type, encoding = self.prepare_response_payload(upstream_response, target_url)
            Debug.log(
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
        text, encoding = self._decode_text(payload, content_type, response)
        rewritten_text = self.app.rewrite_text_response(text, content_type, base_url, self.get_local_origin())
        if rewritten_text is text:
            if self.app.debug_enabled and payload:
                Debug.log(self.app.debug_enabled, f"Response preview: {self._truncate_for_debug(text)}")
            return payload, content_type, encoding
        if self.app.debug_enabled:
            Debug.log(self.app.debug_enabled, f"Rewritten response preview: {self._truncate_for_debug(rewritten_text)}")
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
            Debug.log(
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

    def __init__(self, server_address: tuple[str, int], handler_class: type[ProxyTunnelHandler], app: ProxyApplication) -> None:
        super().__init__(server_address, handler_class)
        self.app = app


def main() -> None:
    parser = ProxyApplication.build_argument_parser()
    args = parser.parse_args()
    if args.proxy and args.rotation_interval is not None:
        raise SystemExit("--rotation-interval can only be used with --proxy-file")

    verify_tls: bool | str = True
    if args.insecure:
        verify_tls = False
    elif args.ca_bundle:
        ca_bundle_path = Path(args.ca_bundle).expanduser().resolve()
        if not ca_bundle_path.is_file():
            raise SystemExit(f"CA bundle file not found: {ca_bundle_path}")
        verify_tls = str(ca_bundle_path)

    proxy_pool: Optional[ProxyPool] = None
    rotation_interval_seconds: Optional[float] = None
    failed_proxies: list[tuple[str, str]] = []
    if args.proxy_file:
        proxy_pool = ProxyPool.load(args.proxy_file, DEFAULT_PROXY_BLACKLIST_FILE, DEFAULT_PROXY_WHITELIST_FILE)
        selected_proxy, failed_proxies = proxy_pool.choose_live(args.timeout, args.debug)
        rotation_interval_seconds = (
            DEFAULT_PROXY_ROTATION_SECONDS if args.rotation_interval is None else args.rotation_interval
        )
    else:
        selected_proxy = args.proxy

    app = ProxyApplication(
        selected_proxy,
        args.timeout,
        debug_enabled=args.debug,
        verify_tls=verify_tls,
        proxy_pool=proxy_pool,
        rotation_interval_seconds=rotation_interval_seconds,
        client_idle_timeout_seconds=args.client_idle_timeout,
    )

    if not args.debug and verify_tls is False:
        warnings.filterwarnings("ignore", category=InsecureRequestWarning)

    with ThreadedHTTPServer((args.host, args.port), ProxyTunnelHandler, app) as server:
        app.start_proxy_rotation()
        app.start_idle_session_reaper()
        print(f"Serving proxy HTTP tunneler on http://{args.host}:{args.port}")
        print(f"Upstream proxy: {app.proxy}")
        print(f"Client idle timeout: {RotationInterval.format_optional(app.client_idle_timeout_seconds)}")
        if proxy_pool is not None:
            print(f"Proxy list: {proxy_pool.file_path}")
            print(f"Proxy blacklist: {proxy_pool.blacklist_file_path}")
            print(f"Proxy whitelist: {proxy_pool.whitelist_file_path}")
            if failed_proxies:
                print("Proxies that failed startup probing:")
                for failed_proxy, reason in failed_proxies:
                    print(f"  - {failed_proxy} ({reason})")
            print(f"Chosen proxy for this session: {app.proxy}")
            print(f"Proxy rotation interval: {RotationInterval.format_optional(app.rotation_interval_seconds)}")
        print(f"Open: http://{args.host}:{args.port}/?url=http://google.com")
        if args.insecure:
            print("Upstream TLS verification disabled")
        elif args.ca_bundle:
            print(f"Using upstream CA bundle: {app.verify_tls}")
        if args.debug:
            print("Debug logging enabled")
            Debug.log(
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
