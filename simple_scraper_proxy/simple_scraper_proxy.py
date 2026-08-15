import argparse
import email.utils
import http.server
import re
import socketserver
import sys
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import TYPE_CHECKING, cast

from bs4 import BeautifulSoup
import yaml

if TYPE_CHECKING:
    from _typeshed import ReadableBuffer


CDATA_PLACEHOLDER_PREFIX = "__CDATA_PLACEHOLDER_"


class TemplateError(ValueError):
    pass


class UpstreamFetchError(RuntimeError):
    pass


def debug_log(enabled: bool, message: str) -> None:
    if enabled:
        sys.stderr.write(f"[DEBUG] {message}\n")


def sanitize_template_name(template_name: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_-]+", template_name):
        raise TemplateError("Invalid template name")
    return template_name


def load_template(template_dir: Path, template_name: str) -> dict:
    safe_name = sanitize_template_name(template_name)
    template_path = (template_dir / f"{safe_name}.yaml").resolve()
    # Keep template lookup constrained to the configured local directory.
    if template_dir.resolve() not in template_path.parents:
        raise TemplateError("Template path escapes template directory")
    if not template_path.is_file():
        raise TemplateError(f"Template not found: {safe_name}")
    with template_path.open("r", encoding="utf-8") as handle:
        template = yaml.safe_load(handle) or {}
    if not isinstance(template, dict):
        raise TemplateError("Template must be a mapping")
    if not isinstance(template.get("channel"), dict):
        raise TemplateError("Template is missing a channel mapping")
    if not isinstance(template.get("items"), dict):
        raise TemplateError("Template is missing an items mapping")
    namespaces = template.get("namespaces", {})
    if not isinstance(namespaces, dict):
        raise TemplateError("Template namespaces must be a mapping")
    for prefix, uri in namespaces.items():
        if not isinstance(prefix, str) or not isinstance(uri, str) or not prefix or not uri:
            raise TemplateError("Template namespaces must map prefixes to URIs")
    if not template["items"].get("selector"):
        raise TemplateError("Template items.selector is required")
    if not isinstance(template["items"].get("fields"), dict):
        raise TemplateError("Template items.fields must be a mapping")
    for section_name in ("channel", "items"):
        namespaced = template[section_name].get("namespaced", {})
        if namespaced and not isinstance(namespaced, dict):
            raise TemplateError(f"Template {section_name}.namespaced must be a mapping")
    return template


def decode_body(body: bytes, content_type: str) -> str:
    match = re.search(r"charset=([^;]+)", content_type or "", re.IGNORECASE)
    charset = match.group(1).strip().strip('"') if match else "utf-8"
    try:
        return body.decode(charset, errors="replace")
    except LookupError:
        return body.decode("utf-8", errors="replace")


def fetch_remote_html(remote_url: str, debug: bool = False) -> tuple[str, str, str]:
    debug_log(debug, f"Fetching upstream URL: {remote_url}")
    request = urllib.request.Request(
        remote_url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/135.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Referer": remote_url,
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            final_url = response.geturl()
            status_code = getattr(response, "status", "unknown")
            content_type = response.headers.get("Content-Type", "")
            body = response.read()
            debug_log(debug, f"Upstream response status={status_code} final_url={final_url} content_type={content_type}")
    except urllib.error.HTTPError as exc:
        response_body = b""
        try:
            response_body = exc.read()
        except Exception:
            response_body = b""

        snippet = response_body.decode("utf-8", errors="ignore")[:200].replace("\n", " ")
        debug_log(debug, f"Upstream HTTPError status={exc.code} url={remote_url} body_snippet={snippet!r}")

        if exc.code == 403:
            lower_body = response_body.decode("utf-8", errors="ignore").casefold()
            if "just a moment" in lower_body or "cloudflare" in lower_body:
                raise UpstreamFetchError(
                    "Upstream HTTP error: 403. The target site returned an anti-bot challenge page."
                ) from exc
        raise UpstreamFetchError(f"Upstream HTTP error: {exc.code}") from exc
    except urllib.error.URLError as exc:
        raise UpstreamFetchError(f"Upstream fetch error: {exc.reason}") from exc
    except Exception as exc:  # pragma: no cover - defensive
        raise UpstreamFetchError(f"Upstream fetch error: {exc}") from exc

    # This proxy only supports scraping markup responses into RSS.
    if "html" not in content_type.lower() and "xml" not in content_type.lower():
        raise UpstreamFetchError(f"Unsupported upstream content type: {content_type or 'unknown'}")

    debug_log(debug, f"Decoded upstream body length={len(body)}")
    return final_url, content_type, decode_body(body, content_type)


def extract_template_name(parsed_url, query: dict[str, list[str]]) -> str:
    path_parts = [part for part in parsed_url.path.split("/") if part]
    if len(path_parts) >= 2 and path_parts[0] == "template":
        return path_parts[1].strip()
    return ""


def extract_remote_url(parsed_url, query: dict[str, list[str]]) -> str:
    raw_query = parsed_url.query
    path_parts = [part for part in parsed_url.path.split("/") if part]
    if len(path_parts) >= 2 and path_parts[0] == "template" and raw_query.startswith("url="):
        return urllib.parse.unquote(raw_query[len("url="):].strip())
    return ""


def normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def safe_cdata(value: str) -> str:
    return value.replace("]]>", "]]]]><![CDATA[>")


def apply_transform(value: str, transform_name: str) -> str:
    if transform_name == "strip":
        return value.strip()
    if transform_name == "normalize_whitespace":
        return normalize_whitespace(value)
    if transform_name == "unix_to_rfc2822_utc":
        timestamp = int(float(value))
        return email.utils.formatdate(timestamp, localtime=False, usegmt=False)
    if transform_name == "nyaa_trusted_from_class":
        classes = value if isinstance(value, list) else str(value).split()
        return "Yes" if "success" in classes else "No"
    raise TemplateError(f"Unsupported transform: {transform_name}")


def build_format_context(global_context: dict, local_context: dict, raw_value: str | None, regex_groups: dict) -> dict:
    format_context = {}
    format_context.update(global_context)
    format_context.update(local_context)
    format_context.update(regex_groups)
    if raw_value is not None:
        format_context.setdefault("value", raw_value)
        format_context.setdefault("raw", raw_value)
    return format_context


def extract_raw_value(node, spec: dict) -> str | list[str] | None:
    source_node = node if spec.get("from_root") else None
    if source_node is None and spec.get("selector"):
        source_node = node.select_one(spec["selector"])
    elif source_node is None:
        source_node = node

    if source_node is None:
        return None

    attr_name = spec.get("attr")
    if attr_name:
        if attr_name == "text":
            return source_node.get_text(" ", strip=True)
        if attr_name == "class":
            return source_node.get("class", [])
        return source_node.get(attr_name)

    return source_node.get_text(" ", strip=True)


def resolve_spec_value(node, spec, global_context: dict, local_context: dict, base_url: str) -> str:
    if isinstance(spec, str):
        spec = {"selector": spec}
    elif spec is None:
        spec = {}
    elif not isinstance(spec, dict):
        raise TemplateError("Field spec must be a mapping, string, or null")

    regex_groups: dict[str, str] = {}
    raw_value = None

    # A spec can provide a constant value or extract one from the current node.
    if "value" in spec:
        raw_value = str(spec["value"])
    elif spec.get("selector") or spec.get("from_root") or spec.get("attr"):
        raw_extracted = extract_raw_value(node, spec)
        if isinstance(raw_extracted, list):
            raw_value = " ".join(str(part) for part in raw_extracted)
        elif raw_extracted is not None:
            raw_value = str(raw_extracted)

    if raw_value is not None and spec.get("regex"):
        match = re.search(spec["regex"], raw_value)
        if not match:
            raw_value = None
        else:
            regex_groups = {key: value for key, value in match.groupdict().items() if value is not None}
            if spec.get("format") is None:
                if regex_groups:
                    first_key = next(iter(regex_groups))
                    raw_value = regex_groups[first_key]
                elif match.groups():
                    raw_value = match.group(1)
                else:
                    raw_value = match.group(0)

    transforms = spec.get("transform")
    if raw_value is not None and transforms:
        if isinstance(transforms, str):
            transforms = [transforms]
        for transform_name in transforms:
            raw_value = apply_transform(raw_value, transform_name)

    # Relative links should be resolved against the final fetched URL, not the request URL.
    if raw_value and spec.get("resolve_url"):
        raw_value = urllib.parse.urljoin(base_url, raw_value)

    if spec.get("format"):
        format_context = build_format_context(global_context, local_context, raw_value, regex_groups)
        try:
            raw_value = spec["format"].format_map(format_context)
        except KeyError as exc:
            raise TemplateError(f"Missing format field: {exc.args[0]}") from exc

    if raw_value is None:
        default_value = spec.get("default")
        if default_value is None:
            return ""
        raw_value = str(default_value)

    if spec.get("prefix"):
        raw_value = f"{spec['prefix']}{raw_value}"
    if spec.get("suffix"):
        raw_value = f"{raw_value}{spec['suffix']}"

    return str(raw_value)


def extract_channel_data(soup: BeautifulSoup, channel_spec: dict, global_context: dict, base_url: str) -> dict:
    channel_data: dict[str, str] = {}
    for field_name, spec in channel_spec.items():
        channel_data[field_name] = resolve_spec_value(soup, spec, global_context, channel_data, base_url)
    return channel_data


def extract_item_data(soup: BeautifulSoup, items_spec: dict, global_context: dict, base_url: str) -> list[dict]:
    items = []
    for item_node in soup.select(items_spec["selector"]):
        item_data: dict[str, str] = {}
        for field_name, spec in items_spec.get("fields", {}).items():
            item_data[field_name] = resolve_spec_value(item_node, spec, global_context, item_data, base_url)
        if item_data:
            items.append(item_data)
    return items


def expand_namespaced_name(name: str, namespaces: dict[str, str]) -> str:
    if ":" not in name:
        return name
    prefix, local_name = name.split(":", 1)
    namespace_uri = namespaces.get(prefix)
    if not namespace_uri:
        raise TemplateError(f"Unknown namespace prefix: {prefix}")
    return f"{{{namespace_uri}}}{local_name}"


def resolve_context_value(spec, context: dict[str, str]) -> str:
    if spec is None:
        return ""
    if isinstance(spec, str):
        try:
            return spec.format_map(context)
        except KeyError as exc:
            raise TemplateError(f"Missing format field: {exc.args[0]}") from exc
    if not isinstance(spec, dict):
        raise TemplateError("Render spec must be a mapping, string, or null")

    if "field" in spec:
        value = context.get(str(spec["field"]), "")
    elif "value" in spec:
        value = str(spec["value"])
    else:
        value = ""

    if spec.get("format"):
        format_context = dict(context)
        format_context.setdefault("value", value)
        format_context.setdefault("raw", value)
        try:
            value = spec["format"].format_map(format_context)
        except KeyError as exc:
            raise TemplateError(f"Missing format field: {exc.args[0]}") from exc

    if not value and spec.get("default") is not None:
        value = str(spec["default"])

    if spec.get("prefix"):
        value = f"{spec['prefix']}{value}"
    if spec.get("suffix"):
        value = f"{value}{spec['suffix']}"

    return value


def append_namespaced_elements(parent: ET.Element, element_specs: dict, context: dict[str, str], namespaces: dict[str, str]) -> None:
    for element_name, spec in element_specs.items():
        if isinstance(spec, dict):
            attrs_spec = spec.get("attrs", {})
            if attrs_spec and not isinstance(attrs_spec, dict):
                raise TemplateError(f"Attributes for {element_name} must be a mapping")
            attrs = {
                attr_name: resolve_context_value(attr_spec, context)
                for attr_name, attr_spec in attrs_spec.items()
            }
            text_spec = spec.get("text")
            if text_spec is None and any(key in spec for key in ("field", "value", "format", "default", "prefix", "suffix")):
                text_spec = {key: spec[key] for key in ("field", "value", "format", "default", "prefix", "suffix") if key in spec}
        else:
            attrs = {}
            text_spec = spec

        text_value = resolve_context_value(text_spec, context) if text_spec is not None else ""
        filtered_attrs = {key: value for key, value in attrs.items() if value != ""}
        if text_value == "" and not filtered_attrs:
            continue

        # Namespace prefixes are resolved from the template at render time.
        element = ET.SubElement(parent, expand_namespaced_name(element_name, namespaces), filtered_attrs)
        if text_value != "":
            element.text = text_value


def render_rss_feed(template: dict, channel_data: dict, items: list[dict]) -> str:
    namespaces = template.get("namespaces", {})
    for prefix, uri in namespaces.items():
        ET.register_namespace(prefix, uri)

    rss = ET.Element("rss", version="2.0")
    channel = ET.SubElement(rss, "channel")

    for field_name in ("title", "description", "link"):
        if channel_data.get(field_name):
            ET.SubElement(channel, field_name).text = channel_data[field_name]

    append_namespaced_elements(channel, template["channel"].get("namespaced", {}), channel_data, namespaces)

    cdata_values: dict[str, str] = {}
    placeholder_index = 0

    for item_data in items:
        item = ET.SubElement(channel, "item")
        for field_name in ("title", "link", "guid", "pubDate"):
            if item_data.get(field_name):
                element = ET.SubElement(item, field_name)
                element.text = item_data[field_name]
                if field_name == "guid":
                    element.set("isPermaLink", "true")
        if item_data.get("description"):
            placeholder = f"{CDATA_PLACEHOLDER_PREFIX}{placeholder_index}__"
            placeholder_index += 1
            # ElementTree does not preserve CDATA sections, so inject them after serialization.
            cdata_values[placeholder] = safe_cdata(item_data["description"])
            ET.SubElement(item, "description").text = placeholder

        append_namespaced_elements(item, template["items"].get("namespaced", {}), item_data, namespaces)

    ET.indent(rss, space="  ")
    xml_output = ET.tostring(rss, encoding="utf-8", xml_declaration=True).decode("utf-8")
    for placeholder, value in cdata_values.items():
        xml_output = xml_output.replace(placeholder, f"<![CDATA[{value}]]>")
    return xml_output


class ScraperProxyHandler(http.server.BaseHTTPRequestHandler):
    server_version = "simple_scraper_proxy/1.0"

    @property
    def template_dir(self) -> Path:
        return cast("ThreadedScraperProxyServer", self.server).template_dir

    @property
    def debug_enabled(self) -> bool:
        return cast("ThreadedScraperProxyServer", self.server).debug_enabled

    def do_GET(self):
        parsed_url = urllib.parse.urlsplit(self.path)
        if parsed_url.path == "/favicon.ico" or parsed_url.path.startswith("/.well-known/"):
            debug_log(self.debug_enabled, f"Ignoring probe request path={parsed_url.path}")
            self.send_response(204)
            self.end_headers()
            return

        query = urllib.parse.parse_qs(parsed_url.query, keep_blank_values=True)
        template_name = extract_template_name(parsed_url, query)
        # The proxy contract is /template/<name>?url=<target_url>.
        remote_url = extract_remote_url(parsed_url, query)
        debug_log(
            self.debug_enabled,
            f"Parsed request path={parsed_url.path!r} raw_query={parsed_url.query!r} template={template_name!r} remote_url={remote_url!r}",
        )

        if not remote_url:
            self.send_error(400, "Missing remote URL. Use /template/<name>?url=<target_url>")
            return
        if not template_name:
            self.send_error(400, "Missing template name. Use /template/<name>?url=<target_url>")
            return
        if not remote_url.startswith(("http://", "https://")):
            self.send_error(400, "Invalid remote URL")
            return

        try:
            template = load_template(self.template_dir, template_name)
            debug_log(self.debug_enabled, f"Loaded template {template_name!r} from {self.template_dir}")
            final_url, _, html_text = fetch_remote_html(remote_url, debug=self.debug_enabled)
            soup = BeautifulSoup(html_text, "html.parser")

            # Feed field formats can reference request-level values and previously extracted channel fields.
            request_url = self.build_request_url()
            global_context = {
                "request_url": request_url,
                "remote_url": remote_url,
                "final_url": final_url,
            }
            channel_data = extract_channel_data(soup, template["channel"], global_context, final_url)
            items = extract_item_data(soup, template["items"], global_context | channel_data, final_url)
            debug_log(
                self.debug_enabled,
                f"Rendered feed with channel_title={channel_data.get('title', '')!r} item_count={len(items)} final_url={final_url}",
            )
            rss_output = render_rss_feed(template, channel_data, items)
        except TemplateError as exc:
            debug_log(self.debug_enabled, f"Template error: {exc}")
            self.send_error(400, f"Template error: {exc}")
            return
        except UpstreamFetchError as exc:
            debug_log(self.debug_enabled, f"Upstream fetch error: {exc}")
            self.send_error(502, str(exc))
            return
        except Exception as exc:  # pragma: no cover - defensive
            debug_log(self.debug_enabled, f"Unhandled scraper proxy error: {exc!r}")
            self.send_error(500, f"Scraper proxy error: {exc}")
            return

        payload = rss_output.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/rss+xml; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def build_request_url(self) -> str:
        server = cast("ThreadedScraperProxyServer", self.server)
        server_address = cast("tuple[ReadableBuffer, int]", server.server_address)
        host = self.headers.get("Host") or f"{server_address[0]}:{server_address[1]}"
        return f"http://{host}{self.path}"

    def log_message(self, format, *args):
        sys.stderr.write(
            "%s - - [%s] %s\n"
            % (self.client_address[0], self.log_date_time_string(), format % args)
        )


class ThreadedScraperProxyServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, server_address, handler_class, template_dir: Path, debug_enabled: bool):
        super().__init__(server_address, handler_class)
        self.template_dir = template_dir
        self.debug_enabled = debug_enabled


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Scrape an upstream HTML page with a local YAML template and expose the result as RSS. "
            "Usage: http://<host>:<port>/template/<template_name>?url=<remote_url>"
        )
    )
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8081, help="Port to listen on (default: 8081)")
    parser.add_argument(
        "--template-dir",
        default=str(Path(__file__).with_name("simple_scraper_proxy_templates")),
        help="Directory containing YAML scraping templates",
    )
    parser.add_argument("--debug", action="store_true", help="Print request parsing and upstream fetch debug logs")
    return parser


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()
    template_dir = Path(args.template_dir).resolve()
    template_dir.mkdir(parents=True, exist_ok=True)

    with ThreadedScraperProxyServer((args.host, args.port), ScraperProxyHandler, template_dir, args.debug) as httpd:
        print(f"Serving scraper proxy on {args.host}:{args.port}")
        print(f"Template directory: {template_dir}")
        if args.debug:
            print("Debug logging enabled")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nShutting down scraper proxy.")
            httpd.shutdown()
        finally:
            httpd.server_close()


if __name__ == "__main__":
    main()