import pytest

from socks5_http_tunneler import ProxyApplication


BASE_URL = "https://example.com/list/index.html"
MAGNET = (
    "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567"
    "&dn=Example%20File&tr=https%3A%2F%2Ftracker.example.com%2Fannounce"
    "&tr=udp%3A%2F%2Ftracker.example.org%3A1337%2Fannounce"
)


@pytest.fixture
def app():
    return ProxyApplication(proxy="socks5h://127.0.0.1:1080", timeout=5)


@pytest.mark.parametrize(
    "value",
    [
        MAGNET,
        MAGNET.replace("&", "&amp;"),
        MAGNET.replace("magnet:", "MAGNET:"),
        "mailto:user@example.com",
        "data:text/plain,example",
        "javascript:void(0)",
        "tel:+123456789",
        "about:blank",
        "custom:resource",
    ],
)
def test_non_http_urls_remain_unchanged(app, value):
    assert app.rewrite_embedded_url(value, BASE_URL) == value
    assert app.rewrite_header_url(value, BASE_URL) == value


def test_html_preserves_magnet_text_and_escaped_href(app):
    anchor = f'<a href="{MAGNET.replace("&", "&amp;")}"></a>'
    body = f"<body>{MAGNET}{anchor}</body>"

    rewritten = app.rewrite_text_response(body, "text/html", BASE_URL)

    assert rewritten.endswith(f"{MAGNET}{anchor}</body>")
    assert "/proxy/magnet/" not in rewritten


def test_feed_preserves_magnet_link_and_attribute(app):
    escaped = MAGNET.replace("&", "&amp;")
    feed = f'<rss><channel><item><link>{escaped}</link><enclosure url="{escaped}"/></item></channel></rss>'

    assert app.rewrite_text_response(feed, "application/rss+xml", BASE_URL) == feed


def test_refresh_header_preserves_magnet(app):
    value = f"0; url={MAGNET}"
    assert app.rewrite_refresh_header(value, BASE_URL) == value


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("../file", "/proxy/https/example.com/file"),
        ("file", "/proxy/https/example.com/list/file"),
        ("/file", "/proxy/https/example.com/file"),
        ("?page=2", "/proxy/https/example.com/list/index.html?page=2"),
        ("//cdn.example.com/file", "/proxy/https/cdn.example.com/file"),
        ("https://example.org/file", "http://localhost:8080/proxy/https/example.org/file"),
        ("http://example.org/file", "http://localhost:8080/proxy/http/example.org/file"),
    ],
)
def test_http_links_still_use_proxy(app, value, expected):
    assert app.rewrite_embedded_url(value, BASE_URL, "http://localhost:8080") == expected
    assert app.rewrite_header_url(value, BASE_URL, "http://localhost:8080") == expected


@pytest.mark.parametrize("value", ["", "#section"])
def test_empty_links_and_fragments_remain_unchanged(app, value):
    assert app.rewrite_embedded_url(value, BASE_URL) == value


@pytest.mark.parametrize(
    ("css", "expected"),
    [
        ("a{background:url(../image.png)}", "a{background:url(/proxy/https/example.com/image.png)}"),
        ("a{background:url('/image.png')}", "a{background:url('/proxy/https/example.com/image.png')}"),
        ('@font-face{src:url("//cdn.example.org/font.woff2")}', '@font-face{src:url("/proxy/https/cdn.example.org/font.woff2")}'),
        (r"a{background:url(image\(1\).png)}", "a{background:url(/proxy/https/example.com/list/image%281%29.png)}"),
        (r'a{background:url("my\20 image.png")}', 'a{background:url("/proxy/https/example.com/list/my%20image.png")}'),
        (r'a{background:url(my\20 image.png)}', 'a{background:url(/proxy/https/example.com/list/my%20image.png)}'),
        ("a{mask:url(  #mask  )}", "a{mask:url(  #mask  )}"),
        ('a{background:url( "data:image/svg+xml,<svg>(test)</svg>" )}', 'a{background:url( "data:image/svg+xml,<svg>(test)</svg>" )}'),
    ],
)
def test_stylesheet_url_rewriting(app, css, expected):
    assert app.rewrite_text_response(css, "text/css; charset=utf-8", BASE_URL) == expected


def test_inline_css_url_rewriting(app):
    body = '<body><style>a{background:url(/image.png)}</style><a style="background:url(../other.png)"></a></body>'
    expected = '<style>a{background:url(/proxy/https/example.com/image.png)}</style><a style="background:url(/proxy/https/example.com/other.png)"></a></body>'
    assert app.rewrite_text_response(body, "text/html", BASE_URL).endswith(expected)


@pytest.mark.parametrize(
    ("font", "suffix", "expected_suffix", "font_format"),
    [
        ("fontawesome-webfont", ".eot?v=4.7.0", ".eot?v=4.7.0", "embedded-opentype"),
        ("fontawesome-webfont", ".eot?#iefix&v=4.7.0", ".eot#iefix&v=4.7.0", "embedded-opentype"),
        ("fontawesome-webfont", ".woff2?v=4.7.0", ".woff2?v=4.7.0", "woff2"),
        ("fontawesome-webfont", ".woff?v=4.7.0", ".woff?v=4.7.0", "woff"),
        ("fontawesome-webfont", ".ttf?v=4.7.0", ".ttf?v=4.7.0", "truetype"),
        ("fontawesome-webfont", ".svg?v=4.7.0#fontawesomeregular", ".svg?v=4.7.0#fontawesomeregular", "svg"),
        ("glyphicons-halflings-regular", ".woff2", ".woff2", "woff2"),
    ],
)
def test_font_face_urls_resolve_relative_to_stylesheet(app, font, suffix, expected_suffix, font_format):
    css = f"@font-face{{font-family:'Example Icons';src:url('../fonts/{font}{suffix}') format('{font_format}')}}"
    expected = f"@font-face{{font-family:'Example Icons';src:url('/proxy/https/example.com/assets/fonts/{font}{expected_suffix}') format('{font_format}')}}"

    rewritten = app.rewrite_text_response(
        css, "text/css", "https://example.com/assets/css/icons.min.css"
    )

    assert rewritten == expected


@pytest.mark.parametrize(
    "attributes",
    [
        'rel="stylesheet" href="https://cdn.example.com/icons.css" integrity="sha256-original"',
        "INTEGRITY='sha256-original' HREF='/icons.css' REL='alternate stylesheet'",
        'rel=stylesheet href="/icons.css" integrity=sha256-original',
        'rel="preload" as="style" href="/icons.css" integrity="sha256-original"',
    ],
)
def test_rewritten_stylesheet_links_drop_obsolete_integrity(app, attributes):
    page = f'<body><link {attributes} crossorigin="anonymous"></body>'
    rewritten = app.rewrite_html(page, BASE_URL)
    assert "sha256-original" not in rewritten
    assert "/proxy/https/" in rewritten
    assert 'crossorigin="anonymous"' in rewritten


@pytest.mark.parametrize(
    "element",
    [
        '<script src="/app.js" integrity="sha256-keep"></script>',
        '<link rel="preload" as="font" href="/font.woff2" integrity="sha256-keep">',
        '<link rel="stylesheet" href="data:text/css,body{}" integrity="sha256-keep">',
        '<link rel=stylesheet href=https://cdn.example.com/icons.css integrity="sha256-keep">',
    ],
)
def test_unmodified_resource_types_keep_integrity(app, element):
    assert 'integrity="sha256-keep"' in app.rewrite_html(element, BASE_URL)
