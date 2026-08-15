"""
Tests that _parse_download_request - the function shared by /download and
/download/start - rejects non-YouTube-video URLs.

Deliberately unit-tests _parse_download_request directly rather than going
through the Flask test client: a URL that passes validation would then
flow into _run_download_deduped -> _process_download -> a real
ytdl_core.fetch_info() network call if driven through the live route, and
this suite otherwise never touches the real network. _parse_download_request
is exactly the single choke point both routes call, so testing it directly
is both sufficient and safe - no need for a client/mod fixture pair here at
all, since nothing Flask-request-specific is being exercised.
"""

import importlib
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

MODULE_NAME = "youtube-video-downloader.youtube-video-downloader-flask-ws"


def _mod():
    m = importlib.import_module(MODULE_NAME)
    m.FFMPEG_PATH = "C:/fake/ffmpeg.exe"
    return m


def test_non_youtube_urls_are_rejected():
    m = _mod()
    non_youtube_urls = [
        "https://evil.example.com/ssrf",
        "http://169.254.169.254/latest/meta-data/",  # cloud metadata endpoint shape
        "https://vimeo.com/12345",
        "https://not-youtube.com/watch?v=abc123",
    ]
    with m.app.app_context():
        for url in non_youtube_urls:
            params, error_response = m._parse_download_request({"url": url})
            assert params is None, f"{url} should have been rejected"
            response, status_code = error_response
            assert status_code == 400
            assert "youtube" in response.get_json()["error"].lower()


def test_real_youtube_watch_url_passes_validation():
    m = _mod()
    with m.app.app_context():
        params, error_response = m._parse_download_request(
            {"url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ"}
        )
    assert error_response is None
    assert params["url"] == "https://www.youtube.com/watch?v=dQw4w9WgXcQ"


def test_youtu_be_short_url_passes_validation():
    m = _mod()
    with m.app.app_context():
        params, error_response = m._parse_download_request({"url": "https://youtu.be/dQw4w9WgXcQ"})
    assert error_response is None
    assert params["url"] == "https://youtu.be/dQw4w9WgXcQ"
