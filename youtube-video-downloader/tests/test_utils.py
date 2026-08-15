r"""
Tests for ytdl_helper.utils.sanitize_filename - specifically that it can
never produce a string containing a path separator, since its output is
joined onto output_dir as a single filename component (ytdl_helper/core.py
uses it for item.final_filepath). A regression here is a path-traversal /
arbitrary-file-write vulnerability: a crafted video title that survives
sanitization with a leading `\` or `/` reaches shutil.move()/.unlink() at
an attacker-chosen path outside the intended output directory.
"""

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from ytdl_helper.utils import sanitize_filename


def test_sanitize_filename_strips_backslash():
    result = sanitize_filename('evil\\..\\..\\pwned')
    assert '\\' not in result


def test_sanitize_filename_strips_forward_slash():
    result = sanitize_filename('evil/../../pwned')
    assert '/' not in result


def test_sanitize_filename_never_contains_a_path_separator():
    malicious_titles = [
        '\\Windows\\System32\\evilmarker',
        '/etc/passwd',
        '..\\..\\..\\Users\\victim\\AppData\\evil',
        '../../../etc/passwd',
        '\\\\network-share\\evil',
        'C:\\absolute\\path\\attempt',
        '\\',
        '/',
        'normal - title (should be untouched)',
    ]
    for title in malicious_titles:
        result = sanitize_filename(title)
        assert '\\' not in result, f"backslash survived sanitizing {title!r}: {result!r}"
        assert '/' not in result, f"forward slash survived sanitizing {title!r}: {result!r}"


def test_sanitize_filename_result_cannot_escape_output_dir():
    """Integration-style proof the exploit is closed: joining the
    sanitized name onto a base directory must always stay under it."""
    base = pathlib.Path('C:/fakebase/.temp').resolve()
    malicious_titles = [
        '\\Windows\\System32\\evilmarker',
        '..\\..\\..\\Users\\victim\\Desktop\\evil',
        '\\\\network-share\\evil',
    ]
    for title in malicious_titles:
        candidate = (base / (sanitize_filename(title) + '.mp4')).resolve()
        assert candidate.is_relative_to(base), (
            f"sanitize_filename({title!r}) -> {sanitize_filename(title)!r} "
            f"escaped {base} to {candidate}"
        )


def test_sanitize_filename_still_handles_reserved_windows_names():
    assert sanitize_filename('CON') == '_CON_'
    assert sanitize_filename('con') == '_con_'
    assert sanitize_filename('LPT1') == '_LPT1_'


def test_sanitize_filename_still_strips_other_invalid_characters():
    result = sanitize_filename('title: with "quotes" <and> *stars*?')
    for ch in '<>:"|?*':
        assert ch not in result


def test_sanitize_filename_empty_and_whitespace_only():
    assert sanitize_filename('') == '_untitled_'
    assert sanitize_filename('   ') == '_sanitized_'
    assert sanitize_filename('...') == '_sanitized_'


def test_sanitize_filename_normal_title_is_readable():
    result = sanitize_filename('Artist Name - Song Title (Official Video)')
    assert result == 'Artist Name - Song Title (Official Video)'
