"""Shared fixtures for the youtube-video-downloader tests."""

import pathlib
import shutil
import subprocess

import pytest

SONG_SECONDS = 35
GAP_SECONDS = 0.5


@pytest.fixture
def make_audio():
    """
    Returns make(path, layout, sample_rate=22050) -> path, which renders a
    mono test file (format from path's extension). layout is a string read
    left to right: each letter is a SONG_SECONDS "song" - pink noise seeded
    by the letter, so "A" always sounds the same and never like "B" - and
    each "_" is a GAP_SECONDS digital-silence gap like the ones between
    songs in real mixes. E.g. "A_B_A_B" is two songs looped twice; "A_BA"
    has no gap between B and the second A. Skips the test if ffmpeg isn't
    on PATH.
    """
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        pytest.skip("ffmpeg not on PATH")

    def make(path: pathlib.Path, layout: str, sample_rate: int = 22050) -> pathlib.Path:
        parts, labels = [], []
        for i, symbol in enumerate(layout):
            if symbol == "_":
                source = f"anullsrc=r={sample_rate}:cl=mono,atrim=duration={GAP_SECONDS}"
            else:
                seed = ord(symbol.upper()) - ord("A") + 1
                source = f"anoisesrc=d={SONG_SECONDS}:c=pink:seed={seed}:a=0.5:r={sample_rate}"
            parts.append(f"{source},aformat=sample_fmts=s16:channel_layouts=mono[s{i}]")
            labels.append(f"[s{i}]")
        graph = ";".join(parts) + ";" + "".join(labels) + f"concat=n={len(layout)}:v=0:a=1[out]"
        subprocess.run(
            [ffmpeg, "-v", "error", "-y", "-filter_complex", graph, "-map", "[out]", str(path)],
            check=True,
        )
        return path

    return make
