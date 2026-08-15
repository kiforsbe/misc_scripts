"""
Tests for eagerly loading the music style ("genre") classifier's ML model
at startup instead of lazily on the first download that needs it.

Background: ytdl_helper.ffmpeg_genre_pp.FFmpegGenrePP calls
music_style_classifier.get_music_genre() during post-processing, which
lazily initializes a Hugging Face transformers pipeline (a real model
download/load, taking anywhere from seconds to minutes on first run) the
very first time it's needed. Left lazy, that cost silently lands on
whichever download happens to be first - these tests cover the three
layers that now warm it up proactively instead: music_style_classifier's
own warm_up(), ytdl_helper.ffmpeg_genre_pp.warm_up_genre_classifier(), and
ytdl_helper.core.warm_up_genre_classifier_if_enabled() (gated on
ENABLE_CUSTOM_GENRE_PP) - plus that the Flask service actually calls the
latter at startup, in a background thread so server boot isn't blocked by
a slow model load.

None of these tests trigger a real model download: _init_pipeline /
warm_up_genre_classifier are monkeypatched throughout.
"""

import importlib
import pathlib
import sys
import threading
import time

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))

import music_style_classifier
from ytdl_helper import core as ytdl_core
from ytdl_helper import ffmpeg_genre_pp


# --- music_style_classifier.warm_up() ---


def test_warm_up_returns_true_when_pipeline_initializes(monkeypatch):
    monkeypatch.setattr(music_style_classifier, "_pipeline", None)
    monkeypatch.setattr(music_style_classifier, "_init_pipeline", lambda: object())

    assert music_style_classifier.warm_up() is True


def test_warm_up_returns_false_when_pipeline_fails_to_initialize(monkeypatch):
    monkeypatch.setattr(music_style_classifier, "_pipeline", None)
    monkeypatch.setattr(music_style_classifier, "_init_pipeline", lambda: None)

    assert music_style_classifier.warm_up() is False


def test_init_pipeline_only_loads_once_across_concurrent_callers(monkeypatch):
    """The warm-up thread and a real classification request could call
    _init_pipeline at roughly the same time; it must not load the
    (expensive) model twice."""
    monkeypatch.setattr(music_style_classifier, "_pipeline", None)
    load_count = {"n": 0}
    start_barrier = threading.Barrier(5)

    def fake_pipeline(*args, **kwargs):
        load_count["n"] += 1
        time.sleep(0.05)  # hold the lock a bit to widen the race window
        return object()

    monkeypatch.setattr(music_style_classifier, "pipeline", fake_pipeline)
    monkeypatch.setattr(music_style_classifier, "_get_device", lambda: -1)

    def worker():
        start_barrier.wait(timeout=2)  # release all 5 threads at once
        music_style_classifier._init_pipeline()

    threads = [threading.Thread(target=worker) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=3)

    assert load_count["n"] == 1, f"expected the model to load exactly once, loaded {load_count['n']} times"


# --- ytdl_helper.ffmpeg_genre_pp ---


def test_is_classifier_available_reflects_classifier_found_flag(monkeypatch):
    monkeypatch.setattr(ffmpeg_genre_pp, "classifier_found", True)
    assert ffmpeg_genre_pp.is_classifier_available() is True

    monkeypatch.setattr(ffmpeg_genre_pp, "classifier_found", False)
    assert ffmpeg_genre_pp.is_classifier_available() is False


def test_warm_up_genre_classifier_noop_when_classifier_unavailable(monkeypatch):
    monkeypatch.setattr(ffmpeg_genre_pp, "classifier_found", False)
    monkeypatch.setattr(music_style_classifier, "warm_up", lambda: pytest.fail("must not be called"))

    assert ffmpeg_genre_pp.warm_up_genre_classifier() is False


def test_warm_up_genre_classifier_delegates_when_available(monkeypatch):
    monkeypatch.setattr(ffmpeg_genre_pp, "classifier_found", True)
    monkeypatch.setattr(music_style_classifier, "warm_up", lambda: True)

    assert ffmpeg_genre_pp.warm_up_genre_classifier() is True


def test_warm_up_genre_classifier_returns_false_on_exception(monkeypatch):
    monkeypatch.setattr(ffmpeg_genre_pp, "classifier_found", True)

    def boom():
        raise RuntimeError("model server unreachable")

    monkeypatch.setattr(music_style_classifier, "warm_up", boom)

    assert ffmpeg_genre_pp.warm_up_genre_classifier() is False


# --- ytdl_helper.core.warm_up_genre_classifier_if_enabled() ---


def test_core_warmup_noop_when_genre_recognition_disabled(monkeypatch):
    monkeypatch.setattr(ytdl_core, "ENABLE_CUSTOM_GENRE_PP", False)
    monkeypatch.setattr(
        ytdl_core, "warm_up_genre_classifier", lambda: pytest.fail("must not be called")
    )

    assert ytdl_core.warm_up_genre_classifier_if_enabled() is False


def test_core_warmup_delegates_when_genre_recognition_enabled(monkeypatch):
    monkeypatch.setattr(ytdl_core, "ENABLE_CUSTOM_GENRE_PP", True)
    monkeypatch.setattr(ytdl_core, "warm_up_genre_classifier", lambda: True)

    assert ytdl_core.warm_up_genre_classifier_if_enabled() is True


def test_full_chain_core_to_classifier_warms_up_without_real_model_load(monkeypatch):
    """
    Exercises core.warm_up_genre_classifier_if_enabled() ->
    ffmpeg_genre_pp.warm_up_genre_classifier() ->
    music_style_classifier.warm_up() as one real call chain (nothing in
    the middle is mocked), with only the deepest layer
    (_init_pipeline/pipeline construction) faked out - proving the three
    modules are actually wired to each other, not just individually
    correct in isolation.
    """
    monkeypatch.setattr(ytdl_core, "ENABLE_CUSTOM_GENRE_PP", True)
    monkeypatch.setattr(ffmpeg_genre_pp, "classifier_found", True)
    monkeypatch.setattr(music_style_classifier, "_pipeline", None)
    monkeypatch.setattr(music_style_classifier, "pipeline", lambda *a, **kw: object())
    monkeypatch.setattr(music_style_classifier, "_get_device", lambda: -1)

    assert ytdl_core.warm_up_genre_classifier_if_enabled() is True
    assert music_style_classifier._pipeline is not None


# --- Flask service startup wiring ---

MODULE_NAME = "youtube-video-downloader.youtube-video-downloader-flask-ws"


@pytest.fixture()
def mod():
    m = importlib.import_module(MODULE_NAME)
    original_enable_genre = m.ytdl_core.ENABLE_CUSTOM_GENRE_PP
    original_warm_up = m.ytdl_core.warm_up_genre_classifier_if_enabled
    yield m
    m.ytdl_core.ENABLE_CUSTOM_GENRE_PP = original_enable_genre
    m.ytdl_core.warm_up_genre_classifier_if_enabled = original_warm_up


def test_maybe_warm_up_starts_a_background_thread_when_enabled(mod):
    called = threading.Event()
    mod.ytdl_core.ENABLE_CUSTOM_GENRE_PP = True
    mod.ytdl_core.warm_up_genre_classifier_if_enabled = called.set

    thread = mod._maybe_warm_up_genre_classifier()

    assert thread is not None
    assert thread.daemon, "warm-up thread must not block process exit"
    thread.join(timeout=2)
    assert called.is_set()


def test_maybe_warm_up_is_a_noop_when_genre_recognition_disabled(mod):
    was_called = threading.Event()
    mod.ytdl_core.ENABLE_CUSTOM_GENRE_PP = False
    mod.ytdl_core.warm_up_genre_classifier_if_enabled = was_called.set

    thread = mod._maybe_warm_up_genre_classifier()

    assert thread is None
    assert not was_called.is_set()
