"""
Tests for ytdl_helper.progress_sink.ProgressSink - the fan-out object every
download entry point in youtube-video-downloader-flask-ws.py uses to report
progress, so that adding a new subscriber (e.g. a console display) can't be
silently forgotten by one entry point the way it previously was for the
/download/start job path (see test_flask_ws_progress.py for the regression
test covering that specific bug).
"""

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from ytdl_helper.progress_sink import ProgressSink


def test_update_calls_every_subscriber_with_the_same_args():
    calls_a = []
    calls_b = []
    sink = ProgressSink([
        lambda percent, message=None: calls_a.append((percent, message)),
        lambda percent, message=None: calls_b.append((percent, message)),
    ])

    sink.update(42, "halfway")

    assert calls_a == [(42, "halfway")]
    assert calls_b == [(42, "halfway")]


def test_call_is_equivalent_to_update():
    calls = []
    sink = ProgressSink([lambda percent, message=None: calls.append((percent, message))])

    sink(10)
    sink(20, "twenty")

    assert calls == [(10, None), (20, "twenty")]


def test_add_appends_a_subscriber_after_construction():
    calls = []
    sink = ProgressSink()
    sink.add(lambda percent, message=None: calls.append(percent))

    sink.update(5)

    assert calls == [5]


def test_a_raising_subscriber_does_not_block_the_others():
    calls = []

    def broken(percent, message=None):
        raise RuntimeError("simulated console renderer failure")

    sink = ProgressSink([
        broken,
        lambda percent, message=None: calls.append(percent),
    ])

    sink.update(75, "almost done")  # must not raise

    assert calls == [75]


def test_no_subscribers_is_a_harmless_no_op():
    ProgressSink().update(50, "no one is listening")
