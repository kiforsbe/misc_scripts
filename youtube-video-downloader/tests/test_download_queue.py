"""
Pure unit tests for ytdl_helper.download_queue.DownloadQueue - no Flask, no
dedup, no real downloads. Flask-level wiring (queue position surfaced via
job status, dedup waiters not consuming a slot) is covered separately in
test_flask_ws_queue.py.
"""

import pathlib
import sys
import threading
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from ytdl_helper.download_queue import DownloadQueue


def test_second_caller_blocks_until_first_releases_with_default_cap():
    q = DownloadQueue(max_concurrent=1)
    first_acquired = threading.Event()
    release_first = threading.Event()
    second_acquired = threading.Event()

    def holder():
        q.acquire("first")
        first_acquired.set()
        release_first.wait(timeout=3)
        q.release("first")

    def waiter():
        first_acquired.wait(timeout=2)
        q.acquire("second")
        second_acquired.set()

    t1 = threading.Thread(target=holder)
    t2 = threading.Thread(target=waiter)
    t1.start()
    t2.start()

    assert first_acquired.wait(timeout=2)
    time.sleep(0.2)
    assert not second_acquired.is_set(), "second caller acquired a slot while cap=1 was already in use"

    release_first.set()
    assert second_acquired.wait(timeout=2), "second caller never acquired after release"

    t1.join(timeout=2)
    t2.join(timeout=2)


def test_configurable_cap_allows_n_concurrent_holders():
    q = DownloadQueue(max_concurrent=2)
    acquired_count = {"n": 0}
    lock = threading.Lock()
    release = threading.Event()

    def worker(ticket_id):
        q.acquire(ticket_id)
        with lock:
            acquired_count["n"] += 1
        release.wait(timeout=3)
        q.release(ticket_id)

    threads = [threading.Thread(target=worker, args=(f"t{i}",)) for i in range(3)]
    for t in threads:
        t.start()

    deadline = time.time() + 2
    while time.time() < deadline and acquired_count["n"] < 2:
        time.sleep(0.02)

    assert acquired_count["n"] == 2, f"expected exactly 2 concurrent holders, got {acquired_count['n']}"

    release.set()
    for t in threads:
        t.join(timeout=2)

    assert acquired_count["n"] == 3


def test_acquire_returns_false_when_cancelled_while_waiting():
    q = DownloadQueue(max_concurrent=1)
    q.acquire("holder")  # occupy the only slot
    cancel_event = threading.Event()
    result = {"value": None}

    def waiter():
        result["value"] = q.acquire("cancelled-waiter", cancel_event=cancel_event)

    t = threading.Thread(target=waiter)
    t.start()
    time.sleep(0.1)
    cancel_event.set()
    t.join(timeout=2)

    assert result["value"] is False
    # A cancelled waiter must not have consumed a slot, or the still-held
    # slot would incorrectly look like 2 are in use.
    assert q._in_use == 1


def test_cancelled_waiter_does_not_block_the_next_caller():
    q = DownloadQueue(max_concurrent=1)
    q.acquire("holder")
    cancel_event = threading.Event()
    cancelled_result = {"value": None}

    def cancelled_waiter():
        cancelled_result["value"] = q.acquire("cancelled-waiter", cancel_event=cancel_event)

    t = threading.Thread(target=cancelled_waiter)
    t.start()
    time.sleep(0.1)
    cancel_event.set()
    t.join(timeout=2)
    assert cancelled_result["value"] is False

    q.release("holder")

    next_acquired = threading.Event()

    def next_waiter():
        q.acquire("next")
        next_acquired.set()

    t2 = threading.Thread(target=next_waiter)
    t2.start()
    assert next_acquired.wait(timeout=2), "next caller never acquired after a cancelled waiter ahead of it"
    t2.join(timeout=2)


def test_on_waiting_reports_fifo_position():
    q = DownloadQueue(max_concurrent=1)
    q.acquire("holder")

    positions_seen = {"first": [], "second": []}
    release = threading.Event()

    def waiter(ticket_id, key):
        def on_waiting(position):
            positions_seen[key].append(position)
        q.acquire(ticket_id, on_waiting=on_waiting)
        release.wait(timeout=3)
        q.release(ticket_id)

    t1 = threading.Thread(target=waiter, args=("first-waiter", "first"))
    t1.start()
    time.sleep(0.1)
    t2 = threading.Thread(target=waiter, args=("second-waiter", "second"))
    t2.start()
    time.sleep(0.4)  # let both report their position at least once via polling

    assert positions_seen["first"] and positions_seen["first"][0] == 1
    assert positions_seen["second"] and positions_seen["second"][0] == 2

    q.release("holder")
    release.set()
    t1.join(timeout=2)
    t2.join(timeout=2)


def test_max_concurrent_must_be_at_least_one():
    try:
        DownloadQueue(max_concurrent=0)
        assert False, "expected ValueError for max_concurrent=0"
    except ValueError:
        pass
