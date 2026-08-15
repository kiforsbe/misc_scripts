"""
Generic progress-reporting fan-out.

A ProgressSink lets one download attach several independent consumers of
its progress ticks (e.g. a per-job dict polled over HTTP, a terminal
console display, a debug log line) without each download entry point
having to hand-roll its own bespoke wiring. That hand-rolling is how the
console display was silently dropped when the job-based /download/start
path was added alongside the older blocking /download route: each entry
point built its own one-off progress_hook closure instead of sharing one
implementation, so it was easy for one of them to simply omit a
subscriber. Centralizing the fan-out here means a new entry point reuses
the same subscriber set instead of re-deriving it, and the fan-out logic
itself is trivial to unit test without Flask or a real terminal.
"""

import logging
from typing import Callable, List, Optional

logger = logging.getLogger(__name__)

ProgressSubscriber = Callable[[float, Optional[str]], None]


class ProgressSink:
    """Fans a single (percent, message) progress tick out to N subscribers.

    A subscriber raising does not prevent the others from being notified -
    e.g. a broken console renderer must not stop job-status updates from
    reaching /download/status.
    """

    def __init__(self, subscribers: Optional[List[ProgressSubscriber]] = None):
        self._subscribers: List[ProgressSubscriber] = list(subscribers or [])

    def add(self, subscriber: ProgressSubscriber) -> None:
        self._subscribers.append(subscriber)

    def update(self, percent: float, message: Optional[str] = None) -> None:
        for subscriber in self._subscribers:
            try:
                subscriber(percent, message)
            except Exception:
                logger.exception("Progress subscriber raised; continuing with others.")

    def __call__(self, percent: float, message: Optional[str] = None) -> None:
        # ProgressSink instances are used as drop-in replacements for the
        # old raw progress_hook(percent, message) callables, so existing
        # call sites (progress_hook(20, "..."), etc.) need no change.
        self.update(percent, message)
