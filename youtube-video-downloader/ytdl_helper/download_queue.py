import threading


class DownloadQueue:
    """
    Limits how many downloads may run concurrently, independent of any
    dedup layer a caller might have in front of it. Callers claim a slot
    with acquire() before doing the real work and give it back with
    release() when done; callers that arrive once the queue is full block
    in FIFO order until a slot frees up.
    """

    def __init__(self, max_concurrent: int = 1):
        if max_concurrent < 1:
            raise ValueError("max_concurrent must be at least 1")
        self.max_concurrent = max_concurrent
        self._condition = threading.Condition(threading.Lock())
        self._in_use = 0
        self._waiting_order: list = []

    def acquire(
        self,
        ticket_id,
        cancel_event: threading.Event | None = None,
        on_waiting=None,
        poll_interval: float = 0.25,
    ) -> bool:
        """
        Blocks until a slot is free and ticket_id is first in line, then
        claims the slot and returns True. If cancel_event is set before a
        slot is claimed, gives up its place in line and returns False
        without ever claiming a slot. on_waiting(position), if given, is
        called with this ticket's 1-based queue position (1 = next in
        line) once immediately and again whenever that position changes,
        so a caller can report live queue position.
        """
        with self._condition:
            self._waiting_order.append(ticket_id)
            self._condition.notify_all()
            try:
                last_reported_position = None
                while True:
                    if cancel_event is not None and cancel_event.is_set():
                        return False
                    position = self._waiting_order.index(ticket_id) + 1
                    if position == 1 and self._in_use < self.max_concurrent:
                        self._in_use += 1
                        return True
                    if on_waiting is not None and position != last_reported_position:
                        on_waiting(position)
                        last_reported_position = position
                    self._condition.wait(timeout=poll_interval)
            finally:
                self._waiting_order.remove(ticket_id)
                self._condition.notify_all()

    def release(self, ticket_id) -> None:
        """Gives back a slot previously claimed by a successful acquire()."""
        with self._condition:
            self._in_use = max(0, self._in_use - 1)
            self._condition.notify_all()
