"""Like batching: count likes per room and hand out the totals every 200 ms.

Sending one event per tap would flood every viewer with tiny messages.
Instead the server adds taps up and sends one "likes" event per room per interval.
"""

from collections import defaultdict


class LikeBatcher:
    """Counts likes per room until the next flush."""

    def __init__(self) -> None:
        self._pending: defaultdict[str, int] = defaultdict(int)

    def add(self, room_id: str, count: int = 1) -> None:
        self._pending[room_id] += count

    def flush(self) -> dict[str, int]:
        """Return {room_id: likes since last flush} and start counting from zero again."""
        totals = dict(self._pending)
        self._pending.clear()
        return totals
