"""Viewer counts reported by the OTHER instances.

Each instance only knows its own connections. Once per second every instance publishes
{"instance": "backend1", "counts": {"room-1": 120, ...}} on the "viewers" channel.
ViewerCounter remembers the latest report of each other instance. The total for a room
is: my own live count + the counts in those reports.
Reports that are too old are ignored, so a crashed instance stops being counted.
"""

import time
from typing import Callable


class ViewerCounter:
    def __init__(self, stale_after: float, clock: Callable[[], float] = time.monotonic):
        self._stale_after = stale_after
        self._clock = clock
        # instance_id -> (time the report arrived, {room_id: viewers})
        self._reports: dict[str, tuple[float, dict[str, int]]] = {}

    def update(self, instance_id: str, counts: dict[str, int]) -> None:
        self._reports[instance_id] = (self._clock(), counts)

    def total(self, room_id: str) -> int:
        """Viewers of this room on all other instances that reported recently."""
        now = self._clock()
        return sum(
            counts.get(room_id, 0)
            for received_at, counts in self._reports.values()
            if now - received_at <= self._stale_after
        )

    def forget_stale(self) -> None:
        now = self._clock()
        stale = [i for i, (received_at, _) in self._reports.items() if now - received_at > self._stale_after]
        for instance_id in stale:
            del self._reports[instance_id]
