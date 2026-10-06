"""Token bucket rate limiting for comments.

A bucket holds up to `capacity` tokens and gains `refill_per_second` tokens every second.
Each comment costs one token. An empty bucket means "too fast, try again later".
The clock is passed in, so tests can move time forward without sleeping.
"""

import time
from typing import Callable

Clock = Callable[[], float]


class TokenBucket:
    """One user's bucket of comment tokens."""

    def __init__(self, capacity: float, refill_per_second: float, clock: Clock = time.monotonic):
        self.capacity = capacity
        self.refill_per_second = refill_per_second
        self._clock = clock
        self._tokens = capacity  # a new user may send a full burst right away
        self._last_refill = clock()

    def allow(self) -> bool:
        """Take one token if there is one. Returns False when the user must slow down."""
        self._refill()
        if self._tokens >= 1:
            self._tokens -= 1
            return True
        return False

    def is_full(self) -> bool:
        """A full bucket behaves exactly like a brand-new one."""
        self._refill()
        return self._tokens >= self.capacity

    def _refill(self) -> None:
        now = self._clock()
        elapsed = now - self._last_refill
        self._tokens = min(self.capacity, self._tokens + elapsed * self.refill_per_second)
        self._last_refill = now


class RateLimiter:
    """Keeps one TokenBucket per user id."""

    def __init__(self, capacity: float, refill_per_second: float, clock: Clock = time.monotonic):
        self.capacity = capacity
        self.refill_per_second = refill_per_second
        self._clock = clock
        self._buckets: dict[str, TokenBucket] = {}

    def allow(self, user_id: str) -> bool:
        bucket = self._buckets.get(user_id)
        if bucket is None:
            bucket = TokenBucket(self.capacity, self.refill_per_second, self._clock)
            self._buckets[user_id] = bucket
        return bucket.allow()

    def forget_idle_users(self) -> None:
        """Drop full buckets to save memory. Safe, because a full bucket equals a new one."""
        idle = [user_id for user_id, bucket in self._buckets.items() if bucket.is_full()]
        for user_id in idle:
            del self._buckets[user_id]

    def __len__(self) -> int:
        return len(self._buckets)
