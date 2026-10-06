"""The most recent comments of each room, sent to viewers when they join.

RedisHistory keeps them in a Redis list per room: RPUSH adds at the end, LTRIM cuts
the list down to the last N items, so it never grows. All instances share it.
MemoryHistory does the same with a deque, for tests and single-instance runs.
Comments are stored as the exact JSON string that was broadcast.
"""

from collections import defaultdict, deque
from typing import Protocol

from redis.asyncio import Redis

HISTORY_TTL_SECONDS = 24 * 60 * 60  # rooms nobody comments in for a day are forgotten


class History(Protocol):
    async def add(self, room_id: str, comment_json: str) -> None: ...

    async def recent(self, room_id: str) -> list[str]:
        """Oldest first, at most `size` items."""
        ...


class RedisHistory:
    def __init__(self, redis: Redis, size: int):
        self._redis = redis
        self._size = size

    async def add(self, room_id: str, comment_json: str) -> None:
        key = _key(room_id)
        # One round trip, and MULTI/EXEC makes the three commands run together.
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.rpush(key, comment_json)
            pipe.ltrim(key, -self._size, -1)
            pipe.expire(key, HISTORY_TTL_SECONDS)
            await pipe.execute()

    async def recent(self, room_id: str) -> list[str]:
        return await self._redis.lrange(_key(room_id), 0, -1)


class MemoryHistory:
    def __init__(self, size: int):
        self._rooms: defaultdict[str, deque[str]] = defaultdict(lambda: deque(maxlen=size))

    async def add(self, room_id: str, comment_json: str) -> None:
        self._rooms[room_id].append(comment_json)  # deque(maxlen) drops the oldest item

    async def recent(self, room_id: str) -> list[str]:
        return list(self._rooms.get(room_id, ()))


def _key(room_id: str) -> str:
    return f"history:{room_id}"
