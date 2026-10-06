"""Pub/Sub between server instances.

A viewer on instance A must see a comment sent to instance B. So no instance sends
events straight to its viewers. Instead it publishes the event to a channel, and every
instance (including itself) listens and forwards the event to its OWN local viewers.

Channels:
  room:{room_id}  comment and likes events of one room (already JSON, sent as-is)
  viewers         once per second, each instance reports its local viewer counts

Two implementations with the same methods:
  RedisBroker   real Redis Pub/Sub, needed when several instances run
  MemoryBroker  in-process stand-in, used by tests and by a single instance without Redis
"""

import asyncio
import logging
from fnmatch import fnmatchcase
from typing import Awaitable, Callable, Protocol

from redis.asyncio import Redis

logger = logging.getLogger(__name__)

# handler(channel, data) is called for every message that matches a pattern.
Handler = Callable[[str, str], Awaitable[None]]


class Broker(Protocol):
    async def publish(self, channel: str, data: str) -> None: ...

    async def listen(self, patterns: list[str], handler: Handler) -> None:
        """Call handler for every message on a channel matching one of the patterns.
        Runs until cancelled or until the connection breaks (then it raises)."""
        ...


class RedisBroker:
    """Redis Pub/Sub. PUBLISH is fire-and-forget: Redis does not store the message."""

    def __init__(self, redis: Redis):
        self._redis = redis

    async def publish(self, channel: str, data: str) -> None:
        await self._redis.publish(channel, data)

    async def listen(self, patterns: list[str], handler: Handler) -> None:
        pubsub = self._redis.pubsub()
        try:
            await pubsub.psubscribe(*patterns)
            async for message in pubsub.listen():
                if message["type"] == "pmessage":  # skip "psubscribe" confirmations
                    await handler(message["channel"], message["data"])
        finally:
            await pubsub.aclose()


class MemoryBroker:
    """Delivers messages to every listener in this process.

    Several apps may share one MemoryBroker to imitate several instances. In tests each
    app runs on its own event loop (one per TestClient), so messages are handed over
    with call_soon_threadsafe.
    """

    def __init__(self) -> None:
        self._listeners: list[tuple[asyncio.AbstractEventLoop, asyncio.Queue]] = []

    async def publish(self, channel: str, data: str) -> None:
        for loop, queue in list(self._listeners):
            try:
                loop.call_soon_threadsafe(queue.put_nowait, (channel, data))
            except RuntimeError:
                logger.debug("listener's event loop is already closed")

    async def listen(self, patterns: list[str], handler: Handler) -> None:
        listener = (asyncio.get_running_loop(), asyncio.Queue())
        self._listeners.append(listener)
        try:
            while True:
                channel, data = await listener[1].get()
                if any(fnmatchcase(channel, pattern) for pattern in patterns):
                    await handler(channel, data)
        finally:
            self._listeners.remove(listener)
