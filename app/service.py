"""The core logic: what happens when viewers join, comment, like and leave.

main.py only maps URLs to the methods of LiveService.

Path of one comment:
  viewer -> handle_viewer -> rate limit -> filter words -> save to history
  -> broker.publish("room:{id}") -> every instance's _on_broker_message
  -> rooms.broadcast -> each local viewer's WebSocket
"""

import asyncio
import json
import logging
import uuid

from fastapi import WebSocket
from pydantic import ValidationError

from app.broker import Broker
from app.config import Settings
from app.history import History
from app.likes import LikeBatcher
from app.moderation import BannedWordFilter
from app.rate_limit import RateLimiter
from app.rooms import Rooms
from app.schemas import (
    CommentEvent,
    ErrorEvent,
    HistoryEvent,
    LikeIn,
    LikesEvent,
    ViewersEvent,
    parse_client_message,
)
from app.viewers import ViewerCounter

logger = logging.getLogger(__name__)

ROOM_CHANNEL_PREFIX = "room:"
VIEWERS_CHANNEL = "viewers"


class LiveService:
    """Owns the local rooms, the rate limiter, the like batcher and the background loops."""

    def __init__(self, settings: Settings, broker: Broker, history: History):
        self.settings = settings
        self.broker = broker
        self.history = history
        self.rooms = Rooms()
        self.rate_limiter = RateLimiter(
            settings.rate_limit_capacity, settings.rate_limit_refill_per_second
        )
        self.word_filter = BannedWordFilter(settings.banned_words)
        self.likes = LikeBatcher()
        self.other_instances = ViewerCounter(stale_after=5 * settings.viewer_update_interval)
        self.comments_received = 0
        self.likes_received = 0
        self.rate_limited = 0
        self._last_viewer_counts: dict[str, int] = {}
        self._tasks: list[asyncio.Task] = []

    # ---------- lifecycle ----------

    async def start(self) -> None:
        self._tasks = [
            asyncio.create_task(self._listen_loop()),
            asyncio.create_task(self._like_flush_loop()),
            asyncio.create_task(self._viewer_count_loop()),
        ]

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    # ---------- one viewer's connection ----------

    async def handle_viewer(self, websocket: WebSocket, room_id: str, user_id: str) -> None:
        """Runs for as long as one viewer stays in the room."""
        await websocket.accept()
        # Join first, then read history: a comment sent in between may arrive twice
        # (live and in history) but never gets lost. Clients drop duplicates by id.
        self.rooms.join(room_id, websocket)
        try:
            await self._send_history(websocket, room_id)
            count = self.viewer_total(room_id)
            await self.rooms.send(websocket, ViewersEvent(room_id=room_id, count=count).model_dump_json())
            while True:
                message = await websocket.receive()
                if message["type"] == "websocket.disconnect":
                    break
                await self._handle_message(websocket, room_id, user_id, message.get("text"))
        finally:
            self.rooms.leave(room_id, websocket)

    async def _send_history(self, websocket: WebSocket, room_id: str) -> None:
        try:
            stored = await self.history.recent(room_id)
        except Exception:
            # Without history the viewer can still watch live comments.
            logger.exception("could not read history")
            stored = []
        event = HistoryEvent(
            room_id=room_id,
            instance=self.settings.instance_id,
            comments=[CommentEvent.model_validate_json(item) for item in stored],
        )
        await self.rooms.send(websocket, event.model_dump_json())

    async def _handle_message(
        self, websocket: WebSocket, room_id: str, user_id: str, raw: str | None
    ) -> None:
        if raw is None:
            await self._send_error(websocket, "invalid_message", "Messages must be JSON text.")
            return
        try:
            message = parse_client_message(raw)
        except ValidationError as error:
            await self._send_error(websocket, "invalid_message", _first_error(error))
            return

        if isinstance(message, LikeIn):
            self.likes_received += 1
            self.likes.add(room_id)
        else:
            await self._handle_comment(websocket, room_id, user_id, message.text)

    async def _handle_comment(
        self, websocket: WebSocket, room_id: str, user_id: str, text: str
    ) -> None:
        if not self.rate_limiter.allow(user_id):
            self.rate_limited += 1
            await self._send_error(
                websocket, "rate_limited", "Too many comments. You can send 1 per second."
            )
            return

        self.comments_received += 1
        event = CommentEvent(
            id=uuid.uuid4().hex,
            room_id=room_id,
            user=user_id,
            text=self.word_filter.clean(text),
            instance=self.settings.instance_id,
        )
        data = event.model_dump_json()
        try:
            await self.history.add(room_id, data)
            await self.publish(room_id, data)
        except Exception:
            logger.exception("could not store or publish a comment")
            await self._send_error(websocket, "server_error", "Comment not sent, please retry.")

    async def _send_error(self, websocket: WebSocket, code: str, message: str) -> None:
        await self.rooms.send(websocket, ErrorEvent(code=code, message=message).model_dump_json())

    # ---------- events between instances ----------

    async def publish(self, room_id: str, data: str) -> None:
        """Send an event to the room on ALL instances (through the broker)."""
        await self.broker.publish(ROOM_CHANNEL_PREFIX + room_id, data)

    async def _on_broker_message(self, channel: str, data: str) -> None:
        if channel == VIEWERS_CHANNEL:
            report = json.loads(data)
            if report["instance"] != self.settings.instance_id:
                self.other_instances.update(report["instance"], report["counts"])
        elif channel.startswith(ROOM_CHANNEL_PREFIX):
            room_id = channel[len(ROOM_CHANNEL_PREFIX):]
            await self.rooms.broadcast(room_id, data)

    def viewer_total(self, room_id: str) -> int:
        """My live count for the room + the latest counts reported by other instances."""
        return self.rooms.viewer_count(room_id) + self.other_instances.total(room_id)

    # ---------- background loops ----------

    async def _listen_loop(self) -> None:
        """Forward broker messages to local viewers; reconnect if Redis goes away."""
        while True:
            try:
                await self.broker.listen(
                    [ROOM_CHANNEL_PREFIX + "*", VIEWERS_CHANNEL], self._on_broker_message
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("broker connection lost, retrying in 1 second")
                await asyncio.sleep(1)

    async def _like_flush_loop(self) -> None:
        """Every 200 ms, publish one "likes" event per room that received likes."""
        while True:
            await asyncio.sleep(self.settings.like_flush_interval)
            try:
                for room_id, count in self.likes.flush().items():
                    event = LikesEvent(room_id=room_id, count=count)
                    await self.publish(room_id, event.model_dump_json())
            except Exception:
                logger.exception("like flush failed")

    async def _viewer_count_loop(self) -> None:
        """Every second: report my counts to the other instances, then update my viewers."""
        while True:
            await asyncio.sleep(self.settings.viewer_update_interval)
            try:
                local_counts = self.rooms.viewer_counts()
                report = {"instance": self.settings.instance_id, "counts": local_counts}
                await self.broker.publish(VIEWERS_CHANNEL, json.dumps(report))
                await self._send_changed_viewer_counts(local_counts)
                self.other_instances.forget_stale()
                self.rate_limiter.forget_idle_users()
            except Exception:
                logger.exception("viewer count update failed")

    async def _send_changed_viewer_counts(self, local_counts: dict[str, int]) -> None:
        """Each instance tells only ITS OWN viewers, so this is not published."""
        totals = {room_id: self.viewer_total(room_id) for room_id in local_counts}
        for room_id, total in totals.items():
            if self._last_viewer_counts.get(room_id) != total:
                event = ViewersEvent(room_id=room_id, count=total)
                await self.rooms.broadcast(room_id, event.model_dump_json())
        self._last_viewer_counts = totals


def _first_error(error: ValidationError) -> str:
    """Turn a pydantic error into one short, readable sentence."""
    first = error.errors()[0]
    field = ".".join(str(part) for part in first["loc"])
    return f"{field}: {first['msg']}" if field else first["msg"]
