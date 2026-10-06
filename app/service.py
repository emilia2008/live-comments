"""The core logic: what happens when viewers join, comment, like and leave.

main.py only maps URLs to the methods of LiveService.

Path of one comment:
  viewer -> handle_viewer -> rate limit -> filter words -> save to history
  -> broker.publish("room:{id}") -> every instance's _on_broker_message
  -> rooms.broadcast -> each local viewer's outbox -> sender task -> WebSocket
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
from app.rooms import Connection, Rooms
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
        self.rooms = Rooms(max_pending=settings.outbox_size)
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
        connection = self.rooms.join(websocket, room_id, user_id)
        sender = None
        try:
            # History is written directly, BEFORE the sender task starts, so it always
            # arrives before the live events already waiting in the outbox.
            await self._send_history(connection)
            viewers = ViewersEvent(room_id=room_id, count=self.viewer_total(room_id))
            self.rooms.send(connection, viewers.model_dump_json())
            sender = asyncio.create_task(self.rooms.run_sender(connection))
            while True:
                message = await websocket.receive()
                if message["type"] == "websocket.disconnect":
                    break
                await self._handle_message(connection, message.get("text"))
        finally:
            self.rooms.leave(connection)
            if sender is not None:
                sender.cancel()

    async def _send_history(self, connection: Connection) -> None:
        try:
            stored = await self.history.recent(connection.room_id)
        except Exception:
            # Without history the viewer can still watch live comments.
            logger.exception("could not read history")
            stored = []
        event = HistoryEvent(
            room_id=connection.room_id,
            instance=self.settings.instance_id,
            comments=[CommentEvent.model_validate_json(item) for item in stored],
        )
        await connection.websocket.send_text(event.model_dump_json())
        self.rooms.messages_sent += 1
        self.rooms.frames_sent += 1

    async def _handle_message(self, connection: Connection, raw: str | None) -> None:
        if raw is None:
            self._send_error(connection, "invalid_message", "Messages must be JSON text.")
            return
        try:
            message = parse_client_message(raw)
        except ValidationError as error:
            self._send_error(connection, "invalid_message", _first_error(error))
            return

        if isinstance(message, LikeIn):
            self.likes_received += 1
            self.likes.add(connection.room_id)
        else:
            await self._handle_comment(connection, message.text)

    async def _handle_comment(self, connection: Connection, text: str) -> None:
        if not self.rate_limiter.allow(connection.user_id):
            self.rate_limited += 1
            self._send_error(connection, "rate_limited", "Too many comments. You can send 1 per second.")
            return

        self.comments_received += 1
        event = CommentEvent(
            id=uuid.uuid4().hex,
            room_id=connection.room_id,
            user=connection.user_id,
            text=self.word_filter.clean(text),
            instance=self.settings.instance_id,
        )
        data = event.model_dump_json()
        try:
            await self.history.add(connection.room_id, data)
            await self.publish(connection.room_id, data)
        except Exception:
            logger.exception("could not store or publish a comment")
            self._send_error(connection, "server_error", "Comment not sent, please retry.")

    def _send_error(self, connection: Connection, code: str, message: str) -> None:
        self.rooms.send(connection, ErrorEvent(code=code, message=message).model_dump_json())

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
            self.rooms.broadcast(room_id, data)

    def stats(self) -> dict:
        """Numbers for /stats and /metrics. They describe THIS instance only."""
        return {
            "instance": self.settings.instance_id,
            "connections": self.rooms.connection_count(),
            "rooms": self.rooms.room_count(),
            "messages_sent": self.rooms.messages_sent,
            "frames_sent": self.rooms.frames_sent,
            "messages_dropped": self.rooms.messages_dropped,
            "comments_received": self.comments_received,
            "likes_received": self.likes_received,
            "rate_limited": self.rate_limited,
        }

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
                self._send_changed_viewer_counts(local_counts)
                self.other_instances.forget_stale()
                self.rate_limiter.forget_idle_users()
            except Exception:
                logger.exception("viewer count update failed")

    def _send_changed_viewer_counts(self, local_counts: dict[str, int]) -> None:
        """Each instance tells only ITS OWN viewers, so this is not published."""
        totals = {room_id: self.viewer_total(room_id) for room_id in local_counts}
        for room_id, total in totals.items():
            if self._last_viewer_counts.get(room_id) != total:
                event = ViewersEvent(room_id=room_id, count=total)
                self.rooms.broadcast(room_id, event.model_dump_json())
        self._last_viewer_counts = totals


def _first_error(error: ValidationError) -> str:
    """Turn a pydantic error into one short, readable sentence."""
    first = error.errors()[0]
    field = ".".join(str(part) for part in first["loc"])
    return f"{field}: {first['msg']}" if field else first["msg"]
