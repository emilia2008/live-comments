"""The core logic: what happens when viewers join, comment, like and leave.

main.py only maps URLs to the methods of LiveService.
"""

import asyncio
import logging
import uuid

from fastapi import WebSocket
from pydantic import ValidationError

from app.config import Settings
from app.likes import LikeBatcher
from app.moderation import BannedWordFilter
from app.rate_limit import RateLimiter
from app.rooms import Rooms
from app.schemas import (
    CommentEvent,
    ErrorEvent,
    LikeIn,
    LikesEvent,
    ViewersEvent,
    parse_client_message,
)

logger = logging.getLogger(__name__)


class LiveService:
    """Owns the rooms, the rate limiter, the like batcher and the background loops."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.rooms = Rooms()
        self.rate_limiter = RateLimiter(
            settings.rate_limit_capacity, settings.rate_limit_refill_per_second
        )
        self.word_filter = BannedWordFilter(settings.banned_words)
        self.likes = LikeBatcher()
        self.comments_received = 0
        self.likes_received = 0
        self.rate_limited = 0
        self._last_viewer_counts: dict[str, int] = {}
        self._tasks: list[asyncio.Task] = []

    # ---------- lifecycle ----------

    async def start(self) -> None:
        self._tasks = [
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
        self.rooms.join(room_id, websocket)
        try:
            count = self.rooms.viewer_count(room_id)
            await self.rooms.send(websocket, ViewersEvent(room_id=room_id, count=count).model_dump_json())
            while True:
                message = await websocket.receive()
                if message["type"] == "websocket.disconnect":
                    break
                await self._handle_message(websocket, room_id, user_id, message.get("text"))
        finally:
            self.rooms.leave(room_id, websocket)

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
        await self.publish(room_id, event.model_dump_json())

    async def _send_error(self, websocket: WebSocket, code: str, message: str) -> None:
        await self.rooms.send(websocket, ErrorEvent(code=code, message=message).model_dump_json())

    # ---------- delivering events ----------

    async def publish(self, room_id: str, data: str) -> None:
        """Deliver an event to everyone in the room."""
        await self.rooms.broadcast(room_id, data)

    # ---------- background loops ----------

    async def _like_flush_loop(self) -> None:
        """Every 200 ms, send one "likes" event per room that received likes."""
        while True:
            await asyncio.sleep(self.settings.like_flush_interval)
            try:
                for room_id, count in self.likes.flush().items():
                    event = LikesEvent(room_id=room_id, count=count)
                    await self.publish(room_id, event.model_dump_json())
            except Exception:
                logger.exception("like flush failed")

    async def _viewer_count_loop(self) -> None:
        """Every second, tell each room its viewer count, but only if it changed."""
        while True:
            await asyncio.sleep(self.settings.viewer_update_interval)
            try:
                await self._send_viewer_counts()
                self.rate_limiter.forget_idle_users()
            except Exception:
                logger.exception("viewer count update failed")

    async def _send_viewer_counts(self) -> None:
        counts = self.rooms.viewer_counts()
        for room_id, count in counts.items():
            if self._last_viewer_counts.get(room_id) != count:
                event = ViewersEvent(room_id=room_id, count=count)
                await self.rooms.broadcast(room_id, event.model_dump_json())
        self._last_viewer_counts = counts


def _first_error(error: ValidationError) -> str:
    """Turn a pydantic error into one short, readable sentence."""
    first = error.errors()[0]
    field = ".".join(str(part) for part in first["loc"])
    return f"{field}: {first['msg']}" if field else first["msg"]
