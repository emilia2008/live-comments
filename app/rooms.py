"""The WebSocket connections open on THIS server instance, grouped by room."""

import logging

from fastapi import WebSocket

logger = logging.getLogger(__name__)


class Rooms:
    """room_id -> set of open WebSockets, plus a counter of messages sent."""

    def __init__(self) -> None:
        self._rooms: dict[str, set[WebSocket]] = {}
        self.messages_sent = 0

    def join(self, room_id: str, websocket: WebSocket) -> None:
        self._rooms.setdefault(room_id, set()).add(websocket)

    def leave(self, room_id: str, websocket: WebSocket) -> None:
        room = self._rooms.get(room_id)
        if room is None:
            return
        room.discard(websocket)
        if not room:
            del self._rooms[room_id]  # forget empty rooms so memory does not grow forever

    def viewer_count(self, room_id: str) -> int:
        return len(self._rooms.get(room_id, ()))

    def viewer_counts(self) -> dict[str, int]:
        return {room_id: len(sockets) for room_id, sockets in self._rooms.items()}

    def room_count(self) -> int:
        return len(self._rooms)

    def connection_count(self) -> int:
        return sum(len(sockets) for sockets in self._rooms.values())

    async def broadcast(self, room_id: str, text: str) -> None:
        """Send an already-serialized JSON message to everyone in the room on this instance."""
        for websocket in list(self._rooms.get(room_id, ())):
            await self.send(websocket, text)

    async def send(self, websocket: WebSocket, text: str) -> None:
        try:
            await websocket.send_text(text)
            self.messages_sent += 1
        except Exception:
            # The socket is closing. Its own handler will notice and call leave().
            logger.debug("send failed, connection is closing")
