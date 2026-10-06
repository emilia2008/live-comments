"""The WebSocket connections open on THIS server instance, grouped by room.

Every connection has its own outbox (a bounded queue) and its own sender task.
broadcast() only puts the message into each outbox and returns at once, so one slow
viewer can never hold up the others. A viewer whose outbox is full is too far behind:
new messages for it are dropped (and counted) until it catches up.

A frame on the wire is either one event (a JSON object) or, when several were waiting,
a JSON array of events.
"""

import asyncio
import logging

from fastapi import WebSocket

logger = logging.getLogger(__name__)

MAX_MESSAGES_PER_FRAME = 50


class Connection:
    """One viewer: the WebSocket plus the messages waiting to be sent to it."""

    def __init__(self, websocket: WebSocket, room_id: str, user_id: str, max_pending: int):
        self.websocket = websocket
        self.room_id = room_id
        self.user_id = user_id
        self.outbox: asyncio.Queue[str] = asyncio.Queue(maxsize=max_pending)

    def enqueue(self, text: str) -> bool:
        """Add a message without waiting. Returns False if the outbox is full."""
        try:
            self.outbox.put_nowait(text)
            return True
        except asyncio.QueueFull:
            return False


class Rooms:
    """room_id -> set of Connections, plus counters for /stats."""

    def __init__(self, max_pending: int = 256) -> None:
        self.max_pending = max_pending
        self._rooms: dict[str, set[Connection]] = {}
        self.messages_sent = 0
        self.frames_sent = 0
        self.messages_dropped = 0

    def join(self, websocket: WebSocket, room_id: str, user_id: str) -> Connection:
        connection = Connection(websocket, room_id, user_id, self.max_pending)
        self._rooms.setdefault(room_id, set()).add(connection)
        return connection

    def leave(self, connection: Connection) -> None:
        room = self._rooms.get(connection.room_id)
        if room is None:
            return
        room.discard(connection)
        if not room:
            del self._rooms[connection.room_id]  # forget empty rooms so memory does not grow

    def viewer_count(self, room_id: str) -> int:
        return len(self._rooms.get(room_id, ()))

    def viewer_counts(self) -> dict[str, int]:
        return {room_id: len(connections) for room_id, connections in self._rooms.items()}

    def room_count(self) -> int:
        return len(self._rooms)

    def connection_count(self) -> int:
        return sum(len(connections) for connections in self._rooms.values())

    def broadcast(self, room_id: str, text: str) -> None:
        """Queue an already-serialized JSON message for everyone in the room on this instance."""
        for connection in self._rooms.get(room_id, ()):
            self.send(connection, text)

    def send(self, connection: Connection, text: str) -> None:
        if not connection.enqueue(text):
            self.messages_dropped += 1

    async def run_sender(self, connection: Connection) -> None:
        """Write the outbox to the socket until cancelled.

        If several messages are already waiting, they go out together in ONE frame as a
        JSON array. Every frame costs a system call and some framework work, so under
        load this cuts the cost per message. It never waits to fill a batch, so a quiet
        room still gets each message immediately as a single JSON object.
        """
        while True:
            batch = [await connection.outbox.get()]
            while not connection.outbox.empty() and len(batch) < MAX_MESSAGES_PER_FRAME:
                batch.append(connection.outbox.get_nowait())
            text = batch[0] if len(batch) == 1 else "[" + ",".join(batch) + "]"
            try:
                await connection.websocket.send_text(text)
            except Exception:
                # The socket is closing. The viewer's handler will notice and clean up.
                logger.debug("send failed, connection is closing")
                return
            self.messages_sent += len(batch)
            self.frames_sent += 1
