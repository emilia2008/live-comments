"""Shared test helpers. No network and no Redis: everything runs in memory."""

import time

from fastapi.testclient import TestClient

from app.config import Settings


def fast_settings(**overrides) -> Settings:
    """Settings with short intervals so tests do not wait long."""
    values = dict(
        redis_url="",  # in-memory mode, even if REDIS_URL is set on this machine
        instance_id="test",
        like_flush_interval=0.05,
        viewer_update_interval=0.05,
        banned_words=["scam", "idiot"],
    )
    values.update(overrides)
    return Settings(**values)


def connect(client: TestClient, room_id: str, user_id: str):
    return client.websocket_connect(f"/ws/rooms/{room_id}?user={user_id}")


def receive_until(websocket, event_type: str, max_messages: int = 200) -> dict:
    """Read messages until one has the given type; skip likes/viewers/etc. on the way."""
    for _ in range(max_messages):
        message = websocket.receive_json()
        if message["type"] == event_type:
            return message
    raise AssertionError(f"no {event_type!r} event in {max_messages} messages")


def wait_for(condition, timeout: float = 2.0) -> None:
    """Poll until condition() is true. Used for things that happen in the server's own task."""
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError("condition not met in time")
        time.sleep(0.01)
