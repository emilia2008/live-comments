"""Pydantic models for every JSON message sent over the WebSocket.

Client -> server:  {"type": "comment", "text": "..."}   or   {"type": "like"}
Server -> client:  comment, likes, viewers, history, error (each has "type" and "sent_at")
"""

import time
from typing import Annotated, Literal, Union

from pydantic import BaseModel, Field, StringConstraints, TypeAdapter

MAX_COMMENT_LENGTH = 200

# Room and user ids are short and URL-safe, e.g. "music-1" or "alice_99".
ID_PATTERN = r"^[A-Za-z0-9_-]{1,32}$"


def now_ms() -> int:
    """Current wall-clock time in milliseconds (used for sent_at and latency measurement)."""
    return int(time.time() * 1000)


# ---------- Client -> server ----------

CommentText = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=MAX_COMMENT_LENGTH)
]


class CommentIn(BaseModel):
    type: Literal["comment"]
    text: CommentText


class LikeIn(BaseModel):
    type: Literal["like"]


ClientMessage = Annotated[Union[CommentIn, LikeIn], Field(discriminator="type")]
_client_message_adapter = TypeAdapter(ClientMessage)


def parse_client_message(raw: str) -> CommentIn | LikeIn:
    """Parse and validate one message from a viewer. Raises pydantic.ValidationError."""
    return _client_message_adapter.validate_json(raw)


# ---------- Server -> client ----------


class CommentEvent(BaseModel):
    type: Literal["comment"] = "comment"
    id: str
    room_id: str
    user: str
    text: str
    instance: str  # which backend accepted the comment
    sent_at: int = Field(default_factory=now_ms)


class LikesEvent(BaseModel):
    type: Literal["likes"] = "likes"
    room_id: str
    count: int  # likes received during the last batching interval
    sent_at: int = Field(default_factory=now_ms)


class ViewersEvent(BaseModel):
    type: Literal["viewers"] = "viewers"
    room_id: str
    count: int
    sent_at: int = Field(default_factory=now_ms)


class HistoryEvent(BaseModel):
    type: Literal["history"] = "history"
    room_id: str
    instance: str  # which backend this viewer is connected to
    comments: list[CommentEvent]
    sent_at: int = Field(default_factory=now_ms)


class ErrorEvent(BaseModel):
    type: Literal["error"] = "error"
    code: str  # "invalid_message" or "rate_limited"
    message: str
    sent_at: int = Field(default_factory=now_ms)
