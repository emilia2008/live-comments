"""Settings read from environment variables, with defaults that work on a laptop."""

import os
import socket
from dataclasses import dataclass, field


def _banned_words_from_env() -> list[str]:
    raw = os.getenv("BANNED_WORDS", "idiot,stupid,scam")
    return [word.strip() for word in raw.split(",") if word.strip()]


@dataclass
class Settings:
    """All tunable values in one place. Tests create their own Settings to speed things up."""

    # Empty REDIS_URL means "single instance, keep everything in memory".
    redis_url: str = field(default_factory=lambda: os.getenv("REDIS_URL", ""))
    instance_id: str = field(default_factory=lambda: os.getenv("INSTANCE_ID", socket.gethostname()))

    max_comment_length: int = 200
    history_size: int = 50
    like_flush_interval: float = 0.2  # seconds between two "likes" events of a room
    viewer_update_interval: float = 1.0  # seconds between two "viewers" events of a room

    # Token bucket per user: burst of 5 comments, then 1 comment per second.
    rate_limit_capacity: float = 5
    rate_limit_refill_per_second: float = 1.0

    banned_words: list[str] = field(default_factory=_banned_words_from_env)
