"""FastAPI application: HTTP and WebSocket routes."""

from contextlib import asynccontextmanager

from fastapi import FastAPI, Path, Query, WebSocket
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff

from app.broker import Broker, MemoryBroker, RedisBroker
from app.config import Settings
from app.history import History, MemoryHistory, RedisHistory
from app.schemas import ID_PATTERN
from app.service import LiveService


def create_app(
    settings: Settings | None = None,
    broker: Broker | None = None,
    history: History | None = None,
) -> FastAPI:
    """Build the app. uvicorn uses the module-level `app` below.

    Tests pass their own settings, and may pass one MemoryBroker + MemoryHistory
    to several apps to imitate several instances sharing one Redis.
    """
    settings = settings or Settings()
    redis = None
    if settings.redis_url:
        redis = connect_redis(settings.redis_url)
        broker = broker or RedisBroker(redis)
        history = history or RedisHistory(redis, settings.history_size)
    else:
        broker = broker or MemoryBroker()
        history = history or MemoryHistory(settings.history_size)
    service = LiveService(settings, broker, history)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await service.start()
        yield
        await service.stop()
        if redis is not None:
            await redis.aclose()

    app = FastAPI(title="Live Comments", lifespan=lifespan)
    app.state.service = service  # lets tests look inside

    @app.websocket("/ws/rooms/{room_id}")
    async def room_socket(
        websocket: WebSocket,
        room_id: str = Path(pattern=ID_PATTERN),
        user: str = Query(pattern=ID_PATTERN),
    ) -> None:
        await service.handle_viewer(websocket, room_id, user)

    @app.get("/health")
    async def health() -> dict:
        return {"status": "ok", "instance": settings.instance_id}

    return app


def connect_redis(redis_url: str) -> Redis:
    """One Redis client (with its connection pool) shared by the broker and the history.

    from_url() does not retry by default, so after a Redis restart the first command on
    an old pooled connection would fail. Retrying makes it reconnect transparently.
    """
    retry = Retry(ExponentialBackoff(cap=1.0, base=0.05), retries=3)
    return Redis.from_url(redis_url, decode_responses=True, retry=retry)


app = create_app()
