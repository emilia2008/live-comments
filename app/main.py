"""FastAPI application: HTTP and WebSocket routes."""

from contextlib import asynccontextmanager
from pathlib import Path as FilePath

from fastapi import FastAPI, Path, Query, WebSocket
from fastapi.responses import FileResponse, PlainTextResponse
from redis.asyncio import BlockingConnectionPool, Redis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff

from app.broker import Broker, MemoryBroker, RedisBroker
from app.config import Settings
from app.history import History, MemoryHistory, RedisHistory
from app.metrics import to_prometheus
from app.schemas import ID_PATTERN
from app.service import LiveService

INDEX_HTML = FilePath(__file__).resolve().parent.parent / "static" / "index.html"


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

    @app.get("/", include_in_schema=False)
    async def demo_page() -> FileResponse:
        return FileResponse(INDEX_HTML)

    @app.get("/health")
    async def health() -> dict:
        return {"status": "ok", "instance": settings.instance_id}

    @app.get("/stats")
    async def stats() -> dict:
        return service.stats()

    @app.get("/metrics", response_class=PlainTextResponse)
    async def metrics() -> str:
        return to_prometheus(service.stats())

    return app


def connect_redis(redis_url: str) -> Redis:
    """One Redis client (with its connection pool) shared by the broker and the history.

    - BlockingConnectionPool: at most 50 connections; when all are busy, wait for one.
      The default pool opens a new connection for every concurrent command, which grew
      to hundreds of Redis connections when many viewers joined at once.
    - retry: after a Redis restart the first command on an old pooled connection fails;
      retrying makes it reconnect transparently (from_url does not retry by default).
    - protocol=2 (RESP2) works with every Redis version, including the Windows port
      used for local benchmarks; we need nothing from RESP3.
    """
    pool = BlockingConnectionPool.from_url(
        redis_url,
        max_connections=50,
        timeout=5,
        decode_responses=True,
        protocol=2,
        retry=Retry(ExponentialBackoff(cap=1.0, base=0.05), retries=3),
    )
    return Redis.from_pool(pool)  # the client owns the pool and closes it in aclose()


app = create_app()
