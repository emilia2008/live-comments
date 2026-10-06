"""FastAPI application: HTTP and WebSocket routes."""

from contextlib import asynccontextmanager

from fastapi import FastAPI, Path, Query, WebSocket

from app.config import Settings
from app.schemas import ID_PATTERN
from app.service import LiveService


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build the app. Tests pass their own Settings; uvicorn uses the module-level `app`."""
    settings = settings or Settings()
    service = LiveService(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await service.start()
        yield
        await service.stop()

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


app = create_app()
