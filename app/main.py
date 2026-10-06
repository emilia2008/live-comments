"""FastAPI application: HTTP and WebSocket routes."""

from fastapi import FastAPI

from app.config import Settings


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build the app. Tests pass their own Settings; uvicorn uses the module-level `app`."""
    settings = settings or Settings()
    app = FastAPI(title="Live Comments")

    @app.get("/health")
    async def health() -> dict:
        return {"status": "ok", "instance": settings.instance_id}

    return app


app = create_app()
