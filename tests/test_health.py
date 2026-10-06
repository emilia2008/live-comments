from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app


def test_health_returns_ok_and_instance_id():
    app = create_app(Settings(instance_id="test-1"))
    with TestClient(app) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "instance": "test-1"}
