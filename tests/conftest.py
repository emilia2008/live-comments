import pytest
from fastapi.testclient import TestClient

from app.main import create_app
from tests.helpers import fast_settings


@pytest.fixture
def client():
    with TestClient(create_app(fast_settings())) as test_client:
        yield test_client
