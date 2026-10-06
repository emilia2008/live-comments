"""Two app instances sharing one in-memory broker and history, like two servers sharing Redis."""

from contextlib import contextmanager

from fastapi.testclient import TestClient

from app.broker import MemoryBroker
from app.history import MemoryHistory
from app.main import create_app
from tests.helpers import connect, fast_settings, receive_until


@contextmanager
def two_instances():
    broker = MemoryBroker()
    history = MemoryHistory(size=50)
    app_a = create_app(fast_settings(instance_id="backend-a"), broker=broker, history=history)
    app_b = create_app(fast_settings(instance_id="backend-b"), broker=broker, history=history)
    with TestClient(app_a) as client_a, TestClient(app_b) as client_b:
        yield client_a, client_b


def test_comment_reaches_a_viewer_on_the_other_instance():
    with two_instances() as (client_a, client_b):
        with connect(client_a, "r1", "alice") as alice, connect(client_b, "r1", "bob") as bob:
            assert receive_until(alice, "history")["instance"] == "backend-a"
            assert receive_until(bob, "history")["instance"] == "backend-b"

            alice.send_json({"type": "comment", "text": "hello from A"})

            event = receive_until(bob, "comment")
            assert event["text"] == "hello from A"
            assert event["instance"] == "backend-a"


def test_likes_reach_a_viewer_on_the_other_instance():
    with two_instances() as (client_a, client_b):
        with connect(client_a, "r1", "alice") as alice, connect(client_b, "r1", "bob") as bob:
            alice.send_json({"type": "like"})
            assert receive_until(bob, "likes")["count"] == 1


def test_viewer_count_includes_viewers_on_both_instances():
    with two_instances() as (client_a, client_b):
        with connect(client_a, "r1", "alice") as alice, connect(client_b, "r1", "bob") as bob:
            for viewer in (alice, bob):
                count = receive_until(viewer, "viewers")["count"]
                while count != 2:
                    count = receive_until(viewer, "viewers")["count"]
                assert count == 2


def test_history_is_shared_between_instances():
    with two_instances() as (client_a, client_b):
        with connect(client_a, "r1", "alice") as alice:
            for number in range(3):
                alice.send_json({"type": "comment", "text": f"comment {number}"})
                receive_until(alice, "comment")

        with connect(client_b, "r1", "carol") as carol:
            history = receive_until(carol, "history")

    assert [c["text"] for c in history["comments"]] == ["comment 0", "comment 1", "comment 2"]
