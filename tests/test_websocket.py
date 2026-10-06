import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from app.main import create_app
from tests.helpers import connect, fast_settings, next_event, receive_until, wait_for


def comment(text: str) -> dict:
    return {"type": "comment", "text": text}


# ---------- joining ----------


def test_new_viewer_gets_empty_history_then_viewer_count(client):
    with connect(client, "r1", "alice") as alice:
        history = next_event(alice)
        assert history["type"] == "history"
        assert history["comments"] == []
        assert history["instance"] == "test"

        viewers = next_event(alice)
        assert viewers["type"] == "viewers"
        assert viewers["count"] == 1


def test_new_viewer_gets_the_last_50_comments_oldest_first():
    # No rate limit for this test: one user posts 55 comments quickly.
    settings = fast_settings(rate_limit_capacity=1000)
    with TestClient(create_app(settings)) as client:
        with connect(client, "r1", "alice") as alice:
            for number in range(55):
                alice.send_json(comment(f"comment {number}"))
                receive_until(alice, "comment")

        with connect(client, "r1", "bob") as bob:
            history = receive_until(bob, "history")

    texts = [c["text"] for c in history["comments"]]
    assert len(texts) == 50
    assert texts[0] == "comment 5"
    assert texts[-1] == "comment 54"


@pytest.mark.parametrize("path", ["/ws/rooms/bad.room?user=alice", "/ws/rooms/" + "x" * 33 + "?user=alice"])
def test_invalid_room_id_is_refused(client, path):
    with pytest.raises(WebSocketDisconnect) as info:
        with client.websocket_connect(path):
            pass
    assert info.value.code == 1008


@pytest.mark.parametrize("query", ["", "?user=", "?user=al ice"])
def test_missing_or_invalid_user_is_refused(client, query):
    with pytest.raises(WebSocketDisconnect) as info:
        with client.websocket_connect("/ws/rooms/r1" + query):
            pass
    assert info.value.code == 1008


# ---------- comments ----------


def test_viewers_in_the_same_room_receive_each_others_comments(client):
    with connect(client, "r1", "alice") as alice, connect(client, "r1", "bob") as bob:
        alice.send_json(comment("hello from alice"))
        for viewer in (alice, bob):
            event = receive_until(viewer, "comment")
            assert event["user"] == "alice"
            assert event["text"] == "hello from alice"
            assert event["room_id"] == "r1"
            assert event["instance"] == "test"
            assert isinstance(event["sent_at"], int)

        bob.send_json(comment("hi alice"))
        assert receive_until(alice, "comment")["text"] == "hi alice"


def test_viewer_in_another_room_does_not_receive_the_comment(client):
    with connect(client, "r1", "alice") as alice, connect(client, "r2", "carol") as carol:
        alice.send_json(comment("only for room 1"))
        receive_until(alice, "comment")

        # If room 1's comment had leaked to carol, it would arrive before her own.
        carol.send_json(comment("room 2 here"))
        assert receive_until(carol, "comment")["text"] == "room 2 here"


def test_banned_words_are_replaced(client):
    with connect(client, "r1", "alice") as alice:
        alice.send_json(comment("this is a SCAM, you idiot"))
        assert receive_until(alice, "comment")["text"] == "this is a ***, you ***"


def test_comment_text_is_trimmed(client):
    with connect(client, "r1", "alice") as alice:
        alice.send_json(comment("   hello   "))
        assert receive_until(alice, "comment")["text"] == "hello"


def test_200_characters_is_accepted_but_201_is_rejected(client):
    with connect(client, "r1", "alice") as alice:
        alice.send_json(comment("a" * 200))
        assert receive_until(alice, "comment")["text"] == "a" * 200

        alice.send_json(comment("a" * 201))
        error = receive_until(alice, "error")
        assert error["code"] == "invalid_message"
        assert "200" in error["message"]


@pytest.mark.parametrize(
    "raw",
    [
        "not json",
        '{"type": "dance"}',
        '{"type": "comment"}',
        '{"type": "comment", "text": 5}',
        '{"type": "comment", "text": "    "}',
        '["comment"]',
    ],
)
def test_invalid_messages_get_an_error_and_keep_the_connection_open(client, raw):
    with connect(client, "r1", "alice") as alice:
        alice.send_text(raw)
        assert receive_until(alice, "error")["code"] == "invalid_message"

        alice.send_json(comment("still connected"))
        assert receive_until(alice, "comment")["text"] == "still connected"


def test_binary_frames_get_an_error(client):
    with connect(client, "r1", "alice") as alice:
        alice.send_bytes(b"\x00\x01")
        assert receive_until(alice, "error")["code"] == "invalid_message"


# ---------- rate limiting ----------


def test_sixth_quick_comment_is_rate_limited(client):
    with connect(client, "r1", "alice") as alice:
        for number in range(6):
            alice.send_json(comment(f"message {number}"))

        events = [receive_until_any(alice, {"comment", "error"}) for _ in range(6)]

    assert [e["type"] for e in events].count("comment") == 5
    errors = [e for e in events if e["type"] == "error"]
    assert len(errors) == 1
    assert errors[0]["code"] == "rate_limited"


def test_rate_limit_is_per_user(client):
    with connect(client, "r1", "alice") as alice, connect(client, "r1", "bob") as bob:
        for number in range(6):
            alice.send_json(comment(f"spam {number}"))
        assert receive_until(alice, "error")["code"] == "rate_limited"

        bob.send_json(comment("bob is fine"))
        event = receive_until(bob, "comment")
        while event["user"] != "bob":
            event = receive_until(bob, "comment")
        assert event["text"] == "bob is fine"


def receive_until_any(websocket, event_types: set[str]) -> dict:
    while True:
        event = next_event(websocket)
        if event["type"] in event_types:
            return event


# ---------- likes ----------


def test_likes_are_batched_into_few_events():
    # A long interval makes it very likely that all 10 taps land in the same batch.
    with TestClient(create_app(fast_settings(like_flush_interval=0.5))) as client:
        with connect(client, "r1", "alice") as alice:
            for _ in range(10):
                alice.send_json({"type": "like"})

            events = []
            while sum(e["count"] for e in events) < 10:
                events.append(receive_until(alice, "likes"))

    assert sum(e["count"] for e in events) == 10
    assert len(events) <= 2  # one per interval, never one per tap


def test_likes_are_counted_per_room(client):
    with connect(client, "r1", "alice") as alice, connect(client, "r2", "carol") as carol:
        for _ in range(3):
            alice.send_json({"type": "like"})
        carol.send_json({"type": "like"})

        alice_total = 0
        while alice_total < 3:
            event = receive_until(alice, "likes")
            assert event["room_id"] == "r1"
            alice_total += event["count"]
        assert alice_total == 3

        carol_event = receive_until(carol, "likes")
        assert carol_event == {**carol_event, "room_id": "r2", "count": 1}


# ---------- viewer count and disconnects ----------


def test_viewer_count_goes_up_and_down(client):
    with connect(client, "r1", "alice") as alice:
        assert receive_until(alice, "viewers")["count"] == 1

        with connect(client, "r1", "bob") as bob:
            assert receive_until(bob, "viewers")["count"] == 2
            assert receive_until(alice, "viewers")["count"] == 2

        assert receive_until(alice, "viewers")["count"] == 1


def test_disconnect_removes_the_connection(client):
    rooms = client.app.state.service.rooms
    with connect(client, "r1", "alice") as alice:
        receive_until(alice, "viewers")
        assert rooms.connection_count() == 1

    wait_for(lambda: rooms.connection_count() == 0)
    assert rooms.room_count() == 0
