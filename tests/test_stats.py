from tests.helpers import connect, receive_until, wait_for


def test_stats_start_at_zero(client):
    stats = client.get("/stats").json()

    assert stats == {
        "instance": "test",
        "connections": 0,
        "rooms": 0,
        "messages_sent": 0,
        "frames_sent": 0,
        "messages_dropped": 0,
        "comments_received": 0,
        "likes_received": 0,
        "rate_limited": 0,
    }


def test_stats_count_connections_rooms_and_messages(client):
    with connect(client, "r1", "alice") as alice, connect(client, "r2", "bob") as bob:
        receive_until(bob, "viewers")
        for number in range(6):  # the 6th is rate limited
            alice.send_json({"type": "comment", "text": f"comment {number}"})
        receive_until(alice, "error")
        alice.send_json({"type": "like"})
        alice.send_json({"type": "like"})
        wait_for(lambda: client.get("/stats").json()["likes_received"] == 2)

        stats = client.get("/stats").json()

    assert stats["connections"] == 2
    assert stats["rooms"] == 2
    assert stats["comments_received"] == 5
    assert stats["rate_limited"] == 1
    # at least: 2 history + 2 viewers + 5 comments + 1 error
    assert stats["messages_sent"] >= 10


def test_metrics_use_prometheus_text_format(client):
    with connect(client, "r1", "alice") as alice:
        receive_until(alice, "viewers")
        response = client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    lines = response.text.splitlines()
    assert "# TYPE live_connections gauge" in lines
    assert "live_connections 1" in lines
    assert "live_rooms 1" in lines
    assert "# TYPE live_messages_sent_total counter" in lines
    assert "live_rate_limited_total 0" in lines
