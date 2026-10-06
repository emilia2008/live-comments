"""Prometheus text format for /metrics.

The format is simple enough to write by hand, so no extra library is needed:
    # HELP live_connections Open WebSocket connections on this instance.
    # TYPE live_connections gauge
    live_connections 42
"""

# (metric name, Prometheus type, key in LiveService.stats(), help text)
METRICS = [
    ("live_connections", "gauge", "connections", "Open WebSocket connections on this instance."),
    ("live_rooms", "gauge", "rooms", "Rooms with at least one viewer on this instance."),
    ("live_messages_sent_total", "counter", "messages_sent", "WebSocket messages sent to viewers."),
    ("live_comments_received_total", "counter", "comments_received", "Comments accepted from viewers."),
    ("live_likes_received_total", "counter", "likes_received", "Likes received from viewers."),
    ("live_rate_limited_total", "counter", "rate_limited", "Comments rejected by the rate limiter."),
]


def to_prometheus(stats: dict) -> str:
    lines = []
    for name, metric_type, key, help_text in METRICS:
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} {metric_type}")
        lines.append(f"{name} {stats[key]}")
    return "\n".join(lines) + "\n"
