"""Check that two backend instances really share comments, likes and viewer counts.

Alice connects to the first URL, Bob to the second, both in the same room.
Usage:
    python loadtest/cross_instance_check.py ws://localhost:8001 ws://localhost:8002
    python loadtest/cross_instance_check.py ws://localhost:8080 ws://localhost:8080 --allow-same-instance
    python loadtest/cross_instance_check.py wss://live-comments-dj5v.onrender.com wss://live-comments-dj5v.onrender.com --allow-same-instance
Exit code 0 means everything arrived on the other viewer's instance.
"""

import argparse
import asyncio
import json
import sys
import time

from websockets.asyncio.client import connect


class Viewer:
    """One WebSocket plus the events it received but nobody looked at yet.

    Waiting for one kind of event must not throw away the others (a frame can also
    hold several events), and the latest viewer count is remembered because the
    server sends it only when it changes.
    """

    def __init__(self, websocket) -> None:
        self.websocket = websocket
        self.pending: list[dict] = []
        self.viewer_count: int | None = None

    async def wait_for(self, predicate, timeout: float = 5.0) -> dict:
        deadline = time.monotonic() + timeout
        while True:
            while self.pending:
                event = self.pending.pop(0)
                if event["type"] == "viewers":
                    self.viewer_count = event["count"]
                if predicate(event):
                    return event
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("expected event did not arrive in time")
            data = json.loads(await asyncio.wait_for(self.websocket.recv(), remaining))
            self.pending.extend(data if isinstance(data, list) else [data])  # one event or a batch

    async def wait_for_viewer_count(self, count: int) -> None:
        if self.viewer_count != count:
            await self.wait_for(lambda e: e["type"] == "viewers" and e["count"] == count)


async def check(url_a: str, url_b: str, allow_same_instance: bool) -> None:
    room = f"check-{int(time.time())}"
    async with connect(f"{url_a}/ws/rooms/{room}?user=alice") as alice_ws, connect(
        f"{url_b}/ws/rooms/{room}?user=bob"
    ) as bob_ws:
        alice, bob = Viewer(alice_ws), Viewer(bob_ws)
        instance_a = (await alice.wait_for(lambda e: e["type"] == "history"))["instance"]
        instance_b = (await bob.wait_for(lambda e: e["type"] == "history"))["instance"]
        print(f"alice is on {instance_a}, bob is on {instance_b}")
        if instance_a == instance_b and not allow_same_instance:
            raise AssertionError("both viewers landed on the same instance")

        # Alice and Bob both run here, so one local clock times the whole trip. (Comparing
        # with the server's sent_at would mix two machines' clocks, which can differ by seconds.)
        started = time.perf_counter()
        await alice_ws.send(json.dumps({"type": "comment", "text": "hello from the other side"}))
        event = await bob.wait_for(lambda e: e["type"] == "comment")
        latency = (time.perf_counter() - started) * 1000
        print(f"OK comment: bob got {event['text']!r} from {event['instance']} in {latency:.1f} ms")

        await alice_ws.send(json.dumps({"type": "like"}))
        event = await bob.wait_for(lambda e: e["type"] == "likes")
        print(f"OK likes: bob got a likes event with count={event['count']}")

        await alice.wait_for_viewer_count(2)
        await bob.wait_for_viewer_count(2)
        print("OK viewers: both see 2 viewers")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("url_a", help="first backend, e.g. ws://localhost:8001")
    parser.add_argument("url_b", help="second backend, e.g. ws://localhost:8002")
    parser.add_argument(
        "--allow-same-instance",
        action="store_true",
        help="behind a load balancer both viewers may land on the same backend",
    )
    args = parser.parse_args()
    try:
        asyncio.run(check(args.url_a.rstrip("/"), args.url_b.rstrip("/"), args.allow_same_instance))
    except Exception as error:
        print(f"FAILED: {error!r}")
        sys.exit(1)
    print("All cross-instance checks passed.")


if __name__ == "__main__":
    main()
