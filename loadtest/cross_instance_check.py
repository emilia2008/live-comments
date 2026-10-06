"""Check that two backend instances really share comments, likes and viewer counts.

Alice connects to the first URL, Bob to the second, both in the same room.
Usage:
    python loadtest/cross_instance_check.py ws://localhost:8001 ws://localhost:8002
    python loadtest/cross_instance_check.py ws://localhost:8080 ws://localhost:8080 --allow-same-instance
Exit code 0 means everything arrived on the other viewer's instance.
"""

import argparse
import asyncio
import json
import sys
import time

from websockets.asyncio.client import connect


async def receive_until(websocket, predicate, timeout: float = 5.0) -> dict:
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("expected event did not arrive in time")
        data = json.loads(await asyncio.wait_for(websocket.recv(), remaining))
        for event in data if isinstance(data, list) else [data]:  # one event or a batch
            if predicate(event):
                return event


async def check(url_a: str, url_b: str, allow_same_instance: bool) -> None:
    room = f"check-{int(time.time())}"
    async with connect(f"{url_a}/ws/rooms/{room}?user=alice") as alice, connect(
        f"{url_b}/ws/rooms/{room}?user=bob"
    ) as bob:
        instance_a = (await receive_until(alice, lambda e: e["type"] == "history"))["instance"]
        instance_b = (await receive_until(bob, lambda e: e["type"] == "history"))["instance"]
        print(f"alice is on {instance_a}, bob is on {instance_b}")
        if instance_a == instance_b and not allow_same_instance:
            raise AssertionError("both viewers landed on the same instance")

        await alice.send(json.dumps({"type": "comment", "text": "hello from the other side"}))
        event = await receive_until(bob, lambda e: e["type"] == "comment")
        latency = time.time() * 1000 - event["sent_at"]
        print(f"OK comment: bob got {event['text']!r} from {event['instance']} in {latency:.1f} ms")

        await alice.send(json.dumps({"type": "like"}))
        event = await receive_until(bob, lambda e: e["type"] == "likes")
        print(f"OK likes: bob got a likes event with count={event['count']}")

        await receive_until(alice, lambda e: e["type"] == "viewers" and e["count"] == 2)
        await receive_until(bob, lambda e: e["type"] == "viewers" and e["count"] == 2)
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
