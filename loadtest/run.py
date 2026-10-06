"""Load test: thousands of viewers in many rooms, some of them commenting.

Viewer i joins room i % rooms and connects to URL i % len(urls), so passing two URLs
spreads the viewers over two backends like a load balancer would.
The first `senders` viewers also send one comment per second (the rate limit allows that).

Latency = time a viewer receives a comment - time the sender sent it.
The sender writes its send time into the comment text. Times come from
time.perf_counter(): precise to 0.1 microsecond and shared by all processes on one
machine. (time.time() on Windows + Python 3.12 can be as coarse as 15.6 ms.)

The viewers are split over several processes, because one Python process uses only
one CPU core and the load generator must not become the bottleneck.

--stuck-viewers N adds N extra viewers that never read (like a phone that lost signal).
They are not measured; the question is whether they slow down everybody else.

Example:
    python loadtest/run.py --url ws://localhost:8001 --url ws://localhost:8002 \\
        --viewers 500,2000,5000 --rooms 10 --senders 100 --duration 30
"""

import argparse
import asyncio
import json
import math
import multiprocessing
import random
import socket
import time
import uuid
from array import array
from collections import Counter
from dataclasses import dataclass, field
from urllib.parse import urlsplit

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed


CONNECT_ATTEMPTS = 5


@dataclass
class Config:
    urls: list[str]
    viewers: int
    rooms: int
    senders: int
    rate: float  # comments per second per sender
    duration: float  # seconds of sending
    grace: float  # seconds to wait for the last comments to arrive
    processes: int
    run_id: str
    stuck_viewers: int = 0


@dataclass
class WorkerResult:
    connected: int = 0
    connect_errors: int = 0
    dropped: int = 0  # connections the server closed during the test
    comments_sent: int = 0
    comments_received: int = 0
    rate_limited: int = 0
    other_errors: int = 0
    connect_error_kinds: Counter = field(default_factory=Counter)
    viewers_per_room: Counter = field(default_factory=Counter)
    sent_per_room: Counter = field(default_factory=Counter)  # accepted comments only
    latencies_ms: array = field(default_factory=lambda: array("d"))


# ---------------------------------------------------------------------------
# Worker process: owns a slice of the viewers
# ---------------------------------------------------------------------------


def room_of(config: Config, index: int) -> str:
    return f"lt-{config.run_id}-{index % config.rooms}"


async def read_events(websocket, room: str, result: WorkerResult, stopping: asyncio.Event) -> None:
    try:
        async for raw in websocket:
            received_at = time.perf_counter()  # take the time before any parsing
            data = json.loads(raw)
            for event in data if isinstance(data, list) else [data]:  # one event or a batch
                if event["type"] == "comment":
                    sent_at = float(event["text"].rsplit("t=", 1)[1])
                    result.latencies_ms.append((received_at - sent_at) * 1000)
                    result.comments_received += 1
                elif event["type"] == "error":
                    if event["code"] == "rate_limited":
                        result.rate_limited += 1
                        result.sent_per_room[room] -= 1  # that comment was not accepted
                    else:
                        result.other_errors += 1
    except ConnectionClosed:
        pass
    if not stopping.is_set():
        result.dropped += 1


async def send_comments(websocket, config: Config, room: str, result: WorkerResult, stop_at: float) -> None:
    interval = 1 / config.rate
    await asyncio.sleep(random.uniform(0, interval))  # spread the senders evenly over time
    next_send = time.perf_counter()
    while next_send < stop_at:
        text = f"load test t={time.perf_counter():.7f}"
        try:
            await websocket.send(json.dumps({"type": "comment", "text": text}))
        except ConnectionClosed:
            return
        result.comments_sent += 1
        result.sent_per_room[room] += 1
        next_send += interval
        await asyncio.sleep(max(0.0, next_send - time.perf_counter()))


async def run_worker(config: Config, indices: list[int], stuck_indices: list[int], barrier) -> WorkerResult:
    result = WorkerResult()
    stopping = asyncio.Event()
    # Windows (desktop editions) refuses connections when ~200 handshakes wait at once.
    limit = asyncio.Semaphore(50)
    sockets: dict[int, object] = {}
    readers: list[asyncio.Task] = []

    async def open_viewer(index: int) -> None:
        url = config.urls[index % len(config.urls)]
        room = room_of(config, index)
        websocket = None
        for attempt in range(CONNECT_ATTEMPTS):  # real apps retry too
            try:
                async with limit:
                    websocket = await connect(
                        f"{url}/ws/rooms/{room}?user=u{index}",
                        open_timeout=60,
                        ping_interval=None,  # the server pings; no need for both sides to do it
                        max_size=None,
                    )
                break
            except Exception as error:
                last_error = error
                await asyncio.sleep(0.2 * 2**attempt + random.uniform(0, 0.2))
        if websocket is None:
            result.connect_errors += 1
            result.connect_error_kinds[type(last_error).__name__] += 1
            return
        sockets[index] = websocket
        result.connected += 1
        result.viewers_per_room[room] += 1
        readers.append(asyncio.create_task(read_events(websocket, room, result, stopping)))

    async def open_stuck_viewer(index: int) -> None:
        """A viewer that never reads. A tiny receive buffer makes the server's
        send buffer for this socket fill up quickly."""
        url = config.urls[index % len(config.urls)]
        parts = urlsplit(url)
        sock = socket.socket()
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        sock.setblocking(False)
        try:
            await asyncio.get_running_loop().sock_connect(sock, (parts.hostname, parts.port))
            websocket = await connect(
                f"{url}/ws/rooms/{room_of(config, index)}?user=stuck{index}",
                sock=sock, max_queue=1, ping_interval=None, close_timeout=1,
            )
        except Exception:
            result.connect_errors += 1
            return
        stuck_sockets.append(websocket)

    stuck_sockets: list = []
    await asyncio.gather(*(open_viewer(i) for i in indices))
    await asyncio.gather(*(open_stuck_viewer(i) for i in stuck_indices))

    # Wait until every process has connected all its viewers, then start together.
    await asyncio.to_thread(barrier.wait)
    stop_at = time.perf_counter() + config.duration
    senders = [
        asyncio.create_task(send_comments(sockets[i], config, room_of(config, i), result, stop_at))
        for i in indices
        if i < config.senders and i in sockets
    ]
    await asyncio.gather(*senders)
    await asyncio.sleep(config.grace)

    stopping.set()
    for websocket in stuck_sockets:
        websocket.transport.abort()
    await asyncio.gather(*(ws.close() for ws in sockets.values()), return_exceptions=True)
    for task in readers:
        task.cancel()
    await asyncio.gather(*readers, return_exceptions=True)
    return result


def worker_entry(config: Config, indices: list[int], stuck_indices: list[int], barrier, results) -> None:
    results.put(asyncio.run(run_worker(config, indices, stuck_indices, barrier)))


# ---------------------------------------------------------------------------
# Parent process: starts workers, merges their results, prints a table
# ---------------------------------------------------------------------------


def run_level(config: Config) -> dict:
    context = multiprocessing.get_context("spawn")
    barrier = context.Barrier(config.processes + 1)  # +1: the parent waits too
    results_queue = context.Queue()
    workers = [
        context.Process(
            target=worker_entry,
            args=(
                config,
                list(range(w, config.viewers, config.processes)),
                list(range(config.viewers + w, config.viewers + config.stuck_viewers, config.processes)),
                barrier,
                results_queue,
            ),
        )
        for w in range(config.processes)
    ]
    started = time.perf_counter()
    for worker in workers:
        worker.start()
    barrier.wait(timeout=600)
    connect_seconds = time.perf_counter() - started
    print(f"  {config.viewers} viewers connected in {connect_seconds:.1f} s, sending for {config.duration:.0f} s...")

    results = [results_queue.get(timeout=config.duration + config.grace + 300) for _ in workers]
    for worker in workers:
        worker.join()
    return summarize(config, results)


def summarize(config: Config, results: list[WorkerResult]) -> dict:
    viewers_per_room: Counter = Counter()
    sent_per_room: Counter = Counter()
    latencies = array("d")
    totals = Counter()
    error_kinds: Counter = Counter()
    for r in results:
        error_kinds.update(r.connect_error_kinds)
        viewers_per_room.update(r.viewers_per_room)
        sent_per_room.update(r.sent_per_room)
        latencies.extend(r.latencies_ms)
        for name in ("connected", "connect_errors", "dropped", "comments_sent",
                     "comments_received", "rate_limited", "other_errors"):
            totals[name] += getattr(r, name)

    # Every accepted comment should reach every viewer of its room.
    expected = sum(sent * viewers_per_room[room] for room, sent in sent_per_room.items())
    ordered = sorted(latencies)
    return {
        "instances": len(config.urls),
        "viewers": totals["connected"],
        "rooms": config.rooms,
        "senders": config.senders,
        "comments_per_s": totals["comments_sent"] / config.duration,
        "deliveries_per_s": totals["comments_received"] / config.duration,
        "p50": percentile(ordered, 50),
        "p95": percentile(ordered, 95),
        "p99": percentile(ordered, 99),
        "max": ordered[-1] if ordered else float("nan"),
        "delivered_pct": 100 * totals["comments_received"] / expected if expected else float("nan"),
        "errors": totals["connect_errors"] + totals["dropped"] + totals["other_errors"] + totals["rate_limited"],
        "error_detail": (
            f"connect={totals['connect_errors']} {dict(error_kinds) or ''} dropped={totals['dropped']} "
            f"server_errors={totals['other_errors']} rate_limited={totals['rate_limited']}"
        ),
    }


def percentile(ordered: list[float], p: float) -> float:
    """Nearest-rank percentile of an already sorted list."""
    if not ordered:
        return float("nan")
    rank = max(1, math.ceil(p / 100 * len(ordered)))
    return ordered[rank - 1]


def print_table(rows: list[dict]) -> None:
    print()
    print("| Instances | Viewers | Rooms | Senders | Comments in/s | Deliveries out/s "
          "| p50 ms | p95 ms | p99 ms | max ms | Delivered | Errors |")
    print("|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for r in rows:
        print(f"| {r['instances']} | {r['viewers']} | {r['rooms']} | {r['senders']} "
              f"| {r['comments_per_s']:.0f} | {r['deliveries_per_s']:,.0f} "
              f"| {r['p50']:.1f} | {r['p95']:.1f} | {r['p99']:.1f} | {r['max']:.0f} "
              f"| {r['delivered_pct']:.2f}% | {r['errors']} |")
    for r in rows:
        if r["errors"]:
            print(f"Errors at {r['viewers']} viewers: {r['error_detail']}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Live Comments load test")
    parser.add_argument("--url", action="append", required=True,
                        help="backend WebSocket base URL; repeat to spread viewers over several backends")
    parser.add_argument("--viewers", default="500",
                        help="number of WebSocket connections; a comma list runs several levels")
    parser.add_argument("--rooms", type=int, default=10)
    parser.add_argument("--senders", type=int, default=100, help="viewers that also comment")
    parser.add_argument("--rate", type=float, default=1.0, help="comments per second per sender")
    parser.add_argument("--duration", type=float, default=30, help="seconds of sending")
    parser.add_argument("--grace", type=float, default=3, help="seconds to wait for late comments")
    parser.add_argument("--processes", type=int, default=4, help="load generator processes")
    parser.add_argument("--pause", type=float, default=10, help="seconds between levels")
    parser.add_argument("--stuck-viewers", type=int, default=0,
                        help="extra viewers that never read, to test slow consumers")
    args = parser.parse_args()

    rows = []
    levels = [int(v) for v in args.viewers.split(",")]
    for number, viewers in enumerate(levels):
        if number:
            time.sleep(args.pause)  # let the servers settle between levels
        config = Config(
            urls=[u.rstrip("/") for u in args.url],
            viewers=viewers,
            rooms=args.rooms,
            senders=min(args.senders, viewers),
            rate=args.rate,
            duration=args.duration,
            grace=args.grace,
            processes=args.processes,
            run_id=uuid.uuid4().hex[:6],  # fresh rooms: no old history, no old viewers
            stuck_viewers=args.stuck_viewers,
        )
        print(f"Level {number + 1}/{len(levels)}: {viewers} viewers on {len(config.urls)} instance(s)")
        rows.append(run_level(config))
    print_table(rows)


if __name__ == "__main__":
    main()
