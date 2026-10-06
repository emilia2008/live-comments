import asyncio

from app.rooms import Rooms


class FakeWebSocket:
    """Records what is sent. A stuck one never finishes sending, like a viewer
    whose network stopped reading."""

    def __init__(self, stuck: bool = False) -> None:
        self.sent: list[str] = []
        self.stuck = stuck
        self._never = asyncio.Event()

    async def send_text(self, text: str) -> None:
        if self.stuck:
            await self._never.wait()
        self.sent.append(text)


async def test_broadcast_reaches_everyone_in_the_room_only():
    rooms = Rooms()
    alice_ws, bob_ws, carol_ws = FakeWebSocket(), FakeWebSocket(), FakeWebSocket()
    connections = [
        rooms.join(alice_ws, "r1", "alice"),
        rooms.join(bob_ws, "r1", "bob"),
        rooms.join(carol_ws, "r2", "carol"),
    ]
    senders = [asyncio.create_task(rooms.run_sender(c)) for c in connections]

    rooms.broadcast("r1", "hello room 1")
    await asyncio.sleep(0.01)
    for task in senders:
        task.cancel()

    assert alice_ws.sent == bob_ws.sent == ["hello room 1"]
    assert carol_ws.sent == []
    assert rooms.messages_sent == 2


async def test_a_stuck_viewer_does_not_delay_the_others():
    rooms = Rooms(max_pending=3)
    stuck = rooms.join(FakeWebSocket(stuck=True), "r1", "slow")
    fast_ws = FakeWebSocket()
    fast = rooms.join(fast_ws, "r1", "fast")
    senders = [asyncio.create_task(rooms.run_sender(c)) for c in (stuck, fast)]

    for number in range(10):
        rooms.broadcast("r1", f"message {number}")
        await asyncio.sleep(0)  # let the sender tasks run

    for task in senders:
        task.cancel()

    assert fast_ws.sent == [f"message {n}" for n in range(10)]
    # The stuck viewer's sender holds 1 message, its outbox 3 more; 6 are dropped.
    assert rooms.messages_dropped == 6


async def test_leaving_the_last_viewer_forgets_the_room():
    rooms = Rooms()
    connection = rooms.join(FakeWebSocket(), "r1", "alice")
    assert rooms.viewer_counts() == {"r1": 1}

    rooms.leave(connection)

    assert rooms.viewer_counts() == {}
    assert rooms.room_count() == 0


async def test_messages_waiting_together_go_out_in_one_frame():
    rooms = Rooms()
    websocket = FakeWebSocket()
    connection = rooms.join(websocket, "r1", "alice")
    for number in range(3):  # queued before the sender task gets to run
        rooms.broadcast("r1", f'{{"n": {number}}}')

    sender = asyncio.create_task(rooms.run_sender(connection))
    await asyncio.sleep(0.01)
    rooms.broadcast("r1", '{"n": 3}')  # arrives alone, so it goes out alone
    await asyncio.sleep(0.01)
    sender.cancel()

    assert websocket.sent == ['[{"n": 0},{"n": 1},{"n": 2}]', '{"n": 3}']
    assert rooms.messages_sent == 4
    assert rooms.frames_sent == 2
