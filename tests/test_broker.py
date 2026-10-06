import asyncio

from app.broker import MemoryBroker


async def start_listener(broker: MemoryBroker, patterns: list[str]) -> tuple[list, asyncio.Task]:
    received: list[tuple[str, str]] = []

    async def handler(channel: str, data: str) -> None:
        received.append((channel, data))

    task = asyncio.create_task(broker.listen(patterns, handler))
    await asyncio.sleep(0)  # let the listener register itself
    return received, task


async def test_delivers_messages_matching_the_pattern_in_order():
    broker = MemoryBroker()
    received, task = await start_listener(broker, ["room:*"])

    await broker.publish("room:a", "first")
    await broker.publish("other", "ignored")
    await broker.publish("room:b", "second")
    await asyncio.sleep(0.01)
    task.cancel()

    assert received == [("room:a", "first"), ("room:b", "second")]


async def test_every_listener_gets_every_message():
    broker = MemoryBroker()
    received_1, task_1 = await start_listener(broker, ["room:*"])
    received_2, task_2 = await start_listener(broker, ["room:*"])

    await broker.publish("room:a", "hello")
    await asyncio.sleep(0.01)
    task_1.cancel()
    task_2.cancel()

    assert received_1 == received_2 == [("room:a", "hello")]


async def test_stopped_listener_is_removed():
    broker = MemoryBroker()
    _, task = await start_listener(broker, ["room:*"])
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    await broker.publish("room:a", "nobody listens")  # must not fail
    assert broker._listeners == []
