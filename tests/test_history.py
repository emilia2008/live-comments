from app.history import MemoryHistory


async def test_recent_returns_comments_oldest_first():
    history = MemoryHistory(size=50)
    for number in range(3):
        await history.add("r1", f"comment {number}")

    assert await history.recent("r1") == ["comment 0", "comment 1", "comment 2"]


async def test_keeps_only_the_last_n_comments():
    history = MemoryHistory(size=50)
    for number in range(60):
        await history.add("r1", f"comment {number}")

    recent = await history.recent("r1")
    assert len(recent) == 50
    assert recent[0] == "comment 10"
    assert recent[-1] == "comment 59"


async def test_rooms_have_separate_history():
    history = MemoryHistory(size=50)
    await history.add("r1", "in room 1")

    assert await history.recent("r2") == []
