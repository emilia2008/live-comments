from app.likes import LikeBatcher


def test_adds_up_likes_per_room():
    batcher = LikeBatcher()
    batcher.add("room-a")
    batcher.add("room-a")
    batcher.add("room-b", count=3)

    assert batcher.flush() == {"room-a": 2, "room-b": 3}


def test_flush_resets_counts_to_zero():
    batcher = LikeBatcher()
    batcher.add("room-a")
    batcher.flush()

    assert batcher.flush() == {}


def test_likes_after_flush_start_a_new_batch():
    batcher = LikeBatcher()
    batcher.add("room-a", count=10)
    batcher.flush()
    batcher.add("room-a")

    assert batcher.flush() == {"room-a": 1}


def test_flush_on_empty_batcher_returns_empty_dict():
    assert LikeBatcher().flush() == {}
