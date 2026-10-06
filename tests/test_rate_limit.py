from app.rate_limit import RateLimiter, TokenBucket
from tests.helpers import FakeClock


def test_allows_five_in_a_row_then_blocks_the_sixth():
    clock = FakeClock()
    bucket = TokenBucket(capacity=5, refill_per_second=1, clock=clock)

    assert [bucket.allow() for _ in range(5)] == [True] * 5
    assert bucket.allow() is False


def test_one_second_later_allows_exactly_one_more():
    clock = FakeClock()
    bucket = TokenBucket(capacity=5, refill_per_second=1, clock=clock)
    for _ in range(5):
        bucket.allow()

    clock.advance(1.0)

    assert bucket.allow() is True
    assert bucket.allow() is False


def test_half_a_second_is_not_enough_for_a_token():
    clock = FakeClock()
    bucket = TokenBucket(capacity=5, refill_per_second=1, clock=clock)
    for _ in range(5):
        bucket.allow()

    clock.advance(0.5)
    assert bucket.allow() is False
    clock.advance(0.5)
    assert bucket.allow() is True


def test_never_holds_more_than_capacity():
    clock = FakeClock()
    bucket = TokenBucket(capacity=5, refill_per_second=1, clock=clock)

    clock.advance(3600)  # a long idle time must not create a huge burst

    assert [bucket.allow() for _ in range(5)] == [True] * 5
    assert bucket.allow() is False


def test_is_full_only_after_refilling_completely():
    clock = FakeClock()
    bucket = TokenBucket(capacity=5, refill_per_second=1, clock=clock)
    assert bucket.is_full()

    bucket.allow()
    assert not bucket.is_full()

    clock.advance(1.0)
    assert bucket.is_full()


def test_rate_limiter_keeps_users_separate():
    clock = FakeClock()
    limiter = RateLimiter(capacity=5, refill_per_second=1, clock=clock)
    for _ in range(5):
        limiter.allow("alice")

    assert limiter.allow("alice") is False
    assert limiter.allow("bob") is True


def test_forget_idle_users_drops_only_full_buckets():
    clock = FakeClock()
    limiter = RateLimiter(capacity=5, refill_per_second=1, clock=clock)
    limiter.allow("alice")
    for _ in range(5):
        limiter.allow("bob")

    clock.advance(1.0)  # alice is full again, bob has 1 of 5 tokens
    limiter.forget_idle_users()

    assert len(limiter) == 1
    assert limiter.allow("bob") is True
    assert limiter.allow("bob") is False
