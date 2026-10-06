from app.viewers import ViewerCounter
from tests.helpers import FakeClock


def test_adds_up_reports_from_all_instances():
    counter = ViewerCounter(stale_after=5, clock=FakeClock())
    counter.update("backend1", {"r1": 10, "r2": 3})
    counter.update("backend2", {"r1": 5})

    assert counter.total("r1") == 15
    assert counter.total("r2") == 3
    assert counter.total("unknown") == 0


def test_newer_report_replaces_older_one():
    counter = ViewerCounter(stale_after=5, clock=FakeClock())
    counter.update("backend1", {"r1": 10})
    counter.update("backend1", {"r1": 4})

    assert counter.total("r1") == 4


def test_ignores_reports_from_an_instance_that_went_silent():
    clock = FakeClock()
    counter = ViewerCounter(stale_after=5, clock=clock)
    counter.update("crashed", {"r1": 100})
    clock.advance(3)
    counter.update("alive", {"r1": 7})

    clock.advance(3)  # "crashed" is now 6 s old, "alive" 3 s old

    assert counter.total("r1") == 7


def test_forget_stale_removes_old_reports():
    clock = FakeClock()
    counter = ViewerCounter(stale_after=5, clock=clock)
    counter.update("crashed", {"r1": 100})
    clock.advance(6)
    counter.forget_stale()

    assert counter._reports == {}
