"""FairMultiFIFOQueue tests."""

import pytest

from alpenhorn.fairmultififo import FairMultiFIFOQueue

@pytest.fixture
def queue():
    """Queue init and teardown"""
    queue = FairMultiFIFOQueue()

    yield queue

    queue.join()
    assert queue.qsize == 0
    assert queue.inprogress_size == 0

def test_emptyget(queue):
    """Try to get() nothing."""
    assert queue.get(timeout=1) is None

def test_putget(queue):
    """Test synchronous put -> get -> task_done."""

    assert queue.qsize == 0
    assert queue.inprogress_size == 0
    assert queue.fifo_size("fifo") == 0 # "fifo" doesn't exist yet

    queue.put("item", "fifo")

    assert queue.qsize == 1
    assert queue.inprogress_size == 0
    assert queue.fifo_size("fifo") == 1

    assert queue.get() == ("item", "fifo")

    assert queue.qsize == 0
    assert queue.inprogress_size == 1
    assert queue.fifo_size("fifo") == 1 # includes in-progress

    queue.task_done("fifo")

    assert queue.qsize == 0
    assert queue.inprogress_size == 0
    assert queue.fifo_size("fifo") == 0

def test_deferred(queue):
    """Test deferred put."""
    queue.put("item", "fifo", wait=2)
    assert queue.qsize == 0
    assert queue.deferred_size == 1

    assert queue.get(timeout=1) is None
    assert queue.qsize == 0
    assert queue.deferred_size == 1

    assert queue.get() == ("item", "fifo")
    assert queue.qsize == 0
    assert queue.deferred_size == 0
    queue.task_done("fifo")
