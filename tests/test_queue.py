"""FairMultiFIFOQueue tests."""

import pytest
import threading
from time import time

from alpenhorn.queue import FairMultiFIFOQueue


@pytest.fixture
def queue():
    """Queue init and teardown"""
    queue = FairMultiFIFOQueue()

    yield queue

    queue.join()

    # Check for clean shutdown
    assert queue.qsize == 0
    assert queue.inprogress_size == 0


def test_emptyget(queue):
    """Try to get() nothing."""
    assert queue.get(timeout=1) is None


def test_putget(queue):
    """Test synchronous put -> get -> task_done."""

    assert queue.qsize == 0
    assert queue.inprogress_size == 0
    assert queue.fifo_size("fifo") == 0  # "fifo" doesn't exist yet

    queue.put("item", "fifo")

    assert queue.qsize == 1
    assert queue.inprogress_size == 0
    assert queue.fifo_size("fifo") == 1

    assert queue.get() == ("item", "fifo")

    assert queue.qsize == 0
    assert queue.inprogress_size == 1
    assert queue.fifo_size("fifo") == 1  # includes in-progress

    queue.task_done("fifo")

    assert queue.qsize == 0
    assert queue.inprogress_size == 0
    assert queue.fifo_size("fifo") == 0


def test_deferred(queue):
    """Test deferred put."""
    queue.put("item", "fifo", wait=2)
    timeout = (
        time() + 2
    )  # This can't be sooner than the expiry time of the deferred put
    assert queue.qsize == 0
    assert queue.deferred_size == 1

    while True:
        item = queue.get(timeout=1)

        if item is None:
            assert queue.qsize == 0
            assert queue.deferred_size == 1
        else:
            assert item == ("item", "fifo")
            assert queue.qsize == 0
            assert queue.deferred_size == 0
            break

        # The get should finish before the timeout
        assert time() < timeout

    queue.task_done("fifo")


def test_wakeget(queue):
    """Test waking up a get from a put."""

    # Consumer thread
    def consumer(queue):
        assert queue.get() == (1, "fifo")
        queue.task_done("fifo")

    # Start the consumer
    thread = threading.Thread(target=consumer, args=(queue,), daemon=True)
    thread.start()

    # Now put something
    queue.put(1, "fifo")

    # Join the consumer
    thread.join()

    # Queue should be empty
    assert queue.qsize == 0
    assert queue.inprogress_size == 0


def test_concurrency(queue):
    """Test concurrent puts and gets"""

    # Threads to join later
    threads = list()

    # Producer thread
    def producer(queue, fifo):
        for i in range(100):
            queue.put(i, fifo)

    # Consumer thread
    def consumer(queue):
        for i in range(100):
            item, key = queue.get()
            queue.task_done(key)

    # Create a bunch of consumers
    for i in range(10):
        threads.append(threading.Thread(target=consumer, args=(queue,), daemon=True))

    # Create the same number producers
    for i in range(10):
        threads.append(threading.Thread(target=producer, args=(queue, i), daemon=True))

    # Start all the threads
    for thread in threads:
        thread.start()

    # Wait for the test to complete
    for thread in threads:
        thread.join()

    # There shouldn't be anything left
    assert queue.qsize == 0
    assert queue.inprogress_size == 0
