"""FairMultiFIFOQueue tests."""

import threading
from time import time

import pytest


@pytest.fixture
def clean_queue(queue):
    """A clean queue init and teardown"""
    yield queue

    # Clean the dirty queue
    queue.join()

    # Check for clean shutdown
    assert queue.qsize == 0
    assert queue.inprogress_size == 0


def test_emptyget(clean_queue):
    """Try to get() nothing."""
    assert clean_queue.get(timeout=0.1) is None


def test_putget(clean_queue):
    """Test synchronous put -> get -> task_done."""

    assert clean_queue.qsize == 0
    assert clean_queue.inprogress_size == 0
    assert clean_queue.fifo_size("fifo") == 0  # "fifo" doesn't exist yet

    clean_queue.put("item", "fifo")

    assert clean_queue.qsize == 1
    assert clean_queue.inprogress_size == 0
    assert clean_queue.fifo_size("fifo") == 1

    assert clean_queue.get() == ("item", "fifo")

    assert clean_queue.qsize == 0
    assert clean_queue.inprogress_size == 1
    assert clean_queue.fifo_size("fifo") == 1  # includes in-progress

    clean_queue.task_done("fifo")

    assert clean_queue.qsize == 0
    assert clean_queue.inprogress_size == 0
    assert clean_queue.fifo_size("fifo") == 0


def test_deferred(clean_queue):
    """Test deferred put."""
    clean_queue.put("item", "fifo", wait=0.1)

    # This won't be sooner than the expiry time of the deferred put
    timeout = time() + 0.1

    assert clean_queue.qsize == 0
    assert clean_queue.deferred_size == 1

    while True:
        item = clean_queue.get(timeout=0.1)

        if item is None:
            assert clean_queue.qsize == 0
            assert clean_queue.deferred_size == 1
        else:
            assert item == ("item", "fifo")
            assert clean_queue.qsize == 0
            assert clean_queue.deferred_size == 0
            break

        # The get should finish before the timeout
        assert time() < timeout

    clean_queue.task_done("fifo")


def test_wakeget(clean_queue):
    """Test waking up a get from a put."""

    # Consumer thread
    def consumer(clean_queue):
        assert clean_queue.get() == (1, "fifo")
        clean_queue.task_done("fifo")

    # Start the consumer
    thread = threading.Thread(target=consumer, args=(clean_queue,), daemon=True)
    thread.start()

    # Now put something
    clean_queue.put(1, "fifo")

    # Join the consumer
    thread.join()

    # Queue should be empty
    assert clean_queue.qsize == 0
    assert clean_queue.inprogress_size == 0


def test_concurrency(clean_queue):
    """Test concurrent puts and gets"""

    # Threads to join later
    threads = []

    # Producer thread
    def producer(clean_queue, fifo):
        for i in range(100):
            clean_queue.put(i, fifo)

    # Consumer thread
    def consumer(clean_queue):
        for i in range(100):
            item, key = clean_queue.get()
            clean_queue.task_done(key)

    # Create a bunch of consumers
    for i in range(10):
        threads.append(
            threading.Thread(target=consumer, args=(clean_queue,), daemon=True)
        )

    # Create the same number producers
    for i in range(10):
        threads.append(
            threading.Thread(target=producer, args=(clean_queue, i), daemon=True)
        )

    # Start all the threads
    for thread in threads:
        thread.start()

    # Wait for the test to complete
    for thread in threads:
        thread.join()

    # There shouldn't be anything left
    assert clean_queue.qsize == 0
    assert clean_queue.inprogress_size == 0


def test_exclusive(clean_queue):
    """Test exclusive puts and gets."""

    # queue some things.  The second thing is exclusive
    clean_queue.put(1, "fifo")
    clean_queue.put(2, "fifo", exclusive=True)
    clean_queue.put(3, "fifo")

    assert clean_queue.qsize == 3
    assert clean_queue.inprogress_size == 0

    # Get the first thing
    item, key = clean_queue.get(timeout=0.1)
    assert item == 1

    # Fail to get the second thing because
    # there's something in-progress from the queue
    assert clean_queue.get(timeout=0.1) is None

    # But getting something from another FIFO
    # should work.
    clean_queue.put(4, "fifo2")
    item, key2 = clean_queue.get(timeout=0.1)
    clean_queue.task_done(key2)

    assert item == 4
    assert key2 == "fifo2"

    # Finish the first thing
    clean_queue.task_done(key)

    # Now we can get the second thing
    item, key = clean_queue.get(timeout=0.1)
    assert item == 2

    # Fail to get the third thing because
    # the fifo is locked during exclusive task
    assert clean_queue.get(timeout=0.1) is None

    # Finish the second thing, unlocking the fifo
    clean_queue.task_done(key)

    # Now we can get the third thing
    item, key = clean_queue.get(timeout=0.1)
    assert item == 3
    clean_queue.task_done(key)

    # Everything's taken care of
    assert clean_queue.qsize == 0
    assert clean_queue.inprogress_size == 0
