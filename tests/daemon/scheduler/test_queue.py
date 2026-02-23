"""FairMultiFIFOQueue tests."""

import threading
from time import time
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def clean_queue(queue):
    """A clean queue init and teardown"""
    yield queue

    # Check that the queue is clean by pulling everything out
    count = 0
    while queue.qsize > 0:
        _, key = queue.get()
        queue.task_done(key)
        count += 1

    queue.join()

    # Check for clean shutdown
    assert count == 0, "Queue not clean during teardown!"
    assert queue.inprogress_size == 0, "Task(s) left in-progress during teardown!"


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
            _, key = clean_queue.get()
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


def test_label(clean_queue):
    """Test label_fifo()"""

    clean_queue.label_fifo(key="fifo", label="label")

    # Check that the label is used
    mock = MagicMock()
    with patch("alpenhorn.daemon.metrics.Metric.add", mock):
        clean_queue.put(None, "fifo")
        clean_queue.get()
        clean_queue.task_done("fifo")

        # Also try an unlabelled fifo
        clean_queue.put(None, "fifo2")
        clean_queue.get()
        clean_queue.task_done("fifo2")

    mock.assert_any_call(1, fifo="label", status="queued")
    mock.assert_any_call(-1, fifo="label", status="queued")
    mock.assert_any_call(1, fifo="fifo2", status="queued")
    mock.assert_any_call(-1, fifo="fifo2", status="queued")


def test_clear_missing(clean_queue):
    """Clearing a non-existent FIFO should work."""

    assert clean_queue.clear_fifo("MISSING") == (0, 0)


def test_clear_nokeep(clean_queue):
    """Test put after clearing a FIFO without keep_clear."""

    assert clean_queue.clear_fifo("fifo") == (0, 0)

    clean_queue.put(None, "fifo")
    assert clean_queue.qsize == 1

    # Clean up
    _, key = clean_queue.get()
    clean_queue.task_done(key)


def test_clear_keep(clean_queue):
    """Test keeping a FIFO clear."""

    assert clean_queue.clear_fifo("fifo", keep_clear=True) == (0, 0)

    with pytest.raises(KeyError):
        clean_queue.put(None, "fifo")

    # Queue empty
    assert clean_queue.qsize == 0

    # Try a deferred put
    with pytest.raises(KeyError):
        clean_queue.put(None, "fifo", wait=1)

    # Queue still empty
    assert clean_queue.qsize == 0


def test_clear_pending(clean_queue):
    """Test clearing pending tasks from a FIFO."""

    # Queue some stuff, in several FIFOs
    clean_queue.put(1, "fifo")
    clean_queue.put(2, "fifo")
    clean_queue.put(3, "fifo")
    clean_queue.put(4, "fifo2")
    clean_queue.put(5, "fifo2")
    clean_queue.put(6, "fifo2")
    assert clean_queue.qsize == 6
    assert clean_queue.fifo_size("fifo") == 3

    assert clean_queue.clear_fifo("fifo") == (3, 0)

    assert clean_queue.qsize == 3
    assert clean_queue.fifo_size("fifo") == 0

    # Remove the rest
    _, key = clean_queue.get()
    clean_queue.task_done(key)
    _, key = clean_queue.get()
    clean_queue.task_done(key)
    _, key = clean_queue.get()
    clean_queue.task_done(key)


def test_clear_inprogress(clean_queue):
    """Test clearing a FIFO with something in-progress."""

    # Queue some stuff, in several FIFOs
    clean_queue.put(1, "fifo")
    clean_queue.put(2, "fifo")
    assert clean_queue.qsize == 2
    assert clean_queue.inprogress_size == 0

    # Retrieve the first thing
    clean_queue.get()
    assert clean_queue.qsize == 1
    assert clean_queue.inprogress_size == 1

    assert clean_queue.clear_fifo("fifo") == (1, 0)

    assert clean_queue.qsize == 0
    assert clean_queue.inprogress_size == 1

    # Complete the task
    clean_queue.task_done("fifo")

    assert clean_queue.qsize == 0
    assert clean_queue.inprogress_size == 0


def test_clear_deferred(clean_queue):
    """Test clearing deferred tasks from a FIFO."""

    # Queue some stuff, in several FIFOs
    clean_queue.put(1, "fifo", wait=1)
    clean_queue.put(2, "fifo", wait=2)
    clean_queue.put(3, "fifo", wait=3)
    assert clean_queue.qsize == 0
    assert clean_queue.deferred_size == 3

    assert clean_queue.clear_fifo("fifo") == (0, 3)

    assert clean_queue.qsize == 0
    assert clean_queue.deferred_size == 0
