"""Test workers and the worker pool."""

import os
import signal
import peewee
import threading

import pytest

from alpenhorn.pool import WorkerPool, EmptyPool, setsignals, global_abort
from test_queue import queue


# Event to indicate that the worker that consumed the opperr_task
# is exiting
operr_done = threading.Condition()


def operr_task():
    """A fake task that raises peewee.OperationalError."""
    raise peewee.OperationalError("test")


# Attributes to make the function look like a Task to the worker
operr_task.do_cleanup = lambda: None

# task.requeue() is the last thing called as the worker exits
# So we trigger the condition here
def trigger_operr_done():
    global operr_done
    with operr_done:
        operr_done.notify()


operr_task.requeue = trigger_operr_done


def empty_task():
    """A fake task that does nothing."""


def crash_task():
    """A fake task that raises RuntimeError."""
    raise RuntimeError("test")


# Used by tests to tell the teardown of the pool() fixture how
# many workers have been deleted and need to be given a task
# to consume to avoid the 5-second timeout.
deleted_count = 0


@pytest.fixture
def pool(dbproxy, queue):
    """Create a WorkerPool."""

    p = WorkerPool(num_workers=2, queue=queue)

    global deleted_count
    deleted_count = 0

    yield p

    # Delete all the workers.  We only need to do this to get the timing
    # correct between the worker_stop() and the putting of all the empty
    # tasks.
    nworkers = len(p)
    for _ in range(nworkers):
        p.del_worker(blocking=True)

    # Add some do-nothing tasks for the workers to consume to avoid having
    # to wait 5 seconds for timeout after each test.
    #
    # This won't take care of previously deleted workers.  The test
    # should adjust deleted_count as necessary.
    for _ in range(nworkers + deleted_count):
        queue.put(empty_task, "fifo")

    # This waits for deleted workers
    p.shutdown()

    # Pool should be empty after shutdown
    assert len(p) == 0


@pytest.fixture
def empty_pool():
    """Create an EmptyPool."""

    p = EmptyPool()

    yield p

    p.shutdown()


def test_adddel(pool):
    """Test adding and deleting workers."""
    assert len(pool) == 2
    pool.add_worker()
    assert len(pool) == 3
    pool.del_worker()
    assert len(pool) == 2
    pool.del_worker()
    assert len(pool) == 1
    pool.del_worker()
    assert len(pool) == 0
    pool.del_worker()  # Should not fail
    assert len(pool) == 0
    pool.add_worker()
    assert len(pool) == 1

    # For fixture teardown (to avoid a 5 second wait)
    global deleted_count
    deleted_count = 3


def test_adddelempty(empty_pool):
    """Test adding and deleting workers from the empty pool."""
    assert len(empty_pool) == 0
    empty_pool.add_worker()
    assert len(empty_pool) == 0
    empty_pool.del_worker()
    assert len(empty_pool) == 0


def test_signal(pool):
    """Test signalling the pool."""
    assert len(pool) == 2
    setsignals(pool)
    os.kill(os.getpid(), signal.SIGUSR1)
    assert len(pool) == 3
    setsignals(pool)
    os.kill(os.getpid(), signal.SIGUSR2)
    assert len(pool) == 2

    # For fixture teardown (to avoid a 5 second wait)
    global deleted_count
    deleted_count = 1


def test_check(queue, pool):
    """Test WorkerPool.check()."""

    # Force a worker to exit
    queue.put(operr_task, "fifo")

    # Wait for the worker to exit
    global operr_done
    with operr_done:
        operr_done.wait()

    # Worker count is still two: dead workers are part of the count.
    assert len(pool) == 2

    # Check
    pool.check()

    # Worker count should still be two.
    assert len(pool) == 2

    # The most important thing in this test is the assert in the
    # teardown of the queue() fixture: if the above pool.check()
    # call hasn't restarted the worker thread, there will be an
    # item left in the queue which was supposed to be consumed by
    # the resurrected worker.


def test_crash(queue, pool):
    """Test the global abort."""

    # Force a worker to crash
    queue.put(crash_task, "fifo")

    # Wait for the worker to crash
    global_abort.wait()

    # Do some clean-up so the queue fixture can exit
    queue.task_done("fifo")

    # Don't add an empty task for the crashed worker.
    global deleted_count
    deleted_count = -1
