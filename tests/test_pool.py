"""Test the worker pool."""

import os
import signal

import pytest

from alpenhorn.pool import WorkerPool, EmptyPool, setsignals
from test_queue import queue

@pytest.fixture
def pool(dbproxy, queue):
    """Create a WorkerPool."""

    p = WorkerPool(num_workers=2, queue=queue)

    yield p

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
