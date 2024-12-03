"""Test the UpDownLock"""

import threading
from time import sleep

import pytest

from alpenhorn.io.updownlock import UpDownLock


@pytest.fixture
def udlock():
    """Provide and check an UpDownLock

    Yields a newly-created lock and after the test
    ensures it's unlocked.
    """

    lock = UpDownLock()

    # Verify we're unlocked to begin with
    assert "unlocked" in repr(lock)

    yield lock

    # Must be unlocked again after the test
    assert "unlocked" in repr(lock)


def test_lock_up(udlock):
    """Test the up lock."""

    assert udlock.up.acquire()

    # Check
    assert "locked up" in repr(udlock)
    assert "count=1 " in repr(udlock)

    udlock.up.release()


def test_lock_down(udlock):
    """Test the down lock."""

    assert udlock.down.acquire()

    # Check
    assert "locked down" in repr(udlock)
    assert "count=1 " in repr(udlock)

    udlock.down.release()


def test_unlock_release(udlock):
    """Releasing an unlocked lock raises RuntimeError."""

    with pytest.raises(RuntimeError):
        udlock.up.release()

    with pytest.raises(RuntimeError):
        udlock.down.release()


def test_antirelease(udlock):
    """Releasing the wrong state raises RuntimeError."""

    assert udlock.up.acquire()
    with pytest.raises(RuntimeError):
        udlock.down.release()
    udlock.up.release()

    assert udlock.down.acquire()
    with pytest.raises(RuntimeError):
        udlock.up.release()
    udlock.down.release()


def test_acquire_both(udlock):
    """Attempting to acquire both lock states raises RuntimeError."""

    assert udlock.up.acquire()
    with pytest.raises(RuntimeError):
        udlock.down.acquire()
    udlock.up.release()

    assert udlock.down.acquire()
    with pytest.raises(RuntimeError):
        udlock.up.acquire()
    udlock.down.release()


def test_reentry_up(udlock):
    """Test lock re-entry."""

    # Test the up lock
    assert udlock.up.acquire()
    assert udlock.up.acquire()
    assert udlock.up.acquire()

    # Check
    assert "locked up" in repr(udlock)
    assert "count=3" in repr(udlock)

    udlock.up.release()
    udlock.up.release()

    # Still locked
    assert "locked up" in repr(udlock)
    assert "count=1" in repr(udlock)

    udlock.up.release()


def test_reentry_down(udlock):
    """Test lock re-entry."""

    # Test the down lock
    assert udlock.down.acquire()
    assert udlock.down.acquire()
    assert udlock.down.acquire()

    # Check
    assert "locked down" in repr(udlock)
    assert "count=3" in repr(udlock)

    udlock.down.release()
    udlock.down.release()

    # Still locked
    assert "locked down" in repr(udlock)
    assert "count=1" in repr(udlock)

    udlock.down.release()


def test_threads_up(udlock):
    """Test acquiring the lock from multiple threads."""

    # Number of threads to run
    n = 3

    # Synchronisation barrier
    barrier = threading.Barrier(1 + n)

    def thread(udlock, barrier):
        udlock.up.acquire()

        # Wait for check to complete
        barrier.wait()
        barrier.wait()

        # Now release
        udlock.up.release()

    # Create threads
    threads = []
    for _ in range(n):
        threads.append(
            threading.Thread(target=thread, args=(udlock, barrier), daemon=True)
        )

    # Start threads
    for thread in threads:
        thread.start()

    # Wait for lock acquisition
    barrier.wait()

    # Check
    assert f"count={n}" in repr(udlock)

    # Clean up
    barrier.wait()
    for thread in threads:
        thread.join()


def test_threads_down(udlock):
    """Test acquiring the lock from multiple threads."""

    # Number of threads to run
    n = 3

    # Synchronisation barriers
    barrier = threading.Barrier(1 + n)

    def thread(udlock, barrier):
        udlock.down.acquire()

        # Wait for check to complete
        barrier.wait()
        barrier.wait()

        # Now release
        udlock.down.release()

    # Create threads
    threads = []
    for _ in range(n):
        threads.append(
            threading.Thread(target=thread, args=(udlock, barrier), daemon=True)
        )

    # Start threads
    for thread in threads:
        thread.start()

    # Wait for lock acquisition
    barrier.wait()

    # Check
    assert f"count={n}" in repr(udlock)

    # Clean up
    barrier.wait()
    for thread in threads:
        thread.join()


def test_nonblocking(udlock):
    """Test a non-blocking acquire."""

    barrier = threading.Barrier(2)

    def thread(udlock, barrier):
        # Lock up
        udlock.up.acquire()

        # Wait for main to test
        barrier.wait()
        barrier.wait()

        udlock.up.release()

    t = threading.Thread(target=thread, args=(udlock, barrier), daemon=True)
    t.start()

    # Wait for lock
    barrier.wait()

    # Try locking
    assert not udlock.down.acquire(blocking=False)

    # Test done
    barrier.wait()
    t.join()


def test_waiting(udlock):
    """Test waiting for a lock."""

    # Synchronisation barrier between main and one test thread
    barrier2 = threading.Barrier(2)

    # Synchronisation barrier between all threads
    barrier3 = threading.Barrier(3)

    def upthread(udlock, barrier2, barrier3):
        udlock.up.acquire()

        # Signal we have the up lock
        barrier3.wait()  # Sync point 1

        # Wait for main to detect downthread waiting
        barrier2.wait()  # Sync point 2

        # Release the lock
        udlock.up.release()

        # Wait for downthread to acquire down lock
        barrier3.wait()  # Sync point 3

        # Attempt to acquire uplock (this blocks now)
        assert udlock.up.acquire()

        # Signal we have the up lock
        barrier3.wait()  # Sync point 5

        # Wait for main thread to check lock state
        barrier2.wait()  # Sync point 6

        # Release the lock again
        udlock.up.release()

    def downthread(udlock, barrier2, barrier3):
        # Wait for upthread to lock up
        barrier3.wait()  # Sync point 1

        # Acquire the down lock (this blocks)
        assert udlock.down.acquire()

        # Signal we have the down lock
        barrier3.wait()  # Sync point 3

        # Wait for main to detect upthread waiting
        barrier2.wait()  # Sync point 4

        # Release the lock
        udlock.down.release()

        # Wait for upthread to re-acquire
        barrier3.wait()  # Sync point 5

    up = threading.Thread(
        target=upthread, args=(udlock, barrier2, barrier3), daemon=True
    )
    down = threading.Thread(
        target=downthread, args=(udlock, barrier2, barrier3), daemon=True
    )

    # Start
    up.start()
    down.start()

    # Wait for lock up
    barrier3.wait()  # Sync point 1

    assert "locked up" in repr(udlock)

    # Wait for a bit to give downthread a chance to block
    sleep(0.05)

    # It's probably fine now
    barrier2.wait()  # Sync point 2

    # Wait for lock change
    barrier3.wait()  # Sync point 3

    assert "locked down" in repr(udlock)

    # Wait for a bit to give upthread a chance to block
    sleep(0.05)
    barrier2.wait()  # Sync point 4

    # Wait for lock up
    barrier3.wait()  # Sync point 5

    assert "locked up" in repr(udlock)
    # We're done
    barrier2.wait()  # Sync point 6

    # Clean up
    up.join()
    down.join()
