"""UpDownLock: A two-way threading lock.

This is a shared two-way lock used for thread synchronization.

This lock behaves similarly to a regular `threading.RLock`, but
can be in one of three states: locked up, locked down, or
unlocked.

All threads wanting to acquire the lock must choose one of the
two locked states to acquire the lock in.  When the lock is
in one of the locked states, any other threads wanting to
acquire the lock in that same state may do so (i.e. multiple
threads can hold the lock in a particular state), but threads
wanting the other lock state must wait for the lock to become
unlocked before they can aquire the lock.

Alpenhorn uses this lock to prevent race conditions while
modifying the directory tree of a StorageNode: threads which
are in the process of creating directories must do so while
holding the up lock and threads which are in the process of
removing directories must do so while holding the down lock.

The lock ensures only one operation (creation or removal) can
happen at any given time.
"""

import threading
import time
from collections import defaultdict


class _UpDownLock:
    """UpDownLock internals.

    Used to prevent a reference loop.
    """

    __slots__ = ["_is_unlocked", "_lock", "_owners", "count"]

    def __init__(self) -> None:
        # This tracks the state of the lock:
        #  > 0: the lock is in the "up" state
        #  = 0: the lock is unlocked
        #  < 0: the lock is in the "down" state
        #
        # The absolute value indicates how many times
        # the lock is held
        self.count = 0

        # Protects self._count
        self._lock = threading.Lock()

        # Dict of threads holding the lock.  Keys are the
        # thread IDs.  Values are how many times that thread
        # owns the lock.
        self._owners = defaultdict(int)

        # Is the lock unlocked?  Handles threads waiting for
        # unlock.
        self._is_unlocked = threading.Condition(threading.RLock())

    def acquire(self, blocking: bool, timeout: float, is_down: bool) -> bool:
        """Acquire the lock in the specified state."""

        now = time.monotonic()
        me = threading.get_ident()

        # Try to acquire the lock
        with self._lock:
            if is_down:
                ok_to_lock = self.count <= 0
            else:
                ok_to_lock = self.count >= 0

            if ok_to_lock:
                if is_down:
                    self.count -= 1
                else:
                    self.count += 1
                self._owners[me] += 1
                return True

            # If we already hold the lock in the other state, fail
            if self._owners[me] > 0:
                raise RuntimeError("Can't acquire both locking states.")

        # If not blocking, fail
        if not blocking:
            return False

        # Otherwise, wait until unlocked
        with self._is_unlocked:
            if timeout < 0:
                self._is_unlocked.wait()
            else:
                end_at = None
                notified = False

                # Wait until either we're notified or else we timeout
                while not notified and (end_at is None or time.monotonic() < end_at):
                    # We do it this way to ensure this loop runs at least once
                    if end_at is None:
                        end_at = now + timeout

                    # This might be negative; threading.condition is okay
                    # with that (essentially it makes wait() non-blocking)
                    timeout = end_at - time.monotonic()
                    notified = self._is_unlocked.wait(timeout)

                # If we're out of time, fail
                if not notified:
                    return False

                # If we were notfied, but we're also out of time, convert to
                # non blocking, to try one last time
                if timeout <= 0:
                    blocking = False

        # If we got here, we were notified of the lock unlocking; try again,
        # possibly with a reduced timeout and/or converted to non-blocking
        return self.acquire(blocking, timeout, is_down)

    def release(self, is_down: bool) -> None:
        """Release the lock with the given state.

        Raises RuntimeError if the lock was not held in the state.
        """

        me = threading.get_ident()

        with self._lock:
            if is_down:
                ok_to_unlock = self.count < 0
            else:
                ok_to_unlock = self.count > 0

            # Check ownership
            if ok_to_unlock:
                if self._owners[me] == 0:
                    ok_to_unlock = False

            if not ok_to_unlock:
                raise RuntimeError(
                    f"Lock not held in {'down' if is_down else 'up'} state."
                )

            # Otherwise, unlock
            if is_down:
                self.count += 1
            else:
                self.count -= 1
            self._owners[me] -= 1

            # If we're now unlocked, notifiy waiters
            if self.count == 0:
                with self._is_unlocked:
                    self._is_unlocked.notify_all()


class _UpDownAccessor:
    """Lock accessor for one of the lock states.

    To acquire the lock in this state, use the `acquire` method.

    To release the lock in this state, use the `release` method.

    This accessor can also be used as a context manager.
    """

    __slots__ = ["_internals", "_is_down"]

    def __init__(self, is_down: bool, internals: _UpDownLock) -> None:
        self._is_down = is_down
        self._internals = internals

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        """Acquire the lock once.

        Parameters
        ----------
        blocking : bool
            If True, block until the lock can be acquired (or timeout expires).
            If False, this method immediately returns False if the lock cannot
            be acquired.
        timeout : float
            If set to a positive value, and `blocking` is True, wait at most
            `timeout` seconds before failing to acquire the lock (returning
            False).

        Returns
        -------
        success : bool
            True if the lock was acquired.  False otherwise.

        Raises
        ------
        RuntimeError:
            Lock is already being held in the opposite state.
        """
        return self._internals.acquire(
            blocking=blocking, timeout=timeout, is_down=self._is_down
        )

    def release(self) -> None:
        """Release the lock once.

        Raises
        ------
        RuntimeError:
            The lock was not being held in this state.
        """
        self._internals.release(is_down=self._is_down)

    # Context manager
    __enter__ = acquire

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()
        return False  # Re-raise exception, if any


class UpDownLock:
    """A two-way thread lock.

    This is a threading synchronization primitive, similar to
    a standard `threading.RLock`, but one which can be locked in
    two different states ("up" and "down).

    When locked in one of the states, any number of threads can
    hold the lock in that state, but threads attempting to
    acquire the lock in the other state must wait for the
    lock to unlock first.

    If a thread holds the lock in one a particular state, it may
    re-acquire the lock in that state again without blocking
    (i.e. the lock is re-entrant in this case), but a thread holding
    the lock in one state may not attempt to also acquire the lock
    in the opposite state as well. (This will result in a
    RuntimeError.) A thread must release the lock once for each time
    it has acquired the lock.

    The two states of the lock are accessed via `UpDownLock.up`
    and `UpDownLock.down`. These accessors provide `acquire` and
    `release` methods which work as they do with the standard
    `threading.Lock`.  The accessors may also be used as context
    managers.
    """

    __slots__ = ["_internals", "down", "up"]

    def __init__(self) -> None:
        self._internals = _UpDownLock()
        self.up = _UpDownAccessor(is_down=False, internals=self._internals)
        self.down = _UpDownAccessor(is_down=True, internals=self._internals)

    def __repr__(self) -> str:
        count = self._internals.count
        if count == 0:
            state = "unlocked"
        elif count > 0:
            state = "locked up"
        else:
            state = "locked down"
        return (
            f"<UpDownLock object state={state} count={abs(count)} at {hex(id(self))}>"
        )
