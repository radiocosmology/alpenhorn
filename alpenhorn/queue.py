"""Fair Multi-FIFO Queue

The Fair Multi-FIFO Queue implements a multi-producer, multi-consumer queue
similar to the queues implemented by the queue module.  This queue has all
the same locking semantics and thread-safety as queue.Queue but is not
subclassed from that.

Each task in the Fair Multi-FIFO Queue is part of a FIFO specified by a key.
The Fair Multi-FIFO Queue may have many such FIFOs, and it will try to ensure
that tasks are removed from the queue in a fair manner which tries to keep
the same number of tasks from each FIFO in progress at all times.

The queue is unbounded.
"""

import heapq
import threading
from time import time
from typing import Any
from collections import deque
from collections.abc import Hashable


class FairMultiFIFOQueue:
    """Create a new Fair Multi-FIFO Queue"""

    __slots__ = [
        "_all_tasks_done",
        "_deferrals",
        "_dlock",
        "_fifos",
        "_inprogress_counts",
        "_joining",
        "_keys_by_inprogress",
        "_lock",
        "_not_empty",
        "_total_inprogress",
        "_total_queued",
    ]

    def __init__(self) -> None:
        # The FIFO dict
        self._fifos = {}

        # Total number of queued tasks
        self._total_queued = 0
        # Total number of in-progress tasks
        self._total_inprogress = 0
        # Counts of in-progress tasks by FIFO
        self._inprogress_counts = {}
        # A list of sets of FIFO keys, indexed by number of in-progress tasks
        #
        # We initialise element 0 to the empty set to simplify creating new
        # FIFOs, which will always get added to that set.
        self._keys_by_inprogress = [set()]

        # The thread lock (mutex) and the conditionals (see queue.py for
        # details, which this implementation broadly follows)
        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)
        self._all_tasks_done = threading.Condition(self._lock)

        # Deferred puts.  This is a heapq.
        self._deferrals = list()
        # Are we in a join() call?
        self._joining = False
        # The lock for _deferrals and _joining, which can be held independently
        # of the primary _lock.
        self._dlock = threading.Lock()

    def task_done(self, key: Hashable) -> None:
        """Report that a task from the FIFO named `key` is done.

        After a consumer has finished processing a task provided to it
        by a get(), the key provided by that get() must be passed back
        into this function to report the completion of that task.

        Parameters
        ----------
        key : hashable
            The name of the FIFO that the completed task came from.  The
            value passed in should previously have been returned from a
            `get()` call.

        Raises
        ------
        ValueError
            The FIFO named `key` has no in-progress tasks.
        """

        with self._all_tasks_done:
            # The number of currently in-progress tasks (including the one
            # that's finishing now)
            count = self._inprogress_counts.get(key, 0)

            # If the specified FIFO has no in-progress tasks, raise value error
            if count <= 0:
                raise ValueError(f"no unfinished tasks for FIFO {key}")

            # Remove the FIFO from the ordered list
            self._keys_by_inprogress[count].remove(key)

            # Decrement counts
            count -= 1
            self._inprogress_counts[key] = count
            self._total_inprogress -= 1

            # Put the fifo back in the ordered list.
            #
            # XXX Could just delete a FIFO which is now empty here.
            #
            # In general, there's no reason to expect an empty FIFO to be reused,
            # by the caller, and numerous empty FIFOs will slow down get() over time,
            # so triming could decreate get() execution time.
            #
            # However, in our particular case (alpenhornd), the maximum number
            # of FIFOs is total number of nodes ever active on this host, which is
            # generally small and static enough not to have to worry about it.
            self._keys_by_inprogress[count].add(key)

            # XXX Could trim _keys_by_inprogress here.
            #
            # In general, it's potentially useful: it's possible for a bunch
            # of unnecessary empty sets to accumulate at the tail of this list,
            # so trimming can save memory in cases where large excursions of
            # in-progress counts happen occasionally.
            #
            # However, in our particular case (alpenhornd), at most, the list
            # will have as many elements as the maximum number of worker threads
            # that have ever existed at one time, so it's probably not worth the
            # trouble.

            # Notify waiters when there are no pending tasks
            if self._total_queued == 0 and self._total_inprogress == 0:
                self._all_tasks_done.notify_all()  # wakes up all waiting threads

    def join(self) -> None:
        """Blocks until there's nothing left in the queue.

        This includes waiting for in-progress tasks.  All deferred puts
        are discarded, including ones added while this call is blocking.
        """
        # Discard deferred puts
        with self._dlock:
            self._joining = True
            self._deferrals = list()

        with self._all_tasks_done:
            while self._total_inprogress > 0 or self._total_queued > 0:
                # woken up by the notify_all() in task_done()
                self._all_tasks_done.wait()

        with self._dlock:
            self._joining = False

    @property
    def qsize(self) -> int:
        """Total number of queued tasks.

        Excludes in-progress tasks and deferred puts not yet processed.

        Not to be relied on."""
        with self._lock:
            return self._total_queued

    @property
    def inprogress_size(self) -> int:
        """Total number of in-progress tasks.

        Not to be relied on.
        """
        with self._lock:
            return self._total_inprogress

    def fifo_size(self, key: Hashable) -> int:
        """Size of the FIFO named `key`.

        Includes both queued and in-progress tasks, but not deferred
        puts not yet expired.

        Returns 0 for any non-existent FIFO (i.e for any `key` not
        previously used).

        Parameters
        ----------
        key : hashable
            The name of the FIFO to return the size of.

        Returns
        -------
        fifo_size : int
            The size of the FIFO as explained above.
        """
        with self._lock:
            if key not in self._fifos:
                return 0
            return len(self._fifos[key]) + self._inprogress_counts[key]

    @property
    def deferred_size(self) -> int:
        """Total number of deferred puts not yet processed."""
        with self._dlock:
            return len(self._deferrals)

    def _put(self, item: Any, key: Hashable) -> None:
        """Put `item` into the FIFO named `key` without locking.

        Never call this function directly; use put() instead.

        NB: The caller must hold the `_lock` when calling this function!

        Parameters
        ----------
        item : anything
            The item to add to the queue
        key : hashable
            The name of the FIFO to which `item` is added
        """
        # Create the FIFO, if necessary
        if key not in self._fifos:
            fifo = deque()
            self._fifos[key] = fifo
            self._inprogress_counts[key] = 0
            self._keys_by_inprogress[0].add(key)
        else:
            fifo = self._fifos[key]

        # push the task onto the FIFO (right-most end)
        fifo.append(item)
        self._total_queued += 1

    def put(self, item: Any, key: Hashable, wait: float = 0) -> bool:
        """Push `item` onto the FIFO named `key`.

        If `wait` <= 0, the item is immediately put into the queue.
        and the call returns True.  This is the default behaviour.

        If `wait` > 0 and another thread has called `join()` on the
        queue, then this function does nothing and returns False.

        Otherwise, if `wait` > 0, the put is delayed by _at least_ that
        many seconds before item is actually added to the queue.  In
        this case, this call does not wait for the put, but returns
        True immediately.

        If the FIFO named `key` doesn't exist, it is created.

        Parameters
        ----------
        item : anything
            The item to add to the queue
        key : hashable
            The name of the FIFO to which `item` is added
        wait : float, optional
            The amount of time (in seconds) to wait before adding `item`
            to the queue.

        Returns
        -------
        result : bool
            False if `wait` > 0 and another thread is `join()`-ing the
            queue.  True otherwise.
        """

        if wait > 0:
            # Deferred put
            with self._dlock:
                # If joining, discard this put
                if self._joining:
                    return False

                heapq.heappush(self._deferrals, (time() + wait, item, key))
        else:
            # Immediate put
            with self._not_empty:
                self._put(item, key)
                self._not_empty.notify()  # wakes up a single waiting thread

        return True

    def _get(self, t: float) -> tuple[Any, Hashable] | None:
        """Iterate the `get()` loop once for at most `t` seconds.

        Never call this function directly.  Use get() instead.

        Parameters
        ----------
        t : float
            Maximum timeout (in seconds) for this iteration of the
            `get()` loop.

        Returns
        -------
        If there was nothing to get, returns None.

        Otherwise:

        item : anything
            The item removed from the queue
        key : hashable
            The name of the FIFO from which `item` was popped
        """
        timeout_at = time() + t
        with self._dlock:
            # If there are delayed puts, don't wait past the expiry of
            # the earliest
            if len(self._deferrals) > 0:
                first_expiry = min(self._deferrals)[0]
                if timeout_at > first_expiry:
                    timeout_at = first_expiry

        # Wait until t seconds elapse, or there's something to get
        wait = timeout_at - time()
        if wait > 0 and self._total_queued == 0:
            self._not_empty.wait(wait)

        # Execute all the expired deferred puts
        with self._dlock:
            # self._deferrals is a heapq, so self._deferrals[0] is always
            # the smallest element
            while len(self._deferrals) > 0 and self._deferrals[0][0] <= time():
                # heappop removes and returns self._deferrals[0]
                _, item, key = heapq.heappop(self._deferrals)
                self._put(item, key)

        # If the queue is still empty, time out
        if self._total_queued < 1:
            return None

        # Otherwise, get the next item from the queue:

        # Choose a FIFO by walking _keys_by_inprogress: find the lowest
        # non-empty set that has a non-empty FIFO in it.
        key = None
        for key_set in self._keys_by_inprogress:
            if len(key_set) > 0:
                # Look for a non-empty FIFO in this set
                for candidate in key_set:
                    if len(self._fifos[candidate]) > 0:
                        key = candidate
                        # remove the key from the set
                        key_set.remove(key)
                        break

                # If we found a non-empty FIFO, we're done
                if key is not None:
                    break

        # Nothing to get
        if key is None:
            return None

        fifo = self._fifos[key]

        # Pop the first (left-most) item from this FIFO
        item = fifo.popleft()
        self._total_queued -= 1
        self._total_inprogress += 1

        # Increment the in-progress count and file the key in the right
        # place in _keys_by_inprogress
        count = self._inprogress_counts[key] + 1
        self._inprogress_counts[key] = count
        # Extend _keys_by_inprogress if necessary
        if len(self._keys_by_inprogress) == count:
            self._keys_by_inprogress.append(set([key]))
        else:
            self._keys_by_inprogress[count].add(key)

        return (item, key)

    def get(self, timeout: float | None = None) -> tuple[Any, Hashable] | None:
        """Take the next item from the queue.

        If the queue is empty, blocks until there is something to
        remove or, if `timeout` is not `None`, until `timeout` seconds
        have elapsed.

        Parameters
        ----------
        timeout : float, optional
            If not None, the length of time (in seconds) to wait for
            an item.

        Returns
        -------
        On timeout, returns None.

        Otherwise:

        item : anything
            The item removed from the queue
        key : hashable
            The name of the FIFO from which `item` was popped.  This key
            _must_ be passed to `task_done()` once processing `item` is
            complete.
        """

        # This defines the rate at which we check for deferred puts expiring
        GET_PERIOD = 10  # seconds

        with self._not_empty:
            if timeout is None:
                # Wait until there is something in the queue
                while True:
                    item = self._get(GET_PERIOD)
                    if item is not None:
                        return item
            else:
                # Wait until woken up or timeout
                wait_until = time() + timeout
                while True:
                    remaining = wait_until - time()
                    if remaining <= 0:
                        return None  # timeout
                    item = self._get(
                        remaining if remaining < GET_PERIOD else GET_PERIOD
                    )
                    if item is not None:
                        return item
