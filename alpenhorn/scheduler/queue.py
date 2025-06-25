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
from collections import deque
from collections.abc import Hashable
from time import monotonic, sleep
from typing import Any

from ..common.metrics import Metric


class FairMultiFIFOQueue:
    """Create a new Fair Multi-FIFO Queue"""

    __slots__ = [
        "_all_tasks_done",
        "_cleared_fifos",
        "_deferrals",
        "_dlock",
        "_fifo_labels",
        "_fifo_locks",
        "_fifos",
        "_inprogress_counts",
        "_joining",
        "_keys_by_inprogress",
        "_lock",
        "_not_empty",
        "_qlock",
        "_qsize",
        "_qsize_all",
        "_qsize_any",
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
        # The set of locked FIFOs
        self._fifo_locks = set()
        # The set of retired FIFOs
        self._cleared_fifos = set()

        # The thread lock (mutex) and the conditionals (see queue.py for
        # details, which this implementation broadly follows)
        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)
        self._all_tasks_done = threading.Condition(self._lock)

        # Deferred puts.  This is a heapq.
        self._deferrals = []
        # Are we in a join() call?
        self._joining = False
        # The lock for _deferrals and _joining, which can be held independently
        # of the primary _lock.
        self._dlock = threading.Lock()

        # METRICS
        # =======

        # FIFO labels.  A dict mapping FIFO keys to labels
        self._fifo_labels = {}

        # Count of tasks
        self._qsize = Metric(
            "queue_size", "Number of queued tasks", unbound=["fifo", "status"]
        )

        # Marginalised over status
        self._qsize_any = self._qsize.bind(status="any")

        # Marignalised over fifo
        self._qsize_all = self._qsize.bind(fifo="_ALL")

        # Track locked fifos
        self._qlock = Metric(
            "queue_locked", "The queue fifo is locked", unbound=["fifo"]
        )

    # METRIC HANDLING
    # ===============

    def _adj_metrics(self, value: int, fifo: Hashable, status: str) -> None:
        """Adjust the queue_size metric by value.

        Adds `value` (which may be negative) to the metric with fifo=fifo and
        status=status.  Also adds one to the marginalised ..._any and ..._all
        metrics.
        """

        try:
            fifo_label = self._fifo_labels[fifo]
        except KeyError:
            fifo_label = str(fifo)
            self._fifo_labels[fifo] = fifo_label

        self._qsize.add(value, fifo=fifo_label, status=status)
        self._qsize_any.add(value, fifo=fifo_label)
        self._qsize_all.add(value, status=status)

    def _inc_metrics(self, fifo: Hashable, status: str) -> None:
        """Increment the queue_size metric.

        Adds one to the metric with fifo=fifo and status=status.  Also adds
        one to the marginalised ..._any and ..._all metrics.
        """
        self._adj_metrics(1, fifo, status)

    def _dec_metrics(self, fifo: Hashable, status: str) -> None:
        """Decrement the queue_size metric.

        Subtracts one from the metric with fifo=fifo and status=status.  Also
        subtracts one from the marginalised ..._any and ..._all metrics.
        """
        self._adj_metrics(-1, fifo, status)

    def label_fifo(self, key: Hashable, label: str) -> None:
        """Set the metric label for `key` to `label`.

        This sets the value of the "fifo" label used in queue metrics
        for the FIFO named `key`.

        Parameters
        ----------
        key : Hashable
            The FIFO to label.  The FIFO does not need to exist in the queue.
        label : string
            The label to use for the FIFO.

        Notes
        -----
        Labels do not need to be unique.  If two FIFOs have the same label,
        their metrics will be combined.

        For unlablled FIFOs, the result of calling `str()` on the FIFO's
        `key` is used in place of a label.
        """
        self._fifo_labels[key] = label

    # QUEUE MAINTENANCE
    # =================

    def join(self) -> None:
        """Blocks until there's nothing left in the queue.

        This includes waiting for in-progress tasks.  All deferred puts
        are discarded, including ones added while this call is blocking.
        """
        # Discard deferred puts
        with self._dlock:
            self._joining = True
            self._deferrals = []

        with self._all_tasks_done:
            while self._total_inprogress > 0 or self._total_queued > 0:
                # woken up by the notify_all() in task_done()
                self._all_tasks_done.wait()

        with self._dlock:
            self._joining = False

        # Clear the metric.  This clears the marginalised versions, too
        self._qsize.clear()

    def clear_fifo(self, key: Hashable, keep_clear: bool = False) -> tuple[int, int]:
        """Remove all items from a fifo.

        Removes all pending and deferred tasks from the FIFO named `key`.
        In-progress tasks are not removed.

        If the FIFO is not present in the queue, this function successfully
        does nothing.

        Parameters
        ----------
        key : hashable
            The name of the FIFO to clear
        keep_clear : bool
            If True, don't accept any further puts on this FIFO.  If this is
            False (the default), the caller should not assume that the specified
            FIFO is empty when this function returns.

        Returns
        -------
        Returns a two-tuple of integers:
          * pending_removed: number of pending tasks removed
          * deferred_removed: number of deferred tasks removed
        """

        # If we're keeping it clear, add the fifo to the list first, so that
        # no concurrent put tries to add something while we're removing stuff
        if keep_clear:
            self._cleared_fifos.add(key)

        # Clear deferred puts
        deferred_removed = 0
        new_deferrals = []
        with self._dlock:
            # Remove everything from the heapq, creating a new one to replace it
            for index in range(len(self._deferrals)):
                # Pop out of old heapq and add to new if not being removed
                item = heapq.heappop(self._deferrals)

                # Here item is a 4-tuple:
                #  0: deferral time
                #  1: item to put
                #  2: FIFO key
                #  3: exclusive flag
                if item[2] == key:
                    deferred_removed += 1
                else:
                    heapq.heappush(new_deferrals, item)
            self._deferrals = new_deferrals

        # Clear queued items
        pending_removed = 0
        with self._not_empty:
            # Is there anything to clear?
            try:
                pending_removed = len(self._fifos[key])
                self._total_queued -= pending_removed

                # Reset the FIFO to an empty deque().  We do this even if we're
                # going to keep the FIFO clear from now on because we need to
                # still track in-progress items
                self._fifos[key] = deque()
            except KeyError:
                pass  # FIFO doesn't exist, so nothing to remove

        # If the queue is now empty, notify
        with self._all_tasks_done:
            if self._total_queued == 0 and self._total_inprogress == 0:
                self._all_tasks_done.notify_all()  # wakes up all waiting threads

        return (pending_removed, deferred_removed)

    # METADATA
    # ========

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

    # QUEUE PRODUCERS
    # ===============

    def _put(self, item: Any, key: Hashable, exclusive: bool) -> None:
        """Put `item` into the FIFO named `key` without locking.

        Never call this function directly; use put() instead.

        NB: The caller must hold the `_lock` when calling this function!

        Parameters
        ----------
        item : anything
            The item to add to the queue
        key : hashable
            The name of the FIFO to which `item` is added
        exclusive : bool
            True if `item` is exclusive
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
        fifo.append((item, exclusive))
        self._total_queued += 1

        self._inc_metrics(fifo=key, status="queued")

    def put(
        self, item: Any, key: Hashable, exclusive: bool = False, wait: float = 0
    ) -> bool:
        """Push `item` onto the FIFO named `key`.

        If `exclusive` is True, then `item` is considered to be exclusive:
        it can only be in progress when no other items from its FIFO are
        in progress.  Exclusive items are only returned by `get` when
        nothing else from their FIFO is in progress.  Then, after being
        returned by `get`, an exclusive item's FIFO is locked, preventing
        further items to be popped from it while the exclusive item is in
        progress.

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
        exclusive : bool, optional
            Whether the item is exclusive.
        wait : float, optional
            The amount of time (in seconds) to wait before adding `item`
            to the queue.

        Returns
        -------
        result : bool
            False if `wait` > 0 and another thread is `join()`-ing the
            queue.  True otherwise.

        Raises
        ------
        KeyError:
            The FIFO named `key` is not accempting items because the queue
            has been asked to keep it clear (see `clear_fifo()`).
        """

        # Check if the fifo is being kept clear
        if key in self._cleared_fifos:
            raise KeyError("FIFO not accepting items")

        if wait > 0:
            # Deferred put
            with self._dlock:
                # If joining, discard this put
                if self._joining:
                    return False

                heapq.heappush(
                    self._deferrals, (monotonic() + wait, item, key, exclusive)
                )
                self._inc_metrics(fifo=key, status="deferred")
        else:
            # Immediate put
            with self._not_empty:
                self._put(item, key, exclusive)
                self._not_empty.notify()  # wakes up a single waiting thread

        return True

    # QUEUE CONSUMERS
    # ===============

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

            # Unlock this FIFO if it was locked.  If the caller has been working on
            # an exclusive item from the FIFO, then this call must be completing it,
            # because there can't be anything else in-progress from this FIFO.  So,
            # it's always reasonable to try to unlock a FIFO here.
            self._fifo_locks.discard(key)
            self._qlock.set(0, fifo=key)

            # Decrement counts
            count -= 1
            self._inprogress_counts[key] = count
            self._total_inprogress -= 1
            self._dec_metrics(fifo=key, status="in-progress")

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
        # Set to true later if we had to skip something due to an
        # exclusive flag because its queue wasn't empty
        skipped_exclusive = False

        timeout_at = monotonic() + t
        with self._dlock:
            # If there are delayed puts, don't wait past the expiry of
            # the earliest
            if len(self._deferrals) > 0:
                first_expiry = min(self._deferrals)[0]
                if timeout_at > first_expiry:
                    timeout_at = first_expiry

        # Wait until t seconds elapse, or there's something to get
        wait = timeout_at - monotonic()
        if wait > 0 and self._total_queued == 0:
            self._not_empty.wait(wait)

        # Execute all the expired deferred puts
        with self._dlock:
            # self._deferrals is a heapq, so self._deferrals[0] is always
            # the smallest element
            while len(self._deferrals) > 0 and self._deferrals[0][0] <= monotonic():
                # heappop removes and returns self._deferrals[0]
                _, item, key, exclusive = heapq.heappop(self._deferrals)
                self._dec_metrics(fifo=key, status="deferred")
                self._put(item, key, exclusive)

        # If the queue is still empty, time out
        if self._total_queued < 1:
            return None

        # Otherwise, get the next item from the queue:

        # Choose a FIFO by walking _keys_by_inprogress: find the lowest
        # non-empty set that has a non-empty FIFO in it.
        key = None
        for count, key_set in enumerate(self._keys_by_inprogress):
            # If the key_set is empty, try the next one
            if not key_set:
                continue

            # Look for a non-empty FIFO in this set
            for candidate in key_set:
                # If the candidate FIFO is locked, skip it
                if candidate in self._fifo_locks:
                    skipped_exclusive = True
                    continue

                # If there's nothing in the FIFO, skip it
                if not self._fifos[candidate]:
                    continue

                # If the first item in the FIFO is exclusive
                # but there's currently in-progress items, skip it
                #
                # Items in the queue are 2-tuples, the second element
                # of which is the exclusive flag, so fifo[0][1] is the
                # exclusive flag for the first (left-most) item in the
                # fifo deque.
                if count and self._fifos[candidate][0][1]:
                    skipped_exclusive = True
                    continue

                # Otherwise, this candidate looks good
                key = candidate
                # remove the key from the set
                key_set.remove(key)
                break

            # If we found a non-empty FIFO, we're done
            if key is not None:
                break

        # Nothing to get
        if key is None:
            if skipped_exclusive:
                # Don't busy-wait if we ended up with nothing
                # because everything was exclusion-blocked
                wait = timeout_at - monotonic()
                if wait > 0:
                    sleep(wait)
            return None

        fifo = self._fifos[key]

        # Pop the first (left-most) item from this FIFO
        item, exclusive = fifo.popleft()
        self._total_queued -= 1
        self._total_inprogress += 1

        # Update metrics
        self._inc_metrics(fifo=key, status="in-progress")
        self._dec_metrics(fifo=key, status="queued")

        # Lock this FIFO, if item is exclusive
        if exclusive:
            self._fifo_locks.add(key)
            self._qlock.set(1, fifo=key)

        # Increment the in-progress count and file the key in the right
        # place in _keys_by_inprogress
        count = self._inprogress_counts[key] + 1
        self._inprogress_counts[key] = count
        # Extend _keys_by_inprogress if necessary
        if len(self._keys_by_inprogress) == count:
            self._keys_by_inprogress.append({key})
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
                wait_until = monotonic() + timeout
                while True:
                    remaining = wait_until - monotonic()
                    if remaining <= 0:
                        return None  # timeout
                    item = self._get(
                        remaining if remaining < GET_PERIOD else GET_PERIOD
                    )
                    if item is not None:
                        return item
