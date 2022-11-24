"""Fair Multi-FIFO Queue

The Fair Multi-FIFO Queue implements a multi-producer, multi-consumer queue
similar to the queues implemented by the queue module.  This queue has all
the same locking semantics and thread-safety as queue.Queue but is not
subclassed from that.

Each task in the Fair Multi-FIFO Queue is part of a FIFO specified by a key.
The Fair Multi-FIFO Queue may have many such FIFOs, and it will try to ensure
that tasks are removed from the queue in a fair manner which prioritises
popping items off FIFOs with fewer in-progress tasks.

The queue is unbounded.
"""

import threading
from time import time
from collections import deque


class FairMultiFIFOQueue:
    """Create a new Fair Multi-FIFO Queue"""

    def __init__(self):
        # The FIFO dict
        self._fifos = {}

        # Total number of queued tasks
        self._total_queued = 0
        # Total number of queued and in-progress tasks
        self._total_inprogress = 0
        # Counts of in-progress tasks by FIFO
        self._inprogress_counts = {}
        # A list of sets of FIFO keys ordered by number of in-progress tasks
        #
        # We initialise element 0 to the empty set to simplify creating new
        # FIFOs (which will always get added to that set)
        self._fifos_by_inprogress = [set()]

        # The thread lock (mutex) and the conditionals (see queue.py for details, which
        # this implementation broadly follows)
        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)
        self._all_tasks_done = threading.Condition(self._lock)

    def task_done(self, key):
        """Report that a task from the FIFO indexed by key is done.

        After a task retreived with get(), the key provided by that get()
        must be passed back into this function to report the completion
        of that task."""

        with self._all_tasks_done:
            # The number of currently in-progress tasks (including the one that's finishing now)
            count = self._inprogress_counts[key]

            # If the specified FIFO has no in-progress tasks, raise value error
            if count <= 0:
                raise ValueError("no unfinished tasks for FIFO {key}")

            # Remove the FIFO from the ordered list
            self._fifos_by_inprogress[count].remove(key)

            # Decrement counts
            count -= 1
            self._inprogress_counts[key] = count
            self._total_unfinished -= 1

            # Put the fifo back in the ordered list.
            #
            # XXX Could just delete a FIFO which is now empty (count == 0) here.
            #
            # In general, there's no reason to expect an empty FIFO to be reused,
            # by the caller, and numerous empty FIFOs will slow down get() over time,
            # so triming could decreate get() execution time.
            #
            # However, in our particular case (alpenhornd), the maximum number
            # of FIFOs is sum of nodes and groups ever active on this host, which is
            # generally small enough not to have to worry about it.
            self._fifos_by_inprogress[count].add(key)

            # XXX Could trim _fifos_by_inprogress here.
            #
            # In general, it's potentially useful: it's possible for a bunch
            # of unnecessary empty sets to accumulate at the tail of this list,
            # so trimming can save memory in cases where large excursions of
            # in-progress counts happen occasionally.
            #
            # However, in our particular case (alpenhornd), at most, the list will
            # have as many elements as the maximum number of worker threads that
            # have existed, so it's probably not worth the trouble.

            # Notify waiters when there are no pending tasks
            if self._total_unfinished == 0:
                self._all_tasks_done.notify_all()  # wakes up all waiting threads

    def join(self):
        """Blocks until there's nothing left in the queue, including in-progress tasks."""
        # Wait until self._total_unfinished drops to zero
        with self._all_tasks_done:
            while self._total_unfinished:
                self._all_tasks_done.wait()  # woken up by the notify_all() in task_done()

    def qsize(self):
        """Total number of queued tasks.  Excludes in-progress tasks.  Not to be relied on."""
        with self._lock:
            return self._total_queued

    def inprogress_size(self):
        """Total number of in-progress tasks.  Not to be relied on."""
        with self._lock:
            return self._total_unfinished - self._total_queued

    def fifo_size(self, key):
        """Total number of tasks queued or in progress for the FIFO specified by key.

        Returns 0 for any key not previously used."""
        with self._lock:
            if key not in self._fifos:
                return 0
            return len(self._fifos[key]) + self._inprogress_counts[key]

    def put(self, item, key):
        """Put an item into the queue by pushing it onto the FIFO specified by key.

        If the FIFO specified by the key doesn't exist, it is created."""
        with self._not_empty:
            # Create the FIFO, if necessary
            if key not in self._fifos:
                fifo = self._new_fifo(key)
            else:
                fifo = self._fifos[key]

            # push the task onto the fifo (right-most end)
            fifo.append(item)
            self._total_unfinished += 1
            self._total_queued += 1
            self._not_empty.notify()  # wakes up a single waiting thread

    def get(self, timeout=None):
        """Take the next item from the queue.

        If the queue is empty, blocks until there is something to remove or, if timeout is
        not None, until "timeout" seconds have elapsed.

        On timeout, returns None; otherwise returns a tuple (key,item) where key is the
        key of the FIFO from which item was taken.  This key must be passed to
        task_done() once processing item is complete.
        """

        with self._not_empty:
            if timeout is None:
                # Wait until there is something in the queue
                while self._total_queued < 1:
                    self._not_empty.wait()  # woken up by the notify() in put()
            else:
                # Wait until woken up or timeout
                wait_until = time() + timeout
                while self._total_queued < 1:
                    remaining = wait_until - time()
                    if remaining <= 0:
                        return None
                    self._not_empty.wait(remaining)  # woken up by the notify() in put()

            # Choose a FIFO by walking _fifos_by_inprogress
            key = None
            for fifo_set in self._fifos_by_inprogress:
                if len(fifo_set) > 0:
                    # This removes and returns an arbitrary element in the set
                    key = fifo_set.pop()
                    break

            fifo = self._fifos[key]

            # Pop the first (left-most) item from this FIFO
            item = fifo.popleft()
            self._total_queued -= 1

            # Increment the in-progress count and re-add the key to in _fifos_by_inprogress
            count = self._inprogress_counts[key] + 1
            self._inprogress_counts[key] = count
            if len(self._fifos_by_inprogress) == count - 1:
                self._fifos_by_inprogress[count] = set([key])
            else:
                self._fifos_by_inprogress[count].add(key)

            return (key, item)

    def _new_fifo(self, key):
        """Create a new FIFO indexed by key"""
        fifo = deque()
        self._fifos[key] = fifo
        self._inprogress_counts[key] = 0
        self._fifos_by_inprogress[0].append(key)
        return fifo
