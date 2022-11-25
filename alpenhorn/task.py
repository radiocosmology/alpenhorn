"""An asynchronous I/O task handled by a worker thread."""

from collections import deque

import logging

log = logging.getLogger(__name__)


class Task:
    """An asynchronous I/O task handled by a worker thread.

    The task is placed in the queue and waits for a worker thread
    to pop and execute it.

    Arguments:
    - func : callable
            the code executed by the worker.  The first
            positional argument to func is always the task
            itself.
    - queue : FairMultiFIFOQueue
            the task is automatically put on this queue
    - key : hashable
            the FIFO key used when put on the queue
    - requeue : boolean
            should the task be requeued if the worker aborts
            due to a DB error?  Typically True for auto_import
            tasks and False for main update loop tasks
    - name : string
            the name of the task.  Used in log messages
    - args : list or tuple
            additional positional arguments passed to func()
    - kwargs : dict
            keywork arguments passed to func()

    While executing, the provided Task object can be used in
    func to register cleanup functions.

    The return value of func is ignored.  Uncaught exceptions
    from func will result in a global abort of alpenhornd.
    """

    def __init__(
        self, func, queue, key, requeue=False, name="Task", args=tuple(), kwargs=dict()
    ):
        self._func = func
        self._args = args
        self._kwargs = kwargs
        self._name = name
        self._queue = queue
        self._key = key
        self._cleanup = deque()

        self._requeue = requeue

        # Enqueue ourself
        queue.put(self, key)

    def __call__(self):
        """This method is invoked by the worker thread to run the task."""
        # Run through the cleanup stack
        for func, args, kwargs in self._cleanup:
            func(*args, **kwargs)

    def __str__(self):
        return self._name

    def requeue(self):
        """If requesteed, re-queue a new copy of this task.

        This method is expected to be called from within the task.
        """
        if self._requeue:
            log.info("Requeueing task {self._name} in FIFO {self._key}")
            Task(
                func=self._func,
                queue=self._queue,
                key=self._key,
                requeue=True,
                name=self._name,
                args=self._args,
                kwargs=self._kwargs,
            )

    def cleanup(self, func, args=tuple(), kwargs=dict(), first=True):
        """Register a cleanup function.

        Add func (with tuple args and dict kwargs) to the list of
        functions to run after the task finishes.

        If first is True, the function is pushed to the start
        of the list (as if the list were a stack); otherwise it
        is pushed to the end of the list (as if the list were a
        FIFO). Both methods of pushing may be freely mixed.

        This method is expected to be called from within the task.
        """
        if first:
            self._cleanup.appendleft((func, args, kwargs))
        else:
            self._cleanup.append((func, args, kwargs))
