"""An asynchronous I/O task handled by a worker thread."""

from collections import deque
from inspect import isgenerator

import logging

log = logging.getLogger(__name__)


class Task:
    """An asynchronous I/O task handled by a worker thread.

    The task is placed in the queue and waits for a worker thread
    to pop and execute it.

    Arguments:
    - func : callable
            the code executed by the worker.  The first positional
            argument to func is always the task itself.
    - queue : FairMultiFIFOQueue
            the task is automatically put on this queue
    - key : hashable
            the FIFO key used when put on the queue
    - requeue : boolean
            should the task be requeued if the worker aborts due to a DB
            error?  Typically True for auto_import tasks and False for
            main update loop tasks
    - name : string
            the name of the task.  Used in log messages
    - args : list or tuple
            additional positional arguments passed to func()
    - kwargs : dict
            keywork arguments passed to func()

    While executing, the provided Task object can be used in
    func to register cleanup functions.

    The value returned from calling func() is ignored.

    If func raises pw.OperationalError, and the task is running in a
    worker, the worker will terminate (and be respawned by the main
    loop).  In this case, the value of `requeue` indicates how to handle
    re-running this task:
     - If requeue is True, the task will put a copy of itself back into
         the queue before exiting.  This is appropriate for Tasks
         produced by the auto_import watchers, which aren't going to
         fire again.
     - If requeue is false, the task is not requeued, but simply
         abandonned.  This is appropriate for Tasks produced by the main
         loop, since a subsequeunt update loop will determine whether
         the task needs to be performed again.

    All other uncaught exceptions from func will result in a global
    abort of alpenhornd.
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

        # a generator returned by calling _func (because it yields)
        self._generator = None

        # Enqueue ourself
        queue.put(self, key)

    def __call__(self):
        """This method is invoked by the worker thread to run the task.

        Returns True if the task is finished.
        """

        # If we have no generator, run _func and check whether it yielded
        if self._generator is None:
            result = self._func(self, *self._args, **self._kwargs)
            if isgenerator(result):
                # If calling _func returned a generator (because it contains a
                # yield statement), remember it so we can iterate it.
                self._generator = result

                # No return here: we need to iterate the generator once to start
                # up the function for the first time.
            else:
                # Otherwise, a regular function.  Task is done.
                self.do_cleanup()
                return True

        # We're working on a generator.  Iterate it once to get the next
        # yielded value.
        try:
            result = next(self._generator)
            # yielding no value results in immediate re-queueing
            if result is None:
                result = 0
            # Requeue ourself so we can iterate another time.
            log.debug(
                f"Requeueing yielded task {self._name} in FIFO {self._key} "
                f"with delay {result} seconds"
            )
            self._queue.put(self, self._key, wait=result)
            return False
        except StopIteration:
            # Function exited without yielding (i.e. we're done)
            self._generator = None
            self.do_cleanup()
            return True

    def do_cleanup(self):
        """Run through the cleanup stack."""

        # We pop here to handle the case where a pw.OperationalError
        # in a cleanup function causes the worker to cancel.
        #
        # If that happens it will call this function on it's way out
        # the door and we don't want to re-reun clean-up functions
        # we've already tried.
        while len(self._cleanup) > 0:
            func, args, kwargs = self._cleanup.popleft()
            func(*args, **kwargs)

    def __str__(self):
        return self._name

    def requeue(self):
        """If requested, re-queue a new copy of this task.

        This method is expected to be called from within the task.

        Note that the task put back into the queue is a _copy_: a yielding
        task will be restarted from the beginning, and won't pick up from
        where it was when it called requeue().
        """
        if self._requeue:
            log.info(f"Requeueing task {self._name} in FIFO {self._key}")
            Task(
                func=self._func,
                queue=self._queue,
                key=self._key,
                requeue=True,
                name=self._name,
                args=self._args,
                kwargs=self._kwargs,
            )

    def on_cleanup(self, func, args=tuple(), kwargs=dict(), first=True):
        """Register a cleanup function.

        Add func (with tuple args and dict kwargs) to the list of
        functions to run after the task finishes.

        If first is True, the function is pushed to the start of the
        list (as if the list were a stack); otherwise it is pushed to
        the end of the list (as if the list were a FIFO). Both methods
        of pushing may be freely mixed.

        This method is expected to be called from within the task.
        """
        if first:
            self._cleanup.appendleft((func, args, kwargs))
        else:
            self._cleanup.append((func, args, kwargs))
