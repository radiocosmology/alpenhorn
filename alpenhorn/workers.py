"""Worker thread framework."""

import signal
import threading
from . import db
from peewee import OperationalError

import logging

log = logging.getLogger(__name__)

# This is the big red button: a worker thread will set this if
# a task produces an uncaught exception.  Once set, all workers
# will exit as soon as possible.
#
# During a global abort, there is no guarantee that the queue
# nor the worker pool are in a consistent state.
global_abort = threading.Event()

# The pool that receives signals
_signalpool = None
# The signal handlers themselves
def _handle_usr1(signum, frame):
    """SIGUSR1 signal handler.

    Sends an increment request to the worker pool.
    """
    log.info("Caught SIGUSR1: incrementing workers.")
    _signalpool.add_worker(blocking=False)


def _handle_usr2(signum, frame):
    """SIGUSR2 signal handler.

    Sends an decrement request to the worker pool.
    """
    log.info("Caught SIGUSR2: decrementing workers.")
    _signalpool.del_worker(blocking=False)


# Called from the main thread start-up to enable the
# worker incrment/decrement signals
def setsignals(pool):
    """Points signal handlers at `pool`.

    SIGUSR1 will result in a new worker being started.
    SIGUSR2 will result in a worker being deleted.
    """
    _signalpool = pool
    signal.signal(signal.SIGUSR1, _handle_usr1)
    signal.signal(signal.SIGUSR2, _handle_usr2)


def _worker(self, stop, queue):
    """The worker thread main loop.

    Invoked by the .start() method of the worker thread.

    Arguments:
     - stop: the stop event for this worker
     - queue: the task queue

    Starts by creating a database connection, which is assumed to be
    thread-safe (re-entrant).

    Waits and executes tasks from "queue" as they become available.
    Runs until its stop event fires.

    A database error (pw.OperationalError), will result in this worker
    cleanly abandonning its current task and exiting.  The main thread
    will restart it to recover the connection.

    Any other exception will result in this thread firing the
    global_abort event which will result in a clean-as-possible exit of
    all of alpenhornd.
    """

    log.info(f"Worker started.")

    db.connect()

    while True:
        # Exit if told to stop
        if global_abort.is_set():
            log.info(f"Stopped due to global abort.")
            return
        if stop.is_set():
            log.info(f"Stopped.")
            return

        # Wait for a task:
        item = queue.get(timeout=5)

        if item is not None:
            # Abandon working if alpenhornd is aborting
            if global_abort.is_set():
                log.info(f"Stopped due to global abort.")
                return

            # Otherwise, execute the task.
            key, task = item

            log.debug(f"Beginning task {task}")
            try:
                task()
            except OperationalError:
                # Try to clean up. This runs task.do_cleanup()
                # until it raises something other than pw.OperationalError
                # or finishes.  Each time it is run, at least one cleanup
                # function will be shifted out of the queue, so at most
                # we'll call it once per registered cleanup function
                try:
                    while True:
                        try:
                            task.do_cleanup()
                            break  # Clean exit, so we're done
                        except pw.OperationalError:
                            pass  # Yeah, we know already; try again
                except Exception as e:
                    # Errors upon errors: just crash and burn
                    global_abort.set()
                    raise RuntimeError(
                        "Aborting due to uncaught exception in task cleanup"
                    ) from e

                log.debug(f"Finished task {task}")
                queue.task_done(key)  # Keep the queue sanitised

                # Requeue this task if necessary
                task.requeue()

                log.error(f"Exiting due to db error: {e}")
                return 1  # Thread exits, will be respawned in the main loop
            except Exception as e:
                global_abort.set()
                raise RuntimeError("Aborting due to uncaught exception in task") from e

            # Who knows what the state of the queue is during a global abort?
            if not global_abort.is_set():
                queue.task_done(key)

            log.debug(f"Finished task {task}")


class WorkerPool:
    """A pool of worker threads to handle asynchronous tasks from a queue.

    The number of workers in the pool may be adjusted on the fly.

    Arguments:
        - num_workers: initial number of workers to start
        - queue: the FairMultiFIFOQueue task queue
    """

    def __init__(self, num_workers, queue):
        self._queue = queue

        # For pool updates
        self._mutex = threading.Lock()

        # A list of stop events for currently-running workers.
        self._stops = list()

        # Running workers (ones being monitored)
        self._workers = list()

        # A list of _all_ workers which aren't known to be dead.  In
        # addition to all the workers in _workers, this also includes all
        # workers stopped by del_worker(), which may still be running.
        self._all_workers = list()

        # Start initial workers
        for _ in range(num_workers):
            self._new_worker()

    def _new_worker(self, index=None):
        """Create and start a new worker thread.

        If index is None, the new worker will be appended to the list of
        workers.  Otherwise the existing worker with the specified index
        is replaced.

        This function assumes the pool is clean.  If used outside of the
        constructor, callers should acquire the mutex first.
        """

        # Create a stop event for this worker
        stop = threading.Event()

        # Create the thread
        if index is None:
            name = f"Worker#{len(self._workers)}"
        else:
            name = f"Worker#{index}"
        thread = threading.Thread(
            target=_worker,
            args=(stop, self._queue),
            name=name,
            # daemon=True means the thread will be cancelled if
            # the main thread is aborted
            daemon=True,
        )

        if index is None:
            # Append
            self._stops.append(stop)
            self._workers.append(thread)
        else:
            # Replace
            self._stops[index] = stop
            self._workers[index] = worker

        # This is always an append
        self._all_workers.append(thread)

        # Start working
        tread.start()

    def add_worker(self, blocking=True):
        """Increment the number of workers in the pool.

        Does nothing if blocking is False and the mutex cannot be acquired.
        """
        if self._mutex.acquire(blocking=blocking):
            self._new_worker()
        else:
            log.warning("WorkerPool ignoring increment request: pool not clean")

    def del_worker(self, blocking=False):
        """Decrement the number of workers in the pool.

        This funciton always attempts to stop the highest indexed worker, even if
        other workers are idle.  If the worker is in the middle of a task, the
        task will be completed before the worker terminates.

        Does nothing if blocking is False and the mutex cannot be acquired.

        Also does nothing if the pool is empty (i.e. there's no worker to stop).
        """

        if self._mutex.acquire(blocking=blocking):
            if len(self._stops) == 0:
                log.warning("WorkerPool ignoring decrement request: no workers")

            # Fire the stop event (also forget it: we don't need it after this)
            stop = self._stops.pop()
            stop.set()

            # Cut the worker loose
            self._workers.pop()
        else:
            log.warning("WorkerPool ignoring decrement request: pool not clean")

    def check(self):
        """Check for workers which have unexpectedly exited and restart them.

        Most crashes of a worker thread will result in a global abort for
        purposes of data integrity, but a OperationaLError, which generally
        results from a lost connection to the database, doesn't.  In that
        case, we just restart the thread to re-try the DB connection.

        If the global_abort has been raised, this function does nothing.
        """
        # No reason to do anything during a global abort
        if global_abort.is_set():
            return

        # Find joined workers
        with self._mutex:
            for (index, worker) in enumerate(self._workers):
                if not worker.is_alive():
                    # Remove from the _all_workers list, since we
                    # obviously don't have to wait for it to join
                    self._all_workers.remove(worker)

                    # Respawn
                    log.warning("Respawning dead worker #{index}")
                    self._new_worker(index)

    def __len__(self):
        """Return the number of running worker threads.

        The value returned does not include workers which have been told to
        stop but haven't yet."""
        with self._mutex:
            return len(self._workers)

    def shutdown(self):
        """Stop all worker threads and wait for them to terminate."""

        with self._mutex:
            # Signal all current workers
            for stop in self._stops:
                stop.set()

            # Wait for _all_ workers
            for worker in self._all_workers:
                worker.join()

            # Probably we're about to exit, but just so everything stays copacetic:
            self._stops = list()
            self._workers = list()
            self._all_workers = list()


class EmptyPool:
    """A stand-in for WorkerPool for when we aren't threaded.

    It has the same methods as WorkerPool, but does nothing and is always
    empty."""

    def __len__(self):
        return 0

    def _do_nothing(self):
        pass

    shutdown = _do_nothing
    add_worker = _do_nothing
    del_worker = _do_nothing
    check = _do_nothing

    # Not quite nothing
    def add_worker(self):
        log.info("Ignoring request to add worker: serial I/O only")
