"""Worker thread framework."""

import logging
import signal
import threading
from types import FrameType

from peewee import OperationalError

from ..common.metrics import Metric
from .queue import FairMultiFIFOQueue

log = logging.getLogger(__name__)

# This is the big red button: a worker thread will set this if
# a task produces an uncaught exception.  Once set, all workers
# will exit as soon as possible.
#
# During a global abort, there is no guarantee that the queue
# nor the worker pool are in a consistent state.
global_abort = threading.Event()


# Thread-local (Worker-local) storage
threadlocal = threading.local()


class Worker(threading.Thread):
    """A worker thread.

    Parameters
    ----------
    queue : FairMultiFIFOQueue
        The queue
    index : integer
        The index of this thread.  Available to tasks as `threadlocal.worker_id`
    """

    def __init__(self, queue: FairMultiFIFOQueue, index: int) -> None:
        # thread constructor; daemon=True means the thread will be cancelled if the
        # main thread dies
        self._worker_id = index + 1

        threading.Thread.__init__(self, name=f"Worker#{self._worker_id}", daemon=True)

        self._worker_stop = threading.Event()
        self._queue = queue

    def run(self) -> None:
        """The worker thread main loop.

        Invoked by the .start() method of the worker thread.

        Starts by creating a database connection, which is assumed to be
        thread-safe (re-entrant).

        Waits and executes tasks from self._queue as they become available.
        Runs until the self._worker_stop event fires.

        A database error (peewee.OperationalError), will result in this worker
        cleanly abandonning its current task and exiting.  The main thread
        will restart it to recover the connection.

        Any other exception will result in this thread firing the
        global_abort event which will result in a clean-as-possible exit of
        all of alpenhornd.
        """

        log.info("Started.")

        # Put the worker id in `threadlocal`, so tasks can access it
        global threadlocal
        threadlocal.worker_id = self._worker_id
        metric_running = Metric(
            "worker_running",
            "worker is running",
            counter=False,
            bound={"id": self._worker_id},
        )
        metric_running.set(1)
        metric_idle = Metric(
            "worker_idle",
            "worker is idle (waiting for a task)",
            counter=False,
            bound={"id": self._worker_id},
        )

        while True:
            metric_idle.set(1)
            # Exit if told to stop
            if global_abort.is_set():
                log.info("Stopped due to global abort.")
                metric_running.set(0)
                metric_idle.remove()
                return None
            if self._worker_stop.is_set():
                log.info("Stopped.")
                metric_running.set(0)
                metric_idle.remove()
                return None

            # Wait for a task:
            item = self._queue.get(timeout=5)

            if item is not None:
                metric_idle.set(0)
                task, key = item

                # Abandon working if alpenhornd is aborting
                if global_abort.is_set():
                    log.info("Stopped due to global abort.")
                    self._queue.task_done(key)
                    metric_running.set(0)
                    metric_idle.remove()
                    return None

                # Otherwise, execute the task.
                log.info(f"Beginning task {task}")
                try:
                    finished = task()
                except OperationalError as operr:
                    # Try to clean up. This runs task.do_cleanup()
                    # until it raises something other than OperationalError
                    # or finishes.  Each time it is run, at least one cleanup
                    # function will be shifted out of the queue, so at most
                    # we'll call it once per registered cleanup function
                    try:
                        while True:
                            try:
                                task.do_cleanup()
                                break  # Clean exit, so we're done
                            except OperationalError:
                                pass  # Yeah, we know already; try the next one
                    except Exception:
                        # Errors upon errors: time to crash and burn
                        global_abort.set()
                        log.exception(
                            "Aborting due to uncaught exception in task cleanup"
                        )
                        metric_running.set(0)
                        metric_idle.remove()
                        return 1

                    log.info(f"Finished task {task}")
                    self._queue.task_done(key)  # Keep the queue sanitised

                    # Requeue this task if necessary
                    task.requeue()

                    log.error(f"Exiting due to db error: {operr}")
                    metric_running.set(0)
                    metric_idle.remove()
                    return 1  # Thread exits, will be respawned in the main loop
                except Exception:
                    global_abort.set()
                    log.exception("Aborting due to uncaught exception in task")
                    metric_running.set(0)
                    metric_idle.remove()
                    return 1

                self._queue.task_done(key)

                if finished:
                    log.info(f"Finished task: {task}")
                else:
                    log.info(f"Deferring task: {task}")

    def stop_working(self) -> None:
        """Tell the worker to stop after finishing the current task."""
        self._worker_stop.set()


class WorkerPool:
    """A pool of worker threads to handle asynchronous tasks from a queue.

    The number of workers in the pool may be adjusted on the fly.

    Parameters
    ----------
    num_workers : int
        The _initial_ number of workers to start
    queue : FairMultiFIFOQueue
        The task queue
    """

    __slots__ = ["_all_workers", "_metric_worker_count", "_mutex", "_queue", "_workers"]

    def __init__(self, num_workers: int, queue: FairMultiFIFOQueue) -> None:
        self._queue = queue

        # For pool updates
        self._mutex = threading.Lock()

        # Running workers (ones being monitored)
        self._workers = []

        # A list of _all_ workers which aren't known to be dead.  In
        # addition to all the workers in _workers, this also includes all
        # workers stopped by del_worker(), which may still be running.
        self._all_workers = []

        self._metric_worker_count = Metric(
            "worker_count",
            "Number of worker threads",
            counter=False,
            bound={"pool_type": "WorkerPool"},
        )

        # Start initial workers
        for _ in range(num_workers):
            self._new_worker()

    def _new_worker(self, index: int | None = None) -> None:
        """Create and start a new worker thread.

        If `index` is None, the new worker will be appended to the list
        of workers.  Otherwise the existing worker with the specified
        `index` is replaced.

        This function assumes the pool is clean.  If used outside of the
        constructor, callers should acquire the mutex first.

        Parameters
        ----------
        index : int or None
            If restartng an exited worker, this is the index of the
            worker to restart.
        """

        # Create the worker
        worker = Worker(
            queue=self._queue,
            index=len(self._workers) if index is None else index,
        )

        if index is None:
            # Append
            self._workers.append(worker)
        else:
            # Replace
            self._workers[index] = worker

        # This is always an append
        self._all_workers.append(worker)

        # Start working
        worker.start()

        self._metric_worker_count.inc()

    def add_worker(self, blocking: bool = True) -> None:
        """Increment the number of workers in the pool.

        Does nothing if blocking is False and the mutex cannot be acquired.

        Parameters
        ----------
        blocking : bool, optional
            If False, exit and do nothing if the lock can't be acquired.
        """
        if self._mutex.acquire(blocking=blocking):
            self._new_worker()
            self._mutex.release()
        else:
            log.warning("WorkerPool ignoring increment request: pool not clean")

    def del_worker(self, blocking: bool = True) -> None:
        """Decrement the number of workers in the pool.

        This funciton always attempts to stop the highest indexed worker, even if
        other workers are idle.  If the worker is in the middle of a task, the
        task will be completed before the worker terminates.

        Does nothing if blocking is False and the mutex cannot be acquired.

        Also does nothing if the pool is empty (i.e. there's no worker to stop).

        Parameters
        ----------
        blocking : bool, optional
            If False, exit and do nothing if the lock can't be acquired.
        """

        if self._mutex.acquire(blocking=blocking):
            if len(self._workers) == 0:
                log.warning("WorkerPool ignoring decrement request: no workers")
            else:
                # Cut the worker loose
                worker = self._workers.pop()

                # Fire the stop event
                worker.stop_working()

                self._metric_worker_count.dec()

            # Release the lock
            self._mutex.release()
        else:
            log.warning("WorkerPool ignoring decrement request: pool not clean")

    def check(self) -> None:
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
            for index, worker in enumerate(self._workers):
                if not worker.is_alive():
                    # Remove from the _all_workers list, since we
                    # obviously don't have to wait for it to join
                    self._all_workers.remove(worker)

                    # Respawn
                    log.warning(f"Respawning dead worker #{1 + index}")
                    self._new_worker(index)

    def __len__(self) -> int:
        """Return the number of running worker threads.

        The value returned does not include workers which have been told to
        stop but haven't yet."""
        with self._mutex:
            return len(self._workers)

    def shutdown(self) -> None:
        """Stop all worker threads and wait for them to terminate."""

        with self._mutex:
            # Signal all current workers
            for worker in self._workers:
                worker.stop_working()

            # Wait for _all_ workers
            for worker in self._all_workers:
                worker.join()

            # Probably we're about to exit, but just so everything stays copacetic:
            self._workers = []
            self._all_workers = []
            self._metric_worker_count.set(0)


class EmptyPool:
    """A stand-in for WorkerPool for when we aren't threaded.

    It has the same methods as WorkerPool, but does nothing and is always
    empty."""

    def __init__(self) -> None:
        # This is never updated
        Metric(
            "worker_count",
            "Number of worker threads",
            counter=False,
            bound={"pool_type": "EmptyPool"},
        )

    def __len__(self) -> None:
        return 0

    def _do_nothing(self) -> None:
        pass

    shutdown = _do_nothing
    del_worker = _do_nothing
    check = _do_nothing

    # Not quite nothing
    def add_worker(self) -> None:
        log.info("Ignoring request to add worker: serial I/O only")


# The pool that receives signals
_signalpool = None


# The signal handlers themselves
def _handle_usr1(signum: int, frame: FrameType | None) -> None:
    """SIGUSR1 signal handler.

    Sends an increment request to the worker pool.
    """
    log.info("Caught SIGUSR1: incrementing workers.")
    _signalpool.add_worker(blocking=False)


def _handle_usr2(signum: int, frame: FrameType | None) -> None:
    """SIGUSR2 signal handler.

    Sends an decrement request to the worker pool.
    """
    log.info("Caught SIGUSR2: decrementing workers.")
    _signalpool.del_worker(blocking=False)


# Called from the main thread start-up to enable the
# worker incrment/decrement signals
def setsignals(pool: WorkerPool | EmptyPool) -> None:
    """Points signal handlers at `pool`.

    SIGUSR1 will result in a new worker being started.
    SIGUSR2 will result in a worker being deleted.

    Parameters
    ----------
    pool : WorkerPool or EmptyPool
        The pool to point the signal handlers to
    """
    global _signalpool
    _signalpool = pool
    signal.signal(signal.SIGUSR1, _handle_usr1)
    signal.signal(signal.SIGUSR2, _handle_usr2)
