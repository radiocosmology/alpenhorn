"""Test Tasks"""

from alpenhorn.scheduler.task import Task


def test_args(queue):
    """Verify that args passed to the task are passed on to the func."""

    task_ran = False

    def _task(task, arg, kwarg=None):
        assert isinstance(task, Task)
        assert arg == "arg"
        assert kwarg == "kwarg"

        nonlocal task_ran
        task_ran = True

    Task(_task, queue, "fifo", args=("arg",), kwargs={"kwarg": "kwarg"})

    # Get the task and execute it
    task, key = queue.get()
    task()
    queue.task_done(key)

    # Check that it ran
    assert task_ran


def test_yield(queue):
    """Test a yielding task."""

    task_stage = 0
    first_task = None
    last_task = None

    def _task(task):
        nonlocal task_stage
        task_stage = 1

        nonlocal first_task
        first_task = task

        yield

        task_stage = 2

        nonlocal last_task
        last_task = task

    Task(_task, queue, "fifo")

    # Pop the task
    task, key = queue.get()

    # Verify queue is empty
    assert queue.qsize == 0

    # Run the task
    task()
    queue.task_done(key)

    # After the yield, the task has requeued itself
    assert queue.qsize == 1
    assert task_stage == 1

    task, key = queue.get()
    task()
    queue.task_done(key)

    # Check that it ran to the end
    assert task_stage == 2

    # Verify that the same Task instance was run both times
    assert first_task == last_task


def test_yieldwait(queue):
    """Test yielding with deferred queueing."""
    task_finished = False

    def _task(task):
        # Any number > 0 causes a deferred put
        yield 1e-5

        nonlocal task_finished
        task_finished = True

    Task(_task, queue, "fifo")
    assert queue.qsize == 1
    assert queue.deferred_size == 0

    # Run the task
    task, key = queue.get()
    task()
    queue.task_done(key)

    # put stays deferred until a get() is performed on the queue
    assert queue.qsize == 0
    assert queue.deferred_size == 1

    task, key = queue.get()
    task()
    queue.task_done(key)

    assert queue.qsize == 0
    assert queue.deferred_size == 0
    assert task_finished


def test_cleanup(queue):
    """Test task clean-up running."""

    cleaned_up = False

    def _task(task):
        nonlocal cleaned_up

        def _cleanup():
            nonlocal cleaned_up
            cleaned_up = True

        task.on_cleanup(_cleanup)

    Task(_task, queue, "fifo")

    # Run the task
    task, key = queue.get()
    task()
    queue.task_done(key)

    # Check that cleanup happened
    assert cleaned_up


def test_requeue(queue):
    """Test requeueing"""

    def _task(task):
        task.requeue()

    # Queue a non-requeuing version to check that the requeue() call is ignored.
    Task(_task, queue, "fifo")

    # Get the task
    task, key = queue.get()

    # Queue is empty
    assert queue.qsize == 0
    task()
    queue.task_done(key)

    # Queue is still empty
    assert queue.qsize == 0

    # Now try a requeuing task
    Task(_task, queue, "fifo", requeue=True)

    # Get the task
    task, key = queue.get()

    # Queue is empty
    assert queue.qsize == 0
    task()
    queue.task_done(key)

    # Queue is _not_ empty
    assert queue.qsize == 1

    # We can do it again: because _task always requeues, there's
    # no way to ever get it out of the queue
    task, key = queue.get()
    assert queue.qsize == 0
    task()
    queue.task_done(key)
    assert queue.qsize == 1


def test_exclusive_task(queue):
    """Test that exclusive task are actually exclusive."""

    # Make some tasks.  The second one is exclusive
    Task(None, queue, "fifo")
    Task(None, queue, "fifo", exclusive=True)
    Task(None, queue, "fifo")

    # They're all there
    assert queue.qsize == 3

    # Pop the first task
    task, key = queue.get(timeout=0.1)

    # Can't pop the second task yet
    assert queue.get(timeout=0.1) is None

    # Finish the first task
    queue.task_done(key)

    # Now we can get the second task
    task, key = queue.get(timeout=0.1)

    # Fail to get the third task because
    # we're now executing an exclusive task
    assert queue.get(timeout=0.1) is None

    # Finish the second task, unlocking the fifo
    queue.task_done(key)

    # Now we can get the third task
    item, key = queue.get(timeout=0.1)
    queue.task_done(key)

    # Everything's taken care of
    assert queue.qsize == 0
