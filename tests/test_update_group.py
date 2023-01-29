"""Tests for UpdateableGroup."""


def test_group_idle(queue, mockgroupandnode):
    """Test DefaultGroupIO.idle."""
    mockio, group, node = mockgroupandnode

    # Currently idle
    assert group.idle is True

    # Enqueue something into this node's queue
    queue.put(None, node.name)

    # Now not idle
    assert group.idle is False

    # Dequeue it
    task, key = queue.get()

    # Still not idle, because task is in-progress
    assert group.idle is False

    # Finish the task
    queue.task_done(node.name)

    # Now idle again
    assert group.idle is True
