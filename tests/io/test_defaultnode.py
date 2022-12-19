"""Test DefaultNodeIO."""

import pytest
import pathlib

from alpenhorn.storage import StorageNode


@pytest.fixture
def node(genericgroup, storagenode, xfs):
    """Create a standard node for testing backed by a pyfakefs filesystem.

    The disk is 10,000 bytes in size."""
    xfs.create_dir("/node")
    xfs.set_disk_usage(10000)
    return storagenode(name="node", root="/node", group=genericgroup)


def test_check_active(fs, node):
    """test DefaultNodeIO.check_active()"""

    assert node.io.check_active() is False

    fs.create_file("/node/ALPENHORN_NODE", contents="not-node")
    assert node.io.check_active() is False

    with open("/node/ALPENHORN_NODE", mode="w") as f:
        f.write("node")
    assert node.io.check_active() is True


def test_bytes_avail(node):
    """test DefaultNodeIO.bytes_avail()"""

    assert node.io.bytes_avail() == 10000.0


def test_update_avail_gb(node):
    """test DefaultNodeIO.update_avail_gb()"""

    # The test node initially doesn't have this set
    assert node.avail_gb is None

    node.io.update_avail_gb()

    # Now the value is set
    assert StorageNode.get(id=node.id).avail_gb == 10000.0 / 2.0**30


def test_fits(node):
    """test DefaultNodeIO.fits()"""
    assert node.io.fits(3000) is True
    assert node.io.fits(30000) is False


def test_walk(node, fs):
    """test DefaultNodeIO.file_walk()"""

    # Make some things.
    fs.create_file("/node/dir/file1")
    fs.create_file("/node/dir/file2")
    fs.create_symlink("/node/dir/file3", "/node/dir/file1")
    fs.create_file("/node/dir/subdir/file4")
    fs.create_link("/node/dir/file2", "/node/dir/subdir/file5")

    assert sorted(list(node.io.file_walk())) == [
        pathlib.PurePath("/node/dir/file1"),
        pathlib.PurePath("/node/dir/file2"),
        pathlib.PurePath("/node/dir/subdir/file4"),
        pathlib.PurePath("/node/dir/subdir/file5"),
    ]


def test_exists(node, fs):
    """test DefaultNodeIO.exists()"""

    fs.create_file("/node/dir/file")

    assert node.io.exists("dir/file") is True
    assert node.io.exists("dir/no-file") is False
    assert node.io.exists("no-dir/no-file") is False


def test_locked(node, fs):
    """test DefaultNodeIO.locked()"""
    fs.create_file("/node/dir/file1")
    fs.create_file("/node/dir/file2")
    fs.create_file("/node/dir/.file2.lock")

    assert node.io.locked("dir", "file1") is False
    assert node.io.locked("dir", "file2") is True
    assert node.io.locked("dir", "file3") is False


def test_md5sum_file(node, fs):
    """test DefaultNodeIO.md5sum_file()"""
    fs.create_file("/node/dir/file1")

    # d41...27e is the MD5sum of nothing (i.e. the zero-length message)
    assert node.io.md5sum_file("dir", "file1") == "d41d8cd98f00b204e9800998ecf8427e"

    # Something slightly less trivial
    fs.create_file(
        "/node/dir/file2", contents="The quick brown fox jumps over the lazy dog"
    )
    assert node.io.md5sum_file("dir", "file2") == "9e107d9d372bb6826bd81d3542a419d6"


def test_filesize(node, fs):
    """test DefaultNodeIO.filesize()"""
    fs.create_file("/node/dir/file1", st_size=1000)

    assert node.io.filesize("dir", "file1") == 1024


def test_reserve_bytes(node):
    """test byte reservations"""

    # We start off with 10,000 bytes available
    assert node.io.fits(4000) is True
    assert node.io.fits(40000) is False

    # Now reserve some bytes
    assert node.io.reserve_bytes(30000) is False

    assert node.io.reserve_bytes(2000) is True  # 4k bytes reserved
    assert node.io.reserve_bytes(2000) is True  # 8k bytes reserved
    assert node.io.reserve_bytes(2000) is False  # can't reserve 12k bytes

    # Release
    node.io.release_bytes(2000)  # 4k bytes reserved
    assert node.io.fits(4000) is False  # needs 8k free (only have 6k)

    with pytest.raises(ValueError):
        node.io.release_bytes(5000)  # too much

    # Release the rest
    node.io.release_bytes(2000)


def test_idle(node, queue):
    """Test node.io.idle"""

    # init
    node.io.set_queue(queue)

    # Currently idle
    assert node.io.idle is True

    # Enqueue something into this node's queue
    queue.put(None, node.name)

    # Now not idle
    assert node.io.idle is False

    # Dequeue it
    task, key = queue.get()

    # Still not idle, because task is in-progress
    assert node.io.idle is False

    # Finish the task
    queue.task_done(node.name)

    # Now idle again
    assert node.io.idle is True
