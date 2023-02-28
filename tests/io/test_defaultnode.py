"""Test DefaultNodeIO."""
import pytest


def test_bytes_avail(xfs, unode):
    """test DefaultNodeIO.bytes_avail()"""

    # Set disk size
    xfs.create_dir("/node")
    xfs.set_disk_usage(10000)

    assert unode.io.bytes_avail() == 10000.0


def test_check_active(xfs, unode):
    """test DefaultNodeIO.check_active()"""

    assert unode.io.check_active() is False

    xfs.create_file("/node/ALPENHORN_NODE", contents="not-node")
    assert unode.io.check_active() is False

    with open("/node/ALPENHORN_NODE", mode="w") as f:
        f.write("simplenode")
    assert unode.io.check_active() is True


def test_exists(unode, xfs):
    """test DefaultNodeIO.exists()"""

    xfs.create_file("/node/dir/file")

    assert unode.io.exists("dir/file") is True
    assert unode.io.exists("dir/no-file") is False
    assert unode.io.exists("no-dir/no-file") is False


def test_filesize(unode, xfs):
    """test DefaultNodeIO.filesize()"""
    xfs.create_file("/node/dir/file1", st_size=1000)

    assert unode.io.filesize("dir/file1") == 1000
    assert unode.io.filesize("/node/dir/file1") == 1000
    assert unode.io.filesize("/node/dir/file1", actual=True) == 1024
    assert unode.io.filesize("dir/file1", actual=True) == 1024


def test_fits(unode, xfs):
    """test DefaultNodeIO.fits()."""

    xfs.create_dir("/node")
    xfs.set_disk_usage(10000)

    assert unode.io.fits(3000) is True
    assert unode.io.fits(30000) is False


def test_md5(unode, xfs):
    """test DefaultNodeIO.md5()"""
    xfs.create_file("/node/dir/file1")

    # d41...27e is the MD5sum of nothing (i.e. the zero-length message)
    assert unode.io.md5("dir", "file1") == "d41d8cd98f00b204e9800998ecf8427e"

    # Something slightly less trivial
    xfs.create_file(
        "/node/dir/file2", contents="The quick brown fox jumps over the lazy dog"
    )
    assert unode.io.md5("dir/file2") == "9e107d9d372bb6826bd81d3542a419d6"


def test_reserve_bytes(unode, xfs):
    """test byte reservations"""

    xfs.create_dir("/node")
    xfs.set_disk_usage(10000)

    # We start off with 10,000 bytes available
    #
    # The reservation system reserves twice as much space
    # as requested (see DefaultNodeIO.reserve_factor), so
    # we only have enough space to concurrenlty reserve 5000 bytes.
    assert unode.io.reserve_bytes(4000, check_only=True) is True
    assert unode.io.reserve_bytes(40000) is False

    # Now reserve some bytes
    assert unode.io.reserve_bytes(30000) is False

    # Each of these uses up 4,000 bytes (twice the amount requested)
    assert unode.io.reserve_bytes(2000) is True  # 4k bytes reserved
    assert unode.io.reserve_bytes(2000) is True  # 8k bytes reserved
    assert unode.io.reserve_bytes(2000) is False  # can't reserve 12k bytes

    # Release
    unode.io.release_bytes(2000)  # 4k bytes reserved
    assert unode.io.reserve_bytes(4000) is False  # needs 8k free (only have 6k)

    with pytest.raises(ValueError):
        unode.io.release_bytes(5000)  # too much

    # Release the rest
    unode.io.release_bytes(2000)
