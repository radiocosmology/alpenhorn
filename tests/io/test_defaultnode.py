"""Test DefaultNodeIO."""


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


def test_filesize(unode, xfs):
    """test DefaultNodeIO.filesize()"""
    xfs.create_file("/node/dir/file1", st_size=1000)

    assert unode.io.filesize("dir/file1") == 1000
    assert unode.io.filesize("/node/dir/file1") == 1000
    assert unode.io.filesize("/node/dir/file1", actual=True) == 1024
    assert unode.io.filesize("dir/file1", actual=True) == 1024


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
