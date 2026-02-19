"""Test DefaultNodeIO.check()."""

import pathlib

from alpenhorn.db.archive import ArchiveFileCopy


def test_check_size(xfs, queue, simpleacq, archivefile, unode, archivefilecopy):
    """Test check async with wrong size."""

    file = archivefile(
        name="file",
        acq=simpleacq,
        size_b=6,
        md5sum="9e107d9d372bb6826bd81d3542a419d6",
    )
    copy = archivefilecopy(file=file, node=unode.db, has_file="M")

    xfs.create_file(copy.path, contents="The quick brown fox jumps over the lazy dog")

    # queue
    unode.io.check(copy)

    # Call the async
    task, key = queue.get()
    task()
    queue.task_done(key)

    # Copy is now corrupt.
    assert ArchiveFileCopy.get(file=file, node=unode.db).has_file == "X"


def test_check_md5sum_good(xfs, queue, simpleacq, archivefile, unode, archivefilecopy):
    """Test check async with good md5sum."""

    file = archivefile(
        name="file",
        acq=simpleacq,
        size_b=43,
        md5sum="9e107d9d372bb6826bd81d3542a419d6",
    )
    copy = archivefilecopy(file=file, node=unode.db, has_file="M")

    xfs.create_file(copy.path, contents="The quick brown fox jumps over the lazy dog")

    # queue
    unode.io.check(copy)

    # Call the async
    task, key = queue.get()
    task()
    queue.task_done(key)

    # Copy is now okay.
    assert ArchiveFileCopy.get(file=file, node=unode.db).has_file == "Y"


def test_check_md5sum_bad(xfs, queue, simpleacq, archivefile, unode, archivefilecopy):
    """Test check async with bad md5sum."""

    file = archivefile(name="file", acq=simpleacq, size_b=43, md5sum="incorrect-md5")
    copy = archivefilecopy(file=file, node=unode.db, has_file="M")

    xfs.create_file(copy.path, contents="The quick brown fox jumps over the lazy dog")

    # queue
    unode.io.check(copy)

    # Call the async
    task, key = queue.get()
    task()
    queue.task_done(key)

    # Copy is now corrupt.
    assert ArchiveFileCopy.get(file=file, node=unode.db).has_file == "X"


def test_check_md5sum_perm(xfs, queue, simpleacq, archivefile, unode, archivefilecopy):
    """Test check async with permission error."""

    file = archivefile(name="file", acq=simpleacq, size_b=43, md5sum="incorrect-md5")
    copy = archivefilecopy(file=file, node=unode.db, has_file="M")

    xfs.create_file(
        copy.path, st_mode=0, contents="The quick brown fox jumps over the lazy dog"
    )

    # queue
    unode.io.check(copy)

    # Call the async
    task, key = queue.get()
    task()
    queue.task_done(key)

    # Copy is now corrupt.
    assert ArchiveFileCopy.get(file=file, node=unode.db).has_file == "X"


def test_check_missing(xfs, queue, simpleacq, archivefile, unode, archivefilecopy):
    """Test check async with missing file."""

    file = archivefile(
        name="file",
        acq=simpleacq,
        size_b=43,
        md5sum="9e107d9d372bb6826bd81d3542a419d6",
    )
    copy = archivefilecopy(file=file, node=unode.db, has_file="M")

    xfs.create_dir(copy.path.parent)

    # queue
    unode.io.check(copy)

    # Call the async
    task, key = queue.get()
    task()
    queue.task_done(key)

    # Copy is now gone.
    assert ArchiveFileCopy.get(file=file, node=unode.db).has_file == "N"


def test_check_async_path(xfs, simpleacq, archivefile, unode, archivefilecopy):
    """Test passing a path to check_async."""
    from alpenhorn.io.default import check_async

    file = archivefile(
        name="file",
        acq=simpleacq,
        size_b=43,
        md5sum="9e107d9d372bb6826bd81d3542a419d6",
    )
    copy = archivefilecopy(file=file, node=unode.db, has_file="M")

    # This one's a red herring.
    xfs.create_file(copy.path, contents="Red herring.")

    # This is the file we're actually going to check.
    xfs.create_file(
        "/other/path", contents="The quick brown fox jumps over the lazy dog"
    )

    # Run the async directly
    check_async(None, unode.io, copy, path=pathlib.Path("/other/path"))

    # copy should be fine
    assert ArchiveFileCopy.get(file=file, node=unode.db).has_file == "Y"
