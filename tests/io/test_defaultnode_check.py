"""Test DefaultNodeIO.check()."""

from alpenhorn.archive import ArchiveFileCopy


def test_check_size(
    xfs, queue, genericacq, filetype, archivefile, genericnode, archivefilecopy
):
    """Test check async with wrong size."""

    ft = filetype(name="ft")
    file = archivefile(
        name="file",
        acq=genericacq,
        size_b=6,
        type=ft,
        md5sum="9e107d9d372bb6826bd81d3542a419d6",
    )
    copy = archivefilecopy(file=file, node=genericnode, has_file="M")

    xfs.create_file(copy.path, contents="The quick brown fox jumps over the lazy dog")

    # queue
    genericnode.io.set_queue(queue)
    genericnode.io.check(copy)

    # Call the async
    task, key = queue.get()
    task()
    queue.task_done(key)

    # Copy is now corrupt.
    assert ArchiveFileCopy.get(file=file, node=genericnode).has_file == "X"


def test_check_md5sum_good(
    xfs, queue, genericacq, filetype, archivefile, genericnode, archivefilecopy
):
    """Test check async with good md5sum."""

    ft = filetype(name="ft")
    file = archivefile(
        name="file",
        acq=genericacq,
        size_b=43,
        type=ft,
        md5sum="9e107d9d372bb6826bd81d3542a419d6",
    )
    copy = archivefilecopy(file=file, node=genericnode, has_file="M")

    xfs.create_file(copy.path, contents="The quick brown fox jumps over the lazy dog")

    # queue
    genericnode.io.set_queue(queue)
    genericnode.io.check(copy)

    # Call the async
    task, key = queue.get()
    task()
    queue.task_done(key)

    # Copy is now okay.
    assert ArchiveFileCopy.get(file=file, node=genericnode).has_file == "Y"


def test_check_md5sum_bad(
    xfs, queue, genericacq, filetype, archivefile, genericnode, archivefilecopy
):
    """Test check async with bad md5sum."""

    ft = filetype(name="ft")
    file = archivefile(
        name="file", acq=genericacq, size_b=43, type=ft, md5sum="incorrect-md5"
    )
    copy = archivefilecopy(file=file, node=genericnode, has_file="M")

    xfs.create_file(copy.path, contents="The quick brown fox jumps over the lazy dog")

    # queue
    genericnode.io.set_queue(queue)
    genericnode.io.check(copy)

    # Call the async
    task, key = queue.get()
    task()
    queue.task_done(key)

    # Copy is now corrupt.
    assert ArchiveFileCopy.get(file=file, node=genericnode).has_file == "X"


def test_check_missing(
    xfs, queue, genericacq, filetype, archivefile, genericnode, archivefilecopy
):
    """Test check async with missing file."""

    ft = filetype(name="ft")
    file = archivefile(
        name="file",
        acq=genericacq,
        size_b=43,
        type=ft,
        md5sum="9e107d9d372bb6826bd81d3542a419d6",
    )
    copy = archivefilecopy(file=file, node=genericnode, has_file="M")

    xfs.create_dir(copy.path.parent)

    # queue
    genericnode.io.set_queue(queue)
    genericnode.io.check(copy)

    # Call the async
    task, key = queue.get()
    task()
    queue.task_done(key)

    # Copy is now gone.
    assert ArchiveFileCopy.get(file=file, node=genericnode).has_file == "N"
