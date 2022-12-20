"""Test DefaultNodeIO.check()."""

from alpenhorn.archive import ArchiveFileCopy


def test_check_size(
    xfs, queue, simpleacq, filetype, archivefile, simplenode, archivefilecopy
):
    """Test check async with wrong size."""

    ft = filetype(name="ft")
    file = archivefile(
        name="file",
        acq=simpleacq,
        size_b=6,
        type=ft,
        md5sum="9e107d9d372bb6826bd81d3542a419d6",
    )
    copy = archivefilecopy(file=file, node=simplenode, has_file="M")

    xfs.create_file(copy.path, contents="The quick brown fox jumps over the lazy dog")

    # queue
    simplenode.io.set_queue(queue)
    simplenode.io.check(copy)

    # Call the async
    task, key = queue.get()
    task()
    queue.task_done(key)

    # Copy is now corrupt.
    assert ArchiveFileCopy.get(file=file, node=simplenode).has_file == "X"


def test_check_md5sum_good(
    xfs, queue, simpleacq, filetype, archivefile, simplenode, archivefilecopy
):
    """Test check async with good md5sum."""

    ft = filetype(name="ft")
    file = archivefile(
        name="file",
        acq=simpleacq,
        size_b=43,
        type=ft,
        md5sum="9e107d9d372bb6826bd81d3542a419d6",
    )
    copy = archivefilecopy(file=file, node=simplenode, has_file="M")

    xfs.create_file(copy.path, contents="The quick brown fox jumps over the lazy dog")

    # queue
    simplenode.io.set_queue(queue)
    simplenode.io.check(copy)

    # Call the async
    task, key = queue.get()
    task()
    queue.task_done(key)

    # Copy is now okay.
    assert ArchiveFileCopy.get(file=file, node=simplenode).has_file == "Y"


def test_check_md5sum_bad(
    xfs, queue, simpleacq, filetype, archivefile, simplenode, archivefilecopy
):
    """Test check async with bad md5sum."""

    ft = filetype(name="ft")
    file = archivefile(
        name="file", acq=simpleacq, size_b=43, type=ft, md5sum="incorrect-md5"
    )
    copy = archivefilecopy(file=file, node=simplenode, has_file="M")

    xfs.create_file(copy.path, contents="The quick brown fox jumps over the lazy dog")

    # queue
    simplenode.io.set_queue(queue)
    simplenode.io.check(copy)

    # Call the async
    task, key = queue.get()
    task()
    queue.task_done(key)

    # Copy is now corrupt.
    assert ArchiveFileCopy.get(file=file, node=simplenode).has_file == "X"


def test_check_missing(
    xfs, queue, simpleacq, filetype, archivefile, simplenode, archivefilecopy
):
    """Test check async with missing file."""

    ft = filetype(name="ft")
    file = archivefile(
        name="file",
        acq=simpleacq,
        size_b=43,
        type=ft,
        md5sum="9e107d9d372bb6826bd81d3542a419d6",
    )
    copy = archivefilecopy(file=file, node=simplenode, has_file="M")

    xfs.create_dir(copy.path.parent)

    # queue
    simplenode.io.set_queue(queue)
    simplenode.io.check(copy)

    # Call the async
    task, key = queue.get()
    task()
    queue.task_done(key)

    # Copy is now gone.
    assert ArchiveFileCopy.get(file=file, node=simplenode).has_file == "N"
