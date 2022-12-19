"""Test DefaultNodeIO.delete()."""

import pytest
import pathlib
from unittest.mock import patch

from alpenhorn.archive import ArchiveFile, ArchiveFileCopy
from alpenhorn.io._default_asyncs import delete_async


@pytest.fixture
def mock_archive_count():
    """Mock ArchiveFile.archive_count() to return a number big enough to allow deletion."""

    def _mock_archive_count(self):
        return 6

    with patch("alpenhorn.archive.ArchiveFile.archive_count", _mock_archive_count):
        yield


def test_zero_len(queue, genericnode):
    """Test doing nothing when given nothing."""

    genericnode.io.set_queue(queue)
    genericnode.io.delete([])

    assert queue.qsize == 0


def test_ncopies(
    xfs,
    queue,
    genericgroup,
    storagenode,
    genericacq,
    genericfiletype,
    archivefile,
    archivefilecopy,
    storage_type="F",
):
    """Test not deleting from non-archival node when there are not enough other copies of the file."""

    # Need to make the containing directory
    xfs.create_dir("/node/genericacq")

    node = storagenode(
        name="node", group=genericgroup, root="/node", storage_type=storage_type
    )
    arc1 = storagenode(name="arc1", group=genericgroup, root="/arc1", storage_type="A")
    arc2 = storagenode(name="arc2", group=genericgroup, root="/arc2", storage_type="A")

    # Can't be deleted from node: only one archive copy
    file1 = archivefile(name="file1", acq=genericacq, type=genericfiletype)
    copy1 = archivefilecopy(file=file1, node=node, has_file="Y")
    archivefilecopy(file=file1, node=arc1, has_file="Y")

    # Can't be deleted from node: only one archive copy
    file2 = archivefile(name="file2", acq=genericacq, type=genericfiletype)
    copy2 = archivefilecopy(file=file2, node=node, has_file="Y")
    archivefilecopy(file=file2, node=arc1, has_file="Y")
    archivefilecopy(file=file2, node=arc2, has_file="N")

    # Can be deleted from node: two archived copies
    file3 = archivefile(name="file3", acq=genericacq, type=genericfiletype)
    copy3 = archivefilecopy(file=file3, node=node, has_file="Y")
    archivefilecopy(file=file3, node=arc1, has_file="Y")
    archivefilecopy(file=file3, node=arc2, has_file="Y")

    node.io.set_queue(queue)
    node.io.delete([copy1, copy2, copy3])

    assert queue.qsize == 1

    # Call the async
    task, key = queue.get()
    task()
    queue.task_done(key)

    # copy1 and copy2 aren't deleted, but copy3 is
    assert list(
        ArchiveFileCopy.select(ArchiveFileCopy.has_file)
        .where(ArchiveFileCopy.node == node)
        .tuples()
        .execute()
    ) == [
        ("Y",),
        ("Y",),
        ("N",),
    ]


def test_ncopies_archive(
    xfs,
    queue,
    genericgroup,
    storagenode,
    genericacq,
    genericfiletype,
    archivefile,
    archivefilecopy,
):
    """Same as previous but on an archive node."""

    test_ncopies(
        xfs,
        queue,
        genericgroup,
        storagenode,
        genericacq,
        genericfiletype,
        archivefile,
        archivefilecopy,
        storage_type="A",
    )


def test_delete_dirs(
    xfs,
    queue,
    genericgroup,
    storagenode,
    genericacqtype,
    archiveacq,
    genericfiletype,
    archivefile,
    archivefilecopy,
    mock_archive_count,
):
    """Test deleting directories (and some files)."""
    node = storagenode(name="node", group=genericgroup, root="/node")

    copies = list()

    acq1 = archiveacq(name="acq/1", type=genericacqtype)
    copies.append(
        archivefilecopy(
            file=archivefile(name="file/1", type=genericfiletype, acq=acq1),
            node=node,
            has_file="Y",
        )
    )
    copies.append(
        archivefilecopy(
            file=archivefile(name="file/2", type=genericfiletype, acq=acq1),
            node=node,
            has_file="Y",
        )
    )
    copies.append(
        archivefilecopy(
            file=archivefile(name="file/3", type=genericfiletype, acq=acq1),
            node=node,
            has_file="Y",
        )
    )

    acq2 = archiveacq(name="acq/2", type=genericacqtype)
    copies.append(
        archivefilecopy(
            file=archivefile(name="file/4", type=genericfiletype, acq=acq2),
            node=node,
            has_file="Y",
        )
    )
    copies.append(
        archivefilecopy(
            file=archivefile(name="file/5", type=genericfiletype, acq=acq2),
            node=node,
            has_file="Y",
        )
    )

    acq3 = archiveacq(name="acq3", type=genericacqtype)
    copies.append(
        archivefilecopy(
            file=archivefile(name="file6", type=genericfiletype, acq=acq3),
            node=node,
            has_file="Y",
        )
    )

    # Create files
    for copy in copies:
        xfs.create_file(copy.path, contents=copy.file.name)

    # Delete everyhing except for copy[2]
    delete_copies = copies.copy()
    del delete_copies[2]

    # Call async directly for simplicity.
    delete_async(None, node, delete_copies)

    # Only copies[2] remains
    assert ArchiveFileCopy.select().where(ArchiveFileCopy.has_file == "Y").count() == 1

    # Check files
    assert pathlib.Path(copies[2].path).exists()
    for copy in delete_copies:
        assert not pathlib.Path(copy.path).exists()

    # Check dirs.  The only one remaining should be copies[2] parents
    # (which are also the parents of copies[0] and [1])
    parent = pathlib.Path(copies[2].path).parent
    # acq/1/file
    assert parent.exists()
    # acq/1
    assert parent.parent.exists()
    # acq
    assert parent.parent.parent.exists()

    parent = pathlib.Path(copies[3].path).parent
    # acq/2/file
    assert not parent.exists()
    # acq/2
    assert not parent.parent.exists()
    # acq
    assert parent.parent.parent.exists()

    # acq3
    assert not pathlib.Path(copies[5].path).parent.exists()
