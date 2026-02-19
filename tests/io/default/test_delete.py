"""Test DefaultNodeIO.delete()."""

import pathlib
from unittest.mock import patch

import pytest

from alpenhorn.db.archive import ArchiveFileCopy
from alpenhorn.io.default import UpDownLock, delete_async, remove_filedir


@pytest.fixture
def mock_archive_count():
    """Mock ArchiveFile.archive_count to return a
    number big enough to allow deletion."""

    @property
    def _mock_archive_count(self):
        return 6

    with patch("alpenhorn.db.archive.ArchiveFile.archive_count", _mock_archive_count):
        yield


def test_zero_len(queue, unode):
    """Test doing nothing when given nothing."""

    unode.io.delete([])

    assert queue.qsize == 0


def test_ncopies(
    xfs,
    queue,
    unode,
    simplegroup,
    storagenode,
    simpleacq,
    archivefile,
    archivefilecopy,
    storage_type="F",
):
    """Test not deleting from non-archival node
    when there are not enough other copies of the file."""

    # Need to make the containing directory
    xfs.create_dir("/node/simpleacq")

    unode.db.storage_type = storage_type
    unode.db.save()

    arc1 = storagenode(name="arc1", group=simplegroup, root="/arc1", storage_type="A")
    arc2 = storagenode(name="arc2", group=simplegroup, root="/arc2", storage_type="A")

    # Can't be deleted from node: only one archive copy
    file1 = archivefile(name="file1", acq=simpleacq)
    copy1 = archivefilecopy(file=file1, node=unode.db, has_file="Y")
    archivefilecopy(file=file1, node=arc1, has_file="Y")

    # Can't be deleted from node: only one archive copy
    file2 = archivefile(name="file2", acq=simpleacq)
    copy2 = archivefilecopy(file=file2, node=unode.db, has_file="Y")
    archivefilecopy(file=file2, node=arc1, has_file="Y")
    archivefilecopy(file=file2, node=arc2, has_file="N")

    # Can be deleted from node: two archived copies
    file3 = archivefile(name="file3", acq=simpleacq)
    copy3 = archivefilecopy(file=file3, node=unode.db, has_file="Y")
    archivefilecopy(file=file3, node=arc1, has_file="Y")
    archivefilecopy(file=file3, node=arc2, has_file="Y")

    unode.io.delete([copy1, copy2, copy3])

    assert queue.qsize == 1

    # Call the async
    task, key = queue.get()
    task()
    queue.task_done(key)

    # copy1 and copy2 aren't deleted, but copy3 is
    assert list(
        ArchiveFileCopy.select(ArchiveFileCopy.has_file)
        .where(ArchiveFileCopy.node == unode.db)
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
    unode,
    simplegroup,
    storagenode,
    simpleacq,
    archivefile,
    archivefilecopy,
):
    """Same as previous but on an archive node."""

    test_ncopies(
        xfs,
        queue,
        unode,
        simplegroup,
        storagenode,
        simpleacq,
        archivefile,
        archivefilecopy,
        storage_type="A",
    )


def test_delete_dirs(
    xfs,
    queue,
    simplegroup,
    storagenode,
    archiveacq,
    archivefile,
    archivefilecopy,
    mock_archive_count,
):
    """Test deleting directories (and some files)."""
    node = storagenode(name="node", group=simplegroup, root="/node")

    copies = []

    acq1 = archiveacq(name="acq/1")
    copies.append(
        archivefilecopy(
            file=archivefile(name="file/1", acq=acq1),
            node=node,
            has_file="Y",
        )
    )
    copies.append(
        archivefilecopy(
            file=archivefile(name="file/2", acq=acq1),
            node=node,
            has_file="Y",
        )
    )
    copies.append(
        archivefilecopy(
            file=archivefile(name="file/3", acq=acq1),
            node=node,
            has_file="Y",
        )
    )

    acq2 = archiveacq(name="acq/2")
    copies.append(
        archivefilecopy(
            file=archivefile(name="file/4", acq=acq2),
            node=node,
            has_file="Y",
        )
    )
    copies.append(
        archivefilecopy(
            file=archivefile(name="file/5", acq=acq2),
            node=node,
            has_file="Y",
        )
    )

    acq3 = archiveacq(name="acq3")
    copies.append(
        archivefilecopy(
            file=archivefile(name="file6", acq=acq3),
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

    # Call async directly with a fake UpDownLock
    delete_async(None, UpDownLock(), delete_copies)

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


def test_remove_filedir_patherror(simplenode):
    """remove_filedir raises ValueErorr if dirname isn't rooted under node.root"""

    with pytest.raises(ValueError):
        remove_filedir(simplenode, pathlib.Path("/some/other/path"), None)


def test_remove_filedir_node_root(simplenode, xfs):
    """remove_filedir must stop removing directories at node.root"""

    udl = UpDownLock()

    # Create something to delete
    path_to_delete = pathlib.Path(f"{simplenode.root}/a/b/c")
    xfs.create_dir(path_to_delete)

    remove_filedir(simplenode, path_to_delete, udl)

    assert not path_to_delete.exists()
    assert not pathlib.Path(f"{simplenode.root}/a/b").exists()
    assert not pathlib.Path(f"{simplenode.root}/a").exists()
    assert pathlib.Path(simplenode.root).exists()


def test_remove_filedir_missing(simplenode, xfs):
    """remove_filedir should be fine with missing subdirs"""

    udl = UpDownLock()

    # Create something to delete
    path_to_delete = pathlib.Path(f"{simplenode.root}/a/b/c")
    xfs.create_dir(path_to_delete)

    remove_filedir(simplenode, path_to_delete.joinpath("d/e/f"), udl)

    assert not path_to_delete.exists()
    assert not pathlib.Path(f"{simplenode.root}/a/b").exists()
    assert not pathlib.Path(f"{simplenode.root}/a").exists()
    assert pathlib.Path(simplenode.root).exists()


def test_remove_filedir_nonempty(simplenode, xfs):
    """remove_filedir should be fine with non-empty dirs"""

    udl = UpDownLock()

    # Create something to delete
    path_to_delete = pathlib.Path(f"{simplenode.root}/a/b/c")
    xfs.create_dir(path_to_delete)

    # Make a file somewhere to block deletion
    xfs.create_file(f"{simplenode.root}/a/b/blocker")

    remove_filedir(simplenode, path_to_delete, udl)

    # Only has been deleted up to /a/b
    assert not path_to_delete.exists()
    assert pathlib.Path(f"{simplenode.root}/a/b").exists()
    assert pathlib.Path(f"{simplenode.root}/a").exists()
    assert pathlib.Path(simplenode.root).exists()
