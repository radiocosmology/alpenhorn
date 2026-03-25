"""Test DefaultNodeIO.delete()."""

import pathlib
from unittest.mock import MagicMock, patch

import pytest

from alpenhorn.db.archive import ArchiveFileCopy
from alpenhorn.io.default import UpDownLock, delete_async, remove_filedir


def test_zero_len(queue, unode):
    """Test doing nothing when given nothing."""

    unode.io.delete([])

    assert queue.qsize == 0


def test_delete_check(
    xfs,
    queue,
    simplegroup,
    storagenode,
    archiveacq,
    archivefile,
    archivefilecopy,
):
    """The delete async calls the ArchiveFileCopy.check_delete."""
    node = storagenode(name="node", group=simplegroup, root="/node")
    acq = archiveacq(name="acq")
    copy1 = archivefilecopy(
        file=archivefile(name="file1", acq=acq),
        node=node,
        has_file="Y",
    )
    xfs.create_file(copy1.path, contents=copy1.file.name)
    copy2 = archivefilecopy(
        file=archivefile(name="file2", acq=acq),
        node=node,
        has_file="Y",
    )
    xfs.create_file(copy2.path, contents=copy2.file.name)

    mock = MagicMock()
    mock.return_value = False

    with patch("alpenhorn.db.archive.ArchiveFileCopy.check_delete", mock):
        # Call async directly with a fake UpDownLock
        delete_async(None, UpDownLock(), [copy1, copy2])

    # Nothing was deleted
    assert ArchiveFileCopy.select().where(ArchiveFileCopy.has_file == "Y").count() == 2

    # Check files
    assert pathlib.Path(copy1.path).exists()
    assert pathlib.Path(copy2.path).exists()

    # Now do the same, but returning true
    mock.return_value = True
    with patch("alpenhorn.db.archive.ArchiveFileCopy.check_delete", mock):
        delete_async(None, UpDownLock(), [copy1, copy2])

    # Both were deleted
    assert ArchiveFileCopy.select().where(ArchiveFileCopy.has_file == "Y").count() == 0

    # Check files
    assert not pathlib.Path(copy1.path).exists()
    assert not pathlib.Path(copy2.path).exists()


def test_delete_dirs(
    xfs,
    queue,
    dbtables,
    simplegroup,
    storagenode,
    archiveacq,
    archivefile,
    archivefilecopy,
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

    with patch(
        "alpenhorn.db.archive.ArchiveFileCopy.check_delete",
        lambda discretionary=True: True,
    ):
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
