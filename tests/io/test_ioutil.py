"""Test alpenhorn.io.ioutil"""

import datetime
import pathlib

import peewee as pw
import pytest

from alpenhorn.db.archive import ArchiveFileCopy, ArchiveFileCopyRequest
from alpenhorn.io import ioutil
from alpenhorn.io.updownlock import UpDownLock


@pytest.mark.run_command_result(0, "", "md5 d41d8cd98f00b204e9800998ecf8427e")
@pytest.mark.alpenhorn_config({"daemon": {"pull_timeout_base": 1000}})
def test_bbcp_config_timeout(mock_run_command, set_config):
    """Test setting timeout via config."""

    assert ioutil.bbcp("from/path", "to/dir", 1e8) == {
        "check_src": True,
        "md5sum": "d41d8cd98f00b204e9800998ecf8427e",
        "ret": 0,
        "stderr": "md5 d41d8cd98f00b204e9800998ecf8427e",
    }

    args = mock_run_command()
    assert args["timeout"] == 1005.0  # 1000 seconds base + 5 seconds for 1e8 bytes


@pytest.mark.run_command_result(0, "", "md5 d41d8cd98f00b204e9800998ecf8427e")
@pytest.mark.alpenhorn_config({"daemon": {"pull_bytes_per_second": 0}})
def test_bbcp_no_timeout(mock_run_command, set_config):
    """Test disabling timeout via config."""

    assert ioutil.bbcp("from/path", "to/dir", 1e8) == {
        "check_src": True,
        "md5sum": "d41d8cd98f00b204e9800998ecf8427e",
        "ret": 0,
        "stderr": "md5 d41d8cd98f00b204e9800998ecf8427e",
    }

    args = mock_run_command()
    assert args["timeout"] is None  # pull_bytes_per_second = 0 disables


@pytest.mark.run_command_result(0, "", "md5 d41d8cd98f00b204e9800998ecf8427e")
def test_bbcp_pathlib(mock_run_command):
    """Test passing pathlib.Path to ioutil.bbcp()."""

    assert ioutil.bbcp(pathlib.Path("from/path"), pathlib.Path("to/dir"), 1e8) == {
        "check_src": True,
        "md5sum": "d41d8cd98f00b204e9800998ecf8427e",
        "ret": 0,
        "stderr": "md5 d41d8cd98f00b204e9800998ecf8427e",
    }

    args = mock_run_command()
    assert "from/path" in args["cmd"]
    assert "to/dir" in args["cmd"]


@pytest.mark.run_command_result(0, "", "md5 d41d8cd98f00b204e9800998ecf8427e")
def test_bbcp_good(mock_run_command):
    """Test a successful ioutil.bbcp() call."""

    assert ioutil.bbcp("from/path", "to/dir", 1e8) == {
        "check_src": True,
        "md5sum": "d41d8cd98f00b204e9800998ecf8427e",
        "ret": 0,
        "stderr": "md5 d41d8cd98f00b204e9800998ecf8427e",
    }

    args = mock_run_command()
    assert args["timeout"] == 305.0  # 300 seconds base + 5 seconds for 1e8 bytes


@pytest.mark.run_command_result(0, "", "md5 d41d8cd98f00b204e9800998ecf8427e")
def test_bbcp_port(mock_run_command):
    """Test setting bbcp from worker threads."""

    from alpenhorn.scheduler.pool import threadlocal

    # Ensure we have no worker id
    try:
        del threadlocal.worker_id
    except AttributeError:
        pass

    ioutil.bbcp("from/path", "to/dir", 1e8)
    args = mock_run_command()
    assert "4200" in args["cmd"]

    # Set worker id
    threadlocal.worker_id = 1

    ioutil.bbcp("from/path", "to/dir", 1e8)
    args = mock_run_command()
    assert "4210" in args["cmd"]

    # Set worker id
    threadlocal.worker_id = 2

    ioutil.bbcp("from/path", "to/dir", 1e8)
    args = mock_run_command()
    assert "4220" in args["cmd"]


@pytest.mark.run_command_result(0, "", "")
def test_bbcp_nomd5(mock_run_command):
    """Test a ioutil.bbcp() call with md5 sum missing from commmand output."""

    assert ioutil.bbcp("from/path", "to/dir", 1e8) == {
        "check_src": False,
        "ret": -1,
        "stderr": "Unable to read m5sum from bbcp output",
    }


@pytest.mark.run_command_result(1, "", "bbcp_stderr")
def test_bbcp_fail(mock_run_command):
    """Test a ioutil.bbcp() failed command."""

    assert ioutil.bbcp("from/path", "to/dir", 1e8) == {
        "check_src": True,
        "ret": 1,
        "stderr": "bbcp_stderr",
    }


@pytest.mark.run_command_result(0, "", "")
def test_rsync_good(mock_run_command):
    """Test successful ioutil.rsync() commands."""

    # Local rsync
    assert ioutil.rsync("from/path", "to/dir", 1e8, True) == {
        "md5sum": True,
        "ret": 0,
        "stderr": "",
    }

    # Local rsync doesn't compress
    args = mock_run_command()
    assert "--compress" not in args["cmd"]
    assert args["timeout"] == 305.0

    # Remote rsync
    assert ioutil.rsync("from/path", "to/dir", 1e8, False) == {
        "md5sum": True,
        "ret": 0,
        "stderr": "",
    }

    # Remote rsync does compress
    args = mock_run_command()
    assert "--compress" in args["cmd"]
    assert args["timeout"] == 305.0


@pytest.mark.run_command_result(0, "", "")
def test_rsync_pathlib(mock_run_command):
    """Test passing pathlib.Path to ioutil.rsync()."""

    # Local rsync
    assert ioutil.rsync(
        pathlib.Path("from/path"), pathlib.Path("to/dir"), 1e8, True
    ) == {
        "md5sum": True,
        "ret": 0,
        "stderr": "",
    }

    # Paths should be stringified
    args = mock_run_command()
    assert "from/path" in args["cmd"]
    assert "to/dir" in args["cmd"]


@pytest.mark.run_command_result(1, "", "mkstemp")
def test_rsync_mkstemp(mock_run_command):
    """Test catching a mkstemp error in ioutil.rsync()."""

    assert ioutil.rsync("from/path", "to/dir", 1e8, True) == {
        "check_src": False,
        "ret": 1,
        "stderr": "mkstemp",
    }


@pytest.mark.run_command_result(1, "", "write failed on")
def test_rsync_write(mock_run_command):
    """Test catching a write error in ioutil.rsync()."""

    assert ioutil.rsync("from/path", "to/dir", 1e8, True) == {
        "check_src": False,
        "ret": 1,
        "stderr": "write failed on",
    }


@pytest.mark.run_command_result(1, "", "rsync_stderr")
def test_rsync_fail(mock_run_command):
    """Test general failure in ioutil.rsync()."""

    assert ioutil.rsync("from/path", "to/dir", 1e8, True) == {
        "check_src": True,
        "ret": 1,
        "stderr": "rsync_stderr",
    }


def test_hardlink(xfs):
    """Test successful ioutil.hardlink() call."""

    # Create src and dest
    file = "/src/file"
    xfs.create_file(file, contents="data")
    dstdir = pathlib.Path("/dest")
    xfs.create_dir(dstdir)
    destfile = dstdir.joinpath("file")

    assert ioutil.hardlink(file, dstdir, "file") == {"ret": 0, "md5sum": True}
    assert destfile.read_text() == "data"


def test_hardlink_clobber(xfs):
    """Test successful overwrite in ioutil.hardlink() call."""

    # Create src and dest
    file = "/src/file"
    xfs.create_file(file, contents="data")
    dstdir = pathlib.Path("/dest")
    destfile = dstdir.joinpath("file")
    xfs.create_file(destfile, contents="other_data")

    assert destfile.read_text() == "other_data"
    assert ioutil.hardlink(file, dstdir, "file") == {"ret": 0, "md5sum": True}
    assert destfile.read_text() == "data"


def test_hardlink_fail(xfs):
    """Test failed ioutil.hardlink() call."""

    # Create src but not destdir
    file = "/src/file"
    xfs.create_file(file, contents="data")
    dstdir = pathlib.Path("/dest")
    destfile = dstdir.joinpath("file")

    assert ioutil.hardlink(file, dstdir, "file") is None
    with pytest.raises(FileNotFoundError):
        destfile.read_text()

    # Try with access error instead.
    xfs.create_file(destfile, contents="other_data")
    xfs.chmod(dstdir, 0o400)

    assert ioutil.hardlink(file, dstdir, "file") is None
    with pytest.raises(PermissionError):
        destfile.read_text()


def test_autosync(dbtables, simplefile, simplenode, simplegroup, storagetransferaction):
    """Test post_add running autosync."""

    storagetransferaction(node_from=simplenode, group_to=simplegroup, autosync=True)

    ioutil.post_add(simplenode, simplefile)

    assert ArchiveFileCopyRequest.get(
        file=simplefile,
        node_from=simplenode,
        group_to=simplegroup,
        completed=0,
        cancelled=0,
    )


def test_autosync_state(
    dbtables,
    archivefilecopy,
    archivefile,
    simpleacq,
    storagenode,
    simplenode,
    simplegroup,
    storagetransferaction,
):
    """post_add autosync copies whenever dest doesn't have a good copy."""

    destnode = storagenode(name="dest", group=simplegroup)
    storagetransferaction(node_from=simplenode, group_to=simplegroup, autosync=True)

    # Copies with different states
    fileY = archivefile(name="fileY", acq=simpleacq)
    archivefilecopy(file=fileY, node=destnode, has_file="Y")

    fileX = archivefile(name="fileX", acq=simpleacq)
    archivefilecopy(file=fileX, node=destnode, has_file="X")

    fileM = archivefile(name="fileM", acq=simpleacq)
    archivefilecopy(file=fileM, node=destnode, has_file="M")

    fileN = archivefile(name="fileN", acq=simpleacq)
    archivefilecopy(file=fileN, node=destnode, has_file="N")

    # This one shouldn't add a new copy request
    ioutil.post_add(simplenode, fileY)

    with pytest.raises(pw.DoesNotExist):
        ArchiveFileCopyRequest.get(file=fileY)

    # But all these should
    for f in [fileX, fileM, fileN]:
        ioutil.post_add(simplenode, f)
        assert ArchiveFileCopyRequest.get(
            file=f, node_from=simplenode, group_to=simplegroup, completed=0, cancelled=0
        )


def test_autosync_loop(dbtables, simplefile, simplenode, storagetransferaction):
    """post_add autosync ignores graph loops."""

    storagetransferaction(
        node_from=simplenode, group_to=simplenode.group, autosync=True
    )

    ioutil.post_add(simplenode, simplefile)

    with pytest.raises(pw.DoesNotExist):
        ArchiveFileCopyRequest.get(file=simplefile)


def test_autoclean(
    archivefilecopy,
    simplefile,
    simplenode,
    storagenode,
    simplegroup,
    storagetransferaction,
):
    """Test post_add running autoclean."""

    before = pw.utcnow() - datetime.timedelta(seconds=2)

    destnode = storagenode(name="dest", group=simplegroup)

    storagetransferaction(node_from=simplenode, group_to=simplegroup, autoclean=True)
    archivefilecopy(file=simplefile, node=simplenode, wants_file="Y", has_file="Y")

    ioutil.post_add(destnode, simplefile)

    copy = ArchiveFileCopy.get(file=simplefile, node=simplenode)
    assert copy.wants_file == "N"
    assert copy.last_update >= before


def test_autoclean_state(
    archivefile,
    archivefilecopy,
    simpleacq,
    storagenode,
    simplenode,
    simplegroup,
    storagetransferaction,
):
    """post_add autoclean only deletes copies with has_file=='Y'."""

    then = pw.utcnow() - datetime.timedelta(seconds=200)

    srcnode = storagenode(name="src", group=simplegroup)
    storagetransferaction(node_from=srcnode, group_to=simplenode.group, autoclean=True)

    # Copies with different states
    fileY = archivefile(name="fileY", acq=simpleacq)
    archivefilecopy(
        file=fileY, node=srcnode, has_file="Y", wants_file="Y", last_update=then
    )

    fileX = archivefile(name="fileX", acq=simpleacq)
    archivefilecopy(
        file=fileX, node=srcnode, has_file="X", wants_file="Y", last_update=then
    )

    fileM = archivefile(name="fileM", acq=simpleacq)
    archivefilecopy(
        file=fileM, node=srcnode, has_file="M", wants_file="Y", last_update=then
    )

    fileN = archivefile(name="fileN", acq=simpleacq)
    archivefilecopy(
        file=fileN, node=srcnode, has_file="N", wants_file="Y", last_update=then
    )

    # None of these should be deleted
    for f in [fileN, fileX, fileM]:
        ioutil.post_add(simplenode, f)

        copy = ArchiveFileCopy.get(file=f, node=srcnode)
        assert copy.last_update == then
        assert copy.wants_file == "Y"

    # But this one should
    ioutil.post_add(simplenode, fileY)

    copy = ArchiveFileCopy.get(file=fileY, node=srcnode)
    assert copy.last_update > then
    assert copy.wants_file == "N"


def test_autoclean_loop(archivefilecopy, simplefile, simplenode, storagetransferaction):
    """post_add autoclean ignores graph loops."""

    storagetransferaction(
        node_from=simplenode, group_to=simplenode.group, autoclean=True
    )
    archivefilecopy(file=simplefile, node=simplenode, wants_file="Y", has_file="Y")

    ioutil.post_add(simplenode, simplefile)

    copy = ArchiveFileCopy.get(file=simplefile, node=simplenode)
    assert copy.wants_file == "Y"


def test_remove_filedir_patherror(simplenode):
    """remove_filedir raises ValueErorr if dirname isn't rooted under node.root"""

    with pytest.raises(ValueError):
        ioutil.remove_filedir(simplenode, pathlib.Path("/some/other/path"), None)


def test_remove_filedir_node_root(simplenode, xfs):
    """remove_filedir must stop removing directories at node.root"""

    udl = UpDownLock()

    # Create something to delete
    path_to_delete = pathlib.Path(f"{simplenode.root}/a/b/c")
    xfs.create_dir(path_to_delete)

    ioutil.remove_filedir(simplenode, path_to_delete, udl)

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

    ioutil.remove_filedir(simplenode, path_to_delete.joinpath("d/e/f"), udl)

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

    ioutil.remove_filedir(simplenode, path_to_delete, udl)

    # Only has been deleted up to /a/b
    assert not path_to_delete.exists()
    assert pathlib.Path(f"{simplenode.root}/a/b").exists()
    assert pathlib.Path(f"{simplenode.root}/a").exists()
    assert pathlib.Path(simplenode.root).exists()


def test_local_copy(xfs, set_config):
    """Test successful ioutil.local_copy() call."""

    # Create src and dest
    file = "/src/file"
    xfs.create_file(file, contents="data")
    dstdir = pathlib.Path("/dest")
    xfs.create_dir(dstdir)
    destfile = dstdir.joinpath("file")

    assert ioutil.local_copy(file, dstdir, "file", 4) == {
        "ret": 0,
        "md5sum": "8d777f385d3dfec8815d20f7496026dc",
    }
    assert destfile.read_text() == "data"


def test_local_copy_clobber(xfs, set_config):
    """Test successful overwrite in ioutil.local_copy() call."""

    # Create src and dest
    file = "/src/file"
    xfs.create_file(file, contents="data")
    dstdir = pathlib.Path("/dest")
    destfile = dstdir.joinpath("file")
    xfs.create_file(destfile, contents="other_data")

    assert destfile.read_text() == "other_data"
    assert ioutil.local_copy(file, dstdir, "file", 4) == {
        "ret": 0,
        "md5sum": "8d777f385d3dfec8815d20f7496026dc",
    }
    assert destfile.read_text() == "data"


def test_local_copy_fail(xfs, set_config):
    """Test failed ioutil.local_copy() call."""

    # Create src but not destdir
    file = "/src/file"
    xfs.create_file(file, contents="data")
    dstdir = pathlib.Path("/dest")
    destfile = dstdir.joinpath("file")

    result = ioutil.local_copy(file, dstdir, "file", 4)
    assert result["ret"] != 0
    assert "stderr" in result

    with pytest.raises(FileNotFoundError):
        destfile.read_text()

    # Try with access error instead.
    xfs.create_file(destfile, contents="other_data")
    xfs.chmod(dstdir, 0o400)

    result = ioutil.local_copy(file, dstdir, "file", 4)
    assert result["ret"] != 0
    assert "stderr" in result

    with pytest.raises(PermissionError):
        destfile.read_text()
