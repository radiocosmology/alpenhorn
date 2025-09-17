import logging
import pathlib
import time
from collections.abc import Callable

import peewee as pw

from ..common import metrics, util
from ._base import EnumField, base_model, database_proxy
from .acquisition import ArchiveFile
from .storage import StorageGroup, StorageNode, StorageTransferAction

log = logging.getLogger(__name__)


class ArchiveFileCopy(base_model):
    """Information about a copy of a file on a node.

    Attributes
    ----------
    file : foreign key
        Reference to the file of which this is a copy.
    node : foreign key
        The node on which this copy lives (or should live).
    has_file : enum
        Is the file on the node?
        - 'Y': yes, the node has a copy of the file.
        - 'N': no, the node does not have the file.
        - 'M': maybe: the file copy needs to be re-checked.
        - 'X': the file is there, but has been verified to be corrupted.
    wants_file : enum
        Does the node want the file?
        - 'Y': yes, keep the file around
        - 'M': maybe, can delete if we need space
        - 'N': no, should be deleted
        In all cases we try to keep at least two copies of the file around.
    ready : bool
        _Some_ StorageNode I/O classes use this to tell other hosts that
        files are ready for access.  Other I/O classes do _not_ use this
        field and assess readiness in some other way, so never check this
        directly; outside of the I/O-class code itself, use
        StorageNode.io.ready_path() or StorageNode.remote.pull_ready()
        to determine whether a remote file is ready for I/O.
    size_b : integer
        Allocated size of file in bytes (i.e. actual size on the Storage
        medium.)
    last_update : datetime
        The time at which this record was last updated.
    """

    file = pw.ForeignKeyField(ArchiveFile, backref="copies")
    node = pw.ForeignKeyField(StorageNode, backref="copies")
    has_file = EnumField(["N", "Y", "M", "X"], default="N")
    wants_file = EnumField(["Y", "M", "N"], default="Y")
    ready = pw.BooleanField(default=False)
    size_b = pw.BigIntegerField(null=True)
    last_update = pw.DateTimeField(default=pw.utcnow)

    @property
    def path(self) -> pathlib.Path:
        """The absolute path to the file copy.

        For a relative path (one omitting node.root), use copy.file.path
        """

        return pathlib.Path(self.node.root, self.file.path)

    @property
    def state(self) -> str:
        """A human-readable description of the copy state."""

        # key is '{has_file}{wants_file}'
        states = {
            # has_file == 'Y'
            "YY": "Healthy",
            "YM": "Removable",
            "YN": "Released",
            # has_file == 'M'
            "MY": "Suspect",
            "MM": "Suspect",
            "MN": "Released",
            # has_file == 'X'
            "XY": "Corrupt",
            "XM": "Corrupt",
            "XN": "Released",
            # has_file == 'N'
            "NY": "Missing",
            "NM": "Removed",
            "NN": "Removed",
        }

        key = self.has_file + self.wants_file
        return states.get(key, "Corrupt")

    def trigger_autoactions(self) -> None:
        """Trigger auto actions for this file copy.

        Possible actions are autosync or autoclean.  These
        are triggered whenever this file copy is created on
        the storage node (i.e. after an import or pull).
        """

        # Autosync: find all StorageTransferActions where we're the source node
        for edge in StorageTransferAction.select().where(
            StorageTransferAction.node_from == self.node,
            StorageTransferAction.group_to != self.node.group,
            StorageTransferAction.autosync == True,  # noqa: E712
        ):
            if edge.group_to.state_on_node(self.file)[0] != "Y":
                log.debug(
                    f"Autosyncing {self.file.path} from node {self.node.name} "
                    f"to group {edge.group_to.name}"
                )

                ArchiveFileCopyRequest.create(
                    node_from=self.node, group_to=edge.group_to, file=self.file
                )

        # Autoclean: find all the StorageTransferActions where we're in the
        # destination group
        for edge in StorageTransferAction.select().where(
            StorageTransferAction.group_to == self.node.group,
            StorageTransferAction.node_from != self.node,
            StorageTransferAction.autoclean == True,  # noqa: E712
        ):
            count = (
                ArchiveFileCopy.update(wants_file="N", last_update=pw.utcnow())
                .where(
                    ArchiveFileCopy.file == self.file,
                    ArchiveFileCopy.node == edge.node_from,
                    ArchiveFileCopy.has_file == "Y",
                    ArchiveFileCopy.wants_file == "Y",
                )
                .execute()
            )

            if count > 0:
                log.debug(
                    f"Autocleaning {self.file.path} from node {edge.node_from.name}"
                )

    class Meta:
        indexes = ((("file", "node"), True),)  # (file, node) is unique


class ArchiveFileCopyRequest(base_model):
    """Requests for file transfer from node to group.

    Attributes
    ----------
    file : foreign key
        Reference to the file to be copied.
    group_to : foreign key
        The storage group to which the file should be copied.
    node_from : foreign key
        The node from which the file should be copied.
    completed : bool
        Set to true when the copy has succeeded.
    cancelled : bool
        Set to true if the copy is no longer wanted.
    timestamp : datetime
        The UTC time when the request was made.
    transfer_started : datetime
        The UTC time when the transfer was started.
    transfer_completed : datetime
        The UTC time when the transfer was completed.
    """

    file = pw.ForeignKeyField(ArchiveFile, backref="requests")
    group_to = pw.ForeignKeyField(StorageGroup, backref="requests_to")
    node_from = pw.ForeignKeyField(StorageNode, backref="requests_from")
    completed = pw.BooleanField(default=False)
    cancelled = pw.BooleanField(default=False)
    timestamp = pw.DateTimeField(default=pw.utcnow, null=True)
    transfer_started = pw.DateTimeField(null=True)
    transfer_completed = pw.DateTimeField(null=True)

    class Meta:
        indexes = ((("file", "group_to", "node_from"), False),)  # non-unique index

    def check(self, node_to: StorageNode | None = None) -> bool:
        """Check whether this pull request should proceed.

        The return value indicates to the caller whether processing
        the request should continue or stop.

        Various DB checks are performed on the request.  Some of
        these checks may cancel the request (if it's no longer valid).
        Other checks may fail, but leave the request pending (so that
        it can be re-attempted later).

        Parameters
        ----------
        node_to : StorageNode, optional
            If provided, this should be the destination StorageNode (i.e.
            the node in `group_to` which will perform the pull).  If given,
            additional checks may be performed on the node.

        Returns
        -------
        result : bool
            True if processing the request should continue.  False if
            the request has been cancelled, or should be skipped.
        """
        from ..daemon import RemoteNode

        # The only label left unbound here is "result"
        comp_metric = metrics.by_name("requests_completed").bind(
            type="copy",
            node=self.node_from.name,
            group=self.group_to.name,
        )

        # What's the current situation on the destination?
        copy_state = self.group_to.state_on_node(self.file)[0]
        if copy_state == "Y":
            # We mark the AFCR cancelled rather than complete becase
            # _this_ AFCR clearly hasn't been responsible for creating
            # the file copy.
            log.info(
                f"Cancelling pull request for "
                f"{self.file.acq.name}/{self.file.name}: "
                f"already present in group {self.group_to.name}."
            )
            self.cancelled = True
            self.save(only=[ArchiveFileCopyRequest.cancelled])
            comp_metric.inc(result="duplicate")
            return False
        if copy_state == "M":
            log.warning(
                f"Skipping pull request for "
                f"{self.file.acq.name}/{self.file.name}: "
                f"existing copy in group {self.group_to.name} needs check."
            )
            return False
        if copy_state == "X":
            # If the file is corrupt, we continue with the
            # pull to overwrite the corrupt file
            pass
        elif copy_state == "N":
            # This is the expected state
            pass
        else:
            # Shouldn't get here
            log.error(
                f"Unexpected copy state: '{copy_state}' "
                f"for file ID={self.file.id} in group {self.group_to.name}."
            )
            return False

        # Skip request unless the source node is active
        if not self.node_from.active:
            log.warning(
                f"Skipping request for {self.file.acq.name}/{self.file.name}:"
                f" source node {self.node_from.name} is not active."
            )
            return False

        # If the source file doesn't exist, cancel the request.  If the
        # source is suspect, skip the request.
        state = self.node_from.filecopy_state(self.file)
        if state == "N" or state == "X":
            log.warning(
                f"Cancelling request for {self.file.acq.name}/{self.file.name}:"
                f" not available on node {self.node_from.name}."
                f" [file_id={self.file.id}]"
            )
            self.cancelled = True
            self.save(only=[ArchiveFileCopyRequest.cancelled])
            comp_metric.inc(result="missing")
            return False
        if state == "M":
            log.info(
                f"Skipping request for {self.file.acq.name}/{self.file.name}:"
                f" source needs check on node {self.node_from.name}."
            )
            return False

        # If the source file is not ready, skip the request.
        remote_note = RemoteNode(self.node_from)
        if not remote_note.io.pull_ready(self.file):
            log.debug(
                f"Skipping request for {self.file.acq.name}/{self.file.name}:"
                f" not ready on node {self.node_from.name}."
            )
            return False

        # group_to and node_from checks all pass; do node_to checks, if given
        # these checks never cancel the request.
        if node_to:
            return node_to.check_pull_dest()

        # Otherwise, request can continue
        return True

    def finish(
        self,
        node_to: StorageNode,
        size: Callable | int | None,
        success: bool,
        md5ok: bool | str,
        start_time: float,
        check_src: bool = True,
        stderr: str | None = None,
    ) -> bool:
        """Update the database after attempting this copy request.

        Parameters
        ----------
        node_to : StorageNode
            The node receiving the copy request.  Must be in `self.group_to`.
        size : Callable or int or None
            If an int or None, the storage used in the new ArchiveFileCopy record.
            If computing this would be an expensive I/O operation, this can instead be a
            callable function which will be passed the file path and should return the
            same.  In that case, the call will only be made if needed.
        success : bool
            True unless the file transfer failed.
        md5ok : boolean or str
            Either a boolean indicating if the MD5 sum was correct or
            else a string MD5 sum which we need to verify.  Ignored if
            success is not True.
        start_time : float
            time.time() when the transfer was started
        check_src : boolean
            if success is False, should the source file be marked suspect?
        stderr : str or None
            if success is False, this will be copied into the log

        Returns
        -------
        good_transfer : bool
            True if the parameters indicate the transfer was successful
            or False if the transfer failed.
        """

        # The only label left unbound here is "result"
        transf_metric = metrics.by_name("transfers").bind(
            node_from=self.node_from.name,
            group_to=self.group_to.name,
        )

        # Check the result
        if not success:
            if stderr is None:
                stderr = "Unspecified error."
                transf_metric.inc(result="failure")
            if check_src:
                # If the copy didn't work, then the remote file may be corrupted.
                log.error("Copy failed.  Marking source file suspect.")
                log.info(f"Output: {stderr}")
                ArchiveFileCopy.update(has_file="M", last_update=pw.utcnow()).where(
                    ArchiveFileCopy.file == self.file,
                    ArchiveFileCopy.node == self.node_from,
                ).execute()
                transf_metric.inc(result="check_src")
            else:
                # An error occurred that can't be due to the source being corrupt
                log.error("Copy failed")
                log.info(f"Output: {stderr}")
                transf_metric.inc(result="failure")
            return False

        # Otherwise, transfer was completed, remember end time
        end_time = time.time()

        # Check integrity.
        if isinstance(md5ok, str):
            md5ok = md5ok == self.file.md5sum
        if not md5ok:
            log.error(
                f"MD5 mismatch on node {node_to.name}; "
                f"Marking source file {self.file.name} "
                f"on node {self.node_from} suspect."
            )
            ArchiveFileCopy.update(has_file="M", last_update=pw.utcnow()).where(
                ArchiveFileCopy.file == self.file,
                ArchiveFileCopy.node == self.node_from,
            ).execute()
            transf_metric.inc(result="integrity")
            return False

        # Transfer successful
        trans_time = end_time - start_time
        rate = self.file.size_b / trans_time
        log.info(
            f"Pull of {self.file.path} complete. "
            f"Transferred {util.pretty_bytes(self.file.size_b)} "
            f"in {util.pretty_deltat(trans_time)} [{util.pretty_bytes(rate)}/s]"
        )

        with database_proxy.atomic():
            # Comput storage used if needed
            if callable(size):
                size = size(self.file.path)
            # Upsert the FileCopy
            try:
                copy = ArchiveFileCopy.create(
                    file=self.file,
                    node=node_to,
                    has_file="Y",
                    wants_file="Y",
                    ready=True,
                    size_b=size,
                    last_update=pw.utcnow(),
                )
            except pw.IntegrityError:
                copy = ArchiveFileCopy.get(file=self.file, node=node_to)
                copy.has_file = "Y"
                copy.wants_file = "Y"
                copy.ready = True
                copy.size_b = size
                copy.last_update = pw.utcnow()
                copy.save()

            # Mark ourselves as completed
            self.completed = True
            self.transfer_started = pw.utcfromtimestamp(start_time)
            self.transfer_completed = pw.utcfromtimestamp(end_time)
            self.save()

        # Update metrics
        metrics.by_name("requests_completed").inc(
            type="copy",
            node=self.node_from.name,
            group=self.group_to.name,
            result="success",
        )
        transf_metric.inc(result="success")

        # This can be used to measure throughput
        metrics.Metric(
            "pulled_bytes",
            "Count of bytes pulled",
            counter=True,
            bound={"node_from": self.node_from.name, "group_to": self.group_to.name},
        ).add(self.file.size_b)

        # Run post-add actions, if any
        copy.trigger_autoactions()

        return True


class ArchiveFileImportRequest(base_model):
    """Requests for the import of new files into a node.

    Attributes
    ----------
    node : foreign key
        The StorageNode on which the import should happen
    path : string
        The path to import.  If this is a directory and recurse is True,
        it will be recursively scanned.
    recurse : bool
        If True, recursively scan path for files, if path is a directory.
    register : bool
        If False, only files with existing ArchiveFile records will be
        imported.
    completed : bool
        Set to true when the import request has completed.
    timestamp : datetime
        The UTC time when the request was made.

    If `path` is the special value "ALPENHORN_NODE", then the request is
    a node initialisation request, instead of a normal import request.
    This only happens on active nodes which aren't already initialised:
    an initialisation request on an already initialised node is ignored.

    Note: How node initialisation works is dependent on the I/O class of
    the node.  There is no requirement for initialisation to create an
    ALPENHORN_NODE node file.
    """

    node = pw.ForeignKeyField(StorageNode, backref="import_requests")
    path = pw.CharField(max_length=1024)
    recurse = pw.BooleanField(default=False)
    register = pw.BooleanField(default=False)
    completed = pw.BooleanField(default=False)
    timestamp = pw.DateTimeField(default=pw.utcnow, null=True)
