"""Functions to update the data index after a pull."""

from __future__ import annotations

import logging
import time

import peewee as pw

from .. import db
from ..common import metrics, util
from ..common.metrics import Metric
from ..db import (
    ArchiveFile,
    ArchiveFileCopy,
    ArchiveFileCopyRequest,
    StorageNode,
    StorageTransferAction,
    utcfromtimestamp,
    utcnow,
)
from ..io.base import BaseNodeIO

log = logging.getLogger(__name__)


def post_add(node: StorageNode, file_: ArchiveFile) -> None:
    """Run any actions after adding `file` to `node`.

    Possible actions are autosync or autoclean.

    Parameters
    ----------
    node : StorageNode
        The node the file copy was added to.
    file_ : ArchiveFile
        The file added.
    """

    # Autosync: find all StorageTransferActions where we're the source node
    for edge in StorageTransferAction.select().where(
        StorageTransferAction.node_from == node,
        StorageTransferAction.group_to != node.group,
        StorageTransferAction.autosync == True,  # noqa: E712
    ):
        if edge.group_to.state_on_node(file_)[0] != "Y":
            log.debug(
                f"Autosyncing {file_.path} from node {node.name} "
                f"to group {edge.group_to.name}"
            )

            ArchiveFileCopyRequest.create(
                node_from=node, group_to=edge.group_to, file=file_
            )

    # Autoclean: find all the StorageTransferActions where we're in the
    # destination group
    for edge in StorageTransferAction.select().where(
        StorageTransferAction.group_to == node.group,
        StorageTransferAction.node_from != node,
        StorageTransferAction.autoclean == True,  # noqa: E712
    ):
        count = (
            ArchiveFileCopy.update(wants_file="N", last_update=utcnow())
            .where(
                ArchiveFileCopy.file == file_,
                ArchiveFileCopy.node == edge.node_from,
                ArchiveFileCopy.has_file == "Y",
                ArchiveFileCopy.wants_file == "Y",
            )
            .execute()
        )

        if count > 0:
            log.debug(f"Autocleaning {file_.path} from node {edge.node_from.name}")


def copy_request_done(
    req: ArchiveFileCopyRequest,
    io: BaseNodeIO,
    success: bool,
    md5ok: bool | str,
    start_time: float,
    check_src: bool = True,
    stderr: str | None = None,
) -> bool:
    """Update the database after attempting a copy request.

    Parameters
    ----------
    req : ArchiveFileCopyRequest
        The copy request that was attempted
    io : Node I/O instance
        The I/O instance of the destination node
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
        node_from=req.node_from.name,
        group_to=req.group_to.name,
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
            ArchiveFileCopy.update(has_file="M", last_update=utcnow()).where(
                ArchiveFileCopy.file == req.file,
                ArchiveFileCopy.node == req.node_from,
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
        md5ok = md5ok == req.file.md5sum
    if not md5ok:
        log.error(
            f"MD5 mismatch on node {io.node.name}; "
            f"Marking source file {req.file.name} on node {req.node_from} suspect."
        )
        ArchiveFileCopy.update(has_file="M", last_update=utcnow()).where(
            ArchiveFileCopy.file == req.file,
            ArchiveFileCopy.node == req.node_from,
        ).execute()
        transf_metric.inc(result="integrity")
        return False

    # Transfer successful
    trans_time = end_time - start_time
    rate = req.file.size_b / trans_time
    log.info(
        f"Pull of {req.file.path} complete. "
        f"Transferred {util.pretty_bytes(req.file.size_b)} "
        f"in {util.pretty_deltat(trans_time)} [{util.pretty_bytes(rate)}/s]"
    )

    with db.database_proxy.atomic():
        # Upsert the FileCopy
        size = io.filesize(req.file.path, actual=True)
        try:
            ArchiveFileCopy.insert(
                file=req.file,
                node=io.node,
                has_file="Y",
                wants_file="Y",
                ready=True,
                size_b=size,
                last_update=utcnow(),
            ).execute()
        except pw.IntegrityError:
            ArchiveFileCopy.update(
                has_file="Y",
                wants_file="Y",
                ready=True,
                size_b=size,
                last_update=utcnow(),
            ).where(
                ArchiveFileCopy.file == req.file, ArchiveFileCopy.node == io.node
            ).execute()

        # Mark AFCR as completed
        ArchiveFileCopyRequest.update(
            completed=True,
            transfer_started=utcfromtimestamp(start_time),
            transfer_completed=utcfromtimestamp(end_time),
        ).where(ArchiveFileCopyRequest.id == req.id).execute()

    # Update metrics
    metrics.by_name("requests_completed").inc(
        type="copy",
        node=req.node_from.name,
        group=req.group_to.name,
        result="success",
    )
    transf_metric.inc(result="success")

    # This can be used to measure throughput
    Metric(
        "pulled_bytes",
        "Count of bytes pulled",
        counter=True,
        bound={"node_from": req.node_from.name, "group_to": req.group_to.name},
    ).add(req.file.size_b)

    # Run post-add actions, if any
    post_add(io.node, req.file)

    return True
