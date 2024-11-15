"""Alpenhorn command-line interface."""

from __future__ import annotations

import click
import datetime
import peewee as pw

from ..common.logger import echo as echo
from ..db import (
    ArchiveAcq,
    ArchiveFile,
    ArchiveFileCopy,
    ArchiveFileCopyRequest,
    StorageGroup,
    StorageNode,
    StorageTransferAction,
)


def update_or_remove(field: str, new: str | None, old: str | None) -> dict:
    """Helper for metadata updates.

    A helper function to determine whether a field needs updating.
    Only works on string fields.  (Numeric fields need some other
    mechanism to let the user specify None/null when necessary.)

    Parameters:
    -----------
    field:
        Name of the field to update
    new:
        The new value of the field, maybe.  From the user.  If this
        is None, there is no update.  If this is the empty string,
        the field should be set to None/null if not that already.
    old:
        The current value of the field, which might be None/null.
        From the database.

    Returns
    -------
    result :  dict
        Has at most one key, `field`, which is present only
        if this function determines an update is required.  It should
        be merged with the dict of updates by the caller.
    """

    # This implies the user didn't specify anything for this field
    if new is None:
        return {}

    # Check for empty string, which means: set to None
    if new == "":
        if old is not None:
            return {field: None}
        return {}

    # Otherwise, new is an actual value. Set it, if different than old
    if new != old:
        return {field: new}

    # Otherwise, no change.  Return no update.
    return {}


# The rest of this file were top-level commands that have been
# temporarily dummied out while they're re-tooled.


# @cli.command()
@click.argument("node_name", metavar="NODE")
@click.argument("group_name", metavar="GROUP")
@click.option(
    "--acq", help="Sync only this acquisition.", metavar="ACQ", type=str, default=None
)
@click.option("--force", "-f", help="proceed without confirmation", is_flag=True)
@click.option("--nice", "-n", help="nice level for transfer", default=0)
@click.option(
    "--target",
    metavar="TARGET_GROUP",
    default=None,
    type=str,
    help="Only transfer files not available on this group.",
)
@click.option(
    "--transport",
    "-t",
    is_flag=True,
    help="[DEPRECATED] transport mode: only copy if fewer than two archived copies exist.",
)
@click.option("--show_acq", help="Summarise acquisitions to be copied.", is_flag=True)
@click.option("--show_files", help="Show files to be copied.", is_flag=True)
def sync(
    node_name, group_name, acq, force, nice, target, transport, show_acq, show_files
):
    """Copy all files from NODE to GROUP that are not already present.

    We can also use the --target option to only transfer files that are not
    available on both the destination group, and the TARGET_GROUP. This is
    useful for transferring data to a staging location before going to a final
    archive (e.g. HPSS, transport disks).
    """

    try:
        from_node = StorageNode.get(name=node_name)
    except pw.DoesNotExist:
        click.echo('Node "%s" does not exist in the DB.' % node_name)
        exit(1)
    try:
        to_group = StorageGroup.get(name=group_name)
    except pw.DoesNotExist:
        click.echo('Group "%s" does not exist in the DB.' % group_name)
        exit(1)

    # Construct list of file copies that are available on the source node, and
    # not available on any nodes at the destination. This query is quite complex
    # so I've broken it up...

    # First get the nodes at the destination...
    nodes_at_dest = StorageNode.select().where(StorageNode.group == to_group)

    # Then use this to get a list of all files at the destination...
    files_at_dest = (
        ArchiveFile.select()
        .join(ArchiveFileCopy)
        .where(ArchiveFileCopy.node << nodes_at_dest, ArchiveFileCopy.has_file == "Y")
    )

    # Then combine to get all file(copies) that are available at the source but
    # not at the destination...
    copy = ArchiveFileCopy.select().where(
        ArchiveFileCopy.node == from_node,
        ArchiveFileCopy.has_file == "Y",
        ~(ArchiveFileCopy.file << files_at_dest),
    )

    # If the target option has been specified, only copy nodes also not
    # available there...
    if target is not None:
        # Fetch a reference to the target group
        try:
            target_group = StorageGroup.get(name=target)
        except pw.DoesNotExist:
            click.echo('Target group "%s" does not exist in the DB.' % target)
            exit(1)

        # First get the nodes at the destination...
        nodes_at_target = StorageNode.select().where(StorageNode.group == target_group)

        # Then use this to get a list of all files at the destination...
        files_at_target = (
            ArchiveFile.select()
            .join(ArchiveFileCopy)
            .where(
                ArchiveFileCopy.node << nodes_at_target,
                ArchiveFileCopy.has_file == "Y",
            )
        )

        # Only match files that are also not available at the target
        copy = copy.where(~(ArchiveFileCopy.file << files_at_target))

    # In transport mode (DEPRECATED) we only move files that don't have an
    # archive copy elsewhere...
    if transport:
        import warnings

        warnings.warn("Transport mode is deprecated. Try to use --target instead.")

        # Get list of other archive nodes
        other_archive_nodes = StorageNode.select().where(
            StorageNode.storage_type == "A", StorageNode.id != from_node
        )

        files_in_archive = (
            ArchiveFile.select()
            .join(ArchiveFileCopy)
            .where(
                ArchiveFileCopy.node << other_archive_nodes,
                ArchiveFileCopy.has_file == "Y",
            )
        )

        copy = copy.where(~(ArchiveFileCopy.file << files_in_archive))

    # Join onto ArchiveFile for later query parts
    copy = copy.join(ArchiveFile)

    # If requested, limit query to a specific acquisition...
    if acq is not None:
        # Fetch acq if specified
        try:
            acq = ArchiveAcq.get(name=acq)
        except pw.DoesNotExist:
            raise Exception('Acquisition "%s" does not exist in the DB.' % acq)

        # Restrict files to be in the acquisition
        copy = copy.where(ArchiveFile.acq == acq)

    if not copy.count():
        print("No files to copy from node %s." % (node_name))
        return

    # Show acquisitions based summary of files to be copied
    if show_acq:
        acqs = [c.file.acq.name for c in copy]

        import collections

        for acq, count in collections.Counter(acqs).items():
            print("%s [%i files]" % (acq, count))

    # Show all files to be copied
    if show_files:
        for c in copy:
            print("%s/%s" % (c.file.acq.name, c.file.name))

    size_bytes = copy.select(pw.fn.Sum(ArchiveFile.size_b)).scalar()
    size_gb = int(size_bytes) / 1073741824.0

    print(
        "Will request that %d files (%.1f GB) be copied from node %s to group %s."
        % (copy.count(), size_gb, node_name, group_name)
    )

    if not (force or click.confirm("Do you want to proceed?")):
        print("Aborted.")
        return

    dtnow = datetime.datetime.now()

    # Perform update in a transaction to avoid any clobbering from concurrent updates
    with ArchiveFileCopyRequest._meta.database.atomic():
        # Get a list of all the file ids for the copies we should perform
        files_ids = [c.file_id for c in copy]

        # Get a list of all the file ids for exisiting requests
        requests = ArchiveFileCopyRequest.select().where(
            ArchiveFileCopyRequest.group_to == to_group,
            ArchiveFileCopyRequest.node_from == from_node,
            ~ArchiveFileCopyRequest.completed,
            ~ArchiveFileCopyRequest.cancelled,
        )
        req_file_ids = [req.file_id for req in requests]

        # Separate the files into ones that already have requests and ones that don't
        files_in = [x for x in files_ids if x in req_file_ids]
        files_out = [x for x in files_ids if x not in req_file_ids]

        click.echo(
            "Adding {} new requests{}.\n".format(
                len(files_out) or "no",
                (
                    ", keeping {} already existing".format(len(files_in))
                    if len(files_in)
                    else ""
                ),
            )
        )

        # Insert any new requests
        if len(files_out) > 0:
            # Construct a list of all the rows to insert
            insert = [
                {
                    "file": fid,
                    "node_from": from_node,
                    "nice": 0,
                    "group_to": to_group,
                    "completed": False,
                    "timestamp": dtnow,
                }
                for fid in files_out
            ]

            # Do a bulk insert of these new rows
            ArchiveFileCopyRequest.insert_many(insert).execute()
