"""Alpenhorn client interface."""

import click
import datetime
import logging
import peewee as pw

from .. import db
from ..common.util import start_alpenhorn, version_option
from ..db import (
    ArchiveAcq,
    ArchiveFile,
    ArchiveFileCopy,
    ArchiveFileCopyRequest,
    StorageGroup,
    StorageNode,
    StorageTransferAction,
)

from . import acq, group, node, transport

log = logging.getLogger(__name__)


def _verbosity_from_cli(verbose: int, debug: int, quiet: int) -> int:
    """Get client verbosity from command line.

    Processes the --verbose, --debug and --quiet flags to determine
    the requested verbosity."""

    if quiet and verbose:
        raise click.UsageError("Cannot use both --quiet and --verbose.")
    if quiet and debug:
        raise click.UsageError("Cannot use both --quiet and --debug.")

    # Default verbosity is 3.  --quiet decreases it.  --verbose increases it.

    # Max verbosity
    if debug or verbose > 2:
        return 5
    # Min verbosity
    if quiet > 2:
        return 1

    return 3 + verbose - quiet


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@version_option
@click.option(
    "--conf",
    "-c",
    type=click.Path(exists=True),
    help="Configuration file to read.",
    default=None,
    metavar="FILE",
)
@click.option(
    "--quiet",
    "-q",
    help="Decrease verbosity.  May be specified mulitple times: "
    "once suppresses normal client output, leaving only warning "
    "and error message.  A second use also suppresses warnings.",
    count=True,
)
@click.option(
    "--verbose",
    "-v",
    help="Increase verbosity.  May be specified mulitple times: "
    "once enables informational messages.  A second use also "
    "enables debugging messages.",
    count=True,
)
@click.option(
    "--debug",
    help="Maximum verbosity.",
    is_flag=True,
    show_default=False,
    default=False,
)
def cli(conf, quiet, verbose, debug):
    """Client interface for alpenhorn."""

    # Initialise alpenhorn
    start_alpenhorn(
        conf, client=True, verbosity=_verbosity_from_cli(verbose, debug, quiet)
    )


@cli.command()
def init():
    """Initialise an alpenhorn database.

    Creates the database tables required for alpenhorn and any extensions
    specified in its configuration.
    """

    # Create any alpenhorn core tables
    core_tables = [
        ArchiveAcq,
        ArchiveFile,
        ArchiveFileCopy,
        ArchiveFileCopyRequest,
        StorageGroup,
        StorageNode,
        StorageTransferAction,
    ]

    db.database_proxy.create_tables(core_tables, safe=True)

    # TODO Create any tables registered by extensions


@cli.command()
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


@cli.command()
@click.option(
    "--all", help="Show the status of all nodes, not just active ones.", is_flag=True
)
def status(all):
    """Summarise the status of alpenhorn storage nodes."""

    import tabulate

    # Data to fetch from the database (node name, total files, total size)
    query_info = (
        StorageNode.name,
        pw.fn.Count(ArchiveFileCopy.id).alias("count"),
        pw.fn.Sum(ArchiveFile.size_b).alias("total_size"),
        StorageNode.host,
        StorageNode.root,
    )

    # Per node totals
    nodes = (
        StorageNode.select(*query_info)
        .join(
            ArchiveFileCopy,
            pw.JOIN.LEFT_OUTER,
            on=(
                (StorageNode.id == ArchiveFileCopy.node_id)
                & (ArchiveFileCopy.has_file == "Y")
            ),
        )
        .join(
            ArchiveFile,
            pw.JOIN.LEFT_OUTER,
            on=(ArchiveFile.id == ArchiveFileCopy.file_id),
        )
        .group_by(StorageNode)
        .order_by(StorageNode.name)
    )

    log.info("Nodes: %s (all=%s)" % (nodes.count(), all))
    if not all:
        nodes = nodes.where(StorageNode.active)

    log.info("Nodes: %s" % nodes.count())

    # Totals for the whole archive
    total_count, total_size = ArchiveFile.select(
        pw.fn.Count(ArchiveFile.id).alias("count"),
        pw.fn.Sum(ArchiveFile.size_b).alias("total_size"),
    ).scalar(as_tuple=True)

    # Create table of node stats to present to the user
    data = []
    for node in nodes.tuples():
        node_name, file_count, file_size, node_host, node_root = node
        pct_count = (100.0 * file_count / total_count) if total_count else None
        pct_size = (
            (100.0 * float(file_size / total_size))
            if total_count and file_size
            else None
        )
        file_size_tb = (float(file_size) / 2**40.0) if file_count else None
        node_path = "%s:%s" % (node_host, node_root)
        data.append(
            [node_name, file_count, file_size_tb, pct_count, pct_size, node_path]
        )

    headers = ["Node", "Files", "Size [TB]", "Files [%]", "Size [%]", "Path"]

    print(tabulate.tabulate(data, headers=headers, floatfmt=".1f"))


cli.add_command(acq.cli, "acq")
cli.add_command(group.cli, "group")
cli.add_command(node.cli, "node")
cli.add_command(transport.cli, "transport")
