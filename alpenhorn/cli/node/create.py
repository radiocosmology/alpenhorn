"""alpenhorn node create command"""

import json

import click
import peewee as pw

from ...db import ArchiveFileImportRequest, StorageGroup, StorageNode, database_proxy
from ..cli import echo
from ..options import (
    cli_option,
    exactly_one,
    resolve_group,
    set_io_config,
    set_storage_type,
)


@click.command()
@click.argument("node_name", metavar="NAME")
@cli_option("address")
@click.option(
    "--activate",
    help="Activate the node immediately upon creation.",
    is_flag=True,
)
@cli_option("archive")
@click.option(
    "--auto-import",
    help="Turn on auto-import for the node. [default: off]",
    is_flag=True,
    default=False,
)
@cli_option(
    "auto_verify",
    help="If COUNT is non-zero, turn on auto-verify for the node node, with "
    "COUNT as the maximum verified copies per iteration.",
    default=0,
    show_default=True,
)
@cli_option("io_class")
@click.option(
    "--create-group",
    help="Create a new Storage Group for this node.  The group will have the "
    "same name as the node (NAME) and use the Default group I/O class, which "
    "only allows one node in it.  Incompatible with --group.",
    is_flag=True,
)
@cli_option(
    "field",
    help="Make node a field node (i.e. neither an archive node nor a transport "
    "node).  This is the default.  Incompatible with --archive or --transport.",
)
@cli_option(
    "group",
    help="Add node to Storage Group GROUP, which must already exist.  "
    "Incompatible with --create-group",
)
@cli_option("host")
@click.option(
    "--init", is_flag=True, help='Initialise the new node (see "node init --help")'
)
@cli_option("io_config")
@cli_option("io_var")
@cli_option("max_total")
@cli_option("min_avail", default=0, show_default=True)
@cli_option("notes")
@cli_option("root")
@cli_option("transport")
@cli_option("username")
def create(
    node_name,
    address,
    activate,
    archive,
    auto_import,
    auto_verify,
    io_class,
    create_group,
    field,
    group,
    host,
    init,
    io_config,
    io_var,
    max_total,
    min_avail,
    notes,
    root,
    transport,
    username,
):
    """Create a new Storage Node.

    The Storage Node will be called NAME, which must not already exist.

    All node must be part of a Storage Group.  Specify an existing Storage
    Group with --group, or else create a single-node group called NAME for
    this node with --create-group.

    When the node is created, it must be assigned a role.  Use --archive to
    specify an archive node, or --transport for transport nodes.  Using neither
    of these flags makes the node a field node, which is the default.  The
    --field flag can be used to explicitly indicate this, but that is not
    required.

    A note on the distinction between HOST and ADDR: the host specifies which
    alpenhorn server instance is responsible for managing the node.  the address
    (and username) are used by remote servers when pulling files off this node.
    """

    # usage checks.
    exactly_one(create_group, "create-group", group, "group")
    if max_total is not None and max_total <= 0:
        raise click.UsageError("--max-total must be positive")
    if auto_verify < 0:
        raise click.UsageError("--auto-verify must be non-negative")
    if min_avail < 0:
        raise click.UsageError("--min-avail must be non-negative")

    io_config = set_io_config(io_config, io_var, {})
    storage_type = set_storage_type(archive, field, transport)

    with database_proxy.atomic():
        # Check name
        try:
            node = StorageNode.get(name=node_name)
            raise click.ClickException(f'node "{node_name}" already exists.')
        except pw.DoesNotExist:
            pass

        # Get/create group
        if create_group:
            try:
                StorageGroup.get(name=node_name)
                raise click.ClickException(f'group "{node_name}" already exists.')
            except pw.DoesNotExist:
                group = StorageGroup.create(name=node_name)
        else:
            group = resolve_group(group)

        node = StorageNode.create(
            name=node_name,
            root=root,
            host=host,
            address=address,
            io_class=io_class,
            group=group,
            active=activate,
            auto_import=auto_import,
            auto_verify=auto_verify,
            storage_type=storage_type,
            max_total_gb=max_total,
            min_avail_gb=min_avail,
            notes=notes,
            username=username,
            io_config=json.dumps(io_config) if io_config else None,
        )

        # Create AFIR for node init
        if init:
            ArchiveFileImportRequest.create(node=node, path="ALPENHORN_NODE")

    if create_group:
        echo(f'Created storage group "{node_name}".')
    echo(f'Created storage node "{node_name}".')
