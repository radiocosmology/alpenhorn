"""alpenhorn node create command"""

import click
import json
import peewee as pw

from ...db import database_proxy, StorageGroup, StorageNode
from ..options import client_option, exactly_one, set_storage_type, set_io_config
from ..cli import echo


@click.command()
@click.argument("node_name", metavar="NAME")
@client_option("address")
@click.option(
    "--activate",
    help="Activate the node immediately upon creation.",
    is_flag=True,
)
@client_option("archive")
@click.option(
    "--auto-import",
    help="Turn on auto-import for the node. [default: off]",
    is_flag=True,
    default=False,
)
@client_option(
    "auto_verify",
    help="If COUNT is non-zero, turn on auto-verify for the node node, with "
    "COUNT as the maximum verified copies per iteration.",
    default=0,
    show_default=True,
)
@client_option("io_class")
@click.option(
    "--create-group",
    help="Create a new Storage Group for this node.  The group will have the "
    "same name as the node (NAME) and use the Default group I/O class, which "
    "only allows one node in it.  Incompatible with --group.",
    is_flag=True,
)
@client_option(
    "field",
    help="Make node a field node (i.e. neither an archive node nor a transport "
    "node).  This is the default.  Incompatible with --archive or --transport.",
)
@client_option(
    "group",
    help="Add node to Storage Group GROUP, which must already exist.  "
    "Incompatible with --create-group",
)
@client_option("host")
@client_option("io_config")
@client_option("io_var")
@client_option("max_total")
@client_option("min_avail", default=0, show_default=True)
@client_option("notes")
@client_option("root")
@client_option("transport")
@client_option("username")
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

    io_config = set_io_config(io_config, io_var, dict())
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
            try:
                group = StorageGroup.get(name=group)
            except pw.DoesNotExist:
                raise click.ClickException("no such group: " + group)

        StorageNode.create(
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

        if create_group:
            echo(f'Created storage group "{node_name}".')
        echo(f'Created storage node "{node_name}".')
