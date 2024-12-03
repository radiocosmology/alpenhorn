"""alpenhorn node create command"""

import json

import click

from ...db import StorageNode, database_proxy
from ..cli import echo, update_or_remove
from ..options import (
    cli_option,
    not_both,
    resolve_group,
    resolve_node,
    set_io_config,
    set_storage_type,
)


@click.command()
@click.argument("name", metavar="NAME")
@cli_option("address")
@cli_option("archive")
@click.option(
    "--auto-import/--no-auto-import",
    help="Turn on/off auto-import for the node.",
    is_flag=True,
    default=None,
)
@cli_option("auto_verify")
@cli_option("io_class")
@cli_option("field")
@cli_option("group", help="Move node to Storage Group GROUP, which must already exist")
@cli_option("host")
@cli_option("io_config")
@cli_option("io_var")
@cli_option("max_total")
@click.option(
    "--no-max-total", is_flag=True, help="Remove any existing max-total limit"
)
@cli_option("min_avail")
@cli_option("notes")
@cli_option("root")
@cli_option("transport")
@cli_option("username")
def modify(
    name,
    address,
    archive,
    auto_import,
    auto_verify,
    io_class,
    field,
    group,
    host,
    io_config,
    io_var,
    max_total,
    no_max_total,
    min_avail,
    notes,
    root,
    transport,
    username,
):
    """Modify a Storage Node.

    This modifies metadata for the Storage Node named NAME, updating field
    specified in the options.

    Other node metadata is modified in other ways:

    \b
    * To activate a node, use:            node activate
    * To deactivate a node, use:          node deactivate
    * To change the name of a node, use:  node rename

    A note on the distinction between HOST and ADDR: the host specifies which
    alpenhorn server instance is responsible for managing the node.  the address
    (and username) are used by remote servers when pulling files off this node.
    """

    # usage checks.
    not_both(max_total is not None, "max-total", no_max_total, "no-max-total")
    if max_total is not None and max_total <= 0:
        raise click.UsageError("--max-total must be positive")
    if auto_verify is not None and auto_verify < 0:
        raise click.UsageError("--auto-verify must be non-negative")
    if min_avail is not None and min_avail < 0:
        raise click.UsageError("--min-avail must be non-negative")

    storage_type = set_storage_type(archive, field, transport, none_ok=True)

    with database_proxy.atomic():
        node = resolve_node(name)

        # Get group
        if group:
            group = resolve_group(group)

        io_config = set_io_config(io_config, io_var, node.io_config)
        if io_config:
            io_config = json.dumps(io_config)

        updates = {}

        # Find updated fields
        updates |= update_or_remove("address", address, node.address)
        updates |= update_or_remove("host", host, node.host)
        updates |= update_or_remove("io_class", io_class, node.io_class)
        updates |= update_or_remove("notes", notes, node.notes)
        updates |= update_or_remove("root", root, node.root)
        updates |= update_or_remove("username", username, node.username)

        if no_max_total and node.max_total_gb is not None:
            updates["max_total_gb"] = None
        elif max_total is not None and max_total != node.max_total_gb:
            updates["max_total_gb"] = max_total

        if auto_import is not None and auto_import is not node.auto_import:
            updates["auto_import"] = auto_import
        if auto_verify is not None and auto_verify != node.auto_verify:
            updates["auto_verify"] = auto_verify
        if group and group != node.group:
            updates["group"] = group
        if io_config != node.io_config:
            updates["io_config"] = io_config
        if min_avail is not None and min_avail != node.min_avail_gb:
            updates["min_avail_gb"] = min_avail
        if storage_type and storage_type != node.storage_type:
            updates["storage_type"] = storage_type

        if updates:
            StorageNode.update(**updates).where(StorageNode.id == node.id).execute()
            echo("Node updated.")
        else:
            echo("No change.")
