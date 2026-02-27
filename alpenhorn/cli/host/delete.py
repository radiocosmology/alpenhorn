"""alpenhorn host delete command"""

import click

from ...db import StorageHost, StorageNode, database_proxy
from ..cli import check_then_update, echo
from ..options import cli_option, not_both, resolve_host


def _run_query(update, ctx, host, remove_nodes, first_time):
    """This runs the delete query, either in check mode or update mode.

    This function is called via `alpenhorn.cli.cli.check_then_update`,
    which sets some of the parameters.

    Parameters
    ----------
    update : bool
        True if we're performing the update; False otherwise.
    ctx
        The Click context
    host : StorageHost
        The StorageHost we're going to delete.
    remove_nodes : bool
        The --remove-nodes flag from the command line
    first_time : bool
        True the first time this function is called.  False the second time.
    """

    with database_proxy.atomic():
        # If updating and removing nodes, just do that
        if update and remove_nodes:
            count = (
                StorageNode.update(host=None).where(StorageNode.host == host).execute()
            )
        else:
            count = 0

        # Now collect a list of nodes present
        node_list = [
            node.name for node in StorageNode.select().where(StorageNode.host == host)
        ]

        # Finally, delete the host, if possible
        if update and not node_list:
            result = StorageHost.delete().where(StorageHost.id == host.id).execute()
        else:
            result = 0

    # Now report
    if node_list and (update or not remove_nodes):
        echo('Unable to delete host with nodes: "' + '", "'.join(node_list) + '"')
        ctx.exit()

    if update:
        if count:
            nodes = "node" if count == 1 else "nodes"
            echo(f'Removed {count} {nodes} from host "{host.name}".')
        if result:
            echo(f'Deleted host "{host.name}".')
        else:
            # Don't know why, but it didn't happen...
            echo(f'Host "{host.name}" not deleted.')
    else:
        if node_list:
            count = len(node_list)
            nodes = "node" if count == 1 else "nodes"
            echo(f'Would remove {count} {nodes} from host "{host.name}":')
            for node in node_list:
                echo("  " + node)
        echo(f'Would delete Storage Host "{host.name}".')


@click.command()
@click.argument("hostname", metavar="HOST")
@cli_option("check")
@cli_option("force")
@click.option(
    "--remove-nodes",
    help="Remove any nodes from HOST before deletion.  Without this "
    "flag, nodes on HOST will block deletion.",
    is_flag=True,
)
@click.pass_context
def delete(
    ctx,
    hostname,
    check,
    force,
    remove_nodes,
):
    """Delete a Storage Host.

    This will delete the host named HOST from the database.  Unless --remove-nodes
    is used, this will fail if the host currently contains nodes.
    """

    # Usage checks
    not_both(check, "check", force, "force")

    check_then_update(
        not force,
        not check,
        _run_query,
        ctx,
        args=[resolve_host(hostname), remove_nodes],
    )
