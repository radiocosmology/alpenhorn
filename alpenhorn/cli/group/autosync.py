"""alpenhorn group autosync command"""

import click
import peewee as pw

from ...db import StorageGroup, StorageNode, StorageTransferAction, database_proxy
from ..cli import echo


@click.command()
@click.argument("group_name", metavar="GROUP")
@click.argument("node_name", metavar="NODE")
@click.option(
    "--remove",
    is_flag=True,
    help="Remove (instead of add) NODE as an autosync source.",
)
@click.pass_context
def autosync(ctx, group_name, node_name, remove):
    """Manage autosync sources for this group.

    This allows you to add (the default) or remove (using --remove)
    the StorageNode named NODE as a an autosync souce for the Storage
    Group named GROUP.

    If NODE is added as an autosync source for GROUP, then, whenever
    a file is added to NODE, it will be automatically synced into the
    Group GROUP, so long as the file isn't already present in the Group.
    """

    with database_proxy.atomic():
        try:
            group = StorageGroup.get(name=group_name)
        except pw.DoesNotExist:
            raise click.ClickException(f"no such group: {group_name}")

        try:
            node = StorageNode.get(name=node_name)
        except pw.DoesNotExist:
            raise click.ClickException(f"no such node: {node_name}")

        # Sanity check: can't autosync within a group
        if group == node.group and not remove:
            raise click.ClickException(
                "can't enable autosync: "
                f'Node "{node_name}" is in group "{group_name}"'
            )

        # What's the current state?
        try:
            action = StorageTransferAction.get(node_from=node, group_to=group)
            if action.autosync is not remove:
                echo("No change")
                ctx.exit()
        except pw.DoesNotExist:
            # No need to create a record to set autosync to zero
            if remove:
                echo("No change")
                ctx.exit()
            action = None

        # Upsert the change
        if action:
            StorageTransferAction.update(autosync=not remove).where(
                StorageTransferAction.id == action.id
            ).execute()
        else:
            StorageTransferAction.create(
                node_from=node, group_to=group, autosync=not remove
            )

        echo(
            'Auto-sync from "'
            + node.name
            + ('" started' if not remove else '" stopped.')
        )