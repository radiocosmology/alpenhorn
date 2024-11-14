"""alpenhorn node autoclean command"""

import click
import peewee as pw

from ...db import StorageGroup, StorageNode, StorageTransferAction, database_proxy
from ..cli import echo


@click.command()
@click.argument("node_name", metavar="NODE")
@click.argument("group_name", metavar="GROUP")
@click.option(
    "--remove",
    is_flag=True,
    help="Remove (instead of add) GROUP as an autoclean trigger.",
)
@click.pass_context
def autoclean(ctx, group_name, node_name, remove):
    """Manage autoclean triggers for this node.

    This allows you to add (the default) or remove (using --remove)
    the StorageGroup named GROUP as a an autoclean trigger for the
    Storage Node named NODE.

    If GROUP is added as an autoclean trigger for NODE, then, whenever
    a file is added to GROUP, it will be automatically released for
    deletion on NODE.
    """

    with database_proxy.atomic():
        try:
            node = StorageNode.get(name=node_name)
        except pw.DoesNotExist:
            raise click.ClickException(f"no such node: {node_name}")

        try:
            group = StorageGroup.get(name=group_name)
        except pw.DoesNotExist:
            raise click.ClickException(f"no such group: {group_name}")

        # Sanity check: can't autoclean within a group
        if group == node.group and not remove:
            raise click.ClickException(
                "can't enable autoclean: "
                f'Node "{node_name}" is in group "{group_name}"'
            )

        # What's the current state?
        try:
            action = StorageTransferAction.get(node_from=node, group_to=group)
            if action.autoclean is not remove:
                echo("No change")
                ctx.exit()
        except pw.DoesNotExist:
            # No need to create a record to set autoclean to zero
            if remove:
                echo("No change")
                ctx.exit()
            action = None

        # Upsert the change
        if action:
            StorageTransferAction.update(autoclean=not remove).where(
                StorageTransferAction.id == action.id
            ).execute()
        else:
            StorageTransferAction.create(
                node_from=node, group_to=group, autoclean=not remove
            )

        echo(
            'Auto-clean trigger: Group "'
            + group.name
            + ('" added' if not remove else '" removed.')
        )
