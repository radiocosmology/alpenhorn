"""alpenhorn group rename command"""

import click
import peewee as pw

from ...db import StorageGroup, database_proxy
from ..cli import echo


@click.command()
@click.argument("group_name", metavar="GROUP")
@click.argument("new_name", metavar="NEW_NAME")
def rename(group_name, new_name):
    """Rename a storage group.

    The existing storage group named GROUP will be renamed to NEW_NAME.
    NEW_NAME must not already be the name of another group.
    """

    if group_name == new_name:
        # The easy case
        echo("No change.")
    else:
        with database_proxy.atomic():
            try:
                StorageGroup.get(name=new_name)
                raise click.ClickException(
                    f'Storage group "{group_name}" already exists.'
                )
            except pw.DoesNotExist:
                pass

            try:
                group = StorageGroup.get(name=group_name)
                group.name = new_name
                group.save()
                echo(f'Storage group "{group_name}" renamed to "new_name"')
            except pw.DoesNotExist:
                raise click.ClickException(f"No such storage group: {group_name}.")
