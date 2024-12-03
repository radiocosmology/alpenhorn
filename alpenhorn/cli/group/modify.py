"""alpenhorn group modify command"""

import json

import click
import peewee as pw

from ...db import StorageGroup, database_proxy
from ..cli import echo
from ..options import cli_option, set_io_config


@click.command()
@click.argument("group_name", metavar="GROUP")
@cli_option("io_class")
@cli_option("io_config")
@cli_option("io_var")
@cli_option("notes")
def modify(group_name, io_class, io_config, io_var, notes):
    """Modify storage group metadata.

    Modifies the metadata of the storage group named GROUP, which must already exist.

    NOTE: to change the name of a storage group, use:

       alpenhorn group rename
    """

    if notes == "":
        notes = None
    if io_class == "":
        io_class = None

    with database_proxy.atomic():
        try:
            group = StorageGroup.get(name=group_name)
        except pw.DoesNotExist:
            raise click.ClickException(f'Storage group "{group_name}" does not exist.')

        io_config = set_io_config(io_config, io_var, group.io_config)

        # collect the updated parameters
        updates = {}
        if notes != group.notes:
            updates["notes"] = notes
        if io_class != group.io_class:
            updates["io_class"] = io_class
        if io_config != group.io_config:
            updates["io_config"] = json.dumps(io_config)

        # Update if necessary.
        if updates:
            update = StorageGroup.update(**updates).where(StorageGroup.id == group.id)
            update.execute()
            echo("Storage group updated.")
        else:
            echo("Nothing to do.")
