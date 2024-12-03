"""alpenhorn group create command"""

import json

import click
import peewee as pw

from ...db import StorageGroup, database_proxy
from ..cli import echo
from ..options import cli_option, set_io_config


@click.command()
@click.argument("group_name", metavar="NAME")
@cli_option("io_class", default="Default", show_default=True)
@cli_option("io_config")
@cli_option("io_var")
@cli_option("notes")
def create(group_name, io_class, io_config, io_var, notes):
    """Create a new storage group.

    The group will be called NAME, which must not already exist.
    """

    io_config = set_io_config(io_config, io_var, {})

    with database_proxy.atomic():
        try:
            StorageGroup.get(name=group_name)
            raise click.ClickException(f'Group "{group_name}" already exists.')
        except pw.DoesNotExist:
            pass

        StorageGroup.create(
            name=group_name,
            notes=notes,
            io_class=io_class,
            io_config=json.dumps(io_config) if io_config else None,
        )
        echo(f'Created storage group "{group_name}".')
