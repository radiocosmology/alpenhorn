"""Alpenhorn CLI for operations on `StorageGroup`s."""

import click
import peewee as pw

from ...db import StorageGroup, StorageNode

from .create import create
from .list import list_
from .modify import modify
from .rename import rename
from .show import show


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
def cli():
    """Manage Storage Groups."""


cli.add_command(create, "create")
cli.add_command(list_, "list")
cli.add_command(modify, "modify")
cli.add_command(rename, "rename")
cli.add_command(show, "show")
