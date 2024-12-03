"""Alpenhorn CLI for operations on `StorageGroup`s."""

import click

from ..cli import dbconnect
from .autosync import autosync
from .create import create
from .list import list_
from .modify import modify
from .rename import rename
from .show import show
from .sync import sync


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
def cli():
    """Manage Storage Groups."""

    dbconnect()


cli.add_command(autosync, "autosync")
cli.add_command(create, "create")
cli.add_command(list_, "list")
cli.add_command(modify, "modify")
cli.add_command(rename, "rename")
cli.add_command(show, "show")
cli.add_command(sync, "sync")
