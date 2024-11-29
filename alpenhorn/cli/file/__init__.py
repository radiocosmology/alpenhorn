"""Alpenhorn CLI for operations on `ArchiveFile`s and `ArchiveFileCopy`s."""

import click
import peewee as pw

from ..cli import dbconnect
from .clean import clean
from .create import create
from .import_ import import_
from .list import list_
from .modify import modify
from .show import show
from .sync import sync


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
def cli():
    """Manage Files."""

    dbconnect()


cli.add_command(clean, "clean")
cli.add_command(create, "create")
cli.add_command(import_, "import")
cli.add_command(list_, "list")
cli.add_command(modify, "modify")
cli.add_command(show, "show")
cli.add_command(sync, "sync")
