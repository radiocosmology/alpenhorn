"""Alpenhorn CLI for operations on `ArchiveAcq`s."""

import click

from ..cli import dbconnect
from .create import create
from .files import files
from .list import list_
from .show import show


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
def cli():
    """Manage Acquisitions."""

    dbconnect()


cli.add_command(create, "create")
cli.add_command(files, "files")
cli.add_command(list_, "list")
cli.add_command(show, "show")
