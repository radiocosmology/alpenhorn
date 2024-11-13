"""Alpenhorn CLI for operations on `ArchiveAcq`s."""

import re
import sys
import click
import peewee as pw

from .files import files
from .list import list_
from .show import show


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
def cli():
    """Manage Acquisitions."""


cli.add_command(files, "files")
cli.add_command(list_, "list")
cli.add_command(show, "show")
