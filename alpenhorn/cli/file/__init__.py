"""Alpenhorn CLI for operations on `ArchiveFile`s and `ArchiveFileCopy`s."""

import click
import peewee as pw

from .show import show


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
def cli():
    """Manage Files."""


cli.add_command(show, "show")
