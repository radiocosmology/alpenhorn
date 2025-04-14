"""Alpenhorn CLI for operations on the database"""

import click

from ..cli import dbconnect
from .init import init


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
def cli():
    """Manage the Data Index."""

    # Don't exit on schema mismatch
    dbconnect(check=False)


cli.add_command(init, "init")
