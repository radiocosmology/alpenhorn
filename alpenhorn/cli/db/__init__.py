"""Alpenhorn CLI for operations on the database"""

import click

from .init import init


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
def cli():
    """Manage the Data Index."""


cli.add_command(init, "init")
