"""Alpenhorn CLI interface for operations on `StorageNode`s."""

import click

from ..cli import dbconnect
from .activate import activate
from .autoclean import autoclean
from .clean import clean
from .create import create
from .deactivate import deactivate
from .init import init
from .list import list_
from .modify import modify
from .rename import rename
from .scan import scan
from .show import show
from .stats import stats
from .sync import sync
from .verify import verify


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
def cli():
    """Manage Storage Nodes."""

    dbconnect()


cli.add_command(activate, "activate")
cli.add_command(autoclean, "autoclean")
cli.add_command(clean, "clean")
cli.add_command(create, "create")
cli.add_command(deactivate, "deactivate")
cli.add_command(init, "init")
cli.add_command(list_, "list")
cli.add_command(modify, "modify")
cli.add_command(rename, "rename")
cli.add_command(scan, "scan")
cli.add_command(show, "show")
cli.add_command(stats, "stats")
cli.add_command(sync, "sync")
cli.add_command(verify, "verify")
