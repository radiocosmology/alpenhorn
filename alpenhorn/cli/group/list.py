"""alpenhorn group list command"""

import click
from tabulate import tabulate

from ...db import StorageGroup
from ..cli import echo


@click.command()
def list_():
    """List all storage groups."""

    data = StorageGroup.select(
        StorageGroup.name, StorageGroup.io_class, StorageGroup.notes
    ).tuples()
    if data:
        # Add Default I/O class where needed
        data = list(data)
        for index, row in enumerate(data):
            if row[1] is None:
                data[index] = (row[0], "Default", row[2])
        echo(tabulate(data, headers=["Name", "I/O Class", "Notes"]))
    else:
        echo("No storage groups found.")
