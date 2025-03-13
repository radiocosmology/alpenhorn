"""alpenhorn node init command"""

import click

from ...db import ArchiveFileImportRequest, database_proxy
from ..cli import echo
from ..options import resolve_node


@click.command()
@click.argument("name", metavar="NODE")
def init(name):
    """Initialise a known Storage Node.

    This requests the daemon initialise the Node named NODE.  NODE must
    already exist in the data index.  NODE must be active for the daemon
    to act on this request.

    What initialisation means can vary by node I/O class, but typically
    it means creating the top-level "ALPENHORN_NODE" file.

    Note: to create a new node, use "node create".  Node initialsation can
    also happen at node creation time by using the "--init" flag with
    "node create".
    """

    with database_proxy.atomic():
        node = resolve_node(name)

        # Add request
        ArchiveFileImportRequest.create(node=node, path="ALPENHORN_NODE")

    echo(f'Requested initialisation of Node "{name}".')
