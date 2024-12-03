"""alpenhorn node scan command"""

import pathlib

import click

from ...db import ArchiveFileImportRequest, database_proxy
from ..cli import echo
from ..options import resolve_node


@click.command()
@click.argument("name", metavar="NODE")
@click.argument("path", metavar="PATH", required=False, default=".")
@click.option(
    "--register-new",
    "-n",
    help="Register unknown files instead of ignoring them.",
    is_flag=True,
)
def scan(name, path, register_new):
    """Scan a node for files to import.

    This command will submit a request to ask the daemon managing NODE to
    scan PATH for new files to add to the Node.  if PATH is absolute, it
    must start with the Node root path.  Otherwise, it's assumed PATH is
    relative to Node root.  If not given, PATH defaults to "." (i.e. the scan
    is performed on the entire Node).

    By default, only files already registered in the data index will be imported
    to NODE.  Unknown files will be skipped.  This can be changed, however, by
    using the --register-new flag, which tells the daemon to create new records
    for new files which are determined to be importable.
    """

    with database_proxy.atomic():
        node = resolve_node(name)

        # Strip node root, if absolute
        path = pathlib.PurePath(path)

        if path.is_absolute():
            if node.root is None:
                raise click.ClickException(
                    f'PATH can\'t be absolute: Node "{node.name}" has no root.'
                )

            try:
                path = path.relative_to(node.root)
            except ValueError as e:
                raise click.ClickException(
                    f'absolute path "{path}" outside node root: ' + node.root
                ) from e

        # submit the request
        ArchiveFileImportRequest.create(
            node=node, path=str(path), recurse=True, register=register_new
        )

        echo(f'Added request for scan of "{path}" on Node "{node.name}".')
