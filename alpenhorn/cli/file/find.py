"""alpenhorn file list command."""

import click
from tabulate import tabulate

from ...db import ArchiveFile, ArchiveFileCopy, StorageNode
from ..cli import echo
from ..options import (
    cli_option,
    resolve_acq,
    resolve_group,
    resolve_node,
    state_constraint,
)


@click.command()
@cli_option("acq")
@click.option("--corrupt", is_flag=True, help="Find corrupt files.")
@click.option(
    "--group",
    metavar="GROUP",
    multiple=True,
    help="May be specified multiple times.  Limit search to GROUP(s).",
)
@click.option("--healthy", is_flag=True, help="Find healthy files.")
@click.option("--missing", is_flag=True, help="Find missing files.")
@click.option(
    "--node",
    metavar="NODE",
    multiple=True,
    help="May be specified multiple times.  Limit search to NODE(s).",
)
@click.option("--suspect", is_flag=True, help="Find suspect files.")
@click.pass_context
def find(ctx, acq, corrupt, group, healthy, missing, node, suspect):
    """Find Files on Nodes.

    Without options, lists every healthy File on every Node in the
    Data Index.  Options can be used to change and limit the list.
    """

    nodes = resolve_node(node)
    groups = resolve_group(group)
    acqs = resolve_acq(acq)

    state_expr = state_constraint(
        corrupt=corrupt, healthy=healthy, missing=missing, suspect=suspect
    )
    if state_expr is None:
        state_expr = state_constraint(healthy=True)

    # Query
    query = ArchiveFileCopy.select().join(ArchiveFile).where(state_expr)

    # Add limits, when given
    if acqs:
        query = query.where(ArchiveFile.acq << acqs)
    if groups:
        query = query.switch(ArchiveFileCopy).join(StorageNode)
        # Groups and Nodes need to be or'd together if both provided
        if nodes:
            query = query.where(
                (ArchiveFileCopy.node << nodes) | (StorageNode.group << groups)
            )
        else:
            query = query.where(StorageNode.group << groups)
    elif nodes:
        query = query.where(ArchiveFileCopy.node << nodes)

    data = []
    for copy in query.execute():
        data.append((copy.file.path, copy.node.name, copy.state))

    if data:
        echo(tabulate(sorted(data), headers=["File", "Node", "State"]))
    else:
        echo("No match.")
