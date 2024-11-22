"""alpenhorn file list command."""

import click
import peewee as pw
from tabulate import tabulate

from ...common.util import pretty_bytes
from ...db import (
    ArchiveAcq,
    ArchiveFile,
    ArchiveFileCopy,
    ArchiveFileCopyRequest,
    StorageGroup,
    StorageNode,
)
from ..cli import echo, pretty_time
from ..options import (
    cli_option,
    not_both,
    both_or_neither,
    files_in_nodes,
    files_in_groups,
    resolve_acqs,
    resolve_group,
    resolve_node,
    state_constraint,
)


def _state_flag_help(name):
    """Returns help for one of the state flags"""

    return "Limit to " + name + "files.  Must be accompanied by at least one "
    "--node or --group to provide a place to probe file state.",


@click.command()
@cli_option("acq")
@cli_option("all_", help="Limit to files that exist on all NODEs and GROUPs provided.")
@click.option("--corrupt", is_flag=True, help=_state_flag_help("corrupt"))
@click.option("--details", is_flag=True, help="Show details for listed files.")
@click.option(
    "from_",
    "--from",
    metavar="SRC",
    help="Must be specified with --to=DEST: limit to files that can be synced "
    "from Node SRC to Group DEST",
)
@click.option(
    "--group",
    metavar="GROUP",
    multiple=True,
    help="Limit to files present (or not present) in GROUP.  "
    "May be specified multiple times.",
)
@click.option("--healthy", is_flag=True, help=_state_flag_help("healthy"))
@click.option("--missing", is_flag=True, help=_state_flag_help("missing"))
@click.option(
    "--node",
    metavar="NODE",
    multiple=True,
    help="Limit to files present (or not present) on NODE.  "
    "May be specified multiple times.",
)
@click.option(
    "not_", "--not", is_flag=True, help="Limit to files absent from NODEs and GROUPs"
)
@click.option("--suspect", is_flag=True, help=_state_flag_help("suspect"))
@click.option(
    "--to",
    metavar="DEST",
    help="Must be specified with --from=SRC: limit to files that can be synced "
    "from Node SRC to Group DEST",
)
@click.pass_context
def list_(
    ctx,
    acq,
    all_,
    corrupt,
    details,
    from_,
    group,
    healthy,
    missing,
    node,
    not_,
    suspect,
    to,
):
    """List Files.

    Without options, lists all Files registered to the Data Index.
    There are several ways to limit the list of files returned:

    \b
    Limit by acqusition
    -------------------

    It is always possible to limit the list to only files in acqusition(s)
    specified in the --acq option.

    \b
    Limit by location
    -----------------

    To limit the list based on where files are or are not present, use
    the --node=NODE and/or --group=GROUP options to list places to search.
    How these are interpreted depend on the "--all" and "--not" options:

    With neither option, files will be listed if they exist on at least one of
    the NODEs or GROUPs.

    With --all, files must exist on all the listed NODEs and GROUPs.

    With --not, files must be absent from all the listed NODEs and GROUPs.

    With --not and --all, files must be absent from at least one of the NODEs or
    GROUPs.

    \b
    Limit by file state
    -------------------

    To limit the list based on file state, you can use the state flags:
    --missing, --corrupt, --suspect, --healthy.  In this case, you must specify
    at least one location (with --node or --group) to indicate where the file
    state should be checked.  If multiple state flags are used, files with any
    one of the specified state will be listed.

    The --not flag cannot be used with the state flags.

    Note: the "missing" state does not mean simply "absent".  The state "missing"
    is a special state tracked by alpenhorn in the data index for files which are
    absent from a node/group when they are expected to be present (i.e they haven't
    been released).  So, these two commands produce (potentially) different results:

    \b
    group list --not --node=NODE
    group list --missing --node=NODE

    The first lists all files absent from NODE.  The second lists only files which are
    absent but not released from the NODE (i.e. files which have been deleted by a
    third-party without alpenhorn's knowledge).

    \b
    Limit based on syncability
    --------------------------

    Using both "--from=SRC" and "--to=DEST" will limit the list to files which
    can be synced (copied) from the Node SRC to the Group DEST.  Specifically,
    this restricts the list to files which are healthy on SRC and absent from
    DEST.

    You cannot simultaneously limit both by file state and syncability.
    """

    # Usage checks.

    # Find a state flag to complain about, if any
    if corrupt:
        state_flag = "corrupt"
    elif missing:
        state_flag = "missing"
    elif healthy:
        state_flag = "healthy"
    elif suspect:
        state_flag = "suspect"
    else:
        state_flag = None

    # Can't use a state flag with --not
    not_both(not_, "not", state_flag, state_flag)
    # Must use --to and --from together
    both_or_neither(from_, "from", to, "to")
    # Can't use a state with --from or --to
    # (checking --from is enough, given the previous test)
    not_both(from_, "from", state_flag, state_flag)

    # Can't use --not or --all if we haven't used --node or --group
    if not_ or all_:
        if not node and not group:
            raise click.UsageError(
                ("--all" if all_ else "--not")
                + " cannot be used without --node or --group"
            )

    # Figure out the state constraint from the flags
    state_expr = state_constraint(
        corrupt=corrupt, healthy=healthy, missing=missing, suspect=suspect
    )

    # Do we want the intersection or union of the nodes and groups?
    # This is just XOR of --all and --not
    intersect = all_ ^ not_

    # Resolve nodes, and groups, if provided
    if node:
        nodes = resolve_node(node)
        node_files = files_in_nodes(nodes, state_expr, in_any=not intersect)
    if group:
        groups = resolve_group(group)
        group_files = files_in_groups(groups, state_expr, in_any=not intersect)

    # Now make a master list from node and group lists
    if node and group:
        if intersect:
            selected_files = node_files & group_files
        else:
            selected_files = node_files | group_files
    elif node:
        selected_files = node_files
    elif group:
        selected_files = group_files
    else:
        selected_files = None

    # Apply syncability limit.  This is just a special kind of
    # file selection where we find files in --from but not in --to.
    if from_:
        syncable_files = files_in_nodes([resolve_node(from_)]) - files_in_groups(
            [resolve_group(to)]
        )

        if selected_files is not None:
            # Merge with selected_files list
            if not_:
                # In not mode, we remove selected_files from the syncable ones,
                selected_files = syncable_files - selected_files
                # The above has converted the negative constraint to a positive one
                not_ = False
            else:
                # Here we take the intersection of the two selections (ignoring --all)
                selected_files &= syncable_files
        else:
            # Otherwise, we're just using the syncable files list
            selected_files = syncable_files

    # Handle no matching files
    if selected_files is not None and len(selected_files) == 0:
        if not_:
            # Here we were asked to omit files in the selection, but
            # there's nothing in the selection, so there's nothing to
            # omit.
            selected_files = None
        else:
            # Otherwise, we were going to list only the files in the
            # selection, but there are none, so we're done.
            ctx.exit()

    # In --details mode, extra node or group details are provided in very
    # restricted circumstatnces:
    #  1. exactly one group or exactly one node was specified
    #  2. --not wasn't used
    #  3. not restricted by syncability (i.e. no --from or --to)
    detail_node = None
    detail_group = None
    if details and not not_ and not from_:
        if node and not group and len(nodes) == 1:
            detail_node = nodes[0]
        elif group and not node and len(groups) == 1:
            detail_group = groups[0]

    # The base query
    query = (
        ArchiveFile.select()
        .join(ArchiveAcq)
        .order_by(ArchiveAcq.name, ArchiveFile.name)
    )

    # Apply the --acq limit, if given
    if acq:
        query = query.where(ArchiveFile.acq << resolve_acqs(acq))

    # Apply file selection, if any
    if selected_files is not None:
        if not_:
            query = query.where(ArchiveFile.id.not_in(selected_files))
        else:
            query = query.where(ArchiveFile.id << selected_files)

    if details:
        # Headers
        headers = ["File", "Size", "MD5 Hash", "Registration Time"]

        # Data
        file_data = {}
        for file in query.execute():
            file_data[file] = (
                file.acq.name + "/" + file.name,
                pretty_bytes(file.size_b),
                "-" if file.md5sum is None else file.md5sum.lower(),
                pretty_time(file.registered),
            )

        # Add extra details, if possible
        if detail_group:
            headers += ["State", "Node"]
            data = []
            for file in file_data:
                state_name = {
                    "Y": "Present",
                    "M": "Suspect",
                    "X": "Corrupt",
                    "N": "Absent",
                }
                state, node = detail_group.state_on_node(file)
                data.append(
                    (
                        *file_data[file],
                        state_name[state],
                        "-" if node is None else node.name,
                    )
                )
        elif detail_node:
            headers += ["State", "Size on Node"]

            # Get all the ArchiveFileCopy data in a single query
            copies = {
                copy.file: copy
                for copy in ArchiveFileCopy.select()
                .where(ArchiveFileCopy.file << [file.id for file in file_data])
                .execute()
            }

            data = []
            for file in file_data:
                copy = copies[file]
                data.append(
                    (
                        *file_data[file],
                        copies[file].state,
                        pretty_bytes(copies[file].size_b),
                    )
                )
        else:
            data = file_data.values()

        if data:
            echo(tabulate(data, headers=headers))
    else:
        echo("\n".join(file.acq.name + "/" + file.name for file in query.execute()))
