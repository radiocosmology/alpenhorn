"""alpenhorn file list command."""

import click
from tabulate import tabulate

from ...common.util import pretty_bytes
from ...db import ArchiveAcq, ArchiveFile, ArchiveFileCopy
from ..cli import echo, pretty_time
from ..options import (
    both_or_neither,
    cli_option,
    files_in_groups,
    files_in_nodes,
    not_both,
    resolve_acq,
    resolve_group,
    resolve_node,
    state_constraint,
)


def _state_flag_help(name):
    """Returns help for one of the state flags"""

    return (
        "Limit to " + name + " files.  Must be accompanied by at least one "
        "--node or --group to provide a place to probe file state."
    )


@click.command()
@click.option(
    "--absent-group",
    metavar="GROUP",
    multiple=True,
    help="Limit to files absent from GROUP.  May be specified multiple times.",
)
@click.option(
    "--absent-node",
    metavar="NODE",
    multiple=True,
    help="Limit to files absent from NODE.  May be specified multiple times.",
)
@cli_option("acq")
@cli_option(
    "all_",
    help="Limit to files that statisfy all --absent-group, "
    "--absent-node, --node, and --group constraints (instead of just one).",
)
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
    help="Limit to files present in GROUP.  May be specified multiple times.",
)
@click.option("--healthy", is_flag=True, help=_state_flag_help("healthy"))
@click.option("--missing", is_flag=True, help=_state_flag_help("missing"))
@click.option(
    "--node",
    metavar="NODE",
    multiple=True,
    help="Limit to files present on NODE.  May be specified multiple times.",
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
    absent_group,
    absent_node,
    acq,
    all_,
    corrupt,
    details,
    from_,
    group,
    healthy,
    missing,
    node,
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

    To limit the list based on where a file is, or is not, present, use the
    --absent-node=NODE, --absent-group=GROUP, --node=NODE and/or --group=GROUP
    options to list places to search.

    The first two of these options are negative constraints (indicating nodes or
    groups which must not have the file) and the last two are positive constraints
    (nodes or groups which must have the file).

    Normally, only one of the location constraints given need be satisfied for a
    file to be listed, but if you also use "--all", then a file will be listed only
    if all location constraints are satisfied.

    \b
    Limit by file state
    -------------------

    To limit the list based on file state, you can use the state flags:
    --missing, --corrupt, --suspect, --healthy.  In this case, you must specify
    at least one location (with --node or --group) to indicate where the file
    state should be checked.  If multiple state flags are used, files with any
    one of the specified state will be listed.

    Note: the "missing" state does not mean simply "absent".  The state "missing"
    is a special state tracked by alpenhorn in the data index for files which are
    absent from a node/group when they are expected to be present (i.e they haven't
    been released).  So, these two commands produce (potentially) different results:

    \b
    group list --absent-node=NODE
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

    # Must use --to and --from together
    both_or_neither(from_, "from", to, "to")
    # Can't use a state with --from or --to
    # (checking --from is enough, given the previous test)
    not_both(from_, "from", state_flag, state_flag)

    # Can't use --all if we haven't used a location constraint
    if all_:
        if not node and not group and not absent_group and not absent_node:
            raise click.UsageError(
                "--all cannot be used without a location constraint "
                "(one or more of --absent-group, --absent-node, --group, --node)"
            )

    # Figure out the state constraint from the flags
    state_expr = state_constraint(
        corrupt=corrupt, healthy=healthy, missing=missing, suspect=suspect
    )

    # Resolve location constraints.
    #
    # The "in_any" flag is "all_" for the negative constraints (This comes
    # from DeMorgan's rule), and "not all_", as expected, for the positive ones.
    if absent_node:
        absent_nodes = resolve_node(absent_node)
        absent_node_files = files_in_nodes(absent_nodes, None, in_any=all_)
    if absent_group:
        absent_groups = resolve_group(absent_group)
        absent_group_files = files_in_groups(absent_groups, None, in_any=all_)
    if node:
        nodes = resolve_node(node)
        node_files = files_in_nodes(nodes, state_expr, in_any=not all_)
    if group:
        groups = resolve_group(group)
        group_files = files_in_groups(groups, state_expr, in_any=not all_)

    # In --details mode, extra node or group details are provided in very
    # restricted circumstances:
    #  1. exactly one --group or exactly one --node was specified
    #  2. not restricted by syncability (i.e. no --from or --to)
    detail_node = None
    detail_group = None
    if details and not from_:
        if node and not group and len(nodes) == 1:
            detail_node = nodes.pop()
        elif group and not node and len(groups) == 1:
            detail_group = groups.pop()

    # The negative selection list
    if absent_node and absent_group:
        # Negative selection: all_ is a union and not all_ an intersect (DeMorgan again)
        if all_:
            omitted_files = absent_node_files | absent_group_files
        else:
            omitted_files = absent_node_files & absent_group_files
    elif absent_node:
        omitted_files = absent_node_files
    elif absent_group:
        omitted_files = absent_group_files
    else:
        omitted_files = None

    # If the negative selection is empty, we can just drop it
    if omitted_files is not None and len(omitted_files) == 0:
        omitted_files = None

    # The positive selection list
    if node and group:
        # Positive selection: all_ is an intersection and not all_ a union
        if all_:
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
    # location selection where we find files in --from but not in --to.
    if from_:
        syncable_files = files_in_nodes([resolve_node(from_)]) - files_in_groups(
            [resolve_group(to)]
        )

        # If we're in --all mode or we don't have a negative constraint, we can
        # combine with positive constraint now.
        #
        # In other cases we have to keep syncable_files separate from selected_files
        # because they combine differently with omitted_files
        if all_ or omitted_files is None:
            if selected_files is not None:
                selected_files &= syncable_files
            else:
                selected_files = syncable_files
            syncable_files = None
    else:
        syncable_files = None

    # In --all mode, if we have both a positive and negative selection,
    # we can combine them now by differencing to simplify things
    if all_ and omitted_files is not None and selected_files is not None:
        selected_files -= omitted_files
        omitted_files = None

    # Handle no matching files: if we have an empty positive selection
    # (either one) there's nothing to list
    if selected_files is not None and len(selected_files) == 0:
        ctx.exit()
    if syncable_files is not None and len(syncable_files) == 0:
        ctx.exit()

    # The base query
    query = (
        ArchiveFile.select()
        .join(ArchiveAcq)
        .order_by(ArchiveAcq.name, ArchiveFile.name)
    )

    # Apply the --acq limit, if given
    if acq:
        query = query.where(ArchiveFile.acq << resolve_acq(acq))

    # Apply syncability, if present
    if syncable_files is not None:
        query = query.where(ArchiveFile.id << syncable_files)

    # Apply file selection, if any
    if selected_files is not None and omitted_files is not None:
        # This is a non-all positive and negative slection.  We
        # need to "or" them together here
        query = query.where(
            (ArchiveFile.id << selected_files) | ArchiveFile.id.not_in(omitted_files)
        )
    elif selected_files is not None:
        query = query.where(ArchiveFile.id << selected_files)
    elif omitted_files is not None:
        query = query.where(ArchiveFile.id.not_in(omitted_files))

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
