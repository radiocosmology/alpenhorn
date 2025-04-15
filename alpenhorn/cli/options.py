"""Common CLI options and option processing code."""

from __future__ import annotations

import fileinput
import json
import logging
import pathlib

import click
import peewee as pw

from ..db import ArchiveAcq, ArchiveFile, ArchiveFileCopy, StorageGroup, StorageNode


def cli_option(option: str, **extra_kwargs):
    """Provide common CLI options.

    Returns a click.option decorator for the common CLI option called
    `option`.  Other keyword arguments are passed on to click.option.
    """

    # Set args for the click.option decorator
    if option == "acq":
        args = ("--acq",)
        kwargs = {
            "metavar": "ACQ",
            "default": None,
            "multiple": True,
            "help": "May be specified multiple times.  "
            "Limits operation to files in acquisition(s) named ACQ.",
        }
    elif option == "address":
        args = ("--address",)
        kwargs = {
            "metavar": "ADDR",
            "help": "Domain name or IP address to use for remote access to the node.",
        }
    elif option == "all_":
        # Has no help; must be provided when used
        args = ("all_", "--all", "-a")
        kwargs = {"is_flag": True}
    elif option == "archive":
        args = ("--archive",)
        kwargs = {
            "is_flag": True,
            "help": "Make node an archive node.  Incompatible with --field "
            "or --transport.",
        }
    elif option == "archive_ok":
        args = ("--archive-ok",)
        kwargs = {
            "is_flag": True,
            "help": "Run the clean, even if NODE is an archive node.",
        }
    elif option == "auto_verify":
        args = ("--auto-verify",)
        kwargs = {
            "metavar": "COUNT",
            "type": int,
            "help": "If COUNT is zero, turn off auto-verify.  If COUNT is "
            "non-zero, turn on auto-verify for the node, with COUNT as the "
            "maximum number of re-verified copies per iteration.",
        }
    elif option == "cancel":
        args = (
            "--cancel",
            "-x",
        )
        kwargs = {"is_flag": True, "help": "Cancel the operation"}
    elif option == "field":
        args = ("--field",)
        kwargs = {
            "is_flag": True,
            "help": "Make node a field node (i.e. neither an archive node "
            "nor a transport node).  Incompatible with --archive or --transport.",
        }
    elif option == "file_list":
        args = ("-F", "--file-list")
        kwargs = {
            "metavar": "PATH",
            "type": click.Path(
                exists=True, dir_okay=False, readable=True, allow_dash=True
            ),
            "help": (
                "Limit operation to files listed in text file at PATH.  "
                "If PATH is -, stdin will be read.  In that case, --check will be "
                "assumed if --force isn't used."
            ),
        }
    elif option == "from_":
        args = ("from_", "--from")
        kwargs = {"metavar": "SOURCE_NODE", "help": "Source Node for the transfer."}
    elif option == "group":
        args = ("--group",)
        kwargs = {
            "metavar": "GROUP",
            "help": "Limit to files in Storage Group named GROUP.",
        }
    elif option == "host":
        args = ("--host",)
        kwargs = {"metavar": "HOST", "help": "The host managing this node."}
    elif option == "io_class":
        args = (
            "io_class",
            "-i",
            "--class",
        )
        kwargs = {
            "metavar": "IO_CLASS",
            "default": None,
            "help": "Set I/O class to IO_CLASS.",
        }
    elif option == "io_config":
        args = ("--io-config",)
        kwargs = {
            "metavar": "CONFIG",
            "default": None,
            "help": "Set I/O config to the JSON object literal CONFIG.  Any "
            "I/O config specified this way may be further modified by "
            "--io-var.  Setting this to nothing (--io-config=) empties the "
            "I/O config.",
        }
    elif option == "io_var":
        args = ("--io-var",)
        kwargs = {
            "metavar": "VAR=VALUE",
            "default": (),
            "multiple": True,
            "help": "Set I/O config variable VAR to the value VALUE.  May be "
            "specified multiple times.  Modifies any config specified by "
            "--io-config.  If VALUE is empty (--io-var VAR=), VAR is deleted "
            "if present.",
        }
    elif option == "max_total":
        args = ("--max-total",)
        kwargs = {
            "metavar": "SIZE",
            "type": float,
            "help": "The maximum allowed size of the node, in GiB",
        }
    elif option == "md5":
        args = ("--md5",)
        kwargs = {
            "metavar": "HASH",
            "help": "The 128-bit MD5 hash of the file expressed as 32 hex digits.",
        }
    elif option == "min_avail":
        args = ("--min-avail",)
        kwargs = {
            "metavar": "SIZE",
            "type": float,
            "help": "The minimum allowed free space on the node, in GiB, "
            "before auto-cleaning happens",
        }
    elif option == "node":
        args = ("--node",)
        kwargs = {
            "metavar": "NODE",
            "help": "Limit to files on Storage Node named NODE.",
        }
    elif option == "notes":
        args = ("--notes",)
        kwargs = {"metavar": "COMMENT", "help": "Set notes to COMMENT."}
    elif option == "root":
        args = ("--root",)
        kwargs = {"metavar": "ROOT", "help": "The node root or mount point."}
    elif option == "size":
        args = ("--size",)
        kwargs = {
            "type": int,
            "default": None,
            "metavar": "SIZE",
            "help": "The size in bytes of the file.",
        }
    elif option == "target":
        args = ("--target",)
        kwargs = {
            "metavar": "GROUP",
            "multiple": True,
            "help": "May be specified multiple times.  Restrict operation to "
            "files which exist in all specified target groups GROUP.",
        }
    elif option == "to":
        args = ("--to",)
        kwargs = {
            "metavar": "DEST_GROUP",
            "help": "Destination Group for the transfer.",
        }
    elif option == "transport":
        args = ("--transport",)
        kwargs = {
            "is_flag": True,
            "help": "Make node a transport node.  Incompatible with --archive "
            "or --field.",
        }
    elif option == "username":
        args = ("--username",)
        kwargs = {
            "metavar": "USER",
            "help": "Username to use for remote access to the node.",
        }
    else:
        raise ValueError(f"Unknown option: {option}")

    # Update kwargs, if given
    kwargs.update(extra_kwargs)

    # Default help string for common options missing them
    if "help" not in kwargs:
        kwargs["help"] = (
            "SOMEONE FORGOT TO WRITE SOME HELP FOR THIS OPTION.  GOOD LUCK, I GUESS!"
        )

    def _decorator(func):
        nonlocal args, kwargs

        return click.option(*args, **kwargs)(func)

    return _decorator


def not_both(opt1_set: bool, opt1_name: str, opt2_set: bool, opt2_name: str) -> None:
    """Check whether two incompatible options were used.

    If they were, raise click.UsageError."""

    if opt1_set and opt2_set:
        raise click.UsageError(f"cannot use both --{opt1_name} and --{opt2_name}")


def both_or_neither(
    opt1_set: bool, opt1_name: str, opt2_set: bool, opt2_name: str
) -> None:
    """Check whether two options which must be used together were.

    If they weren't, raise click.UsageError."""

    # xor
    if bool(opt1_set) is not bool(opt2_set):
        raise click.UsageError(f"--{opt1_name} and --{opt2_name} must be used together")


def at_least_one(
    opt1_set: bool, opt1_name: str, opt2_set: bool, opt2_name: str
) -> None:
    """Check that at least one of two options were used.

    If not, raise click.UsageError."""

    if not (opt1_set or opt2_set):
        raise click.UsageError(f"missing --{opt1_name} or --{opt2_name}")


def exactly_one(opt1_set: bool, opt1_name: str, opt2_set: bool, opt2_name: str) -> None:
    """Check that exactly one of two incompatible options were used.

    If not, raise click.UsageError."""

    not_both(opt1_set, opt1_name, opt2_set, opt2_name)
    at_least_one(opt1_set, opt1_name, opt2_set, opt2_name)


def requires_other(
    opt1_set: bool, opt1_name: str, opt2_set: bool, opt2_name: str
) -> None:
    """If opt1 is set, check that opt2 was also set.

    If not, raise click.UsageError."""

    if opt1_set and not opt2_set:
        raise click.UsageError(f"--{opt1_name} may only be used with --{opt2_name}")


def resolve_group(group: str | list[str]) -> StorageGroup | set[StorageGroup]:
    """Convert group name(s) into StorageGroup(s).

    If given a single `str`, returns a single `StorageGroup`.
    Otherwise, should be given a list of str and will return a
    set of StorageGroups.

    If any name can't be resolved, raises ClickException.
    """
    one_group = isinstance(group, str)
    if one_group:
        group = [group]

    groups = set()
    for name in group:
        try:
            groups.add(StorageGroup.get(name=name))
        except pw.DoesNotExist:
            raise click.ClickException("no such group: " + name)

    if one_group:
        return groups.pop()
    return groups


def resolve_node(node: str | list[str]) -> StorageNode | set[StorageNode]:
    """Convert node name(s) into StorageNode(s).

    If given a single `str`, returns a single `StorageNode`.
    Otherwise, should be given a list of str and will return a
    set of StorageNodes.

    If any name can't be resolved, raises ClickException.
    """
    one_node = isinstance(node, str)
    if one_node:
        node = [node]

    nodes = set()
    for name in node:
        try:
            nodes.add(StorageNode.get(name=name))
        except pw.DoesNotExist:
            raise click.ClickException("no such node: " + name)

    if one_node:
        return nodes.pop()
    return nodes


def resolve_acq(acq: str | list[str]) -> ArchiveAcq | set[ArchiveAcq]:
    """Convert --acq list to ArchiveAcq list.

    If given a single `str`, returns a single `ArchiveAcq`.
    Otherwise, should be given a list of str and will return a
    set of ArchiveAcqs.

    Raises `click.ClickException` if a non-existent acqusition was
    provided.
    """

    one_acq = isinstance(acq, str)
    if one_acq:
        acq = [acq]

    acqs = set()
    for acqname in acq:
        try:
            acqs.add(ArchiveAcq.get(name=acqname))
        except pw.DoesNotExist:
            raise click.ClickException("No such acquisition: " + acqname)

    if one_acq:
        return acqs.pop()
    return acqs


def state_constraint(
    *,
    corrupt: bool = False,
    healthy: bool = False,
    missing: bool = False,
    suspect: bool = False,
) -> pw.Expression | None:
    """Select ArchiveFileCopy constraint for state.

    Processes the --corrupt, --healthy, --missing, --suspect
    flags and returns a peewee.Expression to be added to
    a where() clause.

    If none of the flags are set, returns None.
    """

    if corrupt:
        expr = (ArchiveFileCopy.has_file == "X") & (ArchiveFileCopy.wants_file != "N")
    else:
        expr = None

    if healthy:
        healthy_expr = (ArchiveFileCopy.has_file == "Y") & (
            ArchiveFileCopy.wants_file != "N"
        )
        if expr:
            expr = expr | healthy_expr
        else:
            expr = healthy_expr

    if missing:
        missing_expr = (ArchiveFileCopy.has_file == "N") & (
            ArchiveFileCopy.wants_file == "Y"
        )
        if expr:
            expr = expr | missing_expr
        else:
            expr = missing_expr

    if suspect:
        suspect_expr = (ArchiveFileCopy.has_file == "M") & (
            ArchiveFileCopy.wants_file != "N"
        )
        if expr:
            expr = expr | suspect_expr
        else:
            expr = suspect_expr

    return expr


def files_in_nodes(
    nodes: list[StorageNode],
    state_expr: pw.Expression | None = None,
    in_any: bool = False,
) -> set[int] | None:
    """Returns a set of files on a set of nodes.

    By default, the intersection is returned; pass `in_any=True`
    to return the union, instead.

    Parameters
    ----------
    nodes:
        list of StorageNodes.
    state_expr:
        if given and not None, a peewee.Expression defining the
        state of files we're looking for.  If None, a
        default constraint of only healthy files is used.
    in_any:
        if True, file needs to be only in one group.
        If False, file needs to be in all group.

    Returns
    -------
    files_in_nodes:
        If the input list was empty, this is None.  Otherwise,
        it is a set of ArchiveFile.ids which are on any/all nodes.
        The set may be empty, if no files satisfied the constraint.
    """

    node_files = None
    for node in nodes:
        # Get files in node
        query = (
            ArchiveFile.select(ArchiveFile.id)
            .join(ArchiveFileCopy)
            .where(ArchiveFileCopy.node == node)
        )

        if state_expr is None:
            query = query.where(state_constraint(healthy=True))
        else:
            query = query.where(state_expr)

        if in_any:
            # In "any" mode, run the query and then compute the set union,
            # if we already have a file set
            if node_files:
                node_files |= set(query.scalars())
            else:
                node_files = set(query.scalars())
        else:
            # In "all" mode, intersect the current list with the new query
            # and replace the set with the result
            if node_files:
                query = query.where(ArchiveFile.id << node_files)

            # Execute the query and record the result
            node_files = set(query.scalars())

            # In this mode, there's no reason to continue once the set is empty
            if not node_files:
                break

    return node_files


def files_in_groups(
    groups: set[StorageGroup],
    state_expr: pw.Expression | None = None,
    in_any: bool = False,
) -> set[int] | None:
    """Returns a set of files in a set of groups.

    By default, the intersection is returned; pass `in_any=True`
    to return the union, instead.

    Parameters
    ----------
    groups:
        set of StorageGroups.
    state_expr:
        if given and not None, a peewee.Expression defining the
        state of files we're looking for.  If None, a
        default constraint of has_file='Y & wants_file='Y' is applied.
    in_any:
        if True, file needs to be only in one group.
        If False, file needs to be in all group.

    Returns
    -------
    files_in_groups:
        If the input list was empty, this is None.  Otherwise,
        it is a set of ArchiveFile.ids which are in any/all groups.
        The set may be empty, if no files satisfied the constraint.
    """

    group_files = None
    for group in groups:
        # Get files in group
        query = (
            ArchiveFile.select(ArchiveFile.id)
            .join(ArchiveFileCopy)
            .join(StorageNode)
            .where(StorageNode.group == group)
        )

        if state_expr is None:
            query = query.where(state_constraint(healthy=True))
        else:
            query = query.where(state_expr)

        if in_any:
            # In "any" mode, run the query and then compute the set union,
            # if we already have a file set
            if group_files:
                group_files |= set(query.scalars())
            else:
                group_files = set(query.scalars())
        else:
            # In "all" mode, intersect the current list with the new query
            # and replace the set with the result
            if group_files:
                query = query.where(ArchiveFile.id << group_files)

            # Execute the query and record the result
            group_files = set(query.scalars())

            # In this mode, there's no reason to continue once the set is empty
            if not group_files:
                break

    return group_files


def set_storage_type(
    archive: bool, field: bool, transport: bool, none_ok: bool = False
) -> str | None:
    """Set node storage_type.

    Processes the --archive, --field, --transport options.

    Returns one of 'A', 'F', 'T', depending on which of the options is set.

    If none of the options are set, None is returned if `none_ok` is True, otherwise
    the default 'F' is returned.

    Raises a click.UsageError if more than one flag is set.
    """

    # Usage checks
    not_both(archive, "archive", field, "field")
    not_both(archive, "archive", transport, "transport")
    not_both(field, "field", transport, "transport")

    if archive:
        return "A"
    if transport:
        return "T"
    if not field and none_ok:
        return None

    # Default
    return "F"


def set_io_config(
    io_config: str | None, io_var: list | tuple, default: str | dict = {}
) -> dict | None:
    """Set the I/O config from the command line.

    Processes the --io-config and --io-var options.

    Parameters
    ----------
    io_config:
        The --io-config parameter data from click
    io_var:
        The --io-var parameter data from click
    default:
        If --io-config is None, use this as the default value before applying
        io_var edits.  If a string, will be JSON decoded.

    Returns
    -------
    io_config : dict
        The composed I/O config dict, or None if the dict ended up empty.
    """

    # Attempt to compose the I/O config
    if io_config == "":
        # i.e. the user specified "--io-config=" to delete the I/O config
        new_config = {}
    elif io_config:
        try:
            new_config = json.loads(io_config)
        except json.JSONDecodeError:
            raise click.ClickException("Unable to parse --io-config value as JSON.")

        if not isinstance(new_config, dict):
            raise click.ClickException(
                "Argument to --io-config must be a JSON object literal."
            )
    else:
        # Decoding of default is only done if necessary
        if isinstance(default, str):
            try:
                default = json.loads(default)
            except json.JSONDecodeError:
                raise click.ClickException(
                    f"Invalid I/O config in database: {default}."
                )
        new_config = default

    # Add any --io-var data
    for var in io_var:
        split_data = var.split("=", 1)
        if len(split_data) <= 1:
            raise click.ClickException(f'Missing equals sign in "--io-var={var}"')

        if split_data[1] == "":
            # "--io-var VAR=" means delete VAR if present
            new_config.pop(split_data[0], None)
            continue

        # Otherwise, try coercing the value
        try:
            split_data[1] = int(split_data[1])
        except ValueError:
            try:
                split_data[1] = float(split_data[1])
            except ValueError:
                pass

        # Test jsonifibility.  I have not found a way to make click give this function
        # something it can't encode, but I don't think this hurts.
        try:
            json.dumps(dict([split_data]))
        except json.JSONDecodeError:
            raise click.ClickException(f'Cannot parse argument: "--io-var={var}"')

        # Now set
        new_config[split_data[0]] = split_data[1]

    # Return None instead of an empty I/O config
    return new_config if new_config else None


def file_from_path(path: str, source: str | None = None) -> ArchiveFile:
    """Get file record given a path

    Given an "acqname/filename" path-like string, find and return
    the file name.

    Parameters
    ----------
    path:
        The path to resolve into an ArchiveFile
    source:
        If not None, a location (i.e. a line in a file) added
        to an error string.

    Raises:
    -------
    click.ClickException:
        A record for `path` could not be found.
    """

    path = pathlib.PurePath(path)

    if source:
        errstr = "No such file " + source + ": " + str(path)
    else:
        errstr = "No such file: " + str(path)

    # An absolute path is not allowed
    if path.is_absolute():
        raise click.ClickException(errstr)

    # The trick here is path can have multiple path components: a/b/c/d/e
    # and we don't know which components are part of the acq_name and which
    # are part of the file_name, so we need to iterate through with the
    # help of pathlib.
    #
    # PurePath.parents starts with the deepest parent and works its way up
    for acq_name in path.parents:
        if acq_name == ".":
            # Ran out of parents
            break

        file_name = path.relative_to(acq_name)

        # Try to find the file
        try:
            return (
                ArchiveFile.select()
                .join(ArchiveAcq)
                .where(
                    ArchiveFile.name == str(file_name), ArchiveAcq.name == str(acq_name)
                )
                .limit(1)
                .get()
            )
        except pw.DoesNotExist:
            pass

    raise click.ClickException(errstr)


def check_if_from_stdin(path: str, check: bool, force: bool) -> bool:
    """Check whether to automatically turn on check mode

    The intent here is that the output of this function will be assigned to
    `check` in the caller.  Emits a warning if check mode is to be turned on.

    Parameters
    ----------
    path:
        the argument to --file-list
    check:
        the state of the --check flag
    force:
        the state of the --force flag

    Returns
    -------
    check:
        True if `check` is True or if `check` and `force` are False
        and `path` is "-".  False otherwise.
    """

    # If mode is already selected, just return the input `check` value
    if check or force:
        return check

    # Warn in turning on check mode
    if path == "-":
        log = logging.getLogger(__name__)
        log.warning(
            "stdin has been redirected without --force.  "
            "Automatically enabling --check mode."
        )
        return True

    # Otherwise, continue
    return False


def files_from_file(path: str | None, node: str | None = None) -> set[ArchiveFile]:
    """Read a file list from a file given with --file-list

    Returns a set of ArchiveFiles.

    Parameters
    ----------
    path:
        The path to the file to read.  If this is None, the empty set is returned.
    node:
        If not None, the name of the source StorageNode, whose root will be
        removed from the files, if present.  (If `node` is None, only relative
        paths will be accepted.)
    """

    files = set()

    # Not given; return empty set
    if not path:
        return files

    # Fetch node.root, if necessary
    if node:
        try:
            root = StorageNode.get(name=node).root
        except pw.DoesNotExist:
            raise click.ClickException("No such node: " + node)
    else:
        root = None

    name = "stdin" if path == "-" else path

    try:
        for line in fileinput.input(files=[path]):
            # Skip comment lines.  Note this check happens before whiespace
            # stripping, meaning the "#" _must_ appear in the first column
            # and may not have whitespace before it.
            if line[0] == "#":
                continue

            # Strip leading and trailing whitespace
            line = line.strip()

            # Skip empty lines
            if not line:
                continue

            # If we were given a node, strip node root, if present
            if root and line[0] == "/":
                try:
                    line = str(pathlib.Path(line).relative_to(root))
                except ValueError:
                    pass  # Not relative to root

            files.add(
                file_from_path(
                    line,
                    source=f"on line {fileinput.filelineno()} of {name}",
                )
            )
    except OSError as e:
        raise click.ClickException("error reading {name}: {e}") from e

    return files


def validate_md5(md5: str | None) -> None:
    """Vet a user-provided MD5 hash

    The MD5 may be None.

    raises click.ClickException if validation fails.
    """

    # None is fine.
    if md5 is None:
        return

    # The hash must be a 128-byte number specified as 32 hex digits
    if len(md5) != 32:
        raise click.ClickException(f"invalid hash: {md5}.  Expected 32 hex digits.")

    try:
        int(md5, base=16)
    except ValueError:
        raise click.ClickException(f"invalid hash: {md5}.  Expected 32 hex digits.")
