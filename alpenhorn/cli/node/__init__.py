"""Alpenhorn CLI interface for operations on `StorageNode`s."""

import os
import re
import sys
from collections import defaultdict

import click
import peewee as pw

from ... import db
from ...common import util
from ...db import ArchiveAcq, ArchiveFile, ArchiveFileCopy, StorageGroup, StorageNode

from .activate import activate
from .autoclean import autoclean
from .clean import clean
from .create import create
from .deactivate import deactivate
from .list import list_
from .modify import modify
from .rename import rename
from .show import show
from .stats import stats

RE_LOCK_FILE = re.compile(r"^\..*\.lock$")


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
def cli():
    """Manage Storage Nodes."""


cli.add_command(activate, "activate")
cli.add_command(autoclean, "autoclean")
cli.add_command(clean, "clean")
cli.add_command(create, "create")
cli.add_command(deactivate, "deactivate")
cli.add_command(list_, "list")
cli.add_command(modify, "modify")
cli.add_command(rename, "rename")
cli.add_command(show, "show")
cli.add_command(stats, "stats")


@cli.command()
@click.argument("node_name", metavar="NODE")
@click.option("-v", "--verbose", count=True)
@click.option(
    "--acq",
    help="Limit import to specified acquisition directories.",
    multiple=True,
    default=None,
)
@click.option(
    "--register-new", help="Register new files instead of ignoring them.", is_flag=True
)
@click.option("--dry", "-d", help="Dry run. Do not modify database.", is_flag=True)
def scan(node_name, verbose, acq, register_new, dry):
    """Scan the current directory for known acquisition files and add them into the database for NODE.

    This command is useful for manually maintaining an archive where we cannot
    run alpenhornd in the usual manner.
    """
    config_connect()

    # Keep track of state as we process the files
    added_files = []  # Files we have added to the database
    corrupt_files = []  # Known files which are corrupt
    registered_files = []  # Files already registered in the database
    unknown_files = []  # Files not known in the database

    known_acqs = []  # Directories which are known acquisitions
    new_acqs = []  # Directories which were newly registered acquisitions
    not_acqs = []  # Directories which were not known acquisitions

    # Fetch a reference to the node
    try:
        this_node = StorageNode.select().where(StorageNode.name == node_name).get()
    except pw.DoesNotExist:
        click.echo("Unknown node:", node_name)
        exit(1)

    cwd = os.getcwd()
    # Construct a dictionary of directories that might be acquisitions and the of
    # list files that they contain
    db_acqs = ArchiveAcq.select(ArchiveAcq.name)
    acq_files = defaultdict(list)
    if len(acq) == 0:
        tops = [cwd]
    else:
        db_acqs = db_acqs.where(ArchiveAcq.name >> acq)
        tops = []
        for acq_name in acq:
            acq_dir = os.path.join(this_node.root, acq_name)
            if not os.path.isdir(acq_dir):
                print(
                    'Aquisition "%s" does not exist in this node. Ignoring.' % acq_name,
                    file=sys.stderr,
                )
                continue
            if acq_dir == cwd:
                # the current directory is one of the limiting acquisitions, so
                # we can ignore all others in the `--acq` list
                tops = [acq_dir]
                break
            elif cwd.startswith(acq_dir):
                # the current directory is inside one of the limiting
                # acquisitions, so we can just walk its subtree
                tops = [cwd]
                break
            elif acq_dir.startswith(cwd):
                # the acquisition is inside the current directory, so we can
                # just walk its subtree
                tops.append(acq_dir)
            else:
                print(
                    'Acquisition "%s" is outside the current directory and will be ignored.'
                    % acq_name,
                    file=sys.stderr,
                )

    for top in tops:
        for d, ds, fs in os.walk(top):
            d = os.path.relpath(d, this_node.root)
            if d == ".":  # skip the node root directory
                continue
            acq_type_name = ac.AcqType.detect(d, this_node)
            if acq_type_name:
                _, acq_name = acq_type_name
                if d == acq_name:
                    # the directory is the acquisition
                    acq_files[acq_name] += [
                        f
                        for f in fs
                        if not RE_LOCK_FILE.match(f)
                        and not os.path.isfile(os.path.join(d, ".{}.lock".format(f)))
                    ]
                if d.startswith(acq_name + "/"):
                    # the directory is inside an acquisition
                    acq_dirname = os.path.relpath(d, acq_name)
                    acq_files[acq_name] += [
                        (acq_dirname + "/" + f)
                        for f in fs
                        if not RE_LOCK_FILE.match(f)
                        and not os.path.isfile(os.path.join(d, ".{}.lock".format(f)))
                    ]
            else:
                not_acqs.append(d)

    with click.progressbar(acq_files, label="Scanning acquisitions") as acq_iter:
        for acq_name in acq_iter:
            try:
                acq = ArchiveAcq.select().where(ArchiveAcq.name == acq_name).get()
                known_acqs.append(acq_name)

                # Fetch lists of all files in this acquisition, and all
                # files in this acq with local copies
                file_names = [f.name for f in acq.files]
                local_file_names = [
                    f.name
                    for f in acq.files.join(ArchiveFileCopy).where(
                        ArchiveFileCopy.node == this_node
                    )
                ]
            except pw.DoesNotExist:
                if register_new:
                    acq_type, _ = ac.AcqType.detect(acq_name, this_node)
                    acq = ArchiveAcq(name=acq_name, type=acq_type)
                    if not dry:
                        # TODO: refactor duplication with auto_import.add_acq
                        with db.database_proxy.atomic():
                            # insert the archive record
                            acq.save()
                            # and generate the metadata table
                            acq_type.acq_info.new(acq, this_node)

                    new_acqs.append(acq_name)

                    # Because it's a newly imported acquisition, all files within it are new also
                    file_names = []
                    local_file_names = []
                else:
                    not_acqs.append(acq_name)
                    continue

            for f_name in acq_files[acq_name]:
                file_path = os.path.join(acq_name, f_name)

                # Check if file exists in database
                if not register_new and f_name not in file_names:
                    unknown_files.append(file_path)
                    continue

                # Check if file is already registered on this node
                if f_name in local_file_names:
                    registered_files.append(file_path)
                else:
                    abs_path = os.path.join(this_node.root, file_path)
                    if f_name in file_names:
                        # it is a known file
                        archive_file = (
                            ArchiveFile.select()
                            .where(ArchiveFile.name == f_name, ArchiveFile.acq == acq)
                            .get()
                        )

                        # TODO: decide if, when the file is corrupted, we still
                        # register the file as `has_file="X"` or just _continue_
                        if os.path.getsize(abs_path) != archive_file.size_b:
                            corrupt_files.append(file_path)
                            continue
                        else:
                            if verbose > 2:
                                print('Computing md5sum of "{}"'.format(f_name))
                            md5sum = util.md5sum_file(abs_path, cmd_line=False)
                            if md5sum != archive_file.md5sum:
                                corrupt_files.append(file_path)
                                continue
                    else:
                        # not a known file, register the new ArchiveFile instance
                        file_type = ac.FileType.detect(f_name, acq, this_node)
                        if not file_type:
                            unknown_files.append(file_path)
                            continue

                        if verbose > 2:
                            print('Computing md5sum of "{}"'.format(f_name))
                        md5sum = util.md5sum_file(abs_path, cmd_line=False)
                        size_b = os.path.getsize(abs_path)
                        archive_file = ArchiveFile(
                            name=f_name,
                            acq=acq,
                            type=file_type,
                            size_b=size_b,
                            md5sum=md5sum,
                        )
                        if not dry:
                            archive_file.save()

                    added_files.append(file_path)
                    if not dry:
                        copy_size_b = os.stat(abs_path).st_blocks * 512
                        ArchiveFileCopy.create(
                            file=archive_file,
                            node=this_node,
                            has_file="Y",
                            wants_file="Y",
                            size_b=copy_size_b,
                        )

    # now find the minimum unknown acqs paths that we can report
    not_acqs_roots = []
    last_acq_root = ""
    for d in sorted(not_acqs):
        common = os.path.commonprefix([last_acq_root, d])
        if common == "":
            for acq_name in known_acqs:
                if acq_name.startswith(d):
                    break
            else:
                for acq_name in new_acqs:
                    if acq_name.startswith(d):
                        break
                else:
                    not_acqs_roots.append(d)
            last_acq_root = d

    print("\n==== Summary ====")
    print()
    if register_new:
        print("Registered %i new acquisitions" % len(new_acqs))
    print("Added %i files" % len(added_files))
    print()
    print("%i corrupt files." % len(corrupt_files))
    print("%i files already registered." % len(registered_files))
    print("%i files not known" % len(unknown_files))
    print("%i directories were not acquisitions." % len(not_acqs_roots))

    if verbose > 0:
        print()
        if register_new:
            print("New acquisitions:")
            for an in sorted(new_acqs):
                print(an)
            print()

        print("Added files:")
        for fn in sorted(added_files):
            print(fn)

        print()

    if verbose > 1:
        print("Corrupt:")
        for fn in sorted(corrupt_files):
            print(fn)
        print()

        print("Unknown files:")
        for fn in sorted(unknown_files):
            print(fn)
        print()

        print("Unknown acquisitions:")
        for fn in sorted(not_acqs_roots):
            print(fn)
        print()


@cli.command()
@click.argument("node_name", metavar="NODE")
@click.option("--md5", help="perform full check against md5sum", is_flag=True)
@click.option(
    "--fixdb", help="fix up the database to be consistent with reality", is_flag=True
)
@click.option(
    "--acq",
    metavar="ACQ",
    multiple=True,
    help="Limit verification to specified acquisitions. Use repeated --acq flags to specify multiple acquisitions.",
)
def verify(node_name, md5, fixdb, acq):
    """Verify the archive on NODE against the database.

    If there are no issues with the archive returns with exit status of zero,
    non-zero if there are issues. Specifically:

    `0`
        No problems.
    `1`
        Corrupt files found.
    `2`
        Files missing from archive.
    `3`
        Both corrupt and missing files.
    """

    import os

    config_connect()

    try:
        this_node = StorageNode.get(name=node_name)
    except pw.DoesNotExist:
        click.echo('Storage node "{}" does not exist.'.format(node_name))
        exit(1)

    if not this_node.active:
        click.echo('Node "{}" is not active.'.format(node_name))
        exit(1)
    if not util.alpenhorn_node_check(this_node):
        click.echo(
            'Node "{}" does not match ALPENHORN_NODE: {}'.format(
                node_name, this_node.root
            )
        )
        exit(1)

    # Use a complicated query with a tuples construct to fetch everything we
    # need in a single query. This massively speeds up the whole process versus
    # fetching all the FileCopy's then querying for Files and Acqs.
    lfiles = (
        ArchiveFile.select(
            ArchiveFile.name,
            ArchiveAcq.name,
            ArchiveFile.size_b,
            ArchiveFile.md5sum,
            ArchiveFileCopy.id,
        )
        .join(ArchiveAcq)
        .switch(ArchiveFile)
        .join(ArchiveFileCopy)
        .where(ArchiveFileCopy.node == this_node, ArchiveFileCopy.has_file == "Y")
    )

    if acq:
        lfiles = lfiles.where(ArchiveAcq.name << acq)

    missing_files = []
    corrupt_files = []

    missing_ids = []
    corrupt_ids = []

    nfiles = 0

    with click.progressbar(lfiles.tuples(), label="Scanning files") as lfiles_iter:
        for filename, acqname, filesize, md5sum, fc_id in lfiles_iter:
            nfiles += 1

            filepath = this_node.root + "/" + acqname + "/" + filename

            # Check if file is plain missing
            if not os.path.exists(filepath):
                missing_files.append(filepath)
                missing_ids.append(fc_id)
                continue

            if md5:
                file_md5 = util.md5sum_file(filepath)
                corrupt = file_md5 != md5sum
            else:
                corrupt = os.path.getsize(filepath) != filesize

            if corrupt:
                corrupt_files.append(filepath)
                corrupt_ids.append(fc_id)
                continue

    if len(missing_files) > 0:
        click.echo()
        click.echo("=== Missing files ===")
        for fname in missing_files:
            click.echo(fname)

    if len(corrupt_files) > 0:
        print()
        click.echo("=== Corrupt files ===")
        for fname in corrupt_files:
            click.echo(fname)

    click.echo()
    click.echo("=== Summary ===")
    click.echo("  %i total files" % nfiles)
    click.echo("  %i missing files" % len(missing_files))
    click.echo("  %i corrupt files" % len(corrupt_files))
    click.echo()

    # Fix up the database by marking files as missing, and marking
    # corrupt files for verification by alpenhornd.
    if fixdb:
        # TODO: ensure write access to the database
        # # We need to write to the database.
        # di.connect_database(read_write=True)

        if (len(missing_files) > 0) and click.confirm("Fix missing files"):
            missing_count = (
                ArchiveFileCopy.update(has_file="N")
                .where(ArchiveFileCopy.id << missing_ids)
                .execute()
            )
            click.echo("  %i marked as missing" % missing_count)

        if (len(corrupt_files) > 0) and click.confirm("Fix corrupt files"):
            corrupt_count = (
                ArchiveFileCopy.update(has_file="M")
                .where(ArchiveFileCopy.id << corrupt_ids)
                .execute()
            )
            click.echo("  %i corrupt files marked for verification" % corrupt_count)
    else:
        # Set the exit status
        status = 1 if corrupt_files else 0
        status += 2 if missing_files else 0

        exit(status)
