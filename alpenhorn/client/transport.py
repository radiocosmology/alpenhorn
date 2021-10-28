"""Alpenhorn client interface for operations on transport disks."""

import os
import subprocess
import time

import click
import peewee as pw

import alpenhorn.storage as st

from . import node
from .connect_db import config_connect

# A few utility routines for dealing with filesystems
MAX_E2LABEL_LEN = 16


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
def cli():
    """Commands operating on transport nodes. Use to format, mount, etc."""


@cli.command()
def list():
    """List known transport nodes."""
    config_connect()

    import tabulate

    data = (
        st.StorageNode.select(
            st.StorageNode.name,
            pw.Case(st.StorageNode.active, [(True, "Y"), (False, "-")]),
            st.StorageNode.host,
            st.StorageNode.root,
            st.StorageNode.notes,
        )
        .where(st.StorageNode.storage_type == "T")
        .tuples()
    )
    if data:
        print(
            tabulate.tabulate(
                data, headers=["Name", "Mounted", "Host", "Root", "Notes"]
            )
        )


@cli.command()
@click.argument("serial_num")
def format(serial_num):
    """Interactive routine for formatting a transport disc as a storage
    node; formats and labels the disc as necessary, the adds to the
    database. The disk is specified using the manufacturers
    SERIAL_NUM, which is printed on the disk.
    """
    import glob
    import os

    config_connect()

    if os.getuid() != 0:
        print("You must be root to run mount on a transport disc. I quit.")
        return

    # Find the disc.
    dev = glob.glob("/dev/disk/by-id/*%s" % serial_num)
    if len(dev) == 0:
        print("No disc with that serial number is attached.")
        return
    elif len(dev) > 1:
        print("Confused: found more than one device matching that serial number:")
        for d in dev:
            print("  %s" % dev)
        print("Aborting.")
        return
    dev = dev[0]
    dev_part = "%s-part1" % dev

    # Figure out if it is formatted.
    print("Checking to see if disc is formatted. Please wait.")
    formatted = False
    try:
        # check if the block device is partitioned
        subprocess.check_output(["blkid", "-p", dev])

        # now check if the partition is formatted
        if "TYPE=" in subprocess.check_output(["blkid", "-p", dev_part]):
            formatted = True
    except subprocess.CalledProcessError:
        pass

    if not formatted:
        if not click.confirm("Disc is not formatted. Should I format it?"):
            return
        print("Creating partition. Please wait.")
        try:
            subprocess.check_call(
                [
                    "parted",
                    "-s",
                    "-a",
                    "optimal",
                    dev,
                    "mklabel",
                    "gpt",
                    "--",
                    "mkpart",
                    "primary",
                    "0%",
                    "100%",
                ]
            )
        except subprocess.CalledProcessError as e:
            print(
                "Failed to create the partition! Stat = %s. I quit.\n%s"
                % (e.returncode, e.output)
            )
            exit(1)

        # pause to give udev rules time to get updated
        time.sleep(1)

        print("Formatting disc. Please wait.")
        try:
            subprocess.check_call(
                ["mkfs.ext4", dev_part, "-m", "0", "-L", "CH-{}".format(serial_num)]
            )
        except subprocess.CalledProcessError as e:
            print(
                "Failed to format the disk! Stat = %s. I quit.\n%s"
                % (e.returncode, e.output)
            )
            exit(1)
    else:
        print("Disc is already formatted.")

    e2label = _get_e2label(dev_part)
    name = "CH-%s" % serial_num
    if e2label and e2label != name:
        print(
            "Disc label %s does not conform to labelling standard, "
            "which is CH-<serialnum>."
        )
        exit
    elif not e2label:
        print('Labelling the disc as "%s" (using e2label) ...' % (name))
        assert dev_part is not None
        assert len(name) <= MAX_E2LABEL_LEN
        try:
            subprocess.check_call(["/sbin/e2label", dev_part, name])
        except subprocess.CalledProcessError as e:
            print(
                "Failed to e2label! Stat = %s. I quit.\n%s" % (e.returncode, e.output)
            )
            exit(1)

    # Ensure the mount path exists.
    root = "/mnt/%s" % name
    if not os.path.isdir(root):
        print("Creating mount point %s." % root)
        os.mkdir(root)

    # Check to see if the disc is mounted.
    try:
        output = subprocess.check_output(["df"])
        dev_part_abs = os.path.realpath(dev_part)
        for l in output.split("\n"):
            if l.find(root) > 0:
                if l[: len(dev_part)] == dev or l[: len(dev_part_abs)] == dev_part_abs:
                    print("%s is already mounted at %s" % (l.split()[0], root))
                else:
                    print(
                        "%s is a mount point, but %s is already mounted there."
                        % (root, l.split()[0])
                    )
    except subprocess.CalledProcessError as e:
        print(
            "Failed to check the mountpoint! Stat = %s. I quit.\n%s"
            % (e.returncode, e.output)
        )
        exit(1)

    try:
        node = st.StorageNode.get(name=name)
    except pw.DoesNotExist:
        print(
            "This disc has not been registered yet as a storage node. "
            "Registering now."
        )
        try:
            group = st.StorageGroup.get(name="transport")
        except pw.DoesNotExist:
            print('Hmmm. Storage group "transport" does not exist. I quit.')
            exit(1)

        # TODO: ensure write access to the database
        # # We need to write to the database.
        # di.connect_database(read_write=True)
        node = st.StorageNode.create(
            name=name, root=root, group=group, storage_type="T", min_avail_gb=1
        )

        print("Successfully created storage node.")

    print("Node created but not activated. Run alpenhorn mount_transport for that.")


@cli.command()
@click.pass_context
@click.argument("node_name", metavar="NODE")
@click.option("--user", help="username to access this node.", type=str, default=None)
@click.option(
    "--address", help="address for remote access to this node.", type=str, default=None
)
def mount(ctx, node_name, user, address):
    """Mount a transport disk into the system and then make it available to alpenhorn."""

    mnt_point = "/mnt/%s" % node_name

    if os.path.ismount(mnt_point):
        print(
            "{} is already mounted in the filesystem. Proceeding to activate it.".format(
                node_name
            )
        )
    else:
        print("Mounting disc at %s" % mnt_point)
        os.system("mount %s" % mnt_point)

    ctx.invoke(
        node.activate, name=node_name, path=mnt_point, user=user, address=address
    )


@cli.command()
@click.pass_context
@click.argument("node_name", metavar="NODE")
def unmount(ctx, node_name):
    """Unmount a transport disk from the system and then remove it from alpenhorn."""

    mnt_point = "/mnt/%s" % node_name

    print("Unmounting disc at %s" % mnt_point)
    os.system("umount %s" % mnt_point)

    ctx.invoke(node.deactivate, root_or_name=node_name)


def _get_e2label(dev):
    """Read filesystem label on an Ext{2,3,4}fs device

    Parameters
    ----------
    dev: str
        The path to the device file.

    Returns
    -------
    str or None
        the filesystem label, or None if reading it failed.
    """

    try:
        output = subprocess.check_output(["/sbin/e2label", dev]).strip()
        if len(output) < MAX_E2LABEL_LEN:
            return output
    except subprocess.CalledProcessError:
        return None
