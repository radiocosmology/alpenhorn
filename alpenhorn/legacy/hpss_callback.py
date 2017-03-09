"""Call backs for the HPSS interface.
"""

import peewee as pw
import click

from ch_util import data_index as di

from . import logger  # Import logger here to avoid connection
                      # messages for transfer

# Get a reference to the log
log = logger.get_log()

# Reconnect to the database read/write
di.connect_database(read_write=True)


@click.group()
def cli():
    """Call back commands for updating the database from a shell script after an
    HPSS transfer. """
    pass


@cli.command()
@click.argument('file_id', type=int)
@click.argument('node_id', type=int)
def push_failed(file_id, node_id):
    """Update the database to reflect that the HPSS transfer failed.

    INTERNAL COMMAND. NOT FOR HUMAN USE!
    """
    afile = di.ArchiveFile.select().where(di.ArchiveFile.id == file_id).get()
    node = di.StorageNode.select().where(di.StorageNode.id == node_id).get()

    log.warn('Failed push: %s/%s into node %s' % (afile.acq.name, afile.name, node.name))

    # We don't really need to do anything other than log this (we could reattempt)


@cli.command()
@click.argument('file_id', type=int)
@click.argument('node_id', type=int)
def pull_failed(file_id, node_id):
    """Update the database to reflect that the HPSS transfer failed.

    INTERNAL COMMAND. NOT FOR HUMAN USE!
    """
    afile = di.ArchiveFile.select().where(di.ArchiveFile.id == file_id).get()
    node = di.StorageNode.select().where(di.StorageNode.id == node_id).get()

    log.warn('Failed pull: %s/%s onto node %s' % (afile.acq.name, afile.name, node.name))

    # We don't really need to do anything other than log this (we could reattempt)


@cli.command()
@click.argument('file_id', type=int)
@click.argument('node_id', type=int)
def push_success(file_id, node_id):
    """Update the database to reflect that the HPSS transfer succeeded.

    INTERNAL COMMAND. NOT FOR HUMAN USE!
    """

    afile = di.ArchiveFile.select().where(di.ArchiveFile.id == file_id).get()
    node = di.StorageNode.select().where(di.StorageNode.id == node_id).get()

    # Update the FileCopy (if exists), or insert a new FileCopy
    try:

        fcopy = di.ArchiveFileCopy.select().where(
            di.ArchiveFileCopy.file == afile,
            di.ArchiveFileCopy.node == node).get()

        fcopy.has_file = 'Y'
        fcopy.wants_file = 'Y'
        fcopy.save()

    except pw.DoesNotExist:
        di.ArchiveFileCopy.insert(file=afile, node=node, has_file='Y',
                                  wants_file='Y').execute()

    log.info('Successful push: %s/%s onto node %s' % (afile.acq.name, afile.name, node.name))


@cli.command()
@click.argument('file_id', type=int)
@click.argument('node_id', type=int)
def pull_success(file_id, node_id):
    """Update the database to reflect that the HPSS transfer succeeded.

    INTERNAL COMMAND. NOT FOR HUMAN USE!
    """

    afile = di.ArchiveFile.select().where(di.ArchiveFile.id == file_id).get()
    node = di.StorageNode.select().where(di.StorageNode.id == node_id).get()

    # Update the FileCopy (if exists), or insert a new FileCopy
    try:

        fcopy = di.ArchiveFileCopy.select().where(
            di.ArchiveFileCopy.file == afile,
            di.ArchiveFileCopy.node == node).get()

        fcopy.has_file = 'Y'
        fcopy.wants_file = 'Y'
        fcopy.save()

    except pw.DoesNotExist:
        di.ArchiveFileCopy.insert(file=afile, node=node, has_file='Y',
                                  wants_file='Y').execute()

    log.info('Successful pull: %s/%s into node %s' % (afile.acq.name, afile.name, node.name))
