"""Routines for the importing of new files on a node."""
import time
import os
import re

import peewee as pw

import alpenhorn.db as db
import alpenhorn.archive as ar
import alpenhorn.storage as st
import alpenhorn.acquisition as ac

# Setup the logging
from . import logger
log = logger.get_log()

log.setLevel(logger.logging.DEBUG)

def import_file(node, root, acq_name, file_name):
    done = False
    while not done:
        try:
            _import_file(node, root, acq_name, file_name)
            done = True
        except pw.OperationalError as e:
            log.exception(e)
            log.error("MySQL connexion dropped. Will attempt to reconnect in "
                      "five seconds.")
            time.sleep(5)
            # TODO: reconnection
            db.database_proxy.connect()


def _import_file(node, root, acq_name, file_name):
    """Import a file into the DB.

    This routine adds the following to the database, if they do not already exist
    (or might be corrupted).
    - The acquisition that the file is a part of.
    - Information on the acquisition, if it is of type "corr".
    - The file.
    - Information on the file, if it is of type "corr".
    - Indicates that the file exists on this node.
    """
    # global import_done
    curr_done = True
    fullpath = "%s/%s/%s" % (root, acq_name, file_name)
    log.debug("Considering %s for import." % fullpath)

    # Skip the file if ch_master.py still has a lock on it.
    if os.path.isfile("%s/%s/.%s.lock" % (root, acq_name, file_name)):
        log.debug("Skipping \"%s\", which is locked by ch_master.py." % fullpath)
        return

    # Check if we can handle this acquisition, and skip if we can't
    if ac.AcqType.detect(acq_name, node) is None:
        log.info("Skipping non-acquisition path %s." % acq_name)
        return

    # TODO: imported files caching
    # if import_done is not None:
    #     i = bisect.bisect_left(import_done, fullpath)
    #     if i != len(import_done) and import_done[i] == fullpath:
    #         log.debug("Skipping already-registered file %s." % fullpath)
    #         return

    # Figure out which acquisition this is; add if necessary.
    try:
        acq = ac.ArchiveAcq.get(ac.ArchiveAcq.name == acq_name)
        log.debug("Acquisition \"%s\" already in DB. Skipping." % acq_name)
    except pw.DoesNotExist:
        acq = add_acq(acq_name, node)
        if acq is None:
            return
        log.info("Acquisition \"%s\" added to DB." % acq_name)

    # What kind of file do we have?
    ftype = ac.FileType.detect(file_name, acq, node)

    if ftype is None:
        log.info("Skipping unrecognised file \"%s/%s\"." % (acq_name, file_name))
        return

    # Add the file, if necessary.
    try:
        file_ = ac.ArchiveFile.get(ac.ArchiveFile.name == file_name,
                                   ac.ArchiveFile.acq == acq)
        log.debug("File \"%s/%s\" already in DB. Skipping." % (acq_name, file_name))

    except pw.DoesNotExist:
        log.debug("Computing md5sum of \"%s\"." % file_name)
        md5sum = md5sum_file(fullpath, cmd_line=False)
        size_b = os.path.getsize(fullpath)

        done = False
        while not done:
            try:
                with db.database_proxy.atomic():
                    file_ = ac.ArchiveFile.create(acq=acq, type=ftype, name=file_name,
                                                  size_b=size_b, md5sum=md5sum)

                    ftype.file_info.new(file_, node)

                done = True
            except pw.OperationalError as e:
                log.exception(e)
                log.error("MySQL connexion dropped. Will attempt to reconnect in "
                          "five seconds.")
                time.sleep(5)

                # di.connect_database(True)
        log.info("File \"%s/%s\" added to DB." % (acq_name, file_name))

    # Register the copy of the file here on the collection server, if (1) it
    # does not exist, or (2) if there has previously been a copy here ensure it
    # is checksummed to ensure the archives integrity.
    if not file_.copies.where(ar.ArchiveFileCopy.node == node).count():
        copy = ar.ArchiveFileCopy.create(file=file_, node=node, has_file='Y',
                                         wants_file='Y')
        log.info("Registered file copy \"%s/%s\" to DB." % (acq_name, file_name))
    else:
        # Mark any previous copies as not being present...
        query = (ar.ArchiveFileCopy.update(has_file='N')
                 .where(ar.ArchiveFileCopy.file == file,
                        ar.ArchiveFileCopy.node == node))
        query.execute()

        # ... then take the latest and mark it with has_file=M to force it to be
        # checked.
        copy = (ar.ArchiveFileCopy.select()
                .where(ar.ArchiveFileCopy.file == file,
                       ar.ArchiveFileCopy.node == node)
                .order_by(ar.ArchiveFileCopy.id).get())

        copy.has_file = 'M'
        copy.save()

    # TODO: imported files caching
    # if import_done is not None:
    #     bisect.insort_left(import_done, fullpath)
    #     with open(LOCAL_IMPORT_RECORD, "w") as fp:
    #         fp.write("\n".join(import_done))


# Routines for registering files, acquisitions, copies and info in the DB.
# ========================================================================

def add_acq(name, node, comment=""):
    """Add an aquisition to the database.

    This looks for an appropriate acquisition type, and if successful creates
    the ArchiveAcq and AcqInfo entries for the acquisition.

    Parameters
    ----------
    name : string
        Name of the acquisition directory.
    node : StorageNode
        Node that the acquisition is on.
    comment : string, optional
        An optional comment.

    Returns
    -------
    acq : ArchiveAcq
        The ArchiveAcq entry.
    acqinfo : AcqInfoBase
        The AcqInfo entry.
    """

    # Is the acquisition already in the database?
    if ac.ArchiveAcq.select(ac.ArchiveAcq.id).where(
            ac.ArchiveAcq.name == name).count():
        raise AlreadyExists("Acquisition \"%s\" already exists in DB." %
                            name)

    # Find an acquisition type that can handle this acq
    acq_type = ac.AcqType.detect(name, node)

    if acq_type is None:
        log.debug("No handler available to process \"%s\"" % name)
        return

    # At the moment we need an instrument, so just create one.
    try:
        inst_rec = ac.ArchiveInst.get(name='inst')
    except pw.DoesNotExist:
        print "Creating inst."
        inst_rec = ac.ArchiveInst.create(name='inst')

    # Create the ArchiveAcq entry and the AcqInfo entry for the acquisition. Run
    # in a transaction so we don't end up with inconsistency.
    with db.database_proxy.atomic():
        # Insert the archive record
        acq = ac.ArchiveAcq.create(name=name, inst=inst_rec,
                                   type=acq_type,
                                   comment=comment)

        # Generate the metadata table
        acq_type.acq_info.new(acq, node)

    return acq

# Helper routines for adding files
# ================================

def md5sum_file(filename, hr=True, cmd_line=False):
    """Find the md5sum of a given file.

    Output should reproduce that of UNIX md5sum command.

    Parameters
    ----------
    filename: string
        Name of file to checksum.
    hr: boolean, optional
        Should output be a human readable hexstring (default is True).
    cmd_line: boolean, optional
        If True, then simply do an os call to md5sum (default is False).

    See Also
    --------
    http://stackoverflow.com/questions/1131220/get-md5-hash-of-big-files-in-
    python
    """
    if cmd_line:
        p = os.popen("md5sum %s 2> /dev/null" % filename, "r")
        res = p.read()
        p.close()
        md5 = res.split()[0]
        assert len(md5) == 32
        return md5
    else:
        import hashlib

        block_size = 256 * 128

        md5 = hashlib.md5()
        with open(filename, 'rb') as f:
            for chunk in iter(lambda: f.read(block_size), b''):
                md5.update(chunk)
        if hr:
            return md5.hexdigest()
        return md5.digest()


# Exceptions
# ==========


class Validation(Exception):
    """Raise when validation of a name or field fails."""


class DataBaseError(Exception):
    """Raise when there is some internal inconsistency with the database."""


class AlreadyExists(Exception):
    """Raise when a record already exists in the database."""


class DataFlagged(Exception):
    """Raised when data is affected by a global flag."""
