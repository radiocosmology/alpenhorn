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

def import_file(node, root, acq_name, file_name):
    done = False
    while not done:
        try:
            _import_file(node, root, acq_name, file_name)
            done = True
        except pw.OperationalError:
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

    # Parse the path
    try:
        ts, inst, atype = parse_acq_name(acq_name)
    except Validation:
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
        acq = add_acq(acq_name)
        if acq is None:
            return
        log.info("Acquisition \"%s\" added to DB." % acq_name)

    # TODO: file-type detection
    # What kind of file do we have?
    ftype = detect_file_type(file_name)
    if ftype is None:
        log.info("Skipping unrecognised file \"%s/%s\"." % (acq_name, file_name))
        return

    # TODO: acquisition-type specific handling

    # Add the file, if necessary.
    try:
        file = ac.ArchiveFile.get(ac.ArchiveFile.name == file_name,
                                  ac.ArchiveFile.acq == acq)
        size_b = file.size_b
        log.debug("File \"%s/%s\" already in DB. Skipping." % (acq_name, file_name))
    except pw.DoesNotExist:
        log.debug("Computing md5sum.")
        md5sum = md5sum_file(fullpath, cmd_line=False)
        size_b = os.path.getsize(fullpath)
        done = False
        while not done:
            try:
                file = ac.ArchiveFile.create(acq=acq, type=ftype, name=file_name,
                                             size_b=size_b, md5sum=md5sum)
                done = True
            except pw.OperationalError:
                log.error("MySQL connexion dropped. Will attempt to reconnect in "
                          "five seconds.")
                time.sleep(5)
                # di.connect_database(True)
        log.info("File \"%s/%s\" added to DB." % (acq_name, file_name))

    # Register the copy of the file here on the collection server, if (1) it does
    # not exist, or (2) it does exist but has been labelled as corrupt. If (2),
    # check again.
    if not file.copies.where(ar.ArchiveFileCopy.node == node).count():
        copy = ar.ArchiveFileCopy.create(file=file, node=node, has_file='Y',
                                         wants_file='Y')
        log.info("Registered file copy \"%s/%s\" to DB." % (acq_name, file_name))

    # Make sure information about the file exists in the DB.
    # TODO: file-type specific handling

    # TODO: imported files caching
    # if import_done is not None:
    #     bisect.insort_left(import_done, fullpath)
    #     with open(LOCAL_IMPORT_RECORD, "w") as fp:
    #         fp.write("\n".join(import_done))


# Routines for registering files, acquisitions, copies and info in the DB.
# ========================================================================

def add_acq(name, allow_new_inst=True, allow_new_atype=False, comment=None):
    """Add an aquisition to the database.
    """
    ts, inst, atype = parse_acq_name(name)

    # Is the acquisition already in the database?
    if ac.ArchiveAcq.select(ac.ArchiveAcq.id).where(
            ac.ArchiveAcq.name == name).count():
        raise AlreadyExists("Acquisition \"%s\" already exists in DB." %
                               name)

    # Does the instrument already exist in the database?
    try:
        inst_rec = ac.ArchiveInst.get(ac.ArchiveInst.name == inst)
    except pw.DoesNotExist:
        if allow_new_inst:
            ac.ArchiveInst.insert(name=inst).execute()
            log.info("Added new acquisition instrument \"%s\" to DB." %
                         inst)
            inst_rec = ac.ArchiveInst.get(ac.ArchiveInst.name == inst)
        else:
            raise DataBaseError("Acquisition instrument \"%s\" not in DB." %
                                   inst)

    # Does the archive type already exist in the database?
    try:
        atype_rec = ac.AcqType.get(ac.AcqType.name == atype)
    except pw.DoesNotExist:
        if allow_new_atype:
            ac.AcqType.insert(name=atype).execute()
            log.info("Added new acquisition type \"%s\" to DB." % atype)
        else:
            log.warning("Acquisition type \"%s\" not in DB." % atype)
            return None
           #raise DataBaseError("Acquisition type \"%s\" not in DB." % atype)

    # Giddy up!
    return ac.ArchiveAcq.create(name=name, inst=inst_rec, type=atype_rec,
                                comment=comment)


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

        block_size = 256*128

        md5 = hashlib.md5()
        with open(filename, 'rb') as f:
            for chunk in iter(lambda: f.read(block_size), b''):
                md5.update(chunk)
        if hr:
            return md5.hexdigest()
        return md5.digest()


def parse_acq_name(name):
    """Validate and parse an acquisition name.

    Parameters
    ----------
    name : The name of the acquisition.

    Returns
    -------
    A tuple of timestamp, instrument and type.

    """
    if not re.match(fmt_acq, name):
        raise Validation("Bad acquisition name format for \"%s\"." % name)
    ret = tuple(name.split("_"))
    if len(ret) != 3:
        raise Validation("Bad acquisition name format for \"%s\"." % name)
    return ret


fmt_acq = re.compile("([0-9]{8})T([0-9]{6})Z_([A-Za-z0-9]*)_([A-Za-z]*)")
fmt_log = re.compile("ch_(master|hk)\.log")
def detect_file_type(name):
    """Figure out what kind of file this is.

    Parameters
    ----------
    name : The name of the file.

    Returns
    -------
    An object of FileType, or None if unrecognised.

    """

    if re.match(fmt_log, name):
        return ac.FileType.get(name = "log")
    else:
        return None


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
