"""Routines for the importing of new files on a node."""

import bisect
import calendar
import datetime
import os
import time

import bitshuffle.h5
import configobj
import h5py
import numpy as np
import peewee as pw
from ch_util import data_index as di
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver

# Setup the logging
from . import logger

log = logger.get_log()

# File to use for caching files already imported
LOCAL_IMPORT_RECORD = "/etc/alpenhornd_import.dat"  # default path

if "ALPENHORN_IMPORT_RECORD" in os.environ:
    LOCAL_IMPORT_RECORD = os.environ["ALPENHORN_IMPORT_RECORD"]

import_done = None

obs_list = None


def load_import_cache():

    global import_done

    # Is there a record of already-imported files? If so, we should use it to
    # prevent needless DB queries when crawling the directories. If there is no
    # record, there is no checking. To start checking from scratch, create an
    # empty LOCAL_IMPORT_RECORD file.
    try:
        with open(LOCAL_IMPORT_RECORD, "r") as fp:
            import_done = fp.read().splitlines()
        import_done.sort()
        if import_done is None:
            import_done = []
    except IOError:
        log.info("No local record of imported files: not checking.")


# Routines to control the filesystem watchdogs.
# =============================================


def setup_observers(node_list):
    """Setup the watchdogs to look for new files in the nodes."""

    global obs_list

    # If any node has auto_import set, look for new files and add them to the
    # DB. Then set up a watchdog for it.
    obs_list = []
    for node in node_list:
        if node.auto_import:
            log.info('Crawling base directory "%s" for new files.' % node.root)
            for acq_name, d, f_list in os.walk(node.root):
                log.info("Crawling %s." % acq_name)
                for file_name in sorted(f_list):
                    import_file(node, node.root, os.path.basename(acq_name), file_name)
            # If it is an NFS mount, then the default Observer() doesn't work.
            # Determine this by seeing if the node name is the same as the node host:
            # not failsafe, but it will do for now.
            if node.host == node.name:
                obs_list.append(Observer())
            else:
                obs_list.append(PollingObserver(timeout=30))
            obs_list[-1].schedule(RegisterFile(node), node.root, recursive=True)
        else:
            obs_list.append(None)

    # Start up the watchdog threads
    for obs in obs_list:
        if obs:
            obs.start()


def stop_observers():
    """Stop watchidog threads."""
    for obs in obs_list:
        if obs:
            obs.stop()


def join_observers():
    """Wait for watchdog threads to terminate."""
    for obs in obs_list:
        if obs:
            obs.join()


# Routines for registering files, acquisitions, copies and info in the DB.
# ========================================================================


def add_acq(name, allow_new_inst=True, allow_new_atype=False, comment=None):
    """Add an aquisition to the database."""
    ts, inst, atype = di.parse_acq_name(name)

    # Is the acquisition already in the database?
    if di.ArchiveAcq.select(di.ArchiveAcq.id).where(di.ArchiveAcq.name == name).count():
        raise di.AlreadyExists('Acquisition "%s" already exists in DB.' % name)

    # Does the instrument already exist in the database?
    try:
        inst_rec = di.ArchiveInst.get(di.ArchiveInst.name == inst)
    except pw.DoesNotExist:
        if allow_new_inst:
            di.ArchiveInst.insert(name=inst).execute()
            logging.info('Added new acquisition instrument "%s" to DB.' % inst)
            inst_rec = di.ArchiveInst.get(di.ArchiveInst.name == inst)
        else:
            raise di.DataBaseError('Acquisition instrument "%s" not in DB.' % inst)

    # Does the archive type already exist in the database?
    try:
        atype_rec = di.AcqType.get(di.AcqType.name == atype)
    except pw.DoesNotExist:
        if allow_new_atype:
            di.AcqType.insert(name=atype).execute()
            logging.info('Added new acquisition type "%s" to DB.' % atype)
        else:
            log.warning('Acquisition type "%s" not in DB.' % atype)
            return None
        # raise di.DataBaseError("Acquisition type \"%s\" not in DB." % atype)

    # Giddy up!
    return di.ArchiveAcq.create(
        name=name, inst=inst_rec, type=atype_rec, comment=comment
    )


def get_acqcorrinfo_keywords_from_h5(path):
    f = h5py.File(path, "r")
    try:
        # This works on the 8-channel correlator.
        n_freq = f["/"].attrs["n_freq"][0]
        n_prod = len(f["/"].attrs["chan_indices"])
        integration = (
            f["/"].attrs["acq.udp.spf"][0] * f["/"].attrs["fpga.int_period"][0]
        )
    except:
        # For now, at least, on the GPU correlator, we need to find the integration
        # time by hand. Find the median difference between timestamps.
        try:
            # Archive Version 2
            version = f.attrs["archive_version"][0]
            dt = np.array([])
            t = f["/index_map/time"]
            for i in range(1, len(t)):
                dt = np.append(dt, float(t[i][1]) - float(t[i - 1][1]))
            integration = np.median(dt)
            n_freq = len(f["/index_map/freq"])
            n_prod = len(f["/index_map/prod"])
        except:
            dt = np.array([])
            t = f["timestamp"]
            for i in range(1, len(t)):
                ddt = (
                    float(t[i][1])
                    - float(t[i - 1][1])
                    + (float(t[i][2]) - float(t[i - 1][2])) * 1e-6
                )
                if t[i][2] + 2e-5 < t[i - 1][2]:
                    dt = np.append(dt, ddt + 1.0)
                else:
                    dt = np.append(dt, ddt)
            integration = np.median(dt)
            n_freq = f["/"].attrs["n_freq"][0]
            n_prod = len(f["/"].attrs["chan_indices"])

    f.close()
    return {"integration": integration, "nfreq": n_freq, "nprod": n_prod}


def get_acqhkinfo_keywords_from_h5(path):
    fullpath = "%s/%s" % (path, di.fname_atmel)
    fp = open(fullpath, "r")
    if fp is None:
        raise Exception('Could not find "%s".' % fullpath)

    ret = []
    while 1:
        l = fp.readline()
        if not l:
            break
        if l[0] == "#":
            continue
        if len(l.split()) < 2:
            continue
        name = l.split()[0]
        iid = " ".join(l.split()[1:])
        ret.append({"atmel_id": iid, "atmel_name": name})

    fp.close()
    return ret


def get_rawinfo_keywords(path):
    fname = "%s/%s" % (path, di.fname_raw_settings)
    conf = configobj.ConfigObj(fname)
    d = datetime.datetime.strptime(conf.get("start_time"), "%Y%m%dT%H%M%SZ")
    t = calendar.timegm(d.utctimetuple())
    return {
        "start_time": t,
        "nfreq": conf.get("num_freq"),
        "ninput": conf.get("num_inputs"),
        "nframe": conf.get("num_frames"),
        "packet_len": conf.get("packet_len"),
        "header_len": conf.get("header_len"),
        "footer_len": conf.get("footer_len"),
        "offset": conf.get("offset"),
        "stream_id": conf.get("stream_id"),
        "data_bits": conf.get("data_bits"),
        "stride": conf.get("stride"),
        "note": conf.get("note"),
    }


def get_acqrawadcinfo_keywords_from_h5(acq_name):
    # We need to use the calendar module because the datetime module can't give us
    # timegm() for some strange reason.
    d = datetime.datetime.strptime(acq_name[0:16], "%Y%m%dT%H%M%SZ")
    t = calendar.timegm(d.utctimetuple())
    return {"start_time": t}


def get_filecorrinfo_keywords_from_h5(path):
    f = h5py.File(path, "r")
    try:
        start_time = f["timestamp"][0][1] + f["timestamp"][0][2] * 1e-6
        finish_time = f["timestamp"][-1][1] + f["timestamp"][-1][2] * 1e-6
    except:
        start_time = f["/index_map/time"][0][1]
        finish_time = f["/index_map/time"][-1][1]
    chunk_number, freq_number = di.parse_corrfile_name(os.path.basename(path))
    f.close()
    return {
        "start_time": start_time,
        "finish_time": finish_time,
        "chunk_number": chunk_number,
        "freq_number": freq_number,
    }


def get_fileweatherinfo_keywords_from_h5(path):
    f = h5py.File(path, "r")
    start_time = f["/index_map/time"][0]
    finish_time = f["/index_map/time"][-1]
    d = di.parse_weatherfile_name(os.path.basename(path))
    f.close()
    return {"start_time": start_time, "finish_time": finish_time, "date": d}


def get_filehkinfo_keywords_from_h5(path):
    f = h5py.File(path, "r")
    start_time = f["index_map/time"][0]
    finish_time = f["index_map/time"][-1]
    chunk_number, atmel_name = di.parse_hkfile_name(os.path.basename(path))
    f.close()
    return {
        "start_time": start_time,
        "finish_time": finish_time,
        "atmel_name": atmel_name,
        "chunk_number": chunk_number,
    }


def get_filerawinfo_keywords(rawinfo, size_b, file_name):
    chunk_num = int(file_name[: file_name.find(".")])
    log.debug("Rawinfo: %d %d %d" % (size_b, rawinfo.nframe, rawinfo.packet_len))
    dt = size_b * rawinfo.nframe * 2048 / (rawinfo.packet_len * 8e8)
    start_time = rawinfo.start_time + chunk_num * dt
    finish_time = rawinfo.start_time + (chunk_num + 1) * dt
    return {
        "start_time": start_time,
        "finish_time": finish_time,
        "chunk_number": chunk_num,
    }


def import_file(node, root, acq_name, file_name):
    done = False
    while not done:
        try:
            _import_file(node, root, acq_name, file_name)
            done = True
        except pw.OperationalError:
            log.error(
                "MySQL connexion dropped. Will attempt to reconnect in " "five seconds."
            )
            time.sleep(5)
            di.connect_database(True)


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
    global import_done
    curr_done = True
    fullpath = "%s/%s/%s" % (root, acq_name, file_name)
    log.debug("Considering %s for import." % fullpath)

    # Skip the file if ch_master.py still has a lock on it.
    if os.path.isfile("%s/%s/.%s.lock" % (root, acq_name, file_name)):
        log.debug('Skipping "%s", which is locked by ch_master.py.' % fullpath)
        return

    # Parse the path
    try:
        ts, inst, atype = di.parse_acq_name(acq_name)
    except di.Validation:
        log.info("Skipping non-acquisition path %s." % acq_name)
        return

    if import_done is not None:
        i = bisect.bisect_left(import_done, fullpath)
        if i != len(import_done) and import_done[i] == fullpath:
            log.debug("Skipping already-registered file %s." % fullpath)
            return

    # Figure out which acquisition this is; add if necessary.
    try:
        acq = di.ArchiveAcq.get(di.ArchiveAcq.name == acq_name)
        log.debug('Acquisition "%s" already in DB. Skipping.' % acq_name)
    except pw.DoesNotExist:
        acq = add_acq(acq_name)
        if acq is None:
            return
        log.info('Acquisition "%s" added to DB.' % acq_name)

    # What kind of file do we have?
    ftype = di.detect_file_type(file_name)
    if ftype is None:
        log.info('Skipping unrecognised file "%s/%s".' % (acq_name, file_name))
        return

    # Make sure information about the acquisition exists in the DB.
    if atype == "corr" and ftype.name == "corr":
        if not acq.corrinfos.count():
            try:
                di.CorrAcqInfo.create(
                    acq=acq, **get_acqcorrinfo_keywords_from_h5(fullpath)
                )
                log.info(
                    'Added information for correlator acquisition "%s" to '
                    "DB." % acq_name
                )
            except:
                log.warning(
                    'Missing info for acquistion "%s": HDF5 datasets '
                    "empty. Leaving fields NULL." % (acq_name)
                )
                di.CorrAcqInfo.create(acq=acq)
    elif atype == "hk" and ftype.name == "hk":
        try:
            keywords = get_acqhkinfo_keywords_from_h5("%s/%s" % (root, acq_name))
        except:
            log.warning("Could no open atmel_id.dat file. Skipping.")
            keywords = []
        for kw in keywords:
            if not sum(
                1
                for _ in di.HKAcqInfo.select()
                .where(di.HKAcqInfo.acq == acq)
                .where(di.HKAcqInfo.atmel_name == kw["atmel_name"])
            ):
                try:
                    di.HKAcqInfo.create(acq=acq, **kw)
                    log.info(
                        'Added information for housekeeping acquisition "%s", '
                        "board %s to DB." % (acq_name, kw["atmel_name"])
                    )
                except:
                    log.warning(
                        'Missing info for acquisition "%s": atmel_id.dat '
                        "file missing or corrupt. Skipping this acquisition." % acq_name
                    )
                    return
    elif atype == "rawadc":
        if not acq.rawadcinfos.count():
            di.RawadcAcqInfo.create(
                acq=acq, **get_acqrawadcinfo_keywords_from_h5(acq_name)
            )
            log.info(
                'Added information for raw ADC acquisition "%s" to ' "DB." % acq_name
            )
    elif atype == "raw":
        try:
            raw_info = acq.rawinfos.get()
        except pw.DoesNotExist:
            try:
                raw_info = di.RawAcqInfo.create(
                    acq=acq, **get_rawinfo_keywords("%s/%s" % (root, acq_name))
                )
                log.info('Added information for raw acquisition "%s" to DB.' % acq_name)
            except:
                log.info(
                    'Missing info in settings.dat for acquisition "%s". '
                    "Skipping this acquisition." % acq_name
                )
                return

    # Add the file, if necessary.
    try:
        file = di.ArchiveFile.get(
            di.ArchiveFile.name == file_name, di.ArchiveFile.acq == acq
        )
        size_b = file.size_b
        log.debug('File "%s/%s" already in DB. Skipping.' % (acq_name, file_name))
    except pw.DoesNotExist:
        log.debug("Computing md5sum.")
        md5sum = di.md5sum_file(fullpath, cmd_line=True)
        size_b = os.path.getsize(fullpath)
        done = False
        while not done:
            try:
                file = di.ArchiveFile.create(
                    acq=acq, type=ftype, name=file_name, size_b=size_b, md5sum=md5sum
                )
                done = True
            except pw.OperationalError:
                log.error(
                    "MySQL connexion dropped. Will attempt to reconnect in "
                    "five seconds."
                )
                time.sleep(5)
                di.connect_database(True)
        log.info('File "%s/%s" added to DB.' % (acq_name, file_name))

    # Register the copy of the file here on the collection server, if (1) it does
    # not exist, or (2) it does exist but has been labelled as corrupt. If (2),
    # check again.
    if not file.copies.where(di.ArchiveFileCopy.node == node).count():
        copy = di.ArchiveFileCopy.create(
            file=file, node=node, has_file="Y", wants_file="Y"
        )
        log.info('Registered file copy "%s/%s" to DB.' % (acq_name, file_name))

    # Make sure information about the file exists in the DB.
    if ftype.name == "corr":
        # Add if (1) there is no corrinfo or (2) the corrinfo is missing.
        if not file.corrinfos.count():
            try:
                di.CorrFileInfo.create(
                    file=file, **get_filecorrinfo_keywords_from_h5(fullpath)
                )
                log.info(
                    'Added information for file "%s/%s" to DB.' % (acq_name, file_name)
                )
            except:
                if not file.corrinfos.count():
                    di.CorrFileInfo.create(file=file)
                log.warning(
                    'Missing info for file "%s/%s": HDF5 datasets '
                    "empty or unreadable. Leaving fields NULL." % (acq_name, file_name)
                )
        elif not file.corrinfos[0].start_time:
            try:
                i = file.corrinfos[0]
                k = get_filecorrinfo_keywords_from_h5(fullpath)
            except:
                log.debug('Still missing info for file "%s/%s".')
            else:
                i.start_time = k["start_time"]
                i.finish_time = k["finish_time"]
                i.chunk_number = k["chunk_number"]
                i.freq_number = k["freq_number"]
                i.save()
                log.info(
                    'Added information for file "%s/%s" to DB.' % (acq_name, file_name)
                )
    if ftype.name == "hk":
        # Add if (1) there is no hkinfo or (2) the hkinfo is missing.
        if not file.hkinfos.count():
            try:
                di.HKFileInfo.create(
                    file=file, **get_filehkinfo_keywords_from_h5(fullpath)
                )
                log.info(
                    'Added information for file "%s/%s" to DB.' % (acq_name, file_name)
                )
            except:
                if not file.corrinfos.count():
                    di.HKFileInfo.create(file=file)
                log.warning(
                    'Missing info for file "%s/%s": HDF5 datasets '
                    "empty or unreadable. Leaving fields NULL." % (acq_name, file_name)
                )
        elif not file.hkinfos[0].start_time:
            try:
                i = file.hkinfos[0]
                k = get_filehkinfo_keywords_from_h5(fullpath)
            except:
                log.debug('Still missing info for file "%s/%s".')
            else:
                i.start_time = k["start_time"]
                i.finish_time = k["finish_time"]
                i.atmel_name = k["atmel_name"]
                i.chunk_number = k["chunk_number"]
                i.save()
                log.info(
                    'Added information for file "%s/%s" to DB.' % (acq_name, file_name)
                )
    if ftype.name == "weather":
        # Add if (1) there is no weatherinfo or (2) the weatherinfo is missing.
        if not file.weatherinfos.count():
            #      try:
            di.WeatherFileInfo.create(
                file=file, **get_fileweatherinfo_keywords_from_h5(fullpath)
            )
            log.info(
                'Added information for file "%s/%s" to DB.' % (acq_name, file_name)
            )
        #      except:
        #        if not file.corrinfos.count():
        #          di.WeatherFileInfo.create(file=file)
        #        log.warning("Missing info for file \"%s/%s\": HDF5 datasets " \
        #                    "empty or unreadable. Leaving fields NULL." %
        #                    (acq_name, file_name))
        elif not file.weatherinfos[0].start_time:
            try:
                i = file.weatherinfos[0]
                k = get_fileweatherinfo_keywords_from_h5(fullpath)
            except:
                log.debug('Still missing info for file "%s/%s".')
            else:
                i.start_time = k["start_time"]
                i.finish_time = k["finish_time"]
                i.date = k["date"]
                i.save()
                log.info(
                    'Added information for file "%s/%s" to DB.' % (acq_name, file_name)
                )
    if ftype.name == "raw":
        # Add if (1) there is no rawinfo or (2) the rawinfo is missing.
        if not file.rawinfos.count():
            try:
                di.RawFileInfo.create(
                    file=file, **get_filerawinfo_keywords(raw_info, size_b, file_name)
                )
                log.info(
                    'Added information for file "%s/%s" to DB.' % (acq_name, file_name)
                )
            except:
                if not file.corrinfos.count():
                    di.RawFileInfo.create(file=file)
                log.warning(
                    'Missing info for file "%s/%s". Leaving fields NULL.'
                    % (acq_name, file_name)
                )
        elif not file.rawinfos[0].start_time:
            try:
                i = file.rawinfos[0]
                k = get_filerawinfo_keywords(raw_info, size_b, file_name)
            except:
                log.debug('Still missing info for file "%s/%s".')
            else:
                i.start_time = k["start_time"]
                i.chunk_number = k["chunk_number"]
                i.save()
                log.info(
                    'Added information for file "%s/%s" to DB.' % (acq_name, file_name)
                )

    if import_done is not None:
        bisect.insort_left(import_done, fullpath)
        with open(LOCAL_IMPORT_RECORD, "w") as fp:
            fp.write("\n".join(import_done))


# Watchdog stuff
# ==============


class RegisterFile(FileSystemEventHandler):
    def __init__(self, node):
        log.info('Registering node "%s" for auto_import watchdog.' % (node.name))
        self.node = node
        self.root = node.root
        if self.root[-1] == "/":
            self.root = self.root[0:-1]
        super(RegisterFile, self).__init__()

    def on_modified(self, event):
        # Figure out the parts; it should be ROOT/ACQ_NAME/FILE_NAME
        subpath = event.src_path.replace(self.root + "/", "").split("/")
        if len(subpath) == 2:
            import_file(self.node, self.root, subpath[0], subpath[1])
        return

    def on_moved(self, event):
        # Figure out the parts; it should be ROOT/ACQ_NAME/FILE_NAME
        subpath = event.dest_path.replace(self.root + "/", "").split("/")
        if len(subpath) == 2:
            import_file(self.node, self.root, subpath[0], subpath[1])
        return

    def on_deleted(self, event):
        # For lockfiles: ensure that the file that was locked is added: it is
        # possible that the watchdog notices that a file has been closed before the
        # lockfile is deleted.
        subpath = event.src_path.replace(self.root + "/", "").split("/")
        if len(subpath) == 2:
            if subpath[1][0] == "." and subpath[1][-5:] == ".lock":
                subpath[1] = subpath[1][1:-5]
                import_file(self.node, self.root, subpath[0], subpath[1])
