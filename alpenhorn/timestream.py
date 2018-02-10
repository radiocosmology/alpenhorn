import peewee as pw
from .acquisition import AcqInfoBase, FileInfoBase, ArchiveFile
import peewee as pw

class TimestreamFileInfo(FileInfoBase):
    """Base class for storing metadata for time stream files.

    Attributes
    ----------
    start_time : float
        Start of acquisition in UNIX time.
    finish_time : float
        End of acquisition in UNIX time.
    """
    start_time = pw.DoubleField(null=True)
    finish_time = pw.DoubleField(null=True)

class TimestreamAcqInfo(AcqInfoBase):
    """Base class for acquisitions containing time stream files."""
     @property
     def start_time(self):
         """The start time of this acquisition
         """
         acqtype = self.get_acq_type()
         # Get the different file type instances in this acq type
         file_types_list = acqtype.file_types
         # Loop over the filetype instances
         for filetype in file_types_list:
             # Check if the file type's file info is a timestreamfileinfo
             if issubclass(filetype.fileinfo, TimestreamFileFinfo):
                # Get the start times for each filetype
                starttimes = filetype.fileinfo.select().join(ArchiveFile)\
                             .where(ArchiveFile.acq == self.acq)\
                             .aggregate(pw.fn.Min(filetype.fileinfo.start_time))
                # The minimum of all the startimes should be the start time of
                # this acquisition
                return min(starttimes)

     @property
     def finish_time(self):
         """The finish time of this acquisition"""
         acqtype = self.get_acq_type()
         file_types_list = acqtype.file_types

         for filetype in file_types_list:
             if issubclass(filetype.fileinfo, TimestreamFileFinfo):
                finishtimes = filetype.fileinfo.select().join(ArchiveFile)\
                             .where(ArchiveFile.acq == self.acq)\
                             .aggregate(pw.fn.Min(filetype.fileinfo.finish_time))

                return max(finishtimes)
