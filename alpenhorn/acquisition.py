import peewee as pw
from db import base_model


class ArchiveInst(base_model):
    """Instrument that took the data.

    Attributes
    ----------
    name : string
        Name of instrument.
    """
    name = pw.CharField(max_length=64)
    notes = pw.TextField(null=True)


class AcqType(base_model):
    """The type of data that is being taken in the acquisition.

    Attributes
    ----------
    name : string
        Short name of type. e.g. `raw`, `vis`
    """
    name = pw.CharField(max_length=64)
    notes = pw.TextField(null=True)


class ArchiveAcq(base_model):
    """Describe the acquisition.

    Attributes
    ----------
    name : string
        Name of acquisition.
    inst : foreign key
        Reference to the instrument that took the acquisition.
    type : foreign key
        Reference to the data type type.
    comment : string

    Properties
    ----------
    timed_files
    n_timed_files
    """
    name = pw.CharField(max_length=64)
    inst = pw.ForeignKeyField(ArchiveInst, related_name='acqs')
    type = pw.ForeignKeyField(AcqType, related_name='acqs')
    comment = pw.TextField(null=True)


class FileType(base_model):
    """A file type.

    Attributes
    ----------
    name : string
        The name of this file type.
    notes: string
        Any notes or comments about this file type.
    """
    name = pw.CharField(max_length=64)
    notes = pw.TextField(null=True)


class ArchiveFile(base_model):
    """A file in an acquisition.

    Attributes
    ----------
    acq : foreign key
        Reference to the acquisition this file is part of.
    type : foreign key
        Reference to the type of file that this is.
    name : string
        Name of the file.
    size_b : integer
        Size of file in bytes.
    md5sum : string
        md5 checksum of file. Used for verifying integrity.
    """
    acq = pw.ForeignKeyField(ArchiveAcq, related_name='files')
    type = pw.ForeignKeyField(FileType, related_name='files')
    name = pw.CharField(max_length=64)
    size_b = pw.BigIntegerField(null=True)
    md5sum = pw.CharField(null=True, max_length=32)

