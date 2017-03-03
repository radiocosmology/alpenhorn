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


