import peewee as pw
from .timestream import *


class base_model(pw.Model):
    """Baseclass for all models."""

    class Meta:
        database = database_proxy

class CorrAcqInfo(base_model, TimestreamAcqInfo):
    """Information about a correlation acquisition. Dummy class
    """
    _acq_type = 'corr'
    _file_types = ['corr', 'log']


class CorrFileInfo(base_model, TimestreamFileInfo):
    """Information about a correlation data file. Dummy class.
    """
    _file_types = 'corr'
