"""Custom acquisition and file handlers for the docker integration tests."""

from alpenhorn import generic as ge


# Create handlers for the acquisition and file types
class ZabInfo(ge.GenericAcqInfo):
    _acq_type = "zab"
    _file_types = ["zxc", "log"]
    patterns = ["**zab"]

    @classmethod
    def set_config(cls, configdict):
        pass


class QuuxInfo(ge.GenericAcqInfo):
    _acq_type = "quux"
    _file_types = ["zxc", "log"]
    patterns = ["*quux", "x"]

    @classmethod
    def set_config(cls, configdict):
        pass


class ZxcInfo(ge.GenericFileInfo):
    _file_type = "zxc"
    patterns = ["**.zxc", "jim*", "sheila"]

    @classmethod
    def set_config(cls, configdict):
        pass


class LogInfo(ge.GenericFileInfo):
    _file_type = "log"
    patterns = ["*.log"]

    @classmethod
    def set_config(cls, configdict):
        pass


def register_extension():
    ext_dict = {"acq_types": [ZabInfo, QuuxInfo], "file_types": [ZxcInfo, LogInfo]}

    return ext_dict
