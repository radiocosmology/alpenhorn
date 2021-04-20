"""Generic handlers for files and acquisitions.
"""

from . import acquisition as ac


class GenericAcqInfo(ac.AcqInfoBase):
    """A generic acquisition type that can handle acquisitions matching specific
    patterns, but doesn't keep track of any metadata.
    """

    _acq_type = 'generic'

    _file_types = ['generic']

    patterns = None
    glob = True

    @classmethod
    def set_config(cls, configdict):
        """Set configuration options for this acquisition type.

        There are three supported options:

        `glob`
            If `True` (default) we should interpret the patterns as globs (use
            the extended syntax of `globre`). Otherwise they are treated as
            regular expressions.

        `patterns`
            A list of regular expressions or globs to match supported
            acquisitions.

        `file_types`
            A list of the supported file types within this acquisition.

        """
        # Extract patterns to process from a section of the config file

        cls.patterns = configdict['patterns']

        if 'glob' in configdict:
            cls.glob = configdict['glob']

        if 'file_types' in configdict:
            cls._file_types = configdict['file_types']

    @classmethod
    def _is_type(cls, acqname, node_root):
        """Check whether the acquisition path matches any patterns we can handle.
        """
        return _check_match(acqname, cls.patterns, cls.glob)

    def set_info(self, acqname, node_root):
        """Generic acquisition type has no metadata, so just return.
        """
        return


class GenericFileInfo(ac.FileInfoBase):
    """A generic file type that cen be configured to match a pattern, but stores no
    metadata.
    """

    _file_type = 'generic'

    patterns = None
    glob = True

    @classmethod
    def set_config(cls, configdict):
        """Set the configuration information.

        There are two supported options:

        `glob`
            If `True` (default) we should interpret the patterns as globs (use
            the extended syntax of `globre`). Otherwise they are treated as
            regular expressions.

        `patterns`
            A list of regular expressions or globs to match supported
            acquisitions.

        Parameters
        ----------
        configdict : dict
            Dictionary of configuration options.
        """
        cls.patterns = configdict['patterns']

        if 'glob' in configdict:
            cls.glob = configdict['glob']

    @classmethod
    def _is_type(cls, filename, acq_root):
        """Check whether the file matches any patterns we can handle.
        """
        return _check_match(filename, cls.patterns, cls.glob)

    def set_info(self, filename, acq_root):
        """This file type has no meta data so this method does nothing.
        """
        pass


def register_extension():

    ext_dict = {
        'acq_types': [GenericAcqInfo],
        'file_types': [GenericFileInfo]
    }

    return ext_dict


def _check_match(name, patterns, glob):
    # Get the match function to use depending on whether globbing is enabled.
    if glob:
        import globre
        matchfn = globre.match
    else:
        import re
        matchfn = re.match

    # Loop over patterns and check for matches
    for pattern in patterns:

        if matchfn(pattern, name):
            return True

    return False
