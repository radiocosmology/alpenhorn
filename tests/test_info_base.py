"""Test alpenhorn.info_base"""

import pytest
import peewee as pw

from alpenhorn.db import base_model, database_proxy
from alpenhorn.info_base import (
    info_base,
    _NoInfo,
    no_info,
    acq_info_base,
    file_info_base,
    GenericAcqInfo,
    GenericFileInfo,
)


def test_has_model():
    """Test has_model"""

    assert not info_base.has_model()
    assert not _NoInfo.has_model()
    assert acq_info_base.has_model()
    assert file_info_base.has_model()
    assert GenericAcqInfo.has_model()
    assert GenericFileInfo.has_model()


def test_config_baseclass(simpleacqtype):
    """Attempt to configure a base class should fail."""

    for cls in (
        info_base,
        _NoInfo,
        acq_info_base,
        file_info_base,
        GenericAcqInfo,
        GenericFileInfo,
    ):
        with pytest.raises(TypeError):
            cls.set_config(simpleacqtype)


def test_init_unconfig(simpleacqtype):
    """Attempt to instantiate an unconfigured class should fail."""
    cls = no_info()

    # Doesn't work
    with pytest.raises(RuntimeError):
        cls()

    # Now configure
    cls.set_config(simpleacqtype)

    # and now it should work
    info = cls()
    assert isinstance(info, _NoInfo)


def test_no_info():
    """Test no_info()"""

    ni = no_info()

    # Should be a proper subclass
    assert ni is not _NoInfo
    assert issubclass(ni, _NoInfo)


def test_check_match_globre(acqtype):
    """Test glob matching in _check_match"""

    class cls(GenericAcqInfo):
        pass

    at = acqtype(
        name="at",
        info_config='{"glob": true, "patterns": ["a/?/b", "a/*/c", "a/**/d"]}',
    )
    cls.set_config(at)

    assert not cls.is_type("z", None)
    assert not cls.is_type("a//b", None)
    assert cls.is_type("a/1/b", None)
    assert not cls.is_type("a/12/b", None)
    assert not cls.is_type("a///b", None)
    assert cls.is_type("a//c", None)
    assert cls.is_type("a/1/c", None)
    assert cls.is_type("a/12/c", None)
    assert not cls.is_type("a/1/2/c", None)
    assert cls.is_type("a//d", None)
    assert cls.is_type("a/1/d", None)
    assert cls.is_type("a/12/d", None)
    assert cls.is_type("a/1/2/d", None)


def test_check_match_re(filetype):
    """Test regex matching in _check_match"""

    class cls(GenericFileInfo):
        pass

    ft = filetype(
        name="ft",
        info_config='{"glob": false, "patterns": ["a/1?/b", "a/2*/c", "a/./d"]}',
    )
    cls.set_config(ft)

    assert not cls.is_type("z", None)
    assert cls.is_type("a//b", None)
    assert cls.is_type("a/1/b", None)
    assert not cls.is_type("a/2/b", None)
    assert not cls.is_type("a///b", None)
    assert cls.is_type("a//c", None)
    assert not cls.is_type("a/1/c", None)
    assert cls.is_type("a/2/c", None)
    assert cls.is_type("a/22/c", None)
    assert not cls.is_type("a//d", None)
    assert cls.is_type("a/1/d", None)
    assert not cls.is_type("a/12/d", None)
    assert cls.is_type("a///d", None)


def test_set_info_call_model(simpleacqtype):
    """Test _set_info calling from __init__ with a base_model"""

    class cls(info_base, base_model):
        called = False

        def _set_info(self, *args, **kwargs):
            self.called = True
            return dict()

    cls.set_config(simpleacqtype)

    # Not called if path_ is not provided
    info = cls()
    assert not info.called

    # Called when path_ and node_ are prov
    info = cls(path_="a", node_="b")
    assert info.called

    # This one's an error
    with pytest.raises(ValueError):
        info = cls(path_="a")


def test_set_info_call_nomodel(simpleacqtype):
    """Test _set_info calling from __init__ without a base_model"""

    class cls(info_base):
        called = False

        def _set_info(self, *args, **kwargs):
            self.called = True
            return dict()

    cls.set_config(simpleacqtype)

    # _set_info is never called
    info = cls()
    assert not info.called
    info = cls(path_="a", node_="b")
    assert not info.called
    info = cls(path_="a")
    assert not info.called


def test_set_info_args(simpleacqtype):
    """Test args passed to _set_info"""

    class cls(info_base, base_model):
        args = None
        kwargs = None

        def _set_info(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs
            return dict()

    cls.set_config(simpleacqtype)

    info = cls(path_="path", node_="node")
    assert info.args == ("path", "node", None, None)
    assert info.kwargs == dict()

    info = cls(path_="path", node_="node", acqtype_="at", acqname_="an")
    assert info.args == ("path", "node", "at", "an")
    assert info.kwargs == dict()


def test_set_info_return(simpleacqtype):
    """Test that data returned from set_info makes it to the peewee model."""

    class cls(acq_info_base):
        a = pw.IntegerField()
        b = pw.CharField()

        def _set_info(self, *args, **kwargs):
            return {"a": 1, "b": "b"}

    cls.set_config(simpleacqtype)
    info = cls(path_="path", node_="node")
    assert info.a == 1
    assert info.b == "b"


def test_set_model_init(simpleacqtype, simpleacq):
    """Test that a normal peewee model init can happen."""

    class cls(acq_info_base):
        a = pw.IntegerField()
        b = pw.CharField()

    cls.set_config(simpleacqtype)
    info = cls(a=2, b="B", acq=simpleacq)
    assert info.a == 2
    assert info.b == "B"
    assert info.acq == simpleacq
