"""Test ImportDetectExtension."""

import pytest

from alpenhorn.extensions import ImportDetectExtension


def _func():
    """A dummy function to pass to constructors."""
    pass


def test_detect():
    """Detect must be callable."""

    with pytest.raises(ValueError):
        ImportDetectExtension("Test", "1", detect=None)

    with pytest.raises(ValueError):
        ImportDetectExtension("Test", "1", detect=1)

    # But this is fine
    ImportDetectExtension("Test", "1", detect=_func)
