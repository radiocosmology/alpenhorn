from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
import os

import pytest

from alpenhorn import config


def merge_dict(a, b):

    c = a.copy()
    c.update(b)

    return c


def test_no_config():
    # Check that alpenhorn fails if it has no appropriate configuration

    with pytest.raises(RuntimeError) as excinfo:
        config.load_config()

    assert 'No configuration' in str(excinfo.value)


def test_config_env(fs, monkeypatch):
    # Test that we can load config from an environment variable
    fs.CreateFile('/test/from/env/test.yaml', contents='hello: test\n')

    monkeypatch.setenv('ALPENHORN_CONFIG_FILE', '/test/from/env/test.yaml')
    config.load_config()
    assert config.configdict == merge_dict(config._default_config, {'hello': 'test'})


def test_precendence(fs, monkeypatch):
    # Test the precedence of configuration imported from files is correct

    fs.CreateFile('/etc/alpenhorn/alpenhorn.conf', contents='hello: test\n')
    config.load_config()
    assert config.configdict == merge_dict(config._default_config, {'hello': 'test'})

    fs.CreateFile('/etc/xdg/alpenhorn/alpenhorn.conf', contents='hello: test2\n')
    config.load_config()
    assert config.configdict == merge_dict(config._default_config, {'hello': 'test2'})

    fs.CreateFile(os.path.expanduser('~/.config/alpenhorn/alpenhorn.conf'), contents='hello: test3\nmeh: embiggens')
    config.load_config()
    assert config.configdict == merge_dict(config._default_config, {'hello': 'test3', 'meh': 'embiggens'})

    fs.CreateFile('/test/from/env/test.yaml', contents='hello: test4\n')
    monkeypatch.setenv('ALPENHORN_CONFIG_FILE', '/test/from/env/test.yaml')
    config.load_config()
    assert config.configdict == merge_dict(config._default_config, {'hello': 'test4', 'meh': 'embiggens'})
