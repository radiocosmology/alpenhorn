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
    fs.create_file('/test/from/env/test.yaml', contents='hello: test\n')

    monkeypatch.setenv('ALPENHORN_CONFIG_FILE', '/test/from/env/test.yaml')
    config.load_config()
    assert config.config == merge_dict(config._default_config, {'hello': 'test'})


def test_precendence(fs, monkeypatch):
    # Test the precedence of configuration imported from files is correct

    fs.create_file('/etc/alpenhorn/alpenhorn.conf', contents='hello: test\n')
    config.load_config()
    assert config.config == merge_dict(config._default_config, {'hello': 'test'})

    fs.create_file('/etc/xdg/alpenhorn/alpenhorn.conf', contents='hello: test2\n')
    config.load_config()
    assert config.config == merge_dict(config._default_config, {'hello': 'test2'})

    fs.create_file(os.path.expanduser('~/.config/alpenhorn/alpenhorn.conf'), contents='hello: test3\nmeh: embiggens')
    config.load_config()
    assert config.config == merge_dict(config._default_config, {'hello': 'test3', 'meh': 'embiggens'})

    fs.create_file('/test/from/env/test.yaml', contents='hello: test4\n')
    monkeypatch.setenv('ALPENHORN_CONFIG_FILE', '/test/from/env/test.yaml')
    config.load_config()
    assert config.config == merge_dict(config._default_config, {'hello': 'test4', 'meh': 'embiggens'})


def test_merge():
    # Test the dictionary merging algorithm used by the configuration

    conf_a = {
        'dict1': {
            'dict2': {
                'scalar1': 'a',
                'scalar2': 'a'
            },
            'scalar3': 'a',
            'list1': ['a']
        },
        'dict_or_list': {
            'scalar4': 'a'
        }
    }

    conf_b = {
        'dict1': {
            'dict2': {
                'scalar1': 'b',
                'scalar4': 'b'
            },
            'scalar3': 'b',
            'list1': ['b']
        },
        'dict_or_list': ['b']
    }

    test_c = config.merge_dict_tree(conf_a, conf_b)

    # The correctly merged output
    conf_c = {
        'dict1': {
            'dict2': {
                'scalar1': 'b',
                'scalar2': 'a',
                'scalar4': 'b'
            },
            'scalar3': 'b',
            'list1': ['a', 'b']
        },
        'dict_or_list': ['b']
    }

    assert conf_c == test_c
