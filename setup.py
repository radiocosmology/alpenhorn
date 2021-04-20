#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages
from codecs import open
from os import path

import alpenhorn

here = path.abspath(path.dirname(__file__))


def strip_envmark(requires):
    # Strip out environment markers options
    return [req.split(";")[0].rstrip() for req in requires]


# Get the long description from the README file
with open(path.join(here, "README.rst"), encoding="utf-8") as f:
    long_description = f.read()

with open(path.join(here, "requirements.txt")) as f:
    requirements = strip_envmark(f.readlines())

with open(path.join(here, "test-requirements.txt")) as f:
    test_requirements = strip_envmark(f.readlines())

setup_requirements = [
    "pytest-runner",
]

setup(
    name="alpenhorn",
    version=alpenhorn.__version__,
    description="Data archive management software.",
    long_description=long_description,
    author="CHIME collaboration",
    author_email="richard@phas.ubc.ca",
    url="https://github.com/radiocosmology/alpenhorn",
    license="MIT",
    packages=find_packages(exclude=["docs", "tests"]),
    scripts=[],
    entry_points={
        "console_scripts": [
            "alpenhorn = alpenhorn.client:cli",
            "alpenhornd = alpenhorn.service:cli",
        ]
    },
    python_requires=">=3.6",
    install_requires=requirements,
    tests_require=test_requirements,
    test_suite="tests",
    setup_requires=setup_requirements,
)
