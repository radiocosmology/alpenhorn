"""Alpenhorn client interface for operations on `StorageGroup`s."""
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import click
import peewee as pw

import alpenhorn.storage as st

from .connect_db import config_connect


@click.command()
@click.argument('group_name', metavar='GROUP')
@click.option('--notes', metavar='NOTES')
def create_group(group_name, notes):
    """Create a storage GROUP and add to database.
    """
    config_connect()

    try:
        st.StorageGroup.get(name=group_name)
        print('Group name "%s" already exists! Try a different name!' % group_name)
        exit(1)
    except pw.DoesNotExist:
        st.StorageGroup.create(name=group_name, notes=notes)
        print('Added group "%s" to database.' % group_name)
