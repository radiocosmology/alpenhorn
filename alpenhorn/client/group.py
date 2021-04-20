"""Alpenhorn client interface for operations on `StorageGroup`s."""

import click
import peewee as pw

import alpenhorn.storage as st

from .connect_db import config_connect


@click.group(context_settings={'help_option_names': ['-h', '--help']})
def cli():
    """Commands operating on storage groups. Use to create, modify, and list groups."""
    pass


@cli.command()
@click.argument('group_name', metavar='GROUP')
@click.option('--notes', metavar='NOTES')
def create(group_name, notes):
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


@cli.command()
def list():
    """List known storage groups.
    """
    config_connect()

    import tabulate

    data = (
        st.StorageGroup.select(
            st.StorageGroup.name,
            st.StorageGroup.notes)
        .tuples())
    if data:
        print(tabulate.tabulate(data, headers=['Name', 'Notes']))


@cli.command()
@click.argument('group_name', metavar='GROUP')
@click.argument('new_name', metavar='NEW-NAME')
def rename(group_name, new_name):
    """Change the name of a storage GROUP to NEW-NAME."""
    config_connect()

    try:
        group = st.StorageGroup.get(name=group_name)
        try:
            st.StorageGroup.get(name=new_name)
            print('Group "%s" already exists.' % new_name)
            exit(1)
        except pw.DoesNotExist:
            group.name = new_name
            group.save()
            print('Updated.')
    except pw.DoesNotExist:
        print('Group "%s" does not exist!' % group_name)
        exit(1)


@cli.command()
@click.argument('group_name', metavar='GROUP')
@click.option('--notes', help='Value for the notes field', metavar='NOTES')
def modify(group_name, notes):
    """Change the properties of a storage GROUP."""
    config_connect()

    try:
        group = st.StorageGroup.get(name=group_name)
        if notes is not None:
            if notes == '':
                notes = None
            group.notes = notes
            group.save()
            print('Updated.')
        else:
            print('Nothing to do.')
    except pw.DoesNotExist:
        print('Group "%s" does not exist!' % group_name)
        exit(1)
