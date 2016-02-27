from setuptools import setup, find_packages

setup(
    name='alpenhorn',
    version=0.1,

    packages=find_packages(),

    install_requires=['ch_util', 'h5py', 'MySQL_python', 'peewee >= 2.7.0',
                      'bitshuffle', 'netifaces', 'PyYAML',
                      'ConcurrentLogHandler', 'Click'],
    entry_points="""
        [console_scripts]
        alpenhorn=alpenhorn.client:cli
        alpenhornd=alpenhorn.service.cli
    """,

    # metadata for upload to PyPI
    author="CHIME collaboration",
    author_email="richard@phas.ubc.ca",
    description="Data archive management software.",
    license="GPL v3.0",
    url="https://bitbucket.org/chime/alpenhorn"
)
