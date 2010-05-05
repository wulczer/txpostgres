#!/usr/bin/python

"""txpostgres
==========

A Twisted wrapper for asynchronous PostgreSQL connections.

Based on the interface exposed from the native Postgres C library by the Python
psycopg2 driver.

Can be used as a drop-in replacement for Twisted's adbapi module when working
with PostgreSQL. The only part that does not provide 100% compatibility is
connection pooling, although pooling provided by txpostgres is very similar to
the one Twisted adbapi offers.
"""

import os
import sys

from distutils.core import setup

# # Make sure we import txpostgres from the current directory, not some version
# # that could have been installed earlier on the system
# curdir = sys.path[0]
# sys.path.insert(0, os.path.join(curdir, 'txpostgres'))

# print sys.path
from txpostgres import __versionstr__

# Revert sys.path to the previous state
sys.path = sys.path[1:]

setup(name="txpostgres",
      version=__versionstr__,
      description="Twisted wrapper for asynchronous PostgreSQL connections",
      long_description=__doc__,
      classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Framework :: Twisted",
        "Intended Audience :: Developers",
        "Intended Audience :: End Users/Desktop",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
        ],
      platforms=["any"],
      license="MIT",
      author="Jan Urbanski",
      maintainer="Jan Urbanski",
      author_email="wulczer@wulczer.org",
      maintainer_email="wulczer@wulczer.org",
      url="http://wulczer.org/",
#      package_dir={'': 'lib'},
      packages=["txpostgres"])
