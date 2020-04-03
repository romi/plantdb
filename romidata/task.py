# -*- python -*-
# -*- coding: utf-8 -*-
# 
# romidata - Data handling tools for the ROMI project
# 
# Copyright (C) 2018-2019 Sony Computer Science Laboratories
# Authors: D. Colliaux, T. Wintz, P. Hanappe
# 
# This file is part of romidata.
# 
# romidata is free software: you can redistribute it
# and/or modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation, either
# version 3 of the License, or (at your option) any later version.
# 
# romidata is distributed in the hope that it will be
# useful, but WITHOUT ANY WARRANTY; without even the implied
# warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU General Public License for more details.
# 
# You should have received a copy of the GNU Lesser General Public
# License along with romidata.  If not, see
# <https://www.gnu.org/licenses/>.
# ------------------------------------------------------------------------------

"""
romidata.task
=============

ROMI Luigi Tasks

This module implements subclasses of ``luigi.Config``, ``luigi.Target`` and ``luigi.Tasks``.
The goal is to have luigi tasks work seamlessly with the database API implemented in ``romidata.db``.

A ``FilesetTarget`` is a luigi target corresponding to a ``Fileset`` object.

A ``RomiTask`` must implement two methods : ``run`` and ``requires``.

To check for a task completeness, the fileset existence is checked as well as all it's dependencies.
"""
from os import path
import luigi

from romidata import FSDB

db = None


class ScanParameter(luigi.Parameter):
    """ Register a luigi `Parameter` object to access `fsdb.Scan` class.
    Override the default implementation methods `parse` & `serialize`

    Notes
    -----
    The `parse` method connect to the given path to an `FSDB` database.

    """
    def parse(self, x):
        """Parse an individual value from the input.
        Override the default implementation method for specialized parsing.

        Parameters
        ----------
        x : str
            the value to parse, here the path to an `FSDB` database.

        Returns
        -------
        the parsed value.

        """
        global db
        path = x.rstrip('/')
        path = path.split('/')
        db_path = '/'.join(path[:-1])
        scan_id = path[-1]
        if db is None:  # TODO: cannot change DB during run..
            db = FSDB(db_path)
            db.connect()
        scan = db.get_scan(scan_id)
        if scan is None:
            scan = db.create_scan(scan_id)
        return scan

    def serialize(self, scan):
        """Opposite of `parse()`.
        Converts the value `scan` to a string.

        Parameters
        ----------
        scan : str
            the value to serialize, here an `FSDB` database.

        """
        db_path = scan.db.basedir
        scan_id = scan.id
        return '/'.join([db_path, scan_id])


class DatabaseConfig(luigi.Config):
    """Configuration for the database."""
    scan = ScanParameter()


class FilesetTarget(luigi.Target):
    """Implementation of a luigi ``Target`` for the romidata DB API.

    Attributes
    ----------
    db : romidata.fsdb.FSDB
        database object
    scan : romidata.fsdb.Scan
        scan dataset in which the target is
    fileset_id : str
        id if the target fileset

    """

    def __init__(self, scan, fileset_id):
        """
        Parameters
        ----------
        scan : romidata.fsdb.Scan
            scan where the fileset is located
        fileset_id : str
            id of the target fileset
        """
        self.scan = scan
        self.db = scan.db
        self.fileset_id = fileset_id

    def create(self):
        """Creates a target by creating the fileset using the ``romidata`` DB API.

        Returns
        -------
        romidata.fsdb.Fileset
            The created `Fileset` with given `fileset_id` to contructor.

        """
        return self.scan.create_fileset(self.fileset_id)

    def exists(self):
        """A target exists if the associated fileset exists and is not empty.

        Returns
        -------
        bool
            ``True`` if the target exists, else ``False``.
        """
        fs = self.scan.get_fileset(self.fileset_id)
        return fs is not None and len(fs.get_files()) > 0

    def get(self, create=True):
        """Returns the corresponding fileset object.

        Parameters
        ----------
        create : bool
            create the fileset if it does not exist in the database. (default is `True`)

        Returns
        -------
        romidata.fsdb.Fileset
            Get the `Fileset` corresponding to the given `fileset_id` to
            contructor.

        """
        return self.scan.get_fileset(self.fileset_id, create=create)
