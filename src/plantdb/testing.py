#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# plantdb - Data handling tools for the ROMI project
#
# Copyright (C) 2018-2019 Sony Computer Science Laboratories
# Authors: D. Colliaux, T. Wintz, P. Hanappe
#
# This file is part of plantdb.
#
# plantdb is free software: you can redistribute it
# and/or modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation, either
# version 3 of the License, or (at your option) any later version.
#
# plantdb is distributed in the hope that it will be
# useful, but WITHOUT ANY WARRANTY; without even the implied
# warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with plantdb.  If not, see
# <https://www.gnu.org/licenses/>.
# ------------------------------------------------------------------------------
import shutil
import tempfile
import unittest
from pathlib import Path

from plantdb.fsdb import FSDB
from plantdb.fsdb import dummy_db


class TemporaryCloneDB(object):
    """Class for doing tests on a copy of a local DB.

    Parameters
    ----------
    db_location : str
        Location of the source database to clone in the temporary folder.

    Attributes
    ----------
    tmpdir : tempfile.TemporaryDirectory
        The temporary directory.
    """

    def __init__(self, db_location):
        self.tmpdir = tempfile.TemporaryDirectory()
        shutil.copytree(db_location, self.tmpdir.name)

    def __del__(self):
        try:
            self.tmpdir.cleanup()
        except:
            return


class DummyDBTestCase(unittest.TestCase):
    """A dummy test database.

    Attributes
    ----------
    db : plantdb.fsdb.FSDB
        The temporary directory.
    tmpclone : plantdb.testing.TemporaryCloneDB
        A local temporary copy of a dummy test database.
    """

    def setUp(self):
        """Set up a dummy database with fake scan, fileset & files."""
        self.db = dummy_db(with_scan=True, with_fileset=True, with_file=True)
        self.tmpclone = None

    def tearDown(self):
        """Clean up after test."""
        try:
            self.db.disconnect()
        except:
            return
        from shutil import rmtree
        rmtree(self.db.path(), ignore_errors=True)

    def get_test_db(self, db_path=None):
        """Return the test ``FSDB`` database.

        Parameters
        ----------
        db_path : str, optional
            If `None` (default), return the dummy database.
            Else, should be the location of the source database to clone in the temporary folder.

        Returns
        -------
        plantdb.FSDB
            The database to test.
        """
        if db_path is not None:
            self.tmpclone = TemporaryCloneDB(db_path)
            self.db = FSDB(self.tmpclone.tmpdir.name)

        self.db.connect()
        return self.db

    def get_test_scan(self):
        """Return the default test ``Scan`` object named 'myscan_001'.

        Returns
        -------
        plantdb.Scan
            The default scan instance to test.
        """
        db = self.get_test_db()
        scan = db.get_scan("myscan_001")
        return scan

    def get_test_fileset(self):
        """Return the default test ``Fileset`` object named 'fileset_001'.

        Returns
        -------
        plantdb.Scan
            The default fileset instance to test.
        """
        scan = self.get_test_scan()
        fileset = scan.get_fileset("fileset_001")
        return fileset

    def get_test_image_file(self):
        """Return the default test ``File`` object named 'test_image'.

        Returns
        -------
        plantdb.File
            The default image file instance to test.
        """
        fileset = self.get_test_fileset()
        file = fileset.get_file("test_image")
        return file
