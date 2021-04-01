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

import tempfile
import unittest

from os.path import join, abspath
from pathlib import Path
from dirsync import sync
from plantdb import FSDB

parent_dir = Path(__file__).resolve().parents[1]
DATABASE_LOCATION = abspath(join(parent_dir, "tests", "testdata"))


class TemporaryCloneDB(object):
    """Class for doing tests on a copy of a local DB.

    Parameters
    ----------
    db_location : str
        Location of the source database

    Attributes
    ----------
    tmpdir : tempfile.TemporaryDirectory
        The temporary directory.
    """

    def __init__(self, db_location):
        self.tmpdir = tempfile.TemporaryDirectory()
        sync(db_location, self.tmpdir.name, action="sync")

    def __del__(self):
        self.tmpdir.cleanup()


class DBTestCase(unittest.TestCase):
    def __del__(self):
        try:
            self.db.disconnect()
        except:
            return

    def get_test_db(self, db_path=DATABASE_LOCATION):
        # print(f"Example database location: {DATABASE_LOCATION}")
        self.tmpclone = TemporaryCloneDB(db_path)
        self.db = FSDB(self.tmpclone.tmpdir.name)
        self.db.connect()
        return self.db

    def get_test_scan(self):
        db = self.get_test_db()
        scan = db.get_scan("testscan")
        return scan

    def get_test_fileset(self):
        scan = self.get_test_scan()
        fileset = scan.get_fileset("testfileset")
        return fileset
