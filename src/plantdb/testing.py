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

import unittest

from plantdb.fsdb import dummy_db


class DBTestCase(unittest.TestCase):

    def setUp(self):
        self.db = dummy_db(with_scan=True, with_fileset=True, with_file=True)

    def tearDown(self):
        try:
            self.db.disconnect()
        except:
            return
        from shutil import rmtree
        rmtree(self.db.basedir, ignore_errors=True)

    def get_test_db(self):
        self.db.connect()
        return self.db

    def get_test_scan(self):
        db = self.get_test_db()
        scan = db.get_scan("myscan_001")
        return scan

    def get_test_fileset(self):
        scan = self.get_test_scan()
        fileset = scan.get_fileset("fileset_001")
        return fileset

    def get_test_file(self):
        fileset = self.get_test_fileset()
        file = fileset.get_file("test_image")
        return file
