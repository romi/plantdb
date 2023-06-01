#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import unittest

from plantdb import FSDB
from plantdb.fsdb import File
from plantdb.fsdb import Fileset
from plantdb.fsdb import Scan
from plantdb.testing import DBTestCase


class TestFSDB(DBTestCase):

    def test_connect(self):
        db = self.get_test_db()
        self.assertIsInstance(db, FSDB)
        self.assertTrue(db.path().is_dir())

    def test_get_test_scan(self):
        db = self.get_test_db()
        scan = db.get_scan("myscan_001")  # exists
        self.assertIsInstance(scan, Scan)
        scan = db.get_scan("myscan_002")  # does not exist
        self.assertIsNone(scan)

    def test_get_test_fileset(self):
        scan = self.get_test_scan()
        fileset = scan.get_fileset("fileset_001")  # exists
        self.assertIsInstance(fileset, Fileset)
        fileset = scan.get_fileset("fileset_002")  # does not exist
        self.assertIsNone(fileset)

    def test_get_test_file(self):
        fileset = self.get_test_fileset()
        file = fileset.get_file("test_image")  # exists
        self.assertIsInstance(file, File)

    def test_create_scan(self):
        db = self.get_test_db()
        scan = db.create_scan("testscan_2")
        self.assertTrue(scan.path().is_dir())

    def test_create_fileset(self):
        scan = self.get_test_scan()
        fs = scan.create_fileset("testfileset_2")
        self.assertTrue(fs.path().is_dir())

    def test_delete_file(self):
        fs = self.get_test_fileset()
        f_path = fs.get_file("test_image").path()
        fs.delete_file("test_image")

        self.assertTrue(fs.path().is_dir())
        self.assertIsNone(fs.get_file("test_image"))
        self.assertFalse(f_path.is_file())

    def test_delete_fileset(self):
        scan = self.get_test_scan()
        fs_path = scan.get_fileset("fileset_001").path()
        scan.delete_fileset("fileset_001")

        self.assertTrue(scan.path().is_dir())
        self.assertIsNone(scan.get_fileset("fileset_001"))
        self.assertFalse(fs_path.is_dir())

    def test_delete_scan(self):
        db = self.get_test_db()
        scan_path = db.get_scan("myscan_001").path()
        db.delete_scan("myscan_001")

        self.assertTrue(db.path().is_dir())
        self.assertFalse(scan_path.is_dir())


if __name__ == "__main__":
    unittest.main()
