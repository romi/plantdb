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
        self.assertTrue(db.lock_path.exists())
        self.assertTrue(db.is_connected)

    def test_get_scan(self):
        db = self.get_test_db()
        scan = db.get_scan("myscan_001")  # exists
        self.assertIsInstance(scan, Scan)
        scan = db.get_scan("myscan_002")  # does not exist
        self.assertIsNone(scan)

    def test_create_scan(self):
        db = self.get_test_db()
        scan_id = "testscan_2"
        scan = db.create_scan(scan_id)
        self.assertTrue(scan.id == scan_id)
        self.assertTrue(scan.path().is_dir())

    def test_set_scan_metadata(self):
        scan = self.get_test_scan()
        md = {"test": "value"}
        scan.set_metadata(md)
        db = scan.get_db()
        scan_id = scan.id
        db.disconnect()
        db.connect()
        scan = db.get_scan(scan_id)
        self.assertTrue(scan.get_metadata('test') == md['test'])

    def test_list_scan(self):
        db = self.get_test_db()
        self.assertListEqual(db.list_scans(), ['myscan_001'])

    def test_delete_scan(self):
        db = self.get_test_db()
        scan_path = db.get_scan("myscan_001").path()
        db.delete_scan("myscan_001")

        self.assertTrue(db.path().is_dir())
        self.assertFalse(scan_path.is_dir())

    def test_get_fileset(self):
        scan = self.get_test_scan()
        fileset = scan.get_fileset("fileset_001")  # exists
        self.assertIsInstance(fileset, Fileset)
        fileset = scan.get_fileset("fileset_002")  # does not exist
        self.assertIsNone(fileset)

    def test_create_fileset(self):
        scan = self.get_test_scan()
        fs_id = "testfileset_2"
        fileset = scan.create_fileset(fs_id)
        self.assertTrue(fileset.id == fs_id)
        self.assertTrue(fileset.path().is_dir())

    def test_set_fileset_metadata(self):
        fileset = self.get_test_fileset()
        md = {"test": "value"}
        fileset.set_metadata(md)
        db = fileset.get_db()
        scan_id = fileset.get_scan().id
        fs_id = fileset.id
        db.disconnect()
        db.connect()
        fileset = db.get_scan(scan_id).get_fileset(fs_id)
        self.assertTrue(fileset.get_metadata('test') == md['test'])

    def test_list_fileset(self):
        scan = self.get_test_scan()
        self.assertListEqual(scan.list_filesets(), ['fileset_001'])

    def test_delete_fileset(self):
        scan = self.get_test_scan()
        fs_path = scan.get_fileset("fileset_001").path()
        scan.delete_fileset("fileset_001")

        self.assertTrue(scan.path().is_dir())
        self.assertIsNone(scan.get_fileset("fileset_001"))
        self.assertFalse(fs_path.is_dir())

    def test_get_file(self):
        fileset = self.get_test_fileset()
        file = fileset.get_file("test_image")  # exists
        self.assertIsInstance(file, File)

    def test_create_file(self):
        from plantdb.io import write_json
        scan = self.get_test_scan()
        fileset = scan.get_fileset("fileset_001")
        f_id = "test_file_md"
        file = fileset.create_file(f_id)
        self.assertTrue(file.id == f_id)
        self.assertIsNone(file.filename)  # Do not exist on drive before `io.write_*`
        write_json(file, {"test": "value"})
        self.assertTrue(file.path().is_file())

    def test_set_file_metadata(self):
        file = self.get_test_file()
        md = {"test": "value"}
        file.set_metadata(md)
        db = file.get_db()
        scan_id = file.get_scan().id
        fs_id = file.get_fileset().id
        f_id = file.id
        db.disconnect()
        db.connect()
        file = db.get_scan(scan_id).get_fileset(fs_id).get_file(f_id)
        self.assertTrue(file.get_metadata('test') == md['test'])

    def test_list_file(self):
        fs = self.get_test_fileset()
        self.assertListEqual(fs.list_files(), ['dummy_image', 'test_image', 'test_json'])

    def test_write_raw_file(self):
        fs = self.get_test_fileset()
        new_f = fs.create_file('file_007')
        md = {"Name": "Bond, James Bond"}  # Create an example dictionary to save as JSON
        import json
        data = json.dumps(md).encode()
        new_f.write_raw(data, 'json')
        self.assertTrue(new_f.path().is_file())

    def test_read_raw_file(self):
        fs = self.get_test_fileset()
        new_f = fs.create_file('file_007')
        md = {"Name": "Bond, James Bond"}  # Create an example dictionary to save as JSON
        import json
        data = json.dumps(md).encode()
        new_f.write_raw(data, 'json')
        file = fs.get_file('file_007')
        self.assertTrue(file.read_raw() == data)

    def test_write_file(self):
        fs = self.get_test_fileset()
        new_f = fs.create_file('file_007')
        md = {"Name": "Bond, James Bond"}  # Create an example dictionary to save as JSON
        import json
        data = json.dumps(md)
        new_f.write(data, 'json')
        self.assertTrue(new_f.path().is_file())

    def test_read_file(self):
        fs = self.get_test_fileset()
        new_f = fs.create_file('file_007')
        md = {"Name": "Bond, James Bond"}  # Create an example dictionary to save as JSON
        import json
        data = json.dumps(md)
        new_f.write(data, 'json')
        file = fs.get_file('file_007')
        self.assertTrue(file.read() == data)

    def test_delete_file(self):
        fs = self.get_test_fileset()
        f_path = fs.get_file("test_image").path()
        fs.delete_file("test_image")

        self.assertTrue(fs.path().is_dir())
        self.assertIsNone(fs.get_file("test_image"))
        self.assertFalse(f_path.is_file())

    def test_import_file(self):
        fs = self.get_test_fileset()
        file = fs.get_file('test_image')
        new_file = fs.create_file('test_image2')
        new_file.import_file(file.path())

        self.assertTrue(file.path().is_file())
        self.assertTrue(new_file.path().is_file())
        self.assertFalse(file.path() == new_file.path())
        self.assertFalse(file.path().stat == new_file.path().stat)


if __name__ == "__main__":
    unittest.main()
