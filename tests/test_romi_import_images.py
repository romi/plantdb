#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import subprocess

from plantdb.testing import DBTestCase


class TestFSDB(DBTestCase):

    def test_import(self):
        db = self.get_test_db()
        db.disconnect()
        out = subprocess.run(["romi_import_images", str(db.path()), "testdata/testscan/testfileset/",
                              "--name", "test_import_img",
                              "--metadata", "testdata/testscan/files.json"], capture_output=True)
        self.assertTrue(out.returncode == 0)
        db.connect()
        # Test database path:
        self.assertTrue(db.path().is_dir())
        # Test scan exist and its path exists:
        scan = db.get_scan('test_import_img')
        self.assertIsNotNone(scan)
        self.assertTrue(scan.path().is_dir())
        # Test 'images' fileset exist and its path exists:
        fs = scan.get_fileset('images')
        self.assertIsNotNone(fs)
        self.assertTrue(fs.path().is_dir())
        # Test 'image.png' file exist and its path exists:
        f = fs.get_file('image')
        self.assertIsNotNone(f)
        self.assertTrue(f.path().is_file())
        # Test metadata exists
        self.assertIsNotNone(fs.metadata)
        # Compare to original JSON
        with open(scan.path()/"metadata"/"images.json") as json_f:
            md_json = json.load(json_f)
        self.assertDictEqual(fs.metadata, md_json)