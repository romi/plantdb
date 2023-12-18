#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import subprocess
import unittest
from pathlib import Path

from plantdb.testing import DummyDBTestCase

TESTS_ROOT = Path(__file__).parent  # path to the 'plantdb/tests/' directory


class TestFSDBDummy(DummyDBTestCase):

    def test_import(self):
        db = self.get_test_db()
        db.disconnect()
        copy_path = TESTS_ROOT / "testdata" / "testscan" / "testfileset"
        out = subprocess.run(["romi_import_images", str(db.path()), copy_path,
                              "--name", "test_import_img",
                              "--metadata", TESTS_ROOT / "testdata" / "testscan" / "files.json"], capture_output=True)
        rcode = out.returncode
        if rcode != 0:
            print(f"Return code: {rcode}")
            print(f"Captured stdout: {out.stdout}")
            print(f"Captured stderr: {out.stderr}")
        self.assertTrue(rcode == 0, msg=f"Return code is {rcode}: {out.stderr}")
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
        with open(scan.path() / "metadata" / "images.json") as json_f:
            md_json = json.load(json_f)
        self.assertDictEqual(fs.metadata, md_json)


if __name__ == "__main__":
    unittest.main()
