#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import subprocess
import unittest
from pathlib import Path

from plantdb.commons.testing import DummyDBTestCase

TESTS_ROOT = Path(__file__).parent  # path to the 'plantdb/tests/' directory


class TestFSDBDummy(DummyDBTestCase):

    def test_import(self):
        """
        Tests the successful import of images and associated metadata into a test database.

        This function exercises various components of the system involved in importing image
        data, validating that the operation is performed correctly. This includes checks on
        the database connectivity, the presence and structure of the imported files, as well as
        the correctness of metadata associated with these files.

        The following specific steps are tested:
        - Disconnecting and reconnecting to the test database
        - Importing image files and metadata from a specified path using an external command
        - Validating the database directory and the existence of the imported scan
        - Ensuring that the fileset and a test file exist after import
        - Confirming that metadata associated with the imported files matches the original data
        """
        db = self.get_test_db()
        db.dummy = False  # to avoid clean up by 'disconnect' method
        db.disconnect()
        copy_path = TESTS_ROOT / "testdata" / "testscan" / "testfileset"
        out = subprocess.run(["fsdb_import_images", str(db.path()), copy_path,
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

        db.dummy = True
        db.disconnect()


if __name__ == "__main__":
    unittest.main()
