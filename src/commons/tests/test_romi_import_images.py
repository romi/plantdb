#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import subprocess
import tempfile
import unittest

from plantdb.commons.testing import DummyDBTestCase


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
        # Use the shared dummy dataset (from `plantdb.commons.test_database`) as the import source
        src_fs = self.get_test_fileset()
        copy_path = src_fs.path()
        # Reuse the source fileset metadata as the metadata to import
        md = {k: v for k, v in src_fs.metadata.items()
              if k not in ('owner', 'created', 'created_by', 'last_modified')}
        with tempfile.NamedTemporaryFile('w', suffix='.json', delete=False) as f:
            json.dump(md, f)
            md_path = f.name

        db = self.get_test_db()
        db._is_dummy = False  # to avoid clean up by 'disconnect' method
        db.disconnect()
        cmd = ["fsdb_import_images", str(db.path()), str(copy_path),
                              "--name", "test_import_img",
                              "--metadata", md_path,
                              "--no-auth"]
        print("Calling: " + ' '.join(map(str, cmd)))
        out = subprocess.run(cmd, capture_output=True)
        rcode = out.returncode
        if rcode != 0:
            print(f"Return code: {rcode}")
            print(f"Captured stdout: {out.stdout.decode()}")
            print(f"Captured stderr: {out.stderr.decode()}")
        self.assertTrue(rcode == 0, msg=f"Return code is {rcode}: {out.stderr}")
        db.connect()
        # Test database path:
        self.assertTrue(db.path().is_dir())
        # Test that the scan exists and its path exists:
        scan = db.get_scan('test_import_img')
        self.assertIsNotNone(scan)
        self.assertTrue(scan.path().is_dir())
        # Test 'images' fileset exist and its path exists:
        fs = scan.get_fileset('images')
        self.assertIsNotNone(fs)
        self.assertTrue(fs.path().is_dir())
        # Test 'test_image.png' file exist and its path exists:
        f = fs.get_file('test_image')
        self.assertIsNotNone(f)
        self.assertTrue(f.path().is_file())
        # Test metadata exists
        self.assertIsNotNone(fs.metadata)
        # Compare to the imported metadata
        self.assertTrue(md.items() <= fs.metadata.items())

        db._is_dummy = True
        db.disconnect()


if __name__ == "__main__":
    unittest.main()
