import os
import unittest

from plantdb import io
from plantdb.fsdb import File
from plantdb.fsdb import Scan
from plantdb.fsdb import Fileset
from plantdb.testing import DBTestCase


class TestFSDB(DBTestCase):

    def test_connect(self):
        db = self.get_test_db()

    def test_get_test_scan(self):
        db = self.get_test_db()
        scan = db.get_scan("myscan_001")  # exists
        self.assertIsInstance(scan, Scan)
        scan = db.get_scan("myscan_002")  # does not exists
        self.assertIsNone(scan)

    def test_get_test_fileset(self):
        scan = self.get_test_scan()
        fileset = scan.get_fileset("fileset_001")  # exists
        self.assertIsInstance(fileset, Fileset)
        fileset = scan.get_fileset("fileset_002")  # does not exists
        self.assertIsNone(fileset)

    def test_get_test_file(self):
        fileset = self.get_test_fileset()
        file = fileset.get_file("test_image")  # exists
        self.assertIsInstance(file, File)

    def test_create_scan(self):
        db = self.get_test_db()
        scan = db.create_scan("testscan_2")
        self.assertTrue(os.path.isdir(os.path.join(db.basedir, "testscan_2")))

    def test_create_fileset(self):
        scan = self.get_test_scan()
        fs = scan.create_fileset("testfileset_2")
        self.assertTrue(os.path.isdir(os.path.join(scan.db.basedir, scan.id, "testfileset_2")))

    def test_read_text(self):
        fileset = self.get_test_fileset()
        file = fileset.get_file("test_json")
        txt = io.read_json(file)
        self.assertTrue(txt['Who you gonna call?'] == "Ghostbuster")

    def test_write_text(self):
        fs = self.get_test_fileset()
        text = "hello"

        f = fs.create_file("test_text")
        f_path = os.path.join(fs.scan.db.basedir, fs.scan.id, fs.id, "test_text.txt")
        f.write(text, "txt")

        self.assertTrue(os.path.exists(f_path))
        with open(f_path, 'r') as f_read:
            self.assertEqual(f_read.read(), "hello")

    def test_delete_file(self):
        fs = self.get_test_fileset()
        fs.delete_file("test_image")

        fspath = os.path.join(fs.scan.db.basedir, fs.scan.id, fs.id)
        self.assertTrue(os.path.exists(fspath))
        self.assertIsNone(fs.get_file("test_image"))
        self.assertFalse(os.path.exists(os.path.join(fspath, "test_image.png")))

    def test_delete_fileset(self):
        scan = self.get_test_scan()
        scan.delete_fileset("fileset_001")

        scanpath = os.path.join(scan.db.basedir, scan.id)

        self.assertTrue(os.path.exists(scanpath))
        self.assertIsNone(scan.get_fileset("fileset_001"))
        self.assertFalse(os.path.exists(os.path.join(scanpath, "fileset_001")))

    def test_delete_scan(self):
        db = self.get_test_db()
        db.delete_scan("myscan_001")

        self.assertTrue(os.path.exists(db.basedir))
        self.assertFalse(os.path.exists(os.path.join(db.basedir, "myscan_001")))


if __name__ == "__main__":
    unittest.main()
