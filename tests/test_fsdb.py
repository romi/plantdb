import os
import unittest

from romidata.testing import DBTestCase


class TestFSDB(DBTestCase):
    def get_test_scan(self):
        db = self.get_test_db()
        scan = db.get_scan("testscan")
        return scan

    def get_test_fileset(self):
        scan = self.get_test_scan()
        fileset = scan.get_fileset("testfileset")
        return fileset

    def test_connect(self):
        fileset = self.get_test_fileset()
        fileset.get_file("image")
        fileset.get_file("text")

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
        file = fileset.get_file("text")
        txt = file.read()
        self.assertEqual(txt, "hello")

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
        fs.delete_file("text")

        fspath = os.path.join(fs.scan.db.basedir, fs.scan.id, fs.id)
        self.assertTrue(os.path.exists(fspath))
        self.assertIsNone(fs.get_file("text"))
        self.assertFalse(os.path.exists(os.path.join(fspath, "text.txt")))

    def test_delete_fileset(self):
        scan = self.get_test_scan()
        scan.delete_fileset("testfileset")

        scanpath = os.path.join(scan.db.basedir, scan.id)

        self.assertTrue(os.path.exists(scanpath))
        self.assertIsNone(scan.get_fileset("testfileset"))
        self.assertFalse(os.path.exists(os.path.join(scanpath, "testfileset")))

    def test_delete_scan(self):
        db = self.get_test_db()
        db.delete_scan("testscan")

        self.assertTrue(os.path.exists(db.basedir))
        self.assertFalse(os.path.exists(os.path.join(db.basedir, "testscan")))


if __name__ == "__main__":
    unittest.main()
