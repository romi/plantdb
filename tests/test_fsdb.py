import unittest
import tempfile
import os

from romidata import FSDB
from romidata.testing import DBTestCase

import numpy as np
import imageio

DATABASE_LOCATION = "testdata"

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

    def test_read_image(self):
        fileset = self.get_test_fileset()
        file = fileset.get_file("image")
        img = file.read()

        assert(img[0,0] == 255)
        assert(img[0,1] == 0)
        assert(img[1,0] == 0)
        assert(img[1,1] == 255)

    def test_read_text(self):
        fileset = self.get_test_fileset()
        file = fileset.get_file("text")
        txt = file.read()

        assert(txt == "hello")

    def test_create_scan(self):
        db = self.get_test_db()
        scan = db.create_scan("testscan_2")

        assert(os.path.isdir(os.path.join(db.basedir, "testscan_2")))

    def test_create_fileset(self):
        scan = self.get_test_scan()
        fs = scan.create_fileset("testfileset_2")

        assert(os.path.isdir(os.path.join(scan.db.basedir, scan.id, "testfileset_2")))

    def test_write_image(self):
        fs = self.get_test_fileset()
        img = np.zeros((1,1), dtype=np.uint8)

        f = fs.create_file("test_image_png", "png")
        img_path = os.path.join(fs.scan.db.basedir, fs.scan.id, fs.id, "test_image_png.png")
        f.write(img)
        assert(os.path.exists(img_path))
        img_read = imageio.imread(img_path)
        assert(img_read[0,0] == 0)

        f = fs.create_file("test_image_jpg", "jpg")
        img_path = os.path.join(fs.scan.db.basedir, fs.scan.id, fs.id, "test_image_jpg.jpg")
        f.write(img)
        assert(os.path.exists(img_path))
        img_read = imageio.imread(img_path)
        assert(img_read[0,0] == 0)

    def test_write_text(self):
        fs = self.get_test_fileset()
        text = "hello"

        f = fs.create_file("test_text", "txt")
        f_path = os.path.join(fs.scan.db.basedir, fs.scan.id, fs.id, "test_text.txt")
        f.write(text)

        assert(os.path.exists(f_path))
        with open(f_path, 'r') as f_read:
            assert(f_read.read() == "hello")

    def test_delete_file(self):
        fs = self.get_test_fileset()
        fs.delete_file("text")

        fspath = os.path.join(fs.scan.db.basedir, fs.scan.id, fs.id)

        assert(os.path.exists(fspath))
        assert(fs.get_file("text") is None)
        assert(not os.path.exists(os.path.join(fspath, "text.txt")))

    def test_delete_fileset(self):
        scan = self.get_test_scan()
        scan.delete_fileset("testfileset")

        scanpath = os.path.join(scan.db.basedir, scan.id)

        assert(os.path.exists(scanpath))
        assert(scan.get_fileset("testfileset") is None)
        assert(not os.path.exists(os.path.join(scanpath, "testfileset")))

    def test_delete_scan(self):
        db = self.get_test_db()
        db.delete_scan("testscan")

        assert(os.path.exists(db.basedir))
        assert(not os.path.exists(os.path.join(db.basedir, "testscan")))

if __name__ == "__main__":
    unittest.main()



