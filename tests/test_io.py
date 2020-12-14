import os
import unittest

import numpy as np
from romidata import io
from romidata.testing import DBTestCase


class TestIO(DBTestCase):
    """TODO : tests for all other IO...
    """

    def _test_write_file(self, obj, ext):
        fs = self.get_test_fileset()
        fname = f"test_{ext}"
        f = fs.create_file(fname)
        if ext == "json":
            io.write_json(f, obj, ext)
        elif ext == "toml":
            io.write_toml(f, obj, ext)
        elif ext in ("jpg", "png"):
            io.write_image(f, obj, ext)
        elif ext == "npz":
            io.write_npz(f, obj)
        elif ext == "volume":
            io.write_volume(f, obj)
            ext = "npz"
        else:
            raise ValueError(f"Unknown extension {ext}!")
        fpath = os.path.join(fs.scan.db.basedir, fs.scan.id, fs.id, fname + f".{ext}")
        self.assertTrue(os.path.exists(fpath), msg=f"Could not find: '{fpath}'!")
        return fpath, obj, ext

    def _test_read_file(self, fpath, obj, ext):
        if ext == "json":
            io_obj = io.read_json(fpath)
        elif ext == "toml":
            io_obj = io.read_toml(fpath)
        elif ext in ("jpg", "png"):
            io_obj = io.read_image(fpath)
        elif ext == "volume":
            io_obj = io.read_volume(fpath)
        elif ext == "npz":
            io_obj = io.read_npz(fpath)
        else:
            raise ValueError(f"Unknown extension {ext}!")
        self.assertEqual(io_obj, obj)

    def _test_io_file(self, obj, ext):
        fpath, obj, ext = self._test_write_file(obj, ext)
        self._test_read_file(fpath, obj, ext)

    def _test_write_image(self, ext):
        img = np.zeros((1, 1), dtype=np.uint8)
        self._test_write_file(img, ext)

    def test_write_image_jpg(self):
        self._test_write_image("jpg")

    def test_write_image_png(self):
        self._test_write_image("png")

    def test_write_json(self):
        md = {"test": {"json": 1}}
        self._test_write_file(md, 'json')

    def test_write_toml(self):
        md = {"test": {"toml": 1}}
        self._test_write_file(md, 'toml')

    def test_write_vol(self):
        vol = np.random.rand(50, 50, 200)
        self._test_write_file(vol, 'volume')

    def test_write_npz(self):
        vol = np.random.rand(50, 50, 200)
        self._test_write_file({'arr': vol}, 'npz')

    def test_io_image(self):
        self.test_write_image_jpg()

    def test_read_image(self):
        fileset = self.get_test_fileset()
        file = fileset.get_file("image")
        img = io.read_image(file)

        self.assertEqual(img[0, 0], 255)
        self.assertEqual(img[0, 1], 0)
        self.assertEqual(img[1, 0], 0)
        self.assertEqual(img[1, 1], 255)


if __name__ == "__main__":
    unittest.main()
