#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import unittest

import numpy as np

from plantdb import io
from plantdb.io import fsdbfile_from_local_file
from plantdb.testing import DBTestCase

rng = np.random.default_rng()

class TestIO(DBTestCase):
    """Test the IO module."""

    def _test_write_file(self, obj, ext, **kwargs):
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
            io.write_volume(f, obj, **kwargs)
            ext = "npz"
        else:
            raise ValueError(f"Unknown extension {ext}!")
        fpath = f.path()
        self.assertTrue(fpath.is_file(), msg=f"Could not find: '{fpath}'!")
        return fpath, obj, ext

    def _test_read_file(self, fpath, ref_obj, ext):
        dbfile = fsdbfile_from_local_file(fpath)
        if ext == "json":
            io_obj = io.read_json(dbfile)
        elif ext == "toml":
            io_obj = io.read_toml(dbfile)
        elif ext in ("jpg", "png"):
            io_obj = io.read_image(dbfile)
        elif ext == "volume":
            io_obj = io.read_volume(dbfile)
        elif ext == "npz":
            io_obj = io.read_npz(dbfile)
        else:
            raise ValueError(f"Unknown extension {ext}!")

        if isinstance(io_obj, np.ndarray):
            np.testing.assert_array_equal(io_obj, ref_obj)
        elif isinstance(io_obj, dict):
            first_key = list(io_obj.keys())[0]
            if isinstance(io_obj[first_key], np.ndarray):
                # - NPZ case:
                # Check keys are the same:
                self.assertEqual(io_obj.keys(), ref_obj.keys())
                # Check arrays are the same:
                for key in io_obj.keys():
                    np.testing.assert_array_equal(io_obj[key], ref_obj[key])
            else:
                self.assertDictEqual(io_obj, ref_obj)
        else:
            self.assertEqual(io_obj, ref_obj)

    def _test_io_file(self, obj, ext):
        fpath, obj, ext = self._test_write_file(obj, ext)
        return self._test_read_file(fpath, obj, ext)

    def _test_write_image(self, ext):
        img = np.zeros((1, 1), dtype=np.uint8)
        return self._test_write_file(img, ext)

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
        vol = rng.random((50, 50, 200))
        self._test_write_file(vol, 'volume')

    def test_write_npz(self):
        vol = rng.random((50, 50, 200))
        self._test_write_file({'arr': vol}, 'npz')

    def test_read_image_jpg(self):
        fpath, obj, _ = self._test_write_image("jpg")
        self._test_read_file(fpath, obj, 'jpg')

    def test_read_image_png(self):
        fpath, obj, _ = self._test_write_image("png")
        self._test_read_file(fpath, obj, 'png')

    def test_read_json(self):
        md = {"test": {"json": 1}}
        fpath, obj, _ = self._test_write_file(md, 'json')
        self._test_read_file(fpath, obj, 'json')

    def test_read_toml(self):
        md = {"test": {"toml": 1}}
        fpath, obj, _ = self._test_write_file(md, 'toml')
        self._test_read_file(fpath, obj, 'toml')

    def test_read_vol(self):
        vol = rng.random((50, 50, 200))
        fpath, obj, _ = self._test_write_file(vol, 'volume')
        self._test_read_file(fpath, obj, 'volume')
        fpath, obj, _ = self._test_write_file(vol, 'volume', compress=False)
        self._test_read_file(fpath, obj, 'volume')

    def test_read_npz(self):
        npz = {f"{i}": rng.random((10, 10, 3)) for i in range(5)}
        fpath, obj, _ = self._test_write_file(npz, 'npz')
        self._test_read_file(fpath, obj, 'npz')


if __name__ == "__main__":
    unittest.main()
