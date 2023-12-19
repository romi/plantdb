#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import unittest

import numpy as np
import open3d as o3d

from plantdb import io
from plantdb.io import fsdb_file_from_local_file
from plantdb.testing import DummyDBTestCase
from plantdb.testing import FSDBTestCase

rng = np.random.default_rng()


class TestIODummy(DummyDBTestCase):
    """Test the IO module with dummy data."""

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
        elif ext == "pointcloud":
            ext = "ply"
            io.write_point_cloud(f, obj, ext)
        elif ext == "mesh":
            ext = "ply"
            io.write_triangle_mesh(f, obj, ext)
        else:
            raise ValueError(f"Unknown extension {ext}!")
        fpath = f.path()
        self.assertTrue(fpath.is_file(), msg=f"Could not find: '{fpath}'!")
        return fpath, obj, ext

    def _test_read_file(self, fpath, ref_obj, ext):
        dbfile = fsdb_file_from_local_file(fpath)
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
        elif ext == "pointcloud":
            ext = "ply"
            io_obj = io.read_point_cloud(dbfile, ext=ext)
        elif ext == "mesh":
            ext = "ply"
            io_obj = io.read_triangle_mesh(dbfile, ext=ext)
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
        elif isinstance(io_obj, o3d.geometry.PointCloud):
            # Check the points of the pointcloud are the same:
            np.testing.assert_array_equal(np.asarray(io_obj.points), np.asarray(ref_obj.points))
        elif isinstance(io_obj, o3d.geometry.TriangleMesh):
            # Check the vertices of the triangular mesh are the same:
            np.testing.assert_array_equal(np.asarray(io_obj.vertices), np.asarray(ref_obj.vertices))
            # Check the points of the triangular mesh are the same:
            np.testing.assert_array_equal(np.asarray(io_obj.triangles), np.asarray(ref_obj.triangles))
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

    def test_read_image_jpg(self):
        fpath, obj, _ = self._test_write_image("jpg")
        self._test_read_file(fpath, obj, 'jpg')

    def test_write_image_png(self):
        self._test_write_image("png")

    def test_read_image_png(self):
        fpath, obj, _ = self._test_write_image("png")
        self._test_read_file(fpath, obj, 'png')

    def test_write_json(self):
        md = {"test": {"json": 1}}
        self._test_write_file(md, 'json')

    def test_read_json(self):
        md = {"test": {"json": 1}}
        fpath, obj, _ = self._test_write_file(md, 'json')
        self._test_read_file(fpath, obj, 'json')

    def test_write_toml(self):
        md = {"test": {"toml": 1}}
        self._test_write_file(md, 'toml')

    def test_read_toml(self):
        md = {"test": {"toml": 1}}
        fpath, obj, _ = self._test_write_file(md, 'toml')
        self._test_read_file(fpath, obj, 'toml')

    def test_write_vol(self):
        vol = rng.random((50, 50, 200))
        self._test_write_file(vol, 'volume')
        self._test_write_file(vol, 'volume', compress=False)

    def test_read_vol(self):
        vol = rng.random((50, 50, 200))
        fpath, obj, _ = self._test_write_file(vol, 'volume')
        self._test_read_file(fpath, obj, 'volume')
        fpath, obj, _ = self._test_write_file(vol, 'volume', compress=False)
        self._test_read_file(fpath, obj, 'volume')

    def test_write_npz(self):
        npz = {f"{i}": rng.random((10, 10, 3)) for i in range(5)}
        self._test_write_file(npz, 'npz')

    def test_read_npz(self):
        npz = {f"{i}": rng.random((10, 10, 3)) for i in range(5)}
        fpath, obj, _ = self._test_write_file(npz, 'npz')
        self._test_read_file(fpath, obj, 'npz')

    def test_write_pcd(self):
        pcd = o3d.geometry.PointCloud()
        pcd.points = o3d.utility.Vector3dVector(np.array([[1, 2, 3]]))
        self._test_write_file(pcd, "pointcloud")

    def test_read_pcd(self):
        pcd = o3d.geometry.PointCloud()
        pcd.points = o3d.utility.Vector3dVector(np.array([[1, 2, 3]]))
        fpath, obj, _ = self._test_write_file(pcd, "pointcloud")
        self._test_read_file(fpath, obj, "pointcloud")

    def test_write_mesh(self):
        mesh = o3d.geometry.TriangleMesh.create_arrow(cylinder_radius=1.0, cone_radius=1.5, cylinder_height=5.0,
                                                      cone_height=4.0, resolution=20, cylinder_split=4, cone_split=1)
        self._test_write_file(mesh, "mesh")

    def test_read_mesh(self):
        mesh = o3d.geometry.TriangleMesh.create_arrow(cylinder_radius=1.0, cone_radius=1.5, cylinder_height=5.0,
                                                      cone_height=4.0, resolution=20, cylinder_split=4, cone_split=1)
        fpath, obj, _ = self._test_write_file(mesh, 'mesh')
        self._test_read_file(fpath, obj, "mesh")


class TestIOFSDB(FSDBTestCase):
    """Test the IO module with real data."""

    def test_read_images(self):
        fs = self.get_task_fileset('images')
        img = io.read_image(fs.get_file('00000_rgb'))
        self.assertTrue(isinstance(img, np.ndarray))
        self.assertTrue(img.size > 0)  # Non-empty
        self.assertTrue(len(img.shape) == 3)  # RGB 2D array
        self.assertTrue(img.shape[2] == 3)  # RGB 2D array

    def test_read_volume(self):
        fs = self.get_task_fileset('Voxels')
        vol = io.read_volume(fs.get_file('Voxels'), ext='tiff')
        self.assertTrue(isinstance(vol, np.ndarray))
        self.assertTrue(vol.size > 0)  # Non-empty
        self.assertTrue(len(vol.shape) == 3)  # 3D array

    def test_read_pcd(self):
        fs = self.get_task_fileset('PointCloud')
        pcd = io.read_point_cloud(fs.get_file('PointCloud'), ext='ply')
        self.assertTrue(isinstance(pcd, o3d.geometry.PointCloud))
        self.assertTrue(len(pcd.points) > 0)  # Non-empty

    def test_read_mesh(self):
        fs = self.get_task_fileset('TriangleMesh')
        mesh = io.read_triangle_mesh(fs.get_file('TriangleMesh'), ext='ply')
        self.assertTrue(isinstance(mesh, o3d.geometry.TriangleMesh))
        self.assertTrue(len(mesh.vertices) > 0)  # Non-empty
        self.assertTrue(len(mesh.triangles) > 0)  # Non-empty

    def test_read_skeleton(self):
        fs = self.get_task_fileset('CurveSkeleton')
        skel = io.read_json(fs.get_file('CurveSkeleton'))
        self.assertTrue(len(skel) > 0)  # Non-empty

    def test_read_tree(self):
        fs = self.get_task_fileset('TreeGraph')
        skel = io.read_graph(fs.get_file('TreeGraph'), ext='p')
        self.assertTrue(len(skel) > 0)  # Non-empty


if __name__ == "__main__":
    unittest.main()
