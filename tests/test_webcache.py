#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import unittest

from plantdb.testing import FSDBTestCase
from plantdb.utils import read_image_from_file
from plantdb.webcache import IMG_RESOLUTIONS
from plantdb.webcache import image_path
from plantdb.webcache import pointcloud_path


class TestWebCache(FSDBTestCase):
    """Test the webcache module using the ``real_plant_analyzed`` scan dataset."""

    def test_orig_image_path(self):
        db = self.get_test_db()
        # Get the path to the original file using webcache module:
        img_path = image_path(db, 'real_plant_analyzed', 'images', '00000_rgb', 'orig')
        # Get the path to the original file using FSDB methods:
        img_file = db.get_scan('real_plant_analyzed').get_fileset('images').get_file('00000_rgb').path()
        # Assert both paths are equals:
        self.assertEqual(img_file, img_path)

    def test_large_image_path(self):
        db = self.get_test_db()
        # Get the path to the large file using webcache module:
        img_path = image_path(db, 'real_plant_analyzed', 'images', '00000_rgb', 'large')
        # Assert the folder containing the large image is 'webcache':
        self.assertEqual('webcache', img_path.parent.parts[-1])
        # Assert image size is correct:
        image = read_image_from_file(img_path)
        self.assertGreaterEqual(IMG_RESOLUTIONS['large'], max(image.size))

    def test_thumb_image_path(self):
        db = self.get_test_db()
        # Get the path to the thumb file using webcache module:
        img_path = image_path(db, 'real_plant_analyzed', 'images', '00000_rgb', 'thumb')
        # Assert the folder containing the thumb image is 'webcache':
        self.assertEqual('webcache', img_path.parent.parts[-1])
        # Assert image size is correct:
        image = read_image_from_file(img_path)
        self.assertGreaterEqual(IMG_RESOLUTIONS['thumb'], max(image.size))

    def test_orig_pcd_path(self):
        db = self.get_test_db()
        # Get the name of the fileset containing the outputs of the 'PointCloud' task:
        fs_id = self.get_task_fileset('PointCloud')
        # Get the path to the original file using webcache module:
        pcd_path = pointcloud_path(db, 'real_plant_analyzed', fs_id, 'PointCloud', 'orig')
        # Get the path to the original file using FSDB methods:
        pcd_file = db.get_scan('real_plant_analyzed').get_fileset(fs_id).get_file('PointCloud').path()
        # Assert both paths are equals:
        self.assertEqual(pcd_file, pcd_path)

    def test_preview_pcd_path(self):
        db = self.get_test_db()
        # Get the name of the fileset containing the outputs of the 'PointCloud' task:
        fs_id = self.get_task_fileset('PointCloud')
        # Get the path to the preview file using webcache module:
        pcd_path = pointcloud_path(db, 'real_plant_analyzed', fs_id, 'PointCloud', 'preview')
        # Assert the folder containing the preview pcd is 'webcache':
        self.assertEqual('webcache', pcd_path.parent.parts[-1])

    def test_orig_mesh_path(self):
        db = self.get_test_db()
        # Get the name of the fileset containing the outputs of the 'TriangleMesh' task:
        fs_id = self.get_task_fileset('TriangleMesh')
        # Get the path to the original file using webcache module:
        mesh_path = pointcloud_path(db, 'real_plant_analyzed', fs_id, 'TriangleMesh', 'orig')
        # Get the path to the original file using FSDB methods:
        mesh_file = db.get_scan('real_plant_analyzed').get_fileset(fs_id).get_file('TriangleMesh').path()
        # Assert both paths are equals:
        self.assertEqual(mesh_file, mesh_path)


if __name__ == '__main__':
    unittest.main()
