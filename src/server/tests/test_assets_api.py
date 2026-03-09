#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Unit tests for the assets API endpoints.

This file contains comprehensive tests for asset-related API endpoints
in the plantdb.server.api.assets module, including serving files, creating archives, and image thumbnails.
"""

import os
import tempfile
import unittest

import numpy as np
import requests

from plantdb.commons.test_database import _mkdtemp_romidb
from plantdb.server.test_rest_api import TestRestApiServer


class AssetsApiTests(unittest.TestCase):
    """Test cases for asset API endpoints."""

    @classmethod
    def setUpClass(cls):
        """Set up test fixtures before each test method."""
        # Start a test server with the built-in test dataset
        cls.server = TestRestApiServer(db_path=_mkdtemp_romidb(), test=True)
        cls.server.start()
        cls.base_url = cls.server.get_base_url()

        # Wait briefly for the server to start
        import time
        for _ in range(10):
            try:
                r = requests.get(cls.base_url + "/health")
                if r.status_code == 200:
                    break
            except Exception:
                pass
            time.sleep(1)

        cls.admin_token = cls._login_admin(cls.base_url)

    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests in the class."""
        cls.server.stop()

    @classmethod
    def _login_admin(cls, base_url):
        """Login as admin user and return access token."""
        r = requests.post(base_url + '/login',
                          json={'username': 'admin', 'password': 'admin'})
        return r.json()['access_token']

    def test_file_serve_success(self):
        """Test that serving a file succeeds."""
        # Get a scan ID first
        r = requests.get(self.base_url + '/scans',
                         headers={'Authorization': 'Bearer ' + self.admin_token})
        self.assertEqual(r.status_code, 200)
        scans = r.json()
        self.assertGreater(len(scans), 0)
        scan_id = scans[0]

        # Get metadata to find a file
        r = requests.get(self.base_url + f'/scan/{scan_id}',
                         headers={'Authorization': 'Bearer ' + self.admin_token})
        self.assertEqual(r.status_code, 200)
        metadata = r.json()['metadata']
        self.assertIn('files', metadata)
        self.assertGreater(len(metadata['files']), 0)

        # Try to download the metadata file
        file_name = metadata['files']['metadata']
        r = requests.get(self.base_url + f'{file_name}',
                         headers={'Authorization': 'Bearer ' + self.admin_token})
        self.assertEqual(r.status_code, 200)
        self.assertGreater(len(r.content), 0)

    def test_file_serve_not_found(self):
        """Test that serving a non-existent file fails."""
        r = requests.get(self.base_url + '/file/12345678-1234-1234-1234-123456789012/nonexistent.png',
                         headers={'Authorization': 'Bearer ' + self.admin_token})
        self.assertEqual(r.status_code, 404)

    def test_image_thumbnail_success(self):
        """Test that generating a thumbnail succeeds."""
        from PIL import Image
        from io import BytesIO

        scan_id = "real_plant_analyzed"
        fileset_id = "images"
        file_id = "00000_rgb"

        # Request thumbnail
        r = requests.get(self.base_url + f'/image/{scan_id}/{fileset_id}/{file_id}',
                         stream=True, params={"size": "thumb", "as_base64": 'false'})
        self.assertEqual(r.status_code, 200)
        self.assertIn('image', r.headers.get('Content-Type', ''))
        img = Image.open(BytesIO(r.content))
        np.testing.assert_array_equal(np.asarray(img).shape, (113, 150,   3))

    def test_image_orig_success(self):
        """Test that generating a thumbnail succeeds."""
        from PIL import Image
        from io import BytesIO

        scan_id = "real_plant_analyzed"
        fileset_id = "images"
        file_id = "00000_rgb"

        # Request thumbnail
        r = requests.get(self.base_url + f'/image/{scan_id}/{fileset_id}/{file_id}',
                         stream=True, params={"size": "orig", "as_base64": 'false'})
        self.assertEqual(r.status_code, 200)
        self.assertIn('image', r.headers.get('Content-Type', ''))
        img = Image.open(BytesIO(r.content))
        np.testing.assert_array_equal(np.asarray(img).shape, (1080, 1440, 3))

    def test_image_base64_success(self):
        """Test that generating a thumbnail succeeds."""
        import pybase64
        from PIL import Image
        from io import BytesIO

        scan_id = "real_plant_analyzed"
        fileset_id = "images"
        file_id = "00000_rgb"

        # Request thumbnail
        r = requests.get(self.base_url + f'/image/{scan_id}/{fileset_id}/{file_id}',
                         stream=True, params={"size": "thumb", "as_base64": 'true'})
        self.assertEqual(r.status_code, 200)
        self.assertIn('application/json', r.headers.get('Content-Type'))
        self.assertIn('base64', r.headers.get('X-Content-Encoding'))
        b64_string = r.json()['image']
        mime_type = r.json()['content-type']
        self.assertIn('image', mime_type)
        self.assertIsInstance(b64_string, str)
        image_data = pybase64.b64decode(b64_string)
        img = Image.open(BytesIO(image_data))
        np.testing.assert_array_equal(np.asarray(img).shape, (113, 150,   3))

    def test_archive_download(self):
        """Test that downloading an archive succeeds."""
        # Get a scan ID first
        r = requests.get(self.base_url + '/scans',
                         headers={'Authorization': 'Bearer ' + self.admin_token})
        self.assertEqual(r.status_code, 200)
        scans = r.json()
        self.assertGreater(len(scans), 0)
        scan_id = scans[0]

        # Request archive
        r = requests.get(self.base_url + f'/archive/{scan_id}',
                         headers={'Authorization': 'Bearer ' + self.admin_token})
        self.assertEqual(r.status_code, 200)
        # Should get a ZIP file
        self.assertEqual(r.headers.get('Content-Type'), 'application/zip')


if __name__ == '__main__':
    unittest.main()
