#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Unit tests for the file download API endpoints.

This file contains comprehensive tests for file download endpoints in
the plantdb.server.api.file module, including download of image, pointcloud, mesh, etc.
"""
import json
import unittest
from pathlib import Path

import requests

from plantdb.commons.test_database import _mkdtemp_romidb
from plantdb.server.test_rest_api import TestRestApiServer


class FileApiTests(unittest.TestCase):
    """Test cases for file download API endpoints."""

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
        r = requests.post(base_url + '/login', json={'username': 'admin', 'password': 'admin'})
        return r.json()['access_token']

    def test_download_existing_file(self):
        """Test that downloading an existing file succeeds."""
        # Get scan ID from the scans list
        r = requests.get(self.base_url + '/scans')
        self.assertEqual(r.status_code, 200)
        scans = r.json()
        self.assertGreater(len(scans), 0)
        scan_id = scans[0]

        # Try to download a file
        fileset_id = "images"
        file_id = "00000_rgb"
        r = requests.get(self.base_url + f'/file/{scan_id}/{fileset_id}/{file_id}',
                         headers={'Authorization': 'Bearer ' + self.admin_token})

        self.assertEqual(r.status_code, 200)
        # Just check we got some content back
        self.assertGreater(len(r.content), 0)

    def test_download_nonexistent_file(self):
        """Test that downloading a non-existent file returns 404."""
        r = requests.get(self.base_url + '/file/12345678-1234-1234-1234-123456789012/nonexistent.png',
                         headers={'Authorization': 'Bearer ' + self.admin_token})
        self.assertEqual(r.status_code, 404)

    def test_create_file(self):
        """Test creating a new file."""
        # First get a scan ID
        r = requests.get(self.base_url + '/scans')
        self.assertEqual(r.status_code, 200)
        scans = r.json()
        self.assertGreater(len(scans), 0)
        scan_id = scans[0]

        # Create a YAML temporary test file:
        from tempfile import NamedTemporaryFile
        with NamedTemporaryFile(suffix='.yaml', mode="w", delete=False) as f: f.write('name: my_file')
        file_path = f.name

        # Then get a file ID and try to create it
        fileset_id = "images"
        file_id = Path(file_path).stem
        metadata = {'description': 'Test file description'}

        with open(file_path, 'rb') as file_handle:
            files = {'file': (Path(file_path).name, file_handle, 'application/octet-stream')}
            r = requests.post(self.base_url + f'/file/{scan_id}/{fileset_id}/{file_id}',
                              files=files,
                              data={'ext': '.yaml', 'metadata': json.dumps(metadata)},
                              headers={'Authorization': 'Bearer ' + self.admin_token})
        self.assertEqual(r.status_code, 201)
        response = r.json()
        self.assertIn('message', response)
        self.assertIn('id', response)
        self.assertEqual(response['id'], file_id)


if __name__ == '__main__':
    unittest.main()
