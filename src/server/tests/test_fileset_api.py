#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Unit tests for the fileset API endpoints.

This file contains comprehensive tests for fileset listing and creation API endpoints
in the plantdb.server.api.fileset module.
"""

import unittest
import requests
import tempfile
import os

from plantdb.server.test_rest_api import TestRestApiServer
from plantdb.commons.test_database import _mkdtemp_romidb


class FilesetApiTests(unittest.TestCase):
    """Test cases for fileset API endpoints."""

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

    def test_create_fileset(self):
        """Test creating a new fileset."""
        # First get a scan ID
        r = requests.get(self.base_url + '/scans')
        self.assertEqual(r.status_code, 200)
        scans = r.json()
        self.assertGreater(len(scans), 0)
        scan_id = scans[0]
        # Then create a fileset ID and try to create it
        fileset_id = "test_fileset"
        r = requests.post(self.base_url + f'/fileset/{scan_id}/{fileset_id}',
                          headers={'Authorization': 'Bearer ' + self.admin_token})
        self.assertEqual(r.status_code, 201)
        response = r.json()
        self.assertIn('message', response)
        self.assertIn('id', response)
        self.assertEqual(response['id'], fileset_id)

    def test_create_fileset_without_auth(self):
        """Test creating a fileset without authorization fails."""
        # First get a scan ID
        r = requests.get(self.base_url + '/scans')
        self.assertEqual(r.status_code, 200)
        scans = r.json()
        self.assertGreater(len(scans), 0)
        scan_id = scans[0]
        # Then create a fileset ID and try to create it (withou authentication it should fail)
        fileset_id = "test_fileset_no_auth"
        r = requests.post(self.base_url + f'/fileset/{scan_id}/{fileset_id}')
        self.assertEqual(r.status_code, 401)

    def test_create_fileset_with_metadata(self):
        """Test creating a new fileset with metadata."""
        # First get a scan ID
        r = requests.get(self.base_url + '/scans')
        self.assertEqual(r.status_code, 200)
        scans = r.json()
        self.assertGreater(len(scans), 0)
        scan_id = scans[0]
        # Then create a metadata dictionary
        metadata = {'description': 'This is a test description'}
        # Then create a fileset ID and try to create it
        fileset_id = "test_fileset_md"
        r = requests.post(self.base_url + f'/fileset/{scan_id}/{fileset_id}',
                          json=metadata,
                          headers={'Authorization': 'Bearer ' + self.admin_token})
        self.assertEqual(r.status_code, 201)
        response = r.json()
        self.assertIn('message', response)
        self.assertIn('id', response)
        self.assertEqual(response['id'], fileset_id)


if __name__ == '__main__':
    unittest.main()