#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Unit tests for the scan API endpoints.

This file contains comprehensive tests for scan-related API endpoints
in the plantdb.server.api.scan module, including ScansList, Scan, ScanCreate,
ScanMetadata, and ScanFilesets.
"""

import json
import unittest

import requests

from plantdb.commons.test_database import _mkdtemp_romidb
from plantdb.server.test_rest_api import TestRestApiServer


class ScanApiTests(unittest.TestCase):
    """Test cases for scan API endpoints."""

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
        """Login as an admin user and return an access token."""
        r = requests.post(base_url + '/login',
                          json={'username': 'admin', 'password': 'admin'})
        return r.json()['access_token']

    def test_scans_list_success(self):
        """Test that listing scans succeeds."""
        r = requests.get(self.base_url + '/scans',
                         headers={'Authorization': 'Bearer ' + self.admin_token})
        if not r.ok:
            print(r.json())
        self.assertEqual(r.status_code, 200)
        scans = r.json()
        self.assertIsInstance(scans, list)
        self.assertGreater(len(scans), 0)

    def test_scan_without_auth_success(self):
        """Test that accessing scans without auth fails."""
        r = requests.get(self.base_url + '/scans')
        self.assertEqual(r.status_code, 200)

    def test_scans_list_with_filter(self):
        """Test that filtering scans succeeds."""
        # Use a filter query that should match at least one scan
        filter_query = {"object": {"species": "Arabidopsis.*"}}
        params = {"filterQuery": json.dumps(filter_query), "fuzzy": "true"}
        r = requests.get(self.base_url + '/scans', params=params,
                         headers={'Authorization': 'Bearer ' + self.admin_token})
        if not r.ok:
            print(r.json())
        self.assertEqual(r.status_code, 200)
        scans = r.json()
        self.assertIsInstance(scans, list)

    def test_scan_get_success(self):
        """Test that retrieving a scan succeeds."""
        # Get a scan ID to test
        scan_id = "real_plant"

        r = requests.get(self.base_url + f'/scan/{scan_id}',
                         headers={'Authorization': 'Bearer ' + self.admin_token})
        if not r.ok:
            print(r.json())
        self.assertEqual(r.status_code, 200)
        response = r.json()
        self.assertIn('id', response)
        self.assertEqual(response['id'], scan_id)
        self.assertIn('metadata', response)
        self.assertIn('images', response)

    def test_scan_get_not_found(self):
        """Test that retrieving a non-existent scan fails."""
        r = requests.get(self.base_url + '/scan/nonexistent',
                         headers={'Authorization': 'Bearer ' + self.admin_token})
        self.assertEqual(r.status_code, 404)

    def test_scan_create_success(self):
        """Test that creating a scan succeeds."""
        scan_id = "test_scan_001"
        r = requests.post(self.base_url + f'/scan/{scan_id}',
                          headers={'Authorization': 'Bearer ' + self.admin_token})
        if not r.ok:
            print(r.json())
        self.assertEqual(r.status_code, 201)
        response = r.json()
        self.assertEqual(response['id'], scan_id)

    def test_scan_create_metadata(self):
        """Test that creating a scan with metadata succeeds."""
        scan_id = "test_scan_md"
        metadata = {
            "metadata": {
                "object": {
                    "species": "TestPlant"
                }
            }
        }
        r = requests.post(self.base_url + f'/scan/{scan_id}',
                          json=metadata,
                          headers={'Authorization': 'Bearer ' + self.admin_token})
        if not r.ok:
            print(r.json())
        self.assertEqual(r.status_code, 201)
        response = r.json()
        self.assertEqual(response['id'], scan_id)

    def test_scan_metadata_get(self):
        """Test that getting scan metadata succeeds."""
        # Get a scan ID to test
        scan_id = "real_plant"

        r = requests.get(self.base_url + f'/scan/{scan_id}/metadata',
                         headers={'Authorization': 'Bearer ' + self.admin_token})
        if not r.ok:
            print(r.json())
        self.assertEqual(r.status_code, 200)
        metadata = r.json()['metadata']
        self.assertIn('owner', metadata)
        self.assertIn('acquisition_date', metadata)
        self.assertIn('object', metadata)

    def test_scan_metadata_post(self):
        """Test that updating scan metadata succeeds."""
        # Get a scan ID to test
        scan_id = "real_plant"

        # Get current metadata
        r = requests.get(self.base_url + f'/scan/{scan_id}/metadata',
                         headers={'Authorization': 'Bearer ' + self.admin_token})
        if not r.ok:
            print(r.json())
        self.assertEqual(r.status_code, 200)
        metadata = r.json()['metadata']

        # Update metadata
        metadata["object"].update({"description": "Updated test scan description"})
        r = requests.post(self.base_url + f'/scan/{scan_id}/metadata',
                          json={'metadata': metadata},
                          headers={'Authorization': 'Bearer ' + self.admin_token})
        if not r.ok:
            print(r.json())
        self.assertEqual(r.status_code, 200)
        metadata = r.json()['metadata']
        self.assertIn('object', metadata)
        self.assertIn('description', metadata['object'])

    def test_scan_filesets_list(self):
        """Test that listing scan filesets succeeds."""
        # Get a scan ID to test
        scan_id = "real_plant"

        r = requests.get(self.base_url + f'/scan/{scan_id}/filesets',
                         headers={'Authorization': 'Bearer ' + self.admin_token})
        if not r.ok:
            print(r.json())
        self.assertEqual(r.status_code, 200)
        filesets = r.json()['filesets']
        self.assertIsInstance(filesets, list)

    def test_scan_create_without_auth(self):
        """Test that creating scan without auth fails."""
        r = requests.post(self.base_url + '/scans', json={"id": "test_scan"})
        self.assertEqual(r.status_code, 405)


if __name__ == '__main__':
    unittest.main()
