#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Unit and integration tests for timelapse REST API endpoints.
"""

import json
import time
import unittest

import requests

from plantdb.commons import api_endpoints
from plantdb.commons.test_database import _mkdtemp_romidb
from plantdb.server.test_rest_api import TestRestApiServer


class TimelapseApiTests(unittest.TestCase):
    """Test cases for timelapse API endpoints."""

    @classmethod
    def setUpClass(cls):
        """Set up test fixtures before test methods."""
        cls.server = TestRestApiServer(db_path=_mkdtemp_romidb(), test=True)
        cls.server.start()
        cls.base_url = cls.server.get_base_url()

        for _ in range(10):
            try:
                r = requests.get(cls.base_url + api_endpoints.health())
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
        r = requests.post(base_url + api_endpoints.login(),
                          json={'username': 'admin', 'password': 'admin'})
        return r.json()['access_token']

    def test_timelapse_lifecycle(self):
        """Test full timelapse lifecycle via REST API."""
        headers = {'Authorization': 'Bearer ' + self.admin_token}

        # 1. POST /timelapses
        r = requests.post(self.base_url + api_endpoints.timelapses(),
                          json={"id": "tl_api_01", "metadata": {"project": "chrono"}},
                          headers=headers)
        self.assertEqual(r.status_code, 201)
        created = r.json()
        self.assertEqual(created["id"], "tl_api_01")
        self.assertEqual(created["metadata"]["project"], "chrono")

        # 2. GET /timelapses
        r = requests.get(self.base_url + api_endpoints.timelapses(), headers=headers)
        self.assertEqual(r.status_code, 200)
        self.assertIn("tl_api_01", r.json())

        # 3. GET /timelapses/tl_api_01
        r = requests.get(self.base_url + api_endpoints.timelapse("tl_api_01"), headers=headers)
        self.assertEqual(r.status_code, 200)
        info = r.json()
        self.assertEqual(info["id"], "tl_api_01")
        self.assertEqual(info["counts"]["scans"], 0)

        # 4. Create member scans
        r1 = requests.post(self.base_url + api_endpoints.scan("tl_api_01_1"),
                           json={"metadata": {"timelapse": {"id": "tl_api_01", "scheduled": "2026-09-03T14:00:00Z", "index": 1}}},
                           headers=headers)
        self.assertEqual(r1.status_code, 201)

        r0 = requests.post(self.base_url + api_endpoints.scan("tl_api_01_0"),
                           json={"metadata": {"timelapse": {"id": "tl_api_01", "scheduled": "2026-09-03T10:00:00Z", "index": 0}}},
                           headers=headers)
        self.assertEqual(r0.status_code, 201)

        # Check counts updated
        r = requests.get(self.base_url + api_endpoints.timelapse("tl_api_01"), headers=headers)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["counts"]["scans"], 2)

        # 5. GET /timelapses/tl_api_01/scans
        r = requests.get(self.base_url + api_endpoints.timelapse_scans("tl_api_01"), headers=headers)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json(), ["tl_api_01_0", "tl_api_01_1"])

        # 6. GET /scans?timelapse_id=tl_api_01&sort=timelapse.scheduled
        r = requests.get(self.base_url + api_endpoints.scans(),
                         params={"timelapse_id": "tl_api_01", "sort": "timelapse.scheduled"},
                         headers=headers)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json(), ["tl_api_01_0", "tl_api_01_1"])

        # 7. DELETE /timelapses/tl_api_01 without recursive -> 409 Conflict
        r = requests.delete(self.base_url + api_endpoints.timelapse("tl_api_01"), headers=headers)
        self.assertEqual(r.status_code, 409)

        # 8. DELETE /timelapses/tl_api_01?recursive=true -> 204 No Content
        r = requests.delete(self.base_url + api_endpoints.timelapse("tl_api_01") + "?recursive=true", headers=headers)
        self.assertEqual(r.status_code, 204)

        # Verify it no longer exists
        r = requests.get(self.base_url + api_endpoints.timelapse("tl_api_01"), headers=headers)
        self.assertEqual(r.status_code, 404)

    def test_timelapse_collision_and_validation(self):
        """Test collision detection and ID validation on creation."""
        headers = {'Authorization': 'Bearer ' + self.admin_token}

        # Invalid ID
        r = requests.post(self.base_url + api_endpoints.timelapses(),
                          json={"id": "-invalid_id"},
                          headers=headers)
        self.assertEqual(r.status_code, 400)

        # Collision with existing scan
        r = requests.post(self.base_url + api_endpoints.timelapses(),
                          json={"id": "real_plant"},
                          headers=headers)
        self.assertEqual(r.status_code, 409)


if __name__ == '__main__':
    unittest.main()
