#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Integration test verifying the server injects the deployment prefix into
generated URLs when configured with a ``deploy_prefix``.

The app is mounted at ``'/api/v1'`` (mount prefix); when a ``deploy_prefix`` is
configured, links embedded in responses (the ``Home`` endpoint map, the
``ScansTable`` / ``Scan`` resources) must carry ``<deploy_prefix>/api/v1/<endpoint>``.
"""
import time
import unittest

import requests

from plantdb.commons import api_endpoints
from plantdb.commons.test_database import _mkdtemp_romidb
from plantdb.server.test_rest_api import TestRestApiServer


class TestDeployPrefixInResponses(unittest.TestCase):
    """Server response URLs must include the configured deployment prefix."""

    DEPLOY = "/plantdb"

    @classmethod
    def setUpClass(cls):
        cls.server = TestRestApiServer(db_path=_mkdtemp_romidb(), test=True, deploy_prefix=cls.DEPLOY)
        cls.server.start()
        for _ in range(20):
            try:
                r = requests.get(cls.server.get_base_url() + api_endpoints.health())
                if r.status_code == 200:
                    break
            except Exception:
                pass
            time.sleep(1)

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()

    def test_home_endpoint_map_includes_deploy_prefix(self):
        """Every key in the ``Home.get()`` endpoint map carries the deployment prefix."""
        r = requests.get(self.server.get_base_url() + api_endpoints.home())
        self.assertEqual(r.status_code, 200)
        api_info = r.json()

        groups = [
            "base endpoints",
            "authentication endpoints",
            "scans endpoints",
            "filesets endpoints",
            "files endpoints",
            "assets endpoints",
        ]
        for group in groups:
            for url in api_info[group]:
                self.assertTrue(
                    url.startswith(f"{self.DEPLOY}/api/v1/"),
                    msg=f"[{group}] {url} should start with {self.DEPLOY}/api/v1/",
                )

    def test_scan_info_thumbnails_include_deploy_prefix(self):
        """``ScansTable`` responses embed deployment-prefixed thumbnail URIs."""
        r = requests.get(self.server.get_base_url() + api_endpoints.scans_info())
        self.assertEqual(r.status_code, 200)
        table = r.json()
        self.assertIsInstance(table, list)
        for scan in table:
            self.assertTrue(
                scan["thumbnailUri"].startswith(f"{self.DEPLOY}/api/v1/"),
                msg=f"thumbnailUri for '{scan['id']}' should carry the deployment prefix",
            )

    def test_scan_response_links_include_deploy_prefix(self):
        """``Scan`` responses embed deployment-prefixed archive/thumbnail URIs."""
        scans = requests.get(self.server.get_base_url() + api_endpoints.scans()).json()
        scan_id = scans[0]
        r = requests.get(self.server.get_base_url() + api_endpoints.scan(scan_id))
        self.assertEqual(r.status_code, 200)
        info = r.json()
        self.assertTrue(
            info["metadata"]["files"]["archive"].startswith(f"{self.DEPLOY}/api/v1/"),
            msg="archive URL should carry the deployment prefix",
        )


if __name__ == "__main__":
    unittest.main()