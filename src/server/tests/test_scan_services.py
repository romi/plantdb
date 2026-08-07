#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Tests for the scan service functions ``get_scan_info`` / ``get_scan_data``.

These tests assert that the runtime deployment prefix is threaded through the
services into every generated URL (thumbnail, archive, filesUri, camera poses).
"""
import tempfile
import unittest

from plantdb.commons.test_database import test_database
from plantdb.server.services.scan import get_scan_data
from plantdb.server.services.scan import get_scan_info


class TestScanServicesPrefix(unittest.TestCase):
    """Verify prefix threading in the scan services."""

    PREFIX = "/plantdb"

    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.mkdtemp()
        cls.db = test_database("real_plant_analyzed", db_path=cls._tmp, no_auth=True, keep_tmp=True)
        cls.db.connect()
        cls.scan = cls.db.get_scan("real_plant_analyzed")

    @classmethod
    def tearDownClass(cls):
        cls.db.disconnect()

    def test_get_scan_info_no_prefix(self):
        """Without a prefix, generated URLs start with ``/api/v1``."""
        info = get_scan_info(self.scan)
        self.assertTrue(info["thumbnailUri"].startswith("/api/v1/"))
        self.assertFalse(info["thumbnailUri"].startswith("/plantdb"))
        archive = info["metadata"]["files"]["archive"]
        self.assertTrue(archive.startswith("/api/v1/"))
        # Camera poses must be present (colmap data) and unprefixed.
        if info["camera"].get("poses"):
            self.assertTrue(info["camera"]["poses"][0]["photoUri"].startswith("/api/v1/"))

    def test_get_scan_info_with_prefix(self):
        """Deployment prefix is prepended to every generated URL in ``get_scan_info``."""
        info = get_scan_info(self.scan, prefix=self.PREFIX)
        # Thumbnail / archive / camera pose URLs must carry the deployment prefix.
        self.assertTrue(info["thumbnailUri"].startswith(f"{self.PREFIX}/api/v1/"))
        self.assertTrue(
            info["metadata"]["files"]["archive"].startswith(f"{self.PREFIX}/api/v1/")
        )
        self.assertTrue(
            info["metadata"]["files"]["metadata"].startswith(f"{self.PREFIX}/api/v1/")
        )
        if info["camera"].get("poses"):
            pose = info["camera"]["poses"][0]
            self.assertTrue(pose["photoUri"].startswith(f"{self.PREFIX}/api/v1/"))
            self.assertTrue(pose["thumbnailUri"].startswith(f"{self.PREFIX}/api/v1/"))

    def test_get_scan_data_forwards_prefix_to_get_scan_info(self):
        """``get_scan_data`` must forward the prefix into the nested ``get_scan_info`` call."""
        data = get_scan_data(self.scan, prefix=self.PREFIX)
        # The thumbnailUri and archive are produced by the nested get_scan_info:
        self.assertTrue(data["thumbnailUri"].startswith(f"{self.PREFIX}/api/v1/"))
        self.assertTrue(
            data["metadata"]["files"]["archive"].startswith(f"{self.PREFIX}/api/v1/")
        )
        # filesUri (produced directly by get_scan_data) also carries the prefix.
        for uri_key, uri in data.get("filesUri", {}).items():
            self.assertTrue(
                uri.startswith(f"{self.PREFIX}/api/v1/"),
                msg=f"filesUri['{uri_key}'] should carry the deployment prefix: {uri}",
            )

    def test_get_scan_data_no_prefix(self):
        """Without prefix, ``get_scan_data`` URLs start with ``/api/v1``."""
        data = get_scan_data(self.scan)
        self.assertTrue(data["thumbnailUri"].startswith("/api/v1/"))
        self.assertFalse(data["thumbnailUri"].startswith("/plantdb"))
        for uri in data.get("filesUri", {}).values():
            self.assertTrue(uri.startswith("/api/v1/"), msg=f"unprefixed URI expected, got: {uri}")


if __name__ == "__main__":
    unittest.main()