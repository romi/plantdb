#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Unit tests for the ``api_endpoints`` URL builders.

These tests focus on verifying the **deployment prefix** composition performed
by the ``@api_prefix`` decorator: generated endpoint paths must always follow
``<deploy_prefix>/api/v1/<endpoint>`` and fall back to ``/api/v1/<endpoint>``
when no deployment prefix is provided.
"""

import unittest

from plantdb.commons import api_endpoints


class TestApiPrefixComposition(unittest.TestCase):
    """Test the deployment prefix composition for every endpoint builder."""

    # Map of (callable, args) -> expected no-prefix / with-prefix paths.
    ENDPOINTS = [
        (api_endpoints.home, (), "/api/v1/", "/plantdb/api/v1/"),
        (api_endpoints.health, (), "/api/v1/health", "/plantdb/api/v1/health"),
        (api_endpoints.refresh, (), "/api/v1/refresh", "/plantdb/api/v1/refresh"),
        (api_endpoints.register, (), "/api/v1/auth/register", "/plantdb/api/v1/auth/register"),
        (api_endpoints.login, (), "/api/v1/auth/login", "/plantdb/api/v1/auth/login"),
        (api_endpoints.logout, (), "/api/v1/auth/logout", "/plantdb/api/v1/auth/logout"),
        (api_endpoints.token_refresh, (), "/api/v1/auth/token/refresh", "/plantdb/api/v1/auth/token/refresh"),
        (api_endpoints.token_validation, (), "/api/v1/auth/token/validation", "/plantdb/api/v1/auth/token/validation"),
        (api_endpoints.create_api_token, (), "/api/v1/auth/token/create-api-token", "/plantdb/api/v1/auth/token/create-api-token"),
        (api_endpoints.scans, (), "/api/v1/scans", "/plantdb/api/v1/scans"),
        (api_endpoints.scans_info, (), "/api/v1/scans/info", "/plantdb/api/v1/scans/info"),
        (api_endpoints.scan, ("scan1",), "/api/v1/scans/scan1", "/plantdb/api/v1/scans/scan1"),
        (api_endpoints.scan_metadata, ("scan1",), "/api/v1/scans/scan1/metadata", "/plantdb/api/v1/scans/scan1/metadata"),
        (api_endpoints.scan_filesets_list, ("scan1",), "/api/v1/scans/scan1/filesets", "/plantdb/api/v1/scans/scan1/filesets"),
        (api_endpoints.fileset, ("scan1", "fs1"), "/api/v1/filesets/scan1/fs1", "/plantdb/api/v1/filesets/scan1/fs1"),
        (api_endpoints.fileset_metadata, ("scan1", "fs1"), "/api/v1/filesets/scan1/fs1/metadata", "/plantdb/api/v1/filesets/scan1/fs1/metadata"),
        (api_endpoints.fileset_files_list, ("scan1", "fs1"), "/api/v1/filesets/scan1/fs1/files", "/plantdb/api/v1/filesets/scan1/fs1/files"),
        (api_endpoints.file, ("scan1", "fs1", "f1"), "/api/v1/files/scan1/fs1/f1", "/plantdb/api/v1/files/scan1/fs1/f1"),
        (api_endpoints.file_metadata, ("scan1", "fs1", "f1"), "/api/v1/files/scan1/fs1/f1/metadata", "/plantdb/api/v1/files/scan1/fs1/f1/metadata"),
        (api_endpoints.image, ("scan1", "fs1", "f1"), "/api/v1/assets/image/scan1/fs1/f1", "/plantdb/api/v1/assets/image/scan1/fs1/f1"),
        (api_endpoints.pointcloud, ("scan1",), "/api/v1/assets/pointcloud/scan1?type=default", "/plantdb/api/v1/assets/pointcloud/scan1?type=default"),
        (api_endpoints.mesh, ("scan1",), "/api/v1/assets/mesh/scan1", "/plantdb/api/v1/assets/mesh/scan1"),
        (api_endpoints.sequence, ("scan1",), "/api/v1/assets/sequence/scan1?type=all", "/plantdb/api/v1/assets/sequence/scan1?type=all"),
        (api_endpoints.skeleton, ("scan1",), "/api/v1/assets/skeleton/scan1", "/plantdb/api/v1/assets/skeleton/scan1"),
        (api_endpoints.archive, ("scan1",), "/api/v1/assets/archive/scan1", "/plantdb/api/v1/assets/archive/scan1"),
        (api_endpoints.file_path, ("scan1/fs1/f1",), "/api/v1/assets/files/scan1/fs1/f1", "/plantdb/api/v1/assets/files/scan1/fs1/f1"),
    ]

    def test_no_prefix_composition(self):
        """The default (no deployment prefix) yields exactly ``/api/v1/<endpoint>``."""
        for func, args, expected, _ in self.ENDPOINTS:
            with self.subTest(func=func.__name__):
                self.assertEqual(func(*args), expected)

    def test_no_prefix_explicit_empty_and_none(self):
        """Empty string, ``None``, and omitted ``prefix`` all behave identically."""
        for func, args, expected, _ in self.ENDPOINTS:
            with self.subTest(func=func.__name__):
                self.assertEqual(func(*args, prefix=""), expected)
                self.assertEqual(func(*args, prefix=None), expected)
                self.assertEqual(func(*args, prefix=""), expected)

    def test_with_prefix_composition(self):
        """A deployment prefix is prepended before ``/api/v1/<endpoint>``."""
        for func, args, _, expected in self.ENDPOINTS:
            with self.subTest(func=func.__name__):
                self.assertEqual(func(*args, prefix="/plantdb"), expected)

    def test_prefix_sanitization(self):
        """Leading/trailing slashes in the prefix are stripped then re-added cleanly."""
        self.assertEqual(api_endpoints.login(prefix="/plantdb/"), "/plantdb/api/v1/auth/login")
        self.assertEqual(api_endpoints.login(prefix="plantdb"), "/plantdb/api/v1/auth/login")
        self.assertEqual(api_endpoints.login(prefix="plantdb/"), "/plantdb/api/v1/auth/login")
        self.assertEqual(api_endpoints.login(prefix="//plantdb//"), "/plantdb/api/v1/auth/login")

    def test_multi_segment_prefix(self):
        """A multi-segment deployment prefix stays intact."""
        self.assertEqual(
            api_endpoints.scan("scan1", prefix="/romi/plant"),
            "/romi/plant/api/v1/scans/scan1",
        )

    def test_query_parameters_preserved(self):
        """Query strings must survive prefix composition."""
        self.assertEqual(
            api_endpoints.refresh("scan1", prefix="/plantdb"),
            "/plantdb/api/v1/refresh?scan_id=scan1",
        )
        self.assertEqual(
            api_endpoints.image("scan1", "fs1", "f1", size="thumb", prefix="/plantdb"),
            "/plantdb/api/v1/assets/image/scan1/fs1/f1?size=thumb",
        )

    def test_examples_from_docstrings(self):
        """The docstring examples hold true."""
        self.assertEqual(api_endpoints.login(), "/api/v1/auth/login")
        self.assertEqual(api_endpoints.login(prefix="/plantdb"), "/plantdb/api/v1/auth/login")
        self.assertEqual(api_endpoints.login(prefix=None), "/api/v1/auth/login")
        self.assertEqual(api_endpoints.home(), "/api/v1/")
        self.assertEqual(api_endpoints.home(prefix="/plantdb"), "/plantdb/api/v1/")


if __name__ == "__main__":
    unittest.main()