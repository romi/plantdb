#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Unit tests for the authentication API endpoints.

This file contains comprehensive tests for all authentication API endpoints
in the plantdb.server.api.auth module, including Register, Login, Logout,
TokenValidation, and TokenRefresh.
"""
import logging
import time
import unittest

import requests

from plantdb.commons.test_database import _mkdtemp_romidb
from plantdb.server.test_rest_api import TestRestApiServer


class AuthApiTests(unittest.TestCase):
    """Test cases for authentication API endpoints."""

    @classmethod
    def setUpClass(cls):
        """Set up test fixtures before each test method."""
        test_logger = logging.getLogger("test_rest_api")
        test_logger.setLevel(logging.DEBUG)

        # Start a test server with the built-in test dataset
        cls.server = TestRestApiServer(db_path=_mkdtemp_romidb(), test=True, logger=test_logger)
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

        cls.admin_token = None
        cls._login_admin(cls.base_url)

    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests in the class."""
        cls.server.stop()

    @classmethod
    def _login_admin(cls, base_url):
        """Login as the admin user and return the access token."""
        if cls.admin_token is None:
            r = requests.post(base_url + '/login', json={'username': 'admin', 'password': 'admin'})
            cls.admin_token = r.json()['access_token']
        return

    def test_login_valid_credentials(self):
        """Test that login with valid credentials succeeds."""
        r = requests.post(self.base_url + '/login', json={'username': 'admin', 'password': 'admin'})
        self.assertEqual(r.status_code, 200)
        response = r.json()
        self.assertIn('access_token', response)
        self.assertIn('refresh_token', response)
        self.assertIn('user', response)
        self.assertEqual(response['user']['username'], 'admin')

    def test_login_invalid_password(self):
        """Test that login with an invalid password fails."""
        r = requests.post(self.base_url + '/login', json={'username': 'admin', 'password': 'wrong'})
        self.assertEqual(r.status_code, 401)

    def test_login_missing_field(self):
        """Test that login with missing fields fails."""
        r = requests.post(self.base_url + '/login', json={'username': 'admin'})
        self.assertEqual(r.status_code, 400)

    def test_login_unknown_username(self):
        """Test that login with an unknown username fails."""
        r = requests.post(self.base_url + '/login', json={'username': 'unknown', 'password': 'password'})
        self.assertEqual(r.status_code, 401)

    def test_get_username_exists(self):
        """Test that checking an existing username returns True."""
        r = requests.get(self.base_url + '/login?username=admin')
        self.assertEqual(r.status_code, 200)
        response = r.json()
        self.assertEqual(response['username'], 'admin')
        self.assertTrue(response['exists'])

    def test_get_username_not_exists(self):
        """Test that checking a non-existent username returns False."""
        r = requests.get(self.base_url + '/login?username=unknown')
        self.assertEqual(r.status_code, 200)
        response = r.json()
        self.assertEqual(response['username'], 'unknown')
        self.assertFalse(response['exists'])

    def test_get_username_missing(self):
        """Test that checking a missing username returns an error."""
        r = requests.get(self.base_url + '/login')
        self.assertEqual(r.status_code, 400)

    def test_register_valid_user(self):
        """Test that registration with valid fields succeeds."""
        # Make sure we have an active session as this test relies on this:
        if self.admin_token is None:
            self._login_admin(self.base_url)
        new_user = {
            "username": "testuser",
            "fullname": "Test User",
            "password": "SecurePass123!"
        }
        r = requests.post(self.base_url + '/register',
                          json=new_user,
                          headers={'Authorization': 'Bearer ' + self.admin_token})

        self.assertEqual(r.status_code, 201)
        response = r.json()
        self.assertEqual(response['message'], 'User successfully created')

    def test_register_duplicate_username(self):
        """Test that registering duplicate username fails."""
        # Make sure we have an active session as this test relies on this:
        if self.admin_token is None:
            self._login_admin(self.base_url)
        new_user = {
            "username": "admin",  # already exists
            "fullname": "Admin Duplicate",
            "password": "SecurePass123!"
        }
        r = requests.post(self.base_url + '/register',
                          json=new_user,
                          headers={'Authorization': 'Bearer ' + self.admin_token})
        self.assertEqual(r.status_code, 409)

    def test_register_missing_fields(self):
        """Test that registration with missing fields fails."""
        # Make sure we have an active session as this test relies on this:
        if self.admin_token is None:
            self._login_admin(self.base_url)
        new_user = {
            "username": "testuser",
            "fullname": "Test User"
            # Missing password
        }
        r = requests.post(self.base_url + '/register',
                          json=new_user,
                          headers={'Authorization': 'Bearer ' + self.admin_token})
        self.assertEqual(r.status_code, 400)

    def test_logout_success(self):
        """Test that logout with a valid token succeeds."""
        # Make sure we have an active session as this test relies on this:
        if self.admin_token is None:
            self._login_admin(self.base_url)
        r = requests.post(self.base_url + '/logout', headers={'Authorization': 'Bearer ' + self.admin_token})
        self.assertEqual(r.status_code, 200)
        self.__class__.admin_token = None
        response = r.json()
        self.assertIn('message', response)
        self.assertIn('Logout successful', response['message'])

    def test_logout_without_token(self):
        """Test that logout without token fails."""
        r = requests.post(self.base_url + '/logout')
        self.assertEqual(r.status_code, 401)

    def test_token_validation_valid(self):
        """Test that token validation with valid token succeeds."""
        # Make sure we have an active session as this test relies on this:
        if self.admin_token is None:
            self._login_admin(self.base_url)
        r = requests.post(self.base_url + '/token-validation',
                          headers={'Authorization': 'Bearer ' + self.admin_token})
        self.assertEqual(r.status_code, 200)
        response = r.json()
        self.assertIn('message', response)
        self.assertIn('Token validation successful', response['message'])
        self.assertIn('user', response)
        self.assertEqual(response['user']['username'], 'admin')

    def test_token_validation_invalid(self):
        """Test that token validation with an invalid token fails."""
        r = requests.post(self.base_url + '/token-validation',
                          headers={'Authorization': 'Bearer invalid_token'})
        self.assertEqual(r.status_code, 401)

    def test_token_validation_missing(self):
        """Test that token validation without token fails."""
        r = requests.post(self.base_url + '/token-validation')
        self.assertEqual(r.status_code, 401)

    def test_token_refresh_valid(self):
        """Test that token refresh with a valid refresh token succeeds."""
        # Get a refresh token by logging in
        r = requests.post(self.base_url + '/login', json={'username': 'admin', 'password': 'admin'})
        self.assertEqual(r.status_code, 200)
        refresh_token = r.json()['refresh_token']

        # Now try to refresh
        r = requests.post(self.base_url + '/token-refresh',
                          json={'refresh_token': refresh_token})
        self.assertEqual(r.status_code, 200)
        response = r.json()
        self.assertIn('access_token', response)
        self.assertIn('refresh_token', response)
        self.assertIn('message', response)
        self.assertIn('Token refreshed successfully', response['message'])

    def test_token_refresh_missing_token(self):
        """Test that token refresh with a missing refresh token fails."""
        r = requests.post(self.base_url + '/token-refresh',
                          json={})
        self.assertEqual(r.status_code, 400)

    def test_token_refresh_invalid_token(self):
        """Test that token refresh with an invalid refresh token fails."""
        r = requests.post(self.base_url + '/token-refresh',
                          json={'refresh_token': 'invalid_token'})
        self.assertEqual(r.status_code, 401)


if __name__ == '__main__':
    unittest.main()
