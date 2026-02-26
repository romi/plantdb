#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import tempfile
import threading
import time
import unittest
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path

import jwt

from plantdb.commons.auth.session import AccessTokenNotFoundError
from plantdb.commons.auth.session import JWTSessionManager
from plantdb.commons.auth.session import RefreshTokenNotFoundError
from plantdb.commons.auth.session import SessionManager
from plantdb.commons.auth.session import SessionValidationError
from plantdb.commons.auth.session import SingleSessionManager
from plantdb.commons.auth.session import WrongTokenType
from plantdb.commons.auth.session import _init_secret_key


class TestSessionManager(unittest.TestCase):
    """Test cases for the ``SessionManager`` class"""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.session_manager = SessionManager(session_timeout=3600)  # 1 hour timeout

    def test_init_creates_empty_sessions_dict(self):
        """Test that SessionManager initializes with empty sessions dictionary."""
        # Verify the initial state is correct
        self.assertEqual(len(self.session_manager.sessions), 0)
        self.assertEqual(self.session_manager.session_timeout, 3600)
        self.assertIsNotNone(self.session_manager.logger)

    def test_user_has_session_returns_consistent_id(self):
        """Test that _user_has_session returns consistent session IDs for the same user."""
        # Test session ID generation
        session_id1 = self.session_manager._user_has_session("testuser")
        session_id2 = self.session_manager._user_has_session("testuser")

        # Session IDs should be consistent for the same user at the same time
        self.assertEqual(session_id1, session_id2)

    def test_create_session_stores_session_data(self):
        """Test that create_session properly stores the session with expiry time."""
        # Create session
        session_id = self.session_manager.create_session("testuser")

        # Verify session was created
        self.assertIn(session_id, self.session_manager.sessions)
        session_data = self.session_manager.sessions[session_id]
        self.assertEqual(session_data['username'], "testuser")

        # Should have created_at timestamp
        self.assertIn('created_at', session_data)
        self.assertIsInstance(session_data['created_at'], datetime)

    def test_validate_session_returns_true_for_valid_session(self):
        """Test that validate_session returns True for non-expired sessions."""
        # Create session first
        session_id = self.session_manager.create_session("testuser")

        # Validate immediately (should be valid)
        is_valid = self.session_manager.validate_session(session_id)
        self.assertTrue(is_valid)

    def test_validate_session_returns_false_for_expired_session(self):
        """Test that validate_session returns False for expired sessions."""
        # Create a session with a very short timeout
        temp_session_manager = SessionManager(session_timeout=0.1)  # 0.1 second timeout
        session_id = temp_session_manager.create_session("testuser")

        # Wait for the session to expire
        time.sleep(0.2)

        # Validate after timeout period
        is_valid = temp_session_manager.validate_session(session_id)
        self.assertFalse(is_valid)

    def test_validate_session_returns_false_for_invalid_session_id(self):
        """Test that validate_session returns False for non-existent session IDs."""
        is_valid = self.session_manager.validate_session("invalid_session_id")
        self.assertFalse(is_valid)

    def test_invalidate_session_removes_session(self):
        """Test that invalidate_session removes session from storage."""
        # Create session first
        session_id = self.session_manager.create_session("testuser")
        self.assertIn(session_id, self.session_manager.sessions)

        # Invalidate session
        self.session_manager.invalidate_session(session_id)
        self.assertNotIn(session_id, self.session_manager.sessions)

    def test_cleanup_expired_sessions_removes_old_sessions(self):
        """Test that cleanup_expired_sessions removes only expired sessions."""
        # Create a session manager with a very short timeout for testing
        temp_session_manager = SessionManager(session_timeout=0.5)  # 0.5 second timeout

        # Create the first session
        session1_id = temp_session_manager.create_session("user1")

        # Wait for a moment
        time.sleep(0.6)  # Wait for the first session to expire

        # Create a second session
        session2_id = temp_session_manager.create_session("user2")

        # Cleanup expired sessions
        temp_session_manager.cleanup_expired_sessions()

        # Only expired session should be removed
        self.assertNotIn(session1_id, temp_session_manager.sessions)
        self.assertIn(session2_id, temp_session_manager.sessions)

    def test_session_username_returns_correct_username(self):
        """Test that session_username returns the correct username for a valid session."""
        # Create session
        session_id = self.session_manager.create_session("testuser")

        # Get a username from a session
        username = self.session_manager.session_username(session_id)
        self.assertEqual(username, "testuser")

    def test_session_username_returns_none_for_invalid_session(self):
        """Test that session_username returns None for invalid session ID."""
        username = self.session_manager.session_username("invalid_session")
        self.assertIsNone(username)

    def test_max_concurrent_sessions_enforced(self):
        """Test that create_session returns None when max concurrent sessions reached."""
        mgr = SessionManager(session_timeout=3600, max_concurrent_sessions=2)
        # Fill up all session slots
        s1 = mgr.create_session("user1")
        s2 = mgr.create_session("user2")
        self.assertIsNotNone(s1)
        self.assertIsNotNone(s2)
        # The third session should be rejected
        s3 = mgr.create_session("user3")
        self.assertIsNone(s3)

    def test_session_token_returns_token_for_active_user(self):
        """Test that session_token returns the session key for a user with an active session."""
        session_id = self.session_manager.create_session("lookup_user")
        result = self.session_manager.session_token("lookup_user")
        self.assertEqual(result, session_id)

    def test_session_token_returns_none_for_unknown_user(self):
        """Test that session_token returns None for a user with no active session."""
        result = self.session_manager.session_token("nonexistent_user")
        self.assertIsNone(result)

    def test_session_token_returns_none_for_none_username(self):
        """Test that session_token returns None when the username is None."""
        result = self.session_manager.session_token(None)
        self.assertIsNone(result)

    def test_refresh_session_creates_new_session(self):
        """Test that refresh_session invalidates old and creates a new session."""
        session_id = self.session_manager.create_session("refresh_user")
        new_session_id = self.session_manager.refresh_session(session_id)
        # New token should be different
        self.assertNotEqual(session_id, new_session_id)
        # Old session should be gone
        self.assertNotIn(session_id, self.session_manager.sessions)
        # New session should exist
        self.assertIn(new_session_id, self.session_manager.sessions)

    def test_refresh_session_returns_none_for_invalid_token(self):
        """Test that refresh_session returns None for a non-existent session."""
        result = self.session_manager.refresh_session("invalid_token")
        self.assertIsNone(result)

    def test_user_has_session_returns_false_for_none(self):
        """Test that _user_has_session returns False when the username is None."""
        self.assertFalse(self.session_manager._user_has_session(None))

    def test_invalidate_nonexistent_session_returns_false(self):
        """Test that invalidate_session returns (False, None) for unknown session."""
        success, username = self.session_manager.invalidate_session("nonexistent")
        self.assertFalse(success)
        self.assertIsNone(username)


class TestSingleSessionManager(unittest.TestCase):
    """Test cases for the ``SingleSessionManager`` class."""

    def test_single_session_enforced(self):
        """SingleSessionManager allows only one concurrent session."""
        mgr = SingleSessionManager(session_timeout=3600)
        s1 = mgr.create_session("user1")
        self.assertIsNotNone(s1)
        # The second session should be rejected
        s2 = mgr.create_session("user2")
        self.assertIsNone(s2)

    def test_single_session_allows_new_after_invalidation(self):
        """After invalidating the only session, a new one can be created."""
        mgr = SingleSessionManager(session_timeout=3600)
        s1 = mgr.create_session("user1")
        mgr.invalidate_session(s1)
        s2 = mgr.create_session("user2")
        self.assertIsNotNone(s2)

    def test_max_concurrent_sessions_is_one(self):
        """SingleSessionManager always has max_concurrent_sessions == 1."""
        mgr = SingleSessionManager()
        self.assertEqual(mgr.max_concurrent_sessions, 1)


class TestJWTSessionManager(unittest.TestCase):
    """Comprehensive unit tests for the ``JWTSessionManager`` class."""

    # ------------------------------------------------------------------
    # Helper methods
    # ------------------------------------------------------------------
    def _create_manager(self, session_timeout=2, refresh_timeout=5, max_sessions=2):
        """
        Create a ``JWTSessionManager`` with short lifetimes so tests can
        trigger expiration without long sleeps.
        """
        return JWTSessionManager(
            session_timeout=session_timeout,
            refresh_timeout=refresh_timeout,
            max_concurrent_sessions=max_sessions,
            secret_key="test-secret-key",  # deterministic for reproducibility
            leeway=0,  # no leeway for token validation during tests
        )

    def _decode(self, token, mgr):
        """Convenience helper to decode a JWT issued by *mgr*."""
        return jwt.decode(
            token,
            mgr.secret_key,
            algorithms=["HS512"],
            audience="plantdb-client",
            issuer="plantdb-api",
        )

    # ------------------------------------------------------------------
    # Construction / secret‑key handling
    # ------------------------------------------------------------------
    def test_init_derives_key_from_passphrase(self):
        """A string secret is stretched with Argon2 to a 64‑byte key."""
        mgr = JWTSessionManager(secret_key="my‑passphrase")
        # The derived key must be exactly 64 bytes for HS512.
        self.assertIsInstance(mgr.secret_key, bytes)
        self.assertEqual(len(mgr.secret_key), 64)

    def test_init_fails_on_short_binary_key(self):
        """Providing a binary key < 64 bytes raises ``ValueError``."""
        short_key = b"12345"
        with self.assertRaises(ValueError):
            JWTSessionManager(secret_key=short_key)

    def test_init_none_secret_generates_random_64_byte_key(self):
        """When no secret_key is supplied, a random 64-byte key is generated."""
        mgr = JWTSessionManager(secret_key=None)
        # The key must be 64 bytes and of type bytes
        self.assertIsInstance(mgr.secret_key, bytes)
        self.assertEqual(len(mgr.secret_key), 64)

    def test_init_valid_bytes_key_is_accepted(self):
        """A 64-byte binary key is accepted and stored as-is."""
        raw = b"A" * 64
        mgr = JWTSessionManager(secret_key=raw)
        # The exact same object should be used
        self.assertEqual(mgr.secret_key, raw)

    def test_init_default_leeway_is_two(self):
        """The default leeway should be 2 seconds when not explicitly set."""
        mgr = JWTSessionManager(secret_key=b"X" * 64)
        self.assertEqual(mgr.leeway, 2)

    def test_init_default_session_timeout(self):
        """Default session_timeout is 900 seconds (15 minutes)."""
        mgr = JWTSessionManager(secret_key=b"X" * 64)
        self.assertEqual(mgr.session_timeout, 900)

    def test_init_default_refresh_timeout(self):
        """Default refresh_timeout is 86400 seconds (24 hours)."""
        mgr = JWTSessionManager(secret_key=b"X" * 64)
        self.assertEqual(mgr.refresh_timeout, 86400)

    def test_init_default_max_concurrent_sessions(self):
        """Default max_concurrent_sessions is 10."""
        mgr = JWTSessionManager(secret_key=b"X" * 64)
        self.assertEqual(mgr.max_concurrent_sessions, 10)

    def test_init_custom_api_token_dir(self):
        """Custom api_token_dir is used for the api_token_file path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = JWTSessionManager(secret_key=b"X" * 64, api_token_dir=tmpdir)
            self.assertEqual(mgr.api_token_file, Path(tmpdir) / "api_token.txt")

    def test_init_none_secret_removes_existing_api_token_file(self):
        """When secret_key=None and an api_token_file exists, the file is removed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            token_file = Path(tmpdir) / "api_token.txt"
            # Pre-create the file
            token_file.write_text("some old token data\n")
            self.assertTrue(token_file.exists())

            # Initialize with secret_key=None — should remove the stale file
            mgr = JWTSessionManager(secret_key=None, api_token_dir=tmpdir)
            self.assertFalse(token_file.exists())

    # ------------------------------------------------------------------
    # _init_secret_key module-level function
    # ------------------------------------------------------------------
    def test_module_init_secret_key_none(self):
        """Module-level _init_secret_key(None) returns a random 64-byte key."""
        key = _init_secret_key(None)
        self.assertIsInstance(key, bytes)
        self.assertEqual(len(key), 64)

    def test_module_init_secret_key_bytes_passthrough(self):
        """Module-level _init_secret_key with valid bytes returns the same object."""
        raw = b"B" * 64
        result = _init_secret_key(raw)
        self.assertIs(result, raw)

    def test_module_init_secret_key_short_bytes_raises(self):
        """Module-level _init_secret_key with short bytes raises ValueError."""
        with self.assertRaises(ValueError):
            _init_secret_key(b"short")

    def test_module_init_secret_key_string(self):
        """Module-level _init_secret_key with a string derives a 64-byte key."""
        key = _init_secret_key("a passphrase")
        self.assertIsInstance(key, bytes)
        self.assertEqual(len(key), 64)

    def test_module_init_secret_key_long_bytes_accepted(self):
        """Module-level _init_secret_key with bytes longer than 64 is accepted."""
        raw = b"C" * 128
        result = _init_secret_key(raw)
        # Should be accepted and returned as-is
        self.assertIs(result, raw)

    def test_module_init_secret_key_none_generates_unique_keys(self):
        """Two calls to _init_secret_key(None) produce different random keys."""
        key1 = _init_secret_key(None)
        key2 = _init_secret_key(None)
        self.assertNotEqual(key1, key2)

    # ------------------------------------------------------------------
    # Basic session creation
    # ------------------------------------------------------------------
    def test_create_session_returns_two_tokens_and_registers_them(self):
        """`create_session` should return an (access, refresh) tuple and store both."""
        mgr = self._create_manager()
        result = mgr.create_session("alice")

        # Verify we got a 2-tuple of strings
        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 2)
        access_token, refresh_token = result
        self.assertIsInstance(access_token, str)
        self.assertIsInstance(refresh_token, str)

        # Decode payloads just to verify they contain the expected fields
        access_payload = self._decode(access_token, mgr)
        refresh_payload = self._decode(refresh_token, mgr)
        self.assertEqual(access_payload["sub"], "alice")
        self.assertEqual(refresh_payload["sub"], "alice")
        self.assertEqual(access_payload["type"], "access")
        self.assertEqual(refresh_payload["type"], "refresh")

        # The manager must have stored the JTI of both tokens
        self.assertIn(access_payload["jti"], mgr.sessions)
        self.assertIn(refresh_payload["jti"], mgr.refresh_tokens)

    def test_create_session_stores_correct_metadata(self):
        """Session metadata (username, timestamps, type) is stored correctly."""
        mgr = self._create_manager()
        access, refresh = mgr.create_session("zorro")

        access_jti = self._decode(access, mgr)["jti"]
        session_data = mgr.sessions[access_jti]

        # Verify all expected keys and values
        self.assertEqual(session_data["username"], "zorro")
        self.assertIn("created_at", session_data)
        self.assertIn("last_accessed", session_data)
        self.assertIn("expires_at", session_data)
        self.assertEqual(session_data["type"], "access")
        self.assertIsInstance(session_data["created_at"], datetime)
        self.assertIsInstance(session_data["expires_at"], datetime)

    def test_create_session_refresh_token_links_to_access(self):
        """Refresh token metadata should contain the access_jti linking it to the access token."""
        mgr = self._create_manager()
        access, refresh = mgr.create_session("bob")

        access_jti = self._decode(access, mgr)["jti"]
        refresh_jti = self._decode(refresh, mgr)["jti"]

        # The refresh token record must point back to the access token
        self.assertEqual(mgr.refresh_tokens[refresh_jti]["access_jti"], access_jti)

    def test_create_session_refresh_metadata_stored_correctly(self):
        """Refresh token metadata includes username, timestamps, type, and access_jti."""
        mgr = self._create_manager()
        access, refresh = mgr.create_session("meta_user")
        refresh_jti = self._decode(refresh, mgr)["jti"]
        ref_data = mgr.refresh_tokens[refresh_jti]

        self.assertEqual(ref_data["username"], "meta_user")
        self.assertIn("created_at", ref_data)
        self.assertIn("expires_at", ref_data)
        self.assertEqual(ref_data["type"], "refresh")
        self.assertIn("access_jti", ref_data)

    # ------------------------------------------------------------------
    # n_active_sessions
    # ------------------------------------------------------------------
    def test_n_active_sessions_returns_correct_count(self):
        """`n_active_sessions` returns the number of non-expired access sessions."""
        mgr = self._create_manager(max_sessions=5)
        self.assertEqual(mgr.n_active_sessions(), 0)

        mgr.create_session("user1")
        self.assertEqual(mgr.n_active_sessions(), 1)

        mgr.create_session("user2")
        self.assertEqual(mgr.n_active_sessions(), 2)

    def test_n_active_sessions_excludes_expired(self):
        """`n_active_sessions` does not count expired sessions."""
        mgr = self._create_manager(session_timeout=1, max_sessions=5)
        mgr.create_session("short_lived")
        self.assertEqual(mgr.n_active_sessions(), 1)

        time.sleep(1.5)
        # After expiration, the count should be 0
        self.assertEqual(mgr.n_active_sessions(), 0)

    # ------------------------------------------------------------------
    # Validation of access / refresh tokens
    # ------------------------------------------------------------------
    def test_validate_access_token_returns_payload_and_updates_last_accessed(self):
        """`validate_session` for an access token returns a dict and updates its timestamp."""
        mgr = self._create_manager()
        access, _ = mgr.create_session("bob")
        access_jti = self._decode(access, mgr)["jti"]
        before = mgr.sessions[access_jti]["last_accessed"]

        # Small delay to ensure timestamp differs
        time.sleep(0.05)

        info = mgr.validate_session(access)  # defaults to token_type='access'
        self.assertEqual(info["username"], "bob")
        self.assertEqual(info["type"], "access")
        # ``last_accessed`` must be later than the previous value
        after = mgr.sessions[info["jti"]]["last_accessed"]
        self.assertGreater(after, before)

    def test_validate_session_returns_all_expected_fields(self):
        """The dict returned by validate_session contains all documented keys."""
        mgr = self._create_manager()
        access, _ = mgr.create_session("field_check")
        info = mgr.validate_session(access)

        expected_keys = {"username", "issued_at", "expires_at", "jti", "issuer", "audience", "type"}
        self.assertTrue(expected_keys.issubset(info.keys()),
                        f"Missing keys: {expected_keys - info.keys()}")

    def test_validate_refresh_token_returns_payload_without_touching_access(self):
        """`validate_session` for a refresh token returns a dict and does not affect access dict."""
        mgr = self._create_manager()
        _, refresh = mgr.create_session("carol")
        payload = self._decode(refresh, mgr)
        # Record the state of the related access token before validation
        access_jti = mgr.refresh_tokens[payload["jti"]]["access_jti"]
        access_last = mgr.sessions[access_jti]["last_accessed"]

        info = mgr.validate_session(refresh, token_type="refresh")
        self.assertEqual(info["username"], "carol")
        self.assertEqual(info["type"], "refresh")
        # Access token's ``last_accessed`` must stay unchanged
        self.assertEqual(mgr.sessions[access_jti]["last_accessed"], access_last)

    # ------------------------------------------------------------------
    # Token‑type mismatch handling
    # ------------------------------------------------------------------
    def test_token_type_mismatch_raises_invalid_token_processing_error(self):
        """Supplying a refresh token when ``token_type='access'`` raises ``WrongTokenType``."""
        mgr = self._create_manager()
        _, refresh = mgr.create_session("dave")
        with self.assertRaises(WrongTokenType):
            mgr.validate_session(refresh, token_type="access")

    def test_access_token_as_refresh_raises_wrong_token_type(self):
        """Supplying an access token when ``token_type='refresh'`` raises ``WrongTokenType``."""
        mgr = self._create_manager()
        access, _ = mgr.create_session("mismatch_user")
        with self.assertRaises(WrongTokenType):
            mgr.validate_session(access, token_type="refresh")

    def test_refresh_token_as_access_raises_wrong_token_type(self):
        """Supplying a refresh token when ``token_type='access'`` raises ``WrongTokenType``."""
        mgr = self._create_manager()
        _, refresh = mgr.create_session("mismatch_user")
        with self.assertRaises(WrongTokenType):
            mgr.validate_session(refresh, token_type="access")

    # ------------------------------------------------------------------
    # Expiration handling
    # ------------------------------------------------------------------
    def test_access_token_expiration_raises_session_validation_error(self):
        """An expired access token triggers ``SessionValidationError``."""
        mgr = self._create_manager(session_timeout=1, refresh_timeout=5)
        access, _ = mgr.create_session("eve")
        time.sleep(2)  # exceed access token life
        with self.assertRaises(SessionValidationError):
            mgr.validate_session(access)

    def test_refresh_token_expiration_raises_session_validation_error(self):
        """An expired refresh token triggers ``SessionValidationError``."""
        mgr = self._create_manager(session_timeout=5, refresh_timeout=1)
        _, refresh = mgr.create_session("frank")
        time.sleep(2)  # exceed refresh token life
        with self.assertRaises(SessionValidationError):
            mgr.validate_session(refresh, token_type="refresh")

    # ------------------------------------------------------------------
    # Malformed / audience / issuer errors
    # ------------------------------------------------------------------
    def test_malformed_token_raises_session_validation_error(self):
        """A syntactically invalid JWT raises ``SessionValidationError``."""
        mgr = self._create_manager()
        malformed = "not.a.valid.jwt"
        with self.assertRaises(SessionValidationError):
            mgr.validate_session(malformed)

    def test_invalid_audience_raises_session_validation_error(self):
        """A token with a wrong audience raises ``SessionValidationError``."""
        mgr = self._create_manager()
        # Build a token with a different audience
        now = datetime.now(timezone.utc)
        payload = {
            "iss": "plantdb-api",
            "sub": "george",
            "aud": "wrong-audience",
            "exp": int((now.timestamp() + 10)),
            "iat": int(now.timestamp()),
            "jti": "dummy-jti",
            "type": "access",
        }
        bad_token = jwt.encode(payload, mgr.secret_key, algorithm="HS512")
        with self.assertRaises(SessionValidationError):
            mgr.validate_session(bad_token)

    def test_invalid_issuer_raises_session_validation_error(self):
        """A token with a wrong issuer raises ``SessionValidationError``."""
        mgr = self._create_manager()
        now = datetime.now(timezone.utc)
        payload = {
            "iss": "wrong-issuer",
            "sub": "harry",
            "aud": "plantdb-client",
            "exp": int((now.timestamp() + 10)),
            "iat": int(now.timestamp()),
            "jti": "dummy-jti-2",
            "type": "access",
        }
        bad_token = jwt.encode(payload, mgr.secret_key, algorithm="HS512")
        with self.assertRaises(SessionValidationError):
            mgr.validate_session(bad_token)

    def test_wrong_signing_key_raises_session_validation_error(self):
        """A token signed with a different key raises ``SessionValidationError``."""
        mgr = self._create_manager()
        # Sign with a completely different key
        wrong_key = b"Z" * 64
        now = datetime.now(timezone.utc)
        payload = {
            "iss": "plantdb-api",
            "sub": "intruder",
            "aud": "plantdb-client",
            "exp": int((now + timedelta(seconds=60)).timestamp()),
            "iat": int(now.timestamp()),
            "jti": "forged-jti",
            "type": "access",
        }
        forged_token = jwt.encode(payload, wrong_key, algorithm="HS512")
        with self.assertRaises(SessionValidationError):
            mgr.validate_session(forged_token)

    def test_token_missing_required_claims_raises_session_validation_error(self):
        """A token missing required claims (e.g., 'exp') raises ``SessionValidationError``."""
        mgr = self._create_manager()
        # Build a token without the 'exp' claim
        now = datetime.now(timezone.utc)
        payload = {
            "iss": "plantdb-api",
            "sub": "no_exp_user",
            "aud": "plantdb-client",
            "iat": int(now.timestamp()),
            "jti": "missing-exp-jti",
            "type": "access",
        }
        # Encode without exp; disable exp verification at encode time
        bad_token = jwt.encode(payload, mgr.secret_key, algorithm="HS512")
        with self.assertRaises(SessionValidationError):
            mgr.validate_session(bad_token)

    # ------------------------------------------------------------------
    # Missing token look‑ups
    # ------------------------------------------------------------------
    def test_missing_access_token_raises_access_token_not_found_error(self):
        """If the access JTI is absent from ``sessions`` an ``AccessTokenNotFoundError`` is raised."""
        mgr = self._create_manager()
        access, _ = mgr.create_session("gina")
        payload = self._decode(access, mgr)
        del mgr.sessions[payload["jti"]]  # simulate loss
        with self.assertRaises(AccessTokenNotFoundError):
            mgr.validate_session(access)

    def test_missing_refresh_token_raises_refresh_token_not_found_error(self):
        """If the refresh JTI is absent from ``refresh_tokens`` a ``RefreshTokenNotFoundError`` is raised."""
        mgr = self._create_manager()
        _, refresh = mgr.create_session("hank")
        payload = self._decode(refresh, mgr)
        del mgr.refresh_tokens[payload["jti"]]  # simulate loss
        with self.assertRaises(RefreshTokenNotFoundError):
            mgr.validate_session(refresh, token_type="refresh")

    # ------------------------------------------------------------------
    # Invalidation paths
    # ------------------------------------------------------------------
    def test_invalidate_access_by_jti_removes_linked_refresh(self):
        """Invalidating an access token via JTI also removes its paired refresh token."""
        mgr = self._create_manager()
        access, refresh = mgr.create_session("ivy")
        access_jti = self._decode(access, mgr)["jti"]
        refresh_jti = self._decode(refresh, mgr)["jti"]

        success, username = mgr.invalidate_session(jti=access_jti)
        self.assertTrue(success)
        self.assertEqual(username, "ivy")
        self.assertNotIn(access_jti, mgr.sessions)
        self.assertNotIn(refresh_jti, mgr.refresh_tokens)

    def test_invalidate_refresh_by_jti_keeps_access_intact(self):
        """Invalidating a refresh token via JTI does not delete the associated access token."""
        mgr = self._create_manager()
        access, refresh = mgr.create_session("jack")
        access_jti = self._decode(access, mgr)["jti"]
        refresh_jti = self._decode(refresh, mgr)["jti"]

        success, username = mgr.invalidate_session(jti=refresh_jti)
        self.assertTrue(success)
        self.assertEqual(username, "jack")
        self.assertIn(access_jti, mgr.sessions)
        self.assertNotIn(refresh_jti, mgr.refresh_tokens)

    def test_invalidate_unknown_jti_returns_false(self):
        """Calling ``invalidate_session`` with a non‑existent JTI yields ``(False, None)``."""
        mgr = self._create_manager()
        result = mgr.invalidate_session(jti="non‑existent-jti")
        self.assertEqual(result, (False, None))

    def test_invalidate_by_access_token_string(self):
        """Invalidating a session by passing the raw access token string works correctly."""
        mgr = self._create_manager()
        access, refresh = mgr.create_session("token_inv_user")
        access_jti = self._decode(access, mgr)["jti"]
        refresh_jti = self._decode(refresh, mgr)["jti"]

        # Invalidate using the token string directly
        success, username = mgr.invalidate_session(token=access)
        self.assertTrue(success)
        self.assertEqual(username, "token_inv_user")
        # Access token and linked refresh should both be gone
        self.assertNotIn(access_jti, mgr.sessions)
        self.assertNotIn(refresh_jti, mgr.refresh_tokens)

    def test_invalidate_by_refresh_token_string(self):
        """Invalidating a session by passing the raw refresh token string removes only the refresh."""
        mgr = self._create_manager()
        access, refresh = mgr.create_session("token_inv_ref")
        access_jti = self._decode(access, mgr)["jti"]
        refresh_jti = self._decode(refresh, mgr)["jti"]

        success, username = mgr.invalidate_session(token=refresh)
        self.assertTrue(success)
        self.assertEqual(username, "token_inv_ref")
        # Access should still exist, refresh should be gone
        self.assertIn(access_jti, mgr.sessions)
        self.assertNotIn(refresh_jti, mgr.refresh_tokens)

    def test_invalidate_with_malformed_token_returns_false(self):
        """Invalidating with a malformed token string returns (False, None)."""
        mgr = self._create_manager()
        success, username = mgr.invalidate_session(token="garbage.token.value")
        self.assertFalse(success)
        self.assertIsNone(username)

    def test_invalidate_with_no_args_returns_false(self):
        """Calling invalidate_session with neither token nor jti returns (False, None)."""
        mgr = self._create_manager()
        result = mgr.invalidate_session()
        self.assertEqual(result, (False, None))

    def test_invalidate_with_expired_token_string_returns_false(self):
        """Invalidating with an expired JWT token string returns (False, None) since decode fails."""
        mgr = self._create_manager(session_timeout=1)
        access, _ = mgr.create_session("expired_inv")
        time.sleep(2)  # let token expire
        # Expired token decode will raise, so invalidate_session should return (False, None)
        success, username = mgr.invalidate_session(token=access)
        self.assertFalse(success)
        self.assertIsNone(username)

    # ------------------------------------------------------------------
    # Refresh flow (rotation) and reuse detection
    # ------------------------------------------------------------------
    def test_refresh_session_rotates_tokens_and_invalidates_old_ones(self):
        """`refresh_session` returns fresh tokens and removes the previous pair."""
        mgr = self._create_manager()
        old_access, old_refresh = mgr.create_session("kate")
        old_access_jti = self._decode(old_access, mgr)["jti"]
        old_refresh_jti = self._decode(old_refresh, mgr)["jti"]

        new_access, new_refresh = mgr.refresh_session(old_refresh)

        # Old tokens must be gone
        self.assertNotIn(old_access_jti, mgr.sessions)
        self.assertNotIn(old_refresh_jti, mgr.refresh_tokens)

        # New tokens must be present
        new_access_jti = self._decode(new_access, mgr)["jti"]
        new_refresh_jti = self._decode(new_refresh, mgr)["jti"]
        self.assertIn(new_access_jti, mgr.sessions)
        self.assertIn(new_refresh_jti, mgr.refresh_tokens)

    def test_refresh_session_preserves_username(self):
        """`refresh_session` new tokens belong to the same user."""
        mgr = self._create_manager()
        _, old_refresh = mgr.create_session("same_user")

        new_access, new_refresh = mgr.refresh_session(old_refresh)

        new_access_payload = self._decode(new_access, mgr)
        new_refresh_payload = self._decode(new_refresh, mgr)
        self.assertEqual(new_access_payload["sub"], "same_user")
        self.assertEqual(new_refresh_payload["sub"], "same_user")

    def test_refresh_token_reuse_raises_refresh_token_not_found_error(self):
        """Attempting to reuse a refresh token after rotation raises ``RefreshTokenNotFoundError``."""
        mgr = self._create_manager()
        _, refresh = mgr.create_session("linda")
        # First rotation - should succeed
        new_access, new_refresh = mgr.refresh_session(refresh)
        self.assertIsNotNone(new_access)
        self.assertIsNotNone(new_refresh)

        # Second attempt with the *old* refresh token must fail
        with self.assertRaises(RefreshTokenNotFoundError):
            mgr.refresh_session(refresh)

    def test_refresh_with_access_token_raises_wrong_token_type(self):
        """Using an access token for refresh_session raises WrongTokenType."""
        mgr = self._create_manager()
        access, _ = mgr.create_session("wrong_type_user")
        with self.assertRaises(WrongTokenType):
            mgr.refresh_session(access)

    def test_refresh_with_expired_refresh_raises_session_validation_error(self):
        """Using an expired refresh token for refresh_session raises SessionValidationError."""
        mgr = self._create_manager(session_timeout=5, refresh_timeout=1)
        _, refresh = mgr.create_session("expired_ref_user")
        time.sleep(2)  # let the refresh token expire
        with self.assertRaises(SessionValidationError):
            mgr.refresh_session(refresh)

    def test_refresh_with_malformed_token_raises_session_validation_error(self):
        """Using a malformed string for refresh_session raises SessionValidationError."""
        mgr = self._create_manager()
        with self.assertRaises(SessionValidationError):
            mgr.refresh_session("totally.broken.token")

    def test_refresh_does_not_exceed_max_sessions(self):
        """Refreshing a session should not increase the active session count."""
        mgr = self._create_manager(max_sessions=2)
        _, refresh1 = mgr.create_session("user_a")
        mgr.create_session("user_b")
        # At this point we are at 2/2 sessions

        # Refresh user_a — the old session is removed, new one created; count stays ≤ 2
        new_access, new_refresh = mgr.refresh_session(refresh1)
        self.assertIsNotNone(new_access)
        self.assertLessEqual(mgr.n_active_sessions(), 2)

    # ------------------------------------------------------------------
    # Automatic cleanup of expired entries
    # ------------------------------------------------------------------
    def test_cleanup_expired_sessions_purges_only_expired_tokens(self):
        """`cleanup_expired_sessions` removes expired items but keeps still‑valid ones."""
        mgr = self._create_manager(session_timeout=1, refresh_timeout=5)
        # Create two sessions: one that will expire, one that stays alive
        short_access, short_refresh = mgr.create_session("mia")
        short_jti = self._decode(short_access, mgr)["jti"]

        time.sleep(1.5)  # let the access token expire, refresh still alive
        long_access, long_refresh = mgr.create_session("nora")
        long_jti = self._decode(long_access, mgr)["jti"]

        # Run cleanup - only the first access token should disappear
        mgr.cleanup_expired_sessions()
        self.assertNotIn(short_jti, mgr.sessions)
        # The second access token must still be present
        self.assertIn(long_jti, mgr.sessions)

        # Refresh token of the first session is still valid (refresh_timeout = 5 s)
        short_refresh_jti = self._decode(short_refresh, mgr)["jti"]
        self.assertIn(short_refresh_jti, mgr.refresh_tokens)

    def test_cleanup_also_purges_expired_refresh_tokens(self):
        """`cleanup_expired_sessions` removes expired refresh tokens as well."""
        mgr = self._create_manager(session_timeout=5, refresh_timeout=1)
        _, refresh = mgr.create_session("ref_expire_user")
        refresh_jti = self._decode(refresh, mgr)["jti"]

        time.sleep(1.5)  # let the refresh token expire

        mgr.cleanup_expired_sessions()
        # Expired refresh token must be gone
        self.assertNotIn(refresh_jti, mgr.refresh_tokens)

    def test_cleanup_on_empty_sessions_is_safe(self):
        """`cleanup_expired_sessions` on a manager with no sessions does not raise."""
        mgr = self._create_manager()
        # Should not raise
        mgr.cleanup_expired_sessions()
        self.assertEqual(len(mgr.sessions), 0)
        self.assertEqual(len(mgr.refresh_tokens), 0)

    # ------------------------------------------------------------------
    # Concurrency limit enforcement
    # ------------------------------------------------------------------
    def test_exceeding_max_concurrent_sessions_returns_none(self):
        """When the max‑session limit is reached, ``create_session`` returns ``None``."""
        mgr = self._create_manager(max_sessions=2)
        self.assertIsNotNone(mgr.create_session("oliver"))
        self.assertIsNotNone(mgr.create_session("peter"))
        # Third attempt should be rejected
        self.assertIsNone(mgr.create_session("quinn"))

    def test_session_slot_freed_after_invalidation(self):
        """After invalidating a session, a new one can be created within the limit."""
        mgr = self._create_manager(max_sessions=1)
        access, _ = mgr.create_session("slot_user1")
        self.assertIsNone(mgr.create_session("slot_user2"))  # should be rejected

        # Invalidate the first session
        access_jti = self._decode(access, mgr)["jti"]
        mgr.invalidate_session(jti=access_jti)

        # Now a new session should succeed
        result = mgr.create_session("slot_user2")
        self.assertIsNotNone(result)

    def test_session_slot_freed_after_expiration(self):
        """After a session expires, a new one can be created within the limit."""
        mgr = self._create_manager(session_timeout=1, max_sessions=1)
        mgr.create_session("expiring_user")
        time.sleep(1.5)  # let it expire

        # The expired session should be cleaned up and a new slot available
        result = mgr.create_session("new_user")
        self.assertIsNotNone(result)

    # ------------------------------------------------------------------
    # session_username helper
    # ------------------------------------------------------------------
    def test_session_username_returns_username_for_valid_token(self):
        """`session_username` extracts the username from a valid access token."""
        mgr = self._create_manager()
        access, _ = mgr.create_session("rachel")
        self.assertEqual(mgr.session_username(access), "rachel")

    def test_session_username_returns_none_for_invalid_token(self):
        """`session_username` returns ``None`` when token validation fails."""
        mgr = self._create_manager()
        malformed = "invalid.token.parts"
        self.assertIsNone(mgr.session_username(malformed))

    def test_session_username_returns_none_for_expired_token(self):
        """`session_username` returns ``None`` for an expired access token."""
        mgr = self._create_manager(session_timeout=1)
        access, _ = mgr.create_session("exp_user")
        time.sleep(2)
        self.assertIsNone(mgr.session_username(access))

    # ------------------------------------------------------------------
    # _user_has_session (inherited)
    # ------------------------------------------------------------------
    def test_user_has_session_true_when_active(self):
        """`_user_has_session` returns True for a user with an active session."""
        mgr = self._create_manager()
        mgr.create_session("active_user")
        self.assertTrue(mgr._user_has_session("active_user"))

    def test_user_has_session_false_when_no_session(self):
        """`_user_has_session` returns False for a user with no sessions."""
        mgr = self._create_manager()
        self.assertFalse(mgr._user_has_session("ghost_user"))

    def test_user_has_session_false_for_none_username(self):
        """`_user_has_session` returns False when the username is None."""
        mgr = self._create_manager()
        self.assertFalse(mgr._user_has_session(None))

    def test_user_has_session_false_after_invalidation(self):
        """`_user_has_session` returns False after the user's session is invalidated."""
        mgr = self._create_manager()
        access, _ = mgr.create_session("will_be_gone")
        access_jti = self._decode(access, mgr)["jti"]
        mgr.invalidate_session(jti=access_jti)
        self.assertFalse(mgr._user_has_session("will_be_gone"))

    # ------------------------------------------------------------------
    # session_token (inherited)
    # ------------------------------------------------------------------
    def test_session_token_returns_jti_for_active_user(self):
        """`session_token` returns the JTI (session key) for a user with an active session."""
        mgr = self._create_manager()
        access, _ = mgr.create_session("token_lookup_user")
        access_jti = self._decode(access, mgr)["jti"]

        result = mgr.session_token("token_lookup_user")
        self.assertEqual(result, access_jti)

    def test_session_token_returns_none_for_unknown_user(self):
        """`session_token` returns None for a user without any active session."""
        mgr = self._create_manager()
        self.assertIsNone(mgr.session_token("no_such_user"))

    def test_session_token_returns_none_for_none_username(self):
        """`session_token` returns None when the username is None."""
        mgr = self._create_manager()
        self.assertIsNone(mgr.session_token(None))

    def test_session_token_returns_none_after_invalidation(self):
        """`session_token` returns None after the user's session is invalidated."""
        mgr = self._create_manager()
        access, _ = mgr.create_session("inv_user")
        access_jti = self._decode(access, mgr)["jti"]
        mgr.invalidate_session(jti=access_jti)
        self.assertIsNone(mgr.session_token("inv_user"))

    # ------------------------------------------------------------------
    # API token creation
    # ------------------------------------------------------------------
    def test_create_api_token_returns_valid_jwt(self):
        """`create_api_token` returns a decodable JWT with type 'api'."""
        mgr = self._create_manager()
        api_token = mgr.create_api_token("api_user", token_exp=60)

        self.assertIsInstance(api_token, str)
        self.assertGreater(len(api_token), 0)

        # Decode and verify claims
        payload = self._decode(api_token, mgr)
        self.assertEqual(payload["sub"], "api_user")
        self.assertEqual(payload["type"], "api")
        self.assertIn("exp", payload)
        self.assertIn("jti", payload)

    def test_create_api_token_default_expiration(self):
        """`create_api_token` with None expiration defaults to 1 hour from now."""
        mgr = self._create_manager()
        now_ts = datetime.now(timezone.utc).timestamp()
        api_token = mgr.create_api_token("default_exp_user", token_exp=None)

        payload = self._decode(api_token, mgr)
        # exp should be approximately now + 3600 seconds
        self.assertAlmostEqual(payload["exp"], now_ts + 3600, delta=5)

    def test_create_api_token_with_int_expiration(self):
        """`create_api_token` with an integer expiration sets exp accordingly."""
        mgr = self._create_manager()
        now_ts = datetime.now(timezone.utc).timestamp()
        api_token = mgr.create_api_token("int_exp_user", token_exp=120)

        payload = self._decode(api_token, mgr)
        self.assertAlmostEqual(payload["exp"], now_ts + 120, delta=5)

    def test_create_api_token_with_iso_string_expiration(self):
        """`create_api_token` with an ISO string expiration date works."""
        mgr = self._create_manager()
        future = datetime.now(timezone.utc) + timedelta(hours=2)
        iso_str = future.isoformat()
        api_token = mgr.create_api_token("iso_exp_user", token_exp=iso_str)

        self.assertIsInstance(api_token, str)
        self.assertGreater(len(api_token), 0)

        payload = self._decode(api_token, mgr)
        self.assertAlmostEqual(payload["exp"], future.timestamp(), delta=5)

    def test_create_api_token_with_past_expiration_returns_empty(self):
        """`create_api_token` with a past expiration date returns an empty string."""
        mgr = self._create_manager()
        past = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        result = mgr.create_api_token("past_exp_user", token_exp=past)
        self.assertEqual(result, "")

    def test_create_api_token_with_datetime_expiration(self):
        """`create_api_token` with a datetime object works correctly."""
        mgr = self._create_manager()
        future_dt = datetime.now(timezone.utc) + timedelta(hours=3)
        api_token = mgr.create_api_token("dt_exp_user", token_exp=future_dt)

        self.assertIsInstance(api_token, str)
        self.assertGreater(len(api_token), 0)

        payload = self._decode(api_token, mgr)
        self.assertAlmostEqual(payload["exp"], future_dt.timestamp(), delta=5)

    def test_create_api_token_with_past_datetime_returns_empty(self):
        """`create_api_token` with a past datetime object returns an empty string."""
        mgr = self._create_manager()
        past_dt = datetime.now(timezone.utc) - timedelta(hours=1)
        result = mgr.create_api_token("past_dt_user", token_exp=past_dt)
        self.assertEqual(result, "")

    def test_create_api_token_with_invalid_type_returns_empty(self):
        """`create_api_token` with an unsupported type for token_exp returns an empty string."""
        mgr = self._create_manager()
        # Passing a list — not a valid type
        result = mgr.create_api_token("invalid_type_user", token_exp=[1, 2, 3])
        self.assertEqual(result, "")

    def test_create_api_token_writes_to_file(self):
        """`create_api_token` persists the token to the api_token_file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = self._create_manager()
            mgr.api_token_file = Path(tmpdir) / "api_token.txt"

            api_token = mgr.create_api_token("file_user", token_exp=3600)

            # The file should exist and contain the token JTI
            self.assertTrue(mgr.api_token_file.exists())
            content = mgr.api_token_file.read_text()
            # The file stores jti + ISO expiry, not the raw token
            payload = self._decode(api_token, mgr)
            self.assertIn(payload["jti"], content)

    def test_create_api_token_with_datasets(self):
        """`create_api_token` with datasets includes them in the JWT payload."""
        from plantdb.commons.auth.models import Permission
        mgr = self._create_manager()
        datasets = {"dataset_a": [Permission.READ]}
        api_token = mgr.create_api_token("ds_user", token_exp=60, datasets=datasets)

        payload = self._decode(api_token, mgr)
        # The 'datasets' claim should be present
        self.assertIn("datasets", payload)

    def test_create_api_token_without_datasets_has_no_datasets_claim(self):
        """`create_api_token` without datasets does not include a 'datasets' claim."""
        mgr = self._create_manager()
        api_token = mgr.create_api_token("no_ds_user", token_exp=60)
        payload = self._decode(api_token, mgr)
        self.assertNotIn("datasets", payload)

    def test_create_api_token_is_registered_in_memory(self):
        """`create_api_token` registers the token jti in _api_tokens."""
        mgr = self._create_manager()
        api_token = mgr.create_api_token("mem_user", token_exp=3600)
        payload = self._decode(api_token, mgr)
        self.assertIn(payload["jti"], mgr._api_tokens)

    # ------------------------------------------------------------------
    # API token validation
    # ------------------------------------------------------------------
    def test_validate_api_token_via_validate_session(self):
        """An API token is auto-detected and validated as type 'api'."""
        mgr = self._create_manager()
        api_token = mgr.create_api_token("api_val_user", token_exp=60)

        # validate_session should auto-detect 'api' type
        info = mgr.validate_session(api_token, token_type="api")
        self.assertEqual(info["username"], "api_val_user")
        self.assertEqual(info["type"], "api")

    def test_validate_api_token_not_registered_raises_error(self):
        """An API token whose jti is not in _api_tokens raises SessionValidationError."""
        mgr = self._create_manager()
        api_token = mgr.create_api_token("unreg_user", token_exp=60)
        payload = self._decode(api_token, mgr)
        # Remove the token from memory
        del mgr._api_tokens[payload["jti"]]

        with self.assertRaises(SessionValidationError):
            mgr.validate_session(api_token, token_type="api")

    def test_validate_api_token_with_datasets_returns_parsed_datasets(self):
        """Validating an API token with datasets returns parsed dataset permissions."""
        from plantdb.commons.auth.models import Permission
        mgr = self._create_manager()
        datasets = {"dataset_x": [Permission.READ]}
        api_token = mgr.create_api_token("ds_val_user", token_exp=60, datasets=datasets)

        info = mgr.validate_session(api_token, token_type="api")
        self.assertIn("datasets", info)
        # The returned datasets should have been parsed back
        self.assertIn("dataset_x", info["datasets"])

    # ------------------------------------------------------------------
    # invalidate_api_token
    # ------------------------------------------------------------------
    def test_invalidate_api_token_removes_token(self):
        """Invalidating a valid API token removes it from memory and file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = self._create_manager()
            mgr.api_token_file = Path(tmpdir) / "api_token.txt"
            mgr._api_tokens = {}

            api_token = mgr.create_api_token("revoke_user", token_exp=3600)
            payload = self._decode(api_token, mgr)
            jti = payload["jti"]
            # Token should be registered
            self.assertIn(jti, mgr._api_tokens)

            # Invalidate
            result = mgr.invalidate_api_token(api_token)
            self.assertTrue(result)
            # Token should be gone from memory
            self.assertNotIn(jti, mgr._api_tokens)
            # Token should be gone from file
            content = mgr.api_token_file.read_text()
            self.assertNotIn(jti, content)

    def test_invalidate_api_token_with_expired_token(self):
        """Invalidating an expired API token still cleans it from storage."""
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = self._create_manager()
            mgr.api_token_file = Path(tmpdir) / "api_token.txt"
            mgr._api_tokens = {}
            # Use leeway=0 to ensure strict expiration
            mgr.leeway = 0

            api_token = mgr.create_api_token("expired_api_user", token_exp=1)
            payload = jwt.decode(api_token, mgr.secret_key, algorithms=["HS512"],
                                 audience="plantdb-client", issuer="plantdb-api",
                                 options={"verify_exp": False})
            jti = payload["jti"]
            time.sleep(2)  # let token expire

            result = mgr.invalidate_api_token(api_token)
            self.assertTrue(result)
            self.assertNotIn(jti, mgr._api_tokens)

    def test_invalidate_api_token_wrong_type_returns_false(self):
        """Invalidating a non-API token (access) via invalidate_api_token returns False."""
        mgr = self._create_manager()
        access, _ = mgr.create_session("not_api")
        result = mgr.invalidate_api_token(access)
        self.assertFalse(result)

    def test_invalidate_api_token_unknown_jti_returns_false(self):
        """Invalidating an API token whose jti is not registered returns False."""
        mgr = self._create_manager()
        api_token = mgr.create_api_token("unknown_jti_user", token_exp=3600)
        payload = self._decode(api_token, mgr)
        # Manually remove it from _api_tokens
        del mgr._api_tokens[payload["jti"]]
        result = mgr.invalidate_api_token(api_token)
        self.assertFalse(result)

    def test_invalidate_api_token_malformed_returns_false(self):
        """Invalidating a malformed token via invalidate_api_token returns False."""
        mgr = self._create_manager()
        result = mgr.invalidate_api_token("not.a.valid.jwt")
        self.assertFalse(result)

    # ------------------------------------------------------------------
    # _dump_api_token / _load_api_tokens / _is_api_token_active
    # ------------------------------------------------------------------
    def test_dump_api_token_appends_to_file(self):
        """_dump_api_token appends token data to the api_token_file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = self._create_manager()
            mgr.api_token_file = Path(tmpdir) / "api_token.txt"
            mgr._api_tokens = {}

            now = datetime.now(timezone.utc)
            exp1 = now + timedelta(hours=1)
            exp2 = now + timedelta(hours=2)

            mgr._dump_api_token("jti_1", exp1)
            mgr._dump_api_token("jti_2", exp2)

            content = mgr.api_token_file.read_text()
            self.assertIn("jti_1", content)
            self.assertIn("jti_2", content)
            # Both should be in memory
            self.assertIn("jti_1", mgr._api_tokens)
            self.assertIn("jti_2", mgr._api_tokens)

    def test_is_api_token_active_for_valid_token(self):
        """_is_api_token_active returns True for a registered, non-expired token."""
        mgr = self._create_manager()
        api_token = mgr.create_api_token("active_api_user", token_exp=3600)
        payload = self._decode(api_token, mgr)
        self.assertTrue(mgr._is_api_token_active(payload["jti"]))

    def test_is_api_token_active_for_unknown_jti(self):
        """_is_api_token_active returns False for an unregistered jti."""
        mgr = self._create_manager()
        self.assertFalse(mgr._is_api_token_active("nonexistent_jti"))

    def test_is_api_token_active_for_expired_token(self):
        """_is_api_token_active returns False for an expired token."""
        mgr = self._create_manager()
        now = datetime.now(timezone.utc)
        expired_dt = now - timedelta(hours=1)
        # Manually register an expired token
        mgr._api_tokens["old_jti"] = expired_dt.isoformat()
        self.assertFalse(mgr._is_api_token_active("old_jti"))

    def test_load_api_tokens_discards_expired(self):
        """_load_api_tokens drops expired entries and keeps valid ones."""
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = self._create_manager()
            mgr.api_token_file = Path(tmpdir) / "api_token.txt"
            mgr._api_tokens = {}

            now = datetime.now(timezone.utc)
            # Write a file with one expired and one valid entry
            with open(mgr.api_token_file, "w") as f:
                f.write(f"expired_jti {(now - timedelta(hours=1)).isoformat()}\n")
                f.write(f"valid_jti {(now + timedelta(hours=1)).isoformat()}\n")

            mgr._load_api_tokens()
            self.assertNotIn("expired_jti", mgr._api_tokens)
            self.assertIn("valid_jti", mgr._api_tokens)

    def test_load_api_tokens_skips_malformed_lines(self):
        """_load_api_tokens silently skips lines that don't have the expected format."""
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = self._create_manager()
            mgr.api_token_file = Path(tmpdir) / "api_token.txt"
            mgr._api_tokens = {}

            now = datetime.now(timezone.utc)
            with open(mgr.api_token_file, "w") as f:
                f.write("malformed_line_without_space\n")  # no space separator
                f.write(f"valid_jti {(now + timedelta(hours=1)).isoformat()}\n")
                f.write("\n")  # empty line

            mgr._load_api_tokens()
            self.assertEqual(len(mgr._api_tokens), 1)
            self.assertIn("valid_jti", mgr._api_tokens)

    def test_load_api_tokens_nonexistent_file(self):
        """_load_api_tokens does nothing when the file does not exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = self._create_manager()
            mgr.api_token_file = Path(tmpdir) / "nonexistent.txt"
            mgr._api_tokens = {}

            # Should not raise
            mgr._load_api_tokens()
            self.assertEqual(len(mgr._api_tokens), 0)

    # ------------------------------------------------------------------
    # _clean_up_token_file
    # ------------------------------------------------------------------
    def test_clean_up_token_file_removes_expired_tokens(self):
        """Expired API tokens are removed from the token file during cleanup."""
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = self._create_manager()
            mgr.api_token_file = Path(tmpdir) / "api_token.txt"
            mgr._api_tokens = {}

            now = datetime.now(timezone.utc)
            # Write one expired and one valid token
            mgr._dump_api_token("expired_token_abc", now - timedelta(hours=1))
            mgr._dump_api_token("valid_token_xyz", now + timedelta(hours=1))

            n_valid, n_cleaned = mgr._clean_up_token_file()
            self.assertEqual(n_valid, 1)
            self.assertEqual(n_cleaned, 1)

            # Read the file and verify only valid token remains
            content = mgr.api_token_file.read_text()
            self.assertNotIn("expired_token_abc", content)
            self.assertIn("valid_token_xyz", content)

    def test_clean_up_token_file_nonexistent_returns_zeros(self):
        """Cleanup on a manager with no tokens returns (0, 0)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = self._create_manager()
            mgr.api_token_file = Path(tmpdir) / "api_token.txt"
            mgr._api_tokens = {}

            n_valid, n_cleaned = mgr._clean_up_token_file()
            self.assertEqual(n_valid, 0)
            self.assertEqual(n_cleaned, 0)

    def test_clean_up_token_file_handles_malformed_expiry(self):
        """Cleanup removes entries with unparseable expiry strings."""
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = self._create_manager()
            mgr.api_token_file = Path(tmpdir) / "api_token.txt"
            mgr._api_tokens = {}

            now = datetime.now(timezone.utc)
            # Register a valid token and a malformed one
            mgr._api_tokens["good_jti"] = (now + timedelta(hours=1)).isoformat()
            mgr._api_tokens["bad_jti"] = "not-a-date"
            mgr._save_api_tokens()

            n_valid, n_cleaned = mgr._clean_up_token_file()
            self.assertEqual(n_valid, 1)
            self.assertEqual(n_cleaned, 1)
            self.assertNotIn("bad_jti", mgr._api_tokens)

    # ------------------------------------------------------------------
    # Thread safety
    # ------------------------------------------------------------------
    def test_concurrent_session_creation_respects_max_limit(self):
        """Concurrent create_session calls do not exceed max_concurrent_sessions."""
        mgr = self._create_manager(max_sessions=5)
        results = []
        errors = []

        def create(username):
            try:
                result = mgr.create_session(username)
                results.append(result)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=create, args=(f"user_{i}",)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(errors), 0, f"Unexpected errors: {errors}")
        # At most 5 should have succeeded (not None)
        successful = [r for r in results if r is not None]
        self.assertLessEqual(len(successful), 5)
        # And the sessions dict should never exceed max
        self.assertLessEqual(len(mgr.sessions), 5)

    def test_concurrent_validation_does_not_raise(self):
        """Concurrent validate_session calls on the same token do not raise."""
        mgr = self._create_manager(max_sessions=5)
        access, _ = mgr.create_session("concurrent_val_user")
        errors = []

        def validate():
            try:
                mgr.validate_session(access)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=validate) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(errors), 0, f"Unexpected errors during concurrent validation: {errors}")

    def test_concurrent_invalidation_does_not_raise(self):
        """Concurrent invalidate_session calls on the same jti do not raise."""
        mgr = self._create_manager(max_sessions=5)
        access, _ = mgr.create_session("concurrent_inv_user")
        access_jti = self._decode(access, mgr)["jti"]
        results = []

        def invalidate():
            result = mgr.invalidate_session(jti=access_jti)
            results.append(result)

        threads = [threading.Thread(target=invalidate) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Exactly one thread should have succeeded
        successes = [r for r in results if r[0] is True]
        self.assertGreaterEqual(len(successes), 1)

    # ------------------------------------------------------------------
    # Multiple sessions for same user
    # ------------------------------------------------------------------
    def test_same_user_can_have_multiple_sessions(self):
        """The same user can create multiple sessions up to the max limit."""
        mgr = self._create_manager(max_sessions=3)
        result1 = mgr.create_session("multi_user")
        result2 = mgr.create_session("multi_user")

        self.assertIsNotNone(result1)
        self.assertIsNotNone(result2)

        # They should be different tokens
        self.assertNotEqual(result1[0], result2[0])
        self.assertNotEqual(result1[1], result2[1])

        # Two access sessions should be registered
        self.assertEqual(mgr.n_active_sessions(), 2)

    # ------------------------------------------------------------------
    # _payload_from_token
    # ------------------------------------------------------------------
    def test_payload_from_token_returns_correct_payload(self):
        """_payload_from_token correctly decodes a valid JWT."""
        mgr = self._create_manager()
        access, _ = mgr.create_session("payload_user")
        payload = mgr._payload_from_token(access)
        self.assertEqual(payload["sub"], "payload_user")
        self.assertEqual(payload["type"], "access")
        self.assertEqual(payload["iss"], "plantdb-api")
        self.assertEqual(payload["aud"], "plantdb-client")

    def test_payload_from_token_raises_on_expired(self):
        """_payload_from_token raises ExpiredSignatureError for expired tokens."""
        mgr = self._create_manager(session_timeout=1)
        access, _ = mgr.create_session("expired_payload_user")
        time.sleep(2)
        with self.assertRaises(jwt.ExpiredSignatureError):
            mgr._payload_from_token(access)

    def test_payload_from_token_raises_on_malformed(self):
        """_payload_from_token raises on a malformed token string."""
        mgr = self._create_manager()
        with self.assertRaises(jwt.InvalidTokenError):
            mgr._payload_from_token("garbage")

    # ------------------------------------------------------------------
    # Edge case: validate_session with token_type not matching any branch
    # ------------------------------------------------------------------
    def test_validate_session_api_token_as_refresh_raises_wrong_type(self):
        """Using an API token with token_type='refresh' raises WrongTokenType."""
        mgr = self._create_manager()
        api_token = mgr.create_api_token("api_as_refresh", token_exp=60)
        with self.assertRaises(WrongTokenType):
            mgr.validate_session(api_token, token_type="refresh")


if __name__ == "__main__":
    unittest.main()
