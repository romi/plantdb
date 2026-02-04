#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import unittest
from datetime import datetime
from datetime import timezone

import jwt

from plantdb.commons.auth.session import AccessTokenNotFoundError
from plantdb.commons.auth.session import JWTSessionManager
from plantdb.commons.auth.session import RefreshTokenNotFoundError
from plantdb.commons.auth.session import SessionManager
from plantdb.commons.auth.session import SessionValidationError
from plantdb.commons.auth.session import WrongTokenType


class TestSessionManager(unittest.TestCase):
    """Test cases for SessionManager class"""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.session_manager = SessionManager(session_timeout=3600)  # 1 hour timeout

    def test_init_creates_empty_sessions_dict(self):
        """Test that SessionManager initializes with empty sessions dictionary."""
        # Verify initial state is correct
        self.assertEqual(len(self.session_manager.sessions), 0)
        self.assertEqual(self.session_manager.session_timeout, 3600)
        self.assertIsNotNone(self.session_manager.logger)

    def test_user_has_session_returns_consistent_id(self):
        """Test that _user_has_session returns consistent session IDs for same user."""
        # Test session ID generation
        session_id1 = self.session_manager._user_has_session("testuser")
        session_id2 = self.session_manager._user_has_session("testuser")

        # Session IDs should be consistent for same user at same time
        self.assertEqual(session_id1, session_id2)

    def test_create_session_stores_session_data(self):
        """Test that create_session properly stores session with expiry time."""
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

        # Wait for session to expire
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

        # Create first session
        session1_id = temp_session_manager.create_session("user1")

        # Wait for a moment
        time.sleep(0.6)  # Wait for first session to expire

        # Create second session
        session2_id = temp_session_manager.create_session("user2")

        # Cleanup expired sessions
        temp_session_manager.cleanup_expired_sessions()

        # Only expired session should be removed
        self.assertNotIn(session1_id, temp_session_manager.sessions)
        self.assertIn(session2_id, temp_session_manager.sessions)

    def test_session_username_returns_correct_username(self):
        """Test that session_username returns the correct username for valid session."""
        # Create session
        session_id = self.session_manager.create_session("testuser")

        # Get username from session
        username = self.session_manager.session_username(session_id)
        self.assertEqual(username, "testuser")

    def test_session_username_returns_none_for_invalid_session(self):
        """Test that session_username returns None for invalid session ID."""
        username = self.session_manager.session_username("invalid_session")
        self.assertIsNone(username)


class TestJWTSessionManager(unittest.TestCase):
    """Comprehensive unit tests for :class:`JWTSessionManager`."""

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

    # ------------------------------------------------------------------
    # Basic session creation
    # ------------------------------------------------------------------
    def test_create_session_returns_two_tokens_and_registers_them(self):
        """`create_session` should return an (access, refresh) tuple and store both."""
        mgr = self._create_manager()
        result = mgr.create_session("alice")

        # Verify we got a 2- of strings
        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 2)
        access_token, refresh_token = result
        self.assertIsInstance(access_token, str)
        self.assertIsInstance(refresh_token, str)

        # Decode payloads just to verify they contain the expected fields
        access_payload = jwt.decode(
            access_token,
            mgr.secret_key,
            algorithms=["HS512"],
            audience="plantdb-client",
            issuer="plantdb-api",
        )
        refresh_payload = jwt.decode(
            refresh_token,
            mgr.secret_key,
            algorithms=["HS512"],
            audience="plantdb-client",
            issuer="plantdb-api",
        )
        self.assertEqual(access_payload["sub"], "alice")
        self.assertEqual(refresh_payload["sub"], "alice")
        self.assertEqual(access_payload["type"], "access")
        self.assertEqual(refresh_payload["type"], "refresh")

        # The manager must have stored the JTI of both tokens
        self.assertIn(access_payload["jti"], mgr.sessions)
        self.assertIn(refresh_payload["jti"], mgr.refresh_tokens)

    # ------------------------------------------------------------------
    # Validation of access / refresh tokens
    # ------------------------------------------------------------------
    def test_validate_access_token_returns_payload_and_updates_last_accessed(self):
        """`validate_session` for an access token returns a dict and updates its timestamp."""
        mgr = self._create_manager()
        access, _ = mgr.create_session("bob")
        before = mgr.sessions[jwt.decode(
            access,
            mgr.secret_key,
            algorithms=["HS512"],
            audience="plantdb-client",
            issuer="plantdb-api",
        )["jti"]]["last_accessed"]

        info = mgr.validate_session(access)  # defaults to token_type='access'
        self.assertEqual(info["username"], "bob")
        self.assertEqual(info["type"], "access")
        # ``last_accessed`` must be later than the previous value
        after = mgr.sessions[info["jti"]]["last_accessed"]
        self.assertGreater(after, before)

    def test_validate_refresh_token_returns_payload_without_touching_access(self):
        """`validate_session` for a refresh token returns a dict and does not affect access dict."""
        mgr = self._create_manager()
        _, refresh = mgr.create_session("carol")
        payload = jwt.decode(
            refresh,
            mgr.secret_key,
            algorithms=["HS512"],
            audience="plantdb-client",
            issuer="plantdb-api",
        )
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

    # ------------------------------------------------------------------
    # Missing token look‑ups
    # ------------------------------------------------------------------
    def test_missing_access_token_raises_access_token_not_found_error(self):
        """If the access JTI is absent from ``sessions`` an ``AccessTokenNotFoundError`` is raised."""
        mgr = self._create_manager()
        access, _ = mgr.create_session("gina")
        payload = jwt.decode(
            access,
            mgr.secret_key,
            algorithms=["HS512"],
            audience="plantdb-client",
            issuer="plantdb-api",
        )
        del mgr.sessions[payload["jti"]]  # simulate loss
        with self.assertRaises(AccessTokenNotFoundError):
            mgr.validate_session(access)

    def test_missing_refresh_token_raises_refresh_token_not_found_error(self):
        """If the refresh JTI is absent from ``refresh_tokens`` an ``RefreshTokenNotFoundError`` is raised."""
        mgr = self._create_manager()
        _, refresh = mgr.create_session("hank")
        payload = jwt.decode(
            refresh,
            mgr.secret_key,
            algorithms=["HS512"],
            audience="plantdb-client",
            issuer="plantdb-api",
        )
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
        access_jti = jwt.decode(
            access,
            mgr.secret_key,
            algorithms=["HS512"],
            audience="plantdb-client",
            issuer="plantdb-api",
        )["jti"]
        refresh_jti = jwt.decode(
            refresh,
            mgr.secret_key,
            algorithms=["HS512"],
            audience="plantdb-client",
            issuer="plantdb-api",
        )["jti"]

        success, username = mgr.invalidate_session(jti=access_jti)
        self.assertTrue(success)
        self.assertEqual(username, "ivy")
        self.assertNotIn(access_jti, mgr.sessions)
        self.assertNotIn(refresh_jti, mgr.refresh_tokens)

    def test_invalidate_refresh_by_jti_keeps_access_intact(self):
        """Invalidating a refresh token via JTI does not delete the associated access token."""
        mgr = self._create_manager()
        access, refresh = mgr.create_session("jack")
        access_jti = jwt.decode(
            access,
            mgr.secret_key,
            algorithms=["HS512"],
            audience="plantdb-client",
            issuer="plantdb-api",
        )["jti"]
        refresh_jti = jwt.decode(
            refresh,
            mgr.secret_key,
            algorithms=["HS512"],
            audience="plantdb-client",
            issuer="plantdb-api",
        )["jti"]

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

    # ------------------------------------------------------------------
    # Refresh flow (rotation) and reuse detection
    # ------------------------------------------------------------------
    def test_refresh_session_rotates_tokens_and_invalidates_old_ones(self):
        """`refresh_session` returns fresh tokens and removes the previous pair."""
        mgr = self._create_manager()
        old_access, old_refresh = mgr.create_session("kate")
        old_access_jti = jwt.decode(
            old_access,
            mgr.secret_key,
            algorithms=["HS512"],
            audience="plantdb-client",
            issuer="plantdb-api",
        )["jti"]
        old_refresh_jti = jwt.decode(
            old_refresh,
            mgr.secret_key,
            algorithms=["HS512"],
            audience="plantdb-client",
            issuer="plantdb-api",
        )["jti"]

        new_access, new_refresh = mgr.refresh_session(old_refresh)

        # Old tokens must be gone
        self.assertNotIn(old_access_jti, mgr.sessions)
        self.assertNotIn(old_refresh_jti, mgr.refresh_tokens)

        # New tokens must be present
        new_access_jti = jwt.decode(
            new_access,
            mgr.secret_key,
            algorithms=["HS512"],
            audience="plantdb-client",
            issuer="plantdb-api",
        )["jti"]
        new_refresh_jti = jwt.decode(
            new_refresh,
            mgr.secret_key,
            algorithms=["HS512"],
            audience="plantdb-client",
            issuer="plantdb-api",
        )["jti"]
        self.assertIn(new_access_jti, mgr.sessions)
        self.assertIn(new_refresh_jti, mgr.refresh_tokens)

    def test_refresh_token_reuse_raises_refresh_token_reuse_error(self):
        """Attempting to reuse a refresh token after rotation raises ``RefreshTokenReuseError``."""
        mgr = self._create_manager()
        _, refresh = mgr.create_session("linda")
        # First rotation – should succeed
        new_access, new_refresh = mgr.refresh_session(refresh)
        self.assertIsNotNone(new_access)
        self.assertIsNotNone(new_refresh)

        # Second attempt with the *old* refresh token must fail
        with self.assertRaises(RefreshTokenNotFoundError):
            mgr.refresh_session(refresh)

    # ------------------------------------------------------------------
    # Automatic cleanup of expired entries
    # ------------------------------------------------------------------
    def test_cleanup_expired_sessions_purges_only_expired_tokens(self):
        """`cleanup_expired_sessions` removes expired items but keeps still‑valid ones."""
        mgr = self._create_manager(session_timeout=1, refresh_timeout=5)
        # Create two sessions: one that will expire, one that stays alive
        short_access, short_refresh = mgr.create_session("mia")
        short_jti = jwt.decode(
            short_access,
            mgr.secret_key,
            algorithms=["HS512"],
            audience="plantdb-client",
            issuer="plantdb-api",
        )["jti"]

        time.sleep(1.5)  # let the access token expire, refresh still alive
        long_access, long_refresh = mgr.create_session("nora")
        long_jti = jwt.decode(
            long_access,
            mgr.secret_key,
            algorithms=["HS512"],
            audience="plantdb-client",
            issuer="plantdb-api",
        )["jti"]

        # Run cleanup – only the first access token should disappear
        mgr.cleanup_expired_sessions()
        self.assertNotIn(short_jti, mgr.sessions)
        # The second access token must still be present
        self.assertIn(long_jti, mgr.sessions)

        # Refresh token of the first session is still valid (refresh_timeout = 3 s)
        short_refresh_jti = jwt.decode(
            short_refresh,
            mgr.secret_key,
            algorithms=["HS512"],
            audience="plantdb-client",
            issuer="plantdb-api",
        )["jti"]
        self.assertIn(short_refresh_jti, mgr.refresh_tokens)

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


if __name__ == "__main__":
    unittest.main()
