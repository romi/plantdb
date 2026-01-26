#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Session Management

Provides a flexible session management system supporting both standard and JWT-based
sessions. It handles session creation, validation, expiration, concurrency limits,
and refresh logic, making it suitable for web applications and database
connections.

Key Features
------------
- Centralized session store with expiration tracking
- Support for plain token and JWT tokens with standard claims
- Concurrency control (max concurrent sessions)
- Automatic cleanup of expired sessions
- Session refresh mechanism to extend validity

Usage Examples
--------------
>>> from plantdb.commons.auth.session import JWTSessionManager
>>> manager = JWTSessionManager(session_timeout=1800, secret_key='my_secret')
>>> token = manager.create_session('alice')
>>> user_info = manager.validate_session(token)
>>> print(user_info)
{'username': 'alice', 'issued_at': 1769011058, 'expires_at': 1769012858, 'jti': 'HVaAR4XHmIJgCKbZMDqmwg', 'issuer': 'plantdb-api', 'audience': 'plantdb-client'}
>>> new_token = manager.refresh_session(token)
"""

import secrets
from datetime import datetime
from datetime import timedelta
from typing import Any
from typing import Dict
from typing import Optional
from typing import Tuple
from typing import Union

import jwt
from plantdb.commons.log import get_logger


class SessionManager:
    """
    Manages user sessions with expiration and validation.

    This class provides methods to create, validate, invalidate,
    and cleanup expired sessions. Each session is associated with a
    unique identifier (session_id) and has an expiry time based on the
    session timeout duration specified during initialization.

    Attributes
    ----------
    sessions : Dict[str, dict]
        A dictionary storing active sessions.
        Each key is a session ID, and each value is a dictionary containing:
        - 'username': str - The user associated with this session.
        - 'created_at': datetime - When the session was created.
        - 'last_accessed': datetime - Last time the session was accessed.
        - 'expires_at': datetime - Expiry time of the session.
    session_timeout : int
        Duration in seconds after which a session expires.
    max_concurrent_sessions : int
        The maximum number of concurrent sessions to allow.
    logger : logging.Logger
        The logger to use for this session manager.
    """

    def __init__(self, session_timeout: int = 3600, max_concurrent_sessions: int = 10):  # 1 hour default
        """
        Manage user sessions with timeout.

        Parameters
        ----------
        session_timeout : int, optional
            The duration for which the session should be valid in seconds.
            A session that exceeds this duration will be considered expired and removed.
            Defaults to ``3600`` seconds.
        max_concurrent_sessions : int, optional
            The maximum number of concurrent sessions to allow.
            Defaults to ``10``.
        """
        self.sessions: Dict[str, dict] = {}
        self.session_timeout = session_timeout
        self.max_concurrent_sessions = max_concurrent_sessions

        self.logger = get_logger(__class__.__name__)

    def _user_has_session(self, username) -> bool:
        """
        Check if a user has an active session.

        Parameters
        ----------
        username : str
            The unique identifier for the user whose active session status needs to be checked.

        Returns
        -------
        bool
            ``True`` if the user has an active session, ``False`` otherwise.
        """
        self.cleanup_expired_sessions()
        if username is not None:
            for _, session in self.sessions.items():
                if session['username'] == username:
                    return True
        return False

    def n_active_sessions(self) -> int:
        """
        Returns the number of active sessions.

        Cleans up expired sessions before counting and returns the number
        of remaining active sessions in the collection.

        Returns
        -------
        int
            The number of currently active sessions.

        See Also
        --------
        cleanup_expired_sessions : Cleans up the expired sessions in the collection.
        """
        self.cleanup_expired_sessions()
        return len(self.sessions)

    def _create_token(self, **kwargs) -> str:
        """
        Generate a secure random token.

        Returns
        -------
        str
            A securely generated (URL-safe) token.
        """
        return secrets.token_urlsafe(32)

    def create_session(self, username: str) -> Union[str, None]:
        """
        Create a new session for a user.

        If the user already has an active session, it returns the existing session ID.
        Otherwise, it creates a new session and returns its ID.

        Parameters
        ----------
        username : str
            The unique identifier of the user for whom to create a session.

        Returns
        -------
        Union[str, None]
            The ID of the created or existing session.

        Notes
        -----
        The session ID is a token generated using `secrets.token_urlsafe`.
        The session data includes the user ID, creation timestamp, last accessed timestamp, and expiration timestamp.
        """
        if self._user_has_session(username):
            self.logger.warning(f"User '{username}' already has an active session!")
            return None

        if self.n_active_sessions() >= self.max_concurrent_sessions:
            self.logger.warning(
                f"Reached max concurrent sessions limit ({self.max_concurrent_sessions})")
            return None

        now = datetime.now()
        exp_time = now + timedelta(seconds=self.session_timeout)
        # Create a session token
        session_token = secrets.token_urlsafe(32)

        self.sessions[session_token] = {
            'username': username,
            'created_at': now,
            'last_accessed': now,
            'expires_at': exp_time
        }
        return session_token

    def validate_session(self, session_id: str) -> Optional[dict]:
        """
        Validate a given session by checking its existence and expiration status.

        Parameters
        ----------
        session_id : str
            The unique identifier of the session to be validated.

        Returns
        -------
        dict or None
            A dictionary with user information if valid, ``None`` if invalid/expired.
            Returns dictionary with:
            - username: The authenticated user
            - created_at: When the session was created
            - last_accessed: When the session was last validated
            - expires_at: When the session expires

        Notes
        -----
        The `validate_session` method updates the session's last accessed time upon successful validation.
        """
        if session_id not in self.sessions:
            self.logger.warning(f"Provided session does not exist!")
            return None

        session = self.sessions[session_id]
        now = datetime.now()
        if now > session['expires_at']:
            username = session['username']
            self.logger.warning(f"The session for user '{username}' has expired. Please log back in!")
            success, username = self.invalidate_session(session_id)
            return None

        # Update last accessed time
        session['last_accessed'] = now
        return session

    def invalidate_session(self, session_id: str) -> Tuple[bool, str | None]:
        """
        Remove the given session identifier from the active sessions.

        Parameters
        ----------
        session_id : str
            The unique identifier of the session to be removed.

        Returns
        -------
        bool
            `True` if the specified session was found and removed, `False` otherwise.
        str
            The username corresponding to the invalidated session

        Notes
        -----
        The session ID is removed from the internal session dictionary.
        If the session does not exist, this method has no effect.
        """
        if session_id in self.sessions:
            username = self.sessions[session_id]['username']
            del self.sessions[session_id]
            return True, username

        return False, None

    def cleanup_expired_sessions(self) -> None:
        """
        Remove expired sessions from the session dictionary.

        This method iterates through all stored sessions and deletes any that have
        an expiration time earlier than the current time.

        Notes
        -----
        This function modifies the `self.sessions` dictionary in-place.
        """
        current_time = datetime.now()
        expired_sessions = [
            sid for sid, session in self.sessions.items()
            if current_time > session['expires_at']
        ]
        for sid in expired_sessions:
            del self.sessions[sid]
        return

    def session_username(self, session_id: str) -> Optional[str]:
        """Return the username associated with a session."""
        session_data = self.validate_session(session_id)
        return session_data['username'] if session_data else None

    def refresh_session(self, session_id: str) -> Optional[str]:
        """
        Refresh a session if it's still valid.

        Parameters
        ----------
        session_id : str
            Current session token

        Returns
        -------
        str or None
            New session token if refresh successful
        """
        session_data = self.validate_session(session_id)
        if not session_data:
            return None

        # Invalidate old session
        self.invalidate_session(session_id)

        # Create new session
        username = session_data['username']
        return self.create_session(username)


class SingleSessionManager(SessionManager):
    """
    Generate a single-session manager for handling database connections.

    The `SingleSessionManager` class is designed to manage a single active
    database session at any given time. It inherits from the base `SessionManager`
    class and overrides its initialization to ensure only one concurrent session
    is allowed, even if the base class allows more.

    Parameters
    ----------
    session_timeout : int, optional
        The timeout duration for each database session in seconds.
        If not specified, defaults to 3600 (1 hour).

    Attributes
    ----------
    sessions : Dict[str, dict]
        A dictionary storing active sessions.
        Each key is a session ID, and each value is a dictionary containing:
        - 'username': str - The user associated with this session.
        - 'created_at': datetime - When the session was created.
        - 'last_accessed': datetime - Last time the session was accessed.
        - 'expires_at': datetime - Expiry time of the session.
    session_timeout : int
        The configured timeout duration for sessions.
    max_concurrent_sessions : int
        Always set to 1 to ensure only one concurrent session is allowed.
    logger : logging.Logger
        The logger to use for this session manager.

    Examples
    --------
    >>> from plantdb.commons.auth.session import SingleSessionManager
    >>> # Initialize the session manager
    >>> manager = SingleSessionManager()
    >>> # Create a new session with the username 'test'
    >>> session_token = manager.create_session('test')
    >>> # Attempt to create another session with the username 'test2'
    >>> _ = manager.create_session('test2')
    WARNING  [SessionManager] Reached max concurrent sessions limit (1)
    >>> # Validate the session and get its info
    >>> session = manager.validate_session(session_token)
    >>> print(session['expires_at'])  # Print the expiration date
    >>> # Refresh the session using the existing session token
    >>> new_session_token = manager.refresh_session(session_token)
    >>> # Validate the session and get its info
    >>> session = manager.validate_session(new_session_token)
    >>> print(session['expires_at'])  # Print the expiration date of the refreshed session

    Notes
    -----
    The `SingleSessionManager` enforces a single-session policy, which means any
    attempt to create more than one active session will result in an error or be
    handled according to the logic defined within this class.

    See Also
    --------
    session_manager.SessionManager : Base class for managing database sessions.
    """

    def __init__(self, session_timeout: int = 3600, **kwargs) -> None:
        super().__init__(session_timeout=session_timeout, max_concurrent_sessions=1)


class JWTSessionManager(SessionManager):
    """
    Manages user sessions with expiration and validation.

    This class provides methods to create, validate, invalidate,
    and cleanup expired sessions. Each session is associated with a
    unique identifier (session_id) and has an expiry time based on the
    session timeout duration specified during initialization.

    Attributes
    ----------
    sessions : Dict[str, dict]
        A dictionary storing active sessions.
        Each key is a session ID, and each value is a dictionary containing:
        - 'username': str - The user associated with this session.
        - 'created_at': datetime - When the session was created.
        - 'last_accessed': datetime - Last time the session was accessed.
        - 'expires_at': datetime - Expiry time of the session.
    session_timeout : int
        Duration in seconds after which a session expires.
    max_concurrent_sessions : int
        The maximum number of concurrent sessions to allow.
    secret_key : str
        The session manager secret key to use for authentication.
    logger : logging.Logger
        The logger to use for this session manager.
    """

    def __init__(self, session_timeout: int = 3600, max_concurrent_sessions: int = 10, secret_key: str = None):
        """
        Manage user sessions with timeout.

        Parameters
        ----------
        session_timeout : int, optional
            The duration for which the session should be valid in seconds.
            A session that exceeds this duration will be considered expired and removed.
            Defaults to ``3600`` seconds.
        max_concurrent_sessions : int, optional
            The maximum number of concurrent sessions to allow.
            Defaults to ``10``.
        secret_key : str, optional
            Secret key for JWT signing. If None, generates a random key.
        """
        super().__init__(session_timeout, max_concurrent_sessions)
        self.secret_key = secret_key or secrets.token_urlsafe(32)

    def _create_token(self, username, jti, exp_time, now):
        """
        Create a JSON Web Token (JWT) with registered claims.

        Generates and encodes a JWT using the provided username, unique identifier
        (jti), expiration time, and current time. The token includes standard
        registered claims as defined in RFC 7519.

        Parameters
        ----------
        username : str
            The subject of the JWT (user identifier).
        jti : str
            Unique identifier for the JWT.
        exp_time : datetime.datetime
            Expiration time of the JWT.
        now : datetime.datetime
            Current time when the JWT is issued.

        Returns
        -------
        str
            A string representation of the encoded JWT.

        See Also
        --------
        jwt.encode : Encode a payload into a JWT.
        """
        # Create a JWT payload with registered claims
        payload = {
            # Registered claims (RFC 7519)
            'iss': 'plantdb-api',  # issuer
            'sub': username,  # subject (user identifier)
            'aud': 'plantdb-client',  # audience
            'exp': int(exp_time.timestamp()),  # expiration time (Unix timestamp)
            'iat': int(now.timestamp()),  # issued at (Unix timestamp)
            'jti': jti  # JWT ID (unique identifier)
        }
        return jwt.encode(
            payload,
            self.secret_key,
            algorithm='HS512',
            headers={'typ': 'JWT', 'alg': 'HS512'}
        )

    def create_session(self, username: str) -> Union[str, None]:
        """
        Create a new session for a user.

        If the user already has an active session, it returns the existing session ID.
        Otherwise, it creates a new session and returns its ID.

        Parameters
        ----------
        username : str
            The unique identifier of the user for whom to create a session.

        Returns
        -------
        session_id : Union[str, None]
            The ID of the created or existing session.

        Notes
        -----
        Creates a JWT token following RFC 7519 standards with registered claims:
        - iss (issuer): Identifies the token issuer
        - sub (subject): The username of the authenticated user
        - aud (audience): Intended audience for the token
        - exp (expiration time): Token expiration timestamp
        - iat (issued at): Token creation timestamp
        - jti (JWT ID): Unique identifier for the token generated using `secrets.token_urlsafe`.
        """
        if self._user_has_session(username):
            self.logger.warning(f"User '{username}' already has an active session!")
            return None

        if self.n_active_sessions() >= self.max_concurrent_sessions:
            self.logger.warning(
                f"Too any users currently active, reached max concurrent sessions limit ({self.max_concurrent_sessions})")
            return None

        # Create a JWT payload with registered claims
        now = datetime.now()
        exp_time = now + timedelta(seconds=self.session_timeout)
        jti = secrets.token_urlsafe(16)  # unique token ID for tracking

        try:
            # Generate JWT token
            jwt_token = self._create_token(username, jti, exp_time, now)
        except Exception as e:
            self.logger.error(f"Failed to create JWT token for {username}: {e}")
            return None

        # Track session for concurrent limit enforcement
        self.sessions[jti] = {
            'username': username,
            'created_at': now,
            'last_accessed': now,
            'expires_at': exp_time
        }
        self.logger.debug(f"Created JWT token for '{username}'")
        return jwt_token

    def _payload_from_token(self, jwt_token: str) -> dict:
        """
        Decode the payload from a JWT token.

        This function decodes the JSON Web Token (JWT) using the specified secret key and
        verifies the token's audience and issuer. It returns the decoded payload as a dictionary.

        Parameters
        ----------
        jwt_token : str
            The JWT token to decode.

        Returns
        -------
        dict
            The decoded payload from the JWT token.

        Notes
        -----
        The JWT token must be correctly formatted and signed using the specified secret key.
        If the token is invalid or the signature does not match, a `jwt.ExpiredSignatureError`,
        `jwt.InvalidTokenError`, or `jwt.DecodeError` may be raised.

        See Also
        --------
        jwt.decode : Decodes the JSON Web Token.
        """
        return jwt.decode(
            jwt_token,
            self.secret_key,
            algorithms=['HS512'],
            audience='plantdb-client',  # Verify audience
            issuer='plantdb-api'  # Verify issuer
        )

    def validate_session(self, jwt_token: str) -> Optional[Dict[str, Any]]:
        """
        Validate a JWT token and return user information.

        Parameters
        ----------
        jwt_token : str
            The JWT token to validate.

        Returns
        -------
        dict or None
            User information if valid, ``None`` if invalid/expired.
            Returns dictionary with:
            - username: The authenticated user
            - issued_at: When the token was issued
            - expires_at: When the token expires
            - jti: Unique token identifier
            - issuer: Token issuer
            - audience: Token audience
        """
        try:
            # Decode and verify JWT token with proper validation
            payload = self._payload_from_token(jwt_token)

        except jwt.ExpiredSignatureError:
            self.logger.error("JWT token expired")
            return None
        except jwt.InvalidAudienceError:
            self.logger.error("JWT token has invalid audience")
            return None
        except jwt.InvalidIssuerError:
            self.logger.error("JWT token has invalid issuer")
            return None
        except jwt.InvalidTokenError as e:
            self.logger.error(f"Invalid JWT token: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error validating JWT token: {e}")
            return None

        # Update last accessed time in session tracking
        jti = payload.get('jti')
        if jti and jti in self.sessions:
            self.sessions[jti]['last_accessed'] = datetime.now()

        return {
            'username': payload['sub'],  # subject is the username
            'issued_at': payload['iat'],  # issued at timestamp
            'expires_at': payload['exp'],  # expiration timestamp
            'jti': jti,  # JWT ID
            'issuer': payload['iss'],  # issuer
            'audience': payload['aud']  # audience
        }

    def invalidate_session(self, jwt_token: str = None, jti: str = None) -> Tuple[bool, str | None]:
        """
        Invalidate a session by removing it from tracking.

        Parameters
        ----------
        jwt_token : str, optional
            JWT token to invalidate
        jti : str, optional
            Token ID to invalidate directly

        Returns
        -------
        bool
            `True` if the specified session was found and removed, `False` otherwise.
        str
            The username corresponding to the invalidated JWT token
        """
        if jwt_token:
            try:
                payload = self._payload_from_token(jwt_token)
                jti = payload.get('jti')
            except:
                return False, None

        if jti and jti in self.sessions:
            username = self.sessions[jti]['username']
            del self.sessions[jti]
            return True, username

        return False, None

    def cleanup_expired_sessions(self) -> None:
        """Remove expired sessions from tracking."""
        current_time = datetime.now()
        expired_sessions = [
            jti for jti, session in self.sessions.items()
            if current_time > session['expires_at']
        ]
        for jti in expired_sessions:
            del self.sessions[jti]
        return

    def session_username(self, jwt_token: str) -> Optional[str]:
        """
        Extract username from JWT token.

        Parameters
        ----------
        jwt_token : str
            JWT token

        Returns
        -------
        str or None
            Username if token is valid
        """
        session_data = self.validate_session(jwt_token)
        return session_data['username'] if session_data else None

    def refresh_session(self, jwt_token: str) -> Optional[str]:
        """
        Refresh a JWT token if it's still valid.

        Parameters
        ----------
        jwt_token : str
            Current JWT token

        Returns
        -------
        str or None
            New JWT token if refresh successful
        """
        session_data = self.validate_session(jwt_token)
        if not session_data:
            return None

        # Invalidate old session
        old_jti = session_data['jti']
        self.invalidate_session(jti=old_jti)

        # Create new session
        username = session_data['username']
        return self.create_session(username)
