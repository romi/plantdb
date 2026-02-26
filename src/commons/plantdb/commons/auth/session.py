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
- Support for plain token and JSON Web Tokens with standard claims
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
import threading
import time
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from tempfile import gettempdir
from threading import RLock
from typing import Any
from typing import Dict
from typing import Optional
from typing import Tuple
from typing import Union

import jwt
from argon2 import Type
from argon2.low_level import hash_secret_raw

from plantdb.commons.auth.models import Permission
from plantdb.commons.auth.models import dataset_perm_to_str
from plantdb.commons.auth.models import parse_dataset_perm
from plantdb.commons.log import get_logger


# ----------------------------------------------------------------------
# Custom exception hierarchy for session validation
# ----------------------------------------------------------------------
class SessionValidationError(Exception):
    """Base class for all session‑validation‑related errors."""
    pass


class AccessTokenNotFoundError(SessionValidationError):
    """Raised when an access token isn’t present in the active‑session store."""
    pass


class RefreshTokenNotFoundError(SessionValidationError):
    """Raised when a refresh token isn’t present in the active‑refresh‑store."""
    pass


class InvalidTokenProcessingError(SessionValidationError):
    """Raised for unexpected errors while processing a token (e.g. decoding issues)."""
    pass


class RefreshTokenReuseError(SessionValidationError):
    pass


class WrongTokenType(SessionValidationError):
    pass


class SessionManager:
    """Manages user sessions with expiration and validation.

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

    def __init__(self, session_timeout: int = 3600, max_concurrent_sessions: int = 10):
        """Manage user sessions with timeout.

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
        """Check if a user has an active session.

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
        if username:
            for _, session in self.sessions.items():
                if session['username'] == username:
                    return True
        return False

    def n_active_sessions(self) -> int:
        """Returns the number of active sessions.

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
        """Generate a secure random token.

        Returns
        -------
        str
            A securely generated (URL-safe) token.
        """
        return secrets.token_urlsafe(32)

    def create_session(self, username: str) -> Union[str, None]:
        """Create a new session for a user.

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
        if self.n_active_sessions() >= self.max_concurrent_sessions:
            self.logger.warning(
                f"Reached max concurrent sessions limit ({self.max_concurrent_sessions})")
            return None

        now = datetime.now(timezone.utc)
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

    def validate_session(self, session_id: str) -> Union[dict, None]:
        """Validate a given session by checking its existence and expiration status.

        Parameters
        ----------
        session_id : str
            The unique identifier of the session to be validated.

        Returns
        -------
        Union[dict, None]
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
        now = datetime.now(timezone.utc)
        if now > session['expires_at']:
            username = session['username']
            self.logger.warning(f"The session for user '{username}' has expired. Please log back in!")
            success, username = self.invalidate_session(session_id)
            return None

        # Update last accessed time
        session['last_accessed'] = now
        return session

    def invalidate_session(self, session_id: str) -> Tuple[bool, str | None]:
        """Remove the given session identifier from the active sessions.

        Parameters
        ----------
        session_id : str
            The unique identifier of the session to be removed.

        Returns
        -------
        bool
            ``True`` if the specified session was found and removed, ``False`` otherwise.
        Union[str, None]
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
        """Remove expired sessions from the session dictionary.

        This method iterates through all stored sessions and deletes any that have
        an expiration time earlier than the current time.

        Notes
        -----
        This function modifies the `self.sessions` dictionary in-place.
        """
        current_time = datetime.now(timezone.utc)
        expired_sessions = [
            sid for sid, session in self.sessions.items()
            if current_time > session['expires_at']
        ]
        for sid in expired_sessions:
            del self.sessions[sid]
        return

    def session_username(self, session_id: str) -> Optional[str]:
        """Retrieve the username associated with a given session ID.

        The method validates the supplied session ID by delegating to `validate_session`.
        If the session is active, the username stored in the session data is returned;
        otherwise ``None`` is returned.

        Parameters
        ----------
        session_id
            The unique identifier for the session to query.

        Returns
        -------
        Optional[str]
            The username linked to the session, or ``None`` if the
            session is not found or is invalid.
        """
        session_data = self.validate_session(session_id)
        return session_data['username'] if session_data else None

    def session_token(self, username) -> Optional[str]:
        """Retrieve the active session token, if any, for a given username.

        This method cleans up any expired sessions first and then searches the internal
        ``sessions`` attribute dictionary for a session belonging to the supplied username.

        Parameters
        ----------
        username : str
            The username whose session ID is requested.

        Returns
        -------
        Optional[str]
            The session ID associated with `username` if an active session exists; otherwise, ``None``.
        """
        self.cleanup_expired_sessions()
        if username:
            for session_id, session in self.sessions.items():
                if session['username'] == username:
                    return session_id
        return None

    def refresh_session(self, session_id: str) -> Optional[str]:
        """Refresh a session if it's still valid.

        Parameters
        ----------
        session_id : str
            Current session token

        Returns
        -------
        str or None
            New session token if refresh is successful
        """
        session_data = self.validate_session(session_id)
        if not session_data:
            return None

        # Invalidate old session
        self.invalidate_session(session_id)

        # Create a new session
        username = session_data['username']
        return self.create_session(username)


class SingleSessionManager(SessionManager):
    """Generate a single-session manager for handling database connections.

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


def _derive_key_argon2(password: str) -> bytes:
    """Derive a 64‑byte (512‑bit) secret using Argon2.

    Parameters
    ----------
    password: str
        Human‑readable pass‑phrase supplied by the caller.

    Returns
    -------
    bytes
        64‑byte key suitable for HS512.
    """
    # Argon2 parameters – adjust if you need stronger/higher‑memory settings
    time_cost = 2  # number of iterations
    memory_cost = 102_400  # KiB (≈100 MiB)
    parallelism = 8  # CPU lanes
    salt = secrets.token_bytes(16)  # 128‑bit random salt; stored only in‑memory here
    # hash_secret_raw returns raw bytes (no encoding)
    return hash_secret_raw(
        secret=password.encode('utf‑8'),
        salt=salt,
        time_cost=time_cost,
        memory_cost=memory_cost,
        parallelism=parallelism,
        hash_len=64,  # 64 bytes = 512 bits
        type=Type.ID,
    )


def _init_secret_key(secret_key: Union[str, bytes] = None) -> bytes:
    """Generate or derive a 64‑byte secret key for HS512 signing.

    Parameters
    ----------
    secret_key: Union[str, bytes, None]
        Optional secret key.

    Returns
    -------
    bytes
        A 64‑byte key suitable for HS512 HMAC operations.

    Raises
    ------
    ValueError
        When ``secret_key`` is a ``bytes`` object shorter than 64 bytes.

    Notes
    -----
    The function always returns exactly 64 bytes.
    If ``secret_key`` is ``None``, a fresh random 64‑byte key is generated using ``secrets.token_bytes``.
    If a ``bytes`` object is supplied, it is returned unchanged after verifying that its length is at
    least 64 bytes; otherwise a ``ValueError`` is raised.
    If a ``str`` is supplied, it is interpreted as a pass‑phrase and stretched to 64 bytes with Argon2
    via `_derive_key_argon2`.

    Examples
    --------
    >>> from plantdb.commons.auth.session import _init_secret_key
    >>> # Generate a new random key
    >>> key = _init_secret_key()
    >>> len(key)
    64
    >>> # Use an existing 64‑byte key
    >>> raw = b'A' * 64
    >>> _init_secret_key(raw) is raw
    True
    >>> # Derive a key from a pass‑phrase
    >>> key2 = _init_secret_key('my secret')
    >>> len(key2)
    64
    >>> # Passing a short byte string raises an error
    >>> _init_secret_key(b'short')
    ValueError: Binary secret_key must be at least 64 bytes for HS512
    """
    if secret_key is None:
        # No key supplied → generate a fresh random 64‑byte key
        secret_key = secrets.token_bytes(64)
    elif isinstance(secret_key, bytes):
        # Caller supplied raw bytes – just verify length
        if len(secret_key) < 64:
            raise ValueError("Binary `secret_key` must be at least 64 bytes for HS512")
        secret_key = secret_key
    else:
        # Caller supplied a pass‑phrase string → stretch it with Argon2
        secret_key = _derive_key_argon2(secret_key)
    return secret_key


class JWTSessionManager(SessionManager):
    """Manage JWT-based user sessions with configurable timeouts and concurrency limits.

    This session manager extends `SessionManager` by issuing JSON Web Tokens (JWT) for authentication.
    An *access* token is short‑lived and is used for authorizing API calls, while a *refresh* token
    is long‑lived and can be exchanged for a new access token when the original expires.
    An *api* token is long‑lived and adds dataset-specific rights to the token with a custom ``datasets`` claims.
    The manager keeps track of active access tokens to enforce a maximum number of concurrent
    sessions per application instance. Tokens are signed with a secret key that is either supplied by
    the caller or generated automatically. All tokens conform to RFC7519 and contain the standard
    registered claims (`iss`, `sub`, `aud`, `exp`, `iat`, `jti`) plus a custom ``type`` claim that
    identifies the token as ``'access'``, ``'api'`` or ``'refresh'``.

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
        The default value (``900``) corresponds to 15 minutes.
    max_concurrent_sessions : int
        The maximum number of concurrent sessions to allow.
    logger : logging.Logger
        The logger to use for this session manager.
    refresh_timeout : int
        Lifetime of a refresh token in seconds.
        The default value (``86400``) corresponds to 24 hours.
    secret_key : Union[str, bytes, None]
        Secret used for HS512 signing of JWTs.
        If ``None`` is passed to the constructor, a random 64‑byte key is generated.
        This will break the API tokens persistence across restarts as, after a restart, the new instance
        gets a different secret key, so all previously persisted API tokens in the file become unverifiable.
    refresh_tokens : dict
        Mapping from refresh token identifier (``jti``) to a dictionary containing ``username``,
        creation and expiration timestamps, token type and the associated access token identifier.
        Used to validate and rotate refresh tokens.
    _lock : threading.Lock
        A locking mechanism to lock `self.session` dict for thread‑safe changes
    api_token_dir : str or pathlib.Path
        Directory where the ``api_token.txt`` file will be stored.
    """

    def __init__(self, session_timeout: int = 900, refresh_timeout: int = 86400, max_concurrent_sessions: int = 10,
                 secret_key: Union[str, bytes] = None, leeway: int = 2, api_token_dir=gettempdir()):
        """Manage user sessions with timeout.

        Parameters
        ----------
        session_timeout : int, optional
            The duration for which the access token should be valid in seconds.
            Defaults to ``900`` seconds (15 minutes).
        refresh_timeout : int, optional
            The duration for which the refresh token should be valid in seconds.
            Defaults to ``86400`` seconds (24 hours).
        max_concurrent_sessions : int, optional
            The maximum number of concurrent sessions to allow.
            Defaults to ``10``.
        secret_key : Union[str, bytes, None]
           Secret used for HS512 signing of JWTs.
           - If a ``bytes`` object is supplied, it must be ≥ 64 bytes.
           - If a ``str`` (pass‑phrase) is supplied, it will be stretched with Argon2 to produce a 64‑byte key.
           - If ``None`` (default) a fresh random 64‑byte key is generated.
             This will break the API tokens persistence across restarts as, after a restart, the new instance
             gets a different secret key, so all previously persisted API tokens in the file become unverifiable.
        leeway : int, optional
            Allowed leeway, in seconds, after tokens expiration date, to accommodate for clock-skew.
            Set it to `0` so that the token is considered expired immediately after its exp claim passes.
            Defaults to ``2``.
        api_token_dir : str or pathlib.Path, optional
            Directory where the ``api_token.txt`` file will be stored.
            Defaults to the system temporary directory.
        """
        super().__init__(session_timeout, max_concurrent_sessions)
        self.refresh_timeout = refresh_timeout
        self.leeway = leeway
        self.refresh_tokens = {}  # Track valid refresh tokens (jti -> session_info)
        self._lock = RLock()  # to lock `self.session` dict for thread‑safe changes

        self.secret_key = self._init_secret_key(secret_key)
        self.api_token_file = Path(api_token_dir) / 'api_token.txt'
        self._api_tokens: dict[str, str] = {}  # jti -> ISO expiry string
        self._safe_secret_init(secret_key)

        # Load any existing API tokens from disk
        self._load_api_tokens()

        # ---- Start the daily clean‑up thread ----
        self._start_daily_cleanup_thread(hour=3, minute=0, tz=timezone.utc)

    def _safe_secret_init(self, secret_key):
        """Verifies if the secret key used for signing API tokens is random and nullify an existing api_token_file."""
        if secret_key is None:
            if self.api_token_file.exists():
                self.logger.error("Got an existing API token file with a randomly generated secret key.")
                self.logger.warning(f"Removing existing API token file at: {self.api_token_file}")
                self.api_token_file.unlink()
            else:
                self.logger.warning(
                    "Using a randomly generated secret key for token signature will render all saved API token useless at restart!")
            self.logger.info("Set a secret key value to avoid loosing API tokens.")
            self.logger.info("Use the 'JWT_SECRET_KEY' environment variable if you are using the `fsdb_rest_api` CLI.")

    def _init_secret_key(self, secret_key: Union[str, bytes] = None) -> bytes:
        """Initialize or validate the secret key used for cryptographic operations.

        Parameters
        ----------
        secret_key : Union[str, bytes, None]
            Optional user‑provided secret key as a string.
            When ``None`` a fresh random key is created.

        Returns
        -------
        bytes
            The secret key encoded as UTF‑8 bytes, or a newly generated random
            key when no input is given.

        See Also
        --------
        plantdb.commons.auth.session._init_secret_key
        """
        return _init_secret_key(secret_key)

    def _create_token(self, username, jti, exp_time, now, token_type='access', **kwargs):
        """Create a JSON Web Token (JWT) with registered claims.

        Generates and encodes a JWT using the provided username, unique identifier
        (jti), expiration time, and current time. The token includes standard
        registered claims as defined in RFC 7519.

        Parameters
        ----------
        username : str
            The subject of the JWT (user identifier).
        jti : str
            Unique identifier for the JWT.
        exp_time : datetime
            Expiration time of the JWT.
        now : datetime
            Current time when the JWT is issued.
        token_type : str, optional
            The type of token to create ('access', 'api' or 'refresh').
            Defaults to 'access'.

        Other Parameters
        ----------------
        datasets : dict[str, list[Permission]]
            A dict of dataset with given permissions as ``{'dataset_A': [Permission.READ], 'dataset_B': [Permission.READ, Permission.CREATE]}``.

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
            'jti': jti,  # JWT ID (unique identifier)
            'type': token_type  # Custom claim for token type
        }

        if 'datasets' in kwargs and kwargs['datasets']:
            payload['datasets'] = dataset_perm_to_str(kwargs['datasets'])

        token_bytes = jwt.encode(
            payload,
            self.secret_key,
            algorithm='HS512',
            headers={'typ': 'JWT', 'alg': 'HS512'}
        )
        return token_bytes if isinstance(token_bytes, str) else token_bytes.decode('utf-8')

    def create_session(self, username: str) -> Union[Tuple[str, str], None]:
        """Create a new session for a user.

        If the user already has an active session, it returns the existing session ID.
        Otherwise, it creates a new session and returns its ID.

        Parameters
        ----------
        username : str
            The unique identifier of the user for whom to create a session.

        Returns
        -------
        Tuple[str, str] or None
            A tuple containing (access_token, refresh_token) if successful, ``None`` otherwise.

        Notes
        -----
        Creates JSON Web Tokens following RFC 7519 standards with registered claims:
        - iss (issuer): Identifies the token issuer
        - sub (subject): The username of the authenticated user
        - aud (audience): Intended audience for the token
        - exp (expiration time): Token expiration timestamp
        - iat (issued at): Token creation timestamp
        - jti (JWT ID): Unique identifier for the token generated using `secrets.token_urlsafe`.
        """
        if self.n_active_sessions() >= self.max_concurrent_sessions:
            self.logger.warning(
                f"Too many users currently active, reached max concurrent sessions limit ({self.max_concurrent_sessions})")
            return None

        # Create an access token
        now = datetime.now(timezone.utc)
        access_exp = now + timedelta(seconds=self.session_timeout)
        access_jti = secrets.token_urlsafe(16)

        # Create a refresh token
        refresh_exp = now + timedelta(seconds=self.refresh_timeout)
        refresh_jti = secrets.token_urlsafe(16)

        try:
            # Generate JSON Web Tokens
            access_token = self._create_token(username, access_jti, access_exp, now, token_type='access')
            refresh_token = self._create_token(username, refresh_jti, refresh_exp, now, token_type='refresh')
        except Exception as e:
            self.logger.error(f"Failed to create JSON Web Tokens for {username}: {e}")
            return None

        with self._lock:
            # Track access session for concurrent‑limit enforcement
            self.sessions[access_jti] = {
                'username': username,
                'created_at': now,
                'last_accessed': now,
                'expires_at': access_exp,
                'type': 'access'
            }

            # Track refresh token
            self.refresh_tokens[refresh_jti] = {
                'username': username,
                'created_at': now,
                'expires_at': refresh_exp,
                'type': 'refresh',
                'access_jti': access_jti
            }

        self.logger.debug(f"Created session for '{username}'")
        return access_token, refresh_token

    def _load_api_tokens(self) -> None:
        """Load API tokens from disk into ``self._api_tokens``, discarding expired entries.

        Each line in the file is expected to have the format ``<jti> <ISO-8601 expiry>``.
        Entries that are expired or malformed are silently dropped.  The cleaned state is
        written back to disk so that the file stays tidy after every startup.
        """
        if not self.api_token_file.exists():
            return

        now = datetime.now(timezone.utc)
        loaded: dict[str, str] = {}

        with self._lock:
            with open(self.api_token_file, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    parts = line.split(" ", maxsplit=1)
                    if len(parts) != 2:
                        self.logger.warning(f"Skipping malformed line in token file: {line!r}")
                        continue
                    jti, exp_str = parts
                    try:
                        exp_dt = datetime.fromisoformat(exp_str)
                        if exp_dt.tzinfo is None:
                            exp_dt = exp_dt.replace(tzinfo=timezone.utc)
                    except ValueError:
                        self.logger.warning(f"Skipping token with unparseable expiry: {exp_str!r}")
                        continue
                    if exp_dt > now:
                        loaded[jti] = exp_str
                    else:
                        self.logger.debug(f"Discarding expired API token jti={jti} (expired {exp_str})")

            self._api_tokens = loaded
            # Persist the cleaned state back to disk
            self._save_api_tokens()

        self.logger.debug(f"Loaded {len(self._api_tokens)} valid API token(s) from {self.api_token_file}")

    def _save_api_tokens(self) -> None:
        """Persist the current ``self._api_tokens`` dict to disk.

        The caller is responsible for acquiring ``self._lock`` before calling this method.
        """
        with open(self.api_token_file, "w") as f:
            for jti, exp_str in self._api_tokens.items():
                f.write(f"{jti} {exp_str}\n")

    def _dump_api_token(self, jti: str, token_exp: datetime) -> None:
        """Register a new API token in memory and append it to the token file.

        Parameters
        ----------
        jti : str
            The unique token identifier.
        token_exp : datetime
            The expiration datetime of the token.
        """
        exp_str = token_exp.isoformat()
        with self._lock:
            self._api_tokens[jti] = exp_str
            # Append to file (avoids rewriting the whole file on every creation)
            with open(self.api_token_file, "a") as f:
                f.write(f"{jti} {exp_str}\n")

    def _is_api_token_active(self, jti: str) -> bool:
        """Check whether an API token is registered and not expired.

        Parameters
        ----------
        jti : str
            The unique token identifier to look up.

        Returns
        -------
        bool
            ``True`` if the token is known and its expiry is in the future.
        """
        exp_str = self._api_tokens.get(jti)
        if exp_str is None:
            return False
        exp_dt = datetime.fromisoformat(exp_str)
        if exp_dt.tzinfo is None:
            exp_dt = exp_dt.replace(tzinfo=timezone.utc)
        return exp_dt > datetime.now(timezone.utc)

    def _clean_up_token_file(self) -> Tuple[int, int]:
        """Remove expired API tokens from memory and rewrite the token file.

        Returns
        -------
        tuple[int, int]
            ``(n_valid, n_cleaned)`` — the number of tokens kept and the number removed.
        """
        now = datetime.now(timezone.utc)
        with self._lock:
            initial_count = len(self._api_tokens)
            expired_jtis = []
            # Identifies malformed or expired tokens for removal
            for jti, exp_str in self._api_tokens.items():
                try:
                    exp_dt = datetime.fromisoformat(exp_str)
                    if exp_dt.tzinfo is None:
                        exp_dt = exp_dt.replace(tzinfo=timezone.utc)
                except ValueError:
                    expired_jtis.append(jti)
                    continue
                if exp_dt <= now:
                    expired_jtis.append(jti)

            for jti in expired_jtis:
                self.logger.debug(f"Removing expired API token jti={jti}")
                del self._api_tokens[jti]

            self._save_api_tokens()

        n_valid = len(self._api_tokens)
        return n_valid, initial_count - n_valid

    def _start_daily_cleanup_thread(self, hour: int = 3, minute: int = 0, tz: timezone = timezone.utc) -> None:
        """Start a background daemon thread that clean up the token file every day at the given time.

        Parameters
        ----------
        hour, minute : int
            Time of day when the clean‑up should run (default 03:00).
        tz : datetime.timezone
            Timezone for the schedule (default UTC).
            Use ``datetime.timezone(datetime.timedelta(hours=‑5))`` for EST, etc.
        """

        def _clean_up_worker():
            while True:
                now = datetime.now(tz)
                # Build a datetime for *today* at the requested hour/minute
                today_target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)

                # If that time already passed, schedule for tomorrow
                if today_target <= now:
                    today_target += timedelta(days=1)

                seconds_to_wait = (today_target - now).total_seconds()
                self.logger.debug(
                    f"Daily token‑file clean‑up scheduled for {today_target.isoformat()} "
                    f"(in {seconds_to_wait:.0f}s)"
                )
                # Sleep until the next run time – ``time.sleep`` is safe in a daemon thread
                time.sleep(seconds_to_wait)

                # ----- RUN THE CLEAN‑UP -----
                try:
                    n_valid, n_cleaned = self._clean_up_token_file()
                    self.logger.info(
                        f"Token clean‑up finished at {datetime.now(tz).isoformat()}. "
                        f"{n_valid} token(s) remain valid."
                        f"{n_cleaned} token(s) were cleaned-up."
                    )
                except Exception as exc:  # pragma: no‑cover – defensive
                    self.logger.error(f"Unexpected error during token clean‑up: {exc}")

                # Loop back – the next iteration will recompute the next 03:00

        # The thread is marked as *daemon* so it won’t block interpreter shutdown
        thread = threading.Thread(target=_clean_up_worker, name="api-token-cleanup", daemon=True)
        thread.start()
        self.logger.debug("Started daily API token‑file clean‑up daemon thread")

    def create_api_token(self, username: str, token_exp=3600, datasets=None) -> str:
        """Generate a new API token for a user.

        This method creates a time‑limited API token associated with the specified ``username``.
        If ``token_exp`` is ``None`` the token will expire one hour from the moment of creation.
        An optional list of ``datasets`` can be provided to restrict the token's access scope.
        The generated token is persisted via the internal storage mechanism.

        Parameters
        ----------
        username
            Identifier of the user for whom the token is being created.
        token_exp
            If a string, there should be an iso-formatted expiration date.
            If an integer, act as an expiration interval in seconds.
            Use of a datetime is possible.
            By default, use ``None`` to set the default one‑hour lifetime.
        datasets
            Optional collection of dataset identifiers that the token should be allowed to access.

        Returns
        -------
        str
            The newly created API token.

        Notes
        -----
        The token includes a unique identifier generated with ``secrets`` and is timestamped using UTC.
        The internal helper methods handle token assembly and storage, ensuring consistency across calls.

        Examples
        --------
        >>> from plantdb.commons.auth.session import JWTSessionManager
        >>> from plantdb.commons.auth.models import Permission
        >>> manager = JWTSessionManager()
        >>> api_token = manager.create_api_token('batman', datasets={'joker': [Permission.DELETE]})
        >>> print(manager.api_token_file)
        >>> with open(manager.api_token_file, 'rb') as f: print(f.read())
        """
        now = datetime.now(timezone.utc)
        # Set a default token expiration date
        if token_exp is None:
            token_exp = 3600

        # Convert integer or string representations
        if isinstance(token_exp, int):
            token_exp = now + timedelta(seconds=token_exp)
        elif isinstance(token_exp, str):
            token_exp = datetime.fromisoformat(token_exp)

        # Check the validity of the expiration date
        if isinstance(token_exp, datetime):
            if token_exp <= now:
                self.logger.error(f"Token expiration date ({token_exp}) is in the past.")
                return ""
        else:
            self.logger.error(f"Token expiration date ({token_exp}) is not valid.")
            return ""

        token_jti = secrets.token_urlsafe(16)
        # Create the token:
        api_token = self._create_token(username, token_jti, token_exp, now, "api", datasets=datasets)

        # Register the token by jti:
        self._dump_api_token(token_jti, token_exp)

        return api_token

    def _payload_from_token(self, token: str) -> dict:
        """Decode the payload from a JSON Web Token.

        This function decodes the JSON Web Token (JWT) using the specified secret key and
        verifies the token's audience and issuer. It returns the decoded payload as a dictionary.

        Parameters
        ----------
        token : str
            The JSON Web Token to decode.

        Returns
        -------
        dict
            The decoded payload from the JSON Web Token.

        Notes
        -----
        The JSON Web Token must be correctly formatted and signed using the specified secret key.
        If the token is invalid or the signature does not match, a `jwt.ExpiredSignatureError`,
        `jwt.InvalidTokenError`, or `jwt.DecodeError` may be raised.

        See Also
        --------
        jwt.decode : Decodes the JSON Web Token.
        """
        return jwt.decode(
            token,
            self.secret_key,
            algorithms=['HS512'],
            audience='plantdb-client',  # Verify audience
            issuer='plantdb-api',  # Verify issuer,
            options={"require": ["exp", "iat", "iss", "aud"]},  # force the presence of these claims
            leeway=self.leeway  # allowed clock skew, in seconds
        )

    def validate_session(self, token: str, token_type: str = 'access') -> Optional[Dict[str, Any]]:
        """Validate a JSON Web Token and return user information.

        Parameters
        ----------
        token : str
            The JSON Web Token to validate.
        token_type : str, optional
            The expected token type ('access' or 'refresh').
            Defaults to 'access'.

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
            - type: Token type
        """
        # Decode and verify JSON Web Token with proper validation
        try:
            payload = self._payload_from_token(token)
        except jwt.ExpiredSignatureError as e:
            self.logger.error(f"JSON Web Token ({token_type}) expired")
            raise SessionValidationError(e) from e
        except jwt.InvalidAudienceError as e:
            self.logger.error("JSON Web Token has invalid audience")
            raise SessionValidationError(e) from e
        except jwt.InvalidIssuerError as e:
            self.logger.error("JSON Web Token has invalid issuer")
            raise SessionValidationError(e) from e
        except jwt.InvalidTokenError as e:
            self.logger.error(f"Invalid JSON Web Token: {e}")
            raise SessionValidationError(e) from e
        except Exception as e:
            self.logger.error(f"Error validating JSON Web Token: {e}")
            raise InvalidTokenProcessingError(e) from e

        # Check token type
        if payload.get('type') != token_type:
            self.logger.error(f"Invalid token type: expected {token_type}, got {payload.get('type')}")
            raise WrongTokenType(f"Invalid token type: {token_type}")

        jti = payload.get('jti')

        # Verify it's in our tracking list
        if token_type == 'access':
            if jti not in self.sessions:
                self.logger.error("Access token not found in active sessions")
                raise AccessTokenNotFoundError(f"Access token jti={jti} not found")
            # Update last accessed time
            with self._lock:
                self.sessions[jti]['last_accessed'] = datetime.now(timezone.utc)
        elif token_type == 'refresh':
            if jti not in self.refresh_tokens:
                self.logger.error("Refresh token not found in active refresh tokens")
                raise RefreshTokenNotFoundError(f"Refresh token jti={jti} not found")
        elif token_type == 'api':
            # Check active API tokens in the file
            if not self._is_api_token_active(jti):
                self.logger.error("API token not found in token store or has expired")
                raise SessionValidationError(f"API token jti={jti} is not active")

        payload_dict = {
            'username': payload['sub'],  # subject is the username
            'issued_at': payload['iat'],  # issued at timestamp
            'expires_at': payload['exp'],  # expiration timestamp
            'jti': jti,  # JWT ID
            'issuer': payload['iss'],  # issuer
            'audience': payload['aud'],  # audience
            'type': payload.get('type')  # type of token, 'access', 'api' or 'refresh'
        }
        if 'datasets' in payload:
            payload_dict['datasets'] = parse_dataset_perm(payload['datasets'])
        return payload_dict

    def invalidate_session(self, token: str = None, jti: str = None) -> Tuple[bool, str | None]:
        """Invalidate a session by removing it from tracking.

        Parameters
        ----------
        token : str, optional
            JSON Web Token to invalidate
        jti : str, optional
            Token ID to invalidate directly

        Returns
        -------
        bool
            `True` if the specified session was found and removed, `False` otherwise.
        str
            The username corresponding to the invalidated JSON Web Token
        """
        if token:
            try:
                payload = self._payload_from_token(token)
                jti = payload.get('jti')
                token_type = payload.get('type', 'access')
            except jwt.PyJWTError as e:
                self.logger.error(f"Failed to decode token for invalidation: {e}")
                return False, None
            except KeyError as e:
                self.logger.error(f"Failed to access payload key: {e}")
                return False, None
        else:
            # If jti is provided, we need to know its type or check both
            token_type = None

        with self._lock:
            if token_type == 'access' or token_type is None:
                if jti and jti in self.sessions:
                    username = self.sessions[jti]['username']
                    del self.sessions[jti]
                    # Also invalidate the linked refresh token if any
                    refresh_jtis = [rj for rj, rs in self.refresh_tokens.items() if rs.get('access_jti') == jti]
                    for rj in refresh_jtis:
                        del self.refresh_tokens[rj]
                    return True, username

            if token_type == 'refresh' or token_type is None:
                if jti and jti in self.refresh_tokens:
                    username = self.refresh_tokens[jti]['username']
                    # Optionally invalidate the linked access token?
                    # Usually we just invalidate the refresh token.
                    del self.refresh_tokens[jti]
                    return True, username

        return False, None

    def invalidate_api_token(self, token: str) -> bool:
        """Revoke an API token, removing it from memory and the persistent file.

        Parameters
        ----------
        token : str
            The full JWT string of the API token to revoke.

        Returns
        -------
        bool
            ``True`` if the token was found and removed, ``False`` otherwise.
        """
        try:
            payload = self._payload_from_token(token)
        except jwt.ExpiredSignatureError:
            # Token is expired but we still want to clean it up from storage
            # Decode without verification to extract the jti
            payload = jwt.decode(
                token,
                self.secret_key,
                algorithms=['HS512'],
                audience='plantdb-client',
                issuer='plantdb-api',
                options={"verify_exp": False},
                leeway=self.leeway
            )
        except (jwt.InvalidTokenError, Exception) as e:
            self.logger.error(f"Failed to decode API token for invalidation: {e}")
            return False

        if payload.get('type') != 'api':
            self.logger.warning("Token is not an API token, use invalidate_session() instead")
            return False

        jti = payload.get('jti')
        if not jti:
            return False

        with self._lock:
            if jti in self._api_tokens:
                del self._api_tokens[jti]
                self._save_api_tokens()
                self.logger.debug(f"Invalidated API token jti={jti}")
                return True

        self.logger.warning(f"API token jti={jti} not found in active tokens")
        return False

    def cleanup_expired_sessions(self) -> None:
        """Remove expired sessions from tracking."""
        current_time = datetime.now(timezone.utc)

        with self._lock:
            expired_access = [
                jti for jti, session in self.sessions.items()
                if current_time > session['expires_at']
            ]
            for jti in expired_access:
                del self.sessions[jti]

            expired_refresh = [
                jti for jti, session in self.refresh_tokens.items()
                if current_time > session['expires_at']
            ]
            for jti in expired_refresh:
                del self.refresh_tokens[jti]

        return

    def session_username(self, token: str) -> Optional[str]:
        """Extract username from JSON Web Token.

        Parameters
        ----------
        token : str
            Current JSON Web Token.

        Returns
        -------
        str or None
            Username if token is valid.
        """
        try:
            session_data = self.validate_session(token)
        except SessionValidationError as e:
            self.logger.warning(f"Provided session does not exist: {e}")
            return None
        return session_data['username']

    def refresh_session(self, refresh_token: str) -> Tuple[str, str]:
        """Refresh a session using a valid refresh token.

        Parameters
        ----------
        refresh_token : str
            The refresh token to use.

        Returns
        -------
        Tuple[str, str]
            A tuple containing (new_access_token, new_refresh_token) if successful.
        """
        # Validate the refresh token – will raise if the refresh token is revoked or malformed
        session_data = self.validate_session(refresh_token, token_type='refresh')

        username = session_data['username']
        old_refresh_jti = session_data['jti']
        old_access_jti = self.refresh_tokens[old_refresh_jti].get('access_jti')
        # Invalidate old tokens (Rotation)
        self.invalidate_session(jti=old_refresh_jti)
        if old_access_jti:
            self.invalidate_session(jti=old_access_jti)

        # Create a new session (new access + new refresh)
        return self.create_session(username)
