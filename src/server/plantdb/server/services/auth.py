#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# plantdb - Data handling tools for the ROMI project
#
# Copyright (C) 2018-2019 Sony Computer Science Laboratories
# Authors: D. Colliaux, T. Wintz, P. Hanappe
#
# This file is part of plantdb.
#
# plantdb is free software: you can redistribute it
# and/or modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation, either
# version 3 of the License, or (at your option) any later version.
#
# plantdb is distributed in the hope that it will be
# useful, but WITHOUT ANY WARRANTY; without even the implied
# warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with plantdb.  If not, see
# <https://www.gnu.org/licenses/>.
# ------------------------------------------------------------------------------

"""Authentication service layer.

This module contains pure‑Python functions that implement the business logic for
user registration, login, logout, token validation and token refresh.  The
functions are deliberately small (atomic) and raise domain‑specific exceptions
instead of returning HTTP responses.  Flask‑RESTful resources in
``plantdb.server.api.auth`` act as thin adapters that translate these exceptions
into proper HTTP status codes and JSON payloads.
"""

from __future__ import annotations

import logging
from typing import Any
from typing import Dict
from typing import Mapping
from typing import Tuple

from plantdb.commons.auth.manager import UserExistsError
from plantdb.commons.auth.session import SessionValidationError
from plantdb.commons.fsdb.exceptions import NoAuthUserError


# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------
class MissingFieldError(ValueError):
    """Raised when a required field is missing from the request payload."""


class InvalidCredentialsError(ValueError):
    """Raised when supplied credentials are not valid."""


class TokenError(ValueError):
    """Raised for generic token‑related problems (validation / refresh)."""


# ---------------------------------------------------------------------------
# Helper utilities - internal to this module
# ---------------------------------------------------------------------------

def _get_logger(logger: logging.Logger | None = None) -> logging.Logger:
    """Return ``logger`` if provided, otherwise a module‑level logger.

    Parameters
    ----------
    logger : logging.Logger | None, optional, default ``None``
        Optional logger used for diagnostic messages.
        If ``None`` (default), a module‑level logger is obtained via ``_get_logger``.

    Returns
    -------
    logging.Logger
        The passed or created logger instance.
    """
    return logger if logger is not None else logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Service functions
# ---------------------------------------------------------------------------

def register_user(db: Any, payload: Dict[str, Any], **kwargs) -> None:
    """Create a new user record in the supplied database.

    Parameters
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        An FSDB instance that implements a ``create_user`` method.
    payload : Dict[str, Any]
        Mapping containing the keys ``username``, ``fullname`` and ``password``.
        The values are expected to be strings, but the function does not enforce
        a specific type beyond the presence of the keys.

    Other Parameters
    ----------------
    token : str
        The token for authentication.
    logger : logging.Logger
        Optional logger used for diagnostic messages.
        If ``None`` (default), a module‑level logger is obtained via ``_get_logger``.

    Raises
    ------
    MissingFieldError
        If any of the required keys (``username``, ``fullname``, ``password``) are absent from ``payload``.
    UserAlreadyExistsError
        If the underlying ``db.create_user`` raises an exception that indicates
        a duplicate user or any other database‑level problem.
    SessionValidationError
        Propagated unchanged when the database signals that the operation is
        invalid for the current session (e.g., unauthorized).
    """
    log = _get_logger(kwargs.get('logger'))
    required = {"username", "fullname", "password"}
    missing = required - set(payload.keys())
    if missing:
        raise MissingFieldError(f"Missing required fields: {', '.join(sorted(missing))}")

    try:
        db.create_user(
            new_username=payload["username"],
            fullname=payload["fullname"],
            password=payload["password"],
            token=kwargs.get("token"),
        )
        log.info("User '%s' successfully created", payload["username"])
    except UserExistsError as exc:
        # Keep original semantics - caller will translate to 401
        raise exc
    except SessionValidationError as exc:
        # Keep original semantics - caller will translate to 401
        raise exc
    except Exception as exc:
        # Log the unexpected error and propagate it unchanged
        log.error("Unexpected error while creating user: %s", exc)
        raise  # propagates the original exception (preserves traceback)


def check_username_exists(db: Any, username: str) -> bool:
    """Return ``True`` if ``username`` exists in the database.

    The underlying DB exposes ``rbac_manager.users.exists``.
    """
    return db.rbac_manager.users.exists(username)


def authenticate_user(db: Any, username: str, password: str) -> Tuple[str, str]:
    """Authenticate credentials and return ``(access_token, refresh_token)``.

    Raises
    ------
    InvalidCredentialsError
        If the DB ``login`` method returns ``None``.
    """
    tokens = db.login(username, password)
    if not tokens:
        raise InvalidCredentialsError("Invalid credentials")
    return tokens  # type: ignore[return-value]


def logout_user(db: Any, token: str, logger: logging.Logger | None = None) -> str:
    """Invalidate a session and return the username that was logged out.

    Parameters
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        Database instance exposing ``logout``.
    token: str
        JWT token extracted from the request (passed via ``**kwargs``).
    logger: logging.Logger | None
        Optional logger.

    Returns
    -------
    str
        Username of the user whose session was terminated.

    Raises
    ------
    NoAuthUserError
        If logout fails for any reason.
    """
    log = _get_logger(logger)
    try:
        success, username = db.logout(token=token)
        if not success:
            raise NoAuthUserError("Logout failed")
        log.info("Logout successful for user %s", username)
        return username
    except SessionValidationError as exc:
        raise NoAuthUserError(str(exc)) from exc


def validate_token(db: Any, token: str) -> Mapping[str, Any]:
    """Validate a JWT and return basic user information.

    Returns a mapping with ``username`` and ``fullname``.
    Raises:
        TokenError: If validation fails.
    """
    try:
        user = db.get_user_data(token=token)
        return {"username": user.username, "fullname": user.fullname}
    except Exception as exc:
        raise TokenError(str(exc)) from exc


def refresh_token(db: Any, refresh_token: str) -> Tuple[str, str]:
    """Refresh an access token using a valid refresh token.

    Returns a tuple ``(new_access_token, new_refresh_token)``.
    Raises:
        TokenError: If the refresh token is invalid or the refresh operation fails.
    """
    try:
        tokens = db.session_manager.refresh_session(refresh_token)
        if not tokens:
            raise TokenError("Invalid or expired refresh token")
        return tokens  # type: ignore[return-value]
    except Exception as exc:
        raise TokenError(str(exc)) from exc
