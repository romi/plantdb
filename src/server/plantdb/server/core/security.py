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

"""
# Security and Request Handling Utilities

This module provides decorators and utility functions to enhance the security of Flask-based Web APIs.
It offers mechanisms for rate limiting to prevent abuse and utilities for extracting JSON Web Tokens
(JWT) from HTTP headers to facilitate authentication.

## Key Features

* **Rate Limiting**: A thread-safe decorator to restrict the number of requests from a specific
  IP address within a rolling time window.
* **JWT Extraction**: Functions to seamlessly extract Bearer tokens from the `Authorization`
  header and inject them into function arguments.
* **Flask Integration**: Designed to work directly with Flask's request context and response
  objects, returning standard HTTP status codes (e.g., 429 Too Many Requests).

## Usage Examples

The following example demonstrates how to protect a Flask-RESTful resource using the
security decorators:

```python
from flask import Flask
from flask_restful import Api, Resource
from security import rate_limit, add_jwt_from_header

app = Flask(__name__)
api = Api(app)

class SecureResource(Resource):
    @rate_limit(max_requests=10, window_seconds=60)
    @add_jwt_from_header
    def get(self, **kwargs):
        # The 'token' is automatically extracted and passed in kwargs
        token = kwargs.get('token')
        return {"message": "Access granted", "token_found": bool(token)}

api.add_resource(SecureResource, '/secure')

if __name__ == '__main__':
    app.run()
```
"""
import inspect
import threading
import time
from collections import defaultdict
from functools import wraps
from typing import Callable

from flask import Response
from flask import request


def rate_limit(max_requests=5, window_seconds=60):
    """Limits the number of requests a client can make within a specified time window.

    This function is a decorator that enforces rate limiting based on the maximum
    number of allowed requests (`max_requests`) and the time window size in seconds
    (`window_seconds`). It tracks incoming requests from clients using their IP
    addresses and ensures that they do not exceed the specified limit within the
    time window. If the limit is exceeded, it returns an HTTP ``429`` response.

    Parameters
    ----------
    max_requests : int, optional
        The maximum number of requests permitted within the time window (default is 5).
    window_seconds : int, optional
        The duration of the rate-limiting window, in seconds (default is 60 seconds).

    Returns
    -------
    decorator : Callable
        A decorator that can wrap any function or endpoint to enforce rate limiting.

    Raises
    ------
    http.client.HTTPException
        If the rate limit is exceeded, it returns an HTTP 429 ("Too Many Requests")
        response to the client.

    Notes
    -----
    This implementation uses a thread lock to ensure thread safety when handling
    requests, making it suitable for multithreaded environments. The requests
    data structure is a `defaultdict` that maps client IPs to a list of their
    request timestamps. Old requests outside the rate-limiting window are removed
    to maintain efficient memory usage.
    """
    requests = defaultdict(list)
    lock = threading.Lock()

    def decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            client_ip = request.remote_addr
            current_time = time.time()

            with lock:
                # Remove old requests outside the window
                requests[client_ip] = [req_time for req_time in requests[client_ip]
                                       if current_time - req_time < window_seconds]

                # Check if the rate limit is exceeded
                if len(requests[client_ip]) >= max_requests:
                    return Response(
                        "Rate limit exceeded. Please try again later.",
                        status=429
                    )

                # Add current request
                requests[client_ip].append(current_time)

            return f(*args, **kwargs)

        return wrapped

    return decorator


def jwt_from_header(request) -> str | None:
    """Extract the JWT from the ``Authorization``request header.

    Parameters
    ----------
    request: requests.models.Request
        An HTTP request exposing a ``headers`` mapping.

    Returns
    -------
    str | None
        The extracted JWT or ``None``.

    Notes
    -----
    * The function expects the header to be in the form ``Bearer <token>``.
    * The function performs a simple string replacement and does **not** perform any verification.
    """
    auth = request.headers.get('Authorization')
    if not auth:
        return None
    return auth.replace('Bearer ', '')


def add_jwt_from_header(f: Callable) -> Callable:
    """Retrieve the JSON Web Token from the header and add it to keyword arguments, if any.

    This enables automatic token definition from the request header.
    """

    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Get JSON Web Token from the request header
        jwt_token = jwt_from_header(request)
        if jwt_token:
            kwargs['token'] = jwt_token
        return f(*args, **kwargs)

    return decorated_function


def use_guest_as_default(f: Callable) -> Callable:
    """Injects a guest user when no authentication token is provided.

    This enables methods that expect an authenticated user to operate transparently with a default guest context.
    """

    @wraps(f)
    def decorated_function(self, *args, **kwargs):
        if not kwargs.get('token'):
            kwargs["current_user"] = self.db.get_guest_user()
        return f(self, *args, **kwargs)

    return decorated_function


def sanitize_name(name):
    """Sanitizes and validates the provided name.

    The function ensures that the input string adheres to predefined naming rules by:

    - stripping leading/trailing spaces,
    - isolating the last segment after splitting by slashes,
    - validating the name against an alphanumeric pattern
      with optional underscores (`_`), dashes (`-`), or periods (`.`).

    Parameters
    ----------
    name : str
        The name to sanitize and validate.

    Returns
    -------
    str
        A sanitized name that conforms to the rules.

    Raises
    ------
    ValueError
        If the provided name contains invalid characters or does not meet the naming rules.
    """
    import re
    sanitized_name = name.strip()  # Remove leading/trailing spaces
    sanitized_name = sanitized_name.split('/')[-1]  # isolate the last segment after splitting by slashes
    # Validate against an alphanumeric pattern with optional underscores, dashes, or periods
    if not re.match(r"^[a-zA-Z0-9_.-]+$", sanitized_name):
        raise ValueError(
            f"Invalid name: '{name}'. Names must be alphanumeric and can include underscores, dashes, or periods.")
    return sanitized_name


def sanitize_ids(*param_names):
    """Decorator that sanitizes the given identifier parameters using ``sanitize_name``.

    If sanitization fails, logs a warning and returns a 400 response.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            # Bind the incoming args/kwargs to the function signature so we can
            # address parameters by name regardless of how they were passed.
            bound = inspect.signature(func).bind(self, *args, **kwargs)
            bound.apply_defaults()

            for name in param_names:
                if name in bound.arguments:
                    original = bound.arguments[name]
                    try:
                        bound.arguments[name] = sanitize_name(original)
                    except ValueError as e:
                        # Consistent logging / response pattern
                        self.logger.warning(f"Invalid {name} format: {original}")
                        return {'message': str(e)}, 400

            # Call the original method with the (now sanitized) arguments
            return func(**bound.arguments)

        return wrapper

    return decorator
