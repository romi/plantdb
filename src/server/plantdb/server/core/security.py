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

import threading
import time
from collections import defaultdict
from functools import wraps

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
        The duration of the rate-limiting window in seconds
        (default is 60 seconds).

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
    requests, making it suitable for multi-threaded environments. The requests
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

                # Check if rate limit is exceeded
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


def jwt_from_header(request) -> str:
    """Extracts the JSON Web Token from the Authorization header of an HTTP request.

    Parameters
    ----------
    request : object
        An HTTP request object that provides a ``headers`` attribute.

    Returns
    -------
    str
        The JSON Web Token extracted from the ``Authorization`` header, or an
        empty string if the header is missing or empty.

    Notes
    -----
    * The function performs a simple string replacement and does **not**
      check that the resulting token is a valid JWT.
    """
    return request.headers.get('Authorization', "").replace('Bearer ', '')


def add_jwt_from_header(f):
    """Retrieve the JSON Web Token from the header and add it to keyword arguments, if any."""

    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Try to get JSON Web Token from the request header
        jwt_token = jwt_from_header(request)
        # Do not verify we actually got a token or test its validity; this is done by the database)
        # Add token to keyword arguments (for later use by FSD methods)
        kwargs['token'] = jwt_token
        return f(*args, **kwargs)

    return decorated_function
