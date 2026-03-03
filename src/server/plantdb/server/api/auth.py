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
# License along with plantdb. If not, see
# <https://www.gnu.org/licenses/>.
# ------------------------------------------------------------------------------

"""
# Authentication REST API Resources

Provides Flask‑RESTful resources that implement a complete authentication workflow for the PlantDB server, including user registration, login, logout, JWT validation, and token refresh.
The resources add security features such as rate‑limiting and automatic JWT extraction, making it easy to protect API endpoints while keeping the codebase tidy.

## Key Features

- **Register** - Create new user accounts with input validation, rate limiting, and JWT handling.
- **Login** - Check username existence, authenticate credentials, and return access/refresh tokens.
- **Logout** - Invalidate a user session and log the outcome.
- **TokenValidation** - Verify a JWT and return the associated user's basic profile information.
- **TokenRefresh** - Issue a fresh access token given a valid refresh token.
- Built‑in decorators (`@rate_limit`, `@add_jwt_from_header`) ensure consistent security across all endpoints.

## Usage Examples

Hereafter is a minimal working example that:

1. Creates a `Flask` app
2. Sets up a local test database with a JSON Web Token session manager
3. Registers the `Login` and `Logout` resources to a REST API
4. Starts the app

```python
>>> import logging
>>> from flask import Flask
>>> from flask_restful import Api
>>> from plantdb.server.api.auth import Login, Logout
>>> from plantdb.commons.auth.session import JWTSessionManager
>>> from plantdb.commons.fsdb.core import FSDB
>>> from plantdb.commons.test_database import setup_test_database
>>> # Create a Flask application
>>> app = Flask(__name__)
>>> # Create a logger
>>> logger = logging.getLogger("plantdb.auth")
>>> logger.setLevel(logging.INFO)
>>> # Initialize a test database with a JWTSessionManager
>>> db_path = setup_test_database('real_plant')
>>> mgr = JWTSessionManager()
>>> db = FSDB(db_path, session_manager=mgr)
>>> db.connect()
>>> # RESTful API and resource registration
>>> api = Api(app)
>>> api.add_resource(Login, "/login", resource_class_kwargs={"db": db})
>>> api.add_resource(Logout, "/logout", resource_class_kwargs={"db": db, "logger": logger})
>>> # Start the APP
>>> app.run(host='0.0.0.0', port=5000)
```

It may be used as follows (in another Python REPL):
```python
>>> import requests
>>> # Check if the user exists (valid username):
>>> response = requests.get("http://127.0.0.1:5000/login?username=admin")
>>> print(response.json())
{'username': 'admin', 'exists': True}
>>> # Login with default 'admin' credentials
>>> response = requests.post('http://127.0.0.1:5000/login', json={'username': 'admin', 'password': 'admin'})
>>> print(response.json())
{'access_token': 'eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJwbGFudGRiLWFwaSIsInN1YiI6ImFkbWluIiwiYXVkIjoicGxhbnRkYi1jbGllbnQiLCJleHAiOjE3NzIxMzkxNzcsImlhdCI6MTc3MjEzODI3NywianRpIjoid2N2NnozNHg4aTY5SG5LTG9WaUk4dyIsInR5cGUiOiJhY2Nlc3MifQ.lmOkQMbp-EvfdIiuUq6u-yqIpDC32d8gCoZUBtMUc-mC6gC8pb3RPQPnwIhATyjrqWS-aMq-9WuTD-4ogU5EGg', 'message': 'Login successful', 'refresh_token': 'eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJwbGFudGRiLWFwaSIsInN1YiI6ImFkbWluIiwiYXVkIjoicGxhbnRkYi1jbGllbnQiLCJleHAiOjE3NzIyMjQ2NzcsImlhdCI6MTc3MjEzODI3NywianRpIjoiVW1seVQ4M2J1LUVqcUhaTGhENy1GQSIsInR5cGUiOiJyZWZyZXNoIn0.jP1rPM4lfOwxARksOEK35z2DOK2ntGwFJ7b_waYfKu-SWcQXiS3gujz29RrHr0J_JcUy92vtRa-aYh78FEWfPA', 'user': {'fullname': 'PlantDB Admin', 'username': 'admin'}}
>>> token = response.json()['access_token']
>>> # Now try to log out:
>>> response = requests.post("http://127.0.0.1:5000/logout", headers={'Authorization': 'Bearer ' + token})
>>> print(response.json()['message'])
Logout successful from admin
```
"""

import requests
from flask import jsonify
from flask import make_response
from flask import request
from flask_restful import Resource

from plantdb.commons.auth.manager import UserExistsError
from plantdb.commons.auth.session import SessionValidationError
from plantdb.commons.fsdb.core import FSDB
from plantdb.server.core.security import add_jwt_from_header
from plantdb.server.core.security import rate_limit
from plantdb.server.services.auth import InvalidCredentialsError
from plantdb.server.services.auth import MissingFieldError
from plantdb.server.services.auth import NoAuthUserError
from plantdb.server.services.auth import TokenError
from plantdb.server.services.auth import authenticate_user
from plantdb.server.services.auth import check_username_exists
from plantdb.server.services.auth import logout_user
from plantdb.server.services.auth import refresh_token
from plantdb.server.services.auth import register_user
from plantdb.server.services.auth import validate_token


class Register(Resource):
    """A RESTful resource to manage user registration via HTTP POST requests.

    Responsible for handling the registration process by validating and creating user records in the database.
    This class provides a structured way to interact with user data, ensuring error handling and
    proper responses for client requests.
    Only users with ADMIN rights can add user records.

    Attributes
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        The database providing the resources to serve.
    logger : logging.Logger
        The logger used to record operations and errors.
    """

    def __init__(self, db, logger):
        """Initialize the resource.

        Parameters
        ----------
        db : plantdb.commons.fsdb.core.FSDB
            A database instance providing the resources to serve.
        logger : logging.Logger
            A logger instance to record operations and errors.
        """
        self.db = db
        self.logger = logger

    @rate_limit(max_requests=5, window_seconds=60)  # maximum of 1 requests per minute
    @add_jwt_from_header
    def post(self, **kwargs):
        """Handle HTTP POST request to register a new user.

        Processes user registration by validating the input data and creating a new user in the database.
        Expects a JSON payload in the request body with required user details.
        Only users with ADMIN rights can add user records.

        Returns
        -------
        dict
            A dictionary with the following keys and values:
                - 'success' (bool): Indicates if operation was successful
                - 'message' (str): Description of the operation result
        int
            HTTP status code (``201`` for success, ``400`` for error)

        Raises
        ------
        http.client.HTTPException
             If the rate limit is exceeded, it returns an HTTP 429 ("Too Many Requests") response to the client.
        ValueError
            If required fields are missing from the request
        Exception
            If user creation fails (e.g., duplicate username)

        Notes
        -----
        - The rate limit is enforced per client IP address
        - The method doesn't take direct parameters but expects a JSON payload in the request body with the following fields:
            - username: Unique identifier for the user.
            - fullname: User's full name.
            - password: User's password (will be hashed before storage).

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> import requests
        >>> # Start by login as admin to have permission to create new users
        >>> response = requests.post('http://127.0.0.1:5000/login', json={'username': 'admin', 'password': 'admin'})
        >>> token = response.json()['access_token']
        >>> # Now create a new user:
        >>> new_user = {"username":"batman", "fullname":"Bruce Wayne", "password":"Alfred123!"}
        >>> response = requests.post("http://127.0.0.1:5000/register", json=new_user, headers={'Authorization': 'Bearer ' + token})
        >>> res_dict = response.json()
        >>> res_dict["success"]
        True
        >>> res_dict["message"]
        'User successfully created'
        >>> response = requests.post('http://127.0.0.1:5000/login', json={'username': 'batman', 'password': 'Alfred123!'})
        >>> res_dict = response.json()
        >>> res_dict["message"]
        'Login successful'
        """
        # Parse JSON data from request body
        data = request.get_json()
        # Check if all required fields are present in the request
        required_fields = ['username', 'fullname', 'password']
        if not data or not all(field in data for field in required_fields):
            return {
                'success': False,
                'message': 'Missing required fields. Please provide username, fullname, and password'
            }, 400

        try:
            # Delegate to the service layer
            register_user(self.db, data, token=kwargs.get('token'))
            # Return success response if the user creation succeeds
            return {'message': 'User successfully created'}, 201

        except MissingFieldError as e:
            return {'message': str(e)}, 400  # HTTP 400 Bad Request
        except NoAuthUserError as e:
            return {'message': str(e)}, 401  # HTTP 401 Unauthorized (authentication)
        except SessionValidationError as e:
            return {'message': str(e)}, 401  # HTTP 401 Unauthorized (authentication)
        except UserExistsError as e:
            return {'message': f'Failed to create user: {str(e)}'}, 409  # HTTP 409 Conflict
        except Exception as e:
            return {'message': f'Failed to create user: {str(e)}'}, 500  # HTTP 500 Internal Server Error


class Login(Resource):
    """A RESTful resource to handle user login and authentication processes.

    This class processes HTTP requests for user authentication, including checking
    if a username exists in the database and validating login credentials.

    Attributes
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        The database providing the resources to serve.
    logger : logging.Logger
        The logger used to record operations and errors.
    """

    def __init__(self, db, logger):
        """Initialize the resource.

        Parameters
        ----------
        db : plantdb.commons.fsdb.core.FSDB
            A database instance providing the resources to serve.
        logger : logging.Logger
            A logger instance to record operations and errors.
        """
        self.db = db
        self.logger = logger

    @rate_limit(max_requests=120, window_seconds=60)
    def get(self):
        """Checks if a given username exists in the database and returns the result.

        Returns
        -------
        dict
            A dictionary with the result and an HTTP status code.
            If the `username` is missing, the dictionary will contain an error message.
            Otherwise, the dictionary will contain the `username` and a boolean indicating whether it exist or not.
        int
            An HTTP status code. If the `username` is missing  the status code will be ``400``, otherwise ``200``.

        Raises
        ------
        http.client.HTTPException
             If the rate limit is exceeded, it returns an HTTP 429 ("Too Many Requests") response to the client.

        Notes
        -----
        In the URL, you can use the `username` parameter to check if a user exists.

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> import requests
        >>> # Check if the user exists (valid username):
        >>> response = requests.get("http://127.0.0.1:5000/login?username=admin")
        >>> print(response.json())
        {'username': 'admin', 'exists': True}
        >>> # Check if the user exists (invalid username):
        >>> response = requests.get("http://127.0.0.1:5000/login?username=superman")
        >>> print(response.json())
        {'username': 'superman', 'exists': False}
        """
        # Extract username from query parameters
        username = request.args.get('username', None)
        # Return an error if the username parameter is missing
        if not username:
            return {'message': 'Missing username parameter'}, 400
        # Query database to check if the user exists
        user_exists = check_username_exists(self.db, username)
        return {'username': username, 'exists': user_exists}, 200

    @rate_limit(max_requests=20, window_seconds=60)
    def post(self):
        """Handle user authentication via POST request with username and password.

        This method processes a POST request containing user credentials (username and password)
        and validates them against stored user data. It returns authentication status and
        a descriptive message.

        Returns
        -------
        dict
            A dictionary with the following keys and values:
                - 'authenticated' : bool
                    The result of the authentication process (``True`` if successful, ``False`` otherwise).
                - 'message' : str
                    A message describing the result of the authentication attempt.
        int
            The HTTP status code (``200`` for successful response, ``400`` for bad request).

        Raises
        ------
        werkzeug.exceptions.BadRequest
            If the request doesn't contain valid JSON data (handled by Flask)
        http.client.HTTPException
             If the rate limit is exceeded, it returns an HTTP 429 ("Too Many Requests") response to the client.

        Notes
        -----
        The method expects a JSON payload with 'username' and 'password' fields.
        The authentication process uses the ``check_credentials`` method to validate
        the provided credentials against the database.

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> import requests
        >>> # Valid login request
        >>> response = requests.post('http://127.0.0.1:5000/login', json={'username': 'admin', 'password': 'admin'})
        >>> print(response.json())
        {'authenticated': True, 'message': 'Login successful. Welcome, Guy Fawkes!'}
        >>> print(response.status_code)
        200
        >>> # Invalid request (missing credentials)
        >>> response = requests.post('http://127.0.0.1:5000/login', json={'username': 'admin'})
        >>> print(response.json())
        {'authenticated': False, 'message': 'Missing username or password'}
        >>> print(response.status_code)
        400
        """
        # Get JSON data from the request body
        data = request.get_json()
        # Validate that required fields are present in the request
        if not data or 'username' not in data or 'password' not in data:
            return {'authenticated': False, 'message': 'Missing username or password'}, 400

        # Extract credentials from request data
        username = data['username']
        password = data['password']

        try:
            access_token, refresh_token = authenticate_user(self.db, username, password)
        except InvalidCredentialsError:
            return {'message': 'Invalid credentials'}, 401  # HTTP 401 Unauthorized (authentication)
        except Exception as e:
            return {'message': f'Authentication failed: {str(e)}'}, 500  # HTTP 500 Internal Server Error

        # Build the successful response with user info, access token, and refresh token
        user = self.db.get_user_data(token=access_token)
        response_data = {
            'message': 'Login successful',
            'user': {
                'username': user.username,
                'fullname': user.fullname,
            },
            'access_token': access_token,
            'refresh_token': refresh_token
        }
        return response_data, 200


class Logout(Resource):
    """Resource handling user logout requests.

    Attributes
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        The database providing the resources to serve.
    logger : logging.Logger
        The logger used to record operations and errors.
    """

    def __init__(self, db, logger):
        """Initialize the resource.

        Parameters
        ----------
        db : plantdb.commons.fsdb.core.FSDB
            A database instance providing the resources to serve.
        logger : logging.Logger
            A logger instance to record operations and errors.
        """
        self.db = db
        self.logger = logger

    @add_jwt_from_header
    def post(self, **kwargs):
        """Handle user logout.

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> import requests
        >>> # Start by log in as 'admin'
        >>> response = requests.post('http://127.0.0.1:5000/login', json={'username': 'admin', 'password': 'admin'})
        >>> print(response.json()['message'])
        Login successful
        >>> token = response.json()['access_token']
        >>> # Now try to log out:
        >>> response = requests.post("http://127.0.0.1:5000/logout", headers={'Authorization': 'Bearer ' + token})
        >>> print(response.json()['message'])
        Logout successful
        """
        try:
            # The decorator supplies the token via kwargs
            token = kwargs.get('token')
            if not token:
                self.logger.error("Logout error: no active session!")
                return {'message': 'Logout failed, no active session!'}, 401  # HTTP 401 Unauthorized (authentication)

            # Delegate to the service layer
            username = logout_user(self.db, token, logger=self.logger)

            return {'message': f'Logout successful from {username}'}, 200

        except NoAuthUserError as e:
            return {'message': str(e)}, 401  # HTTP 401 Unauthorized (authentication)
        except Exception as e:
            self.logger.error(f"Logout error: {str(e)}")
            return {'message': 'Logout failed!'}, 500  # HTTP 500 Internal Server Error


class TokenValidation(Resource):
    """Validate a JSON Web Token (JWT) and retrieve associated user data.

    The resource exposes a POST endpoint that accepts a JSON Web Token, verifies its
    validity against the database session manager, and returns the authenticated
    user’s basic profile information. On success a 200 response is returned
    containing the user’s ``username`` and ``fullname``; on failure a 401
    response is returned with an error message.

    Attributes
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        The database providing the resources to serve.
    logger : logging.Logger
        The logger used to record operations and errors.
    """

    def __init__(self, db, logger):
        """Initialize the resource.

        Parameters
        ----------
        db : plantdb.commons.fsdb.core.FSDB
            A database instance providing the resources to serve.
        logger : logging.Logger
            A logger instance to record operations and errors.
        """
        self.db = db
        self.logger = logger

    @add_jwt_from_header
    def post(self, **kwargs):
        """Handle JSON Web Token validation.

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> import requests
        >>> # Start by login as admin
        >>> response = requests.post('http://127.0.0.1:5000/login', json={'username': 'admin', 'password': 'admin'})
        >>> token = response.json()['access_token']
        >>> # Now create a new user:
        >>> response = requests.post("http://127.0.0.1:5000/token-validation", headers={'Authorization': 'Bearer ' + token})
        >>> print(response.json()['message'])
        Token validation successful
        """
        try:
            token = kwargs.get('token')
            if not token:
                raise TokenError('No token supplied')

            # Delegate to the service layer
            user_info = validate_token(self.db, token)

            response = {
                'message': 'Token validation successful',
                'user': user_info,
            }, 200

        except NoAuthUserError as e:
            return {'message': str(e)}, 401  # HTTP 401 Unauthorized (authentication)
        except TokenError as e:
            response = {'message': f'Token validation failed: {str(e)}'}, 401  # HTTP 401 Unauthorized (authentication)
        except Exception as e:
            response = {'message': f'Unexpected error: {str(e)}'}, 500  # HTTP 500 Internal Server Error

        return response


class TokenRefresh(Resource):
    """Refresh JSON Web Token for an authenticated user.

    The `TokenRefresh` resource provides an endpoint that accepts an
    existing JSON Web Token, validates the current session, and issues a new
    access token when the refresh is successful. The resource interacts
    with a database session manager that exposes a ``refresh_session`` method
    to perform the actual token renewal.

    Attributes
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        The database providing the resources to serve.
    logger : logging.Logger
        The logger used to record operations and errors.
    """

    def __init__(self, db, logger):
        """Initialize the resource.

        Parameters
        ----------
        db : plantdb.commons.fsdb.core.FSDB
            A database instance providing the resources to serve.
        logger : logging.Logger
            A logger instance to record operations and errors.
        """
        self.db = db
        self.logger = logger

    def post(self, **kwargs):
        """Refresh JSON Web Token.

        This method expects a JSON payload containing a 'refresh_token'.
        It validates the refresh token and issues a new access/refresh token pair.
        """
        data = request.get_json()
        if not data or 'refresh_token' not in data:
            return {'message': 'Missing refresh_token'}, 400

        refresh_token_str = data['refresh_token']

        try:
            # Delegate to the service layer
            access_token, new_refresh_token = refresh_token(self.db, refresh_token_str)

            response = {
                'message': 'Token refreshed successfully',
                'access_token': access_token,
                'refresh_token': new_refresh_token
            }, 200
            return response

        except NoAuthUserError as e:
            return {'message': str(e)}, 401  # HTTP 401 Unauthorized (authentication)
        except TokenError as e:
            return {'message': f'Invalid or expired refresh token: {str(e)}'}, 401  # HTTP 401 Unauthorized (authentication)
        except Exception as e:
            return {'message': f'Token refresh failed: {str(e)}'}, 500  # HTTP 500 Internal Server Error

class CreateApiToken(Resource):
    """

    Attributes
    ----------
    db : plantdb.core.FSDB
        Database handler with a ``session_manager`` attribute.

    Parameters
    ----------
    db : Any
        Database handler providing access to the session manager.

    """

    def __init__(self, db, logger):
        """Initialize the TokenRefresh resource."""
        self.db: FSDB = db
        self.logger = logger

    @add_jwt_from_header
    def post(self, **kwargs):
        """Refresh JSON Web Token.

        This method expects a JSON payload containing a 'refresh_token'.
        It validates the refresh token and issues a new access/refresh token pair.
        """
        data = request.get_json()
        if "datasets" not in data:
            return {'message': 'Missing "datasets" field'}, 400
        if "token_exp" not in data:
            return {'message': 'Missing "token_exp" field'}, 400

        try:
            token = self.db.create_api_token(
                token_exp=int(data['token_exp']),
                datasets=data['datasets'],
                **kwargs
            )

            if token:
                response = {
                    'message': 'Token refreshed successfully',
                    'api_token': token,
                }, 200
                return response
            else:
                return {'message': 'Could not create token.'}, 401

        except Exception as e:
            return {'message': f'Token creation failed: {e}'}, 500
