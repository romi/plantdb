#!/usr/bin/env python3
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
# Base REST API Resources

Provides Flask‑RESTful resources that expose the PlantDB database through a
well‑documented HTTP API. The module bundles endpoints for health checks,
metadata queries, file access, and on‑demand database reloading, all protected
by configurable rate‑limiting to safeguard the service.

## Key Features

- **Health check endpoint** - quickly verify that the service and underlying
  database are operational.
- **Dynamic database refresh** - reload a single scan or the entire dataset
  without restarting the server.
- **Self‑describing root resource** - returns API name, description,
  version information, and a list of all available routes.

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
>>> from plantdb.server.api.base import Home, HealthCheck
>>> from plantdb.commons.auth.session import JWTSessionManager
>>> from plantdb.commons.fsdb.core import FSDB
>>> from plantdb.commons.test_database import setup_test_database
>>> # Create a Flask application
>>> app = Flask(__name__)
>>> # Create a logger
>>> logger = logging.getLogger("plantdb.base")
>>> logger.setLevel(logging.INFO)
>>> # Initialize a test database with a JWTSessionManager
>>> db_path = setup_test_database('real_plant')
>>> mgr = JWTSessionManager()
>>> db = FSDB(db_path, session_manager=mgr)
>>> db.connect()
>>> # RESTful API and resource registration
>>> api = Api(app)
>>> api.add_resource(Home, "/")
>>> api.add_resource(HealthCheck, "/health", resource_class_kwargs={"db": db})
>>> # Start the APP
>>> app.run(host='0.0.0.0', port=5000)
```

It may be used as follows (in another Python REPL):
```python
>>> import requests
>>> # Check if the user exists (valid username):
>>> response = requests.get("http://127.0.0.1:5000/")
>>> print(response.json()['name'])
PlantDB REST API
>>> # Check server status
>>> response = requests.get("http://127.0.0.1:5000/health")
>>> print(response.json()['status'])
healthy
"""
import logging

from flask import request
from flask_restful import Resource
from plantdb.commons.fsdb.core import FSDB
from plantdb.commons.log import get_logger
from plantdb.server.core.security import rate_limit
from plantdb.server.core.security import sanitize_ids

task_filesUri_mapping = {
    "PointCloud": "pointCloud",
    "TriangleMesh": "mesh",
    "CurveSkeleton": "skeleton",
    "TreeGraph": "tree",
}


# Home page resource
class Home(Resource):

    @rate_limit(max_requests=120, window_seconds=60)
    def get(self):
        """Return basic API information and documentation.

        Raises
        ------
        http.client.HTTPException
             If the rate limit is exceeded, it returns an HTTP 429 ("Too Many Requests") response to the client.
        """

        def _package_version(package_name):
            # Get plantdb.server version
            from importlib.metadata import version, PackageNotFoundError
            try:
                package_version = version(package_name)
            except PackageNotFoundError:
                package_version = "unknown"
            return package_version

        api_info = {
            "name": "PlantDB REST API",
            "description": "RESTful API for querying PlantDB",
            "plantdb.commons": _package_version("plantdb.commons"),
            "plantdb.server": _package_version("plantdb.server"),

            "base endpoints": {
                "/": "Provides general information about the PlantDB REST API.",
                "/health": "Health‑check endpoint that verifies the API is operational.",
                "/refresh/<scan_id>": "Refreshes the database or a specific scan if provided."
            },

            "authentication endpoints": {
                "/login": "Logs a user in.",
                "/logout": "Logs a user out.",
                "/register": "Registers a new user.",
                "/token-validation": "Validates a token.",
                "/token-refresh": "Refreshes a user’s access and refresh tokens.",
                "/create-api-token": "Creates a new API token."
            },

            "scans endpoints": {
                "/scans": "Returns a list of all available scans.",
                "/scans_info": "Provides a table containing scan metadata.",
                "/scan/<scan_id>": "Retrieves an existing scan or creates a new one.",
                "/scan/<scan_id>/metadata": "Gets or updates metadata for the specified scan.",
                "/scan/<scan_id>/filesets": "Lists the filesets belonging to the specified scan."
            },

            "filesets endpoints": {
                "/fileset/<scan_id>/<fileset_id>": "Retrieves an existing fileset or creates a new one.",
                "/fileset/<scan_id>/<fileset_id>/metadata": "Gets or updates metadata for the specified fileset.",
                "/fileset/<scan_id>/<fileset_id>/files": "Lists the files contained in the specified fileset."
            },

            "files endpoints": {
                "/file/<scan_id>/<fileset_id>/<file_id>": "Retrieves an existing file or creates a new one.",
                "/file/<scan_id>/<fileset_id>/<file_id>/metadata": "Gets or updates metadata for the specified file."
            },

            "assets endpoints": {
                "/archive/<scan_id>": "Downloads or updates the archive for the given scan.",
                "/files/<path>": "Retrieves a file located at the specified path.",
                "/image/<scan_id>/<fileset_id>/<file_id>": "Returns a specific image.",
                "/pointcloud/<scan_id>/<fileset_id>/<file_id>": "Returns a specific point‑cloud file.",
                "/pcGroundTruth/<scan_id>/<fileset_id>/<file_id>": "Returns a ground‑truth point‑cloud file.",
                "/mesh/<scan_id>/<fileset_id>/<file_id>": "Returns a specific mesh file.",
                "/sequence/<scan_id>": "Returns sequence data for the given scan.",
                "/skeleton/<scan_id>": "Returns curve‑skeleton data for the given scan."
            }
        }
        return api_info


# Resource HealthCheck
class HealthCheck(Resource):
    """Simple health‑check resource exposing an endpoint that verifies the API and its database connectivity.

    Attributes
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        The database providing the resources to serve.
    logger : logging.Logger
        The logger used to record operations and errors.
    """

    def __init__(self, db, logger=None):
        """Initialize the resource.

        Parameters
        ----------
        db : plantdb.commons.fsdb.core.FSDB
            A database instance providing the resources to serve.
        logger : logging.Logger
            A logger instance to record operations and errors.
        """
        self.db: FSDB = db
        self.logger: logging.Logger = logger if logger else get_logger(self.__class__.__name__)

    @rate_limit(max_requests=120, window_seconds=60)
    def get(self):
        """Simple test endpoint to verify the API is working correctly.

        Raises
        ------
        http.client.HTTPException
             If the rate limit is exceeded, it returns an HTTP 429 ("Too Many Requests") response to the client.
        """
        try:
            # Try to check database connection
            scan_count = len(self.db.list_scans(owner_only=False))
            return {
                "status": "healthy",
                "message": "API is running correctly",
                "database": {
                    "location": str(self.db.path()),
                    "scan_count": scan_count
                }
            }, 200
        except Exception as e:
            return {
                "status": "error",
                "message": f"API encountered an issue: {str(e)}"
            }, 500  # HTTP 500 Internal Server Error


class Refresh(Resource):
    """RESTful resource for reloading the database on demand.

    A concrete implementation of Flask-RESTful Resource that provides an endpoint
    to force reload the plant database. This is useful when the underlying data
    has changed and needs to be refreshed in the running application.

    Attributes
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        The database providing the resources to serve.
    logger : logging.Logger
        The logger used to record operations and errors.
    """

    def __init__(self, db, logger=None):
        """Initialize the resource.

        Parameters
        ----------
        db : plantdb.commons.fsdb.core.FSDB
            A database instance providing the resources to serve.
        logger : logging.Logger
            A logger instance to record operations and errors.
        """
        self.db: FSDB = db
        self.logger: logging.Logger = logger if logger else get_logger(self.__class__.__name__)

    @sanitize_ids('scan_id')
    @rate_limit(max_requests=60, window_seconds=60)
    def get_specific_scan(self, scan_id):
        """Reload data for a specific scan in the database.

        Parameters
        ----------
        scan_id : str
            Identifier for the specific plant scan to reload

        Returns
        -------
        dict, int
            A dictionary with a success message and HTTP status code 200,
            or an error message and status code 500

        Raises
        ------
        http.client.HTTPException
             If the rate limit is exceeded, it returns an HTTP 429 ("Too Many Requests") response to the client.
        """
        try:
            self.db.reload(scan_id)
            return {'message': f"Successfully reloaded scan '{scan_id}'."}, 200
        except Exception as e:
            return {'message': f"Error during scan reload: {str(e)}"}, 500  # HTTP 500 Internal Server Error

    @rate_limit(max_requests=12, window_seconds=60)
    def get_full_database(self):
        """Reload the entire plant database.

        Returns
        -------
        dict, int
            A dictionary with a success message and HTTP status code 200,
            or an error message and status code 500

        Raises
        ------
        http.client.HTTPException
             If the rate limit is exceeded, it returns an HTTP 429 ("Too Many Requests") response to the client.
        """
        try:
            self.db.reload(None)
            return {'message': f"Successfully reloaded entire database with {len(self.db.list_scans())} scans."}, 200
        except Exception as e:
            return {'message': f"Error during full database reload: {str(e)}"}, 500  # HTTP 500 Internal Server Error

    def get(self):
        """Force the plant database to reload.

        This endpoint triggers a reload of the plant database data. It can either reload the
        entire database or selectively reload data for a specific plant scan.

        Returns
        -------
        flask.Response
            A Response object with:

            - Status code ``200`` and success message on successful reload
            - Status code ``500`` and error message if reload fails

        Raises
        ------
        plantdb.commons.fsdb.exceptions.FilesetNotFoundError
            If the specified scan_id refers to a non-existent fileset
        plantdb.commons.fsdb.exceptions.ScanNotFoundError
            If the specified scan_id refers to a non-existent scan
        Exception
            For any other unexpected errors during reload

        Notes
        -----
        - In the URL, you can use the `scan_id` parameter to reload a specific scan.
        - If no scan_id is provided, reloads the entire database.
        - This endpoint has a request rate-limit to prevent excessive database reloads.

        See Also
        --------
        plantdb.server.rest_api.rate_limit
        plantsb.fsdb.FSDB.reload

        Examples
        --------
        >>> # Start the REST API server (in test mode)
        >>> # fsdb_rest_api --test
        >>> import requests
        >>> # Refresh the entire database
        >>> response = requests.get("http://127.0.0.1:5000/refresh")
        >>> response.status_code
        200
        >>>
        >>> # Refresh a specific scan
        >>> response = requests.get("http://127.0.0.1:5000/refresh?scan_id=real_plant")
        >>> response.status_code
        200
        """
        scan_id = request.args.get('scan_id', default=None, type=str)

        if scan_id:
            return self.get_specific_scan(scan_id)
        else:
            return self.get_full_database()
