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

Provides Flask-RESTful resources that expose the PlantDB database through a
well-documented HTTP API. The module bundles endpoints for health checks,
metadata queries, file access, and on-demand database reloading, all protected
by configurable rate-limiting to safeguard the service.

## Key Features

- **Health check endpoint**: quickly verify that the service and underlying
  database are operational.
- **Dynamic database refresh**: reload a single scan or the entire dataset
  without restarting the server.
- **Self-describing root resource**: returns API name, description,
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
>>> from plantdb.commons import api_endpoints
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

from plantdb.commons import api_endpoints
from plantdb.commons.api_endpoints import ARCHIVE
from plantdb.commons.api_endpoints import CREATE_API_TOKEN
from plantdb.commons.api_endpoints import FILE
from plantdb.commons.api_endpoints import FILESET
from plantdb.commons.api_endpoints import FILESET_FILES
from plantdb.commons.api_endpoints import FILESET_MD
from plantdb.commons.api_endpoints import FILE_MD
from plantdb.commons.api_endpoints import FILE_PATH
from plantdb.commons.api_endpoints import HEALTH
from plantdb.commons.api_endpoints import HOME
from plantdb.commons.api_endpoints import IMAGE
from plantdb.commons.api_endpoints import LOGIN
from plantdb.commons.api_endpoints import LOGOUT
from plantdb.commons.api_endpoints import MESH
from plantdb.commons.api_endpoints import POINTCLOUD
from plantdb.commons.api_endpoints import REFRESH
from plantdb.commons.api_endpoints import REGISTER
from plantdb.commons.api_endpoints import SCAN
from plantdb.commons.api_endpoints import SCANS
from plantdb.commons.api_endpoints import SCANS_INFO
from plantdb.commons.api_endpoints import SCAN_FILESETS
from plantdb.commons.api_endpoints import SCAN_MD
from plantdb.commons.api_endpoints import SEQUENCE
from plantdb.commons.api_endpoints import SKELETON
from plantdb.commons.api_endpoints import TOKEN_REFRESH
from plantdb.commons.api_endpoints import TOKEN_VALIDATION
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

    def __init__(self, db, logger=None, deploy_prefix=""):
        """Initialize the resource.

        Parameters
        ----------
        db : plantdb.commons.fsdb.core.FSDB
            A database instance providing the resources to serve.
        logger : logging.Logger
            A logger instance to record operations and errors.
        deploy_prefix : str, optional
            Deployment (reverse-proxy) prefix prepended before ``/api/v1/...``
            when generating endpoint URLs in responses.
        """
        self.db: FSDB = db
        self.logger: logging.Logger = logger if logger else get_logger(self.__class__.__name__)
        self.deploy_prefix: str = deploy_prefix

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

        p  = self.deploy_prefix

        api_info = {
            "name": "PlantDB REST API",
            "description": "RESTful API for querying PlantDB",
            "plantdb.commons": _package_version("plantdb.commons"),
            "plantdb.server": _package_version("plantdb.server"),

            "base endpoints": {
                api_endpoints.home(prefix=p): "Provides general information about the PlantDB REST API.",
                api_endpoints.health(prefix=p): "Health‑check endpoint that verifies the API is operational.",
                api_endpoints.refresh(prefix=p) + f"?scan_id='scan_id'": "Refreshes the database or a specific scan if provided."
            },

            "authentication endpoints": {
                api_endpoints.register(prefix=p): "Registers a new user.",
                api_endpoints.login(prefix=p): "Logs a user in.",
                api_endpoints.logout(prefix=p): "Logs a user out.",
                api_endpoints.token_validation(prefix=p): "Validates a token.",
                api_endpoints.token_refresh(prefix=p): "Refreshes a user's access and refresh tokens.",
                api_endpoints.create_api_token(prefix=p): "Creates a new API token."
            },

            "scans endpoints": {
                api_endpoints.scans(prefix=p): "Returns a list of all available scans.",
                api_endpoints.scans_info(prefix=p): "Provides a table containing scan metadata.",
                api_endpoints.scan('scan_id', prefix=p): "Retrieves an existing scan or creates a new one.",
                api_endpoints.scan_metadata('scan_id', prefix=p): "Gets or updates metadata for the specified scan.",
                api_endpoints.scan_filesets_list('scan_id', prefix=p): "Lists the filesets belonging to the specified scan."
            },

            "filesets endpoints": {
                api_endpoints.fileset('scan_id', 'fileset_id', prefix=p): "Retrieves an existing fileset or creates a new one.",
                api_endpoints.fileset_metadata('scan_id', 'fileset_id', prefix=p): "Gets or updates metadata for the specified fileset.",
                api_endpoints.fileset_files_list('scan_id', 'fileset_id', prefix=p): "Lists the files contained in the specified fileset."
            },

            "files endpoints": {
                api_endpoints.file('scan_id', 'fileset_id', 'file_id', prefix=p): "Retrieves an existing file or creates a new one.",
                api_endpoints.file_metadata('scan_id', 'fileset_id', 'file_id', prefix=p): "Gets or updates metadata for the specified file."
            },

            "assets endpoints": {
                api_endpoints.file_path('file_path', prefix=p): "Retrieves a file located at the specified path.",
                api_endpoints.image('scan_id', 'fileset_id', 'file_id', prefix=p): "Returns a specific image.",
                api_endpoints.archive('scan_id', prefix=p): "Downloads or updates the archive for the given scan.",
                api_endpoints.pointcloud('scan_id', prefix=p): "Returns a specific point‑cloud file.",
                api_endpoints.mesh('scan_id', prefix=p): "Returns a specific mesh file.",
                api_endpoints.sequence('scan_id', prefix=p): "Returns sequence data for the given scan.",
                api_endpoints.skeleton('scan_id', prefix=p): "Returns curve‑skeleton data for the given scan."
            }
        }
        return api_info


# Resource HealthCheck
class HealthCheck(Resource):
    """Simple health-check resource exposing an endpoint that verifies the API and its database connectivity.

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

        Examples
        --------
        >>> # Start the REST API server (in test mode)
        >>> from plantdb.server.test_rest_api import TestRestApiServer
        >>> # Create a test database and start the Flask App serving a REST API
        >>> server = TestRestApiServer(test=True)
        >>> server.start()

        >>> import requests
        >>> from plantdb.commons import api_endpoints
        >>> response = requests.get("http://127.0.0.1:5000" + api_endpoints.health())
        >>> response.ok
        True
        >>> # Stop the test server
        >>> server.stop()
        """
        try:
            # Try to check database connection
            scan_count = len(self.db.list_scans(owner_only=False))
        except Exception as e:
            return {
                "status": "error",
                "error": f"API encountered an issue: {str(e)}"
            }, 500  # HTTP 500 Internal Server Error
        else:
            return {
                "status": "healthy",
                "message": "API is running correctly",
                "database": {
                    "location": str(self.db.path()),
                    "scan_count": scan_count
                }
            }, 200


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
        except Exception as e:
            return {'error': f"Error during scan reload: {str(e)}"}, 500  # HTTP 500 Internal Server Error
        else:
            return {'message': f"Successfully reloaded scan '{scan_id}'."}, 200

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
        except Exception as e:
            return {'error': f"Error during full database reload: {str(e)}"}, 500  # HTTP 500 Internal Server Error
        else:
            return {'message': f"Successfully reloaded entire database with {len(self.db.list_scans())} scans."}, 200

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
        plantdb.server.core.security.rate_limit
        plantsb.fsdb.FSDB.reload

        Examples
        --------
        >>> # Start the REST API server (in test mode)
        >>> from plantdb.server.test_rest_api import TestRestApiServer
        >>> # Create a test database and start the Flask App serving a REST API
        >>> server = TestRestApiServer(test=True)
        >>> server.start()

        >>> import requests
        >>> from plantdb.commons import api_endpoints
        >>> response = requests.get("http://127.0.0.1:5000" + api_endpoints.refresh())
        >>> response.ok
        True
        >>> response = requests.get("http://127.0.0.1:5000" + api_endpoints.refresh('real_plant'))
        >>> response.ok
        True
        >>> # Stop the test server
        >>> server.stop()
        """
        scan_id = request.args.get('scan_id', default=None, type=str)

        if scan_id:
            return self.get_specific_scan(scan_id)
        else:
            return self.get_full_database()
