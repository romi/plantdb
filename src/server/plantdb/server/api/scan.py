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
# Scan REST API Resources

Provides Flask‑RESTful resources for managing scan datasets in PlantDB.
The resources expose endpoints that allow clients to list scans, retrieve detailed
information, create new scans, and manipulate scan metadata and filesets.
These APIs simplify integration with PlantDB by handling request parsing,
security (rate‑limiting and JWT extraction), and data sanitization.

## Key Features

- **ScansList** - Returns a list of scan identifiers with optional JSON‑based
  filtering and fuzzy matching.
- **ScansTable** - Supplies detailed information (metadata, tasks, files) for
  each scan, supporting the same filtering capabilities.
- **Scan** - Retrieves or creates a single scan, performing ID sanitization and
  returning comprehensive scan data.
- **ScanCreate** - Dedicated endpoint for creating new scans with optional
  metadata.
- **ScanMetadata** - GET/POST endpoints to read and update a scan’s metadata,
  with support for partial updates or full replacement.
- **ScanFilesets** - Lists filesets contained in a scan, also supporting query
  filtering and fuzzy matching.
- **Security Integration** - Leverages JWT-based authentication via the `@add_jwt_from_header` decorator to ensure secure access to file operations.

## Usage Examples

Below is a minimal example that demonstrates how to register the **ScansList**
resource with a Flask‑RESTful application.

```python
>>> import logging
>>> from flask import Flask
>>> from flask_restful import Api
>>> from plantdb.server.api.scan import ScansList
>>> from plantdb.commons.auth.session import JWTSessionManager
>>> from plantdb.commons.fsdb.core import FSDB
>>> from plantdb.commons.test_database import setup_test_database
>>> # Create a Flask application
>>> app = Flask(__name__)
>>> # Create a logger
>>> logger = logging.getLogger("plantdb.scan")
>>> logger.setLevel(logging.INFO)
>>> # Initialize a test database with a JWTSessionManager
>>> db_path = setup_test_database('real_plant')
>>> mgr = JWTSessionManager()
>>> db = FSDB(db_path, session_manager=mgr)
>>> db.connect()
>>> # RESTful API and resource registration
>>> api = Api(app)
>>> api.add_resource(ScansList, "/scans", resource_class_kwargs={"db": db})
>>> # Start the APP
>>> app.run(host='0.0.0.0', port=5000)
```

It may be used as follows (in another Python REPL):
```python
>>> import requests
>>> import json
>>> # List all scans
>>> response = requests.get("http://127.0.0.1:5000/scans")
>>> scans = response.json()
>>> print(scans)
['real_plant_analyzed', 'real_plant', 'virtual_plant_analyzed', 'virtual_plant']
>>> # Run a filtered, fuzzy search on metadata
>>> filter_query = {"object": {"environment":"Lyon.*"}}
>>> params = {"filterQuery": json.dumps(filter_query), "fuzzy": "true"}
>>> response = requests.get("http://127.0.0.1:5000/scans", params=params)
>>> filtered_scans = response.json()
>>> print(filtered_scans)  # list of scan IDs matching the filter
['real_plant_analyzed', 'real_plant']
```
"""

import json

import requests
from flask import request
from flask import request
from flask import request
from flask import request
from flask import request
from flask import request
from flask import request
from flask import request
from flask import request
from flask_restful import Resource
from flask_restful import Resource
from flask_restful import Resource
from flask_restful import Resource
from flask_restful import Resource
from flask_restful import Resource

import plantdb
from plantdb.client.rest_api import plantdb_url
from plantdb.client.rest_api import plantdb_url
from plantdb.client.rest_api import plantdb_url
from plantdb.client.rest_api import plantdb_url
from plantdb.client.rest_api import plantdb_url
from plantdb.client.rest_api import plantdb_url
from plantdb.commons.auth.session import SessionValidationError
from plantdb.commons.auth.session import SessionValidationError

from plantdb.server.core.security import add_jwt_from_header
from plantdb.server.core.security import add_jwt_from_header
from plantdb.server.core.security import add_jwt_from_header
from plantdb.server.core.security import add_jwt_from_header
from plantdb.server.core.security import add_jwt_from_header
from plantdb.server.core.security import rate_limit
from plantdb.server.core.security import rate_limit
from plantdb.server.core.security import rate_limit
from plantdb.server.core.security import rate_limit
from plantdb.server.core.utils import sanitize_name
from plantdb.server.core.utils import sanitize_name
from plantdb.server.core.utils import sanitize_name
from plantdb.server.services.scan import get_scan_info
from plantdb.server.services.scan import get_scan_info


class ScansList(Resource):
    """A RESTful resource for managing and retrieving scan datasets.

    This class implements a REST API endpoint that provides access to scan datasets.
    It supports filtered queries and fuzzy matching capabilities through HTTP GET
    requests.

    Attributes
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        Database connection object used to interact with the scan datasets.

    Parameters
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        Database connection object for accessing scan data.

    See Also
    --------
    flask_restful.Resource : Base class for RESTful resources
    """

    def __init__(self, db):
        """Initialize the ScansList resource."""
        self.db = db

    @rate_limit(max_requests=30, window_seconds=60)
    def get(self):
        """Retrieve a list of scan datasets with optional filtering.

        This endpoint provides access to scan datasets stored in the database. It allows
        filtering of results using a JSON-formatted query string and supports fuzzy
        matching for string-based searches.

        Returns
        -------
        list
            The List of scan datasets matching the filter criteria.
            Each item is a dictionary containing dataset metadata.
        int
            HTTP status code (``200`` for success, ``400``/``500`` for errors).

        Raises
        ------
        http.client.HTTPException
             If the rate limit is exceeded, it returns an HTTP 429 ("Too Many Requests") response to the client.

        Notes
        -----
        The method can take direct parameters in the request body with the following fields:
            - filter_query: JSON string representing the filter query, example: ``{"object":{"species":"Arabidopsis.*"}}``.
            - fuzzy: Boolean indicating whether to perform fuzzy filtering, ``false`` by default.

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> import requests
        >>> # Get an info dict about all datasets:
        >>> response = requests.get("http://127.0.0.1:5000/scans")
        >>> scans_list = response.json()
        >>> print(scans_list)  # List the known dataset ids
        ['arabidopsis000', 'virtual_plant_analyzed', 'real_plant_analyzed', 'real_plant', 'virtual_plant']
        >>> # Get datasets with fuzzy filtering on metadata
        >>> filter_query = {"object": {"environment": "Lyon.*"}}
        >>> response = requests.get("http://127.0.0.1:5000/scans", params={"filterQuery": json.dumps(filter_query), "fuzzy": "true"})
        >>> filtered_scans = response.json()
        >>> print(filtered_scans)  # list of scan IDs matching the filter
        ['real_plant_analyzed', 'real_plant']
        """
        try:
            # Get filter query and fuzzy match parameters from request URL
            query = request.args.get('filterQuery', None)
            fuzzy = request.args.get('fuzzy', False, type=bool)
            # Parse JSON filter query if provided
            if query is not None:
                try:
                    query = json.loads(query)
                except json.JSONDecodeError:
                    return {'message': 'Invalid JSON format in filterQuery parameter.'}, 400
            # Query database for matching scans, allowing access to all owners
            scans = self.db.list_scans(query=query, fuzzy=fuzzy, owner_only=False)
            return scans, 200
        except Exception as e:
            # Return error response if any exception occurs
            return {'message': f'Error retrieving scan list: {str(e)}'}, 500


class ScansTable(Resource):
    """A RESTful resource for managing and retrieving scan dataset information.

    This class provides a REST API endpoint to serve information about scan datasets.
    It supports filtering datasets based on query parameters and returns detailed
    information about each matching scan.

    Attributes
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        Database connection object used to interact with the scan datasets.
    logger : Logger
        The logger instance for this resource.

    See Also
    --------
    plantdb.server.rest_api.get_scan_info : Function used to extract information for each scan

    Examples
    --------
    >>> # Start a test REST API server first:
    >>> # $ fsdb_rest_api --test
    >>> import requests
    >>> # Get all scan datasets
    >>> response = requests.get("http://127.0.0.1:5000/scans_info")
    >>> scans = response.json()
    >>> print(scans[0]['id'])  # print the id of the first scan dataset
    >>> print(scans[0]['metadata'])  # print the metadata of the first scan dataset
    >>> # Get filtered results using a query
    >>> query = {"object": {"species": "Arabidopsis.*"}}
    >>> response = requests.get("http://127.0.0.1:5000/scans_info", params={"filterQuery": json.dumps(query), "fuzzy": "true"})
    >>> filtered_scans = response.json()
    >>> print(filtered_scans[0]['id'])  # print the id of the first scan dataset matching the query
    """

    def __init__(self, db, logger):
        """Initialize the resource.

        Parameters
        ----------
        db : plantdb.commons.fsdb.core.FSDB
            A database instance providing access to scan data.
        logger : logging.Logger
            A logger instance for recording operations and errors.
        """
        self.db = db
        self.logger = logger

    @rate_limit(max_requests=120, window_seconds=60)
    @add_jwt_from_header
    def get(self, **kwargs):
        """Retrieve a list of scan dataset information.

        This method handles GET requests to retrieve scan information. It supports
        filtering through query parameters and returns detailed information about
        matching scans.

        Returns
        -------
        list of dict
            List of dictionaries containing scan information with:
            - 'metadata' (dict): Scan metadata including acquisition date, object info
            - 'tasks' (dict): Information about processing tasks
            - 'files' (dict): File paths and URIs related to the scan

        Raises
        ------
        JSONDecodeError
            If the provided filterQuery parameter is not valid JSON.
        http.client.HTTPException
             If the rate limit is exceeded, it returns an HTTP 429 ("Too Many Requests") response to the client.

        Notes
        -----
        The method can take direct parameters in the request body with the following fields:
            - filter_query: JSON string representing the filter query, example: ``{"object":{"species":"Arabidopsis.*"}}``.
            - fuzzy: Boolean indicating whether to perform fuzzy filtering, ``False`` by default.

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> import requests
        >>> # Get a list of information dictionaries about all datasets:
        >>> response = requests.get("http://127.0.0.1:5000/scans_info")
        >>> scans_info = response.json()
        >>> # List the known dataset id:
        >>> print(sorted(scan['id'] for scan in scans_info))
        ['arabidopsis000', 'real_plant', 'real_plant_analyzed', 'virtual_plant', 'virtual_plant_analyzed']
        >>> # Add a metadata filter to the query:
        >>> response = requests.get('http://127.0.0.1:5000/scans_info?filterQuery={"object":{"species":"Arabidopsis.*"}}&fuzzy="true"')
        >>> scans_info = response.json()
        >>> print(sorted(scan['id'] for scan in scans_info))
        ['virtual_plant', 'virtual_plant_analyzed']
        """
        query = request.args.get('filterQuery', None)
        fuzzy = request.args.get('fuzzy', False, type=bool)
        if query is not None:
            query = json.loads(query)
        scans_list = self.db.list_scans(query=query, fuzzy=fuzzy, owner_only=False)

        scans_info = []
        for scan_id in scans_list:
            scans_info.append(get_scan_info(self.db.get_scan(scan_id, **kwargs), logger=self.logger))
        return scans_info


class Scan(Resource):
    """A RESTful resource class for serving scan dataset information.

    This class handles HTTP GET requests for scan datasets, providing detailed
    information about the scan including metadata, file locations, and task status.

    Attributes
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        The database instance used to retrieve scan information.
    logger : logging.Logger
        The logger instance for recording operations.

    Notes
    -----
    The class sanitizes scan IDs before processing requests to ensure security.
    All responses are returned as JSON-serializable dictionaries.

    See Also
    --------
    plantdb.server.rest_api.get_scan_info : Function used to collect and format scan information
    plantdb.server.rest_api.sanitize_name : Function used to validate and clean scan IDs
    """

    def __init__(self, db, logger):
        """Initialize the resource.

        Parameters
        ----------
        db : plantdb.commons.fsdb.core.FSDB
            A database instance providing access to scan data.
        logger : logging.Logger
            A logger instance for recording operations and errors.
        """
        self.db = db
        self.logger = logger

    @rate_limit(max_requests=120, window_seconds=60)
    @add_jwt_from_header
    def get(self, scan_id, **kwargs):
        """Retrieve detailed information about a specific scan dataset.

        Parameters
        ----------
        scan_id : str
            Identifier for the scan dataset. Must contain only alphanumeric
            characters, underscores, dashes, or periods.

        Returns
        -------
        dict
            A dictionary containing scan information with the following keys:
            - 'metadata' (dict): Contains scan date, object information, and image count
            - 'files' (dict): Contains paths to related files and archives
            - 'tasks' (dict): Contains information about processing task status
            - 'thumbnail' (str): URI to the scan's thumbnail image

        Raises
        ------
        ValueError
            If the scan_id contains invalid characters
        NotFoundError
            If the specified scan does not exist in the database
        http.client.HTTPException
             If the rate limit is exceeded, it returns an HTTP 429 ("Too Many Requests") response to the client.

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> import requests
        >>> # Get detailed information about a specific dataset
        >>> response = requests.get("http://127.0.0.1:5000/scans/real_plant_analyzed")
        >>> scan_data = response.json()
        >>> # Access metadata information
        >>> print(scan_data['metadata']['date'])
        2024-08-19 11:12:25
        >>> # Check if point cloud processing is complete
        >>> print(scan_data['tasks']['point_cloud'])
        True
        """
        scan_id = sanitize_name(scan_id)
        # return get_scan_data(self.db.get_scan(scan_id), logger=self.logger)
        if self.db.scan_exists(scan_id):
            return get_scan_info(self.db.get_scan(scan_id, **kwargs), logger=self.logger)
        else:
            return {'message': f"Scan id '{scan_id}' does not exist"}, 404

    @rate_limit(max_requests=15, window_seconds=60)
    @add_jwt_from_header
    def post(self, scan_id, **kwargs):
        """Create a new scan dataset.

        Parameters
        ----------
        scan_id : str
            Identifier for the new scan dataset.
            Must contain only alphanumeric characters, underscores, dashes, or periods.

        Returns
        -------
        dict
            A dictionary containing the response with following possible structures:
                - On success: {'message': 'Scan created successfully', 'scan_id': scan_id}
                - On error: {'error': error_message}

        Raises
        ------
        http.client.HTTPException
             If the rate limit is exceeded, it returns an HTTP 429 ("Too Many Requests") response to the client.

        Notes
        -----
        HTTP status codes:
            - 201 : Created successfully
            - 400 : Bad request (invalid scan_id)
            - 409 : Conflict (scan already exists)
            - 500 : Internal server error
        """
        # Sanitize and validate the scan_id
        scan_id = sanitize_name(scan_id)
        try:
            # Attempt to create a new scan in the database with the given scan_id
            scan = self.db.create_scan(scan_id, **kwargs)
            # Check if scan creation was successful
            if scan is None:
                self.logger.error(f"Failed to create scan: {scan_id}")
                return {'error': 'Failed to create scan'}, 500
            self.logger.info(f"Successfully created scan: {scan_id}")
            # Return success response with HTTP 201 (Created) status code
            return {'message': 'Scan created successfully', 'scan_id': scan_id}, 201
        except ValueError as e:
            # Handle case where scan_id format is invalid (e.g., wrong characters or length)
            self.logger.warning(f"Invalid scan_id format: {scan_id}")
            return {'error': str(e)}, 400  # HTTP 400 Bad Request
        except Exception as e:
            # Handle all other exceptions including duplicate scans
            self.logger.error(f"Error creating scan {scan_id}: {str(e)}")
            # Check if error is due to duplicate scan_id
            if "already exists" in str(e).lower():
                return {'error': f"Scan '{scan_id}' already exists"}, 409  # HTTP 409 Conflict
            # Return generic server error for all other exceptions
            return {'error': 'Internal server error'}, 500  # HTTP 500 Internal Server Error


class ScanCreate(Resource):
    """Represents a Scan resource creation endpoint in the application.

    This class provides the functionality to create new scans in the database.

    Attributes
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        A database instance used to create scans.
    logger : logging.Logger
        A logger instance for recording operations.
    """

    def __init__(self, db, logger):
        self.db = db
        self.logger = logger

    @add_jwt_from_header
    def post(self, **kwargs):
        """Create a new scan in the database.

        This method handles POST requests to create a new scan. It validates the input data,
        ensures required fields are present, and creates the scan with the specified name
        and optional metadata.

        Notes
        -----
        The method expects a JSON request body with the following structure:
        {
            'name': str,          # Required: Name of the scan
            'metadata': dict      # Optional: Additional metadata for the scan
        }

        Raises
        ------
        Exception
            Any unexpected errors during scan creation are caught and
            returned as 500 error responses.

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> import requests
        >>> from plantdb.client.rest_api import plantdb_url
        >>> # Create a new scan with metadata:
        >>> metadata = {'description': 'Test plant scan'}
        >>> url = f"{plantdb_url('localhost', port=5000)}/api/scan"
        >>> response = requests.post(url, json={'name': 'test_plant', 'metadata': metadata})
        >>> print(response.status_code)
        201
        >>> print(response.json())
        {'message': "Scan 'test_plant' created successfully."}
        """
        # Get JSON data from request
        data = request.get_json()
        if not data:
            return {'message': 'No input data provided'}, 400
        # Validate required fields
        if 'name' not in data:
            return {'message': 'Name is required'}, 400
        # Get metadata if provided
        metadata = data.get('metadata', {})

        try:
            # Sanitize the name
            scan_id = sanitize_name(data['name'])
            # Create the scan
            scan = self.db.create_scan(scan_id, **kwargs)
            # Set metadata if provided
            if metadata:
                scan.set_metadata(metadata, **kwargs)
            return {'message': f"Scan '{scan_id}' created successfully."}, 201

        except SessionValidationError as e:
            return {'message': f'Invalid credentials: {str(e)}'}, 401

        except Exception as e:
            return {'message': f'Error creating scan: {str(e)}'}, 500


class ScanMetadata(Resource):
    """Resource class for managing scan metadata operations through REST API endpoints.

    This class provides HTTP endpoints for retrieving and updating scan metadata through
    a RESTful interface. It handles both complete metadata operations and individual key access.

    Attributes
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        Database instance for accessing and managing scan data.
    logger : logging.Logger
        Logger instance for recording operations and errors.
    """

    def __init__(self, db, logger):
        self.db = db
        self.logger = logger

    def get(self, scan_id):
        """Retrieve metadata for a specified scan.

        This method retrieves the metadata dictionary for a scan. Optionally, it can
        return the value for a specific metadata key.

        Parameters
        ----------
        scan_id : str
            The ID of the scan.

        Returns
        -------
        Union[dict, Any]
            Without a 'key' URL parameter, it returns the complete metadata dictionary.
            If a 'key' URL parameter is provided, it returns the value for that key.

        Raises
        ------
        plantdb.commons.fsdb.exceptions.ScanNotFoundError
            If the specified scan doesn't exist.
        KeyError
            If the specified key doesn't exist in the metadata.

        Notes
        -----
        The method can take direct parameters in the request body with the following fields:
            - key: If provided, returns only the value for this specific metadata key.

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> import requests
        >>> from plantdb.client.rest_api import plantdb_url
        >>> # Create a new scan with metadata:
        >>> metadata = {'description': 'Test plant scan'}
        >>> url = f"{plantdb_url('localhost', port=5000)}/api/scan"
        >>> response = requests.post(url, json={'name': 'test_plant', 'metadata': metadata})
        >>> # Get all metadata:
        >>> url = f"{plantdb_url('localhost', port=5000)}/api/scan/test_plant/metadata"
        >>> response = requests.get(url)
        >>> print(response.json())
        {'metadata': {'owner': 'guest', 'description': 'Test plant scan'}}
        >>> # Get a specific metadata key:
        >>> response = requests.get(url+"?key=description")
        >>> print(response.json())
        {'metadata': 'Test plant scan'}
        """
        key = request.args.get('key', default=None, type=str)
        try:
            # Get the scan
            scan = self.db.get_scan(scan_id)
            if not scan:
                return {'message': 'Scan not found'}, 404

            # Get the metadata
            self.logger.debug(f"Got a metadata key '{key}' for scan '{scan_id}'...")
            metadata = scan.get_metadata(key)
            return {'metadata': metadata}, 200

        except Exception as e:
            self.logger.error(f'Error retrieving metadata: {str(e)}')
            return {'message': f'Error retrieving metadata: {str(e)}'}, 500

    @add_jwt_from_header
    def post(self, scan_id, **kwargs):
        """Update metadata for a specified scan.

        Parameters
        ----------
        scan_id : str
            The ID of the scan to update metadata for

        Returns
        -------
        dict
            Response dictionary with either:
                - 'metadata': Updated metadata dictionary on success
                - 'message': Error message on failure
        int
            HTTP status code (200 for success, 4xx/5xx for errors)

        Notes
        -----
        The request body should be a JSON object containing:
        - 'metadata' (dict): Required. The metadata to update/set
        - 'replace' (bool): Optional. If ``True``, replaces entire metadata.
                           If ``False`` (default), updates only specified keys.

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> import requests
        >>> from plantdb.client.rest_api import plantdb_url
        >>> # Create a new scan with metadata:
        >>> metadata = {'description': 'Test plant scan'}
        >>> url = f"{plantdb_url('localhost', port=5000)}/api/scan"
        >>> response = requests.post(url, json={'name': 'test_plant', 'metadata': metadata})
        >>> # Update scan metadata:
        >>> url = f"{plantdb_url('localhost', port=5000)}/api/scan/test_plant/metadata"
        >>> data = {"metadata": {"description": "Updated scan description"}}
        >>> response = requests.post(url, json=data)
        >>> print(response.json())
        {'metadata': {'owner': 'guest', 'description': 'Updated scan description'}}
        """
        try:
            # Get request data
            data = request.get_json()
            if not data or 'metadata' not in data:
                return {'message': 'No metadata provided in request'}, 400

            metadata = data['metadata']
            replace = data.get('replace', False)

            if not isinstance(metadata, dict):
                return {'message': 'Metadata must be a dictionary'}, 400

            # Get the scan
            scan = self.db.get_scan(scan_id, **kwargs)
            if not scan:
                return {'message': 'Scan not found'}, 404

            # Update the metadata
            scan.set_metadata(metadata, **kwargs)
            # TODO: make this works:
            # if replace:
            #    # Replace entire metadata dictionary
            #    scan.set_metadata(metadata)
            # else:
            #    # Update only specified keys
            #    current_metadata = scan.get_metadata()
            #    current_metadata.update(metadata)
            #    scan.set_metadata(current_metadata)

            # Return updated metadata
            updated_metadata = scan.get_metadata()
            return {'metadata': updated_metadata}, 200

        except SessionValidationError as e:
            return {'message': 'Invalid credentials'}, 401

        except Exception as e:
            self.logger.error(f'Error updating metadata: {str(e)}')
            return {'message': f'Error updating metadata: {str(e)}'}, 500


class ScanFilesets(Resource):
    """REST API resource for managing scan filesets operations.

    This class provides endpoints to interact with filesets within a scan. It allows
    listing filesets with optional query filtering and fuzzy matching capabilities.

    Attributes
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        A database instance for accessing scan and create fileset.
    logger : logging.Logger
        A logger instance for recording operations.
    """

    def __init__(self, db, logger):
        self.db = db
        self.logger = logger

    def get(self, scan_id):
        """List all filesets in a specified scan.

        This method retrieves the list of filesets contained in a scan using the
        `list_filesets()` method from `plantdb.commons.fsdb.core.Scan`.

        Parameters
        ----------
        scan_id : str
            The ID of the scan.

        Returns
        -------
        dict
            Response containing either:
              - On success (200): {'filesets': list of fileset IDs}
              - On error (404, 500): {'message': error description}
        int
            HTTP status code (200, 404, or 500)

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> import requests
        >>> from plantdb.client.rest_api import plantdb_url
        >>> # List filesets in a scan:
        >>> url = f"{plantdb_url('localhost', port=5000)}/api/scan/real_plant/filesets"
        >>> response = requests.get(url)
        >>> print(response.status_code)
        200
        >>> print(response.json())
        {'filesets': ['images']}
        """
        query = request.args.get('query', default=None, type=str)
        fuzzy = request.args.get('fuzzy', default=False, type=bool)

        try:
            # Get the scan
            scan = self.db.get_scan(scan_id)
            if not scan:
                return {'message': 'Scan not found'}, 404

            # Get the list of filesets
            filesets = scan.list_filesets(query, fuzzy)
            return {'filesets': filesets}, 200

        except Exception as e:
            self.logger.error(f'Error listing filesets: {str(e)}')
            return {'message': f'Error listing filesets: {str(e)}'}, 500
