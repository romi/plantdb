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
>>> # List all scans
>>> response = requests.get("http://127.0.0.1:5000/scans")
>>> scans = response.json()
>>> print(scans)
['real_plant_analyzed', 'real_plant', 'virtual_plant_analyzed', 'virtual_plant']
>>> # Run a filtered, fuzzy search on metadata
>>> query = {"object": {"environment":"Lyon.*"}}
>>> response = requests.get("http://127.0.0.1:5000/scans", params= {"filterQuery": query, "fuzzy": "true"})
>>> filtered_scans = response.json()
>>> print(filtered_scans)  # list of scan IDs matching the filter
['real_plant_analyzed', 'real_plant']
```
"""

import json
import logging

import requests
from flask import request
from flask_restful import Resource

from plantdb.commons.auth.session import SessionValidationError
from plantdb.commons.fsdb.core import FSDB
from plantdb.commons.fsdb.exceptions import NoAuthUserError
from plantdb.commons.fsdb.exceptions import ScanExistsError
from plantdb.commons.fsdb.exceptions import ScanNotFoundError
from plantdb.commons.log import get_logger
from plantdb.server.core.security import add_jwt_from_header
from plantdb.server.core.security import rate_limit
from plantdb.server.core.security import sanitize_ids
from plantdb.server.core.security import use_guest_as_default
from plantdb.server.services.scan import get_scan_info


class ScansList(Resource):
    """A RESTful resource for managing and retrieving scan datasets.

    This class implements a REST API endpoint that provides access to scan datasets.
    It supports filtered queries and fuzzy matching capabilities through HTTP GET
    requests.

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

            - filter_query (str): JSON string representing the filter query, example: ``{"object":{"species":"Arabidopsis.*"}}``.
            - fuzzy (str): Boolean indicating whether to perform fuzzy filtering, ``false`` by default.

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> import requests
        >>> # Get a list of all datasets from the DB:
        >>> response = requests.get("http://127.0.0.1:5000/scans")
        >>> scans_list = response.json()
        >>> print(scans_list)  # List the known dataset ids
        ['arabidopsis000', 'virtual_plant_analyzed', 'real_plant_analyzed', 'real_plant', 'virtual_plant']
        >>> # Get  a list of datasets with fuzzy filtering on metadata
        >>> query = {"object": {"environment": "Lyon.*"}}
        >>> response = requests.get("http://127.0.0.1:5000/scans", params={"filterQuery": query, "fuzzy": "true"})
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
        except Exception as e:
            # Return an error response if any exception occurs
            return {'message': f'Error retrieving scan list: {str(e)}'}, 500  # HTTP 500 Internal Server Error
        else:
            return scans, 200


class ScansTable(Resource):
    """A RESTful resource for managing and retrieving scan dataset information.

    This class provides a REST API endpoint to serve information about scan datasets.
    It supports filtering datasets based on query parameters and returns detailed
    information about each matching scan.

    Attributes
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        The database providing the resources to serve.
    logger : Logger
        The logger instance for this resource.

    See Also
    --------
    plantdb.server.rest_api.get_scan_info : Function used to extract information for each scan
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
    @add_jwt_from_header
    @use_guest_as_default  # FIXME: Remove this if we want strict token identification
    def get(self, **kwargs):
        """Retrieve a list of scan dataset information.

        This method handles GET requests to retrieve scan information. It supports
        filtering through query parameters and returns detailed information about
        matching scans.

        Returns
        -------
        List[Dict]
            List of dictionaries containing scan information with:

                - metadata (dict): Scan metadata including acquisition date, object info
                - tasks (dict): Information about processing tasks
                - files (dict): File paths and URIs related to the scan

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
        >>> import json
        >>> # Get a list of information dictionaries about all datasets:
        >>> response = requests.get("http://127.0.0.1:5000/scans_info")
        >>> scans_info = response.json()
        >>> # List the known dataset id:
        >>> print(sorted(scan['id'] for scan in scans_info))
        ['arabidopsis000', 'real_plant', 'real_plant_analyzed', 'virtual_plant', 'virtual_plant_analyzed']
        >>> # Add a metadata filter to the query:
        >>> query = {"object": {"species": "Arabidopsis.*"}}
        >>> response = requests.get("http://127.0.0.1:5000/scans_info", params={"filterQuery": json.dumps(query), "fuzzy": "true"})
        >>> scans_info = response.json()
        >>> print(sorted(scan['id'] for scan in scans_info))
        ['arabidopsis000']
        """
        query = request.args.get('filterQuery', None)
        fuzzy = request.args.get('fuzzy', False, type=bool)

        if query is not None:
            query = json.loads(query)

        scans_list = self.db.list_scans(query=query, fuzzy=fuzzy, owner_only=False)

        scans_info = []
        for scan_id in scans_list:
            try:
                scan = self.db.get_scan(scan_id, **kwargs)
                scan_info = get_scan_info(scan, logger=self.logger)
            except NoAuthUserError as e:
                return {'message': str(e)}, 401  # HTTP 401 Unauthorized (authentication)
            except ScanNotFoundError as e:
                return {'message': str(e)}, 404  # HTTP 404 Not Found
            except Exception as e:
                return {'message': f"Error while accessing '{scan_id}': {str(e)}"}, 404  # HTTP 404 Not Found
            else:
                scans_info.append(scan_info)
        return scans_info, 200


class Scan(Resource):
    """A RESTful resource class for serving scan dataset information.

    This class handles HTTP GET requests for scan datasets, providing detailed
    information about the scan, including metadata, file locations, and task status.

    Attributes
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        The database providing the resources to serve.
    logger : logging.Logger
        The logger used to record operations and errors.

    Notes
    -----
    The class sanitizes scan IDs before processing requests to ensure security.
    All responses are returned as JSON-serializable dictionaries.

    See Also
    --------
    plantdb.server.rest_api.get_scan_info : Function used to collect and format scan information
    plantdb.server.rest_api.sanitize_name : Function used to validate and clean scan IDs
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
    @rate_limit(max_requests=120, window_seconds=60)
    @add_jwt_from_header
    @use_guest_as_default  # FIXME: Remove this if we want strict token identification
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
        >>> response = requests.get("http://127.0.0.1:5000/scan/real_plant_analyzed")
        >>> scan_data = response.json()
        >>> # Access metadata information
        >>> print(scan_data['metadata']['nbPhotos'])
        60
        """
        try:
            scan = self.db.get_scan(scan_id, **kwargs)
            scan_info = get_scan_info(scan, logger=self.logger)

        except NoAuthUserError as e:
            return {'message': str(e)}, 401  # HTTP 401 Unauthorized (authentication)
        except ScanNotFoundError as e:
            return {'message': str(e)}, 404  # HTTP 404 Not Found
        except Exception as e:
            return {'message': f"Error while accessing '{scan_id}': {str(e)}"}, 404  # HTTP 404 Not Found
        else:
            return scan_info, 200

    @rate_limit(max_requests=15, window_seconds=60)
    @sanitize_ids('scan_id')
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
            A dictionary containing the response with the following possible structures:

                - On success: {'message': 'Scan created successfully', 'id': scan_id}
                - On error: {'message': error_message}

        Raises
        ------
        http.client.HTTPException
             If the rate limit is exceeded, it returns an HTTP 429 ("Too Many Requests") response to the client.

        Notes
        -----
        The request body accepts a JSON object containing:

            - 'metadata' (dict): the new metadata

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> import requests
        >>> # Start by log in as 'admin'
        >>> response = requests.post('http://127.0.0.1:5000/login', json={'username': 'admin', 'password': 'admin'})
        >>> token = response.json()['access_token']
        >>> # Create a new scan dataset
        >>> response = requests.post("http://127.0.0.1:5000/scan/test_scan_001", headers={'Authorization': 'Bearer ' + token})
        >>> print(response.ok)
        True
        >>> scan_data = response.json()
        >>> print(scan_data['id'])
        test_scan_001
        >>> md = {'metadata': {'test_metadata': True}}
        >>> response = requests.post("http://127.0.0.1:5000/scan/test_scan_md", json=md, headers={'Authorization': 'Bearer ' + token})
        >>> print(response.ok)
        """
        # Get JSON data from request
        data = request.get_json(silent=True)
        # Get metadata if provided
        if data:
            metadata = data.get('metadata', None)
        else:
            metadata = None

        try:
            # Attempt to create a new scan in the database with the given scan_id
            scan = self.db.create_scan(scan_id, metadata=metadata, **kwargs)
            # Check if scan creation was successful
            if scan is None:
                self.logger.error(f"Failed to create scan: {scan_id}")
                return {'message': 'Failed to create scan'}, 500  # HTTP 500 Internal Server Error
            self.logger.info(f"Successfully created scan: {scan_id}")

        except NoAuthUserError as e:
            return {'message': str(e)}, 401  # HTTP 401 Unauthorized (authentication)
        except ScanExistsError as e:
            return {'message': str(e)}, 409  # HTTP 409 Conflict
        except Exception as e:
            # Handle all other exceptions including duplicate scans
            self.logger.error(f"Error creating scan {scan_id}: {str(e)}")
            # Return generic server error for all other exceptions
            return {'message': f'Internal server error: {str(e)}'}, 500  # HTTP 500 Internal Server Error
        else:
            # Return success response with HTTP 201 (Created) status code
            return {'message': 'Scan created successfully', 'id': scan.id}, 201


class ScanMetadata(Resource):
    """Resource class for managing scan metadata operations through REST API endpoints.

    This class provides HTTP endpoints for retrieving and updating scan metadata through
    a RESTful interface. It handles both complete metadata operations and individual key access.

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
    @rate_limit(max_requests=120, window_seconds=60)
    @add_jwt_from_header
    @use_guest_as_default  # FIXME: Remove this if we want strict token identification
    def get(self, scan_id, **kwargs):
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

            - key (str): If provided, returns only the value for this specific metadata key.

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> import requests
        >>> url = "http://127.0.0.1:5000/scan/real_plant/metadata"
        >>> response = requests.get(url)  # Get all metadata
        >>> metadata = response.json()['metadata']
        >>> print(metadata['owner'])
        guest
        >>> response = requests.get(url+"?key=owner")  # Get a specific metadata key
        >>> print(response.json()['metadata'])
        guest
        """
        key = request.args.get('key', default=None, type=str)
        if key:
            self.logger.debug(f"Got a metadata key '{key}' for scan '{scan_id}'...")

        try:
            # Get the scan
            scan = self.db.get_scan(scan_id, **kwargs)
            # Get the metadata
            metadata = scan.get_metadata(key)

        except NoAuthUserError as e:
            return {'message': str(e)}, 401  # HTTP 401 Unauthorized (authentication)
        except ScanNotFoundError as e:
            return {'message': str(e)}, 404  # HTTP 404 Not Found
        except Exception as e:
            self.logger.error(f'Error retrieving metadata: {str(e)}')
            return {'message': f'Error retrieving metadata: {str(e)}'}, 500  # HTTP 500 Internal Server Error
        else:
            return {'metadata': metadata}, 200

    @sanitize_ids('scan_id')
    @rate_limit(max_requests=120, window_seconds=60)
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
        The request body accepts a JSON object containing:

            - 'metadata' (dict): the new metadata

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> import requests
        >>> # Start by log in as 'admin'
        >>> response = requests.post('http://127.0.0.1:5000/login', json={'username': 'admin', 'password': 'admin'})
        >>> token = response.json()['access_token']
        >>> # Get the whole metadata dictionary for an existing dataset:
        >>> url = "http://127.0.0.1:5000/scan/real_plant/metadata"
        >>> response = requests.get(url)  # Get all metadata
        >>> metadata = response.json()['metadata']
        >>> # Update the original metadata dictionary and upload it to the database:
        >>> metadata['object']['description'] = 'Test plant scan'
        >>> response = requests.post(url, json={'metadata': metadata}, headers={'Authorization': 'Bearer ' + token})
        >>> print(response.ok)
        True
        >>> print(response.json()['metadata']['object']['description'])
        Test plant scan
        """
        # Get request data
        data = request.get_json()
        if not data or 'metadata' not in data:
            return {'message': 'No metadata provided in request'}, 400

        metadata = data['metadata']

        if not isinstance(metadata, dict):
            return {'message': 'Metadata must be a dictionary'}, 400

        try:
            # Get the scan
            scan = self.db.get_scan(scan_id, **kwargs)
            # Update the metadata
            scan.set_metadata(metadata, **kwargs)
            # Return updated metadata
            updated_metadata = scan.get_metadata()

        except NoAuthUserError as e:
            return {'message': str(e)}, 401  # HTTP 401 Unauthorized (authentication)
        except ScanNotFoundError as e:
            return {'message': str(e)}, 404  # HTTP 404 Not Found
        except SessionValidationError as e:
            return {'message': 'Invalid credentials'}, 401  # HTTP 401 Unauthorized (authentication)
        except Exception as e:
            self.logger.error(f'Error updating {scan_id} scan metadata: {str(e)}')
            return {'message': f'Error updating {scan_id} scan metadata: {str(e)}'}, 500  # HTTP 500 Internal Server Error
        else:
            return {'metadata': updated_metadata}, 200


class ScanFilesets(Resource):
    """REST API resource for managing scan filesets operations.

    This class provides endpoints to interact with filesets within a scan. It allows
    listing filesets with optional query filtering and fuzzy matching capabilities.

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
    @add_jwt_from_header
    @use_guest_as_default  # FIXME: Remove this if we want strict token identification
    def get(self, scan_id, **kwargs):
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
        >>> # List filesets in a scan:
        >>> url = "http://127.0.0.1:5000/scan/real_plant/filesets"
        >>> response = requests.get(url)
        >>> print(response.status_code)
        200
        >>> print(response.json())
        {'filesets': ['images']}
        >>> url = "http://127.0.0.1:5000/scan/real_plant_analyzed/filesets"
        >>> response = requests.get(url)
        >>> print(len(response.json()['filesets']))  # Get the number of filesets
        10
        """
        query = request.args.get('query', default=None, type=str)
        fuzzy = request.args.get('fuzzy', default=False, type=bool)

        try:
            # Get the scan
            scan = self.db.get_scan(scan_id, **kwargs)
            # Get the list of filesets
            filesets = scan.list_filesets(query, fuzzy)

        except NoAuthUserError as e:
            return {'message': str(e)}, 401  # HTTP 401 Unauthorized (authentication)
        except ScanNotFoundError as e:
            return {'message': str(e)}, 404  # HTTP 404 Not Found
        except Exception as e:
            self.logger.error(f'Error listing filesets: {str(e)}')
            return {'message': f'Error listing filesets: {str(e)}'}, 500  # HTTP 500 Internal Server Error
        else:
            return {'filesets': filesets}, 200
