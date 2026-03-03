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
# Fileset REST API Resources

Provides Flask-RESTful resources for managing filesets within the PlantDB server, allowing for the creation of filesets, management of their metadata, and listing of associated files.
These resources interface with the underlying filesystem database (FSDB) to provide a standardized API for handling grouped data assets and their descriptive information.

## Key Features

- **FilesetCreate** - Create new filesets associated with specific scans, including initial metadata assignment and name sanitization.
- **FilesetMetadata** - Retrieve or update metadata for a specific fileset, supporting both full dictionary retrieval and specific key lookups.
- **FilesetFiles** - List and query files contained within a fileset, with support for fuzzy searching and filtering.
- **Security** - Integrated JWT validation via decorators to ensure authorized access to data modification endpoints.

## Usage Examples

Hereafter is a minimal working example that:

1. Creates a `Flask` app
2. Sets up a local test database
3. Registers the `FilesetMetadata` resource to a REST API
4. Starts the app

```python
>>> import logging
>>> from flask import Flask
>>> from flask_restful import Api
>>> from plantdb.server.api.fileset import FilesetMetadata
>>> from plantdb.commons.auth.session import JWTSessionManager
>>> from plantdb.commons.fsdb.core import FSDB
>>> from plantdb.commons.test_database import setup_test_database
>>> # Create a Flask application
>>> app = Flask(__name__)
>>> # Create a logger
>>> logger = logging.getLogger("plantdb.fileset")
>>> logger.setLevel(logging.INFO)
>>> # Initialize a test database with a JWTSessionManager
>>> db_path = setup_test_database('real_plant')
>>> mgr = JWTSessionManager()
>>> db = FSDB(db_path, session_manager=mgr)
>>> db.connect()
>>> # RESTful API and resource registration
>>> api = Api(app)
>>> api.add_resource(FilesetMetadata, "/fileset/<string:scan_id>/<string:fileset_id>/metadata", resource_class_kwargs={"db": db, "logger": logger})
>>> # Start the APP
>>> app.run(host='0.0.0.0', port=5000)
```

It may be used as follows (in another Python REPL):
```python
>>> import requests
>>> # Retrieve metadata for the 'images' fileset in the 'real_plant' scan
>>> url = "http://127.0.0.1:5000/fileset/real_plant/images/metadata"
>>> response = requests.get(url)
>>> print(response.json())
{'metadata': {'channels': ['rgb'], 'object': {'age': '0', 'environment': 'Lyon indoor', 'experiment_id': 'calibration01', 'object': 'random objects'}, 'task_params': {'fileset_id': 'images', 'output_file_id': 'out', 'scan_id': ''}, 'workspace': {'x': [340, 440], 'y': [330, 410], 'z': [-180, 105]}}}
>>> # Retrieve a specific metadata key
>>> response = requests.get(url + "?key=channels")
>>> print(response.json())
{'metadata': ['rgb']}
```
"""
import logging

import requests
from flask import request
from flask_restful import Resource

from plantdb.commons.fsdb.core import FSDB
from plantdb.commons.fsdb.exceptions import FilesetExistsError
from plantdb.commons.fsdb.exceptions import FilesetNotFoundError
from plantdb.commons.fsdb.exceptions import NoAuthUserError
from plantdb.commons.fsdb.exceptions import ScanNotFoundError
from plantdb.commons.log import get_logger
from plantdb.server.core.security import add_jwt_from_header
from plantdb.server.core.security import rate_limit
from plantdb.server.core.security import sanitize_ids
from plantdb.server.core.security import use_guest_as_default


class Fileset(Resource):
    """Represents a Fileset resource in the application.

    This class provides the functionality to create and manage filesets associated with scans.

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
    @sanitize_ids('fileset_id')
    @rate_limit(max_requests=60, window_seconds=60)
    @add_jwt_from_header
    def post(self, scan_id, fileset_id, **kwargs):
        """Create a new fileset associated with a scan.

        This method handles POST requests to create a new fileset. It validates the input data,
        creates the fileset with the specified name, and associates it with the given scan ID.
        Optional metadata can be attached to the fileset.

        Returns
        -------
        dict
            Response containing a success message or error description.
            If successful, also returns the created fileset ID under 'id' key, as sanitization may have happened.
        int
            HTTP status code (201, 400, 404, or 500)

        Notes
        -----
        The method accepts a JSON request body with the following structure:

            - metadata (dict, optional): Additional metadata for the fileset.

        Raises
        ------
        Exception
            Any unexpected errors during fileset creation are caught and
            returned as 500 error responses.

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> import requests
        >>> # Start by log in as 'admin'
        >>> response = requests.post('http://127.0.0.1:5000/login', json={'username': 'admin', 'password': 'admin'})
        >>> token = response.json()['access_token']
        >>> # Create a new fileset with metadata:
        >>> scan_id = 'real_plant'
        >>> fileset_id = 'test_fileset'
        >>> metadata = {'description': 'This is a test fileset'}
        >>> url = f"http://127.0.0.1:5000/fileset/{scan_id}/{fileset_id}"
        >>> response = requests.post(url, json={'metadata': metadata}, headers={'Authorization': 'Bearer ' + token})
        >>> response = requests.post(url, json={'metadata': metadata})
        >>> print(response.status_code)
        201
        >>> print(response.json())
        {'message': "Fileset 'my_fileset' created successfully in 'real_plant'."}
        """
        # Get JSON data from request
        data = request.get_json(silent=True)
        # Get metadata if provided
        if data:
            metadata = data.get('metadata', None)
        else:
            metadata = None

        try:
            # Get the scan
            scan = self.db.get_scan(scan_id, **kwargs)

        except NoAuthUserError as e:
            return {'message': str(e)}, 401  # HTTP 401 Unauthorized (authentication)
        except ScanNotFoundError as e:
            return {'message': str(e)}, 404  # HTTP 404 Not Found
        except Exception as e:
            return {'message': f'Error accessing the scan {scan_id}: {str(e)}'}, 500  # HTTP 500 Internal Server Error

        try:
            # Create the fileset
            fileset = scan.create_fileset(fileset_id, metadata=metadata, **kwargs)

        except FilesetExistsError as e:
            return {'message': str(e)}, 409  # HTTP 409 Conflict
        except Exception as e:
            # Handle all other exceptions including duplicate scans
            self.logger.error(f"Error creating fileset {fileset_id} in scan {scan_id}: {str(e)}")
            return {'message': f'Error creating fileset {fileset_id}: {str(e)}'}, 500  # HTTP 500 Internal Server Error
        else:
            return {
                'message': f"Fileset created successfully in '{scan.id}'.",
                'id': fileset.id
            }, 201


class FilesetMetadata(Resource):
    """A REST resource for managing fileset metadata operations.

    This class provides HTTP endpoints for retrieving and updating metadata
    associated with filesets within a scan. It supports both complete metadata
    retrieval and specific key lookups, as well as partial and full metadata updates.

    Attributes
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        The database providing the resources to serve.
    logger : logging.Logger
        The logger used to record operations and errors.

    Notes
    -----
    All fileset names are sanitized before processing to ensure they contain only
    alphanumeric characters, underscores, dashes, or periods.
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
    @sanitize_ids('fileset_id')
    @rate_limit(max_requests=120, window_seconds=60)
    @add_jwt_from_header
    @use_guest_as_default  # FIXME: Remove this if we want strict token identification
    def get(self, scan_id, fileset_id, **kwargs):
        """Retrieve metadata for a specified fileset.

        This method retrieves the metadata dictionary for a fileset. Optionally, it can
        return the value for a specific metadata key.

        Parameters
        ----------
        scan_id : str
            The ID of the scan containing the fileset.
        fileset_id : str
            The name of the fileset.

        Returns
        -------
        Union[dict, Any]
            Without a 'key' URL parameter, it returns the complete metadata dictionary.
            If a 'key' URL parameter is provided, it returns the value for that key.

        Raises
        ------
        plantdb.commons.fsdb.exceptions.FilesetNotFoundError
            If the specified fileset doesn't exist.
        KeyError
            If the specified key doesn't exist in the metadata.

        Notes
        -----
        In the URL, you can use the `key` parameter to retrieve specific metadata keys.

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> import requests
        >>> # Get all metadata:
        >>> url = "http://127.0.0.1:5000/fileset/real_plant/images/metadata"
        >>> response = requests.get(url)
        >>> metadata = response.json()['metadata']
        >>> print(metadata['channels'])
        ['rgb']
        >>> # Get a specific metadata key:
        >>> response = requests.get(url+"?key=channels")
        >>> print(response.json()['metadata'])
        ['rgb']
        """
        key = request.args.get('key', default=None, type=str)
        if key:
            self.logger.debug(f"Got a metadata key '{key}' for scan '{scan_id}'...")

        try:
            # Get the scan
            scan = self.db.get_scan(scan_id, **kwargs)

        except NoAuthUserError as e:
            return {'message': str(e)}, 401  # HTTP 401 Unauthorized (authentication)
        except ScanNotFoundError as e:
            return {'message': str(e)}, 404  # HTTP 404 Not Found
        except Exception as e:
            return {'message': f'Error accessing the scan {scan_id}: {str(e)}'}, 500  # HTTP 500 Internal Server Error

        try:
            # Get the fileset
            fileset = scan.get_fileset(fileset_id)
            # Get the metadata
            metadata = fileset.get_metadata(key)

        except FilesetNotFoundError as e:
            return {'message': str(e)}, 404  # HTTP 404 Not Found
        except Exception as e:
            self.logger.error(f'Error retrieving metadata: {str(e)}')
            return {'message': f'Error retrieving metadata: {str(e)}'}, 500  # HTTP 500 Internal Server Error
        else:
            return {'metadata': metadata}, 200

    @sanitize_ids('scan_id')
    @sanitize_ids('fileset_id')
    @rate_limit(max_requests=60, window_seconds=60)
    @add_jwt_from_header
    def post(self, scan_id, fileset_id, **kwargs):
        """Update metadata for a specified fileset.

        This method handles updating metadata for a fileset within a scan. It supports both
        full metadata replacement and partial updates of specific key-value pairs.

        Parameters
        ----------
        scan_id : str
            Unique identifier for the scan containing the fileset
        fileset_id : str
            Name of the fileset to update metadata for

        Returns
        -------
        dict
            Response dictionary with either:

                - 'metadata': Updated metadata dictionary on success
                - 'message': Error message on failure
        int
            HTTP status code (200 for success, 4xx/5xx for errors)

        Raises
        ------
        Exception
            Any unexpected errors during metadata update will be caught,
            logged, and returned as a 500 error response.

        Notes
        -----
        The request body should be a JSON object containing:

            - metadata (dict): The metadata to update/set
            - replace (bool, optional). If ``True``, replaces entire metadata.
              If ``False`` (default), updates only specified keys.

        Examples
        --------
        >>> import requests
        >>> # Start by log in as 'admin'
        >>> response = requests.post('http://127.0.0.1:5000/login', json={'username': 'admin', 'password': 'admin'})
        >>> token = response.json()['access_token']
        >>> # Get all metadata:
        >>> url = "http://127.0.0.1:5000/fileset/real_plant/images/metadata"
        >>> response = requests.get(url)
        >>> metadata = response.json()['metadata']
        >>> # Update the original metadata dictionary and upload it to the database:
        >>> metadata['object']['description'] = 'Test plant scan images'
        >>> response = requests.post(url, json={'metadata': metadata}, headers={'Authorization': 'Bearer ' + token})
        >>> print(response.ok)
        True
        >>> print(response.json()['metadata']['object']['description'])
        Test plant scan images
        >>> # Replace metadata:
        >>> metadata_update = {"metadata": {"description": "Brand new description", "version": "2.0"}, "replace": True}
        >>> response = requests.post(url, json=metadata_update)
        >>> print(response.json())
        """
        # Get request data
        data = request.get_json()
        if not data or 'metadata' not in data:
            return {'message': 'No metadata provided in request'}, 400

        metadata = data['metadata']
        replace = data.get('replace', False)

        if not isinstance(metadata, dict):
            return {'message': 'Metadata must be a dictionary'}, 400

        try:
            # Get the scan
            scan = self.db.get_scan(scan_id, **kwargs)

        except NoAuthUserError as e:
            return {'message': str(e)}, 401  # HTTP 401 Unauthorized (authentication)
        except ScanNotFoundError as e:
            return {'message': str(e)}, 404  # HTTP 404 Not Found
        except Exception as e:
            return {'message': f'Error accessing the scan {scan_id}: {str(e)}'}, 500  # HTTP 500 Internal Server Error

        try:
            # Get the fileset
            fileset = scan.get_fileset(fileset_id)
            # Update the metadata
            fileset.set_metadata(metadata, **kwargs)
            # TODO: make this works:
            # if replace:
            #    # Replace entire metadata dictionary
            #    fileset.set_metadata(metadata)
            # else:
            #    # Update only specified keys
            #    current_metadata = fileset.get_metadata()
            #    current_metadata.update(metadata)
            #    fileset.set_metadata(current_metadata)
            # Return updated metadata
            updated_metadata = fileset.get_metadata()

        except FilesetNotFoundError as e:
            return {'message': str(e)}, 404  # HTTP 404 Not Found
        except Exception as e:
            self.logger.error(f'Error updating {fileset_id} fileset metadata: {str(e)}')
            return {'message': f'Error updating {fileset_id} fileset metadata: {str(e)}'}, 500  # HTTP 500 Internal Server Error
        else:
            return {'metadata': updated_metadata}, 200


class FilesetFiles(Resource):
    """Resource for handling fileset files operations.

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
    @sanitize_ids('fileset_id')
    @rate_limit(max_requests=120, window_seconds=60)
    @add_jwt_from_header
    @use_guest_as_default  # FIXME: Remove this if we want strict token identification
    def get(self, scan_id, fileset_id, **kwargs):
        """List all files in a specified fileset.

        Parameters
        ----------
        scan_id : str
            The ID of the scan containing the fileset.
        fileset_id : str
            The name of the fileset.

        Returns
        -------
        dict
            Response containing either:
              - On success (200): {'files': list of file information}
              - On error (404, 500): {'message': error description}
        int
            HTTP status code (200, 404, or 500)

        Notes
        -----
        This method retrieves the list of files contained in a fileset using the
        `list_files()` method from `plantdb.commons.fsdb.core.Fileset`.

        See Also
        --------
        plantdb.commons.fsdb.core.Fileset.list_files

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> import requests
        >>> # List files in a fileset:
        >>> url = f"http://127.0.0.1:5000/fileset/real_plant/images/files"
        >>> response = requests.get(url)
        >>> print(response.status_code)
        200
        >>> print(response.json())
        {'files': ['00000_rgb', '00001_rgb', '00002_rgb', '00003_rgb', '00004_rgb', '00005_rgb', '00006_rgb', '00007_rgb', '00008_rgb', '00009_rgb', '00010_rgb', '00011_rgb', '00012_rgb', '00013_rgb', '00014_rgb', '00015_rgb', '00016_rgb', '00017_rgb', '00018_rgb', '00019_rgb', '00020_rgb', '00021_rgb', '00022_rgb', '00023_rgb', '00024_rgb', '00025_rgb', '00026_rgb', '00027_rgb', '00028_rgb', '00029_rgb', '00030_rgb', '00031_rgb', '00032_rgb', '00033_rgb', '00034_rgb', '00035_rgb', '00036_rgb', '00037_rgb', '00038_rgb', '00039_rgb', '00040_rgb', '00041_rgb', '00042_rgb', '00043_rgb', '00044_rgb', '00045_rgb', '00046_rgb', '00047_rgb', '00048_rgb', '00049_rgb', '00050_rgb', '00051_rgb', '00052_rgb', '00053_rgb', '00054_rgb', '00055_rgb', '00056_rgb', '00057_rgb', '00058_rgb', '00059_rgb']}
        """
        query = request.args.get('query', default=None, type=str)
        fuzzy = request.args.get('fuzzy', default=False, type=bool)

        try:
            # Get the scan
            scan = self.db.get_scan(scan_id, **kwargs)

        except NoAuthUserError as e:
            return {'message': str(e)}, 401  # HTTP 401 Unauthorized (authentication)
        except ScanNotFoundError as e:
            return {'message': str(e)}, 404  # HTTP 404 Not Found
        except Exception as e:
            return {'message': f'Error accessing the scan {scan_id}: {str(e)}'}, 500  # HTTP 500 Internal Server Error

        try:
            # Get the fileset
            fileset = scan.get_fileset(fileset_id)
            # Get the list of files
            files = fileset.list_files(query, fuzzy)

        except FilesetNotFoundError as e:
            return {'message': str(e)}, 404  # HTTP 404 Not Found
        except Exception as e:
            self.logger.error(f'Error listing files: {str(e)}')
            return {'message': f'Error listing files: {str(e)}'}, 500  # HTTP 500 Internal Server Error
        else:
            return {'files': files}, 200
