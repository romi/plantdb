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
# File REST API Resources

Provides Flask-RESTful resources for managing file creation and metadata operations within the PlantDB server.
This module allows users to upload files to specific filesets and manipulate associated metadata through a structured REST API.

## Key Features

- **File Creation** - Upload files with automatic extension validation, name sanitization, and support for both binary and text modes.
- **Metadata Retrieval** - Access complete metadata dictionaries or specific keys for any file stored in the database.
- **Metadata Management** - Update or set metadata for existing files with support for partial updates.
- **Security Integration** - Leverages JWT-based authentication via the `@add_jwt_from_header` decorator to ensure secure access to file operations.

## Usage Examples

Hereafter is a minimal working example that:

1. Creates a `Flask` app
2. Sets up a local test database
3. Registers the `FileMetadata` resource to a REST API
4. Starts the app

```python
>>> import logging
>>> from flask import Flask
>>> from flask_restful import Api
>>> from plantdb.server.api.file import FileMetadata
>>> from plantdb.commons.auth.session import JWTSessionManager
>>> from plantdb.commons.fsdb.core import FSDB
>>> from plantdb.commons.test_database import setup_test_database
>>> import logging
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
>>> api.add_resource(FileMetadata, "/file/<scan_id>/<fileset_id>/<file_id>/metadata", resource_class_kwargs={"db": db, "logger": logger})
>>> # Start the APP
>>> app.run(host='0.0.0.0', port=5000)
```

It may be used as follows (in another Python REPL):
```python
>>> import requests
>>> # Retrieve metadata for a specific file
>>> url = "http://127.0.0.1:5000/file/real_plant/images/00001_rgb/metadata"
>>> response = requests.get(url)
>>> print(response.json())
{'metadata': {'approximate_pose': [76.64343138951801, 343.64146101970397, 80, 276.0, 0], 'channel': 'rgb', 'shot_id': '000001'}}
>>> # Try to update the metadata for that file (need login)
>>> data = {"metadata": {"description": "Updated via API", "author": "bot"}}
>>> response = requests.post(url, json=data)
>>> print(response.json())
{'message': 'Error processing request: User guest does not have required permissions to use set_metadata'}
```
"""
# ... existing code ...
import json
import logging

import requests
from flask import request
from flask import send_from_directory
from flask_restful import Resource

from plantdb.client.rest_api import plantdb_url
from plantdb.commons.auth.session import SessionValidationError
from plantdb.commons.fsdb.core import FSDB
from plantdb.commons.fsdb.exceptions import FileNotFoundError
from plantdb.commons.fsdb.exceptions import FilesetNotFoundError
from plantdb.commons.fsdb.exceptions import NoAuthUserError
from plantdb.commons.fsdb.exceptions import ScanNotFoundError
from plantdb.commons.log import get_logger
from plantdb.server.core.security import add_jwt_from_header
from plantdb.server.core.security import rate_limit
from plantdb.server.core.security import sanitize_ids
from plantdb.server.core.security import use_guest_as_default

#: Define valid files extensions
VALID_FILE_EXT = ['.jpg', '.jpeg', '.png', '.tif', '.txt', '.json', '.ply', '.yaml', '.toml']


class File(Resource):
    """Represents a File resource creation endpoint in the application.

    This class provides the functionality to create new files in filesets.

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
    @sanitize_ids('file_id')
    @rate_limit(max_requests=240, window_seconds=60)
    @add_jwt_from_header
    @use_guest_as_default  # FIXME: Remove this if we want strict token identification
    def get(self, scan_id, fileset_id, file_id, **kwargs):
        """Retreive a file from a given scan and fileset.

        Returns
        -------
        dict
            Response containing a success message or error description.
            If successful, also returns the created file ID under 'id' key, as sanitization may have happened.
        int
            HTTP status code (201, 400, 404, or 500)

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> import requests
        >>> # Get a file from the database:
        >>> scan_id = 'real_plant'
        >>> fileset_id = 'images'
        >>> file_id = '00000_rgb'
        >>> url = f"http://127.0.0.1:5000/file/{scan_id}/{fileset_id}/{file_id}"
        >>> response = requests.get(url)
        >>> print(response.status_code)
        201
        >>> print(response.json())
        {'message': "File 'test_file.yaml' created and written successfully in fileset 'images'."}
        >>> file_path.unlink()  # Delete the YAML test file
        """

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

        except FilesetNotFoundError as e:
            return {'message': str(e)}, 404  # HTTP 404 Not Found
        except Exception as e:
            return {
                'message': f'Error accessing the fileset {fileset_id}: {str(e)}'}, 500  # HTTP 500 Internal Server Error

        try:
            # Create the file
            file = fileset.get_file(file_id)

        except FileNotFoundError as e:
            return {'message': str(e)}, 404  # HTTP 404 Not Found
        except Exception as e:
            self.logger.error(f"Error creating file: {str(e)}")
            return {'message': f'Error creating file: {str(e)}'}, 500  # HTTP 500 Internal Server Error
        else:
            rel_file_path = file.path().relative_to(self.db.path())
            return send_from_directory(self.db.path(), rel_file_path)

    @sanitize_ids('scan_id')
    @sanitize_ids('fileset_id')
    @sanitize_ids('file_id')
    @add_jwt_from_header
    def post(self, scan_id, fileset_id, file_id, **kwargs):
        """Create a new file in a fileset and write data to it.

        This method handles POST requests to create a new file with data. It validates the input data,
        creates the file with the specified name, and associates it with the given scan and fileset IDs.
        Optional metadata can be attached to the file.

        Returns
        -------
        dict
            Response containing a success message or error description.
            If successful, also returns the created file ID under 'id' key, as sanitization may have happened.
        int
            HTTP status code (201, 400, 404, or 500)

        Notes
        -----
        The method accepts a JSON request body with the following structure:

            - file (Any): The actual file data
            - ext (str): File extension (must be one of VALID_FILE_EXT)
            - metadata (dict, optional): Additional metadata for the fileset

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> import requests
        >>> import json
        >>> from pathlib import Path
        >>> from tempfile import NamedTemporaryFile
        >>> # Create a YAML temporary file:
        >>> with NamedTemporaryFile(suffix='.yaml', mode="w", delete=False) as f: f.write('name: my_file')
        >>> file_path = f.name
        >>> login_res = requests.post(f"http://127.0.0.1:5000/login", json={'username': 'admin', 'password': 'admin'})
        >>> token = login_res.json()['access_token']
        >>> # Create a new file with metadata in the database:
        >>> scan_id = "real_plant"
        >>> fileset_id = "images"
        >>> file_id = Path(file_path).stem
        >>> url = f"http://127.0.0.1:5000/file/{scan_id}/{fileset_id}/{file_id}"
        >>> metadata = {'description': 'Test file description'}
        >>> data = {'ext': '.yaml', 'metadata': json.dumps(metadata)}
        >>> file_handle = open(file_path, 'rb')
        >>> files = {'file': (Path(file_path).name, file_handle, 'application/octet-stream')}
        >>> response = requests.post(url, files=files, data=data, headers={'Authorization': 'Bearer ' + token})
        >>> print(response.status_code)
        201
        >>> print(response.json())
        {'message': "File 'test_file.yaml' created and written successfully in fileset 'images'."}
        >>> file_handle.close()
        >>> Path(file_path).unlink()  # Delete the YAML test file
        """
        # Check if the request has the file part
        if 'file' not in request.files:
            return {'message': 'No file provided'}, 400

        file_data = request.files['file']

        # Multipart/form‑data: fields are in ``request.form`` (`json=` is ignored when `files=` is present)
        data = request.form.to_dict()

        # Get form data
        ext = data.get('ext', None)
        # Validate required fields
        if not ext:
            return {'message': "'ext' field is required"}, 400
        # Validate file extension
        if not ext.startswith('.'):
            ext = f'.{ext}'
        if ext not in VALID_FILE_EXT:
            return {
                'message': f'Invalid file extension. Must be one of: {", ".join(VALID_FILE_EXT)}'
            }, 400

        # Get metadata if provided
        metadata_raw = data.get('metadata')
        if metadata_raw:
            try:
                metadata = json.loads(metadata_raw)
            except json.JSONDecodeError:
                import ast
                try:
                    metadata = ast.literal_eval(metadata_raw)
                except (ValueError, SyntaxError):
                    return {'message': "Invalid metadata format – must be JSON or a Python dict string"}, 400
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
            # Get the fileset
            fileset = scan.get_fileset(fileset_id)

        except FilesetNotFoundError as e:
            return {'message': str(e)}, 404  # HTTP 404 Not Found
        except Exception as e:
            return {
                'message': f'Error accessing the fileset {fileset_id}: {str(e)}'}, 500  # HTTP 500 Internal Server Error

        try:
            # Create the file
            file = fileset.create_file(file_id, metadata=metadata, **kwargs)
            try:
                # Write the file data with the specified extension
                if ext in ['.jpg', '.jpeg', '.png', '.tif']:
                    file.write_raw(file_data.read(), ext=ext[1:], **kwargs)  # Binary mode
                else:
                    file.write(file_data.read().decode(), ext=ext[1:], **kwargs)  # Text mode
            except Exception as e:
                fileset.delete_file(file_id, **kwargs)
                self.logger.error(f'Error writing file: {str(e)}')
                return {'message': f'Error writing file: {str(e)}'}, 500  # HTTP 500 Internal Server Error

        except SessionValidationError as e:
            return {'message': 'Invalid credentials'}, 401  # HTTP 401 Unauthorized (authentication)
        except Exception as e:
            self.logger.error(f"Error creating file: {str(e)}")
            return {'message': f'Error creating file: {str(e)}'}, 500  # HTTP 500 Internal Server Error
        else:
            return {
                'message': f"File created and written successfully in fileset '{fileset.id}'.",
                'id': f"{file_id}",
            }, 201


class FileMetadata(Resource):
    """REST API resource for managing file metadata operations.

    This class provides endpoints for retrieving and updating metadata associated with
    files within a scan's fileset.

    Attributes
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        The database providing the resources to serve.
    logger : Logger
        The logger instance for this resource.

    Notes
    -----
    All file and fileset names are sanitized before processing to ensure valid formats.
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
    @sanitize_ids('file_id')
    @add_jwt_from_header
    @use_guest_as_default  # FIXME: Remove this if we want strict token identification
    def get(self, scan_id, fileset_id, file_id, **kwargs):
        """Retrieve metadata for a specified file.

        Parameters
        ----------
        scan_id : str
            The ID of the scan containing the fileset.
        fileset_id : str
            The name of the fileset containing the file.
        file_id : str
            The name of the file.

        Returns
        -------
        Union[dict, Any]
            Without a 'key' URL parameter, it returns the complete metadata dictionary.
            If a 'key' URL parameter is provided, it returns the value for that key.

        Notes
        -----
        In the URL, you can use the `key` parameter to retrieve specific metadata keys.

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> import requests
        >>> from plantdb.client.rest_api import plantdb_url
        >>> # Get all metadata:
        >>> url = f"http://127.0.0.1:5000/file/test_plant/images/image_001/metadata"
        >>> response = requests.get(url)
        >>> print(response.json())
        {'metadata': {'description': 'Test file'}}
        >>> # Get a specific metadata key:
        >>> response = requests.get(url+"?key=description")
        >>> print(response.json())
        {'metadata': 'Test file'}
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

        except FilesetNotFoundError as e:
            return {'message': str(e)}, 404  # HTTP 404 Not Found
        except Exception as e:
            return {'message': f'Error accessing the fileset {fileset_id}: {str(e)}'}, 500  # HTTP 500 Internal Server Error

        try:
            file = fileset.get_file(file_id)
            # Get the metadata
            metadata = file.get_metadata(key)

        except FileNotFoundError as e:
            return {'message': str(e)}, 404  # HTTP 404 Not Found
        except Exception as e:
            self.logger.error(f'Error retrieving metadata: {str(e)}')
            return {'message': f'Error retrieving metadata: {str(e)}'}, 500  # HTTP 500 Internal Server Error
        else:
            return {'metadata': metadata}, 200

    @sanitize_ids('scan_id')
    @sanitize_ids('fileset_id')
    @sanitize_ids('file_id')
    @add_jwt_from_header
    def post(self, scan_id, fileset_id, file_id, **kwargs):
        """Update metadata for a specified file.

        Parameters
        ----------
        scan_id : str
            The ID of the scan containing the fileset.
        fileset_id : str
            The name of the fileset containing the file.
        file_id : str
            The name of the file.

        Returns
        -------
        dict
            Response dictionary with either:
                * 'metadata': containing updated metadata for successful requests
                * 'message': for error cases
        int
            HTTP status code (200, 400, 404, or 500).

        Notes
        -----
        The request body accepts a JSON object containing:

            - 'metadata' (dict): the new metadata

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> import requests
        >>> from plantdb.client.rest_api import plantdb_url
        >>> url = f"http://127.0.0.1:5000/file/test_plant/images/image_001/metadata"
        >>> data = {"metadata": {"description": "Updated description"}}
        >>> response = requests.post(url, json=data)
        >>> print(response.json())
        {'metadata': {'description': 'Updated description'}}
        """
        # Get request data
        data = request.get_json()
        if not data or 'metadata' not in data:
            return {'message': 'Missing metadata in request body'}, 400

        metadata = data['metadata']

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

        except FilesetNotFoundError as e:
            return {'message': str(e)}, 404  # HTTP 404 Not Found
        except Exception as e:
            return {'message': f'Error accessing the fileset {fileset_id}: {str(e)}'}, 500  # HTTP 500 Internal Server Error

        try:
            # Get the file
            file = fileset.get_file(file_id)
            # Update the metadata
            file.set_metadata(metadata, **kwargs)
            # Return updated metadata
            updated_metadata = file.get_metadata()

        except FileNotFoundError as e:
            return {'message': str(e)}, 404  # HTTP 404 Not Found
        except SessionValidationError as e:
            return {'message': 'Invalid credentials'}, 401  # HTTP 401 Unauthorized (authentication)
        except Exception as e:
            self.logger.error(f'Error processing request: {str(e)}')
            return {'message': f'Error processing request: {str(e)}'}, 500  # HTTP 500 Internal Server Error
        else:
            return {'metadata': updated_metadata}, 200
