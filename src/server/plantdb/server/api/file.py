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
>>> api.add_resource(FileMetadata, "/api/file/<scan_id>/<fileset_id>/<file_id>/metadata", resource_class_kwargs={"db": db, "logger": logger})
>>> # Start the APP
>>> app.run(host='0.0.0.0', port=5000)
```

It may be used as follows (in another Python REPL):
```python
>>> import requests
>>> # Retrieve metadata for a specific file
>>> url = "http://127.0.0.1:5000/api/file/real_plant/images/00001_rgb/metadata"
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
import tempfile
from tempfile import NamedTemporaryFile

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

import plantdb
from plantdb.client.rest_api import plantdb_url
from plantdb.client.rest_api import plantdb_url
from plantdb.client.rest_api import plantdb_url
from plantdb.client.rest_api import plantdb_url
from plantdb.commons.auth.session import SessionValidationError
from plantdb.commons.auth.session import SessionValidationError
from plantdb.server.core.security import add_jwt_from_header
from plantdb.server.core.security import add_jwt_from_header
from plantdb.server.core.utils import sanitize_name
from plantdb.server.core.utils import sanitize_name
from plantdb.server.core.utils import sanitize_name
from plantdb.server.core.utils import sanitize_name
from plantdb.server.core.utils import sanitize_name
from plantdb.server.core.utils import sanitize_name


#: Define valid files extensions
VALID_FILE_EXT = ['.jpg', '.jpeg', '.png', '.tif', '.txt', '.json', '.ply', '.yaml', '.toml']


class FileCreate(Resource):
    """Represents a File resource creation endpoint in the application.

    This class provides the functionality to create new files in filesets.

    Attributes
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        The database instance used to create files.
    logger : logging.Logger
        The logger instance for recording operations.
    """

    def __init__(self, db, logger):
        self.db = db
        self.logger = logger

    @add_jwt_from_header
    def post(self, **kwargs):
        """Create a new file in a fileset and write data to it.

        This method handles POST requests to create a new file with data. It expects
        multipart/form-data with the file data and JSON metadata.

        Notes
        -----
        The method expects the following form fields:
        - file: The actual file data
        - file_id: str       # Required: ID of the file
        - ext: str           # Required: File extension (must be one of VALID_FILE_EXT)
        - scan_id: str       # Required: ID of the scan
        - fileset_id: str    # Required: ID of the fileset
        - metadata: dict     # Optional: Additional metadata for the file (as JSON string)

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
        >>> from tempfile import NamedTemporaryFile
        >>> from plantdb.client.rest_api import plantdb_url
        >>> # Create a YAML temporary file:
        >>> with NamedTemporaryFile(suffix='.yaml', mode="w", delete=False) as f: f.write('name: my_file')
        >>> file_path = f.name
        >>> login_res = requests.post(f"{plantdb_url('localhost', port=5000)}/login", json={'username': 'admin', 'password': 'admin'})
        >>> token = login_res.json()['access_token']
        >>> # Create a new file with metadata in the database:
        >>> url = f"{plantdb_url('localhost', port=5000)}/api/file"
        >>> # Open the file separately for sending
        >>> with open(file_path, 'rb') as file_handle:
        ...     files = {
        ...         'file': ('test_file.yaml', file_handle, 'application/octet-stream')
        ...     }
        ...     metadata = json.dumps({'description': 'Test document', 'author': 'John Doe'})
        ...     data = {
        ...         'file_id': 'new_file',
        ...         'ext': 'yaml',
        ...         'scan_id': 'real_plant',
        ...         'fileset_id': 'images',
        ...         'metadata': metadata
        ...     }
        ...     response = requests.post(url, files=files, data=data, headers={'Authorization': 'Bearer ' + token})
        >>> print(response.status_code)
        201
        >>> print(response.json())
        {'message': "File 'test_file.yaml' created and written successfully in fileset 'images'."}
        >>> file_path.unlink()  # Delete the YAML test file
        """
        # Check if the request has the file part
        if 'file' not in request.files:
            return {'message': 'No file provided'}, 400

        file_data = request.files['file']

        # Get form data
        file_id = request.form.get('file_id', None)
        ext = request.form.get('ext', None)
        scan_id = request.form.get('scan_id', None)
        fileset_id = request.form.get('fileset_id', None)

        # Validate required fields
        if not all([file_id, ext, scan_id, fileset_id]):
            missing_fields = [form_field.__name__ for form_field in [file_id, ext, scan_id, fileset_id] if
                              form_field is None]
            return {
                'message': f"'file_id', 'ext', 'scan_id', and 'fileset_id' fields are required, missing: {missing_fields}"}, 400

        # Validate file extension
        if not ext.startswith('.'):
            ext = f'.{ext}'
        if ext not in VALID_FILE_EXT:
            return {
                'message': f'Invalid file extension. Must be one of: {", ".join(VALID_FILE_EXT)}'
            }, 400

        # Parse metadata if provided
        try:
            metadata = json.loads(request.form.get('metadata', '{}'))
        except json.JSONDecodeError:
            return {'message': 'Invalid metadata JSON format'}, 400

        try:
            # Get the scan
            scan = self.db.get_scan(scan_id, **kwargs)
            if not scan:
                return {'message': 'Scan not found'}, 404
            # Get the fileset
            fileset = scan.get_fileset(sanitize_name(fileset_id))
            if not fileset:
                return {'message': 'Fileset not found'}, 404
            # Create the file
            file_id = sanitize_name(file_id)
            file = fileset.create_file(file_id, **kwargs)
            try:
                # Write the file data with the specified extension
                if ext in ['.jpg', '.jpeg', '.png', '.tif']:
                    file.write_raw(file_data.read(), ext=ext[1:], **kwargs)  # Binary mode
                else:
                    file.write(file_data.read().decode(), ext=ext[1:], **kwargs)  # Text mode
            except Exception as e:
                fileset.delete_file(file_id, **kwargs)
                self.logger.error(f'Error writing file: {str(e)}')
                return {'message': f'Error writing file: {str(e)}'}, 500
            # Set metadata if provided
            if metadata:
                file.set_metadata(metadata, **kwargs)
            return {
                'message': f"File '{file_id}{ext}' created and written successfully in fileset '{fileset.id}'.",
                'id': f"{file_id}",
            }, 201

        except SessionValidationError as e:
            return {'message': 'Invalid credentials'}, 401

        except Exception as e:
            self.logger.error(f"Error creating file: {str(e)}")
            return {'message': f'Error creating file: {str(e)}'}, 500


class FileMetadata(Resource):
    """REST API resource for managing file metadata operations.

    This class provides endpoints for retrieving and updating metadata associated with
    files within a scan's fileset.

    Attributes
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        Database instance for accessing file storage.
    logger : Logger
        The logger instance for this resource.

    Notes
    -----
    All file and fileset names are sanitized before processing to ensure valid formats.
    """

    def __init__(self, db, logger):
        self.db = db
        self.logger = logger

    def get(self, scan_id, fileset_id, file_id):
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
        >>> url = f"{plantdb_url('localhost', port=5000)}/api/file/test_plant/images/image_001/metadata"
        >>> response = requests.get(url)
        >>> print(response.json())
        {'metadata': {'description': 'Test file'}}
        >>> # Get a specific metadata key:
        >>> response = requests.get(url+"?key=description")
        >>> print(response.json())
        {'metadata': 'Test file'}
        """
        key = request.args.get('key', default=None, type=str)

        try:
            # Get the scan
            scan = self.db.get_scan(scan_id)
            if not scan:
                return {'message': 'Scan not found'}, 404
            # Get the fileset
            fileset = scan.get_fileset(sanitize_name(fileset_id))
            if not fileset:
                return {'message': 'Fileset not found'}, 404
            # Get the file
            file = fileset.get_file(sanitize_name(file_id))
            if not file:
                return {'message': 'File not found'}, 404
            # Get the metadata
            metadata = file.get_metadata(key)
            return {'metadata': metadata}, 200

        except Exception as e:
            self.logger.error(f'Error retrieving metadata: {str(e)}')
            return {'message': f'Error retrieving metadata: {str(e)}'}, 500

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
                * {'metadata': dict} containing updated metadata for successful requests
                * {'message': str} for error cases
        int
            HTTP status code (200, 400, 404, or 500).

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
        >>> url = f"{plantdb_url('localhost', port=5000)}/api/file/test_plant/images/image_001/metadata"
        >>> data = {"metadata": {"description": "Updated description"}}
        >>> response = requests.post(url, json=data)
        >>> print(response.json())
        {'metadata': {'description': 'Updated description'}}
        """
        try:
            # Get request data
            data = request.get_json()
            if not data or 'metadata' not in data:
                return {'message': 'Missing metadata in request body'}, 400

            metadata = data['metadata']
            replace = data.get('replace', False)

            if not isinstance(metadata, dict):
                return {'message': 'Metadata must be a dictionary'}, 400

            # Get the scan
            scan = self.db.get_scan(scan_id)
            if not scan:
                return {'message': 'Scan not found'}, 404

            # Get the fileset
            fileset = scan.get_fileset(sanitize_name(fileset_id))
            if not fileset:
                return {'message': 'Fileset not found'}, 404

            # Get the file
            file = fileset.get_file(sanitize_name(file_id))
            if not file:
                return {'message': 'File not found'}, 404

            # Update the metadata
            file.set_metadata(metadata, **kwargs)
            # TODO: make this works:
            # if replace:
            #    # Replace entire metadata dictionary
            #    file.set_metadata(metadata)
            # else:
            #    # Update only specified keys
            #    current_metadata = file.get_metadata()
            #    current_metadata.update(metadata)
            #    file.set_metadata(current_metadata)

            # Return updated metadata
            updated_metadata = file.get_metadata()
            return {'metadata': updated_metadata}, 200

        except SessionValidationError as e:
            return {'message': 'Invalid credentials'}, 401

        except Exception as e:
            self.logger.error(f'Error processing request: {str(e)}')
            return {'message': f'Error processing request: {str(e)}'}, 500
