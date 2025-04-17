#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""PlantDB Client Module

A client library for interacting with the PlantDB API, providing a streamlined interface for managing plant-related data including scans, filesets, and associated metadata.

Key Features
------------
- Scan Management: Create new scans and manage scan metadata
- Fileset Operations: Create and manage collections of files associated with scans
- File Handling: Upload and manage individual files within filesets
- Metadata Management: Comprehensive CRUD operations for scan, fileset, and file metadata
- RESTful Interface: Implements standard HTTP methods for API communication

Usage Examples
--------------

>>> # Start a test REST API server first:
>>> # $ fsdb_rest_api --test
>>> from plantdb.client.plantdb_client import PlantDBClient
>>> from plantdb.client.rest_api import base_url
>>> client = PlantDBClient(base_url())
>>> # Create a new scan
>>> scan_id = client.create_scan(
...     name="Plant Sample 001",
...     description="Arabidopsis specimen under controlled conditions"
... )
>>> # Create a fileset for the scan
>>> fileset_id = client.create_fileset(
...     scan_id=scan_id,
...     fileset_id="RGB Images",
...     description="Top view RGB images"
... )
"""

import mimetypes

import requests
from requests import RequestException


def get_mime_type(extension):
    """Determine the MIME type from a file extension.

    Parameters
    ----------
    extension : str
        File extension (with or without leading dot)

    Returns
    -------
    str
        The MIME type string or 'application/octet-stream' if not found
    """
    # Ensure the extension starts with a dot
    if not extension.startswith('.'):
        extension = f'.{extension}'

    mime_type, _ = mimetypes.guess_type(f'file{extension}')

    # Return a default for unknown types
    if mime_type is None:
        return 'application/octet-stream'

    return mime_type


class PlantDBClient:
    """Client for interacting with the PlantDB REST API."""

    def __init__(self, base_url):
        self.base_url = base_url
        self.session = requests.Session()

    def list_scans(self, query=None, fuzzy=False):
        """List all scans in the database.

        Parameters
        ----------
        query : str, optional
            Query string to filter scans
        fuzzy : bool, optional
            Whether to use fuzzy matching for the query (default: False)

        Returns
        -------
        dict
            Server response containing the list of scan IDs

        Raises
        ------
        requests.exceptions.RequestException
            If the request fails

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> from plantdb.client.plantdb_client import PlantDBClient
        >>> from plantdb.client.rest_api import base_url
        >>> client = PlantDBClient(base_url())
        >>> response = client.list_scans()
        >>> print(response)
        ['virtual_plant', 'real_plant_analyzed', 'real_plant', 'virtual_plant_analyzed', 'arabidopsis000']
        """
        url = f"{self.base_url}/scans"
        params = {}
        if query is not None:
            params['query'] = query
        if fuzzy:
            params['fuzzy'] = fuzzy
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def create_scan(self, name, metadata=None):
        """Create a new scan in the database.

        Parameters
        ----------
        name : str
            Name of the scan to create
        metadata : dict, optional
            Additional metadata for the scan

        Returns
        -------
        dict
            Server response containing creation confirmation message

        Raises
        ------
        requests.exceptions.RequestException
            If the request fails

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> from plantdb.client.plantdb_client import PlantDBClient
        >>> from plantdb.client.rest_api import base_url
        >>> client = PlantDBClient(base_url())
        >>> metadata = {'description': 'Test plant scan'}
        >>> response = client.create_scan('test_plant', metadata=metadata)
        >>> print(response)
        {'message': "Scan 'test_plant' created successfully."}
        """
        url = f"{self.base_url}/api/scan"
        data = {'name': name}
        if metadata:
            data['metadata'] = metadata
        response = self.session.post(url, json=data)
        response.raise_for_status()
        return response.json()

    def get_scan_metadata(self, scan_id, key=None):
        """Retrieve metadata for a specified scan.

        Parameters
        ----------
        scan_id : str
            The ID of the scan
        key : str, optional
            If provided, returns only the value for this specific metadata key

        Returns
        -------
        dict
            Server response containing the metadata or specific key value

        Raises
        ------
        requests.exceptions.RequestException
            If the request fails

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> from plantdb.client.plantdb_client import PlantDBClient
        >>> from plantdb.client.rest_api import base_url
        >>> client = PlantDBClient(base_url())
        >>> # Get all metadata
        >>> metadata = client.get_scan_metadata('test_plant')
        >>> print(metadata)
        {'metadata': {'owner': 'anonymous', 'description': 'Test plant scan'}}
        >>> # Get specific metadata key
        >>> value = client.get_scan_metadata('test_plant', key='description')
        >>> print(value)
        {'metadata': 'Test plant scan'}
        """
        url = f"{self.base_url}/api/scan/{scan_id}/metadata"
        params = {'key': key} if key else None
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def update_scan_metadata(self, scan_id, metadata, replace=False):
        """Update metadata for a specified scan.

        Parameters
        ----------
        scan_id : str
            The ID of the scan to update metadata for
        metadata : dict
            The metadata to update/set
        replace : bool, optional
            If ``True``, replaces entire metadata. If ``False`` (default),
            updates only specified keys.

        Returns
        -------
        dict
            Server response containing the updated metadata

        Raises
        ------
        requests.exceptions.RequestException
            If the request fails

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> from plantdb.client.plantdb_client import PlantDBClient
        >>> from plantdb.client.rest_api import base_url
        >>> client = PlantDBClient(base_url())
        >>> new_metadata = {'description': 'Updated scan description'}
        >>> response = client.update_scan_metadata('test_plant', new_metadata)
        >>> print(response)
        {'metadata': {'owner': 'anonymous', 'description': 'Updated scan description'}}
        """
        url = f"{self.base_url}/api/scan/{scan_id}/metadata"
        data = {
            'metadata': metadata,
            'replace': replace
        }
        response = self.session.post(url, json=data)
        response.raise_for_status()
        return response.json()

    def list_scan_filesets(self, scan_id, query=None, fuzzy=False):
        """List all filesets in a specified scan.

        Parameters
        ----------
        scan_id : str
            The ID of the scan
        query : str, optional
            Query string to filter filesets
        fuzzy : bool, optional
            Whether to use fuzzy matching for the query

        Returns
        -------
        dict
            Server response containing the list of fileset IDs

        Raises
        ------
        requests.exceptions.RequestException
            If the request fails

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> from plantdb.client.plantdb_client import PlantDBClient
        >>> from plantdb.client.rest_api import base_url
        >>> client = PlantDBClient(base_url())
        >>> response = client.list_scan_filesets('real_plant')
        >>> print(response)
        {'filesets': ['images']}
        """
        url = f"{self.base_url}/api/scan/{scan_id}/filesets"
        params = {}
        if query is not None:
            params['query'] = query
        if fuzzy:
            params['fuzzy'] = fuzzy
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def create_fileset(self, fileset_id, scan_id, metadata=None):
        """Create a new fileset associated with a scan.

        Parameters
        ----------
        fileset_id : str
            The ID of the fileset to create
        scan_id : str
            The ID of the scan to associate the fileset with
        metadata : dict, optional
            Additional metadata for the fileset

        Returns
        -------
        dict
            Server response containing creation confirmation message

        Raises
        ------
        requests.exceptions.RequestException
            If the request fails

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> from plantdb.client.plantdb_client import PlantDBClient
        >>> from plantdb.client.rest_api import base_url
        >>> client = PlantDBClient(base_url())
        >>> metadata = {'description': 'This is a test fileset'}
        >>> response = client.create_fileset('my_fileset', 'real_plant', metadata=metadata)
        >>> print(response)
        {'message': "Fileset 'my_fileset' created successfully in 'real_plant'."}
        """
        url = f"{self.base_url}/api/fileset"
        data = {
            'fileset_id': fileset_id,
            'scan_id': scan_id
        }
        if metadata:
            data['metadata'] = metadata
        response = self.session.post(url, json=data)

        # Handle HTTP errors with explicit messages
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            # Get the error message from the response if available
            error_detail = ""
            try:
                error_data = response.json()
                if 'message' in error_data:
                    error_detail = f": {error_data['message']}"
            except:
                pass

            if response.status_code == 400:
                raise ValueError(f"Invalid request data{error_detail}")
            elif response.status_code == 404:
                raise ValueError(f"Resource not found{error_detail}")
            elif response.status_code == 500:
                raise ValueError(f"Server error{error_detail}")
            else:
                # Re-raise the original exception if we can't provide a better message
                raise

        response_data = response.json()

        # Check if the response contains an error message despite successful status code
        if response.status_code >= 200 and response.status_code < 300:
            return response_data
        else:
            error_message = response_data.get('message', 'Unknown error')
            raise ValueError(f"Failed to create fileset: {error_message}")

    def get_fileset_metadata(self, scan_id, fileset_id, key=None):
        """Retrieve metadata for a specified fileset.

        Parameters
        ----------
        scan_id : str
            The ID of the scan containing the fileset
        fileset_id : str
            The ID of the fileset
        key : str, optional
            If provided, returns only the value for this specific metadata key

        Returns
        -------
        dict
            Server response containing the metadata or specific key value

        Raises
        ------
        requests.exceptions.RequestException
            If the request fails

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> from plantdb.client.plantdb_client import PlantDBClient
        >>> from plantdb.client.rest_api import base_url
        >>> client = PlantDBClient(base_url())
        >>> # Get all metadata
        >>> metadata = client.get_fileset_metadata('real_plant', 'my_fileset')
        >>> print(metadata)
        {'metadata': {'description': 'This is a test fileset'}}
        >>> # Get specific metadata key
        >>> value = client.get_fileset_metadata('real_plant', 'my_fileset', key='description')
        >>> print(value)
        {'metadata': 'This is a test fileset'}
        """
        url = f"{self.base_url}/api/fileset/{scan_id}/{fileset_id}/metadata"
        params = {'key': key} if key else None
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def update_fileset_metadata(self, scan_id, fileset_id, metadata, replace=False):
        """Update metadata for a specified fileset.

        Parameters
        ----------
        scan_id : str
            The ID of the scan containing the fileset
        fileset_id : str
            The ID of the fileset
        metadata : dict
            The metadata to update/set
        replace : bool, optional
            If ``True``, replaces entire metadata. If ``False`` (default),
            updates only specified keys.

        Returns
        -------
        dict
            Server response containing the updated metadata

        Raises
        ------
        requests.exceptions.RequestException
            If the request fails

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> from plantdb.client.plantdb_client import PlantDBClient
        >>> from plantdb.client.rest_api import base_url
        >>> client = PlantDBClient(base_url())
        >>> # Update metadata
        >>> new_metadata = {'description': 'Updated fileset description', 'author': 'John Doe'}
        >>> response = client.update_fileset_metadata('real_plant', 'my_fileset', new_metadata)
        >>> print(response)
        {'metadata': {'description': 'Updated fileset description', 'author': 'John Doe'}}
        """
        url = f"{self.base_url}/api/fileset/{scan_id}/{fileset_id}/metadata"
        data = {
            'metadata': metadata,
            'replace': replace
        }
        response = self.session.post(url, json=data)
        response.raise_for_status()
        return response.json()

    def list_fileset_files(self, scan_id, fileset_id, query=None, fuzzy=False):
        """List all files in a specified fileset.

        Parameters
        ----------
        scan_id : str
            The ID of the scan containing the fileset
        fileset_id : str
            The ID of the fileset
        query : str, optional
            Query string to filter files
        fuzzy : bool, optional
            Whether to use fuzzy matching for the query

        Returns
        -------
        dict
            Server response containing the list of files

        Raises
        ------
        requests.exceptions.RequestException
            If the request fails

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> from plantdb.client.plantdb_client import PlantDBClient
        >>> from plantdb.client.rest_api import base_url
        >>> client = PlantDBClient(base_url())
        >>> response = client.list_fileset_files('real_plant', 'images')
        >>> print(response)
        {'files': ['00000_rgb', '00001_rgb', '00002_rgb', ...]}
        """
        url = f"{self.base_url}/api/fileset/{scan_id}/{fileset_id}/files"
        params = {}
        if query is not None:
            params['query'] = query
        if fuzzy:
            params['fuzzy'] = fuzzy
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def create_file(self, file_data, file_id, ext, scan_id, fileset_id, metadata=None):
        """Create a new file in a fileset and upload its data.

        Parameters
        ----------
        file_data : str, pathlib.Path, or BytesIO
            Path to the file to upload or BytesIO object containing file data
        file_id : str
            The ID of the file in the database
        ext : str
            File extension (must be one of the valid extensions)
        scan_id : str
            The ID of the scan containing the fileset
        fileset_id : str
            The ID of the fileset to create the file in
        metadata : dict, optional
            Additional metadata for the file

        Returns
        -------
        dict
            Server response containing creation confirmation message

        Raises
        ------
        requests.exceptions.RequestException
            If the request fails
        ValueError
            If required parameters are missing or invalid

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> import tempfile
        >>> import yaml
        >>> from plantdb.client.plantdb_client import PlantDBClient
        >>> from plantdb.client.rest_api import base_url
        >>> client = PlantDBClient(base_url())
        >>> # Example 1 - Existing YAML file path as string
        >>> metadata = {'description': 'Test document', 'author': 'John Doe'}
        >>> dummy_data = {'name': 'Test Plant', 'species': 'Arabidopsis thaliana'}
        >>> with tempfile.NamedTemporaryFile(suffix='.yaml', mode='w', delete=False) as f: temp_file_name = f.name; yaml.dump(dummy_data, f)
        >>> response = client.create_file(temp_file_name, file_id='new_file',ext='yaml',scan_id='real_plant',fileset_id='images',metadata=metadata)
        >>> print(response)
        {'message': "File 'new_file.yaml' created and written successfully in fileset 'images'.", 'id': 'new_file'}
        >>> # Example 2 - RGB Image with BytesIO
        >>> import numpy as np
        >>> from PIL import Image
        >>> from io import BytesIO
        >>> # Generate random RGB data (values from 0-255)
        >>> rgb_data = np.random.randint(0, 256, (200, 150, 3), dtype=np.uint8)
        >>> # Create PIL Image from NumPy array
        >>> img = Image.fromarray(rgb_data, 'RGB')
        >>> # Save image to BytesIO object
        >>> image_data = BytesIO()
        >>> img.save(image_data, format='PNG')
        >>> image_data.seek(0)  # Move to the beginning of the BytesIO object
        >>> metadata = {'description': 'Random RGB test image', 'author': 'John Doe'}
        >>> response = client.create_file(image_data, file_id='random_image', ext='png', scan_id='real_plant', fileset_id='images', metadata=metadata)
        >>> print(response)

        """
        import os
        import json
        from io import BytesIO
        from pathlib import Path

        url = f"{self.base_url}/api/file"

        ext = ext.lstrip('.').lower()  # Remove leading dot if present
        # Prepare form data
        data = {
            'file_id': file_id,
            'ext': ext,
            'scan_id': scan_id,
            'fileset_id': fileset_id
        }

        # Add metadata if provided
        if metadata:
            data['metadata'] = json.dumps(metadata)

        # Prepare file data based on the type of file_data
        if isinstance(file_data, BytesIO):
            # If it's already a BytesIO object, use it directly
            filename = f"{file_id}.{ext}"
            files = {
                'file': (filename, file_data, get_mime_type(ext))
            }
            response = self.session.post(url, files=files, data=data)
        else:
            # Convert to Path object if it's a string
            file_path = Path(file_data) if isinstance(file_data, str) else file_data

            # Handle file from path
            with open(file_path, 'rb') as file_handle:
                filename = os.path.basename(str(file_path))
                files = {
                    'file': (filename, file_handle, 'application/octet-stream')
                }
                response = self.session.post(url, files=files, data=data)
        try:
            response.raise_for_status()
        except RequestException as e:
            response_data = response.json()["message"]
            raise type(e)(response_data) from e
        return response.json()

    def get_file_metadata(self, scan_id, fileset_id, file_id, key=None):
        """Retrieve metadata for a specified file.

        Parameters
        ----------
        scan_id : str
            The ID of the scan containing the fileset
        fileset_id : str
            The ID of the fileset containing the file
        file_id : str
            The ID of the file
        key : str, optional
            If provided, returns only the value for this specific metadata key

        Returns
        -------
        dict
            Server response containing the metadata or specific key value

        Raises
        ------
        requests.exceptions.RequestException
            If the request fails

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> from plantdb.client.plantdb_client import PlantDBClient
        >>> from plantdb.client.rest_api import base_url
        >>> client = PlantDBClient(base_url())
        >>> # Get all metadata
        >>> metadata = client.get_file_metadata('test_plant', 'images', 'image_001')
        >>> print(metadata)
        {'metadata': {'description': 'Test file'}}
        >>> # Get specific metadata key
        >>> value = client.get_file_metadata('test_plant', 'images', 'image_001', key='description')
        >>> print(value)
        {'metadata': 'Test file'}
        """
        url = f"{self.base_url}/api/file/{scan_id}/{fileset_id}/{file_id}/metadata"
        params = {'key': key} if key else None
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def update_file_metadata(self, scan_id, fileset_id, file_id, metadata, replace=False):
        """Update metadata for a specified file.

        Parameters
        ----------
        scan_id : str
            The ID of the scan containing the fileset
        fileset_id : str
            The ID of the fileset containing the file
        file_id : str
            The ID of the file
        metadata : dict
            The metadata to update/set
        replace : bool, optional
            If ``True``, replaces entire metadata. If ``False`` (default),
            updates only specified keys.

        Returns
        -------
        dict
            Server response containing the updated metadata

        Raises
        ------
        requests.exceptions.RequestException
            If the request fails

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> from plantdb.client.plantdb_client import PlantDBClient
        >>> from plantdb.client.rest_api import base_url
        >>> client = PlantDBClient(base_url())
        >>> # Update metadata
        >>> new_metadata = {'description': 'Updated description'}
        >>> response = client.update_file_metadata(
        ...     'test_plant',
        ...     'images',
        ...     'image_001',
        ...     new_metadata
        ... )
        >>> print(response)
        {'metadata': {'description': 'Updated description'}}
        """
        url = f"{self.base_url}/api/file/{scan_id}/{fileset_id}/{file_id}/metadata"
        data = {
            'metadata': metadata,
            'replace': replace
        }
        response = self.session.post(url, json=data)
        response.raise_for_status()
        return response.json()
