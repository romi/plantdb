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
>>> from plantdb.plantdb_client import PlantDBClient
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
...     name="RGB Images",
...     description="Top view RGB images"
... )
"""

import json
import os

import requests


class PlantDBClient:
    """Client for interacting with the PlantDB REST API."""

    def __init__(self, base_url):
        self.base_url = base_url
        self.session = requests.Session()

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
        >>> from plantdb.plantdb_client import PlantDBClient
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
        >>> from plantdb.plantdb_client import PlantDBClient
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
            If True, replaces entire metadata. If False (default),
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
        >>> from plantdb.plantdb_client import PlantDBClient
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
        >>> from plantdb.plantdb_client import PlantDBClient
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

    def create_fileset(self, name, scan_id, metadata=None):
        """Create a new fileset associated with a scan.

        Parameters
        ----------
        name : str
            Name of the fileset to create
        scan_id : str
            ID of the scan to associate the fileset with
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
        >>> from plantdb.plantdb_client import PlantDBClient
        >>> from plantdb.client.rest_api import base_url
        >>> client = PlantDBClient(base_url())
        >>> metadata = {'description': 'This is a test fileset'}
        >>> response = client.create_fileset('my_fileset', 'real_plant', metadata=metadata)
        >>> print(response)
        {'message': "Fileset 'my_fileset' created successfully in 'real_plant'."}
        """
        url = f"{self.base_url}/api/fileset"
        data = {
            'name': name,
            'scan_id': scan_id
        }
        if metadata:
            data['metadata'] = metadata
        response = self.session.post(url, json=data)
        response.raise_for_status()
        return response.json()

    def get_fileset_metadata(self, scan_id, fileset_name, key=None):
        """Retrieve metadata for a specified fileset.

        Parameters
        ----------
        scan_id : str
            The ID of the scan containing the fileset
        fileset_name : str
            The name of the fileset
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
        >>> from plantdb.plantdb_client import PlantDBClient
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
        url = f"{self.base_url}/api/fileset/{scan_id}/{fileset_name}/metadata"
        params = {'key': key} if key else None
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def update_fileset_metadata(self, scan_id, fileset_name, metadata, replace=False):
        """Update metadata for a specified fileset.

        Parameters
        ----------
        scan_id : str
            The ID of the scan containing the fileset
        fileset_name : str
            The name of the fileset
        metadata : dict
            The metadata to update/set
        replace : bool, optional
            If True, replaces entire metadata. If False (default),
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
        >>> from plantdb.plantdb_client import PlantDBClient
        >>> from plantdb.client.rest_api import base_url
        >>> client = PlantDBClient(base_url())
        >>> # Update metadata
        >>> new_metadata = {'description': 'Updated fileset description', 'author': 'John Doe'}
        >>> response = client.update_fileset_metadata('real_plant', 'my_fileset', new_metadata)
        >>> print(response)
        {'metadata': {'description': 'Updated fileset description', 'author': 'John Doe'}}
        """
        url = f"{self.base_url}/api/fileset/{scan_id}/{fileset_name}/metadata"
        data = {
            'metadata': metadata,
            'replace': replace
        }
        response = self.session.post(url, json=data)
        response.raise_for_status()
        return response.json()

    def list_fileset_files(self, scan_id, fileset_name, query=None, fuzzy=False):
        """List all files in a specified fileset.

        Parameters
        ----------
        scan_id : str
            The ID of the scan containing the fileset
        fileset_name : str
            The name of the fileset
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
        >>> from plantdb.plantdb_client import PlantDBClient
        >>> from plantdb.client.rest_api import base_url
        >>> client = PlantDBClient(base_url())
        >>> response = client.list_fileset_files('real_plant', 'images')
        >>> print(response)
        {'files': ['00000_rgb', '00001_rgb', '00002_rgb', ...]}
        """
        url = f"{self.base_url}/api/fileset/{scan_id}/{fileset_name}/files"
        params = {}
        if query is not None:
            params['query'] = query
        if fuzzy:
            params['fuzzy'] = fuzzy
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def create_file(self, file_path, name, ext, scan_id, fileset_name, metadata=None):
        """Create a new file in a fileset and upload its data.

        Parameters
        ----------
        file_path : str or Path
            Path to the file to upload
        name : str
            Name to give the file in the database
        ext : str
            File extension (must be one of the valid extensions)
        scan_id : str
            ID of the scan containing the fileset
        fileset_name : str
            Name of the fileset to create the file in
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
        >>> from plantdb.plantdb_client import PlantDBClient
        >>> from plantdb.client.rest_api import base_url
        >>> client = PlantDBClient(base_url())
        >>> metadata = {'description': 'Test document', 'author': 'John Doe'}
        >>> response = client.create_file(
        ...     'path/to/file.yaml',
        ...     name='new_file',
        ...     ext='yaml',
        ...     scan_id='real_plant',
        ...     fileset_name='images',
        ...     metadata=metadata
        ... )
        >>> print(response)
        {'message': "File 'new_file.yaml' created and written successfully in fileset 'images'."}
        """
        url = f"{self.base_url}/api/file"

        # Prepare form data
        data = {
            'name': name,
            'ext': ext.lstrip('.'),  # Remove leading dot if present
            'scan_id': scan_id,
            'fileset_name': fileset_name
        }

        # Add metadata if provided
        if metadata:
            data['metadata'] = json.dumps(metadata)

        # Prepare file data
        with open(file_path, 'rb') as file_handle:
            files = {
                'file': (os.path.basename(file_path), file_handle, 'application/octet-stream')
            }
            response = self.session.post(url, files=files, data=data)

        response.raise_for_status()
        return response.json()

    def get_file_metadata(self, scan_id, fileset_name, file_name, key=None):
        """Retrieve metadata for a specified file.

        Parameters
        ----------
        scan_id : str
            The ID of the scan containing the fileset
        fileset_name : str
            The name of the fileset containing the file
        file_name : str
            The name of the file
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
        >>> from plantdb.plantdb_client import PlantDBClient
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
        url = f"{self.base_url}/api/file/{scan_id}/{fileset_name}/{file_name}/metadata"
        params = {'key': key} if key else None
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def update_file_metadata(self, scan_id, fileset_name, file_name, metadata, replace=False):
        """Update metadata for a specified file.

        Parameters
        ----------
        scan_id : str
            The ID of the scan containing the fileset
        fileset_name : str
            The name of the fileset containing the file
        file_name : str
            The name of the file
        metadata : dict
            The metadata to update/set
        replace : bool, optional
            If True, replaces entire metadata. If False (default),
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
        >>> from plantdb.plantdb_client import PlantDBClient
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
        url = f"{self.base_url}/api/file/{scan_id}/{fileset_name}/{file_name}/metadata"
        data = {
            'metadata': metadata,
            'replace': replace
        }
        response = self.session.post(url, json=data)
        response.raise_for_status()
        return response.json()
