#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
# PlantDB Client Module

A client library for interacting with the PlantDB API, providing a streamlined interface for managing plant-related data including scans, filesets, and associated metadata.

## Key Features

- Scan Management: Create new scans and manage scan metadata
- Fileset Operations: Create and manage collections of files associated with scans
- File Handling: Upload and manage individual files within filesets
- Metadata Management: Comprehensive CRUD operations for scan, fileset, and file metadata
- RESTful Interface: Implements standard HTTP methods for API communication

## Usage Examples

```python
>>> # Start a test REST API server first:
>>> # $ fsdb_rest_api --test
>>> from plantdb.client.plantdb_client import PlantDBClient
>>> from plantdb.client.rest_api import plantdb_url
>>> client = PlantDBClient(plantdb_url('localhost', port=5000))
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
```
"""
import json
import mimetypes
import os
from functools import wraps

import requests
from ada_url import join_url
from requests import RequestException

from plantdb.client import api_endpoints
from plantdb.commons.auth.models import Permission
from plantdb.commons.log import get_logger


def get_mime_type(extension):
    """Determine the MIME type from a file extension.

    Parameters
    ----------
    extension : str
        File extension (with or without a leading dot)

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


def _auth_via_token(method):
    """Decorator that injects the correct request function based on ``use_api_token``.

    The wrapped method receives a private ``_request_fn`` argument that is either
    ``self._request_with_refresh`` (default, uses the JWT) or
    ``self._request_with_api_token`` (when ``use_api_token=True``).
    """

    @wraps(method)
    def wrapper(self, *args, use_api_token: bool = False, **kwargs):
        # Choose the request helper
        request_fn = (self._request_with_api_token if use_api_token else self._request_with_refresh)
        # Inject it as a private kwarg – the original signature stays clean
        kwargs["_request_fn"] = request_fn
        # Remove the public flag so the original method signature does not see it
        return method(self, *args, **kwargs)

    return wrapper


class PlantDBClient:
    """Client for interacting with the PlantDB REST API.

    This class provides methods to interact with a PlantDB REST API, allowing operations
    on scans, filesets, and files. It handles authentication, error processing, and
    provides a consistent interface for all API endpoints.

    Parameters
    ----------
    base_url : str
        The base URL of the PlantDB REST API.
    prefix : str, optional
        The URL prefix used by the PlantDB REST API.

    Attributes
    ----------
    base_url : str
        The base URL of the PlantDB REST API.
    _session : requests.Session
        HTTP session that maintains cookies and connection pooling.
    _access_token : str
        The JSON Web Token to authenticate with the PlantDB REST API.
    _refresh_token : str
        The refresh token to obtain new access tokens.
    _api_token : str
        The long-lived API token granting permission to specific datasets.
    _username :str
        The login username.
    logger : logging.Logger
        The logger to use.

    Notes
    -----
    This client automatically handles HTTP errors and extracts meaningful error messages
    from the API responses. All methods will raise appropriate exceptions with
    descriptive messages when API requests fail.

    Examples
    --------
    >>> from plantdb.server.test_rest_api import TestRestApiServer
    >>> # Start a test PlantDB REST API server first:
    >>> server = TestRestApiServer(test=True, port=5000)
    >>> server.start()
    >>> # Use the client against the server
    >>> from plantdb.client.plantdb_client import PlantDBClient
    >>> from plantdb.client.rest_api import plantdb_url
    >>> client = PlantDBClient(plantdb_url('localhost', port=5000))
    >>> client.login('admin', 'admin')
    >>> print(client._access_token)
    >>> client2 = PlantDBClient(plantdb_url('localhost', port=5000))
    >>> client2.validate_token(client._access_token)
    >>> print(client.plantdb_url)
    >>> scans = client.list_scans()
    >>> print(scans)
    ['virtual_plant', 'real_plant_analyzed', 'real_plant', 'virtual_plant_analyzed', 'arabidopsis000']
    >>> # Finally, stop the server
    >>> server.stop()
    """

    def __init__(self, base_url, prefix=None):
        """Initialize the PlantDBClient with a base URL."""
        if prefix is None:
            prefix = api_prefix()
        self.base_url = f"{base_url}{prefix}"

        # Dedicated session for access‑token requests
        self._session = requests.Session()
        # Dedicated session for API‑token requests
        self._api_session = requests.Session()

        self._access_token = None
        self._refresh_token = None
        self._api_token = None
        self._username = None

        self.logger = get_logger(__class__.__name__)

    def login(self, username: str, password: str) -> bool:
        """
        Authenticate the user with the PlantDB API.

        Parameters
        ----------
        username : str
            Username for authentication.
        password : str
            Password for authentication.

        Returns
        -------
        bool
            ``True`` if login successful, ``False`` otherwise

        Examples
        --------
        >>> from plantdb.server.test_rest_api import TestRestApiServer
        >>> # Start a test PlantDB REST API server first:
        >>> server = TestRestApiServer(test=True, port=5000)
        >>> server.start()
        >>> # Create a client
        >>> from plantdb.client.plantdb_client import PlantDBClient
        >>> from plantdb.client.rest_api import plantdb_url
        >>> client = PlantDBClient(plantdb_url('localhost', port=5000))
        >>> # Use it to log in as 'admin'
        >>> client.login('admin', 'admin')
        >>> print(client._access_token)  # print the 'admin' access token
        >>> # Finally, stop the server
        >>> server.stop()
        """
        url = join_url(self.base_url, api_endpoints.login())
        data = {
            'username': username,
            'password': password
        }

        try:
            # Use _session.request directly for login to avoid using expired tokens in headers
            response = self._session.request("POST", url, json=data)
            if response.ok:
                result = response.json()
                self._access_token = result.get('access_token')
                self._refresh_token = result.get('refresh_token')
                self._username = username
                # Add the JWT to the header
                self._session.headers.update({'Authorization': f'Bearer {self._access_token}'})
                return True
            else:
                error_msg = response.json().get('message', 'Login failed')
                self.logger.error(f"Login failed: {error_msg}")
                return False

        except RequestException as e:
            self.logger.error(f"Login request failed: {e}")
            return False

    def _request_with_refresh(self, method, url, **kwargs):
        """Perform an HTTP request with automatic token refresh on 401."""
        response = self._session.request(method, url, **kwargs)

        if response.status_code == 401 and self._refresh_token:
            self.logger.info("Access token expired, attempting to refresh...")
            if self.refresh_token():
                self.logger.info("Token refresh successful, retrying request...")
                # Update headers for the retry
                if 'headers' in kwargs:
                    kwargs['headers'].update({'Authorization': f'Bearer {self._access_token}'})
                else:
                    # session already has the updated Authorization header
                    pass
                return self._session.request(method, url, **kwargs)
            else:
                self.logger.error("Token refresh failed, user needs to re-authenticate.")

        return response

    def _request_with_api_token(self, method, url, **kwargs):
        """Perform an HTTP request using the long‑lived API token.

        The method creates (or re‑uses) a separate ``requests.Session`` that
        carries the ``Authorization: Bearer <api_token>`` header, ensuring the
        primary ``self.session`` (used for JWT auth) remains untouched.

        Parameters
        ----------
        method : str
            HTTP method, e.g. ``'GET'`` or ``'POST'``.
        url : str
            Fully‑qualified request URL.
        **kwargs
            Additional arguments forwarded to ``requests.Session.request``.

        Returns
        -------
        requests.Response
            The raw response object.
        """
        # If no API token is configured, just fall back to the regular session.
        if not self._api_token:
            self.logger.warning("API token not set; use `create_api_token` method first!")
            return None

        # Ensure the Authorization header contains the fresh API token.
        # We replace any existing Authorization header in this dedicated session.
        self._api_session.headers.update({"Authorization": f"Bearer {self._api_token}"})

        return self._api_session.request(method, url, **kwargs)

    def logout(self) -> bool:
        """Logout user from the PlantDB API.

        Returns
        -------
        bool
            ``True`` if logout successful, ``False`` otherwise.

        Examples
        --------
        >>> from plantdb.server.test_rest_api import TestRestApiServer
        >>> # Start a test PlantDB REST API server first:
        >>> server = TestRestApiServer(test=True, port=5000)
        >>> server.start()
        >>> # Create a client
        >>> from plantdb.client.plantdb_client import PlantDBClient
        >>> from plantdb.client.rest_api import plantdb_url
        >>> client = PlantDBClient(plantdb_url('localhost', port=5000))
        >>> # Use it to log in as 'admin'
        >>> client.login('admin', 'admin')
        >>> # Then log out
        >>> client.logout()
        >>> # Finally, stop the server
        >>> server.stop()
        """
        url = join_url(self.base_url, api_endpoints.logout())
        try:
            # Use _request_with_refresh for logout as it requires authentication
            response = self._request_with_refresh("POST", url)
            if response.ok:
                self._username = None
                self._access_token = None
                self._refresh_token = None
                # Remove the Authorization with the JWT from the header
                if 'Authorization' in self._session.headers:
                    self._session.headers.pop('Authorization')
                return True
            return False
        except Exception:
            return False

    def create_user(self, username: str, password: str, fullname: str) -> bool:
        """Create a new user in the PlantDB API.

        Parameters
        ----------
        username : str
            New username to create.
        password : str
            Password for authentication.
        fullname : str
            The full name of the user.

        Returns
        -------
        bool
            ``True`` if user creation is successful, ``False`` otherwise

        Examples
        --------
        >>> from plantdb.server.test_rest_api import TestRestApiServer
        >>> # Start a test PlantDB REST API server first:
        >>> server = TestRestApiServer(test=True, port=5000)
        >>> server.start()
        >>> # Create a client
        >>> from plantdb.client.plantdb_client import PlantDBClient
        >>> from plantdb.client.rest_api import plantdb_url
        >>> client = PlantDBClient(plantdb_url('localhost', port=5000))
        >>> # Use it to log in as 'admin'
        >>> _ = client.login('admin', 'admin')
        >>> # Create a new user
        >>> _ = client.create_user('batman', 'JokerInArkham', 'Bruce Wayne')
        >>> # Finally, stop the server
        >>> server.stop()
        """
        url = join_url(self.base_url, api_endpoints.create_user())
        data = {
            'username': username,
            'password': password,
            'fullname': fullname,
        }

        try:
            # create_user usually requires admin, use _request_with_refresh
            response = self._request_with_refresh("POST", url, json=data)
            if response.ok:
                return True
            else:
                error_msg = response.json().get('message', 'Unknown server error.')
                self.logger.error(f"Failed to create user: {error_msg}")
                return False

        except RequestException as e:
            self.logger.error(f"User registration request failed: {e}")
            return False

    def create_api_token(
            self,
            token_exp: int,
            dataset_permissions: dict[str, tuple[Permission | str, ...] | Permission | str]
    ) -> str | None:
        """
        Creates an API token with a specified expiration time and dataset permissions.

        This method generates an API token that is tied to the permissions of specific
        datasets. The permissions for each dataset must be provided, and they will be
        validated against the defined `Permission` type. This allows fine-grained
        control over dataset access via the generated token. The token can only be
        created successfully if the server accepts the provided data.

        Parameters
        ----------
        token_exp : int
            The expiration time for the API token in seconds.
        dataset_permissions : dict of str to tuple[Permission | str, ...] or Permission or str
            A dictionary where each key is a dataset name (unix globbing possible),
            and the corresponding value is the permission(s) for that dataset.
            Permissions can be a single `Permission` instance, a string, or a
            tuple of `Permission` instances or strings.

        Returns
        -------
        str or None
            Returns the generated API token as a string if successful. Returns None
            if the token creation fails due to server error or other issues.

        Examples
        --------
        >>> from plantdb.server.test_rest_api import TestRestApiServer
        >>> # Start a test PlantDB REST API server first:
        >>> server = TestRestApiServer(test=True, port=5000)
        >>> server.start()
        >>> # Create a client
        >>> from plantdb.client.plantdb_client import PlantDBClient
        >>> from plantdb.client.rest_api import plantdb_url
        >>> from plantdb.commons.auth.models import Permission
        >>> client = PlantDBClient(plantdb_url('localhost', port=5000))
        >>> # Use it to log in as 'admin'
        >>> client.login('admin', 'admin')
        >>> # Create an API token with 'WRITE' permission to create a new scan dataset 'new_scan'
        >>> print(client.create_api_token(3600, {'new_scan': Permission.WRITE}))
        >>> # Create a new scan dataset using the API token:
        >>> client.create_scan('new_scan', {'description': "Test API token dataset"}, use_api_token=True)
        >>> # Finally, stop the server
        >>> server.stop()
        """
        url = join_url(self.base_url, api_endpoints.create_api_token())

        # Validate dataset permissions
        datasets = {}
        for dataset, permissions in dataset_permissions.items():
            if not isinstance(permissions, tuple):
                permissions = (permissions,)
            datasets[dataset] = []
            for permission in permissions:
                if isinstance(permission, str) and permission not in Permission:
                    raise ValueError(f"Invalid permission: {permission}. "
                                     f"Must be one of {[p.value for p in Permission]}.")
                datasets[dataset].append(str(permission))

        data = {
            "token_exp": token_exp,
            "datasets": datasets,
        }
        try:
            response = self._request_with_refresh("POST", url, json=data)
            if response.ok:
                self._api_token = response.json().get('api_token')
                return self._api_token
            else:
                error_msg = response.json().get('message', 'Unknown server error.')
                self.logger.error(f"Failed to create API token: {error_msg}")
                return None
        except RequestException as e:
            self.logger.error(f"Failed to create API token: {e}")
            return None

    def refresh(self) -> bool:
        """Refresh the database."""
        url = join_url(self.base_url, api_endpoints.refresh())
        try:
            response = self._request_with_refresh("GET", url)
            if response.ok:
                return True
            return False
        except Exception:
            return False

    def validate_token(self, token) -> bool:
        """Validate an authentication token against the remote service.

        This method sends a ``POST`` request to the token‑validation endpoint
        using the supplied ``token`` in the ``Authorization`` header.  The
        request is performed via :meth:`_request_with_refresh`, which will
        transparently refresh the session if necessary.  The response's
        ``ok`` attribute determines the boolean result.

        Parameters
        ----------
        token : str
            The bearer token to be validated.

        Returns
        -------
        True if the token is accepted by the server, otherwise ``False``.

        Examples
        --------
        >>> client = MyApiClient(base_url='https://api.example.com')
        >>> token = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...'
        >>> client.validate_token(token)
        True
        >>> client.validate_token('invalid')
        False

        See Also
        --------
        _request_with_refresh : Internal helper that handles token refresh.
        api_endpoints.token_validation : Returns the relative URL for token validation.
        """
        url = join_url(self.base_url, api_endpoints.token_validation())
        response = self._request_with_refresh("POST", url, headers={"Authorization": f"Bearer {token}"})
        if response.ok:
            resp_username = response.json()['user']['username']
            if not self._username:
                self._username = resp_username
                self.refresh_token()
            if self._username and resp_username != self._username:
                self.logger.warning(f"Given token correspond to a different username")
            return True
        else:
            return False

    def refresh_token(self) -> bool:
        """Refresh the JSON Web Token.

        Uses the stored refresh token to obtain a new access/refresh token pair.
        """
        if not self._refresh_token:
            self.logger.error("No refresh token available")
            return False

        url = join_url(self.base_url, api_endpoints.token_refresh())
        data = {'refresh_token': self._refresh_token}
        try:
            # Use _session.request directly to avoid infinite recursion with _request_with_refresh
            response = self._session.request("POST", url, json=data)
            if response.ok:
                result = response.json()
                self._access_token = result.get('access_token')
                self._refresh_token = result.get('refresh_token')
                # Update the header with the new access token
                self._session.headers.update({'Authorization': f'Bearer {self._access_token}'})
                return True
            else:
                error_msg = response.json().get('message', 'Token refresh failed')
                self.logger.error(f"Token refresh failed: {error_msg}")
                self._access_token = None
                self._refresh_token = None
                self._username = None
                return False
        except Exception as e:
            self.logger.error(f"Token refresh request failed: {e}")
            return False

    def _handle_http_errors(self, response):
        """Handles HTTP errors by logging a message appropriate to the severity of the HTTP status code."""
        # If the response is successful, nothing to do
        if response.ok:
            return

        # Determine severity and log accordingly
        if response.status_code >= 500:
            # Server error - treat as serious
            self.logger.error(
                f"Server error {response.status_code}: {response.reason}"
            )
        else:
            # Client error - treat as a warning
            self.logger.warning(
                f"Client error {response.status_code}: {response.reason}"
            )

        # Try to pull a helpful message from the JSON payload
        try:
            response_data = response.json().get("message", response.text)
        except ValueError:
            # Fallback to raw text if JSON cannot be decoded
            response_data = response.text

        # Re‑raise a generic RequestException with the extracted message
        raise RequestException(response_data)

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
        >>> from plantdb.client.rest_api import plantdb_url
        >>> client = PlantDBClient(plantdb_url('localhost', port=5000))
        >>> response = client.list_scans()
        >>> print(response)
        ['virtual_plant', 'real_plant_analyzed', 'real_plant', 'virtual_plant_analyzed', 'arabidopsis000']
        """
        url = join_url(self.base_url, api_endpoints.scans())
        params = {}
        if query is not None:
            params['query'] = query
        if fuzzy:
            params['fuzzy'] = fuzzy
        response = self._request_with_refresh('GET', url, params=params)

        # Handle HTTP errors with explicit messages
        self._handle_http_errors(response)
        return response.json()

    def list_scans_info(self, query=None, fuzzy=False):
        """Retrieve detailed scan information dictionaries from the ScansTable resource.

        Parameters
        ----------
        query : dict, optional
            A dictionary that will be JSON‑encoded and sent as the ``filterQuery`` URL
            parameter.  Use the same structure accepted by the server, _e.g._
            ``{"object": {"species": "Arabidopsis.*"}}``.
        fuzzy : bool, optional
            When ``True`` the server performs fuzzy matching (default ``False``).

        Returns
        -------
        list[dict]
            A list where each entry is a dictionary containing the scan’s
            ``metadata``, ``tasks``, ``files`` and other information as defined by `ScansTable`.

        Raises
        ------
        requests.exceptions.RequestException
            If the request fails or the server returns an error status.
        """
        # Build the URL for the “scans info” endpoint - the server side class is ScansTable
        url = join_url(self.base_url, api_endpoints.scans_info())

        # Prepare query parameters exactly as the REST API expects
        params = {}
        if query is not None:
            # The API expects a JSON string in the ``filterQuery`` parameter
            params["filterQuery"] = json.dumps(query)
        if fuzzy:
            params["fuzzy"] = fuzzy

        # Perform the request; token refresh is handled automatically
        response = self._request_with_refresh("GET", url, params=params)

        # Turn HTTP errors into readable exceptions
        self._handle_http_errors(response)

        # Return the parsed JSON payload (list of dicts)
        return response.json()

    @_auth_via_token
    def create_scan(self, name, metadata=None, *, _request_fn):
        """Create a new scan in the database.

        Parameters
        ----------
        name : str
            Name of the scan to create
        metadata : dict, optional
            Additional metadata for the scan
        use_api_token : bool, optional (keyword‑only)
            If ``True`` the request is sent with the long‑lived API token.
            Defaults to ``False`` (uses the JWT access token).

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
        >>> from plantdb.client.rest_api import plantdb_url
        >>> client = PlantDBClient(plantdb_url('localhost', port=5000))
        >>> metadata = {'description': 'Test plant scan'}
        >>> # Scan creation requires authentication
        >>> response = client.create_scan('test_plant', metadata=metadata)
        ERROR    [PlantDBClient] Server error 500: INTERNAL SERVER ERROR
        requests.exceptions.RequestException: Error creating scan: Insufficient permissions to create a scan as 'guest' user!
        >>> # Log in as admin to get sufficient rights
        >>> client.login('admin', 'admin')
        >>> response = client.create_scan('test_plant', metadata=metadata)
        >>> print(response['message'])
        {'message': "Scan 'test_plant' created successfully."}
        """
        url = join_url(self.base_url, api_endpoints.scan(name))

        data = {}
        if metadata:
            data['metadata'] = metadata

        response = _request_fn("POST", url, json=data)  # the decorator injects '_request_fn'

        # Handle HTTP errors with explicit messages
        self._handle_http_errors(response)
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
        >>> from plantdb.client.rest_api import plantdb_url
        >>> client = PlantDBClient(plantdb_url('localhost', port=5000))
        >>> # Get all metadata
        >>> metadata = client.get_scan_metadata('test_plant')
        >>> print(metadata)
        {'metadata': {'owner': 'admin', 'created': '2026-02-04T00:25:13.869891', 'last_modified': '2026-02-04T00:25:13.871581', 'created_by': 'PlantDB Admin', 'description': 'Test plant scan'}}
        >>> # Get a specific metadata key
        >>> value = client.get_scan_metadata('test_plant', key='description')
        >>> print(value)
        {'metadata': 'Test plant scan'}
        """
        url = f"{self.base_url}/scan/{scan_id}/metadata"
        params = {'key': key} if key else None
        response = self._request_with_refresh("GET", url, params=params)

        # Handle HTTP errors with explicit messages
        self._handle_http_errors(response)
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
        >>> from plantdb.client.rest_api import plantdb_url
        >>> client = PlantDBClient(plantdb_url('localhost', port=5000))
        >>> # Log in as admin to get sufficient rights
        >>> client.login('admin', 'admin')
        >>> new_metadata = {'description': 'Updated scan description'}
        >>> response = client.update_scan_metadata('test_plant', new_metadata)
        >>> print(response['metadata']['description'])
        Updated scan description
        """
        url = f"{self.base_url}/scan/{scan_id}/metadata"
        data = {
            'metadata': metadata,
            'replace': replace
        }
        response = self._request_with_refresh("POST", url, json=data)

        # Handle HTTP errors with explicit messages
        self._handle_http_errors(response)
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
        >>> from plantdb.client.rest_api import plantdb_url
        >>> client = PlantDBClient(plantdb_url('localhost', port=5000))
        >>> response = client.list_scan_filesets('real_plant')
        >>> print(response)
        {'filesets': ['images']}
        """
        url = f"{self.base_url}/scan/{scan_id}/filesets"
        params = {}
        if query is not None:
            params['query'] = query
        if fuzzy:
            params['fuzzy'] = fuzzy
        response = self._request_with_refresh("GET", url, params=params)

        # Handle HTTP errors with explicit messages
        self._handle_http_errors(response)
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
            Server response containing a creation confirmation message

        Raises
        ------
        requests.exceptions.RequestException
            If the request fails

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> from plantdb.client.plantdb_client import PlantDBClient
        >>> from plantdb.client.rest_api import plantdb_url
        >>> client = PlantDBClient(plantdb_url('localhost', port=5000))
        >>> # Log in as admin to get sufficient rights
        >>> client.login('admin', 'admin')
        >>> metadata = {'description': 'This is a test fileset'}
        >>> response = client.create_fileset('my_fileset', 'real_plant', metadata=metadata)
        >>> print(response)
        {'message': "Fileset 'my_fileset' created successfully in 'real_plant'."}
        """
        url = f"{self.base_url}/fileset"
        data = {
            'fileset_id': fileset_id,
            'scan_id': scan_id
        }
        if metadata:
            data['metadata'] = metadata
        response = self._request_with_refresh("POST", url, json=data)

        # Handle HTTP errors with explicit messages
        self._handle_http_errors(response)
        return response.json()

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
        >>> from plantdb.client.rest_api import plantdb_url
        >>> client = PlantDBClient(plantdb_url('localhost', port=5000))
        >>> # Get all metadata
        >>> metadata = client.get_fileset_metadata('real_plant', 'my_fileset')
        >>> print(metadata)
        {'metadata': {'description': 'This is a test fileset'}}
        >>> # Get a specific metadata key
        >>> value = client.get_fileset_metadata('real_plant', 'my_fileset', key='description')
        >>> print(value)
        {'metadata': 'This is a test fileset'}
        """
        url = f"{self.base_url}/fileset/{scan_id}/{fileset_id}/metadata"
        params = {'key': key} if key else None
        response = self._request_with_refresh("GET", url, params=params)

        # Handle HTTP errors with explicit messages
        self._handle_http_errors(response)
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
        >>> from plantdb.client.rest_api import plantdb_url
        >>> client = PlantDBClient(plantdb_url('localhost', port=5000))
        >>> # Log in as admin to get sufficient rights
        >>> client.login('admin', 'admin')
        >>> # Update metadata
        >>> new_metadata = {'description': 'Updated fileset description', 'author': 'John Doe'}
        >>> response = client.update_fileset_metadata('real_plant', 'my_fileset', new_metadata)
        >>> print(response)
        {'metadata': {'description': 'Updated fileset description', 'author': 'John Doe'}}
        """
        url = f"{self.base_url}/fileset/{scan_id}/{fileset_id}/metadata"
        data = {
            'metadata': metadata,
            'replace': replace
        }
        response = self._request_with_refresh("POST", url, json=data)

        # Handle HTTP errors with explicit messages
        self._handle_http_errors(response)
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
        >>> from plantdb.client.rest_api import plantdb_url
        >>> client = PlantDBClient(plantdb_url('localhost', port=5000))
        >>> response = client.list_fileset_files('real_plant', 'images')
        >>> print(response)
        {'files': ['00000_rgb', '00001_rgb', '00002_rgb', ...]}
        """
        url = f"{self.base_url}/fileset/{scan_id}/{fileset_id}/files"
        params = {}
        if query is not None:
            params['query'] = query
        if fuzzy:
            params['fuzzy'] = fuzzy
        response = self._request_with_refresh("GET", url, params=params)

        # Handle HTTP errors with explicit messages
        self._handle_http_errors(response)
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
        >>> from plantdb.client.rest_api import plantdb_url
        >>> client = PlantDBClient(plantdb_url('localhost', port=5000))
        >>> # Log in as admin to get sufficient rights
        >>> client.login('admin', 'admin')
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

        url = f"{self.base_url}/file"

        ext = ext.lstrip('.').lower()  # Remove the leading dot if present
        # Prepare data
        data = {
            'file_id': file_id,
            'ext': ext,
            'scan_id': scan_id,
            'fileset_id': fileset_id
        }

        # Add metadata if provided
        if metadata:
            if isinstance(metadata, dict):
                data['metadata'] = json.dumps(metadata)
            elif isinstance(metadata, str):
                data['metadata'] = metadata
            else:
                raise TypeError("Invalid metadata type. Must be a dictionary or string.")

        # Prepare file data based on the type of file_data
        if isinstance(file_data, BytesIO):
            # If it's already a BytesIO object, use it directly
            filename = f"{file_id}.{ext}"
            files = {
                'file': (filename, file_data, get_mime_type(ext))
            }
            response = self._request_with_refresh("POST", url, files=files, data=data)
        else:
            # Convert to a Path object if it's a string
            file_path = Path(file_data) if isinstance(file_data, str) else file_data

            # Handle file from a path
            with open(file_path, 'rb') as file_handle:
                filename = os.path.basename(str(file_path))
                files = {
                    'file': (filename, file_handle, 'application/octet-stream')
                }
                response = self._request_with_refresh("POST", url, files=files, data=data)

        # Handle HTTP errors with explicit messages
        self._handle_http_errors(response)
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
        >>> from plantdb.client.rest_api import plantdb_url
        >>> client = PlantDBClient(plantdb_url('localhost', port=5000))
        >>> # Get all metadata
        >>> metadata = client.get_file_metadata('test_plant', 'images', 'image_001')
        >>> print(metadata)
        {'metadata': {'description': 'Test file'}}
        >>> # Get a specific metadata key
        >>> value = client.get_file_metadata('test_plant', 'images', 'image_001', key='description')
        >>> print(value)
        {'metadata': 'Test file'}
        """
        url = f"{self.base_url}/file/{scan_id}/{fileset_id}/{file_id}/metadata"
        params = {'key': key} if key else None
        response = self._request_with_refresh("GET", url, params=params)

        # Handle HTTP errors with explicit messages
        self._handle_http_errors(response)
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
        >>> from plantdb.client.rest_api import plantdb_url
        >>> client = PlantDBClient(plantdb_url('localhost', port=5000))
        >>> # Log in as admin to get sufficient rights
        >>> client.login('admin', 'admin')
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
        url = f"{self.base_url}/file/{scan_id}/{fileset_id}/{file_id}/metadata"
        data = {
            'metadata': metadata,
            'replace': replace
        }
        response = self._request_with_refresh("POST", url, json=data)

        # Handle HTTP errors with explicit messages
        self._handle_http_errors(response)
        return response.json()


def api_prefix(prefix=""):
    """Set the API prefix for all URL generation functions.

    Parameters
    ----------
    prefix : str, optional
        The prefix to add to all API URLs, e.g., '/plantdb'. Defaults to empty string.

    Examples
    --------
    >>> import os
    >>> from plantdb.client.plantdb_client import api_prefix
    >>> api_prefix()
    ''
    >>> os.environ['PLANTDB_PREFIX'] = "/plantdb"
    >>> api_prefix()
    '/plantdb'
    """
    if prefix is None or prefix == "":
        prefix = os.getenv("PLANTDB_PREFIX", "")  # Default to no prefix

    prefix = prefix.rstrip('/')  # Remove the trailing slash if present
    os.environ['PLANTDB_PREFIX'] = prefix
    return prefix
