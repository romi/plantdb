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
# License along with plantdb.  If not, see
# <https://www.gnu.org/licenses/>.
# ------------------------------------------------------------------------------

"""
# REST API Requests Module

Provides a unified interface for communicating with the PlantDB REST API, handling
authentication, data retrieval, and file transfers. It abstracts HTTP request details,
allowing client code to focus on business logic.

## Key Features

- Centralized `make_api_request` supporting GET, POST, PUT, DELETE with timeout,
  SSL verification, and custom headers.
- Convenience helpers for authentication (login, logout, token validation/refresh).
- Utilities for managing scans, images, archives, and dataset files.
- Streamed download/upload support with timing for performance monitoring.

## Usage Examples

```python
>>> from plantdb.client.rest_api.requests import request_login, request_scan_image
>>> login = request_login('localhost', 'admin', 'admin', port=5000)
>>> content_type, _, img_bytes = request_scan_image(
...     'localhost', 'real_plant', 'images', '00000_rgb',
...     port=5000, session_token=login['access_token'])
>>> # `img_bytes` now contains the raw image data.
```
"""

import io
import os
from io import BytesIO
from pathlib import Path
from typing import Union

import pybase64
import requests
from PIL import Image
from ada_url import join_url

from plantdb.client.rest_api.urls import api_token_url
from plantdb.client.rest_api.urls import archive_url
from plantdb.client.rest_api.urls import login_url
from plantdb.client.rest_api.urls import logout_url
from plantdb.client.rest_api.urls import origin_url
from plantdb.client.rest_api.urls import refresh_url
from plantdb.client.rest_api.urls import register_url
from plantdb.client.rest_api.urls import scan_image_url
from plantdb.client.rest_api.urls import scan_url
from plantdb.client.rest_api.urls import scans_url
from plantdb.client.rest_api.urls import token_refresh_url
from plantdb.client.rest_api.urls import token_validation_url
from plantdb.commons.api_endpoints import sanitize_name
from plantdb.commons.log import get_logger

logger = get_logger(__name__)


def make_api_request(url, method="GET", params=None, json_data=None,
                     allow_redirects=True, **kwargs):
    """Function to make an API request with various HTTP methods and options.

    Parameters
    ----------
    url : str
        The URL for the API endpoint.
    method : {'GET', 'POST', 'PUT', 'DELETE'}, optional
        The HTTP method to use. Default is 'GET'.
    params : dict, optional
        Dictionary of query parameters to append to the URL.
    json_data : dict, optional
        JSON payload to send in the body of the request for 'POST' and 'PUT' methods.
    allow_redirects : bool, optional
        Whether to allow redirects. Default is True.

    Other Parameters
    ----------------
    header : dict
        The HTTP headers to send in the request. Default is None.
    files : dict
        Additional files to send in the request. Default is None.
    data : dict, list, or bytes
        The data to send in the request. Default is None.
    timeout : int
        Timeout to use for the request. Default is 5 seconds.
    stream : bool
        Flag indicating whether to stream the request. Default is False.
    session_token : str
        The PlantDB REST API session token of the user.
        It should be supplied for every request that requires authentication on the server-side.

    Returns
    -------
    requests.Response
        The response object from the API request.

    Raises
    ------
    ValueError
        If an unsupported HTTP method is provided.
    requests.exceptions.SSLError
        If there's an SSL error during the request.
    requests.exceptions.RequestException
        For any other exception raised by the underlying `requests` library.

    Notes
    -----
    This function is designed to handle various HTTP methods (GET, POST, PUT, DELETE) and provides a unified interface for making API requests. It supports SSL verification and allows for custom parameters and JSON data to be sent with the request.
    It passes keyword arguments to the underlying `requests` library.

    Examples
    --------
    >>> from plantdb.client.rest_api.requests import make_api_request
    >>> from plantdb.client.rest_api.urls import login_url
    >>> response = make_api_request(login_url('localhost', port=5000), "POST", json_data={'username': 'admin', 'password': 'admin'})
    >>> access_token, refresh_token = response.json()['access_token'], response.json()['refresh_token']
    >>> user = response.json()['user']
    """
    requests_kwargs = {}
    requests_kwargs['params'] = params
    requests_kwargs['allow_redirects'] = allow_redirects

    # Add a default timeout of 5 seconds if not provided
    requests_kwargs['timeout'] = kwargs.get('timeout', 5.0)

    # Prepare SSL/TLS verification; if CERT_PATH is supplied, use it,
    # otherwise default to requests' built‑in verification
    requests_kwargs['verify'] = os.getenv('CERT_PATH', True)

    requests_kwargs['headers'] = kwargs.get('headers', {})
    # If a session token is supplied, add it to the Authorization header
    if 'session_token' in kwargs:
        requests_kwargs['headers'].update({'Authorization': f"Bearer {kwargs.get('session_token')}"})

    # Normalize the HTTP method name to uppercase for comparison
    method = method.upper()

    # Add an empty JSON payload to forces the requests library to add the correct `Content‑Type: application/json` header
    if not json_data:
        json_data = {}

    try:
        if method.upper() == "GET":
            # GET: retrieve a resource, may include query params
            response = requests.get(url, **requests_kwargs)
        elif method.upper() == "POST":
            # POST: send data (json_data or raw binary)
            response = requests.post(url, json=json_data, **requests_kwargs)
        elif method.upper() == "PUT":
            # PUT: replace or update a resource
            response = requests.put(url, json=json_data, **requests_kwargs)
        elif method.upper() == "DELETE":
            # DELETE: remove a resource
            response = requests.delete(url, **requests_kwargs)
        else:
            # Unsupported HTTP method
            raise ValueError(f"Unsupported HTTP method: {method}")

        response.raise_for_status()  # Raise exception for 4XX/5XX responses
        return response
    except requests.exceptions.SSLError as e:
        logger.error(f"SSL Error: {e}")
        raise e from e
    except requests.exceptions.RequestException as e:
        logger.error(f"Request Error: {e}")
        raise e from e


def request_login(host, username, password, **kwargs) -> dict:
    """Send a login request to the authentication service.

    This helper function constructs a POST request to the login endpoint
    and forwards any additional keyword arguments to the URL generator
    function.

    Parameters
    ----------
    host : str
        The hostname or IP address of the PlantDB REST API server.
    username : str
        The user identifier for authentication.
    password : str
        The user's secret password. It is sent in the request body and
        should be handled securely (_e.g._, over HTTPS).

    Other Parameters
    ----------------
    port : int
        The PlantDB API port number, defaults to ``None``.
    prefix : str
        A path prefix for the PlantDB API, defaults to ``None``.
    ssl : bool
        A boolean flag indicating whether to use HTTPS (``True``) or HTTP (``False``). Defaults to ``False``.

    Returns
    -------
    dict
        The login data from the response if successful.

    Notes
    -----
    * The password is transmitted as plain JSON in the request body;
      ensure the endpoint is served over HTTPS to protect credentials.
    * The function does not perform any client‑side validation of the credentials;
      errors are reported by the API response.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api.requests import request_login
    >>> login_data = request_login('localhost', 'admin', 'admin', port=5000)
    >>> print(list(login_data))
    ['access_token', 'message', 'refresh_token', 'user']
    """
    url = login_url(host, **kwargs)
    data = {
        'username': username,
        'password': password
    }
    return make_api_request(url, method="POST", json_data=data).json()


def request_check_username(host, username, **kwargs) -> bool:
    """Send a username availability request to the authentication service.

    This helper function constructs a GET request to the login endpoint
    and forwards any additional keyword arguments to the URL generator
    function.

    Parameters
    ----------
    host : str
        The hostname or IP address of the PlantDB REST API server.
    username : str
        The user identifier for authentication.

    Other Parameters
    ----------------
    port : int
        The PlantDB API port number, defaults to ``None``.
    prefix : str
        A path prefix for the PlantDB API, defaults to ``None``.
    ssl : bool
        A boolean flag indicating whether to use HTTPS (``True``) or HTTP (``False``). Defaults to ``False``.

    Returns
    -------
    bool
        A boolean flag indicating whether the username is valid (``True``) or not (``False``).

    Notes
    -----
    * The password is transmitted as plain JSON in the request body;
      ensure the endpoint is served over HTTPS to protect credentials.
    * The function does not perform any client‑side validation of the credentials;
      errors are reported by the API response.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api.requests import request_check_username
    >>> username_exists = request_check_username('localhost', 'admin', port=5000)
    >>> print(username_exists)
    True
    """
    url = login_url(host, **kwargs)
    return make_api_request(url, method="GET", params={'username': username}).json()['exists']


def request_logout(host, **kwargs) -> tuple[bool, str]:
    """Send a logout request to the authentication service.

    This helper function constructs a POST request to the logout endpoint
    and forwards any additional keyword arguments to the URL generator
    function.

    Parameters
    ----------
    host : str
        The hostname or IP address of the PlantDB REST API server.

    Other Parameters
    ----------------
    port : int
        The PlantDB API port number, defaults to ``None``.
    prefix : str
        A path prefix for the PlantDB API, defaults to ``None``.
    ssl : bool
        A boolean flag indicating whether to use HTTPS (``True``) or HTTP (``False``). Defaults to ``False``.
    session_token : str
        The PlantDB REST API session token of the user.

    Returns
    -------
    tuple[bool, str]
        A boolean flag indicating whether the logout request was successful (``True``) or not (``False``).
        A string with the log out message.

    Notes
    -----
    * The session_token is transmitted as plain JSON in the request header;
      ensure the endpoint is served over HTTPS to protect credentials.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api.requests import request_login
    >>> from plantdb.client.rest_api.requests import request_logout
    >>> login_data = request_login('localhost', 'admin', 'admin', port=5000)
    >>> success, msg = request_logout('localhost', port=5000, session_token=login_data['access_token'])
    >>> print(success)
    True
    """
    url = logout_url(host, **kwargs)
    response = make_api_request(url, method="POST", session_token=kwargs.get('session_token', None))
    return response.ok, response.json()['message']


def request_token_validation(host, **kwargs) -> dict:
    """Validate a token by making a POST request to the token validation endpoint.

    Parameters
    ----------
    host : str
        The hostname or base URL used to construct the validation endpoint.

    Other Parameters
    ----------------
    port : int
        The PlantDB API port number, defaults to ``None``.
    prefix : str
        A path prefix for the PlantDB API, defaults to ``None``.
    ssl : bool
        A boolean flag indicating whether to use HTTPS (``True``) or HTTP (``False``). Defaults to ``False``.
    session_token : str
        The PlantDB REST API session token of the user.

    Returns
    -------
    dict
        The token validation data from the response, if successful.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api.requests import request_login
    >>> from plantdb.client.rest_api.requests import request_token_validation
    >>> login_data = request_login('localhost', 'admin', 'admin', port=5000)
    >>> token_data = request_token_validation('localhost', port=5000, session_token=login_data['access_token'])
    >>> print(token_data['user'])
    {'username': 'admin', 'fullname': 'PlantDB Admin'}
    """
    url = token_validation_url(host, **kwargs)
    return make_api_request(url, method="POST", session_token=kwargs.get('session_token', None)).json()


def request_token_refresh(host, **kwargs) -> dict:
    """Refresh a token by making a POST request to the token refresh endpoint.

    Parameters
    ----------
    host : str
        The hostname or base URL used to construct the refresh endpoint.

    Other Parameters
    ----------------
    port : int
        The PlantDB API port number, defaults to ``None``.
    prefix : str
        A path prefix for the PlantDB API, defaults to ``None``.
    ssl : bool
        A boolean flag indicating whether to use HTTPS (``True``) or HTTP (``False``). Defaults to ``False``.
    session_token : str
        The PlantDB REST API session token of the user.

    Returns
    -------
    dict
        The token refresh data from the response, if successful.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api.requests import request_login
    >>> from plantdb.client.rest_api.requests import request_token_refresh
    >>> login_data = request_login('localhost', 'admin', 'admin', port=5000)
    >>> token_refresh = request_token_refresh('localhost', port=5000, refresh_token=login_data['refresh_token'])
    >>> print([key for key in token_refresh.json() if 'token' in key])
    ['access_token', 'refresh_token']
    """
    url = token_refresh_url(host, **kwargs)
    return make_api_request(url, method="POST", json_data={'refresh_token': kwargs.get('refresh_token', None)}).json()


def request_api_token(host, token_exp, datasets, **kwargs) -> dict:
    """Refresh a token by making a POST request to the token refresh endpoint.

    Parameters
    ----------
    host : str
        The hostname or base URL used to construct the refresh endpoint.
    token_exp : int
        The expiration duration of the API token in seconds.
    datasets : list[dict[str, Tuple[Permissions]]
        A dictionary where the keys are dataset names, and the values are either
        a tuple of `Permission` instances or a single `Permission` instance
        defining the access levels for each dataset.


    Other Parameters
    ----------------
    port : int
        The PlantDB API port number, defaults to ``None``.
    prefix : str
        A path prefix for the PlantDB API, defaults to ``None``.
    ssl : bool
        A boolean flag indicating whether to use HTTPS (``True``) or HTTP (``False``). Defaults to ``False``.
    session_token : str
        The PlantDB REST API session token of the user.

    Returns
    -------
    dict
        The token refresh data from the response, if successful.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api.requests import request_login
    >>> from plantdb.client.rest_api.requests import request_api_token
    >>> login_data = request_login('localhost', 'admin', 'admin', port=5000)
    >>> api_token_data = request_api_token('localhost', 3600, {"Dataset_A": ('read', 'write', 'create')}, port=5000, session_token=login_data['access_token'])
    >>> api_token = api_token_data['api_token']
    >>> print(api_token)
    """
    url = api_token_url(host, **kwargs)
    # Extract the payload arguments (they are optional so we provide sensible defaults)
    payload = {"datasets": datasets, "token_exp": token_exp}
    return make_api_request(url, method="POST", json_data=payload,
                            session_token=kwargs.get('session_token', None)).json()


def request_new_user(host, username, password, fullname, **kwargs) -> bool:
    """Send a registration request to the authentication service.

    This helper function constructs a POST request to the register endpoint
    and forwards any additional keyword arguments to the URL generator
    function.

    Parameters
    ----------
    host : str
        The hostname or IP address of the PlantDB REST API server.
    username : str
        The user identifier to add.
    password : str
        The user's secret password to use. It is sent in the request body and
        should be handled securely (e.g., over HTTPS).
    fullname : str
        The user's full name to use.

    Other Parameters
    ----------------
    port : int
        The PlantDB API port number, defaults to ``None``.
    prefix : str
        A path prefix for the PlantDB API, defaults to ``None``.
    ssl : bool
        A boolean flag indicating whether to use HTTPS (``True``) or HTTP (``False``). Defaults to ``False``.
    session_token : str
        The PlantDB REST API session token of the user.

    Returns
    -------
    bool
        A boolean indicating whether the request was successful (``True``) or not (``False``).

    Notes
    -----
    * The session_token is transmitted as plain JSON in the request header;
      ensure the endpoint is served over HTTPS to protect credentials.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api.requests import request_login
    >>> from plantdb.client.rest_api.requests import request_logout
    >>> from plantdb.client.rest_api.requests import request_new_user
    >>> login_data = request_login('localhost', 'admin', 'admin', port=5000)
    >>> user_added = request_new_user('localhost', 'testuser', 'fake_password', 'Test User', port=5000, session_token=login_data['access_token'])
    >>> print(user_added)
    True
    >>> logout = request_logout('localhost', port=5000, session_token=login_data['access_token'])
    >>> login_data = request_login('localhost', 'testuser', 'fake_password', port=5000)
    >>> print(login_data['user']['username'])
    testuser
    """
    url = register_url(host, **kwargs)
    data = {'username': username, 'fullname': fullname, 'password': password}
    return make_api_request(url, method="POST", json_data=data, session_token=kwargs.get('session_token', None)).ok


def request_scan_names_list(host, **kwargs) -> list[str]:
    """Get the list of the scan datasets names served by the PlantDB REST API.

    Parameters
    ----------
    host : str
        The hostname or IP address of the PlantDB REST API server.

    Other Parameters
    ----------------
    port : int
        The PlantDB API port number, defaults to ``None``.
    prefix : str
        A path prefix for the PlantDB API, defaults to ``None``.
    ssl : bool
        A boolean flag indicating whether to use HTTPS (``True``) or HTTP (``False``). Defaults to ``False``.
    session_token : str
        The PlantDB REST API session token of the user.

    Returns
    -------
    list[str]
        The list of the scan datasets names from the response, if successful.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api.requests import request_scan_names_list
    >>> print(request_scan_names_list('localhost', port=5000)
    ['arabidopsis000', 'real_plant', 'real_plant_analyzed', 'virtual_plant', 'virtual_plant_analyzed']
    """
    url = scans_url(host, **kwargs)
    return make_api_request(url=url, method="GET", session_token=kwargs.get('session_token', None)).json()


def request_scans_info(host, **kwargs) -> list[dict]:
    """Retrieve the information dictionary for all scans from the PlantDB REST API.

    Other Parameters
    ----------------
    port : int
        The PlantDB API port number, defaults to ``None``.
    prefix : str
        A path prefix for the PlantDB API, defaults to ``None``.
    ssl : bool
        A boolean flag indicating whether to use HTTPS (``True``) or HTTP (``False``). Defaults to ``False``.
    session_token : str
        The PlantDB REST API session token of the user.

    Returns
    -------
    list[dict]
        The list of scan information dictionaries obtained from the response, if successful.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api.requests import request_scans_info
    >>> from plantdb.client.rest_api.requests import request_login
    >>> login_data = request_login('localhost', 'admin', 'admin', port=5000)
    >>> scans_info = request_scans_info('localhost', port=5000, session_token=login_data['access_token'])
    >>> print(sorted([scan['id'] for scan in scans_info]))
    ['arabidopsis000', 'real_plant', 'real_plant_analyzed', 'virtual_plant', 'virtual_plant_analyzed']
    """
    scan_list = request_scan_names_list(host, **kwargs)
    return [make_api_request(url=scan_url(host, scan, **kwargs), session_token=kwargs.get('session_token', None)).json()
            for scan in scan_list]


def request_scan_data(host, scan_id, **kwargs) -> dict:
    """Retrieve the data dictionary for a given scan dataset from the PlantDB REST API.

    Parameters
    ----------
    host : str
        The hostname or IP address of the PlantDB REST API server.
    scan_id : str
        The name of the scan dataset to be retrieved.

    Other Parameters
    ----------------
    port : int
        The PlantDB API port number, defaults to ``None``.
    prefix : str
        A path prefix for the PlantDB API, defaults to ``None``.
    ssl : bool
        A boolean flag indicating whether to use HTTPS (``True``) or HTTP (``False``). Defaults to ``False``.
    session_token : str
        The PlantDB REST API session token of the user.

    Returns
    -------
    dict
        The data dictionary for the given scan dataset obtained from the response, if successful.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api.requests import request_scan_data
    >>> from plantdb.client.rest_api.requests import request_login
    >>> login_data = request_login('localhost', 'admin', 'admin', port=5000)
    >>> scan_data = request_scan_data('localhost', 'real_plant', port=5000, session_token=login_data['access_token'])
    >>> print(scan_data['id'])
    real_plant
    >>> print(scan_data['hasColmap'])
    False
    """
    scan_id = sanitize_name(scan_id)
    url = scan_url(host, scan_id, **kwargs)
    response = make_api_request(url=url, session_token=kwargs.get('session_token', None))
    if response.ok:
        return response.json()
    elif response.status_code == 404:
        print(response.json()['message'])
        return {}
    else:
        print(response.json()['message'])
        return {}


def request_scan_image(host, scan_id, fileset_id, file_id,
                       size='orig', as_base64=False, **kwargs) -> tuple[str, str, Union[str, bytes]]:
    """Get the image for a scan dataset and task fileset served by the PlantDB REST API.

    Parameters
    ----------
    host : str
        The hostname or IP address of the PlantDB REST API server.
    scan_id : str
        The name of the scan dataset to be retrieved.
    fileset_id : str
        The name of the fileset containing the image to be retrieved.
    file_id : str
        The name of the image file to be retrieved.
    size : {'orig', 'large', 'thumb'} or int, optional
        If an integer, use  it as the size of the cached image to create and return.
        Else, should be a string, defaulting to ``'orig'``, and it works as follows:
           * ``'thumb'``: image max width and height to `150`.
           * ``'large'``: image max width and height to `1500`;
           * ``'orig'``: original image, no cache;
    as_base64 : bool
        A boolean flag indicating whether to return an image as a base64 string.

    Other Parameters
    ----------------
    port : int
        The PlantDB API port number, defaults to ``None``.
    prefix : str
        A path prefix for the PlantDB API, defaults to ``None``.
    ssl : bool
        A boolean flag indicating whether to use HTTPS (``True``) or HTTP (``False``). Defaults to ``False``.
    session_token : str
        The PlantDB REST API session token of the user.

    Returns
    -------
    tuple[str, str, Union[str, bytes]]
        If ``as_base64==True``, a dictionary with the 'image' encoded as base64 and the mimetype in 'content-type'.
        Else the image data as bytes.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api.requests import request_scan_image
    >>> import pybase64
    >>> from PIL import Image
    >>> from io import BytesIO
    >>> # Example #1 - Get an image as binary data:
    >>> db_img = ['real_plant', 'images', '00000_rgb']
    >>> _, _, img_bytes = request_scan_image('localhost', *db_img, port=5000)  # download the image
    >>> print(img_bytes[:10])
    b'\xff\xd8\xff\xe0\x00\x10JFIF'
    >>> image = Image.open(BytesIO(img_bytes))  # Open the image from the bytes data
    >>> image.show()  # Display the image
    >>> # Example #2 - Get an image as base64 data:
    >>> _, _, b64_string = request_scan_image('localhost', *db_img, port=5000, as_base64=True)
    >>> print(b64_string[:50])
    /9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAAUEBAQEAwUEBAQGBQ
    >>> image_data = pybase64.b64decode(b64_string)
    >>> image = Image.open(BytesIO(image_data))  # Open the image from the base64 data
    >>> image.show()
    """
    url = scan_image_url(host, scan_id, fileset_id, file_id, size, as_base64, **kwargs)
    response = make_api_request(url=url, session_token=kwargs.get('session_token', None))
    content_type = response.headers.get('Content-Type')
    encoding = response.headers.get("X-Content-Encoding")
    if as_base64:
        content_type = response.json()['content-type']
        img_str = response.json()['image']
        return content_type, encoding, img_str
    else:
        return content_type, encoding, response.content


def request_scan_tasks_fileset(host, scan_id, **kwargs) -> dict:
    """Get the task name to fileset name mapping dictionary from the REST API.

    Parameters
    ----------
    host : str
        The hostname or IP address of the PlantDB REST API server.
    scan_id : str
        The name of the dataset to retrieve the mapping for.

    Other Parameters
    ----------------
    port : int
        The PlantDB API port number, defaults to ``None``.
    prefix : str
        A path prefix for the PlantDB API, defaults to ``None``.
    ssl : bool
        A boolean flag indicating whether to use HTTPS (``True``) or HTTP (``False``). Defaults to ``False``.
    session_token : str
        The PlantDB REST API session token of the user.

    Returns
    -------
    dict
        The mapping of the task name to fileset name.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api.requests import request_scan_tasks_fileset
    >>> request_scan_tasks_fileset('localhost', 'real_plant', port=5000)
    {'images': 'images'}
    >>> request_scan_tasks_fileset('localhost', 'real_plant_analyzed', port=5000)
    {'images': 'images',
     'AnglesAndInternodes': 'AnglesAndInternodes_1_0_2_0_6_0_6dd64fc595',
     'TreeGraph': 'TreeGraph__False_CurveSkeleton_c304a2cc71',
     'CurveSkeleton': 'CurveSkeleton__TriangleMesh_0393cb5708',
     'TriangleMesh': 'TriangleMesh_9_most_connected_t_open3d_00e095c359',
     'PointCloud': 'PointCloud_1_0_1_0_10_0_7ee836e5a9',
     'Voxels': 'Voxels___x____300__450__colmap_camera_False_2a093f0ccc',
     'Masks': 'Masks_1__0__1__0____channel____rgb_5619aa428d',
     'Colmap': 'Colmap_True_null_SIMPLE_RADIAL_ffcef49fdc',
     'Undistorted': 'Undistorted_SIMPLE_RADIAL_Colmap__a333f181b7'}
     """
    return request_scan_data(host, scan_id, **kwargs).get('tasks_fileset', dict())


def request_refresh(host, scan_id=None, **kwargs) -> tuple[bool, str]:
    """Refreshes the database, potentialy only for a specified dataset.

    Parameters
    ----------
    host : str
        The hostname or IP address of the PlantDB REST API server.
    scan_id : str or None
        The name of the dataset to trigger a refresh.
        If ``None``, the entire database is refreshed.

    Other Parameters
    ----------------
    port : int or str, optional
        The port number of the PlantDB REST API server. Defaults to ``PLANTDB_PORT``.
    prefix : str, optional
        The prefix to be prepended to the URL. If provided, it will be stripped of leading and trailing slashes.
        Defaults to ``None``.
    ssl : bool, optional
        Flag indicating whether to use HTTPS (``True``) or HTTP (``False``). Defaults to ``False``.
    timeout : int, optional
        A timeout, in seconds, to succeed the refresh request. Defaults to ``5``.
    session_token : str
        The PlantDB REST API session token of the user.

    Returns
    -------
    tuple[bool, str]
        A boolean indicating whether the refresh request succeeded.

    Raises
    ------
    HTTPError
        If the request fails or the response status is not successful.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api.requests import request_refresh
    >>> success, message = request_refresh('localhost', "arabidopsis000", port = 5000)
    >>> print(message)
    Successfully reloaded scan 'arabidopsis000'
    """
    url = refresh_url(host, scan_id, **kwargs)
    response = make_api_request(url, session_token=kwargs.get('session_token', None))
    return response.ok, response.json()["message"]


def request_archive_download(host, scan_id, out_dir=None, **kwargs):
    """Downloads a scan archive file from a defined dataset based on the specified API parameters.

    This function fetches a scan archive in stream mode from a remote API. The archive
    is expected to be in the form of a binary content stream. The success of the
    operation is determined by the HTTP response received from the API.

    Parameters
    ----------
    host : str
        The hostname or IP address of the PlantDB REST API server.
    scan_id : str
        The name of the dataset from which the scan archive file is to be downloaded.
    out_dir : str or pathlib.Path, optional
        A path to the directory where to save the archive.

    Other Parameters
    ----------------
    host : str, optional
        The hostname or IP address of the PlantDB REST API server. Defaults to ``REST_API_URL``.
    port : int or str, optional
        The port number of the PlantDB REST API server. Defaults to ``PLANTDB_PORT``.
    prefix : str, optional
        The prefix to be prepended to the URL. If provided, it will be stripped of leading and trailing slashes.
        Defaults to ``None``.
    ssl : bool, optional
        Flag indicating whether to use HTTPS (``True``) or HTTP (``False``). Defaults to ``False``.
    timeout : int, optional
        A timeout, in seconds, to succeed the download request. Defaults to ``10``.
    session_token : str
        The PlantDB REST API session token of the user.

    Returns
    -------
    BytesIO or str
        A `BytesIO` object containing the binary content of the downloaded scan archive.
        A path to the downloaded file, if a directory path is specified.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api.requests import request_archive_download
    >>> request_archive_download('localhost', "arabidopsis000", out_dir='/tmp', port=5000)
    ('/tmp/arabidopsis000.zip', 'Download completed in 0.05 seconds.')
    """
    import time
    # Construct API URL for archive download using dataset name and optional parameters
    url = archive_url(host, scan_id, **kwargs)

    request_kwargs = {
        'session_token': kwargs.get('session_token', None),
        'timeout': kwargs.get('timeout', 10),
    }

    # Track download duration for performance monitoring
    start_time = time.time()  # Start timing
    # Make streaming API request with configurable timeout and optional certificate
    response = make_api_request(url, stream=True, **request_kwargs)

    end_time = time.time()  # End timing
    duration = end_time - start_time
    msg = f"Download completed in {duration:.2f} seconds."

    if out_dir is not None:
        # Save archive to specified directory with dataset name as filename
        out_dir = Path(out_dir) / f"{scan_id}.zip"
        with open(out_dir, "wb") as archive_file:
            archive_file.write(response.content)
        return f"{out_dir}", msg
    else:
        # Return archive content in memory if no output directory specified
        return BytesIO(response.content), msg


def request_archive_upload(host, scan_id, path, **kwargs):
    """Upload a scan archive file to a specified dataset on a server.

    This function sends a POST request to upload a scan archive file to a
    particular dataset, utilizing the archive URL and optionally specified
    additional API-related request parameters. Ensures proper handling of
    file opening/closing procedures and response status checks.

    Parameters
    ----------
    host : str
        The hostname or IP address of the PlantDB REST API server.
    scan_id : str
        The name of the target dataset for the archive upload.
    path : str, pathlib.Path
        The local file system path to the archive to be uploaded.

    Other Parameters
    ----------------
    host : str, optional
        The hostname or IP address of the PlantDB REST API server. Defaults to ``REST_API_URL``.
    port : int or str, optional
        The port number of the PlantDB REST API server. Defaults to ``PLANTDB_PORT``.
    prefix : str, optional
        The prefix to be prepended to the URL. If provided, it will be stripped of leading and trailing slashes.
        Defaults to ``None``.
    ssl : bool, optional
        Flag indicating whether to use HTTPS (``True``) or HTTP (``False``). Defaults to ``False``.
    timeout : int, optional
        A timeout, in seconds, to succeed the upload request. Defaults to ``120``.
    session_token : str
        The PlantDB REST API session token of the user.

    Returns
    -------
    str
        The time it took to upload the archive.

    Raises
    ------
    requests.exceptions.RequestException
        If the HTTP request fails for any reason.
    requests.exceptions.HTTPError
        If the request returns an unsuccessful HTTP status code.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api.requests import request_archive_upload
    >>> request_archive_upload('localhost', "arabidopsis000", path='/tmp/arabidopsis000.zip', port=5000)
    'Upload completed in 0.10 seconds.'
    """
    import time
    from zipfile import ZipFile

    if isinstance(path, str):
        path = Path(path)
    # Verify path existence
    if not path.is_file():
        raise FileNotFoundError(f"The file at path '{path}' does not exist!")
    # Verify the integrity of the ZIP file
    try:
        with ZipFile(path, 'r') as zip_file:
            zip_file.testzip()
    except Exception as e:
        print(e)
        raise IOError(f"Invalid ZIP file '{path}!'")

    # Construct the URL for the archive upload:
    url = archive_url(host, scan_id, **kwargs)

    request_kwargs = {
        'session_token': kwargs.get('session_token', None),
        'timeout': kwargs.get('timeout', 120),
    }

    start_time = time.time()  # Start timing
    with open(path, "rb") as f:
        try:
            res = make_api_request(url,
                                   method="POST",
                                   files={"zip_file": (path.name, f, "application/zip")},
                                   stream=True,
                                   **request_kwargs)
        except requests.exceptions.Timeout:
            timeout = kwargs.get("timeout", 120)
            raise RuntimeError(f"The upload request timed out after {timeout} seconds.")
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"An error occurred during the upload: {e}")
    end_time = time.time()  # End timing

    if res.ok:
        duration = end_time - start_time
        return f"Upload completed in {duration:.2f} seconds."
    else:
        res.raise_for_status()  # Raise an error if the request failed


def request_dataset_file_upload(host, scan_id, file_path, chunk_size=0, **kwargs):
    """Uploads a file to the server using the DatasetFile POST endpoint.

    Parameters
    ----------
    host : str
        The hostname or IP address of the PlantDB REST API server.
    scan_id : str
        The unique identifier of the scan associated with the file upload.
    file_path : str
        The path to the file to be uploaded.
    chunk_size : int, optional
        The size of chunks (in bytes) to read and send, by default 0 (no chunking).

    Other Parameters
    ----------------
    port : int
        The PlantDB API port number, defaults to ``None``.
    prefix : str
        A path prefix for the PlantDB API, defaults to ``None``.
    ssl : bool
        A boolean flag indicating whether to use HTTPS (``True``) or HTTP (``False``). Defaults to ``False``.
    session_token : str
        The PlantDB REST API session token of the user.

    Returns
    -------
    dict
        A dictionary containing the server's response.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api.requests import request_dataset_file_upload
    >>> request_dataset_file_upload(host, 'arabidopsis000', '/path/to/local/file.txt')
    """
    from os.path import basename
    from os.path import getsize
    # Prepare the URL and headers
    url = origin_url(host, **kwargs)
    url = join_url(url, f"files/{scan_id}")

    filename = basename(file_path)
    file_size = getsize(file_path)
    # Create the request header
    headers = {
        "Content-Disposition": f"attachment; filename={filename}",
        "Content-Length": str(file_size),
        "X-File-Path": filename,
    }

    try:
        # Open the file for reading
        with open(file_path, 'rb') as f:
            if chunk_size > 0:
                # Upload in chunks
                headers["X-Chunk-Size"] = str(chunk_size)
                bytes_sent = 0
                while bytes_sent < file_size:
                    chunk = f.read(chunk_size)
                    response = make_api_request(
                        url,
                        method="POST",
                        headers=headers,
                        data=chunk,
                        session_token=kwargs.get('session_token', None)
                    )
                    bytes_sent += len(chunk)
                    # Check if the request was successful
                    if response.status_code not in (200, 201):
                        return {"error": "File upload failed", "status_code": response.status_code,
                                "response": response.json()}
            else:
                # Upload the entire file
                response = make_api_request(url, method='POST', headers=headers, data=f,
                                            session_token=kwargs.get('session_token', None))

        # Return the server's response
        if response.status_code in (200, 201):
            return response.json()
        else:
            return {"error": "File upload failed", "status_code": response.status_code, "response": response.json()}
    except Exception as e:
        return {"error": str(e)}
