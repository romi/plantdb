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
# REST API Client Module

A Python module that provides a comprehensive interface for interacting with a scanning and image processing REST API service.
This client handles various operations related to scan management, image processing, and data retrieval while abstracting away the complexities of HTTP communications.

## Key Features

- **Scan Management**: Create, retrieve, refresh, and archive scans
- **Image Processing**: Handle scan images, preview generations, and image data retrieval
- **Configuration Management**: Load and manage scan and reconstruction configurations
- **Data Parsing**: Support for multiple data formats including PCD, mesh, skeleton, and JSON
- **File Operations**: Upload and download capabilities for datasets and scan archives
- **Security**: Built-in certificate handling for secure API communications
- **Task Management**: Retrieve and process task-related data and file sets
- **URL Generation**: Automated URL construction for various API endpoints

## Environment variables

- `PLANTDB_HOST`: default hostname to PlantDB REST API
- `PLANTDB_PORT`: default port to PlantDB REST API
- `PLANTDB_PREFIX`: default URL prefix for the plantdb REST API
- `CERT_PATH`: Path to a certificate file for SSL verification. If ``None`` (default), default SSL verification is used.

"""

import json
import os
from io import BytesIO
from pathlib import Path

import requests
from PIL import Image
from ada_url import join_url
from plyfile import PlyData

from plantdb.client import api_endpoints
from plantdb.client.api_endpoints import sanitize_name

#: Default hostname to PlantDB REST API is 'localhost':
PLANTDB_HOST = os.getenv('PLANTDB_HOST', "localhost")
#: Default port to PlantDB REST API:
PLANTDB_PORT = os.getenv('PLANTDB_PORT', '')
if PLANTDB_PORT.strip() == '' or PLANTDB_PORT.lower() == 'none':
    PLANTDB_PORT = None  # explicit “no‑port” requested
else:
    try:
        PLANTDB_PORT = int(PLANTDB_PORT)  # normal integer port
    except ValueError:
        # the value is something unexpected – fall back to the default
        PLANTDB_PORT = None
#: Default URL prefix for the plantdb REST API
PLANTDB_PREFIX = os.getenv('PLANTDB_PREFIX', None)


# -----------------------------------------------------------------------------
# URL construction methods
# -----------------------------------------------------------------------------
def origin_url(host, port=None, ssl=False, **kwargs) -> str:
    # Attempt to split the host to check for an existing scheme (http/https)
    try:
        scheme, host = host.split('://')
    except ValueError:
        pass  # If no scheme is found, proceed with the default
    else:
        # If 's' is in the scheme, it indicates HTTPS
        if 's' in scheme:
            ssl = True

    # Ensure port is converted to string and has no leading colon
    if port:
        if isinstance(port, int):
            port = str(port)
        port = ':' + port.lstrip(':')
    else:
        port = ''

    # Construct the final URL
    return f"http{'s' if ssl else ''}://{host}{port}"


def plantdb_url(host, port=PLANTDB_PORT, prefix=PLANTDB_PREFIX, ssl=False) -> str:
    """Generates the URL for the PlantDB REST API using the specified host and port.

    This function constructs a URL by combining the provided host, port, prefix, and SSL settings.

    Parameters
    ----------
    host : str
        The hostname or IP address of the PlantDB REST API server.
    port : int or str, optional
        The port number of the PlantDB REST API server. Defaults to ``None``.
    prefix : str, optional
        The prefix to be prepended to the URL. If provided, it will be stripped of leading and trailing slashes.
        If provided, it will be added to the end of the URL.
        Defaults to ``None``.
    ssl : bool, optional
        Flag indicating whether to use HTTPS (``True``) or HTTP (``False``). Defaults to ``False``.

    Returns
    -------
    str
        A properly formatted URL for the PlantDB REST API.

    Notes
    -----
    - The function ensures that the prefix is correctly formatted by stripping leading and trailing slashes.
    - The SSL flag determines whether 'http' or 'https' is used in the URL scheme.

    Examples
    --------
    >>> from plantdb.client.rest_api import plantdb_url
    >>> plantdb_url('localhost')
    'http://localhost'
    >>> plantdb_url('api.example.com', port=8443, ssl=True)
    'https://api.example.com:8443'
    >>> plantdb_url('localhost', port=5000, prefix='/plantdb', ssl=True)
    'https://localhost:5000/plantdb/'
    """
    origin = origin_url(host, port, ssl)

    # Format the prefix by stripping leading and trailing slashes and adding a leading slash
    if prefix:
        prefix = '/' + prefix.lstrip('/').rstrip('/') + '/'
    else:
        prefix = ''

    return f"{origin}{prefix}"


def login_url(host, **kwargs):
    """Generate the full URL for the PlantDB API login endpoint.

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

    Returns
    -------
    str
        The fully qualified login URL as a string.

    Examples
    --------
    >>> from plantdb.client.rest_api import login_url
    >>> # Default URL using module level constants
    >>> url = login_url('localhost')
    >>> print(url)
    http://localhost/login
    >>> # Override host, add a prefix and enable SSL
    >>> url = login_url('dev.romi.local', prefix="/plantdb", ssl=True)
    >>> print(url)
    https://dev.romi.local/plantdb/login
    """
    origin = origin_url(host, **kwargs)
    return join_url(origin, api_endpoints.login(**kwargs))


def logout_url(host, **kwargs):
    """Generate the full URL for the PlantDB API logoutn endpoint.

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

    Returns
    -------
    str
        The fully qualified logoutn URL as a string.

    Examples
    --------
    >>> from plantdb.client.rest_api import logout_url
    >>> # Basic usage with default configuration
    >>> url = logout_url('localhost')
    >>> print(url)
    http://localhost/logout
    >>> # Specify a custom prefix and enable SSL
    >>> url = logout_url('dev.romi.local', prefix="/plantdb", ssl=True)
    >>> print(url)
    https://dev.romi.local/plantdb/logout
    """
    url = origin_url(host, **kwargs)
    return join_url(url, api_endpoints.logout(**kwargs))


def register_url(host, **kwargs):
    """Generate the full URL for the PlantDB API register endpoint.

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

    Returns
    -------
    str
        The fully qualified register URL as a string.

    Examples
    --------
    >>> from plantdb.client.rest_api import logout_url
    >>> # Basic usage with default configuration
    >>> url = logout_url('localhost')
    >>> print(url)
    http://localhost/logout
    >>> # Specify a custom prefix and enable SSL
    >>> url = logout_url('dev.romi.local', prefix="/plantdb", ssl=True)
    >>> print(url)
    https://dev.romi.local/plantdb/logout
    """
    url = origin_url(host, **kwargs)
    return join_url(url, api_endpoints.register(**kwargs))



def scans_url(host, **kwargs):
    """Generates the URL listing the scans from the PlantDB REST API.

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

    Returns
    -------
    str
        A properly formatted URL of the PlantDB REST API pointing to the scans list.

    Examples
    --------
    >>> from plantdb.client.rest_api import scans_url
    >>> scans_url('127.0.0.1')
    'http://127.0.0.1/scans'
    >>> scans_url('localhost', prefix='/plantdb')
    'http://localhost/plantdb/scans'
    >>> scans_url('dev.romi.local', prefix='/plantdb/', ssl=True)
    'https://dev.romi.local/plantdb/scans'
    """
    url = origin_url(host, **kwargs)
    return join_url(url, api_endpoints.scans(**kwargs))


def scan_url(host, scan_id, **kwargs):
    """Generates the URL pointing to the scan JSON from the PlantDB REST API.

    Parameters
    ----------
    scan_id : str
        The name of the scan dataset to retrieve the JSON from.

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
    str
        A properly formatted URL of the PlantDB REST API pointing to the scans list.

    Examples
    --------
    >>> from plantdb.client.rest_api import scan_url
    >>> scan_url('localhost', "real_plant")
    'http://localhost/scans/real_plant'
    >>> scan_url('localhost', "real_plant", prefix='/plantdb')
    'http://localhost/plantdb/scans/real_plant'
    """
    url = origin_url(host, **kwargs)
    return join_url(url, api_endpoints.scan(scan_id, **kwargs))


def scan_preview_image_url(host, scan_id, size="thumb", **kwargs):
    """Get the URL to the preview image for a scan dataset served by the PlantDB REST API.

    Parameters
    ----------
    host : str
        The hostname or IP address of the PlantDB REST API server.
    scan_id : str
        The name of the scan dataset to be retrieved.
    size : {'orig', 'large', 'thumb'} or int, optional
        If an integer, use  it as the size of the cached image to create and return.
        Else, should be a string, defaulting to ``'thumb'``, and it works as follows:
           * ``'thumb'``: image max width and height to `150`.
           * ``'large'``: image max width and height to `1500`;
           * ``'orig'``: original image, no cache;

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
    str
        The URL to the preview image for a scan dataset.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api import scan_preview_image_url
    >>> img_url = scan_preview_image_url('localhost', 'real_plant')
    >>> print(img_url)
    http://localhost/image/real_plant/images/00000_rgb?size=thumb
    >>> img_url = scan_preview_image_url('localhost', 'real_plant', size=100)
    >>> print(img_url)
    http://localhost/image/real_plant/images/00000_rgb?size=100
    >>> # Download and display the image
    >>> import requests
    >>> from PIL import Image
    >>> from io import BytesIO
    >>> response = requests.get(img_url)  # Send a GET request to the URL
    >>> image = Image.open(BytesIO(response.content))  # Open the image from the bytes data
    >>> image.show()  # Display the image
    """
    from plantdb.client.api_endpoints import sanitize_name
    scan_id = sanitize_name(scan_id)
    scan_names = request_scan_names_list(host, **kwargs)
    if scan_id not in scan_names:
        return None

    thumb_uri = request_scan_data(host, scan_id, **kwargs)["thumbnailUri"]
    if size != "thumb":
        thumb_uri = thumb_uri.replace("size=thumb", f"size={size}")
    url = origin_url(host, **kwargs)
    return join_url(url, thumb_uri)


def scan_image_url(host, scan_id, fileset_id, file_id, size='orig', **kwargs):
    """Get the URL to the image for a scan dataset and task fileset served by the PlantDB REST API.

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
    str
        The URL to an image of a scan dataset and task fileset.

    Examples
    --------
    >>> from plantdb.client.rest_api import scan_image_url
    >>> scan_image_url('localhost', "real_plant", "images", "00000_rgb")
    'http://localhost/image/real_plant/images/00000_rgb?size=orig'
    >>> scan_image_url('localhost', "real_plant", "images", "00000_rgb", prefix='/plantdb')
    'http://localhost/plantdb/image/real_plant/images/00000_rgb?size=orig'
    """
    url = origin_url(host, **kwargs)
    return join_url(url, api_endpoints.image(scan_id, fileset_id, file_id, size, **kwargs))


def refresh_url(host, scan_id=None, **kwargs):
    """Generates a formatted URL for refreshing a specific dataset or the entire database.

    Parameters
    ----------
    host : str
        The hostname or IP address of the PlantDB REST API server.
    scan_id : str or None, optional
        The name of the dataset for which the refresh URL needs to be generated.
        If not provided, the refresh URL for the entire server is returned instead.
        Defaults to ``None``.

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
    str
        A correctly formatted URL for refreshing the specified dataset or the entire PlantDB REST API server.

    Examples
    --------
    >>> from plantdb.client.rest_api import refresh_url
    >>> refresh_url('localhost', "real_plant")
    'http://localhost/refresh?scan_id=real_plant'
    >>> refresh_url('localhost', "real_plant", prefix='/plantdb')
    'http://localhost/plantdb/refresh?scan_id=real_plant'
    """
    url = origin_url(host, **kwargs)
    return join_url(url, api_endpoints.refresh(scan_id, **kwargs))


def archive_url(host, scan_id, **kwargs):
    """Generates a formatted URL for accessing the archive of a specific dataset.

    Parameters
    ----------
    host : str
        The hostname or IP address of the PlantDB REST API server.
    scan_id : str
        Name of the dataset to access in the archive.

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
    str
        Fully constructed URL for accessing the specified dataset archive.

    Examples
    --------
    >>> from plantdb.client.rest_api import archive_url
    >>> archive_url('localhost', 'arabidopsis000')
    'http://localhost/archive/arabidopsis000'
    >>> archive_url('localhost', '../arabidopsis000')
    'http://localhost/archive/arabidopsis000'
    >>> archive_url('localhost', 'arabidopsis+000')
    ValueError: Invalid dataset name: 'arabidopsis+000'. Dataset names must be alphanumeric and can include underscores or dashes.
    >>> archive_url('localhost', 'arabidopsis000', prefix='/plantdb')
    'http://localhost/plantdb/archive/arabidopsis000'
    """
    url = origin_url(host, **kwargs)
    return join_url(url, api_endpoints.archive(scan_id, **kwargs))


def scan_file_url(host, scan_id, file_path, **kwargs):
    """Build the URL for accessing a dataset file.

    Parameters
    ----------
    host : str
        The hostname or IP address of the PlantDB REST API server.
    scan_id : str
        The name of the dataset.
    file_path : str
        The path to the file in the databse.

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
    str
        The complete URL for the dataset file.
    """
    url = origin_url(host, **kwargs)
    return join_url(url, api_endpoints.scan_file(scan_id, file_path, **kwargs))


def scan_config_url(host, scan_id, cfg_fname='scan.toml', **kwargs):
    """Return the scan URL to access the scanning configuration file.

    Parameters
    ----------
    host : str
        The hostname or IP address of the PlantDB REST API server.
    scan_id : str
        The name of the dataset.
    cfg_fname : str, optional
        The name of the TOML scan file, defaults to ``'scan.toml'``.

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
    str
        The URL to the scanning configuration file.

    Examples
    --------
    >>> from plantdb.client.rest_api import scan_config_url
    >>> scan_config_url('localhost', 'real_plant')
    'http://localhost/files/real_plant/scan.toml'
    >>> scan_config_url('localhost', 'real_plant', prefix='/plantdb')
    'http://localhost/plantdb/files/real_plant/scan.toml'
    """
    return scan_file_url(host, scan_id, cfg_fname, **kwargs)


def scan_reconstruction_url(host, scan_id, cfg_fname='pipeline.toml', **kwargs):
    """Return the scan URL to access the reconstruction configuration file.

    Parameters
    ----------
    scan_id : str
        The name of the dataset.
    cfg_fname : str, optional
        The name of the TOML scan file, defaults to ``'pipeline.toml'``.

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
    str
        The URL to the reconstruction configuration file.

    Examples
    --------
    >>> from plantdb.client.rest_api import scan_reconstruction_url
    >>> scan_reconstruction_url('localhost', 'real_plant')
    'http://localhost/files/real_plant/pipeline.toml'
    >>> scan_reconstruction_url('localhost', 'real_plant', prefix='/plantdb')
    'http://localhost/plantdb/files/real_plant/pipeline.toml'
    """
    return scan_file_url(host, scan_id, cfg_fname, **kwargs)


def list_task_images_uri(host, scan_id, task_name='images', size='orig', **kwargs):
    """Get the list of images URI for a given dataset and task name.

    Parameters
    ----------
    host : str
        The hostname or IP address of the PlantDB REST API server.
    scan_id : str
        The name of the dataset to retrieve the images for.
    task_name : str, optional
        The name of the task to retrieve the images from. Defaults to 'images'.
    size : {'orig', 'large', 'thumb'} or int, optional
        If an integer, use  it as the size of the cached image to create and return.
        Else, should be a string, defaulting to `'orig'`, and it works as follows:
           * `'thumb'`: image max width and height to `150`.
           * `'large'`: image max width and height to `1500`;
           * `'orig'`: original image, no cache;

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
    list of str
        The list of image URI strings for the PlantDB REST API.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api import list_task_images_uri
    >>> print(list_task_images_uri('localhost', 'real_plant')[2])
    http://localhost/image/real_plant/images/00002_rgb?size=orig
    >>> print(list_task_images_uri('localhost', 'real_plant', size=100)[2])
    http://localhost/image/real_plant/images/00002_rgb?size=100
    """
    scan_info = request_scan_data(host, scan_id, **kwargs)
    tasks_fileset = scan_info["tasks_fileset"]
    images = scan_info["images"]
    url = origin_url(host, **kwargs)
    return [join_url(url, api_endpoints.image(scan_id, tasks_fileset[task_name], Path(img).stem, size, **kwargs)) for
            img in
            images]


# -----------------------------------------------------------------------------
# REQUEST methods
# -----------------------------------------------------------------------------

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

    Returns
    -------
    response : requests.Response
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
    """
    requests_kwargs = {}
    requests_kwargs['params'] = params
    requests_kwargs['allow_redirects'] = allow_redirects

    # Add a default timeout of 5 seconds if not provided
    requests_kwargs['timeout'] = kwargs.get('timeout', 5.0)

    # Prepare SSL/TLS verification; if CERT_PATH is supplied, use it,
    # otherwise default to requests' built‑in verification
    requests_kwargs['verify'] = os.getenv('CERT_PATH', True)

    # If a session token is supplied, add it to the Authorization header
    requests_kwargs['headers'] = kwargs.get('headers', {})
    if 'session_token' in kwargs:
        requests_kwargs['headers'].update({'Authorization': f"Bearer {kwargs.get('session_token')}"})

    # Normalise the HTTP method name to uppercase for comparison
    method = method.upper()

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
        print(f"SSL Error: {e}")
        raise
    except requests.exceptions.RequestException as e:
        print(f"Request Error: {e}")
        raise


def request_login(host, username, password, **kwargs):
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
        The parsed JSON response from the authentication API.
        In successful cases this will include tokens or user metadata.

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
    >>> from plantdb.client.rest_api import request_login
    >>> login_data = request_login('localhost', 'admin', 'admin', port=5000)
    >>> print(login_data)
    """
    url = login_url(host, **kwargs)
    data = {
        'username': username,
        'password': password
    }
    return make_api_request(url, method="POST", json_data=data).json()


def request_check_username(host, username, **kwargs):
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
    dict
        The parsed JSON response from the authentication API.
        In successful cases this will include tokens or user metadata.

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
    >>> from plantdb.client.rest_api import request_check_username
    >>> username_exists = request_check_username('localhost', 'admin', port=5000)
    >>> print(username_exists)
    """
    url = login_url(host, **kwargs)
    return make_api_request(url, method="GET", params={'username': username}).json()


def request_logout(host, **kwargs):
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
    bool
        Indicate if the logout was successful.

    Notes
    -----
    * The session_token is transmitted as plain JSON in the request header;
      ensure the endpoint is served over HTTPS to protect credentials.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api import request_login
    >>> from plantdb.client.rest_api import request_logout
    >>> login_data = request_login('localhost', 'admin', 'admin', port=5000)
    >>> logout = request_logout('localhost', port=5000, session_token=login_data['access_token'])
    >>> print(logout)
    """
    url = logout_url(host, **kwargs)
    return make_api_request(url, method="POST", session_token=kwargs.get('session_token', None)).ok


def request_new_user(host, username, password, fullname, **kwargs):
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
        Indicate if the logout was successful.

    Notes
    -----
    * The session_token is transmitted as plain JSON in the request header;
      ensure the endpoint is served over HTTPS to protect credentials.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api import request_login
    >>> from plantdb.client.rest_api import request_logout
    >>> from plantdb.client.rest_api import request_new_user
    >>> login_data = request_login('localhost', 'admin', 'admin', port=5000)
    >>> user_added = request_new_user('localhost', 'testuser', 'fake_password', 'Test User', port=5000, session_token=login_data['access_token'])
    >>> print(user_added)
    True
    >>> logout = request_logout('localhost', port=5000, session_token=login_data['access_token'])
    """
    url = register_url(host, **kwargs)
    data = {'username': username, 'fullname': fullname, 'password': password}
    return make_api_request(url, method="POST", json_data=data, session_token=kwargs.get('session_token', None)).ok


def request_scan_names_list(host, **kwargs):
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
    list
        The list of scan dataset names served by the PlantDB REST API.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api import request_scan_names_list
    >>> print(request_scan_names_list('localhost', port=5000))
    ['arabidopsis000', 'real_plant', 'real_plant_analyzed', 'virtual_plant', 'virtual_plant_analyzed']
    """
    url = scans_url(host, **kwargs)
    response = make_api_request(url=url, method="GET", session_token=kwargs.get('session_token', None))
    return sorted(response.json())


def request_scans_info(host, **kwargs):
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
    list
        The list of scan information dictionaries.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api import request_scans_info
    >>> scans_info = request_scans_info('localhost', port=5000)
    >>>
    """
    scan_list = request_scan_names_list(host, **kwargs)
    return [make_api_request(url=scan_url(host, scan, **kwargs), session_token=kwargs.get('session_token', None)).json()
            for
            scan in scan_list]


def request_scan_data(host, scan_id, **kwargs):
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

    Returns
    -------
    dict
        The data dictionary for the given scan dataset.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api import request_scan_data
    >>> scan_data = request_scan_data('localhost', 'real_plant', port=5000)
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


def request_scan_image(host, scan_id, fileset_id, file_id, size='orig', **kwargs):
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
    requests.Response
        The URL to an image of a scan dataset and task fileset.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api import request_scan_image
    >>> response = request_scan_image('real_plant', 'images', '00000_rgb', port=5000)  # download the image
    >>> print(response.status_code)
    200
    >>> # Display the image
    >>> from PIL import Image
    >>> from io import BytesIO
    >>> image = Image.open(BytesIO(response.content))  # Open the image from the bytes data
    >>> image.show()  # Display the image
    """
    url = scan_image_url(host, scan_id, fileset_id, file_id, size, **kwargs)
    return make_api_request(url=url, session_token=kwargs.get('session_token', None))


def request_scan_tasks_fileset(host, scan_id, **kwargs):
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

    Returns
    -------
    dict
        The mapping of the task name to fileset name.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api import request_scan_tasks_fileset
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


def request_refresh(host, scan_id=None, **kwargs):
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

    Returns
    -------
    response : requests.Response
        The response object from the refresh request.

    Raises
    ------
    HTTPError
        If the request fails or the response status is not successful.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api import request_refresh
    >>> res = request_refresh('localhost', "arabidopsis000", port = 5000)
    >>> print(res.json()["message"])
    Successfully reloaded scan 'arabidopsis000'
    """
    url = refresh_url(host, scan_id, **kwargs)
    return make_api_request(url, session_token=kwargs.get('session_token', None))


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

    Returns
    -------
    BytesIO or str
        A `BytesIO` object containing the binary content of the downloaded scan archive.
        A path to the downloaded file, if a directory path is specified.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api import request_archive_download
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
    >>> from plantdb.client.rest_api import request_archive_upload
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
    >>> from plantdb.client.rest_api import request_dataset_file_upload
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


def parse_scans_info(host, **kwargs):
    """Parse the information dictionary for all scans served by the PlantDB REST API.

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

    Returns
    -------
    dict
        The scan-id (dataset name) indexed information dictionary.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api import parse_scans_info
    >>> scan_dict = parse_scans_info('localhost', port=5000)
    >>> print(sorted(scan_dict.keys()))
    ['arabidopsis000', 'real_plant', 'real_plant_analyzed', 'virtual_plant', 'virtual_plant_analyzed']
    """
    scan_json = request_scans_info(host, **kwargs)
    scan_dict = {}
    for scan in scan_json:
        name = scan.pop('id')
        scan_dict[name] = scan
    return scan_dict


def parse_task_images(host, scan_id, task_name='images', size='orig', **kwargs):
    """Get the list of images data for a given dataset and task name.

    Parameters
    ----------
    host : str
        The hostname or IP address of the PlantDB REST API server.
    scan_id : str
        The name of the dataset to retrieve the images for.
    task_name : str, optional
        The name of the task to retrieve the images from. Defaults to 'images'.
    size : {'orig', 'large', 'thumb'} or int, optional
        If an integer, use  it as the size of the cached image to create and return.
        Else, should be a string, defaulting to `'orig'`, and it works as follows:
           * `'thumb'`: image max width and height to `150`.
           * `'large'`: image max width and height to `1500`;
           * `'orig'`: original image, no chache;

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
    list of PIL.Image
        The list of PIL.Image from the PlantDB REST API.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api import parse_task_images
    >>> images = parse_task_images('localhost', 'real_plant', port=5000)
    >>> print(len(images))
    60
    >>> img1 = images[0]
    >>> print(img1.size)
    (1440, 1080)
    """
    images = []
    for img_uri in list_task_images_uri(host, scan_id, task_name, size, **kwargs):
        images.append(
            Image.open(BytesIO(make_api_request(url=img_uri, session_token=kwargs.get('session_token', None)).content)))
    return images


def _ply_vertex_to_array(data):
    """Convert the `PlyData` 'vertex' data into an XYZ array of vertex coordinates.

    Parameters
    ----------
    data : PlyData
        The `PlyData` object to be converted as numpy array.

    Returns
    -------
    list
        The XYZ array of vertex coordinates, returned as a list to be JSON serializable.
    """
    return [list(data['vertex']['x']), list(data['vertex']['y']), list(data['vertex']['z'])]


def _ply_face_to_array(data):
    """Convert the `PlyData` 'face' data into an XYZ array of triangle coordinates.

    Parameters
    ----------
    data : PlyData
        The `PlyData` object to be converted as numpy array.

    Returns
    -------
    list
        The XYZ array of triangle coordinates, returned as a list to be JSON serializable.
    """
    return [list(d) for d in data['face'].data['vertex_indices']]


def parse_requests_pcd(data):
    """Parse a requests content, should be from a PointCloud task source.

    Parameters
    ----------
    data : buffer
        The data source from a requests content.

    Returns
    -------
    numpy.ndarray
        The parsed pointcloud with vertex coordinates sorted as XYZ.
    """
    ## Read the pointcloud PLY as a `PlyData`:
    ply_pcd = PlyData.read(BytesIO(data))
    ## Convert the `PlyData`:
    return _ply_vertex_to_array(ply_pcd)


def parse_requests_mesh(data):
    """Parse a requests content, should be from a TriangleMesh task source.

    Parameters
    ----------
    data : buffer
        The data source from a requests content.

    Returns
    -------
    dict
        The parsed triangular mesh with two entries: 'vertices' for vertex coordinates and 'triangles' for triangle coordinates.
    """
    ## Read the PLY as a `PlyData`:
    mesh_data = PlyData.read(BytesIO(data))
    ## Convert the `PlyData`:
    return {"vertices": _ply_vertex_to_array(mesh_data),
            "triangles": _ply_face_to_array(mesh_data)}


def parse_requests_skeleton(data):
    """Parse a requests content, should be from a CurveSkeleton task source.

    Parameters
    ----------
    data : buffer
        The data source from a requests content.

    Returns
    -------
    dict
        The parsed skeleton with two entries: 'points' for points coordinates and 'lines' joining them.
    """
    return json.loads(data)


def parse_requests_tree(data):
    """Parse a requests content, should be from a TreeGraph task source.

    Parameters
    ----------
    data : buffer
        The data source from a requests content.

    Returns
    -------
    networkx.Graph
        The loaded (tree) graph object.
    """
    import pickle
    tree = pickle.load(BytesIO(data))
    # FIXME: it would be better to return something that is JSON serializable...
    #  but the tree is not directed, so the `json_graph.tree_data` fails!
    # from networkx.readwrite import json_graph
    # data = json_graph.tree_data(tree, root=0)
    # return json.dumps(data)
    return tree


def parse_requests_json(data):
    """Parse a requests content, should be from a AnglesAndInternodes task source.

    Parameters
    ----------
    data : buffer
        The data source from a requests content.

    Returns
    -------
    dict
        The full angles and internodes dictionary with 'angles', 'internodes', '' & '' entries.
    """
    return json.loads(data)


PARSER_DICT = {
    "PointCloud": parse_requests_pcd,
    "TriangleMesh": parse_requests_mesh,
    "CurveSkeleton": parse_requests_skeleton,
    "TreeGraph": parse_requests_tree,
    "AnglesAndInternodes": parse_requests_json,
}

EXT_PARSER_DICT = {
    "json": parse_requests_json,
}


def parse_task_requests_data(task, data, extension=None):
    """Parse raw request data for a specified task.

    The function selects an appropriate parser based on the provided
    *extension* (if any) or the *task* name, then applies that parser
    to the raw *data* payload.  This is a small dispatcher that
    centralises the logic for choosing between the generic
    :func:`parse_requests_json` parser and any custom parsers defined
    in :data:`PARSER_DICT` and :data:`EXT_PARSER_DICT`.

    Parameters
    ----------
    task : str
        Identifier for the task whose data is being parsed.  Used as a
        key to look up the default parser in :data:`PARSER_DICT`.
    data : str or bytes
        Raw payload that contains the request data.  The parser returned
        by the dispatcher is expected to accept this type and convert it
        into a Python object (e.g. a dictionary).
    extension : str, optional
        File‑extension or MIME‑type hint.  If supplied, the parser is
        taken from :data:`EXT_PARSER_DICT`; otherwise the default parser
        for *task* is used.

    Returns
    -------
    Any
        The result of the chosen parser applied to *data*.  The exact
        type depends on the parser implementation (commonly a dict).

    Examples
    --------
    >>> # Assume the following parsers are defined
    >>> def parse_json(data): return {"parsed": data}
    >>> PARSER_DICT = {"task1": parse_json}
    >>> EXT_PARSER_DICT = {"txt": parse_json}
    >>> # Example with task-based parser
    >>> result = parse_task_requests_data("task1", '{"key": "value"}')
    >>> print(result)
    {'parsed': '{"key": "value"}'}
    >>> # Example with extension-based parser
    >>> result = parse_task_requests_data("unknown", "raw data", extension="txt")
    >>> print(result)
    {'parsed': 'raw data'}

    Notes
    -----
    - The function does not perform any validation of *data*; it
      delegates all parsing logic to the chosen parser.
    - If *task* is not found in :data:`PARSER_DICT` and *extension* is
      ``None``, the fallback parser :func:`parse_requests_json` is used.

    See Also
    --------
    parse_requests_json : Default JSON parser used when no task matches.
    """
    if extension is not None:
        data_parser = EXT_PARSER_DICT[extension]
    else:
        data_parser = PARSER_DICT.get(task, parse_requests_json)
    return data_parser(data)


task_filesUri_mapping = {
    "PointCloud": "pointCloud",
    "TriangleMesh": "mesh",
    "CurveSkeleton": "skeleton",
    "TreeGraph": "tree",
}


def get_task_data(host, scan_id, task, filename=None, api_data=None, **kwargs):
    """Get the data corresponding to a `dataset/task/filename`.

    Parameters
    ----------
    host : str
        The hostname or IP address of the PlantDB REST API server.
    scan_id : str
        The name of the dataset.
    task : str
        The name of the task.
    filename : str, optional
        The name of the file to load.
        If not specified defaults to the main file returned by the task as defined in `filesUri_task_mapping`.
    api_data : dict, optional
        The dictionary of information for the dataset as returned by the REST API.
        If not specified, fetch it from the REST API.

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
    any
        The parsed data.

    See Also
    --------
    plantdb.client.rest_api.parse_task_requests_data

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> import numpy as np
    >>> from plantdb.client.rest_api import get_task_data
    >>> pcd = get_task_data('real_plant_analyzed', 'PointCloud')
    >>> np.array(pcd).shape
    (3, 57890)
    """
    if api_data is None:
        api_data = request_scan_data(host, scan_id, **kwargs)
    # Get data from `File` resource of REST API:
    ext = None
    if filename is None:
        file_uri = api_data["filesUri"][task_filesUri_mapping[task]]
    else:
        _, ext = Path(filename).suffix.split('.')
        file_uri = api_endpoints.file(scan_id, api_data["tasks_fileset"][task], filename)

    url = origin_url(host, **kwargs)

    data = make_api_request(url + file_uri, session_token=kwargs.get('session_token', None)).content
    return parse_task_requests_data(task, data, ext)


def _load_toml_from_url(url, **kwargs):
    """Load and parse a TOML file from a given URL.

    Parameters
    ----------
    url : str
        The URL to fetch the TOML file from.

    Returns
    -------
    dict or None
        The parsed TOML data as a dictionary, or None if the request fails.
    """
    import toml
    response = make_api_request(url, **kwargs)
    if response.ok:
        return toml.loads(response.content.decode('utf-8'))
    return None


def get_toml_file(host, scan_id, file_path, **kwargs):
    """Return a loaded TOML file for selected dataset, if it exists.

    Parameters
    ----------
    scan_id : str
        The name of the dataset.
    file_path : str
        The path to the TOML file.

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
        The configuration dictionary.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api import get_toml_file
    >>> cfg = get_toml_file('real_plant_analyzed', 'pipeline.toml')
    >>> cfg['PointCloud']
    {'upstream_task': 'Voxels', 'level_set_value': 1.0}
    """
    url = scan_file_url(host, scan_id, file_path, **kwargs)
    return _load_toml_from_url(url, **kwargs)


def get_scan_config(host, scan_id, cfg_fname='scan.toml', **kwargs):
    """Return the scan configuration for selected dataset, if it exists.

    Parameters
    ----------
    scan_id : str
        The name of the dataset.
    cfg_fname : str, optional
        The name of the configuration file.

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
        The configuration dictionary.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api import get_scan_config
    >>> cfg = get_scan_config('real_plant')
    >>> cfg['ScanPath']['class_name']
    'Circle'

    """
    return get_toml_file(host, scan_id, cfg_fname, **kwargs)


def get_reconstruction_config(host, scan_id, cfg_fname='pipeline.toml', **kwargs):
    """Return the reconstruction configuration for selected dataset, if it exists.

    Parameters
    ----------
    scan_id : str
        The name of the dataset.
    cfg_fname : str, optional
        The name of the configuration file.

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
        The configuration dictionary.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api import get_reconstruction_config
    >>> cfg = get_reconstruction_config('real_plant_analyzed')
    >>> cfg['PointCloud']['upstream_task']
    'Voxels'

    """
    return get_toml_file(host, scan_id, cfg_fname, **kwargs)


def get_angles_and_internodes_data(host, scan_id, **kwargs):
    """Return a dictionary with 'angles' and 'internodes' data for selected dataset, if it exists.

    Parameters
    ----------
    host : str
        The hostname or IP address of the PlantDB REST API server.
    scan_id : str
        The name of the dataset.

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
        A dictionary with 'angles' and 'internodes' data.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api import get_angles_and_internodes_data
    >>> data = get_angles_and_internodes_data('real_plant_analyzed')
    >>> print(list(data.keys()))
    ['angles', 'internodes']
    >>> print(len(data['angles']))
    33
    """
    url = origin_url(host, **kwargs)

    response = make_api_request(join_url(url, f"sequence/{scan_id}"),
                                session_token=kwargs.get('session_token', None))
    if response.ok:
        data = json.loads(response.content.decode('utf-8'))
        return {seq: data[seq] for seq in ['angles', 'internodes']}
    else:
        return None
