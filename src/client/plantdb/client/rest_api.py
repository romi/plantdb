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
This module regroup the client-side methods of the REST API.
"""
import json
import logging
import os
from io import BytesIO
from pathlib import Path
from urllib.parse import urljoin
from urllib.parse import urlparse

import requests
from PIL import Image
from plyfile import PlyData

#: Default URL to REST API is 'localhost':
REST_API_URL = "127.0.0.1"
# Default port to REST API:
REST_API_PORT = 5000


def sanitize_name(name):
    """Sanitizes and validates the provided name.

    The function ensures that the input string adheres to predefined naming rules by:

    - stripping leading/trailing spaces,
    - isolating the last segment after splitting by slashes,
    - validating the name against an alphanumeric pattern
      with optional underscores (`_`), dashes (`-`), or periods (`.`).

    Parameters
    ----------
    name : str
        The name to sanitize and validate.

    Returns
    -------
    str
        Sanitized name that conforms to the rules.

    Raises
    ------
    ValueError
        If the provided name contains invalid characters or does not meet the naming rules.
    """
    import re
    sanitized_name = name.strip()  # Remove leading/trailing spaces
    sanitized_name = sanitized_name.split('/')[-1]  # isolate the last segment after splitting by slashes
    # Validate against an alphanumeric pattern with optional underscores, dashes, or periods
    if not re.match(r"^[a-zA-Z0-9_.-]+$", sanitized_name):
        raise ValueError(
            f"Invalid name: '{name}'. Names must be alphanumeric and can include underscores, dashes, or periods.")
    return sanitized_name


def base_url(host=REST_API_URL, port=REST_API_PORT, prefix=None, ssl=False) -> str:
    """
    Generates the URL for the PlantDB REST API using the specified host and port.

    This function constructs a URL by combining the provided host, port, prefix, and SSL settings.
    The default values are used if no arguments are supplied.

    Parameters
    ----------
    host : str, optional
        The hostname or IP address of the PlantDB REST API server. Defaults to ``REST_API_URL``.
    port : int or str, optional
        The port number of the PlantDB REST API server. Defaults to ``REST_API_PORT``.
    prefix : str, optional
        The prefix to be prepended to the URL. If provided, it will be stripped of leading and trailing slashes.
        If provided, it will be added to the end of the URL, excluding the port number.
        Defaults to ``None``.
    ssl : bool, optional
        Flag indicating whether to use HTTPS (True) or HTTP (False). Defaults to ``False``.

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
    >>> from plantdb.client.rest_api import base_url
    >>> base_url()
    'http://127.0.0.1:5000'
    >>> base_url(host='api.example.com', port=8443, ssl=True)
    'https://api.example.com:8443'
    >>> base_url(port=5000, prefix='/plantdb', ssl=True)
    'https://127.0.0.1/plantdb/'
    """
    # Attempt to split the host to check for an existing scheme (http/https)
    try:
        scheme, host = host.split('://')
    except ValueError:
        pass  # If no scheme is found, proceed with the default
    else:
        # If 's' is in the scheme, it indicates HTTPS
        if 's' in scheme:
            ssl = True

    # Format the prefix by stripping leading and trailing slashes and adding a leading slash
    if prefix:
        prefix = '/' + prefix.lstrip('/').rstrip('/') + '/'

    # Ensure port is converted to string and has no leading colon
    if port:
        if isinstance(port, int):
            port = str(port)
        port = ':' + port.lstrip(':')

    # Construct the final URL using f-string for clarity and formatting
    return f"http{'s' if ssl else ''}://{host}{prefix if prefix else port}"


def scans_url(**kwargs):
    """Generates the URL listing the scans from the PlantDB REST API.

    Other Parameters
    ----------------
    host : str, optional
        The hostname or IP address of the PlantDB REST API server. Defaults to ``REST_API_URL``.
    port : int or str, optional
        The port number of the PlantDB REST API server. Defaults to ``REST_API_PORT``.
    prefix : str, optional
        The prefix to be prepended to the URL. If provided, it will be stripped of leading and trailing slashes.
        Defaults to ``None``.
    ssl : bool, optional
        Flag indicating whether to use HTTPS (True) or HTTP (False). Defaults to ``False``.

    Returns
    -------
    str
        A properly formatted URL of the PlantDB REST API pointing to the scans list.

    Examples
    --------
    >>> from plantdb.client.rest_api import scans_url
    >>> scans_url()
    'http://127.0.0.1:5000/scans'
    >>> scans_url(prefix='/plantdb')
    'http://127.0.0.1/plantdb/scans'
    >>> scans_url(host='mellitus.biologie.ens-lyon.fr', port=433, prefix='/plantdb/', ssl=True)
    """
    url = base_url(host=kwargs.get("host", REST_API_URL),
                   port=kwargs.get("port", REST_API_PORT),
                   prefix=kwargs.get('prefix', os.environ.get('PLANTDB_API_PREFIX', None)),
                   ssl=kwargs.get("ssl", False))
    return urljoin(url, "scans")


def configure_requests_with_certificate(cert_path, logger=None):
    """
    Configure the Requests library to use a specific SSL certificate.

    This function sets up the Requests library to use a given SSL certificate by
    modifying the environment variable `REQUESTS_CA_BUNDLE`. It also ensures that
    the specified certificate file exists. If the file does not exist, it raises
    a FileNotFoundError.

    Parameters
    ----------
    cert_path : str
        The path to the SSL certificate file.
    logger : logging.Logger, optional
        A logger instance to use. Defaults to ``None``.

    Raises
    ------
    FileNotFoundError
        If `cert_path` does not point to a valid certificate file.
    """
    if not os.path.exists(cert_path):
        raise FileNotFoundError(f"Certificate file not found: {cert_path}")

    os.environ['REQUESTS_CA_BUNDLE'] = cert_path
    if logger is not None and isinstance(logger, logging.Logger):
        logger.debug(f"Requests configured to use certificate: {cert_path}")


def make_api_request(url, method="GET", params=None, json_data=None,
                     cert_path=None, allow_redirects=True, **kwargs):
    """
    Function to make an API request with various HTTP methods and options.

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
    cert_path : str or None, optional
        Path to a certificate file for SSL verification. If None, default SSL verification is used.
    allow_redirects : bool, optional
        Whether to allow redirects. Default is True.

    Other Parameters
    ----------------
    header : dict, optional
        The HTTP headers to send in the request. Default is None.
    files : dict, optional
        Additional files to send in the request. Default is None.
    data : dict, list, or bytes, optional
        The data to send in the request. Default is None.
    timeout : int, optional
        Timeout to use for the request. Default is 5 seconds.
    stream : bool, optional
        Flag indicating whether to stream the request. Default is False.

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
    # Add a default timeout of 5 seconds if not provided
    if 'timeout' not in kwargs:
        kwargs['timeout'] = 5.0

    try:
        verify = cert_path if cert_path else True

        if method.upper() == "GET":
            response = requests.get(url, params=params, verify=verify, allow_redirects=allow_redirects, **kwargs)
        elif method.upper() == "POST":
            response = requests.post(url, params=params, json=json_data, verify=verify, allow_redirects=allow_redirects, **kwargs)
        elif method.upper() == "PUT":
            response = requests.put(url, params=params, json=json_data, verify=verify, allow_redirects=allow_redirects, **kwargs)
        elif method.upper() == "DELETE":
            response = requests.delete(url, params=params, verify=verify, allow_redirects=allow_redirects, **kwargs)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

        response.raise_for_status()  # Raise exception for 4XX/5XX responses
        return response
    except requests.exceptions.SSLError as e:
        print(f"SSL Error: {e}")
        print("Try providing the specific certificate path with cert_path parameter")
        raise
    except requests.exceptions.RequestException as e:
        print(f"Request Error: {e}")
        raise


def scan_url(scan_id, **kwargs):
    """Generates the URL pointing to the scan JSON from the PlantDB REST API.

    Parameters
    ----------
    scan_id : str
        The name of the scan dataset to retrieve the JSON from.

    Other Parameters
    ----------------
    host : str, optional
        The hostname or IP address of the PlantDB REST API server. Defaults to ``REST_API_URL``.
    port : int or str, optional
        The port number of the PlantDB REST API server. Defaults to ``REST_API_PORT``.
    prefix : str, optional
        The prefix to be prepended to the URL. If provided, it will be stripped of leading and trailing slashes.
        Defaults to ``None``.
    ssl : bool, optional
        Flag indicating whether to use HTTPS (True) or HTTP (False). Defaults to ``False``.

    Returns
    -------
    str
        A properly formatted URL of the PlantDB REST API pointing to the scans list.

    Examples
    --------
    >>> from plantdb.client.rest_api import scan_url
    >>> scan_url("real_plant")
    'http://127.0.0.1:5000/scans/real_plant'
    >>> scan_url("real_plant", prefix='/plantdb')
    'http://127.0.0.1/plantdb/scans/real_plant'
    """
    url = base_url(host=kwargs.get("host", REST_API_URL),
                   port=kwargs.get("port", REST_API_PORT),
                   prefix=kwargs.get('prefix', os.environ.get('PLANTDB_API_PREFIX', None)),
                   ssl=kwargs.get("ssl", False))
    return urljoin(url, f"scans/{sanitize_name(scan_id)}")


def scan_image_url(scan_id, fileset_id, file_id, size='orig', **kwargs):
    """Get the URL to the image for a scan dataset and task fileset served by the PlantDB REST API.

    Parameters
    ----------
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
    host : str, optional
        The hostname or IP address of the PlantDB REST API server. Defaults to ``REST_API_URL``.
    port : int or str, optional
        The port number of the PlantDB REST API server. Defaults to ``REST_API_PORT``.
    prefix : str, optional
        The prefix to be prepended to the URL. If provided, it will be stripped of leading and trailing slashes.
        Defaults to ``None``.
    ssl : bool, optional
        Flag indicating whether to use HTTPS (True) or HTTP (False). Defaults to ``False``.

    Returns
    -------
    str
        The URL to an image of a scan dataset and task fileset.

    Examples
    --------
    >>> from plantdb.client.rest_api import scan_image_url
    >>> scan_image_url("real_plant", "images", "00000_rgb")
    'http://127.0.0.1:5000/image/real_plant/images/00000_rgb?size=orig'
    >>> scan_image_url("real_plant", "images", "00000_rgb", prefix='/plantdb')
    'http://127.0.0.1/plantdb/image/real_plant/images/00000_rgb?size=orig'
    """
    scan_id = sanitize_name(scan_id)
    fileset_id = sanitize_name(fileset_id)
    file_id = sanitize_name(file_id)
    url = base_url(host=kwargs.get("host", REST_API_URL),
                   port=kwargs.get("port", REST_API_PORT),
                   prefix=kwargs.get('prefix', os.environ.get('PLANTDB_API_PREFIX', None)),
                   ssl=kwargs.get("ssl", False))
    return urljoin(url, f"image/{scan_id}/{fileset_id}/{file_id}?size={size}")


def refresh_url(dataset_name=None, **kwargs):
    """Generates a formatted URL for refreshing a specific dataset or the entire database.

    Parameters
    ----------
    dataset_name : str or None, optional
        The name of the dataset for which the refresh URL needs to be generated.
        If not provided, the refresh URL for the entire server is returned instead.
        Defaults to ``None``.

    Other Parameters
    ----------------
    host : str, optional
        The hostname or IP address of the PlantDB REST API server. Defaults to ``REST_API_URL``.
    port : int or str, optional
        The port number of the PlantDB REST API server. Defaults to ``REST_API_PORT``.
    prefix : str, optional
        The prefix to be prepended to the URL. If provided, it will be stripped of leading and trailing slashes.
        Defaults to ``None``.
    ssl : bool, optional
        Flag indicating whether to use HTTPS (True) or HTTP (False). Defaults to ``False``.

    Returns
    -------
    str
        A correctly formatted URL for refreshing the specified dataset or the entire PlantDB REST API server.

    Examples
    --------
    >>> from plantdb.client.rest_api import refresh_url
    >>> refresh_url("real_plant")
    'http://127.0.0.1:5000/refresh?scan_id=real_plant'
    >>> refresh_url("real_plant", prefix='/plantdb')
    'http://127.0.0.1/plantdb/refresh?scan_id=real_plant'
    """
    url = base_url(host=kwargs.get("host", REST_API_URL),
                   port=kwargs.get("port", REST_API_PORT),
                   prefix=kwargs.get('prefix', os.environ.get('PLANTDB_API_PREFIX', None)),
                   ssl=kwargs.get("ssl", False))
    url = urljoin(url, "refresh")
    if dataset_name is None:
        return url
    else:
        dataset_name = sanitize_name(dataset_name)
        return f"{url}?scan_id={dataset_name}"


def archive_url(dataset_name, **kwargs):
    """Generates a formatted URL for accessing the archive of a specific dataset.

    Parameters
    ----------
    dataset_name : str
        Name of the dataset to access in the archive.

    Other Parameters
    ----------------
    host : str, optional
        The hostname or IP address of the PlantDB REST API server. Defaults to ``REST_API_URL``.
    port : int or str, optional
        The port number of the PlantDB REST API server. Defaults to ``REST_API_PORT``.
    prefix : str, optional
        The prefix to be prepended to the URL. If provided, it will be stripped of leading and trailing slashes.
        Defaults to ``None``.
    ssl : bool, optional
        Flag indicating whether to use HTTPS (True) or HTTP (False). Defaults to ``False``.

    Returns
    -------
    str
        Fully constructed URL for accessing the specified dataset archive.

    Examples
    --------
    >>> from plantdb.client.rest_api import archive_url
    >>> archive_url('arabidopsis000')
    'http://127.0.0.1:5000/archive/arabidopsis000'
    >>> archive_url('../arabidopsis000')
    'http://127.0.0.1:5000/archive/arabidopsis000'
    >>> archive_url('arabidopsis+000')
    ValueError: Invalid dataset name: 'arabidopsis+000'. Dataset names must be alphanumeric and can include underscores or dashes.
    >>> archive_url('arabidopsis000', prefix='/plantdb')
    'http://127.0.0.1/plantdb/archive/arabidopsis000'
    """
    dataset_name = sanitize_name(dataset_name)
    url = base_url(host=kwargs.get("host", REST_API_URL),
                   port=kwargs.get("port", REST_API_PORT),
                   prefix=kwargs.get('prefix', os.environ.get('PLANTDB_API_PREFIX', None)),
                   ssl=kwargs.get("ssl", False))
    url = urljoin(url, f"archive/{dataset_name}"
                  )
    return url


def get_file_uri(scan, fileset, file):
    """Return the URI for the corresponding `scan/fileset/file` tree.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.Scan or str
        A ``Scan`` instance or the name of the scan dataset.
    fileset : plantdb.commons.fsdb.Fileset or str
        A ``Fileset`` instance or the name of the fileset.
    file : plantdb.commons.fsdb.File or str
        A ``File`` instance or the name of the file.

    Returns
    -------
    str
        The URI for the corresponding `scan/fileset/file` tree.

    Examples
    --------
    >>> from plantdb.client.rest_api import get_file_uri
    >>> get_file_uri('real_plant', 'images', '00000_rgb')
    'files/real_plant/images/00000_rgb'
    """
    from plantdb.commons.fsdb import Scan
    from plantdb.commons.fsdb import Fileset
    from plantdb.commons.fsdb import File
    scan_id = scan.id if isinstance(scan, Scan) else scan
    fileset_id = fileset.id if isinstance(fileset, Fileset) else fileset
    file_id = file.path().name if isinstance(file, File) else file
    return f"files/{scan_id}/{fileset_id}/{file_id}"


def test_availability(url):
    """Verifies the connectivity to a given host and port from a URL-like string.
    This function parses a URL string into host and port components, attempts to establish a
    socket connection to check its availability, and raises appropriate exceptions on failure.
    Parameters
    ----------
    url : str
        A URL string in various formats like 'http://host:port', 'https://host/path/', etc.
    Raises
    ------
    ValueError
        If the input URL is not a valid URL format.
    ConnectionError
        If the specified host cannot be connected, indicating that the server might
        be unavailable.
    RuntimeError
        If an unexpected error occurs during the verification process.
    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api import test_availability
    >>> test_availability('http://127.0.0.1:5000')
    >>> test_availability('https://127.0.0.1/plantdb/')
    >>> test_availability('https://mellitus.biologie.ens-lyon.fr/plantdb/')
    """
    import socket
    try:
        parsed_url = urlparse(url)

        # Validate URL has at least a scheme and hostname
        if not parsed_url.scheme or not parsed_url.netloc:
            raise ValueError(f"Invalid URL format: '{url}'. Must include scheme (http/https) and hostname.")

        host = parsed_url.hostname
        # Use default port based on scheme if not specified
        port = parsed_url.port
        if port is None:
            port = 443 if parsed_url.scheme == 'https' else 80

        socket.setdefaulttimeout(2)  # Set a timeout for the connection check
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex((host, port)) != 0:
                raise ConnectionError(f"Cannot connect to {host}:{port}. Server might be unavailable.")
    except ValueError as e:
        raise ValueError(f"{e}")
    except ConnectionError as e:
        raise e
    except Exception as e:
        raise RuntimeError(f"Unexpected error during connection check: {e}")


def list_scan_names(**kwargs):
    """List the names of the scan datasets served by the PlantDB REST API.

    Other Parameters
    ----------------
    host : str, optional
        The hostname or IP address of the PlantDB REST API server. Defaults to ``REST_API_URL``.
    port : int or str, optional
        The port number of the PlantDB REST API server. Defaults to ``REST_API_PORT``.
    prefix : str, optional
        The prefix to be prepended to the URL. If provided, it will be stripped of leading and trailing slashes.
        Defaults to ``None``.
    ssl : bool, optional
        Flag indicating whether to use HTTPS (True) or HTTP (False). Defaults to ``False``.

    Returns
    -------
    list
        The list of scan dataset names served by the PlantDB REST API.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api import list_scan_names
    >>> print(list_scan_names())
    ['arabidopsis000', 'real_plant', 'real_plant_analyzed', 'virtual_plant', 'virtual_plant_analyzed']
    >>> print(list_scan_names(host='mellitus.biologie.ens-lyon.fr', port=433, prefix='/plantdb/', ssl=True))
    """
    url = scans_url(**kwargs)
    response = make_api_request(url=url, cert_path=kwargs.get('cert_path', None))
    return sorted(response.json())


def get_scans_info(**kwargs):
    """Retrieve the information dictionary for all scans from the PlantDB REST API.

    Other Parameters
    ----------------
    host : str, optional
        The hostname or IP address of the PlantDB REST API server. Defaults to ``REST_API_URL``.
    port : int or str, optional
        The port number of the PlantDB REST API server. Defaults to ``REST_API_PORT``.
    prefix : str, optional
        The prefix to be prepended to the URL. If provided, it will be stripped of leading and trailing slashes.
        Defaults to ``None``.
    ssl : bool, optional
        Flag indicating whether to use HTTPS (True) or HTTP (False). Defaults to ``False``.

    Returns
    -------
    dict
        The scans information dictionary.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api import get_scans_info
    >>> get_scans_info()
    """
    scan_list = list_scan_names(**kwargs)
    return [make_api_request(url=scan_url(scan, **kwargs), cert_path=kwargs.get('cert_path', None)).json() for scan in
            scan_list]


def parse_scans_info(**kwargs):
    """Parse the information dictionary for all scans served by the PlantDB REST API.

    Other Parameters
    ----------------
    host : str, optional
        The hostname or IP address of the PlantDB REST API server. Defaults to ``REST_API_URL``.
    port : int or str, optional
        The port number of the PlantDB REST API server. Defaults to ``REST_API_PORT``.
    prefix : str, optional
        The prefix to be prepended to the URL. If provided, it will be stripped of leading and trailing slashes.
        Defaults to ``None``.
    ssl : bool, optional
        Flag indicating whether to use HTTPS (True) or HTTP (False). Defaults to ``False``.

    Returns
    -------
    dict
        The scan-id (dataset name) indexed information dictionary.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api import parse_scans_info
    >>> scan_dict = parse_scans_info()
    >>> print(sorted(scan_dict.keys()))
    ['arabidopsis000', 'real_plant', 'real_plant_analyzed', 'virtual_plant', 'virtual_plant_analyzed']
    """
    scan_json = get_scans_info(**kwargs)
    scan_dict = {}
    for scan in scan_json:
        name = scan.pop('id')
        scan_dict[name] = scan
    return scan_dict


def get_scan_data(scan_id, **kwargs):
    """Retrieve the data dictionary for a given scan dataset from the PlantDB REST API.

    Parameters
    ----------
    scan_id : str
        The name of the scan dataset to be retrieved.

    Other Parameters
    ----------------
    host : str, optional
        The hostname or IP address of the PlantDB REST API server. Defaults to ``REST_API_URL``.
    port : int or str, optional
        The port number of the PlantDB REST API server. Defaults to ``REST_API_PORT``.
    prefix : str, optional
        The prefix to be prepended to the URL. If provided, it will be stripped of leading and trailing slashes.
        Defaults to ``None``.
    ssl : bool, optional
        Flag indicating whether to use HTTPS (True) or HTTP (False). Defaults to ``False``.

    Returns
    -------
    dict
        The data dictionary for the given scan dataset.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api import get_scan_data
    >>> scan_data = get_scan_data('real_plant')
    >>> print(scan_data['id'])
    real_plant
    >>> print(scan_data['hasColmap'])
    False
    """
    scan_id = sanitize_name(scan_id)
    scan_names = list_scan_names(**kwargs)
    if scan_id in scan_names:
        url = scan_url(scan_id, **kwargs)
        return make_api_request(url=url, cert_path=kwargs.get('cert_path', None)).json()
    else:
        return {}


def scan_preview_image_url(scan_id, size="thumb", **kwargs):
    """Get the URL to the preview image for a scan dataset served by the PlantDB REST API.

    Parameters
    ----------
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
    host : str, optional
        The hostname or IP address of the PlantDB REST API server. Defaults to ``REST_API_URL``.
    port : int or str, optional
        The port number of the PlantDB REST API server. Defaults to ``REST_API_PORT``.
    prefix : str, optional
        The prefix to be prepended to the URL. If provided, it will be stripped of leading and trailing slashes.
        Defaults to ``None``.
    ssl : bool, optional
        Flag indicating whether to use HTTPS (True) or HTTP (False). Defaults to ``False``.

    Returns
    -------
    str
        The URL to the preview image for a scan dataset.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api import scan_preview_image_url
    >>> img_url = scan_preview_image_url('real_plant')
    >>> print(img_url)
    http://127.0.0.1:5000/image/real_plant/images/00000_rgb?size=thumb
    >>> img_url = scan_preview_image_url('real_plant', size=100)
    >>> print(img_url)
    http://127.0.0.1:5000/image/real_plant/images/00000_rgb?size=100
    >>> # Download and display the image
    >>> import requests
    >>> from PIL import Image
    >>> from io import BytesIO
    >>> response = requests.get(img_url)  # Send a GET request to the URL
    >>> image = Image.open(BytesIO(response.content))  # Open the image from the bytes data
    >>> image.show()  # Display the image
    """
    scan_id = sanitize_name(scan_id)
    scan_names = list_scan_names(**kwargs)
    if scan_id not in scan_names:
        return None

    thumb_uri = get_scan_data(scan_id)["thumbnailUri"]
    if size != "thumb":
        thumb_uri = thumb_uri.replace("size=thumb", f"size={size}")
    url = base_url(host=kwargs.get("host", REST_API_URL),
                   port=kwargs.get("port", REST_API_PORT),
                   prefix=kwargs.get('prefix', os.environ.get('PLANTDB_API_PREFIX', None)),
                   ssl=kwargs.get("ssl", False))
    return urljoin(url, thumb_uri.lstrip("/"))


def get_scan_image(scan_id, fileset_id, file_id, size='orig', **kwargs):
    """Get the image for a scan dataset and task fileset served by the PlantDB REST API.

    Parameters
    ----------
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
    host : str, optional
        The hostname or IP address of the PlantDB REST API server. Defaults to ``REST_API_URL``.
    port : int or str, optional
        The port number of the PlantDB REST API server. Defaults to ``REST_API_PORT``.
    prefix : str, optional
        The prefix to be prepended to the URL. If provided, it will be stripped of leading and trailing slashes.
        Defaults to ``None``.
    ssl : bool, optional
        Flag indicating whether to use HTTPS (True) or HTTP (False). Defaults to ``False``.

    Returns
    -------
    requests.Response
        The URL to an image of a scan dataset and task fileset.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api import get_scan_image
    >>> response = get_scan_image('real_plant', 'images', '00000_rgb')  # download the image
    >>> print(response.status_code)
    200
    >>> # Display the image
    >>> from PIL import Image
    >>> from io import BytesIO
    >>> image = Image.open(BytesIO(response.content))  # Open the image from the bytes data
    >>> image.show()  # Display the image
    """
    url = scan_image_url(scan_id, fileset_id, file_id, size, **kwargs)
    return make_api_request(url=url, cert_path=kwargs.get('cert_path', None))


def get_tasks_fileset_from_api(dataset_name, **kwargs):
    """Get the task name to fileset name mapping dictionary from the REST API.

    Parameters
    ----------
    dataset_name : str
        The name of the dataset to retrieve the mapping for.

    Other Parameters
    ----------------
    host : str, optional
        The hostname or IP address of the PlantDB REST API server. Defaults to ``REST_API_URL``.
    port : int or str, optional
        The port number of the PlantDB REST API server. Defaults to ``REST_API_PORT``.
    prefix : str, optional
        The prefix to be prepended to the URL. If provided, it will be stripped of leading and trailing slashes.
        Defaults to ``None``.
    ssl : bool, optional
        Flag indicating whether to use HTTPS (True) or HTTP (False). Defaults to ``False``.

    Returns
    -------
    dict
        The mapping of the task name to fileset name.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api import get_tasks_fileset_from_api
    >>> get_tasks_fileset_from_api('real_plant')
    {'images': 'images'}
    """
    return get_scan_data(dataset_name, **kwargs).get('tasks_fileset', dict())


def list_task_images_uri(dataset_name, task_name='images', size='orig', **kwargs):
    """Get the list of images URI for a given dataset and task name.

    Parameters
    ----------
    dataset_name : str
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
    host : str, optional
        The hostname or IP address of the PlantDB REST API server. Defaults to ``REST_API_URL``.
    port : int or str, optional
        The port number of the PlantDB REST API server. Defaults to ``REST_API_PORT``.
    prefix : str, optional
        The prefix to be prepended to the URL. If provided, it will be stripped of leading and trailing slashes.
        Defaults to ``None``.
    ssl : bool, optional
        Flag indicating whether to use HTTPS (True) or HTTP (False). Defaults to ``False``.

    Returns
    -------
    list of str
        The list of image URI strings for the PlantDB REST API.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api import list_task_images_uri
    >>> print(list_task_images_uri('real_plant')[2])
    http://127.0.0.1:5000/image/real_plant/images/00002_rgb?size=orig
    >>> print(list_task_images_uri('real_plant', size=100)[2])
    http://127.0.0.1:5000/image/real_plant/images/00002_rgb?size=100
    """
    dataset_name = sanitize_name(dataset_name)
    task_name = sanitize_name(task_name)
    scan_info = get_scan_data(dataset_name, **kwargs)
    tasks_fileset = scan_info["tasks_fileset"]
    images = scan_info["images"]
    url = base_url(host=kwargs.get("host", REST_API_URL),
                   port=kwargs.get("port", REST_API_PORT),
                   prefix=kwargs.get('prefix', os.environ.get('PLANTDB_API_PREFIX', None)),
                   ssl=kwargs.get("ssl", False))
    return [urljoin(url, f"image/{dataset_name}/{tasks_fileset[task_name]}/{Path(img).stem}?size={size}") for img in
            images]


def get_images_from_task(dataset_name, task_name='images', size='orig', **kwargs):
    """Get the list of images data for a given dataset and task name.

    Parameters
    ----------
    dataset_name : str
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
    host : str, optional
        The hostname or IP address of the PlantDB REST API server. Defaults to ``REST_API_URL``.
    port : int or str, optional
        The port number of the PlantDB REST API server. Defaults to ``REST_API_PORT``.
    prefix : str, optional
        The prefix to be prepended to the URL. If provided, it will be stripped of leading and trailing slashes.
        Defaults to ``None``.
    ssl : bool, optional
        Flag indicating whether to use HTTPS (True) or HTTP (False). Defaults to ``False``.

    Returns
    -------
    list of PIL.Image
        The list of PIL.Image from the PlantDB REST API.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api import get_images_from_task
    >>> images = get_images_from_task('real_plant')
    >>> print(len(images))
    60
    >>> img1 = images[0]
    >>> print(img1.size)
    (1440, 1080)
    """
    images = []
    for img_uri in list_task_images_uri(dataset_name, task_name, size, **kwargs):
        images.append(Image.open(BytesIO(make_api_request(url=img_uri, cert_path=kwargs.get('cert_path', None)).content)))
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
    """The task data parser, behave according to the source and default to JSON parser."""
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


def get_task_data(dataset_name, task, filename=None, api_data=None, **kwargs):
    """Get the data corresponding to a `dataset/task/filename`.

    Parameters
    ----------
    dataset_name : str
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
    host : str, optional
        The hostname or IP address of the PlantDB REST API server. Defaults to ``REST_API_URL``.
    port : int or str, optional
        The port number of the PlantDB REST API server. Defaults to ``REST_API_PORT``.
    prefix : str, optional
        The prefix to be prepended to the URL. If provided, it will be stripped of leading and trailing slashes.
        Defaults to ``None``.
    ssl : bool, optional
        Flag indicating whether to use HTTPS (True) or HTTP (False). Defaults to ``False``.

    Returns
    -------
    any
        The parsed data.

    See Also
    --------
    plantdb.server.rest_api.filesUri_task_mapping
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
        api_data = get_scan_data(dataset_name, **kwargs)
    # Get data from `File` resource of REST API:
    ext = None
    if filename is None:
        file_uri = api_data["filesUri"][task_filesUri_mapping[task]]
    else:
        _, ext = Path(filename).suffix.split('.')
        file_uri = get_file_uri(dataset_name, api_data["tasks_fileset"][task], filename)

    url = base_url(host=kwargs.get("host", REST_API_URL),
                   port=kwargs.get("port", REST_API_PORT),
                   prefix=kwargs.get('prefix', os.environ.get('PLANTDB_API_PREFIX', None)),
                   ssl=kwargs.get("ssl", False))
    data = make_api_request(url + file_uri, cert_path=kwargs.get('cert_path', None)).content
    return parse_task_requests_data(task, data, ext)


def scan_file_url(dataset_name, file_path, **kwargs):
    """Build the URL for accessing a dataset file.

    Parameters
    ----------
    dataset_name : str
        The name of the dataset.
    file_path : str
        The path to the file.
    **kwargs
        Keyword arguments passed to base_url().

    Returns
    -------
    str
        The complete URL for the dataset file.
    """
    url = base_url(
        host=kwargs.get("host", REST_API_URL),
        port=kwargs.get("port", REST_API_PORT),
        prefix=kwargs.get('prefix', os.environ.get('PLANTDB_API_PREFIX', None)),
        ssl=kwargs.get("ssl", False)
    )
    return urljoin(url, f"files/{dataset_name}/{file_path}")


def scan_config_url(dataset_name, cfg_fname='scan.toml', **kwargs):
    """Return the scan URL to access the scanning configuration file.

    Parameters
    ----------
    dataset_name : str
        The name of the dataset.
    cfg_fname : str, optional
        The name of the TOML scan file, defaults to ``'scan.toml'``.

    Other Parameters
    ----------------
    host : str, optional
        The hostname or IP address of the PlantDB REST API server. Defaults to ``REST_API_URL``.
    port : int or str, optional
        The port number of the PlantDB REST API server. Defaults to ``REST_API_PORT``.
    prefix : str, optional
        The prefix to be prepended to the URL. If provided, it will be stripped of leading and trailing slashes.
        Defaults to ``None``.
    ssl : bool, optional
        Flag indicating whether to use HTTPS (True) or HTTP (False). Defaults to ``False``.

    Returns
    -------
    str
        The URL to the scanning configuration file.

    Examples
    --------
    >>> from plantdb.client.rest_api import scan_config_url
    >>> scan_config_url('real_plant')
    'http://127.0.0.1:5000/files/real_plant/scan.toml'
    >>> scan_config_url('real_plant', prefix='/plantdb')
    'http://127.0.0.1/plantdb/files/real_plant/scan.toml'
    """
    return scan_file_url(dataset_name, cfg_fname, **kwargs)


def scan_reconstruction_url(dataset_name, cfg_fname='pipeline.toml', **kwargs):
    """Return the scan URL to access the reconstruction configuration file.

    Parameters
    ----------
    dataset_name : str
        The name of the dataset.
    cfg_fname : str, optional
        The name of the TOML scan file, defaults to ``'pipeline.toml'``.

    Other Parameters
    ----------------
    host : str, optional
        The hostname or IP address of the PlantDB REST API server. Defaults to ``REST_API_URL``.
    port : int or str, optional
        The port number of the PlantDB REST API server. Defaults to ``REST_API_PORT``.
    prefix : str, optional
        The prefix to be prepended to the URL. If provided, it will be stripped of leading and trailing slashes.
        Defaults to ``None``.
    ssl : bool, optional
        Flag indicating whether to use HTTPS (True) or HTTP (False). Defaults to ``False``.

    Returns
    -------
    str
        The URL to the reconstruction configuration file.

    Examples
    --------
    >>> from plantdb.client.rest_api import scan_reconstruction_url
    >>> scan_reconstruction_url('real_plant')
    'http://127.0.0.1:5000/files/real_plant/pipeline.toml'
    >>> scan_reconstruction_url('real_plant', prefix='/plantdb')
    'http://127.0.0.1/plantdb/files/real_plant/pipeline.toml'
    """
    return scan_file_url(dataset_name, cfg_fname, **kwargs)


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
    response = make_api_request(url, cert_path=kwargs.get('cert_path', None))
    if response.ok:
        return toml.loads(response.content.decode('utf-8'))
    return None


def get_toml_file(dataset_name, file_path, **kwargs):
    """Return a loaded TOML file for selected dataset, if it exists.

    Parameters
    ----------
    dataset_name : str
        The name of the dataset.
    file_path : str
        The path to the TOML file.
    Other Parameters
    ----------------
    host : str, optional
        The hostname or IP address of the PlantDB REST API server. Defaults to ``REST_API_URL``.
    port : int or str, optional
        The port number of the PlantDB REST API server. Defaults to ``REST_API_PORT``.
    prefix : str, optional
        The prefix to be prepended to the URL. If provided, it will be stripped of leading and trailing slashes.
        Defaults to ``None``.
    ssl : bool, optional
        Flag indicating whether to use HTTPS (True) or HTTP (False). Defaults to ``False``.

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
    url = scan_file_url(dataset_name, file_path, **kwargs)
    return _load_toml_from_url(url, **kwargs)


def get_scan_config(dataset_name, cfg_fname='scan.toml', **kwargs):
    """Return the scan configuration for selected dataset, if it exists.

    Parameters
    ----------
    dataset_name : str
        The name of the dataset.
    cfg_fname : str, optional
        The name of the configuration file.
    Other Parameters
    ----------------
    host : str, optional
        The hostname or IP address of the PlantDB REST API server. Defaults to ``REST_API_URL``.
    port : int or str, optional
        The port number of the PlantDB REST API server. Defaults to ``REST_API_PORT``.
    prefix : str, optional
        The prefix to be prepended to the URL. If provided, it will be stripped of leading and trailing slashes.
        Defaults to ``None``.
    ssl : bool, optional
        Flag indicating whether to use HTTPS (True) or HTTP (False). Defaults to ``False``.

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
    return get_toml_file(dataset_name, cfg_fname, **kwargs)


def get_reconstruction_config(dataset_name, cfg_fname='pipeline.toml', **kwargs):
    """Return the reconstruction configuration for selected dataset, if it exists.

    Parameters
    ----------
    dataset_name : str
        The name of the dataset.
    cfg_fname : str, optional
        The name of the configuration file.
    Other Parameters
    ----------------
    host : str, optional
        The hostname or IP address of the PlantDB REST API server. Defaults to ``REST_API_URL``.
    port : int or str, optional
        The port number of the PlantDB REST API server. Defaults to ``REST_API_PORT``.
    prefix : str, optional
        The prefix to be prepended to the URL. If provided, it will be stripped of leading and trailing slashes.
        Defaults to ``None``.
    ssl : bool, optional
        Flag indicating whether to use HTTPS (True) or HTTP (False). Defaults to ``False``.

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
    return get_toml_file(dataset_name, cfg_fname, **kwargs)


def get_angles_and_internodes_data(dataset_name, **kwargs):
    """Return a dictionary with 'angles' and 'internodes' data for selected dataset, if it exists.

    Parameters
    ----------
    dataset_name : str
        The name of the dataset.

    Other Parameters
    ----------------
    host : str, optional
        The hostname or IP address of the PlantDB REST API server. Defaults to ``REST_API_URL``.
    port : int or str, optional
        The port number of the PlantDB REST API server. Defaults to ``REST_API_PORT``.
    prefix : str, optional
        The prefix to be prepended to the URL. If provided, it will be stripped of leading and trailing slashes.
        Defaults to ``None``.
    ssl : bool, optional
        Flag indicating whether to use HTTPS (True) or HTTP (False). Defaults to ``False``.

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
    url = base_url(host=kwargs.get("host", REST_API_URL),
                   port=kwargs.get("port", REST_API_PORT),
                   prefix=kwargs.get('prefix', os.environ.get('PLANTDB_API_PREFIX', None)),
                   ssl=kwargs.get("ssl", False))
    response = make_api_request(urljoin(url, f"sequence/{dataset_name}"), cert_path=kwargs.get('cert_path', None))
    if response.ok:
        data = json.loads(response.content.decode('utf-8'))
        return {seq: data[seq] for seq in ['angles', 'internodes']}
    else:
        return None


def upload_dataset_file(scan_id, file_path, chunk_size=0, **kwargs):
    """Uploads a file to the server using the DatasetFile POST endpoint.

    Parameters
    ----------
    server_url : str
        The base URL of the server hosting the REST API.
    scan_id : str
        The unique identifier of the scan associated with the file upload.
    file_path : str
        The path to the file to be uploaded.
    chunk_size : int, optional
        The size of chunks (in bytes) to read and send, by default 0 (no chunking).

    Other Parameters
    ----------------
    host : str, optional
        The hostname or IP address of the PlantDB REST API server. Defaults to ``REST_API_URL``.
    port : int or str, optional
        The port number of the PlantDB REST API server. Defaults to ``REST_API_PORT``.
    prefix : str, optional
        The prefix to be prepended to the URL. If provided, it will be stripped of leading and trailing slashes.
        Defaults to ``None``.
    ssl : bool, optional
        Flag indicating whether to use HTTPS (True) or HTTP (False). Defaults to ``False``.

    Returns
    -------
    dict
        A dictionary containing the server's response.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api import upload_dataset_file
    >>> upload_dataset_file('arabidopsis000', '/path/to/local/file.txt')
    """
    from os.path import basename
    from os.path import getsize
    # Prepare the URL and headers
    scan_id = sanitize_name(scan_id)
    url = base_url(host=kwargs.get("host", REST_API_URL),
                   port=kwargs.get("port", REST_API_PORT),
                   prefix=kwargs.get('prefix', os.environ.get('PLANTDB_API_PREFIX', None)),
                   ssl=kwargs.get("ssl", False))
    url = urljoin(url, f"files/{scan_id}")

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
                        cert_path=kwargs.get('cert_path', None)
                    )
                    bytes_sent += len(chunk)
                    # Check if the request was successful
                    if response.status_code not in (200, 201):
                        return {"error": "File upload failed", "status_code": response.status_code,
                                "response": response.json()}
            else:
                # Upload the entire file
                response = make_api_request(url, method='POST', headers=headers, data=f, cert_path=kwargs.get('cert_path', None))

        # Return the server's response
        if response.status_code in (200, 201):
            return response.json()
        else:
            return {"error": "File upload failed", "status_code": response.status_code, "response": response.json()}
    except Exception as e:
        return {"error": str(e)}


def refresh(dataset_name=None, **kwargs):
    """Refreshes the database, potentialy only for a specified dataset.

    Parameters
    ----------
    dataset_name : str or None
        The name of the dataset to trigger a refresh.
        If ``None``, the entire database is refreshed.

    Other Parameters
    ----------------
    host : str, optional
        The hostname or IP address of the PlantDB REST API server. Defaults to ``REST_API_URL``.
    port : int or str, optional
        The port number of the PlantDB REST API server. Defaults to ``REST_API_PORT``.
    prefix : str, optional
        The prefix to be prepended to the URL. If provided, it will be stripped of leading and trailing slashes.
        Defaults to ``None``.
    ssl : bool, optional
        Flag indicating whether to use HTTPS (True) or HTTP (False). Defaults to ``False``.
    timeout : int, optional
        A timeout, in seconds, to succeed the refresh request. Defaults to ``5``.

    Returns
    -------
    dict
        Parsed JSON response from the refresh API if the request is successful.

    Raises
    ------
    HTTPError
        If the request fails or the response status is not successful.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api import refresh
    >>> refresh("arabidopsis000")
    {'message': "Successfully reloaded scan 'arabidopsis000'."}
    """
    url = refresh_url(dataset_name, **kwargs)
    response = make_api_request(url, cert_path=kwargs.get('cert_path', None))
    if response.ok:
        return response.json()
    else:
        response.raise_for_status()  # Raise an error if the request failed


def download_scan_archive(dataset_name, out_dir=None, **kwargs):
    """Downloads a scan archive file from a defined dataset based on the specified API parameters.

    This function fetches a scan archive in stream mode from a remote API. The archive
    is expected to be in the form of a binary content stream. The success of the
    operation is determined by the HTTP response received from the API.

    Parameters
    ----------
    dataset_name : str
        The name of the dataset from which the scan archive file is to be downloaded.
    out_dir : str or pathlib.Path, optional
        A path to the directory where to save the archive.

    Other Parameters
    ----------------
    host : str, optional
        The hostname or IP address of the PlantDB REST API server. Defaults to ``REST_API_URL``.
    port : int or str, optional
        The port number of the PlantDB REST API server. Defaults to ``REST_API_PORT``.
    prefix : str, optional
        The prefix to be prepended to the URL. If provided, it will be stripped of leading and trailing slashes.
        Defaults to ``None``.
    ssl : bool, optional
        Flag indicating whether to use HTTPS (True) or HTTP (False). Defaults to ``False``.
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
    >>> from plantdb.client.rest_api import download_scan_archive
    >>> download_scan_archive("arabidopsis000", out_dir='/tmp')
    ('/tmp/arabidopsis000.zip', 'Download completed in 0.05 seconds.')
    """
    import time
    # Construct API URL for archive download using dataset name and optional parameters
    url = archive_url(dataset_name, **kwargs)

    # Track download duration for performance monitoring
    start_time = time.time()  # Start timing
    # Make streaming API request with configurable timeout and optional certificate
    response = make_api_request(url, stream=True,
                                timeout=kwargs.get("timeout", 10),
                                cert_path=kwargs.get('cert_path', None))
    end_time = time.time()  # End timing
    duration = end_time - start_time
    msg = f"Download completed in {duration:.2f} seconds."

    if response.ok:
        if out_dir is not None:
            # Save archive to specified directory with dataset name as filename
            out_dir = Path(out_dir) / f"{dataset_name}.zip"
            with open(out_dir, "wb") as archive_file:
                archive_file.write(response.content)
            return f"{out_dir}", msg
        else:
            # Return archive content in memory if no output directory specified
            return BytesIO(response.content), msg
    else:
        response.raise_for_status()  # Raise an error if the request failed


def upload_scan_archive(dataset_name, path, **kwargs):
    """Upload a scan archive file to a specified dataset on a server.

    This function sends a POST request to upload a scan archive file to a
    particular dataset, utilizing the archive URL and optionally specified
    additional API-related request parameters. Ensures proper handling of
    file opening/closing procedures and response status checks.

    Parameters
    ----------
    dataset_name : str
        The name of the target dataset for the archive upload.
    path : str, pathlib.Path
        The local file system path to the archive to be uploaded.

    Other Parameters
    ----------------
    host : str, optional
        The hostname or IP address of the PlantDB REST API server. Defaults to ``REST_API_URL``.
    port : int or str, optional
        The port number of the PlantDB REST API server. Defaults to ``REST_API_PORT``.
    prefix : str, optional
        The prefix to be prepended to the URL. If provided, it will be stripped of leading and trailing slashes.
        Defaults to ``None``.
    ssl : bool, optional
        Flag indicating whether to use HTTPS (True) or HTTP (False). Defaults to ``False``.
    timeout : int, optional
        A timeout, in seconds, to succeed the upload request. Defaults to ``120``.

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
    >>> from plantdb.client.rest_api import upload_scan_archive
    >>> upload_scan_archive("arabidopsis000", path='/tmp/arabidopsis000.zip')
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
    url = archive_url(dataset_name, **kwargs)

    start_time = time.time()  # Start timing
    with open(path, "rb") as f:
        try:
            res = make_api_request(url,
                                   method="POST",
                                   files={"zip_file": (path.name, f, "application/zip")},
                                   stream=True,
                                   timeout=kwargs.get("timeout", 120),
                                   cert_path=kwargs.get("cert_path", None))
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
