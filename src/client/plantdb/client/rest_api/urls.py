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
# REST API URL Construction Module

Provides utilities for building URLs for the PlantDB REST API without performing network calls.
These helpers make it easy to generate consistent endpoint URLs based on host, port, optional
prefixes, and SSL settings, which is essential for client applications that need to interact
with PlantDB services.

## Key Features

- Construct base origin URLs with optional scheme, port, and SSL flag.
- Generate full endpoint URLs for authentication (login, logout, register, token handling).
- Build URLs for scans, individual scan data, preview images, and task‑specific images.
- Create URLs for archive access, configuration files, and reconstruction pipelines.
- Support custom host, port, URL prefix, and SSL toggles via environment variables or function arguments.
- Validate and sanitize dataset names before forming URLs.
- Provide helper to list image URIs with size and base64 options.

## Environment variables

- `PLANTDB_HOST`: default hostname to PlantDB REST API
- `PLANTDB_PORT`: default port to PlantDB REST API
- `PLANTDB_PREFIX`: default URL prefix for the plantdb REST API

## Usage Examples

```python
>>> from plantdb.client.rest_api.urls import plantdb_url, scan_url, scan_image_url
    >>> # Base PlantDB API URL with custom port, prefix and HTTPS
    >>> base = plantdb_url('localhost', port=5000, prefix='plantdb', ssl=True)
    >>> base
    'https://localhost:5000/plantdb/'
    >>> # URL for a specific scan
    >>> scan = scan_url('localhost', 'real_plant')
    >>> scan
    'http://localhost/api/v1/scans/real_plant'
    >>> # URL for a thumbnail image of a scan
    >>> img = scan_image_url('localhost', 'real_plant', 'images', '00000_rgb', size='thumb')
    >>> img
    'http://localhost/api/v1/assets/image/real_plant/images/00000_rgb?size=thumb'
```
"""

import io
import os
from pathlib import Path
from urllib.parse import urlparse
from urllib.parse import urlunparse

from ada_url import join_url

from plantdb.commons import api_endpoints
from plantdb.commons.log import get_logger

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
        # the value is something unexpected - fall back to the default
        PLANTDB_PORT = None
#: Default URL prefix for the plantdb REST API
PLANTDB_PREFIX = os.getenv('PLANTDB_PREFIX', None)

logger = get_logger(__name__)


def origin_url(host, port=None, ssl=False, **kwargs) -> str:
    """Construct a URL string from host, optional port, and SSL flag.

    Parameters
    ----------
    host : str
        Hostname or URL. May optionally include a scheme (e.g., ``http://`` or
        ``https://``). If a scheme is present and contains the character ``s``,
        the function treats it as HTTPS and forces ``ssl`` to ``True``.
    port : int or str, optional
        Port number to append to the host. If an ``int`` is supplied, it is
        converted to a string; a leading colon is stripped before it is added.
        The default is ``None`` which results in no port being added.
    ssl : bool, optional
        When ``True`` the URL will use the ``https`` scheme. The value is
        overridden to ``True`` if the supplied ``host`` already contains a scheme
        with an ``s`` character.

    Returns
    -------
    url
        The fully‑qualified URL string constructed from the supplied parts.

    Raises
    ------
    TypeError
        If ``host`` is not a string or does not support ``split`` (e.g., ``None``).

    Notes
    -----
    The function does **not** validate that the resulting URL points to a
    reachable endpoint; it only assembles the string. Supplying both a scheme
    in ``host`` and ``ssl=True`` will result in the scheme dictated by the
    original ``host`` (HTTPS if the original scheme contains ``s``).

    Examples
    --------
    >>> from plantdb.client.rest_api.urls import origin_url
    >>> origin_url('example.com')
    'http://example.com'
    >>> origin_url('example.com', 8080)
    'http://example.com:8080'
    >>> origin_url('https://example.com')
    'https://example.com'
    >>> origin_url('https://example.com/api/v1')
    'https://example.com'
    >>> origin_url('http://example.com', ssl=True)
    'https://example.com'
    >>> origin_url('example.com', port='443', ssl=True)
    'https://example.com:443'
    """
    if not isinstance(host, str):
        raise TypeError("host must be a string")

    # Parse the incoming host value
    parsed = urlparse(host)

    # If no scheme was supplied, ``urlparse`` treats the whole string as a
    # path.  In that case we split the first “/” to obtain the netloc.
    if not parsed.scheme:
        # e.g. "example.com/api/v1" -> netloc="example.com", path="/api/v1"
        first_slash = parsed.path.find("/")
        if first_slash == -1:
            netloc, path = parsed.path, ""
        else:
            netloc = parsed.path[:first_slash]
            path = parsed.path[first_slash:]
        scheme = ""
    else:
        scheme = parsed.scheme
        netloc = parsed.netloc
        path = parsed.path

    # If the original string already contains a scheme that contains an “s” (i.e. https) it forces ``ssl=True``.
    if scheme and "s" in scheme.lower():
        ssl = True
    final_scheme = "https" if ssl else "http"

    # Apply an explicit ``port`` argument (overwrites any existing one)
    if port is not None:
        # separates host from any existing port.
        try:
            hostname, _ = netloc.split(":")
        except ValueError:
            hostname = netloc
        # Ensure ``port`` is a clean string without a leading colon.
        clean_port = str(port).lstrip(":")
        netloc = f"{hostname}:{clean_port}"

    # Re‑assemble the URL, excluding the original path (if any)
    return urlunparse((final_scheme, netloc, "", "", "", ""))


def plantdb_url(host, port=PLANTDB_PORT, prefix=PLANTDB_PREFIX, ssl=False) -> str:
    """Generates the **origin** URL for the PlantDB REST API using the specified host and port.

    This function returns a *pure origin* (scheme + host + port).

    Parameters
    ----------
    host : str
        The hostname or IP address of the PlantDB REST API server.
    port : int or str, optional
        The port number of the PlantDB REST API server. Defaults to ``None``.
    prefix : str, optional
        **Deprecated.** Kept only for backward compatibility; ignored because the
        deployment prefix is now passed directly to ``api_endpoints.*`` calls.
    ssl : bool, optional
        Flag indicating whether to use HTTPS (``True``) or HTTP (``False``). Defaults to ``False``.

    Returns
    -------
    str
        A properly formatted origin URL for the PlantDB REST API (e.g. ``http://localhost:5000``).

    Examples
    --------
    >>> from plantdb.client.rest_api.urls import plantdb_url
    >>> plantdb_url('localhost')
    'http://localhost'
    >>> plantdb_url('api.example.com', port=8443, ssl=True)
    'https://api.example.com:8443'
    """
    return origin_url(host, port, ssl)


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
    >>> from plantdb.client.rest_api.urls import login_url
    >>> # Default URL using module level constants
    >>> url = login_url('localhost')
    >>> print(url)
    http://localhost/api/v1/auth/login
    >>> # Override host, add a prefix and enable SSL
    >>> url = login_url('dev.romi.local', prefix="/plantdb", ssl=True)
    >>> print(url)
    https://dev.romi.local/plantdb/api/v1/auth/login
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
    >>> from plantdb.client.rest_api.urls import logout_url
    >>> # Basic usage with default configuration
    >>> url = logout_url('localhost')
    >>> print(url)
    http://localhost/api/v1/auth/logout
    >>> # Specify a custom prefix and enable SSL
    >>> url = logout_url('dev.romi.local', prefix="/plantdb", ssl=True)
    >>> print(url)
    https://dev.romi.local/plantdb/api/v1/auth/logout
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
    >>> from plantdb.client.rest_api.urls import register_url
    >>> # Basic usage with default configuration
    >>> url = register_url('localhost')
    >>> print(url)
    http://localhost/api/v1/auth/register
    >>> # Specify a custom prefix and enable SSL
    >>> url = register_url('dev.romi.local', prefix="/plantdb", ssl=True)
    >>> print(url)
    https://dev.romi.local/plantdb/api/v1/auth/register
    """
    url = origin_url(host, **kwargs)
    return join_url(url, api_endpoints.register(**kwargs))


def token_validation_url(host, **kwargs):
    """Generate the full URL for the PlantDB API token validation endpoint.

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
    >>> from plantdb.client.rest_api.urls import token_validation_url
    >>> # Basic usage with default configuration
    >>> url = token_validation_url('localhost')
    >>> print(url)
    http://localhost/api/v1/auth/token/validation
    """
    url = origin_url(host, **kwargs)
    return join_url(url, api_endpoints.token_validation(**kwargs))


def token_refresh_url(host, **kwargs):
    """Generate the full URL for the PlantDB API token refresh endpoint.

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
    >>> from plantdb.client.rest_api.urls import token_refresh_url
    >>> # Basic usage with default configuration
    >>> url = token_refresh_url('localhost')
    >>> print(url)
    http://localhost/api/v1/auth/token/refresh
    """
    url = origin_url(host, **kwargs)
    return join_url(url, api_endpoints.token_refresh(**kwargs))


def api_token_url(host, **kwargs):
    """Generate the full URL for the PlantDB API token endpoint.

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
        The fully qualified URL as a string.

    Examples
    --------
    >>> from plantdb.client.rest_api.urls import api_token_url
    >>> # Basic usage with default configuration
    >>> url = api_token_url('localhost')
    >>> print(url)
    http://localhost/api/v1/auth/token/create-api-token
    """
    url = origin_url(host, **kwargs)
    return join_url(url, api_endpoints.create_api_token(**kwargs))


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
    >>> from plantdb.client.rest_api.urls import scans_url
    >>> scans_url('127.0.0.1')
    'http://127.0.0.1/api/v1/scans'
    >>> scans_url('localhost', prefix='/plantdb')
    'http://localhost/plantdb/api/v1/scans'
    >>> scans_url('dev.romi.local', prefix='/plantdb/', ssl=True)
    'https://dev.romi.local/plantdb/api/v1/scans'
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
    >>> from plantdb.client.rest_api.urls import scan_url
    >>> scan_url('localhost', "real_plant")
    'http://localhost/api/v1/scans/real_plant'
    >>> scan_url('localhost', "real_plant", prefix='/plantdb')
    'http://localhost/plantdb/api/v1/scans/real_plant'
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
    >>> from plantdb.client.rest_api.urls import scan_preview_image_url
    >>> img_url = scan_preview_image_url('localhost', 'real_plant')
    >>> print(img_url)
    http://localhost/api/v1/assets/image/real_plant/images/00000_rgb?size=thumb
    >>> img_url = scan_preview_image_url('localhost', 'real_plant', size=100)
    >>> print(img_url)
    http://localhost/api/v1/assets/image/real_plant/images/00000_rgb?size=100
    >>> # Download and display the image
    >>> import requests
    >>> from PIL import Image
    >>> from io import BytesIO
    >>> response = requests.get(img_url)  # Send a GET request to the URL
    >>> image = Image.open(BytesIO(response.content))  # Open the image from the bytes data
    >>> image.show()  # Display the image
    """
    from plantdb.commons.utils import sanitize_name
    from plantdb.client.rest_api.requests import request_scan_names_list
    from plantdb.client.rest_api.requests import request_scan_data

    scan_id = sanitize_name(scan_id)
    scan_names = request_scan_names_list(host, **kwargs)
    if scan_id not in scan_names:
        return None

    thumb_uri = request_scan_data(host, scan_id, **kwargs)["thumbnailUri"]
    if size != "thumb":
        thumb_uri = thumb_uri.replace("size=thumb", f"size={size}")
    url = origin_url(host, **kwargs)
    return join_url(url, thumb_uri)


def scan_image_url(host, scan_id, fileset_id, file_id, size='orig', as_base64=False, **kwargs):
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
        If an integer, use it as the size of the cached image to create and return.
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


    Returns
    -------
    str
        The URL to an image of a scan dataset and task fileset.

    Examples
    --------
    >>> from plantdb.client.rest_api.urls import scan_image_url
    >>> scan_image_url('localhost', "real_plant", "images", "00000_rgb")
    'http://localhost/api/v1/assets/image/real_plant/images/00000_rgb?size=orig'
    >>> scan_image_url('localhost', "real_plant", "images", "00000_rgb", as_base64=True)
    'http://localhost/api/v1/assets/image/real_plant/images/00000_rgb?size=orig&as_base64=true'
    >>> scan_image_url('localhost', "real_plant", "images", "00000_rgb", prefix='/plantdb')
    'http://localhost/plantdb/api/v1/assets/image/real_plant/images/00000_rgb?size=orig'
    """
    url = origin_url(host, **kwargs)
    return join_url(url, api_endpoints.image(scan_id, fileset_id, file_id, size, as_base64, **kwargs))


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
    >>> from plantdb.client.rest_api.urls import refresh_url
    >>> refresh_url('localhost', "real_plant")
    'http://localhost/api/v1/refresh?scan_id=real_plant'
    >>> refresh_url('localhost', "real_plant", prefix='/plantdb')
    'http://localhost/plantdb/api/v1/refresh?scan_id=real_plant'
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
    >>> from plantdb.client.rest_api.urls import archive_url
    >>> archive_url('localhost', 'arabidopsis000')
    'http://localhost/api/v1/assets/archive/arabidopsis000'
    >>> archive_url('localhost', '../arabidopsis000')
    'http://localhost/api/v1/assets/archive/arabidopsis000'
    >>> archive_url('localhost', 'arabidopsis+000')
    ValueError: Invalid dataset name: 'arabidopsis+000'. Dataset names must be alphanumeric and can include underscores or dashes.
    >>> archive_url('localhost', 'arabidopsis000', prefix='/plantdb')
    'http://localhost/plantdb/api/v1/assets/archive/arabidopsis000'
    """
    url = origin_url(host, **kwargs)
    return join_url(url, api_endpoints.archive(scan_id, **kwargs))


def scan_file_url(host, file_path, **kwargs):
    """Build the URL for accessing a dataset file.

    Parameters
    ----------
    host : str
        The hostname or IP address of the PlantDB REST API server.
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
    return join_url(url, api_endpoints.file_path(file_path, **kwargs))


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
    >>> from plantdb.client.rest_api.urls import scan_config_url
    >>> scan_config_url('localhost', 'real_plant')
    'http://localhost/api/v1/assets/files/real_plant/scan.toml'
    >>> scan_config_url('localhost', 'real_plant', prefix='/plantdb')
    'http://localhost/plantdb/api/v1/assets/files/real_plant/scan.toml'
    """
    return scan_file_url(host, f"{scan_id}/{cfg_fname}", **kwargs)


def scan_reconstruction_url(host, scan_id, cfg_fname='pipeline.toml', **kwargs):
    """Return the scan URL to access the reconstruction configuration file.

    Parameters
    ----------
    host : str
        The hostname or IP address of the PlantDB REST API server.
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
    >>> from plantdb.client.rest_api.urls import scan_reconstruction_url
    >>> scan_reconstruction_url('localhost', 'real_plant')
    'http://localhost/api/v1/assets/files/real_plant/pipeline.toml'
    >>> scan_reconstruction_url('localhost', 'real_plant', prefix='/plantdb')
    'http://localhost/plantdb/api/v1/assets/files/real_plant/pipeline.toml'
    """
    return scan_file_url(host, f"{scan_id}/{cfg_fname}", **kwargs)


def list_task_images_uri(host, scan_id, task_name='images', size='orig', as_base64=True, **kwargs):
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

    Returns
    -------
    list of str
        The list of image URI strings for the PlantDB REST API.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api.urls import list_task_images_uri
    >>> print(list_task_images_uri('localhost', 'real_plant')[2])
    http://localhost/api/v1/assets/image/real_plant/images/00002_rgb?size=orig
    >>> print(list_task_images_uri('localhost', 'real_plant', size=100)[2])
    http://localhost/api/v1/assets/image/real_plant/images/00002_rgb?size=100
    """
    from plantdb.client.rest_api.requests import request_scan_data

    scan_info = request_scan_data(host, scan_id, **kwargs)
    tasks_id = scan_info["tasks_fileset"][task_name]
    images = scan_info["images"]
    url = origin_url(host, **kwargs)
    return [join_url(url, api_endpoints.image(scan_id, tasks_id, Path(img).stem, size, as_base64, **kwargs))
            for img in images]
