#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""API Endpoints for PlantDB client.

This module provides helper functions to construct URL paths for the PlantDB
REST API. Each function returns the endpoint string with optional prefix
and performs basic sanitization of identifiers.

Key Features
------------
- Sanitizes and validates scan, fileset, and file names.
- Supports optional URL prefixes for API versioning.
- Generates paths for authentication, health checks, scans, images, and archives.

Usage Examples
--------------
>>> from plantdb.client import api_endpoints
>>> api_endpoints.login(prefix='/api/v1')
'/api/v1/login'
>>> api_endpoints.scan('plant1', prefix='/api/v1')
'/api/v1/scan/plant1'
"""

from typing import Optional
from urllib import parse


def sanitize_name(name) -> str:
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
        If the provided name contains invalid characters or does not meet
        the naming rules.
    """
    import re
    sanitized_name = name.strip()  # Remove leading/trailing spaces
    sanitized_name = sanitized_name.split('/')[-1]  # isolate the last segment after splitting by slashes
    # Validate against an alphanumeric pattern with optional underscores, dashes, or periods
    if not re.match(r"^[a-zA-Z0-9_.-]+$", sanitized_name):
        raise ValueError(
            f"Invalid name: '{name}'. Names must be alphanumeric and can include underscores, dashes, or periods.")
    return sanitized_name


def url_prefix(endpoint_path):
    """Wrap an endpoint path generator with an optional URL prefix."""

    def wrapper(*args, **kwargs):
        if "prefix" in kwargs and kwargs["prefix"]:
            prefix = kwargs["prefix"]
            prefix = '/' + prefix.lstrip('/').rstrip('/')
            return prefix + endpoint_path(*args, **kwargs)
        else:
            return endpoint_path(*args, **kwargs)

    return wrapper


# ------------------------------------------------------------------------
# - Base Endpoints
# ------------------------------------------------------------------------

@url_prefix
def health(**kwargs) -> str:
    """Return the URL path to the health endpoint.

    Other Parameters
    ----------------
    prefix : str
        An optional prefix to prepend to the URL path.

    Returns
    -------
    str
        The URL path to the health endpoint.

    Examples
    --------
    >>> from plantdb.client import api_endpoints
    >>> api_endpoints.health(prefix='/api/v1')
    '/api/v1/health'
    """
    return "/health"


@url_prefix
def refresh(scan_id: str = None, **kwargs) -> str:
    """Return the URL path to the dataset archive endpoint.

    Parameters
    ----------
    scan_id : str
        The name of the scan dataset to archive.

    Other Parameters
    ----------------
    prefix : str
        An optional prefix to prepend to the URL path.

    Returns
    -------
    str
        The URL path to the refresh endpoint.

    Examples
    --------
    >>> from plantdb.client import api_endpoints
    >>> api_endpoints.refresh(prefix='/api/v1')
    '/api/v1/refresh'
    >>> api_endpoints.refresh('scan1')
    '/refresh?scan_id=scan1'
    """
    params = ""
    if scan_id:
        scan_id = sanitize_name(scan_id)
        params = f"?scan_id={scan_id}"
    return f"/refresh{params}"


# ------------------------------------------------------------------------
# - Authentication Endpoints
# ------------------------------------------------------------------------

@url_prefix
def register(**kwargs) -> str:
    """Return the URL path to the register endpoint.

    Other Parameters
    ----------------
    prefix : str
        An optional prefix to prepend to the URL path.

    Returns
    -------
    str
        The URL path to the register endpoint.

    Examples
    --------
    >>> from plantdb.client import api_endpoints
    >>> api_endpoints.register(prefix='/api/v1')
    '/api/v1/register'
    """
    return "/register"


@url_prefix
def login(**kwargs) -> str:
    """Return the URL path to the login endpoint.

    Other Parameters
    ----------------
    prefix : str
        An optional prefix to prepend to the URL path.

    Returns
    -------
    str
        The URL path to the login endpoint.

    Examples
    --------
    >>> from plantdb.client import api_endpoints
    >>> api_endpoints.login(prefix='/api/v1')
    '/api/v1/login'
    """
    return "/login"


@url_prefix
def logout(**kwargs) -> str:
    """Return the URL path to the logout endpoint.

    Other Parameters
    ----------------
    prefix : str
        An optional prefix to prepend to the URL path.

    Returns
    -------
    str
        The URL path to the logout endpoint.

    Examples
    --------
    >>> from plantdb.client import api_endpoints
    >>> api_endpoints.logout(prefix='/api/v1')
    '/api/v1/logout'
    """
    return "/logout"


@url_prefix
def token_refresh(**kwargs) -> str:
    """Return the URL path to the token refresh endpoint.

    Other Parameters
    ----------------
    prefix : str
        An optional prefix to prepend to the URL path.

    Returns
    -------
    str
        The URL path to the token refresh endpoint.

    Examples
    --------
    >>> from plantdb.client import api_endpoints
    >>> api_endpoints.token_refresh(prefix='/api/v1')
    '/api/v1/token-refresh'
    """
    return "/token-refresh"


@url_prefix
def token_validation(**kwargs) -> str:
    """Return the URL path to the token validation endpoint.

    Other Parameters
    ----------------
    prefix : str
        An optional prefix to prepend to the URL path.

    Returns
    -------
    str
        The URL path to the token validation endpoint.

    Examples
    --------
    >>> from plantdb.client import api_endpoints
    >>> api_endpoints.token_validation(prefix='/api/v1')
    '/api/v1/token-validation'
    """
    return "/token-validation"


@url_prefix
def create_api_token():
    """Return the URL path to the API token creation endpoint.

    Other Parameters
    ----------------
    prefix : str
        An optional prefix to prepend to the URL path.

    Returns
    -------
    str
        The URL path to the API token creation endpoint.

    Examples
    --------
    >>> from plantdb.client import api_endpoints
    >>> api_endpoints.create_api_token(prefix='/api/v1')
    '/api/v1/create-api-token'
    """
    return "/create-api-token"


# ------------------------------------------------------------------------
# - Scan, Fileset & File Endpoints
# ------------------------------------------------------------------------


@url_prefix
def scans(**kwargs) -> str:
    """Return the URL path to the scans' endpoint.

    Other Parameters
    ----------------
    prefix : str
        An optional prefix to prepend to the URL path.

    Returns
    -------
    str
        The URL path to the scans' endpoint.

    Examples
    --------
    >>> from plantdb.client import api_endpoints
    >>> api_endpoints.scans(prefix='/api/v1')
    '/api/v1/scans'
    """
    return "/scans"


@url_prefix
def scans_info(**kwargs) -> str:
    """Return the URL path to the list of scan dataset information endpoint.

    Other Parameters
    ----------------
    prefix : str
        An optional prefix to prepend to the URL path.

    Returns
    -------
    str
        The URL path to the list of scan dataset information endpoint.

    Examples
    --------
    >>> from plantdb.client import api_endpoints
    >>> api_endpoints.scans_info(prefix='/api/v1')
    '/api/v1/scans_info'
    """
    return "/scans_info"


@url_prefix
def scan(scan_id: str, **kwargs) -> str:
    """Return the URL path to the scan endpoint.

    Parameters
    ----------
    scan_id : str
        The name of the scan to access.

    Other Parameters
    ----------------
    prefix : str
        An optional prefix to prepend to the URL path.

    Returns
    -------
    str
        The URL path to the scan endpoint.

    Examples
    --------
    >>> from plantdb.client import api_endpoints
    >>> api_endpoints.scan('scan1')
    '/scan/scan1'
    >>> api_endpoints.scan('scan1', prefix='/api/v1')
    '/api/v1/scan/scan1'
    """
    scan_id = sanitize_name(scan_id)
    return f"/scan/{scan_id}"


@url_prefix
def metadata_scan(scan_id: str, **kwargs) -> str:
    """URL to access the metadata associated with the given scan name.

    Parameters
    ----------
    scan_id : str
        The name of the scan to access.

    Other Parameters
    ----------------
    prefix : str
        An optional prefix to prepend to the URL path.

    Returns
    -------
    str
        The URL path to scan metadata.
    """
    scan_id = sanitize_name(scan_id)
    return f"/scan/{scan_id}/metadata"


@url_prefix
def list_scan_filesets(scan_id: str, **kwargs) -> str:
    """URL to list the filesets associated with the given scan name.

    Parameters
    ----------
    scan_id : str
        The name of the scan to access.

    Other Parameters
    ----------------
    prefix : str
        An optional prefix to prepend to the URL path.

    Returns
    -------
    str
        The URL path to filesets.
    """
    scan_id = sanitize_name(scan_id)
    return f"/scan/{scan_id}/filesets"


@url_prefix
def fileset(scan_id, fileset_id, **kwargs) -> str:
    """URL path for a fileset belonging to a scan.

    Parameters
    ----------
    scan_id : str
        The name of the scan to access.
    fileset_id : str
        The name of the fileset to access.
    file_id : str
        The name of the file to access.

    Other Parameters
    ----------------
    prefix : str
        An optional prefix to prepend to the URL path.

    Returns
    -------
    str
        The URL path to file metadata.
    """
    scan_id = sanitize_name(scan_id)
    fileset_id = sanitize_name(fileset_id)
    return f"/fileset/{scan_id}/{fileset_id}"


@url_prefix
def metadata_fileset(scan_id: str, fileset_id: str, **kwargs) -> str:
    """URL to access the fileset metadata associated with the given scan and fileset name.

    Parameters
    ----------
    scan_id : str
        The name of the scan to access.
    fileset_id : str
        The name of the fileset to access.

    Other Parameters
    ----------------
    prefix : str
        An optional prefix to prepend to the URL path.

    Returns
    -------
    str
        The URL path to fileset metadata.
    """
    scan_id = sanitize_name(scan_id)
    fileset_id = sanitize_name(fileset_id)
    return f"/scan/{scan_id}/{fileset_id}/metadata"


@url_prefix
def list_fileset_files(scan_id: str, fileset_id: str, **kwargs) -> str:
    """URL to list the file associated with the given scan and filesets names.

    Parameters
    ----------
    scan_id : str
        The name of the scan to access.
    fileset_id : str
        The name of the fileset to access.

    Other Parameters
    ----------------
    prefix : str
        An optional prefix to prepend to the URL path.

    Returns
    -------
    str
        The URL path to the filesets list of files.
    """
    scan_id = sanitize_name(scan_id)
    fileset_id = sanitize_name(fileset_id)
    return f"/scan/{scan_id}/{fileset_id}/files"


@url_prefix
def file(scan_id: str, fileset_id: str, file_id: str, **kwargs) -> str:
    """Return the URL path to the `scan/fileset/file` endpoint.

    Parameters
    ----------
    scan_id : str
        The name of the scan dataset containing the file.
    fileset_id : str
        The name of the fileset containing the file.
    file_id : str
        The name of the file.

    Returns
    -------
    str
        The URL path to the file endpoint.

    Examples
    --------
    >>> from plantdb.client import api_endpoints
    >>> api_endpoints.file('real_plant','images','00000_rgb')
    '/files/real_plant/images/00000_rgb'
    """
    scan_id = sanitize_name(scan_id)
    fileset_id = sanitize_name(fileset_id)
    file_id = sanitize_name(file_id)
    return f"/file/{scan_id}/{fileset_id}/{file_id}"


@url_prefix
def metadata_file(scan_id: str, fileset_id: str, file_id: str, **kwargs) -> str:
    """URL to access the file metadata associated with the given scan and fileset name.

    Parameters
    ----------
    scan_id : str
        The name of the scan to access.
    fileset_id : str
        The name of the fileset to access.
    file_id : str
        The name of the file to access.

    Other Parameters
    ----------------
    prefix : str
        An optional prefix to prepend to the URL path.

    Returns
    -------
    str
        The URL path to file metadata.
    """
    scan_id = sanitize_name(scan_id)
    fileset_id = sanitize_name(fileset_id)
    file_id = sanitize_name(file_id)
    return f"/file/{scan_id}/{fileset_id}/{file_id}/metadata"


# ------------------------------------------------------------------------
# - Asset Endpoints
# ------------------------------------------------------------------------

@url_prefix
def image(scan_id: str, fileset_id: str, file_id: str, size: str, as_base64: bool, **kwargs) -> str:
    """Return the URL path to the image endpoint.

    Parameters
    ----------
    scan_id : str
        The name of the scan dataset containing the image.
    fileset_id : str
        The name of the fileset containing the image.
    file_id : str
        The name of the image.
    size : str or int
        The size parameter of the image request.
    as_base64 : bool
        A boolean flag indicating whether to return an image as a base64 string.

    Returns
    -------
    str
        The URL path to the image endpoint.

    Examples
    --------
    >>> from plantdb.client import api_endpoints
    >>> api_endpoints.image('real_plant','images','00000_rgb', 'orig', False)
    '/image/real_plant/images/00000_rgb?size=orig'
    >>> api_endpoints.image('real_plant','images','00000_rgb', 'thumb', True)
    '/image/real_plant/images/00000_rgb?size=thumb&as_base64=true'
    """
    scan_id = sanitize_name(scan_id)
    fileset_id = sanitize_name(fileset_id)
    file_id = sanitize_name(file_id)

    # Assemble optional query parameters
    query: dict[str, str] = {}
    if size is not None:
        query["size"] = str(size)
    if as_base64 is not None:
        # Use lower‑case JSON‑style booleans for consistency
        query["as_base64"] = str(as_base64).lower()

    query_str = f"?{parse.urlencode(query)}" if query else ""
    return f"/image/{scan_id}/{fileset_id}/{file_id}{query_str}"


@url_prefix
def sequence(scan_id: str, type: str, **kwargs) -> str:
    """Return the URL path to the sequence endpoint.

    Parameters
    ----------
    scan_id : str
        The name of the scan dataset containing the angles and internodes sequence.
    type : str or int
        The type of measure to reques, in ['all', 'angles', 'internodes', 'fruit_points',
         'manual_angles', 'manual_internodes'].

    Returns
    -------
    str
        The URL path to the angles and internodes sequences endpoint.

    Examples
    --------
    >>> from plantdb.client import api_endpoints
    >>> api_endpoints.sequence('real_plant', 'all')
    '/sequence/real_plant?type=all'
    """
    valid_types = ['all', 'angles', 'internodes', 'fruit_points', 'manual_angles', 'manual_internodes']
    type = 'all' if type not in valid_types else type
    scan_id = sanitize_name(scan_id)

    # Assemble optional query parameters
    query: dict[str, str] = {}
    if type is not None:
        query["type"] = str(type)

    query_str = f"?{parse.urlencode(query)}" if query else ""
    return f"/sequence/{scan_id}{query_str}"


@url_prefix
def archive(scan_id: str, **kwargs) -> str:
    """Return the URL path to the dataset archive endpoint.

    Parameters
    ----------
    scan_id : str
        The name of the scan dataset to archive.

    Returns
    -------
    str
        The URL path to the dataset archive endpoint.

    Examples
    --------
    >>> from plantdb.client import api_endpoints
    >>> api_endpoints.archive('scan1')
    '/archive/scan1'
    """
    scan_id = sanitize_name(scan_id)
    return f"/archive/{scan_id}"


@url_prefix
def file_path(scan_id: str, file_path: str, **kwargs) -> str:
    """Return the URL path to the `scan/file_path` endpoint.

    Parameters
    ----------
    scan_id : str
        The name of the scan dataset containing the file.
    file_path : str
        The path to the file in the database.

    Returns
    -------
    str
        The URL path to the file path endpoint.

    Examples
    --------
    >>> from plantdb.client import api_endpoints
    >>> api_endpoints.file_path('real_plant','images/00000_rgb.jpg')
    '/files/real_plant/images/00000_rgb.jpg'
    """
    scan_id = sanitize_name(scan_id)
    return f"/files/{scan_id}/{file_path.lstrip('/')}"


@url_prefix
def list_dataset_files(scan_id: Optional[str] = None, **kwargs) -> str:
    """Return the URL path to list all files of a dataset (scan)."""
    if scan_id:
        scan_id = sanitize_name(scan_id)
    return f"/files/{scan_id}" if scan_id else "/files"


@url_prefix
def pointcloud(scan_id: str, fileset_id: str, file_id: str, **kwargs) -> str:
    """Return the URL path to the point‑cloud endpoint."""
    scan_id = sanitize_name(scan_id)
    fileset_id = sanitize_name(fileset_id)
    file_id = sanitize_name(file_id)
    return f"/pointcloud/{scan_id}/{fileset_id}/{file_id}"


@url_prefix
def pc_ground_truth(scan_id: str, fileset_id: str, file_id: str, **kwargs) -> str:
    """Return the URL path to the point‑cloud ground‑truth endpoint."""
    scan_id = sanitize_name(scan_id)
    fileset_id = sanitize_name(fileset_id)
    file_id = sanitize_name(file_id)
    return f"/pcGroundTruth/{scan_id}/{fileset_id}/{file_id}"


@url_prefix
def mesh(scan_id: str, fileset_id: str, file_id: str, **kwargs) -> str:
    """Return the URL path to the mesh endpoint."""
    scan_id = sanitize_name(scan_id)
    fileset_id = sanitize_name(fileset_id)
    file_id = sanitize_name(file_id)
    return f"/mesh/{scan_id}/{fileset_id}/{file_id}"


@url_prefix
def skeleton(scan_id: str, **kwargs) -> str:
    """Return the URL path to the skeleton endpoint."""
    scan_id = sanitize_name(scan_id)
    return f"/skeleton/{scan_id}"
