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
'/api/v1/scans/plant1'
"""


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


def url_prefix(endpoint_path):
    """
    Wrap an endpoint path generator with an optional URL prefix.

    Examples
    --------
    >>> def base_path(user_id):
    ...     return f"/users/{user_id}"
    ...
    >>> prefixed = url_prefix(base_path)
    >>> prefixed(42)
    '/users/42'
    >>> prefixed(42, prefix='api')
    '/api/users/42'
    >>> prefixed(42, prefix='/api/')
    '/api/users/42'
    """

    def wrapper(*args, **kwargs):
        if "prefix" in kwargs and kwargs["prefix"]:
            prefix = kwargs["prefix"]
            prefix = '/' + prefix.lstrip('/').rstrip('/')
            return prefix + endpoint_path(*args, **kwargs)
        else:
            return endpoint_path(*args, **kwargs)

    return wrapper


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


@url_prefix
def scans(**kwargs) -> str:
    """Return the URL path to the scans endpoint.

    Other Parameters
    ----------------
    prefix : str
        An optional prefix to prepend to the URL path.

    Returns
    -------
    str
        The URL path to the scans endpoint.

    Examples
    --------
    >>> from plantdb.client import api_endpoints
    >>> api_endpoints.scans(prefix='/api/v1')
    '/api/v1/scans'
    """
    return "/scans"


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
    '/scans/scan1'
    >>> api_endpoints.scan('scan1', prefix='/api/v1')
    '/api/v1/scans/scan1'
    """
    return f"/scans/{sanitize_name(scan_id)}"


@url_prefix
def image(scan_id: str, fileset_id: str, file_id: str, size: str, **kwargs) -> str:
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

    Returns
    -------
    str
        The URL path to the image endpoint.

    Examples
    --------
    >>> from plantdb.client import api_endpoints
    >>> api_endpoints.image('real_plant','images','00000_rgb', 'orig')
    '/image/real_plant/images/00000_rgb?size=orig'
    """
    scan_id = sanitize_name(scan_id)
    fileset_id = sanitize_name(fileset_id)
    file_id = sanitize_name(file_id)
    return f"/image/{scan_id}/{fileset_id}/{file_id}?size={size}"


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
    return f"/sequence/{scan_id}?type={type}"


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
    return f"/files/{scan_id}/{fileset_id}/{file_id}"


@url_prefix
def scan_file(scan_id: str, file_path: str, **kwargs) -> str:
    """Return the URL path to the `scan/fileset/file` endpoint.

    Parameters
    ----------
    scan_id : str
        The name of the scan dataset containing the file.
    file_path : str
        The path to the file in the database.

    Returns
    -------
    str
        The URL path to the file endpoint.

    Examples
    --------
    >>> from plantdb.client import api_endpoints
    >>> api_endpoints.scan_file('real_plant','images/00000_rgb.jpg')
    '/files/real_plant/images/00000_rgb.jpg'
    """
    scan_id = sanitize_name(scan_id)
    return f"/files/{scan_id}/{file_path.lstrip('/')}"
