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
>>> from plantdb.commons import api_endpoints
>>> api_endpoints.login()
'/api/v1/login'
>>> api_endpoints.scan('plant1', )
'/api/v1/scan/plant1'
"""

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
    sanitized_name = sanitized_name.split("/")[-1]  # isolate the last segment after splitting by slashes
    # Validate against an alphanumeric pattern with optional underscores, dashes, or periods
    if not re.match(r"^[a-zA-Z0-9_.-]+$", sanitized_name):
        raise ValueError(
            f"Invalid name: '{name}'. Names must be alphanumeric and can include underscores, dashes, or periods."
        )
    return sanitized_name


# ------------------------------------------------------------------------
# Resource mapping
# ------------------------------------------------------------------------
# /api/v1/
#       ├─ health       (GET) → REST API status
#       ├─ refresh/     (GET) → Reload the whole database
#       │   └─ {scan_id}     (GET) → Reload the scan
#       ├─ auth/
#       │   ├─ login         (POST) → user login
#       │   ├─ logout        (POST) → user logout
#       │   ├─ register      (POST) → new user registration
#       │   └─ tokens/
#       │       ├─ refresh            (GET) → retrieve a specific scan
#       │       ├─ validation         (GET) → retrieve a specific scan
#       │       └─ create-api-token   (POST) → create-api-token
#       ├─ scans/
#       │   ├─ (GET)     → list scans
#       │   ├─ info      (GET) → list scans
#       │   └─ {scan_id}/
#       │       ├─ (GET)    → retrieve a specific scan
#       │       ├─ (POST)   → create a new scan
#       │       ├─ metadata/
#       │       │   ├─ (GET)   → get `scan_id` metadata
#       │       │   └─ (POST)  → update `scan_id` metadata
#       │       └─ filesets/
#       │           ├─ (GET)       → list filesets for scan
#       │           └─ {fileset_id}/
#       │               ├─ (POST)      → create new fileset
#       │               ├─ metadata/
#       │               │   ├─ (GET)   → get `scan_id/fileset_id` metadata
#       │               │   └─ (POST)  → update `scan_id/fileset_id` metadata
#       │               └─ files/
#       │                   ├─ (GET)           → list files
#       │                   └─ {file_id}/
#       │                       ├─ (GET)       → retrieve file
#       │                       ├─ (POST)      → create new file
#       │                       └─ metadata/   → (GET, PATCH)
#       │                           ├─ (GET)   → get `scan_id/fileset_id/file_id` metadata
#       │                           └─ (POST)  → update `scan_id/fileset_id/file_id` metadata
#       └─ assets/
#           ├─ files/{file_path}      (GET) → retrieve a specific scan
#           ├─ archive/{scan_id}
#           │   ├─ (GET)              → retrieve scan archive
#           │   └─ (POST)             → create a new scan by uploading a scan archive
#           ├─ image/{scan_id}/{fileset_id}/{file_id}
#           │   ├─ (GET)              → get `scan_id/fileset_id/file_id` image
#           │   └─ (POST)             → create a new `scan_id/fileset_id/file_id` image
#           ├─ pointcloud/{scan_id}   (GET) → retrieve scan pointcloud
#           ├─ mesh/{scan_id}         (GET) → retrieve scan triangular mesh
#           ├─ sequence/{scan_id}     (GET) → retrieve scan sequence
#           └─ skeleton/{scan_id}     (GET) → retrieve scan skeleton
# ------------------------------------------------------------------------
URL_PREFIX = "/api/v1"
HOME = "/"
HEALTH = "/health"
REFRESH = "/refresh"
# --- Authentication ---
REGISTER = "/auth/register"
LOGIN = "/auth/login"
LOGOUT = "/auth/logout"
TOKEN_REFRESH = "/auth/token/refresh"
TOKEN_VALIDATION = "/auth/token/validation"
CREATE_API_TOKEN = "/auth/token/create-api-token"
# --- Scans ---
SCANS = "/scans"
SCANS_INFO = SCANS + "/info"
# --- Scan object ---
SCAN = SCANS + "/{scan_id}"
SCAN_MD = SCAN + "/metadata"
SCAN_FILESETS = SCAN + "/filesets"
# --- Fileset object ---
FILESET = "/filesets/{scan_id}/{fileset_id}"
FILESET_MD = FILESET + "/metadata"
FILESET_FILES = FILESET + "/files"
# --- Fileset object ---
FILE = "/files/{scan_id}/{fileset_id}/{file_id}"
FILE_MD = FILE + "/metadata"
# --- Assets ---
IMAGE = "/assets/image/{scan_id}/{fileset_id}/{file_id}"
POINTCLOUD = "/assets/pointcloud/{scan_id}"
MESH = "/assets/mesh/{scan_id}"
SEQUENCE = "/assets/sequence/{scan_id}"
SKELETON = "/assets/skeleton/{scan_id}"
ARCHIVE = "/assets/archive/{scan_id}"
FILE_PATH = "/assets/files/{file_path}"


def url_prefix(endpoint_path):
    """Wrap an endpoint path generator with an optional URL prefix."""

    def wrapper(*args, **kwargs):
        prefix = kwargs.get("prefix", URL_PREFIX)
        if prefix:
            prefix = "/" + prefix.lstrip("/").rstrip("/")
            return prefix + endpoint_path(*args, **kwargs)
        else:
            return endpoint_path(*args, **kwargs)

    return wrapper


# ------------------------------------------------------------------------
# - Base Endpoints
# ------------------------------------------------------------------------


@url_prefix
def home(**kwargs) -> str:
    """Return the URL path to the home endpoint.

    Other Parameters
    ----------------
    prefix : str
        An optional prefix to prepend to the URL path.

    Returns
    -------
    str
        The URL path to the home endpoint.

    Examples
    --------
    >>> from plantdb.commons import api_endpoints
    >>> api_endpoints.home()
    '/api/v1/'
    >>> api_endpoints.home(prefix='/plantdb')
    '/plantdb/'
    """
    return HOME


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
    >>> from plantdb.commons import api_endpoints
    >>> api_endpoints.health()
    '/api/v1/health'
    """
    return HEALTH


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
    >>> from plantdb.commons import api_endpoints
    >>> api_endpoints.refresh()
    '/api/v1/refresh'
    >>> api_endpoints.refresh('scan1')
    '/refresh?scan_id=scan1'
    """
    params = ""
    if scan_id:
        scan_id = sanitize_name(scan_id)
        params = f"?scan_id={scan_id}"
    return f"{REFRESH}{params}"


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
    >>> from plantdb.commons import api_endpoints
    >>> api_endpoints.register()
    '/api/v1/auth/register'
    """
    return REGISTER


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
    >>> from plantdb.commons import api_endpoints
    >>> api_endpoints.login()
    '/api/v1/auth/login'
    """
    return LOGIN


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
    >>> from plantdb.commons import api_endpoints
    >>> api_endpoints.logout()
    '/api/v1/auth/logout'
    """
    return LOGOUT


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
    >>> from plantdb.commons import api_endpoints
    >>> api_endpoints.token_refresh()
    '/api/v1/auth/token/refresh'
    """
    return TOKEN_REFRESH


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
    >>> from plantdb.commons import api_endpoints
    >>> api_endpoints.token_validation()
    '/api/v1/auth/token/validation'
    """
    return TOKEN_VALIDATION


@url_prefix
def create_api_token(**kwargs):
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
    >>> from plantdb.commons import api_endpoints
    >>> api_endpoints.create_api_token()
    '/api/v1/auth/token/create-api-token'
    """
    return CREATE_API_TOKEN


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
    >>> from plantdb.commons import api_endpoints
    >>> api_endpoints.scans()
    '/api/v1/scans'
    """
    return SCANS


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
    >>> from plantdb.commons import api_endpoints
    >>> api_endpoints.scans_info()
    '/api/v1/scans/info'
    """
    return SCANS_INFO


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
    >>> from plantdb.commons import api_endpoints
    >>> api_endpoints.scan('scan1')
    '/api/v1/scans/scan1'
    """
    scan_id = sanitize_name(scan_id)
    return SCAN.format(scan_id=scan_id)


@url_prefix
def scan_metadata(scan_id: str, key: str | None = None, **kwargs) -> str:
    """URL to access the metadata associated with the given scan name.

    Parameters
    ----------
    scan_id : str
        The name of the scan to access.
    key : str
        A specific metadata key to fetch.

    Other Parameters
    ----------------
    prefix : str
        An optional prefix to prepend to the URL path.

    Returns
    -------
    str
        The URL path to scan metadata.

    Examples
    --------
    >>> from plantdb.commons import api_endpoints
    >>> api_endpoints.scan_metadata('real_plant')
    '/api/v1/scans/real_plant/metadata'
    """
    scan_id = sanitize_name(scan_id)

    # Assemble optional query parameters
    query: dict[str, str] = {}
    if key is not None:
        query["key"] = str(key)

    query_str = f"?{parse.urlencode(query)}" if query else ""
    return SCAN_MD.format(scan_id=scan_id) + f"{query_str}"


@url_prefix
def scan_filesets_list(scan_id: str, **kwargs) -> str:
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

    Examples
    --------
    >>> from plantdb.commons import api_endpoints
    >>> api_endpoints.scan_filesets_list('real_plant')
    '/api/v1/scans/real_plant/filesets'
    """
    scan_id = sanitize_name(scan_id)
    return SCAN_FILESETS.format(scan_id=scan_id)


@url_prefix
def fileset(scan_id, fileset_id, **kwargs) -> str:
    """URL path for a fileset belonging to a scan.

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
        The URL path to file metadata.

    Examples
    --------
    >>> from plantdb.commons import api_endpoints
    >>> api_endpoints.fileset('real_plant', 'images')
    '/api/v1/filesets/real_plant/images'
    """
    scan_id = sanitize_name(scan_id)
    fileset_id = sanitize_name(fileset_id)
    return FILESET.format(scan_id=scan_id, fileset_id=fileset_id)


@url_prefix
def fileset_metadata(
    scan_id: str, fileset_id: str, key: str | None = None, **kwargs
) -> str:
    """URL to access the fileset metadata associated with the given scan and fileset name.

    Parameters
    ----------
    scan_id : str
        The name of the scan to access.
    fileset_id : str
        The name of the fileset to access.
    key : str
        A specific metadata key to fetch.

    Other Parameters
    ----------------
    prefix : str
        An optional prefix to prepend to the URL path.

    Returns
    -------
    str
        The URL path to fileset metadata.

    Examples
    --------
    >>> from plantdb.commons import api_endpoints
    >>> api_endpoints.fileset_metadata('real_plant', 'images')
    '/api/v1/filesets/real_plant/images/metadata'
    """
    scan_id = sanitize_name(scan_id)
    fileset_id = sanitize_name(fileset_id)

    # Assemble optional query parameters
    query: dict[str, str] = {}
    if key is not None:
        query["key"] = str(key)

    query_str = f"?{parse.urlencode(query)}" if query else ""
    return FILESET_MD.format(scan_id=scan_id, fileset_id=fileset_id) + f"{query_str}"


@url_prefix
def fileset_files_list(scan_id: str, fileset_id: str, **kwargs) -> str:
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

    Examples
    --------
    >>> from plantdb.commons import api_endpoints
    >>> api_endpoints.fileset_files_list('real_plant', 'images')
    '/api/v1/filesets/real_plant/images/files'
    """
    scan_id = sanitize_name(scan_id)
    fileset_id = sanitize_name(fileset_id)
    return FILESET_FILES.format(scan_id=scan_id, fileset_id=fileset_id)


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

    Other Parameters
    ----------------
    prefix : str
        An optional prefix to prepend to the URL path.

    Returns
    -------
    str
        The URL path to the file endpoint.

    Examples
    --------
    >>> from plantdb.commons import api_endpoints
    >>> api_endpoints.file('real_plant', 'images', '00000_rgb')
    '/api/v1/files/real_plant/images/00000_rgb'
    """
    scan_id = sanitize_name(scan_id)
    fileset_id = sanitize_name(fileset_id)
    file_id = sanitize_name(file_id)
    return FILE.format(scan_id=scan_id, fileset_id=fileset_id, file_id=file_id)


@url_prefix
def file_metadata(
    scan_id: str, fileset_id: str, file_id: str, key: str | None = None, **kwargs
) -> str:
    """URL to access the file metadata associated with the given scan and fileset name.

    Parameters
    ----------
    scan_id : str
        The name of the scan to access.
    fileset_id : str
        The name of the fileset to access.
    file_id : str
        The name of the file to access.
    key : str
        A specific metadata key to fetch.

    Other Parameters
    ----------------
    prefix : str
        An optional prefix to prepend to the URL path.

    Returns
    -------
    str
        The URL path to file metadata.

    Examples
    --------
    >>> from plantdb.commons import api_endpoints
    >>> api_endpoints.file_metadata('real_plant', 'images', '00000_rgb')
    '/api/v1/files/real_plant/images/00000_rgb/metadata'
    """
    scan_id = sanitize_name(scan_id)
    fileset_id = sanitize_name(fileset_id)
    file_id = sanitize_name(file_id)

    # Assemble optional query parameters
    query: dict[str, str] = {}
    if key is not None:
        query["key"] = str(key)

    query_str = f"?{parse.urlencode(query)}" if query else ""
    return (
        FILE_MD.format(scan_id=scan_id, fileset_id=fileset_id, file_id=file_id)
        + f"{query_str}"
    )


# ------------------------------------------------------------------------
# - Asset Endpoints
# ------------------------------------------------------------------------


@url_prefix
def image(
    scan_id: str,
    fileset_id: str,
    file_id: str,
    size: int | str | None = None,
    as_base64: bool | None = None,
    **kwargs,
) -> str:
    """Return the URL path to the image endpoint.

    Parameters
    ----------
    scan_id : str
        The name of the scan dataset containing the image.
    fileset_id : str
        The name of the fileset containing the image.
    file_id : str
        The name of the image.
    size : str or int, optional
        The size parameter of the image request.
    as_base64 : bool, optional
        A boolean flag indicating whether to return an image as a base64 string.

    Other Parameters
    ----------------
    prefix : str
        An optional prefix to prepend to the URL path.

    Returns
    -------
    str
        The URL path to the image endpoint.

    Examples
    --------
    >>> from plantdb.commons import api_endpoints
    >>> api_endpoints.image('real_plant', 'images', '00000_rgb', 'orig', False)
    '/api/v1/assets/image/real_plant/images/00000_rgb?size=orig&as_base64=false'
    >>> api_endpoints.image('real_plant', 'images', '00000_rgb', 'thumb', True)
    '/api/v1/assets/image/real_plant/images/00000_rgb?size=thumb&as_base64=true'
    """
    scan_id = sanitize_name(scan_id)
    fileset_id = sanitize_name(fileset_id)
    file_id = sanitize_name(file_id)

    # Assemble optional query parameters
    query: dict[str, str] = {}
    if size is not None:
        query["size"] = str(size)
    if as_base64 is not None:
        # Use lower-case JSON-style booleans for consistency
        query["as_base64"] = str(as_base64).lower()

    query_str = f"?{parse.urlencode(query)}" if query else ""
    return (
        IMAGE.format(scan_id=scan_id, fileset_id=fileset_id, file_id=file_id)
        + f"{query_str}"
    )


@url_prefix
def sequence(scan_id: str, seq_type: str | None = None, **kwargs) -> str:
    """Return the URL path to the sequence endpoint.

    Parameters
    ----------
    scan_id : str
        The name of the scan dataset containing the angles and internodes sequence.
    seq_type : str, optional
        The type of measure to request, in ``['all', 'angles', 'internodes', 'fruit_points',
         'manual_angles', 'manual_internodes']``.

    Other Parameters
    ----------------
    prefix : str
        An optional prefix to prepend to the URL path.

    Returns
    -------
    str
        The URL path to the angles and internodes sequences endpoint.

    Examples
    --------
    >>> from plantdb.commons import api_endpoints
    >>> api_endpoints.sequence('real_plant','all')
    '/api/v1/assets/sequence/real_plant?type=all'
    """
    valid_types = [
        "all",
        "angles",
        "internodes",
        "fruit_points",
        "manual_angles",
        "manual_internodes",
    ]
    seq_type = "all" if seq_type not in valid_types else seq_type
    scan_id = sanitize_name(scan_id)

    # Assemble optional query parameters
    query: dict[str, str] = {}
    if seq_type is not None:
        query["type"] = str(seq_type)

    query_str = f"?{parse.urlencode(query)}" if query else ""
    return SEQUENCE.format(scan_id=scan_id) + f"{query_str}"


@url_prefix
def pointcloud(
    scan_id: str,
    size: int | float | str | None = None,
    coords: bool | None = None,
    pcd_type: str = "default",
    **kwargs,
) -> str:
    """Return the URL path to the point-cloud endpoint.

    Parameters
    ----------
    scan_id : str
        The name of the scan dataset containing the point-cloud.
    size : str or int or float, optional
        Query parameter controlling downsampling.
        Accepted values:
            * 'orig' - serve the original point cloud.
            * 'preview' - serve a precomputed preview (default).
            * A float value - perform on-the-fly voxel downsampling using the specified voxel size.
        If an invalid string is supplied, the default 'preview' is used.
    coords : bool, optional
        Query parameter indicating whether to return the point coordinates as JSON.
        Defaults to 'false', which streams the PLY file.
        If set, returns the data as a list under the 'coordinates' JSON dictionary entry.
    pcd_type : str or int, optional
        Query parameter indicating whether to return the reconstructed point cloud (default) or
        the ground truth ('type=gt').

    Other Parameters
    ----------------
    prefix : str
        An optional prefix to prepend to the URL path.

    Returns
    -------
    str
        The URL path to the point-cloud endpoint.

    Examples
    --------
    >>> from plantdb.commons import api_endpoints
    >>> api_endpoints.pointcloud('real_plant')
    '/api/v1/assets/pointcloud/real_plant?type=default'
    >>> api_endpoints.pointcloud('real_plant', pcd_type='gt')
    '/api/v1/assets/pointcloud/real_plant?type=gt'
    >>> api_endpoints.pointcloud('real_plant', coords=True)
    '/api/v1/assets/pointcloud/real_plant?type=default&coords=True'
    """
    VALID_SIZES = {"orig", "preview"}
    VALID_TYPES = ["default", "gt"]
    seq_type = "" if pcd_type not in VALID_TYPES else pcd_type
    scan_id = sanitize_name(scan_id)

    # Assemble optional query parameters
    query: dict[str, str] = {}
    if size is not None:
        if isinstance(size, (int, float)):
            query["size"] = str(size)
        elif isinstance(size, str) and size in VALID_SIZES:
            query["size"] = size.lower()
        else:
            raise ValueError(
                f"Invalid size '{size}'. Valid options: integer value or {VALID_SIZES}"
            )
    if seq_type is not None:
        query["type"] = str(seq_type)
    if coords is not None:
        query["coords"] = str(coords)

    query_str = f"?{parse.urlencode(query)}" if query else ""
    return POINTCLOUD.format(scan_id=scan_id) + f"{query_str}"


@url_prefix
def mesh(
    scan_id: str, size: int | str | None = None, coords: bool | None = None, **kwargs
) -> str:
    """Return the URL path to the mesh endpoint.

    Parameters
    ----------
    scan_id : str
        The name of the scan dataset containing the mesh.
    size : str or int, optional
        Query parameter controlling downsampling.
        Accepted values:
            * 'orig' - serve the original point cloud.
    coords : bool, optional
        Query parameter indicating whether to return the vertices coordinates and triangle IDs as JSON.
        Defaults to 'false', which streams the PLY file.

    Other Parameters
    ----------------
    prefix : str
        An optional prefix to prepend to the URL path.

    Returns
    -------
    str
        The URL path to the mesh endpoint.

    Examples
    --------
    >>> from plantdb.commons import api_endpoints
    >>> api_endpoints.mesh('real_plant')
    '/api/v1/assets/mesh/real_plant'
    >>> api_endpoints.mesh('real_plant', coords=True)
    """
    VALID_SIZES = {"orig"}
    scan_id = sanitize_name(scan_id)

    # Assemble optional query parameters
    query: dict[str, str] = {}
    if size is not None:
        if isinstance(size, int):
            query["size"] = str(size)
        elif isinstance(size, str) and size in VALID_SIZES:
            query["size"] = size.lower()
        else:
            raise ValueError(
                f"Invalid size '{size}'. Valid options: integer value or {VALID_SIZES}"
            )
    if coords is not None:
        query["coords"] = str(coords)

    query_str = f"?{parse.urlencode(query)}" if query else ""
    return MESH.format(scan_id=scan_id) + f"{query_str}"


@url_prefix
def skeleton(scan_id: str, **kwargs) -> str:
    """Return the URL path to the skeleton endpoint.

    Parameters
    ----------
    scan_id : str
        The name of the scan dataset containing the skeleton.

    Other Parameters
    ----------------
    prefix : str
        An optional prefix to prepend to the URL path.

    Returns
    -------
    str
        The URL path to the skeleton endpoint.

    Examples
    --------
    >>> from plantdb.commons import api_endpoints
    >>> api_endpoints.skeleton('real_plant')
    '/api/v1/assets/skeleton/real_plant'
    """
    scan_id = sanitize_name(scan_id)
    return SKELETON.format(scan_id=scan_id)


@url_prefix
def archive(scan_id: str, **kwargs) -> str:
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
        The URL path to the dataset archive endpoint.

    Examples
    --------
    >>> from plantdb.commons import api_endpoints
    >>> api_endpoints.archive('scan1')
    '/api/v1/assets/archive/scan1'
    """
    scan_id = sanitize_name(scan_id)
    return ARCHIVE.format(scan_id=scan_id)


@url_prefix
def file_path(file_path: str, **kwargs) -> str:
    """Return the URL path to the `scan/file_path` endpoint.

    Parameters
    ----------
    scan_id : str
        The name of the scan dataset containing the file.
    file_path : str
        The path to the file in the database.

    Other Parameters
    ----------------
    prefix : str
        An optional prefix to prepend to the URL path.

    Returns
    -------
    str
        The URL path to the file path endpoint.

    Examples
    --------
    >>> from plantdb.commons import api_endpoints
    >>> api_endpoints.file_path('real_plant/images/00000_rgb.jpg')
    '/api/v1/assets/files/real_plant/images/00000_rgb.jpg'
    """
    return FILE_PATH.format(file_path=file_path.lstrip("/"))
