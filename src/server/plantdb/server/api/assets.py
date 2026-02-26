#!/usr/bin/env python
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
# Assets REST API Resources

Provides a collection of Flask‑RESTful resources for exposing plant‑database assets over HTTP.
The module enables serving and managing files, images, point clouds, meshes, curve skeletons,
sequence data, and whole‑dataset archives, making it easy to build a REST API that gives
programmatic access to plant scan data.

## Key Features

- **Resource classes**: `File`, `DatasetFile`, `Image`, `PointCloud`, `PointCloudGroundTruth`,
  `Mesh`, `CurveSkeleton`, `Sequence`, `Archive`.
- **Safety**: automatic input sanitization, directory‑traversal protection, and
  rate‑limiting decorators on every endpoint.
- **Flexible output**: optional resizing, thumbnail generation, base‑64 encoding,
  and on‑the‑fly down‑sampling for large assets (point clouds, meshes, images).
- **Archive handling**: creation, validation, and extraction of ZIP archives with
  robust error handling and temporary‑file cleanup.
- **Utility helpers**: `is_within_directory` and `is_directory_in_archive` for
  safe path checks inside the filesystem and ZIP files.

## Usage Examples

Hereafter is a minimal working example that:

1. Creates a `Flask` app
2. Sets up a local test database with a JSON Web Token session manager
3. Registers the `Login` and `Logout` resources to a REST API
4. Starts the app

```python
>>> import logging
>>> from flask import Flask
>>> from flask_restful import Api
>>> from plantdb.server.api.assets import File
>>> from plantdb.commons.auth.session import JWTSessionManager
>>> from plantdb.commons.fsdb.core import FSDB
>>> from plantdb.commons.test_database import setup_test_database
>>> # Create a Flask application
>>> app = Flask(__name__)
>>> # Create a logger
>>> logger = logging.getLogger("plantdb.assets")
>>> logger.setLevel(logging.INFO)
>>> # Initialize a test database with a JWTSessionManager
>>> db_path = setup_test_database('real_plant')
>>> mgr = JWTSessionManager()
>>> db = FSDB(db_path, session_manager=mgr)
>>> db.connect()
>>> # RESTful API and resource registration
>>> api = Api(app)
>>> api.add_resource(File, '/files/<path:path>', resource_class_kwargs={"db": db})
>>> # Start the API
>>> app.run(host='0.0.0.0', port=5000)
```

It may be used as follows (in another Python REPL):
```python
>>> import requests
>>> import toml
>>> # Request a TOML configuration file
>>> response = requests.get("http://127.0.0.1:5000/files/real_plant/scan.toml")
>>> cfg = toml.loads(response.content.decode())
>>> print(cfg['ScanPath']['class_name'])
Circle
```
"""

import json
import mimetypes
import os
import pathlib
from http.client import HTTPException
from io import BytesIO
from pathlib import Path
from zipfile import ZipFile

import numpy as np
import pybase64
import requests
from flask import after_this_request
from flask import jsonify
from flask import make_response
from flask import request
from flask import send_file
from flask import send_from_directory
from flask_restful import Resource

from plantdb.commons.fsdb.exceptions import FilesetNotFoundError
from plantdb.commons.fsdb.exceptions import ScanNotFoundError
from plantdb.commons.io import read_json
from plantdb.server import webcache
from plantdb.server.core.security import add_jwt_from_header
from plantdb.server.core.security import rate_limit
from plantdb.server.core.utils import compute_fileset_matches
from plantdb.server.core.utils import sanitize_name
from plantdb.server.services.assets import ArchiveError
from plantdb.server.services.assets import ExtractionError
from plantdb.server.services.assets import FileUploadError
from plantdb.server.services.assets import ValidationError
from plantdb.server.services.assets import _ensure_file_present
from plantdb.server.services.assets import _ensure_valid_structure
from plantdb.server.services.assets import _save_to_temp
from plantdb.server.services.assets import _test_zip_integrity
from plantdb.server.services.assets import _validate_extension
from plantdb.server.services.assets import _validate_mime_type
from plantdb.server.services.assets import create_zip_for_scan
from plantdb.server.services.assets import ensure_directory
from plantdb.server.services.assets import extract_zip_to_scan
from plantdb.server.services.assets import get_scan_path
from plantdb.server.services.assets import validate_upload_headers
from plantdb.server.services.assets import write_file
from plantdb.server.services.assets import write_streamed_file


class File(Resource):
    """A RESTful resource class for serving files via HTTP GET requests.

    This class implements a REST API endpoint that serves files from a specified database location.

    Attributes
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        A database instance containing the file path configuration.

    Notes
    -----
    The class requires proper initialization with A database instance that
    provides a valid path() method for file location resolution.
    """

    def __init__(self, db):
        """Initialize the resource.

        Parameters
        ----------
        db : plantdb.commons.fsdb.core.FSDB
            A database instance providing access to file locations.
        """
        self.db = db

    @rate_limit(max_requests=120, window_seconds=60)
    def get(self, path):
        """Serve a file from the database directory via HTTP.

        This method handles GET requests by serving the requested file from
        the configured database directory. It uses Flask's `send_from_directory`
        to safely serve the file.

        Parameters
        ----------
        path : str
            Relative path to the requested file within the database directory.
            This path will be resolved against the database root path.

        Returns
        -------
        flask.Response
            A Flask response object containing the requested file or an
            appropriate error response if the file is not found.

        Raises
        ------
        werkzeug.exceptions.NotFound
            If the requested file does not exist
        werkzeug.exceptions.Forbidden
            If the file access is forbidden
        http.client.HTTPException
             If the rate limit is exceeded, it returns an HTTP 429 ("Too Many Requests") response to the client.

        Notes
        -----
        The file serving is handled securely through Flask's `send_from_directory`,
        which prevents directory traversal attacks and handles file access permissions.

        Examples
        --------
        >>> # Start the REST API server (in test mode)
        >>> # fsdb_rest_api --test
        >>> import requests
        >>> import toml
        >>> # Request a TOML configuration file
        >>> response = requests.get("http://127.0.0.1:5000/files/real_plant_analyzed/pipeline.toml")
        >>> cfg = toml.loads(response.content.decode())
        >>> print(cfg['Undistorted'])
        {'upstream_task': 'ImagesFilesetExists'}
        >>> # Request a JSON file
        >>> response = requests.get("http://127.0.0.1:5000/files/real_plant_analyzed/files.json")
        >>> scan_files = response.json()
        >>> print([fs['id'] for fs in scan_files['filesets']])
        ['images', 'AnglesAndInternodes_1_0_2_0_6_0_6dd64fc595', 'TreeGraph__False_CurveSkeleton_c304a2cc71', 'CurveSkeleton__TriangleMesh_0393cb5708', 'TriangleMesh_9_most_connected_t_open3d_00e095c359', 'PointCloud_1_0_1_0_10_0_7ee836e5a9', 'Voxels___x____300__450__colmap_camera_False_2a093f0ccc', 'Masks_1__0__1__0____channel____rgb_5619aa428d', 'Colmap_True_null_SIMPLE_RADIAL_ffcef49fdc', 'Undistorted_SIMPLE_RADIAL_Colmap__a333f181b7']
        """
        return send_from_directory(self.db.path(), path)


class DatasetFile(Resource):
    """A RESTful resource handler for file upload operations in a plant database system.

    Attributes
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        Database instance that provides access to scan data and file locations.
        Used for validating scan IDs and determining file storage paths.

    Notes
    -----
    File operations are performed with proper error handling and cleanup
    of partial uploads in case of failures.

    See Also
    --------
    plantdb.server.rest_api.ScansList : Resource for managing scan listings
    plantdb.server.rest_api.File : Resource for file retrieval operations
    """

    def __init__(self, db, logger):
        """Initialize the resource.

        Parameters
        ----------
        db : plantdb.commons.fsdb.core.FSDB
            A database instance providing access to file locations.
        logger : logging.Logger
            A logger instance for recording operations and errors.
        """
        self.db = db
        self.logger = logger

    @rate_limit(max_requests=30, window_seconds=60)
    def post(self, scan_id):
        """Handle POST request to upload and save a file to the server.

        This endpoint processes file uploads and saves them to the specified location. It supports
        both full file uploads and chunked uploads based on the provided headers. The method
        ensures data integrity by validating the received file size against the Content-Length.

        Parameters
        ----------
        scan_id : str
            Unique identifier for the scan associated with the file upload. Used to determine
            the base storage path for the file.

        Returns
        -------
        flask.Response
            JSON response with status code and message:

            - 201: Successful upload with a message confirming file save
            - 400: Bad request (missing headers or invalid parameters)
            - 500: Server error during file processing

        Notes
        -----
        Required HTTP headers:

        - 'Content-Disposition': Contains file information
        - 'Content-Length': Size of the file in bytes
        - 'X-File-Path': Relative path where the file should be saved
        - 'X-Chunk-Size' (optional): Size of chunks for streamed upload

        The method will automatically create any necessary directories in the path.
        Partial uploads are automatically cleaned up if they fail.

        Raises
        ------
        Exception
            When database access fails or file operations encounter errors.
            All exceptions are caught and returned as HTTP 400 or 500 responses.
        http.client.HTTPException
             If the rate limit is exceeded, it returns an HTTP 429 ("Too Many Requests") response to the client.

        See Also
        --------
        plantdb.commons.io.write_stream
        plantdb.commons.io.write_data

        Examples
        --------
        >>> # Start the REST API server (in test mode)
        >>> # fsdb_rest_api --test
        >>> # Request a TOML configuration file
        >>> import requests
        >>> import toml
        >>> # Example POST request with required headers
        >>> headers = {
        ...     'Content-Disposition': 'attachment; filename=data.txt',
        ...     'Content-Length': '1024',
        ...     'X-File-Path': 'path/to/data.txt'
        ... }
        >>> response = requests.post(
        ...     'http://api/scans/scan123/files',
        ...     headers=headers,
        ...     data=file_content
        ... )
        >>> response.status_code
        201
        >>> response.json()
        {'message': 'File path/to/data.txt received and saved'}
        """
        try:
            # 1.  Validate request headers and extract useful values
            headers = validate_upload_headers(request.headers)

            # 2️. Resolve the target location on the filesystem
            scan_root: Path = get_scan_path(self.db, scan_id)
            file_path: Path = scan_root / headers["rel_filename"]
            ensure_directory(file_path.parent)

            # 3. Persist the payload
            if headers["chunk_size"] == 0:
                bytes_written = write_file(file_path, request.data)
            else:
                bytes_written = write_streamed_file(
                    file_path,
                    headers["content_length"],
                    headers["chunk_size"],
                )

            # 4. Verify the received size matches the declared size
            if bytes_written != headers["content_length"]:
                file_path.unlink(missing_ok=True)
                raise FileUploadError(
                    f"Received {bytes_written} bytes, expected "
                    f"{headers['content_length']} bytes"
                )

        except FileUploadError as exc:
            self.logger.error("File upload failed: %s", exc)
            return make_response(jsonify({"error": str(exc)}), 500)

        except HTTPException:
            # Let Flask‑RESTful propagate rate‑limit (429) or other HTTP errors.
            raise

        except Exception as exc:  # pragma: no cover - defensive fallback
            self.logger.exception("Unexpected error while handling file upload")
            return make_response(jsonify({"error": str(exc)}), 400)

        message = f"File {headers['rel_filename']} received and saved"
        self.logger.info(message)
        return make_response(jsonify({"message": message}), 201)


class Image(Resource):
    """RESTful resource for serving and resizing images on demand.

    This class handles HTTP GET requests for images stored in the database,
    with optional resizing capabilities. It serves both original and
    thumbnail versions of images based on the request parameters.

    Attributes
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        Database instance containing the image data.

    Notes
    -----
    The class sanitizes all input parameters to prevent path traversal
    attacks and ensure valid file access.
    """

    def __init__(self, db):
        """Initialize the Image resource with a database connection.

        Parameters
        ----------
        db : plantdb.commons.fsdb.core.FSDB
            Database instance for accessing stored images.
        """
        self.db = db

    @staticmethod
    def wants_base64(request) -> bool:
        """
        Return ``True`` when the query string contains ``as_base64`` with a truthy value.
        """
        flag = request.args.get('as_base64', default='false', type=str).lower()
        return flag in ('true', '1', 'yes')

    @rate_limit(max_requests=3000, window_seconds=60)
    def get(self, scan_id, fileset_id, file_id):
        """Retrieve and serve an image from the database.

        Handles image retrieval requests, optionally resizing the image
        based on the 'size' query parameter. Supports both original size
        and thumbnail versions.

        Parameters
        ----------
        scan_id : str
            Identifier for the scan containing the image.
        fileset_id : str
            Identifier for the fileset within the scan.
        file_id : str
            Identifier for the specific image file.

        Other Parameters
        ----------------
        size : str or float
            Query parameter controlling downsampling.
            Accepted values:
                * `'thumb'`: image max width and height to `150` (default);
                * `'large'`: image max width and height to `1500`;
                * `'orig'`: original image, no chache;
            If an invalid string is supplied, the default 'thumb' is used.
        as_base64 : str
            Query parameter indicating whether to return the image encoded in base64.
            Accepts 'true', '1', 'yes' (case‑insensitive) to enable.
            Defaults to 'false', which streams the image file.
            If set, returns the image in base64 under the 'image' JSON dictionary entry and mimetype under 'content-type'.

        Returns
        -------
        flask.Response
            HTTP response containing the image data with 'content-type' mimetype.

        Raises
        ------
        werkzeug.exceptions.NotFound
            If the requested image file doesn't exist.
        http.client.HTTPException
             If the rate limit is exceeded, it returns an HTTP 429 ("Too Many Requests") response to the client.

        Notes
        -----
        - All input parameters are sanitized before use.

        See Also
        --------
        plantdb.server.rest_api.sanitize_name : Input sanitization and validation function.
        plantdb.server.webcache.image_path : Image path resolution function with caching and resizing options.

        Examples
        --------
        >>> # In a terminal, start a (test) REST API with `fsdb_rest_api --test`, then:
        >>> import numpy as np
        >>> import requests
        >>> import pybase64
        >>> from io import BytesIO
        >>> from PIL import Image
        >>> # Example #1 - Get the first image as a thumbnail (default):
        >>> response = requests.get("http://127.0.0.1:5000/image/real_plant_analyzed/images/00000_rgb", stream=True)
        >>> img = Image.open(BytesIO(response.content))
        >>> image.show()
        >>> np.asarray(img).shape
        (113, 150, 3)
        >>> # Example #2 - Get the first image in original size:
        >>> response = requests.get("http://127.0.0.1:5000/image/real_plant_analyzed/images/00000_rgb", stream=True, params={"size": "orig"})
        >>> img = Image.open(BytesIO(response.content))
        >>> image.show()
        >>> np.asarray(img).shape
        (1080, 1440, 3)
        >>> # Example #3 - Get a base64 encoded image:
        >>> response = requests.get("http://127.0.0.1:5000/image/real_plant_analyzed/images/00000_rgb", stream=True, params={"size": "orig", "as_base64": 'true'})
        >>> print(response.json()['content-type'])
        'image/jpeg'
        >>> b64_string = response.json()['image']
        >>> print(b64_string[:30])  # print the first 30 characters
        '/9j/4AAQSkZJRgABAQAAAQABAAD/2w'
        >>> image_data = pybase64.b64decode(b64_string)
        >>> image = Image.open(BytesIO(image_data))
        >>> image.show()
        """
        # Sanitize identifiers
        scan_id = sanitize_name(scan_id)
        fileset_id = sanitize_name(fileset_id)
        file_id = sanitize_name(file_id)

        # Parse the `size` flag
        size = request.args.get('size', default='thumb', type=str)
        # Get the path to the image resource:
        path = webcache.image_path(self.db, scan_id, fileset_id, file_id, size)
        mime_type, _ = mimetypes.guess_type(path)

        if self.wants_base64(request):
            # ---------- JSON (base64) ----------
            with open(path, 'rb') as f:
                b64_str = pybase64.b64encode(f.read()).decode('ascii')
            # ``decode('ascii')`` gives us a plain string that can be JSON‑encoded.
            payload = {
                "image": b64_str,
                "content-type": mime_type
            }
            # Wrap ``jsonify`` with ``make_response`` to add custom headers
            resp = make_response(jsonify(payload))
            resp.headers["Content-Type"] = "application/json"
            resp.headers["X-Content-Encoding"] = "base64"
        else:
            # ---------- Binary (streaming) ----------
            resp = make_response(send_file(path, mimetype=mime_type))
            resp.headers["X-Content-Encoding"] = "binary"

        return resp


class PointCloud(Resource):
    """RESTful resource for serving and optionally downsampling point cloud data.

    This class handles HTTP GET requests for point cloud data stored in PLY format,
    with support for different sampling densities. It can serve both original and
    preview versions of point clouds, or custom downsampling based on voxel size.

    Attributes
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        Database instance containing the point cloud data.

    Notes
    -----
    The class sanitizes all input parameters to prevent path traversal attacks
    and ensures valid file access. Point clouds are served in PLY format with
    'application/octet-stream' mimetype.
    """

    def __init__(self, db):
        """Initialize the PointCloud resource with a database connection.

        Parameters
        ----------
        db : plantdb.commons.fsdb.core.FSDB
            Database instance for accessing stored point cloud data.
        """
        self.db = db

    @rate_limit(max_requests=5, window_seconds=60)
    def get(self, scan_id, fileset_id, file_id):
        """Retrieve and serve a point cloud from the database.

        Handles point cloud retrieval requests with optional downsampling based on
        the 'size' query parameter. Supports original size, preview, and custom
        voxel-based downsampling.

        Parameters
        ----------
        scan_id : str
            Identifier for the scan containing the point cloud.
        fileset_id : str
            Identifier for the fileset within the scan.
        file_id : str
            Identifier for the specific point cloud file.

        Other Parameters
        ----------------
        size : str or float
            Query parameter controlling downsampling.
            Accepted values:
                * 'orig' - serve the original point cloud.
                * 'preview' - serve a precomputed preview (default).
                * A float value - perform on‑the‑fly voxel downsampling using the specified voxel size.
            If an invalid string is supplied, the default 'preview' is used.
        coords : str
            Query parameter indicating whether to return the point coordinates as JSON.
            Accepts 'true', '1', 'yes' (case‑insensitive) to enable.
            Defaults to 'false', which streams the PLY file.
            If set, returns the data as list under the 'coordinates' JSON dictionary entry.

        Returns
        -------
        flask.Response
            HTTP response containing the PLY data with 'application/octet-stream' mimetype.

        Raises
        ------
        werkzeug.exceptions.NotFound
            If the requested point-cloud file doesn't exist.
        http.client.HTTPException
             If the rate limit is exceeded, it returns an HTTP 429 ("Too Many Requests") response to the client.

        Notes
        -----
        - All input parameters are sanitized before use

        See Also
        --------
        plantdb.server.rest_api.sanitize_name : Input sanitization & validation function.
        plantdb.server.webcache.pointcloud_path : Point cloud path resolution function with caching and downsampling options.

        Examples
        --------
        >>> # In a terminal, start a (test) REST API with `fsdb_rest_api --test`, then:
        >>> import requests
        >>> from plyfile import PlyData
        >>> from io import BytesIO
        >>> # Get original point cloud:
        >>> response = requests.get("http://127.0.0.1:5000/pointcloud/real_plant_analyzed/PointCloud_1_0_1_0_10_0_7ee836e5a9/PointCloud")
        >>> pcd_data = PlyData.read(BytesIO(response.content))
        >>> # Access point X-coordinates:
        >>> list(pcd_data['vertex']['x'])
        >>> # Get preview (downsampled) version
        >>> response = requests.get("http://127.0.0.1:5000/pointcloud/real_plant_analyzed/PointCloud_1_0_1_0_10_0_7ee836e5a9/PointCloud", params={"size": "preview"})
        >>> # Get custom downsampled version (voxel size 0.01)
        >>> response = requests.get("http://127.0.0.1:5000/pointcloud/real_plant_analyzed/PointCloud_1_0_1_0_10_0_7ee836e5a9/PointCloud", params={"size": "0.01"})
        >>> # Send the coordinates (read the file on the server-side)
        >>> response = requests.get("http://127.0.0.1:5000/pointcloud/real_plant_analyzed/PointCloud_1_0_1_0_10_0_7ee836e5a9/PointCloud", params={"size": "preview", 'coords': 'true'})
        >>> coordinates = np.array(response.json()['coordinates'])
        """
        # Sanitize identifiers
        scan_id = sanitize_name(scan_id)
        fileset_id = sanitize_name(fileset_id)
        file_id = sanitize_name(file_id)

        # Parse the `size` flag
        size = request.args.get('size', default='preview', type=str)
        # Try to convert the 'size' argument as a float:
        try:
            vxs = float(size)
        except ValueError:
            pass
        else:
            size = vxs
        # If a string, make sure that the 'size' argument we got is a valid option, else default to 'preview':
        if isinstance(size, str) and size not in ['orig', 'preview']:
            size = 'preview'

        # Parse the coords flag (accepting true/1/yes in any case)
        coords_flag = request.args.get('coords', default='false', type=str).lower() in ('true', '1', 'yes')

        try:
            # Get the path to the pointcloud resource:
            path = webcache.pointcloud_path(self.db, scan_id, fileset_id, file_id, size)
        except FileNotFoundError:
            return {'error': f"No '{scan_id}/{fileset_id}/{file_id}' file found!"}, 400
        except FilesetNotFoundError:
            return {'error': f"No '{scan_id}/{fileset_id}' fileset found!"}, 400
        except ScanNotFoundError:
            return {'error': f"No '{scan_id}' scan found!"}, 400
        except Exception as e:
            return {'error': f"Unknown error: {e}"}, 400

        # If coords_flag is set, read the file and return JSON
        if coords_flag:
            import numpy as np
            from open3d import io
            pcd = io.read_point_cloud(path, print_progress=False)
            # Convert the Open3D Vector3dVector to a plain Python list so JSON can serialize it.
            return jsonify({'coordinates': np.array(pcd.points).tolist()})
        # Otherwise, return the file directly
        return send_file(path, mimetype='application/octet-stream')


class PointCloudGroundTruth(Resource):
    """A RESTful resource for serving ground-truth point-cloud data.

    This class handles HTTP GET requests for point-cloud data, with optional
    downsampling capabilities based on the requested size parameter.

    Attributes
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        The database instance used to retrieve point-cloud data.
    """

    def __init__(self, db):
        """Initialize the PointCloudGroundTruth resource.

        Parameters
        ----------
        db : plantdb.commons.fsdb.core.FSDB
            Database instance providing access to the point-cloud data.
        """
        self.db = db

    @rate_limit(max_requests=5, window_seconds=60)
    def get(self, scan_id, fileset_id, file_id):
        """Retrieve and serve a ground-truth point-cloud file.

        Fetches the requested point-cloud data from the cache, potentially
        downsampling it based on the size parameter provided in the query string.

        Parameters
        ----------
        scan_id : str
            Identifier for the scan to retrieve.
        fileset_id : str
            Identifier for the fileset within the scan.
        file_id : str
            Identifier for the specific point-cloud file.

        Other Parameters
        ----------------
        size : str or float
            Query parameter controlling downsampling.
            Accepted values:
                * 'orig' - serve the original point cloud.
                * 'preview' - serve a precomputed preview (default).
                * A float value - perform on‑the‑fly voxel downsampling using the specified voxel size.
            If an invalid string is supplied, the default 'preview' is used.
        coords : str
            Query parameter indicating whether to return the point coordinates as JSON.
            Accepts 'true', '1', 'yes' (case‑insensitive) to enable.
            Defaults to 'false', which streams the PLY file.
            If set, returns the data as list under the 'coordinates' JSON dictionary entry.

        Returns
        -------
        flask.Response
            HTTP response containing the point-cloud data as an octet-stream.

        Raises
        ------
        werkzeug.exceptions.NotFound
            If the requested point-cloud file doesn't exist.
        http.client.HTTPException
             If the rate limit is exceeded, it returns an HTTP 429 ("Too Many Requests") response to the client.

        Notes
        -----
        - In the URL, you can use the 'size' parameter to specify the size of the point-cloud:
            * 'orig': Original size
            * 'preview': Preview size (default)
            * A float value: Custom voxel size for downsampling
        - All identifiers are sanitized before use
        - Invalid size parameters default to 'preview'
        - Response mimetype is 'application/octet-stream'
        """
        # Sanitize identifiers
        scan_id = sanitize_name(scan_id)
        fileset_id = sanitize_name(fileset_id)
        file_id = sanitize_name(file_id)

        # Parse the `size` flag
        size = request.args.get('size', default='preview', type=str)
        # Try to convert the 'size' argument as a float:
        try:
            vxs = float(size)
        except ValueError:
            pass
        else:
            size = vxs
        # If a string, make sure that the 'size' argument we got is a valid option, else default to 'preview':
        if isinstance(size, str) and size not in ['orig', 'preview']:
            size = 'preview'

        # Parse the coords flag (accepting true/1/yes in any case)
        coords_flag = request.args.get('coords', default='false', type=str).lower() in ('true', '1', 'yes')

        try:
            # Get the path to the pointcloud resource:
            path = webcache.pointcloud_path(self.db, scan_id, fileset_id, file_id, size)
        except FileNotFoundError:
            return {'error': f"No '{scan_id}/{fileset_id}/{file_id}' file found!"}, 400
        except FilesetNotFoundError:
            return {'error': f"No '{scan_id}/{fileset_id}' fileset found!"}, 400
        except ScanNotFoundError:
            return {'error': f"No '{scan_id}' scan found!"}, 400
        except Exception as e:
            return {'error': f"Unknown error: {e}"}, 400

        # If coords_flag is set, read the file and return JSON
        if coords_flag:
            import numpy as np
            from open3d import io
            pcd = io.read_point_cloud(path, print_progress=False)
            # Convert the Open3D Vector3dVector to a plain Python list so JSON can serialize it.
            return jsonify({'coordinates': np.array(pcd.points).tolist()})
        # Otherwise, return the file directly
        return send_file(path, mimetype='application/octet-stream')


class Mesh(Resource):
    """RESTful resource for serving triangular mesh data via HTTP.

    This class implements a REST endpoint that provides access to triangular mesh data
    stored in a database. It supports GET requests and can optionally handle mesh
    size parameters.

    Attributes
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        Reference to the database instance.

    Notes
    -----
    The mesh data is served in PLY format as an octet-stream.
    """

    def __init__(self, db):
        """Initialize the Mesh resource.

        Parameters
        ----------
        db : plantdb.commons.fsdb.core.FSDB
            The database instance containing the mesh data.
        """
        self.db = db

    @rate_limit(max_requests=5, window_seconds=60)
    def get(self, scan_id, fileset_id, file_id):
        """Retrieve and serve a triangular mesh file.

        This method handles GET requests for mesh data, supporting optional size
        parameters. It sanitizes input parameters and serves the mesh file from
        the cache.

        Parameters
        ----------
        scan_id : str
            Identifier for the scan containing the mesh.
        fileset_id : str
            Identifier for the fileset within the scan.
        file_id : str
            Identifier for the specific mesh file.

        Other Parameters
        ----------------
        coords : str
            Query parameter indicating whether to return the vertices coordinates and triangle IDs as JSON.
            Accepts 'true', '1', 'yes' (case‑insensitive) to enable.
            Defaults to 'false', which streams the PLY file.
            If set, returns the data as list under the 'vertices' & 'triangles' JSON dictionary entry.

        Returns
        -------
        flask.Response
            HTTP response containing the mesh data as an octet-stream.

        Raises
        ------
        werkzeug.exceptions.NotFound
            If the requested mesh file doesn't exist
        http.client.HTTPException
             If the rate limit is exceeded, it returns an HTTP 429 ("Too Many Requests") response to the client.

        Notes
        -----
        - In the URL, you can use the `size` parameter to retrieve a resized mesh.
        - The 'size' parameter currently only supports 'orig' value
        - All identifiers are sanitized before use
        - The mesh is served as a binary PLY file

        See Also
        --------
        plantdb.server.rest_api.sanitize_name : Function used to validate input parameters
        plantdb.server.webcache.mesh_path : Function to retrieve mesh file path

        Examples
        --------
        >>> # In a terminal, start a (test) REST API with `fsdb_rest_api --test`, then:
        >>> import requests
        >>> from plyfile import PlyData
        >>> from io import BytesIO
        >>> # Request a mesh file
        >>> url = "http://127.0.0.1:5000/mesh/real_plant_analyzed/TriangleMesh_9_most_connected_t_open3d_00e095c359/TriangleMesh"
        >>> response = requests.get(url)
        >>> # Parse the PLY data
        >>> mesh_data = PlyData.read(BytesIO(response.content))
        >>> # Access vertex coordinates
        >>> vertices = mesh_data['vertex']
        >>> x_coords = list(vertices['x'])
        """
        # Sanitize identifiers
        scan_id = sanitize_name(scan_id)
        fileset_id = sanitize_name(fileset_id)
        file_id = sanitize_name(file_id)

        # Parse the `size` flag
        size = request.args.get('size', default='orig', type=str)
        # Make sure that the 'size' argument we got is a valid option, else default to 'orig':
        if not size in ['orig']:
            size = 'orig'

        # Parse the coords flag (accepting true/1/yes in any case)
        coords_flag = request.args.get('coords', default='false', type=str).lower() in ('true', '1', 'yes')

        try:
            # Get the path to the mesh resource:
            path = webcache.mesh_path(self.db, scan_id, fileset_id, file_id, size)
        except FileNotFoundError:
            return {'error': f"No '{scan_id}/{fileset_id}/{file_id}' file found!"}, 400
        except FilesetNotFoundError:
            return {'error': f"No '{scan_id}/{fileset_id}' fileset found!"}, 400
        except ScanNotFoundError:
            return {'error': f"No '{scan_id}' scan found!"}, 400
        except Exception as e:
            return {'error': f"Unknown error: {e}"}, 400

        # If coords_flag is set, read the file and return JSON
        if coords_flag:
            import numpy as np
            from open3d import io
            pcd = io.read_triangle_mesh(path, print_progress=False)
            # Convert the Open3D Vector3dVector to a plain Python list so JSON can serialize it.
            return jsonify({'vertices': np.array(pcd.vertices).tolist(), 'triangles': np.array(pcd.triangles).tolist()})
        # Otherwise, return the file directly
        return send_file(path, mimetype='application/octet-stream')


class CurveSkeleton(Resource):
    """A RESTful resource that provides access to curve skeleton data for plant scans.

    This class implements a REST API endpoint that serves curve skeleton data stored in JSON
    format. It handles GET requests to retrieve skeleton data for a specific scan ID.

    Attributes
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        Database instance containing plant scan data and associated filesets.
    """

    def __init__(self, db):
        """Initialize the CurveSkeleton resource.

        Parameters
        ----------
        db : plantdb.commons.fsdb.core.FSDB
            Database instance providing access to plant scan data.
        """
        self.db = db

    @rate_limit(max_requests=5, window_seconds=60)
    def get(self, scan_id):
        """Retrieve the curve skeleton data for a specific scan.

        This method handles GET requests to fetch curve skeleton data. It performs
        validation of the scan ID, retrieves the appropriate fileset, and returns
        the skeleton data in JSON format.

        Parameters
        ----------
        scan_id : str
            Identifier for the plant scan to retrieve skeleton data for.
            Must contain only alphanumeric characters, underscores, dashes, or periods.

        Returns
        -------
        Union[dict, Tuple[dict, int]]
            On success: Dictionary containing the curve skeleton data
            On failure: Tuple of (error_dict, http_status_code)

        Raises
        ------
        plantdb.commons.fsdb.exceptions.ScanNotFoundError
            If the requested scan ID doesn't exist in the database
        plantdb.commons.fsdb.exceptions.FilesetNotFoundError
            If the CurveSkeleton fileset is not found for the scan
        plantdb.commons.fsdb.exceptions.FileNotFoundError
            If the CurveSkeleton file is missing from the fileset
        http.client.HTTPException
             If the rate limit is exceeded, it returns an HTTP 429 ("Too Many Requests") response to the client.

        Notes
        -----
        - The scan_id is sanitized before processing to ensure security
        - Returns HTTP 400 status code for all error conditions with appropriate error messages
        - The skeleton data is expected to be in JSON format in the database

        Examples
        --------
        >>> # Start the REST API server
        >>> # Then in a Python console:
        >>> import requests
        >>> # Fetch skeleton data for a valid scan
        >>> response = requests.get("http://127.0.0.1:5000/skeleton/Col-0_E1_1")
        >>> skeleton_data = response.json()
        >>> print(list(skeleton_data.keys()))
        ['angles', 'internodes', 'metadata']
        >>> # Example with invalid scan ID
        >>> response = requests.get("http://127.0.0.1:5000/skeleton/invalid_id")
        >>> print(response.status_code)
        400
        >>> print(response.json())
        {'error': "Scan 'invalid_id' not found!"}
        """
        # Sanitize identifiers
        scan_id = sanitize_name(scan_id)
        # Get the corresponding `Scan` instance
        try:
            scan = self.db.get_scan(scan_id)
        except ScanNotFoundError:
            return {"error": f"Scan '{scan_id}' not found!"}, 400
        task_fs_map = compute_fileset_matches(scan)
        # Get the corresponding `Fileset` instance
        try:
            fs = scan.get_fileset(task_fs_map['CurveSkeleton'])
        except KeyError:
            return {'error': "No 'CurveSkeleton' fileset mapped!"}, 400
        except FilesetNotFoundError:
            return {'error': "No 'CurveSkeleton' fileset found!"}, 400
        # Get the `File` corresponding to the CurveSkeleton resource
        try:
            file = fs.get_file('CurveSkeleton')
        except FileNotFoundError:
            return {'error': "No 'CurveSkeleton' file found!"}, 400
        except Exception as e:
            return json.dumps({'error': str(e)}), 400
        # Load the JSON file:
        try:
            skeleton = read_json(file.path())
        except Exception as e:
            return json.dumps({'error': str(e)}), 400
        else:
            return skeleton


class Sequence(Resource):
    """A RESTful resource class that serves angle and internode sequences data.

    This class provides a REST API endpoint to retrieve angle and internode sequence data
    for plant scans. It handles data retrieval from a database and supports filtering
    by sequence type (angles, internodes, or fruit_points).

    Attributes
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        The database instance used for retrieving scan data.
    logger : Logger
        The logger instance for this resource.

    Parameters
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        A database instance used for retrieving scan data.
    logger : logging.Logger
        A logger instance for this resource.
    """

    def __init__(self, db, logger):
        """Initialize the Sequence resource."""
        self.db = db
        self.logger = logger

    @rate_limit(max_requests=60, window_seconds=60)
    def get(self, scan_id):
        """Retrieve angle and internode sequences data for a given scan.

        This method serves as a REST API endpoint to fetch angle, internode, and fruit point
        sequence data from plant scans. It can return either all sequence data or specific
        sequence types based on the query parameter 'type'.

        Parameters
        ----------
        scan_id : str
            Unique identifier for the plant scan. Must contain only alphanumeric
            characters, underscores, dashes, or periods.

        Returns
        -------
        Union[dict, list, tuple[dict, int]]
            If successful and type='all' (default):
                Dictionary containing all sequence data with the following keys: 'angles', 'internodes',
                'fruit_points', 'manual_angles', 'manual_internodes'
            If successful and type in ['angles', 'internodes', 'fruit_points', 'manual_angles', 'manual_internodes']:
                List of sequence values for the specified type
            If error:
                Tuple of (error_dict, HTTP_status_code)

        Raises
        ------
        plantdb.commons.fsdb.exceptions.ScanNotFoundError
            If the specified scan_id does not exist in the database
        plantdb.commons.fsdb.exceptions.FilesetNotFoundError
            If the AnglesAndInternodes fileset is not found
        plantdb.commons.fsdb.exceptions.FileNotFoundError
            If the AnglesAndInternodes file is not found within the fileset
        http.client.HTTPException
             If the rate limit is exceeded, it returns an HTTP 429 ("Too Many Requests") response to the client.

        Notes
        -----
        - The 'type' query parameter accepts 'angles', 'internodes', or 'fruit_points'
        - Invalid 'type' parameters will return the complete data dictionary
        - All responses are JSON-encoded
        - Input scan_id is sanitized before processing

        See Also
        --------
        plantdb.server.rest_api.sanitize_name : Function used to validate and clean scan_id
        plantdb.server.rest_api.compute_fileset_matches : Function to match filesets with tasks

        Examples
        --------
        >>> # Get all sequence data
        >>> import requests
        >>> response = requests.get("http://127.0.0.1:5000/sequence/real_plant_analyzed")
        >>> data = response.json()  # Expected output: {'angles': [...], 'internodes': [...], 'fruit_points': [...]}
        >>> print(list(data))
        ['angles', 'internodes', 'fruit_points', 'manual_angles', 'manual_internodes']
        >>> # Get only angles data
        >>> response = requests.get("http://127.0.0.1:5000/sequence/real_plant_analyzed", params={'type': 'angles'})
        >>> angles = response.json()
        >>> print(angles[:5])
        [47.13015345294241, 239.43543078022594, 311.8816488465762, 251.0289289739646, 249.56560354730826]
        """
        # Sanitize identifiers
        scan_id = sanitize_name(scan_id)
        type = request.args.get('type', default='all', type=str)
        # Get the corresponding `Scan` instance
        try:
            scan = self.db.get_scan(scan_id)
        except ScanNotFoundError:
            return {"error": f"Scan '{scan_id}' not found!"}, 400
        task_fs_map = compute_fileset_matches(scan)
        # Get the corresponding `Fileset` instance
        try:
            fs = scan.get_fileset(task_fs_map['AnglesAndInternodes'])
        except KeyError:
            return {'error': "No 'AnglesAndInternodes' fileset mapped!"}, 400
        except FilesetNotFoundError:
            return {'error': "No 'AnglesAndInternodes' fileset found!"}, 400
        # Get the `File` corresponding to the AnglesAndInternodes resource
        try:
            file = fs.get_file('AnglesAndInternodes')
        except FileNotFoundError:
            return {'error': "No 'AnglesAndInternodes' file found!"}, 400
        except Exception as e:
            return json.dumps({'error': str(e)}), 400
        # Load the JSON file:
        try:
            measures = read_json(file.path())
        except Exception as e:
            return json.dumps({'error': str(e)}), 400
        # Load the manual 'measures.json' JSON file:
        manual_measures_file = scan.path() / 'measures.json'
        try:
            manual_measures = read_json(manual_measures_file)
        except Exception as e:
            if manual_measures_file.exists():
                self.logger.warning(f"Failed to load manual measures file: {manual_measures_file}")
                self.logger.warning(e)
            pass
        else:
            measures['manual_angles'] = manual_measures['angles']
            measures['manual_internodes'] = manual_measures['internodes']

        # Make sure that the 'type' argument we got is a valid option, else default to 'all':
        if type in ['angles', 'internodes', 'fruit_points', 'manual_angles', 'manual_internodes']:
            return measures[type]
        else:
            return measures


def is_within_directory(directory, target):
    """Check if a target path is within a directory.

    This function determines if the absolute path of the target is located
    within the absolute path of the directory. It uses `os.path.commonpath`
    to perform the comparison.

    Parameters
    ----------
    directory : str or pathlib.Path
        The path to the directory to check against.
    target : str or pathlib.Path
        The path to the target to check if it resides within the directory.

    Returns
    -------
    bool
        ``True`` if the target path is within the directory, ``False`` otherwise.
    """
    abs_directory = os.path.abspath(directory)
    abs_target = os.path.abspath(target)
    return os.path.commonpath([abs_directory]) == os.path.commonpath([abs_directory, abs_target])


def is_directory_in_archive(archive_path, target_dir):
    """Check if a specific directory exists within an archive file.

    This function checks whether a given directory is present at the top level of a ZIP archive.

    Parameters
    ----------
    archive_path : str or pathlib.Path
        The path to the ZIP archive file.
    target_dir : str
        The name of the target directory to check for within the archive.

    Returns
    -------
    bool
        True if the target directory exists at the top level of the archive, False otherwise.
    """
    with ZipFile(archive_path, 'r') as zip_ref:
        # List all members in the zip file
        top_level_members = [name for name in zip_ref.namelist() if '/' not in name]
        # Check if the target directory is among them
        return f"{target_dir}/" in top_level_members or target_dir in top_level_members


class Archive(Resource):
    """A RESTful resource class for managing dataset archives.

    This class provides functionality to serve and upload dataset archives through HTTP GET and POST methods.
    It handles ZIP file creation, validation, and extraction while maintaining security and proper cleanup
    of temporary files.

    Attributes
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        A database instance for accessing and managing scan data.
    logger : logging.Logger
        A logger instance for recording operations and errors.
    """

    def __init__(self, db, logger):
        """Initialize the Archive resource.

        Parameters
        ----------
        db : plantdb.commons.fsdb.core.FSDB
            A database instance for accessing and managing scan data.
        logger : logging.Logger
            A logger instance for recording operations and errors.
        """
        self.db = db
        self.logger = logger

    @rate_limit(max_requests=5, window_seconds=60)
    def get(self, scan_id, **kwargs):
        """Create and serve a ZIP archive for the specified scan dataset.

        This method creates a temporary ZIP archive containing all files from the specified
        scan directory (excluding 'webcache' directories) and serves it as a downloadable file.

        Parameters
        ----------
        scan_id : str
            Unique identifier for the scan dataset to be archived.

        Returns
        -------
        flask.Response or tuple
            If successful, returns a Flask response object with the ZIP file for download.
            If unsuccessful, returns a tuple (dict, int) containing an error message and
            HTTP status code ``400``.

        Notes
        -----
        - The scan_id is sanitized before processing
        - 'webcache' directories are automatically excluded from the archive
        - Temporary files are created with 'fsdb_rest_api_' prefix
        - Clean-up is handled automatically after the request

        Examples
        --------
        >>> import os
        >>> import requests
        >>> import shutil
        >>> import tempfile
        >>> from io import BytesIO
        >>> from pathlib import Path
        >>> from zipfile import ZipFile
        >>> from plantdb.client.rest_api import request_scan_names_list
        >>> from plantdb.server.test_rest_api import TestRestApiServer
        >>> # Create a test database and start the Flask App serving a REST API
        >>> server = TestRestApiServer(test=True)
        >>> server.start()
        >>> # Get the archive for the 'real_plant' dataset with
        >>> zip_file = requests.get("http://127.0.0.1:5000/archive/real_plant", stream=True)
        >>> # EXAMPLE 1 - Write the archive to disk:
        >>> # Create a unique temporary file name with .zip extension
        >>> temp_zip_handle, temp_zip_path = tempfile.mkstemp(suffix='.zip')
        >>> os.close(temp_zip_handle)  # Close the file handle immediately
        >>> # Write to disk
        >>> with open(temp_zip_path, 'wb') as zip_f: zip_f.write(zip_file.content)
        >>> print(f"Successfully wrote to {temp_zip_path}")
        >>> # EXAMPLE 2 - Extract the archive:
        >>> # Create a temporary path to extract the archived data
        >>> tmp_dir = Path(tempfile.mkdtemp())
        >>> # Open the zip file and extract non-existing files
        >>> extracted_files = []
        >>> with ZipFile(BytesIO(zip_file.content), 'r') as zip_obj:
        ...     for file in zip_obj.namelist():
        ...         file_path = tmp_dir / file
        ...         zip_obj.extract(file, path=tmp_dir)
        ...         extracted_files.append(file)
        ...
        >>> # Print the list of extracted files
        >>> print(extracted_files)
        >>> shutil.rmtree(tmp_dir)  # Remove the temporary directory (and its contents)
        >>> # Stop the test server
        >>> server.stop()
        """
        scan_id = sanitize_name(scan_id)

        try:
            scan = self.db.get_scan(scan_id, **kwargs)
        except ScanNotFoundError:
            return {'error': f'Could not find a scan named `{scan_id}`!'}, 404

        try:
            zip_path = create_zip_for_scan(Path(scan.path()), self.logger)
        except ArchiveError as exc:
            return {"error": str(exc)}, 400

        # Schedule the temporary file for cleanup after request completion
        @after_this_request
        def cleanup_temp_file(response):
            try:
                if zip_path.exists():
                    zip_path.unlink()
                    self.logger.info(f"Temporary archive `{zip_path}` deleted.")
            except Exception as e:
                self.logger.error(f"Failed to delete temporary file `{zip_path}`: {e}")
            return response

        return send_file(zip_path, download_name=f'{scan_id}.zip', mimetype='application/zip')

    @rate_limit(max_requests=5, window_seconds=60)
    @add_jwt_from_header
    def post(self, scan_id, **kwargs):
        """Handle ZIP file upload and extraction for a scan dataset.

        This method processes an uploaded ZIP file, validates its contents and structure,
        and extracts it to the appropriate location in the database. It includes various
        security checks and ensures safe extraction of files.

        Parameters
        ----------
        scan_id : str
            Unique identifier for the scan dataset where the ZIP contents will be extracted.

        Returns
        -------
        tuple
            A tuple containing (dict, int) where the dict contains either:
            - On success: {'success': message, 'files': list_of_extracted_files}
            - On failure: {'error': error_message}
            The integer represents the HTTP status code (``200`` for success, ``400`` or ``500`` for errors)

        Notes
        -----
        - Performs the following validations:
            * Checks for ZIP file presence
            * Validates MIME type (must be 'application/zip')
            * Verifies file extension (.zip)
            * Tests ZIP file integrity
            * Validates filename encodings
            * Prevents path traversal attacks
        - Only extracts files that don't already exist
        - Automatically cleans up temporary files

        Examples
        --------
        >>> import requests
        >>> from pathlib import Path
        >>> from tempfile import gettempdir
        >>> from plantdb.server.test_rest_api import TestRestApiServer
        >>> from plantdb.client.rest_api import request_scan_names_list
        >>> # Create a test database and start the Flask App serving a REST API
        >>> server = TestRestApiServer(test=True)
        >>> server.start()
        >>> zip_file = Path(gettempdir()) / 'real_plant.zip'  # should be in the temporary directory from the TestRestApiServer setup
        >>> print(zip_file.exists())
        True
        >>> # You need to be logged to be able to POST archives
        >>> r = requests.post('http://127.0.0.1:5000/login', json={'username': 'admin', 'password': 'admin'})
        >>> jwt_token = r.json()['access_token']  # get the JSON Web Token
        >>> # Upload it as a new dataset named 'real_plant_test'
        >>> new_dataset = 'real_plant_test'
        >>> with open(zip_file, 'rb') as zip_f:
        ...    files = {'zip_file': (str(zip_file), zip_f, 'application/zip')}
        ...    response = requests.post(f'http://127.0.0.1:5000/archive/{new_dataset}', files=files, headers={'Authorization': 'Bearer ' + jwt_token})
        >>> print(response.json())
        >>> _ = requests.get(f"http://127.0.0.1:5000/refresh?scan_id={new_dataset}")
        >>> r = requests.get("http://127.0.0.1:5000/scans")
        >>> scans_list = r.json()
        >>> print(new_dataset in scans_list)
        True
        >>> server.stop()
        """
        scan_id = sanitize_name(scan_id)

        # 1. Basic pre‑condition checks
        if self.db.scan_exists(scan_id):
            self.logger.error("Dataset `%s` already exists.", scan_id)
            return {"error": f"Dataset `{scan_id}` already exists!"}, 400

        # Get the zip file from the request
        uploaded = request.files.get("zip_file")
        try:
            # Check if a file was provided
            _ensure_file_present(uploaded)
            # Validate the file MIME type
            _validate_mime_type(uploaded)
            # Validate the file extension
            _validate_extension(uploaded)
        except ValidationError as exc:
            self.logger.error("Upload validation failed: %s", exc)
            return {"error": str(exc)}, 400

        # 2. Store uploaded file temporarily
        try:
            temp_zip = _save_to_temp(uploaded, self.logger)
        except ValidationError as exc:
            return {"error": str(exc)}, 500

        # try/except/finally to ensure the temporary file is always removed
        try:
            # 3. Archive integrity & domain validation
            _test_zip_integrity(temp_zip, self.logger)
            _ensure_valid_structure(temp_zip)

            # 4. Create destination scan and extract files
            try:
                scan_path = Path(self.db.create_scan(scan_id, **kwargs).path())
            except PermissionError as exc:
                return {"message": f"Invalid credentials: {exc}"}, 401
            extracted_files = extract_zip_to_scan(temp_zip, scan_path, self.logger)

            # 5. Refresh DB state and respond
            self.db.reload(scan_id)
            return {"message": "ZIP file processed successfully", "files": extracted_files}, 200

        except (ValidationError, ExtractionError) as exc:
            # Validation / extraction errors are client‑side problems → 400
            return {"error": str(exc)}, 400
        except Exception as exc:
            # Unexpected server‑side errors → 500
            self.logger.exception("Unexpected error while processing archive")
            return {"error": f"Internal server error: {exc}"}, 500
        finally:
            temp_zip.unlink(missing_ok=True)
