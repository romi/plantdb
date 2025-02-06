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
This module regroup the classes and methods used to serve a REST API using ``fsdb_rest_api`` CLI.
"""

import datetime
import json
import os
from io import BytesIO
from math import radians
from pathlib import Path
from tempfile import mkdtemp
from tempfile import mkstemp
from zipfile import ZipFile

from flask import after_this_request
from flask import jsonify
from flask import make_response
from flask import request
from flask import send_file
from flask import send_from_directory
from flask_restful import Resource

from plantdb import webcache
from plantdb.fsdb import ScanNotFoundError
from plantdb.io import read_json
from plantdb.log import get_logger
from plantdb.utils import is_radians


def get_scan_date(scan):
    """Get the acquisition datetime of a scan.

    Try to get the data from the scan metadata 'acquisition_date', else from the directory creation time.

    Parameters
    ----------
    scan : plantdb.fsdb.Scan
        The scan instance to get the date & time from.

    Returns
    -------
    str
        The formatted datetime string.

    Examples
    --------
    >>> from plantdb.rest_api import get_scan_date
    >>> from plantdb.test_database import test_database
    >>> db = test_database(['real_plant_analyzed', 'virtual_plant_analyzed'])
    >>> db.connect()
    >>> scan = db.get_scan('real_plant_analyzed')
    >>> print(get_scan_date(scan))
    >>> scan = db.get_scan('virtual_plant_analyzed')
    >>> print(get_scan_date(scan))
    >>> db.disconnect()
    """
    dt = scan.get_metadata('acquisition_date')
    try:
        assert isinstance(dt, str)
    except:
        # Get directory creation date as acquisition date
        c_time = scan.path().lstat().st_ctime
        dt = datetime.datetime.fromtimestamp(c_time)
        date = dt.strftime("%Y-%m-%d")
        time = dt.strftime("%H:%M:%S")
    else:
        date, time = dt.split(' ')
    return f"{date} {time}"


def compute_fileset_matches(scan):
    """Return a dictionary mapping the scan tasks to fileset names.

    Parameters
    ----------
    scan : plantdb.fsdb.Scan
        The scan instance to list the filesets from.

    Returns
    -------
    dict
        A dictionary mapping the scan tasks to fileset names.

    Examples
    --------
    >>> from plantdb.rest_api import compute_fileset_matches
    >>> from plantdb.fsdb import dummy_db
    >>> db = dummy_db(with_fileset=True)
    >>> scan = db.get_scan("myscan_001")
    >>> compute_fileset_matches(scan)
    {'fileset': 'fileset_001'}
    >>> db.disconnect()  # clean up (delete) the temporary dummy database
    """
    filesets_matches = {}
    for fs in scan.get_filesets():
        x = fs.id.split('_')[0]  # get the task name
        filesets_matches[x] = fs.id
    return filesets_matches


def get_path(f, db_prefix="/files/"):
    """Return the path to a file.

    Parameters
    ----------
    f : plantdb.FSDB.File
        The file to get the path for.
    db_prefix : str, optional
        A prefix to use... ???

    Returns
    -------
    str
        The path to the file.
    """
    fs = f.fileset  # get the corresponding fileset
    scan = fs.scan  # get the corresponding scan
    return os.path.join(db_prefix, scan.id, fs.id, f.filename)


def get_scan_template(scan_id: str, error=False) -> dict:
    """Template dictionary for a scan."""
    return {
        "id": scan_id,
        "metadata": {
            "date": "01-01-00 00:00:00",
            "species": "N/A",
            "plant": "N/A",
            "environment": "N/A",
            "nbPhotos": 0,
            "files": {
                "metadatas": None,
                "archive": None
            }
        },
        "thumbnailUri": "",
        "images": None,  # list of original image filenames
        "tasks_fileset": None,  # dict mapping task names to fileset names
        "filesUri": {},  # dict mapping task names to task file URI
        "isVirtual": False,
        "hasColmap": False,
        "hasPointCloud": False,
        "hasTriangleMesh": False,
        "hasCurveSkeleton": False,
        "hasTreeGraph": False,
        "hasAnglesAndInternodes": False,
        "hasAutomatedMeasures": False,
        "hasManualMeasures": False,
        "hasSegmentation2D": False,
        "hasPcdGroundTruth": False,
        "hasPointCloudEvaluation": False,
        "hasSegmentedPointCloud": False,
        "hasSegmentedPcdEvaluation": False,
        "error": error,
    }


def get_scan_info(scan, **kwargs):
    """Get the information related to a single scan dataset.

    Parameters
    ----------
    scan : plantdb.fsdb.Scan
        The scan instance to get information from.

    Other Parameters
    ----------------
    logger : logging.Logger
        A logger to use with this method, default to a logger created on the fly with a `module.function` name.

    Returns
    -------
    dict
        The scan information dictionary.

    Examples
    --------
    >>> from plantdb.rest_api import get_scan_info
    >>> from plantdb.test_database import test_database
    >>> db = test_database('real_plant_analyzed')
    >>> db.connect()
    >>> scan = db.get_scan('real_plant_analyzed')
    >>> scan_info = get_scan_info(scan)
    >>> print(scan_info)
    {'id': 'real_plant_analyzed', 'metadata': {'date': '2023-12-15 16:37:15', 'species': 'N/A', 'plant': 'N/A', 'environment': 'Lyon indoor', 'nbPhotos': 60, 'files': {'metadatas': None, 'archive': None}}, 'thumbnailUri': '', 'hasTriangleMesh': True, 'hasPointCloud': True, 'hasPcdGroundTruth': False, 'hasCurveSkeleton': True, 'hasAnglesAndInternodes': True, 'hasSegmentation2D': False, 'hasSegmentedPcdEvaluation': False, 'hasPointCloudEvaluation': False, 'hasManualMeasures': False, 'hasAutomatedMeasures': True, 'hasSegmentedPointCloud': False, 'error': False, 'hasTreeGraph': True}
    >>> db.disconnect()
    """
    logger = kwargs.get("logger", get_logger(__name__))
    logger.info(f"Accessing scan info for `{scan.id}` dataset.")

    # Initialize the scan information template:
    scan_info = get_scan_template(scan.id)

    # Map the scan tasks to fileset names:
    task_fs_map = compute_fileset_matches(scan)
    scan_info["tasks_fileset"] = task_fs_map

    # Get the list of original image filenames:
    img_fs = scan.get_fileset("images")
    scan_info["images"] = [img_f.filename for img_f in img_fs.get_files(query={"channel": 'rgb'})]

    # Gather "metadata" information from scan:
    scan_md = scan.get_metadata()
    ## Get acquisition date:
    scan_info["metadata"]['date'] = get_scan_date(scan)
    ## Import 'object' related scan metadata to scan info template:
    if 'object' in scan_md:
        scan_obj = scan_md['object']  # get the 'object' related dictionary
        scan_info["metadata"]["species"] = scan_obj.get('species', 'N/A')
        scan_info["metadata"]["environment"] = scan_obj.get('environment', 'N/A')
        scan_info["metadata"]["plant"] = scan_obj.get('plant_id', 'N/A')
    ## Get the number of 'images' in the dataset:
    scan_info["metadata"]['nbPhotos'] = len(scan_info["images"])
    ## Get the URL to the archive:
    scan_info["metadata"]["files"]["archive"] = f"/archive/{scan.id}"
    ## Get the path to the JSON metadata file:
    metadata_json_path = os.path.join("/files/", scan.id, "metadata", "metadata.json")
    scan_info["metadata"]["files"]["metadatas"] = metadata_json_path

    # Get the URI to first image to create thumbnail:
    # It is used by the `plant-3d-explorer`, in its landing page, as image presenting the dataset
    img_f = img_fs.get_files()[0]
    scan_info["thumbnailUri"] = f"/image/{scan.id}/{img_fs.id}/{img_f.id}?size=thumb"

    def _try_has_file(task, file):
        if task not in task_fs_map:
            # If not in the dict mapping `task` names to fileset names
            return False
        elif scan.get_fileset(task_fs_map[task]) is None:
            # If ``Fileset`` is None:
            return False
        else:
            # Test if `file` is found in `task` ``Fileset``:
            try:
                file = scan.get_fileset(task_fs_map[task]).get_file(file)
            except FileNotFoundError:
                return False
            else:
                return file is not None

    # Define is the scan is a virtual plant dataset:
    scan_info["isVirtual"] = "VirtualPlant" in scan_info["tasks_fileset"]

    # Set boolean information about tasks presence/absence for given dataset:
    scan_info["hasColmap"] = _try_has_file('Colmap', 'cameras')
    scan_info["hasPointCloud"] = _try_has_file('PointCloud', 'PointCloud')
    scan_info["hasTriangleMesh"] = _try_has_file('TriangleMesh', 'TriangleMesh')
    scan_info["hasCurveSkeleton"] = _try_has_file('CurveSkeleton', 'CurveSkeleton')
    scan_info["hasTreeGraph"] = _try_has_file('TreeGraph', 'TreeGraph')
    scan_info["hasAnglesAndInternodes"] = _try_has_file('AnglesAndInternodes', 'AnglesAndInternodes')
    scan_info["hasAutomatedMeasures"] = _try_has_file('AnglesAndInternodes', 'AnglesAndInternodes')
    scan_info["hasManualMeasures"] = "measures.json" in [f.name for f in scan.path().iterdir()]
    scan_info["hasSegmentation2D"] = _try_has_file('Segmentation2D', '')
    scan_info["hasPcdGroundTruth"] = _try_has_file('PointCloudGroundTruth', 'PointCloudGroundTruth')
    scan_info["hasPointCloudEvaluation"] = _try_has_file('PointCloudEvaluation', 'PointCloudEvaluation')
    scan_info["hasSegmentedPointCloud"] = _try_has_file('SegmentedPointCloud', 'SegmentedPointCloud')
    scan_info["hasSegmentedPcdEvaluation"] = _try_has_file('SegmentedPointCloudEvaluation',
                                                           'SegmentedPointCloudEvaluation')

    ## Get the URI (file path) to the output of the `PointCloud` task:
    for task, uri_key in task_filesUri_mapping.items():
        if scan_info[f"has{task}"]:
            fs = scan.get_fileset(task_fs_map[task])
            scan_info["filesUri"][uri_key] = get_file_uri(scan, fs, fs.get_file(task))

    return scan_info


def get_file_uri(scan, fileset, file):
    """Return the URI for the corresponding `scan/fileset/file` tree.

    Parameters
    ----------
    scan : plantdb.fsdb.Scan or str
        A ``Scan`` instance or the name of the scan dataset.
    fileset : plantdb.fsdb.Fileset or str
        A ``Fileset`` instance or the name of the fileset.
    file : plantdb.fsdb.File or str
        A ``File`` instance or the name of the file.

    Returns
    -------
    str
        The URI for the corresponding `scan/fileset/file` tree.

    Examples
    --------
    >>> from plantdb.rest_api import get_file_uri
    >>> from plantdb.test_database import test_database
    >>> from plantdb.rest_api import compute_fileset_matches
    >>> db = test_database('real_plant_analyzed')
    >>> db.connect()
    >>> scan = db.get_scan('real_plant_analyzed')
    >>> fs_match = compute_fileset_matches(scan)
    >>> fs = scan.get_fileset(fs_match['PointCloud'])
    >>> f = fs.get_file("PointCloud")
    >>> get_file_uri(scan, fs, f)
    '/files/real_plant_analyzed/PointCloud_1_0_1_0_10_0_7ee836e5a9/PointCloud.ply'
    """
    from plantdb.fsdb import Scan
    from plantdb.fsdb import Fileset
    from plantdb.fsdb import File
    scan_id = scan.id if isinstance(scan, Scan) else scan
    fileset_id = fileset.id if isinstance(fileset, Fileset) else fileset
    file_name = file.path().name if isinstance(file, File) else file
    return f"/files/{scan_id}/{fileset_id}/{file_name}"


task_filesUri_mapping = {
    "PointCloud": "pointCloud",
    "TriangleMesh": "mesh",
    "CurveSkeleton": "skeleton",
    "TreeGraph": "tree",
}


def get_scan_data(scan, **kwargs):
    """Get the scan information and data.

    Parameters
    ----------
    scan : plantdb.fsdb.Scan
        The scan instance to get the information and data from.

    Other Parameters
    ----------------
    logger : logging.Logger
        A logger to use with this method, default to a logger created on the fly with a `module.function` name.

    Returns
    -------
    dict
        The scan information dictionary.

    Examples
    --------
    >>> from plantdb.rest_api import get_scan_data
    >>> from plantdb.test_database import test_database
    >>> db = test_database('real_plant_analyzed')
    >>> db.connect()
    >>> scan = db.get_scan('real_plant_analyzed')
    >>> scan_data = get_scan_data(scan)
    >>> print(scan_data['id'])
    real_plant_analyzed
    >>> print(scan_data['filesUri'])
    {'pointCloud': PosixPath('/tmp/ROMI_DB/real_plant_analyzed/PointCloud_1_0_1_0_10_0_7ee836e5a9/PointCloud.ply'), 'mesh': PosixPath('/tmp/ROMI_DB/real_plant_analyzed/TriangleMesh_9_most_connected_t_open3d_00e095c359/TriangleMesh.ply'), 'skeleton': PosixPath('/tmp/ROMI_DB/real_plant_analyzed/CurveSkeleton__TriangleMesh_0393cb5708/CurveSkeleton.json'), 'tree': PosixPath('/tmp/ROMI_DB/real_plant_analyzed/TreeGraph__False_CurveSkeleton_c304a2cc71/TreeGraph.p')}
    >>> print(scan_data['camera']["model"])
    SIMPLE_RADIAL
    >>> db.disconnect()
    """
    logger = kwargs.get("logger", get_logger(__name__))

    task_fs_map = compute_fileset_matches(scan)
    scan_data = get_scan_info(scan, logger=logger)
    img_fs = scan.get_fileset(task_fs_map['images'])

    # Get the paths to data files:
    scan_data["filesUri"] = {}
    ## Get the URI (file path) to the output of the `PointCloud` task:
    for task, uri_key in task_filesUri_mapping.items():
        if scan_data[f"has{task}"]:
            fs = scan.get_fileset(task_fs_map[task])
            scan_data["filesUri"][uri_key] = get_file_uri(scan, fs, fs.get_file(task))

    # Load some of the data:
    scan_data["data"] = {}
    ## Load the skeleton data:
    if scan_data["hasCurveSkeleton"]:
        fs = scan.get_fileset(task_fs_map['CurveSkeleton'])
        scan_data["data"]["skeleton"] = read_json(fs.get_file('CurveSkeleton'))
    ## Load the angles and internodes data:
    scan_data["data"]["angles"] = {}
    ### Load the manually measured angles and internodes:
    if scan_data["hasManualMeasures"]:
        measures = scan.get_measures()
        if measures is None:
            measures = dict([])
        scan_data["data"]["angles"]["measured_angles"] = measures.get('angles', [])
        scan_data["data"]["angles"]["measured_internodes"] = measures.get("internodes", [])
    ### Load the measured angles and internodes:
    if scan_data["hasAnglesAndInternodes"]:
        fs = scan.get_fileset(task_fs_map['AnglesAndInternodes'])
        # Load the JSON file, this should return a dict with at least 'angles' & 'internodes' keys:
        measures = read_json(fs.get_file('AnglesAndInternodes'))
        if 'angles' not in measures and 'internodes' not in measures:
            missing = [key not in measures.keys() for key in ['angles', 'internodes']]
            logger.error(f"Missing {', '.join(missing)} entries in AnglesAndInternodes JSON output!")
        else:
            # Make sure we get angles in radians as the plant-3d-explorer always tries to convert to degrees:
            if not is_radians(measures["angles"]):
                measures["angles"] = list(map(radians, measures["angles"]))
            scan_data["data"]["angles"].update(measures)

    # Load the reconstruction bounding-box:
    scan_data["workspace"] = img_fs.get_metadata("bounding_box", None)
    if scan_data['hasColmap']:
        ## Load the workspace, aka bounding-box:
        try:
            # old version: get scanner workspace
            scan_data["workspace"] = scan.get_metadata("scanner")["workspace"]
            logger.warning(f"You are using a DEPRECATED version of the PlantDB API.")
        except KeyError:
            # new version: get it from Colmap fileset metadata 'bounding-box'
            fs = scan.get_fileset(task_fs_map['Colmap'])
            scan_data["workspace"] = fs.get_metadata("bounding_box")

    # Load the camera parameters (intrinsic and extrinsic)
    scan_data["camera"] = {}
    if scan_data['hasColmap']:
        ## Load the camera model (intrinsic parameters):
        try:
            # old version
            scan_data["camera"]["model"] = scan.get_metadata("computed")["camera_model"]
            logger.warning(f"You are using a DEPRECATED version of the PlantDB API.")
        except KeyError:
            # new version: get it from Colmap 'cameras.json':
            fs = scan.get_fileset(task_fs_map['Colmap'])
            scan_data["camera"]["model"] = json.loads(fs.get_file("cameras").read())['1']
        ## Load the camera poses (extrinsic parameters) from the images metadata:
        scan_data["camera"]["poses"] = []  # initialize list of poses to gather
        for img_idx, img_f in enumerate(img_fs.get_files()):
            camera_md = img_f.get_metadata("colmap_camera")
            scan_data["camera"]["poses"].append({
                "id": img_idx + 1,
                "tvec": camera_md['tvec'],
                "rotmat": camera_md['rotmat'],
                "photoUri": str(webcache.image_path(scan.db, scan.id, img_fs.id, img_f.id, 'orig')),
                "thumbnailUri": str(webcache.image_path(scan.db, scan.id, img_fs.id, img_f.id, 'thumb')),
                "isMatched": True
            })
    elif scan_data["isVirtual"]:
        ## Load the camera model (intrinsic parameters):
        img_f = img_fs.get_files()[0]  # from the first image of the 'images' fileset
        scan_data["camera"]["model"] = img_f.get_metadata("camera")["camera_model"]
        ## Load the camera poses (extrinsic parameters) from the images metadata:
        scan_data["camera"]["poses"] = []  # initialize list of poses to gather
        for img_idx, img_f in enumerate(img_fs.get_files(query={"channel": "rgb"})):
            camera_md = img_f.get_metadata("camera")
            scan_data["camera"]["poses"].append({
                "id": img_idx + 1,
                "tvec": camera_md['tvec'],
                "rotmat": camera_md['rotmat'],
                "photoUri": str(webcache.image_path(scan.db, scan.id, img_fs.id, img_f.id, 'orig')),
                "thumbnailUri": str(webcache.image_path(scan.db, scan.id, img_fs.id, img_f.id, 'thumb')),
                "isMatched": True
            })
    else:
        pass
    return scan_data


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


class ScansList(Resource):
    """Concrete RESTful resource to serve the list of scan datasets and some info upon request (GET method)."""

    def __init__(self, db):
        self.db = db

    def get(self):
        """Returns a list of scan dataset information.

        Returns
        -------
        list of dict
            The list of dictionaries to serve (as JSON)

        Examples
        --------
        >>> # In a terminal, start a (test) REST API with `fsdb_rest_api --test`, then:
        >>> import requests
        >>> import json
        >>> # Get an info dict about all dataset:
        >>> res = requests.get("http://127.0.0.1:5000/scans")
        >>> scans_list = json.loads(res.content)
        >>> # List the known dataset id:
        >>> print(scans_list)
        ['arabidopsis000', 'virtual_plant_analyzed', 'real_plant_analyzed', 'real_plant', 'virtual_plant', 'models']
        >>> res = requests.get('http://127.0.0.1:5000/scans?filterQuery={"object":{"species":"Arabidopsis.*"}}&fuzzy="true"')
        >>> res.content.decode()

        """
        query = request.args.get('filterQuery', None)
        fuzzy = request.args.get('fuzzy', False, type=bool)
        if query is not None:
            query = json.loads(query)
        return self.db.list_scans(query=query, fuzzy=fuzzy, owner_only=False)


class Scan(Resource):
    """Concrete RESTful resource to serve a scan dataset upon request (GET method)."""

    def __init__(self, db, logger):
        self.db = db
        self.logger = logger

    def get(self, scan_id):
        """Returns the scan dataset information.

        Parameters
        ----------
        scan_id : str
            The name of the scan for which to serve the information.

        Returns
        -------
        dict
            The dictionary to serve (as JSON)

        Examples
        --------
        >>> # In a terminal, start a (test) REST API with `fsdb_rest_api --test`, then:
        >>> import requests
        >>> import json
        >>> # Get a detailed info dict about a specific dataset:
        >>> res = requests.get("http://127.0.0.1:5000/scans/real_plant_analyzed")
        >>> scan = json.loads(res.content)
        >>> # Get the date from the metadata:
        >>> print(scan['metadata']['date'])
        2024-08-19 11:12:25

        """
        scan_id = sanitize_name(scan_id)
        # return get_scan_data(self.db.get_scan(scan_id), logger=self.logger)
        return get_scan_info(self.db.get_scan(scan_id, create=False), logger=self.logger)


class File(Resource):
    """Concrete RESTful resource to serve a file upon request (GET method)."""

    def __init__(self, db):
        self.db = db

    def get(self, path):
        """Returns a requested file.

        Parameters
        ----------
        path : str
            The relative path to the file to serve.
            The global variable `db_location` is used to set the absolute path.

        Returns
        -------
        flask.Response
            The HTTP response from the flask server.

        Examples
        --------
        >>> # In a terminal, start a (test) REST API with `fsdb_rest_api --test`, then:
        >>> import requests
        >>> import toml
        >>> # Load the configuration file 'pipeline.toml':
        >>> res = requests.get("http://127.0.0.1:5000/files/real_plant_analyzed/pipeline.toml")
        >>> cfg = toml.loads(res.content.decode())
        >>> print(cfg['Undistorted'])
        {'upstream_task': 'ImagesFilesetExists'}

        """
        return send_from_directory(self.db.path(), path)


class DatasetFile(Resource):
    """Concrete RESTful resource to serve a file upon request (GET method)."""

    def __init__(self, db):
        self.db = db

    def post(self, scan_id):
        """Handles a POST request to upload and save a file to the server.

        The method validates the required HTTP headers, processes the uploaded file either as a whole or in
        chunks, and ensures the file is saved correctly to the desired location.
        The method responds with appropriate HTTP status codes based on the success or failure of the operation.

        Parameters
        ----------
        scan_id : str
            Unique identifier of the scan associated with the file upload.
            Used to retrieve the base path for the file being saved.

        Returns
        -------
        flask.Response
            A Flask response object with an appropriate HTTP status code and JSON
            message. Status code '201' is returned for successful file upload, while
            error codes ('400', '500') are returned for various error conditions.

        Notes
        -----
        - The `Content-Disposition`, `Content-Length`, and `X-File-Path` headers are
          mandatory for this endpoint to function correctly.
        - If a `X-Chunk-Size` value is provided, the file is uploaded in chunks;
          otherwise, the file is written in one step.
        - If the number of received bytes differs from the expected number, the
          partially uploaded file is removed.
        """
        # Check the header used to pass the filename, or return '400' for "bad request":
        if 'Content-Disposition' not in request.headers:
            return make_response(jsonify({"error": "No 'Content-Disposition' header!"}), 400)
        if 'Content-Length' not in request.headers:
            return make_response(jsonify({"error": "No 'Content-Length' header!"}), 400)
        if 'X-File-Path' not in request.headers:
            return make_response(jsonify({"error": "No 'X-File-Path' header!"}), 400)

        # Get the filename:
        rel_filename = request.headers['X-File-Path']
        # Check the received filename, or return '400' for "bad request":
        if not rel_filename:
            return make_response(jsonify({"error": "No valid filename provided!"}), 400)

        # Get the chuk size:
        content_length = int(request.headers.get('Content-Length', 0))
        # Check the received chuk size, or return '400' for "bad request":
        if content_length == 0:
            return make_response(jsonify({"error": "No valid 'Content-Length' provided!"}), 400)

        # Get the chuk size:
        chunk_size = int(request.headers.get('X-Chunk-Size', 0))

        # Check the dataset exists, or return '400' for "bad request":
        try:
            scan = self.db.get_scan(scan_id)
        except Exception as e:
            return make_response(jsonify({"error": str(e)}), 400)
        else:
            # Root path to write the data:
            root_path = scan.path()

        # Create the full file path:
        file_path = os.path.join(root_path, rel_filename)
        # Ensure the directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        def write_stream(file_path, content_length, chunk_size):
            bytes_received = 0
            with open(file_path, 'wb') as file:
                print(f"Received: {bytes_received}")
                while bytes_received < content_length:
                    chunk = request.stream.read(min(chunk_size, content_length - bytes_received))
                    if not chunk:
                        break  # Stream ended prematurely
                    file.write(chunk)
                    bytes_received += len(chunk)
            return bytes_received

        def write_data(file_path):
            with open(file_path, 'wb') as file:
                file.write(request.data)
            return os.path.getsize(file_path)

        # Write streamed file:
        try:
            if chunk_size == 0:
                bytes_received = write_data(file_path)
            else:
                bytes_received = write_stream(file_path, content_length, chunk_size)
            if bytes_received != content_length:
                # If something went wrong, remove the file and raise a ValueError:
                os.remove(file_path)
                raise ValueError(f"Received {bytes_received} bytes, expected {content_length} bytes")
        except Exception as e:
            # Return '500' for "server error" if anything went wrong:
            return make_response(jsonify({"error": f"Error saving file: {str(e)}"}), 500)
        else:
            # Return '201' for "created" if it went fine:
            return make_response(jsonify({"message": f"File {rel_filename} received and saved"}), 201)


class Refresh(Resource):
    """Concrete RESTful resource to reload the database upon request (GET method)."""

    def __init__(self, db):
        self.db = db

    def get(self):
        """Force the plant database to reload.

        Examples
        --------
        >>> # In a terminal, start a (test) REST API with `fsdb_rest_api --test`, then:
        >>> import requests
        >>> # Refresh the database:
        >>> res = requests.get("http://127.0.0.1:5000/refresh")
        >>> res.ok
        True
        >>> res = requests.get("http://127.0.0.1:5000/refresh?scan_id=real_plant")

        """
        scan_id = request.args.get('scan_id', default=None, type=str)
        self.db.reload(scan_id)
        return 200


class Image(Resource):
    """Concrete RESTful resource to serve an image upon request (GET method)."""

    def __init__(self, db):
        self.db = db

    def get(self, scan_id, fileset_id, file_id):
        """Send the requested (cached) image, may resize it if necessary.

        Parameters
        ----------
        scan_id : str
            The scan id.
        fileset_id : str
            The fileset id.
        file_id : str
            The file id.

        Returns
        -------
        flask.Response
            The HTTP response from the flask server.

        Examples
        --------
        >>> # In a terminal, start a (test) REST API with `fsdb_rest_api --test`, then:
        >>> import numpy as np
        >>> import requests
        >>> from io import BytesIO
        >>> from PIL import Image
        >>> # Get the first image as a thumbnail (default):
        >>> res = requests.get("http://127.0.0.1:5000/image/real_plant_analyzed/images/00000_rgb", stream=True)
        >>> img = Image.open(BytesIO(res.content))
        >>> np.asarray(img).shape
        (113, 150, 3)
        >>> # Get the first image in original size:
        >>> res = requests.get("http://127.0.0.1:5000/image/real_plant_analyzed/images/00000_rgb", stream=True, params={"size": "orig"})
        >>> img = Image.open(BytesIO(res.content))
        >>> np.asarray(img).shape
        (1080, 1440, 3)

        """
        # Sanitize identifiers
        scan_id = sanitize_name(scan_id)
        fileset_id = sanitize_name(fileset_id)
        file_id = sanitize_name(file_id)

        size = request.args.get('size', default='thumb', type=str)
        # Get the path to the image resource:
        path = webcache.image_path(self.db, scan_id, fileset_id, file_id, size)
        return send_file(path, mimetype='image/jpeg')


class PointCloud(Resource):
    """Concrete RESTful resource to serve a point-cloud upon request (GET method)."""

    def __init__(self, db):
        self.db = db

    def get(self, scan_id, fileset_id, file_id):
        """Send the requested (cached) point-cloud, may down-sample it if necessary.

        Parameters
        ----------
        scan_id : str
            The scan id.
        fileset_id : str
            The fileset id.
        file_id : str
            The file id.

        Returns
        -------
        flask.Response
            The HTTP response from the flask server.

        Examples
        --------
        >>> # In a terminal, start a (test) REST API with `fsdb_rest_api --test`, then:
        >>> import requests
        >>> from plyfile import PlyData
        >>> from io import BytesIO
        >>> # Get the triangular mesh:
        >>> res = requests.get("http://127.0.0.1:5000/pointcloud/real_plant_analyzed/PointCloud_1_0_1_0_10_0_7ee836e5a9/PointCloud")
        >>> pcd_data = PlyData.read(BytesIO(res.content))
        >>> # Access point X-coordinates:
        >>> list(pcd_data['vertex']['x'])

        """
        # Sanitize identifiers
        scan_id = sanitize_name(scan_id)
        fileset_id = sanitize_name(fileset_id)
        file_id = sanitize_name(file_id)

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
        # Get the path to the pointcloud resource:
        path = webcache.pointcloud_path(self.db, scan_id, fileset_id, file_id, size)
        return send_file(path, mimetype='application/octet-stream')


class PointCloudGroundTruth(Resource):
    """Concrete RESTful resource to serve a ground-truth point-cloud upon request (GET method)."""

    def __init__(self, db):
        self.db = db

    def get(self, scan_id, fileset_id, file_id):
        """Send the requested (cached) ground-truth point-cloud, may down-sample it if necessary.

        Parameters
        ----------
        scan_id : str
            The scan id.
        fileset_id : str
            The fileset id.
        file_id : str
            The file id.

        Returns
        -------
        flask.Response
            The HTTP response from the flask server.
        """
        # Sanitize identifiers
        scan_id = sanitize_name(scan_id)
        fileset_id = sanitize_name(fileset_id)
        file_id = sanitize_name(file_id)

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
        # Get the path to the pointcloud resource:
        path = webcache.pointcloud_path(self.db, scan_id, fileset_id, file_id, size)
        return send_file(path, mimetype='application/octet-stream')


class Mesh(Resource):
    """Concrete RESTful resource to serve a triangular mesh upon request (GET method)."""

    def __init__(self, db):
        self.db = db

    def get(self, scan_id, fileset_id, file_id):
        """Send the requested (cached) triangular mesh, may down-sample it if necessary.

        Parameters
        ----------
        scan_id : str
            The scan id.
        fileset_id : str
            The fileset id.
        file_id : str
            The file id.

        Returns
        -------
        flask.Response
            The HTTP response from the flask server.

        Examples
        --------
        >>> # In a terminal, start a (test) REST API with `fsdb_rest_api --test`, then:
        >>> import requests
        >>> from plyfile import PlyData
        >>> from io import BytesIO
        >>> # Get the triangular mesh:
        >>> res = requests.get("http://127.0.0.1:5000/mesh/real_plant_analyzed/TriangleMesh_9_most_connected_t_open3d_00e095c359/TriangleMesh")
        >>> mesh_data = PlyData.read(BytesIO(res.content))
        >>> # Access vertices X-coordinates:
        >>> list(mesh_data['vertex']['x'])

        """
        # Sanitize identifiers
        scan_id = sanitize_name(scan_id)
        fileset_id = sanitize_name(fileset_id)
        file_id = sanitize_name(file_id)

        size = request.args.get('size', default='orig', type=str)
        # Make sure that the 'size' argument we got is a valid option, else default to 'orig':
        if not size in ['orig']:
            size = 'orig'
        # Get the path to the mesh resource:
        path = webcache.mesh_path(self.db, scan_id, fileset_id, file_id, size)
        return send_file(path, mimetype='application/octet-stream')


class Sequence(Resource):
    """Concrete RESTful resource to serve an angle and internode sequences upon request (GET method)."""

    def __init__(self, db):
        self.db = db

    def get(self, scan_id):
        """Send the requested angle and internode sequences.

        Parameters
        ----------
        scan_id : str
            The scan id.

        Returns
        -------
        flask.Response
            The HTTP response from the flask server.

        Examples
        --------
        >>> # In a terminal, start a (test) REST API with `fsdb_rest_api --test`, then:
        >>> import requests
        >>> import json
        >>> # Get the whole dict with 'angles', 'internodes' & 'fruit_points' lists of values:
        >>> res = requests.get("http://127.0.0.1:5000/sequence/real_plant_analyzed")
        >>> json.loads(res.content.decode('utf-8'))
        >>> # Get the list of 'angles' values, could also append '?type=angles' to the URI:
        >>> res = requests.get("http://127.0.0.1:5000/sequence/real_plant_analyzed", params={'type': 'angles'})
        >>> json.loads(res.content.decode('utf-8'))

        """
        # Sanitize identifiers
        scan_id = sanitize_name(scan_id)

        type = request.args.get('type', default='all', type=str)
        # Get the `File` corresponding to the sequence resource:
        scan = self.db.get_scan(scan_id)
        task_fs_map = compute_fileset_matches(scan)
        fs = scan.get_fileset(task_fs_map['AnglesAndInternodes'])
        # Load the JSON file:
        measures = read_json(fs.get_file('AnglesAndInternodes'))
        # Make sure that the 'type' argument we got is a valid option, else default to 'all':
        if type in ['angles', 'internodes', 'fruit_points']:
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
    directory : str
        The path to the directory to check against.
    target : str
        The path to the target to check if it resides within the directory.

    Returns
    -------
    bool
        ``True`` if the target path is within the directory, ``False`` otherwise.
    """
    abs_directory = os.path.abspath(directory)
    abs_target = os.path.abspath(target)
    return os.path.commonpath([abs_directory]) == os.path.commonpath([abs_directory, abs_target])


class Archive(Resource):
    """Concrete RESTful resource to serve an archive of the dataset upon request (GET method)."""

    def __init__(self, db, logger):
        self.db = db
        self.logger = logger

    def get(self, scan_id):
        """Creates a ZIP archive for the specified scan dataset and serves it as adownloadable file.
        
        The archive is created as a temporary file and is scheduled for cleanup after the HTTP request is processed.
        Any parts of the dataset directory named 'webcache' are excluded from the archive.

        Parameters
        ----------
        scan_id : str
            The unique identifier for the scan dataset to be archived and
            served. This identifier is used to locate the relevant scan
            directory.

        Returns
        -------
        tuple
            If the scan is not found, returns a tuple containing an error message
            and an HTTP status code (400).
        flask.Response
            If the scan is found, returns a Flask response object to send the
            ZIP archive as a downloadable file.

        Examples
        --------
        >>> # In a terminal, start a (test) REST API with `fsdb_rest_api --test`, then:
        >>> import requests
        >>> import tempfile
        >>> from io import BytesIO
        >>> from pathlib import Path
        >>> from zipfile import ZipFile
        >>> # Get the archive for the 'real_plant_analyzed' dataset with:
        >>> zip_file = requests.get("http://127.0.0.1:5000/archive/real_plant_analyzed", stream=True)
        >>> # - Extract the archive:
        >>> # Read the zip file data into a BytesIO object
        >>> zip_data = BytesIO(zip_file.content)
        >>> # Create a temporary path to extract the archived data:
        >>> tmp_dir = Path(tempfile.mkdtemp())
        >>> # Open the zip file and extract non-existing files:
        >>> extracted_files = []
        >>> with ZipFile(zip_data, 'r') as zip_obj:
        >>>     for file in zip_obj.namelist():
        >>>         file_path = tmp_dir / file
        >>>         zip_obj.extract(file, path=tmp_dir)
        >>>         extracted_files.append(file)
        >>> # Print the list of extracted files:
        >>> extracted_files

        """
        scan_id = sanitize_name(scan_id)

        try:
            scan = self.db.get_scan(scan_id)
        except ScanNotFoundError:
            return {'error': f'Could not find a scan named `{scan_id}`!'}, 400

        tmp_dir = Path(mkdtemp(prefix='fsdb_rest_api_'))
        zpath = tmp_dir / f'{scan_id}.zip'

        self.logger.info(f"Creating archive for `{scan_id}` dataset.")
        try:
            with ZipFile(zpath, 'w') as zf:
                path = str(scan.path())
                for root, _dirs, files in os.walk(path):
                    # Exclude 'webcache' from the archive:
                    if 'webcache' in root:
                        continue
                    for file in files:
                        zf.write(
                            os.path.join(root, file),
                            os.path.relpath(os.path.join(root, file), os.path.join(path, '..'))
                        )
        except Exception as e:
            self.logger.error(f"Failed to create archive for `{scan_id}` dataset: {e}")
            return {'error': f'Failed to create archive for `{scan_id}` dataset: {e}'}, 500

        # Schedule the temporary file for cleanup after request completion
        @after_this_request
        def cleanup_temp_file(response):
            try:
                if zpath.exists():
                    zpath.unlink()
                    self.logger.info(f"Temporary archive `{zpath}` deleted.")
            except Exception as e:
                self.logger.error(f"Failed to delete temporary file `{zpath}`: {e}")
            return response

        return send_file(zpath, mimetype='application/zip')

    def post(self, scan_id):
        """Handles the HTTP POST request for uploading and processing a ZIP file.

        Validates the uploaded file's MIME type, extension, and integrity before
        extracting its content into a specific file system path. Only ensures the
        extracted files do not overwrite existing files and remain within the
        designated directory structure.

        Parameters
        ----------
        scan_id : any
            The identifier for the scan, used to map and associate the upload
            with the corresponding scan entry in the database.

        Returns
        -------
        dict
            A dictionary containing a success message and a list of the extracted
            files if the operation completes successfully.
            In case of failure, an error message and HTTP status code are returned.
        """
        # Get the zip file from the request
        zip_file = request.files.get('zip_file')
        # Check if a file was provided
        if not zip_file:
            return {'error': 'No ZIP file provided!'}, 400

        # Validate the file MIME type
        if zip_file.mimetype != 'application/zip':
            self.logger.error(f"Invalid MIME type: '{zip_file.mimetype}'. Expected 'application/zip'.")
            return {'error': 'Invalid file type. Only ZIP files are allowed.'}, 400

        # Validate the file extension
        if not zip_file.filename.lower().endswith('.zip'):
            self.logger.error(f"Invalid file extension for file: '{zip_file.filename}'.")
            return {'error': 'Invalid file extension. Only ZIP files are allowed.'}, 400

        # Create a temporary file to save the uploaded ZIP file to disk
        try:
            _, temp_path = mkstemp(prefix='tmp_file.name', suffix='.zip')
            temp_path = Path(temp_path)
            self.logger.debug(f"Saving uploaded ZIP temporary file to: '{temp_path}'")
            zip_file.save(temp_path)
        except Exception as e:
            self.logger.error(f"Error saving file to temporary location: {e}")
            return {'error': 'Failed to save file to disk.'}, 500

        # Verify the file is a valid ZIP archive before proceeding
        from zipfile import BadZipFile
        try:
            with ZipFile(temp_path, 'r') as zip_obj:
                # Test the ZIP file to ensure it's valid (raises an exception if corrupt)
                zip_obj.testzip()
        except BadZipFile:
            self.logger.error("The provided file is not a valid ZIP archive.")
            # Cleanup temporary file before returning
            Path(temp_path).unlink(missing_ok=True)
            return {'error': 'Invalid ZIP file provided.'}, 400

        # Proceed with processing the valid ZIP file
        self.logger.debug(f"REST API path to fsdb is '{self.db.path()}'...")
        scan_path = Path(self.db.get_scan(scan_id, create=True).path())
        self.logger.debug(f"Exporting archive contents to '{scan_path}'...")
        db_path = scan_path.parent  # move up to db path as the archive contain the top level

        # Open the zip file and extract non-existing files:
        extracted_files = []
        try:
            with ZipFile(temp_path, 'r') as zip_obj:
                for file in zip_obj.namelist():
                    # Ensure that filenames are properly encoded
                    try:
                        file = file.encode('utf-8').decode('utf-8')
                    except UnicodeDecodeError:
                        self.logger.error(f"Filename encoding issue detected in ZIP: '{file}'")
                        Path(temp_path).unlink(missing_ok=True)  # Cleanup temporary file
                        return {'error': 'Filename encoding error in zip archive'}, 400

                    file_path = db_path / file
                    # Ensure the extracted files remain within the target directory
                    if not is_within_directory(db_path, file_path):
                        self.logger.error(f"Invalid file path detected in ZIP: '{file}'")
                        Path(temp_path).unlink(missing_ok=True)  # Cleanup temporary file
                        return {'error': 'Invalid file paths in zip archive'}, 400

                    # Extract only if the file does not already exist
                    if not file_path.exists():
                        zip_obj.extract(file, path=db_path)
                        extracted_files.append(file)
        except Exception as e:
            self.logger.error(f"Failed to extract ZIP archive: {e}")
            Path(temp_path).unlink(missing_ok=True)  # Cleanup temporary file
            return {'error': f'Failed to extract ZIP archive: {e}'}, 500
        finally:
            # Always clean up the temporary ZIP file after processing
            Path(temp_path).unlink(missing_ok=True)

        # Return a success response
        return {'message': 'Zip file processed successfully', 'files': extracted_files}, 200
