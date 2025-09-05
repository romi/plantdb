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
import threading
import time
from collections import defaultdict
from functools import wraps
from io import BytesIO
from math import radians
from pathlib import Path
from tempfile import mkdtemp
from tempfile import mkstemp
from zipfile import ZipFile

from flask import Response
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
from plantdb.commons.log import get_logger
from plantdb.commons.utils import is_radians
from plantdb.server import webcache


def get_scan_date(scan):
    """Get the acquisition datetime of a scan.

    Try to get the data from the scan metadata 'acquisition_date', else from the directory creation time.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.Scan
        The scan instance to get the date & time from.

    Returns
    -------
    str
        The formatted datetime string.

    Examples
    --------
    >>> from plantdb.server.rest_api import get_scan_date
    >>> from plantdb.commons.test_database import test_database
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
    scan : plantdb.commons.fsdb.Scan
        The scan instance to list the filesets from.

    Returns
    -------
    dict
        A dictionary mapping the scan tasks to fileset names.

    Examples
    --------
    >>> from plantdb.server.rest_api import compute_fileset_matches
    >>> from plantdb.commons.fsdb import dummy_db
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
    f : plantdb.commons.fsdb.File
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
                "metadata": None,
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
    scan : plantdb.commons.fsdb.Scan
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
    >>> from plantdb.server.rest_api import get_scan_info
    >>> from plantdb.commons.test_database import test_database
    >>> db = test_database('real_plant_analyzed')
    >>> db.connect()
    >>> scan = db.get_scan('real_plant_analyzed')
    >>> scan_info = get_scan_info(scan)
    >>> print(scan_info)
    {'id': 'real_plant_analyzed', 'metadata': {'date': '2023-12-15 16:37:15', 'species': 'N/A', 'plant': 'N/A', 'environment': 'Lyon indoor', 'nbPhotos': 60, 'files': {'metadata': None, 'archive': None}}, 'thumbnailUri': '', 'hasTriangleMesh': True, 'hasPointCloud': True, 'hasPcdGroundTruth': False, 'hasCurveSkeleton': True, 'hasAnglesAndInternodes': True, 'hasSegmentation2D': False, 'hasSegmentedPcdEvaluation': False, 'hasPointCloudEvaluation': False, 'hasManualMeasures': False, 'hasAutomatedMeasures': True, 'hasSegmentedPointCloud': False, 'error': False, 'hasTreeGraph': True}
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
    metadata_json_path = os.path.join(f"/files/", scan.id, "metadata", "metadata.json")
    scan_info["metadata"]["files"]["metadata"] = metadata_json_path

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

    # Get the URI (file path) to the output of the tasks:
    for task, uri_key in task_filesUri_mapping.items():
        if scan_info[f"has{task}"]:
            fs = scan.get_fileset(task_fs_map[task])
            scan_info["filesUri"][uri_key] = get_file_uri(scan, fs, fs.get_file(task))

    # Get the workspace metadata
    scan_info["workspace"] = img_fs.get_metadata("workspace")
    # Get the camera metadata
    scan_info["camera"] = {}
    if img_f.get_metadata("colmap_camera") != {}:
        model, poses = _get_colmap_camera_model(scan)
        scan_info["camera"]["model"] = model
        scan_info["camera"]["poses"] = poses

    return scan_info


def _get_colmap_camera_model(scan):
    """Retrieve the COLMAP camera model and camera poses from a scan object.

    This function extracts the COLMAP camera model from the metadata of the first image file
    in the specified scan's image fileset. It then iterates over all RGB images in the same
    fileset to collect their camera poses, including translation vectors (`tvec`), rotation
    matrices (`rotmat`), and URIs for both original and thumbnail images.

    Parameters
    ----------
    scan : Scan object
        The scan object containing the image fileset with COLMAP metadata.

    Returns
    -------
    Tuple[str, List[Dict[str, Union[int, str, np.ndarray]]]]
        A tuple where the first element is the COLMAP camera model as a string, and the second
        element is a list of dictionaries. Each dictionary contains the following keys:
        - 'id': The unique identifier for the image file (int).
        - 'tvec': The translation vector of the camera pose (np.ndarray).
        - 'rotmat': The rotation matrix of the camera pose (np.ndarray).
        - 'photoUri': The URI to the original size image (str).
        - 'thumbnailUri': The URI to the thumbnail size image (str).

    See Also
    --------
    get_image_uri : Function used to generate URIs for images based on scan, fileset, and file objects.

    Examples
    --------
    >>> from plantdb.server.rest_api import _get_colmap_camera_model
    >>> from plantdb.commons.test_database import test_database
    >>> db = test_database('real_plant_analyzed')
    >>> db.connect()
    >>> scan = db.get_scan('real_plant_analyzed')
    >>> camera_model, poses = _get_colmap_camera_model(scan)
    >>> print(camera_model)
    {'height': 1080, 'id': 1, 'model': 'OPENCV', 'params': [1166.9518889440105, 1166.9518889440105, 720.0, 540.0, -0.0013571157486977348, -0.0013571157486977348, 0.0, 0.0], 'width': 1440}
    >>> print(poses[0])
    {'id': '00000_rgb', 'tvec': [369.4279687732083, 120.36109311437637, -62.07043190848918], 'rotmat': [[0.06475585405884698, -0.9971710205080586, 0.038165890845442085], [-0.3390191175518756, -0.0579549181538338, -0.9389926865509284], [0.9385481965778085, 0.04786630673761355, -0.34181295964290737]], 'photoUri': '/image/real_plant_analyzed/images/00000_rgb.jpg?size=orig', 'thumbnailUri': '/image/real_plant_analyzed/images/00000_rgb.jpg?size=thumb'}

    """
    img_fs = scan.get_fileset("images")
    img_f = img_fs.get_files()[0]

    model = img_f.get_metadata("colmap_camera")['camera_model']
    poses = []
    for img_f in img_fs.get_files(query={"channel": 'rgb'}):
        camera_md = img_f.get_metadata("colmap_camera")
        poses.append({
            "id": img_f.id,
            "tvec": camera_md['tvec'],
            "rotmat": camera_md['rotmat'],
            "photoUri": get_image_uri(scan.id, img_fs.id, img_f, size="orig"),
            "thumbnailUri": get_image_uri(scan.id, img_fs.id, img_f, size="thumb")
        })
    return model, poses


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
    >>> from plantdb.server.rest_api import get_file_uri
    >>> from plantdb.commons.test_database import test_database
    >>> from plantdb.server.rest_api import compute_fileset_matches
    >>> db = test_database('real_plant_analyzed')
    >>> db.connect()
    >>> scan = db.get_scan('real_plant_analyzed')
    >>> fs_match = compute_fileset_matches(scan)
    >>> fs = scan.get_fileset(fs_match['PointCloud'])
    >>> f = fs.get_file("PointCloud")
    >>> get_file_uri(scan, fs, f)
    '/files/real_plant_analyzed/PointCloud_1_0_1_0_10_0_7ee836e5a9/PointCloud.ply'
    """
    from plantdb.commons.fsdb import Scan
    from plantdb.commons.fsdb import Fileset
    from plantdb.commons.fsdb import File
    scan_id = scan.id if isinstance(scan, Scan) else scan
    fileset_id = fileset.id if isinstance(fileset, Fileset) else fileset
    file_id = file.path().name if isinstance(file, File) else file
    return f"/files/{scan_id}/{fileset_id}/{file_id}"


def get_image_uri(scan, fileset, file, size="orig"):
    """Return the URI for the corresponding `scan/fileset/file` tree.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.Scan or str
        A ``Scan`` instance or the name of the scan dataset.
    fileset : plantdb.commons.fsdb.Fileset or str
        A ``Fileset`` instance or the name of the fileset.
    file : plantdb.commons.fsdb.File or str
        A ``File`` instance or the name of the file.
    size : {'orig', 'large', 'thumb'} or int, optional
        If an integer, use  it as the size of the cached image to create and return.
        Otherwise, should be one of the following strings, default to `'orig'`:

          - `'thumb'`: image max width and height to `150`.
          - `'large'`: image max width and height to `1500`;
          - `'orig'`: original image, no chache;

    Returns
    -------
    str
        The URI for the corresponding `scan/fileset/file` tree.

    Examples
    --------
    >>> from plantdb.server.rest_api import get_image_uri
    >>> from plantdb.commons.test_database import test_database
    >>> from plantdb.server.rest_api import compute_fileset_matches
    >>> db = test_database('real_plant_analyzed')
    >>> db.connect()
    >>> scan = db.get_scan('real_plant_analyzed')
    >>> get_image_uri(scan, 'images', '00000_rgb.jpg', size='orig')
    '/image/real_plant_analyzed/images/00000_rgb.jpg?size=orig'
    >>> get_image_uri(scan, 'images', '00011_rgb.jpg', size='thumb')
    '/image/real_plant_analyzed/images/00011_rgb.jpg?size=thumb'
    """
    from plantdb.commons.fsdb import Scan
    from plantdb.commons.fsdb import Fileset
    from plantdb.commons.fsdb import File
    scan_id = scan.id if isinstance(scan, Scan) else scan
    fileset_id = fileset.id if isinstance(fileset, Fileset) else fileset
    file_id = file.path().name if isinstance(file, File) else file
    return f"/image/{scan_id}/{fileset_id}/{file_id}?size={size}"


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
    scan : plantdb.commons.fsdb.Scan
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
    >>> from plantdb.server.rest_api import get_scan_data
    >>> from plantdb.commons.test_database import test_database
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
        A sanitized name that conforms to the rules.

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


def rate_limit(max_requests=5, window_seconds=60):
    """Limits the number of requests a client can make within a specified time window.

    This function is a decorator that enforces rate limiting based on the maximum
    number of allowed requests (`max_requests`) and the time window size in seconds
    (`window_seconds`). It tracks incoming requests from clients using their IP
    addresses and ensures that they do not exceed the specified limit within the
    time window. If the limit is exceeded, it returns an HTTP ``429`` response.

    Parameters
    ----------
    max_requests : int, optional
        The maximum number of requests permitted within the time window (default is 5).
    window_seconds : int, optional
        The duration of the rate-limiting window in seconds
        (default is 60 seconds).

    Returns
    -------
    decorator : Callable
        A decorator that can wrap any function or endpoint to enforce rate limiting.

    Raises
    ------
    HTTPException
        If the rate limit is exceeded, it returns an HTTP 429 ("Too Many Requests")
        response to the client.

    Notes
    -----
    This implementation uses a thread lock to ensure thread safety when handling
    requests, making it suitable for multi-threaded environments. The requests
    data structure is a `defaultdict` that maps client IPs to a list of their
    request timestamps. Old requests outside the rate-limiting window are removed
    to maintain efficient memory usage.
    """
    requests = defaultdict(list)
    lock = threading.Lock()

    def decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            client_ip = request.remote_addr
            current_time = time.time()

            with lock:
                # Remove old requests outside the window
                requests[client_ip] = [req_time for req_time in requests[client_ip]
                                       if current_time - req_time < window_seconds]

                # Check if rate limit is exceeded
                if len(requests[client_ip]) >= max_requests:
                    return Response(
                        "Rate limit exceeded. Please try again later.",
                        status=429
                    )

                # Add current request
                requests[client_ip].append(current_time)

            return f(*args, **kwargs)

        return wrapped

    return decorator


# Home page resource
class Home(Resource):

    @rate_limit(max_requests=120, window_seconds=60)
    def get(self):
        """Return basic API information and documentation.

        Raises
        ------
        HTTPException
             If the rate limit is exceeded, it returns an HTTP 429 ("Too Many Requests") response to the client.
        """

        def _package_version(package_name):
            # Get plantdb.server version
            from importlib.metadata import version, PackageNotFoundError
            try:
                package_version = version(package_name)
            except PackageNotFoundError:
                package_version = "unknown"
            return package_version

        api_info = {
            "name": "PlantDB REST API",
            "description": "RESTful API for PlantDB querying",
            "plantdb.commons": _package_version("plantdb.commons"),
            "plantdb.server": _package_version("plantdb.server"),
            "endpoints": {
                "/": "This endpoint provides information about the PlantDB REST API",
                "/health": "Health check endpoint to verify API is working",
                "/scans": "List all available scans",
                "/scans_info": "Table with scan information",
                "/scans/<scan_id>": "Get information about a specific scan",
                "/files/<path>": "Access files from the database",
                "/files/<scan_id>": "Access dataset files for a specific scan",
                "/refresh": "Refresh the database",
                "/image/<scan_id>/<fileset_id>/<file_id>": "Access specific images",
                "/pointcloud/<scan_id>/<fileset_id>/<file_id>": "Access specific point clouds",
                "/pcGroundTruth/<scan_id>/<fileset_id>/<file_id>": "Access ground truth point clouds",
                "/mesh/<scan_id>/<fileset_id>/<file_id>": "Access specific meshes",
                "/skeleton/<scan_id>": "Access curve skeleton data",
                "/sequence/<scan_id>": "Access sequence data",
                "/archive/<scan_id>": "Access archive data",
                "/register": "Register a new user",
                "/login": "User login endpoint",
                "/api/scan": "Create a new scan",
                "/api/scan/<scan_id>/metadata": "Access/modify scan metadata",
                "/api/scan/<scan_id>/filesets": "Access scan filesets",
                "/api/fileset": "Create a new fileset",
                "/api/fileset/<scan_id>/<fileset_id>/metadata": "Access/modify fileset metadata",
                "/api/fileset/<scan_id>/<fileset_id>/files": "Access fileset files",
                "/api/file": "Create a new file",
                "/api/file/<scan_id>/<fileset_id>/<file_id>/metadata": "Access/modify file metadata"
            }
        }
        return api_info


# Resource HealthCheck
class HealthCheck(Resource):
    def __init__(self, db):
        self.db = db

    @rate_limit(max_requests=120, window_seconds=60)
    def get(self):
        """Simple test endpoint to verify the API is working correctly.

        Raises
        ------
        HTTPException
             If the rate limit is exceeded, it returns an HTTP 429 ("Too Many Requests") response to the client.
        """
        try:
            # Try to check database connection
            scan_count = len(self.db.list_scans(owner_only=False))
            return {
                "status": "healthy",
                "message": "API is running correctly",
                "database": {
                    "location": str(self.db.path()),
                    "scan_count": scan_count
                }
            }, 200
        except Exception as e:
            return {
                "status": "error",
                "message": f"API encountered an issue: {str(e)}"
            }, 500


class Register(Resource):
    """A RESTful resource to manage user registration via HTTP POST requests.

    Responsible for handling the registration process by validating and creating user records in the database.
    This class provides a structured way to interact with user data, ensuring error handling and
    proper responses for client requests.

    Attributes
    ----------
    db : plantdb.commons.fsdb.FSDB
        Database instance used for storing and managing user records.
    """

    def __init__(self, db):
        """Initialize the resource.

        Parameters
        ----------
        db : plantdb.commons.fsdb.FSDB
            Database object for with user records.
        """
        self.db = db

    @rate_limit(max_requests=5, window_seconds=60)  # maximum of 1 requests per minute
    def post(self):
        """Handle HTTP POST request to register a new user.

        Processes user registration by validating the input data and creating a new user in the database.
        Expects a JSON payload in the request body with required user details.

        Parameters
        ----------
        username : str
            Unique identifier for the user.
            Should originate from the JSON payload and be a non-empty string.
        fullname : str
            User's full name.
            Should originate from the JSON payload and be a non-empty string.
        password : str
            User's password for authentication.
            Should originate from the JSON payload and be a non-empty string.

        Returns
        -------
        dict
            A dictionary with the following keys and values:
                - 'success' (bool): Indicates if operation was successful
                - 'message' (str): Description of the operation result
        int
            HTTP status code (``201`` for success, ``400`` for error)

        Raises
        ------
        HTTPException
             If the rate limit is exceeded, it returns an HTTP 429 ("Too Many Requests") response to the client.

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> import requests
        >>> import json
        >>> # Create a new user:
        >>> new_user = {"username":"batman", "fullname":"Bruce Wayne", "password":"Alfred"}
        >>> response = requests.post("http://127.0.0.1:5000/register", json=new_user)
        >>> res_dict = response.json()
        >>> res_dict["success"]
        True
        >>> res_dict["message"]
        'User successfully created'
        """
        # Parse JSON data from request body
        data = request.get_json()

        # Check if all required fields are present in the request
        required_fields = ['username', 'fullname', 'password']
        if not data or not all(field in data for field in required_fields):
            return {
                'success': False,
                'message': 'Missing required fields. Please provide username, fullname, and password'
            }, 400

        try:
            # Attempt to create new user in database
            self.db.create_user(
                username=data['username'],
                fullname=data['fullname'],
                password=data['password']
            )
            # Return success response if user creation succeeds
            return {
                'success': True,
                'message': 'User successfully created'
            }, 201
        except Exception as e:
            # Return error response if user creation fails (e.g., duplicate username)
            return {
                'success': False,
                'message': f'Failed to create user: {str(e)}'
            }, 400


class Login(Resource):
    """A RESTful resource to handle user login and authentication processes.

    This class processes HTTP requests for user authentication, including checking
    if a username exists in the database and validating login credentials.

    Attributes
    ----------
    db : plantdb.commons.fsdb.FSDB
        A database object that provides access to user-related operations such as
        checking if a user exists and validating user credentials.
    """

    def __init__(self, db):
        """Initialize the resource.

        Parameters
        ----------
        db : plantdb.commons.fsdb.FSDB
            Database object for accessing user data.
        """
        self.db = db

    @rate_limit(max_requests=120, window_seconds=60)
    def get(self):
        """Checks if a given username exists in the database and returns the result.

        Parameters
        ----------
        username : str
            The username to check in the database.
            This must be a non-empty string and served as a query parameter.

        Returns
        -------
        dict
            A dictionary with the result and an HTTP status code.
            If the `username` is missing, the dictionary will contain an error message.
            Otherwise, the dictionary will contain the `username` and a boolean indicating whether it exist or not.
        int
            An HTTP status code. If the `username` is missing  the status code will be ``400``, otherwise ``200``.

        Raises
        ------
        HTTPException
             If the rate limit is exceeded, it returns an HTTP 429 ("Too Many Requests") response to the client.

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> import requests
        >>> import json
        >>> # Check if user exists (valid username):
        >>> response = requests.get("http://127.0.0.1:5000/login?username=anonymous")
        >>> print(response.json())
        {'username': 'anonymous', 'exists': True}
        >>> # Check if user exists (invalid username):
        >>> response = requests.get("http://127.0.0.1:5000/login?username=superman")
        >>> print(response.json())
        {'username': 'superman', 'exists': False}
        """
        # Extract username from query parameters
        username = request.args.get('username', None)
        # Return error if username parameter is missing
        if not username:
            return {'error': 'Missing username parameter'}, 400
        # Query database to check if user exists
        user_exists = self.db.user_exists(username)
        return {'username': username, 'exists': user_exists}, 200

    @rate_limit(max_requests=20, window_seconds=60)
    def post(self):
        """Handle user authentication via POST request with username and password.

        This method processes a POST request containing user credentials (username and password)
        and validates them against stored user data. It returns authentication status and
        a descriptive message.

        Returns
        -------
        dict
            A dictionary with the following keys and values:
                - 'authenticated' : bool
                    The result of the authentication process (``True`` if successful, ``False`` otherwise).
                - 'message' : str
                    A message describing the result of the authentication attempt.
        int
            The HTTP status code (``200`` for successful response, ``400`` for bad request).

        Raises
        ------
        BadRequest
            If the request doesn't contain valid JSON data (handled by Flask)
        HTTPException
             If the rate limit is exceeded, it returns an HTTP 429 ("Too Many Requests") response to the client.

        Notes
        -----
        The method expects a JSON payload with 'username' and 'password' fields.
        The authentication process uses the ``check_credentials`` method to validate
        the provided credentials against the database.

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> import requests
        >>> # Valid login request
        >>> response = requests.post('http://127.0.0.1:5000/login', json={'username': 'anonymous', 'password': 'AlanMoore'})
        >>> print(response.json())
        {'authenticated': True, 'message': 'Login successful. Welcome, Guy Fawkes!'}
        >>> print(response.status_code)
        200
        >>> # Invalid request (missing credentials)
        >>> response = requests.post('http://127.0.0.1:5000/login', json={'username': 'anonymous'})
        >>> print(response.json())
        {'authenticated': False, 'message': 'Missing username or password'}
        >>> print(response.status_code)
        400
        """
        # Get JSON data from the request body
        data = request.get_json()
        # Validate that required fields are present in the request
        if not data or 'username' not in data or 'password' not in data:
            return {'authenticated': False, 'message': 'Missing username or password'}, 400

        # Extract credentials from request data
        username = data['username']
        password = data['password']
        # Attempt to authenticate user with provided credentials
        is_authenticated = self.check_credentials(username, password)

        # Prepare response based on authentication result
        if is_authenticated:
            # Get user's full name from database for welcome message
            fullname = self.db.users[username]['fullname']
            message = f"Login successful. Welcome, {self.db.users[username]['fullname']}!"
        else:
            fullname = "None"
            message = f"Login failed. Please check your username and password!"

        # Return authentication result and appropriate message with 200 status code
        return {'authenticated': is_authenticated, 'fullname': fullname, 'message': message}, 200

    def check_credentials(self, username, password):
        """Validates user credentials against the database.

        Parameters
        ----------
        username : str
            The username provided by the user or client for authentication.
        password : str
            The password corresponding to the username for authentication.

        Returns
        -------
        bool
            ``True`` if the credentials are valid, ``False`` otherwise.
        """
        return self.db.validate_user(username, password)


class ScansList(Resource):
    """A RESTful resource for managing and retrieving scan datasets.

    This class implements a REST API endpoint that provides access to scan datasets.
    It supports filtered queries and fuzzy matching capabilities through HTTP GET
    requests.

    Attributes
    ----------
    db : plantdb.commons.fsdb.FSDB
        Database connection object used to interact with the scan datasets.

    See Also
    --------
    flask_restful.Resource : Base class for RESTful resources
    """

    def __init__(self, db):
        """Initialize the ScansList resource.

        Parameters
        ----------
        db : plantdb.commons.fsdb.FSDB
            Database connection object for accessing scan data.
        """
        self.db = db

    @rate_limit(max_requests=30, window_seconds=60)
    def get(self):
        """Retrieve a list of scan datasets with optional filtering.

        This endpoint provides access to scan datasets stored in the database. It allows
        filtering of results using a JSON-formatted query string and supports fuzzy
        matching for string-based searches.

        Parameters
        ----------
        filterQuery : str, optional
            JSON-formatted string containing filter criteria for querying datasets.
            Should be passed as a URL query parameter.
            Example: ``{"object":{"species":"Arabidopsis.*"}}``
        fuzzy : bool, optional
            Whether to enable fuzzy matching for string fields in the filter query.
            Should be passed as a URL query parameter.
            Default is ``False``.

        Returns
        -------
        list
            The List of scan datasets matching the filter criteria.
            Each item is a dictionary containing dataset metadata.
        int
            HTTP status code (``200`` for success, ``400``/``500`` for errors).

        Raises
        ------
        HTTPException
             If the rate limit is exceeded, it returns an HTTP 429 ("Too Many Requests") response to the client.

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> import requests
        >>> # Get an info dict about all dataset:
        >>> response = requests.get("http://127.0.0.1:5000/scans")
        >>> scans_list = response.json()
        >>> print(scans_list)  # List the known dataset ids
        ['arabidopsis000', 'virtual_plant_analyzed', 'real_plant_analyzed', 'real_plant', 'virtual_plant']
        >>> # Get datasets with fuzzy filtering
        >>> filter_query = {"object":{"species":"Arabidopsis.*"}}
        >>> response = requests.get("http://127.0.0.1:5000/scans", params={"filterQuery": json.dumps(filter_query), "fuzzy": "true"})
        >>> filtered_scans = response.json()
        >>> print(filtered_scans)  # List the filtered dataset ids
        """
        try:
            # Get filter query and fuzzy match parameters from request URL
            query = request.args.get('filterQuery', None)
            fuzzy = request.args.get('fuzzy', False, type=bool)
            # Parse JSON filter query if provided
            if query is not None:
                try:
                    query = json.loads(query)
                except json.JSONDecodeError:
                    return {'message': 'Invalid JSON format in filterQuery parameter.'}, 400
            # Query database for matching scans, allowing access to all owners
            scans = self.db.list_scans(query=query, fuzzy=fuzzy, owner_only=False)
            return scans, 200
        except Exception as e:
            # Return error response if any exception occurs
            return {'message': f'Error retrieving scan list: {str(e)}'}, 500


class ScansTable(Resource):
    """A RESTful resource for managing and retrieving scan dataset information.

    This class provides a REST API endpoint to serve information about scan datasets.
    It supports filtering datasets based on query parameters and returns detailed
    information about each matching scan.

    Attributes
    ----------
    db : plantdb.commons.fsdb.FSDB
        Database connection object used to interact with the scan datasets.
    logger : Logger
        The logger instance for this resource.

    See Also
    --------
    plantdb.server.rest_api.get_scan_info : Function used to extract information for each scan

    Examples
    --------
    >>> # Start a test REST API server first:
    >>> # $ fsdb_rest_api --test
    >>> import requests
    >>> import json
    >>> # Get all scan datasets
    >>> response = requests.get("http://127.0.0.1:5000/scans_info")
    >>> scans = json.loads(response.content)
    >>> print(scans[0]['id'])  # print the id of the first scan dataset
    >>> print(scans[0]['metadata'])  # print the metadata of the first scan dataset
    >>> # Get filtered results using query
    >>> query = {"object": {"species": "Arabidopsis.*"}}
    >>> response = requests.get("http://127.0.0.1:5000/scans_info", params={"filterQuery": json.dumps(query), "fuzzy": "true"})
    >>> filtered_scans = json.loads(response.content)
    >>> print(filtered_scans[0]['id'])  # print the id of the first scan dataset matching the query
    """

    def __init__(self, db, logger):
        """Initialize the resource.

        Parameters
        ----------
        db : plantdb.commons.fsdb.FSDB
            A database instance providing access to scan data.
        logger : loggin.Logger
            A logger instance for recording operations and errors.
        """
        self.db = db
        self.logger = logger

    @rate_limit(max_requests=120, window_seconds=60)
    def get(self):
        """Retrieve a list of scan dataset information.

        This method handles GET requests to retrieve scan information. It supports
        filtering through query parameters and returns detailed information about
        matching scans.

        Parameters
        ----------
        filterQuery : str, optional
            JSON string containing filter criteria for scans.
            Must be valid JSON that can be parsed into a query dict.
        fuzzy : bool, optional
            If ``True``, enables fuzzy matching in filter queries.
            Defaults to ``False``.

        Returns
        -------
        list of dict
            List of dictionaries containing scan information with:
            - 'metadata' (dict): Scan metadata including acquisition date, object info
            - 'tasks' (dict): Information about processing tasks
            - 'files' (dict): File paths and URIs related to the scan

        Raises
        ------
        JSONDecodeError
            If the provided filterQuery parameter is not valid JSON.
        HTTPException
             If the rate limit is exceeded, it returns an HTTP 429 ("Too Many Requests") response to the client.

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> import requests
        >>> import json
        >>> # Get an info dict about all dataset:
        >>> res = requests.get("http://127.0.0.1:5000/scans_info")
        >>> scans_list = json.loads(res.content)
        >>> # List the known dataset id:
        >>> print(scans_list)
        ['arabidopsis000', 'virtual_plant_analyzed', 'real_plant_analyzed', 'real_plant', 'virtual_plant', 'models']
        >>> res = requests.get('http://127.0.0.1:5000/scans_info?filterQuery={"object":{"species":"Arabidopsis.*"}}&fuzzy="true"')
        >>> res.content.decode()

        """
        query = request.args.get('filterQuery', None)
        fuzzy = request.args.get('fuzzy', False, type=bool)
        if query is not None:
            query = json.loads(query)
        scans_list = self.db.list_scans(query=query, fuzzy=fuzzy, owner_only=False)

        scans_info = []
        for scan_id in scans_list:
            scans_info.append(get_scan_info(self.db.get_scan(scan_id, create=False), logger=self.logger))
        return scans_info


class Scan(Resource):
    """A RESTful resource class for serving scan dataset information.

    This class handles HTTP GET requests for scan datasets, providing detailed
    information about the scan including metadata, file locations, and task status.

    Attributes
    ----------
    db : plantdb.commons.fsdb.FSDB
        The database instance used to retrieve scan information.
    logger : logging.Logger
        The logger instance for recording operations.

    Notes
    -----
    The class sanitizes scan IDs before processing requests to ensure security.
    All responses are returned as JSON-serializable dictionaries.

    See Also
    --------
    plantdb.server.rest_api.get_scan_info : Function used to collect and format scan information
    plantdb.server.rest_api.sanitize_name : Function used to validate and clean scan IDs
    """

    def __init__(self, db, logger):
        """Initialize the resource.

        Parameters
        ----------
        db : plantdb.commons.fsdb.FSDB
            A database instance providing access to scan data.
        logger : loggin.Logger
            A logger instance for recording operations and errors.
        """
        self.db = db
        self.logger = logger

    @rate_limit(max_requests=120, window_seconds=60)
    def get(self, scan_id):
        """Retrieve detailed information about a specific scan dataset.

        Parameters
        ----------
        scan_id : str
            Identifier for the scan dataset. Must contain only alphanumeric
            characters, underscores, dashes, or periods.

        Returns
        -------
        dict
            A dictionary containing scan information with the following keys:
            - 'metadata' (dict): Contains scan date, object information, and image count
            - 'files' (dict): Contains paths to related files and archives
            - 'tasks' (dict): Contains information about processing task status
            - 'thumbnail' (str): URI to the scan's thumbnail image

        Raises
        ------
        ValueError
            If the scan_id contains invalid characters
        NotFoundError
            If the specified scan does not exist in the database
        HTTPException
             If the rate limit is exceeded, it returns an HTTP 429 ("Too Many Requests") response to the client.

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> import requests
        >>> import json
        >>> # Get detailed information about a specific dataset
        >>> response = requests.get("http://127.0.0.1:5000/scans/real_plant_analyzed")
        >>> scan_data = json.loads(response.content)
        >>> # Access metadata information
        >>> print(scan_data['metadata']['date'])
        2024-08-19 11:12:25
        >>> # Check if point cloud processing is complete
        >>> print(scan_data['tasks']['point_cloud'])
        True
        """
        scan_id = sanitize_name(scan_id)
        # return get_scan_data(self.db.get_scan(scan_id), logger=self.logger)
        return get_scan_info(self.db.get_scan(scan_id, create=False), logger=self.logger)

    @rate_limit(max_requests=15, window_seconds=60)
    def post(self, scan_id):
        """Create a new scan dataset.

        Parameters
        ----------
        scan_id : str
            Identifier for the new scan dataset.
            Must contain only alphanumeric characters, underscores, dashes, or periods.

        Returns
        -------
        dict
            A dictionary containing the response with following possible structures:
                - On success: {'message': 'Scan created successfully', 'scan_id': scan_id}
                - On error: {'error': error_message}

        Raises
        ------
        HTTPException
             If the rate limit is exceeded, it returns an HTTP 429 ("Too Many Requests") response to the client.

        Notes
        -----
        HTTP status codes:
            - 201 : Created successfully
            - 400 : Bad request (invalid scan_id)
            - 409 : Conflict (scan already exists)
            - 500 : Internal server error
        """
        # Sanitize and validate the scan_id
        scan_id = sanitize_name(scan_id)
        try:
            # Attempt to create a new scan in the database with the given scan_id
            scan = self.db.get_scan(scan_id, create=True)
            # Check if scan creation was successful
            if scan is None:
                self.logger.error(f"Failed to create scan: {scan_id}")
                return {'error': 'Failed to create scan'}, 500
            self.logger.info(f"Successfully created scan: {scan_id}")
            # Return success response with HTTP 201 (Created) status code
            return {
                'message': 'Scan created successfully',
                'scan_id': scan_id
            }, 201
        except ValueError as e:
            # Handle case where scan_id format is invalid (e.g., wrong characters or length)
            self.logger.warning(f"Invalid scan_id format: {scan_id}")
            return {'error': str(e)}, 400  # HTTP 400 Bad Request
        except Exception as e:
            # Handle all other exceptions including duplicate scans
            self.logger.error(f"Error creating scan {scan_id}: {str(e)}")
            # Check if error is due to duplicate scan_id
            if "already exists" in str(e).lower():
                return {'error': f"Scan '{scan_id}' already exists"}, 409  # HTTP 409 Conflict
            # Return generic server error for all other exceptions
            return {'error': 'Internal server error'}, 500  # HTTP 500 Internal Server Error


class File(Resource):
    """A RESTful resource class for serving files via HTTP GET requests.

    This class implements a REST API endpoint that serves files from a specified database location.

    Attributes
    ----------
    db : plantdb.commons.fsdb.FSDB
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
        db : plantdb.commons.fsdb.FSDB
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
        HTTPException
             If the rate limit is exceeded, it returns an HTTP 429 ("Too Many Requests") response to the client.

        Notes
        -----
        The file serving is handled securely through Flask's `send_from_directory`,
        which prevents directory traversal attacks and handles file access permissions.

        Examples
        --------
        >>> # Start the REST API server (in test mode)
        >>> # fsdb_rest_api --test
        >>> # Request a TOML configuration file
        >>> import requests
        >>> import toml
        >>> res = requests.get("http://127.0.0.1:5000/files/real_plant_analyzed/pipeline.toml")
        >>> cfg = toml.loads(res.content.decode())
        >>> print(cfg['Undistorted'])
        {'upstream_task': 'ImagesFilesetExists'}
        >>> # Request a JSON file
        >>> import json
        >>> res = requests.get("http://127.0.0.1:5000/files/Col-0_E1_1/files.json")
        >>> json.loads(res.content.decode())
        """
        return send_from_directory(self.db.path(), path)


class DatasetFile(Resource):
    """A RESTful resource handler for file upload operations in a plant database system.

    Attributes
    ----------
    db : plantdb.commons.fsdb.FSDB
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

    def __init__(self, db):
        """Initialize the resource.

        Parameters
        ----------
        db : plantdb.commons.fsdb.FSDB
            A database instance providing access to file locations.
        """
        self.db = db

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
            - 201: Successful upload with message confirming file save
            - 400: Bad request (missing headers or invalid parameters)
            - 500: Server error during file processing

        Notes
        -----
        Required HTTP headers:
        - 'Content-Disposition': Contains file information
        - 'Content-Length': Size of file in bytes
        - 'X-File-Path': Relative path where file should be saved
        - 'X-Chunk-Size' (optional): Size of chunks for streamed upload

        The method will automatically create any necessary directories in the path.
        Partial uploads are automatically cleaned up if they fail.

        Raises
        ------
        Exception
            When database access fails or file operations encounter errors.
            All exceptions are caught and returned as HTTP 400 or 500 responses.
        HTTPException
             If the rate limit is exceeded, it returns an HTTP 429 ("Too Many Requests") response to the client.

        See Also
        --------
        plantdb.commons.io.write_stream : Helper function for chunked file uploads
        plantdb.commons.io.write_data : Helper function for complete file uploads

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
    """RESTful resource for reloading the database on demand.

    A concrete implementation of Flask-RESTful Resource that provides an endpoint
    to force reload the plant database. This is useful when the underlying data
    has changed and needs to be refreshed in the running application.

    Attributes
    ----------
    db : plantdb.commons.fsdb.FSDB
        The database instance used for reloading data.
    """

    def __init__(self, db):
        """Initialize the resource.

        Parameters
        ----------
        db : plantdb.commons.fsdb.FSDB
            A database instance to reload.
        """
        self.db = db

    @rate_limit(max_requests=60, window_seconds=60)
    def get_specific_scan(self, scan_id):
        """Reload data for a specific scan in the database.

        Parameters
        ----------
        scan_id : str
            Identifier for the specific plant scan to reload

        Returns
        -------
        dict, int
            A dictionary with a success message and HTTP status code 200,
            or an error message and status code 500

        Raises
        ------
        HTTPException
             If the rate limit is exceeded, it returns an HTTP 429 ("Too Many Requests") response to the client.
        """
        try:
            self.db.reload(scan_id)
            return {'message': f"Successfully reloaded scan '{scan_id}'."}, 200
        except Exception as e:
            return {'message': f"Error during scan reload: {str(e)}"}, 500

    @rate_limit(max_requests=12, window_seconds=60)
    def get_full_database(self):
        """Reload the entire plant database.

        Returns
        -------
        dict, int
            A dictionary with a success message and HTTP status code 200,
            or an error message and status code 500

        Raises
        ------
        HTTPException
             If the rate limit is exceeded, it returns an HTTP 429 ("Too Many Requests") response to the client.
        """
        try:
            self.db.reload(None)
            return {'message': f"Successfully reloaded entire database with {len(self.db.list_scans())} scans."}, 200
        except Exception as e:
            return {'message': f"Error during full database reload: {str(e)}"}, 500

    def get(self):
        """Force the plant database to reload.

        This endpoint triggers a reload of the plant database data. It can either reload the
        entire database or selectively reload data for a specific plant scan.

        Parameters
        ----------
        scan_id : str, optional
            Identifier for a specific plant scan to reload. If not provided,
            reloads the entire database.

        Returns
        -------
        flask.Response
            A Response object with:
            - Status code ``200`` and success message on successful reload
            - Status code ``500`` and error message if reload fails

        Raises
        ------
        FilesetNotFoundError
            If the specified scan_id refers to a non-existent fileset
        ScanNotFoundError
            If the specified scan_id refers to a non-existent scan
        Exception
            For any other unexpected errors during reload

        Notes
        -----
        This endpoint is rate-limited to ``1`` request per minute to prevent excessive
        database reloads.

        See Also
        --------
        plantdb.server.rest_api.rate_limit : Decorator that implements request rate limiting
        plantsb.fsdb.FSDB.reload : The underlying database reload method

        Examples
        --------
        >>> # Start the REST API server (in test mode)
        >>> # fsdb_rest_api --test
        >>> import requests
        >>> # Refresh entire database
        >>> response = requests.get("http://127.0.0.1:5000/refresh")
        >>> response.status_code
        200
        >>>
        >>> # Refresh specific scan
        >>> response = requests.get("http://127.0.0.1:5000/refresh?scan_id=real_plant")
        >>> response.status_code
        200
        """
        scan_id = request.args.get('scan_id', default=None, type=str)

        if scan_id:
            return self.get_specific_scan(scan_id)
        else:
            return self.get_full_database()


class Image(Resource):
    """RESTful resource for serving and resizing images on demand.

    This class handles HTTP GET requests for images stored in the database,
    with optional resizing capabilities. It serves both original and
    thumbnail versions of images based on the request parameters.

    Attributes
    ----------
    db : plantdb.commons.fsdb.FSDB
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
        db : plantdb.commons.fsdb.FSDB
            Database instance for accessing stored images.
        """
        self.db = db

    @rate_limit(max_requests=120, window_seconds=60)
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
        size : {'orig', 'large', 'thumb'} or int, optional
            If an integer, use  it as the size of the cached image to create and return.
            Otherwise, should be one of the valid string.
            Should be passed as a URL query parameter.
            Default to `'thumb'`

        Returns
        -------
        flask.Response
            HTTP response containing the image data with 'image/jpeg' mimetype.

        Raises
        ------
        werkzeug.exceptions.NotFound
            If the requested image file doesn't exist.
        HTTPException
             If the rate limit is exceeded, it returns an HTTP 429 ("Too Many Requests") response to the client.

        Notes
        -----
        - All input parameters are sanitized before use.
        - The 'size' parameter defaults to 'thumb' if not specified.
        - Supported string size values are:
            * `'thumb'`: image max width and height to `150`;
            * `'large'`: image max width and height to `1500`;
            * `'orig'`: original image, no chache;

        See Also
        --------
        plantdb.server.rest_api.sanitize_name : Input sanitization & validation function.
        plantdb.server.webcache.image_path : Image path resolution function with caching and resizing options.

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
    """RESTful resource for serving and optionally downsampling point cloud data.

    This class handles HTTP GET requests for point cloud data stored in PLY format,
    with support for different sampling densities. It can serve both original and
    preview versions of point clouds, or custom downsampling based on voxel size.

    Attributes
    ----------
    db : plantdb.commons.fsdb.FSDB
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
        db : plantdb.commons.fsdb.FSDB
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
        size : {'orig', 'preview'} or float, optional
            If a float, use it to downsample the pointcloud and return.
            Otherwise, should be one of the valid string.
            Should be passed as a URL query parameter.
            Default to `'orig'`

        Returns
        -------
        flask.Response
            HTTP response containing the PLY data with 'application/octet-stream' mimetype.

        Raises
        ------
        werkzeug.exceptions.NotFound
            If the requested point-cloud file doesn't exist.
        HTTPException
             If the rate limit is exceeded, it returns an HTTP 429 ("Too Many Requests") response to the client.

        Notes
        -----
        - The 'size' parameter can be:
            * 'orig': original point cloud
            * 'preview': downsampled preview version
            * float value: custom voxel size for downsampling
        - Defaults to 'preview' if size parameter is invalid
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
        >>> res = requests.get("http://127.0.0.1:5000/pointcloud/real_plant_analyzed/PointCloud_1_0_1_0_10_0_7ee836e5a9/PointCloud")
        >>> pcd_data = PlyData.read(BytesIO(res.content))
        >>> # Access point X-coordinates:
        >>> list(pcd_data['vertex']['x'])
        >>> # Get preview (downsampled) version
        >>> res = requests.get("http://127.0.0.1:5000/pointcloud/real_plant_analyzed/PointCloud_1_0_1_0_10_0_7ee836e5a9/PointCloud", params={"size": "preview"})
        >>> # Get custom downsampled version (voxel size 0.01)
        >>> res = requests.get("http://127.0.0.1:5000/pointcloud/real_plant_analyzed/PointCloud_1_0_1_0_10_0_7ee836e5a9/PointCloud", params={"size": "0.01"})

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
    """A RESTful resource for serving ground-truth point-cloud data.

    This class handles HTTP GET requests for point-cloud data, with optional
    downsampling capabilities based on the requested size parameter.

    Attributes
    ----------
    db : plantdb.commons.fsdb.FSDB
        The database instance used to retrieve point-cloud data.
    """

    def __init__(self, db):
        """Initialize the PointCloudGroundTruth resource.

        Parameters
        ----------
        db : plantdb.commons.fsdb.FSDB
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
        size : {'orig', 'preview'} or float, optional
            If a float, use it to downsample the pointcloud and return.
            Otherwise, should be one of the valid string.
            Should be passed as a URL query parameter.
            Default to `'orig'`

        Returns
        -------
        flask.Response
            HTTP response containing the point-cloud data as an octet-stream.

        Raises
        ------
        werkzeug.exceptions.NotFound
            If the requested point-cloud file doesn't exist.
        HTTPException
             If the rate limit is exceeded, it returns an HTTP 429 ("Too Many Requests") response to the client.

        Notes
        -----
        - The 'size' parameter can be specified in the query string as:
            * 'orig': Original size
            * 'preview': Preview size (default)
            * A float value: Custom voxel size for downsampling
        - All identifiers are sanitized before use
        - Invalid size parameters default to 'preview'
        - Response mimetype is 'application/octet-stream'

        Examples
        --------
        >>> # Request original size point-cloud
        >>> response = get('/api/scan123/fileset1/cloud1?size=orig')
        >>>
        >>> # Request preview size
        >>> response = get('/api/scan123/fileset1/cloud1?size=preview')
        >>>
        >>> # Request custom voxel size
        >>> response = get('/api/scan123/fileset1/cloud1?size=0.01')
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
    """RESTful resource for serving triangular mesh data via HTTP.

    This class implements a REST endpoint that provides access to triangular mesh data
    stored in a database. It supports GET requests and can optionally handle mesh
    size parameters.

    Attributes
    ----------
    db : plantdb.commons.fsdb.FSDB
        Reference to the database instance.

    Notes
    -----
    The mesh data is served in PLY format as an octet-stream.
    """

    def __init__(self, db):
        """Initialize the Mesh resource.

        Parameters
        ----------
        db : plantdb.commons.fsdb.FSDB
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
        size : {'orig'}, optional
            A string value specifying the size of the mesh to return.
            Should be passed as a URL query parameter.
            Default to `'orig'` (currently the only valid options).

        Returns
        -------
        flask.Response
            HTTP response containing the mesh data as an octet-stream.

        Raises
        ------
        werkzeug.exceptions.NotFound
            If the requested mesh file doesn't exist
        HTTPException
             If the rate limit is exceeded, it returns an HTTP 429 ("Too Many Requests") response to the client.

        Notes
        -----
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

        size = request.args.get('size', default='orig', type=str)
        # Make sure that the 'size' argument we got is a valid option, else default to 'orig':
        if not size in ['orig']:
            size = 'orig'
        # Get the path to the mesh resource:
        path = webcache.mesh_path(self.db, scan_id, fileset_id, file_id, size)
        return send_file(path, mimetype='application/octet-stream')


class CurveSkeleton(Resource):
    """A RESTful resource that provides access to curve skeleton data for plant scans.

    This class implements a REST API endpoint that serves curve skeleton data stored in JSON
    format. It handles GET requests to retrieve skeleton data for a specific scan ID.

    Attributes
    ----------
    db : plantdb.commons.fsdb.FSDB
        Database instance containing plant scan data and associated filesets.
    """

    def __init__(self, db):
        """Initialize the CurveSkeleton resource.

        Parameters
        ----------
        db : plantdb.commons.fsdb.FSDB
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
        ScanNotFoundError
            If the requested scan ID doesn't exist in the database
        FilesetNotFoundError
            If the CurveSkeleton fileset is not found for the scan
        FileNotFoundError
            If the CurveSkeleton file is missing from the fileset
        HTTPException
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
        >>> import json
        >>>
        >>> # Fetch skeleton data for a valid scan
        >>> response = requests.get("http://127.0.0.1:5000/skeleton/Col-0_E1_1")
        >>> skeleton_data = json.loads(response.content)
        >>> print(list(skeleton_data.keys()))
        ['angles', 'internodes', 'metadata']
        >>>
        >>> # Example with invalid scan ID
        >>> response = requests.get("http://127.0.0.1:5000/skeleton/invalid_id")
        >>> print(response.status_code)
        400
        >>> print(json.loads(response.content))
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
    db : plantdb.commons.fsdb.FSDB
        The database instance used for retrieving scan data.
    """

    def __init__(self, db):
        """Initialize the Sequence resource.

        Parameters
        ----------
        db : plantdb.commons.fsdb.FSDB
            A database instance containing plant scan data and related measurements.
        """
        self.db = db

    @rate_limit(max_requests=5, window_seconds=60)
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
                Dictionary containing all sequence data with keys 'angles', 'internodes',
                and 'fruit_points'
            If successful and type in ['angles', 'internodes', 'fruit_points']:
                List of sequence values for the specified type
            If error:
                Tuple of (error_dict, HTTP_status_code)

        Raises
        ------
        ScanNotFoundError
            If the specified scan_id does not exist in the database
        FilesetNotFoundError
            If the AnglesAndInternodes fileset is not found
        FileNotFoundError
            If the AnglesAndInternodes file is not found within the fileset
        HTTPException
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
        >>> import json
        >>> response = requests.get("http://127.0.0.1:5000/sequence/real_plant_analyzed")
        >>> data = json.loads(response.content.decode('utf-8'))
        >>> # Expected output: {'angles': [...], 'internodes': [...], 'fruit_points': [...]}

        >>> # Get only angles data
        >>> response = requests.get(
        ...     "http://127.0.0.1:5000/sequence/real_plant_analyzed",
        ...     params={'type': 'angles'}
        ... )
        >>> angles = json.loads(response.content.decode('utf-8'))
        >>> # Expected output: [angle1, angle2, ...]
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


def is_directory_in_archive(archive_path, target_dir):
    """
    Check if a specific directory exists within an archive file.

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
    db : plantdb.commons.fsdb.FSDB
        A database instance for accessing and managing scan data.
    logger : logging.Logger
        A logger instance for recording operations and errors.
    """

    def __init__(self, db, logger):
        """Initialize the Archive resource.

        Parameters
        ----------
        db : plantdb.commons.fsdb.FSDB
            A database instance for accessing and managing scan data.
        logger : logging.Logger
            A logger instance for recording operations and errors.
        """
        self.db = db
        self.logger = logger

    @rate_limit(max_requests=5, window_seconds=60)
    def get(self, scan_id):
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
            return {'error': f'Failed to create archive for `{scan_id}` dataset: {e}'}, 400

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

    @rate_limit(max_requests=5, window_seconds=60)
    def post(self, scan_id):
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
        >>> # Using requests to upload a ZIP file:
        >>> import requests
        >>> files = {'zip_file': open('dataset.zip', 'rb')}
        >>> response = requests.post("http://127.0.0.1:5000/archive/my_scan", files=files)
        >>> print(response.json())
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

        if is_directory_in_archive(temp_path, scan_id):
            extract_to_path = scan_path.parent  # move up to db path as the archive contain the top level
        else:
            extract_to_path = scan_path

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

                    file_path = extract_to_path / file
                    # Ensure the extracted files remain within the target directory
                    if not is_within_directory(extract_to_path, file_path):
                        self.logger.error(f"Invalid file path detected in ZIP: '{file}'")
                        Path(temp_path).unlink(missing_ok=True)  # Cleanup temporary file
                        return {'error': 'Invalid file paths in zip archive'}, 400

                    # Extract only if the file does not already exist
                    if not file_path.exists():
                        zip_obj.extract(file, path=extract_to_path)
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


class ScanCreate(Resource):
    """Represents a Scan resource creation endpoint in the application.

    This class provides the functionality to create new scans in the database.

    Attributes
    ----------
    db : plantdb.commons.fsdb.FSDB
        A database instance used to create scans.
    logger : logging.Logger
        A logger instance for recording operations.
    """

    def __init__(self, db, logger):
        self.db = db
        self.logger = logger

    def post(self):
        """Create a new scan in the database.

        This method handles POST requests to create a new scan. It validates the input data,
        ensures required fields are present, and creates the scan with the specified name
        and optional metadata.

        Notes
        -----
        The method expects a JSON request body with the following structure:
        {
            'name': str,          # Required: Name of the scan
            'metadata': dict      # Optional: Additional metadata for the scan
        }

        Raises
        ------
        Exception
            Any unexpected errors during scan creation are caught and
            returned as 500 error responses.

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> import requests
        >>> from plantdb.client.rest_api import base_url
        >>> # Create a new scan with metadata:
        >>> metadata = {'description': 'Test plant scan'}
        >>> url = f"{base_url()}/api/scan"
        >>> response = requests.post(url, json={'name': 'test_plant', 'metadata': metadata})
        >>> print(response.status_code)
        201
        >>> print(response.json())
        {'message': "Scan 'test_plant' created successfully."}
        """
        # Get JSON data from request
        data = request.get_json()
        if not data:
            return {'message': 'No input data provided'}, 400
        # Validate required fields
        if 'name' not in data:
            return {'message': 'Name is required'}, 400
        # Get metadata if provided
        metadata = data.get('metadata', {})

        try:
            # Sanitize the name
            scan_id = sanitize_name(data['name'])
            # Create the scan
            scan = self.db.create_scan(scan_id)
            # Set metadata if provided
            if metadata:
                scan.set_metadata(metadata)
            return {'message': f"Scan '{scan_id}' created successfully."}, 201

        except Exception as e:
            return {'message': f'Error creating scan: {str(e)}'}, 500


class ScanMetadata(Resource):
    """Resource class for managing scan metadata operations through REST API endpoints.

    This class provides HTTP endpoints for retrieving and updating scan metadata through
    a RESTful interface. It handles both complete metadata operations and individual key access.

    Attributes
    ----------
    db : plantdb.commons.fsdb.FSDB
        Database instance for accessing and managing scan data.
    logger : logging.Logger
        Logger instance for recording operations and errors.
    """

    def __init__(self, db, logger):
        self.db = db
        self.logger = logger

    def get(self, scan_id):
        """Retrieve metadata for a specified scan.

        This method retrieves the metadata dictionary for a scan. Optionally, it can
        return the value for a specific metadata key.

        Parameters
        ----------
        scan_id : str
            The ID of the scan.
        key : str, optional
            If provided, returns only the value for this specific metadata key.

        Returns
        -------
        Union[dict, Any]
            If key is None, returns the complete metadata dictionary.
            If key is provided, returns the value for that key.

        Raises
        ------
        ScanNotFoundError
            If the specified scan doesn't exist.
        KeyError
            If the specified key doesn't exist in the metadata.

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> import requests
        >>> from plantdb.client.rest_api import base_url
        >>> # Create a new scan with metadata:
        >>> metadata = {'description': 'Test plant scan'}
        >>> url = f"{base_url()}/api/scan"
        >>> response = requests.post(url, json={'name': 'test_plant', 'metadata': metadata})
        >>> # Get all metadata:
        >>> url = f"{base_url()}/api/scan/test_plant/metadata"
        >>> response = requests.get(url)
        >>> print(response.json())
        {'metadata': {'owner': 'anonymous', 'description': 'Test plant scan'}}
        >>> # Get specific metadata key:
        >>> response = requests.get(url+"?key=description")
        >>> print(response.json())
        {'metadata': 'Test plant scan'}
        """
        key = request.args.get('key', default=None, type=str)
        try:
            # Get the scan
            scan = self.db.get_scan(scan_id, create=False)
            if not scan:
                return {'message': 'Scan not found'}, 404

            # Get the metadata
            self.logger.debug(f"Got a metadata key '{key}' for scan '{scan_id}'...")
            metadata = scan.get_metadata(key)
            return {'metadata': metadata}, 200

        except Exception as e:
            self.logger.error(f'Error retrieving metadata: {str(e)}')
            return {'message': f'Error retrieving metadata: {str(e)}'}, 500

    def post(self, scan_id):
        """Update metadata for a specified scan.

        Parameters
        ----------
        scan_id : str
            The ID of the scan to update metadata for

        Returns
        -------
        dict
            Response dictionary with either:
                - 'metadata': Updated metadata dictionary on success
                - 'message': Error message on failure
        int
            HTTP status code (200 for success, 4xx/5xx for errors)

        Notes
        -----
        The request body should be a JSON object containing:
        - 'metadata' (dict): Required. The metadata to update/set
        - 'replace' (bool): Optional. If ``True``, replaces entire metadata.
                           If ``False`` (default), updates only specified keys.

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> import requests
        >>> from plantdb.client.rest_api import base_url
        >>> # Create a new scan with metadata:
        >>> metadata = {'description': 'Test plant scan'}
        >>> url = f"{base_url()}/api/scan"
        >>> response = requests.post(url, json={'name': 'test_plant', 'metadata': metadata})
        >>> # Update scan metadata:
        >>> url = f"{base_url()}/api/scan/test_plant/metadata"
        >>> data = {"metadata": {"description": "Updated scan description"}}
        >>> response = requests.post(url, json=data)
        >>> print(response.json())
        {'metadata': {'owner': 'anonymous', 'description': 'Updated scan description'}}
        """
        try:
            # Get request data
            data = request.get_json()
            if not data or 'metadata' not in data:
                return {'message': 'No metadata provided in request'}, 400

            metadata = data['metadata']
            replace = data.get('replace', False)

            if not isinstance(metadata, dict):
                return {'message': 'Metadata must be a dictionary'}, 400

            # Get the scan
            scan = self.db.get_scan(scan_id, create=False)
            if not scan:
                return {'message': 'Scan not found'}, 404

            # Update the metadata
            scan.set_metadata(metadata)
            # TODO: make this works:
            # if replace:
            #    # Replace entire metadata dictionary
            #    scan.set_metadata(metadata)
            # else:
            #    # Update only specified keys
            #    current_metadata = scan.get_metadata()
            #    current_metadata.update(metadata)
            #    scan.set_metadata(current_metadata)

            # Return updated metadata
            updated_metadata = scan.get_metadata()
            return {'metadata': updated_metadata}, 200

        except Exception as e:
            self.logger.error(f'Error updating metadata: {str(e)}')
            return {'message': f'Error updating metadata: {str(e)}'}, 500


class ScanFilesets(Resource):
    """REST API resource for managing scan filesets operations.

    This class provides endpoints to interact with filesets within a scan. It allows
    listing filesets with optional query filtering and fuzzy matching capabilities.

    Attributes
    ----------
    db : plantdb.commons.fsdb.FSDB
        A database instance for accessing scan and create fileset.
    logger : logging.Logger
        A logger instance for recording operations.
    """

    def __init__(self, db, logger):
        self.db = db
        self.logger = logger

    def get(self, scan_id):
        """List all filesets in a specified scan.

        This method retrieves the list of filesets contained in a scan using the
        `list_filesets()` method from `plantdb.commons.fsdb.Scan`.

        Parameters
        ----------
        scan_id : str
            The ID of the scan.

        Returns
        -------
        dict
            Response containing either:
              - On success (200): {'filesets': list of fileset IDs}
              - On error (404, 500): {'message': error description}
        int
            HTTP status code (200, 404, or 500)

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> import requests
        >>> from plantdb.client.rest_api import base_url
        >>> # List filesets in a scan:
        >>> url = f"{base_url()}/api/scan/real_plant/filesets"
        >>> response = requests.get(url)
        >>> print(response.status_code)
        200
        >>> print(response.json())
        {'filesets': ['images']}
        """
        query = request.args.get('query', default=None, type=str)
        fuzzy = request.args.get('fuzzy', default=False, type=bool)

        try:
            # Get the scan
            scan = self.db.get_scan(scan_id, create=False)
            if not scan:
                return {'message': 'Scan not found'}, 404

            # Get the list of filesets
            filesets = scan.list_filesets(query, fuzzy)
            return {'filesets': filesets}, 200

        except Exception as e:
            self.logger.error(f'Error listing filesets: {str(e)}')
            return {'message': f'Error listing filesets: {str(e)}'}, 500


class FilesetCreate(Resource):
    """Represents a Fileset resource in the application.

    This class provides the functionality to create and manage filesets associated with scans.

    Attributes
    ----------
    db : plantdb.commons.fsdb.FSDB
        A dDatabase instance for accessing scan and create fileset.
    logger : logging.Logger
        A logger instance for recording operations.
    """

    def __init__(self, db, logger):
        self.db = db
        self.logger = logger

    def post(self):
        """Create a new fileset associated with a scan.

        This method handles POST requests to create a new fileset. It validates the input data,
        ensures required fields are present, creates the fileset with the specified name,
        and associates it with the given scan ID. Optional metadata can be attached to the fileset.

        Returns
        -------
        dict
            Response containing success message or error description.
            If successful, also returns the created fileset ID under 'id' key, as sanitization may have happened.
        int
            HTTP status code (201, 400, 404, or 500)

        Notes
        -----
        The method expects a JSON request body with the following structure:
        {
            'fileset_id': str,    # Required: ID of the fileset
            'scan_id': str,       # Required: ID of the associated scan
            'metadata': dict      # Optional: Additional metadata for the fileset
        }

        Raises
        ------
        Exception
            Any unexpected errors during fileset creation are caught and
            returned as 500 error responses.

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> import requests
        >>> from plantdb.client.rest_api import base_url
        >>> # Create a new fileset with metadata:
        >>> metadata = {'description': 'This is a test fileset'}
        >>> url = f"{base_url()}/api/fileset"
        >>> response = requests.post(url, json={'fileset_id': 'my_fileset', 'scan_id': 'real_plant', 'metadata': metadata})
        >>> print(response.status_code)
        201
        >>> print(response.json())
        {'message': "Fileset 'my_fileset' created successfully in 'real_plant'."}
        """
        # Check authentication first
        # if not request.authorization:
        #    return {'message': 'Authentication required'}, 401

        # Get JSON data from request
        data = request.get_json()
        if not data:
            return {'message': 'No input data provided'}, 400

        # Validate required fields
        if 'fileset_id' not in data:
            return {'message': 'Name is required'}, 400
        if 'scan_id' not in data:
            return {'message': 'Scan ID is required'}, 400

        # Get metadata if provided
        metadata = data.get('metadata', {})

        try:
            # Sanitize the name
            fs_id = sanitize_name(data['fileset_id'])
            # Get the scan
            scan = self.db.get_scan(data['scan_id'], create=False)
            if not scan:
                return {'message': 'Scan not found'}, 404
            # Create the fileset
            fileset = scan.create_fileset(fs_id, )
            # Set metadata if provided
            if metadata:
                fileset.set_metadata(metadata)
            return {
                'message': f"Fileset '{fs_id}' created successfully in '{scan.id}'.",
                "id": fs_id
            }, 201

        except Exception as e:
            return {'message': f'Error creating fileset: {str(e)}'}, 500


class FilesetMetadata(Resource):
    """A REST resource for managing fileset metadata operations.

    This class provides HTTP endpoints for retrieving and updating metadata
    associated with filesets within a scan. It supports both complete metadata
    retrieval and specific key lookups, as well as partial and full metadata updates.

    Attributes
    ----------
    db : plantdb.commons.fsdb.FSDB
        A database instance for accessing scan and fileset metadata.
    logger : logging.Logger
        A logger instance for error tracking and debugging.

    Notes
    -----
    All fileset names are sanitized before processing to ensure they contain only
    alphanumeric characters, underscores, dashes, or periods.
    """

    def __init__(self, db, logger):
        self.db = db
        self.logger = logger

    def get(self, scan_id, fileset_id):
        """Retrieve metadata for a specified fileset.

        This method retrieves the metadata dictionary for a fileset. Optionally, it can
        return the value for a specific metadata key.

        Parameters
        ----------
        scan_id : str
            The ID of the scan containing the fileset.
        fileset_id : str
            The name of the fileset.
        key : str, optional
            If provided, returns only the value for this specific metadata key.

        Returns
        -------
        Union[dict, Any]
            If key is None, returns the complete metadata dictionary.
            If key is provided, returns the value for that key.

        Raises
        ------
        FilesetNotFoundError
            If the specified fileset doesn't exist.
        KeyError
            If the specified key doesn't exist in the metadata.

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> import requests
        >>> from plantdb.client.rest_api import base_url
        >>> # Create a new fileset with metadata:
        >>> metadata = {'description': 'This is a test fileset'}
        >>> url = f"{base_url()}/api/fileset"
        >>> response = requests.post(url, json={'name': 'my_fileset', 'scan_id': 'real_plant', 'metadata': metadata})
        >>> # Get all metadata:
        >>> url = f"{base_url()}/api/fileset/real_plant/my_fileset/metadata"
        >>> response = requests.get(url)
        >>> print(response.json())
        {'metadata': {'description': 'This is a test fileset'}}
        >>> # Get specific metadata key:
        >>> response = requests.get(url+"?key=description")
        >>> print(response.json())
        {'metadata': 'This is a test fileset'}
        """
        key = request.args.get('key', default=None, type=str)

        try:
            # Get the scan
            scan = self.db.get_scan(scan_id, create=False)
            if not scan:
                return {'message': 'Scan not found'}, 404
            # Get the fileset
            fileset = scan.get_fileset(sanitize_name(fileset_id))
            if not fileset:
                return {'message': 'Fileset not found'}, 404
            # Get the metadata
            metadata = fileset.get_metadata(key)
            return {'metadata': metadata}, 200

        except Exception as e:
            self.logger.error(f'Error retrieving metadata: {str(e)}')
            return {'message': f'Error retrieving metadata: {str(e)}'}, 500

    def post(self, scan_id, fileset_id):
        """Update metadata for a specified fileset.

        This method handles updating metadata for a fileset within a scan. It supports both
        full metadata replacement and partial updates of specific key-value pairs.

        Parameters
        ----------
        scan_id : str
            Unique identifier for the scan containing the fileset
        fileset_id : str
            Name of the fileset to update metadata for

        Returns
        -------
        dict
            Response dictionary with either:
                - 'metadata': Updated metadata dictionary on success
                - 'message': Error message on failure
        int
            HTTP status code (200 for success, 4xx/5xx for errors)

        Raises
        ------
        Exception
            Any unexpected errors during metadata update will be caught,
            logged, and returned as a 500 error response.

        Notes
        -----
        The request body should be a JSON object containing:
        - 'metadata' (dict): Required. The metadata to update/set
        - 'replace' (bool): Optional. If ``True``, replaces entire metadata.
                           If ``False`` (default), updates only specified keys.

        Examples
        --------
        >>> import requests
        >>> from plantdb.client.rest_api import base_url
        >>> # Create a new fileset with metadata:
        >>> metadata = {'description': 'This is a test fileset'}
        >>> url = f"{base_url()}/api/fileset"
        >>> data = {'name': 'my_fileset', 'scan_id': 'real_plant', 'metadata': metadata}
        >>> response = requests.post(url, json=data)
        >>> # Get the original metadata:
        >>> url = f"{base_url()}/api/fileset/{data['scan_id']}/{data['name']}/metadata"
        >>> response = requests.get(url)
        >>> print(response.json())
        {'metadata': {'description': 'This is a test fileset'}}
        >>> # Update metadata:
        >>> metadata_update = {"metadata": {"description": "Updated fileset description", "author": "John Doe"}, "replace": False}
        >>> response = requests.post(url, json=metadata_update)
        >>> print(response.json())
        {'metadata': {'description': 'Updated fileset description', 'author': 'John Doe'}}
        >>> # Replace metadata:
        >>> metadata_update = {"metadata": {"description": "Brand new description", "version": "2.0"}, "replace": True}
        >>> response = requests.post(url, json=metadata_update)
        >>> print(response.json())

        """
        try:
            # Get request data
            data = request.get_json()
            if not data or 'metadata' not in data:
                return {'message': 'No metadata provided in request'}, 400

            metadata = data['metadata']
            replace = data.get('replace', False)

            if not isinstance(metadata, dict):
                return {'message': 'Metadata must be a dictionary'}, 400

            # Get the scan
            scan = self.db.get_scan(scan_id, create=False)
            if not scan:
                return {'message': 'Scan not found'}, 404

            # Get the fileset
            fileset = scan.get_fileset(sanitize_name(fileset_id))
            if not fileset:
                return {'message': 'Fileset not found'}, 404

            # Update the metadata
            fileset.set_metadata(metadata)
            # TODO: make this works:
            # if replace:
            #    # Replace entire metadata dictionary
            #    fileset.set_metadata(metadata)
            # else:
            #    # Update only specified keys
            #    current_metadata = fileset.get_metadata()
            #    current_metadata.update(metadata)
            #    fileset.set_metadata(current_metadata)

            # Return updated metadata
            updated_metadata = fileset.get_metadata()
            return {'metadata': updated_metadata}, 200

        except Exception as e:
            self.logger.error(f'Error updating metadata: {str(e)}')
            return {'message': f'Error updating metadata: {str(e)}'}, 500


class FilesetFiles(Resource):
    """Resource for handling fileset files operations."""

    def __init__(self, db, logger):
        self.db = db
        self.logger = logger

    def get(self, scan_id, fileset_id):
        """List all files in a specified fileset.

        This method retrieves the list of files contained in a fileset using the
        `list_files()` method from `plantdb.commons.fsdb.Fileset`.

        Parameters
        ----------
        scan_id : str
            The ID of the scan containing the fileset.
        fileset_id : str
            The name of the fileset.

        Returns
        -------
        dict
            Response containing either:
              - On success (200): {'files': list of file information}
              - On error (404, 500): {'message': error description}
        int
            HTTP status code (200, 404, or 500)

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> import requests
        >>> from plantdb.client.rest_api import base_url
        >>> # List files in a fileset:
        >>> url = f"{base_url()}/api/fileset/real_plant/images/files"
        >>> response = requests.get(url)
        >>> print(response.status_code)
        200
        >>> print(response.json())
        {'files': ['00000_rgb', '00001_rgb', '00002_rgb', '00003_rgb', '00004_rgb', '00005_rgb', '00006_rgb', '00007_rgb', '00008_rgb', '00009_rgb', '00010_rgb', '00011_rgb', '00012_rgb', '00013_rgb', '00014_rgb', '00015_rgb', '00016_rgb', '00017_rgb', '00018_rgb', '00019_rgb', '00020_rgb', '00021_rgb', '00022_rgb', '00023_rgb', '00024_rgb', '00025_rgb', '00026_rgb', '00027_rgb', '00028_rgb', '00029_rgb', '00030_rgb', '00031_rgb', '00032_rgb', '00033_rgb', '00034_rgb', '00035_rgb', '00036_rgb', '00037_rgb', '00038_rgb', '00039_rgb', '00040_rgb', '00041_rgb', '00042_rgb', '00043_rgb', '00044_rgb', '00045_rgb', '00046_rgb', '00047_rgb', '00048_rgb', '00049_rgb', '00050_rgb', '00051_rgb', '00052_rgb', '00053_rgb', '00054_rgb', '00055_rgb', '00056_rgb', '00057_rgb', '00058_rgb', '00059_rgb']}
        """
        query = request.args.get('query', default=None, type=str)
        fuzzy = request.args.get('fuzzy', default=False, type=bool)

        try:
            # Get the scan
            scan = self.db.get_scan(scan_id, create=False)
            if not scan:
                return {'message': 'Scan not found'}, 404
            # Get the fileset
            fileset = scan.get_fileset(sanitize_name(fileset_id))
            if not fileset:
                return {'message': 'Fileset not found'}, 404
            # Get the list of files
            files = fileset.list_files(query, fuzzy)
            return {'files': files}, 200

        except Exception as e:
            self.logger.error(f'Error listing files: {str(e)}')
            return {'message': f'Error listing files: {str(e)}'}, 500


# Define valid files extensions
VALID_FILE_EXT = ['.jpg', '.jpeg', '.png', '.tif', '.txt', '.json', '.ply', '.yaml', '.toml']


class FileCreate(Resource):
    """Represents a File resource creation endpoint in the application.

    This class provides the functionality to create new files in filesets.

    Attributes
    ----------
    db : plantdb.commons.fsdb.FSDB
        The database instance used to create files.
    logger : logging.Logger
        The logger instance for recording operations.
    """

    def __init__(self, db, logger):
        self.db = db
        self.logger = logger

    def post(self):
        """Create a new file in a fileset and write data to it.

        This method handles POST requests to create a new file with data. It expects
        multipart/form-data with the file data and JSON metadata.

        Notes
        -----
        The method expects the following form fields:
        - file: The actual file data
        - file_id: str       # Required: ID of the file
        - ext: str           # Required: File extension (must be one of VALID_FILE_EXT)
        - scan_id: str       # Required: ID of the scan
        - fileset_id: str    # Required: ID of the fileset
        - metadata: dict     # Optional: Additional metadata for the file (as JSON string)

        Returns
        -------
        dict
            Response containing success message or error description.
            If successful, also returns the created file ID under 'id' key, as sanitization may have happened.
        int
            HTTP status code (201, 400, 404, or 500)

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> import requests
        >>> import json
        >>> from tempfile import NamedTemporaryFile
        >>> from plantdb.client.rest_api import base_url
        >>> # Create a YAML temporary file:
        >>> with NamedTemporaryFile(suffix='.yaml', mode="w", delete=False) as f: f.write('name: my_file')
        >>> file_path = f.name
        >>> # Create a new file with metadata in the database:
        >>> url = f"{base_url()}/api/file"
        >>> # Open the file separately for sending
        >>> with open(file_path, 'rb') as file_handle:
        ...     files = {
        ...         'file': ('test_file.yaml', file_handle, 'application/octet-stream')
        ...     }
        ...     metadata = json.dumps({'description': 'Test document', 'author': 'John Doe'})
        ...     data = {
        ...         'file_id': 'new_file',
        ...         'ext': 'yaml',
        ...         'scan_id': 'real_plant',
        ...         'fileset_id': 'images',
        ...         'metadata': metadata
        ...     }
        ...     response = requests.post(url, files=files, data=data)
        >>> print(response.status_code)
        201
        >>> print(response.json())
        {'message': "File 'test_file.yaml' created and written successfully in fileset 'images'."}
        >>> file_path.unlink()  # Delete the YAML test file
        """
        # Check if the request has the file part
        if 'file' not in request.files:
            return {'message': 'No file provided'}, 400

        file_data = request.files['file']

        # Get form data
        file_id = request.form.get('file_id', None)
        ext = request.form.get('ext', None)
        scan_id = request.form.get('scan_id', None)
        fileset_id = request.form.get('fileset_id', None)

        # Validate required fields
        if not all([file_id, ext, scan_id, fileset_id]):
            missing_fields = [form_field.__name__ for form_field in [file_id, ext, scan_id, fileset_id] if
                              form_field is None]
            return {
                'message': f"'file_id', 'ext', 'scan_id', and 'fileset_id' fields are required, missing: {missing_fields}"}, 400

        # Validate file extension
        if not ext.startswith('.'):
            ext = f'.{ext}'
        if ext not in VALID_FILE_EXT:
            return {
                'message': f'Invalid file extension. Must be one of: {", ".join(VALID_FILE_EXT)}'
            }, 400

        # Parse metadata if provided
        try:
            metadata = json.loads(request.form.get('metadata', '{}'))
        except json.JSONDecodeError:
            return {'message': 'Invalid metadata JSON format'}, 400

        try:
            # Get the scan
            scan = self.db.get_scan(scan_id, create=False)
            if not scan:
                return {'message': 'Scan not found'}, 404
            # Get the fileset
            fileset = scan.get_fileset(sanitize_name(fileset_id))
            if not fileset:
                return {'message': 'Fileset not found'}, 404
            # Create the file
            file_id = sanitize_name(file_id)
            file = fileset.create_file(file_id)
            try:
                # Write the file data with the specified extension
                if ext in ['.jpg', '.jpeg', '.png', '.tif']:
                    file.write_raw(file_data.read(), ext=ext[1:])  # Binary mode
                else:
                    file.write(file_data.read().decode(), ext=ext[1:])  # Text mode
            except Exception as e:
                fileset.delete_file(file_id)
                self.logger.error(f'Error writing file: {str(e)}')
                return {'message': f'Error writing file: {str(e)}'}, 500
            # Set metadata if provided
            if metadata:
                file.set_metadata(metadata)
            return {
                'message': f"File '{file_id}{ext}' created and written successfully in fileset '{fileset.id}'.",
                'id': f"{file_id}",
            }, 201

        except Exception as e:
            self.logger.error(f"Error creating file: {str(e)}")
            return {'message': f'Error creating file: {str(e)}'}, 500


class FileMetadata(Resource):
    """REST API resource for managing file metadata operations.

    This class provides endpoints for retrieving and updating metadata associated with
    files within a scan's fileset.

    Attributes
    ----------
    db : plantdb.commons.fsdb.FSDB
        Database instance for accessing file storage.
    logger : Logger
        The logger instance for this resource.

    Notes
    -----
    All file and fileset names are sanitized before processing to ensure valid formats.
    """

    def __init__(self, db, logger):
        self.db = db
        self.logger = logger

    def get(self, scan_id, fileset_id, file_id):
        """Retrieve metadata for a specified file.

        Parameters
        ----------
        scan_id : str
            The ID of the scan containing the fileset.
        fileset_id : str
            The name of the fileset containing the file.
        file_id : str
            The name of the file.
        key : str, optional
            If provided, returns only the value for this specific metadata key.

        Returns
        -------
        Union[dict, Any]
            If key is None, returns the complete metadata dictionary.
            If key is provided, returns the value for that key.

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> import requests
        >>> from plantdb.client.rest_api import base_url
        >>> # Get all metadata:
        >>> url = f"{base_url()}/api/file/test_plant/images/image_001/metadata"
        >>> response = requests.get(url)
        >>> print(response.json())
        {'metadata': {'description': 'Test file'}}
        >>> # Get specific metadata key:
        >>> response = requests.get(url+"?key=description")
        >>> print(response.json())
        {'metadata': 'Test file'}
        """
        key = request.args.get('key', default=None, type=str)

        try:
            # Get the scan
            scan = self.db.get_scan(scan_id, create=False)
            if not scan:
                return {'message': 'Scan not found'}, 404
            # Get the fileset
            fileset = scan.get_fileset(sanitize_name(fileset_id))
            if not fileset:
                return {'message': 'Fileset not found'}, 404
            # Get the file
            file = fileset.get_file(sanitize_name(file_id))
            if not file:
                return {'message': 'File not found'}, 404
            # Get the metadata
            metadata = file.get_metadata(key)
            return {'metadata': metadata}, 200

        except Exception as e:
            self.logger.error(f'Error retrieving metadata: {str(e)}')
            return {'message': f'Error retrieving metadata: {str(e)}'}, 500

    def post(self, scan_id, fileset_id, file_id):
        """Update metadata for a specified file.

        Parameters
        ----------
        scan_id : str
            The ID of the scan containing the fileset.
        fileset_id : str
            The name of the fileset containing the file.
        file_id : str
            The name of the file.

        Returns
        -------
        dict
            Response dictionary with either:
                * {'metadata': dict} containing updated metadata for successful requests
                * {'message': str} for error cases
        int
            HTTP status code (200, 400, 404, or 500).

        Notes
        -----
        The request body should be a JSON object containing:
        - 'metadata' (dict): Required. The metadata to update/set
        - 'replace' (bool): Optional. If ``True``, replaces entire metadata.
                           If ``False`` (default), updates only specified keys.

        Examples
        --------
        >>> # Start a test REST API server first:
        >>> # $ fsdb_rest_api --test
        >>> import requests
        >>> from plantdb.client.rest_api import base_url
        >>> url = f"{base_url()}/api/file/test_plant/images/image_001/metadata"
        >>> data = {"metadata": {"description": "Updated description"}}
        >>> response = requests.post(url, json=data)
        >>> print(response.json())
        {'metadata': {'description': 'Updated description'}}
        """
        try:
            # Get request data
            data = request.get_json()
            if not data or 'metadata' not in data:
                return {'message': 'Missing metadata in request body'}, 400

            metadata = data['metadata']
            replace = data.get('replace', False)

            if not isinstance(metadata, dict):
                return {'message': 'Metadata must be a dictionary'}, 400

            # Get the scan
            scan = self.db.get_scan(scan_id, create=False)
            if not scan:
                return {'message': 'Scan not found'}, 404

            # Get the fileset
            fileset = scan.get_fileset(sanitize_name(fileset_id))
            if not fileset:
                return {'message': 'Fileset not found'}, 404

            # Get the file
            file = fileset.get_file(sanitize_name(file_id))
            if not file:
                return {'message': 'File not found'}, 404

            # Update the metadata
            file.set_metadata(metadata)
            # TODO: make this works:
            # if replace:
            #    # Replace entire metadata dictionary
            #    file.set_metadata(metadata)
            # else:
            #    # Update only specified keys
            #    current_metadata = file.get_metadata()
            #    current_metadata.update(metadata)
            #    file.set_metadata(current_metadata)

            # Return updated metadata
            updated_metadata = file.get_metadata()
            return {'metadata': updated_metadata}, 200

        except Exception as e:
            self.logger.error(f'Error processing request: {str(e)}')
            return {'message': f'Error processing request: {str(e)}'}, 500
