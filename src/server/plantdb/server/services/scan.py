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

import json
import os
from math import radians
from plantdb.commons.fsdb.core import Scan
from typing import Any, Dict, List, Optional, Literal, Protocol, Tuple

from plantdb.commons.fsdb.exceptions import FileNotFoundError
from plantdb.commons.io import read_json
from plantdb.commons.log import get_logger
from plantdb.commons.utils import is_radians
from plantdb.server import webcache
from plantdb.server.core.utils import _get_colmap_camera_model
from plantdb.server.core.utils import compute_fileset_matches
from plantdb.server.core.utils import get_file_uri
from plantdb.server.core.utils import get_scan_date
from plantdb.server.core.utils import get_scan_template
from plantdb.server.api.base import task_filesUri_mapping


def get_scan_info(scan, **kwargs):
    """Get the information related to a single scan dataset.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.core.Scan
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
    >>> from plantdb.server.services.scan import get_scan_info
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


def get_scan_data(scan, **kwargs):
    """Get the scan information and data.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.core.Scan
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
    >>> from plantdb.server.services.scan import get_scan_data
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
