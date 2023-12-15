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
plantdb.rest_api
================

This module regroup the methods used to serve a REST API using ``plantdb_rest_api`` CLI.
"""

import datetime
import json
import os

from plantdb.io import read_json


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
    >>> db = test_database('real_plant_analyzed')
    >>> db.connect()
    >>> scan = db.get_scan('real_plant_analyzed')
    >>> print(get_scan_date(scan))
    2023-12-15 16:37:15
    >>> db.disconnect()
    """
    dt = scan.get_metadata('acquisition_date')
    try:
        assert dt is not None
    except:
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
    >>> db.connect()
    >>> scan = db.get_scan("myscan_001")
    >>> compute_fileset_matches(scan)
    {'fileset': 'fileset_001'}
    >>> db.disconnect()
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
            "species": 'N/A',
            "plant": "N/A",
            "environment": "N/A",
            "nbPhotos": 0,
            "files": {
                "metadatas": None,
                "archive": None
            }
        },
        "thumbnailUri": "",
        "hasMesh": False,
        "hasPointCloud": False,
        "hasPcdGroundTruth": False,
        "hasSkeleton": False,
        "hasAngleData": False,
        "hasSegmentation2D": False,
        "hasSegmentedPcdEvaluation": False,
        "hasPointCloudEvaluation": False,
        "hasManualMeasures": False,
        "hasAutomatedMeasures": False,
        "hasSegmentedPointCloud": False,
        "error": error
    }


def list_scans_info(scans, query=None):
    """List scans information.

    Parameters
    ----------
    scans : list of plantdb.fsdb.Scan
        The list of scan instances to get information from.
    query : str, optional
        A scan filtering query, to be matched in the scan metadata keys.

    Returns
    -------
    list of dict
        The list of scans information dictionaries.

    Examples
    --------
    >>> from plantdb.rest_api import list_scans_info
    >>> from plantdb.test_database import test_database
    >>> db = test_database('real_plant_analyzed')
    >>> db.connect()
    >>> scans_info = list_scans_info(db.get_scans())
    >>> print(scans_info)
    [{'id': 'real_plant_analyzed', 'metadata': {'date': '2023-12-15 16:37:15', 'species': 'N/A', 'plant': 'N/A', 'environment': 'Lyon indoor', 'nbPhotos': 60, 'files': {'metadatas': None, 'archive': None}}, 'thumbnailUri': '', 'hasMesh': True, 'hasPointCloud': True, 'hasPcdGroundTruth': False, 'hasSkeleton': True, 'hasAngleData': True, 'hasSegmentation2D': False, 'hasSegmentedPcdEvaluation': False, 'hasPointCloudEvaluation': False, 'hasManualMeasures': False, 'hasAutomatedMeasures': True, 'hasSegmentedPointCloud': False, 'error': False, 'hasTreeGraph': True}]
    >>> db.disconnect()
    """
    res = []
    for scan in scans:
        metadata = scan.get_metadata()
        if query is not None and not (query.lower() in json.dumps(metadata).lower()):
            continue  # filter scans info list by matching the query with metadata keys
        try:
            scan_info = get_scan_info(scan)
        except:
            # logger.error(f"Could not obtain information from scan dataset '{scan.id}'...")
            scan_info = get_scan_template(scan.id, error=True)
        res.append(scan_info)
    return res


def get_scan_info(scan):
    """Get the information related to a single scan dataset.

    Parameters
    ----------
    scans : plantdb.fsdb.Scan
        The scan instances to get information from.

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
    {'id': 'real_plant_analyzed', 'metadata': {'date': '2023-12-15 16:37:15', 'species': 'N/A', 'plant': 'N/A', 'environment': 'Lyon indoor', 'nbPhotos': 60, 'files': {'metadatas': None, 'archive': None}}, 'thumbnailUri': '', 'hasMesh': True, 'hasPointCloud': True, 'hasPcdGroundTruth': False, 'hasSkeleton': True, 'hasAngleData': True, 'hasSegmentation2D': False, 'hasSegmentedPcdEvaluation': False, 'hasPointCloudEvaluation': False, 'hasManualMeasures': False, 'hasAutomatedMeasures': True, 'hasSegmentedPointCloud': False, 'error': False, 'hasTreeGraph': True}
    >>> db.disconnect()

    """
    # Map the scan tasks to fileset names:
    task_fs_map = compute_fileset_matches(scan)
    # Get the scan metadata dictionary:
    scan_md = scan.get_metadata()
    # Initialize the scan information template:
    scan_info = get_scan_template(scan.id)

    # - Gather "metadata" information from scan:
    # Get acquisition date:
    scan_info["metadata"]['date'] = get_scan_date(scan)
    # Import 'object' related scan metadata to scan info template:
    if 'object' in scan_md:
        scan_obj = scan_md['object']  # get the 'object' related dictionary
        scan_info["metadata"]['species'] = scan_obj.get('species', 'N/A')
        scan_info["metadata"]['environment'] = scan_obj.get('environment', 'N/A')
        scan_info["metadata"]['plant'] = scan_obj.get('plant_id', 'N/A')
    # Get the number of 'images' in the dataset:
    scan_info["metadata"]['nbPhotos'] = len(scan.get_fileset('images').get_files())

    def _try_has_file(task, file):
        if task not in task_fs_map:
            return False
        elif scan.get_fileset(task_fs_map[task]) is None:
            return False
        else:
            return scan.get_fileset(task_fs_map[task]).get_file(file) is not None

    # - Gather information about tasks:
    scan_info['hasPointCloud'] = _try_has_file('PointCloud', 'PointCloud')
    scan_info['hasMesh'] = _try_has_file('TriangleMesh', 'TriangleMesh')
    scan_info['hasSkeleton'] = _try_has_file('CurveSkeleton', 'CurveSkeleton')
    scan_info['hasTreeGraph'] = _try_has_file('TreeGraph', 'TreeGraph')
    scan_info['hasAngleData'] = _try_has_file('AnglesAndInternodes', 'AnglesAndInternodes')
    scan_info['hasAutomatedMeasures'] = _try_has_file('AnglesAndInternodes', 'AnglesAndInternodes')
    scan_info['hasManualMeasures'] = 'measures.json' in scan.path().iterdir()
    scan_info['hasSegmentation2D'] = _try_has_file('Segmentation2D', '')
    scan_info['hasPcdGroundTruth'] = _try_has_file('PointCloudGroundTruth', 'PointCloudGroundTruth')
    scan_info['hasPointCloudEvaluation'] = _try_has_file('PointCloudEvaluation', 'PointCloudEvaluation')
    scan_info['hasSegmentedPointCloud'] = _try_has_file('SegmentedPointCloud', 'SegmentedPointCloud')
    scan_info['hasSegmentedPcdEvaluation'] = _try_has_file('SegmentedPointCloudEvaluation',
                                                           'SegmentedPointCloudEvaluation')

    return scan_info


def get_scan_data(scan):
    """Get the scan information and data.

    Parameters
    ----------
    scan : plantdb.fsdb.Scan
        The scan instance to get the information and data from.

    Returns
    -------
    dict
        The scan information dictionary.

    Examples
    --------
    >>> from os import environ
    >>> from plantdb.rest_api import get_scan_data
    >>> from plantdb.fsdb import FSDB
    >>> db = FSDB(environ.get('ROMI_DB', "/data/ROMI/DB/"))
    >>> db.connect(unsafe=True)
    >>> scan = db.get_scan('sango_90_300_36')
    >>> scan_data = get_scan_data(scan)
    >>> print(scan_data)
    >>> db.disconnect()
    """
    task_fs_map = compute_fileset_matches(scan)
    scan_data = get_scan_info(scan)

    # - Get the paths to data files:
    scan_data["filesUri"] = {}
    # Get the URI to the PointCloud related file:
    if scan_data["hasPointCloud"]:
        scan_data["filesUri"]["pointCloud"] = scan.get_fileset(task_fs_map['PointCloud']).get_file('PointCloud').path()
    # Get the URI to the TriangleMesh related file:
    if scan_data["hasMesh"]:
        scan_data["filesUri"]["mesh"] = scan.get_fileset(task_fs_map['TriangleMesh']).get_file('TriangleMesh').path()
    # Get the URI to the TreeGraph related file:
    if scan_data["hasSkeleton"]:
        scan_data["filesUri"]["skeleton"] = scan.get_fileset(task_fs_map['Skeleton']).get_file('Skeleton').path()
    # Get the URI to the TreeGraph related file:
    if scan_data["hasTreeGraph"]:
        scan_data["filesUri"]["tree"] = scan.get_fileset(task_fs_map['TreeGraph']).get_file('TreeGraph').path()

    # - Load some of the data:
    scan_data['data'] = {}
    # Load the skeleton data:
    if scan_data["hasSkeleton"]:
        scan_data['data']["skeleton"] = read_json(scan.get_fileset(task_fs_map['Skeleton']).get_file('Skeleton'))
    # Load the measured angles and internodes:
    if scan_data["hasAngleData"]:
        measures = read_json(scan.get_fileset(task_fs_map['AnglesAndInternodes']).get_file('AnglesAndInternodes'))
        # scan_data['data']["angles"] = measures.get("angles", {})
        # scan_data['data']["internodes"] = measures.get("internodes", {})
        scan_data['data']["angles"] = measures
    # Load the manually measured angles and internodes:
    if scan_data["hasManualMeasures"]:
        measures = scan.get_measures()
        if measures is None:
            measures = dict([])
        scan_data['data']["angles"]["measured_angles"] = measures.get('angles', [])
        scan_data['data']["angles"]["measured_internodes"] = measures.get("internodes", [])
    # Load the workspace, aka bounding-box:
    try:
        # old version: get scanner workspace
        scan_data["workspace"] = scan.get_metadata("scanner")["workspace"]
    except KeyError:
        # new version: get it from colmap fileset metadata 'bounding-box'
        scan_data["workspace"] = scan.get_fileset(task_fs_map['Colmap']).get_metadata("bounding_box")

    # - Load camera information
    scan_data["camera"] = {}
    # Load the camera model:
    try:
        # old version
        scan_data["camera"]["model"] = scan.get_metadata("computed")["camera_model"]
    except KeyError:
        # new version: get it from colmap fileset metadata 'task_params'/'camera_model':
        scan_data["camera"]["model"] = scan.get_fileset(task_fs_map['Colmap']).get_metadata("task_params")[
            'camera_model']
    # Load the camera poses from the images metadata:
    img_fs = scan.get_fileset(task_fs_map['images'])  # get the 'images' fileset
    for img_idx, img_f in enumerate(img_fs.get_files()):
        camera_md = img_f.get_metadata("colmap_camera")
        scan_data['camera']['poses'].append({
            'id': img_idx+1,
            'tvec': camera_md['tvec'],
            'rotmat': camera_md['rotmat'],
            'photoUri': img_f.path(),
            'isMatched': True
        })
