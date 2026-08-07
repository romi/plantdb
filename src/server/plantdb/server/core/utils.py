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
# PlantDB Server Core Utilities

Utility functions for managing scan data in the PlantDB server.
They provide convenient ways to retrieve acquisition dates, map file sets, generate scan templates, and extract
COLMAP camera information, simplifying data handling and API development.

## Usage Examples

```python
>>> from plantdb.server.core.utils import get_scan_date
>>> from plantdb.server.core.utils import compute_fileset_matches
>>> from plantdb.server.core.utils import get_scan_template
>>> from plantdb.server.core.utils import _get_colmap_camera_model
>>> from plantdb.commons.test_database import test_database
>>> # Initialize the database (creates base directory if needed)
>>> db = test_database(no_auth=True)
>>> db.connect()
>>> scan = db.get_scan('real_plant_analyzed')
>>> # Assuming `db` is a connected PlantDB instance and `scan` is a Scan object
>>> print(get_scan_date(scan))
2026-07-02 13:34:09
>>> print(compute_fileset_matches(scan))
{'images': 'images', 'AnglesAndInternodes': 'AnglesAndInternodes_1_0_2_0_6_0_6dd64fc595', 'TreeGraph': 'TreeGraph__False_CurveSkeleton_c304a2cc71', 'CurveSkeleton': 'CurveSkeleton__TriangleMesh_0393cb5708', 'TriangleMesh': 'TriangleMesh_9_most_connected_t_open3d_00e095c359', 'PointCloud': 'PointCloud_1_0_1_0_10_0_7ee836e5a9', 'Voxels': 'Voxels___x____300__450__colmap_camera_False_2a093f0ccc', 'Masks': 'Masks_1__0__1__0____channel____rgb_5619aa428d', 'Colmap': 'Colmap_True_null_SIMPLE_RADIAL_ffcef49fdc', 'Undistorted': 'Undistorted_SIMPLE_RADIAL_Colmap__a333f181b7'}
>>> template = get_scan_template(scan.id)
>>> print(template['id'])
real_plant_analyzed
>>> camera_model, poses = _get_colmap_camera_model(scan)
>>> print(camera_model)
{'height': 1080, 'id': 1, 'model': 'OPENCV', 'params': [1166.9518889440105, 1166.9518889440105, 720.0, 540.0, -0.0013571157486977348, -0.0013571157486977348, 0.0, 0.0], 'width': 1440}
>>> print(poses[0])
{'id': '00000_rgb', 'tvec': [369.4279687732083, 120.36109311437637, -62.07043190848918], 'rotmat': [[0.06475585405884698, -0.9971710205080586, 0.038165890845442085], [-0.3390191175518756, -0.0579549181538338, -0.9389926865509284], [0.9385481965778085, 0.04786630673761355, -0.34181295964290737]], 'photoUri': '/api/v1/assets/image/real_plant_analyzed/images/00000_rgb?size=orig', 'thumbnailUri': '/api/v1/assets/image/real_plant_analyzed/images/00000_rgb?size=thumb'}
```
"""

import datetime

from plantdb.commons import api_endpoints


def get_scan_date(scan):
    """Get the acquisition datetime of a scan.

    Try to get the data from the scan metadata 'acquisition_date', else from the directory creation time.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.core.Scan
        The scan instance to get the date & time from.

    Returns
    -------
    str
        The formatted datetime string.

    Examples
    --------
    >>> from plantdb.server.core.utils import get_scan_date
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
    scan : plantdb.commons.fsdb.core.Scan
        The scan instance to list the filesets from.

    Returns
    -------
    dict
        A dictionary mapping the scan tasks to fileset names.

    Examples
    --------
    >>> from plantdb.server.core.utils import compute_fileset_matches
    >>> from plantdb.commons.test_database import dummy_db
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


def _get_colmap_camera_model(scan, **kwargs):
    """Retrieve the COLMAP camera model and camera poses from a scan object.

    This function extracts the COLMAP camera model from the metadata of the first image file
    in the specified scan's image fileset. It then iterates over all RGB images in the same
    fileset to collect their camera poses, including translation vectors (`tvec`), rotation
    matrices (`rotmat`), and URIs for both original and thumbnail images.

    Parameters
    ----------
    scan : Scan object
        The scan object containing the image fileset with COLMAP metadata.

    Other Parameters
    ----------------
    prefix : str, optional
        Deployment (reverse-proxy) prefix prepended before ``/api/v1/...``
        in the generated image URIs.

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
    api_endpoints.image : Function used to generate URIs for images based on scan, fileset, and file objects.

    Examples
    --------
    >>> from plantdb.commons.test_database import test_database
    >>> from plantdb.server.core.utils import _get_colmap_camera_model
    >>> db = test_database('real_plant_analyzed', no_auth=True)
    >>> db.connect()
    >>> scan = db.get_scan('real_plant_analyzed')
    >>> camera_model, poses = _get_colmap_camera_model(scan)
    >>> print(camera_model)
    {'height': 1080, 'id': 1, 'model': 'OPENCV', 'params': [1166.9518889440105, 1166.9518889440105, 720.0, 540.0, -0.0013571157486977348, -0.0013571157486977348, 0.0, 0.0], 'width': 1440}
    >>> print(poses[0])
    {'id': '00000_rgb', 'tvec': [369.4279687732083, 120.36109311437637, -62.07043190848918], 'rotmat': [[0.06475585405884698, -0.9971710205080586, 0.038165890845442085], [-0.3390191175518756, -0.0579549181538338, -0.9389926865509284], [0.9385481965778085, 0.04786630673761355, -0.34181295964290737]], 'photoUri': '/api/v1/assets/image/real_plant_analyzed/images/00000_rgb?size=orig', 'thumbnailUri': '/api/v1/assets/image/real_plant_analyzed/images/00000_rgb?size=thumb'}

    """
    _prefix = kwargs.get("prefix", "")
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
            "photoUri": api_endpoints.image(scan.id, img_fs.id, img_f.id, size="orig", prefix=_prefix),
            "thumbnailUri": api_endpoints.image(scan.id, img_fs.id, img_f.id, size="thumb", prefix=_prefix)
        })
    return model, poses
