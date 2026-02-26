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

import datetime
from urllib import parse


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
    >>> from plantdb.commons.test_database import test_database
    >>> from plantdb.server.core.utils import _get_colmap_camera_model
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
    scan : plantdb.commons.fsdb.core.Scan or str
        A ``Scan`` instance or the name of the scan dataset.
    fileset : plantdb.commons.fsdb.core.Fileset or str
        A ``Fileset`` instance or the name of the fileset.
    file : plantdb.commons.fsdb.core.File or str
        A ``File`` instance or the name of the file.

    Returns
    -------
    str
        The URI for the corresponding `scan/fileset/file` tree.

    Examples
    --------
    >>> from plantdb.server.core.utils import compute_fileset_matches
    >>> from plantdb.server.core.utils import get_file_uri
    >>> from plantdb.commons.test_database import test_database
    >>> db = test_database('real_plant_analyzed')
    >>> db.connect()
    >>> scan = db.get_scan('real_plant_analyzed')
    >>> fs_match = compute_fileset_matches(scan)
    >>> fs = scan.get_fileset(fs_match['PointCloud'])
    >>> f = fs.get_file("PointCloud")
    >>> get_file_uri(scan, fs, f)
    '/files/real_plant_analyzed/PointCloud_1_0_1_0_10_0_7ee836e5a9/PointCloud.ply'
    """
    from plantdb.commons.fsdb.core import Scan
    from plantdb.commons.fsdb.core import Fileset
    from plantdb.commons.fsdb.core import File
    scan_id = scan.id if isinstance(scan, Scan) else scan
    fileset_id = fileset.id if isinstance(fileset, Fileset) else fileset
    file_id = file.path().name if isinstance(file, File) else file
    return f"/files/{scan_id}/{fileset_id}/{file_id}"


def get_image_uri(scan, fileset, file, size="orig", as_base64=False):
    """Return the URI for the corresponding `scan/fileset/file` tree.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.core.Scan or str
        A ``Scan`` instance or the name of the scan dataset.
    fileset : plantdb.commons.fsdb.core.Fileset or str
        A ``Fileset`` instance or the name of the fileset.
    file : plantdb.commons.fsdb.core.File or str
        A ``File`` instance or the name of the file.
    size : {'orig', 'large', 'thumb'} or int, optional
        If an integer, use it as the size of the cached image to create and return.
        Otherwise, should be one of the following strings, default to `'orig'`:

          - `'thumb'`: image max width and height to `150`.
          - `'large'`: image max width and height to `1500`;
          - `'orig'`: original image, no chache;
    as_base64 : bool, optional
        A boolean flag indicating whether to return an image as a base64 string.

    Returns
    -------
    str
        The URI for the corresponding `scan/fileset/file` tree.

    Examples
    --------
    >>> from plantdb.server.core.utils import get_image_uri
    >>> from plantdb.commons.test_database import test_database
    >>> from plantdb.server.core.utils import compute_fileset_matches
    >>> db = test_database('real_plant_analyzed')
    >>> db.connect()
    >>> scan = db.get_scan('real_plant_analyzed')
    >>> get_image_uri(scan, 'images', '00000_rgb.jpg', size='orig')
    '/image/real_plant_analyzed/images/00000_rgb.jpg?size=orig'
    >>> get_image_uri(scan, 'images', '00011_rgb.jpg', size='thumb', as_base64=True)
    '/image/real_plant_analyzed/images/00011_rgb.jpg?size=thumb&as_base64=true'
    """
    from plantdb.commons.fsdb.core import Scan
    from plantdb.commons.fsdb.core import Fileset
    from plantdb.commons.fsdb.core import File
    scan_id = scan.id if isinstance(scan, Scan) else scan
    fileset_id = fileset.id if isinstance(fileset, Fileset) else fileset
    file_id = file.path().name if isinstance(file, File) else file

    # Assemble optional query parameters
    query: dict[str, str] = {}
    if size is not None:
        query["size"] = str(size)
    if as_base64:
        # Use lower‑case JSON‑style booleans for consistency
        query["as_base64"] = str(as_base64).lower()

    query_str = f"?{parse.urlencode(query)}" if query else ""
    return f"/image/{scan_id}/{fileset_id}/{file_id}{query_str}"


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


