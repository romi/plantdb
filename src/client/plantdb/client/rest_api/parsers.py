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
# REST API Parser Module

Module containing functions for parsing responses from the PlantDB REST API. This module handles the conversion of raw API responses into Python objects, including images, point clouds, meshes, skeletons, and configuration data.

## Key Features

- **Image Parsing**: Retrieve and parse images from scan datasets with various size options
- **Data Parsing**: Parse different data types from API responses including PointCloud, TriangleMesh, CurveSkeleton, TreeGraph, and AnglesAndInternodes
- **Configuration Access**: Load TOML configuration files from datasets
- **Scan Information**: Parse scan metadata and task information
- **Task Data Retrieval**: Fetch and parse data from specific tasks within datasets

## Usage Examples

Hereafter is a minimal working example that:

1. Starts a test PlantDB REST API server
2. Parses scan information
3. Retrieves image data from a scan

```python
>>> # Start a test PlantDB REST API server first, in a terminal:
>>> # $ fsdb_rest_api --test
>>> from plantdb.client.rest_api.parsers import parse_scans_info, parse_task_images
>>> # Get scan information
>>> scan_dict = parse_scans_info('localhost', port=5000)
>>> print(sorted(scan_dict.keys()))
['arabidopsis000', 'real_plant', 'real_plant_analyzed', 'virtual_plant', 'virtual_plant_analyzed']
>>> # Get images from a scan
>>> images = parse_task_images('localhost', 'real_plant', port=5000)
>>> print(len(images))
60
>>> img1 = images[0]
>>> print(img1.size)
(1440, 1080)
```
"""

import json
from io import BytesIO
from pathlib import Path

import numpy
from PIL import Image
from ada_url import join_url
from plyfile import PlyData

from plantdb.client.rest_api.requests import make_api_request
from plantdb.client.rest_api.requests import request_scan_data
from plantdb.client.rest_api.requests import request_scans_info
from plantdb.client.rest_api.urls import list_task_images_uri
from plantdb.client.rest_api.urls import origin_url
from plantdb.client.rest_api.urls import scan_file_url
from plantdb.commons import api_endpoints
from plantdb.commons.log import get_logger

logger = get_logger(__name__)


def parse_scans_info(host, **kwargs):
    """Parse the information dictionary for all scans served by the PlantDB REST API.

    Parameters
    ----------
    host : str
        The hostname or IP address of the PlantDB REST API server.

    Other Parameters
    ----------------
    port : int
        The PlantDB API port number, defaults to ``None``.
    prefix : str
        A path prefix for the PlantDB API, defaults to ``None``.
    ssl : bool
        A boolean flag indicating whether to use HTTPS (``True``) or HTTP (``False``). Defaults to ``False``.

    Returns
    -------
    dict
        The scan-id (dataset name) indexed information dictionary.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api.parsers import parse_scans_info
    >>> scan_dict = parse_scans_info('localhost', port=5000)
    >>> print(sorted(scan_dict.keys()))
    ['arabidopsis000', 'real_plant', 'real_plant_analyzed', 'virtual_plant', 'virtual_plant_analyzed']
    """
    scan_json = request_scans_info(host, **kwargs)
    scan_dict = {}
    for scan in scan_json:
        name = scan.pop('id')
        scan_dict[name] = scan
    return scan_dict


def parse_task_images(host, scan_id, task_name='images', size='orig', as_base64=False, **kwargs):
    """Get the list of images data for a given dataset and task name.

    Parameters
    ----------
    host : str
        The hostname or IP address of the PlantDB REST API server.
    scan_id : str
        The name of the dataset to retrieve the images for.
    task_name : str, optional
        The name of the task to retrieve the images from. Defaults to 'images'.
    size : {'orig', 'large', 'thumb'} or int, optional
        If an integer, use  it as the size of the cached image to create and return.
        Else, should be a string, defaulting to `'orig'`, and it works as follows:
           * `'thumb'`: image max width and height to `150`.
           * `'large'`: image max width and height to `1500`;
           * `'orig'`: original image, no chache;
    as_base64 : bool
        A boolean flag indicating whether to return an image as a base64 string.

    Other Parameters
    ----------------
    port : int
        The PlantDB API port number, defaults to ``None``.
    prefix : str
        A path prefix for the PlantDB API, defaults to ``None``.
    ssl : bool
        A boolean flag indicating whether to use HTTPS (``True``) or HTTP (``False``). Defaults to ``False``.

    Returns
    -------
    list of PIL.Image
        The list of PIL.Image from the PlantDB REST API.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api.parsers import parse_task_images
    >>> images = parse_task_images('localhost', 'real_plant', port=5000)
    >>> print(len(images))
    60
    >>> img1 = images[0]
    >>> print(img1.size)
    (1440, 1080)
    """
    images = []
    for img_uri in list_task_images_uri(host, scan_id, task_name, size, as_base64, **kwargs):
        images.append(
            Image.open(BytesIO(make_api_request(url=img_uri, session_token=kwargs.get('session_token', None)).content)))
    return images


def _ply_vertex_to_array(data):
    """Convert the `PlyData` 'vertex' data into an XYZ array of vertex coordinates.

    Parameters
    ----------
    data : PlyData
        The `PlyData` object to be converted as a numpy array.

    Returns
    -------
    list
        The XYZ array of vertex coordinates, returned as a list to be JSON serializable.
    """
    return [list(data['vertex']['x']), list(data['vertex']['y']), list(data['vertex']['z'])]


def _ply_face_to_array(data):
    """Convert the `PlyData` 'face' data into an XYZ array of triangle coordinates.

    Parameters
    ----------
    data : PlyData
        The `PlyData` object to be converted as a numpy array.

    Returns
    -------
    list
        The XYZ array of triangle coordinates, returned as a list to be JSON serializable.
    """
    return [list(d) for d in data['face'].data['vertex_indices']]


def parse_requests_pcd(data):
    """Parse a requests content, should be from a PointCloud task source.

    Parameters
    ----------
    data : buffer
        The data source from a requests content.

    Returns
    -------
    numpy.ndarray
        The parsed pointcloud with vertex coordinates sorted as XYZ.
    """
    ## Read the pointcloud PLY as a `PlyData`:
    ply_pcd = PlyData.read(BytesIO(data))
    ## Convert the `PlyData`:
    return _ply_vertex_to_array(ply_pcd)


def parse_requests_mesh(data):
    """Parse a requests content, should be from a TriangleMesh task source.

    Parameters
    ----------
    data : buffer
        The data source from a requests content.

    Returns
    -------
    dict
        The parsed triangular mesh with two entries: 'vertices' for vertex coordinates and 'triangles' for triangle coordinates.
    """
    ## Read the PLY as a `PlyData`:
    mesh_data = PlyData.read(BytesIO(data))
    ## Convert the `PlyData`:
    return {"vertices": _ply_vertex_to_array(mesh_data),
            "triangles": _ply_face_to_array(mesh_data)}


def parse_requests_skeleton(data):
    """Parse a requests content, should be from a CurveSkeleton task source.

    Parameters
    ----------
    data : buffer
        The data source from a requests content.

    Returns
    -------
    dict
        The parsed skeleton with two entries: 'points' for points coordinates and 'lines' joining them.
    """
    return json.loads(data)


def parse_requests_tree(data):
    """Parse a requests content, should be from a TreeGraph task source.

    Parameters
    ----------
    data : buffer
        The data source from a requests content.

    Returns
    -------
    networkx.Graph
        The loaded (tree) graph object.
    """
    import pickle
    tree = pickle.load(BytesIO(data))
    # FIXME: it would be better to return something that is JSON serializable...
    #  but the tree is not directed, so the `json_graph.tree_data` fails!
    # from networkx.readwrite import json_graph
    # data = json_graph.tree_data(tree, root=0)
    # return json.dumps(data)
    return tree


def parse_requests_json(data):
    """Parse a requests content, should be from a AnglesAndInternodes task source.

    Parameters
    ----------
    data : buffer
        The data source from a requests content.

    Returns
    -------
    dict
        The full angles and internodes dictionary with 'angles', 'internodes', '' & '' entries.
    """
    return json.loads(data)


PARSER_DICT = {
    "PointCloud": parse_requests_pcd,
    "TriangleMesh": parse_requests_mesh,
    "CurveSkeleton": parse_requests_skeleton,
    "TreeGraph": parse_requests_tree,
    "AnglesAndInternodes": parse_requests_json,
}
EXT_PARSER_DICT = {
    "json": parse_requests_json,
}


def parse_task_requests_data(task, data, extension=None):
    """Parse raw request data for a specified task.

    The function selects an appropriate parser based on the provided
    *extension* (if any) or the *task* name, then applies that parser
    to the raw *data* payload.  This is a small dispatcher that
    centralises the logic for choosing between the generic
    :func:`parse_requests_json` parser and any custom parsers defined
    in :data:`PARSER_DICT` and :data:`EXT_PARSER_DICT`.

    Parameters
    ----------
    task : str
        Identifier for the task whose data is being parsed.  Used as a
        key to look up the default parser in :data:`PARSER_DICT`.
    data : str or bytes
        Raw payload that contains the request data.  The parser returned
        by the dispatcher is expected to accept this type and convert it
        into a Python object (e.g. a dictionary).
    extension : str, optional
        File‑extension or MIME‑type hint.  If supplied, the parser is
        taken from :data:`EXT_PARSER_DICT`; otherwise the default parser
        for *task* is used.

    Returns
    -------
    Any
        The result of the chosen parser applied to *data*.  The exact
        type depends on the parser implementation (commonly a dict).

    Examples
    --------
    >>> # Assume the following parsers are defined
    >>> def parse_json(data): return {"parsed": data}
    >>> PARSER_DICT = {"task1": parse_json}
    >>> EXT_PARSER_DICT = {"txt": parse_json}
    >>> # Example with task-based parser
    >>> result = parse_task_requests_data("task1", '{"key": "value"}')
    >>> print(result)
    {'parsed': '{"key": "value"}'}
    >>> # Example with extension-based parser
    >>> result = parse_task_requests_data("unknown", "raw data", extension="txt")
    >>> print(result)
    {'parsed': 'raw data'}

    Notes
    -----
    - The function does not perform any validation of *data*; it
      delegates all parsing logic to the chosen parser.
    - If *task* is not found in :data:`PARSER_DICT` and *extension* is
      ``None``, the fallback parser :func:`parse_requests_json` is used.

    See Also
    --------
    parse_requests_json : Default JSON parser used when no task matches.
    """
    if extension is not None:
        data_parser = EXT_PARSER_DICT[extension]
    else:
        data_parser = PARSER_DICT.get(task, parse_requests_json)
    return data_parser(data)


task_filesUri_mapping = {
    "PointCloud": "pointCloud",
    "TriangleMesh": "mesh",
    "CurveSkeleton": "skeleton",
    "TreeGraph": "tree",
}


def get_task_data(host, scan_id, task, filename=None, api_data=None, **kwargs):
    """Get the data corresponding to a `dataset/task/filename`.

    Parameters
    ----------
    host : str
        The hostname or IP address of the PlantDB REST API server.
    scan_id : str
        The name of the dataset.
    task : str
        The name of the task.
    filename : str, optional
        The name of the file to load.
        If not specified defaults to the main file returned by the task as defined in `filesUri_task_mapping`.
    api_data : dict, optional
        The dictionary of information for the dataset as returned by the REST API.
        If not specified, fetch it from the REST API.

    Other Parameters
    ----------------
    port : int
        The PlantDB API port number, defaults to ``None``.
    prefix : str
        A path prefix for the PlantDB API, defaults to ``None``.
    ssl : bool
        A boolean flag indicating whether to use HTTPS (``True``) or HTTP (``False``). Defaults to ``False``.

    Returns
    -------
    any
        The parsed data.

    See Also
    --------
    plantdb.client.rest_api.parse_task_requests_data

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> import numpy as np
    >>> from plantdb.client.rest_api.parsers import get_task_data
    >>> pcd = get_task_data('real_plant_analyzed', 'PointCloud')
    >>> np.array(pcd).shape
    (3, 57890)
    """
    if api_data is None:
        api_data = request_scan_data(host, scan_id, **kwargs)
    # Get data from `File` resource of REST API:
    ext = None
    if filename is None:
        file_uri = api_data["filesUri"][task_filesUri_mapping[task]]
    else:
        _, ext = Path(filename).suffix.split('.')
        file_uri = api_endpoints.file(scan_id, api_data["tasks_fileset"][task], filename)

    url = origin_url(host, **kwargs)

    data = make_api_request(url + file_uri, session_token=kwargs.get('session_token', None)).content
    return parse_task_requests_data(task, data, ext)


def _load_toml_from_url(url, **kwargs):
    """Load and parse a TOML file from a given URL.

    Parameters
    ----------
    url : str
        The URL to fetch the TOML file from.

    Returns
    -------
    dict or None
        The parsed TOML data as a dictionary, or None if the request fails.
    """
    import toml
    response = make_api_request(url, **kwargs)
    if response.ok:
        return toml.loads(response.content.decode('utf-8'))
    return None


def get_toml_file(host, file_path, **kwargs):
    """Return a loaded TOML file for selected dataset, if it exists.

    Parameters
    ----------
    scan_id : str
        The name of the dataset.
    file_path : str
        The path to the TOML file.

    Other Parameters
    ----------------
    port : int
        The PlantDB API port number, defaults to ``None``.
    prefix : str
        A path prefix for the PlantDB API, defaults to ``None``.
    ssl : bool
        A boolean flag indicating whether to use HTTPS (``True``) or HTTP (``False``). Defaults to ``False``.

    Returns
    -------
    dict
        The configuration dictionary.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api.parsers import get_toml_file
    >>> cfg = get_toml_file('real_plant_analyzed/pipeline.toml')
    >>> cfg['PointCloud']
    {'upstream_task': 'Voxels', 'level_set_value': 1.0}
    """
    url = scan_file_url(host, file_path, **kwargs)
    return _load_toml_from_url(url, **kwargs)


def get_scan_config(host, scan_id, cfg_fname='scan.toml', **kwargs):
    """Return the scan configuration for selected dataset, if it exists.

    Parameters
    ----------
    scan_id : str
        The name of the dataset.
    cfg_fname : str, optional
        The name of the configuration file.

    Other Parameters
    ----------------
    port : int
        The PlantDB API port number, defaults to ``None``.
    prefix : str
        A path prefix for the PlantDB API, defaults to ``None``.
    ssl : bool
        A boolean flag indicating whether to use HTTPS (``True``) or HTTP (``False``). Defaults to ``False``.

    Returns
    -------
    dict
        The configuration dictionary.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api.parsers import get_scan_config
    >>> cfg = get_scan_config('real_plant')
    >>> cfg['ScanPath']['class_name']
    'Circle'

    """
    return get_toml_file(host, f"{scan_id}/{cfg_fname}", **kwargs)


def get_reconstruction_config(host, scan_id, cfg_fname='pipeline.toml', **kwargs):
    """Return the reconstruction configuration for selected dataset, if it exists.

    Parameters
    ----------
    scan_id : str
        The name of the dataset.
    cfg_fname : str, optional
        The name of the configuration file.

    Other Parameters
    ----------------
    port : int
        The PlantDB API port number, defaults to ``None``.
    prefix : str
        A path prefix for the PlantDB API, defaults to ``None``.
    ssl : bool
        A boolean flag indicating whether to use HTTPS (``True``) or HTTP (``False``). Defaults to ``False``.

    Returns
    -------
    dict
        The configuration dictionary.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api.parsers import get_reconstruction_config
    >>> cfg = get_reconstruction_config('real_plant_analyzed')
    >>> cfg['PointCloud']['upstream_task']
    'Voxels'

    """
    return get_toml_file(host, f"{scan_id}/{cfg_fname}", **kwargs)


def get_angles_and_internodes_data(host, scan_id, **kwargs):
    """Return a dictionary with 'angles' and 'internodes' data for selected dataset, if it exists.

    Parameters
    ----------
    host : str
        The hostname or IP address of the PlantDB REST API server.
    scan_id : str
        The name of the dataset.

    Other Parameters
    ----------------
    port : int
        The PlantDB API port number, defaults to ``None``.
    prefix : str
        A path prefix for the PlantDB API, defaults to ``None``.
    ssl : bool
        A boolean flag indicating whether to use HTTPS (``True``) or HTTP (``False``). Defaults to ``False``.

    Returns
    -------
    dict
        A dictionary with 'angles' and 'internodes' data.

    Examples
    --------
    >>> # Start a test PlantDB REST API server first, in a terminal:
    >>> # $ fsdb_rest_api --test
    >>> from plantdb.client.rest_api.parsers import get_angles_and_internodes_data
    >>> data = get_angles_and_internodes_data('real_plant_analyzed')
    >>> print(list(data.keys()))
    ['angles', 'internodes']
    >>> print(len(data['angles']))
    33
    """
    url = join_url(origin_url(host, **kwargs), api_endpoints.sequence(scan_id))

    response = make_api_request(url, session_token=kwargs.get('session_token', None))
    if response.ok:
        data = json.loads(response.content.decode('utf-8'))
        return {seq: data[seq] for seq in ['angles', 'internodes']}
    else:
        return None
