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
This module regroup the client-side methods of the REST API.
"""
import json
from io import BytesIO
from pathlib import Path

import numpy as np
import requests
from PIL import Image
from plyfile import PlyData

from plantdb.rest_api import filesUri_task_mapping
from plantdb.rest_api import get_file_uri

#: Default URL to REST API is 'localhost':
REST_API_URL = "127.0.0.1"
# Default port to REST API:
REST_API_PORT = 5000


def base_url(host=REST_API_URL, port=REST_API_PORT):
    """Format the URL for the PlantDB REST API at given host and port.

    Parameters
    ----------
    host : str
        The hostname or IP address of the PlantDB REST API server. Defaults to ``"127.0.0.1"``.
    port : str
        The port number of the PlantDB REST API server. Defaults to ``5000``.

    Returns
    -------
    str
        The formatted URL.

    Examples
    --------
    >>> from plantdb.rest_api_client import base_url
    >>> base_url()
    'http://127.0.0.1:5000'
    """
    return f"http://{host}:{port}"


def test_db_availability(host=REST_API_URL, port=REST_API_PORT):
    """Test the REST API server availability.

    Parameters
    ----------
    host : str, optional
        The IP address of the PlantDB REST API. Defaults to ``"127.0.0.1"``.
    port : str or int, optional
        The port of the PlantDB REST API. Defaults to ``5000``.

    Returns
    -------
    bool
        The scans information dictionary.

    Examples
    --------
    >>> from plantdb.rest_api_client import test_db_availability
    >>> test_db_availability()
    """
    try:
        requests.get(url=f"{base_url(host, port)}/scans")
    except requests.exceptions.ConnectionError:
        return False
    else:
        return True


def get_scans_info(host=REST_API_URL, port=REST_API_PORT):
    """Retrieve the information dictionary for all scans from the PlantDB REST API.

    Parameters
    ----------
    host : str, optional
        The IP address of the PlantDB REST API. Defaults to ``"127.0.0.1"``.
    port : str or int, optional
        The port of the PlantDB REST API. Defaults to ``5000``.

    Returns
    -------
    dict
        The scans information dictionary.

    Examples
    --------
    >>> from plantdb.rest_api_client import get_scans_info
    >>> # This example requires the PlantDB REST API to be active (`fsdb_rest_api --test` from plantdb library)
    >>> get_scans_info()
    """
    return requests.get(url=f"{base_url(host, port)}/scans").json()


def parse_scans_info(host=REST_API_URL, port=REST_API_PORT):
    """Parse the information dictionary for all scans served by the PlantDB REST API.

    Parameters
    ----------
    host : str, optional
        The IP address of the PlantDB REST API. Defaults to ``"127.0.0.1"``.
    port : str or int, optional
        The port of the PlantDB REST API. Defaults to ``5000``.

    Returns
    -------
    dict
        The scan-id (dataset name) indexed information dictionary.

    Examples
    --------
    >>> from plantdb.rest_api_client import parse_scans_info
    >>> # This example requires the PlantDB REST API to be active (`fsdb_rest_api --test` from plantdb library)
    >>> scan_dict = parse_scans_info()
    >>> print(sorted(scan_dict.keys()))
    ['arabidopsis000', 'real_plant', 'real_plant_analyzed', 'virtual_plant', 'virtual_plant_analyzed']
    """
    scan_json = get_scans_info(host, port)
    scan_dict = {}
    for scan in scan_json:
        name = scan.pop('id')
        scan_dict[name] = scan
    return scan_dict


def get_scan_data(scan_id, host=REST_API_URL, port=REST_API_PORT):
    """Retrieve the data dictionary for a given scan dataset from the PlantDB REST API.

    Parameters
    ----------
    scan_id : str
        The name of the scan dataset to be retrieved.
    host : str, optional
        The IP address of the PlantDB REST API. Defaults to ``"127.0.0.1"``.
    port : str or int, optional
        The port of the PlantDB REST API. Defaults to ``5000``.

    Returns
    -------
    dict
        The data dictionary for the given scan dataset.

    Examples
    --------
    >>> from plantdb.rest_api_client import get_scan_data
    >>> # This example requires the PlantDB REST API to be active (`fsdb_rest_api --test` from plantdb library)
    >>> scan_data = get_scan_data('real_plant')
    >>> print(scan_data['id'])
    real_plant
    >>> print(scan_data['hasColmap'])
    False
    """
    return requests.get(url=f"{base_url(host, port)}/scans/{scan_id}").json()


def list_scan_names(host=REST_API_URL, port=REST_API_PORT):
    """List the names of the scan datasets served by the PlantDB REST API.

    Parameters
    ----------
    host : str, optional
        The IP address of the PlantDB REST API. Defaults to ``"127.0.0.1"``.
    port : str or int, optional
        The port of the PlantDB REST API. Defaults to ``5000``.

    Returns
    -------
    list
        The list of scan dataset names served by the PlantDB REST API.

    Examples
    --------
    >>> from plantdb.rest_api_client import list_scan_names
    >>> # This example requires the PlantDB REST API to be active (`fsdb_rest_api --test` from plantdb library)
    >>> print(list_scan_names())
    ['arabidopsis000', 'real_plant', 'real_plant_analyzed', 'virtual_plant', 'virtual_plant_analyzed']
    """
    return sorted(parse_scans_info(host, port).keys())


def scan_preview_image_url(scan_id, host=REST_API_URL, port=REST_API_PORT, size="thumb"):
    """Get the URL to the preview image for a scan dataset served by the PlantDB REST API.

    Parameters
    ----------
    scan_id : str
        The name of the scan dataset to be retrieved.
    host : str, optional
        The IP address of the PlantDB REST API. Defaults to ``"127.0.0.1"``.
    port : str or int, optional
        The port of the PlantDB REST API. Defaults to ``5000``.
    size : {'orig', 'large', 'thumb'} or int, optional
        If an integer, use  it as the size of the cached image to create and return.
        Else, should be a string, defaulting to ``'thumb'``, and it works as follows:
           * ``'thumb'``: image max width and height to `150`.
           * ``'large'``: image max width and height to `1500`;
           * ``'orig'``: original image, no cache;

    Returns
    -------
    str
        The URL to the preview image for a scan dataset.
    """
    thumb_uri = get_scan_data(scan_id, host, port)["thumbnailUri"]
    if size != "thumb":
        thumb_uri = thumb_uri.replace("size=thumb", f"size={size}")
    return f"{base_url(host, port)}{thumb_uri}"


def scan_image_url(scan_id, fileset_id, file_id, size='orig', host=REST_API_URL, port=REST_API_PORT):
    """Get the URL to the image for a scan dataset and task fileset served by the PlantDB REST API.

    Parameters
    ----------
    scan_id : str
        The name of the scan dataset to be retrieved.
    fileset_id : str
        The name of the fileset containing the image to be retrieved.
    file_id : str
        The name of the image file to be retrieved.
    size : {'orig', 'large', 'thumb'} or int, optional
        If an integer, use  it as the size of the cached image to create and return.
        Else, should be a string, defaulting to ``'orig'``, and it works as follows:
           * ``'thumb'``: image max width and height to `150`.
           * ``'large'``: image max width and height to `1500`;
           * ``'orig'``: original image, no cache;
    host : str, optional
        The IP address of the PlantDB REST API. Defaults to ``"127.0.0.1"``.
    port : str or int, optional
        The port of the PlantDB REST API. Defaults to ``5000``.

    Returns
    -------
    str
        The URL to an image of a scan dataset and task fileset.
    """
    return f"{base_url(host, port)}/image/{scan_id}/{fileset_id}/{file_id}?size={size}"


def get_scan_image(scan_id, fileset_id, file_id, size='orig', host=REST_API_URL, port=REST_API_PORT):
    """Get the image for a scan dataset and task fileset served by the PlantDB REST API.

    Parameters
    ----------
    scan_id : str
        The name of the scan dataset to be retrieved.
    fileset_id : str
        The name of the fileset containing the image to be retrieved.
    file_id : str
        The name of the image file to be retrieved.
    size : {'orig', 'large', 'thumb'} or int, optional
        If an integer, use  it as the size of the cached image to create and return.
        Else, should be a string, defaulting to ``'orig'``, and it works as follows:
           * ``'thumb'``: image max width and height to `150`.
           * ``'large'``: image max width and height to `1500`;
           * ``'orig'``: original image, no cache;
    host : str, optional
        The IP address of the PlantDB REST API. Defaults to ``"127.0.0.1"``.
    port : str or int, optional
        The port of the PlantDB REST API. Defaults to ``5000``.

    Returns
    -------
    requests.Response
        The URL to an image of a scan dataset and task fileset.
    """
    return requests.get(url=scan_image_url(scan_id, fileset_id, file_id, size, host, port))


def get_tasks_fileset_from_api(dataset_name, host=REST_API_URL, port=REST_API_PORT):
    """Get the task name to fileset name mapping dictionary from the REST API.

    Parameters
    ----------
    dataset_name : str
        The name of the dataset to retrieve the mapping for.
    host : str
        The IP address of the PlantDB REST API. Defaults to ``"127.0.0.1"``.
    port : str or int
        The port of the PlantDB REST API. Defaults to ``5000``.

    Returns
    -------
    dict
        The mapping of the task name to fileset name.
    """
    return get_scan_data(dataset_name, host, port).get('tasks_fileset', dict())


def list_task_images_uri(dataset_name, task_name='images', size='orig', **api_kwargs):
    """Get the list of images URI for a given dataset and task name.

    Parameters
    ----------
    dataset_name : str
        The name of the dataset to retrieve the images for.
    task_name : str, optional
        The name of the task to retrieve the images from. Defaults to 'images'.
    size : {'orig', 'large', 'thumb'} or int, optional
        If an integer, use  it as the size of the cached image to create and return.
        Else, should be a string, defaulting to `'orig'`, and it works as follows:
           * `'thumb'`: image max width and height to `150`.
           * `'large'`: image max width and height to `1500`;
           * `'orig'`: original image, no cache;

    Other Parameters
    ----------------
    host : str
        The IP address of the PlantDB REST API. Defaults to ``"127.0.0.1"``.
    port : str or int
        The port of the PlantDB REST API. Defaults to ``5000``.

    Returns
    -------
    list of str
        The list of image URI strings for the PlantDB REST API.
    """
    scan_info = get_scan_data(dataset_name, **api_kwargs)
    tasks_fileset = scan_info["tasks_fileset"]
    images = scan_info["images"]
    url = base_url(**api_kwargs)
    return [url + f"/image/{dataset_name}/{tasks_fileset[task_name]}/{Path(img).stem}?size={size}" for img in images]


def get_images_from_task(dataset_name, task_name='images', size='orig', **api_kwargs):
    """Get the list of images data for a given dataset and task name.

    Parameters
    ----------
    dataset_name : str
        The name of the dataset to retrieve the images for.
    task_name : str, optional
        The name of the task to retrieve the images from. Defaults to 'images'.
    size : {'orig', 'large', 'thumb'} or int, optional
        If an integer, use  it as the size of the cached image to create and return.
        Else, should be a string, defaulting to `'orig'`, and it works as follows:
           * `'thumb'`: image max width and height to `150`.
           * `'large'`: image max width and height to `1500`;
           * `'orig'`: original image, no chache;

    Other Parameters
    ----------------
    host : str
        The IP address of the PlantDB REST API. Defaults to ``"127.0.0.1"``.
    port : str or int
        The port of the PlantDB REST API. Defaults to ``5000``.

    Returns
    -------
    list of PIL.Image
        The list of PIL.Image from the PlantDB REST API.

    Examples
    --------
    >>> from plantdb.rest_api_client import get_images_from_task
    >>> images = get_images_from_task('real_plant', host='127.0.0.1', port='5000')
    >>> print(len(images))
    60
    >>> img1 = images[0]
    >>> print(img1.size)
    (1440, 1080)
    """
    images = []
    for img_uri in list_task_images_uri(dataset_name, task_name, size, **api_kwargs):
        images.append(Image.open(BytesIO(requests.get(img_uri).content)))
    return images


def _ply_vertex_to_array(data):
    """Convert the `PlyData` 'vertex' data into an XYZ array of vertex coordinates.

    Parameters
    ----------
    data : PlyData
        The `PlyData` object to be converted as numpy array.

    Returns
    -------
    numpy.ndarray
        The XYZ array of vertex coordinates.
    """
    return np.array([data['vertex']['x'], data['vertex']['y'], data['vertex']['z']]).T

def _ply_face_to_array(data):
    """Convert the `PlyData` 'face' data into an XYZ array of triangle coordinates.

    Parameters
    ----------
    data : PlyData
        The `PlyData` object to be converted as numpy array.

    Returns
    -------
    numpy.ndarray
        The XYZ array of triangle coordinates.
    """
    return np.array([d for d in data['face'].data['vertex_indices']]).T

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
    return pickle.load(BytesIO(data))


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
    """The task data parser, behave according to the source and default to JSON parser."""
    if extension is not None:
        data_parser = EXT_PARSER_DICT[extension]
    else:
        data_parser = PARSER_DICT.get(task, parse_requests_json)
    return data_parser(data)


def get_task_data(dataset_name, task, filename=None, api_data=None, **api_kwargs):
    """Get the data corresponding to a `dataset/task/filename`.

    Parameters
    ----------
    dataset_name : str
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
    host : str
        The IP address of the PlantDB REST API. Defaults to ``"127.0.0.1"``.
    port : str or int
        The port of the PlantDB REST API. Defaults to ``5000``.

    Returns
    -------
    any
        The parsed data.

    See Also
    --------
    plantdb.rest_api.filesUri_task_mapping
    plantdb.rest_api_client.parse_task_requests_data
    """
    if api_data is None:
        api_data = get_scan_data(dataset_name, **api_kwargs)
    # Get data from `File` resource of REST API:
    ext = None
    if filename is None:
        file_uri = api_data["filesUri"][filesUri_task_mapping[task]]
    else:
        _, ext = Path(filename).suffix.split('.')
        file_uri = get_file_uri(dataset_name, api_data["tasks_fileset"][task], filename)

    url = base_url(**api_kwargs)
    data = requests.get(url + file_uri).content
    return parse_task_requests_data(task, data, ext)
