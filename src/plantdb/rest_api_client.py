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
from io import BytesIO
from pathlib import Path

import numpy as np
import requests
from PIL import Image
from plyfile import PlyData

#: Default URL to REST API is 'localhost':
REST_API_URL = "127.0.0.1"
# Default port to REST API:
REST_API_PORT = 5000


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
        requests.get(url=f"http://{host}:{port}/scans")
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
    return requests.get(url=f"http://{host}:{port}/scans").json()


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
    return requests.get(url=f"http://{host}:{port}/scans/{scan_id}").json()


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
    return f"http://{host}:{port}{thumb_uri}"


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
    return f"http://{host}:{port}/image/{scan_id}/{fileset_id}/{file_id}?size={size}"


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
    base_url = f"http://{api_kwargs['host']}:{api_kwargs['port']}"
    return [base_url + f"/image/{dataset_name}/{tasks_fileset[task_name]}/{Path(img).stem}?size={size}" for img in
            images]


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
    >>> images = get_images_from_task('real_plant')
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


def _ply_pcd_to_array(ply_pcd):
    """Convert the `PlyData` into an XYZ array of vertex coordinates.

    Parameters
    ----------
    ply_pcd : PlyData
        The `PlyData` object to be converted as numpy array.

    Returns
    -------
    numpy.ndarray
        The XYZ array of vertex coordinates.
    """
    return np.array([ply_pcd['vertex']['x'], ply_pcd['vertex']['y'], ply_pcd['vertex']['z']]).T


def get_dataset_data(dataset_name, **api_kwargs):
    """"""
    base_url = f"http://{api_kwargs['host']}:{api_kwargs['port']}"
    scan_info = get_scan_data(dataset_name, **api_kwargs)

    # Get pointcloud data from RET API:
    ## Use the file URI to get the fileset id & file id:
    pcd_fileset_id = scan_info["tasks_fileset"].get('PointCloud', None)
    pcd_file_id = "PointCloud"
    ## Use the REST API to stream the pointcloud:
    pcd_file = requests.get(base_url + f"/pointcloud/{dataset_name}/{pcd_fileset_id}/{pcd_file_id}").content
    ## Read the PLY as a `PlyData`:
    ply_pcd = PlyData.read(BytesIO(pcd_file))
    ## Convert the `PlyData` into an XYZ array of vertex coordinates:
    pcd_array = _ply_pcd_to_array(ply_pcd)

    # Get mesh data from RET API:
    ## Use the file URI to get the fileset id & file id:
    mesh_uri = scan_info["filesUri"]["mesh"]
    mesh_fileset_id, mesh_file_id = Path(mesh_uri).parts[-2:]
    mesh_file_id = Path(mesh_file_id).stem
    ## Use the REST API to stream the pointcloud:
    mesh_file = requests.get(base_url + f"/mesh/{dataset_name}/{mesh_fileset_id}/{mesh_file_id}").content
    mesh_data = PlyData.read(BytesIO(mesh_file))
    mesh_data = None

    # Get tree data from RET API:
    ## Use the file URI to get the fileset id & file id:
    tree_uri = scan_info["filesUri"]["tree"]
    ## Use the REST API to stream the pointcloud:
    tree_file = requests.get(base_url + tree_uri).content
    tree_data = None  # tree files are NetworkX graphs!

    fruit_dir = None
    stem_dir = None
    return {"PointCloud": pcd_array, "TriangleMesh": mesh_data, "TreeGraph": tree_data,
            "FruitDirection": fruit_dir, "StemDirection": stem_dir}
