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
from urllib.parse import urljoin
from urllib.parse import urlparse

import requests
from PIL import Image
from plyfile import PlyData

#: Default URL to REST API is 'localhost':
REST_API_URL = "127.0.0.1"
# Default port to REST API:
REST_API_PORT = 5000


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


def base_url(host=REST_API_URL, port=REST_API_PORT):
    """Generates the URL for the PlantDB REST API using the specified host and port.

    Parameters
    ----------
    host : str, optional
        The hostname or IP address of the PlantDB REST API server. Defaults to ``"127.0.0.1"``.
    port : str or int, optional
        The port number of the PlantDB REST API server. Defaults to ``5000``.

    Returns
    -------
    str
        A properly formatted URL of the PlantDB REST API.

    Examples
    --------
    >>> from plantdb.client.rest_api import base_url
    >>> base_url()
    'http://127.0.0.1:5000'
    """
    return f"http://{host}:{port}"


def scans_url(host=REST_API_URL, port=REST_API_PORT):
    """Generates the URL listing the scans from the PlantDB REST API.

    Parameters
    ----------
    host : str, optional
        The hostname or IP address of the PlantDB REST API server. Defaults to ``"127.0.0.1"``.
    port : str or int, optional
        The port number of the PlantDB REST API server. Defaults to ``5000``.

    Returns
    -------
    str
        A properly formatted URL of the PlantDB REST API pointing to the scans list.

    Examples
    --------
    >>> from plantdb.client.rest_api import scans_url
    >>> scans_url()
    'http://127.0.0.1:5000/scans'
    """
    return urljoin(base_url(host, port), "/scans")


def scan_url(scan_id, host=REST_API_URL, port=REST_API_PORT):
    """Generates the URL pointing to the scan JSON from the PlantDB REST API.

    Parameters
    ----------
    scan_id : str
        The name of the scan dataset to retrieve the JSON from.
    host : str, optional
        The hostname or IP address of the PlantDB REST API server. Defaults to ``"127.0.0.1"``.
    port : str or int, optional
        The port number of the PlantDB REST API server. Defaults to ``5000``.

    Returns
    -------
    str
        A properly formatted URL of the PlantDB REST API pointing to the scans list.

    Examples
    --------
    >>> from plantdb.client.rest_api import scan_url
    >>> scan_url("real_plant")
    'http://127.0.0.1:5000/scans/real_plant'
    """
    return urljoin(base_url(host, port), f"/scans/{sanitize_name(scan_id)}")


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

    Examples
    --------
    >>> from plantdb.client.rest_api import scan_image_url
    >>> scan_image_url("real_plant", "images", "00000_rgb")
    'http://127.0.0.1:5000/image/real_plant/images/00000_rgb?size=orig'
    """
    scan_id = sanitize_name(scan_id)
    fileset_id = sanitize_name(fileset_id)
    file_id = sanitize_name(file_id)
    return urljoin(base_url(host, port), f"/image/{scan_id}/{fileset_id}/{file_id}?size={size}")


def refresh_url(dataset_name=None, **kwargs):
    """Generates a formatted URL for refreshing a specific dataset or the entire database.

    Parameters
    ----------
    dataset_name : str or None, optional
        The name of the dataset for which the refresh URL needs to be generated.
        If not provided, the refresh URL for the entire server is returned instead.
        Defaults to ``None``.

    Other Parameters
    ----------------
    host : str
        The hostname or IP address of the PlantDB REST API server. Defaults to ``"127.0.0.1"``.
    port : str or int
        The port number of the PlantDB REST API server. Defaults to ``5000``.

    Returns
    -------
    str
        A correctly formatted URL for refreshing the specified dataset or the entire PlantDB REST API server.
    """
    url = urljoin(
        base_url(host=kwargs.get("host", None), port=kwargs.get("port", None)),
        "/refresh"
    )
    if dataset_name is None:
        return url
    else:
        dataset_name = sanitize_name(dataset_name)
        return f"{url}?scan_id={dataset_name}"


def archive_url(dataset_name, **kwargs):
    """Generates a formatted URL for accessing the archive of a specific dataset.

    Parameters
    ----------
    dataset_name : str
        Name of the dataset to access in the archive.

    Other Parameters
    ----------------
    host : str
        The hostname or IP address of the PlantDB REST API server. Defaults to ``"127.0.0.1"``.
    port : str or int
        The port number of the PlantDB REST API server. Defaults to ``5000``.

    Returns
    -------
    str
        Fully constructed URL for accessing the specified dataset archive.

    Examples
    --------
    >>> from plantdb.client.rest_api import archive_url
    >>> archive_url('arabidopsis000')
    'http://127.0.0.1:5000/archive/arabidopsis000'
    >>> archive_url('../arabidopsis000')
    'http://127.0.0.1:5000/archive/arabidopsis000'
    >>> archive_url('arabidopsis+000')
    ValueError: Invalid dataset name: 'arabidopsis+000'. Dataset names must be alphanumeric and can include underscores or dashes.
    """
    dataset_name = sanitize_name(dataset_name)
    url = urljoin(
        base_url(host=kwargs.get("host", None), port=kwargs.get("port", None)),
        f"/archive/{dataset_name}"
    )
    return url


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
    """
    from plantdb.commons.fsdb import Scan
    from plantdb.commons.fsdb import Fileset
    from plantdb.commons.fsdb import File
    scan_id = scan.id if isinstance(scan, Scan) else scan
    fileset_id = fileset.id if isinstance(fileset, Fileset) else fileset
    file_name = file.path().name if isinstance(file, File) else file
    return f"/files/{scan_id}/{fileset_id}/{file_name}"


def test_host_port_availability(url):
    """Verifies the connectivity to a given host and port from a URL-like string.

    This function parses a URL string into host and port components, attempts to establish a
    socket connection to check its availability, and raises appropriate exceptions on failure.

    Parameters
    ----------
    url : str
        A string specifying the host and port in the format 'host:port'.

    Raises
    ------
    ValueError
        If the input URL is not in the correct format 'host:port'.
    ConnectionError
        If the specified host and port cannot be connected, indicating that the port might
        be closed or unavailable.
    RuntimeError
        If an unexpected error occurs during the verification process.

    Examples
    --------
    >>> from plantdb.client.rest_api import test_host_port_availability
    >>> test_host_port_availability('127.0.0.1:5000')
    """
    import socket
    try:
        parsed_url = urlparse(url)
        host, port = parsed_url.hostname, parsed_url.port
        socket.setdefaulttimeout(2)  # Set a timeout for the connection check
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex((host, port)) != 0:
                raise ConnectionError(f"Cannot connect to {host}:{port}. Port might be closed or unavailable.")
    except ValueError:
        raise ValueError(f"Database URL should be 'host:port', got '{url}' instead.")
    except Exception as e:
        raise RuntimeError(f"Error verifying the URL '{url}': {e}")


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
    >>> from plantdb.client.rest_api import list_scan_names
    >>> # This example requires the PlantDB REST API to be active (`fsdb_rest_api --test` from plantdb library)
    >>> print(list_scan_names())
    ['arabidopsis000', 'real_plant', 'real_plant_analyzed', 'virtual_plant', 'virtual_plant_analyzed']
    """
    return sorted(requests.get(url=scans_url(host, port)).json())


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
    >>> from plantdb.client.rest_api import get_scans_info
    >>> # This example requires the PlantDB REST API to be active (`fsdb_rest_api --test` from plantdb library)
    >>> get_scans_info()
    """
    scan_list = list_scan_names(host, port)
    return [requests.get(url=scan_url(scan, host, port)).json() for scan in scan_list]


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
    >>> from plantdb.client.rest_api import parse_scans_info
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
    >>> from plantdb.client.rest_api import get_scan_data
    >>> # This example requires the PlantDB REST API to be active (`fsdb_rest_api --test` from plantdb library)
    >>> scan_data = get_scan_data('real_plant')
    >>> print(scan_data['id'])
    real_plant
    >>> print(scan_data['hasColmap'])
    False
    """
    return requests.get(url=scan_url(scan_id, host, port)).json()


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
    scan_id = sanitize_name(scan_id)
    thumb_uri = get_scan_data(scan_id, host, port)["thumbnailUri"]
    if size != "thumb":
        thumb_uri = thumb_uri.replace("size=thumb", f"size={size}")
    return f"{base_url(host, port)}{thumb_uri}"


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
    dataset_name = sanitize_name(dataset_name)
    task_name = sanitize_name(task_name)
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
    >>> from plantdb.client.rest_api import get_images_from_task
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
    list
        The XYZ array of vertex coordinates, returned as a list to be JSON serializable.
    """
    return [list(data['vertex']['x']), list(data['vertex']['y']), list(data['vertex']['z'])]


def _ply_face_to_array(data):
    """Convert the `PlyData` 'face' data into an XYZ array of triangle coordinates.

    Parameters
    ----------
    data : PlyData
        The `PlyData` object to be converted as numpy array.

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
    """The task data parser, behave according to the source and default to JSON parser."""
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
    plantdb.server.rest_api.filesUri_task_mapping
    plantdb.client.rest_api.parse_task_requests_data
    """
    if api_data is None:
        api_data = get_scan_data(dataset_name, **api_kwargs)
    # Get data from `File` resource of REST API:
    ext = None
    if filename is None:
        file_uri = api_data["filesUri"][task_filesUri_mapping[task]]
    else:
        _, ext = Path(filename).suffix.split('.')
        file_uri = get_file_uri(dataset_name, api_data["tasks_fileset"][task], filename)

    url = base_url(**api_kwargs)
    data = requests.get(url + file_uri).content
    return parse_task_requests_data(task, data, ext)


def get_toml_file(dataset_name, file_path, **api_kwargs):
    """Return a loaded TOML file for selected dataset, if it exists.

    Parameters
    ----------
    dataset_name : str
        The name of the dataset.
    file_path : str
        The path to the TOML file.

    Other Parameters
    ----------------
    host : str
        The IP address of the PlantDB REST API. Defaults to ``"127.0.0.1"``.
    port : str or int
        The port of the PlantDB REST API. Defaults to ``5000``.

    Returns
    -------
    dict
        The configuration dictionary.
    """
    import toml
    url = base_url(**api_kwargs)
    res = requests.get(url + f"/files/{dataset_name}/{file_path}")
    if res.ok:
        data = toml.loads(res.content.decode('utf-8'))
        return data
    else:
        return None


def get_scan_config(dataset_name, cfg_fname='scan.toml', **api_kwargs):
    """Return the scan configuration for selected dataset, if it exists.

    Parameters
    ----------
    dataset_name : str
        The name of the dataset.
    cfg_fname : str, optional
        The name of the configuration file.

    Other Parameters
    ----------------
    host : str
        The IP address of the PlantDB REST API. Defaults to ``"127.0.0.1"``.
    port : str or int
        The port of the PlantDB REST API. Defaults to ``5000``.

    Returns
    -------
    dict
        The configuration dictionary.
    """
    return get_toml_file(dataset_name, cfg_fname, **api_kwargs)


def get_reconstruction_config(dataset_name, cfg_fname='pipeline.toml', **api_kwargs):
    """Return the reconstruction configuration for selected dataset, if it exists.

    Parameters
    ----------
    dataset_name : str
        The name of the dataset.
    cfg_fname : str, optional
        The name of the configuration file.

    Other Parameters
    ----------------
    host : str
        The IP address of the PlantDB REST API. Defaults to ``"127.0.0.1"``.
    port : str or int
        The port of the PlantDB REST API. Defaults to ``5000``.

    Returns
    -------
    dict
        The configuration dictionary.
    """
    return get_toml_file(dataset_name, cfg_fname, **api_kwargs)


def get_angles_and_internodes_data(dataset_name, **api_kwargs):
    """Return a dictionary with 'angles' and 'internodes' data for selected dataset, if it exists.

    Parameters
    ----------
    dataset_name : str
        The name of the dataset.

    Other Parameters
    ----------------
    host : str
        The IP address of the PlantDB REST API. Defaults to ``"127.0.0.1"``.
    port : str or int
        The port of the PlantDB REST API. Defaults to ``5000``.

    Returns
    -------
    dict
        A dictionary with 'angles' and 'internodes' data.
    """
    url = base_url(**api_kwargs)
    res = requests.get(url + f"/sequence/{dataset_name}")
    if res.ok:
        data = json.loads(res.content.decode('utf-8'))
        return {seq: data[seq] for seq in ['angles', 'internodes']}
    else:
        return None


def upload_dataset_file(scan_id, file_path, chunk_size=0, **kwargs):
    """Uploads a file to the server using the DatasetFile POST endpoint.

    Parameters
    ----------
    server_url : str
        The base URL of the server hosting the REST API.
    scan_id : str
        The unique identifier of the scan associated with the file upload.
    file_path : str
        The path to the file to be uploaded.
    chunk_size : int, optional
        The size of chunks (in bytes) to read and send, by default 0 (no chunking).

    Other Parameters
    ----------------
    host : str
        The IP address of the PlantDB REST API. Defaults to ``"127.0.0.1"``.
    port : str or int
        The port of the PlantDB REST API. Defaults to ``5000``.

    Returns
    -------
    dict
        A dictionary containing the server's response.

    Examples
    --------
    >>> from plantdb.client.rest_api import upload_dataset_file
    >>> upload_dataset_file('arabidopsis000', '/path/to/local/file.txt')
    """
    from os.path import basename
    from os.path import getsize
    # Prepare the URL and headers
    scan_id = sanitize_name(scan_id)
    url = urljoin(
        base_url(host=kwargs.get("host", None), port=kwargs.get("port", None)),
        f"/files/{scan_id}"
    )

    filename = basename(file_path)
    file_size = getsize(file_path)
    # Create the request header
    headers = {
        "Content-Disposition": f"attachment; filename={filename}",
        "Content-Length": str(file_size),
        "X-File-Path": filename,
    }

    try:
        # Open the file for reading
        with open(file_path, 'rb') as f:
            if chunk_size > 0:
                # Upload in chunks
                headers["X-Chunk-Size"] = str(chunk_size)
                bytes_sent = 0
                while bytes_sent < file_size:
                    chunk = f.read(chunk_size)
                    response = requests.post(
                        url,
                        headers=headers,
                        data=chunk,
                    )
                    bytes_sent += len(chunk)
                    # Check if the request was successful
                    if response.status_code not in (200, 201):
                        return {"error": "File upload failed", "status_code": response.status_code,
                                "response": response.json()}
            else:
                # Upload the entire file
                response = requests.post(url, headers=headers, data=f)

        # Return the server's response
        if response.status_code in (200, 201):
            return response.json()
        else:
            return {"error": "File upload failed", "status_code": response.status_code, "response": response.json()}
    except Exception as e:
        return {"error": str(e)}


def refresh(dataset_name=None, **kwargs):
    """Refreshes the database, potentialy only for a specified dataset.

    Parameters
    ----------
    dataset_name : str or None
        The name of the dataset to trigger a refresh.
        If ``None``, the entire database is refreshed.

    Other Parameters
    ----------------
    host : str
        The IP address of the PlantDB REST API. Defaults to ``"127.0.0.1"``.
    port : str or int
        The port of the PlantDB REST API. Defaults to ``5000``.
    timeout : int, optional
        A timeout, in seconds, to suceed the refresh request. Defaults to ``5``.

    Returns
    -------
    dict
        Parsed JSON response from the refresh API if the request is successful.

    Raises
    ------
    HTTPError
        If the request fails or the response status is not successful.

    Examples
    --------
    >>> from plantdb.client.rest_api import refresh
    >>> refresh("arabidopsis000", host="127.0.0.1", port="5000")
    """
    res = requests.post(
        refresh_url(dataset_name, host=kwargs.get("host", None), port=kwargs.get("port", None)),
        timeout=kwargs.get("timeout", 5)
    )
    if res.ok:
        return res.json()
    else:
        res.raise_for_status()  # Raise an error if the request failed


def download_scan_archive(dataset_name, out_dir=None, **kwargs):
    """Downloads a scan archive file from a defined dataset based on the specified API parameters.

    This function fetches a scan archive in stream mode from a remote API. The archive
    is expected to be in the form of a binary content stream. The success of the
    operation is determined by the HTTP response received from the API.

    Parameters
    ----------
    dataset_name : str
        The name of the dataset from which the scan archive file is to be downloaded.
    out_dir : str or pathlib.Path, optional
        A path to the directory where to save the archive.

    Other Parameters
    ----------------
    host : str
        The IP address of the PlantDB REST API. Defaults to ``"127.0.0.1"``.
    port : str or int
        The port of the PlantDB REST API. Defaults to ``5000``.
    timeout : int, optional
        A timeout, in seconds, to suceed the download request. Defaults to ``10``.

    Returns
    -------
    BytesIO or str
        A `BytesIO` object containing the binary content of the downloaded scan archive.
        A path to the downloaded file, if a directory path is specified.

    Examples
    --------
    >>> from plantdb.client.rest_api import download_scan_archive
    >>> download_scan_archive("arabidopsis000", out_dir='/tmp', host="127.0.0.1", port="5000")
    """
    import time
    url = archive_url(dataset_name, host=kwargs.get("host", None), port=kwargs.get("port", None))

    start_time = time.time()  # Start timing
    res = requests.get(url, stream=True, timeout=kwargs.get("timeout", 10))
    end_time = time.time()  # End timing
    duration = end_time - start_time
    msg = f"Download completed in {duration:.2f} seconds."

    if res.ok:
        if out_dir is not None:
            out_dir = Path(out_dir) / f"{dataset_name}.zip"
            with open(out_dir, "wb") as archive_file:
                archive_file.write(res.content)
            return f"{out_dir}", msg
        else:
            return BytesIO(res.content), msg
    else:
        res.raise_for_status()  # Raise an error if the request failed


def upload_scan_archive(dataset_name, path, **kwargs):
    """Upload a scan archive file to a specified dataset on a server.

    This function sends a POST request to upload a scan archive file to a
    particular dataset, utilizing the archive URL and optionally specified
    additional API-related request parameters. Ensures proper handling of
    file opening/closing procedures and response status checks.

    Parameters
    ----------
    dataset_name : str
        The name of the target dataset for the archive upload.
    path : str
        The local file system path to the archive to be uploaded.

    Other Parameters
    ----------------
    host : str
        The IP address of the PlantDB REST API. Defaults to ``"127.0.0.1"``.
    port : str or int
        The port of the PlantDB REST API. Defaults to ``5000``.
    timeout : int, optional
        A timeout, in seconds, to suceed the upload request. Defaults to ``120``.

    Returns
    -------
    str
        The time it took to upload the archive.

    Raises
    ------
    requests.exceptions.RequestException
        If the HTTP request fails for any reason.
    requests.exceptions.HTTPError
        If the request returns an unsuccessful HTTP status code.

    Examples
    --------
    >>> from plantdb.client.rest_api import upload_scan_archive
    >>> upload_scan_archive("arabidopsis000", path='/tmp/arabidopsis000.zip', host="127.0.0.1", port="5002")
    """
    import time
    from zipfile import ZipFile

    path = Path(path)
    # Verify path existence
    if not path.is_file():
        raise FileNotFoundError(f"The file at path '{path}' does not exist!")
    # Verify the integrity of the ZIP file
    try:
        with ZipFile(path, 'r') as zip_file:
            zip_file.testzip()
    except Exception as e:
        print(e)
        raise IOError(f"Invalid ZIP file '{path}!'")

    # Construct the URL for the archive upload:
    url = archive_url(dataset_name, host=kwargs.get("host", None), port=kwargs.get("port", None))

    start_time = time.time()  # Start timing
    with open(path, "rb") as f:
        timeout = kwargs.get("timeout", 120)
        try:
            res = requests.post(url, files={"zip_file": (path.name, f, "application/zip")}, stream=True,
                                timeout=timeout)
        except requests.exceptions.Timeout:
            raise RuntimeError(f"The upload request timed out after {timeout} seconds.")
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"An error occurred during the upload: {e}")
    end_time = time.time()  # End timing

    if res.ok:
        duration = end_time - start_time
        return f"Upload completed in {duration:.2f} seconds."
    else:
        res.raise_for_status()  # Raise an error if the request failed
