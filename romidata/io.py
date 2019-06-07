"""
romidata.io
===========

ROMI Data IO
This module contains all functions for reading and writing data.

File formats
------------

json
****

* Python objects: dict, list
* File extensions: 'json'

toml
****

* Python objects: dict, list
* File extensions: 'toml'

2D image
********

* Image data read and written using ``imageio``

Python objects: np.ndarray
File extensions: 'jpg', 'png'

3D Volumes
**********

* Volume data (3D numpy arrays) read and written using ``imageio``

Python objects: np.ndarray
File extensions: 'tiff'

Point Clouds
************

* Python object: open3d.geometry.PointCloud
* File extesions: 'ply'

Triangle  Meshes
****************

* Python object: open3d.geometry.TriangleMesh
* File extesions: 'ply'

"""

import open3d
import os
import imageio
import json
import toml
import numpy as np
import networkx as nx
import tempfile

def read_json(dbfile):
    """Reads json from a DB file.
    Parameters
    __________
    dbfile : db.File

    Returns
    _______
    jsonifyable object
    """

    return json.loads(dbfile.read())

def write_json(dbfile, data, ext="json"):
    """Writes json to a DB file.
    Parameters
    __________
    dbfile : db.File
    data : jsonifyable object
    ext : str
        file extension (defaults to "json")
    """
    dbfile.write(json.dumps(data), ext)

def read_toml(dbfile):
    """Reads toml from a DB file.
    Parameters
    __________
    dbfile : db.File

    Returns
    _______
    jsonifyable object
    """
    return toml.loads(dbfile.read())

def write_toml(dbfile, data, ext="toml"):
    """Writes toml to a DB file.
    Parameters
    __________
    dbfile : db.File
    data : jsonifyable object
    ext : str
        file extension (defaults to "toml")
    """
    dbfile.write(toml.dumps(data), ext)

def read_image(dbfile):
    """Reads image from a DB file.
    Parameters
    __________
    dbfile : db.File

    Returns
    _______
    np.ndarray
    """
    return imageio.imread(dbfile.read_raw())

def write_image(dbfile, data, ext="jpg"):
    """Writes image to a DB file.
    Parameters
    __________
    dbfile : db.File
    data : imageifyable object
    ext : str
        file extension (defaults to "jpg")
    """
    if ext == "jpg" and len(data.shape) == 3:
        data = data[:,:,:3]
    b = imageio.imwrite(imageio.RETURN_BYTES, data, format=ext)
    dbfile.write_raw(b, ext)

def read_volume(dbfile):
    """Reads volume from a DB file.
    Parameters
    __________
    dbfile : db.File

    Returns
    _______
    np.ndarray
    """
    return imageio.volread(dbfile.read_raw(), format="npz")

def write_volume(dbfile, data):
    """Writes volume to a DB file.
    Parameters
    __________
    dbfile : db.File
    data : 3D numpy array
    ext : str
        file extension (defaults to "tiff").
    """
    b = imageio.volwrite(imageio.RETURN_BYTES, data, format="npz")
    dbfile.write_raw(b, "npz")

def read_point_cloud(dbfile, ext="ply"):
    """Reads point cloud from a DB file.
    Parameters
    __________
    dbfile : db.File

    Returns
    _______
    PointCloud
    """
    b = dbfile.read_raw()
    with tempfile.TemporaryDirectory() as d:
        fname = os.path.join(d, "temp.%s"%ext)
        with open(fname, "wb") as fh:
            fh.write(b)
        return open3d.io.read_point_cloud(fname)

def write_point_cloud(dbfile, data, ext="ply"):
    """Writes point cloud to a DB file.
    Parameters
    __________
    dbfile : db.File
    data : PointCloud
    ext : str
        file extension (defaults to "ply").
    """
    with tempfile.TemporaryDirectory() as d:
        fname = os.path.join(d, "temp.%s"%ext)
        open3d.io.write_point_cloud(fname, data)
        dbfile.import_file(fname)

def read_triangle_mesh(dbfile, ext="ply"):
    """Reads triangle mesh from a DB file.
    Parameters
    __________
    dbfile : db.File

    Returns
    _______
    PointCloud
    """
    b = dbfile.read_raw()
    with tempfile.TemporaryDirectory() as d:
        fname = os.path.join(d, "temp.%s"%ext)
        with open(fname, "wb") as fh:
            fh.write(b)
        return open3d.io.read_triangle_mesh(fname)

def write_triangle_mesh(dbfile, data, ext="ply"):
    """Writes triangle mesh to a DB file.
    Parameters
    __________
    dbfile : db.File
    data : PointCloud
    ext : str
        file extension (defaults to "ply").
    """
    with tempfile.TemporaryDirectory() as d:
        fname = os.path.join(d, "temp.%s"%ext)
        open3d.io.write_triangle_mesh(fname, data)
        dbfile.import_file(fname)


def read_graph(dbfile):
    """Reads treex tree from a DB file.
    Parameters
    __________
    dbfile : db.File

    Returns
    _______
    TriangleMesh
    """
    b = dbfile.read_raw()
    ext = "p"
    with tempfile.TemporaryDirectory() as d:
        fname = os.path.join(d, "temp.%s"%ext)
        with open(fname, "wb") as fh:
            fh.write(b)
        return nx.read_gpickle(fname)

def write_graph(dbfile, data):
    """Writes treex tree to a DB file.
    Parameters
    __________
    dbfile : db.File
    data : treex.tree.Tree
    ext : str
        file extension (defaults to "treex").
    """
    ext = "p"
    with tempfile.TemporaryDirectory() as d:
        fname = os.path.join(d, "temp.%s"%ext)
        nx.write_gpickle(data, fname)
        dbfile.import_file(fname)
