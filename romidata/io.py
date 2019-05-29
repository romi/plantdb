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
    return imageio.volread(dbfile.read_raw())

def write_volume(dbfile, data, ext="tiff"):
    """Writes volume to a DB file.
    Parameters
    __________
    dbfile : db.File
    data : 3D numpy array
    ext : str
        file extension (defaults to "tiff").
    """
    b = imageio.volwrite(imageio.RETURN_BYTES, data, format=ext)
    dbfile.write_raw(b, ext)

def read_point_cloud(dbfile):
    """Reads point cloud from a DB file.
    Parameters
    __________
    dbfile : db.File

    Returns
    _______
    PointCloud
    """
    b = dbfile.read_raw()
    with tempfile.NamedTemporaryFile() as fh:
        fh.write(b)
        fh.close()
        return open3d.io.read_point_cloud(fh.name)

def write_point_cloud(dbfile, data, ext="ply"):
    """Writes point cloud to a DB file.
    Parameters
    __________
    dbfile : db.File
    data : PointCloud
    ext : str
        file extension (defaults to "ply").
    """
    with tempfile.NamedTemporaryFile() as fh:
        fh.close()
        open3d.write_point_cloud("temp.%s"%ext, data)
        dbfile.import_file(fh.name)

def read_triangle_mesh(dbfile):
    """Reads point cloud from a DB file.
    Parameters
    __________
    dbfile : db.File

    Returns
    _______
    TriangleMesh
    """
    b = dbfile.read_raw()
    with tempfile.NamedTemporaryFile() as fh:
        fh.write(b)
        fh.close()
        return open3d.io.read_triangle_mesh(fh.name)

def write_triangle_mesh(dbfile, data, ext="ply"):
    """Writes point cloud to a DB file.
    Parameters
    __________
    dbfile : db.File
    data : PointCloud
    ext : str
        file extension (defaults to "ply").
    """
    with tempfile.NamedTemporaryFile() as fh:
        fh.close()
        open3d.io.write_triangle_mesh("temp.%s"%ext, data)
        dbfile.import_file(fh.name)

