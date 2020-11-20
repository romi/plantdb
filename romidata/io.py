#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# romidata - Data handling tools for the ROMI project
#
# Copyright (C) 2018-2019 Sony Computer Science Laboratories
# Authors: D. Colliaux, T. Wintz, P. Hanappe
#
# This file is part of romidata.
#
# romidata is free software: you can redistribute it
# and/or modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation, either
# version 3 of the License, or (at your option) any later version.
#
# romidata is distributed in the hope that it will be
# useful, but WITHOUT ANY WARRANTY; without even the implied
# warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with romidata.  If not, see
# <https://www.gnu.org/licenses/>.
# ------------------------------------------------------------------------------

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
* File extensions: 'ply'

Triangle  Meshes
****************

* Python object: open3d.geometry.TriangleMesh
* File extensions: 'ply'

"""

import os
import tempfile

from romidata import fsdb
from romidata.db import DB
from romidata.db import File
from romidata.db import Fileset
from romidata.db import Scan


def read_json(dbfile):
    """Reads json from a ROMI database file.

    Parameters
    ----------
    dbfile : db.File
        The `File` object used to load the associated file.

    Returns
    -------
    dict
        The deserialized JSON file.
    """
    import json
    return json.loads(dbfile.read())


def write_json(dbfile, data, ext="json"):
    """Writes json to a DB file.

    Parameters
    ----------
    dbfile : db.File
        The `File` object used to write the associated file.
    data : dict
        The dictionary to save as a JSON file.
    ext : str, optional
        File extension, defaults to "json".
    """
    import json
    dbfile.write(json.dumps(data, indent=4), ext)


def read_toml(dbfile):
    """Reads toml from a DB file.

    Parameters
    ----------
    dbfile : db.File
        The `File` object used to load the associated file.

    Returns
    -------
    dict
        The deserialized TOML file.
    """
    import toml
    return toml.loads(dbfile.read())


def write_toml(dbfile, data, ext="toml"):
    """Writes toml to a DB file.

    Parameters
    ----------
    dbfile : db.File
        The `File` object used to write the associated file.
    data : dict
        The dictionary to save as a TOML file.
    ext : str, optional
        File extension, defaults to "toml".
    """
    import toml
    dbfile.write(toml.dumps(data), ext)


def read_image(dbfile):
    """Reads image from a DB file.

    Parameters
    ----------
    dbfile : db.File
        The `File` object used to load the associated file.

    Returns
    -------
    numpy.ndarray
        The image array.
    """
    import imageio
    return imageio.imread(dbfile.read_raw())


def write_image(dbfile, data, ext="png"):
    """Writes image to a DB file.

    Parameters
    ----------
    dbfile : db.File
        The `File` object used to write the associated file.
    data : array like
        The array to save as an image.
    ext : {'png', 'jpeg', 'tiff'}, optional
        File extension, defaults to "png".
    """
    import imageio
    if ext == "jpg" and len(data.shape) == 3:
        data = data[:, :, :3]
    b = imageio.imwrite(imageio.RETURN_BYTES, data, format=ext)
    dbfile.write_raw(b, ext)


def read_volume(dbfile, ext="npz"):
    """Reads volume from a DB file.

    Parameters
    ----------
    dbfile : db.File
        The `File` object used to load the associated file.
    ext : str, optional
        File extension, defaults to "npz".

    Returns
    -------
    np.ndarray
        The volume array.
    """
    import imageio
    return imageio.volread(dbfile.read_raw(), format=ext)


def write_volume(dbfile, data, ext="npz"):
    """Writes volume to a DB file.

    Parameters
    ----------
    dbfile : db.File
        The `File` object used to write the associated file.
    data : array like
        The 3D array to save as volume. 
    ext : str, optional
        File extension, defaults to "npz".
    """
    import imageio
    b = imageio.volwrite(imageio.RETURN_BYTES, data, format=ext)
    dbfile.write_raw(b, ext)


def read_npz(dbfile):
    """Reads npz from a DB file.

    Parameters
    ----------
    dbfile : db.File
        The `File` object used to load the associated file.

    Returns
    -------
    numpy.ndarray
        The uncompressed numpy array.
    """
    import numpy as np
    b = dbfile.read_raw()
    with tempfile.TemporaryDirectory() as d:
        fname = os.path.join(d, "temp.npz")
        with open(fname, "wb") as fh:
            fh.write(b)
        return np.load(fname)


def write_npz(dbfile, data):
    """Writes npz to a DB file.

    Parameters
    ----------
    dbfile : db.File
        The `File` object used to write the associated file.
    data : array like
        A 3D array to save as volume.
    """
    import numpy as np
    with tempfile.TemporaryDirectory() as d:
        fname = os.path.join(d, "temp.npz")
        np.savez_compressed(fname, **data)
        dbfile.import_file(fname)


def read_point_cloud(dbfile, ext="ply"):
    """Reads point cloud from a DB file.

    Parameters
    ----------
    dbfile : db.File
        The `File` object used to load the associated file.
    ext : str, optional
        File extension, defaults to "ply".

    Returns
    -------
    open3d.geometry.PointCloud
        The loaded point cloud object. 
    """
    from open3d import io
    b = dbfile.read_raw()
    with tempfile.TemporaryDirectory() as d:
        fname = os.path.join(d, "temp.%s" % ext)
        with open(fname, "wb") as fh:
            fh.write(b)
        return io.read_point_cloud(fname)


def write_point_cloud(dbfile, data, ext="ply"):
    """Writes point cloud to a DB file.

    Parameters
    ----------
    dbfile : db.File
        The `File` object used to write the associated file.
    data : open3d.geometry.PointCloud
        The point cloud object to save.
    ext : str, optional
        File extension, defaults to "ply".
    """
    from open3d import io
    with tempfile.TemporaryDirectory() as d:
        fname = os.path.join(d, "temp.%s" % ext)
        io.write_point_cloud(fname, data)
        dbfile.import_file(fname)


def read_triangle_mesh(dbfile, ext="ply"):
    """Reads triangular mesh from a DB file.

    Parameters
    ----------
    dbfile : db.File
        The `File` object used to load the associated file.
    ext : str, optional
        File extension, defaults to "ply".

    Returns
    -------
    open3d.geometry.PointCloud
        The loaded point cloud object.
    """
    from open3d import io
    b = dbfile.read_raw()
    with tempfile.TemporaryDirectory() as d:
        fname = os.path.join(d, "temp.%s" % ext)
        with open(fname, "wb") as fh:
            fh.write(b)
        return io.read_triangle_mesh(fname)


def write_triangle_mesh(dbfile, data, ext="ply"):
    """Writes triangular mesh to a DB file.

    Parameters
    ----------
    dbfile : db.File
        The `File` object used to write the associated file.
    data : open3d.geometry.TriangleMesh
        The triangular mesh object to save.
    ext : str, optional
        File extension, defaults to "ply".
    """
    from open3d import io
    with tempfile.TemporaryDirectory() as d:
        fname = os.path.join(d, "temp.%s" % ext)
        io.write_triangle_mesh(fname, data)
        dbfile.import_file(fname)


def read_voxel_grid(dbfile, ext="ply"):
    """Reads voxel grid from a DB file.

    Parameters
    ----------
    dbfile : db.File
        The `File` object used to load the associated file.
    ext : str, optional
        File extension, defaults to "ply".

    Returns
    -------
    open3d.geometry.PointCloud
        The loaded point cloud object.
    """
    from open3d import io
    b = dbfile.read_raw()
    with tempfile.TemporaryDirectory() as d:
        fname = os.path.join(d, "temp.%s" % ext)
        with open(fname, "wb") as fh:
            fh.write(b)
        return io.read_voxel_grid(fname)


def write_voxel_grid(dbfile, data, ext="ply"):
    """Writes voxel grid to a DB file.

    Parameters
    ----------
    dbfile : db.File
        The `File` object used to write the associated file.
    data : open3d.geometry.PointCloud
        The point cloud object to save.
    ext : str, optional
        File extension, defaults to "ply".
    """
    from open3d import io
    with tempfile.TemporaryDirectory() as d:
        fname = os.path.join(d, "temp.%s" % ext)
        io.write_voxel_grid(fname, data)
        dbfile.import_file(fname)


def read_graph(dbfile, ext="p"):
    """Reads treex tree from a DB file.

    Parameters
    ----------
    dbfile : db.File
        The `File` object used to load the associated file.
    ext : str, optional
        File extension, defaults to "ply".

    Returns
    -------
    networkx.Graph
        The loaded tree graph object.
    """
    import networkx as nx
    b = dbfile.read_raw()
    with tempfile.TemporaryDirectory() as d:
        fname = os.path.join(d, "temp.%s" % ext)
        with open(fname, "wb") as fh:
            fh.write(b)
        return nx.read_gpickle(fname)


def write_graph(dbfile, data, ext="p"):
    """Writes treex tree to a DB file.

    Parameters
    ----------
    dbfile : db.File
        The `File` object used to write the associated file.
    data : treex.tree.Tree
        The tree graph object to save.
    ext : str, optional
        File extension, defaults to "p".
    """
    import networkx as nx
    with tempfile.TemporaryDirectory() as d:
        fname = os.path.join(d, "temp.%s" % ext)
        nx.write_gpickle(data, fname)
        dbfile.import_file(fname)


def read_torch(dbfile, ext="pt"):
    """Reads torch tensor from a DB file.

    Parameters
    ----------
    dbfile : db.File
        The `File` object used to load the associated file.
    ext : str, optional
        File extension, defaults to "pt".

    Returns
    -------
    Torch.Tensor
        The loaded tensor object.
    """
    import torch
    b = dbfile.read_raw()
    with tempfile.TemporaryDirectory() as d:
        fname = os.path.join(d, "temp.%s" % ext)
        with open(fname, "wb") as fh:
            fh.write(b)
        return torch.load(fname)


def write_torch(dbfile, data, ext="pt"):
    """Writes point cloud to a DB file.

    Parameters
    ----------
    dbfile : db.File
        The `File` object used to load the associated file.
    data : TorchTensor
        The torch tensor object to save.
    ext : str, optional
        File extension, defaults to "pt".
    """
    import torch
    with tempfile.TemporaryDirectory() as d:
        fname = os.path.join(d, "temp.%s" % ext)
        torch.save(data, fname)
        dbfile.import_file(fname)


def to_file(dbfile: File, path: str):
    """Helper to write a `dbfile` to a file in the filesystem. """
    b = dbfile.read_raw()
    with open(path, "wb") as fh:
        fh.write(b)


def dbfile_from_local_file(path: str):
    """Creates a temporary (*i.e.* not in a DB) ``File`` object from a local file. """
    dirname, fname = os.path.split(path)
    id = os.path.splitext(fname)[0]
    # Initialise the `DB` abstract class:
    db = DB()
    db.basedir = ""
    # Initialize a `Scan` instance:
    scan = Scan(db, "")
    # Initialize a `Fileset` instance:
    fileset = Fileset(db, scan, dirname)
    # Initialize a `File` instance & return it:
    f = fsdb.File(db=db, fileset=fileset, id=id)
    f.filename = fname
    f.metadata = None
    return f


def tmpdir_from_fileset(fileset: Fileset):
    """Creates a temporary directory (*i.e.* not in a DB) to host the ``Fileset`` object and write files. """
    tmpdir = tempfile.TemporaryDirectory()
    for f in fileset.get_files():
        filepath = os.path.join(tmpdir.name, f.filename)
        to_file(f, filepath)
    return tmpdir
