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
plantdb.io
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

from plantdb import fsdb
from plantdb.db import DB
from plantdb.db import File
from plantdb.db import Fileset
from plantdb.db import Scan

try:
    import imageio.v3 as iio
except:
    import imageio as iio


def read_json(dbfile):
    """Reads a JSON from a ROMI database file.

    Parameters
    ----------
    dbfile : DB.db.File
        The `File` object used to load the associated file.

    Returns
    -------
    dict
        The deserialized JSON file.

    Example
    -------
    >>> from plantdb.io import read_json, write_json
    >>> from plantdb.fsdb import dummy_db
    >>> db = dummy_db(with_fileset=True)
    >>> db.connect()
    >>> scan = db.get_scan("myscan_001")
    >>> fs = scan.get_fileset("fileset_001")
    >>> f = fs.create_file("test_json")
    >>> data = {"test": False, 'ROMI': 'RObotics for MIcrofarms'}
    >>> write_json(f, data)
    >>> f = fs.get_file("test_json")
    >>> read_json(f)
    {'test': False, 'ROMI': 'RObotics for MIcrofarms'}

    """
    import json
    return json.loads(dbfile.read())


def write_json(dbfile, data, ext="json"):
    """Writes a JSON to a ROMI database file.

    Parameters
    ----------
    dbfile : DB.db.File
        The `File` object used to write the associated file.
    data : dict
        The dictionary to save as a JSON file.
    ext : str, optional
        File extension, defaults to "json".

    Example
    -------
    >>> from plantdb.io import write_json
    >>> from plantdb.fsdb import dummy_db
    >>> db = dummy_db(with_fileset=True)
    >>> db.connect()
    >>> scan = db.get_scan("myscan_001")
    >>> fs = scan.get_fileset("fileset_001")
    >>> f = fs.create_file("test_json")
    >>> data = {"test": False, 'ROMI': 'RObotics for MIcrofarms'}
    >>> write_json(f, data)

    """
    import json
    dbfile.write(json.dumps(data, indent=4), ext)
    return


def read_toml(dbfile):
    """Reads a TOML from a ROMI database file.

    Parameters
    ----------
    dbfile : DB.db.File
        The `File` object used to load the associated file.

    Returns
    -------
    dict
        The deserialized TOML file.

    Example
    -------
    >>> from plantdb.io import read_toml, write_toml
    >>> from plantdb.fsdb import dummy_db
    >>> db = dummy_db(with_fileset=True)
    >>> db.connect()
    >>> scan = db.get_scan("myscan_001")
    >>> fs = scan.get_fileset("fileset_001")
    >>> f = fs.create_file("test_json")
    >>> data = {"test": True, 'ROMI': 'RObotics for MIcrofarms'}
    >>> write_toml(f, data)
    >>> f = fs.get_file("test_json")
    >>> read_toml(f)
    {'test': True}

    """
    import toml
    return toml.loads(dbfile.read())


def write_toml(dbfile, data, ext="toml"):
    """Writes a TOML to a ROMI database file.

    Parameters
    ----------
    dbfile : DB.db.File
        The `File` object used to write the associated file.
    data : dict
        The dictionary to save as a TOML file.
    ext : str, optional
        File extension, defaults to "toml".

    Example
    -------
    >>> from plantdb.io import write_toml
    >>> from plantdb.fsdb import dummy_db
    >>> db = dummy_db(with_fileset=True)
    >>> db.connect()
    >>> scan = db.get_scan("myscan_001")
    >>> fs = scan.get_fileset("fileset_001")
    >>> f = fs.create_file("test_json")
    >>> data = {"test": True, 'ROMI': 'RObotics for MIcrofarms'}
    >>> write_toml(f, data)

    """
    import toml
    dbfile.write(toml.dumps(data), ext)
    return


def read_image(dbfile):
    """Reads an image from a ROMI database file.

    Parameters
    ----------
    dbfile : DB.db.File
        The `File` object used to load the associated file.

    Returns
    -------
    numpy.ndarray
        The image array.

    Example
    -------
    >>> import numpy as np
    >>> from plantdb.io import read_image, write_image
    >>> from plantdb.fsdb import dummy_db
    >>> db = dummy_db(with_fileset=True)
    >>> db.connect()
    >>> scan = db.get_scan("myscan_001")
    >>> fs = scan.get_fileset("fileset_001")
    >>> f = fs.create_file("test_image")
    >>> img = np.array(np.random.random((5, 5, 3))*255, dtype='uint8')  # an 8bit 5x5 RGB image
    >>> write_image(f, img)
    >>> f = fs.get_file("test_image")
    >>> img2 = read_image(f)
    >>> np.testing.assert_array_equal(img, img2)  # raise an exception if not equal!

    """
    return iio.imread(dbfile.read_raw())


def write_image(dbfile, data, ext="png"):
    """Writes an image to a ROMI database file.

    Parameters
    ----------
    dbfile : DB.db.File
        The `File` object used to write the associated file.
    data : array like
        The array to save as an image.
    ext : {'png', 'jpeg', 'tiff'}, optional
        File extension, defaults to "png".

    Example
    -------
    >>> import numpy as np
    >>> from plantdb.io import write_image
    >>> from plantdb.fsdb import dummy_db
    >>> db = dummy_db(with_fileset=True)
    >>> db.connect()
    >>> scan = db.get_scan("myscan_001")
    >>> fs = scan.get_fileset("fileset_001")
    >>> f = fs.create_file("test_image")
    >>> img = np.array(np.random.random((5, 5, 3))*255, dtype='uint8')  # an 8bit 5x5 RGB image
    >>> write_image(f, img)

    """
    if ext == "jpg" and len(data.shape) == 3:
        data = data[:, :, :3]
    b = iio.imwrite("<bytes>", data, format=ext)
    dbfile.write_raw(b, ext)
    return


def read_volume(dbfile, ext="npz"):
    """Reads a volume from a ROMI database file.

    Parameters
    ----------
    dbfile : DB.db.File
        The `File` object used to load the associated file.
    ext : str, optional
        File extension, defaults to "npz".

    Returns
    -------
    numpy.ndarray
        The volume array.

    Examples
    --------
    >>> import numpy as np
    >>> from plantdb.io import read_volume, write_volume
    >>> from plantdb.fsdb import dummy_db
    >>> db = dummy_db(with_fileset=True)
    >>> db.connect()
    >>> scan = db.get_scan("myscan_001")
    >>> fs = scan.get_fileset("fileset_001")
    >>> f = fs.create_file('test_volume')
    >>> vol = np.random.rand(50, 10, 10)
    >>> write_volume(f, vol)
    >>> f = fs.get_file("test_volume")
    >>> vol2 = read_volume(f)
    >>> np.testing.assert_array_equal(vol, vol2)  # raise an exception if not equal!

    """
    try:
        # Try old `volread` method:
        arr = iio.volread(dbfile.read_raw(), format=ext)
    except AttributeError:
        # Use v3 method:
        arr = iio.imread(dbfile.read_raw(), extension=ext if ext.startswith('.') else f".{ext}")
    return arr


def write_volume(dbfile, data, ext="npz"):
    """Writes a volume to a ROMI database file.

    Parameters
    ----------
    dbfile : DB.db.File
        The `File` object used to write the associated file.
    data : array like
        The 3D array to save as volume. 
    ext : str, optional
        File extension, defaults to "npz".

    Examples
    --------
    >>> import numpy as np
    >>> from plantdb.fsdb import FSDB
    >>> from plantdb.io import write_volume
    >>> from plantdb.fsdb import Scan, Fileset, File
    >>> from plantdb.fsdb import dummy_db
    >>> db = dummy_db()
    >>> # Example #1: Initialize a `Scan` object using an `FSBD` object:
    >>> scan = db.get_scan('007', create=True)
    >>> fs = scan.get_fileset('volume', create=True)
    >>> f = fs.get_file('test_volume', create=True)
    >>> write_volume(f, np.random.rand(50, 10, 10))

    """
    import numpy as np
    with tempfile.TemporaryDirectory() as d:
        fname = os.path.join(d, "temp.npz")
        try:
            # Try old `volwrite` method:
            iio.volwrite(fname, data, format=ext)
        except AttributeError:
            # Use v3 method:
            iio.imwrite(fname, data, extension=ext if ext.startswith('.') else f".{ext}")
        dbfile.import_file(fname)
    return


def read_npz(dbfile):
    """Reads a dictionary of arrays from an '.npz' ROMI database file.

    Parameters
    ----------
    dbfile : DB.db.File
        The `File` object used to load the associated file.

    Returns
    -------
    dict of numpy.ndarray
        The uncompressed numpy array.

    Examples
    --------
    >>> import numpy as np
    >>> from plantdb.io import read_npz, write_npz
    >>> from plantdb.fsdb import dummy_db
    >>> db = dummy_db(with_fileset=True)
    >>> db.connect()
    >>> scan = db.get_scan("myscan_001")
    >>> fs = scan.get_fileset("fileset_001")
    >>> f = fs.create_file('test_npz')
    >>> npz = {f"{i}": np.random.rand(10, 10) for i in range(5)}
    >>> write_npz(f, npz)
    >>> f = fs.get_file("test_npz")
    >>> npz2 = read_npz(f)
    >>> np.testing.assert_array_equal(npz["0"], npz2["0"])  # raise an exception if not equal!

    """
    import numpy as np
    b = dbfile.read_raw()
    with tempfile.TemporaryDirectory() as d:
        fname = os.path.join(d, "temp.npz")
        with open(fname, "wb") as fh:
            fh.write(b)
        return np.load(fname)


def write_npz(dbfile, data):
    """Writes a dictionary of arrays from an '.npz' ROMI database file.

    Parameters
    ----------
    dbfile : DB.db.File
        The `File` object used to write the associated file.
    data : dict of numpy.ndarray
        A dictionary of arrays to save as a single compressed '.npz' file.

    Examples
    --------
    >>> import numpy as np
    >>> from plantdb.io import read_npz, write_npz
    >>> from plantdb.fsdb import dummy_db
    >>> db = dummy_db(with_fileset=True)
    >>> db.connect()
    >>> scan = db.get_scan("myscan_001")
    >>> fs = scan.get_fileset("fileset_001")
    >>> f = fs.create_file('test_npz')
    >>> npz = {f"{i}": np.random.rand(10, 10) for i in range(5)}
    >>> write_npz(f, npz)

    """
    import numpy as np
    with tempfile.TemporaryDirectory() as d:
        fname = os.path.join(d, "temp.npz")
        np.savez_compressed(fname, **data)
        dbfile.import_file(fname)
    return


def read_point_cloud(dbfile, ext="ply"):
    """Reads a point cloud from a ROMI database file.

    Parameters
    ----------
    dbfile : DB.db.File
        The `File` object used to load the associated file.
    ext : str, optional
        File extension, defaults to "ply".

    Returns
    -------
    open3d.geometry.PointCloud
        The loaded point cloud object.

    Examples
    --------
    >>> import open3d as o3d
    >>> import numpy as np
    >>> from plantdb.io import read_point_cloud, write_point_cloud
    >>> from plantdb.fsdb import dummy_db
    >>> db = dummy_db(with_fileset=True)
    >>> db.connect()
    >>> scan = db.get_scan("myscan_001")
    >>> fs = scan.get_fileset("fileset_001")
    >>> f = fs.create_file('test_npz')
    >>> pcd = o3d.geometry.PointCloud()
    >>> pcd.points = o3d.utility.Vector3dVector(np.array([[1, 2, 3]]))
    >>> write_point_cloud(f, pcd)
    >>> f = fs.get_file('test_npz')
    >>> pcd = read_point_cloud(f)
    >>> print(type(pcd))
    <class 'open3d.cuda.pybind.geometry.PointCloud'>
    >>> print(np.asarray(pcd.points))
    [[1. 2. 3.]]

    """
    from open3d import io
    b = dbfile.read_raw()
    with tempfile.TemporaryDirectory() as d:
        fname = os.path.join(d, "temp.%s" % ext)
        with open(fname, "wb") as fh:
            fh.write(b)
        return io.read_point_cloud(fname)


def write_point_cloud(dbfile, data, ext="ply"):
    """Writes a point cloud to a ROMI database file.

    Parameters
    ----------
    dbfile : DB.db.File
        The `File` object used to write the associated file.
    data : open3d.geometry.PointCloud
        The point cloud object to save.
    ext : str, optional
        File extension, defaults to "ply".

    Examples
    --------
    >>> import open3d as o3d
    >>> import numpy as np
    >>> from plantdb.io import write_point_cloud
    >>> from plantdb.fsdb import dummy_db
    >>> db = dummy_db(with_fileset=True)
    >>> db.connect()
    >>> scan = db.get_scan("myscan_001")
    >>> fs = scan.get_fileset("fileset_001")
    >>> f = fs.create_file('test_npz')
    >>> pcd = o3d.geometry.PointCloud()
    >>> pcd.points = o3d.utility.Vector3dVector(np.array([[1, 2, 3]]))
    >>> write_point_cloud(f, pcd)

    """
    from open3d import io
    with tempfile.TemporaryDirectory() as d:
        fname = os.path.join(d, "temp.%s" % ext)
        io.write_point_cloud(fname, data)
        dbfile.import_file(fname)
    return


def read_triangle_mesh(dbfile, ext="ply"):
    """Reads a triangular mesh from a ROMI database file.

    Parameters
    ----------
    dbfile : DB.db.File
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
    """Writes a triangular mesh to a ROMI database file.

    Parameters
    ----------
    dbfile : DB.db.File
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
    return


def read_voxel_grid(dbfile, ext="ply"):
    """Reads a voxel grid from a ROMI database file.

    Parameters
    ----------
    dbfile : DB.db.File
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
    """Writes a voxel grid to a ROMI database file.

    Parameters
    ----------
    dbfile : DB.db.File
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
    return


def read_graph(dbfile, ext="p"):
    """Reads a networkx ``Graph`` from a ROMI database file.

    Parameters
    ----------
    dbfile : DB.db.File
        The `File` object used to load the associated file.
    ext : str, optional
        File extension, defaults to "ply".

    Returns
    -------
    networkx.Graph
        The loaded (tree) graph object.

    Example
    -------
    >>> import networkx as nx
    >>> from plantdb.io import read_graph, write_graph
    >>> from plantdb.fsdb import dummy_db
    >>> db = dummy_db(with_fileset=True)
    >>> db.connect()
    >>> scan = db.get_scan("myscan_001")
    >>> fs = scan.get_fileset("fileset_001")
    >>> f = fs.create_file("test_nx_graph")
    >>> g = nx.path_graph(4)
    >>> print(g)
    Graph with 4 nodes and 3 edges
    >>> write_graph(f, g)
    >>> f = fs.get_file("test_nx_graph")
    >>> g2 = read_graph(f)
    >>> print(g2)
    Graph with 4 nodes and 3 edges

    """
    import pickle
    b = dbfile.read_raw()
    with tempfile.TemporaryDirectory() as d:
        fname = os.path.join(d, "temp.%s" % ext)
        with open(fname, "wb") as fh:
            fh.write(b)
        with open(fname, 'rb') as f:
            G = pickle.load(f)
        return G


def write_graph(dbfile, data, ext="p"):
    """Writes a networkx ``Graph`` to a ROMI database file.

    Parameters
    ----------
    dbfile : DB.db.File
        The `File` object used to write the associated file.
    data : networkx.Graph
        The (tree) graph object to save.
    ext : str, optional
        File extension, defaults to "p".

    Example
    -------
    >>> import networkx as nx
    >>> from plantdb.io import write_graph
    >>> from plantdb.fsdb import dummy_db
    >>> db = dummy_db(with_fileset=True)
    >>> db.connect()
    >>> scan = db.get_scan("myscan_001")
    >>> fs = scan.get_fileset("fileset_001")
    >>> f = fs.create_file("test_nx_graph")
    >>> g = nx.path_graph(4)
    >>> write_graph(f, g)

    """
    import pickle
    with tempfile.TemporaryDirectory() as d:
        fname = os.path.join(d, "temp.%s" % ext)
        with open(fname, 'wb') as f:
            pickle.dump(data, f, pickle.HIGHEST_PROTOCOL)
        dbfile.import_file(fname)
    return


def read_torch(dbfile, ext="pt"):
    """Reads torch tensor from a ROMI database file.

    Parameters
    ----------
    dbfile : DB.db.File
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
    """Writes point cloud to a ROMI database file.

    Parameters
    ----------
    dbfile : DB.db.File
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
    return


def to_file(dbfile: File, path: str):
    """Helper to write a `dbfile` to a file in the filesystem. """
    b = dbfile.read_raw()
    with open(path, "wb") as fh:
        fh.write(b)
    return


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
