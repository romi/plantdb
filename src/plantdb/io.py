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
The ``io`` module of the ROMI ``plantdb`` library contains all functions for reading and writing data to database.

Hereafter we detail the formats and their associated Python types and meanings.

File formats
------------

json
****

Dictionaries or lists, read and written using ``json``.

* Python objects: ``dict``, ``list``
* File extensions: 'json'

toml
****

Dictionaries or lists, read and written using ``toml``.

* Python objects: ``dict``, ``list``
* File extensions: 'toml'

2D image
********

RGB or RGBA image data, read and written using ``imageio``.

* Python objects: ``numpy.ndarray``
* File extensions: 'jpg', 'png'

3D volume
*********

Grayscale or binary volume image data, read and written using ``imageio``.

* Python objects: ``numpy.ndarray``
* File extensions: 'tiff'

Labelled 3D volume
******************

Labelled volume image data, converted to dictionary of 3D (binary) numpy arrays, read and written using ``numpy``.

* Python objects: ``dict`` of 3D ``numpy.ndarray``
* File extensions: 'npz'

Point cloud
***********

Point clouds, read and written using ``open3d``.

* Python object: ``open3d.geometry.PointCloud``
* File extensions: 'ply'

Triangle mesh
*************

Triangular meshes, read and written using ``open3d``.

* Python object: ``open3d.geometry.TriangleMesh``
* File extensions: 'ply'

Voxel grid
**********

Voxel grids, read and written using ``open3d``.

* Python object: ``open3d.geometry.VoxelGrid``
* File extensions: 'ply'

Tree graph
**********

Tree graphs, read and written using ``networkx``.

* Python object: ``networkx.Graph``
* File extensions: 'p'

Pytorch tensor
**************

Trained tensor, read and written using ``torch``.

* Python object: ``torch.tensor``
* File extensions: 'pt'

"""

import tempfile
from pathlib import Path

from plantdb import fsdb
from plantdb.db import File
from plantdb.db import Fileset
from plantdb.db import Scan
from plantdb.log import configure_logger

logger = configure_logger(__name__)


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
    ext = ext.replace('.', '')  # remove potential leading dot from extension
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
    ext = ext.replace('.', '')  # remove potential leading dot from extension
    dbfile.write(toml.dumps(data), ext)
    return


def read_image(dbfile):
    """Reads a 2D image from a ROMI database file.

    Parameters
    ----------
    dbfile : DB.db.File
        The `File` object used to load the associated file.

    Returns
    -------
    numpy.ndarray
        The image as an RGB(A) array.

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
    >>> rng = np.random.default_rng()
    >>> img = np.array(rng.random((5, 5, 3))*255, dtype='uint8')  # an 8bit 5x5 RGB image
    >>> write_image(f, img)
    >>> f = fs.get_file("test_image")
    >>> img2 = read_image(f)
    >>> np.testing.assert_array_equal(img, img2)  # raise an exception if not equal!

    """
    import imageio.v3 as iio
    return iio.imread(dbfile.read_raw())


def write_image(dbfile, data, ext="png"):
    """Writes a 2D image to a ROMI database file.

    Parameters
    ----------
    dbfile : DB.db.File
        The `File` object used to write the associated file.
    data : array like
        The 2D image (RGB array) to save.
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
    >>> rng = np.random.default_rng()
    >>> img = np.array(rng.random((5, 5, 3))*255, dtype='uint8')  # an 8bit 5x5 RGB image
    >>> write_image(f, img)

    """
    import imageio.v3 as iio
    ext = ext.replace('.', '')  # remove potential leading dot from extension
    if ext == "jpg" and len(data.shape) == 3:
        # Remove any alpha channel if JPEG format as it cannot handle it
        data = data[:, :, :3]
    b = iio.imwrite("<bytes>", data, extension=f".{ext}")
    dbfile.write_raw(b, ext)
    return


def read_volume(dbfile, ext="tiff"):
    """Reads a volume image from a ROMI database file.

    Parameters
    ----------
    dbfile : DB.db.File
        The `File` object used to load the associated file.
    ext : str, optional
        File extension, defaults to "tiff".

    Returns
    -------
    numpy.ndarray
        The volume as a 3D array.

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
    >>> rng = np.random.default_rng()
    >>> vol = rng.random((50, 10, 10))
    >>> write_volume(f, vol)
    >>> f = fs.get_file("test_volume")
    >>> vol2 = read_volume(f)
    >>> np.testing.assert_array_equal(vol, vol2)  # raise an exception if not equal!

    """
    import imageio.v3 as iio
    ext = ext.replace('.', '')  # remove potential leading dot from extension
    return iio.imread(dbfile.read_raw(), extension=f".{ext}")


def write_volume(dbfile, data, ext="tiff", compress=True):
    """Writes a volume image to a ROMI database file.

    Parameters
    ----------
    dbfile : DB.db.File
        The `File` object used to write the associated file.
    data : array like
        The 3D array to save as volume image.
    ext : str, optional
        File extension, defaults to "tiff".
    compress : bool, optional
        Indicate if the volume file should be compressed.
        Defaults to ``True``.

    Examples
    --------
    >>> import numpy as np
    >>> from plantdb.fsdb import FSDB
    >>> from plantdb.io import write_volume
    >>> from plantdb.fsdb import Scan, Fileset, File
    >>> from plantdb.fsdb import dummy_db
    >>> db = dummy_db(with_fileset=True)
    >>> db.connect()
    >>> scan = db.get_scan("myscan_001")
    >>> fs = scan.get_fileset("fileset_001")
    >>> vol = np.ones((50, 10, 10))  # stupid volume file with only `1` values
    >>> f_lzw = fs.create_file('test_volume_compress')
    >>> write_volume(f_lzw, vol)
    >>> f = fs.create_file('test_volume')
    >>> write_volume(f, vol, compress=False)
    >>> print(f_lzw.path().stat().st_size)
    15283
    >>> print(f.path().stat().st_size)
    48994

    """
    import imageio.v3 as iio
    import tifffile
    try:
        import imagecodecs
    except ImportError:
        pip_install = "pip install imagecodecs"
        conda_install = "conda install -c conda-forge imagecodecs"
        logger.error(f"Missing 'imagecodecs' library to compress!")
        logger.info(f"Install it with `{pip_install}` or `{conda_install}`.")
        compress = False

    ext = ext.replace('.', '')  # remove potential leading dot from extension
    with tempfile.TemporaryDirectory() as tmpdir:
        fname = Path(tmpdir) / f"temp.{ext}"
        if compress:
            # Use LZW compression with `tifffile.imwrite`:
            tifffile.imwrite(fname, data, compression="LZW")
        else:
            # No compression with `imageio.v3`
            iio.imwrite(fname, data, extension=f".{ext}")
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
        The dictionary of numpy arrays.

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
    >>> rng = np.random.default_rng()
    >>> npz = {f"{i}": rng.random((10, 10, 3)) for i in range(5)}
    >>> write_npz(f, npz)
    >>> f = fs.get_file("test_npz")
    >>> npz2 = read_npz(f)
    >>> np.testing.assert_array_equal(npz["0"], npz2["0"])  # raise an exception if not equal!

    """
    import numpy as np
    b = dbfile.read_raw()
    with tempfile.TemporaryDirectory() as tmpdir:
        fname = Path(tmpdir) / f"temp.npz"
        with fname.open(mode="wb") as fh:
            fh.write(b)
        return dict(np.load(fname))


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
    >>> rng = np.random.default_rng()
    >>> npz = {f"{i}": rng.random((10, 10, 3)) for i in range(5)}
    >>> write_npz(f, npz)

    """
    import numpy as np
    with tempfile.TemporaryDirectory() as tmpdir:
        fname = Path(tmpdir) / f"temp.npz"
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
    ext = ext.replace('.', '')  # remove potential leading dot from extension
    b = dbfile.read_raw()
    with tempfile.TemporaryDirectory() as tmpdir:
        fname = Path(tmpdir) / f"temp.{ext}"
        with fname.open(mode="wb") as fh:
            fh.write(b)
        return io.read_point_cloud(str(fname))


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
    ext = ext.replace('.', '')  # remove potential leading dot from extension
    with tempfile.TemporaryDirectory() as tmpdir:
        fname = Path(tmpdir) / f"temp.{ext}"
        io.write_point_cloud(str(fname), data)
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
    open3d.geometry.TriangleMesh
        The loaded triangular mesh object.
    """
    from open3d import io
    ext = ext.replace('.', '')  # remove potential leading dot from extension
    b = dbfile.read_raw()
    with tempfile.TemporaryDirectory() as tmpdir:
        fname = Path(tmpdir) / f"temp.{ext}"
        with fname.open(mode="wb") as fh:
            fh.write(b)
        return io.read_triangle_mesh(str(fname))


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
    ext = ext.replace('.', '')  # remove potential leading dot from extension
    with tempfile.TemporaryDirectory() as tmpdir:
        fname = Path(tmpdir) / f"temp.{ext}"
        io.write_triangle_mesh(str(fname), data)
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
    open3d.geometry.VoxelGrid
        The loaded point cloud object.
    """
    from open3d import io
    ext = ext.replace('.', '')  # remove potential leading dot from extension
    b = dbfile.read_raw()
    with tempfile.TemporaryDirectory() as tmpdir:
        fname = Path(tmpdir) / f"temp.{ext}"
        with fname.open(mode="wb") as fh:
            fh.write(b)
        return io.read_voxel_grid(str(fname))


def write_voxel_grid(dbfile, data, ext="ply"):
    """Writes a voxel grid to a ROMI database file.

    Parameters
    ----------
    dbfile : DB.db.File
        The `File` object used to write the associated file.
    data : open3d.geometry.VoxelGrid
        The voxel grid object to save.
    ext : str, optional
        File extension, defaults to "ply".
    """
    from open3d import io
    ext = ext.replace('.', '')  # remove potential leading dot from extension
    with tempfile.TemporaryDirectory() as tmpdir:
        fname = Path(tmpdir) / f"temp.{ext}"
        io.write_voxel_grid(str(fname), data)
        dbfile.import_file(fname)
    return


def read_graph(dbfile, ext="p"):
    """Reads a networkx ``Graph`` from a ROMI database file.

    Parameters
    ----------
    dbfile : DB.db.File
        The `File` object used to load the associated file.
    ext : str, optional
        File extension, defaults to "p".

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
    ext = ext.replace('.', '')  # remove potential leading dot from extension
    b = dbfile.read_raw()
    with tempfile.TemporaryDirectory() as tmpdir:
        fname = Path(tmpdir) / f"temp.{ext}"
        with fname.open(mode="wb") as fh:
            fh.write(b)
        with fname.open(mode='rb') as f:
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
    ext = ext.replace('.', '')  # remove potential leading dot from extension
    with tempfile.TemporaryDirectory() as tmpdir:
        fname = Path(tmpdir) / f"temp.{ext}"
        with fname.open(mode='wb') as f:
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
    ext = ext.replace('.', '')  # remove potential leading dot from extension
    b = dbfile.read_raw()
    with tempfile.TemporaryDirectory() as tmpdir:
        fname = Path(tmpdir) / f"temp.{ext}"
        with fname.open(mode="wb") as fh:
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
    ext = ext.replace('.', '')  # remove potential leading dot from extension
    with tempfile.TemporaryDirectory() as tmpdir:
        fname = Path(tmpdir) / f"temp.{ext}"
        torch.save(data, fname)
        dbfile.import_file(fname)
    return


def write_torch(file, data, ext="pt", **kwargs):
    """Writes a torch tensor to a ROMI database file or to a given path.

    Parameters
    ----------
    file : plantdb.db.File or pathlib.Path or str
        If a ``File`` instance, the file will be saved to the associated database/scan/fileset.
        Else, write the `data` to the given file path, ignoring the extension parameter `ext`.
    data : TorchTensor
        The torch tensor object to save.
    ext : str, optional
        File extension, defaults to "pt".
    """
    _writer(file, data, ext, _write_torch, **kwargs)
    return
