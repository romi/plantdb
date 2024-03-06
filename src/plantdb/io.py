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

## json
Dictionaries or lists, read and written using ``json``.

* Python objects: ``dict``, ``list``
* File extensions: 'json'

## toml
Dictionaries or lists, read and written using ``toml``.

* Python objects: ``dict``, ``list``
* File extensions: 'toml'

## 2D image

RGB or RGBA image data, read and written using ``imageio``.

* Python objects: ``numpy.ndarray``
* File extensions: 'jpg', 'png'

## 3D volume
Grayscale or binary volume image data, read and written using ``imageio``.

* Python objects: ``numpy.ndarray``
* File extensions: 'tiff'

## Labelled 3D volume
Labelled volume image data, converted to dictionary of 3D (binary) numpy arrays, read and written using ``numpy``.

* Python objects: ``dict`` of 3D ``numpy.ndarray``
* File extensions: 'npz'

## Point cloud
Point clouds, read and written using ``open3d``.

* Python object: ``open3d.geometry.PointCloud``
* File extensions: 'ply'

## Triangle mesh
Triangular meshes, read and written using ``open3d``.

* Python object: ``open3d.geometry.TriangleMesh``
* File extensions: 'ply'

## Voxel grid
Voxel grids, read and written using ``open3d``.

* Python object: ``open3d.geometry.VoxelGrid``
* File extensions: 'ply'

## Tree graph
Tree graphs, read and written using ``networkx``.

* Python object: ``networkx.Graph``
* File extensions: 'p'

## Pytorch tensor
Trained tensor, read and written using ``torch``.

* Python object: ``torch.tensor``
* File extensions: 'pt'

"""

import tempfile
from pathlib import Path

import plantdb.db
from plantdb.log import configure_logger

logger = configure_logger(__name__)


def _reader(file, reader, **kwargs):
    """Read the data from given file, can be from a database or a path.

    Parameters
    ----------
    file : plantdb.db.File or pathlib.Path or str
        A ``File`` instance or file path, to read from.
    reader : function
        The function to use to read the data.

    Returns
    -------
    Any
        The loaded data.
    """
    # Get path to file if in a database:
    if isinstance(file, plantdb.fsdb.File):
        file = file.path()
    # Load the file with selected reader:
    with open(file, 'r') as f:
        data = reader(f, **kwargs)
    return data


def _write_db_file(dbfile, data, ext, writer, **kwargs):
    """Write the data in the database to the associated database/scan/fileset.

    Parameters
    ----------
    dbfile : plantdb.db.File
        The ``File`` instance to save the data to.
    data : Any
        The data to save.
    ext : str
        The file extension to use.
    writer : function
        The function to use to write the `data`.
    """
    if ext.startswith('.'):
        ext = ext[1:]  # remove potential leading dot from extension
    with tempfile.NamedTemporaryFile(suffix=f".{ext}") as fname:
        writer(fname.name, data, **kwargs)
        dbfile.import_file(fname.name)
    return


def _writer(file, data, ext, writer, **kwargs):
    """Write the data as a file or to a database.

    Parameters
    ----------
    file : plantdb.db.File or pathlib.Path or str
        If a ``File`` instance, the file will be saved to the associated database/scan/fileset.
        Else, write the point cloud data to the given file path, ignoring the extension parameter `ext`.
    data : Any
        The data to save.
    ext : str
        The file extension to use.
    writer : function
        The function to use to write the `data`.
    """
    if isinstance(file, plantdb.db.File):
        _write_db_file(file, data, ext, writer, **kwargs)
    else:
        writer(file, data, **kwargs)
    return


def read_json(file, **kwargs):
    """Reads a JSON from a ROMI database file or a path.

    Parameters
    ----------
    file : plantdb.db.File or pathlib.Path or str
        A ``File`` instance or file path, to read from.

    Returns
    -------
    dict
        The deserialized JSON file.

    Examples
    --------
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
    # Get path to file if in a database:
    if isinstance(file, plantdb.fsdb.File):
        file = file.path()
    # Load the file:
    with open(file, mode='r') as f:
        data = json.loads(f.read(), **kwargs)
    return data


def _write_json(fname, data, **kwargs):
    """Writes a JSON to a file.

    Parameters
    ----------
    fname : pathlib.Path or str
        The file path to use to save the `data`.
    data : Any
        The data object to save.
    """
    import json
    if 'indent' not in kwargs:
        kwargs.update({'indent': 2})
    with open(fname, 'w') as f:
        f.writelines(json.dumps(data, **kwargs))
    return


def write_json(file, data, ext="json", **kwargs):
    """Writes a JSON to a ROMI database file or to a given path.

    Parameters
    ----------
    file : plantdb.db.File or pathlib.Path or str
        If a ``File`` instance, the file will be saved to the associated database/scan/fileset.
        Else, write the `data` to the given file path, ignoring the extension parameter `ext`.
    data : dict
        The dictionary to save as a JSON file.
    ext : str, optional
        File extension, defaults to "json".

    Examples
    --------
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
    _writer(file, data, ext, _write_json, **kwargs)
    return


def read_toml(file, **kwargs):
    """Reads a TOML from a ROMI database file or a path.

    Parameters
    ----------
    file : plantdb.db.File or pathlib.Path or str
        A ``File`` instance or file path, to read from.

    Returns
    -------
    dict
        The deserialized TOML file.

    Examples
    --------
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
    # Get path to file if in a database:
    if isinstance(file, plantdb.fsdb.File):
        file = file.path()
    # Load the file:
    with open(file, mode='r') as f:
        data = toml.loads(f.read(), **kwargs)
    return data


def _write_toml(fname, data, **kwargs):
    """Writes a TOML to a file.

    Parameters
    ----------
    fname : pathlib.Path or str
        The file path to use to save the `data`.
    data : Any
        The data object to save.
    """
    import toml
    with open(fname, 'w') as f:
        f.writelines(toml.dumps(data, **kwargs))
    return


def write_toml(file, data, ext="toml", **kwargs):
    """Writes a TOML to a ROMI database file or to a given path.

    Parameters
    ----------
    file : plantdb.db.File or pathlib.Path or str
        If a ``File`` instance, the file will be saved to the associated database/scan/fileset.
        Else, write the `data` to the given file path, ignoring the extension parameter `ext`.
    data : dict
        The dictionary to save as a TOML file.
    ext : str, optional
        File extension, defaults to "toml".

    Examples
    --------
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
    _writer(file, data, ext, _write_toml, **kwargs)
    return


def read_image(file, **kwargs):
    """Reads a 2D image from a ROMI database file or a path.

    Parameters
    ----------
    file : plantdb.db.File or pathlib.Path or str
        A ``File`` instance or file path, to read from.

    Returns
    -------
    numpy.ndarray
        The image as an RGB(A) array.

    Examples
    --------
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
    # Get path to file if in a database:
    if isinstance(file, plantdb.fsdb.File):
        file = file.path()
    return iio.imread(file, **kwargs)


def _write_image(fname, data, **kwargs):
    """Writes a 2D image to a file.

    Parameters
    ----------
    fname : pathlib.Path or str
        The file path to use to save the `data`.
    data : array like
        The 2D image, RGB(A) array to save.

    Examples
    --------
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
    fname = Path(fname)
    ext = fname.suffix[1:]  # get the extension, without the leading dot
    # Override extension if specified in keyword-argument:
    kwargs['extension'] = f".{ext}"
    # Remove any alpha channel if JPEG format as it cannot handle it
    has_alpha_channel = data.ndim == 3 and data.shape[2] > 3
    if ext in ["jpg", "jpeg"] and has_alpha_channel:
        data = data[:, :, :3]
    iio.imwrite(fname, data, **kwargs)
    return


def write_image(file, data, ext="png", **kwargs):
    """Writes a 2D image to a ROMI database file or to a given path.

    Parameters
    ----------
    file : plantdb.db.File or pathlib.Path or str
        If a ``File`` instance, the file will be saved to the associated database/scan/fileset.
        Else, write the `data` to the given file path, ignoring the extension parameter `ext`.
    data : array like
        The 2D image (RGB array) to save.
    ext : {'png', 'jpeg', 'tiff'}, optional
        File extension, defaults to "png".

    Examples
    --------
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
    _writer(file, data, ext, _write_image, **kwargs)
    return


def read_volume(file, ext="tiff", **kwargs):
    """Reads a volume image from a ROMI database file or a path.

    Parameters
    ----------
    file : plantdb.db.File or pathlib.Path or str
        A ``File`` instance or file path, to read from.
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
    # Get path to file if in a database:
    if isinstance(file, plantdb.fsdb.File):
        file = file.path()
    return iio.imread(file, **kwargs)


def _write_volume(fname, data, **kwargs):
    """Writes a volume image to a file.

    Parameters
    ----------
    fname : pathlib.Path or str
        The file path to use to save the `data`.
    data : Any
        The data object to save.
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
        kwargs['compress'] = False

    fname = Path(fname)
    ext = fname.suffix  # get the extension
    # Override extension if specified in keyword-argument:
    kwargs['extension'] = ext
    if kwargs.get('compress', False):
        # By default, use LZW compression with `tifffile.imwrite`:
        if 'compression' not in kwargs:
            kwargs['compression'] = 'LZW'
        tifffile.imwrite(fname, data, **kwargs)
    else:
        kwargs.pop('compress', None)
        # No compression with `imageio.v3`
        iio.imwrite(fname, data, **kwargs)
    return


def write_volume(file, data, ext="tiff", **kwargs):
    """Writes a volume image to a ROMI database file.

    Parameters
    ----------
    file : plantdb.db.File
        The `File` object used to write the associated file.
    data : array like
        The 3D array to save as volume image.
    ext : str, optional
        File extension, defaults to "tiff".

    Other Parameters
    ----------------
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
    _writer(file, data, ext, _write_volume, **kwargs)
    return


def read_npz(file, **kwargs):
    """Reads a dictionary of arrays from a ROMI database file or a path.

    Parameters
    ----------
    file : plantdb.db.File or pathlib.Path or str
        A ``File`` instance or file path, to read from.

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
    # Get path to file if in a database:
    if isinstance(file, plantdb.fsdb.File):
        file = file.path()
    return dict(np.load(file, **kwargs))


def _write_npz(fname, data, **kwargs):
    """Writes a dictionary of arrays to a file.

    Parameters
    ----------
    fname : pathlib.Path or str
        The file path to use to save the `data`.
    data : Any
        The data object to save.
    """
    import numpy as np
    np.savez_compressed(fname, **data)
    return


def write_npz(file, data, **kwargs):
    """Writes a dictionary of arrays to a ROMI database file or to a given path.

    Parameters
    ----------
    dbfile : plantdb.db.File or pathlib.Path or str
        If a ``File`` instance, the file will be saved to the associated database/scan/fileset.
        Else, write the `data` to the given file path, ignoring the extension parameter `ext`.
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
    _writer(file, data, 'npz', _write_npz, **kwargs)
    return


def read_point_cloud(file, **kwargs):
    """Reads a point cloud from a ROMI database file or a path.

    Parameters
    ----------
    file : plantdb.db.File or pathlib.Path or str
        A ``File`` instance or file path, to read from.

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
    >>> write_point_cloud(f,pcd)
    >>> f = fs.get_file('test_npz')
    >>> pcd = read_point_cloud(f)
    >>> print(type(pcd))
    <class 'open3d.cuda.pybind.geometry.PointCloud'>
    >>> print(np.asarray(pcd.points))
    [[1. 2. 3.]]

    """
    from open3d import io
    # Get path to file if in a database:
    if isinstance(file, plantdb.fsdb.File):
        file = file.path()
    return io.read_point_cloud(str(file), **kwargs)


def _write_point_cloud(fname, data, **kwargs):
    """Writes a point cloud to a file using the ``open3d`` library.

    Parameters
    ----------
    fname : pathlib.Path or str
        The file path to use to save the `data`.
    data : open3d.geometry.PointCloud
        The point cloud object to save.
    """
    from open3d import io
    io.write_point_cloud(str(fname), data, **kwargs)
    return


def write_point_cloud(file, data, ext="ply", **kwargs):
    """Writes a point cloud to a ROMI database file or to a given path.

    Parameters
    ----------
    file : plantdb.db.File or pathlib.Path or str
        If a ``File`` instance, the file will be saved to the associated database/scan/fileset.
        Else, write the `data` to the given file path, ignoring the extension parameter `ext`.
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
    >>> write_point_cloud(f,pcd)

    """
    _writer(file, data, ext, _write_point_cloud, **kwargs)
    return


def read_triangle_mesh(file, **kwargs):
    """Reads a triangular mesh from a ROMI database file or a path.

    Parameters
    ----------
    file : plantdb.db.File or pathlib.Path or str
        A ``File`` instance or file path, to read from.

    Returns
    -------
    open3d.geometry.TriangleMesh
        The loaded triangular mesh object.
    """
    from open3d import io
    # Get path to file if in a database:
    if isinstance(file, plantdb.fsdb.File):
        file = file.path()
    return io.read_triangle_mesh(str(file), **kwargs)


def _write_triangle_mesh(fname, data, **kwargs):
    """Writer for point-cloud data using the ``open3d`` library.

    Parameters
    ----------
    fname : pathlib.Path or str
        The file path to use to save the `data`.
    data : open3d.geometry.TriangleMesh
        The triangular mesh object to save.
    """
    from open3d import io
    io.write_triangle_mesh(str(fname), data, **kwargs)
    return


def write_triangle_mesh(file, data, ext="ply", **kwargs):
    """Writes a triangular mesh to a ROMI database file or to a given path.

    Parameters
    ----------
    file : plantdb.db.File or pathlib.Path or str
        If a ``File`` instance, the file will be saved to the associated database/scan/fileset.
        Else, write the `data` to the given file path, ignoring the extension parameter `ext`.
    data : open3d.geometry.TriangleMesh
        The triangular mesh object to save.
    ext : str, optional
        File extension, defaults to "ply".
    """
    _writer(file, data, ext, _write_triangle_mesh, **kwargs)
    return


def read_voxel_grid(file, **kwargs):
    """Reads a voxel grid from a ROMI database file or a path.

    Parameters
    ----------
    file : plantdb.db.File or pathlib.Path or str
        A ``File`` instance or file path, to read from.

    Returns
    -------
    open3d.geometry.VoxelGrid
        The loaded point cloud object.
    """
    from open3d import io
    # Get path to file if in a database:
    if isinstance(file, plantdb.fsdb.File):
        file = file.path()
    return io.read_voxel_grid(str(file), **kwargs)


def _write_voxel_grid(fname, data, **kwargs):
    """Writes a voxel grid to a file.

    Parameters
    ----------
    file : pathlib.Path or str
        The file path to use to save the `data`.
    data : open3d.geometry.VoxelGrid
        The voxel grid object to save.
    """
    from open3d import io
    io.write_voxel_grid(str(fname), data, **kwargs)
    return


def write_voxel_grid(file, data, ext="ply", **kwargs):
    """Writes a voxel grid to a ROMI database file.

    Parameters
    ----------
    file : plantdb.db.File
        The `File` object used to write the associated file.
    data : open3d.geometry.VoxelGrid
        The voxel grid object to save.
    ext : str, optional
        File extension, defaults to "ply".

    See Also
    --------
    plantdb.io._write_voxel_grid
    open3d.cuda.pybind.io.write_voxel_grid
    """
    _writer(file, data, ext, _write_voxel_grid, **kwargs)
    return


def read_graph(file, **kwargs):
    """Reads a networkx ``Graph`` from a ROMI database file or a path.

    Parameters
    ----------
    file : plantdb.db.File or pathlib.Path or str
        A ``File`` instance or file path, to read from.
    ext : str, optional
        File extension, defaults to "p".

    Returns
    -------
    networkx.Graph
        The loaded (tree) graph object.

    Examples
    --------
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
    # Get path to file if in a database:
    if isinstance(file, plantdb.fsdb.File):
        file = file.path()
    # Load the pickled file:
    with open(file, mode='rb') as f:
        G = pickle.load(f, **kwargs)
    return G


def _write_graph(fname, data, **kwargs):
    """Writes a networkx ``Graph`` to a file.

    Parameters
    ----------
    fname : pathlib.Path or str
        The file path to use to save the `data`.
    data : networkx.Graph
        The (tree) graph object to save.
    """
    import pickle
    if 'protocol' not in kwargs:
        kwargs['protocol'] = pickle.HIGHEST_PROTOCOL
    if isinstance(fname, str):
        fname = Path(fname)
    with fname.open(mode='wb') as f:
        pickle.dump(data, f, **kwargs)
    return


def write_graph(file, data, ext="p", **kwargs):
    """Writes a networkx ``Graph`` to a ROMI database file.

    Parameters
    ----------
    file : plantdb.db.File or pathlib.Path or str
        If a ``File`` instance, the file will be saved to the associated database/scan/fileset.
        Else, write the `data` to the given file path, ignoring the extension parameter `ext`.
    data : networkx.Graph
        The (tree) graph object to save.
    ext : str, optional
        File extension, defaults to "p".

    Examples
    --------
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
    _writer(file, data, ext, _write_graph, **kwargs)
    return


def read_torch(file, ext="pt", **kwargs):
    """Reads a torch tensor from a ROMI database file or a path.

    Parameters
    ----------
    file : plantdb.db.File or pathlib.Path or str
        A ``File`` instance or file path, to read from.
    ext : str, optional
        File extension, defaults to "pt".

    Returns
    -------
    Torch.Tensor
        The loaded tensor object.
    """
    import torch
    # Get path to file if in a database:
    if isinstance(file, plantdb.fsdb.File):
        file = file.path()
    return torch.load(file, **kwargs)


def _write_torch(fname, data, **kwargs):
    """Writes a torch tensor to a file.

    Parameters
    ----------
    fname : pathlib.Path or str
        The file path to use to save the `data`.
    data : TorchTensor
        The torch tensor object to save.
    """
    import torch
    torch.save(data, fname, **kwargs)
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
