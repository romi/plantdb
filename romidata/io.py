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

import os
import tempfile
from .db import File, Fileset, DB, Scan
from . import fsdb


def read_json(dbfile):
    """Reads json from a DB file.

    Parameters
    ----------
    dbfile : db.File

    Returns
    -------
    jsonifyable object
    """
    import json
    return json.loads(dbfile.read())


def write_json(dbfile, data, ext="json"):
    """Writes json to a DB file.

    Parameters
    ----------
    dbfile : db.File
    data : jsonifyable object
    ext : str
        file extension (defaults to "json")
    """
    import json
    dbfile.write(json.dumps(data, indent=4), ext)


def read_toml(dbfile):
    """Reads toml from a DB file.

    Parameters
    ----------
    dbfile : db.File

    Returns
    -------
    jsonifyable object
    """
    import toml
    return toml.loads(dbfile.read())


def write_toml(dbfile, data, ext="toml"):
    """Writes toml to a DB file.

    Parameters
    ----------
    dbfile : db.File
    data : jsonifyable object
    ext : str
        file extension (defaults to "toml")
    """
    import toml
    dbfile.write(toml.dumps(data), ext)


def read_image(dbfile):
    """Reads image from a DB file.

    Parameters
    ----------
    dbfile : db.File

    Returns
    -------
    np.ndarray
        The image array.
    """
    import imageio
    return imageio.imread(dbfile.read_raw())


def write_image(dbfile, data, ext="jpg"):
    """Writes image to a DB file.

    Parameters
    ----------
    dbfile : db.File
    data : imageifyable object
    ext : str
        file extension (defaults to "jpg")
    """
    import imageio
    if ext == "jpg" and len(data.shape) == 3:
        data = data[:, :, :3]
    b = imageio.imwrite(imageio.RETURN_BYTES, data, format=ext)
    dbfile.write_raw(b, ext)


def read_volume(dbfile):
    """Reads volume from a DB file.

    Parameters
    ----------
    dbfile : db.File

    Returns
    -------
    np.ndarray
        The volume array.
    """
    import imageio
    return imageio.volread(dbfile.read_raw(), format="npz")


def write_volume(dbfile, data):
    """Writes volume to a DB file.

    Parameters
    ----------
    dbfile : db.File
    data : 3D numpy array
    ext : str
        file extension (defaults to "tiff").
    """
    import imageio
    b = imageio.volwrite(imageio.RETURN_BYTES, data, format="npz")
    dbfile.write_raw(b, "npz")


def read_npz(dbfile):
    """Reads npz from a DB file.

    Parameters
    ----------
    dbfile : db.File

    Returns
    -------
    np.ndarray
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
    data : 3D numpy array
    ext : str
        file extension (defaults to "tiff").
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

    Returns
    -------
    PointCloud
    """
    try:
        from open3d import open3d
    except:
        import open3d

    try:  # 0.7 -> 0.8 breaking
        o3d_read_point_cloud = open3d.geometry.read_point_cloud
    except:
        o3d_read_point_cloud = open3d.io.read_point_cloud
    b = dbfile.read_raw()
    with tempfile.TemporaryDirectory() as d:
        fname = os.path.join(d, "temp.%s" % ext)
        with open(fname, "wb") as fh:
            fh.write(b)
        return o3d_read_point_cloud(fname)


def write_point_cloud(dbfile, data, ext="ply"):
    """Writes point cloud to a DB file.

    Parameters
    ----------
    dbfile : db.File
    data : PointCloud
    ext : str
        file extension (defaults to "ply").
    """
    try:
        from open3d import open3d
    except:
        import open3d

    try:  # 0.7 -> 0.8 breaking
        o3d_write_point_cloud = open3d.geometry.write_point_cloud
    except:
        o3d_write_point_cloud = open3d.io.write_point_cloud
    with tempfile.TemporaryDirectory() as d:
        fname = os.path.join(d, "temp.%s" % ext)
        o3d_write_point_cloud(fname, data)
        dbfile.import_file(fname)


def read_triangle_mesh(dbfile, ext="ply"):
    """Reads triangle mesh from a DB file.
    Parameters
    ----------
    dbfile : db.File

    Returns
    -------
    PointCloud
    """
    try:
        from open3d import open3d
    except:
        import open3d

    try:  # 0.7 -> 0.8 breaking
        o3d_read_triangle_mesh = open3d.geometry.read_triangle_mesh
    except:
        o3d_read_triangle_mesh = open3d.io.read_triangle_mesh
    b = dbfile.read_raw()
    with tempfile.TemporaryDirectory() as d:
        fname = os.path.join(d, "temp.%s" % ext)
        with open(fname, "wb") as fh:
            fh.write(b)
        return o3d_read_triangle_mesh(fname)


def write_triangle_mesh(dbfile, data, ext="ply"):
    """Writes triangle mesh to a DB file.

    Parameters
    ----------
    dbfile : db.File
    data : PointCloud
    ext : str
        file extension (defaults to "ply").
    """
    try:
        from open3d import open3d
    except:
        import open3d

    try:  # 0.7 -> 0.8 breaking
        o3d_write_triangle_mesh = open3d.geometry.write_triangle_mesh
    except:
        o3d_write_triangle_mesh = open3d.io.write_triangle_mesh

    with tempfile.TemporaryDirectory() as d:
        fname = os.path.join(d, "temp.%s" % ext)
        o3d_write_triangle_mesh(fname, data)
        dbfile.import_file(fname)


def read_voxel_grid(dbfile, ext="ply"):
    """Reads voxel grid from a DB file.

    Parameters
    ----------
    dbfile : db.File

    Returns
    -------
    PointCloud
    """
    try:
        from open3d import open3d
    except:
        import open3d

    try:  # 0.7 -> 0.8 breaking
        o3d_read_voxel_grid = open3d.geometry.read_voxel_grid
    except:
        o3d_read_voxel_grid = open3d.io.read_voxel_grid
    b = dbfile.read_raw()
    with tempfile.TemporaryDirectory() as d:
        fname = os.path.join(d, "temp.%s" % ext)
        with open(fname, "wb") as fh:
            fh.write(b)
        return o3d_read_voxel_grid(fname)


def write_voxel_grid(dbfile, data, ext="ply"):
    """Writes voxel grid to a DB file.

    Parameters
    ----------
    dbfile : db.File
    data : PointCloud
    ext : str
        file extension (defaults to "ply").
    """
    try:
        from open3d import open3d
    except:
        import open3d

    try:  # 0.7 -> 0.8 breaking
        o3d_write_voxel_grid = open3d.geometry.write_voxel_grid
    except:
        o3d_write_voxel_grid = open3d.io.write_voxel_grid

    with tempfile.TemporaryDirectory() as d:
        fname = os.path.join(d, "temp.%s" % ext)
        o3d_write_voxel_grid(fname, data)
        dbfile.import_file(fname)


def read_graph(dbfile):
    """Reads treex tree from a DB file.

    Parameters
    ----------
    dbfile : db.File

    Returns
    -------
    TriangleMesh
    """
    import networkx as nx
    b = dbfile.read_raw()
    ext = "p"
    with tempfile.TemporaryDirectory() as d:
        fname = os.path.join(d, "temp.%s" % ext)
        with open(fname, "wb") as fh:
            fh.write(b)
        return nx.read_gpickle(fname)


def write_graph(dbfile, data):
    """Writes treex tree to a DB file.

    Parameters
    ----------
    dbfile : db.File
    data : treex.tree.Tree
    ext : str
        file extension (defaults to "treex").
    """
    import networkx as nx
    ext = "p"
    with tempfile.TemporaryDirectory() as d:
        fname = os.path.join(d, "temp.%s" % ext)
        nx.write_gpickle(data, fname)
        dbfile.import_file(fname)


def read_torch(dbfile, ext="pt"):
    """Reads torch tensor from a DB file.

    Parameters
    ----------
    dbfile : db.File

    Returns
    -------
    Torch.Tensor
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
    data : TorchTensor
    ext : str
        file extension (defaults to "pt").
    """
    import torch
    with tempfile.TemporaryDirectory() as d:
        fname = os.path.join(d, "temp.%s" % ext)
        torch.save(data, fname)

        dbfile.import_file(fname)


def to_file(dbfile: File, path: str):
    """
    Helper to write a dbfile to a file in the filesystem
    """
    b = dbfile.read_raw()
    with open(path, "wb") as fh:
        fh.write(b)


def dbfile_from_local_file(path: str):
    """
    Creates a temporary (not in a DB) File object from a local file
    """
    dirname, fname = os.path.split(path)
    id = os.path.splitext(fname)[0]

    db = DB()
    db.basedir = ""
    scan = Scan(db, "")
    fileset = Fileset(db, scan, dirname)

    f = fsdb.File(db=db, fileset=fileset, id=id)
    f.filename = fname
    f.metadata = None

    return f


def tmpdir_from_fileset(fileset: Fileset):
    tmpdir = tempfile.TemporaryDirectory()
    for f in fileset.get_files():
        filepath = os.path.join(tmpdir.name, f.filename)
        to_file(f, filepath)
    return tmpdir
