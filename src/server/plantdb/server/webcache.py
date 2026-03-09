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
This module provides three utility functions that are used in combination with the
DB interface to create downsized versions of images, point clouds, and mesh resources.
The resources are identified using the ``Scan``, ``Fileset``, and `File` IDs.
The downsized versions are cached in the `'webcache'` directory in the scan directory.

The following **size specifications** are available:

* Images: 'thumb' (max. 150x150), 'large' (max. 1500x1500), and 'orig' (original size).
* Point clouds: 'preview' (max. 10k points), and 'orig' (original size).
* Mesh: 'orig' (original size). TODO: add remeshing

Examples
--------
>>> from plantdb.server import webcache
>>> from os import environ
>>> from plantdb.commons.fsdb.core import FSDB
>>> db = FSDB(environ.get('ROMI_DB', "/data/ROMI/DB/"))
>>> db.connect()
>>> # Get the path to the original image:
>>> webcache.image_path(db,'sango36','images','00000_rgb','orig')
>>> # Get the path to the thumb image (resized and cached):
>>> webcache.image_path(db,'sango36','images','00000_rgb','thumb')
>>> # Get the path to the original pointcloud:
>>> webcache.pointcloud_path(db,'sango_90_300_36','PointCloud_1_0_0_0_10_0_ca07eb2790','PointCloud','orig')
>>> # Get the path to the preview pointcloud (resized and cached):
>>> webcache.pointcloud_path(db,'sango_90_300_36','PointCloud_1_0_0_0_10_0_ca07eb2790','PointCloud','preview')
>>> # Get the path to a downsampled pointcloud (resized and cached):
>>> webcache.pointcloud_path(db,'sango_90_300_36','PointCloud_1_0_0_0_10_0_ca07eb2790','PointCloud','2.3')
>>> db.disconnect()
"""
import hashlib
from pathlib import Path
from plantdb.commons.log import get_logger

from PIL import Image

try:
    import open3d as o3d
except ModuleNotFoundError:
    msg = "Please install Open3D with the following command: `python -m pip install open3d`"
    raise ModuleNotFoundError(msg)

IMG_RESOLUTIONS = {"large": 1500, "thumb": 150}

# Get logger instance
logger = get_logger(__name__)


def __webcache_path(db, scan_id: str) -> Path:
    """Creates a 'webcache' directory in the scan directory.

    Parameters
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        The database object.
    scan_id : str
        The ID of the scan in the database.

    Returns
    -------
    pathlib.Path
        The path to the 'webcache' directory for the given scan.
    """
    directory = db.basedir / scan_id / "webcache"
    directory.mkdir(exist_ok=True)
    return directory


def __file_path(db, scan_id, fileset_id, file_id, **kwargs) -> Path:
    """Return the path to a file.

    Parameters
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        The database object.
    scan_id : str
        The ID of the scan in the database.
    fileset_id : str
        The ID of the fileset in the scan.
    file_id : str
        The ID of the file in the fileset.

    Returns
    -------
    pathlib.Path
        The path to the file.
    """
    scan = db.get_scan(scan_id, **kwargs)
    fs = scan.get_fileset(fileset_id)
    f = fs.get_file(file_id)
    return db.basedir / scan.id / fs.id / f.filename


def __hash(resource_type, scan_id, fileset_id, file_id, size) -> str:
    """Create a hash for a resource.

    Parameters
    ----------
    resource_type : str
        The name of the resource type, *e.g.* "image" or "pointcloud".
    scan_id : str
        The ID of the scan in the database.
    fileset_id : str
        The ID of the fileset in the scan.
    file_id : str
        The ID of the file in the fileset.
    size : str
        The requested size.

    Returns
    -------
    str
        The hash of the resource.
    """
    m = hashlib.sha1()
    key = f"{resource_type}|{scan_id}|{fileset_id}|{file_id}|{size}"
    m.update(key.encode('utf-8'))
    return m.hexdigest()


# -----------------------------------------------------------------------------
# Image
# -----------------------------------------------------------------------------
def __image_hash(scan_id, fileset_id, file_id, size) -> str:
    """Create a file name for an image resource using a hash value.

    Parameters
    ----------
    scan_id : str
        The ID of the scan in the database.
    fileset_id : str
        The ID of the fileset in the scan.
    file_id : str
        The ID of the file in the fileset.
    size : str | int, optional
        If an integer, use it as the size of the cached image to create and return.
        Else, it should be a string from ``['orig', 'large', 'thumb']``.

    Returns
    -------
    str
        The image resource file name.
    """
    return __hash("image", scan_id, fileset_id, file_id, size) + ".jpeg"


def __image_resize(img, max_size) -> Image.Image:
    """Resize a ``Pillow`` image.

    Parameters
    ----------
    img : PIL.Image.Image
        A ``Pillow`` image to resize.
    max_size : int
        The requested max width or height size, in pixels.

    Returns
    -------
    PIL.Image.Image
        The resized image.
    """
    img.thumbnail((max_size, max_size))
    return img


def __image_cache(db, scan_id, fileset_id, file_id, size, **kwargs):
    """Create a cache for an image resource.

    Parameters
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        The database object.
    scan_id : str
        The ID of the scan in the database.
    fileset_id : str
        The ID of the fileset in the scan.
    file_id : str
        The ID of the file in the fileset.
    size : str | int, optional
        If an integer, use it as the size of the cached image to create and return.
        Else, it should be a string from ``['orig', 'large', 'thumb']``.

    Returns
    -------
    pathlib.Path
        The path to the cached image.
    """
    # Get the path to the original image:
    src = __file_path(db, scan_id, fileset_id, file_id, **kwargs)

    # Get the path to the 'webcache' directory:
    cache_dir = __webcache_path(db, scan_id)
    dst = cache_dir / __image_hash(scan_id, fileset_id, file_id, size)

    # Load the image and resize it:
    image = Image.open(src)
    image.load()
    if isinstance(size, int):
        maxsize = size
    else:
        maxsize = IMG_RESOLUTIONS.get(size)
    image = __image_resize(image, maxsize)
    # Make sure we have an RGB image to be able to save in this format:
    if image.mode != "RGB":
        image = image.convert(mode="RGB")
    save_kwargs = {'quality': 84, 'optimize': True}
    # Save the resized image in the "webcache" directory:
    image.save(dst, **save_kwargs)
    logger.info(f"Converted '{src}' to '{dst}', using size '{maxsize}'")

    return dst


def __image_cached_path(db, scan_id, fileset_id, file_id, size, **kwargs):
    """Get The path to the cached image.

    Parameters
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        The database object.
    scan_id : str
        The ID of the scan in the database.
    fileset_id : str
        The ID of the fileset in the scan.
    file_id : str
        The ID of the file in the fileset.
    size : str | int, optional
        If an integer, use it as the size of the cached image to create and return.
        Else, it should be a string from ``['orig', 'large', 'thumb']``.

    Returns
    -------
    pathlib.Path
        The path to the cached image.
    """
    cache_dir = __webcache_path(db, scan_id)
    img_path = cache_dir / __image_hash(scan_id, fileset_id, file_id, size)
    if not img_path.is_file():
        __image_cache(db, scan_id, fileset_id, file_id, size, **kwargs)
    return img_path


def image_path(db, scan_id, fileset_id, file_id, size='orig', **kwargs):
    """Get the path to an image file.

    Parameters
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        The database object.
    scan_id : str
        The ID of the scan in the database.
    fileset_id : str
        The ID of the fileset in the scan.
    file_id : str
        The ID of the file in the fileset.
    size : str | int, optional
        If an integer, use  it as the size of the cached image to create and return.
        Else, it should be a string from ``['orig', 'large', 'thumb']``, where: 

          - `'thumb'`: image max width and height to `150`.
          - `'large'`: image max width and height to `1500`;
          - `'orig'`: original image, no chache;

    Returns
    -------
    pathlib.Path
        The path to the original or cached image.

    Examples
    --------
    >>> from plantdb.server.webcache import image_path
    >>> from plantdb.commons.test_database import test_database
    >>> db = test_database('real_plant_analyzed')
    >>> db.connect()
    >>> db.login('guest', 'guest')
    >>> # Example 1: Get the original image:
    >>> image_path(db, 'real_plant_analyzed', 'images', '00000_rgb', 'orig')
    PosixPath('/tmp/ROMI_DB/real_plant_analyzed/images/00000_rgb.jpg')
    >>> # Example 2: Get a thumbnail of the image:
    >>> image_path(db, 'real_plant_analyzed', 'images', '00000_rgb', 'thumb')
    PosixPath('/tmp/ROMI_DB/real_plant_analyzed/webcache/6fbae08f195837c511af7c2864d075dd5cd153bc.jpeg')
    >>> db.disconnect()
    """
    try:
        size = int(size)
    except ValueError:
        pass

    if size == "orig":
        return __file_path(db, scan_id, fileset_id, file_id, **kwargs)
    elif size == "large" or size == "thumb" or isinstance(size, int):
        return __image_cached_path(db, scan_id, fileset_id, file_id, size, **kwargs)
    else:
        raise ValueError(f"Unknown image size specification: {size}")


# -----------------------------------------------------------------------------
# PointCloud
# -----------------------------------------------------------------------------
def __pointcloud_hash(scan_id, fileset_id, file_id, size):
    """Create a hash for a pointcloud resource.

    Parameters
    ----------
    scan_id : str
        The ID of the scan in the database.
    fileset_id : str
        The ID of the fileset in the scan.
    file_id : str
        The ID of the file in the fileset.
    size : str
        The requested size ('orig' or 'preview').

    Returns
    -------
    str
        The hash of the pointcloud resource.
    """
    return __hash("pointcloud", scan_id, fileset_id, file_id, size) + ".ply"


def __pointcloud_resize(pointcloud, voxel_size):
    """Resize a pointcloud to given voxelsize.

    Parameters
    ----------
    pointcloud : open3d.geometry.PointCloud
        A pointcloud to resize to given voxelsize.
    voxel_size : float
        The voxelsize to use for resampling.

    Returns
    -------
    open3d.geometry.PointCloud
        The resized pointcloud.
    """
    return pointcloud.voxel_down_sample(voxel_size)


def __pointcloud_cache(db, scan_id, fileset_id, file_id, size, **kwargs):
    """Create a cache for a pointcloud resource.

    Parameters
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        The database object.
    scan_id : str
        The ID of the scan in the database.
    fileset_id : str
        The ID of the fileset in the scan.
    file_id : str
        The ID of the file in the fileset.
    size : str | float
        The requested size of the point cloud.
        Use 'orig' (default) to preserve the original point cloud.
        Use 'preview' to resize the point cloud to a `1.8` voxel size.
        A float value will resize the point cloud to a given voxel size.

    See Also
    --------
    __pointcloud_resize

    Returns
    -------
    pathlib.Path
        The path to the cached pointcloud.
    """
    read_pointcloud = o3d.io.read_point_cloud
    write_pointcloud = o3d.io.write_point_cloud
    # Get the path to the 'webcache' directory:
    cache_dir = __webcache_path(db, scan_id)
    dst = cache_dir / __pointcloud_hash(scan_id, fileset_id, file_id, size)

    # Load the pointcloud and resize it:
    src = __file_path(db, scan_id, fileset_id, file_id, **kwargs)
    pcd = read_pointcloud(str(src))
    pcd_npts = len(pcd.points)  # get the number of points
    if isinstance(size, float):
        vxs = size
    else:
        vxs = 1.8
    pcd_lowres = __pointcloud_resize(pcd, vxs)
    pcd_lowres_npts = len(pcd_lowres.points)  # get the number of points
    write_pointcloud(str(dst), pcd_lowres)

    logger.info(f"Converted '{src}' to '{dst}', using voxelsize '{vxs}'")
    logger.info(f"  - Original number of points: {pcd_npts}")
    logger.info(f"  - Resized number of points: {pcd_lowres_npts}")

    return dst


def __pointcloud_cached_path(db, scan_id, fileset_id, file_id, size, **kwargs):
    """Get The path to the cached pointcloud.

    Parameters
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        The database object.
    scan_id : str
        The ID of the scan in the database.
    fileset_id : str
        The ID of the fileset in the scan.
    file_id : str
        The ID of the file in the fileset.
    size : str | float
        The requested size of the point cloud.
        Use 'orig' (default) to preserve the original point cloud.
        Use 'preview' to resize the point cloud to a `1.8` voxel size.
        A float value will resize the point cloud to a given voxel size.

    Returns
    -------
    pathlib.Path
        The path to the cached pointcloud.
    """
    cache_dir = __webcache_path(db, scan_id)
    pcd_path = cache_dir / __pointcloud_hash(scan_id, fileset_id, file_id, size)
    if not pcd_path.is_file():
        __pointcloud_cache(db, scan_id, fileset_id, file_id, size, **kwargs)
    return pcd_path


def pointcloud_path(db, scan_id, fileset_id, file_id, size='orig', **kwargs):
    """Get the path to a point cloud file.

    Parameters
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        The database object.
    scan_id : str
        The ID of the scan in the database.
    fileset_id : str
        The ID of the fileset in the scan.
    file_id : str
        The ID of the file in the fileset.
    size : str | float
        The requested size of the point cloud.
        Use 'orig' (default) to preserve the original point cloud.
        Use 'preview' to resize the point cloud to a `1.8` voxel size.
        A float value will resize the point cloud to a given voxel size.

    Returns
    -------
    pathlib.Path
        The path to the original or cached pointcloud.

    Examples
    --------
    >>> from plantdb.server.webcache import pointcloud_path
    >>> from plantdb.commons.test_database import test_database
    >>> db = test_database('real_plant_analyzed')
    >>> db.connect()
    >>> # Example 1: Get the original pointcloud:
    >>> pointcloud_path(db, 'real_plant_analyzed', 'PointCloud_1_0_1_0_10_0_7ee836e5a9', 'PointCloud', 'orig')
    PosixPath('/tmp/ROMI_DB/real_plant_analyzed/PointCloud_1_0_1_0_10_0_7ee836e5a9/PointCloud.ply')
    >>> # Example 2: Get a down-sampled version of the pointcloud:
    >>> pointcloud_path(db, 'real_plant_analyzed', 'PointCloud_1_0_1_0_10_0_7ee836e5a9', 'PointCloud', 'preview')
    PosixPath('/tmp/ROMI_DB/real_plant_analyzed/webcache/77e25820ddd8facd7d7a4bc5b17ad3c81046becc.ply')
    >>> db.disconnect()
    """
    if size == "orig":
        logger.info("Using original pointcloud file")
        return __file_path(db, scan_id, fileset_id, file_id, **kwargs)
    elif size == "preview":
        logger.info("Using cached pointcloud file")
        return __pointcloud_cached_path(db, scan_id, fileset_id, file_id, size, **kwargs)
    else:
        try:
            path = __pointcloud_cached_path(db, scan_id, fileset_id, file_id, float(size), **kwargs)
        except ValueError:
            raise ValueError(f"Unknown pointcloud size specification: {size}")
        else:
            return path


# -----------------------------------------------------------------------------
# Mesh
# -----------------------------------------------------------------------------
def __mesh_hash(scan_id, fileset_id, file_id, size):
    """Create a hash for a mesh resource.

    Parameters
    ----------
    scan_id : str
        The ID of the scan in the database.
    fileset_id : str
        The ID of the fileset in the scan.
    file_id : str
        The ID of the file in the fileset.
    size : str
        The requested size ('orig' or 'preview').

    Returns
    -------
    str
        The hash of the mesh resource.
    """
    return __hash("mesh", scan_id, fileset_id, file_id, size) + ".ply"


def __mesh_resize(mesh, voxel_size):
    """Resize a mesh to given voxelsize within vertices are pooled.

    Parameters
    ----------
    mesh : open3d.geometry.TriangleMesh
        A mesh to resize to given voxelsize.
    voxel_size : float
        The voxelsize to use for resampling.

    Returns
    -------
    open3d.geometry.TriangleMesh
        The resized mesh.
    """
    # Use vertex clustering for meshes
    return mesh.simplify_vertex_clustering(voxel_size)


def __mesh_cache(db, scan_id, fileset_id, file_id, size, **kwargs):
    """Create a cache for a mesh resource.

    Parameters
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        The database object.
    scan_id : str
        The ID of the scan in the database.
    fileset_id : str
        The ID of the fileset in the scan.
    file_id : str
        The ID of the file in the fileset.
    size : str | float
        The requested size of the mesh.
        Use 'orig' (default) to preserve the original mesh.
        Use 'preview' to resize the mesh to a `1.8` voxel size.
        A float value will resize the mesh to a given voxel size.

    Returns
    -------
    pathlib.Path
        The path to the cached mesh.
    """
    read_mesh = o3d.io.read_triangle_mesh
    write_mesh = o3d.io.write_triangle_mesh
    # Get the path to the 'webcache' directory:
    cache_dir = __webcache_path(db, scan_id)
    dst = cache_dir / __mesh_hash(scan_id, fileset_id, file_id, size)

    # Load the mesh and resize it:
    src = __file_path(db, scan_id, fileset_id, file_id, **kwargs)
    mesh = read_mesh(str(src))
    mesh_npts = len(mesh.vertices)  # get the number of vertices
    
    if isinstance(size, float):
        vxs = size
    else:
        vxs = 1.8
    mesh_lowres = __mesh_resize(mesh, vxs)
    mesh_lowres_npts = len(mesh_lowres.vertices)  # get the number of vertices
    
    write_mesh(str(dst), mesh_lowres)

    logger.info(f"Converted '{src}' to '{dst}', using voxelsize '{vxs}'")
    logger.info(f"  - Original number of vertices: {mesh_npts}")
    logger.info(f"  - Resized number of vertices: {mesh_lowres_npts}")

    return dst


def __mesh_cached_path(db, scan_id, fileset_id, file_id, size, **kwargs):
    """Get The path to the cached mesh.

    Parameters
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        The database object.
    scan_id : str
        The ID of the scan in the database.
    fileset_id : str
        The ID of the fileset in the scan.
    file_id : str
        The ID of the file in the fileset.
    size : str | float
        The requested size of the mesh.
        Use 'orig' (default) to preserve the original mesh.
        Use 'preview' to resize the mesh to a `1.8` voxel size.
        A float value will resize the mesh to a given voxel size.

    Returns
    -------
    pathlib.Path
        The path to the cached mesh.
    """
    cache_dir = __webcache_path(db, scan_id)
    mesh_path = cache_dir / __mesh_hash(scan_id, fileset_id, file_id, size)
    if not mesh_path.is_file():
        __mesh_cache(db, scan_id, fileset_id, file_id, size, **kwargs)
    return mesh_path


def mesh_path(db, scan_id, fileset_id, file_id, size='orig', **kwargs):
    """Get the path to a mesh file.

    Parameters
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        The database object.
    scan_id : str
        The ID of the scan in the database.
    fileset_id : str
        The ID of the fileset in the scan.
    file_id : str
        The ID of the file in the fileset.
    size : str | float
        The requested size of the mesh.
        Use 'orig' (default) to preserve the original mesh.
        Use 'preview' to resize the mesh to a `1.8` voxel size.
        A float value will resize the mesh to a given voxel size.

    Returns
    -------
    pathlib.Path
        The path to the original or cached mesh.

    Examples
    --------
    >>> from plantdb.server.webcache import mesh_path
    >>> from plantdb.commons.test_database import test_database
    >>> db = test_database('real_plant_analyzed')
    >>> db.connect()
    >>> # Example 1: Get the original mesh:
    >>> mesh_path(db, 'real_plant_analyzed', 'TriangleMesh_9_most_connected_t_open3d_00e095c359', 'TriangleMesh', 'orig')
    PosixPath('/tmp/ROMI_DB/real_plant_analyzed/TriangleMesh_9_most_connected_t_open3d_00e095c359/TriangleMesh.ply')
    >>> # Example 2: Get a down-sampled version of the mesh:
    >>> mesh_path(db, 'real_plant_analyzed', 'TriangleMesh_9_most_connected_t_open3d_00e095c359', 'TriangleMesh', 'preview')
    PosixPath('/tmp/ROMI_DB/real_plant_analyzed/webcache/77e25820ddd8facd7d7a4bc5b17ad3c81046becc.ply')
    >>> db.disconnect()
    """
    if size == "orig":
        logger.info("Using original mesh file")
        return __file_path(db, scan_id, fileset_id, file_id, **kwargs)
    elif size == "preview":
        logger.info("Using cached mesh file")
        return __mesh_cached_path(db, scan_id, fileset_id, file_id, size, **kwargs)
    else:
        try:
            path = __mesh_cached_path(db, scan_id, fileset_id, file_id, float(size), **kwargs)
        except ValueError:
            raise ValueError(f"Unknown mesh size specification: {size}")
        else:
            return path
