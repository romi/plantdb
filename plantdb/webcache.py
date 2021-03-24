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
plantdb.webcache
=============

Provides three utility functions that are used in combination with the
DB interface to create downsized versions of images, point clouds, and
mesh resources. The resources are identified using the scan, fileset,
and file IDs.  The downsized versions are cached in the 'webcache'
directory in the scan directory.

The following size specifications are available:

* Images: 'thumb' (max. 150x150), 'large' (max. 1500x1500), and 'orig' (original size).
* Point clouds: 'preview' (max. 10k points), and 'orig' (original size).
* Mesh: 'orig' (original size). [TODO]

Examples
--------
>>> from plantdb import FSDB
>>> import plantdb.webcache as webcache
>>> db = FSDB("path/to/db"))
>>> db.connect()
>>> path = webcache.image_path(db, 'scan000', 'images', 'image000', 'thumb')
>>> db.disconnect()

"""
import hashlib
import os

from PIL import Image


def __file_path(db, scanid, filesetid, fileid):
    scan = db.get_scan(scanid)
    fs = scan.get_fileset(filesetid)
    f = fs.get_file(fileid)
    return os.path.join(db.basedir, scan.id, fs.id, f.filename)


def __hash(resource_type, scanid, filesetid, fileid, size):
    m = hashlib.sha1()
    key = "%s|%s|%s|%s|%s" % (resource_type, scanid, filesetid, fileid, size)
    m.update(key.encode('utf-8'))
    return m.hexdigest()


# Image

def __image_hash(scanid, filesetid, fileid, size):
    return __hash("image", scanid, filesetid, fileid, size)


def __image_resize(img, max_size):
    img.thumbnail((max_size, max_size))
    return img


def __image_cache(db, scanid, filesetid, fileid, size):
    src = __file_path(db, scanid, filesetid, fileid)
    directory = os.path.join(db.basedir, scanid, "webcache")
    os.makedirs(directory, exist_ok=True)
    dst = os.path.join(directory, __image_hash(scanid, filesetid, fileid, size))

    resolutions = {"large": 1500, "thumb": 150}
    maxsize = resolutions.get(size)

    image = Image.open(src)
    image.load()
    image = __image_resize(image, maxsize)
    image.save(dst, "JPEG", quality=84)

    print("Converted %s to %s, size %d" % (src, dst, maxsize))

    return dst


def __image_cached_path(db, scanid, filesetid, fileid, size):
    path = os.path.join(db.basedir, scanid, "webcache",
                        __image_hash(scanid, filesetid, fileid, size))
    if not os.path.isfile(path):
        __image_cache(db, scanid, filesetid, fileid, size)
    return path


def image_path(db, scanid, filesetid, fileid, size):
    """Point cloud path in the webcache.
        
       Returns the path in the webcache of a given point cloud file in the database.
    
       Parameters
       ----------
       db: DB
            The database object
       scanid: str
            The ID of the scan in the database
       filesetid: str
            The ID of the fileset in the scan
       fileid: str
            The ID of the file in the fileset
       size: str
            The requested size ('orig', 'large', or 'thumb')

    """
    if size == "orig":
        print("Using original file")
        return __file_path(db, scanid, filesetid, fileid)
    elif size == "large" or size == "thumb":
        print("Using cached file")
        return __image_cached_path(db, scanid, filesetid, fileid, size)
    else:
        raise ValueError("Unknow size specification: %s" % size)


# PointCloud

def __load_open3d():
    try:
        import open3d as o3d
    except ModuleNotFoundError:
        msg = "Please install Open3D with the following command: `python -m pip install open3d`"
        raise ModuleNotFoundError(msg)


def __pointcloud_hash(scanid, filesetid, fileid, size):
    return __hash("pointcloud", scanid, filesetid, fileid, size)


def __pointcloud_resize(pointcloud, max_size):
    __load_open3d()
    if len(pointcloud.points) < max_size:
        return pointcloud
    downsample = len(pointcloud.points) // max_size + 1
    return pointcloud.voxel_down_sample(downsample)


def __pointcloud_cache(db, scanid, filesetid, fileid, size):
    __load_open3d()
    read_pointcloud = o3d.io.read_point_cloud
    write_pointcloud = o3d.io.write_point_cloud

    src = __file_path(db, scanid, filesetid, fileid)
    directory = os.path.join(db.basedir, scanid, "webcache")
    os.makedirs(directory, exist_ok=True)
    dst = os.path.join(directory, __pointcloud_hash(scanid, filesetid, fileid, size))

    max_pointcloud_size = 10000
    pointcloud = read_pointcloud(src)
    pointcloud_lowres = __pointcloud_resize(pointcloud, max_pointcloud_size)
    write_pointcloud(dst, pointcloud_lowres)

    return dst


def __pointcloud_cached_path(db, scanid, filesetid, fileid, size):
    path = os.path.join(db.basedir, scanid, "webcache",
                        __pointcloud_hash(scanid, filesetid, fileid, size))
    if not os.path.isfile(path):
        __pointcloud_cache(db, scanid, filesetid, fileid, size)
    return path


def pointcloud_path(db, scanid, filesetid, fileid, size):
    """Point cloud path in the webcache.
        
       Returns the path in the webcache of a given point cloud file in the database.
    
       Parameters
       ----------
       db: DB
            The database object
       scanid: str
            The ID of the scan in the database
       filesetid: str
            The ID of the fileset in the scan
       fileid: str
            The ID of the file in the fileset
       size: str
            The requested size ('orig' or 'preview')

    """
    if size == "orig":
        print("Using original file")
        return __file_path(db, scanid, filesetid, fileid)
    elif size == "preview":
        print("Using cached file")
        return __pointcloud_cached_path(db, scanid, filesetid, fileid, size)
    else:
        raise ValueError("Unknow size specification: %s" % size)


# Mesh

def mesh_path(db, scanid, filesetid, fileid, size):
    """Mesh path in the webcache.
        
       Returns the path in the webcache of a given mesh file in the database.
    
       Parameters
       ----------
       db: DB
            The database object
       scanid: str
            The ID of the scan in the database
       filesetid: str
            The ID of the fileset in the scan
       fileid: str
            The ID of the file in the fileset
       size: str
            The requested size (orig)

    """
    print("Using original file")
    return __file_path(db, scanid, filesetid, fileid)
