# -*- python -*-
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
# License along with romidata.  If not, see <https://www.gnu.org/licenses/>.
# ------------------------------------------------------------------------------
import json
import os
import hashlib
from romidata import io
from romidata import FSDB as DB
from skimage.transform import resize
import imageio
import numpy as np

def file_path(db, scanid, filesetid, fileid):
    scan = db.get_scan(scanid)
    fs = scan.get_fileset(filesetid)
    f = fs.get_file(fileid)
    return os.path.join(db.basedir, scan.id, fs.id, f.filename)

def _hash(type, scanid, filesetid, fileid, size):
    m = hashlib.sha1()
    key = "%s|%s|%s|%s|%s" % (type, scanid, filesetid, fileid, size)
    m.update(key.encode('utf-8'))
    return m.hexdigest()

# Image

def image_hash(scanid, filesetid, fileid, size):
    return _hash("image", scanid, filesetid, fileid, size)

def image_resize(img, max_size):
    i = np.argmax(img.shape[0:2])
    if img.shape[i] <= max_size:
        return img
    if i == 0:
        new_shape = [max_size, int(max_size * img.shape[1]/img.shape[0])]
    else:
        new_shape = [int(max_size * img.shape[0]/img.shape[1]), max_size]
    return resize(img, new_shape)


def image_cache(db, scanid, filesetid, fileid, size):
    src = file_path(db, scanid, filesetid, fileid)
    dir = os.path.join(db.basedir, scanid, "webcache")
    os.makedirs(dir, exist_ok=True)
    dst = os.path.join(dir, image_hash(scanid, filesetid, fileid, size))
    
    resolutions = {
        "large": 1500,
        "thumb": 150
    }
    maxsize = resolutions.get(size) 

    image = imageio.imread(src)
    # remove alpha channel
    if image.shape[2] == 4:
        image = image[:,:,:3]
    cached_image = image_resize(image, maxsize)
    imageio.imwrite(dst, cached_image, format="jpg")

    print("Converted %s to %s, size %d" % (src, dst, maxsize))

    return dst;


def image_cached_path(db, scanid, filesetid, fileid, size):
    path = os.path.join(db.basedir, scanid, "webcache",
                        image_hash(scanid, filesetid, fileid, size))
    if not os.path.isfile(path):
        image_cache(db, scanid, filesetid, fileid, size)
    return path


def image_path(db, scanid, filesetid, fileid, size):
    if size == "orig":
        print("Using original file")
        return file_path(db, scanid, filesetid, fileid)
    else:
        print("Using cached file")
        return image_cached_path(db, scanid, filesetid, fileid, size)

# PointCloud

def pointcloud_hash(scanid, filesetid, fileid, size):
    return _hash("pointcloud", scanid, filesetid, fileid, size)


def pointcloud_resize(pointcloud, max_size):
    try:
        from open3d import open3d
    except:
        import open3d
        
    if len(pointcloud.points) < max_pointcloud_size:
        return pointcloud

    downsample = len(pointcloud.points) // max_size + 1
    try:
        return open3d.geometry.uniform_down_sample(pointcloud, downsample)
    except:
        return pointcloud.voxel_down_sample(downsample)


def pointcloud_cache(db, scanid, filesetid, fileid, size):
    try:
        from open3d import open3d
    except:
        import open3d
        
    try:  # 0.7 -> 0.8 breaking
        read_pointcloud = open3d.geometry.read_point_cloud
        write_pointcloud = open3d.geometry.write_point_cloud
    except:
        read_pointcloud = open3d.io.read_point_cloud
        write_pointcloud = open3d.io.write_point_cloud
        
    src = file_path(db, scanid, filesetid, fileid)
    dir = os.path.join(db.basedir, scanid, "webcache")
    os.makedirs(dir, exist_ok=True)
    dst = os.path.join(dir, pointcloud_hash(scanid, filesetid, fileid, size))

    max_pointcloud_size = 10000
    pointcloud = read_pointcloud(src)
    pointcloud_lowres = pointcloud_resize(pointcloud, max_pointcloud_size)
    write_pointcloud(dst, pointcloud_lowres)
    
    return dst;


def pointcloud_cached_path(db, scanid, filesetid, fileid, size):
    path = os.path.join(db.basedir, scanid, "webcache",
                        pointcloud_hash(scanid, filesetid, fileid, size))
    if not os.path.isfile(path):
        pointcloud_cache(db, scanid, filesetid, fileid, size)
    return path


def pointcloud_path(db, scanid, filesetid, fileid, size):
    if size == "orig":
        print("Using original file")
        return file_path(db, scanid, filesetid, fileid)
    else:
        print("Using cached file")
        return pointcloud_cached_path(db, scanid, filesetid, fileid, size)


# Mesh

def mesh_path(db, scanid, filesetid, fileid, size):
    print("Using original file")
    return file_path(db, scanid, filesetid, fileid)

    
