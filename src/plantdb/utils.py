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
This module contains utility functions.
"""
import tempfile
from pathlib import Path

from PIL import Image


def read_image_from_file(filename):
    """Read an image from a file and return it.

    Parameters
    ----------
    filename : str or pathlib.Path
        The path to the image to read.

    Returns
    -------
    image : PIL.Image.Image
        The loaded image.

    Examples
    --------
    >>> from plantdb.utils import read_image_from_file
    >>> from plantdb.webcache import image_path
    >>> from plantdb.test_database import test_database
    >>> db = test_database('real_plant')
    >>> db.connect()
    >>> img_path = image_path(db, 'real_plant', 'images', '00000_rgb', 'orig')
    >>> image = read_image_from_file(img_path)
    >>> print(image.size)
    (1440, 1080)
    >>> db.disconnect()
    """
    image = Image.open(filename)
    image.load()
    return image


def locate_task_filesets(scan, tasks):
    """Map the task names to task filesets.

    Parameters
    ----------
    scan : plantdb.fsdb.Scan
        A ``Scan`` instance from a local plant database (FSDB).
    tasks : list of str
        A list of task names to look up in the scan's list of filesets.

    Returns
    -------
    dict
        A task indexed dictionary of fileset ids, value may be "None" if no matching fileset was found.

    Notes
    -----
    If more than one fileset id matches a task name, only the first one (found) will be returned!

    Examples
    --------
    >>> from plantdb.utils import locate_task_filesets
    >>> from plantdb.fsdb import FSDB
    >>> from plantdb.test_database import test_database
    >>> db = test_database('real_plant_analyzed')
    >>> db.connect()
    >>> scan = db.get_scan('real_plant_analyzed')
    >>> tasks_fs = locate_task_filesets(scan, ['Masks', 'PointCloud', 'UnknownTask'])
    >>> print(tasks_fs)
    {'Masks': 'Masks_1__0__1__0____channel____rgb_5619aa428d', 'PointCloud': 'PointCloud_1_0_1_0_10_0_7ee836e5a9', 'UnknownTask': 'None'}
    >>> db.disconnect()
    """
    # List all filesets in the scan dataset:
    fs_list = scan.list_filesets()
    # Find the fileset corresponding to the given list of tasks, if any:
    fileset_names = {}
    for task in tasks:
        try:
            # TODO: could be improved by using the saved config ('pipeline.toml') and recreate the name hash from luigi...
            fileset_names[task] = [fs for fs in fs_list if fs.startswith(task)][0]
        except IndexError:
            fileset_names[task] = "None"
    return fileset_names


def is_radians(angles):
    """Guess if the sequence of angles is in radians.

    Parameters
    ----------
    angles : list of float
        Sequence of angle values.

    Returns
    -------
    bool
        `True` if the sequence is in radians, else `False.

    Notes
    -----
    This assumes that the angles can not be greater than 360 degrees or its equivalent in radians.
    """
    from math import radians
    if all([angle < radians(360) for angle in angles]):
        return True
    else:
        return False


def to_file(dbfile, path):
    """Write a `dbfile` to a file in the filesystem.

    Parameters
    ----------
    dbfile : plantdb.fsdb.File
        The ``File`` instance to save under given `path`.
    path : pathlib.Path or str
        The file path to use to save the `dbfile`.
    """
    b = dbfile.read_raw()
    path = Path(path)
    with path.open(mode="wb") as fh:
        fh.write(b)
    return


def fsdb_file_from_local_file(path):
    """Creates a temporary ``fsdb.File`` object from a local file.

    Parameters
    ----------
    path : pathlib.Path or str
        The file path to use to create the temporary local database.

    Returns
    -------
    plantdb.fsdb.File
        The temporary ``fsdb.File``.
    """
    from plantdb.fsdb import FSDB
    from plantdb.fsdb import Scan
    from plantdb.fsdb import Fileset
    from plantdb.fsdb import File
    from plantdb.fsdb import MARKER_FILE_NAME
    path = Path(path)
    dirname, fname = path.parent, path.name
    id = Path(fname).stem
    with tempfile.TemporaryDirectory() as tmpdir:
        # Initialise a temporary `FSDB`:
        Path(f"{tmpdir}/{MARKER_FILE_NAME}").touch()  # add the db marker file
        db = FSDB(tmpdir)
        # Initialize a `Scan` instance:
        scan = Scan(db, "tmp")
        # Initialize a `Fileset` instance:
        fileset = Fileset(scan, dirname)
        # Initialize a `File` instance & return it:
        f = File(db=db, fileset=fileset, id=id)
        f.filename = fname
        f.metadata = None
    return f


def tmpdir_from_fileset(fileset):
    """Creates a temporary directory to host the ``Fileset`` object and write files.

    Parameters
    ----------
    fileset : plantdb.fsdb.Fileset
        The fileset to use to create the temporary local database.

    Returns
    -------
    tempfile.TemporaryDirectory
        The temporary directory hosting the fileset and file(s), if any.
    """
    tmpdir = tempfile.TemporaryDirectory()
    for f in fileset.get_files():
        filepath = Path(tmpdir.name) / f.filename
        to_file(f, filepath)
    return tmpdir
