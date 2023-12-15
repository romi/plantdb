#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
plantdb.utils
=============

This module contains utility functions.
"""

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
    >>> from plantdb.fsdb import FSDB
    >>> from plantdb.test_database import setup_test_database
    >>> db = FSDB(setup_test_database('real_plant', '/tmp/ROMI_DB'))
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
    >>> from plantdb.test_database import setup_test_database
    >>> db = FSDB(setup_test_database('real_plant_analyzed', '/tmp/ROMI_DB'))
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
