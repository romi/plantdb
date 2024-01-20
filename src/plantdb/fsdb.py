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
This module implement a database as a **local file structure**.

Assuming that the ``FSDB`` root database directory is ``dbroot/``, there is a ``Scan`` with ``'myscan_001'`` as ``Scan.id`` and there are some metadata (see below), you should have the following file structure:

.. code-block::

    dbroot/                            # base directory of the database
    ├── myscan_001/                    # scan dataset directory, id=`myscan_001`
    │   ├── files.json                 # JSON file referencing the all files for the dataset
    │   ├── images/                    # `Fileset` gathering the 'images'
    │   │   ├── scan_img_01.jpg        # 'image' `File` 01
    │   │   ├── scan_img_02.jpg        # 'image' `File` 02
    │   │   ├── [...]                  #
    │   │   └── scan_img_99.jpg        # 'image' `File` 99
    │   ├── metadata/                  # metadata directory
    │   │   ├── images                 # 'images' metadata directory
    │   │   │   ├── scan_img_01.json   # JSON metadata attached to this file
    │   │   │   ├── scan_img_02.json   #
    │   │   │   [...]                  #
    │   │   │   └── scan_img_99.json   #
    │   │   ├── Task_A/                # optional, only present if metadata attached to one of the outputs from `Task_A`
    │   │   │   └── outfile.json       # optional metadata attached to the output file from `Task_A`
    │   │   └── (metadata.json)        # optional metadata attached to the dataset
    │   ├── task_A/                    # `Fileset` gathering the outputs of `Task_A`
    │   │   └── outfile.ext            # output file from `Task_A`
    │   └── (measures.json)            # optional manual measurements file
    ├── myscan_002/                    # scan dataset directory, id=`myscan_002`
    :
    ├── (LOCK_FILE_NAME)               # "lock file", present if DB is connected
    └── MARKER_FILE_NAME               # ROMI DB marker file

The ``myscan_001/files.json`` file then contains the following structure:

.. code-block:: json

    {
        "filesets": [
            {
                "id": "images",
                "files": [
                    {
                        "id": "scan_img_01",
                        "file": "scan_img_01.jpg"
                    },
                    {
                        "id": "scan_img_02",
                        "file": "scan_img_02.jpg"
                    },
                    [...]
                    {
                        "id": "scan_img_99",
                        "file": "scan_img_99.jpg"
                    }
                ]
            }
        ]
    }

The metadata of the scan (``metadata.json``), of the set of 'images' files (``<Fileset.id>.json``) and of each 'image' files (``<File.id>.json``) are all stored as JSON files in a separate directory:

.. code-block::

    myscan_001/metadata/
    myscan_001/metadata/metadata.json
    myscan_001/metadata/images.json
    myscan_001/metadata/images/scan_img_01.json
    myscan_001/metadata/images/scan_img_02.json
    [...]
    myscan_001/metadata/images/scan_img_99.json

"""

import atexit
import copy
import json
import logging
import os
import pathlib
from pathlib import Path
from shutil import copyfile
from shutil import rmtree

from tqdm import tqdm

from plantdb import db
from plantdb.db import DBBusyError
from plantdb.log import configure_logger

logger = configure_logger(__name__)

#: This file must exist in the root of a folder for it to be considered a valid DB
MARKER_FILE_NAME = "romidb"
#: This file prevents opening the DB if it is present in the root folder of a DB
LOCK_FILE_NAME = "lock"


def dummy_db(with_scan=False, with_fileset=False, with_file=False):
    """Create a dummy temporary database.

    Parameters
    ----------
    with_scan : bool, optional
        Add a dummy ``Scan`` to the dummy database.
    with_fileset : bool, optional
        Add a dummy ``Fileset`` to the dummy database.
    with_file : bool, optional
        Add a dummy ``File`` to the dummy database.

    Returns
    -------
    plantdb.fsdb.FSDB
        The dummy database.

    Examples
    --------
    >>> import os
    >>> from plantdb.fsdb import dummy_db
    >>> db = dummy_db()
    >>> db.connect()
    >>> print(db.is_connected)
    True
    >>> print(db.path())  # the database directory
    /tmp/romidb_********

    >>> db = dummy_db(with_scan=True)
    >>> db.connect()
    >>> scan = db.get_scan("myscan_001")  # get the created scan
    >>> print(type(scan))
    <class 'NoneType'>
    >>> print(os.listdir(db.path()))
    ['lock', 'romidb', 'myscan_001']
    >>> print(os.listdir(scan.path()))
    ['metadata']

    >>> db = dummy_db(with_fileset=True)
    >>> db.connect()
    >>> scan = db.get_scan("myscan_001")
    >>> print(os.listdir(scan.path()))
    ['metadata', 'files.json', 'fileset_001']
    >>> fs = scan.get_fileset("fileset_001")
    >>> print(type(fs))
    <class 'plantdb.fsdb.Fileset'>
    >>> print(list(fs.path().iterdir()))  # the fileset is empty as no file has been created
    []

    >>> db = dummy_db(with_file=True)
    >>> db.connect()
    >>> scan = db.get_scan("myscan_001")
    >>> fs = scan.get_fileset("fileset_001")
    >>> f = fs.get_file("test_image")
    >>> print(type(f))
    >>> print(list(fs.path().iterdir()))
    ['test_image.png', 'test_json.json']
    >>> print(f.path())

    """
    from tempfile import mkdtemp
    from plantdb import io

    mydb = Path(mkdtemp(prefix='romidb_'))
    marker_file = mydb / MARKER_FILE_NAME
    marker_file.open(mode='w').close()
    db = FSDB(mydb)

    if with_file:
        # To create a `File`, existing `Scan` & `Fileset` are required
        with_scan, with_fileset = True, True
    if with_fileset:
        # To create a `Fileset`, an existing `Scan` is required
        with_scan = True

    # Initialize an `FSDB` to add required objects:
    if with_scan or with_fileset or with_file:
        db.connect()

    # Create a `Scan` object if required:
    if with_scan:
        scan = db.create_scan("myscan_001")
        scan.set_metadata("test", 1)

    # Create a `Fileset` object if required:
    if with_fileset:
        fs = scan.create_fileset("fileset_001")
        fs.set_metadata("test_fileset", 1)

    # Create a `Fileset` object if required:
    if with_file:
        import numpy as np
        # -- Create a fixed dummy image:
        f = fs.create_file("dummy_image")
        img = np.array([[255, 0], [0, 255]]).astype('uint8')
        io.write_image(f, img, "png")
        f.set_metadata("dummy image", True)
        # -- Create a random RGB image:
        f = fs.create_file("test_image")
        rng = np.random.default_rng()
        img = np.array(255 * rng.random((50, 50, 3)), dtype='uint8')
        io.write_image(f, img, "png")
        f.set_metadata("random image", True)
        # -- Create a dummy JSON
        f = fs.create_file("test_json")
        md = {"Who you gonna call?": "Ghostbuster"}
        io.write_json(f, md, "json")
        f.set_metadata("random json", True)

    if with_scan or with_fileset or with_file:
        db.disconnect()

    return db


class NotAnFSDBError(Exception):
    def __init__(self, message):
        self.message = message


class FSDB(db.DB):
    """Implement a local *File System DataBase* version of abstract class ``db.DB``.

    Implement as a simple local file structure with following directory structure and marker files:
      * directory ``${FSDB.basedir}`` as database root directory;
      * marker file ``MARKER_FILE_NAME`` at database root directory;
      * (OPTIONAL) lock file ``LOCK_FILE_NAME`` at database root directory when connected;

    Attributes
    ----------
    basedir : pathlib.Path
        Absolute path to the base directory hosting the database.
    lock_path : pathlib.Path
        Absolute path to the lock file.
    scans : list
        The list of ``Scan`` objects found in the database.
    is_connected : bool
        ``True`` if the database is connected (locked directory), else ``False``.

    Notes
    -----
    Requires the marker file ``MARKER_FILE_NAME`` at the given ``basedir``.
    Lock file ``LOCK_FILE_NAME`` is found only when connecting an FSBD instance to the given ``basedir``.

    See Also
    --------
    plantdb.db.DB
    plantdb.fsdb.MARKER_FILE_NAME
    plantdb.fsdb.LOCK_FILE_NAME

    Examples
    --------
    >>> # EXAMPLE 1: Use a temporary dummy local database:
    >>> from plantdb.fsdb import dummy_db
    >>> db = dummy_db()
    >>> print(type(db))
    <class 'plantdb.fsdb.FSDB'>
    >>> print(db.path())
    /tmp/romidb_********
    >>> # Now connecting to this dummy local database...
    >>> db.connect()
    >>> # ...allows creating new `Scan` in it:
    >>> new_scan = db.create_scan("007")
    >>> print(type(new_scan))
    <class 'plantdb.fsdb.Scan'>
    >>> db.disconnect()

    >>> # EXAMPLE 2: Use a local database:
    >>> import os
    >>> from plantdb.fsdb import FSDB
    >>> db = FSDB(os.environ.get('ROMI_DB', "/data/ROMI/DB/"))
    >>> db.connect()
    >>> [scan.id for scan in db.get_scans()]  # list scan ids found in database
    >>> scan = db.get_scans()[1]
    >>> [fs.id for fs in scan.get_filesets()]  # list fileset ids found in scan
    >>> db.disconnect()

    """

    def __init__(self, basedir):
        """Database constructor.

        Check given ``basedir`` directory exists and load accessible ``Scan`` objects.

        Parameters
        ----------
        basedir : str or pathlib.Path
            The path to the root directory of the database.

        Raises
        ------
        NotADirectoryError
            If the given `basedir` is not an existing directory.
        NotAnFSDBError
            If the `MARKER_FILE_NAME` is missing from the `basedir`.

        See Also
        --------
        plantdb.fsdb.MARKER_FILE_NAME
        """
        super().__init__()

        basedir = Path(basedir)
        # Check the given path to root directory of the database is a directory:
        if not basedir.is_dir():
            raise NotADirectoryError(f"Directory {basedir} does not exists!")
        # Check the given path to root directory of the database is a "romi DB", i.e. have the `MARKER_FILE_NAME`:
        if not _is_fsdb(basedir):
            raise NotAnFSDBError(f"Not an FSDB! Check that there is a file named {MARKER_FILE_NAME} in {basedir}")

        # Initialize attributes:
        self.basedir = Path(basedir).resolve()
        self.lock_path = self.basedir / LOCK_FILE_NAME
        self.scans = []
        self.is_connected = False

    def connect(self, login_data=None, unsafe=False):
        """Connect to the local database.

        Handle DB "locking" system by adding a `LOCK_FILE_NAME` file in the DB.

        Parameters
        ----------
        login_data : bool
            UNUSED
        unsafe : bool
            If ``True`` do not use the `LOCK_FILE_NAME` file.

        Raises
        ------
        DBBusyError
            If the `LOCK_FILE_NAME` lock fil is found in the `basedir`.

        See Also
        --------
        plantdb.fsdb.LOCK_FILE_NAME

        Examples
        --------
        >>> from plantdb.fsdb import FSDB
        >>> from plantdb.fsdb import dummy_db
        >>> db = dummy_db()
        >>> print(db.is_connected)
        False
        >>> db.connect()
        >>> print(db.is_connected)
        True
        >>> db.disconnect()

        """
        if not self.is_connected:
            if unsafe:
                self.scans = _load_scans(self)
                self.is_connected = True
            else:
                try:
                    with self.lock_path.open(mode="x") as _:
                        self.scans = _load_scans(self)
                        self.is_connected = True
                    atexit.register(self.disconnect)
                except FileExistsError:
                    raise DBBusyError(f"File {LOCK_FILE_NAME} exists in DB root: DB is busy, cannot connect.")
        else:
            logger.info(f"Already connected to the database '{self.path()}'")
        return

    def disconnect(self):
        """Disconnect from the local database.

        Handle DB "locking" system by removing the `LOCK_FILE_NAME` file from the DB.

        Raises
        ------
        IOError
            If the `LOCK_FILE_NAME` cannot be removed using the ``lock_path`` attribute.

        Examples
        --------
        >>> from plantdb.fsdb import dummy_db
        >>> db = dummy_db()
        >>> print(db.is_connected)
        False
        >>> db.connect()
        >>> print(db.is_connected)
        True
        >>> db.disconnect()
        >>> print(db.is_connected)
        False

        """
        if self.is_connected:
            for s in self.scans:
                s._erase()
            if _is_safe_to_delete(self.lock_path):
                self.lock_path.unlink(missing_ok=True)
                atexit.unregister(self.disconnect)
            else:
                raise IOError("Could not remove lock, maybe you messed with the `lock_path` attribute?")
            self.scans = []
            self.is_connected = False
        else:
            logger.info(f"Already disconnected from the database '{self.path()}'")
        return

    def reload(self):
        """Reload the database by scanning datasets."""
        if self.is_connected:
            logger.error("Reloading the database...")
            self.scans = _load_scans(self)
            logger.info("Done!")
        else:
            logger.error(f"You are not connected to the database!")

    def get_scans(self, query=None):
        """Get the list of `Scan` instances defined in the local database, possibly filtered using a `query`.

        Parameters
        ----------
        query : dict, optional
            Query to use to get a list of scans.

        Returns
        -------
        list of plantdb.fsdb.Scan
            List of `Scan`s, filtered by the `query` if any.

        See Also
        --------
        plantdb.fsdb._filter_query

        Examples
        --------
        >>> from plantdb.fsdb import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> db.connect()
        >>> db.get_scans()
        [<plantdb.fsdb.Scan at *x************>]
        >>> db.disconnect()

        """
        return _filter_query(self.scans, query)

    def get_scan(self, id, create=False):
        """Get or create a `Scan` instance in the local database.

        Parameters
        ----------
        id : str
            The name of the scan dataset to get/create.
            It should exist if `create` is `False`.
        create : bool, optional
            If ``False`` (default), the given `id` should exist in the local database.
            Else the given `id` should NOT exist as they are unique.

        Notes
        -----
        If the `id` do not exist in the local database and `create` is `False`, `None` is returned.

        Examples
        --------
        >>> from plantdb.fsdb import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> db.connect()
        >>> db.list_scans()
        ['myscan_001']
        >>> new_scan = db.get_scan('007', create=True)
        >>> print(new_scan)
        <plantdb.fsdb.Scan object at **************>
        >>> db.list_scans()
        ['myscan_001', '007']
        >>> unknown_scan = db.get_scan('unknown')
        >>> print(unknown_scan)
        None
        >>> db.disconnect()

        """
        ids = [f.id for f in self.scans]
        if id not in ids:
            if create:
                return self.create_scan(id)
            return None
        return self.scans[ids.index(id)]

    def create_scan(self, id):
        """Create a new `Scan` instance in the local database.

        Parameters
        ----------
        id : str
            The name of the scan to create. It should not exist in the local database.

        Returns
        -------
        plantdb.fsdb.Scan
            The `Scan` instance created in the local database.

        Raises
        ------
        IOError
            If the `id` already exists in the local database.
            If the `id` is not valid.

        See Also
        --------
        plantdb.fsdb._is_valid_id
        plantdb.fsdb._make_scan

        Examples
        --------
        >>> from plantdb.fsdb import dummy_db
        >>> db = dummy_db()
        >>> db.connect()
        >>> new_scan = db.create_scan('007')
        >>> scan = db.create_scan('007')
        OSError: Duplicate scan name: 007
        >>> db.disconnect()

        """
        if not _is_valid_id(id):
            raise IOError("Invalid id")
        if self.get_scan(id) != None:
            raise IOError(f"Duplicate scan name: {id}")
        scan = Scan(self, id)
        _make_scan(scan)
        self.scans.append(scan)
        return scan

    def delete_scan(self, id):
        """Delete an existing `Scan` from the local database.

        Parameters
        ----------
        id : str
            The name of the scan to create. It should exist in the local database.

        Raises
        ------
        IOError
            If the `id` do not exist in the local database.

        See Also
        --------
        plantdb.fsdb._delete_scan

        Examples
        --------
        >>> from plantdb.fsdb import FSDB
        >>> from plantdb.fsdb import dummy_db
        >>> db = dummy_db()
        >>> db.connect()
        >>> new_scan = db.create_scan('007')
        >>> print(new_scan)
        <plantdb.fsdb.Scan object at 0x7f0730b1e390>
        >>> db.delete_scan('007')
        >>> scan = db.get_scan('007')
        >>> print(scan)
        None
        >>> db.delete_scan('008')
        OSError: Invalid id
        >>> db.disconnect()

        """
        scan = self.get_scan(id)
        if scan is None:
            raise IOError("Invalid id")
        _delete_scan(scan)
        self.scans.remove(scan)
        return

    def path(self) -> pathlib.Path:
        """Get the path to the local database root directory.

        Examples
        --------
        >>> from plantdb.fsdb import dummy_db
        >>> db = dummy_db(with_scan=True, with_file=True)
        >>> db.connect()
        >>> db.path()
        >>> db.disconnect()

        """
        return copy.deepcopy(self.basedir)

    def list_scans(self, query=None) -> list:
        """Get the list of scans in the local database.

        Examples
        --------
        >>> from plantdb.fsdb import dummy_db
        >>> db = dummy_db(with_scan=True, with_file=True)
        >>> db.connect()
        >>> db.list_scans()
        ['myscan_001']
        >>> db.disconnect()

        """
        return [f.id for f in self.get_scans(query)]


class Scan(db.Scan):
    """Implement ``Scan`` for the local *File System DataBase* from abstract class ``db.Scan``.

    Implementation of a scan as a simple file structure with:
      * directory ``${Scan.db.basedir}/${Scan.db.id}`` as scan root directory;
      * (OPTIONAL) directory ``${Scan.db.basedir}/${Scan.db.id}/metadata`` containing JSON metadata file
      * (OPTIONAL) JSON file ``metadata.json`` with Scan metadata

    Attributes
    ----------
    db : plantdb.fsdb.FSDB
        Database where to find the scan.
    id : str
        The scan unique name in the local database.
    metadata : dict
        Dictionary of metadata attached to the scan.
    filesets : list of plantdb.fsdb.Fileset
        List of ``Fileset`` objects.

    Notes
    -----
    Optional directory ``metadata`` & JSON file ``metadata.json`` are found when using method ``set_metadata()``.

    See Also
    --------
    plantdb.db.Scan

    Examples
    --------
    >>> import os
    >>> from plantdb.fsdb import Scan
    >>> from plantdb.fsdb import dummy_db
    >>> db = dummy_db()
    >>> # Example #1: Initialize a `Scan` object using an `FSBD` object:
    >>> scan = Scan(db, '007')
    >>> print(type(scan))
    <class 'plantdb.fsdb.Scan'>
    >>> print(scan.path())  # the obtained path should be different as the path to the created `dummy_db` change...
    /tmp/romidb_j0pbkoo0/007
    >>> print(db.get_scan('007'))  # Note that it did NOT create this `Scan` in the database!
    None
    >>> print(os.listdir(db.path()))  # And it is NOT found under the `basedir` directory
    ['romidb']
    >>> # HOWEVER if you add metadata to the `Scan` object:
    >>> scan.set_metadata({'Name': "Bond... James Bond!"})
    >>> print(scan.metadata)
    {'Name': 'Bond... James Bond!'}
    >>> print(db.get_scan('007'))  # The `Scan` is still not found in the database!
    None
    >>> print(os.listdir(db.path()))  # BUT it is now found under the `basedir` directory
    ['007', 'romidb']
    >>> print(os.listdir(os.path.join(db.path(), scan.id)))  # Same goes for the metadata
    ['metadata']
    >>> print(os.listdir(os.path.join(db.path(), scan.id, "metadata")))  # Same goes for the metadata
    >>> db.disconnect()

    >>> # Example #2: Get it from an `FSDB` object:
    >>> db = dummy_db()
    >>> scan = db.get_scan('007', create=True)
    >>> print(type(scan))
    <class 'plantdb.fsdb.Scan'>
    >>> print(db.get_scan('007'))  # This time the `Scan` object is found in the `FSBD`
    <plantdb.fsdb.Scan object at 0x7f34fc860fd0>
    >>> print(os.listdir(db.path()))  # And it is found under the `basedir` directory
    ['007', 'romidb']
    >>> print(os.listdir(os.path.join(db.path(), scan.id)))  # Same goes for the metadata
    ['metadata']
    >>> db.disconnect()
    >>> # When reconnecting to db, if created scan is EMPTY (no Fileset & File) it is not found!
    >>> db.connect()
    >>> print(db.get_scan('007'))
    None

    >>> # Example #3: Use an existing database:
    >>> from os import environ
    >>> from plantdb.fsdb import FSDB
    >>> db = FSDB(environ.get('ROMI_DB', "/data/ROMI/DB/"))
    >>> db.connect(unsafe=True)
    >>> scan = db.get_scan('sango_90_300_36')
    >>> scan.get_metadata()

    """

    def __init__(self, db, id):
        """Scan dataset constructor.

        Parameters
        ----------
        db : plantdb.fsdb.FSDB
            The database to put/find the scan dataset.
        id : str
            The scan dataset name, should be unique in the `db`.
        """
        super().__init__(db, id)
        # Defines attributes:
        self.metadata = None
        self.measures = None
        self.filesets = []
        return

    def _erase(self):
        """Erase the filesets and metadata associated to this scan."""
        for f in self.filesets:
            f._erase()
        del self.metadata
        del self.filesets
        return

    def get_filesets(self, query=None):
        """Get the list of `Fileset` instances defined in the current scan dataset, possibly filtered using a `query`.

        Parameters
        ----------
        query : dict, optional
            Query to use to get a list of filesets.

        Returns
        -------
        list of plantdb.fsdb.Fileset
            List of `Fileset`s, filtered by the `query` if any.

        See Also
        --------
        plantdb.fsdb._filter_query

        Examples
        --------
        >>> from plantdb.fsdb import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> db.connect()
        >>> scan = db.get_scan('myscan_001')
        >>> scan.get_filesets()
        [<plantdb.fsdb.Fileset at *x************>]
        >>> db.disconnect()

        """
        return _filter_query(self.filesets, query)

    def get_fileset(self, id, create=False):
        """Get or create a `Fileset` instance, of given `id`, in the current scan dataset.

        Parameters
        ----------
        id : str
            The name of the fileset to get/create.
            It should exist if `create` is `False`.
        create : bool, optional
            If ``False`` (default), the given `id` should exist in the local database.
            Else the given `id` should NOT exist as they are unique.

        Returns
        -------
        Fileset
            The retrieved or created fileset.

        Notes
        -----
        If the `id` do not exist in the local database and `create` is `False`, `None` is returned.

        Examples
        --------
        >>> from plantdb.fsdb import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> db.connect()
        >>> scan = db.get_scan('myscan_001')
        >>> scan.list_filesets()
        ['fileset_001']
        >>> new_fileset = scan.get_fileset('007', create=True)
        >>> print(new_fileset)
        <plantdb.fsdb.Fileset object at **************>
        >>> scan.list_filesets()
        ['fileset_001', '007']
        >>> unknown_fs = scan.get_fileset('unknown')
        >>> print(unknown_fs)
        None
        >>> db.disconnect()

        """
        ids = [f.id for f in self.filesets]
        if id not in ids:
            if create:
                return self.create_fileset(id)
            return None
        return self.filesets[ids.index(id)]

    def get_metadata(self, key=None):
        """Get the metadata associated to a scan.

        Parameters
        ----------
        key : str
            A key that should exist in the scan's metadata.

        Returns
        -------
        any
            If `key` is ``None``, returns a dictionary.
            Else, returns the value attached to this key.

        """
        return _get_metadata(self.metadata, key)

    def get_measures(self, key=None):
        """Get the manual measurements associated to a scan.

        Parameters
        ----------
        key : str
            A key that should exist in the scan's manual measurements.

        Returns
        -------
        any
            If `key` is ``None``, returns a dictionary.
            Else, returns the value attached to this key.

        Notes
        -----
        These manual measurements should be a JSON file named `measures.json`.
        It is located at the root folder of the scan dataset.

        """
        return _get_metadata(self.measures, key)

    def set_metadata(self, data, value=None):
        """Add a new metadata to the scan.

        Parameters
        ----------
        data : str or dict
            If a string, a key to address the `value`.
            If a dictionary, update the metadata dictionary with `data` (`value` is then unused).
        value : any, optional
            The value to assign to `data` if the latest is not a dictionary.

        Examples
        --------
        >>> import json
        >>> from plantdb.fsdb import dummy_db
        >>> from plantdb.fsdb import _scan_metadata_path
        >>> db = dummy_db(with_file=True)
        >>> db.connect()
        >>> scan = db.get_scan("myscan_001")
        >>> scan.set_metadata("test", "value")
        >>> p = _scan_metadata_path(scan)
        >>> print(p.exists())
        True
        >>> print(json.load(p.open(mode='r')))
        {'test': 'value'}
        >>> db.disconnect()

        """
        if self.metadata == None:
            self.metadata = {}
        _set_metadata(self.metadata, data, value)
        _store_scan_metadata(self)
        return

    def create_fileset(self, id):
        """Create a new `Fileset` instance in the local database attached to the current `Scan` instance.

        Parameters
        ----------
        id : str
            The name of the fileset to create. It should not exist in the current `Scan` instance.

        Returns
        -------
        plantdb.fsdb.Fileset
            The `Fileset` instance created in the current `Scan` instance.

        Raises
        ------
        IOError
            If the `id` already exists in the current `Scan` instance.
            If the `id` is not valid.

        See Also
        --------
        plantdb.fsdb._is_valid_id
        plantdb.fsdb._make_fileset

        Examples
        --------
        >>> from plantdb.fsdb import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> db.connect()
        >>> scan = db.get_scan('myscan_001')
        >>> scan.list_filesets()
        ['fileset_001']
        >>> new_fs = scan.create_fileset('fs_007')
        >>> scan.list_filesets()
        ['fileset_001', 'fs_007']
        >>> wrong_fs = scan.create_fileset('fileset_001')
        OSError: Duplicate scan name: fileset_001
        >>> db.disconnect()

        """
        if not _is_valid_id(id):
            raise IOError(f"Invalid fileset id: {id}")
        if self.get_fileset(id) != None:
            raise IOError(f"Duplicate fileset name: {id}")
        fileset = Fileset(self, id)
        _make_fileset(fileset)
        self.filesets.append(fileset)
        self.store()
        return fileset

    def store(self):
        """Save changes to the scan main JSON FILE (files.json)."""
        _store_scan(self)
        logger.debug(f"The `files.json` file for scan '{self.id}' has been updated!")
        return

    def delete_fileset(self, fileset_id):
        """Delete a given fileset from the scan dataset.

        Parameters
        ----------
        fileset_id : str
            Name of the fileset to delete.

        Examples
        --------
        >>> from plantdb.fsdb import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> db.connect()
        >>> scan = db.get_scan('myscan_001')
        >>> scan.list_filesets()
        ['fileset_001']
        >>> scan.delete_fileset('fileset_001')
        >>> scan.list_filesets()
        []
        >>> db.disconnect()

        """
        fs = self.get_fileset(fileset_id)
        if fs is None:
            logging.warning(f"Could not get the Fileset to delete: '{fileset_id}'!")
            return
        _delete_fileset(fs)
        self.filesets.remove(fs)
        self.store()
        return

    def path(self) -> pathlib.Path:
        """Get the path to the local scan dataset.

        Examples
        --------
        >>> from plantdb.fsdb import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> db.connect()
        >>> scan = db.get_scan("myscan_001")
        >>> scan.path()  # should be '/tmp/romidb_********/myscan_001'
        >>> db.disconnect()

        """
        return _scan_path(self)

    def list_filesets(self, query=None) -> list:
        """Get the list of filesets in the scan dataset.

        Examples
        --------
        >>> from plantdb.fsdb import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> db.connect()
        >>> scan = db.get_scan("myscan_001")
        >>> scan.list_filesets()
        ['fileset_001']
        >>> db.disconnect()

        """
        return [f.id for f in self.get_filesets(query)]


class Fileset(db.Fileset):
    """Implement ``Fileset`` for the local *File System DataBase* from abstract class ``db.Fileset``.

    Implementation of a fileset as a simple files structure with:
      * directory ``${FSDB.basedir}/${FSDB.scan.id}/${Fileset.id}`` containing set of files;
      * directory ``${FSDB.basedir}/${FSDB.scan.id}/metadata`` containing JSON metadata associated to files;
      * JSON file ``files.json`` containing the list of files from fileset;

    Attributes
    ----------
    db : plantdb.fsdb.FSDB
        Database where to find the `scan`.
    id : str
        Name of the scan in the database `db`.
    scan : plantdb.fsdb.Scan
        Scan containing the set of files of interest.
    metadata : dict
        Dictionary of metadata attached to the fileset.
    files : list of plantdb.fsdb.File
        List of `File` objects.

    See Also
    --------
    plantdb.db.Fileset

    """

    def __init__(self, scan, id):
        """Constructor.

        Parameters
        ----------
        scan : plantdb.fsdb.Scan
            Scan instance containing the fileset.
        id : str
            Id of the fileset instance.
        """
        super().__init__(scan, id)
        self.metadata = None
        self.files = []

    def _erase(self):
        """Erase the files and metadata associated to this fileset."""
        for f in self.files:
            f._erase()
        self.metadata = None
        self.files = None
        return

    def get_files(self, query=None):
        """Get the list of `File` instances defined in the current fileset, possibly filtered using a `query`.

        Parameters
        ----------
        query : dict, optional
            Query to use to get a list of files.

        Returns
        -------
        list of plantdb.fsdb.File
            List of `File`s, filtered by the query if any.

        See Also
        --------
        plantdb.fsdb._filter_query

        Examples
        --------
        >>> from plantdb.fsdb import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> db.connect()
        >>> scan = db.get_scan('myscan_001')
        >>> fs = scan.get_fileset('fileset_001')
        >>> fs.get_files()
        [<plantdb.fsdb.File at *x************>,
         <plantdb.fsdb.File at *x************>,
         <plantdb.fsdb.File at *x************>]
        >>> db.disconnect()

        """
        return _filter_query(self.files, query)

    def get_file(self, id, create=False):
        """Get or create a `File` instance, of given `id`, in the current fileset.

        Parameters
        ----------
        id : str
            Name of the file to get/create.
        create : bool
            If ``False`` (default), the given `id` should exist in the local database.
            Else the given `id` should NOT exist as they are unique.

        Returns
        -------
        plantdb.fsdb.File
            The retrieved or created file.

        Examples
        --------
        >>> from plantdb.fsdb import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> db.connect()
        >>> scan = db.get_scan("myscan_001")
        >>> fs = scan.get_fileset("fileset_001")
        >>> f = fs.get_file("test_image")
        >>> # To read the file you need to load the right reader from plantdb.io
        >>> from plantdb.io import read_image
        >>> img = read_image(f)
        >>> db.disconnect()

        """
        ids = [f.id for f in self.files]
        if id not in ids:
            if create:
                return self.create_file(id)
            return None
        return self.files[ids.index(id)]

    def get_metadata(self, key=None):
        """Get the metadata associated to a fileset.

        Parameters
        ----------
        key : str
            A key that should exist in the fileset's metadata.

        Returns
        -------
        any
            If `key` is ``None``, returns a dictionary.
            Else, returns the value attached to this key.

        Examples
        --------
        >>> import json
        >>> from plantdb.fsdb import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> db.connect()
        >>> scan = db.get_scan("myscan_001")
        >>> fs = scan.get_fileset('fileset_001')
        >>> fs.set_metadata("test", "value")
        >>> print(fs.get_metadata("test"))
        'value'
        >>> db.disconnect()
        >>> db.connect()
        >>> scan = db.get_scan("myscan_001")
        >>> fs = scan.get_fileset('fileset_001')
        >>> print(fs.get_metadata("test"))
        'value'

        """
        return _get_metadata(self.metadata, key)

    def set_metadata(self, data, value=None):
        """Add a new metadata to the fileset.

        Parameters
        ----------
        data : str or dict
            If a string, a key to address the `value`.
            If a dictionary, update the metadata dictionary with `data` (`value` is then unused).
        value : any, optional
            The value to assign to `data` if the latest is not a dictionary.

        Examples
        --------
        >>> import json
        >>> from plantdb.fsdb import dummy_db
        >>> from plantdb.fsdb import _fileset_metadata_json_path
        >>> db = dummy_db(with_file=True)
        >>> db.connect()
        >>> scan = db.get_scan("myscan_001")
        >>> fs = scan.get_fileset('fileset_001')
        >>> fs.set_metadata("test", "value")
        >>> p = _fileset_metadata_json_path(fs)
        >>> print(p.exists())
        True
        >>> print(json.load(p.open(mode='r')))
        {'test': 'value'}
        >>> db.disconnect()

        """
        if self.metadata == None:
            self.metadata = {}
        _set_metadata(self.metadata, data, value)
        _store_fileset_metadata(self)
        return

    def create_file(self, id):
        """Create a new `File` instance in the local database attached to the current `Fileset` instance.

        Parameters
        ----------
        id : str
            The name of the file to create.

        Returns
        -------
        plantdb.fsdb.File
            The `File` instance created in the current `Fileset` instance.

        Examples
        --------
        >>> from plantdb.fsdb import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> db.connect()
        >>> scan = db.get_scan('myscan_001')
        >>> fs = scan.get_fileset('fileset_001')
        >>> fs.list_files()
        ['dummy_image', 'test_image', 'test_json']
        >>> new_f = fs.create_file('file_007')
        >>> fs.list_files()
        ['dummy_image', 'test_image', 'test_json', 'file_007']
        >>> import os
        >>> os.listdir(fs.path())  # the file only exist in the database, not on drive!
        ['test_image.png', 'test_json.json', 'dummy_image.png']
        >>> md = {"Name": "Bond, James Bond"}  # Create an example dictionary to save as JSON
        >>> from plantdb import io
        >>> io.write_json(new_f, md, "json")  # write the file on drive
        >>> os.listdir(fs.path())
        ['file_007.json', 'test_image.png', 'test_json.json', 'dummy_image.png']
        >>> db.disconnect()

        """
        file = File(self, id)
        self.files.append(file)
        self.store()
        return file

    def delete_file(self, file_id):
        """Delete a given file from the current fileset.

        Parameters
        ----------
        fileset_id : str
            Name of the file to delete.

        Examples
        --------
        >>> from plantdb.fsdb import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> db.connect()
        >>> scan = db.get_scan('myscan_001')
        >>> fs = scan.get_fileset('fileset_001')
        >>> fs.list_files()
        ['dummy_image', 'test_image', 'test_json']
        >>> fs.delete_file('dummy_image')
        delete /tmp/romidb_********/myscan_001/fileset_001/dummy_image.png
        >>> fs.list_files()
        ['test_image', 'test_json']
        >>> import os
        >>> list(fs.path().iterdir())  # the file has been removed from the drive and the database
        ['test_image.png', 'test_json.json']
        >>> db.disconnect()

        """
        x = self.get_file(file_id, create=False)
        if x is None:
            raise IOError(f"Invalid file ID: {file_id}")
        _delete_file(x)
        self.files.remove(x)
        self.store()
        return

    def store(self):
        """Save changes to the scan main JSON FILE (files.json)."""
        self.scan.store()
        return

    def path(self) -> pathlib.Path:
        """Get the path to the local fileset.

        Examples
        --------
        >>> from plantdb.fsdb import dummy_db
        >>> db = dummy_db(with_scan=True, with_file=True)
        >>> db.connect()
        >>> [scan.id for scan in db.get_scans()]  # list scan ids found in database
        ['myscan_001']
        >>> scan = db.get_scan("myscan_001")
        >>> print(scan.path())  # should be '/tmp/romidb_********/myscan_001'
        >>> [fs.id for fs in scan.get_filesets()]  # list fileset ids found in scan
        ['fileset_001']
        >>> fs = scan.get_fileset("fileset_001")
        >>> print(fs.path())  # should be '/tmp/romidb_********/myscan_001/fileset_001'
        >>> db.disconnect()

        """
        return _fileset_path(self)

    def list_files(self, query=None) -> list:
        """Get the list of files in the fileset.

        Examples
        --------
        >>> from plantdb.fsdb import dummy_db
        >>> db = dummy_db(with_scan=True, with_file=True)
        >>> db.connect()
        >>> scan = db.get_scan("myscan_001")
        >>> fs = scan.get_fileset("fileset_001")
        >>> fs.list_files()
        ['dummy_image', 'test_image', 'test_json']
        >>> db.disconnect()

        """
        return [f.id for f in self.get_files(query)]


class File(db.File):
    """Implement ``File`` for the local *File System DataBase* from abstract class ``db.File``.

    Attributes
    ----------
    db : plantdb.fsdb.FSDB
        Database where to find the fileset.
    fileset : plantdb.fsdb.Fileset
        Set of files containing the file.
    id : str
        Name of the file in the ``FSDB`` local database.
    filename : str
        File name.
    metadata : dict
        Dictionary of metadata attached to the file.

    See Also
    --------
    plantdb.db.File

    Notes
    -----
    `File` must be writen using ``write_raw`` or ``write`` methods to exist on disk.
    Else they are just referenced in the database!

    Contrary to other classes (``Scan`` & ``Fileset``) the uniqueness is not checked!
    """

    def __init__(self, fileset, id, **kwargs):
        super().__init__(fileset, id, **kwargs)
        self.metadata = None

    def _erase(self):
        self.id = None
        self.metadata = None
        return

    def get_metadata(self, key=None):
        """Get the metadata associated to a file.

        Parameters
        ----------
        key : str
            A key that should exist in the file's metadata.

        Returns
        -------
        any
            If `key` is ``None``, returns a dictionary.
            Else, returns the value attached to this key.

        Examples
        --------
        >>> from plantdb.fsdb import dummy_db
        >>> from plantdb.fsdb import _file_metadata_path
        >>> db = dummy_db(with_file=True)
        >>> db.connect()
        >>> scan = db.get_scan("myscan_001")
        >>> fs = scan.get_fileset('fileset_001')
        >>> f = fs.get_file("test_json")
        >>> print(f.get_metadata())
        {'random json': True}
        >>> db.disconnect()

        """
        return _get_metadata(self.metadata, key)

    def set_metadata(self, data, value=None):
        """Add a new metadata to the file.

        Parameters
        ----------
        data : str or dict
            If a string, a key to address the `value`.
            If a dictionary, update the metadata dictionary with `data` (`value` is then unused).
        value : any, optional
            The value to assign to `data` if the latest is not a dictionary.

        Examples
        --------
        >>> import json
        >>> from plantdb.fsdb import dummy_db
        >>> from plantdb.fsdb import _file_metadata_path
        >>> db = dummy_db(with_file=True)
        >>> db.connect()
        >>> scan = db.get_scan("myscan_001")
        >>> fs = scan.get_fileset('fileset_001')
        >>> file = fs.get_file("test_json")
        >>> file.set_metadata("test", "value")
        >>> p = _file_metadata_path(file)
        >>> print(p.exists())
        True
        >>> print(json.load(p.open(mode='r')))
        {'random json': True, 'test': 'value'}
        >>> db.disconnect()

        """
        if self.metadata == None:
            self.metadata = {}
        _set_metadata(self.metadata, data, value)
        _store_file_metadata(self)
        return

    def import_file(self, path):
        """Import the file from its local path to the current fileset.

        Parameters
        ----------
        path : str or pathlib.Path
            The path to the file to import.

        Examples
        --------
        >>> from plantdb.fsdb import dummy_db
        >>> from plantdb.fsdb import _file_metadata_path
        >>> db = dummy_db(with_file=True)
        >>> db.connect()
        >>> scan = db.get_scan("myscan_001")
        >>> fs = scan.get_fileset('fileset_001')
        >>> file = fs.get_file("test_json")
        >>> new_file = fs.create_file('test_json2')
        >>> new_file.import_file(file.path())
        >>> print(new_file.path().exists())
        True
        >>> db.disconnect()

        """
        if isinstance(path, str):
            path = Path(path)
        ext = path.suffix[1:]
        self.filename = _get_filename(self, ext)
        newpath = _file_path(self)
        copyfile(path, newpath)
        self.store()
        return

    def store(self):
        """Save changes to the scan main JSON FILE (files.json)."""
        self.fileset.store()
        return

    def read_raw(self):
        """Read the file and return its contents.

        Returns
        -------
        bytes
            The contents of the file.

        Example
        -------
        >>> from plantdb.fsdb import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> db.connect()
        >>> scan = db.get_scan("myscan_001")
        >>> fs = scan.get_fileset("fileset_001")
        >>> f = fs.get_file("test_json")
        >>> js = f.read_raw()
        >>> print(js)  # print the raw bytes content
        >>> # Convert this raw json into a dictionary with dedicated method from `json` library:
        >>> import json
        >>> js_dict = json.loads(js)
        >>> print(js_dict)
        {'Who you gonna call?': 'Ghostbuster'}
        >>> print(type(js_dict))
        <class 'dict'>
        >>> db.disconnect()

        """
        path = _file_path(self)
        with path.open(mode="rb") as f:
            return f.read()

    def write_raw(self, data, ext=""):
        """Write a file from raw byte data.

        Parameters
        ----------
        data : bytes
            The raw byte content to write.
        ext : str, optional
            The extension to use to save the file.

        Examples
        --------
        >>> from plantdb.fsdb import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> db.connect()
        >>> scan = db.get_scan("myscan_001")
        >>> fs = scan.get_fileset("fileset_001")
        >>> new_f = fs.create_file('file_007')
        >>> md = {"Name": "Bond, James Bond"}  # Create an example dictionary to save as JSON
        >>> import json
        >>> data = json.dumps(md).encode()
        >>> print(data)
        b'{"Name": "Bond, James Bond"}'
        >>> new_f.write_raw(data, 'json')
        >>> import os
        >>> os.listdir(fs.path())
        ['file_007.json', 'test_image.png', 'test_json.json', 'dummy_image.png']
        >>> db.disconnect()

        """
        self.filename = _get_filename(self, ext)
        path = _file_path(self)
        with path.open(mode="wb") as f:
            f.write(data)
        self.store()
        return

    def read(self):
        """Read the file and return its contents.

        Returns
        -------
        str
            The contents of the file.

        Examples
        --------
        >>> from plantdb.fsdb import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> db.connect()
        >>> scan = db.get_scan("myscan_001")
        >>> fs = scan.get_fileset("fileset_001")
        >>> f = fs.get_file("test_json")
        >>> js = f.read()
        >>> print(js)  # print the content of the file
        {
            "Who you gonna call?": "Ghostbuster"
        }
        >>> # Convert this raw json into a dictionary with dedicated method from `json` library:
        >>> import json
        >>> js_dict = json.loads(js)
        >>> print(js_dict)
        {'Who you gonna call?': 'Ghostbuster'}
        >>> print(type(js_dict))
        <class 'dict'>
        >>> db.disconnect()

        """
        path = _file_path(self)
        with path.open(mode="r") as f:
            return f.read()

    def write(self, data, ext=""):
        """Write a file from data.

        Parameters
        ----------
        data : str
            A string representation of the content to write.
        ext : str, optional
            The extension to use to save the file.

        Examples
        --------
        >>> from plantdb.fsdb import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> db.connect()
        >>> scan = db.get_scan("myscan_001")
        >>> fs = scan.get_fileset("fileset_001")
        >>> new_f = fs.create_file('file_007')
        >>> md = {"Name": "Bond, James Bond"}  # Create an example dictionary to save as JSON
        >>> import json
        >>> data = json.dumps(md)
        >>> print(data)
        {"Name": "Bond, James Bond"}
        >>> print(type(data))
        <class 'str'>
        >>> new_f.write(data, 'json')
        >>> import os
        >>> os.listdir(fs.path())
        ['file_007.json', 'test_image.png', 'test_json.json', 'dummy_image.png']
        >>> db.disconnect()

        """
        self.filename = _get_filename(self, ext)
        path = _file_path(self)
        with path.open(mode="w") as f:
            f.write(data)
        self.store()
        return

    def path(self) -> pathlib.Path:
        """Get the path to the local file.

        Examples
        --------
        >>> from plantdb.fsdb import dummy_db
        >>> db = dummy_db(with_scan=True, with_file=True)
        >>> db.connect()
        >>> scan = db.get_scan("myscan_001")
        >>> fs = scan.get_fileset("fileset_001")
        >>> fs.list_files()
        ['dummy_image', 'test_image', 'test_json']
        >>> f = fs.get_file('dummy_image')
        >>> f.path()  # should be '/tmp/romidb_********/myscan_001/fileset_001/dummy_image.png'
        >>> db.disconnect()

        """
        return _file_path(self)


##################################################################
#
# the ugly stuff...
#

# load the database

def _load_scans(db):
    """Load list of ``Scan`` from given database.

    List subdirectories of ``db.basedir`` with an `'images'` directory.

    Parameters
    ----------
    db : plantdb.fsdb.FSDB
        The database object to use to get the list of ``fsdb.Scan``

    Returns
    -------
    list of plantdb.fsdb.Scan
         The list of ``fsdb.Scan`` found in the database.

    See Also
    --------
    plantdb.fsdb._scan_path
    plantdb.fsdb._scan_files_json
    plantdb.fsdb._load_scan_filesets
    plantdb.fsdb._load_scan_metadata

    Examples
    --------
    >>> from plantdb.fsdb import FSDB
    >>> from plantdb.fsdb import dummy_db, _load_scans
    >>> db = dummy_db()
    >>> db.connect()
    >>> db.create_scan("007")
    >>> db.create_scan("111")
    >>> scans = _load_scans(db)
    >>> print(scans)
    []
    >>> db = dummy_db(with_fileset=True)
    >>> db.connect()
    >>> scans = _load_scans(db)
    >>> print(scans)
    [<plantdb.fsdb.Scan object at 0x7fa01220bd50>]

    """
    scans = []
    names = os.listdir(db.path())
    for name in tqdm(names, unit="scan"):
        scan = Scan(db, name)
        scan_path = _scan_path(scan)
        scan_image_path = scan_path / 'images'
        if scan_path.is_dir() and scan_image_path.is_dir():
            # If the `files.json` associated to the scan is found, parse it:
            if _scan_json_file(scan).is_file():
                scan.filesets = _load_scan_filesets(scan)
                scan.metadata = _load_scan_metadata(scan)
                scan.measures = _load_scan_measures(scan)
            scans.append(scan)
    return scans


def _load_scan_filesets(scan):
    """Load the list of ``Fileset`` from given `scan` dataset.

    Load the list of filesets using "filesets" top-level entry from ``files.json``.

    Parameters
    ----------
    scan : plantdb.fsdb.Scan
        The instance to use to get the list of ``Fileset``.

    Returns
    -------
    list of plantdb.fsdb.Fileset
         The list of ``Fileset`` found in the scan.

    See Also
    --------
    plantdb.fsdb._scan_files_json
    plantdb.fsdb._load_scan_filesets

    Notes
    -----
    May delete a fileset if unable to load it!

    Examples
    --------
    >>> from plantdb.fsdb import FSDB
    >>> from plantdb.fsdb import dummy_db, _load_scan_filesets
    >>> db = dummy_db(with_fileset=True)
    >>> db.connect()
    >>> scan = db.get_scan("myscan_001")
    >>> fs = _load_scan_filesets(scan)
    >>> print(fs)
    [<plantdb.fsdb.Fileset object at 0x7fa0122232d0>]

    """
    filesets = []
    # Get the path to the `files.json` associated to the `scan`:
    files_json = _scan_json_file(scan)
    # Load it:
    with files_json.open(mode="r") as f:
        structure = json.load(f)
    # Get the list of info (dict) about the filesets
    filesets_info = structure["filesets"]
    if isinstance(filesets_info, list):
        for n, fileset_info in enumerate(filesets_info):
            try:
                fileset = _load_fileset(scan, fileset_info)
            except FilesetNoIDError:
                logger.error(f"Could not get an 'id' entry for the {n}-th 'filesets' entry from '{scan.id}'.")
                logger.debug(f"Current `fileset_info`: {fileset_info}")
            except FilesetNotFoundError:
                fsid = fileset_info.get("id", None)
                logger.error(f"Fileset directory '{fsid}' not found for scan {scan.id}, skip it.")
            else:
                filesets.append(fileset)
    else:
        raise IOError(f"Could not find a list of filesets in '{files_json}'.")
    return filesets


def _load_fileset(scan, fileset_info):
    """Load a fileset and set its attributes.

    Parameters
    ----------
    scan : plantdb.fsdb.Scan
        The scan object to use to get the list of ``fsdb.Fileset``
    fileset_info: dict
        Dictionary with the fileset id and listing its files, ``{'files': [], 'id': str}``.

    Returns
    -------
    plantdb.fsdb.Fileset
        A fileset with its ``files`` & ``metadata`` attributes restored.

    Examples
    --------
    >>> import json
    >>> from plantdb.fsdb import dummy_db, _load_fileset, _scan_json_file
    >>> db = dummy_db(with_file=True)
    >>> db.connect()
    >>> scan = db.get_scan("myscan_001")
    >>> db.disconnect()
    >>> json_path = _scan_json_file(scan)
    >>> with json_path.open(mode="r") as f: structure = json.load(f)
    >>> filesets_info = structure["filesets"]
    >>> fs = _load_fileset(scan, filesets_info[0])
    >>> print(fs.id)
    fileset_001
    >>> print([f.id for f in files])
    ['dummy_image', 'test_image', 'test_json']

    """
    fileset = _parse_fileset(scan, fileset_info)
    fileset.files = _load_fileset_files(fileset, fileset_info)
    fileset.metadata = _load_fileset_metadata(fileset)
    return fileset


class FilesetNoIDError(Exception):
    """No 'id' entry could be found for this fileset."""


class FilesetNotFoundError(Exception):
    """Could not find the fileset directory."""


def _parse_fileset(scan, fileset_info):
    """Get a `Fileset` instance for given `db` & `scan` by parsing provided `fileset_info`.

    Parameters
    ----------
    db : plantdb.fsdb.FSDB
        The database instance to associate the returned ``Fileset`` to.
    scan : plantdb.fsdb.Scan
        The scan instance to associate the returned ``Fileset`` to.
    fileset_info : dict
        The fileset dictionary with the fileset 'id' entry

    Returns
    -------
    plantdb.fsdb.Fileset
        The ``Fileset`` instance from parsed JSON.

    """
    fsid = fileset_info.get("id", None)
    if fsid is None:
        raise FilesetNoIDError("Fileset: No ID")

    fileset = Fileset(scan, fsid)
    # Get the expected directory path and check it exists:
    path = _fileset_path(fileset)
    if not path.is_dir():
        logger.debug(f"Missing fileset directory: {path}")
        raise FilesetNotFoundError(f"Fileset directory '{fsid}' not found for scan '{scan.id}'")
    return fileset


def _load_fileset_files(fileset, fileset_info):
    """Load the list of ``File`` from given `fileset`.

    Parameters
    ----------
    fileset : plantdb.fsdb.Fileset
        The instance to use to get the list of ``File``.
    fileset_info : dict
        Dictionary with the fileset id and listing its files, ``{'files': [], 'id': str}``.

    Returns
    -------
    list of plantdb.fsdb.File
         The list of ``File`` found in the `fileset`.

    See Also
    --------
    plantdb.fsdb._load_file

    Notes
    -----
    May delete a file if unable to load it!

    Examples
    --------
    >>> import json
    >>> from plantdb.fsdb import FSDB
    >>> from plantdb.fsdb import dummy_db, _scan_json_file, _parse_fileset, _load_fileset_files
    >>> db = dummy_db(with_fileset=True, with_file=True)
    >>> db.connect()
    >>> scan = db.get_scan("myscan_001")
    >>> db.disconnect()
    >>> json_path = _scan_json_file(scan)
    >>> with json_path.open(mode="r") as f: structure = json.load(f)
    >>> filesets_info = structure["filesets"]
    >>> fileset = _parse_fileset(scan.db, scan, filesets_info[0])
    >>> files = _load_fileset_files(fileset, filesets_info[0])
    >>> print([f.id for f in files])
    ['dummy_image', 'test_image', 'test_json']

    """
    scan_id = fileset.scan.id
    files = []
    files_info = fileset_info.get("files", None)
    if isinstance(files_info, list):
        for n, file_info in enumerate(files_info):
            try:
                file = _load_file(fileset, file_info)
            except FileNoIDError:
                logger.error(f"Could not get an 'id' entry for the {n}-th 'files' entry from '{scan_id}'.")
                logger.debug(f"Current `file_info`: {file_info}")
            except FileNoFileNameError:
                fs_id = fileset.id
                fid = file_info.get("id")
                logger.error(f"Could not get a 'file' entry for file id '{fid}' from '{scan_id}/{fs_id}'.")
                logger.debug(f"Current `file_info`: {file_info}")
            except FileNotFoundError:
                fs_id = fileset.id
                fname = file_info.get("file")
                logger.error(f"Could not find file '{fname}' for '{scan_id}/{fs_id}'.")
                logger.debug(f"Current `file_info`: {file_info}")
            else:
                files.append(file)
    else:
        raise IOError(f"Expected a list of files in `files.json` from dataset '{fileset.scan.id}'!")
    return files


def _load_fileset_metadata(fileset):
    """Load the metadata for a fileset.

    Parameters
    ----------
    fileset : plantdb.fsdb.Fileset
        The fileset to load the metadata for.

    Returns
    -------
    dict
        The metadata dictionary.
    """
    return _load_metadata(_fileset_metadata_json_path(fileset))


class FileNoIDError(Exception):
    """No 'id' entry could be found for this file."""


class FileNoFileNameError(Exception):
    """No 'file' entry could be found for this file."""


def _load_file(fileset, file_info):
    """Get a `File` instance for given `fileset` using provided `file_info`.

    Parameters
    ----------
    fileset : plantdb.fsdb.Fileset
        The instance to associate the returned ``File`` to.
    file_info : dict
        Dictionary with the file 'id' and 'file' entries, ``{'file': str, 'id': str}``.

    Returns
    -------
    plantdb.fsdb.File
        The `File` instance with metadata.

    See Also
    --------
    plantdb.fsdb._parse_file
    plantdb.fsdb._load_file_metadata
    """
    file = _parse_file(fileset, file_info)
    file.metadata = _load_file_metadata(file)
    return file


def _parse_file(fileset, file_info):
    """Get a `File` instance for given `fileset` by parsing provided `file_info`.

    Parameters
    ----------
    fileset : plantdb.fsdb.Fileset
        The fileset instance to associate the returned ``File`` to.
    file_info : dict
        The file dictionary with the file 'id' and 'file' entries, ``{'file': str, 'id': str}``.

    Returns
    -------
    plantdb.fsdb.File
        The ``File`` instance from parsed JSON.

    Raises
    ------
    FileNoIDError
        If the 'id' entry is missing from `file_info`.
    FileNoFileNameError
        If the 'file' entry is missing from `file_info`.
    FileNotFoundError
        If the file is not found on drive.
    """
    fid = file_info.get("id", None)
    if fid is None:
        logger.debug(f"Input `file_info`: {file_info}")
        raise FileNoIDError("File: No ID")

    filename = file_info.get("file", None)
    if filename is None:
        logger.debug(f"Input `file_info`: {file_info}")
        raise FileNoFileNameError("File: No filename")

    file = File(fileset, fid)
    file.filename = filename  # set the filename attribute
    # Get the expected file path and check it exists:
    path = _file_path(file)
    if not path.is_file():
        logger.debug(f"Missing file: {path}")
        raise FileNotFoundError("File: Not found")
    return file


# load/store metadata from disk

def _load_metadata(path):
    """Load a metadata dictionary from a JSON file.

    Parameters
    ----------
    path : str or pathlib.Path
        The path to the file containing the metadata to load.

    Returns
    -------
    dict
        The metadata dictionary.

    Raises
    ------
    IOError
        If the data returned by ``json.load`` is not a dictionary.
    """
    from json import JSONDecodeError
    path = Path(path)
    md = {}
    if path.is_file():
        with path.open(mode="r") as f:
            try:
                md = json.load(f)
            except JSONDecodeError:
                logger.error(f"Could not load JSON file '{path}'")
        if not isinstance(md, dict):
            raise IOError(f"Could not obtain a dictionary from JSON: {path}")
    else:
        logger.debug(f"Could not find JSON file '{path}'")

    return md


def _load_measures(path):
    """Load a measure dictionary from a JSON file.

    Parameters
    ----------
    path : str or pathlib.Path
        The path to the file containing the measure to load.

    Returns
    -------
    dict
        The measure dictionary.

    Raises
    ------
    IOError
        If the data returned by ``json.load`` is not a dictionary.
    """
    return _load_metadata(path)


def _load_scan_metadata(scan):
    """Load the metadata for a scan dataset.

    Parameters
    ----------
    scan : plantdb.fsdb.Scan
        The dataset to load the metadata for.

    Returns
    -------
    dict
        The metadata dictionary.
    """
    scan_md = {}
    md_path = _scan_metadata_path(scan)
    if md_path.exists():
        scan_md.update(_load_metadata(md_path))
    # FIXME: next lines are here to solve the issue that most scans dataset have no metadata as they have been saved in the 'images' fileset metadata...
    img_fs_path = md_path.parent / 'images.json'  # path to 'images' fileset metadata
    if img_fs_path.exists():
        img_fs_md = _load_metadata(img_fs_path)
        scan_md.update({'object': img_fs_md.get('object', {})})
        scan_md.update({'hardware': img_fs_md.get('hardware', {})})
        scan_md.update({'acquisition_date': img_fs_md.get('acquisition_date', None)})
    return scan_md


def _load_scan_measures(scan):
    """Load the measures for a dataset.

    Parameters
    ----------
    scan : plantdb.fsdb.Scan
        The dataset to load the measures for.

    Returns
    -------
    dict
        The measures' dictionary.
    """
    return _load_measures(_scan_measures_path(scan))


def _load_file_metadata(file):
    """Load the metadata for a file.

    Parameters
    ----------
    file : plantdb.fsdb.File
        The file to load the metadata for.

    Returns
    -------
    dict
        The metadata dictionary.
    """
    return _load_metadata(_file_metadata_path(file))


def _mkdir_metadata(path):
    """Create the parent directories from given path.

    Parameters
    ----------
    path : str or pathlib.Path
        The path to a file.

    Notes
    -----
    Used to check the availability of the 'metadata' directory inside a dataset.
    """
    dir = Path(path).parent
    if not dir.is_dir():
        dir.mkdir(parents=True, exist_ok=True)
    return


def _store_metadata(path, metadata):
    """Save a metadata dictionary as a JSON file.

    Parameters
    ----------
    path : str or pathlib.Path
        JSON file path to use for saving the metadata.
    metadata : dict
        The metadata dictionary to save.
    """
    _mkdir_metadata(path)
    with path.open(mode="w") as f:
        json.dump(metadata, f, sort_keys=True, indent=4, separators=(',', ': '))
    return


def _store_scan_metadata(scan):
    """Save the metadata for a dataset.

    Parameters
    ----------
    scan : plantdb.fsdb.Scan
        The dataset to save the metadata for.
    """
    _store_metadata(_scan_metadata_path(scan), scan.metadata)
    return


def _store_fileset_metadata(fileset):
    """Save the metadata for a dataset.

    Parameters
    ----------
    scan : plantdb.fsdb.Fileset
        The fileset to save the metadata for.
    """
    _store_metadata(_fileset_metadata_json_path(fileset), fileset.metadata)
    return


def _store_file_metadata(file):
    """Save the metadata for a dataset.

    Parameters
    ----------
    scan : plantdb.fsdb.File
        The file to save the metadata for.
    """
    _store_metadata(_file_metadata_path(file), file.metadata)
    return


def _get_metadata(metadata=None, key=None):
    """Get a copy of `metadata[key]`.

    Parameters
    ----------
    metadata : dict, optional
        The metadata dictionary to get the key from.
    key : str, optional
        The key to get from the metadata dictionary.
        By default, return a copy of the whole metadata dictionary.

    Returns
    -------
    Any
        The value saved under `metadata['key']`, if any.
    """
    # Do a deepcopy of the value because we don't want the caller to inadvertently change the values.
    if metadata is None:
        return {}
    elif key is None:
        return copy.deepcopy(metadata)
    else:
        return copy.deepcopy(metadata.get(str(key), {}))


def _set_metadata(metadata, data, value):
    """Set a `data` `value` in `metadata` dictionary.

    Parameters
    ----------
    metadata : dict
        The metadata dictionary to get the key from.
    data : str or dict
        If a string, a key to address the `value`.
        If a dictionary, update the metadata dictionary with `data` (`value` is then unused).
    value : any, optional
        The value to assign to `data` if the latest is not a dictionary.

    Raises
    ------
    IOError
        If `value` is `None` when `data` is a string.
        If `data` is not of the right type.

    """
    if isinstance(data, str):
        if value is None:
            raise IOError(f"No value given for key '{data}'!")
        # Do a deepcopy of the value because we don't want the caller to inadvertently change the values.
        metadata[data] = copy.deepcopy(value)
    elif isinstance(data, dict):
        for key, value in data.items():
            _set_metadata(metadata, key, value)
    else:
        raise IOError(f"Invalid key: {data}")
    return


def _get_filename(file, ext):
    """Returns a `file` name using its ``id`` attribute and given extension.

    Parameters
    ----------
    file : plantdb.fsdb.File
        A File object.
    ext : str
        The file extension to use.

    Returns
    -------
    str
        The corresponding file's name.
    """
    # Remove starting dot from extension:
    if ext.startswith('.'):
        ext = ext[1:]
    return f"{file.id}.{ext}"


################################################################################
# paths - creators
################################################################################

def _make_fileset(fileset):
    """Create the fileset directory.

    Parameters
    ----------
    fileset : plantdb.fsdb.Fileset
        The fileset to use for directory creation.

    See Also
    --------
    plantdb.fsdb._fileset_path
    """
    path = _fileset_path(fileset)
    # Create the fileset directory if it does not exist:
    if not path.is_dir():
        path.mkdir(parents=True)
    return


def _make_scan(scan):
    """Create the scan directory.

    Parameters
    ----------
    scan : plantdb.fsdb.Scan
        The scan to use for directory creation.

    See Also
    --------
    plantdb.fsdb._scan_path
    """
    path = _scan_path(scan)
    # Create the scan directory if it does not exist:
    if not path.is_dir():
        path.mkdir(parents=True)
    return


################################################################################
# paths - getters
################################################################################


def _scan_path(scan):
    """Get the path to given scan.

    Parameters
    ----------
    scan : plantdb.fsdb.Scan
        The scan to get the path from.

    Returns
    -------
    pathlib.Path
        The path to the scan directory.
    """
    return (scan.db.basedir / scan.id).resolve()


def _scan_json_file(scan):
    """Get the path to scan's "files.json" file.

    Parameters
    ----------
    scan : plantdb.fsdb.Scan
        The scan to get the files JSON file path from.

    Returns
    -------
    pathlib.Path
        Path to the scan's "files.json" file.
    """
    return _scan_path(scan) / "files.json"


def _scan_metadata_path(scan):
    """Get the path to scan's "metadata.json" file.

    Parameters
    ----------
    scan : plantdb.fsdb.Scan
        The scan to get the metadata JSON file path from.

    Returns
    -------
    pathlib.Path
        Path to the scan's "metadata.json" file.
    """
    return _scan_path(scan) / "metadata" / "metadata.json"


def _scan_measures_path(scan):
    """Get the path to scan's "measures.json" file.

    Parameters
    ----------
    scan : plantdb.fsdb.Scan
        The scan to get the measures JSON file path from.

    Returns
    -------
    pathlib.Path
        Path to the scan's "measures.json" file.
    """
    return _scan_path(scan) / "measures.json"


def _fileset_path(fileset):
    """Get the path to given fileset directory.

    Parameters
    ----------
    fileset : plantdb.fsdb.Fileset
        The fileset to get the path from.

    Returns
    -------
    pathlib.Path
        The path to the fileset directory.
    """
    return _scan_path(fileset.scan) / fileset.id


def _fileset_metadata_path(fileset):
    """Get the path to given fileset metadata directory.

    Parameters
    ----------
    fileset : plantdb.fsdb.Fileset
        The fileset to get the metadata directory path from.

    Returns
    -------
    pathlib.Path
        Path to the fileset metadata directory.
    """
    return _scan_path(fileset.scan) / "metadata" / fileset.id


def _fileset_metadata_json_path(fileset):
    """Get the path to `fileset.id` metadata JSON file.

    Parameters
    ----------
    fileset : plantdb.fsdb.Fileset
        Fileset to get the JSON file path from.

    Returns
    -------
    pathlib.Path
        Path to the f`fileset.id` metadata JSON file.
    """
    return _scan_path(fileset.scan) / "metadata" / f"{fileset.id}.json"


def _file_path(file):
    """Get the path to given file.

    Parameters
    ----------
    file : plantdb.fsdb.File
        The file to get the path from.

    Returns
    -------
    pathlib.Path
        The path to the file.
    """
    return _fileset_path(file.fileset) / file.filename


def _file_metadata_path(file):
    """Get the path to `file.id` metadata JSON file.

    Parameters
    ----------
    file : plantdb.fsdb.File
        File to get the metadata JSON path from.

    Returns
    -------
    pathlib.Path
        Path to the "<File.id>.json" file.
    """
    return _scan_path(file.fileset.scan) / "metadata" / file.fileset.id / f"{file.id}.json"


################################################################################
# store a scan to disk
################################################################################

def _file_to_dict(file):
    """Returns a dict with the file "id" and "filename".

    Parameters
    ----------
    file : plantdb.fsdb.File
        File to get "id" and "filename" from.

    Returns
    -------
    dict
        ``{"id": file.get_id(), "file": file.filename}``
    """
    return {"id": file.get_id(), "file": file.filename}


def _fileset_to_dict(fileset):
    """Returns a dict with the fileset "id" and the "files" dict.

    Parameters
    ----------
    fileset : plantdb.fsdb.Fileset
        Fileset to get "id" and "files" dictionary from.

    Returns
    -------
    dict
        ``{"id": fileset.get_id(), "files": {"id": file.get_id(), "file": file.filename}}``

    See Also
    --------
    plantdb.fsdb._file_to_dict
    """
    files = []
    for f in fileset.get_files():
        files.append(_file_to_dict(f))
    return {"id": fileset.get_id(), "files": files}


def _scan_to_dict(scan):
    """Returns a dict with the scan's filesets and files structure.

    Parameters
    ----------
    scan : plantdb.fsdb.Scan
        Scan instance to get underlying filesets and files structure from.

    Returns
    -------
    dict
        ``{"filesets": {"id": fileset.get_id(), "files": {"id": file.get_id(), "file": file.filename}}}``

    See Also
    --------
    plantdb.fsdb._fileset_to_dict
    plantdb.fsdb._file_to_dict
    """
    filesets = []
    for fileset in scan.get_filesets():
        filesets.append(_fileset_to_dict(fileset))
    return {"filesets": filesets}


def _store_scan(scan):
    """Dump the fileset and files structure associated to a `scan` on drive.

    Parameters
    ----------
    scan : plantdb.fsdb.Scan
        Scan instance to save by dumping its underlying structure in its "files.json".

    See Also
    --------
    plantdb.fsdb._scan_to_dict
    plantdb.fsdb._scan_files_json
    """
    structure = _scan_to_dict(scan)
    files_json = _scan_json_file(scan)
    with files_json.open(mode="w") as f:
        json.dump(structure, f, sort_keys=True, indent=4, separators=(',', ': '))
    return


def _is_valid_id(id):
    return True  # haha  (FIXME!)


def _is_fsdb(path):
    """Test if the given path is indeed an FSDB database.

    Do it by checking the presence of the ``MARKER_FILE_NAME``.

    Parameters
    ----------
    path : str or pathlib.Path
        A path to test as a valid FSDB database.

    Returns
    -------
    bool
        ``True`` if an FSDB database, else ``False``.
    """
    path = Path(path)
    marker_path = path / MARKER_FILE_NAME
    return marker_path.is_file()


def _is_safe_to_delete(path):
    """Tests if given path is safe to delete.

    Parameters
    ----------
    path : str or pathlib.Path
        A path to test for safe deletion.

    Returns
    -------
    bool
        ``True`` if the path is safe to delete, else ``False``.

    Notes
    -----
    A path is safe to delete only if it's a sub-folder of a db.
    """
    path = Path(path).resolve()
    while True:
        # Test if the current path is a local DB (FSDB):
        if _is_fsdb(path):
            return True  # exit and return `True` if it is
        # Else, move to the parent directory & try again:
        newpath = path.parent
        # Check if we have indeed moved up to the parent directory
        if newpath == path:
            # Stop if we did not
            return False
        path = newpath


def _delete_file(file):
    """Delete the given file.

    Parameters
    ----------
    file : plantdb.fsdb.File
        The file instance to delete.

    Raises
    ------
    IOError
        If the file path is outside the database.

    Notes
    -----
    We have to delete:
      - the JSON metadata file associated to the file.
      - the file

    See Also
    --------
    plantdb.fsdb._file_path
    plantdb.fsdb._is_safe_to_delete
    """
    if file.filename is None:
        # The filename attribute is defined when the file is written!
        logger.error(f"No 'filename' attribute defined for file id '{file.id}'.")
        logger.info("It means the file is not written on disk.")
        return

    file_path = _file_path(file)
    if not _is_safe_to_delete(file_path):
        logger.error(f"File {file.filename} is not in the current database.")
        logger.debug(f"File path: '{file_path}'")
        raise IOError("Cannot delete files or directories outside of a local DB.")

    # - Delete the JSON metadata file associated to the `File` instance:
    file_md_path = _file_metadata_path(file)
    if file_md_path.is_file():
        try:
            file_md_path.unlink(missing_ok=False)
        except FileNotFoundError:
            logger.error(
                f"Could not delete the JSON metadata file for file '{file.id}' from '{file.fileset.scan.id}/{file.fileset.id}'.")
            logger.debug(f"JSON metadata file path: '{file_md_path}'.")
        else:
            logger.info(
                f"Deleted JSON metadata file for file '{file.id}' from '{file.fileset.scan.id}/{file.fileset.id}'.")

    # - Delete the file associated to the `File` instance:
    if file_path.is_file():
        try:
            file_path.unlink(missing_ok=False)
        except FileNotFoundError:
            logger.error(f"Could not delete file '{file.id}' from '{file.fileset.scan.id}/{file.fileset.id}'.")
            logger.debug(f"File path: '{file_path}'.")
        else:
            logger.info(f"Deleted file '{file.id}' from '{file.fileset.scan.id}/{file.fileset.id}'.")

    return


def _delete_fileset(fileset):
    """Delete the given fileset.

    Parameters
    ----------
    fileset : plantdb.fsdb.Fileset
        The fileset instance to delete.

    Raises
    ------
    IOError
        If the fileset path is outside the database.

    Notes
    -----
    We have to delete:
      - the files in the fileset
      - the fileset JSON metadata file
      - the fileset metadata directory
      - the fileset directory

    See Also
    --------
    plantdb.fsdb._scan_path
    plantdb.fsdb._fileset_path
    plantdb.fsdb._is_safe_to_delete
    """
    fileset_path = _fileset_path(fileset)
    if not _is_safe_to_delete(fileset_path):
        logger.error(f"Fileset {fileset.id} is not in the current database.")
        logger.debug(f"Fileset path: '{fileset_path}'.")
        raise IOError("Cannot delete files or directories outside of a local DB.")

    # - Delete the `Files` (and their metadata) belonging to the `Fileset` instance:
    for f in fileset.files:
        fileset.delete_file(f.id)

    # - Delete the JSON metadata file associated to the `Fileset` instance:
    json_md = _fileset_metadata_json_path(fileset)
    try:
        json_md.unlink(missing_ok=False)
    except FileNotFoundError:
        logger.warning(f"Could not find the JSON metadata file for fileset '{fileset.id}'.")
        logger.debug(f"JSON metadata file path: '{json_md}'.")
    else:
        logger.info(f"Deleted the JSON metadata file for fileset '{fileset.id}'.")

    # - Delete the metadata directory associated to the `Fileset` instance:
    dir_md = _fileset_metadata_path(fileset)
    try:
        rmtree(dir_md, ignore_errors=True)
    except:
        logger.warning(f"Could not find metadata directory for fileset '{fileset.id}'.")
        logger.debug(f"Metadata directory path: '{dir_md}'.")
    else:
        logger.info(f"Deleted metadata directory for fileset '{fileset.id}'.")

    # - Delete the directory associated to the `Fileset` instance:
    try:
        rmtree(fileset_path, ignore_errors=True)
    except:
        logger.warning(f"Could not find directory for fileset '{fileset.id}'.")
        logger.debug(f"Fileset directory path: '{fileset_path}'.")
    else:
        logger.info(f"Deleted directory for fileset '{fileset.id}'.")
    return


def _delete_scan(scan):
    """Delete the given scan, starting by its `Fileset`s.

    Parameters
    ----------
    scan : plantdb.fsdb.Scan
        The scan instance to delete.

    Raises
    ------
    IOError
        If the scan path is outside the database.

    See Also
    --------
    plantdb.fsdb._scan_path
    plantdb.fsdb._is_safe_to_delete
    """
    scan_path = _scan_path(scan)
    if not _is_safe_to_delete(scan_path):
        raise IOError("Cannot delete files outside of a DB.")

    # - Delete the whole directory will get rid of everything (metadata, filesets, files):
    try:
        rmtree(scan_path, ignore_errors=True)
    except:
        logger.warning(f"Could not find directory for scan '{scan.id}'.")
        logger.debug(f"Scan path: '{scan_path}'.")
    else:
        logger.info(f"Deleted directory for scan '{scan.id}'.")

    return


def _filter_query(l, query=None):
    """Filter a list of `Scan`s, `Fileset`s or `File`s using a `query` on their metadata.

    Parameters
    ----------
    l : list
        List of `Scan`s, `Fileset`s or `File`s to filter.
    query : dict, optional
        Filtering query in the form of a dictionary.
        The list of instances must have metadata matching ``key`` and ``value`` from the `query`.

    Returns
    -------
    list
        List of `Scan`s, `Fileset`s or `File`s filtered by the query, if any.

    Examples
    --------
    >>> from plantdb.fsdb import dummy_db
    >>> from plantdb.fsdb import _filter_query
    >>> db = dummy_db(with_scan=True, with_file=True)
    >>> db.connect()
    >>> scan = db.get_scan("myscan_001")
    >>> fs = scan.get_fileset("fileset_001")
    >>> print({f.id: f.metadata for f in fs.get_files()})
    {'dummy_image': {'dummy image': True}, 'test_image': {'random image': True}, 'test_json': {'random json': True}}
    >>> files = _filter_query(fs.get_files(), query=None)
    >>> print(len(files))  # no filtering so all three files are here!
    3
    >>> files = _filter_query(fs.get_files(), query={"channel": "rgb"})
    >>> print(len(files))  # should be empty as no file has this metadata
    0
    >>> files = _filter_query(fs.get_files(), query={"random image": True})
    >>> print(len(files))  # should be `1` as only one file has this metadata
    1
    >>> db.disconnect()

    """
    if query is None or query == {}:
        # If there is no `query` return the unfiltered list of instances
        query_result = [f for f in l]
    else:
        # Else apply the filter on metadata using key(s) and value(s) from `query`:
        query_result = []
        for f in l:
            f_query = []  # boolean list gathering the "filter test results"
            for q in query.keys():
                try:
                    assert f.get_metadata(q) == query[q]
                except AssertionError:
                    f_query.append(False)
                else:
                    f_query.append(True)
            # All requirements have to be fulfilled:
            if all(f_query):
                query_result.append(f)
    return query_result
