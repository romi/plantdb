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
plantdb.fsdb
=============

Implementation of a database as a local file structure.

Assuming that the ``FSDB`` root database directory is ``dbroot/``, there is a
 ``Scan`` with ``'myscan_001'`` as ``Scan.id`` and there are some metadata (see
  below), you should have the following file structure:

.. code-block::

    dbroot/                            # base directory of the database
    ├── myscan_001/                    # scan dataset directory, id=`myscan_001`
    │   ├── files.json                 # JSON file referencing the files of the datataset
    │   ├── images/                    # gather the 'images' `Fileset`
    │   │   ├── scan_img_01.jpg        # 'image' `File` 01
    │   │   ├── scan_img_02.jpg        # 'image' `File` 02
    │   │   ├── [...]
    │   │   └── scan_img_99.jpg        # 'image' `File` 99
    │   ├── metadata/                  # metadata directory
    │   │   ├── images                 # 'images' metadata directory
    │   │   │   ├── scan_img_01.json   # JSON file with 'image' file metadata
    │   │   │   ├── scan_img_02.json   #
    │   │   ├── [...]
    │   │   │   └── scan_img_99.json   #
    │   │   └── metadata.json          # scan dataset metadata
    ├── (LOCK_FILE_NAME)               # "lock file", present if DB is connected
    └── MARKER_FILE_NAME               # ROMI DB marker file

The ``myscan_001/files.json`` file then contains the following structure:

.. code-block:: JSON

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

The metadata of the scan (``metadata.json``), of the set of 'images' files
 (``<Fileset.id>.json``) and of each 'image' files (``<File.id>.json``) are all
 stored as JSON files in a separate directory:

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
import glob
import json
import logging
import os
from shutil import copyfile

from plantdb import db
from plantdb.db import DBBusyError

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
    >>> print(db.basedir)
    /tmp/romidb_*******

    >>> db = dummy_db(with_scan=True)
    >>> db.connect()
    >>> scan = db.get_scan("myscan_001")
    >>> print(type(scan))
    <class 'NoneType'>
    >>> print(os.listdir(db.basedir))
    ['lock', 'romidb', 'myscan_001']
    >>> print(os.listdir(os.path.join(db.basedir, "myscan_001")))  # Same goes for the metadata
    ['metadata']

    >>> db = dummy_db(with_fileset=True)
    >>> db.connect()
    >>> scan = db.get_scan("myscan_001")
    >>> print(os.listdir(os.path.join(db.basedir, scan.id)))  # Same goes for the metadata
    ['metadata', 'files.json', 'fileset_001']
    >>> fs = scan.get_fileset("fileset_001")
    >>> print(type(fs))
    <class 'plantdb.fsdb.Fileset'>
    >>> print(os.listdir(os.path.join(db.basedir, scan.id, fs.id)))  # Same goes for the metadata
    []

    >>> db = dummy_db(with_file=True)
    >>> db.connect()
    >>> scan = db.get_scan("myscan_001")
    >>> fs = scan.get_fileset("fileset_001")
    >>> f = fs.get_file("test_image")
    >>> print(type(f))
    >>> print(os.listdir(os.path.join(db.basedir, scan.id, fs.id)))  # Same goes for the metadata
    ['test_image.png', 'test_json.json']
    >>> fpath = os.path.join(db.basedir, scan.id, fs.id, f.id)

    """
    from os.path import join
    from tempfile import mkdtemp
    from plantdb import io

    mydb = mkdtemp(prefix='romidb_')
    open(join(mydb, MARKER_FILE_NAME), 'w').close()
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
        img = np.array(255 * np.random.rand(50, 50, 3), dtype='uint8')
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


class FSDB(db.DB):
    """Class defining the database object from abstract class ``DB``.

    Implementation of a database as a simple local file structure:

      * directory ``${FSDB.basedir}`` as database root directory;
      * marker file ``MARKER_FILE_NAME`` at database root directory;
      * (OPTIONAL) lock file ``LOCK_FILE_NAME`` at database root directory when connected;

    Attributes
    ----------
    basedir : str
        Path to the base directory containing the database
    lock_path : str
        Absolute path to the lock file.
    scans : list
        List of ``Scan`` objects found in the database.
    is_connected : bool
        ``True`` if the database is connected (locked directory), else ``False``.

    Notes
    -----
    Requires the marker file ``MARKER_FILE_NAME`` at the given ``basedir``.
    Lock file ``LOCK_FILE_NAME`` is found only when connecting an FSBD instance to the given ``basedir``.

    See Also
    --------
    MARKER_FILE_NAME
    LOCK_FILE_NAME

    Examples
    --------
    >>> # EXAMPLE 1: Use a temporary dummy database:
    >>> from plantdb.fsdb import dummy_db
    >>> db = dummy_db()
    >>> print(type(db))
    <class 'plantdb.fsdb.FSDB'>
    >>> print(db.basedir)
    /tmp/romidb_***
    >>> # Now connecting to this dummy DB...
    >>> db.connect()
    >>> # ...allows to create new `Scan` in it:
    >>> new_scan = db.create_scan("007")
    >>> print(type(new_scan))
    <class 'plantdb.fsdb.Scan'>
    >>> db.disconnect()

    >>> # EXAMPLE 2: Use a local database:
    >>> import os
    >>> from plantdb.fsdb import FSDB
    >>> db = FSDB(os.environ.get('DB_LOCATION', "/data/ROMI/DB/"))
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
            Path to the root directory of the database.

        """
        super().__init__()
        # Defines attributes:
        self.basedir = basedir
        self.lock_path = os.path.abspath(os.path.join(basedir, LOCK_FILE_NAME))
        self.scans = []
        self.is_connected = False

    def connect(self, login_data=None):
        """Connect to the local database.

        Handle DB "locking" system by adding a `LOCK_FILE_NAME` file in the DB.

        Parameters
        ----------
        login_data : bool
            UNUSED

        Raises
        ------
        IOError
            If the given `basedir` is not an existing directory.
            If the `MARKER_FILE_NAME` is missing from the `basedir`.
        DBBusyError
            If the `LOCK_FILE_NAME` lock fil is found in the `basedir`.

        See Also
        --------
        MARKER_FILE_NAME
        LOCK_FILE_NAME

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

        """
        # Check the given path to root directory of the database is a directory:
        if not os.path.isdir(self.basedir):
            raise IOError("Not a directory: %s" % self.basedir)
        # Check the given path to root directory of the database is a "romi db", ie. have the `MARKER_FILE_NAME`:
        if not _is_db(self.basedir):
            raise IOError(
                "Not a DB. Check that there is a marker named %s in %s" % (
                    MARKER_FILE_NAME, self.basedir))
        if not self.is_connected:
            try:
                with open(self.lock_path, "x") as _:
                    self.scans = _load_scans(self)
                    self.is_connected = True
                atexit.register(self.disconnect)
            except FileExistsError:
                raise DBBusyError(
                    "File %s exists in DB root: DB is busy, cannot connect." % LOCK_FILE_NAME)
        else:
            print(f"Already connected to the database '{self.basedir}'")

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
                os.remove(self.lock_path)
                atexit.unregister(self.disconnect)
            else:
                raise IOError(
                    "Could not remove lock, maybe you messed with the lock_path attribute?")
            self.scans = []
            self.is_connected = False
        else:
            print(f"Already disconnected from the database '{self.basedir}'")

    def get_scans(self, query=None):
        """Get the list of `Scan` instances defined in the local database, possibly filtered using a `query`.

        Parameters
        ----------
        query : dict, optional
            Query to use to get a list of scans.

        Returns
        -------
        list of plantdb.fsdb.Scan
            The list of `Scan` resulting form the query.

        See Also
        --------
        _filter_query

        Examples
        --------
        >>> from plantdb.fsdb import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> db.connect()
        >>> db.get_scans()
        [<plantdb.fsdb.Scan at *x************>]

        """
        if query is None:
            return self.scans
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
        _is_valid_id
        _make_scan

        Examples
        --------
        >>> from plantdb.fsdb import dummy_db
        >>> db = dummy_db()
        >>> db.connect()
        >>> new_scan = db.create_scan('007')
        >>> scan = db.create_scan('007')
        OSError: Duplicate scan name: 007

        """
        if not _is_valid_id(id):
            raise IOError("Invalid id")
        if self.get_scan(id) != None:
            raise IOError("Duplicate scan name: %s" % id)
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
        _delete_scan

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

        """
        scan = self.get_scan(id)
        if scan is None:
            raise IOError("Invalid id")
        _delete_scan(scan)
        self.scans.remove(scan)

    def path(self) -> str:
        """Get the path to the local database root directory.

        Examples
        --------
        >>> from plantdb.fsdb import dummy_db
        >>> db = dummy_db(with_scan=True, with_file=True)
        >>> db.connect()
        >>> db.path()

        """
        return self.basedir

    def list_scans(self, query=None) -> list:
        """Get the list of scans in the local database.

        Examples
        --------
        >>> from plantdb.fsdb import dummy_db
        >>> db = dummy_db(with_scan=True, with_file=True)
        >>> db.connect()
        >>> db.list_scans()
        ['myscan_001']

        """
        return [f.id for f in self.get_scans(query)]


class Scan(db.Scan):
    """Class defining the scan object from abstract class ``Scan``.

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
    filesets : list of Fileset
        List of ``Fileset`` objects.

    Notes
    -----
    Optional directory ``metadata`` & JSON file ``metadata.json`` are found when using method ``set_metadata()``.

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
    >>> print(os.listdir(db.basedir))  # And it is NOT found under the `basedir` directory
    ['romidb']
    >>> # HOWEVER if you add metadata to the `Scan` object:
    >>> scan.set_metadata({'Name': "Bond... James Bond!"})
    >>> print(scan.metadata)
    {'Name': 'Bond... James Bond!'}
    >>> print(db.get_scan('007'))  # The `Scan` is still not found in the database!
    None
    >>> print(os.listdir(db.basedir))  # BUT it is now found under the `basedir` directory
    ['007', 'romidb']
    >>> print(os.listdir(os.path.join(db.basedir, scan.id)))  # Same goes for the metadata
    ['metadata']
    >>> print(os.listdir(os.path.join(db.basedir, scan.id, "metadata")))  # Same goes for the metadata
    >>> db.disconnect()

    >>> # Example #2: Get it from an `FSDB` object:
    >>> db = dummy_db()
    >>> scan = db.get_scan('007', create=True)
    >>> print(type(scan))
    <class 'plantdb.fsdb.Scan'>
    >>> print(db.get_scan('007'))  # This time the `Scan` object is found in the `FSBD`
    <plantdb.fsdb.Scan object at 0x7f34fc860fd0>
    >>> print(os.listdir(db.basedir))  # And it is found under the `basedir` directory
    ['007', 'romidb']
    >>> print(os.listdir(os.path.join(db.basedir, scan.id)))  # Same goes for the metadata
    ['metadata']
    >>> db.disconnect()
    >>> # When reconnecting to db, if created scan is EMPTY (no Fileset & File) it is not found!
    >>> db.connect()
    >>> print(db.get_scan('007'))
    None

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

    def _erase(self):
        for f in self.filesets:
            f._erase()
        del self.metadata
        del self.filesets

    def get_filesets(self, query=None):
        """Get the list of `Fileset` instances defined in the current scan dataset, possibly filtered using a `query`.

        Parameters
        ----------
        query : dict, optional
            Query to use to get a list of filesets.

        Returns
        -------
        list of plantdb.fsdb.Fileset
            The list of `Fileset` resulting form the query.

        See Also
        --------
        _filter_query

        Examples
        --------
        >>> from plantdb.fsdb import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> db.connect()
        >>> scan = db.get_scan('myscan_001')
        >>> scan.get_filesets()
        [<plantdb.fsdb.Fileset at *x************>]

        """
        if query is None:
            return self.filesets  # Copy?
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
        return _get_measures(self.measures, key)

    def set_metadata(self, data, value=None):
        """Add a new metadata to the scan.

        Parameters
        ----------
        data : str
            Name of the metadata.
        value : any
            Value to attach to this metadata. Should be transformable to JSON.

        """
        if self.metadata == None:
            self.metadata = {}
        _set_metadata(self.metadata, data, value)
        _store_scan_metadata(self)

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
        _is_valid_id
        _make_scan

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

        """
        if not _is_valid_id(id):
            raise IOError("Invalid id")
        if self.get_fileset(id) != None:
            raise IOError("Duplicate fileset name: %s" % id)
        fileset = Fileset(self.db, self, id)
        _make_fileset(fileset)
        self.filesets.append(fileset)
        self.store()
        return fileset

    def store(self):
        """Save changes to the scan's JSON."""
        _store_scan(self)

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

        """
        fs = self.get_fileset(fileset_id)
        if fs is None:
            logging.warning(f"Could not get the Fileset to delete: '{fileset_id}'!")
            return
        _delete_fileset(fs)
        self.filesets.remove(fs)
        self.store()

    def path(self) -> str:
        """Get the path to the local scan dataset.

        Examples
        --------
        >>> from plantdb.fsdb import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> db.connect()
        >>> scan = db.get_scan("myscan_001")
        >>> scan.path()  # should be '/tmp/romidb_********/myscan_001'

        """
        return _scan_path(self)

    def list_filesets(self, query=None) -> list:
        """Get the list of filesets in the scan dataset.

        Examples
        --------
        >>> from plantdb.fsdb import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> db.connect()
        >>> db.list_scans()
        >>> scan = db.get_scan("myscan_001")
        >>> scan.list_filesets()
        ['fileset_001']

        """
        return [f.id for f in self.get_filesets(query)]


class Fileset(db.Fileset):
    """Class defining the fileset object from abstract class `Fileset`.

    Implementation of a fileset as a simple file structure with:
      * directory `${FSDB.basedir}/${FSDB.scan.id}/${Fileset.id}` containing set of files;
      * directory `${FSDB.basedir}/${FSDB.scan.id}/metadata` containing JSON metadata associated to files;
      * JSON file `files.json` containing the list of files from fileset;

    Attributes
    ----------
    db : fsdb.DB
        Database where to find the scan.
    id : int
        Id of the scan in the database `FSDB`.
    scan : fsdb.Scan
        Scan containing the set of files.
    metadata : dict
        Dictionary of metadata attached to the fileset.
    files : list of File
        list of `File` objects.

    """

    def __init__(self, db, scan, id):
        super().__init__(db, scan, id)
        self.metadata = None
        self.files = []

    def _erase(self):
        for f in self.files:
            f._erase()
        self.metadata = None
        self.files = None

    def get_files(self, query=None):
        """Get the list of `File` instances defined in the current fileset, possibly filtered using a `query`.

        Parameters
        ----------
        query : dict, optional
            Query to use to get a list of files.

        Returns
        -------
        list of plantdb.fsdb.File
            The list of `File` resulting form the query.

        See Also
        --------
        _filter_query

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

        """
        if query is None:
            return self.files
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
        File
            The retrieved or created file.

        Examples
        --------
        >>> from plantdb.fsdb import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> scan = db.get_scan("myscan_001")
        >>> fs = scan.get_fileset("fileset_001")
        >>> f = fs.get_file("test_image")
        >>> # To read the file you need to load the right reader from plantdb.io
        >>> from plantdb.io import read_image
        >>> img = read_image(f)

        """
        ids = [f.id for f in self.files]
        if id not in ids:
            if create:
                return self.create_file(id)
            return None
        return self.files[ids.index(id)]

    def get_metadata(self, key=None):
        return _get_metadata(self.metadata, key)

    def set_metadata(self, data, value=None):
        if self.metadata == None:
            self.metadata = {}
        _set_metadata(self.metadata, data, value)
        _store_fileset_metadata(self)

    def create_file(self, id):
        file = File(self.db, self, id)
        self.files.append(file)
        self.store()
        return file

    def delete_file(self, file_id):
        x = self.get_file(file_id)
        if x is None:
            raise IOError("Invalid file ID: %s" % file_id)
        _delete_file(x)
        self.files.remove(x)
        self.store()

    def store(self):
        self.scan.store()

    def path(self) -> str:
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

        """
        return [f.id for f in self.get_files(query)]

class File(db.File):
    def __init__(self, db, fileset, id):
        super().__init__(db, fileset, id)
        self.metadata = None

    def _erase(self):
        self.id = None
        self.metadata = None

    def get_metadata(self, key=None):
        return _get_metadata(self.metadata, key)

    def set_metadata(self, data, value=None):
        if self.metadata == None:
            self.metadata = {}
        _set_metadata(self.metadata, data, value)
        _store_file_metadata(self)

    def import_file(self, path):
        filename = os.path.basename(path)
        ext = os.path.splitext(filename)[-1][1:]
        self.filename = '%s.%s' % (self.id, ext)
        newpath = _file_path(self)
        copyfile(path, newpath)
        self.store()

    def store(self):
        self.fileset.store()

    def read_raw(self):
        path = _file_path(self)
        with open(path, "rb") as f:
            return f.read()

    def write_raw(self, data, ext=""):
        self.filename = '%s.%s' % (self.id, ext)
        path = _file_path(self)
        with open(path, "wb") as f:
            f.write(data)
        self.store()

    def read(self):
        path = _file_path(self)
        with open(path, "r") as f:
            return f.read()

    def write(self, data, ext=""):
        self.filename = '%s.%s' % (self.id, ext)
        path = _file_path(self)
        with open(path, "w") as f:
            f.write(data)
        self.store()

    def path(self) -> str:
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

        """
        return _file_path(self)


##################################################################
#
# the ugly stuff...
#

# load the database

def _load_scans(db):
    """Load list of ``Scan`` from given database.

    List sub-directories of ``db.basedir``

    Parameters
    ----------
    db : FSDB
        The database object to use to get the list of ``fsdb.Scan``

    Returns
    -------
    list of plantdb.fsdb.Scan
         The list of ``fsdb.Scan`` found in the database.

    See Also
    --------
    _scan_path
    _scan_files_json
    _load_scan_filesets
    _load_scan_metadata

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
    names = os.listdir(db.basedir)
    for name in names:
        scan = Scan(db, name)
        if (os.path.isdir(_scan_path(scan))
                and os.path.isfile(_scan_files_json(scan))):
            scan.filesets = _load_scan_filesets(scan)
            scan.metadata = _load_scan_metadata(scan)
            scan.measures = _load_scan_measures(scan)
            scans.append(scan)
            # scan.store()
    return scans


def _load_scan_filesets(scan):
    """Load list of ``Fileset`` from given scan.

    Load the list of filesets using "filesets" top-level entry from ``files.json``.

    Parameters
    ----------
    scan : Scan
        The scan object to use to get the list of ``fsdb.Fileset``

    Returns
    -------
    list of plantdb.fsdb.Fileset
         The list of ``fsdb.Fileset`` found in the scan.

    See Also
    --------
    _scan_files_json
    _load_scan_filesets

    Notes
    -----
    May delete a detected fileset if unable to load it!

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
    files_json = _scan_files_json(scan)
    with open(files_json, "r") as f:
        structure = json.load(f)
    filesets_info = structure["filesets"]
    if isinstance(filesets_info, list):
        for fileset_info in filesets_info:
            try:
                fileset = _load_fileset(scan, fileset_info)
                filesets.append(fileset)
            except:
                id = fileset_info.get("id")
                print("Warning: unable to load fileset %s, deleting..." % id)
                scan.delete_fileset(id)
    else:
        raise IOError("%s: filesets is not a list" % files_json)
    return filesets


def _load_fileset(scan, fileset_info):
    """Load a fileset and set its attributes.

    Parameters
    ----------
    scan : Scan
        The scan object to use to get the list of ``fsdb.Fileset``
    fileset_info: dict
        Dictionary with the fileset id and listing its files, {'files': [], 'id': str}.

    Returns
    -------
    fsdb.Fileset
        A fileset with its ``files`` & ``metadata`` attributes restored.

    Examples
    --------
    >>> import json
    >>> from plantdb.fsdb import dummy_db, _load_fileset, _scan_files_json
    >>> db = dummy_db(with_file=True)
    >>> db.connect()
    >>> scan = db.get_scan("myscan_001")
    >>> json_path = _scan_files_json(scan)
    >>> with open(json_path, "r") as f: structure = json.load(f)
    >>> filesets_info = structure["filesets"]
    >>> fs = _load_fileset(scan, filesets_info[0])
    >>> print(fs)
    <plantdb.fsdb.Fileset object at 0x7f86bdf7a250>
    >>> print(fs.files)
    [<plantdb.fsdb.File object at 0x7f8690459b50>, <plantdb.fsdb.File object at 0x7f8690459750>]

    """
    fileset = _parse_fileset(scan.db, scan, fileset_info)
    fileset.files = _load_fileset_files(fileset, fileset_info)
    fileset.metadata = _load_fileset_metadata(fileset)
    return fileset


def _parse_fileset(db, scan, fileset_info):
    id = fileset_info.get("id")
    if id == None:
        raise IOError("Fileset: No ID")
    fileset = Fileset(db, scan, id)
    path = _fileset_path(fileset)
    if not os.path.isdir(path):
        raise IOError(
            "Fileset: Fileset directory doesn't exists: %s" % path)
    return fileset


def _load_fileset_files(fileset, fileset_info):
    files = []
    files_info = fileset_info.get("files", [])
    if isinstance(files_info, list):
        for file_info in files_info:
            try:
                file = _load_file(fileset, file_info)
                files.append(file)
            except:
                id = file_info.get("id")
                print("Warning: unable to load file %s, deleting..." % id)
                fileset.delete_file(id)
    else:
        raise IOError("files.json: expected a list for files")
    return files


def _load_file(fileset, file_info):
    file = _parse_file(fileset, file_info)
    file.metadata = _load_file_metadata(file)
    return file


def _parse_file(fileset, file_info):
    id = file_info.get("id")
    if id == None:
        raise IOError("File: No ID")
    filename = file_info.get("file")
    if filename == None:
        raise IOError("File: No filename")
    file = File(fileset.db, fileset, id)
    file.filename = filename
    path = _file_path(file)
    if not os.path.isfile(path):
        raise IOError("File: File doesn't exists: %s" % path)
    return file


# load/store metadata from disk

def _load_metadata(path):
    if os.path.isfile(path):
        with open(path, "r") as f:
            r = json.load(f)
        if not isinstance(r, dict):
            raise IOError("Not a JSON object: %s" % path)
        return r
    else:
        return {}


def _load_measures(path):
    if os.path.isfile(path):
        with open(path, "r") as f:
            r = json.load(f)
        if not isinstance(r, dict):
            raise IOError("Not a JSON object: %s" % path)
        return r
    else:
        return {}


def _load_scan_metadata(scan):
    return _load_metadata(_scan_metadata_path(scan))


def _load_scan_measures(scan):
    return _load_measures(_scan_measures_path(scan))


def _load_fileset_metadata(fileset):
    return _load_metadata(_fileset_metadata_path(fileset))


def _load_file_metadata(file):
    return _load_metadata(_file_metadata_path(file))


def _mkdir_metadata(path):
    dir = os.path.dirname(path)
    if not os.path.isdir(dir):
        os.makedirs(dir)


def _store_metadata(path, metadata):
    _mkdir_metadata(path)
    with open(path, "w") as f:
        json.dump(metadata, f, sort_keys=True,
                  indent=4, separators=(',', ': '))


def _store_scan_metadata(scan):
    _store_metadata(_scan_metadata_path(scan),
                    scan.metadata)


def _store_fileset_metadata(fileset):
    _store_metadata(_fileset_metadata_path(fileset),
                    fileset.metadata)


def _store_file_metadata(file):
    _store_metadata(_file_metadata_path(file),
                    file.metadata)


#

def _get_metadata(metadata, key):
    # Do a deepcopy of the return value because we don't want to
    # caller the inadvertedly change the values.
    if metadata == None:
        return {}
    elif key == None:
        return copy.deepcopy(metadata)
    else:
        return copy.deepcopy(metadata.get(str(key)))


def _get_measures(measures, key):
    # Do a deepcopy of the return value because we don't want to
    # caller the inadvertedly change the values.
    if measures == None:
        return {}
    elif key == None:
        return copy.deepcopy(measures)
    else:
        return copy.deepcopy(measures.get(str(key)))


def _set_metadata(metadata, data, value):
    if isinstance(data, str):
        if value is None:
            raise IOError("No value given for key %s" % data)
        # Do a deepcopy of the value because we don't want to caller
        # the inadvertedly change the values.
        metadata[data] = copy.deepcopy(value)
    elif isinstance(data, dict):
        for key, value in data.items():
            _set_metadata(metadata, key, value)
    else:
        raise IOError("Invalid key: ", data)


################################################################################
# paths - creators
################################################################################

def _make_fileset(fileset):
    """Create the fileset directory.

    Parameters
    ----------
    fileset : fsdb.Fileset
        The fileset to use for directory creation.

    See Also
    --------
    _fileset_path
    """
    path = _fileset_path(fileset)
    # Create the fileset directory if it does not exists:
    if not os.path.isdir(path):
        os.makedirs(path)


def _make_scan(scan):
    """Create the scan directory.

    Parameters
    ----------
    scan : fsdb.Scan
        The scan to use for directory creation.

    See Also
    --------
    _scan_path
    """
    path = _scan_path(scan)
    # Create the scan directory if it does not exists:
    if not os.path.isdir(path):
        os.makedirs(path)


################################################################################
# paths - getters
################################################################################

def _get_filename(file, ext):
    """Returns a file's name.

    Parameters
    ----------
    file : fsdb.File
        A File object.
    ext : str
        The file's extension to use.

    Returns
    -------
    str
        The corresponding file's name.
    """
    return file.id + "." + ext


def _scan_path(scan):
    """Get the path to given scan.

    Parameters
    ----------
    scan : fsdb.Scan
        The scan to get the path from.

    Returns
    -------
    str
        The path to the scan.
    """
    return os.path.join(scan.db.basedir,
                        scan.id)


def _fileset_path(fileset):
    """Get the path to given fileset.

    Parameters
    ----------
    fileset : fsdb.Fileset
        The fileset to get the path from.

    Returns
    -------
    str
        The path to the fileset.
    """
    return os.path.join(fileset.db.basedir,
                        fileset.scan.id,
                        fileset.id)


def _file_path(file):
    """Get the path to given file.

    Parameters
    ----------
    file : fsdb.File
        The file to get the path from.

    Returns
    -------
    str
        The path to the file.
    """
    return os.path.join(file.db.basedir,
                        file.fileset.scan.id,
                        file.fileset.id,
                        file.filename)


def _scan_files_json(scan):
    """Get the path to scan's "files.json" file.

    Parameters
    ----------
    scan : fsdb.Scan
        The scan to get the files JSON file path from.

    Returns
    -------
    str
        Path to the scan's "files.json" file.
    """
    return os.path.join(scan.db.basedir,
                        scan.id,
                        "files.json")


def _scan_metadata_path(scan):
    """Get the path to scan's "metadata.json" file.

    Parameters
    ----------
    scan : fsdb.Scan
        The scan to get the metadata JSON file path from.

    Returns
    -------
    str
        Path to the scan's "metadata.json" file.
    """
    return os.path.join(scan.db.basedir,
                        scan.id,
                        "metadata",
                        "metadata.json")


def _scan_measures_path(scan):
    """Get the path to scan's "measures.json" file.

    Parameters
    ----------
    scan : fsdb.Scan
        The scan to get the measures JSON file path from.

    Returns
    -------
    str
        Path to the scan's "measures.json" file.
    """
    return os.path.join(scan.db.basedir,
                        scan.id,
                        "measures.json")


def _fileset_metadata_path(fileset):
    """Get the path to `fileset.id` metadata JSON file.

    Parameters
    ----------
    fileset : fsdb.Fileset
        Fileset to get the JSON file path from.

    Returns
    -------
    str
        Path to the "<Fileset.id>.json" file.
    """
    return os.path.join(fileset.db.basedir,
                        fileset.scan.id,
                        "metadata",
                        fileset.id + ".json")


def _file_metadata_path(file):
    """Get the path to `file.id` metadata JSON file.

    Parameters
    ----------
    file : fsdb.File
        File to get the metadata JSON path from.

    Returns
    -------
    str
        Path to the "<File.id>.json" file.
    """
    return os.path.join(file.db.basedir,
                        file.fileset.scan.id,
                        "metadata",
                        file.fileset.id,
                        file.id + ".json")


################################################################################
# store a scan to disk
################################################################################

def _file_to_dict(file):
    """Returns a dict with the file "id" and "filename".

    Parameters
    ----------
    file : fsdb.File
        File to get "id" and "filename" from.

    Returns
    -------
    dict
        {"id": file.get_id(), "file": file.filename}
    """
    return {"id": file.get_id(), "file": file.filename}


def _fileset_to_dict(fileset):
    """Returns a dict with the fileset "id" and the list of "files" dict.

    Parameters
    ----------
    fileset : fsdb.Fileset
        File to get "id" and "files" list from.

    Returns
    -------
    dict
        {"id": fileset.get_id(), "files": [file.filename]}

    See Also
    --------
    _file_to_dict
    """
    files = []
    for f in fileset.get_files():
        files.append(_file_to_dict(f))
    return {"id": fileset.get_id(), "files": files}


def _scan_to_dict(scan):
    """Returns a dict with the scan's dictionary of `Fileset`s.

    Parameters
    ----------
    scan : fsdb.Scan
        Scan to get "filesets" dictionary from.

    Returns
    -------
    dict
        {"id": fileset.get_id(), "files": [file.filename]}

    See Also
    --------
    _fileset_to_dict
    _file_to_dict
    """
    filesets = []
    for fileset in scan.get_filesets():
        filesets.append(_fileset_to_dict(fileset))
    return {"filesets": filesets}


def _store_scan(scan):
    """Dump the scan's JSON on drive.

    Parameters
    ----------
    scan : fsdb.Scan
        Scan to dump in the "files.json".

    See Also
    --------
    _scan_to_dict
    _scan_files_json
    """
    structure = _scan_to_dict(scan)
    files_json = _scan_files_json(scan)
    with open(files_json, "w") as f:
        json.dump(structure, f, sort_keys=True,
                  indent=4, separators=(',', ': '))


def _is_valid_id(id):
    return True  # haha  (FIXME!)


def _is_db(path):
    """Test if the given path is indeed an FSDB database.

    Do it by checking the presence of the ``MARKER_FILE_NAME``.

    Parameters
    ----------
    path : str
        Path to test as an FSDB database.

    Returns
    -------
    bool
        ``True`` if an FSDB database, else ``False``.
    """
    return os.path.exists(os.path.join(path, MARKER_FILE_NAME))


def _is_safe_to_delete(path):
    """Tests if given path is safe to delete.

    Parameters
    ----------
    path : str
        Path to test for safe deletion.

    Returns
    -------
    bool
        ``True`` if the path is safe to delete, else ``False``.

    Notes
    -----
    A path is safe to delete only if it's a sub-folder of a db.
    """
    path = os.path.abspath(path)
    while True:
        if _is_db(path):
            return True
        newpath = os.path.abspath(os.path.join(path, os.path.pardir))
        if newpath == path:
            return False
        path = newpath


def _delete_file(file):
    """Delete the given file.

    Parameters
    ----------
    file : fsdb.File
        The file to delete.

    Raises
    ------
    OSError
        If the file's path is outside the file's database.

    See Also
    --------
    _is_safe_to_delete
    """
    if file.filename is None:
        return
    fullpath = os.path.join(file.fileset.scan.db.basedir, file.fileset.scan.id,
                            file.fileset.id, file.filename)
    print("delete %s" % fullpath)
    if not _is_safe_to_delete(fullpath):
        raise IOError("Cannot delete files outside of a DB.")
    if os.path.exists(fullpath):
        os.remove(fullpath)


def _delete_fileset(fileset):
    """Delete the given fileset, starting by its `File`(s).

    Parameters
    ----------
    fileset : fsdb.Fileset
        The fileset to delete.

    Raises
    ------
    OSError
        If the fileset's path is outside the fileset's database.

    See Also
    --------
    _is_safe_to_delete
    """
    for f in fileset.files:
        fileset.delete_file(f.id)
    fullpath = os.path.join(fileset.scan.db.basedir, fileset.scan.id,
                            fileset.id)
    if not _is_safe_to_delete(fullpath):
        raise IOError("Cannot delete files outside of a DB.")
    for f in glob.glob(os.path.join(fullpath, "*")):
        os.remove(f)
    if os.path.exists(fullpath):
        os.rmdir(fullpath)


def _delete_scan(scan):
    """Delete the given scan, starting by its `Fileset`s.

    Parameters
    ----------
    scan : fsdb.Scan
        The scan to delete.

    Raises
    ------
    OSError
        If the scan's path is outside the scan's database.

    See Also
    --------
    _is_safe_to_delete
    """
    from shutil import rmtree
    for f in scan.filesets:
        scan.delete_fileset(f.id)
    fullpath = os.path.join(scan.db.basedir, scan.id)
    if not _is_safe_to_delete(fullpath):
        raise IOError("Cannot delete files outside of a DB.")
    rmtree(fullpath, ignore_errors=True)
    if os.path.exists(fullpath):
        os.rmdir(fullpath)


def _filter_query(l, query):
    query_result = []
    for f in l:
        flag_add = True
        for q in query.keys():
            if f.get_metadata(q) is not None and f.get_metadata(q) != query[q]:
                flag_add = False
                break
        if flag_add:
            query_result.append(f)
    return query_result
