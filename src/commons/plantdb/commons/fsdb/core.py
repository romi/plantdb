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

Assuming that the `FSDB` root database directory is `dbroot/`, there is a `Scan` with `'myscan_001'` as `Scan.id` and there are some metadata (see below), you should have the following file structure:
```
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
    ├── users.json                     # user registry
    └── MARKER_FILE_NAME               # ROMI DB marker file
```

The `myscan_001/files.json` file then contains the following structure:
```json
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
```

The metadata of the scan (`metadata.json`), of the set of 'images' files (`<Fileset.id>.json`) and of each 'image' files (`<File.id>.json`) are all stored as JSON files in a separate directory:
```
myscan_001/metadata/
myscan_001/metadata/metadata.json
myscan_001/metadata/images.json
myscan_001/metadata/images/scan_img_01.json
myscan_001/metadata/images/scan_img_02.json
[...]
myscan_001/metadata/images/scan_img_99.json
```
"""

import copy
import json
import logging
import os
import pathlib
import shutil
from collections.abc import Iterable
from pathlib import Path
from shutil import copyfile
from typing import Dict
from typing import Union

from plantdb.commons import db
from plantdb.commons.log import get_logger
from .auth import JWTSessionManager
from .auth import Permission
from .auth import RBACManager
from .exceptions import FileNotFoundError
from .exceptions import FilesetNotFoundError
from .exceptions import ScanNotFoundError
from .file_ops import _delete_file
from .file_ops import _delete_fileset
from .file_ops import _delete_scan
from .file_ops import _load_scan
from .file_ops import _load_scans
from .file_ops import _make_fileset
from .file_ops import _make_scan
from .file_ops import _store_scan
from .lock import LockType
from .lock import ScanLockManager
from .metadata import _get_metadata
from .metadata import _set_metadata
from .metadata import _store_file_metadata
from .metadata import _store_fileset_metadata
from .metadata import _store_scan_metadata
from .path_helpers import _file_path
from .path_helpers import _fileset_path
from .path_helpers import _get_filename
from .path_helpers import _scan_path
from .validation import _is_valid_id
from ..utils import iso_date_now

logger = get_logger(__name__)

#: This file must exist in the root of a folder for it to be considered a valid DB
MARKER_FILE_NAME = "romidb"


def dummy_db(with_scan=False, with_fileset=False, with_file=False):
    """Create a dummy temporary database.

    Parameters
    ----------
    with_scan : bool, optional
        If ``True`` (default to ``False``), add a ``Scan``, named ``"myscan_001"``, to the database.
    with_fileset : bool, optional
        If ``True`` (default to ``False``), add a ``Fileset``, named ``"fileset_001"``, to the scan ``"myscan_001"``.
    with_file : bool, optional
        If ``True`` (default to ``False``), add three ``File``, to the fileset ``"fileset_001"``:

        - a dummy PNG array, named ``"dummy_image"``;
        - a dummy RGB image, named ``"test_image"``;
        - a dummy JSON file, named ``"test_json"``;

    Returns
    -------
    plantdb.commons.fsdb.FSDB
        The dummy database.

    Notes
    -----
    - Returns a 'connected' database, no need to call the `connect()` method.
    - Uses the 'anonymous' user to login.

    Examples
    --------
    >>> from plantdb.commons.fsdb.core import dummy_db
    >>> db = dummy_db(with_file=True)
    >>> db.connect()
    INFO     [plantdb.commons.fsdb] Already connected as 'anonymous' to the database '/tmp/romidb_********'!
    >>> print(db.path())  # the database directory
    /tmp/romidb_********
    >>> print(db.list_scans())
    ['myscan_001']
    >>> scan = db.get_scan("myscan_001")  # get the existing scan
    >>> print(scan.list_filesets())
    ['fileset_001']
    >>> fs = scan.get_fileset("fileset_001")
    >>> print(list(fs.list_files()))
    ['dummy_image', 'test_image', 'test_json']
    >>> f = fs.get_file("test_image")
    >>> print(f.path())
    /tmp/romidb_********/myscan_001/fileset_001/test_image.png
    >>> db.disconnect()  # clean up (delete) the temporary dummy database
    >>> print(db.path().exists())
    False
    """
    from tempfile import mkdtemp
    from plantdb.commons import io

    mydb = Path(mkdtemp(prefix='romidb_'))
    marker_file = mydb / MARKER_FILE_NAME
    marker_file.open(mode='w').close()
    db = FSDB(mydb, dummy=True)
    db.connect()

    if with_file:
        # To create a `File`, existing `Scan` & `Fileset` are required
        with_scan, with_fileset = True, True
    if with_fileset:
        # To create a `Fileset`, an existing `Scan` is required
        with_scan = True

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

    return db


class FSDB(db.DB):
    """Implement a local *File System DataBase* version of abstract class ``db.DB``.

    Implement as a simple local file structure with following directory structure and marker files:
      * directory ``${FSDB.basedir}`` as database root directory;
      * marker file ``MARKER_FILE_NAME`` at database root directory;

    Attributes
    ----------
    basedir : pathlib.Path
        The absolute path to the base directory hosting the database.
    scans : dict[str, plantdb.commons.fsdb.Scan]
        The dictionary of ``Scan`` instances attached to the database, indexed by their identifier.
    is_connected : bool
        ``True`` if the database is connected (locked directory), else ``False``.

    Notes
    -----
    Requires the marker file ``MARKER_FILE_NAME`` at the given ``basedir``.

    See Also
    --------
    plantdb.commons.db.DB
    plantdb.commons.fsdb.core.MARKER_FILE_NAME

    Examples
    --------
    >>> # EXAMPLE 1: Use a temporary dummy local database:
    >>> from plantdb.commons.fsdb.core import dummy_db
    >>> db = dummy_db()
    >>> print(type(db))
    <class 'plantdb.commons.fsdb.FSDB'>
    >>> print(db.path())
    /tmp/romidb_********
    >>> # Create a new `Scan`:
    >>> new_scan = db.create_scan("007")
    >>> print(type(new_scan))
    <class 'plantdb.commons.fsdb.Scan'>
    >>> db.disconnect()  # clean up (delete) the temporary dummy database

    >>> # EXAMPLE 2: Use a local database:
    >>> import os
    >>> from plantdb.commons.fsdb.core import FSDB
    >>> db = FSDB(os.environ.get('ROMI_DB', "/data/ROMI/DB/"))
    >>> db.connect()
    >>> [scan.id for scan in db.get_scans()]  # list scan ids found in database
    >>> scan = db.get_scans()[1]
    >>> [fs.id for fs in scan.get_filesets()]  # list fileset ids found in scan
    >>> db.disconnect()  # clean up (delete) the temporary dummy database
    """

    def __init__(self, basedir: str | Path, required_filesets=['metadata'],
                 dummy: bool = False, logger=logger, session_timeout: int = 3600,
                 max_login_attempts=3, lockout_duration=900, max_concurrent_sessions=10):
        """Database constructor.

        Check given ``basedir`` directory exists and load accessible ``Scan`` objects.

        Parameters
        ----------
        basedir : str or pathlib.Path
            The path to the root directory of the database.
        required_filesets : list of str, optional
            A list of required filesets to consider a scan valid.
            Set it to ``None`` to accept any subdirectory of `basedir` as a valid scan.
            Defaults to ``['metadata']`` to limit scans to the `basedir` subdirectories that have an 'metadata' directory.
        dummy : bool, optional
            If ``True``, deactivate any requirements `required_filesets` & `required_files_json`.
        logger : logging.Logger, optional
            Logger instance to use for logging. Defaults to the module logger.
        session_timeout : int, optional
            Session timeout in seconds. Defaults to 3600 (1 hour).

        Raises
        ------
        NotADirectoryError
            If the given `basedir` is not an existing directory.
        NotAnFSDBError
            If the `MARKER_FILE_NAME` is missing from the `basedir`.

        See Also
        --------
        plantdb.commons.fsdb.core.MARKER_FILE_NAME
        """
        super().__init__()
        self.logger = logger or get_logger(__class__.__name__)
        self.dummy = dummy

        basedir = Path(basedir)
        # Check the given path to root directory of the database is a directory:
        if not basedir.is_dir():
            raise NotADirectoryError(f"Directory {basedir} does not exists!")
        self.basedir = Path(basedir).resolve()
        self.scans = {}
        self.is_connected = False
        self.required_filesets = required_filesets or []

        # Initialize scan lock manager
        self.lock_manager = ScanLockManager(basedir)

        # Initialize session manager
        self.session_manager = JWTSessionManager(session_timeout, max_concurrent_sessions)
        self.current_session_id = None

        # Initialize RBAC manager with groups file in basedir
        users_file = os.path.join(basedir, "users.json")
        groups_file = os.path.join(basedir, "groups.json")
        self.rbac_manager = RBACManager(users_file, groups_file, max_login_attempts, lockout_duration)

    def connect(self):
        """Connect the database by loading the scans' dataset."""
        try:
            # Initialize scan discovery
            self.scans = _load_scans(self)
            self.is_connected = True
            self.logger.info("Connected to database successfully")
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {e}")
            raise

        return True

    def disconnect(self):
        """
        Disconnect from the database and perform cleanup tasks.

        This method disconnects from the database if currently connected.
        If a dummy database is in use, it cleans up by deleting the temporary directory.
        Otherwise, it erases all scans (from memory) and resets the connection status.

        Examples
        --------
        >>> from plantdb.commons.fsdb.core import dummy_db
        >>> db = dummy_db()
        >>> print(db.is_connected)
        True
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        >>> print(db.is_connected)
        False
        """
        if self.dummy:
            logger.info(f"Cleaning up the temporary dummy database at '{self.basedir}'...")
            # Check if directory exists before deleting it
            if os.path.exists(self.basedir):
                shutil.rmtree(self.basedir)
            self.scans = {}
            self.is_connected = False
            self.current_session_id = None
            return

        if self.is_connected:
            for s_id, scan in self.scans.items():
                scan._erase()
            self.scans = {}
            self.is_connected = False
            self.current_session_id = None
        else:
            logger.info(f"Not connected!")
        return

    def reload(self, scan_id=None):
        """Reload the database by scanning datasets.

        Parameters
        ----------
        scan_id : str or list of str, optional
            The name of the scan(s) to reload.
        """
        if self.is_connected:
            if scan_id is None:
                logger.info("Reloading the database...")
                self.scans = _load_scans(self)
            elif isinstance(scan_id, str):
                logger.info(f"Reloading scan '{scan_id}'...")
                self.scans[scan_id] = _load_scan(self, scan_id)
            elif isinstance(scan_id, Iterable):
                [self.reload(scan_i) for scan_i in scan_id]
            else:
                logger.error(f"Wrong parameter `scan_name`, expected a string or list of string but got '{scan_id}'!")
            logger.info("Done!")
        else:
            logger.error(f"You are not connected to the database!")
        return

    def scan_exists(self, scan_id: str) -> bool:
        """Check if a given scan ID exists in the database.

        Parameters
        ----------
        scan_id : str
            The ID of the scan to check.

        Returns
        -------
        bool
            ``True`` if the scan exists, ``False`` otherwise.

        Examples
        --------
        >>> from plantdb.commons.fsdb.core import dummy_db
        >>> db = dummy_db(with_scan=True)
        >>> db.scan_exists("myscan_001")
        True
        >>> db.scan_exists("nonexistent_id")
        False
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        return scan_id in self.scans

    def get_scans(self, query=None, **kwargs):
        """Get the list of `Scan` instances defined in the local database, possibly filtered using a `query`.

        Parameters
        ----------
        query : dict, optional
            A query to use to filter the returned list of scans.
            The metadata must match given ``key`` and ``value`` from the `query` dictionary.

        Other Parameters
        ----------------
        fuzzy : bool, optional
            Whether to use fuzzy matching or not, that is the use of regular expressions.
        owner_only : bool, optional
            Whether to filter the returned list of scans to only include scans owned by the current user.
            Default is ``True``.

        Returns
        -------
        list of plantdb.commons.fsdb.Scan
            List of `Scan`s, filtered by the `query` if any.

        See Also
        --------
        plantdb.commons.fsdb._filter_query

        Examples
        --------
        >>> from plantdb.commons.fsdb.core import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> db.get_scans()
        [<plantdb.commons.fsdb.Scan at *x************>]
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        if not self.is_connected:
            raise ValueError("Database not connected")

        current_user = self.get_current_user()
        if not current_user:
            self.logger.warning("No authenticated user for get_scans")
            return {}

        # Get all scans and filter by access permissions
        accessible_scans = {}

        for scan_id, scan in self.scans.items():
            try:
                metadata = self.rbac_manager.ensure_scan_owner(scan.get_metadata())
                if self.rbac_manager.can_access_scan(current_user, metadata, Permission.READ):
                    accessible_scans[scan_id] = scan
            except Exception as e:
                self.logger.warning(f"Error checking access for scan {scan_id}: {e}")
                continue

        if kwargs.get('owner_only', False):
            if query is None:
                query = {'owner': current_user.username}
            else:
                query.update({'owner': current_user.username})

        # Apply query filter if provided
        if query:
            accessible_scans = _filter_query(list(accessible_scans.values()), query, kwargs.get('fuzzy', False))

        return accessible_scans

    def get_scan(self, scan_id):
        """Get or create a `Scan` instance in the local database.

        Parameters
        ----------
        scan_id : str
            The name of the scan dataset to get/create.
            It should exist if `create` is `False`.

        Raises
        ------
        plantdb.commons.fsdb.ScanNotFoundError
            If the `scan_id` do not exist in the local database and `create` is ``False``.

        Examples
        --------
        >>> from plantdb.commons.fsdb.core import dummy_db
        >>> db = dummy_db(with_scan=True)
        >>> scan = db.get_scan('myscan_001')
        >>> print(scan)
        <plantdb.commons.fsdb.Scan object at **************>
        >>> db.list_scans()
        ['007']
        >>> unknown_scan = db.get_scan('unknown')
        plantdb.commons.fsdb.ScanNotFoundError: Unknown scan id 'unknown'!
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        if not self.is_connected:
            raise ValueError("Database not connected")

        if not self.scan_exists(scan_id):
            raise ScanNotFoundError(self, scan_id)

        current_user = self.get_current_user()
        if not current_user:
            self.logger.warning(f"No authenticated user for `get_scan({scan_id})`")
            # return None
            current_user = self.get_guest_user()
            self.logger.info(f"Using guest user account")

        scan = self.scans[scan_id]
        metadata = self.rbac_manager.ensure_scan_owner(scan.get_metadata())
        if self.rbac_manager.can_access_scan(current_user, metadata, Permission.READ):
            # Use shared lock for read operations
            with self.lock_manager.acquire_lock(scan_id, LockType.SHARED, current_user.username):
                return scan
        else:
            self.logger.warning(f"User '{current_user.username}' cannot access scan {scan_id}!")

        return None

    def create_scan(self, scan_id, metadata=None):
        """Create a new ``Scan`` instance in the local database.

        Parameters
        ----------
        scan_id : str
            The identifier of the scan to create.
            It should contain only alphanumeric characters, underscores, dashes and dots.
            It should be non-empty and not longer than 255 characters
            It should not exist in the local database
        metadata : dict, optional
            A dictionary of metadata to append to the new ``Scan`` instance.
            The key 'owner' will be added/updated using the currently connected user.
            Default is ``None``.

        Returns
        -------
        plantdb.commons.fsdb.Scan
            The ``Scan`` instance created in the local database.

        Raises
        ------
        OSError
            If the `scan_id` is not valid or already exists in the local database.

        See Also
        --------
        plantdb.commons.fsdb._is_valid_id
        plantdb.commons.fsdb._make_scan

        Examples
        --------
        >>> from plantdb.commons.fsdb.core import dummy_db
        >>> db = dummy_db()
        >>> new_scan = db.create_scan('007', metadata={'project': 'GoldenEye'})  # create a new scan dataset
        >>> print(new_scan.get_metadata('owner'))  # default user 'anonymous' for dummy database
        anonymous
        >>> print(new_scan.get_metadata('project'))
        GoldenEye
        >>> scan = db.create_scan('007')  # attempt to create an existing scan dataset
        OSError: Given scan identifier '007' already exists!
        >>> scan = db.create_scan('0/07')  # attempt to create a scan dataset using invalid characters
        OSError: Invalid scan identifier '0/07'!
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        current_user = self.get_current_user()
        if not current_user:
            raise PermissionError("No authenticated user")

        # Check CREATE permission
        if not self.rbac_manager.has_permission(current_user, Permission.CREATE):
            raise PermissionError("Insufficient permissions to create scan")

        if self.scan_exists(scan_id):
            raise ValueError(f"Scan {scan_id} already exists")

        # Prepare metadata with ownership
        if metadata is None:
            metadata = {}

        # Set current user as owner if not specified
        if 'owner' not in metadata:
            metadata['owner'] = current_user.username
            now = iso_date_now()
            metadata['created'] = now  # creation timestamp
            metadata['last_modified'] = now  # modification timestamp
            metadata['created_by'] = current_user.name

        # Validate sharing groups if specified
        if 'sharing' in metadata:
            sharing_groups = metadata['sharing']
            if not isinstance(sharing_groups, list):
                raise ValueError("Sharing field must be a list of group names")
            if not self.rbac_manager.validate_sharing_groups(sharing_groups):
                raise ValueError("One or more sharing groups do not exist")

        # Use exclusive lock for scan creation
        with self.lock_manager.acquire_lock(scan_id, LockType.EXCLUSIVE, current_user.username):
            # Verify if the given `scan_id` already exists in the local database
            if self.scan_exists(scan_id):
                raise IOError(f"Given scan identifier '{scan_id}' already exists!")
            try:
                # Initialize scan object
                scan = Scan(self, scan_id)  # Initialize a new Scan instance
                scan_path = _make_scan(scan)  # Create directory structure

                # Cannot use scan.set_metadata(initial_metadata) here as ownership is not granted yet!
                _set_metadata(scan.metadata, metadata, None)  # add metadata dictionary to the new scan
                _store_scan_metadata(scan)

                scan.store()  # store the new scan in the local database
                self.scans[scan_id] = scan  # Update scans dictionary with newly created

                logger.info(f"Created scan '{scan_id}' for user '{current_user.username}'")
                return scan

            except Exception as e:
                self.logger.error(f"Failed to create scan {scan_id}: {e}")
                # Cleanup on failure
                try:
                    if os.path.exists(scan_path):
                        import shutil
                        shutil.rmtree(scan_path)
                except:
                    pass
                return None

    def delete_scan(self, scan_id):
        """Delete an existing `Scan` from the local database.

        Parameters
        ----------
        scan_id : str
            The name of the scan to delete from the local database.

        Raises
        ------
        IOError
            If the `id` do not exist in the local database.

        See Also
        --------
        plantdb.commons.fsdb._delete_scan

        Examples
        --------
        >>> from plantdb.commons.fsdb.core import FSDB
        >>> from plantdb.commons.fsdb.core import dummy_db
        >>> db = dummy_db()
        >>> new_scan = db.create_scan('007')
        >>> print(new_scan)
        <plantdb.commons.fsdb.Scan object at 0x7f0730b1e390>
        >>> db.delete_scan('007')
        >>> scan = db.get_scan('007')
        >>> print(scan)
        None
        >>> db.delete_scan('008')
        OSError: Invalid id
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        current_user = self.get_current_user()
        if not current_user:
            raise PermissionError("No authenticated user")

        if not self.scan_exists(scan_id):
            raise ValueError(f"Scan {scan_id} does not exist")

        # Check DELETE permission for this specific scan
        scan = self.scans[scan_id]
        metadata = self.rbac_manager.ensure_scan_owner(scan.get_metadata())

        if not self.rbac_manager.can_access_scan(current_user, metadata, Permission.DELETE):
            raise PermissionError("Insufficient permissions to delete scan")

        # Check if scan is locked
        lock_status = self.get_scan_lock_status(scan_id)
        if lock_status['is_locked'] and lock_status['locked_by'] != current_user.username:
            raise RuntimeError(f"Scan {scan_id} is locked by another user")

        # Use exclusive lock for scan deletion
        with self.lock_manager.acquire_lock(scan_id, LockType.EXCLUSIVE, current_user.username):
            try:
                # Get the Scan instance from database
                scan = self.scans[scan_id]
                _delete_scan(scan)  # delete the scan directory
                self.scans.pop(scan_id)  # remove the scan from the scan list
                logger.info(f"Deleted scan '{scan_id}' by user '{current_user.username}'")
                return True

            except Exception as e:
                self.logger.error(f"Failed to delete scan {scan_id}: {e}")
                raise
        return

    def path(self) -> pathlib.Path:
        """Get the path to the local database root directory.

        Examples
        --------
        >>> from plantdb.commons.fsdb.core import dummy_db
        >>> db = dummy_db()
        >>> print(db.path())
        /tmp/romidb_********
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        return copy.deepcopy(self.basedir)

    def list_scans(self, query=None, fuzzy=False, owner_only=True) -> list:
        """Get the list of scans in identifiers the local database.

        Parameters
        ----------
        query : dict, optional
            A query to use to filter the returned list of scans.
            The metadata must match given ``key`` and ``value`` from the `query` dictionary.
        fuzzy : bool
            Whether to use fuzzy matching or not, that is the use of regular expressions.

        Returns
        -------
        list[str]
            The list of scan identifiers in the local database.

        See Also
        --------
        plantdb.commons.fsdb._filter_query

        Examples
        --------
        >>> from plantdb.commons.fsdb.core import dummy_db
        >>> db = dummy_db(with_scan=True)
        >>> db.list_scans()  # list scans owned by the current user
        ['myscan_001']
        >>> db.create("batman", "Bruce Wayne", 'joker')
        >>> db.connect('batman', 'joker')
        >>> db.list_scans()  # list scans owned by the current user
        >>> []
        >>> db.list_scans(owner_only=False)  # list all scans
        ['myscan_001']
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        current_user = self.get_current_user()
        if not current_user:
            owner_only = False

        if query is None and not owner_only:
            return list(self.scans.keys())
        else:
            if owner_only:
                if query is None:
                    query = {'owner': current_user.username}
                else:
                    query.update({'owner': current_user.username})
            return [scan.id for scan in _filter_query(list(self.scans.values()), query, fuzzy)]

    def get_scan_lock_status(self, scan_id: str) -> Dict:
        """
        Get current lock status for a specific scan.

        Parameters
        ----------
        scan_id : str
            The name of the scan in the local database.

        Returns
        -------
        dict
            Dictionary with lock status information
        """
        return self.lock_manager.get_lock_status(scan_id)

    def cleanup_scan_locks(self):
        """
        Emergency cleanup of all scan locks.
        Use with caution - only call when you're sure no operations are in progress.
        """
        self.lock_manager.cleanup_all_locks()
        logger.warning("All scan locks have been cleaned up")

    def list_active_locks(self) -> Dict[str, Dict]:
        """
        List all currently active locks across all scans.

        Returns
        -------
        dict
            Dictionary mapping scan IDs to their lock status
        """
        active_locks = {}
        for scan_id in self.scans.keys():
            lock_status = self.get_scan_lock_status(scan_id)
            if lock_status['exclusive'] or lock_status['shared']:
                active_locks[scan_id] = lock_status

        return active_locks

    # User management methods

    def validate_user(self, username: str, password: str) -> bool:
        """Validate the user credentials.

        Parameters
        ----------
        username : str
            The username provided by the user attempting to log in.
        password : str
            The password provided by the user attempting to log in.

        Returns
        -------
        bool
            ``True`` if the login attempt is successful, ``False`` otherwise.

        Raises
        ------
        KeyError
            If there is an issue accessing necessary user data.
        """
        return self.rbac_manager.users.validate(username, password)

    def login(self, username: str, password: str) -> Union[str, None]:
        """Authenticate user and create session.

        Parameters
        ----------
        username : str
            Username for authentication
        password : str
            Password for authentication

        Returns
        -------
        Union[str, None]
            Returns the user session ID if successful, ``None`` otherwise.
        """
        if not self.is_connected:
            self.logger.error("Database not connected")
            return None

        if self.validate_user(username, password):
            session_id = self.session_manager.create_session(username)
            try:
                assert session_id is not None
            except AssertionError:
                self.logger.warning(f"User {username} has reached max concurrent sessions")
            else:
                # Store current session for this connection
                self.current_session_id = session_id
                self.logger.info(f"User {username} logged in successfully")
            return session_id
        else:
            self.logger.error(f"Failed to login user {username}")
            return None

    def logout(self, session_id: str = None):
        """Logout current user and invalidate session."""
        if not session_id:
            session_id = self.current_session_id

        if session_id and session_id in self.session_manager.sessions:
            username = self.session_manager.sessions[session_id]['username']
            del self.session_manager.sessions[session_id]
            self.session_manager.invalidate_session(session_id)

            if session_id == self.current_session_id:
                self.current_session_id = None

            self.logger.info(f"User {username} logged out successfully")
            return True

        return False

    def create_user(self, username, fullname, password, roles=None) -> None:
        """
        Create a new user with the specified details.

        Parameters
        ----------
        username : str
            The unique username for the new user.
        fullname : str
            The full name of the new user.
        password : str
            The password for the new user.
        roles : list[str], optional
            A list of roles to assign to the new user. Default is None.

        See Also
        --------
        RBACManager.users.create : Method used to actually create the user.
        """
        return self.rbac_manager.users.create(username, fullname, password, roles)

    def get_guest_user(self):
        """
        Retrieve the guest user information from the RBAC manager.

        Returns the guest user object containing all relevant data.

        Parameters
        ----------
        None

        Returns
        -------
        dict
            A dictionary representing the guest user with all attributes.
            For example, it might contain keys like 'id', 'name', etc.

        Examples
        --------
        >>> rbac_manager = RBACManager()
        >>> user_info = rbac_manager.get_guest_user()
        >>> print(user_info)
        {'id': 12345, 'name': 'Guest User', 'role': 'Guest'}

        Notes
        -----
        This method interacts with the underlying RBAC manager to fetch guest
        user information. Ensure that the RBAC manager is correctly configured.

        See Also
        --------
        rbac_manager.get_guest_user : The underlying method used by this function.
        """
        return self.rbac_manager.get_guest_user()

    def get_current_user(self):
        """Get the currently authenticated user.

        Returns
        -------
        User or None
            Current user object if authenticated, None otherwise
        """
        if not self.current_session_id:
            return None

        username = self.session_manager.session_username(self.current_session_id)
        if not username:
            # Session expired or invalid
            self.current_session_id = None
            return None
        else:
            return self.rbac_manager.users.get_user(username)

    # Group management methods

    def create_group(self, name, users=None, description=None):
        """Create a new group.

        Parameters
        ----------
        name : str
            Unique name for the group
        users : set, optional
            Initial set of users to add to the group
        description : str, optional
            An optional description of the group

        Returns
        -------
        Group
            The created group object

        Raises
        ------
        PermissionError
            If user lacks permission to create groups
        ValueError
            If group already exists
        """
        current_user = self.get_current_user()
        if not current_user:
            raise PermissionError("No authenticated user")

        return self.rbac_manager.create_group(current_user, name, users, description)

    def add_user_to_group(self, group_name, username):
        """Add a user to a group.

        Parameters
        ----------
        group_name : str
            Name of the group
        username : str
            Username to add to the group

        Returns
        -------
        bool
            True if user was added successfully

        Raises
        ------
        PermissionError
            If user lacks permission to modify the group
        """
        current_user = self.get_current_user()
        if not current_user:
            raise PermissionError("No authenticated user")

        if not self.rbac_manager.add_user_to_group(current_user, group_name, username):
            raise PermissionError("Insufficient permissions or operation failed")
        return True

    def remove_user_from_group(self, group_name, username):
        """Remove a user from a group.

        Parameters
        ----------
        group_name : str
            Name of the group
        username : str
            Username to remove from the group

        Returns
        -------
        bool
            True if user was removed successfully

        Raises
        ------
        PermissionError
            If user lacks permission to modify the group
        """
        current_user = self.get_current_user()
        if not current_user:
            raise PermissionError("No authenticated user")

        if not self.rbac_manager.remove_user_from_group(current_user, group_name, username):
            raise PermissionError("Insufficient permissions or operation failed")
        return True

    def delete_group(self, group_name):
        """Delete a group.

        Parameters
        ----------
        group_name : str
            Name of the group to delete

        Returns
        -------
        bool
            True if group was deleted successfully

        Raises
        ------
        PermissionError
            If user lacks permission to delete groups
        """
        current_user = self.get_current_user()
        if not current_user:
            raise PermissionError("No authenticated user")

        if not self.rbac_manager.delete_group(current_user, group_name):
            raise PermissionError("Insufficient permissions or group not found")
        return True

    def list_groups(self):
        """List all groups.

        Returns
        -------
        list
            List of Group objects

        Raises
        ------
        PermissionError
            If user is not authenticated
        """
        current_user = self.get_current_user()
        if not current_user:
            raise PermissionError("No authenticated user")

        groups = self.rbac_manager.list_groups(current_user)
        return groups if groups is not None else []

    def get_user_groups(self, username=None):
        """Get groups for a user.

        Parameters
        ----------
        username : str, optional
            Username to query. If None, uses current user.

        Returns
        -------
        list
            List of Group objects the user belongs to

        Raises
        ------
        PermissionError
            If no authenticated user
        """
        current_user = self.get_current_user()
        if not current_user:
            raise PermissionError("No authenticated user")

        if username is None:
            username = current_user.username

        return self.rbac_manager.get_user_groups(username)

    def get_scan_access_summary(self, scan_id):
        """Get access summary for current user on a scan.

        Parameters
        ----------
        scan_id : str
            The scan identifier

        Returns
        -------
        dict or None
            Access summary dictionary if scan exists and is accessible

        Raises
        ------
        PermissionError
            If no authenticated user
        """
        current_user = self.get_current_user()
        if not current_user:
            raise PermissionError("No authenticated user")

        if scan_id not in self.scans:
            return None

        scan = self.scans[scan_id]
        try:
            metadata = self.rbac_manager.ensure_scan_owner(scan.get_metadata())
            return self.rbac_manager.get_user_scan_role_summary(current_user, metadata)
        except Exception as e:
            self.logger.warning(f"Error getting access summary for scan {scan_id}: {e}")
            return None


class Scan(db.Scan):
    """Implement ``Scan`` for the local *File System DataBase* from abstract class ``db.Scan``.

    Implementation of a scan as a simple file structure with:
      * directory ``${Scan.db.basedir}/${Scan.db.id}`` as scan root directory;
      * (OPTIONAL) directory ``${Scan.db.basedir}/${Scan.db.id}/metadata`` containing JSON metadata file
      * (OPTIONAL) JSON file ``metadata.json`` with Scan metadata

    Attributes
    ----------
    db : plantdb.commons.fsdb.FSDB
        A local database instance hosting this ``Scan`` instance.
    id : str
        The identifier of this ``Scan`` instance in the local database `db`.
    metadata : dict
        A metadata dictionary.
    filesets : dict[str, plantdb.commons.fsdb.core.Fileset]
        A dictionary of `Fileset` instances, indexed by their identifier.

    Notes
    -----
    Optional directory ``metadata`` & JSON file ``metadata.json`` are found when using method ``set_metadata()``.

    See Also
    --------
    plantdb.commons.db.Scan

    Examples
    --------
    >>> import os
    >>> from plantdb.commons.fsdb.core import Scan
    >>> from plantdb.commons.fsdb.core import dummy_db
    >>> db = dummy_db()
    >>> # Example #1: Initialize a `Scan` object using an `FSBD` object:
    >>> scan = Scan(db, '007')
    >>> print(type(scan))
    <class 'plantdb.commons.fsdb.Scan'>
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
    >>> db.disconnect()  # clean up (delete) the temporary dummy database

    >>> # Example #2: Get it from an `FSDB` object:
    >>> db = dummy_db()
    >>> scan = db.create_scan('007')
    >>> print(type(scan))
    <class 'plantdb.commons.fsdb.Scan'>
    >>> print(db.get_scan('007'))  # This time the `Scan` object is found in the `FSBD`
    <plantdb.commons.fsdb.Scan object at 0x7f34fc860fd0>
    >>> print(os.listdir(db.path()))  # And it is found under the `basedir` directory
    ['007', 'romidb']
    >>> print(os.listdir(os.path.join(db.path(), scan.id)))  # Same goes for the metadata
    ['metadata']
    >>> db.dummy = False  # to avoid cleaning up the
    >>> db.disconnect()
    >>> # When reconnecting to db, if created scan is EMPTY (no Fileset & File) it is not found!
    >>> db.connect()
    >>> print(db.get_scan('007'))
    None
    >>> db.dummy = True  # to clean up the temporary dummy database
    >>> db.disconnect()  # clean up (delete) the temporary dummy database

    >>> # Example #3: Use an existing database:
    >>> from os import environ
    >>> from plantdb.commons.fsdb.core import FSDB
    >>> db = FSDB(environ.get('ROMI_DB', "/data/ROMI/DB/"))
    >>> db.connect()
    >>> scan = db.get_scan('sango_90_300_36')
    >>> scan.get_metadata()
    """

    def __init__(self, db, scan_id):
        """Scan dataset constructor.

        Parameters
        ----------
        db : plantdb.commons.fsdb.FSDB
            The database to put/find the scan dataset.
        scan_id : str
            The scan dataset name, should be unique in the `db`.
        """
        super().__init__(db, scan_id)
        # Defines attributes:
        self.metadata = {}
        self.filesets = {}
        self.measures = None

    def _erase(self):
        """Erase the filesets and metadata associated to this scan."""
        for fs_id, fs in self.filesets.items():
            fs._erase()
        self.metadata = {}
        self.filesets = {}
        self.measures = None
        return

    @property
    def owner(self):
        # If no owner is defined, set it to the anonymous user
        if 'owner' not in self.metadata:
            _set_metadata(self.metadata, 'owner', 'anonymous')
            _store_scan_metadata(self)
            self.db.reload(self.id)
        return self.metadata.get('owner')

    def fileset_exists(self, fileset_id: str) -> bool:
        """Check if a given fileset ID exists in the database.

        Parameters
        ----------
        fileset_id : str
            The ID of the fileset to check.

        Returns
        -------
        bool
            ``True`` if the fileset exists, ``False`` otherwise.

        Examples
        --------
        >>> from plantdb.commons.fsdb.core import dummy_db
        >>> db = dummy_db(with_scan=True)
        >>> scan = db.get_scan('myscan_001')
        >>> scan.fileset_exists("myfileset_001")
        False
        >>> scan.create_fileset("myfileset_001")
        >>> scan.fileset_exists("myfileset_001")
        True
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        return fileset_id in self.filesets

    def get_filesets(self, query=None, fuzzy=False):
        """Get the list of `Fileset` instances defined in the current scan dataset, possibly filtered using a `query`.

        Parameters
        ----------
        query : dict, optional
            A query to use to filter the returned list of files.
            The metadata must match given ``key`` and ``value`` from the `query` dictionary.
        fuzzy : bool
            Whether to use fuzzy matching or not, that is the use of regular expressions.

        Returns
        -------
        list of plantdb.commons.fsdb.core.Fileset
            List of `Fileset`s, filtered by the `query` if any.

        See Also
        --------
        plantdb.commons.fsdb._filter_query

        Examples
        --------
        >>> from plantdb.commons.fsdb.core import dummy_db
        >>> db = dummy_db(with_fileset=True)
        >>> scan = db.get_scan('myscan_001')
        >>> scan.get_filesets()
        [<plantdb.commons.fsdb.core.Fileset at *x************>]
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        return [self.get_fileset(fs.id) for fs in _filter_query(list(self.filesets.values()), query, fuzzy)]

    def get_fileset(self, fs_id):
        """Get or create a `Fileset` instance, of given `id`, in the current scan dataset.

        Parameters
        ----------
        fs_id : str
            The name of the fileset to get.

        Returns
        -------
        Fileset
            The retrieved or created fileset.

        Notes
        -----
        If the `id` do not exist in the local database and `create` is `False`, `None` is returned.

        Examples
        --------
        >>> from plantdb.commons.fsdb.core import dummy_db
        >>> db = dummy_db(with_fileset=True)
        >>> scan = db.get_scan('myscan_001')
        >>> scan.list_filesets()
        ['fileset_001']
        >>> new_fileset = scan.create_fileset('007')
        >>> print(new_fileset)
        <plantdb.commons.fsdb.core.Fileset object at **************>
        >>> scan.list_filesets()
        ['fileset_001', '007']
        >>> unknown_fs = scan.get_fileset('unknown')
        plantdb.commons.fsdb.core.FilesetNotFoundError: Unknown fileset id 'unknown'!
        >>> print(unknown_fs)
        None
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        # Use shared lock for read operations
        with self.db.lock_manager.acquire_lock(self.id, LockType.SHARED, self.db.user or "anonymous"):
            if not self.fileset_exists(fs_id):
                raise FilesetNotFoundError(self, fs_id)

            return self.filesets[fs_id]

    def get_metadata(self, key=None, default={}):
        """Get the metadata associated to a scan.

        Parameters
        ----------
        key : str
            A key that should exist in the scan's metadata.
        default : Any, optional
            The default value to return if the key do not exist in the metadata.
            Default is an empty dictionary``{}``.

        Returns
        -------
        any
            If `key` is ``None``, returns a dictionary.
            Else, returns the value attached to this key.
        """
        # Use shared lock for read operations
        with self.db.lock_manager.acquire_lock(self.id, LockType.SHARED, self.db.user or "anonymous"):
            return _get_metadata(self.metadata, key, default)

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
        return _get_metadata(self.measures, key, default={})

    def set_metadata(self, data, value=None):
        """Add a new metadata to the scan.

        Parameters
        ----------
        data : str or dict
            If a string, a key to address the `value`.
            If a dictionary, update the metadata dictionary with `data` (`value` is then unused).
        value : any, optional
            The value to assign to `data` if the latest is not a dictionary.

        Raises
        ------
        PermissionError
            If user lacks permission to modify metadata
        ValueError
            If metadata validation fails

        Examples
        --------
        >>> import json
        >>> from plantdb.commons.fsdb.core import dummy_db
        >>> from plantdb.commons.fsdb.path_helpers import _scan_metadata_path
        >>> db = dummy_db(with_file=True)
        >>> scan = db.get_scan("myscan_001")
        >>> scan.set_metadata("test", "value")
        >>> p = _scan_metadata_path(scan)
        >>> print(p.exists())
        True
        >>> print(json.load(p.open(mode='r')))
        {'test': 'value'}
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        if not self.db.user:
            raise ValueError("No user authenticated")

        current_user = self.db.get_current_user()
        if not current_user:
            raise PermissionError("No authenticated user")

        # Get current metadata for validation
        old_metadata = self.db.rbac_manager.ensure_scan_owner(self.get_metadata())

        if isinstance(data, str):
            if value is None:
                logger.warning(f"No value given for key '{data}'!")
            new_metadata = {data: value}
        else:
            try:
                assert isinstance(data, dict)
            except AssertionError:
                raise PermissionError(f"Invalid metadata type '{type(data)}'")
            else:
                new_metadata = {data: value}

        # Validate metadata changes
        if not self.db.rbac_manager.validate_scan_metadata_access(current_user, old_metadata, new_metadata):
            raise PermissionError("Insufficient permissions to modify scan metadata")

        # Validate sharing groups if present
        if 'sharing' in new_metadata:
            sharing_groups = new_metadata['sharing']
            if not isinstance(sharing_groups, list):
                raise ValueError("Sharing field must be a list of group names")
            if not self.db.rbac_manager.validate_sharing_groups(sharing_groups):
                raise ValueError("One or more sharing groups do not exist")

        # Check WRITE permission for this scan
        if not self.db.rbac_manager.can_access_scan(current_user, old_metadata, Permission.WRITE):
            raise PermissionError("Insufficient permissions to modify scan")

        # Use exclusive lock for metadata updates
        with self.db.lock_manager.acquire_lock(self.id, LockType.EXCLUSIVE, self.db.user):
            # Update metadata
            _set_metadata(self.metadata, new_metadata, None)
            # Ensure modification timestamp
            _set_metadata(self.metadata, 'last_modified', iso_date_now())
            _store_scan_metadata(self)

            logger.info(f"Updated metadata for scan '{self.id}' by user '{self.db.user}'")

        return

    def create_fileset(self, fs_id, metadata=None):
        """Create a new `Fileset` instance in the local database attached to the current `Scan` instance.

        Parameters
        ----------
        fs_id : str
            The name of the fileset to create. It should not exist in the current `Scan` instance.
        metadata : dict, optional
            A dictionary with the initial metadata for this new fileset.

        Returns
        -------
        plantdb.commons.fsdb.core.Fileset
            The `Fileset` instance created in the current `Scan` instance.

        Raises
        ------
        IOError
            If the `id` already exists in the current `Scan` instance.
            If the `id` is not valid.

        See Also
        --------
        plantdb.commons.fsdb._is_valid_id
        plantdb.commons.fsdb._make_fileset

        Examples
        --------
        >>> from plantdb.commons.fsdb.core import dummy_db
        >>> db = dummy_db(with_fileset=True)
        >>> scan = db.get_scan('myscan_001')
        >>> scan.list_filesets()
        ['fileset_001']
        >>> new_fs = scan.create_fileset('fs_007')
        >>> scan.list_filesets()
        ['fileset_001', 'fs_007']
        >>> wrong_fs = scan.create_fileset('fileset_001')
        OSError: Given fileset identifier 'fileset_001' already exists!
        >>> wrong_fs = scan.create_fileset('fileset/001')
        OSError: Invalid fileset identifier 'fileset/001'!
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        # Check authentication for fileset creation
        if not self.db.user:
            raise ValueError("No user authenticated")
        # Check ownership
        if self.owner != self.db.user:
            raise PermissionError(f"Only the owner can create filesets in scan '{self.id}'")
        # Verify if the given `fs_id` is valid
        if not _is_valid_id(fs_id):
            raise IOError(f"Invalid fileset identifier '{fs_id}'!")

        # Use exclusive lock for fileset creation
        logger.info(f"Creating fileset '{fs_id}' from scan '{self.id}'")
        with self.db.lock_manager.acquire_lock(self.id, LockType.EXCLUSIVE, self.db.user):
            # Verify if the given `fs_id` already exists in the local database
            if self.fileset_exists(fs_id):
                raise ValueError(f"Fileset '{fs_id}' already exists in scan '{self.id}'")

            # Create the new Fileset
            fileset = Fileset(self, fs_id)  # Initialize a new Fileset instance
            _make_fileset(fileset)  # Create directory structure

            # Set initial metadata
            initial_metadata = metadata or {}
            now = iso_date_now()
            initial_metadata['created'] = now  # creation timestamp
            initial_metadata['last_modified'] = now  # modification timestamp
            initial_metadata['created_by'] = self.db.user

            # Cannot use fileset.set_metadata(initial_metadata) here as ownership is not granted yet!
            _set_metadata(fileset.metadata, initial_metadata, None)  # add metadata dictionary to the new fileset
            _store_fileset_metadata(fileset)

            self.filesets.update({fs_id: fileset})  # Update scan's filesets dictionary
            self.store()  # Store fileset instance to the JSON

            logger.info(f"Created new fileset '{fs_id}' in scan '{self.id}' for user '{self.db.user}'")

        return fileset

    def store(self):
        """Save changes to the scan main JSON FILE (``files.json``)."""
        _store_scan(self)
        return

    def delete_fileset(self, fs_id):
        """Delete a given fileset from the scan dataset.

        Parameters
        ----------
        fs_id : str
            Name of the fileset to delete.

        Examples
        --------
        >>> from plantdb.commons.fsdb.core import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> scan = db.get_scan('myscan_001')
        >>> scan.list_filesets()
        ['fileset_001']
        >>> scan.delete_fileset('fileset_001')
        >>> scan.list_filesets()
        []
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        # Check authentication for fileset creation
        if not self.db.user:
            raise ValueError("No user authenticated")
        # Check ownership
        if self.owner != self.db.user:
            raise PermissionError(f"Only the owner can delete filesets from scan '{self.id}'")

        # Use exclusive lock for fileset deletion
        with self.db.lock_manager.acquire_lock(self.id, LockType.EXCLUSIVE, self.db.user):
            # Verify if the given `fs_id` exists in the local database
            if not self.fileset_exists(fs_id):
                raise ValueError(f"Fileset '{fs_id}' does not exist in scan '{self.id}'")

            fs = self.filesets[fs_id]
            _delete_fileset(fs)  # delete the fileset
            self.filesets.pop(fs_id)  # remove the Fileset instance from the scan
            self.store()  # save the changes to the scan main JSON FILE (``files.json``)

            logger.info(f"Deleted fileset '{fs_id}' from scan '{self.id}' by user '{self.db.user}'")
        return

    def path(self) -> pathlib.Path:
        """Get the path to the local scan dataset.

        Examples
        --------
        >>> from plantdb.commons.fsdb.core import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> scan = db.get_scan("myscan_001")
        >>> scan.path()  # should be '/tmp/romidb_********/myscan_001'
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        return _scan_path(self)

    def list_filesets(self, query=None, fuzzy=False) -> list:
        """Get the list of filesets identifiers in the scan dataset.

        Parameters
        ----------
        query : dict, optional
            A query to use to filter the returned list of filesets.
            The metadata must match given ``key`` and ``value`` from the `query` dictionary.
        fuzzy : bool
            Whether to use fuzzy matching or not, that is the use of regular expressions.

        Returns
        -------
        list[str]
            The list of filesets identifiers in the scan dataset.

        See Also
        --------
        plantdb.commons.fsdb._filter_query

        Examples
        --------
        >>> from plantdb.commons.fsdb.core import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> scan = db.get_scan("myscan_001")
        >>> scan.list_filesets()
        ['fileset_001']
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        if query == None:
            return list(self.filesets.keys())
        else:
            return [fs.id for fs in _filter_query(list(self.filesets.values()), query, fuzzy)]


class Fileset(db.Fileset):
    """Implement ``Fileset`` for the local *File System DataBase* from abstract class ``db.Fileset``.

    Implementation of a fileset as a simple files structure with:
      * directory ``${FSDB.basedir}/${FSDB.scan.id}/${Fileset.id}`` containing set of files;
      * directory ``${FSDB.basedir}/${FSDB.scan.id}/metadata`` containing JSON metadata associated to files;
      * JSON file ``files.json`` containing the list of files from fileset;

    Attributes
    ----------
    db : plantdb.commons.fsdb.FSDB
        A local database instance hosting the ``Scan`` instance.
    scan : plantdb.commons.fsdb.Scan
        A scan instance hosting this ``Fileset`` instance.
    id : str
        The identifier of this ``Fileset`` instance in the `scan`.
    metadata : dict
        A metadata dictionary.
    files : dict[str, plantdb.commons.fsdb.core.File]
        A dictionary of `File` instances attached to the fileset, indexed by their identifier.

    See Also
    --------
    plantdb.commons.db.Fileset
    """

    def __init__(self, scan, fs_id):
        """Constructor.

        Parameters
        ----------
        scan : plantdb.commons.fsdb.Scan
            A scan instance containing the fileset.
        fs_id : str
            The identifier of the fileset instance.
        """
        super().__init__(scan, fs_id)
        # Defines attributes:
        self.metadata = {}
        self.files = {}

    def _erase(self):
        """Erase the files and metadata associated to this fileset."""
        for f_id, f in self.files.items():
            f._erase()
        # Reinitialize the attributes
        self.metadata = {}
        self.files = {}
        return

    def file_exists(self, file_id: str) -> bool:
        """Check if a given file ID exists in the database.

        Parameters
        ----------
        file_id : str
            The ID of the file to check.

        Returns
        -------
        bool
            ``True`` if the file exists, ``False`` otherwise.

        Examples
        --------
        >>> from plantdb.commons.fsdb.core import dummy_db
        >>> db = dummy_db(with_scan=True)
        >>> scan = db.get_scan('myscan_001')
        >>> scan.file_exists("myfile_001")
        False
        >>> scan.create_file("myfile_001")
        >>> scan.file_exists("myfile_001")
        True
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        return file_id in self.files

    def get_files(self, query=None, fuzzy=False):
        """Get the list of `File` instances defined in the current fileset, possibly filtered using a `query`.

        Parameters
        ----------
        query : dict, optional
            Query to use to get a list of files.
        fuzzy : bool
            Whether to use fuzzy matching or not, that is the use of regular expressions.

        Returns
        -------
        list of plantdb.commons.fsdb.core.File
            List of `File`s, filtered by the query if any.

        See Also
        --------
        plantdb.commons.fsdb._filter_query

        Examples
        --------
        >>> from plantdb.commons.fsdb.core import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> scan = db.get_scan('myscan_001')
        >>> fs = scan.get_fileset('fileset_001')
        >>> fs.get_files()
        [<plantdb.commons.fsdb.core.File at *x************>,
         <plantdb.commons.fsdb.core.File at *x************>,
         <plantdb.commons.fsdb.core.File at *x************>]
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        return _filter_query(list(self.files.values()), query, fuzzy)

    def get_file(self, f_id):
        """Get or create a `File` instance, of given `f_id`, in the current fileset.

        Parameters
        ----------
        f_id : str
            Name of the file to get/create.

        Returns
        -------
        plantdb.commons.fsdb.core.File
            The retrieved or created file.

        Examples
        --------
        >>> from plantdb.commons.fsdb.core import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> scan = db.get_scan("myscan_001")
        >>> fs = scan.get_fileset("fileset_001")
        >>> f = fs.get_file("test_image")
        >>> # To read the file you need to load the right reader from plantdb.commons.io
        >>> from plantdb.commons.io import read_image
        >>> img = read_image(f)
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        # Use shared lock for read operations
        with self.db.lock_manager.acquire_lock(self.scan.id, LockType.SHARED, self.db.user or "anonymous"):
            if not self.file_exists(f_id):
                raise FileNotFoundError(self, f_id)

            return self.files[f_id]

    def get_metadata(self, key=None, default={}):
        """Get the metadata associated to a fileset.

        Parameters
        ----------
        key : str
            A key that should exist in the fileset's metadata.
        default : Any, optional
            The default value to return if the key do not exist in the metadata.
            Default is an empty dictionary``{}``.

        Returns
        -------
        any
            If `key` is ``None``, returns a dictionary.
            Else, returns the value attached to this key.

        Examples
        --------
        >>> import json
        >>> from plantdb.commons.fsdb.core import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> scan = db.get_scan("myscan_001")
        >>> fs = scan.get_fileset('fileset_001')
        >>> fs.set_metadata("test", "value")
        >>> print(fs.get_metadata("test"))
        'value'
        >>> db.dummy=False  # to avoid cleaning up the temporary dummy database
        >>> db.disconnect()
        >>> db.connect('anonymous')
        >>> scan = db.get_scan("myscan_001")
        >>> fs = scan.get_fileset('fileset_001')
        >>> print(fs.get_metadata("test"))
        'value'
        >>> db.dummy=True
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        return _get_metadata(self.metadata, key, default)

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
        >>> from plantdb.commons.fsdb.core import dummy_db
        >>> from plantdb.commons.fsdb.path_helpers import _fileset_metadata_json_path
        >>> db = dummy_db(with_file=True)
        >>> scan = db.get_scan("myscan_001")
        >>> fs = scan.get_fileset('fileset_001')
        >>> fs.set_metadata("test", "value")
        >>> p = _fileset_metadata_json_path(fs)
        >>> print(p.exists())
        True
        >>> print(json.load(p.open(mode='r')))
        {'test': 'value'}
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        _set_metadata(self.metadata, data, value)
        # Ensure modification timestamp
        self.metadata['last_modified'] = iso_date_now()
        _store_fileset_metadata(self)
        return

    def create_file(self, f_id, metadata=None):
        """Create a new `File` instance in the local database attached to the current `Fileset` instance.

        Parameters
        ----------
        f_id : str
            The name of the file to create.

        Returns
        -------
        plantdb.commons.fsdb.core.File
            The `File` instance created in the current `Fileset` instance.

        Examples
        --------
        >>> from plantdb.commons.fsdb.core import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> scan = db.get_scan('myscan_001')
        >>> fs = scan.get_fileset('fileset_001')
        >>> fs.list_files()
        ['dummy_image', 'test_image', 'test_json']
        >>> new_f = fs.create_file('file_007')
        >>> fs.list_files()
        ['dummy_image', 'test_image', 'test_json', 'file_007']
        >>> print([f.name for f in fs.path().iterdir()])  # the file only exist in the database, not on drive!
        ['dummy_image.png', 'test_json.json', 'test_image.png']
        >>> md = {"Name": "Bond, James Bond"}  # Create an example dictionary to save as JSON
        >>> from plantdb.commons import io
        >>> io.write_json(new_f, md, "json")  # write the file on drive
        >>> print([f.name for f in fs.path().iterdir()])
        ['file_007.json', 'test_image.png', 'test_json.json', 'dummy_image.png']
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        # Check authentication for file creation
        if not self.db.user:
            raise ValueError("No user authenticated")
        # Verify if the given `fs_id` is valid
        if not _is_valid_id(f_id):
            raise IOError(f"Invalid file identifier '{f_id}'!")

        # Use exclusive lock for file creation
        with self.db.lock_manager.acquire_lock(self.scan.id, LockType.EXCLUSIVE, self.db.user):
            # Verify if the given `fs_id` already exists in the local database
            if self.file_exists(f_id):
                raise IOError(f"Given file identifier '{f_id}' already exists!")

            # Create the new File
            file = File(self, f_id)  # Initialize a new File instance

            # Set initial metadata
            initial_metadata = metadata or {}
            now = iso_date_now()
            initial_metadata['created'] = now  # creation timestamp
            initial_metadata['last_modified'] = now  # modification timestamp
            initial_metadata['created_by'] = self.db.user

            # Cannot use fileset.set_metadata(initial_metadata) here as ownership is not granted yet!
            _set_metadata(file.metadata, initial_metadata, None)  # add metadata dictionary to the new scan
            _store_file_metadata(file)

            self.files.update({f_id: file})  # Update filesets's files dictionary
            self.store()  # Store fileset instance to the JSON

            logger.debug(f"Created new file '{f_id}' in '{self.scan.id}/{self.id}' for user '{self.db.user}'")

        return file

    def delete_file(self, f_id):
        """Delete a given file from the current fileset.

        Parameters
        ----------
        f_id : str
            Name of the file to delete.

        Examples
        --------
        >>> from plantdb.commons.fsdb.core import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> scan = db.get_scan('myscan_001')
        >>> fs = scan.get_fileset('fileset_001')
        >>> fs.list_files()
        ['dummy_image', 'test_image', 'test_json']
        >>> fs.delete_file('dummy_image')
        INFO     [plantdb.commons.fsdb] Deleted JSON metadata file for file 'dummy_image' from 'myscan_001/fileset_001'.
        INFO     [plantdb.commons.fsdb] Deleted file 'dummy_image' from 'myscan_001/fileset_001'.
        >>> fs.list_files()
        ['test_image', 'test_json']
        >>> print([f.name for f in fs.path().iterdir()])  # the file has been removed from the drive and the database
        ['test_json.json', 'test_image.png']
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        # Verify if the given `fs_id` exists in the local database
        if not self.file_exists(f_id):
            logging.warning(f"Given file identifier '{f_id}' does NOT exists!")
            return

        f = self.files[f_id]
        _delete_file(f)  # delete the file
        self.files.pop(f_id)  # remove the File instance from the fileset
        self.store()  # save the changes to the scan main JSON FILE (``files.json``)
        return

    def store(self):
        """Save changes to the scan main JSON FILE (``files.json``)."""
        self.scan.store()
        return

    def path(self) -> pathlib.Path:
        """Get the path to the local fileset.

        Examples
        --------
        >>> from plantdb.commons.fsdb.core import dummy_db
        >>> db = dummy_db(with_scan=True, with_file=True)
        >>> [scan.id for scan in db.get_scans()]  # list scan ids found in database
        ['myscan_001']
        >>> scan = db.get_scan("myscan_001")
        >>> print(scan.path())
        /tmp/romidb_********/myscan_001
        >>> [fs.id for fs in scan.get_filesets()]  # list fileset ids found in scan
        ['fileset_001']
        >>> fs = scan.get_fileset("fileset_001")
        >>> print(fs.path())
        /tmp/romidb_********/myscan_001/fileset_001
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        return _fileset_path(self)

    def list_files(self, query=None, fuzzy=False) -> list:
        """Get the list of files identifiers in the fileset.

        Parameters
        ----------
        query : dict, optional
            A query to use to filter the returned list of files.
            The metadata must match given ``key`` and ``value`` from the `query` dictionary.
        fuzzy : bool
            Whether to use fuzzy matching or not, that is the use of regular expressions.

        Returns
        -------
        list[str]
            The list of file identifiers in the fileset.

        See Also
        --------
        plantdb.commons.fsdb._filter_query

        Examples
        --------
        >>> from plantdb.commons.fsdb.core import dummy_db
        >>> db = dummy_db(with_scan=True, with_file=True)
        >>> scan = db.get_scan("myscan_001")
        >>> fs = scan.get_fileset("fileset_001")
        >>> fs.list_files()
        ['dummy_image', 'test_image', 'test_json']
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        if query is None:
            return list(self.files.keys())
        else:
            return [f.id for f in _filter_query(list(self.files.values()), query, fuzzy)]


class File(db.File):
    """Implement ``File`` for the local *File System DataBase* from abstract class ``db.File``.

    Attributes
    ----------
    db : plantdb.commons.fsdb.FSDB
        Database where to find the fileset.
    fileset : plantdb.commons.fsdb.core.Fileset
        Set of files containing the file.
    id : str
        Name of the file in the ``FSDB`` local database.
    filename : str
        File name.
    metadata : dict
        Dictionary of metadata attached to the file.

    See Also
    --------
    plantdb.commons.db.File

    Notes
    -----
    `File` must be writen using ``write_raw`` or ``write`` methods to exist on disk.
    Else they are just referenced in the database!

    Contrary to other classes (``Scan`` & ``Fileset``) the uniqueness is not checked!
    """

    def __init__(self, fileset, f_id, **kwargs):
        super().__init__(fileset, f_id, **kwargs)
        self.metadata = {}

    def _erase(self):
        self.id = None
        self.metadata = {}
        return

    def get_metadata(self, key=None, default={}):
        """Get the metadata associated to a file.

        Parameters
        ----------
        key : str
            A key that should exist in the file's metadata.
        default : Any, optional
            The default value to return if the key do not exist in the metadata.
            Default is an empty dictionary``{}``.

        Returns
        -------
        any
            If `key` is ``None``, returns a dictionary.
            Else, returns the value attached to this key.

        Examples
        --------
        >>> from plantdb.commons.fsdb.core import dummy_db
        >>> from plantdb.commons.fsdb.path_helpers import _file_metadata_path
        >>> db = dummy_db(with_file=True)
        >>> scan = db.get_scan("myscan_001")
        >>> fs = scan.get_fileset('fileset_001')
        >>> f = fs.get_file("test_json")
        >>> print(f.get_metadata())
        {'random json': True}
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        return _get_metadata(self.metadata, key, default)

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
        >>> from plantdb.commons.fsdb.core import dummy_db
        >>> from plantdb.commons.fsdb.path_helpers import _file_metadata_path
        >>> db = dummy_db(with_file=True)
        >>> scan = db.get_scan("myscan_001")
        >>> fs = scan.get_fileset('fileset_001')
        >>> file = fs.get_file("test_json")
        >>> file.set_metadata("test", "value")
        >>> p = _file_metadata_path(file)
        >>> print(p.exists())
        True
        >>> print(json.load(p.open(mode='r')))
        {'random json': True, 'test': 'value'}
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        _set_metadata(self.metadata, data, value)
        # Ensure modification timestamp
        self.metadata['last_modified'] = iso_date_now()
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
        >>> from plantdb.commons.fsdb.core import dummy_db
        >>> from plantdb.commons.fsdb.path_helpers import _file_metadata_path
        >>> db = dummy_db(with_file=True)
        >>> scan = db.get_scan("myscan_001")
        >>> fs = scan.get_fileset('fileset_001')
        >>> file = fs.get_file("test_json")
        >>> new_file = fs.create_file('test_json2')
        >>> new_file.import_file(file.path())
        >>> print(new_file.path().exists())
        True
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
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
        """Save changes to the scan main JSON FILE (``files.json``)."""
        self.fileset.store()
        return

    def read_raw(self):
        """Read the file and return its contents.

        Returns
        -------
        bytes
            The contents of the file.

        Examples
        --------
        >>> from plantdb.commons.fsdb.core import dummy_db
        >>> db = dummy_db(with_file=True)
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
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
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
        >>> from plantdb.commons.fsdb.core import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> scan = db.get_scan("myscan_001")
        >>> fs = scan.get_fileset("fileset_001")
        >>> new_f = fs.create_file('file_007')
        >>> md = {"Name": "Bond, James Bond"}  # Create an example dictionary to save as JSON
        >>> import json
        >>> data = json.dumps(md).encode()
        >>> print(data)
        b'{"Name": "Bond, James Bond"}'
        >>> new_f.write_raw(data, 'json')
        >>> print([f.name for f in fs.path().iterdir()])
        ['dummy_image.png', 'test_json.json', 'test_image.png', 'file_007.json']
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
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
        >>> from plantdb.commons.fsdb.core import dummy_db
        >>> db = dummy_db(with_file=True)
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
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
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
        >>> from plantdb.commons.fsdb.core import dummy_db
        >>> db = dummy_db(with_file=True)
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
        >>> print([f.name for f in fs.path().iterdir()])
        ['dummy_image.png', 'test_json.json', 'test_image.png', 'file_007.json']
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
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
        >>> from plantdb.commons.fsdb.core import dummy_db
        >>> db = dummy_db(with_scan=True, with_file=True)
        >>> scan = db.get_scan("myscan_001")
        >>> fs = scan.get_fileset("fileset_001")
        >>> fs.list_files()
        ['dummy_image', 'test_image', 'test_json']
        >>> f = fs.get_file('dummy_image')
        >>> f.path()
        /tmp/romidb_********/myscan_001/fileset_001/dummy_image.png
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        return _file_path(self)


def _filter_query(obj_list, query=None, fuzzy=False, debug=False):
    """Filter a list of `Scan`s, `Fileset`s or `File`s using a `query` on their metadata.

    Parameters
    ----------
    obj_list : list
        A list of `Scan`s, `Fileset`s or `File`s to filter.
    query : dict, optional
        A filtering query in the form of a dictionary.
        The list of instances must have metadata matching ``key`` and ``value`` from the `query`.
    fuzzy : bool
        Whether to use fuzzy matching or not, that is the use of regular expressions.
    debug : bool
        If active, print each step of the fuzzy matching process for each object of the list.

    Returns
    -------
    list
        The list of `Scan`s, `Fileset`s or `File`s filtered by the query, if any.

    See Also
    --------
    plantdb.commons.utils.partial_match

    Examples
    --------
    >>> from plantdb.commons.fsdb.core import dummy_db
    >>> from plantdb.commons.fsdb.core import _filter_query
    >>> db = dummy_db(with_scan=True, with_file=True)
    >>> db.connect()
    >>> scan = db.get_scan("myscan_001")
    >>> fs = scan.get_fileset("fileset_001")
    >>> print({obj.id: obj.metadata for f in fs.get_files()})
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
    >>> # Let's create some metadata for the Scan instance:
    >>> scan.set_metadata("object", {"env":"virtual", "test": "ok"})
    >>> # Now try to filter the list of Scan instances using a partial metadata dictionary:
    >>> scans = _filter_query(db.get_scans(), query={"object": {"env":"virtual"}})
    >>> print([scan.id for scan in scans])
    ['myscan_001']
    >>> # You may use regular expression using `fuzzy=True`:
    >>> scans = _filter_query(db.get_scans(), query={"object": {"env":"virt.*"}}, fuzzy=True)
    >>> print([scan.id for scan in scans])
    ['myscan_001']
    >>> db.disconnect()  # clean up (delete) the temporary dummy database
    """
    from plantdb.commons.utils import partial_match
    if query is None or query == {}:
        # If there is no `query` return the unfiltered list of instances
        query_result = [f for f in obj_list]
    else:
        # Else apply the filter on metadata using key(s) and value(s) from `query`:
        query_result = []
        query_debug = {}
        for obj in obj_list:
            query_debug[obj.id] = {}
            f_query = []  # boolean list gathering the "filter test results"
            for q in query.keys():
                query_test = partial_match(obj.get_metadata(q), query[q], fuzzy=fuzzy)
                query_debug[obj.id][q] = {
                    'query_value': query[q],
                    'metadata_value': obj.get_metadata(q),
                    'fuzzy': fuzzy,
                    'result': query_test,
                }
                try:
                    # assert f.get_metadata(q) == query[q]
                    assert query_test
                except AssertionError:
                    f_query.append(False)
                else:
                    f_query.append(True)
            # All requirements have to be fulfilled:
            if all(f_query):
                query_result.append(obj)
        if debug:
            print("query_debug: ", query_debug)
    return query_result
