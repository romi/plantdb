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
    ├── (LOCK_FILE_NAME)               # "lock file", present if DB is connected
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
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from shutil import copyfile
from typing import Dict

import bcrypt

from plantdb.commons import db
from plantdb.commons.log import get_logger
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
from ..utils import date_now

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
    >>> from plantdb.commons.fsdb import dummy_db
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
      * (OPTIONAL) lock file ``LOCK_FILE_NAME`` at database root directory when connected;

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
    Lock file ``LOCK_FILE_NAME`` is found only when connecting an FSBD instance to the given ``basedir``.

    See Also
    --------
    plantdb.commons.db.DB
    plantdb.commons.fsdb.core.MARKER_FILE_NAME
    plantdb.commons.fsdb.core.LOCK_FILE_NAME

    Examples
    --------
    >>> # EXAMPLE 1: Use a temporary dummy local database:
    >>> from plantdb.commons.fsdb import dummy_db
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
    >>> from plantdb.commons.fsdb import FSDB
    >>> db = FSDB(os.environ.get('ROMI_DB', "/data/ROMI/DB/"))
    >>> db.connect()
    >>> [scan.id for scan in db.get_scans()]  # list scan ids found in database
    >>> scan = db.get_scans()[1]
    >>> [fs.id for fs in scan.get_filesets()]  # list fileset ids found in scan
    >>> db.disconnect()  # clean up (delete) the temporary dummy database
    """

    def __init__(self, basedir, required_filesets=['metadata'], dummy=False):
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

        basedir = Path(basedir)
        # Check the given path to root directory of the database is a directory:
        if not basedir.is_dir():
            raise NotADirectoryError(f"Directory {basedir} does not exists!")

        self.basedir = Path(basedir).resolve()
        self.dummy = dummy

        # User management attributes
        self.users = {}  # {username: {password: str, created: timestamp}}
        self._load_users()
        # Create or load the users database
        self.user = None
        self.max_login_attempts = 5
        self.lockout_duration = timedelta(minutes=15)

        # Database state
        self.is_connected = False
        self.scans = {}

        # Configuration
        self.required_filesets = required_filesets if not dummy else None

        # Initialize scan lock manager
        self.lock_manager = ScanLockManager(basedir)

    def _load_users(self):
        """Load sers dictionary from file."""
        users_file = self.basedir / 'users.json'
        if not users_file.is_file():
            # Create the users database as a JSON file
            users_file.touch()
            self.users = {}
            with open(users_file, "w") as f:
                json.dump(self.users, f, indent=2)
            logger.info(f"Initialized the new database at '{self.basedir}'!")
        else:
            # Load the users database
            with open(users_file, "r") as f:
                self.users = json.load(f)
            logger.debug(f"Loaded {len(self.users)} users from '{users_file}'.")
        return

    def _save_users(self):
        """Save users dictionary to file using an atomic write pattern."""
        users_file = self.basedir / 'users.json'
        # Create a temporary file in the same directory
        temp_file = users_file.with_suffix('.tmp')
        try:
            # Write to the temporary file first
            with open(temp_file, "w") as f:
                json.dump(self.users, f, indent=2)

            # Rename the temporary file to the final filename (atomic operation on most file systems)
            os.replace(temp_file, users_file)
        except Exception as e:
            # If anything goes wrong, clean up the temporary file
            if temp_file.exists():
                temp_file.unlink()
            logger.error(f"Failed to save users database: {str(e)}")
            raise
        return

    def create_user(self, username, fullname, password):
        """Create a new user and store the user information in a file.

        Parameters
        ----------
        username : str
            The username of the user to be created.
            This will be converted to lowercase.
        fullname : str
            The full name of the user to be created.
        password : str
            The password of the user to be created.

        Examples
        --------
        >>> from plantdb.commons.fsdb import FSDB
        >>> from plantdb.commons.fsdb import dummy_db
        >>> db = dummy_db()
        >>> db.create_user('batman', "Bruce Wayne", "joker")
        >>> db.connect('batman', 'joker')
        >>> print(db.user)
        batman
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        username = username.lower()  # Convert the username to lowercase to maintain uniformity.
        timestamp = date_now(fmt='%Y-%m-%d_%H:%M:%S')  # Get the current timestamp for tracking user creation time.

        # Verify if the login is available
        try:
            assert username not in self.users
        except AssertionError:
            logger.error(f"User '{username}' already exists!")
            return

        # Generate salt and hash password
        salt = bcrypt.gensalt()
        hashed = bcrypt.hashpw(password.encode('utf-8'), salt)

        # Add the new user's data to the `self.users` dictionary.
        self.users[username] = {
            'password': hashed.decode('utf-8'),  # Store the hashed password as a string.
            'fullname': fullname,  # Store the provided full name of the user.
            'created': timestamp,  # Store the formatted timestamp to record when the user was created.
            'last_login': None,  # Store the timestamp of the last login.
            'failed_attempts': 0,  # Store the number of failed login attempts.
        }

        # Save all user data (including the newly created user) to 'users.json' file.
        self._save_users()
        logger.info(f"Created user '{username}' with fullname '{fullname}'.")

        return f"Welcome {self.users[username]['fullname']}, please login...'"

    def _lock_db(self):
        """
        DEPRECATED: Database-level locking replaced by scan-level locking
        This method is kept for backward compatibility but does nothing
        """
        logger.warning("_lock_db is deprecated. Use scan-level locking instead.")
        pass

    def _unlock_db(self):
        """
        DEPRECATED: Database-level locking replaced by scan-level locking
        This method is kept for backward compatibility but does nothing
        """
        logger.warning("_unlock_db is deprecated. Use scan-level locking instead.")
        pass

    def connect(self, login=None, password=""):
        """Connect to the local database.

        Handle DB "locking" system by adding a `LOCK_FILE_NAME` file in the DB.

        Parameters
        ----------
        login : str, optional
            The user login, if not defined, use the ``'anonymous'`` user.
            If defined, it should match a known user.

        Raises
        ------
        plantdb.commons.db.DBBusyError
            If the database is already used by another process.
            This is achieved by searching for a `LOCK_FILE_NAME` lock file in the `basedir`.

        See Also
        --------
        plantdb.commons.fsdb.LOCK_FILE_NAME

        Examples
        --------
        >>> from plantdb.commons.fsdb import dummy_db
        >>> db = dummy_db()
        >>> print(db.is_connected)
        True
        >>> db.create_user("batman", "Bruce Wayne", 'joker')
        >>> db.connect('batman', 'joker')
        >>> print(db.is_connected)
        True
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        >>> print(db.is_connected)
        False
        """
        # Store current user before attempting connection
        prev_user = copy.copy(self.user)

        # Handle user authentication
        if login is not None and login != "anonymous":
            try:
                # Validate user credentials
                assert self.validate_user(login, password)
            except AssertionError:
                # User doesn't exist or wrong password
                logger.error(f"Did not connect to database.")
                return
            else:
                self.user = login
                self.users[self.user]['last_login'] = date_now(fmt='%Y-%m-%d_%H:%M:%S')
                self._save_users()
        else:
            # Create and use anonymous user if no login provided
            if 'anonymous' not in self.users:
                self.create_user("anonymous", "Guy Fawkes", "AlanMoore")
            self.user = "anonymous"
            # Warn about anonymous user usage except for dummy databases
            if not self.dummy:
                logger.warning("Using anonymous user is discouraged!")
                logger.info("Use `connect(login='username')` to login as a user.")

        # Handle database connection
        if not self.is_connected:
            self.scans = _load_scans(self)
            self.is_connected = True
        else:
            # Already connected - log appropriate message based on user change
            if self.user != prev_user:
                logger.info(f"Connected as '{self.user}' to the database '{self.path()}'.")
            else:
                logger.info(f"Already connected as '{self.user}' to the database '{self.path()}'!")
        return

    def validate_user(self, username: str, password: str) -> bool:
        """Validate the user login.

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
        if self._is_account_locked(username):
            logger.warning(f"Account locked: {username}")
            return False

        if username not in self.users:
            logger.error(f"Login attempt for non-existent user: {username}")
            return False

        # Verify password
        stored_hash = self.users[username]['password']
        if bcrypt.checkpw(password.encode('utf-8'), stored_hash.encode('utf-8')):
            # Reset failed attempts on successful login
            self.users[username]['failed_attempts'] = 0
            self.users[username]['last_login'] = date_now(fmt='%Y-%m-%d_%H:%M:%S')
            return True

        # Handle failed login attempt
        self._record_failed_attempt(username)
        logger.error(f"Invalid credentials for user '{username}'")
        return False

    def user_exists(self, username: str) -> bool:
        """Check if the user exists in the local database."""
        return username in self.users

    def _is_account_locked(self, username: str) -> bool:
        """Verify if the account is locked.

        Parameters
        ----------
        username : str
            The username of the account to check for lock status.

        Returns
        -------
        bool
            ``True`` if the account is locked, otherwise ``False``.
        """
        attempts = self.users[username].get('failed_attempts', 0)
        if attempts >= self.max_login_attempts:
            last_attempt_str = self.users[username].get('last_failed_attempt', date_now(fmt='%Y-%m-%d_%H:%M:%S'))
            last_attempt = datetime.strptime(last_attempt_str, '%Y-%m-%d_%H:%M:%S')
            if datetime.now() - last_attempt < self.lockout_duration:
                logger.warning(f"Locking account {username} for more than {self.max_login_attempts} consecutive failed login attempts.")
                logger.info(f"Try logging in after {self.lockout_duration}min.")
                return True
        return False

    def _record_failed_attempt(self, username: str) -> None:
        """Record failed login attempt.

        Parameters
        ----------
        username : str
            The username for which the failed login attempt is being recorded.
        """
        self.users[username]['failed_attempts'] += 1
        self.users[username]['last_failed_attempt'] = date_now(fmt='%Y-%m-%d_%H:%M:%S')
        # Save the updated user data to file
        self._save_users()
        logger.warning(f"Failed login attempt (n={self.users[username]['failed_attempts']}) for user: {username}")
        return

    def disconnect(self):
        """
        Disconnect from the database and perform cleanup tasks.

        This method disconnects from the database if currently connected.
        If a dummy database is in use, it cleans up by deleting the temporary directory.
        Otherwise, it erases all scans (from memory) and resets the connection status.

        Examples
        --------
        >>> from plantdb.commons.fsdb import dummy_db
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
            return

        if self.is_connected:
            for s_id, scan in self.scans.items():
                scan._erase()
            self.scans = {}
            self.is_connected = False
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
        >>> from plantdb.commons.fsdb import dummy_db
        >>> db = dummy_db(with_scan=True)
        >>> db.scan_exists("myscan_001")
        True
        >>> db.scan_exists("nonexistent_id")
        False
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        return scan_id in self.scans

    def get_scans(self, query=None, fuzzy=False, owner_only=True):
        """Get the list of `Scan` instances defined in the local database, possibly filtered using a `query`.

        Parameters
        ----------
        query : dict, optional
            A query to use to filter the returned list of scans.
            The metadata must match given ``key`` and ``value`` from the `query` dictionary.
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
        >>> from plantdb.commons.fsdb import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> db.get_scans()
        [<plantdb.commons.fsdb.Scan at *x************>]
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        if owner_only:
            if query is None:
                query = {'owner': self.user}
            else:
                query.update({'owner': self.user})
        return [self.get_scan(scan.id) for scan in _filter_query(list(self.scans.values()), query, fuzzy)]

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
        >>> from plantdb.commons.fsdb import dummy_db
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

        # Use shared lock for read operations
        with self.lock_manager.acquire_lock(scan_id, LockType.SHARED, self.user or "anonymous"):
            if not self.scan_exists(scan_id):
                raise ValueError(f"Scan '{scan_id}' does not exist")

            return Scan(self, scan_id)

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
        >>> from plantdb.commons.fsdb import dummy_db
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
        # Verify if the connection is established first
        if not self.is_connected:
            raise ValueError("Database not connected")
        # Verify if a user is authenticated
        if not self.user:
            raise ValueError("No user authenticated")

        # Use exclusive lock for scan creation
        with self.lock_manager.acquire_lock(scan_id, LockType.EXCLUSIVE, self.user):
            # Verify if the given `scan_id` is valid
            if not _is_valid_id(scan_id):
                raise IOError(f"Invalid scan identifier '{scan_id}'!")
            # Verify if the given `scan_id` already exists in the local database
            if self.scan_exists(scan_id):
                raise IOError(f"Given scan identifier '{scan_id}' already exists!")

            # Initialize scan object
            scan = Scan(self, scan_id)
            _make_scan(scan)

            # Set initial metadata including owner
            initial_metadata = metadata or {}
            initial_metadata['owner'] = self.user
            initial_metadata['created'] = date_now('%Y-%m-%d_%H:%M:%S')
            initial_metadata['created_by'] = self.user

            scan.set_metadata(metadata)  # add metadata dictionary to the new scan

            # Reload scans to include the new one
            self.reload(scan_id)

            logger.info(f"Created scan '{scan_id}' for user '{self.user}'")
            return scan

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
        >>> from plantdb.commons.fsdb import FSDB
        >>> from plantdb.commons.fsdb import dummy_db
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
        # Verify if the connection is established first
        if not self.is_connected:
            raise ValueError("Database not connected")
        # Verify if a user is authenticated
        if not self.user:
            raise ValueError("No user authenticated")

        # Use exclusive lock for scan deletion
        with self.lock_manager.acquire_lock(scan_id, LockType.EXCLUSIVE, self.user):
            # Verify if the given `scan_id` exists in the local database
            if not self.scan_exists(scan_id):
                logging.warning(f"Given scan identifier '{scan_id}' does NOT exists!")
                return

            # Get the Scan instance from database
            scan = self.get_scan(scan_id)
            # Check ownership
            if scan.owner() != self.user:
                raise PermissionError(f"Only the owner can delete scan '{scan_id}'")

            _delete_scan(scan)  # delete the scan directory
            self.scans.pop(scan_id)  # remove the scan from the scan list

            logger.info(f"Deleted scan '{scan_id}' by user '{self.user}'")

        return

    def path(self) -> pathlib.Path:
        """Get the path to the local database root directory.

        Examples
        --------
        >>> from plantdb.commons.fsdb import dummy_db
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
        >>> from plantdb.commons.fsdb import dummy_db
        >>> db = dummy_db(with_scan=True)
        >>> db.list_scans()  # list scans owned by the current user
        ['myscan_001']
        >>> db.create_user("batman", "Bruce Wayne", 'joker')
        >>> db.connect('batman', 'joker')
        >>> db.list_scans()  # list scans owned by the current user
        >>> []
        >>> db.list_scans(owner_only=False)  # list all scans
        ['myscan_001']
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        if query is None and not owner_only:
            return list(self.scans.keys())
        else:
            return [scan.id for scan in self.get_scans(query, fuzzy, owner_only)]

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
    filesets : dict[str, plantdb.commons.fsdb.Fileset]
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
    >>> from plantdb.commons.fsdb import Scan
    >>> from plantdb.commons.fsdb import dummy_db
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
    >>> scan = db.get_scan('007', create=True)
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
    >>> from plantdb.commons.fsdb import FSDB
    >>> db = FSDB(environ.get('ROMI_DB', "/data/ROMI/DB/"))
    >>> db.connect(unsafe=True)
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
        self.metadata = None
        self.filesets = {}
        self.measures = None

    def _erase(self):
        """Erase the filesets and metadata associated to this scan."""
        for fs_id, fs in self.filesets.items():
            fs._erase()
        self.metadata = None
        self.filesets = {}
        self.measures = None
        return

    @property
    def owner(self):
        # If no owner is defined, set it to the anonymous user
        if 'owner' not in self.metadata:
            self.set_metadata('owner', "anonymous")
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
        >>> from plantdb.commons.fsdb import dummy_db
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
        list of plantdb.commons.fsdb.Fileset
            List of `Fileset`s, filtered by the `query` if any.

        See Also
        --------
        plantdb.commons.fsdb._filter_query

        Examples
        --------
        >>> from plantdb.commons.fsdb import dummy_db
        >>> db = dummy_db(with_fileset=True)
        >>> scan = db.get_scan('myscan_001')
        >>> scan.get_filesets()
        [<plantdb.commons.fsdb.Fileset at *x************>]
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
        >>> from plantdb.commons.fsdb import dummy_db
        >>> db = dummy_db(with_fileset=True)
        >>> scan = db.get_scan('myscan_001')
        >>> scan.list_filesets()
        ['fileset_001']
        >>> new_fileset = scan.get_fileset('007', create=True)
        >>> print(new_fileset)
        <plantdb.commons.fsdb.Fileset object at **************>
        >>> scan.list_filesets()
        ['fileset_001', '007']
        >>> unknown_fs = scan.get_fileset('unknown')
        plantdb.commons.fsdb.FilesetNotFoundError: Unknown fileset id 'unknown'!
        >>> print(unknown_fs)
        None
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        # Use shared lock for read operations
        with self.db.lock_manager.acquire_lock(self.id, LockType.SHARED, self.db.user or "anonymous"):
            if not self.fileset_exists(fs_id):
                raise ValueError(f"Fileset '{fs_id}' does not exist in scan '{self.id}'")

            return Fileset(self, fs_id)

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

        Examples
        --------
        >>> import json
        >>> from plantdb.commons.fsdb import dummy_db
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

        # Check ownership for metadata changes
        current_owner = self.owner()
        if current_owner and current_owner != self.db.user:
            raise PermissionError(f"Only the owner can modify metadata for scan '{self.id}'")

        # Use exclusive lock for metadata updates
        with self.db.lock_manager.acquire_lock(self.id, LockType.EXCLUSIVE, self.db.user):
            if self.metadata == None:
                self.metadata = {}
            # Update metadata
            _set_metadata(self.metadata, data, value)
            # Ensure modification timestamp
            self.metadata['last_modified'] = date_now('%Y-%m-%d_%H:%M:%S')
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
        plantdb.commons.fsdb.Fileset
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
        >>> from plantdb.commons.fsdb import dummy_db
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
        if self.owner() != self.db.user:
            raise PermissionError(f"Only the owner can create filesets in scan '{self.id}'")
        # Verify if the given `fs_id` is valid
        if not _is_valid_id(fs_id):
            raise IOError(f"Invalid fileset identifier '{fs_id}'!")

        # Use exclusive lock for fileset creation
        with self.db.lock_manager.acquire_lock(self.id, LockType.EXCLUSIVE, self.db.user):
            # Verify if the given `fs_id` already exists in the local database
            if self.fileset_exists(fs_id):
                raise ValueError(f"Fileset '{fs_id}' already exists in scan '{self.id}'")

            # Create the new fileset
            fileset = Fileset(self, fs_id)  # Initialize fileset instance
            _make_fileset(fileset)  # Create directory structure
            self.store()  # Store fileset instance to the
            self.filesets.update({fs_id: fileset})  # Update scan's filesets dictionary

            # Set initial metadata
            initial_metadata = metadata or {}
            initial_metadata['created'] = date_now('%Y-%m-%d_%H:%M:%S')
            initial_metadata['created_by'] = self.db.user
            fileset.set_metadata(initial_metadata)

            logger.info(f"Created new fileset '{fs_id}' in scan  '{self.id}' for user '{self.db.user}'")

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
        >>> from plantdb.commons.fsdb import dummy_db
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
        if self.owner() != self.db.user:
            raise PermissionError(f"Only the owner can delete filesets from scan '{self.id}'")

        # Use exclusive lock for fileset deletion
        with self.db.lock_manager.acquire_lock(self.id, LockType.EXCLUSIVE, self.db.user):
            # Verify if the given `fs_id` exists in the local database
            if not self.fileset_exists(fs_id):
                raise ValueError(f"Fileset '{fs_id}' does not exist in scan '{self.id}'")

            _delete_fileset(self.get_fileset(fs_id, create=False))  # delete the fileset
            self.filesets.pop(fs_id)  # remove the Fileset instance from the scan
            self.store()  # save the changes to the scan main JSON FILE (``files.json``)

            logger.info(f"Deleted fileset '{fs_id}' from scan '{self.id}' by user '{self.db.user}'")
        return

    def path(self) -> pathlib.Path:
        """Get the path to the local scan dataset.

        Examples
        --------
        >>> from plantdb.commons.fsdb import dummy_db
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
        >>> from plantdb.commons.fsdb import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> scan = db.get_scan("myscan_001")
        >>> scan.list_filesets()
        ['fileset_001']
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        if query == None:
            return list(self.filesets.keys())
        else:
            return [fs.id for fs in self.get_filesets(query, fuzzy)]


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
    files : dict[str, plantdb.commons.fsdb.File]
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
        self.metadata = None
        self.files = {}

    def _erase(self):
        """Erase the files and metadata associated to this fileset."""
        for f_id, f in self.files.items():
            f._erase()
        # Reinitialize the attributes
        self.metadata = None
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
        >>> from plantdb.commons.fsdb import dummy_db
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
        list of plantdb.commons.fsdb.File
            List of `File`s, filtered by the query if any.

        See Also
        --------
        plantdb.commons.fsdb._filter_query

        Examples
        --------
        >>> from plantdb.commons.fsdb import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> scan = db.get_scan('myscan_001')
        >>> fs = scan.get_fileset('fileset_001')
        >>> fs.get_files()
        [<plantdb.commons.fsdb.File at *x************>,
         <plantdb.commons.fsdb.File at *x************>,
         <plantdb.commons.fsdb.File at *x************>]
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
        plantdb.commons.fsdb.File
            The retrieved or created file.

        Examples
        --------
        >>> from plantdb.commons.fsdb import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> scan = db.get_scan("myscan_001")
        >>> fs = scan.get_fileset("fileset_001")
        >>> f = fs.get_file("test_image")
        >>> # To read the file you need to load the right reader from plantdb.commons.io
        >>> from plantdb.commons.io import read_image
        >>> img = read_image(f)
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        if self.file_exists(f_id):
            return self.files[f_id]
        else:
            raise FileNotFoundError(f_id)

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
        >>> from plantdb.commons.fsdb import dummy_db
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
        >>> from plantdb.commons.fsdb import dummy_db
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
        if self.metadata == None:
            self.metadata = {}
        _set_metadata(self.metadata, data, value)
        # Ensure modification timestamp
        self.metadata['last_modified'] = date_now('%Y-%m-%d_%H:%M:%S')
        _store_fileset_metadata(self)
        return

    def create_file(self, f_id):
        """Create a new `File` instance in the local database attached to the current `Fileset` instance.

        Parameters
        ----------
        f_id : str
            The name of the file to create.

        Returns
        -------
        plantdb.commons.fsdb.File
            The `File` instance created in the current `Fileset` instance.

        Examples
        --------
        >>> from plantdb.commons.fsdb import dummy_db
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
        # Verify if the given `fs_id` is valid
        if not _is_valid_id(f_id):
            raise IOError(f"Invalid file identifier '{f_id}'!")
        # Verify if the given `fs_id` already exists in the local database
        if self.file_exists(f_id):
            raise IOError(f"Given file identifier '{f_id}' already exists!")

        file = File(self, f_id)
        self.files.update({f_id: file})
        self.store()
        return file

    def delete_file(self, f_id):
        """Delete a given file from the current fileset.

        Parameters
        ----------
        f_id : str
            Name of the file to delete.

        Examples
        --------
        >>> from plantdb.commons.fsdb import dummy_db
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

        _delete_file(self.get_file(f_id, create=False))  # delete the file
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
        >>> from plantdb.commons.fsdb import dummy_db
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
        >>> from plantdb.commons.fsdb import dummy_db
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
            return [f.id for f in self.get_files(query, fuzzy)]


class File(db.File):
    """Implement ``File`` for the local *File System DataBase* from abstract class ``db.File``.

    Attributes
    ----------
    db : plantdb.commons.fsdb.FSDB
        Database where to find the fileset.
    fileset : plantdb.commons.fsdb.Fileset
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
        self.metadata = None

    def _erase(self):
        self.id = None
        self.metadata = None
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
        >>> from plantdb.commons.fsdb import dummy_db
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
        >>> from plantdb.commons.fsdb import dummy_db
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
        if self.metadata == None:
            self.metadata = {}
        _set_metadata(self.metadata, data, value)
        # Ensure modification timestamp
        self.metadata['last_modified'] = date_now('%Y-%m-%d_%H:%M:%S')
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
        >>> from plantdb.commons.fsdb import dummy_db
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
        >>> from plantdb.commons.fsdb import dummy_db
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
        >>> from plantdb.commons.fsdb import dummy_db
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
        >>> from plantdb.commons.fsdb import dummy_db
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
        >>> from plantdb.commons.fsdb import dummy_db
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
        >>> from plantdb.commons.fsdb import dummy_db
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
    >>> from plantdb.commons.fsdb import dummy_db
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
