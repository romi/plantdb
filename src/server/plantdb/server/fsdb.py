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

import atexit
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
from shutil import rmtree

import bcrypt
from tqdm import tqdm

from plantdb.server import db
from plantdb.server.db import DBBusyError
from plantdb.commons.log import get_logger

logger = get_logger(__name__)

#: This file must exist in the root of a folder for it to be considered a valid DB
MARKER_FILE_NAME = "romidb"
#: This file prevents opening the DB if it is present in the root folder of a DB
LOCK_FILE_NAME = "lock"


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
    plantdb.server.fsdb.FSDB
        The dummy database.

    Notes
    -----
    - Returns a 'connected' database, no need to call the `connect()` method.
    - Uses the 'anonymous' user to login.

    Examples
    --------
    >>> from plantdb.server.fsdb import dummy_db
    >>> db = dummy_db(with_file=True)
    >>> db.connect()
    INFO     [plantdb.server.fsdb] Already connected as 'anonymous' to the database '/tmp/romidb_********'!
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
    from plantdb.server import io

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


class NotAnFSDBError(Exception):
    def __init__(self, message):
        self.message = message


class ScanNotFoundError(Exception):
    """Could not find the scan directory."""

    def __init__(self, scan_id: str):
        super().__init__(f"Unknown scan id '{scan_id}'!")


class FilesetNotFoundError(Exception):
    """Could not find the fileset directory."""

    def __init__(self, fs_id: str):
        super().__init__(f"Unknown fileset id '{fs_id}'!")


class FilesetNoIDError(Exception):
    """No 'id' entry could be found for this fileset."""


class FileNoIDError(Exception):
    """No 'id' entry could be found for this file."""


class FileNoFileNameError(Exception):
    """No 'file' entry could be found for this file."""


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
    lock_path : pathlib.Path
        The absolute path to the lock file.
    scans : dict[str, plantdb.server.fsdb.Scan]
        The dictionary of ``Scan`` instances attached to the database, indexed by their identifier.
    is_connected : bool
        ``True`` if the database is connected (locked directory), else ``False``.

    Notes
    -----
    Requires the marker file ``MARKER_FILE_NAME`` at the given ``basedir``.
    Lock file ``LOCK_FILE_NAME`` is found only when connecting an FSBD instance to the given ``basedir``.

    See Also
    --------
    plantdb.db.DB
    plantdb.server.fsdb.MARKER_FILE_NAME
    plantdb.server.fsdb.LOCK_FILE_NAME

    Examples
    --------
    >>> # EXAMPLE 1: Use a temporary dummy local database:
    >>> from plantdb.server.fsdb import dummy_db
    >>> db = dummy_db()
    >>> print(type(db))
    <class 'plantdb.server.fsdb.FSDB'>
    >>> print(db.path())
    /tmp/romidb_********
    >>> # Create a new `Scan`:
    >>> new_scan = db.create_scan("007")
    >>> print(type(new_scan))
    <class 'plantdb.server.fsdb.Scan'>
    >>> db.disconnect()  # clean up (delete) the temporary dummy database

    >>> # EXAMPLE 2: Use a local database:
    >>> import os
    >>> from plantdb.server.fsdb import FSDB
    >>> db = FSDB(os.environ.get('ROMI_DB', "/data/ROMI/DB/"))
    >>> db.connect()
    >>> [scan.id for scan in db.get_scans()]  # list scan ids found in database
    >>> scan = db.get_scans()[1]
    >>> [fs.id for fs in scan.get_filesets()]  # list fileset ids found in scan
    >>> db.disconnect()  # clean up (delete) the temporary dummy database
    """

    def __init__(self, basedir, required_filesets=['metadata'], required_files_json=True, dummy=False):
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
        required_files_json : bool, optional
            Require the `files.json` file to exist in the `Scan` directory to consider it valid.
            Defaults to ``True``.
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
        plantdb.server.fsdb.MARKER_FILE_NAME
        """
        super().__init__()

        basedir = Path(basedir)
        # Check the given path to root directory of the database is a directory:
        if not basedir.is_dir():
            raise NotADirectoryError(f"Directory {basedir} does not exists!")
        # Check the given path to root directory of the database is a "romi DB", i.e. have the `MARKER_FILE_NAME`:
        if not _is_fsdb(basedir):
            raise NotAnFSDBError(f"Not an FSDB! Check that there is a file named {MARKER_FILE_NAME} in {basedir}")

        # Create or load the users database
        users_file = basedir / 'users.json'
        if not users_file.is_file():
            # Create the users database as a JSON file
            users_file.touch()
            self.users = {}
            with open(users_file, "w") as f:
                json.dump(self.users, f, indent=2)
            logger.info(f"Initialized the new database at '{basedir}'!")
        else:
            # Load the users database
            with open(users_file, "r") as f:
                self.users = json.load(f)
            logger.debug(f"Loaded {len(self.users)} users from '{users_file}'.")
        self.user = None
        self.failed_login_attempts = {}
        self.max_login_attempts = 5
        self.lockout_duration = timedelta(minutes=15)

        # Initialize attributes:
        self.basedir = Path(basedir).resolve()
        self.lock_path = self.basedir / LOCK_FILE_NAME
        self.scans = {}
        self.is_connected = False
        self.dummy = dummy
        self.required_filesets = required_filesets if not dummy else None
        self.required_files_json = required_files_json if not dummy else False

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
        >>> from plantdb.server.fsdb import FSDB
        >>> from plantdb.server.fsdb import dummy_db
        >>> db = dummy_db()
        >>> db.create_user('batman', "Bruce Wayne", "joker")
        >>> db.connect('batman', 'joker')
        >>> print(db.user)
        batman
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        from datetime import datetime
        username = username.lower()  # Convert the username to lowercase to maintain uniformity.
        now = datetime.now()  # Get the current timestamp for tracking user creation time.
        timestamp = now.strftime("%y%m%d_%H%M%S")  # Format the timestamp as 'YYMMDD_HHMMSS'.

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
        with open(self.basedir / 'users.json', "w") as f:
            json.dump(self.users, f, indent=2)
        logger.info(f"Created user '{username}' with fullname '{fullname}'.")

        return f"Welcome {self.users[username]['fullname']}!'"

    def connect(self, login=None, password="", unsafe=False):
        """Connect to the local database.

        Handle DB "locking" system by adding a `LOCK_FILE_NAME` file in the DB.

        Parameters
        ----------
        login : str, optional
            The user login, if not defined, use the ``'anonymous'`` user.
            If defined, it should match a known user.
        unsafe : bool, optional
            If ``True`` do not use the `LOCK_FILE_NAME` file.

        Raises
        ------
        plantdb.db.DBBusyError
            If the database is already used by another process.
            This is achieved by searching for a `LOCK_FILE_NAME` lock file in the `basedir`.

        See Also
        --------
        plantdb.server.fsdb.LOCK_FILE_NAME

        Examples
        --------
        >>> from plantdb.server.fsdb import dummy_db
        >>> db = dummy_db()
        >>> print(db.is_connected)
        True
        >>> db.create_user("batman", "Bruce Wayne")
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
                self.validate_user(login, password)
            except AssertionError:
                # User doesn't exist - log error and suggest creating new user
                logger.error(f"Unknown user '{login}', cannot connect to the database!")
                logger.info(f"Create a new user '{login}' with the `create_user` method.")
                return
            else:
                self.user = login
        else:
            # Create and use anonymous user if no login provided
            self.create_user("anonymous", "Guy Fawkes", "AlanMoore")
            self.user = "anonymous"
            # Warn about anonymous user usage except for dummy databases
            if not self.dummy:
                logger.warning("Using anonymous user is discouraged!")
                logger.info("Use `connect(login='username')` to login as a user.")

        # Handle database connection
        if not self.is_connected:
            if unsafe:
                # Skip lock file in unsafe mode
                self.scans = _load_scans(self)
                self.is_connected = True
            else:
                try:
                    # Create lock file to prevent concurrent access
                    with self.lock_path.open(mode="x") as _:
                        self.scans = _load_scans(self)
                        self.is_connected = True
                    # Register disconnect function to run at program exit
                    atexit.register(self.disconnect)
                except FileExistsError:
                    # Database is already locked by another process
                    raise DBBusyError(f"File {LOCK_FILE_NAME} exists in DB root: DB is busy, cannot connect.")
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
            logger.warning(f"Login attempt for non-existent user: {username}")
            return False

        # Verify password
        stored_hash = self.users[username]['password']
        if bcrypt.checkpw(password.encode('utf-8'), stored_hash.encode('utf-8')):
            # Reset failed attempts on successful login
            self.failed_login_attempts[username] = 0
            self.users[username]['last_login'] = datetime.now().strftime("%y%m%d_%H%M%S")
            return True

        # Handle failed login attempt
        self._record_failed_attempt(username)
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
        if username not in self.failed_login_attempts:
            return False

        attempts = self.failed_login_attempts.get(username, 0)
        if attempts >= self.max_login_attempts:
            last_attempt_str = self.users[username].get('last_failed_attempt')
            last_attempt = datetime.strptime(last_attempt_str, "%y%m%d_%H%M%S")
            if last_attempt and datetime.now() - last_attempt < self.lockout_duration:
                return True
        return False

    def _record_failed_attempt(self, username: str) -> None:
        """Record failed login attempt.

        Parameters
        ----------
        username : str
            The username for which the failed login attempt is being recorded.
        """
        current = self.failed_login_attempts.get(username, 0)
        self.failed_login_attempts[username] = current + 1
        self.users[username]['last_failed_attempt'] = datetime.now()
        logger.warning(f"Failed login attempt (n={self.failed_login_attempts[username]}) for user: {username}")

    def disconnect(self):
        """Disconnect from the local database.

        Handle DB "locking" system by removing the `LOCK_FILE_NAME` file from the DB.

        Raises
        ------
        IOError
            If the `LOCK_FILE_NAME` cannot be removed using the ``lock_path`` attribute.

        Examples
        --------
        >>> from plantdb.server.fsdb import dummy_db
        >>> db = dummy_db()
        >>> print(db.is_connected)
        True
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        >>> print(db.is_connected)
        False
        """
        if self.dummy:
            logger.info(f"Cleaning up the temporary dummy database at '{self.basedir}'...")
            shutil.rmtree(self.basedir)
            self.scans = {}
            self.is_connected = False
            return

        if self.is_connected:
            for s_id, scan in self.scans.items():
                scan._erase()
            if _is_safe_to_delete(self.lock_path):
                self.lock_path.unlink(missing_ok=True)
                atexit.unregister(self.disconnect)
            else:
                raise IOError("Could not remove lock, maybe you messed with the `lock_path` attribute?")
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
        >>> from plantdb.server.fsdb import dummy_db
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
        list of plantdb.server.fsdb.Scan
            List of `Scan`s, filtered by the `query` if any.

        See Also
        --------
        plantdb.server.fsdb._filter_query

        Examples
        --------
        >>> from plantdb.server.fsdb import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> db.get_scans()
        [<plantdb.server.fsdb.Scan at *x************>]
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        if owner_only:
            if query is None:
                query = {'owner': self.user}
            else:
                query.update({'owner': self.user})
        return _filter_query(list(self.scans.values()), query, fuzzy)

    def get_scan(self, scan_id, create=False):
        """Get or create a `Scan` instance in the local database.

        Parameters
        ----------
        scan_id : str
            The name of the scan dataset to get/create.
            It should exist if `create` is `False`.
        create : bool, optional
            If ``False`` (default), the given `id` should exist in the local database.
            Else the given `id` should NOT exist as they are unique.

        Raises
        ------
        plantdb.server.fsdb.ScanNotFoundError
            If the `scan_id` do not exist in the local database and `create` is ``False``.

        Examples
        --------
        >>> from plantdb.server.fsdb import dummy_db
        >>> db = dummy_db()
        >>> db.list_scans()
        []
        >>> new_scan = db.get_scan('007', create=True)
        >>> print(new_scan)
        <plantdb.server.fsdb.Scan object at **************>
        >>> db.list_scans()
        ['007']
        >>> unknown_scan = db.get_scan('unknown')
        plantdb.server.fsdb.ScanNotFoundError: Unknown scan id 'unknown'!
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        if self.scan_exists(scan_id):
            return self.scans[scan_id]
        else:
            if create:
                return self.create_scan(scan_id)
            else:
                raise ScanNotFoundError(scan_id)

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
        plantdb.server.fsdb.Scan
            The ``Scan`` instance created in the local database.

        Raises
        ------
        OSError
            If the `scan_id` is not valid or already exists in the local database.

        See Also
        --------
        plantdb.server.fsdb._is_valid_id
        plantdb.server.fsdb._make_scan

        Examples
        --------
        >>> from plantdb.server.fsdb import dummy_db
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
        # Verify if the given `scan_id` is valid
        if not _is_valid_id(scan_id):
            raise IOError(f"Invalid scan identifier '{scan_id}'!")
        # Verify if the given `scan_id` already exists in the local database
        if self.scan_exists(scan_id):
            raise IOError(f"Given scan identifier '{scan_id}' already exists!")

        scan = Scan(self, scan_id)
        _make_scan(scan)
        self.scans[scan_id] = scan

        if metadata is None:
            metadata = {}
        metadata.update({'owner': self.user})  # add/update 'owner' metadata to the new scan
        scan.set_metadata(metadata)  # add metadata dictionary to the new scan
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
        plantdb.server.fsdb._delete_scan

        Examples
        --------
        >>> from plantdb.server.fsdb import FSDB
        >>> from plantdb.server.fsdb import dummy_db
        >>> db = dummy_db()
        >>> new_scan = db.create_scan('007')
        >>> print(new_scan)
        <plantdb.server.fsdb.Scan object at 0x7f0730b1e390>
        >>> db.delete_scan('007')
        >>> scan = db.get_scan('007')
        >>> print(scan)
        None
        >>> db.delete_scan('008')
        OSError: Invalid id
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        # Verify if the given `scan_id` exists in the local database
        if not self.scan_exists(scan_id):
            logging.warning(f"Given scan identifier '{scan_id}' does NOT exists!")
            return

        _delete_scan(self.get_scan(scan_id))  # delete the Scan instance
        self.scans.pop(scan_id)  # remove the scan from the local database
        return

    def path(self) -> pathlib.Path:
        """Get the path to the local database root directory.

        Examples
        --------
        >>> from plantdb.server.fsdb import dummy_db
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
        plantdb.server.fsdb._filter_query

        Examples
        --------
        >>> from plantdb.server.fsdb import dummy_db
        >>> db = dummy_db(with_scan=True)
        >>> db.list_scans()
        ['myscan_001']
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        if owner_only:
            if query is None:
                query = {'owner': self.user}
            else:
                query.update({'owner': self.user})

        if query is None:
            return list(self.scans.keys())
        else:
            return [scan.id for scan in self.get_scans(query, fuzzy, owner_only)]


class Scan(db.Scan):
    """Implement ``Scan`` for the local *File System DataBase* from abstract class ``db.Scan``.

    Implementation of a scan as a simple file structure with:
      * directory ``${Scan.db.basedir}/${Scan.db.id}`` as scan root directory;
      * (OPTIONAL) directory ``${Scan.db.basedir}/${Scan.db.id}/metadata`` containing JSON metadata file
      * (OPTIONAL) JSON file ``metadata.json`` with Scan metadata

    Attributes
    ----------
    db : plantdb.server.fsdb.FSDB
        A local database instance hosting this ``Scan`` instance.
    id : str
        The identifier of this ``Scan`` instance in the local database `db`.
    metadata : dict
        A metadata dictionary.
    filesets : dict[str, plantdb.server.fsdb.Fileset]
        A dictionary of `Fileset` instances, indexed by their identifier.

    Notes
    -----
    Optional directory ``metadata`` & JSON file ``metadata.json`` are found when using method ``set_metadata()``.

    See Also
    --------
    plantdb.db.Scan

    Examples
    --------
    >>> import os
    >>> from plantdb.server.fsdb import Scan
    >>> from plantdb.server.fsdb import dummy_db
    >>> db = dummy_db()
    >>> # Example #1: Initialize a `Scan` object using an `FSBD` object:
    >>> scan = Scan(db, '007')
    >>> print(type(scan))
    <class 'plantdb.server.fsdb.Scan'>
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
    <class 'plantdb.server.fsdb.Scan'>
    >>> print(db.get_scan('007'))  # This time the `Scan` object is found in the `FSBD`
    <plantdb.server.fsdb.Scan object at 0x7f34fc860fd0>
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
    >>> from plantdb.server.fsdb import FSDB
    >>> db = FSDB(environ.get('ROMI_DB', "/data/ROMI/DB/"))
    >>> db.connect(unsafe=True)
    >>> scan = db.get_scan('sango_90_300_36')
    >>> scan.get_metadata()
    """

    def __init__(self, db, scan_id):
        """Scan dataset constructor.

        Parameters
        ----------
        db : plantdb.server.fsdb.FSDB
            The database to put/find the scan dataset.
        scan_id : str
            The scan dataset name, should be unique in the `db`.
        """
        super().__init__(db, scan_id)
        # Defines attributes:
        self.metadata = None
        self.filesets = {}
        self.measures = None
        return

    def _erase(self):
        """Erase the filesets and metadata associated to this scan."""
        for fs_id, fs in self.filesets.items():
            fs._erase()
        self.metadata = None
        self.filesets = {}
        self.measures = None
        return

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
        >>> from plantdb.server.fsdb import dummy_db
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
        list of plantdb.server.fsdb.Fileset
            List of `Fileset`s, filtered by the `query` if any.

        See Also
        --------
        plantdb.server.fsdb._filter_query

        Examples
        --------
        >>> from plantdb.server.fsdb import dummy_db
        >>> db = dummy_db(with_fileset=True)
        >>> scan = db.get_scan('myscan_001')
        >>> scan.get_filesets()
        [<plantdb.server.fsdb.Fileset at *x************>]
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        return _filter_query(list(self.filesets.values()), query, fuzzy)

    def get_fileset(self, fs_id, create=False):
        """Get or create a `Fileset` instance, of given `id`, in the current scan dataset.

        Parameters
        ----------
        fs_id : str
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
        >>> from plantdb.server.fsdb import dummy_db
        >>> db = dummy_db(with_fileset=True)
        >>> scan = db.get_scan('myscan_001')
        >>> scan.list_filesets()
        ['fileset_001']
        >>> new_fileset = scan.get_fileset('007', create=True)
        >>> print(new_fileset)
        <plantdb.server.fsdb.Fileset object at **************>
        >>> scan.list_filesets()
        ['fileset_001', '007']
        >>> unknown_fs = scan.get_fileset('unknown')
        plantdb.server.fsdb.FilesetNotFoundError: Unknown fileset id 'unknown'!
        >>> print(unknown_fs)
        None
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        if self.fileset_exists(fs_id):
            return self.filesets[fs_id]
        else:
            if create:
                return self.create_fileset(fs_id)
            else:
                raise FilesetNotFoundError(fs_id)

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
        >>> from plantdb.server.fsdb import dummy_db
        >>> from plantdb.server.fsdb import _scan_metadata_path
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
        if self.metadata == None:
            self.metadata = {}
        _set_metadata(self.metadata, data, value)
        _store_scan_metadata(self)
        return

    def create_fileset(self, fs_id):
        """Create a new `Fileset` instance in the local database attached to the current `Scan` instance.

        Parameters
        ----------
        fs_id : str
            The name of the fileset to create. It should not exist in the current `Scan` instance.

        Returns
        -------
        plantdb.server.fsdb.Fileset
            The `Fileset` instance created in the current `Scan` instance.

        Raises
        ------
        IOError
            If the `id` already exists in the current `Scan` instance.
            If the `id` is not valid.

        See Also
        --------
        plantdb.server.fsdb._is_valid_id
        plantdb.server.fsdb._make_fileset

        Examples
        --------
        >>> from plantdb.server.fsdb import dummy_db
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
        # Verify if the given `fs_id` is valid
        if not _is_valid_id(fs_id):
            raise IOError(f"Invalid fileset identifier '{fs_id}'!")
        # Verify if the given `fs_id` already exists in the local database
        if self.fileset_exists(fs_id):
            raise IOError(f"Given fileset identifier '{fs_id}' already exists!")

        fileset = Fileset(self, fs_id)
        _make_fileset(fileset)
        self.filesets.update({fs_id: fileset})
        self.store()
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
        >>> from plantdb.server.fsdb import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> scan = db.get_scan('myscan_001')
        >>> scan.list_filesets()
        ['fileset_001']
        >>> scan.delete_fileset('fileset_001')
        >>> scan.list_filesets()
        []
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        # Verify if the given `fs_id` exists in the local database
        if not self.fileset_exists(fs_id):
            logging.warning(f"Given fileset identifier '{fs_id}' does NOT exists!")
            return

        _delete_fileset(self.get_fileset(fs_id, create=False))  # delete the fileset
        self.filesets.pop(fs_id)  # remove the Fileset instance from the scan
        self.store()  # save the changes to the scan main JSON FILE (``files.json``)
        return

    def path(self) -> pathlib.Path:
        """Get the path to the local scan dataset.

        Examples
        --------
        >>> from plantdb.server.fsdb import dummy_db
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
        plantdb.server.fsdb._filter_query

        Examples
        --------
        >>> from plantdb.server.fsdb import dummy_db
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
    db : plantdb.server.fsdb.FSDB
        A local database instance hosting the ``Scan`` instance.
    scan : plantdb.server.fsdb.Scan
        A scan instance hosting this ``Fileset`` instance.
    id : str
        The identifier of this ``Fileset`` instance in the `scan`.
    metadata : dict
        A metadata dictionary.
    files : dict[str, plantdb.server.fsdb.File]
        A dictionary of `File` instances attached to the fileset, indexed by their identifier.

    See Also
    --------
    plantdb.db.Fileset
    """

    def __init__(self, scan, fs_id):
        """Constructor.

        Parameters
        ----------
        scan : plantdb.server.fsdb.Scan
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
        >>> from plantdb.server.fsdb import dummy_db
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
        list of plantdb.server.fsdb.File
            List of `File`s, filtered by the query if any.

        See Also
        --------
        plantdb.server.fsdb._filter_query

        Examples
        --------
        >>> from plantdb.server.fsdb import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> scan = db.get_scan('myscan_001')
        >>> fs = scan.get_fileset('fileset_001')
        >>> fs.get_files()
        [<plantdb.server.fsdb.File at *x************>,
         <plantdb.server.fsdb.File at *x************>,
         <plantdb.server.fsdb.File at *x************>]
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        return _filter_query(list(self.files.values()), query, fuzzy)

    def get_file(self, f_id, create=False):
        """Get or create a `File` instance, of given `id`, in the current fileset.

        Parameters
        ----------
        f_id : str
            Name of the file to get/create.
        create : bool
            If ``False`` (default), the given `id` should exist in the local database.
            Else the given `id` should NOT exist as they are unique.

        Returns
        -------
        plantdb.server.fsdb.File
            The retrieved or created file.

        Examples
        --------
        >>> from plantdb.server.fsdb import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> scan = db.get_scan("myscan_001")
        >>> fs = scan.get_fileset("fileset_001")
        >>> f = fs.get_file("test_image")
        >>> # To read the file you need to load the right reader from plantdb.server.io
        >>> from plantdb.server.io import read_image
        >>> img = read_image(f)
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        if self.file_exists(f_id):
            return self.files[f_id]
        else:
            if create:
                return self.create_file(f_id)
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
        >>> from plantdb.server.fsdb import dummy_db
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
        >>> from plantdb.server.fsdb import dummy_db
        >>> from plantdb.server.fsdb import _fileset_metadata_json_path
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
        plantdb.server.fsdb.File
            The `File` instance created in the current `Fileset` instance.

        Examples
        --------
        >>> from plantdb.server.fsdb import dummy_db
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
        >>> from plantdb.server import io
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
        fileset_id : str
            Name of the file to delete.

        Examples
        --------
        >>> from plantdb.server.fsdb import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> scan = db.get_scan('myscan_001')
        >>> fs = scan.get_fileset('fileset_001')
        >>> fs.list_files()
        ['dummy_image', 'test_image', 'test_json']
        >>> fs.delete_file('dummy_image')
        INFO     [plantdb.server.fsdb] Deleted JSON metadata file for file 'dummy_image' from 'myscan_001/fileset_001'.
        INFO     [plantdb.server.fsdb] Deleted file 'dummy_image' from 'myscan_001/fileset_001'.
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
        >>> from plantdb.server.fsdb import dummy_db
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
        plantdb.server.fsdb._filter_query

        Examples
        --------
        >>> from plantdb.server.fsdb import dummy_db
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
    db : plantdb.server.fsdb.FSDB
        Database where to find the fileset.
    fileset : plantdb.server.fsdb.Fileset
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
        >>> from plantdb.server.fsdb import dummy_db
        >>> from plantdb.server.fsdb import _file_metadata_path
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
        >>> from plantdb.server.fsdb import dummy_db
        >>> from plantdb.server.fsdb import _file_metadata_path
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
        >>> from plantdb.server.fsdb import dummy_db
        >>> from plantdb.server.fsdb import _file_metadata_path
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
        >>> from plantdb.server.fsdb import dummy_db
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
        >>> from plantdb.server.fsdb import dummy_db
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
        >>> from plantdb.server.fsdb import dummy_db
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
        >>> from plantdb.server.fsdb import dummy_db
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
        >>> from plantdb.server.fsdb import dummy_db
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


##################################################################
#
# the ugly stuff...
#

# load the database

def _load_scan(db, scan_id):
    """Load ``Scan`` from given database.

    List subdirectories of ``db.basedir`` as ``Scan`` instances.
    May be restrited to the presense of subdirectories in .

    Parameters
    ----------
    db : plantdb.server.fsdb.FSDB
        The database instance to use to list the ``Scan``.
    scan_id : str
        The name of the scan to load.

    Returns
    -------
    list of plantdb.server.fsdb.Scan
         The list of ``fsdb.Scan`` found in the database.

    See Also
    --------
    plantdb.server.fsdb._scan_path
    plantdb.server.fsdb._scan_files_json
    plantdb.server.fsdb._load_scan_filesets
    plantdb.server.fsdb._load_scan_metadata

    Examples
    --------
    >>> from plantdb.server.fsdb import FSDB
    >>> from plantdb.server.fsdb import dummy_db, _load_scans
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
    [<plantdb.server.fsdb.Scan object at 0x7fa01220bd50>]
    """
    required_fs = db.required_filesets
    req_files_json = db.required_files_json

    scan = Scan(db, scan_id)
    scan_path = _scan_path(scan)

    # If specific filesets are required, test if they exist as subdirectories:
    if required_fs is not None:
        req_subdir = all([scan_path.joinpath(subdir).is_dir() for subdir in required_fs])
    else:
        req_subdir = True

    # If the `files.json` associated to the scan is required, test if it exists:
    if req_files_json:
        req_file = _scan_json_file(scan).is_file()
    else:
        req_file = True

    if db.dummy and scan_path.is_dir():
        return scan
    elif scan_path.is_dir() and req_subdir and req_file:
        # Parse the fileset, metadata and measure if:
        #  - path to scan directory exists
        #  - required subdirectories exists, if any
        #  - required files exists, if any
        scan.filesets = _load_scan_filesets(scan)
        scan.metadata = _load_scan_metadata(scan)
        scan.measures = _load_scan_measures(scan)
    else:
        scan = None
    return scan


def _load_scans(db):
    """Load list of ``Scan`` from given database.

    List subdirectories of ``db.basedir`` as ``Scan`` instances.
    May be restrited to the presense of subdirectories in .

    Parameters
    ----------
    db : plantdb.server.fsdb.FSDB
        The database instance to use to list the ``Scan``.

    Returns
    -------
    list of plantdb.server.fsdb.Scan
         The list of ``fsdb.Scan`` found in the database.

    See Also
    --------
    plantdb.server.fsdb._scan_path
    plantdb.server.fsdb._scan_files_json
    plantdb.server.fsdb._load_scan_filesets
    plantdb.server.fsdb._load_scan_metadata

    Examples
    --------
    >>> from plantdb.server.fsdb import FSDB
    >>> from plantdb.server.fsdb import dummy_db, _load_scans
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
    [<plantdb.server.fsdb.Scan object at 0x7fa01220bd50>]
    """
    scans = {}
    dir_names = os.listdir(db.path())
    for scan_name in tqdm(dir_names, unit="scan"):
        scan = _load_scan(db, scan_name)
        if scan is not None:
            scans[scan_name] = scan
    return scans


def _load_dummy_fileset(scan):
    """

    Parameters
    ----------
    scan : plantdb.server.fsdb.Scan
        The instance to use to get the list of ``Fileset``.

    Returns
    -------

    """
    filesets = {}
    for fs_id in scan.path().iterdir():
        fs = Fileset(scan, fs_id)
        fs.files = list(fs.path().iterdir())
        filesets[fs_id] = fs

    return filesets


def _load_scan_filesets(scan):
    """Load the list of ``Fileset`` from given `scan` dataset and return them as a dict.

    Load the list of filesets using "filesets" top-level entry from ``files.json``.

    Parameters
    ----------
    scan : plantdb.server.fsdb.Scan
        The instance to use to get the list of ``Fileset``.

    Returns
    -------
    dict
        A dictionary where keys are `fsid` (id of the filesets) and values are
        the `Fileset` instances.

    See Also
    --------
    plantdb.server.fsdb._scan_files_json
    plantdb.server.fsdb._load_scan_filesets

    Notes
    -----
    May delete a fileset if unable to load it!

    Examples
    --------
    >>> from plantdb.server.fsdb import FSDB
    >>> from plantdb.server.fsdb import dummy_db, _load_scan_filesets
    >>> db = dummy_db(with_fileset=True)
    >>> db.connect()
    >>> scan = db.get_scan("myscan_001")
    >>> filesets = _load_scan_filesets(scan)
    >>> print(filesets)
    {'fsid_001': <plantdb.server.fsdb.Fileset object at 0x7fa0122232d0>}
    """
    filesets = {}
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
                fsid = fileset_info.get("id")
                if fsid is None:
                    raise FilesetNoIDError(f"Missing 'id' for the {n}-th 'filesets' entry.")
                filesets[fsid] = fileset
            except FilesetNoIDError:
                logger.error(f"Could not get an 'id' entry for the {n}-th 'filesets' entry from '{scan.id}'.")
                logger.debug(f"Current `fileset_info`: {fileset_info}")
            except FilesetNotFoundError:
                fsid = fileset_info.get("id", None)
                logger.error(f"Fileset directory '{fsid}' not found for scan {scan.id}, skip it.")
    else:
        raise IOError(f"Could not find a list of filesets in '{files_json}'.")

    return filesets


def _load_fileset(scan, fileset_info):
    """Load a fileset and set its attributes.

    Parameters
    ----------
    scan : plantdb.server.fsdb.Scan
        The scan object to use to get the list of ``fsdb.Fileset``
    fileset_info: dict
        Dictionary with the fileset id and listing its files, ``{'files': [], 'id': str}``.

    Returns
    -------
    plantdb.server.fsdb.Fileset
        A fileset with its ``files`` & ``metadata`` attributes restored.

    Examples
    --------
    >>> import json
    >>> from plantdb.server.fsdb import dummy_db, _load_fileset, _scan_json_file
    >>> db = dummy_db(with_file=True)
    >>> db.connect()
    >>> scan = db.get_scan("myscan_001")
    >>> db.disconnect()  # clean up (delete) the temporary dummy database
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


def _parse_fileset(scan, fileset_info):
    """Get a `Fileset` instance for given `db` & `scan` by parsing provided `fileset_info`.

    Parameters
    ----------
    scan : plantdb.server.fsdb.Scan
        The scan instance to associate the returned ``Fileset`` to.
    fileset_info : dict
        The fileset dictionary with the fileset 'id' entry

    Returns
    -------
    plantdb.server.fsdb.Fileset
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
    fileset : plantdb.server.fsdb.Fileset
        The instance to use to get the list of ``File``.
    fileset_info : dict
        Dictionary with the fileset id and listing its files, ``{'files': [], 'id': str}``.

    Returns
    -------
    list of plantdb.server.fsdb.File
         The list of ``File`` found in the `fileset`.

    See Also
    --------
    plantdb.server.fsdb._load_file

    Notes
    -----
    May delete a file if unable to load it!

    Examples
    --------
    >>> import json
    >>> from plantdb.server.fsdb import FSDB
    >>> from plantdb.server.fsdb import dummy_db, _scan_json_file, _parse_fileset, _load_fileset_files
    >>> db = dummy_db(with_fileset=True, with_file=True)
    >>> db.connect()
    >>> scan = db.get_scan("myscan_001")
    >>> db.disconnect()  # clean up (delete) the temporary dummy database
    >>> json_path = _scan_json_file(scan)
    >>> with json_path.open(mode="r") as f: structure = json.load(f)
    >>> filesets_info = structure["filesets"]
    >>> fileset = _parse_fileset(scan.db, scan, filesets_info[0])
    >>> files = _load_fileset_files(fileset, filesets_info[0])
    >>> print([f.id for f in files])
    ['dummy_image', 'test_image', 'test_json']
    """
    scan_id = fileset.scan.id
    files = {}
    files_info = fileset_info.get("files", None)
    if isinstance(files_info, list):
        for n, file_info in enumerate(files_info):
            try:
                file = _load_file(fileset, file_info)
                fid = file_info.get("id")
                if fid is None:
                    raise FileNotFoundError(f"Missing 'id' for the {n}-th 'files' entry.")
            except FileNoIDError:
                logger.error(f"Could not get an 'id' entry for the {n}-th 'files' entry from '{scan_id}'.")
                logger.debug(f"Current `file_info`: {file_info}")
            except FileNotFoundError:
                fs_id = fileset.id
                fname = file_info.get("file")
                logger.error(f"Could not find file '{fname}' for '{scan_id}/{fs_id}'.")
                logger.debug(f"Current `file_info`: {file_info}")
            else:
                files[fid] = file
    else:
        raise IOError(f"Expected a list of files in `files.json` from dataset '{fileset.scan.id}'!")
    return files


def _load_fileset_metadata(fileset):
    """Load the metadata for a fileset.

    Parameters
    ----------
    fileset : plantdb.server.fsdb.Fileset
        The fileset to load the metadata for.

    Returns
    -------
    dict
        The metadata dictionary.
    """
    return _load_metadata(_fileset_metadata_json_path(fileset))


def _load_file(fileset, file_info):
    """Get a `File` instance for given `fileset` using provided `file_info`.

    Parameters
    ----------
    fileset : plantdb.server.fsdb.Fileset
        The instance to associate the returned ``File`` to.
    file_info : dict
        Dictionary with the file 'id' and 'file' entries, ``{'file': str, 'id': str}``.

    Returns
    -------
    plantdb.server.fsdb.File
        The `File` instance with metadata.

    See Also
    --------
    plantdb.server.fsdb._parse_file
    plantdb.server.fsdb._load_file_metadata
    """
    file = _parse_file(fileset, file_info)
    file.metadata = _load_file_metadata(file)
    return file


def _parse_file(fileset, file_info):
    """Get a `File` instance for given `fileset` by parsing provided `file_info`.

    Parameters
    ----------
    fileset : plantdb.server.fsdb.Fileset
        The fileset instance to associate the returned ``File`` to.
    file_info : dict
        The file dictionary with the file 'id' and 'file' entries, ``{'file': str, 'id': str}``.

    Returns
    -------
    plantdb.server.fsdb.File
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
        raise FileNoIDError(f"File: No ID for file '{file_info}'")

    filename = file_info.get("file", None)
    if filename is None:
        logger.debug(f"Input `file_info`: {file_info}")
        raise FileNoFileNameError(f"File: No filename for file '{file_info}'")

    file = File(fileset, fid)
    file.filename = filename  # set the filename attribute
    # Get the expected file path and check it exists:
    path = _file_path(file)
    if not path.is_file():
        logger.debug(f"Missing file: {path}")
        raise FileNotFoundError(f"File: Not found at '{path}'")
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
    scan : plantdb.server.fsdb.Scan
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
    scan : plantdb.server.fsdb.Scan
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
    file : plantdb.server.fsdb.File
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
    scan : plantdb.server.fsdb.Scan
        The dataset to save the metadata for.
    """
    _store_metadata(_scan_metadata_path(scan), scan.metadata)
    return


def _store_fileset_metadata(fileset):
    """Save the metadata for a dataset.

    Parameters
    ----------
    scan : plantdb.server.fsdb.Fileset
        The fileset to save the metadata for.
    """
    _store_metadata(_fileset_metadata_json_path(fileset), fileset.metadata)
    return


def _store_file_metadata(file):
    """Save the metadata for a dataset.

    Parameters
    ----------
    scan : plantdb.server.fsdb.File
        The file to save the metadata for.
    """
    _store_metadata(_file_metadata_path(file), file.metadata)
    return


def _get_metadata(metadata=None, key=None, default={}):
    """Get a copy of `metadata[key]`.

    Parameters
    ----------
    metadata : dict, optional
        The metadata dictionary to get the key from.
    key : str, optional
        The key to get from the metadata dictionary.
        By default, return a copy of the whole metadata dictionary.
    default : Any, optional
        The default value to return if the key do not exist in the metadata.
        Default is an empty dictionary ``{}``.

    Returns
    -------
    Any
        The value saved under `metadata['key']`, if any.
    """
    # Do a deepcopy of the value because we don't want the caller to inadvertently change the values.
    if metadata is None:
        return default
    elif key is None:
        return copy.deepcopy(metadata)
    else:
        return copy.deepcopy(metadata.get(str(key), default))


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
    file : plantdb.server.fsdb.File
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
    fileset : plantdb.server.fsdb.Fileset
        The fileset to use for directory creation.

    See Also
    --------
    plantdb.server.fsdb._fileset_path
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
    scan : plantdb.server.fsdb.Scan
        The scan to use for directory creation.

    See Also
    --------
    plantdb.server.fsdb._scan_path
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
    scan : plantdb.server.fsdb.Scan
        A scan to get the path from.

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
    scan : plantdb.server.fsdb.Scan
        A scan to get the files JSON file path from.

    Returns
    -------
    pathlib.Path
        The path to the scan's "files.json" file.
    """
    return _scan_path(scan) / "files.json"


def _scan_metadata_path(scan):
    """Get the path to scan's "metadata.json" file.

    Parameters
    ----------
    scan : plantdb.server.fsdb.Scan
        A scan to get the metadata JSON file path from.

    Returns
    -------
    pathlib.Path
        The path to the scan's "metadata.json" file.
    """
    return _scan_path(scan) / "metadata" / "metadata.json"


def _scan_measures_path(scan):
    """Get the path to scan's "measures.json" file.

    Parameters
    ----------
    scan : plantdb.server.fsdb.Scan
        A scan to get the measures JSON file path from.

    Returns
    -------
    pathlib.Path
        The path to the scan's "measures.json" file.
    """
    return _scan_path(scan) / "measures.json"


def _fileset_path(fileset):
    """Get the path to given fileset directory.

    Parameters
    ----------
    fileset : plantdb.server.fsdb.Fileset
        A fileset to get the path from.

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
    fileset : plantdb.server.fsdb.Fileset
        A fileset to get the metadata directory path from.

    Returns
    -------
    pathlib.Path
        The path to the fileset metadata directory.
    """
    return _scan_path(fileset.scan) / "metadata" / fileset.id


def _fileset_metadata_json_path(fileset):
    """Get the path to `fileset.id` metadata JSON file.

    Parameters
    ----------
    fileset : plantdb.server.fsdb.Fileset
        A fileset to get the JSON file path from.

    Returns
    -------
    pathlib.Path
        The path to the f`fileset.id` metadata JSON file.
    """
    return _scan_path(fileset.scan) / "metadata" / f"{fileset.id}.json"


def _file_path(file):
    """Get the path to given file.

    Parameters
    ----------
    file : plantdb.server.fsdb.File
        A file to get the path from.

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
    file : plantdb.server.fsdb.File
        A file to get the metadata JSON path from.

    Returns
    -------
    pathlib.Path
        The path to the "<File.id>.json" file.
    """
    return _scan_path(file.fileset.scan) / "metadata" / file.fileset.id / f"{file.id}.json"


################################################################################
# store a scan to disk
################################################################################

def _file_to_dict(file):
    """Returns a dict with the file "id" and "filename".

    Parameters
    ----------
    file : plantdb.server.fsdb.File
        A file to get "id" and "filename" from.

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
    fileset : plantdb.server.fsdb.Fileset
        A fileset to get "id" and "files" dictionary from.

    Returns
    -------
    dict
        ``{"id": fileset.get_id(), "files": {"id": file.get_id(), "file": file.filename}}``

    See Also
    --------
    plantdb.server.fsdb._file_to_dict
    """
    files = []
    for f in fileset.get_files():
        files.append(_file_to_dict(f))
    return {"id": fileset.get_id(), "files": files}


def _scan_to_dict(scan):
    """Returns a dict with the scan's filesets and files structure.

    Parameters
    ----------
    scan : plantdb.server.fsdb.Scan
        A scan instance to get underlying filesets and files structure from.

    Returns
    -------
    dict
        ``{"filesets": {"id": fileset.get_id(), "files": {"id": file.get_id(), "file": file.filename}}}``

    See Also
    --------
    plantdb.server.fsdb._fileset_to_dict
    plantdb.server.fsdb._file_to_dict
    """
    filesets = []
    for fileset in scan.get_filesets():
        filesets.append(_fileset_to_dict(fileset))
    return {"filesets": filesets}


def _store_scan(scan):
    """Dump the fileset and files structure associated to a `scan` on drive.

    Parameters
    ----------
    scan : plantdb.server.fsdb.Scan
        A scan instance to save by dumping its underlying structure in its "files.json".

    See Also
    --------
    plantdb.server.fsdb._scan_to_dict
    plantdb.server.fsdb._scan_files_json
    """
    structure = _scan_to_dict(scan)
    files_json = _scan_json_file(scan)
    with files_json.open(mode="w") as f:
        json.dump(structure, f, sort_keys=True, indent=4, separators=(',', ': '))
    logger.debug(f"The `files.json` file for scan '{scan.id}' has been updated!")
    return


def _is_valid_id(name):
    """Checks the validity of a given identifier name based on specified conditions.

    This function validates whether the provided name is a valid identifier by ensuring it 
    is a string, non-empty, not longer than 255 characters, and contains only allowable
    characters. It logs errors for invalid input.

    Parameters
    ----------
    name : str
        The identifier name to be validated.

    Returns
    -------
    bool
        ``True`` if the name is valid, otherwise ``False``.
    """
    import re
    # Check if the name is a string
    if not isinstance(name, str):
        logger.error(f"Given name is not a string: '{name}'")
        return False

    # Check if the string is empty or too long (e.g., limit to 255 characters)
    if not name or len(name) > 255:
        logger.error(f"Given name is empty or too long: '{name}'")
        return False

    # Check for invalid characters (disallow slashes, backslashes, etc.)
    # Here we use a regex to allow alphanumeric, underscores, dashes and dots only
    if not re.match(r'^[\w\-\.]+$', name):
        logger.error(f"Given name contains invalid characters: '{name}'.")
        logger.info("Only alphanumeric characters, underscores, dashes and dots are allowed.")
        return False

    return True


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
    file : plantdb.server.fsdb.File
        A file instance to delete.

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
    plantdb.server.fsdb._file_path
    plantdb.server.fsdb._is_safe_to_delete
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
    fileset : plantdb.server.fsdb.Fileset
        A fileset instance to delete.

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
    plantdb.server.fsdb._scan_path
    plantdb.server.fsdb._fileset_path
    plantdb.server.fsdb._is_safe_to_delete
    """
    fileset_path = _fileset_path(fileset)
    if not _is_safe_to_delete(fileset_path):
        logger.error(f"Fileset {fileset.id} is not in the current database.")
        logger.debug(f"Fileset path: '{fileset_path}'.")
        raise IOError("Cannot delete files or directories outside of a local DB.")

    # - Delete the `Files` (and their metadata) belonging to the `Fileset` instance:
    files_list = fileset.list_files()
    for f_id in files_list:
        fileset.delete_file(f_id)

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
    scan : plantdb.server.fsdb.Scan
        A scan instance to delete.

    Raises
    ------
    IOError
        If the scan path is outside the database.

    See Also
    --------
    plantdb.server.fsdb._scan_path
    plantdb.server.fsdb._is_safe_to_delete
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


def _filter_query(l, query=None, fuzzy=False):
    """Filter a list of `Scan`s, `Fileset`s or `File`s using a `query` on their metadata.

    Parameters
    ----------
    l : list
        A list of `Scan`s, `Fileset`s or `File`s to filter.
    query : dict, optional
        A filtering query in the form of a dictionary.
        The list of instances must have metadata matching ``key`` and ``value`` from the `query`.
    fuzzy : bool
        Whether to use fuzzy matching or not, that is the use of regular expressions.

    Returns
    -------
    list
        The list of `Scan`s, `Fileset`s or `File`s filtered by the query, if any.

    See Also
    --------
    plantdb.server.utils.partial_match

    Examples
    --------
    >>> from plantdb.server.fsdb import dummy_db
    >>> from plantdb.server.fsdb import _filter_query
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
    from plantdb.server.utils import partial_match
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
                    # assert f.get_metadata(q) == query[q]
                    assert partial_match(query[q], f.get_metadata(q), fuzzy)
                except AssertionError:
                    f_query.append(False)
                else:
                    f_query.append(True)
            # All requirements have to be fulfilled:
            if all(f_query):
                query_result.append(f)
    return query_result
