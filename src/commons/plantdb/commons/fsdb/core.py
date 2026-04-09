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
import functools
import json
import logging
import os
import pathlib
from collections.abc import Iterable
from pathlib import Path
from shutil import copyfile
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

from plantdb.commons import db
from plantdb.commons.auth.models import Group
from plantdb.commons.auth.models import Permission
from plantdb.commons.auth.models import Role
from plantdb.commons.auth.models import TokenUser
from plantdb.commons.auth.models import User
from plantdb.commons.auth.rbac import RBACManager
from plantdb.commons.auth.session import JWTSessionManager
from plantdb.commons.auth.session import SessionManager
from plantdb.commons.auth.session import SingleSessionManager
from plantdb.commons.fsdb.exceptions import FileNotFoundError
from plantdb.commons.fsdb.exceptions import FilesetExistsError
from plantdb.commons.fsdb.exceptions import FilesetNotFoundError
from plantdb.commons.fsdb.exceptions import NoAuthUserError
from plantdb.commons.fsdb.exceptions import ScanExistsError
from plantdb.commons.fsdb.exceptions import ScanNotFoundError
from plantdb.commons.fsdb.file_ops import _delete_file
from plantdb.commons.fsdb.file_ops import _delete_fileset
from plantdb.commons.fsdb.file_ops import _delete_scan
from plantdb.commons.fsdb.file_ops import _load_scan
from plantdb.commons.fsdb.file_ops import _load_scans
from plantdb.commons.fsdb.file_ops import _make_fileset
from plantdb.commons.fsdb.file_ops import _make_scan
from plantdb.commons.fsdb.file_ops import _store_scan
from plantdb.commons.fsdb.lock import LockType
from plantdb.commons.fsdb.lock import LockManager
from plantdb.commons.fsdb.lock import LockLevel
from plantdb.commons.fsdb.metadata import MetadataManager
from plantdb.commons.fsdb.metadata import _get_metadata
from plantdb.commons.fsdb.metadata import _set_metadata
from plantdb.commons.fsdb.metadata import _store_file_metadata
from plantdb.commons.fsdb.metadata import _store_fileset_metadata
from plantdb.commons.fsdb.metadata import _store_scan_metadata
from plantdb.commons.fsdb.path_helpers import _file_path
from plantdb.commons.fsdb.path_helpers import _fileset_path
from plantdb.commons.fsdb.path_helpers import _get_filename
from plantdb.commons.fsdb.path_helpers import _scan_path
from plantdb.commons.fsdb.validation import _is_valid_id
from plantdb.commons.log import get_logger
from plantdb.commons.utils import iso_date_now

#: This file must exist in the root of a folder for it to be considered a valid FSDB
MARKER_FILE_NAME = "romidb"


def require_connected_db(method):
    """Decorator that ensures the method is only called when the database is connected.

    This ensures that operations that require a valid database connection are properly guarded against calls when the connection is inactive.

    Raises
    ------
    ValueError
        If the decorated method is called while the database connection status is not active

    See Also
    --------
    plantdb.commons.fsdb.core.FSDB.connect : Method typically used to establish a database connection.
    """

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        if not self.is_connected:
            raise ValueError("Database not connected, use the 'connect()' method first!")

        return method(self, *args, **kwargs)

    return wrapper


def require_token(method):
    """Decorator that passes the token to the decorated method depending on the session manager."""

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):

        if isinstance(self.session_manager, SingleSessionManager):
            # If a Single SessionManager, get the token from the session manager
            session = list(self.session_manager.sessions.keys())[0]
            kwargs['token'] = session

        elif isinstance(self.session_manager, JWTSessionManager):
            # If a JSON Web Token Session Manager, require the token
            if not 'token' in kwargs:
                self.logger.warning("The 'token' argument is required when using JWTSessionManager")

        elif isinstance(self.session_manager, SessionManager):
            # If a regular Session Manager, require the session token
            if not 'token' in kwargs:
                self.logger.warning("The 'token' argument is required when using JWTSessionManager")

        else:
            self.logger.error("Can't serve a local PlantDB without a session manager!")

        return method(self, *args, **kwargs)

    return wrapper


def get_logged_username(fsdb: "FSDB", default_user=None, token=None, **kwargs):
    """Returns the username of the currently logged user based on the session management system.

    This function identifies the username of the logged-in user by inspecting the session manager
    associated with the given `fsdb` object. It supports multiple types of session managers, including
    ``SingleSessionManager``, ``JWTSessionManager``, and generic ``SessionManager``.
    If no valid session is found or if necessary arguments for session validation are missing, it
    falls back to the `default_user`.

    Parameters
    ----------
    fsdb : plantd.commons.fsdb.FSDB
        The filesystem database instance containing configuration, logger, and session manager.
        The session manager is responsible for handling user sessions.
    default_user : str, optional
        A fallback username to use if no valid session or token is found. Defaults to ``None``.
    token : str, optional
        The session token or JWT for validating the user's session.

    Returns
    -------
    str
        The username of the logged-in user or the fallback `default_user`.

    Notes
    -----
    - If no valid session manager is attached to the `fsdb` object, an error will be logged.
    - Token validation for both JWTSessionManager and SessionManager assumes that `fsdb` implements methods like
      `get_username` for retrieving usernames based on the provided token.

    See Also
    --------
    plantdb.commons.fsdb.auth.session.SingleSessionManager : Manages single session systems.
    plantdb.commons.fsdb.auth.session.JWTSessionManager : Handles JSON Web Token-based authentication.
    plantdb.commons.fsdb.auth.session.SessionManager : Manages multiple generic user sessions.

    Examples
    --------
    >>> import os
    >>> from plantdb.commons.test_database import dummy_db
    >>> from plantdb.commons.fsdb.core import get_logged_username
    >>> db = dummy_db()  # SingleSessionManager with automatic login as 'admin'
    >>> get_logged_username(db)
    'admin'
    >>> db.logout()
    >>> get_logged_username(db) is None
    True
    >>> get_logged_username(db, default_user='guest')
    """
    if isinstance(fsdb.session_manager, SingleSessionManager):
        # If a Single SessionManager, get the username from the session manager (as only one user can be logged at once)
        try:
            session = list(fsdb.session_manager.sessions.keys())[0]
        except IndexError:
            logged_user = fsdb.get_user_data(username=default_user)
        else:
            logged_user = fsdb.get_user_data(username=fsdb.session_manager.validate_session(session)['username'])
    elif isinstance(fsdb.session_manager, (JWTSessionManager, SessionManager)):
        # If a JSON Web Token Session Manager or a Session Manager, require the token to retrieve the username
        if token:
            logged_user = fsdb.get_user_data(token=token)
        elif default_user:
            logged_user = fsdb.get_user_data(username=default_user)
        else:
            logged_user = None
    else:
        raise ValueError("Can't serve a local PlantDB without a session manager!")

    return logged_user


def get_authentication(method):
    """Get authentication and inject ``current_user`` into the wrapped call.

    The wrapper expects the following keyword arguments (all optional):
        * ``default_user`` : fallback username (default: ``None`` → unauthenticated).
        * ``token`` : JWT supplied by the client.

    Any other ``**kwargs`` are passed through unchanged.

    See Also
    --------
    get_logged_username : Retrieves the username of the currently logged-in user.
    """

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        """
        Other Parameters
        ----------------
        default_user : str, optional
            A fallback username to use if no valid session or token is found. Defaults to ``None``.
        token : str, optional
            The session token or JWT for validating the user's session.
        """
        # Pull and normalize the authentication‑related arguments
        default_user: Optional[str] = kwargs.pop('default_user', None)
        token: Optional[str] = kwargs.get('token', None)

        # Retrieve username based on the type of `self`
        if isinstance(self, (Scan, Fileset, File)):
            user = get_logged_username(self.db, default_user=default_user, token=token)
        else:
            user = get_logged_username(self, default_user=default_user, token=token)

        # Update the `current_user` only if not already defined...
        # This is to preserve any existing value of `current_user` directly passed to the method (like a guest user)
        if not kwargs.get('current_user'):
            kwargs['current_user'] = user

        # Call the original method, injecting ``current_user``
        return method(self, *args, **kwargs)

    return wrapper


def require_authentication(method):
    """Enforce authentication."""

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        # Abort if no authenticated user could be determined
        if not kwargs.get('current_user'):
            raise NoAuthUserError()
        return method(self, *args, **kwargs)

    return wrapper


def _get_fsdb_and_scan(obj, *args) -> tuple["FSDB", "Scan"]:
    """Retrieve the `FSDB` and the `Scan` instance associated with a given object.

    Parameters
    ----------
    obj : Scan or FSDB or Fileset or File
        Object from which to obtain a ``Scan``. The function determines the
        appropriate attribute based on the runtime type of ``obj``.
    *args : tuple
        Additional positional arguments. When ``obj`` is an ``FSDB``, the first
        element should be the index or key identifying the desired scan.

    Returns
    -------
    FSDB
        The ``FSDB`` instance linked to ``obj``.
    Scan
        The ``Scan`` instance linked to ``obj``.

    Raises
    ------
    TypeError
        If ``obj`` is not an instance of ``Scan``, ``FSDB``, ``Fileset``, or ``File``.
    plantdb.commons.fsdb.exceptions.ScanNotFoundError
        If ``obj`` is an instance of ``FSDB`` and ``args[0]`` is not an existing ``Scan``.

    Notes
    -----
    The dispatch logic is as follows:

    - ``Scan``: returned directly.
    - ``FSDB``: ``obj.scans[args[0]]`` is accessed; ``args`` must contain at
      least one element identifying the scan.
    - ``Fileset`` or ``File``: the ``scan`` attribute of the object is returned.
    """
    if isinstance(obj, Scan):
        return obj.db, obj
    elif isinstance(obj, FSDB):
        try:
            return obj, obj.scans[args[0]]
        except KeyError:
            raise ScanNotFoundError(obj, args[0])
    elif isinstance(obj, Fileset):
        return obj.db, obj.scan
    elif isinstance(obj, File):
        return obj.db, obj.scan
    else:
        raise TypeError(f"Unsupported object type: {type(obj)}")


def requires_permission(required_permissions: Union[Permission, Tuple[Permission, ...]],
                        check_scan_access: bool = False):
    """Decorator that enforces permission checks before the execution of a method.

    The decorator inspects the ``current_user`` keyword argument supplied
    to the wrapped method and verifies that the user holds the required permissions.
    When ``check_scan_access`` is ``True``, the check is performed against a specific
    scan dataset, including additional token‑based dataset permissions. If the
    user is missing any required permission, a ``PermissionError`` is raised.

    Parameters
    ----------
    required_permissions : Union[Permission, Tuple[Permission, ...]]
        Permission or tuple of permissions that the user must possess to invoke
        the wrapped method.
    check_scan_access : bool, optional
        Determines whether the permission check is scoped to a particular scan
        dataset (``True``) or performed at the generic user level (``False``).
        Default is ``False``.

    Returns
    -------
    function
        A decorator that can be applied to instance methods. The decorator wraps
        the original method with the described permission validation logic.

    Raises
    ------
    PermissionError
        Raised when no ``current_user`` is provided or when the user does not have
        all of the required permissions for the operation.

    Notes
    -----
    * The wrapper extracts the ``current_user`` from ``kwargs``; it must be passed
      explicitly when the decorated method is called.
    * When ``check_scan_access`` is enabled, the wrapper retrieves the target
      ``Scan`` object via ``_get_scan`` and its metadata via ``_get_metadata`` to
      perform dataset‑specific permission checks.
    * For ``TokenUser`` instances, the wrapper also verifies that the token’s
      dataset‑level permissions include the required permissions.

    See Also
    --------
    functools.wraps
        Preserves the original method’s metadata (name, docstring, etc.) when
        creating the wrapper.
    """

    def decorator(method):
        @functools.wraps(method)
        def wrapper(self, *args, **kwargs):
            if isinstance(required_permissions, Permission):
                required_permissions_ = (required_permissions,)
            else:
                required_permissions_ = required_permissions

            user: User | TokenUser = kwargs.get("current_user", None)
            if user is None:
                raise NoAuthUserError()

            # TokenUser have scan-based permissions, disabling scan access check is done for scan creation only
            if isinstance(user, TokenUser) and not check_scan_access:
                scan_perms = user.get_permissions_for_dataset(args[0])
                has_perms = all(perm in scan_perms for perm in required_permissions_)
                if not has_perms:
                    raise PermissionError(
                        f"Token from User {user.username} does not have required permissions to use {method.__name__}")
                return method(self, *args, **kwargs)

            # check dataset
            if check_scan_access:
                db, scan = _get_fsdb_and_scan(self, *args)
                # Access metadata directly to avoid nested lock acquisition
                metadata = _get_metadata(scan.metadata, None, {})
                has_perms = all(
                    db.rbac_manager.can_access_scan(user, metadata, perm) for perm in required_permissions_
                )
                # token user have restricted permissions per dataset
                if isinstance(user, TokenUser):
                    token_permissions = user.get_permissions_for_dataset(scan.id)
                    has_perms = has_perms and all(perm in token_permissions for perm in required_permissions_)

            else:
                has_perms = all(self.rbac_manager.has_permission(user, perm) for perm in required_permissions_)

            if not has_perms:
                raise PermissionError(
                    f"User {user.username} does not have required permissions to use {method.__name__}")

            return method(self, *args, **kwargs)

        return wrapper

    return decorator


class FSDB(db.DB):
    """Implement a local *File System DataBase* version of abstract class ``db.DB``.

    Implement as a simple local file structure with the following directory structure and marker files:
      * directory ``${FSDB.basedir}`` as the database root directory;
      * marker file ``MARKER_FILE_NAME`` at the database root directory;

    Attributes
    ----------
    basedir : pathlib.Path
        The absolute path to the base directory hosting the database.
    scans : dict[str, plantdb.commons.fsdb.core.Scan]
        The dictionary of ``Scan`` instances attached to the database, indexed by their identifier.
    is_connected : bool
        ``True`` if the database is connected (locked directory), else ``False``.
    required_filesets : List[str]
        A list of required filesets to consider a scan valid. Set it to ``None`` to accept any subdirectory of basedir as a valid scan. Defaults to ['metadata'].
    logger : logging.Logger
        An instance to use for logging. Defaults to the module logger.
    session_manager : Union[SingleSessionManager, SessionManager, JWTSessionManager]
        The session manager to use for session authentication.
    lock_manager : ScanLockManager
        Manager for scanning lock operations.
    rbac_manager : RBACManager
        Manager for role-based access control.

    Notes
    -----
    Requires the marker file ``MARKER_FILE_NAME`` at the given ``basedir``.

    See Also
    --------
    plantdb.commons.db.DB
    plantdb.commons.fsdb.core.MARKER_FILE_NAME

    Examples
    --------
    >>> from plantdb.commons.test_database import dummy_db
    >>> # EXAMPLE 1: Use a temporary dummy local database:
    >>> db = dummy_db()
    >>> print(type(db))
    <class 'plantdb.commons.fsdb.core.FSDB'>
    >>> print(db.path())
    /tmp/romidb_********
    >>> # Create a new `Scan`:
    >>> new_scan = db.create_scan("007")
    >>> print(type(new_scan))
    <class 'plantdb.commons.fsdb.core.Scan'>
    >>> db.disconnect()  # clean up (delete) the temporary dummy database

    >>> # EXAMPLE 2: Use a local database:
    >>> import os
    >>> from plantdb.commons.fsdb.core import FSDB
    >>> db = FSDB(os.environ.get('ROMI_DB', "/data/ROMI/DB/"))
    >>> db.connect()
    >>> token = db.login('guest', 'guest')
    >>> scan_ids = db.list_scans(owner_only=False, token=token)
    >>> [scan.id for scan in db.get_scans(token=token)]  # list scan ids found in database
    >>> scan = db.get_scans()[1]
    >>> [fs.id for fs in scan.get_filesets()]  # list fileset ids found in scan
    >>> db.disconnect()  # clean up (delete) the temporary dummy database
    """

    def __init__(self, basedir: Union[str, Path],
                 required_filesets: Optional[List[str]] = None,
                 logger: Optional[logging.Logger] = None,
                 session_manager: Union[SingleSessionManager, SessionManager, JWTSessionManager] = None,
                 session_timeout: int = 3600, max_login_attempts: int = 3,
                 lockout_duration: int = 900, max_concurrent_sessions: int = 10):
        """Database constructor.

        Check the given `` basedir `` directory exists and load accessible ``Scan`` objects.

        Parameters
        ----------
        basedir : str or pathlib.Path
            The path to the root directory of the database.
        required_filesets : list of str, optional
            A list of required filesets to consider a scan valid.
            By default, ``None``, will set it to ``['metadata']`` to define as a "scan" the subdirectories with a 'metadata' directory.
            Use `[]` to accept any subdirectory of `basedir` as a valid "scan".
        logger : logging.Logger, optional
            Logger instance to use for logging. Defaults to the module logger.
        session_manager : Union[SingleSessionManager, SessionManager, JWTSessionManager], optional
            The session manager to use for session authentication.
            Defaults to ``SingleSessionManager``.
        session_timeout : int, optional
            Session timeout in seconds.
            Defaults to 3600 (1 hour).
        lockout_duration : int, optional
            The number of seconds to wait for a lockout before giving up.
            Defaults to 900 seconds (15 minutes).
        max_login_attempts : int, optional
            The maximum number of attempts to login before locking up.
            Defaults to 3.
        max_concurrent_sessions : int, optional
            The maximum number of concurrent sessions to login before locking up.
            Defaults to 10.

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

        basedir = Path(basedir)
        # Check the given path to the root directory of the database is a directory:
        if not basedir.is_dir():
            raise NotADirectoryError(f"Directory {basedir} does not exists!")
        self.basedir = Path(basedir).resolve()
        self.scans = {}
        self.is_connected: bool = False
        self.required_filesets = required_filesets or ['metadata']

        # Initialize lock manager
        self.lock_manager = LockManager(basedir)

        # Initialize the session manager
        if session_manager is None:
            self.session_manager = SingleSessionManager(
                session_timeout=session_timeout,
            )
        elif isinstance(session_manager, (SingleSessionManager, SessionManager, JWTSessionManager)):
            self.session_manager = session_manager
        else:
            raise ValueError(
                f"session_manager must be an instance of 'SingleSessionManager', 'SessionManager', "
                f"or 'JWTSessionManager', got {type(session_manager)}"
            )

        # Initialize RBAC manager with groups file in basedir
        users_file = os.path.join(basedir, "users.json")
        groups_file = os.path.join(basedir, "groups.json")
        self.rbac_manager = RBACManager(users_file, groups_file, max_login_attempts, lockout_duration)

    def path(self) -> pathlib.Path:
        """Get the path to the local database root directory.

        Examples
        --------
        >>> from plantdb.commons.test_database import dummy_db
        >>> db = dummy_db()
        >>> print(db.path())
        /tmp/romidb_********
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        return copy.deepcopy(self.basedir)

    def connect(self) -> bool:
        """Connect the database by loading the scans' dataset."""
        try:
            # Initialize scan discovery
            self.scans = _load_scans(self)
            self.is_connected = True
            self.logger.info("Successfully connected to the database")
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {e}")
            raise

        return True

    @require_connected_db
    def disconnect(self) -> None:
        """Disconnect from the database.

        This method disconnects from the database, if currently connected, by erasing all scans (from memory)
        and reseting the connection status.

        Examples
        --------
        >>> from plantdb.commons.test_database import dummy_db
        >>> db = dummy_db()
        >>> print(db.is_connected)
        True
        >>> print(db.path().exists())
        True
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        >>> print(db.is_connected)
        False
        >>> print(db.path().exists())
        False
        """
        for s_id, scan in self.scans.items():
            scan._erase()
        self.scans = {}
        self.is_connected = False

        # If this FSDB instance was created by dummy_db, clean up the temp directory
        if getattr(self, "_is_dummy", False):
            import shutil
            try:
                shutil.rmtree(self.basedir)
                self.logger.info(f"Removed temporary database directory {self.basedir}")
            except Exception as e:
                self.logger.warning(f"Failed to remove temporary directory {self.basedir}: {e}")
        return

    @require_connected_db
    def reload(self, scan_id: Optional[Union[str, Iterable[str]]] = None) -> None:
        """Reload the database by scanning datasets.

        Parameters
        ----------
        scan_id : str or list of str, optional
            The name of the scan(s) to reload.
        """
        if scan_id is None:
            self.logger.info("Reloading the database...")
            self.scans = _load_scans(self)
        elif isinstance(scan_id, str):
            self.logger.info(f"Reloading scan '{scan_id}'...")
            scan = _load_scan(self, scan_id)
            if not scan:
                self.logger.error(f"Failed to reload scan '{scan_id}'!")
            self.scans[scan_id] = scan
        elif isinstance(scan_id, Iterable):
            [self.reload(scan_i) for scan_i in scan_id]
        else:
            self.logger.error(f"Wrong parameter `scan_name`, expected a string or list of string but got '{scan_id}'!")
        self.logger.info("Done!")
        return

    @require_connected_db
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
        >>> from plantdb.commons.test_database import dummy_db
        >>> db = dummy_db(with_scan=True)
        >>> db.scan_exists("myscan_001")
        True
        >>> db.scan_exists("nonexistent_id")
        False
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        return scan_id in self.scans

    @require_connected_db
    @get_authentication
    @require_authentication
    def get_scans(self, query=None, current_user=None, **kwargs) -> List:
        """Get a list of `Scan` instances defined in the local database, possibly filtered using a `query`.

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
        list of plantdb.commons.fsdb.core.Scan
            A list of `Scan`s, filtered by the `query` if any.

        See Also
        --------
        plantdb.commons.fsdb._filter_query

        Examples
        --------
        >>> from plantdb.commons.test_database import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> db.get_scans()
        [<plantdb.commons.fsdb.core.Scan at *x************>]
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        # Get all scans and filter by access permissions
        accessible_scans = {}

        for scan_id, scan in self.scans.items():
            try:
                # Access metadata directly to avoid nested lock acquisition
                metadata = _get_metadata(scan.metadata, None, {})
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
        else:
            accessible_scans = list(accessible_scans.values())

        return accessible_scans

    @require_connected_db
    @get_authentication
    @require_authentication
    @requires_permission(Permission.READ, check_scan_access=True)
    def get_scan(self, scan_id, current_user=None, **kwargs):
        """Get a ` Scan ` instance in the local database.

        Parameters
        ----------
        scan_id : str
            The name of the scan dataset to get/create.
            It should exist if `create` is `False`.

        Raises
        ------
        plantdb.commons.fsdb.ScanNotFoundError
            If the `scan_id` does not exist in the local database and `create` is ``False``.

        Returns
        -------
        plantdb.commons.fsdb.core.Scan
            The `Scan` identified by the `scan_id`.

        Examples
        --------
        >>> from plantdb.commons.test_database import dummy_db
        >>> db = dummy_db(with_scan=True)
        >>> scan = db.get_scan('myscan_001')
        >>> print(scan)
        <plantdb.commons.fsdb.core.Scan object at **************>
        >>> db.list_scans()
        ['007']
        >>> unknown_scan = db.get_scan('unknown')
        plantdb.commons.fsdb.ScanNotFoundError: Unknown scan id 'unknown'!
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        with self.lock_manager.acquire_lock(scan_id, LockType.SHARED, current_user.username, LockLevel.SCAN):
            if not self.scan_exists(scan_id):
                ScanNotFoundError(self, scan_id)
            return self.scans[scan_id]

    @require_connected_db
    @get_authentication
    @require_authentication
    @requires_permission(Permission.CREATE, check_scan_access=False)
    def create_scan(self, scan_id, metadata=None, current_user=None, **kwargs):
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
        Optional[plantdb.commons.fsdb.core.Scan]
            The ``Scan`` instance created in the local database.

        Raises
        ------
        PermissionError
            If no user is authenticated.
            If the user lacks permission to create groups.
        ScanExistsError
            If the ``scan_id`` already exists in the local database.
        ValueError
            If the given ``scan_id`` is invalid.

        See Also
        --------
        plantdb.commons.fsdb.validation._is_valid_id
        plantdb.commons.fsdb.file_ops._make_scan

        Examples
        --------
        >>> from plantdb.commons.test_database import dummy_db
        >>> db = dummy_db()
        >>> new_scan = db.create_scan('007', metadata={'project': 'GoldenEye'})  # create a new scan dataset
        >>> print(new_scan.get_metadata('owner'))  # default user 'admin' for dummy database
        admin
        >>> print(new_scan.get_metadata('project'))
        GoldenEye
        >>> scan = db.create_scan('007')  # attempt to create an existing scan dataset
        plantdb.commons.fsdb.exceptions.ScanExistsError: Scan id '007' already exists in database '/tmp/ROMI_DB_bx519w11'!
        >>> scan = db.create_scan('0/07')  # attempt to create a scan dataset using invalid characters
        ValueError: Invalid scan identifier '0/07'!
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        # Verify if the given `fs_id` is valid
        if not _is_valid_id(scan_id):
            raise ValueError(f"Invalid scan identifier '{scan_id}'!")
        # Verify if the given `scan_id` already exists in the local database
        if self.scan_exists(scan_id):
            raise ScanExistsError(self, scan_id)

        # Prepare metadata with ownership
        if metadata is None:
            metadata = {}

        # Set basic metadata
        metadata['owner'] = current_user.username
        now = iso_date_now()
        metadata['created'] = now  # creation timestamp
        metadata['last_modified'] = now  # modification timestamp
        metadata['created_by'] = current_user.fullname

        # Validate sharing groups if specified
        if 'sharing' in metadata:
            sharing_groups = metadata['sharing']
            if isinstance(sharing_groups, str):
                sharing_groups = [sharing_groups]
            if not isinstance(sharing_groups, list):
                raise ValueError("Sharing field must be a list of group names")
            if not self.rbac_manager.validate_sharing_groups(sharing_groups):
                raise ValueError("One or more sharing groups do not exist")

        # Use exclusive lock for scan creation
        self.logger.debug(f"Creating a scan '{scan_id}' as user '{current_user.username}'...")
        with self.lock_manager.acquire_lock(scan_id, LockType.EXCLUSIVE, current_user.username, LockLevel.SCAN):
            # Initialize scan object
            scan = Scan(self, scan_id)  # Initialize a new Scan instance
            scan_path = _make_scan(scan)  # Create directory structure
            # Cannot use scan.set_metadata(initial_metadata) here as ownership is not granted yet!
            _set_metadata(scan.metadata, metadata, None)  # add metadata dictionary to the new scan
            _store_scan_metadata(scan)
            scan.store()  # store the new scan in the local database
            self.scans[scan_id] = scan  # Update scans dictionary with the new one

        self.logger.debug(f"Done creating scan.")
        return scan

    @require_connected_db
    @get_authentication
    @require_authentication
    @requires_permission(Permission.DELETE, check_scan_access=True)
    def delete_scan(self, scan_id, current_user=None, **kwargs) -> bool:
        """Delete an existing `Scan` from the local database.

        Parameters
        ----------
        scan_id : str
            The name of the scan to delete from the local database.

        Returns
        -------
        bool
            A boolean value indicating whether the scan was successfully deleted.

        Raises
        ------
        PermissionError
            If no user is authenticated.
            If the user lacks permission to create groups.
        ValueError
            If the ``scan_id`` does not exist in the local database.
        IOError
            If the scan is locked by another user.

        See Also
        --------
        plantdb.commons.fsdb.file_ops._delete_scan

        Examples
        --------
        >>> from plantdb.commons.fsdb.core import FSDB
        >>> from plantdb.commons.test_database import dummy_db
        >>> db = dummy_db()
        >>> new_scan = db.create_scan('007')
        >>> print(new_scan)
        <plantdb.commons.fsdb.core.Scan object at 0x7f0730b1e390>
        >>> db.delete_scan('007')
        >>> scan = db.get_scan('007')
        >>> print(scan)
        None
        >>> db.delete_scan('008')
        OSError: Invalid id
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        # Use exclusive lock for scan deletion
        self.logger.debug(f"Deleting scan '{scan_id}' as '{current_user.username}' user...")
        with self.lock_manager.acquire_lock(scan_id, LockType.EXCLUSIVE, current_user.username, LockLevel.SCAN):
            # Get the Scan instance from the database
            scan = self.scans[scan_id]
            _delete_scan(scan)  # delete the scan directory
            self.scans.pop(scan_id)  # remove the scan from the scan list

        self.logger.debug(f"Done deleting scan.")
        return True

    @require_connected_db
    @get_authentication
    def list_scans(self, query=None, fuzzy=False, owner_only=True, current_user=None, **kwargs) -> list[str]:
        """Get the list of scan identifiers from the local database.

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
            The list of scan identifiers from the local database.

        See Also
        --------
        plantdb.commons.fsdb.core._filter_query

        Examples
        --------
        >>> from plantdb.commons.test_database import dummy_db
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
        if not current_user:
            owner_only = False

        if owner_only:
            if query is None:
                query = {'owner': current_user.username}
            else:
                query.update({'owner': current_user.username})

        if query is None:
            return list(self.scans.keys())
        else:
            return [scan.id for scan in _filter_query(list(self.scans.values()), query, fuzzy)]

    @require_connected_db
    def get_scan_lock_status(self, scan_id: str) -> Dict:
        """Get the current lock status for a specific scan.

        Parameters
        ----------
        scan_id : str
            The name of the scan in the local database.

        Returns
        -------
        dict
            Dictionary with lock status information
        """
        return self.lock_manager.get_lock_status(scan_id, LockLevel.SCAN)

    @require_connected_db
    def is_scan_locked(self, scan_id: str) -> bool:
        """Check if a scan is locked in the system.

        This method determines whether the specified scan is currently locked by
        fetching its lock status. A scan is considered locked if it does not have
        an exclusive lock and there are no shared locks. This can be useful for
        ensuring that certain operations are not performed on scans that are not
        yet locked.

        Parameters
        ----------
        scan_id : str
            The unique identifier of the scan whose lock status is to be checked.

        Returns
        -------
        bool
            True if the scan is locked (having neither an exclusive lock nor any
            shared locks), False otherwise.

        See Also
        --------
        ScanManager.lock_manager : Component responsible for managing scan locks.
        """
        lock_status = self.lock_manager.get_lock_status(scan_id)
        if lock_status['exclusive'] is None and len(lock_status['shared']) == 0:
            return True
        return False

    def cleanup_scan_locks(self) -> None:
        """Emergency cleanup of all scan locks.
        Use with caution - only call when you're sure no operations are in progress.
        """
        self.lock_manager.cleanup_all_locks()
        self.logger.warning("All scan locks have been cleaned up")

    @require_connected_db
    def list_active_locks(self) -> Dict[str, Dict]:
        """List all currently active locks across all scans.

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

    @require_connected_db
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

        Examples
        --------
        >>> from plantdb.commons.test_database import dummy_db
        >>> db = dummy_db()  # SingleSessionManager with automatic login as 'admin'
        >>> db.validate_user('guest', 'guest')
        True
        >>> db.disconnect()
        """
        return self.rbac_manager.users.validate(username, password)

    @require_connected_db
    def login(self, username: str, password: str, **kwargs) -> Optional[str]:
        """Authenticate a user and create a session.

        Parameters
        ----------
        username : str
            Username for authentication.
        password : str
            Password for authentication.

        Returns
        -------
        Optional[str]
            Returns the user session ID if successful, ``None`` otherwise.

        Examples
        --------
        >>> from plantdb.commons.test_database import dummy_db
        >>> db = dummy_db()  # SingleSessionManager with automatic login as 'admin'
        >>> token = db.login('guest', 'guest')
        ERROR    [FSDB] Failed to login as 'guest'! Another user is logged in.
        >>> db.logout()
        INFO     [FSDB] User 'admin' logged out successfully.
        True
        >>> token = db.login('guest', 'guest')
        [FSDB] Successfully logged in as 'guest'.
        >>> db.disconnect()
        """
        if self.validate_user(username, password):

            # If a SingleSessionManager and a currently logged user, abort
            if isinstance(self.session_manager, SingleSessionManager):
                current_username = get_logged_username(self)
                if current_username:
                    if current_username != username:
                        self.logger.error(f"Failed to login as '{username}'! Another user is logged in.")
                        return None
                    else:
                        self.logger.warning(f"Already logged in as '{username}'.")
                        return

            # Else try to create a new session:
            session_token = self.session_manager.create_session(username)
            try:
                assert session_token is not None
            except AssertionError:
                self.logger.warning(f"User '{username}' has reached max concurrent sessions")
            else:
                self.logger.debug(f"Successfully logged in as '{username}'.")
            return session_token
        else:
            self.logger.error(f"Failed to login as '{username}'!")
            return None

    @require_token
    def logout(self, **kwargs) -> tuple[bool, str]:
        """Log out a user by invalidating its session.

        Examples
        --------
        >>> from plantdb.commons.test_database import dummy_db
        >>> db = dummy_db()  # automatic login as 'admin'
        INFO     [FSDB] Successfully logged in as 'admin'.
        >>> db.logout()
        INFO     [FSDB] Successfully logged out from 'admin'.
        (True, 'admin')
        >>> db.disconnect()
        """
        success, username = self.session_manager.invalidate_session(kwargs.get('token', None))
        if success:
            self.logger.debug(f"Successfully logged out from '{username}'.")
            return success, username
        else:
            self.logger.warning(f"Failed to logout!")
            return success, username

    @get_authentication
    @require_authentication
    @requires_permission(Permission.MANAGE_USERS, check_scan_access=False)
    def create_user(self, new_username, fullname, password, roles=None, **kwargs) -> None:
        """Create a new user with the specified details.

        Parameters
        ----------
        new_username : str
            The unique username for the new user.
        fullname : str
            The full name of the new user.
        password : str
            The password for the new user.
        roles : list[str], optional
            A list of roles to assign to the new user. Default is None.

        Raises
        ------
        PermissionError
            If no user is authenticated.
            If the user lacks permission to create groups.

        See Also
        --------
        RBACManager.users.create : Method used to actually create the user.

        Examples
        --------
        >>> from plantdb.commons.auth.models import Role
        >>> from plantdb.commons.test_database import dummy_db
        >>> db = dummy_db()  # automatic login as 'admin'
        INFO     [FSDB] Successfully logged in as 'admin'.
        >>> db.create_user('batman', 'Bruce Wayne', 'joker', roles=Role.CONTRIBUTOR)
        INFO     [UserManager] Welcome Bruce Wayne, please log in...'
        >>> db.logout()
        >>> token = db.login('batman', 'joker')
        INFO     [FSDB] Successfully logged in as 'batman'.
        >>> db.disconnect()
        """
        return self.rbac_manager.users.create(new_username, fullname, password, roles)

    @require_connected_db
    @get_authentication
    @require_authentication
    def create_api_token(
            self, token_exp: int,
            datasets: dict[str, tuple[Permission, ...] | Permission],
            current_user: User | None = None, **kwargs
    ) -> str:
        """
        Creates an API token for the current user with optional dataset permissions.

        This method generates a new API token using the session manager, which must
        be an instance of `JWTSessionManager`. The token is valid for a specified
        expiration duration and optionally includes permissions for datasets. The
        generated token can be used for authenticated API access.

        Parameters
        ----------
        token_exp : int
            The expiration duration of the API token in seconds.
        datasets : dict of str to tuple of Permission or Permission
            A dictionary where the keys are dataset names, and the values are either
            a tuple of `Permission` instances or a single `Permission` instance
            defining the access levels for each dataset.
        current_user : plantdb.commons.fsdb.auth.models.User, optional
            The user object representing the currently authenticated user. The username
            from this object is used to associate the API token.
        **kwargs
            Additional keyword arguments passed for possible extended functionality.

        Returns
        -------
        str
            The generated API token.
        """
        if not isinstance(self.session_manager, JWTSessionManager):
            raise RuntimeError("Session manager must be an instance of JWTSessionManager.")
        session_manager: JWTSessionManager = self.session_manager
        return session_manager.create_api_token(current_user.username, token_exp, datasets)

    def get_guest_user(self) -> User:
        """Retrieve the guest user information from the RBAC manager.

        Returns
        -------
        plantdb.commons.auth.models.User
            The User object corresponding to the guest username.

        Notes
        -----
        This method interacts with the underlying RBAC manager to fetch guest user information.

        See Also
        --------
        rbac_manager.get_guest_user : The underlying method used by this function.

        Examples
        --------
        >>> from plantdb.commons.test_database import dummy_db
        >>> db = dummy_db()  # automatic login as 'admin'
        >>> db.get_guest_user()
        User(username='guest', fullname='PlantDB Guest', password_hash='$argon2id$v=19$m=65536,t=3,p=4$2++/KY75t4qvt5x1fO4dJA$MDmREeceXOJhcupT1G6yuRFvPUJ3SjNpuSga5wkUEYw', roles={<Role.READER: 'reader'>}, created_at=datetime.datetime(2026, 1, 29, 17, 19, 13, 313677), permissions=None, last_login=datetime.datetime(2026, 1, 29, 17, 19, 20, 177023), is_active=True, failed_attempts=0, last_failed_attempt=None, locked_until=None, password_last_change=datetime.datetime(2026, 1, 29, 17, 19, 13, 313677))
        >>> db.disconnect()
        """
        return self.rbac_manager.get_guest_user()

    def get_username(self, token) -> Optional[str]:
        """Get the username.

        Parameters
        ----------
        token : str
            The token provided by the RBAC manager.

        Returns
        -------
        Optional[str]
            The ``User.username`` if the token is valid, ``None`` otherwise.

        Examples
        --------
        >>> from plantdb.commons.test_database import dummy_db
        >>> db = dummy_db()  # automatic login as 'admin'
        >>> db.logout()
        INFO     [FSDB] Successfully logged out from 'admin'.
        >>> token = db.login('guest', 'guest')
        INFO     [FSDB] Successfully logged in as 'guest'.
        >>> db.get_username(token)
        'guest'
        >>> db.disconnect()
        """
        return self.session_manager.session_username(token)

    def get_user_data(self, username=None, token=None) -> User | TokenUser | None:
        """Get the user data.

        Parameters
        ----------
        username : str
            The username to retrieve the user data from.
        token : str
            The token provided by the RBAC manager.

        Returns
        -------
        plantdb.commons.auth.models.User | plantdb.commons.auth.models.TokenUser | None
            A ``User`` instance corresponding to the currently authenticated user, if any, ``None`` otherwise.

        Notes
        -----
        If both `username` and `token` are provided, prefer `token` for accessing user data.

        Examples
        --------
        >>> from plantdb.commons.test_database import dummy_db
        >>> db = dummy_db()  # automatic login as 'admin'
        >>> db.get_user_data(username='admin')
        User(username='admin', fullname='PlantDB Admin', password_hash='$argon2id$v=19$m=65536,t=3,p=4$zMr0ZhclnHHdOgwWKv3Hbg$SZshbPdNiCdBONb8vgzZAKyWPl5sNIUwB8mQWkzGYOQ', roles={<Role.ADMIN: 'admin'>}, created_at=datetime.datetime(2026, 1, 29, 17, 22, 49, 683163), permissions=None, last_login=datetime.datetime(2026, 1, 29, 17, 22, 49, 770793), is_active=True, failed_attempts=0, last_failed_attempt=None, locked_until=None, password_last_change=datetime.datetime(2026, 1, 29, 17, 22, 49, 683163))
        >>> db.logout()
        INFO     [FSDB] Successfully logged out from 'admin'.
        >>> token = db.login('guest', 'guest')
        INFO     [FSDB] Successfully logged in as 'guest'.
        >>> user_data = db.get_user_data(token=token)
        >>> print(user_data.fullname)
        PlantDB Guest
        >>> db.disconnect()
        """
        if username and token:
            self.logger.warning("Trying to retrieve user data from both 'username' and token!")
            self.logger.debug("Using 'token' to access user data.")
            username = None

        if username:
            return self.rbac_manager.users.get_user(username)
        elif token:
            return self.rbac_manager.users.get_user_from_decoded_token(self.session_manager.validate_session(token))
        else:
            self.logger.error("No username or token provided")
            return None

    # Group management methods

    @get_authentication
    @require_authentication
    def create_group(self, name, users=None, description=None, current_user=None, **kwargs) -> Optional[Group]:
        """Create a new group.

        Parameters
        ----------
        name : str
            Unique name for the group.
        users : set, optional
            Initial set of users to add to the group.
        description : str, optional
            An optional description of the group.

        Other Parameters
        ----------------
        username : str
            The username formulating the request.
        token : str
            A token referring to the username formulating the request.

        Returns
        -------
        Optional[Group]
            The created group object if successful, ``None`` otherwise.

        Raises
        ------
        PermissionError
            If no user is authenticated.
            If the user lacks permission to create groups.
        ValueError
            If the group already exists.

        Examples
        --------
        >>> from plantdb.commons.auth.models import Role
        >>> from plantdb.commons.test_database import dummy_db
        >>> db = dummy_db()  # automatic login as 'admin'
        >>> db.create_user('batman', 'Bruce Wayne', 'joker', roles=Role.CONTRIBUTOR)
        >>> group_a = db.create_group('groupA', ['batman'], description="The group A.")
        >>> prin(group_a.users)
        {'admin', 'batman'}
        >>> db.disconnect()
        """
        if isinstance(users, str):
            users = [users]
        if isinstance(users, Iterable):
            users = set(users)

        return self.rbac_manager.create_group(current_user, name, users, description)

    @get_authentication
    @require_authentication
    def add_user_to_group(self, group_name, user, current_user=None, **kwargs):
        """Add a user to a group.

        Parameters
        ----------
        group_name : str
            Name of the group to add the user to.
        user : str
            Name of the user to add to the group.

        Other Parameters
        ----------------
        username : str
            The username formulating the request.
        token : str
            A token referring to the username formulating the request.

        Returns
        -------
        bool
            ``True`` if the `user` was successfully added to the group, ``False`` otherwise.

        Raises
        ------
        PermissionError
            If no user is authenticated.
            If the authenticated user lacks permission to modify the group.

        Examples
        --------
        >>> from plantdb.commons.auth.models import Role
        >>> from plantdb.commons.test_database import dummy_db
        >>> db = dummy_db()  # automatic login as 'admin'
        >>> db.create_user('batman', 'Bruce Wayne', 'joker', roles=Role.CONTRIBUTOR)
        >>> group_a = db.create_group('groupA', ['batman'], description="The group A.")
        >>> print(group_a.users)
        {'admin', 'batman'}
        >>> db.create_user('hquinn', 'Harley Quinn', 'joker', roles=Role.READER)
        >>> db.add_user_to_group('groupA', 'hquinn')
        >>> print(group_a.users)
        {'hquinn', 'batman', 'admin'}
        >>> db.disconnect()
        """
        if not self.rbac_manager.add_user_to_group(current_user, group_name, user):
            raise PermissionError("Insufficient permissions or operation failed")
        return True

    @get_authentication
    @require_authentication
    def remove_user_from_group(self, group_name, user, current_user=None, **kwargs):
        """Remove a user from a group.

        Parameters
        ----------
        group_name : str
            Name of the group to remove the user from.
        user : str
            Name of the user to remove from the group.

        Other Parameters
        ----------------
        username : str
            The username formulating the request.
        token : str
            A token referring to the username formulating the request.

        Returns
        -------
        bool
            ``True`` if the `user` was successfully removed from the group, ``False`` otherwise.

        Raises
        ------
        PermissionError
            If no user is authenticated.
            If the authenticated user lacks permission to remove the user from the group.

        Examples
        --------
        >>> from plantdb.commons.auth.models import Role
        >>> from plantdb.commons.test_database import dummy_db
        >>> db = dummy_db()  # automatic login as 'admin'
        >>> db.create_user('batman', 'Bruce Wayne', 'joker', roles=Role.CONTRIBUTOR)
        >>> group_a = db.create_group('groupA', ['batman'], description="The group A.")
        >>> print(group_a.users)
        {'admin', 'batman'}
        >>> db.remove_user_from_group('groupA', 'batman')
        >>> print(group_a.users)
        {'admin'}
        >>> db.disconnect()
        """
        if not self.rbac_manager.remove_user_from_group(current_user, group_name, user):
            raise PermissionError("Insufficient permissions or operation failed")
        return True

    @get_authentication
    @require_authentication
    def delete_group(self, group_name, current_user=None, **kwargs) -> bool:
        """Delete a group.

        Parameters
        ----------
        group_name : str
            Name of the group to delete

        Other Parameters
        ----------------
        username : str
            The username formulating the request.
        token : str
            A token referring to the username formulating the request.

        Returns
        -------
        bool
            True if the group was deleted successfully

        Raises
        ------
        PermissionError
            If no user is authenticated.
            If the authenticated user lacks permission to delete this group.

        Examples
        --------
        >>> from plantdb.commons.auth.models import Role
        >>> from plantdb.commons.test_database import dummy_db
        >>> db = dummy_db()  # automatic login as 'admin'
        >>> db.create_user('batman', 'Bruce Wayne', 'joker', roles=Role.CONTRIBUTOR)
        >>> group_a = db.create_group('groupA', ['batman'], description="The group A.")
        >>> print(group_a.users)
        {'admin', 'batman'}
        >>> db.logout()
        >>> token = db.login('batman', 'joker')
        ERROR    [RBACManager] Insufficient permission to delete group 'groupA' by user 'batman!
        PermissionError: Insufficient permissions or group 'groupA' not found
        >>> db.delete_group('groupA')
        >>> db.logout()
        >>> token = db.login('admin', 'admin')
        >>> db.delete_group('groupA')
        WARNING  [RBACManager] Deleting group 'groupA' by user 'admin'!
        >>> db.disconnect()
        """
        if not self.rbac_manager.delete_group(current_user, group_name):
            raise PermissionError(f"Insufficient permissions or group '{group_name}' not found")
        return True

    @get_authentication
    @require_authentication
    def list_groups(self, current_user=None, **kwargs) -> list[Group]:
        """List all groups.

        Returns
        -------
        list[Group]
            A list of Group objects

        Raises
        ------
        PermissionError
            If no user is authenticated.

        Examples
        --------
        >>> from plantdb.commons.auth.models import Role
        >>> from plantdb.commons.test_database import dummy_db
        >>> db = dummy_db()  # automatic login as 'admin'
        >>> db.create_user('batman', 'Bruce Wayne', 'joker', roles=Role.CONTRIBUTOR)
        >>> group_a = db.create_group('groupA', ['batman'], description="The group A.")
        >>> print([g.name for g in db.list_groups()])
        ['groupA']
        >>> db.disconnect()
        """
        groups = self.rbac_manager.list_groups(current_user)
        return groups if groups is not None else []

    @get_authentication
    @require_authentication
    def get_user_groups(self, user=None, current_user=None, **kwargs) -> list[Group]:
        """Get groups for a user.

        Parameters
        ----------
        user : str, optional
            Username to query.
            If None, uses the currently authenticated user.

        Other Parameters
        ----------------
        username : str
            The username formulating the request.
        token : str
            A token referring to the username formulating the request.

        Returns
        -------
        list[Groups]
            A list of Group objects the user belongs to.

        Raises
        ------
        PermissionError
            If no user is authenticated.

        Examples
        --------
        >>> from plantdb.commons.auth.models import Role
        >>> from plantdb.commons.test_database import dummy_db
        >>> db = dummy_db()  # automatic login as 'admin'
        >>> db.create_user('batman', 'Bruce Wayne', 'joker', roles=Role.CONTRIBUTOR)
        >>> group_a = db.create_group('groupA', ['batman'], description="The group A.")
        >>> print([g.name for g in db.get_user_groups('batman')])
        ['groupA']
        >>> db.disconnect()
        """
        if user is None:
            user = current_user.username
        return self.rbac_manager.get_user_groups(user)

    @get_authentication
    @require_authentication
    def get_scan_access_summary(self, scan_id, current_user=None, **kwargs):
        """Get access summary for the current user on a scan.

        Parameters
        ----------
        scan_id : str
            The scan identifier

        Other Parameters
        ----------------
        username : str
            The username formulating the request.
        token : str
            A token referring to the username formulating the request.

        Returns
        -------
        dict or None
            An access summary dictionary if scan exists and is accessible

        Raises
        ------
        PermissionError
            If no user is authenticated.

        Examples
        --------
        >>> from plantdb.commons.test_database import test_database
        >>> db = test_database(dataset="all")
        >>> db.connect()
        >>> token = db.login('guest', 'guest')
        >>> scan_access = db.get_scan_access_summary("real_plant")
        >>> print(scan_access["effective_role"])
        contributor
        >>> print(scan_access["permissions"])
        ['write', 'create', 'read']
        >>> print(scan_access["is_owner"])
        True
        >>> db.logout()
        >>> token = db.login('admin', 'admin')
        >>> scan_access = db.get_scan_access_summary("real_plant")
        >>> print(scan_access["effective_role"])
        admin
        >>> print(scan_access["is_owner"])
        False
        >>> print(scan_access["access_reason"])
        ['admin_role']
        >>> db.disconnect()
        """
        if scan_id not in self.scans:
            return None

        scan = self.scans[scan_id]
        try:
            metadata = scan.get_metadata()
            return self.rbac_manager.get_user_scan_role_summary(current_user, metadata)
        except Exception as e:
            self.logger.warning(f"Error getting access summary for scan {scan_id}: {e}")
            return None


class Scan(db.Scan, MetadataManager):
    """Implement ``Scan`` for the local *File System DataBase* from the abstract class ``db.Scan``.

    Implementation of a scan as a simple file structure with:
      * directory ``${Scan.db.basedir}/${Scan.db.id}`` as scan root directory;
      * (OPTIONAL) directory ``${Scan.db.basedir}/${Scan.db.id}/metadata`` containing JSON metadata file
      * (OPTIONAL) JSON file ``metadata.json`` with Scan metadata

    Attributes
    ----------
    db : plantdb.commons.fsdb.core.FSDB
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
    >>> from plantdb.commons.test_database import dummy_db
    >>> db = dummy_db()
    >>> # Example #1: Initialize a `Scan` object using an `FSBD` object:
    >>> scan = Scan(db, '007')
    >>> print(type(scan))
    <class 'plantdb.commons.fsdb.core.Scan'>
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
    <class 'plantdb.commons.fsdb.core.Scan'>
    >>> print(db.get_scan('007'))  # This time the `Scan` object is found in the `FSBD`
    <plantdb.commons.fsdb.core.Scan object at 0x7f34fc860fd0>
    >>> print(os.listdir(db.path()))  # And it is found under the `basedir` directory
    ['007', 'romidb']
    >>> print(os.listdir(os.path.join(db.path(), scan.id)))  # Same goes for the metadata
    ['metadata']
    >>> db._is_dummy = False  # to avoid cleaning up the
    >>> db.disconnect()
    >>> # When reconnecting to db, if created scan is EMPTY (no Fileset & File) it is not found!
    >>> db.connect()
    >>> print(db.get_scan('007'))
    None
    >>> db._is_dummy = True  # to clean up the temporary dummy database
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
        db : plantdb.commons.fsdb.core.FSDB
            The database to put/find the scan dataset.
        scan_id : str
            The scan dataset name, should be unique in the `db`.
        """
        super().__init__(db, scan_id)
        # Defines attributes:
        self.metadata = {}
        self.filesets = {}
        self.measures = None

        self.session_manager = self.db.session_manager
        self.logger = self.db.logger

    def _erase(self):
        """Erase the filesets and metadata associated with this scan."""
        for fs_id, fs in self.filesets.items():
            fs._erase()
        self.metadata = {}
        self.filesets = {}
        self.measures = None
        return

    @property
    def owner(self) -> str:
        """A property method to retrieve or set the `owner` of a resource.

        If the `owner` is not already defined in the resource's metadata, this property
        ensures that a default owner is assigned (using a guest user). The updated metadata
        is then stored in the database, and the resource is reloaded.

        Returns
        -------
        str
            The owner of the resource, as defined in the metadata.

        See Also
        --------
        rbac_manager.ensure_scan_owner : Ensures the presence of a valid owner in the metadata.
        _store_scan_metadata : Saves updated metadata to the database.
        db.reload : Reloads the resource from the database after updates.
        """
        # If no owner is defined, set it to the guest user, save it and reload it into the DB
        if 'owner' not in self.metadata:
            metadata = self.db.rbac_manager.ensure_scan_owner(self.metadata)
            _set_metadata(self.metadata, metadata, None)
            _store_scan_metadata(self)
            self.db.reload(self.id)
        return self.metadata.get('owner')

    def is_locked(self) -> bool:
        """Check if a scan is locked in the system.

        Returns
        -------
        bool
            ``True`` if the scan is locked (having neither an exclusive lock nor any shared locks), ``False`` otherwise.

        See Also
        --------
        ScanManager.lock_manager: Component responsible for managing scan locks.
        """
        return self.db.is_scan_locked(self.id)

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
        >>> from plantdb.commons.test_database import dummy_db
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
        >>> from plantdb.commons.test_database import dummy_db
        >>> db = dummy_db(with_fileset=True)
        >>> scan = db.get_scan('myscan_001')
        >>> scan.get_filesets()
        [<plantdb.commons.fsdb.core.Fileset at *x************>]
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        return [self.get_fileset(fs.id) for fs in _filter_query(list(self.filesets.values()), query, fuzzy)]

    def get_fileset(self, fs_id):
        """Get a `Fileset` instance, of given `id`, in the current scan dataset.

        Parameters
        ----------
        fs_id : str
            The name of the fileset to get.

        Returns
        -------
        Fileset
            The retrieved or created fileset.

        Examples
        --------
        >>> from plantdb.commons.test_database import dummy_db
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
        if not self.fileset_exists(fs_id):
            raise FilesetNotFoundError(self, fs_id)

        return self.filesets[fs_id]

    @get_authentication
    @require_authentication
    @requires_permission(Permission.READ, check_scan_access=False)
    def get_metadata(self, key=None, default={}, current_user=None, **kwargs):
        """Get the metadata associated with a scan.

        Parameters
        ----------
        key : str
            A key that should exist in the scan's metadata.
        default : Any, optional
            The default value to return if the key does not exist in the metadata.
            Default is an empty dictionary``{}``.

        Returns
        -------
        any
            If `key` is ``None``, returns a dictionary.
            Else, returns the value attached to this key.
        """
        # Use shared lock for read operations
        with self.db.lock_manager.acquire_lock(self.id, LockType.SHARED, current_user.username, LockLevel.SCAN):
            return _get_metadata(self.metadata, key, default)

    @get_authentication
    @require_authentication
    @requires_permission(Permission.READ, check_scan_access=True)
    def get_measures(self, key=None, current_user=None, **kwargs):
        """Get the manual measurements associated with a scan.

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
        # Use shared lock for read operations
        with self.db.lock_manager.acquire_lock(self.id, LockType.SHARED, current_user.username, LockLevel.SCAN):
            return _get_metadata(self.measures, key, default={})

    @get_authentication
    @require_authentication
    @requires_permission(Permission.WRITE, check_scan_access=True)
    def set_metadata(self, data, value=None, current_user=None, **kwargs):
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
            If no user is authenticated.
            If the user lacks permission to modify the metadata.
        ValueError
            If metadata validation fails

        Examples
        --------
        >>> from plantdb.commons.test_database import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> scan = db.get_scan("myscan_001")
        >>> print(scan.get_metadata('test'))
        1
        >>> scan.set_metadata("test", "value")
        >>> print(scan.get_metadata('test'))
        value
        >>> # Changing scan ownership through metadata is blocked:
        >>> scan.set_metadata("owner", "guest")
        WARNING  [FSDB] Excluding 'owner' key from Scan metadata update!
        >>> # Read the scan metadata file
        >>> import json
        >>> from plantdb.commons.fsdb.path_helpers import _scan_metadata_path
        >>> p = _scan_metadata_path(scan)
        >>> print(p.exists())
        True
        >>> print(json.load(p.open(mode='r')))
        {'test': 'value'}
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        # The locking mechanism is handled by the `MetadataManager._update_metadata` method
        self._update_metadata(data, value, current_user, _store_scan_metadata, cls_name="Scan")

    @get_authentication
    @require_authentication
    @requires_permission(Permission.DELETE, check_scan_access=True)
    def change_owner(self, new_owner, current_user=None, **kwargs):
        """Change the owner of the scan.

        Examples
        --------
        >>> from plantdb.commons.test_database import dummy_db
        >>> db = dummy_db()
        >>> new_scan = db.create_scan('007', metadata={'project': 'GoldenEye'})  # create a new scan dataset
        >>> print(new_scan.get_metadata('owner'))
        admin
        >>> new_scan.change_owner('guest')
        >>> print(new_scan.get_metadata('owner'))
        guest
        >>> new_scan.change_owner('admin')
        """
        # Verify that the new owner exists:
        if not self.db.rbac_manager.users.exists(new_owner):
            self.logger.error(f"Owner '{new_owner}' does not exist, can not change ownership!")
            return

        # Use exclusive lock for metadata updates
        self.logger.debug(f"Updating '{self.id}' scan owner to '{new_owner}' user...")
        with self.db.lock_manager.acquire_lock(self.id, LockType.EXCLUSIVE, current_user.username, LockLevel.SCAN):
            # Update metadata
            _set_metadata(self.metadata, 'owner', new_owner)
            # Ensure modification timestamp
            _set_metadata(self.metadata, 'last_modified', iso_date_now())
            _store_scan_metadata(self)

        self.logger.debug(f"Done updating the scan owner.")
        return

    @get_authentication
    @require_authentication
    @requires_permission(Permission.DELETE, check_scan_access=True)
    def group_share(self, groups, current_user=None, **kwargs):
        """Change the group sharing of the scan.

        Examples
        --------
        >>> from plantdb.commons.test_database import dummy_db
        >>> db = dummy_db()
        >>> new_scan = db.create_scan('007', metadata={'project': 'GoldenEye'})  # create a new scan dataset
        >>> print(new_scan.get_metadata('owner'))
        admin
        >>> print(new_scan.get_metadata('sharing', default=None))
        None
        >>> new_group = db.create_group('dummy_group', ['admin', 'guest'], 'A test group')
        >>> new_scan.group_share('dummy_group')
        >>> print(new_scan.get_metadata('sharing'))
        ['dummy_group']
        """
        if isinstance(groups, str):
            groups = [groups]
        # Verify that the new group(s) exists:
        valid_groups = self.db.rbac_manager.valid_sharing_groups(groups)

        if len(valid_groups) == 0:
            self.logger.error(f"No valid group(s) given, can not share to non-existent group!")
            return

        # Validate sharing groups if present
        if 'sharing' in self.metadata:
            sharing_groups = self.metadata['sharing']
            if not self.db.rbac_manager.validate_sharing_groups(sharing_groups):
                raise ValueError("One or more sharing groups do not exist")

        # Use exclusive lock for metadata updates
        valid_groups_str = ", ".join(valid_groups)
        self.logger.debug(f"Updating '{self.id}' scan group sharing to: '{valid_groups_str}'")
        with self.db.lock_manager.acquire_lock(self.id, LockType.EXCLUSIVE, current_user.username, LockLevel.SCAN):
            # Update metadata
            _set_metadata(self.metadata, 'sharing', valid_groups)
            # Ensure modification timestamp
            _set_metadata(self.metadata, 'last_modified', iso_date_now())
            _store_scan_metadata(self)

        self.logger.debug(f"Done updating the scan group sharing.")
        return

    @get_authentication
    @require_authentication
    @requires_permission(Permission.WRITE, check_scan_access=True)
    def create_fileset(self, fs_id, metadata=None, current_user=None, **kwargs):
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
        PermissionError
            If no user is authenticated.
            If the user lacks permission to create a fileset.
        FilesetExistsError
            If the ``fs_id`` already exists in the local database.
        ValueError
            If the given ``fs_id`` is invalid.

        See Also
        --------
        plantdb.commons.fsdb.validation._is_valid_id
        plantdb.commons.fsdb.file_ops._make_fileset

        Examples
        --------
        >>> from plantdb.commons.test_database import dummy_db
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
        # Access scan metadata directly to avoid nested lock acquisition
        metadata = _get_metadata(self.metadata, None, {})

        # Verify if the given `fs_id` is valid
        if not _is_valid_id(fs_id):
            raise ValueError(f"Invalid fileset identifier '{fs_id}'!")
        # Verify if the given `fs_id` already exists in the local database
        if self.fileset_exists(fs_id):
            raise FilesetExistsError(self, fs_id)

        # Use fileset-level exclusive lock for fileset creation
        self.logger.debug(f"Creating a fileset '{fs_id}' in scan '{self.id}' as '{current_user.username}' user...")
        with self.db.lock_manager.acquire_lock(f"{self.id}/{fs_id}", LockType.EXCLUSIVE, current_user.username, LockLevel.FILESET):
            # Create the new Fileset
            fileset = Fileset(self, fs_id)  # Initialize a new Fileset instance
            _make_fileset(fileset)  # Create directory structure

            # Set initial metadata
            initial_metadata = metadata or {}
            now = iso_date_now()
            initial_metadata['created'] = now  # creation timestamp
            initial_metadata['last_modified'] = now  # modification timestamp
            initial_metadata['created_by'] = current_user.fullname

            # Cannot use fileset.set_metadata(initial_metadata) here as ownership is not granted yet!
            _set_metadata(fileset.metadata, initial_metadata, None)  # add metadata dictionary to the new fileset
            _store_fileset_metadata(fileset)

            self.filesets.update({fs_id: fileset})  # Update scan's filesets dictionary
            self.store()  # Store fileset instance to the JSON

        self.logger.debug(f"Done creating the fileset.")
        return fileset

    @get_authentication
    @require_authentication
    @requires_permission(Permission.DELETE, check_scan_access=True)
    def delete_fileset(self, fs_id, current_user=None, **kwargs) -> None:
        """Delete a given fileset from the scan dataset.

        Parameters
        ----------
        fs_id : str
            Name of the fileset to delete.

        Raises
        ------
        PermissionError
            If no user is authenticated.
            If the user does not have permission to delete this fileset.
        ValueError
            If the fileset does not exist.

        See Also
        --------
        plantdb.commons.fsdb.file_ops._delete_fileset

        Examples
        --------
        >>> from plantdb.commons.test_database import dummy_db
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
            raise ValueError(f"Fileset '{fs_id}' does not exist in scan '{self.id}'")

        # Use exclusive lock for fileset deletion
        self.logger.debug(f"Deleting fileset '{fs_id}' from scan '{self.id}' as '{current_user.username}' user...")
        with self.db.lock_manager.acquire_lock(f"{self.id}/{fs_id}", LockType.EXCLUSIVE, current_user.username, LockLevel.FILESET):
            fs = self.filesets[fs_id]
            _delete_fileset(fs)  # delete the fileset
            self.filesets.pop(fs_id)  # remove the Fileset instance from the scan
            self.store()  # save the changes to the scan main JSON FILE (``files.json``)

        self.logger.debug(f"Done deleting fileset.")
        return

    def store(self):
        """Save changes to the scan main JSON FILE (``files.json``)."""
        _store_scan(self)
        return

    def path(self) -> pathlib.Path:
        """Get the path to the local scan dataset.

        Examples
        --------
        >>> from plantdb.commons.test_database import dummy_db
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
        >>> from plantdb.commons.test_database import dummy_db
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


class Fileset(db.Fileset, MetadataManager):
    """Implement ``Fileset`` for the local *File System DataBase* from the abstract class ``db.Fileset``.

    Implementation of a fileset as a simple files structure with:
      * directory ``${FSDB.basedir}/${FSDB.scan.id}/${Fileset.id}`` containing a set of files;
      * directory ``${FSDB.basedir}/${FSDB.scan.id}/metadata`` containing JSON metadata associated with files;
      * JSON file ``files.json`` containing the list of files from fileset;

    Attributes
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        A local database instance hosting the ``Scan`` instance.
    scan : plantdb.commons.fsdb.core.Scan
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
        scan : plantdb.commons.fsdb.core.Scan
            A scan instance containing the fileset.
        fs_id : str
            The identifier of the fileset instance.
        """
        super().__init__(scan, fs_id)
        # Defines attributes:
        self.metadata = {}
        self.files = {}

        self.session_manager = self.db.session_manager
        self.logger = self.db.logger

    def _erase(self):
        """Erase the files and metadata associated with this fileset."""
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
        >>> from plantdb.commons.test_database import dummy_db
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
        >>> from plantdb.commons.test_database import dummy_db
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
        """Get a `File` instance, of given `f_id`, in the current fileset.

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
        >>> from plantdb.commons.test_database import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> scan = db.get_scan("myscan_001")
        >>> fs = scan.get_fileset("fileset_001")
        >>> f = fs.get_file("test_image")
        >>> # To read the file you need to load the right reader from plantdb.commons.io
        >>> from plantdb.commons.io import read_image
        >>> img = read_image(f)
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        if not self.file_exists(f_id):
            raise FileNotFoundError(self, f_id)

        return self.files[f_id]

    @get_authentication
    @require_authentication
    @requires_permission(Permission.READ, check_scan_access=True)
    def get_metadata(self, key=None, default={}, current_user=None, **kwargs):
        """Get the metadata associated with a fileset.

        Parameters
        ----------
        key : str
            A key that should exist in the fileset's metadata.
        default : Any, optional
            The default value to return if the key does not exist in the metadata.
            Default is an empty dictionary``{}``.

        Returns
        -------
        any
            If `key` is ``None``, returns a dictionary.
            Else, returns the value attached to this key.

        Examples
        --------
        >>> import json
        >>> from plantdb.commons.test_database import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> scan = db.get_scan("myscan_001")
        >>> fs = scan.get_fileset('fileset_001')
        >>> fs.set_metadata("test", "value")
        >>> print(fs.get_metadata("test"))
        'value'
        >>> db._is_dummy=False  # to avoid cleaning up the temporary dummy database
        >>> db.disconnect()
        >>> db.connect()
        >>> scan = db.get_scan("myscan_001")
        >>> fs = scan.get_fileset('fileset_001')
        >>> print(fs.get_metadata("test"))
        'value'
        >>> db._is_dummy=True
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        # Use shared lock for read operations
        with self.db.lock_manager.acquire_lock(f"{self.scan.id}/{self.id}", LockType.SHARED, current_user.username, LockLevel.FILESET):
            return _get_metadata(self.metadata, key, default)

    @get_authentication
    @require_authentication
    @requires_permission(Permission.WRITE, check_scan_access=True)
    def set_metadata(self, data, value=None, current_user=None, **kwargs):
        """Add a new metadata to the fileset.

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
            If no user is authenticated.
            If the user lacks permission to modify the metadata.

        Examples
        --------
        >>> import json
        >>> from plantdb.commons.test_database import dummy_db
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
        # The locking mechanism is handled by the `MetadataManager._update_metadata` method
        self._update_metadata(data, value, current_user, _store_fileset_metadata, cls_name="Fileset")

    @get_authentication
    @require_authentication
    @requires_permission(Permission.WRITE, check_scan_access=True)
    def create_file(self, f_id, metadata=None, current_user=None, **kwargs):
        """Create a new `File` instance in the local database attached to the current `Fileset` instance.

        Parameters
        ----------
        f_id : str
            The name of the file to create.

        Returns
        -------
        plantdb.commons.fsdb.core.File
            The `File` instance created in the current `Fileset` instance.

        Raises
        ------
        PermissionError
            If no user is authenticated.
            If the user lacks permission to create a fileset.
        FileExistsError
            If the ``f_id`` already exists in the local database.
        ValueError
            If the given ``f_id`` is invalid.

        See Also
        --------
        plantdb.commons.fsdb.validation._is_valid_id

        Examples
        --------
        >>> from plantdb.commons.test_database import dummy_db
        >>> db = dummy_db(with_file=True)
        >>> scan = db.get_scan('myscan_001')
        >>> fs = scan.get_fileset('fileset_001')
        >>> fs.list_files()
        ['dummy_image', 'test_image', 'test_json']
        >>> new_f = fs.create_file('file_007')
        >>> fs.list_files()
        ['dummy_image', 'test_image', 'test_json', 'file_007']
        >>> print([f.name for f in fs.path().iterdir()])  # the file only exists in the database, not on drive!
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
            raise ValueError(f"Invalid file identifier '{f_id}'!")
        # Verify if the given `f_id` already exists in the local database
        if self.file_exists(f_id):
            raise FileExistsError(self, f_id)

        # Use file-level exclusive lock for file creation
        self.logger.debug(f"Creating a file '{f_id}' in '{self.scan.id}/{self.id}' as '{current_user.username}' user...")
        with self.db.lock_manager.acquire_lock(f"{self.scan.id}/{self.id}/{f_id}", LockType.EXCLUSIVE, current_user.username, LockLevel.FILE):
            # Create the new File
            file = File(self, f_id)  # Initialize a new File instance

            # Set initial metadata
            initial_metadata = metadata or {}
            now = iso_date_now()
            initial_metadata['created'] = now  # creation timestamp
            initial_metadata['last_modified'] = now  # modification timestamp
            initial_metadata['created_by'] = current_user.fullname

            # Cannot use fileset.set_metadata(initial_metadata) here as ownership is not granted yet!
            _set_metadata(file.metadata, initial_metadata, None)  # add metadata dictionary to the new scan
            _store_file_metadata(file)

            self.files.update({f_id: file})  # Update filesets's file dictionary
            self.store()  # Store fileset instance to the JSON

        self.logger.debug(f"Done creating the file.")
        return file

    @get_authentication
    @require_authentication
    @requires_permission(Permission.DELETE, check_scan_access=True)
    def delete_file(self, f_id, current_user=None, **kwargs):
        """Delete a given file from the current fileset.

        Parameters
        ----------
        f_id : str
            Name of the file to delete.

        Raises
        ------
        PermissionError
            If no user is authenticated.
            If the user does not have permission to delete this file.
        ValueError
            If the file does not exist.

        See Also
        --------
        plantdb.commons.fsdb.file_ops._delete_file

        Examples
        --------
        >>> from plantdb.commons.test_database import dummy_db
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
            raise ValueError(f"File '{f_id}' does not exist in '{self.scan.id}/{self.id}'")

        # Use exclusive lock for fileset creation
        self.logger.debug(f"Deleting file '{f_id}' from '{self.scan.id}/{self.id}' as '{current_user.username}' user...")
        with self.db.lock_manager.acquire_lock(f"{self.scan.id}/{self.id}/{f_id}", LockType.EXCLUSIVE, current_user.username, LockLevel.FILE):
            f = self.files[f_id]
            _delete_file(f)  # delete the file
            self.files.pop(f_id)  # remove the File instance from the fileset
            self.store()  # save the changes to the scan main JSON FILE (``files.json``)

        self.logger.debug(f"Done deleting file.")
        return

    def store(self):
        """Save changes to the scan main JSON FILE (``files.json``)."""
        self.scan.store()
        return

    def path(self) -> pathlib.Path:
        """Get the path to the local fileset.

        Examples
        --------
        >>> from plantdb.commons.test_database import dummy_db
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
        >>> from plantdb.commons.test_database import dummy_db
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


class File(db.File, MetadataManager):
    """Implement ``File`` for the local *File System DataBase* from abstract class ``db.File``.

    Attributes
    ----------
    db : plantdb.commons.fsdb.core.FSDB
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
    `File` must be written using ``write_raw`` or ``write`` methods to exist on disk.
    Else they are just referenced in the database!

    Contrary to other classes (``Scan`` & ``Fileset``) the uniqueness is not checked!
    """

    def __init__(self, fileset, f_id, **kwargs):
        super().__init__(fileset, f_id, **kwargs)
        self.metadata = {}

        self.session_manager = self.db.session_manager
        self.logger = self.db.logger

    def _erase(self):
        self.id = None
        self.metadata = {}
        return

    @get_authentication
    @require_authentication
    @requires_permission(Permission.READ, check_scan_access=True)
    def get_metadata(self, key=None, default={}, current_user=None, **kwargs):
        """Get the metadata associated with a file.

        Parameters
        ----------
        key : str
            A key that should exist in the file's metadata.
        default : Any, optional
            The default value to return if the key does not exist in the metadata.
            Default is an empty dictionary``{}``.

        Returns
        -------
        any
            If `key` is ``None``, returns a dictionary.
            Else, returns the value attached to this key.

        Examples
        --------
        >>> from plantdb.commons.test_database import dummy_db
        >>> from plantdb.commons.fsdb.path_helpers import _file_metadata_path
        >>> db = dummy_db(with_file=True)
        >>> scan = db.get_scan("myscan_001")
        >>> fs = scan.get_fileset('fileset_001')
        >>> f = fs.get_file("test_json")
        >>> print(f.get_metadata())
        {'random json': True}
        >>> db.disconnect()  # clean up (delete) the temporary dummy database
        """
        # Use shared lock for read operations
        with self.db.lock_manager.acquire_lock(f"{self.scan.id}/{self.fileset.id}/{self.id}", LockType.SHARED, current_user.username, LockLevel.FILE):
            return _get_metadata(self.metadata, key, default)

    @get_authentication
    @require_authentication
    @requires_permission(Permission.WRITE, check_scan_access=True)
    def set_metadata(self, data, value=None, current_user=None, **kwargs):
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
        >>> from plantdb.commons.test_database import dummy_db
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
        # The locking mechanism is handled by the `MetadataManager._update_metadata` method
        self._update_metadata(data, value, current_user, _store_file_metadata, cls_name="File")

    @get_authentication
    @require_authentication
    @requires_permission(Permission.WRITE, check_scan_access=True)
    def import_file(self, path, current_user=None, **kwargs):
        """Import the file from its local path to the current fileset.

        Parameters
        ----------
        path : str or pathlib.Path
            The path to the file to import.

        Examples
        --------
        >>> from plantdb.commons.test_database import dummy_db
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
        # Check if the path is a file
        if isinstance(path, str):
            path = Path(path)
        if not os.path.isfile(path):
            raise ValueError(f"The provided path is not a file: {path}.")

        # Use exclusive lock for this operation
        self.logger.debug(
            f"Importing file '{self.id}' in '{self.scan.id}/{self.fileset.id}' as user '{current_user.username}'...")
        with self.db.lock_manager.acquire_lock(f"{self.scan.id}/{self.fileset.id}/{self.id}", LockType.EXCLUSIVE, current_user.username, LockLevel.FILE):
            # Get the file name and extension
            ext = path.suffix[1:]
            self.filename = _get_filename(self, ext)
            # Get the path to the new `File` instance
            newpath = _file_path(self)
            # Copy the file to its new destination
            copyfile(path, newpath)
            self.store()  # register it to the scan main JSON FILE

        self.logger.debug(f"Done importing file.")
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
        >>> from plantdb.commons.test_database import dummy_db
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

    @get_authentication
    @require_authentication
    @requires_permission(Permission.WRITE, check_scan_access=True)
    def write_raw(self, data, ext="", current_user=None, **kwargs):
        """Write a file from raw byte data.

        Parameters
        ----------
        data : bytes
            The raw byte content to write.
        ext : str, optional
            The extension to use to save the file.

        Examples
        --------
        >>> from plantdb.commons.test_database import dummy_db
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
        # Use file-level exclusive lock for this operation
        self.logger.debug(
            f"Writing raw file '{self.id}' in '{self.scan.id}/{self.fileset.id}' as user '{current_user.username}'...")
        with self.db.lock_manager.acquire_lock(f"{self.scan.id}/{self.fileset.id}/{self.id}", LockType.EXCLUSIVE, current_user.username, LockLevel.FILE):
            self.filename = _get_filename(self, ext)
            path = _file_path(self)
            with path.open(mode="wb") as f:
                f.write(data)
            self.store()

        self.logger.debug(f"Done writing raw file.")
        return

    def read(self):
        """Read the file and return its contents.

        Returns
        -------
        str
            The contents of the file.

        Examples
        --------
        >>> from plantdb.commons.test_database import dummy_db
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

    @get_authentication
    @require_authentication
    @requires_permission(Permission.WRITE, check_scan_access=True)
    def write(self, data, ext="", current_user=None, **kwargs):
        """Write a file from data.

        Parameters
        ----------
        data : str
            A string representation of the content to write.
        ext : str, optional
            The extension to use to save the file.

        Examples
        --------
        >>> from plantdb.commons.test_database import dummy_db
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
        # Use exclusive lock for this operation
        self.logger.debug(
            f"Writing file '{self.id}' in '{self.scan.id}/{self.fileset.id}' as user '{current_user.username}'...")
        with self.db.lock_manager.acquire_lock(f"{self.scan.id}/{self.fileset.id}/{self.id}", LockType.EXCLUSIVE, current_user.username, LockLevel.FILE):
            self.filename = _get_filename(self, ext)
            path = _file_path(self)
            with path.open(mode="w") as f:
                f.write(data)
            self.store()

        self.logger.debug(f"Done writing file.")
        return

    def path(self) -> pathlib.Path:
        """Get the path to the local file.

        Examples
        --------
        >>> from plantdb.commons.test_database import dummy_db
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
    >>> from plantdb.commons.test_database import dummy_db
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
                query_test = partial_match(query[q], obj.get_metadata(q), fuzzy=fuzzy)
                if debug:
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
