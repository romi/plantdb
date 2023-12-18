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
This module implement a database as a **file structure on a remote server using SSHFS**.
"""

import subprocess

from plantdb import db
from plantdb import fsdb
from plantdb.log import configure_logger

MARKER_FILE_NAME = "romidb"  # This file must exist in the root of a folder for it to be considered a valid DB
LOCK_FILE_NAME = "lock"  # This file prevents opening the DB if it is present in the root folder of a DB

logger = configure_logger(__name__)


class SSHFSDB(fsdb.FSDB):
    """Subclass of FSDB that first mounts a remote directory using SSHFS.

    Implementation of a database on a remote file system.

    Attributes
    ----------
    basedir : str
        Path to the local directory where to mount the remote directory.
    remotedir : str, optional
        Path to the remote directory containing the database.
        Should be in the format ``user@server:path``
    scans : list
        The list of ``Scan`` objects found in the database
    is_connected : bool
        ``True`` if the DB is connected (locked the directory), else ``False``

    """

    def __init__(self, basedir, remotedir=None):
        """Database constructor.

        Mount ``remotedir`` directory on the ``basedir`` directory and load accessible ``Scan`` objects.

        Parameters
        ----------
        basedir : str
            Path to local directory of the database
        remotedir : str, optional
            Path to the remote directory containing the database.
            Should be in the format ``user@server:path``

        Examples
        --------
        >>> # EXAMPLE 1: Use a temporary dummy database:
        >>> from plantdb import SSHFSDB
        >>> db = SSHFSDB("db", "someone@example.com:/data")
        >>> print(db.basedir)
        db
        >>> print(db.remotedir)
        someone@example.com:/data
        >>> # Now connecting to this remote DB...
        >>> db.connect()
        >>> # ...allows to create new `Scan` in it:
        >>> new_scan = db.create_scan("007")
        >>> print(type(new_scan))
        <class 'plantdb.fsdb.Scan'>
        >>> db.disconnect()
        """
        super().__init__(basedir)
        self.remotedir = remotedir

    def connect(self, login_data=None):
        """Connect to the remote database.

        Handle DB "locking" system by adding a ``LOCK_FILE_NAME`` file in the DB.

        Parameters
        ----------
        login_data : bool
            UNUSED

        Examples
        --------
        >>> from plantdb import SSHFSDB
        >>> db = SSHFSDB("db", "someone@example.com:/data")
        >>> print(db.is_connected)
        False
        >>> db.connect()
        >>> print(db.is_connected)
        True

        """
        if not self.path().is_dir():
            self.path().mkdir(exist_ok=True)
        if self.remotedir is not None:
            cmd = ["sshfs", "-o", "idmap=user", self.remotedir, self.path()]
            logger.info(f"Connecting with: '{cmd}'")
            p = subprocess.run(cmd)
            logger.debug(f"The exit code was: {p.returncode}")
        super().connect()

    def disconnect(self):
        """Disconnect from the database.

        Handle DB "locking" system by removing the ``LOCK_FILE_NAME`` file from the DB.

        Examples
        --------
        >>> from plantdb import SSHFSDB
        >>> db = SSHFSDB("db", "someone@example.com:/data")
        >>> print(db.is_connected)
        False
        >>> db.connect()
        >>> print(db.is_connected)
        True
        >>> db.disconnect()
        >>> print(db.is_connected)
        False

        """
        super().disconnect()
        p = subprocess.run(["fusermount", "-u", self.path()])
        print(f"The exit code was: {p.returncode}")
