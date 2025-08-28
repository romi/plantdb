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

"""Synchronization Utility for PlantDB Databases

This module provides a robust synchronization mechanism for File System Databases (FSDB) in the PlantDB project, enabling seamless data transfer between local and remote database instances.

## Key Features

- Support for local and remote database synchronization
- Intelligent file transfer with modification time and size checking
- Automatic locking and unlocking of source and target databases
- Recursive directory synchronization
- SSH-based remote file transfer using SFTP
- Error handling and validation of database paths

## Usage Examples

Create two test databases, a source with a dataset and a target without dataset, then sync them.
```python
>>> from plantdb.commons.sync import FSDBSync
>>> from plantdb.commons.test_database import test_database
>>> # Create a test source database
>>> db_source = test_database()
>>> db_source.connect()
>>> print(db_source.list_scans())  # list scans in the source database
['real_plant_analyzed']
>>> db_source._unlock_db()  # Unlock the database (remove lock file)
>>> # Create a test target database
>>> db_target = test_database(dataset=None)
>>> db_target.connect()
>>> print(db_target.list_scans())  # verify that target database is empty
[]
>>> db_target._unlock_db()  # Unlock the database (remove lock file)
>>> # Sync target database with source
>>> db_sync = FSDBSync(db_source.path(), db_target.path())
>>> db_sync.sync()
>>> # List directories in the target database
>>> print([i for i in db_target.path().iterdir()])
>>> # Reload target database to ensure that the new scans are available
>>> db_target.reload()
>>> print(db_target.list_scans())  # verify that target database contains 1 new scan
['real_plant_analyzed']
>>> db_source.disconnect()  # Remove the test database
>>> db_target.disconnect()  # Remove the test database
```
"""


import os
import shutil
import stat
from pathlib import Path

import paramiko

from plantdb.commons.fsdb.core import LOCK_FILE_NAME
from plantdb.commons.fsdb.core import MARKER_FILE_NAME
from plantdb.commons.fsdb.validation import _is_fsdb


class FSDBSync():
    """Class for sync between two FSDB databases.

    It checks for the validity of both source and target by checking that:

      * there is a marker file in the DB path root
      * the DB is not busy by checking for the lock file in the DB path root.

    It locks the two databases during the sync. The sync is done using rsync as a subprocess

    Attributes
    ----------
    source_str : str
        Source path
    target_str : str
        Target path
    source : dict
        Source path description
    target : dict
        Target path description
    ssh_clients : dict
        Dictionary of SSH clients, keyed by host name.

    Examples
    --------
    >>> from plantdb.commons.sync import FSDBSync
    >>> from plantdb.commons.test_database import test_database
    >>> # Example: Create two test databases, a source with a dataset and a target without dataset, then sync them.
    >>> # Create a test source database
    >>> db_source = test_database()
    >>> db_source.connect()
    >>> print(db_source.list_scans())  # list scans in the source database
    ['real_plant_analyzed']
    >>> db_source._unlock_db()  # Unlock the database (remove lock file)
    >>> # Create a test target database
    >>> db_target = test_database(dataset=None)
    >>> db_target.connect()
    >>> print(db_target.list_scans())  # verify that target database is empty
    []
    >>> db_target._unlock_db()  # Unlock the database (remove lock file)
    >>> # Sync target database with source
    >>> db_sync = FSDBSync(db_source.path(), db_target.path())
    >>> db_sync.sync()
    >>> # List directories in the target database
    >>> print([i for i in db_target.path().iterdir()])
    >>> # Reload target database to ensure that the new scans are available
    >>> db_target.reload()
    >>> print(db_target.list_scans())  # verify that target database contains 1 new scan
    ['real_plant_analyzed']
    >>> db_source.disconnect()  # Remove the test database
    >>> db_target.disconnect()  # Remove the test database
    """

    def __init__(self, source, target):
        """Class constructor.

        Parameters
        ----------
        source : str or pathlib.Path
            Source database path (remote or local)
        target : str or pathlib.Path
            Target database path (remote or local)
        ssh_clients : dict
            Dictionary of SSH clients, keyed by host name.
        """
        self.source_str = source
        self.target_str = target
        self.source = _fmt_path(source)
        self.target = _fmt_path(target)
        self.ssh_clients = {}  # Store SSH connections

    def __del__(self):
        """Ensure unlocking on object destruction."""
        try:
            self.unlock()
        except:
            return

    def lock(self):
        """Lock both source and target databases prior to sync."""
        for db in [self.source, self.target]:
            if db["type"] == "local":
                self._lock_local(db)
            else:
                self._lock_remote(db)

    def unlock(self):
        """Unlock both source and target databases after sync."""
        for db in [self.source, self.target]:
            if db["type"] == "local":
                self._unlock_local(db)
            else:
                self._unlock_remote(db)

    def _lock_local(self, db):
        """Create a local lock file."""
        lock_path = db["lock_path"]
        try:
            # Use 'x' mode for exclusive creation
            with lock_path.open(mode="x") as _:
                pass
        except FileExistsError:
            raise IOError(f"Could not secure lock, {lock_path.name} is present in DB path.")

    def _lock_remote(self, db):
        """Create a remote lock file using SFTP."""
        ssh = self._get_ssh_client(db["host"])
        sftp = ssh.open_sftp()

        try:
            # Check for marker file
            try:
                sftp.stat(str(db["marker_path"]))
            except FileNotFoundError:
                raise OSError(f"Not a valid DB path, missing {db['marker_path'].name}")

            # Try to create lock file
            try:
                with sftp.file(str(db["lock_path"]), mode='x') as f:
                    f.write(str(os.getpid()))
            except IOError:
                raise IOError(f"Could not secure lock, {db['lock_path'].name} is present in DB path.")
        finally:
            sftp.close()

    def _unlock_local(self, db):
        """Remove local lock file."""
        try:
            db["lock_path"].unlink()
        except FileNotFoundError:
            pass  # Ignore if file doesn't exist

    def _unlock_remote(self, db):
        """Remove remote lock file using SFTP."""
        ssh = self._get_ssh_client(db["host"])
        sftp = ssh.open_sftp()

        try:
            try:
                sftp.remove(str(db["lock_path"]))
            except FileNotFoundError:
                pass  # Ignore if file doesn't exist
        finally:
            sftp.close()

    def sync(self):
        """Sync the two DBs using modern Python approaches with proper locking."""
        self.lock()
        try:
            if self.source["type"] == "local" and self.target["type"] == "local":
                self._sync_local()
            else:
                self._sync_remote()
        finally:
            self.unlock()
            self._close_ssh_connections()

    def _sync_local(self):
        """Synchronize local directories using shutil."""
        src_path = self.source["path"]
        dst_path = self.target["path"]

        # Create destination if it doesn't exist
        dst_path.mkdir(parents=True, exist_ok=True)

        # Walk through source directory
        for src_dir, dirs, files in os.walk(src_path):
            # Get relative path
            rel_path = Path(src_dir).relative_to(src_path)
            dst_dir = dst_path / rel_path

            # Create destination directory
            dst_dir.mkdir(exist_ok=True)

            # Copy files
            for file in files:
                src_file = Path(src_dir) / file
                dst_file = dst_dir / file

                # Check if file needs to be updated
                if self._should_update_file(src_file, dst_file):
                    shutil.copy2(src_file, dst_file)

    def _sync_remote(self):
        """Synchronize with remote system using SFTP."""
        if self.source["type"] == "remote":
            remote = self.source
            local = self.target["path"]
            download = True
        else:
            remote = self.target
            local = self.source["path"]
            download = False

        # Setup SFTP client
        ssh = self._get_ssh_client(remote["host"])
        sftp = ssh.open_sftp()

        try:
            if download:
                self._download_recursive(sftp, str(remote["path"]), local)
            else:
                self._upload_recursive(sftp, local, str(remote["path"]))
        finally:
            sftp.close()

    def _should_update_file(self, src_file, dst_file):
        """Check if file needs to be updated based on mtime and size."""
        if not dst_file.exists():
            return True

        src_stat = src_file.stat()
        dst_stat = dst_file.stat()

        # Compare modification times and sizes
        return (src_stat.st_mtime > dst_stat.st_mtime or
                src_stat.st_size != dst_stat.st_size)

    def _download_recursive(self, sftp, remote_dir, local_dir):
        """Download directory recursively using SFTP."""
        local_dir = Path(local_dir)
        local_dir.mkdir(parents=True, exist_ok=True)

        for entry in sftp.listdir_attr(remote_dir):
            remote_path = f"{remote_dir}/{entry.filename}"
            local_path = local_dir / entry.filename

            if stat.S_ISDIR(entry.st_mode):
                self._download_recursive(sftp, remote_path, local_path)
            else:
                # Check if we need to download
                if (not local_path.exists() or
                        entry.st_mtime > local_path.stat().st_mtime):
                    sftp.get(remote_path, str(local_path))

    def _upload_recursive(self, sftp, local_dir, remote_dir):
        """Upload directory recursively using SFTP."""
        local_dir = Path(local_dir)

        # Ensure remote directory exists
        try:
            sftp.stat(remote_dir)
        except FileNotFoundError:
            sftp.mkdir(remote_dir)

        for item in local_dir.iterdir():
            remote_path = f"{remote_dir}/{item.name}"

            if item.is_dir():
                self._upload_recursive(sftp, item, remote_path)
            else:
                # Check if we need to upload
                try:
                    remote_stat = sftp.stat(remote_path)
                    local_stat = item.stat()
                    if local_stat.st_mtime > remote_stat.st_mtime:
                        sftp.put(str(item), remote_path)
                except FileNotFoundError:
                    sftp.put(str(item), remote_path)

    def _get_ssh_client(self, host):
        """Get or create SSH client for a host."""
        if host not in self.ssh_clients:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(hostname=host)
            self.ssh_clients[host] = client
        return self.ssh_clients[host]

    def _close_ssh_connections(self):
        """Close all SSH connections."""
        for client in self.ssh_clients.values():
            client.close()
        self.ssh_clients.clear()


def _fmt_path(path):
    """
    Format and validate a filesystem database (DB) path, determining local or remote type.

    This function preprocesses path inputs, performing syntax validation and
    generating a structured representation of the database path. It supports
    both local and remote filesystem paths with consistent output formatting.

    Parameters
    ----------
    path : str or pathlib.Path
        Path to a filesystem database resource. Supports two formats:
        - Local paths: Absolute or relative filesystem paths
        - Remote paths: Hostname and path separated by a colon (e.g., 'hostname:path')

    Returns
    -------
    dict
        A comprehensive dictionary describing the database path with keys:
        - "type": str, either "local" or "remote"
        - "path": pathlib.Path, resolved absolute path
        - "host": str, remote hostname, empty for local paths
        - "lock_path": pathlib.Path, path to the lock file
        - "marker_path": pathlib.Path, path to the marker file

    Raises
    ------
    OSError
        If the path is invalid or does not meet database structure requirements.
        - For remote paths: Incorrect hostname:path format
        - For local paths: Path is not a valid filesystem database

    Examples
    --------
    >>> from pathlib import Path
    >>> # Local path example
    >>> local_result = _fmt_path('/home/user/mydb')
    >>> local_result['type']
    'local'
    >>> local_result['path']  # Resolves to absolute path
    PosixPath('/home/user/mydb')

    >>> # Remote path example
    >>> remote_result = _fmt_path('server.example.com:/data/mydb')
    >>> remote_result['type']
    'remote'
    >>> remote_result['host']
    'server.example.com'

    Notes
    -----
    - The function uses `_is_fsdb()` to validate local database paths
    - Lock and marker file paths are automatically generated
    - Paths are resolved to absolute paths for consistency
    """

    if ':' in str(path):  # Remote path
        path_split = str(path).split(':')
        if len(path_split) != 2:
            raise OSError(f"Invalid remote path format: {path}")

        host = path_split[0]
        path = Path(path_split[1])

        return {
            "type": "remote",
            "host": host,
            "path": path,
            "lock_path": path / LOCK_FILE_NAME,
            "marker_path": path / MARKER_FILE_NAME,
        }
    else:  # Local path
        path = Path(path).resolve()
        if not _is_fsdb(path):
            raise OSError(f"Not a valid DB path: {path}")

        return {
            "type": "local",
            "host": '',
            "path": path,
            "lock_path": path / LOCK_FILE_NAME,
            "marker_path": path / MARKER_FILE_NAME,
        }
