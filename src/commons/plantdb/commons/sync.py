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
- HTTP(S) REST API synchronization with archive transfer
- Support for FSDB instances, local paths, and various remote protocols
- Error handling and validation of database paths

## Usage Examples

### Local Path to Local Path
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

### FSDB instance to Local Path
```python
>>> from plantdb.commons.sync import FSDBSync
>>> from plantdb.commons.test_database import test_database
>>> # Create a test source database
>>> db_source = test_database()
>>> # Create a test target database
>>> db_target = test_database(dataset=None)
>>> # FSDB instance to local path
>>> db_sync = FSDBSync(db_source, db_target.path())
>>> db_sync.sync()
>>> # Connect to the target and list scans
>>> db_target.connect()
>>> print(db_target.list_scans())  # verify that target now has the 'real_plant_analyzed' dataset
['real_plant_analyzed']
>>> db_target.disconnect()
>>> db_source.disconnect()
```

### Local path to HTTP REST API
```python
>>> from plantdb.commons.sync import FSDBSync
>>> from plantdb.server.test_rest_api import TestRestApiServer
>>> from plantdb.client.rest_api import list_scan_names
>>> from plantdb.commons.test_database import test_database
>>> # Create a test source database with all 5 test dataset
>>> db_source = test_database("all")
>>> # Create a test target database
>>> db_target = TestRestApiServer(test=True, empty=True)
>>> db_target.start()
Test REST API server started at http://127.0.0.1:5000
>>> server_cfg = db_target.get_server_config()
>>> # Use REST API to list scans and verify target DB is empty
>>> scans_list = list_scan_names(**server_cfg)
>>> print(scans_list)
[]
>>> # Asynchronous sync target database with source
>>> db_sync = FSDBSync(db_source.path(), db_target.get_base_url())
>>> thread = db_sync.sync(thread=True)  # Returns immediately
>>> print(f"Database synchronization in progress: {db_sync.is_synchronizing()}")
>>> # Monitor progress
>>> import time
>>> while db_sync.is_synchronizing():
...     progress = db_sync.get_sync_progress()
...     print(f"Progress: {progress:.1f}%")
...     time.sleep(0.3)
>>> # Wait for completion
>>> db_sync.wait_for_sync()
>>> # Check for errors
>>> if db_sync.get_sync_error():
...     print(f"Sync failed: {db_sync.get_sync_error()}")
>>> else:
...     print("Sync completed successfully")
>>> # Use REST API endpoint to refresh scans
>>> from plantdb.client.rest_api import refresh
>>> refresh(**server_cfg)
>>> # Use REST API to list scans and verify target DB contains the new scans
>>> scans_list = list_scan_names(**server_cfg)
>>> print(scans_list)
['arabidopsis000', 'real_plant', 'real_plant_analyzed', 'virtual_plant', 'virtual_plant_analyzed']
>>> db_target.stop()
```

### SSH to local with credentials
```python
>>> db_sync = FSDBSync("ssh://server.example.com:/data/sourcedb", "/local/target/db")
>>> db_sync.set_ssh_credentials("server.example.com", "username", "password")
>>> db_sync.sync()
```

"""

import getpass
import os
import shutil
import stat
import tempfile
import threading
import zipfile
from pathlib import Path
from urllib.parse import urlparse

import paramiko
import requests
import urllib3

from plantdb.client.rest_api import download_scan_archive
from plantdb.client.rest_api import upload_scan_archive
from plantdb.commons.fsdb.core import FSDB
from plantdb.commons.fsdb.core import LOCK_FILE_NAME
from plantdb.commons.fsdb.core import MARKER_FILE_NAME
from plantdb.commons.fsdb.validation import _is_fsdb


class FSDBSync():
    """Class for sync between two FSDB databases with support for multiple protocols.

    It supports synchronization between different types of database sources and targets:
    - FSDB instances or local paths: Uses file system operations
    - HTTP(S) URLs: Uses REST API with archive transfer
    - SSH URLs: Uses SFTP protocol

    It checks for the validity of both source and target and locks the databases during sync.

    Attributes
    ----------
    source_str : str or FSDB or pathlib.Path
        Source database specification
    target_str : str or FSDB or pathlib.Path
        Target database specification
    source : dict
        Source database description
    target : dict
        Target database description
    ssh_clients : dict
        Dictionary of SSH clients, keyed by host name.
    ssh_credentials : dict
        Dictionary of SSH credentials, keyed by host name.
    synchronizing : bool
        Flag indicating whether a sync operation is currently in progress.
    sync_progress : float
        Progress of the sync operation as a percentage.

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
        source : str or pathlib.Path or FSDB
            Source database specification:
            - FSDB instance or local path for file system databases
            - HTTP(S) URL for REST API access
            - SSH URL (ssh://server.example.com:/path/to/db) for SFTP access
        target : str or pathlib.Path or FSDB
            Target database specification (same format as source)
        """
        self.source_str = source
        self.target_str = target
        self.source = _parse_database_spec(source)
        self.target = _parse_database_spec(target)
        self.ssh_clients = {}  # Store SSH connections
        self.ssh_credentials = {}  # Store SSH credentials
        self.sync_progress = 0.
        self._sync_thread = None
        self._sync_error = None

    def __del__(self):
        """Ensure unlocking on object destruction."""
        try:
            self.unlock()
        except:
            return

    def set_ssh_credentials(self, host, username, password=None):
        """Set SSH credentials for a specific host.

        Parameters
        ----------
        host : str
            The hostname for SSH connection
        username : str
            SSH username
        password : str, optional
            SSH password. If not provided, will be prompted during connection.
        """
        self.ssh_credentials[host] = {
            'username': username,
            'password': password
        }

    def lock(self):
        """Lock both source and target databases prior to sync."""
        for db in [self.source, self.target]:
            if db["type"] == "fsdb":
                self._lock_fsdb(db)
            elif db["type"] == "local":
                self._lock_local(db)
            elif db["type"] == "ssh":
                self._lock_remote(db)
            # HTTP databases don't need locking

    def unlock(self):
        """Unlock both source and target databases after sync."""
        for db in [self.source, self.target]:
            if db["type"] == "fsdb":
                self._unlock_fsdb(db)
            elif db["type"] == "local":
                self._unlock_local(db)
            elif db["type"] == "ssh":
                self._unlock_remote(db)
            # HTTP databases don't need locking

    def _lock_fsdb(self, db):
        """Lock an FSDB instance."""
        fsdb = db["fsdb"]
        if not fsdb.is_connected:
            fsdb.connect()

    def _unlock_fsdb(self, db):
        """Unlock an FSDB instance."""
        fsdb = db["fsdb"]
        if fsdb.is_connected:
            fsdb.disconnect()

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
        """Remove the local lock file."""
        try:
            db["lock_path"].unlink()
        except FileNotFoundError:
            pass  # Ignore if a file doesn't exist

    def _unlock_remote(self, db):
        """Remove the remote lock file using SFTP."""
        ssh = self._get_ssh_client(db["host"])
        sftp = ssh.open_sftp()

        try:
            try:
                sftp.remove(str(db["lock_path"]))
            except FileNotFoundError:
                pass  # Ignore if a file doesn't exist
        finally:
            sftp.close()

    def sync(self, source_scans=None, thread=False):
        """Sync the two DBs using the appropriate strategy based on database types.

        Parameters
        ----------
        source_scans : list of str, optional
            List of scan IDs to synchronize. If `None`, synchronizes all scans.
        thread : bool, optional
            If `True`, run synchronization in a separate thread. Default is `False`.
            When running in a thread, use `is_synchronizing()` to check status
            and `get_sync_progress()` to track progress.

        Returns
        -------
        threading.Thread or None
            If `thread=True`, returns the `Thread` object. Otherwise, returns `None`.
        """
        if thread:
            if self.is_synchronizing():
                raise RuntimeError("Synchronization is already in progress")

            self._sync_thread = threading.Thread(target=self._sync_worker, args=(source_scans,))
            self._sync_thread.daemon = True
            self._sync_thread.start()
            return self._sync_thread
        else:
            self._sync_worker(source_scans)

    def _sync_worker(self, source_scans=None):
        """Internal worker method that performs the actual synchronization.

        Parameters
        ----------
        source_scans : list of str, optional
            List of scan IDs to synchronize. If `None`, synchronizes all scans.
        """
        self.lock()
        try:
            self._sync_error = None
            # Determine a sync strategy based on source and target types
            src_type = self.source["type"]
            tgt_type = self.target["type"]

            if src_type in ["fsdb", "local"] and tgt_type in ["fsdb", "local"]:
                self._sync_local_to_local(source_scans)
            elif src_type in ["fsdb", "local"] and tgt_type == "http":
                self._sync_local_to_http(source_scans)
            elif src_type == "http" and tgt_type in ["fsdb", "local"]:
                self._sync_http_to_local(source_scans)
            elif src_type in ["fsdb", "local"] and tgt_type == "ssh":
                self._sync_local_to_ssh(source_scans)
            elif src_type == "ssh" and tgt_type in ["fsdb", "local"]:
                self._sync_ssh_to_local(source_scans)
            elif src_type == "ssh" and tgt_type == "ssh":
                self._sync_ssh_to_ssh(source_scans)
            elif src_type == "http" and tgt_type == "http":
                self._sync_http_to_http(source_scans)
            elif src_type == "http" and tgt_type == "ssh":
                self._sync_http_to_ssh(source_scans)
            elif src_type == "ssh" and tgt_type == "http":
                self._sync_ssh_to_http(source_scans)
            else:
                raise NotImplementedError(f"Sync from {src_type} to {tgt_type} not implemented")
        except Exception as e:
            self._sync_error = e
            raise
        finally:
            self._sync_thread = None
            self.unlock()
            self._close_ssh_connections()

    def is_synchronizing(self):
        """Check if synchronization is currently in progress.

        Returns
        -------
        bool
            `True` if synchronization is in progress, `False` otherwise.
        """
        return self._sync_thread is not None

    def get_sync_progress(self):
        """Get the current synchronization progress.

        Returns
        -------
        float
            Progress as a percentage (0.0 to 100.0).
        """
        return self.sync_progress

    def get_sync_error(self):
        """Get any error that occurred during threaded synchronization.

        Returns
        -------
        Exception or None
            The exception that occurred during sync, or `None` if no error.
        """
        return self._sync_error

    def wait_for_sync(self, timeout=None):
        """Wait for the synchronization thread to complete.

        Parameters
        ----------
        timeout : float, optional
            Maximum time to wait in seconds. If `None`, wait indefinitely.

        Returns
        -------
        bool
            `True` if the thread is completed, `False` if timeout was reached.
        """
        if self._sync_thread is not None:
            self._sync_thread.join(timeout)
            return not self._sync_thread.is_alive()
        return True

    def _sync_local_to_local(self, source_scans=None):
        """Synchronize between local databases using shutil.

        Parameters
        ----------
        source_scans : list of str, optional
            List of scan IDs to synchronize. If None, synchronizes all scans.
        """
        src_path = self._get_local_path(self.source)
        dst_path = self._get_local_path(self.target)

        # Create a destination directory if it doesn't exist
        dst_path.mkdir(parents=True, exist_ok=True)

        # Get the list of scans to sync
        available_scans = self._list_local_scans(src_path)
        if source_scans is None:
            scans_to_sync = available_scans
        else:
            # Filter to only include scans that exist in source and are requested
            scans_to_sync = [scan for scan in source_scans if scan in available_scans]

        n_scans_to_sync = len(scans_to_sync)
        self.sync_progress = 0.
        for n, scan_id in enumerate(scans_to_sync):
            self.sync_progress = (n + 1) / float(n_scans_to_sync) * 100
            src_scan_path = src_path / scan_id
            dst_scan_path = dst_path / scan_id

            # Sync scan directory
            self._sync_directory_local(src_scan_path, dst_scan_path)

    def _sync_local_to_http(self, source_scans=None):
        """Sync from a local database to HTTP REST API.

        Parameters
        ----------
        source_scans : list of str, optional
            List of scan IDs to synchronize. If None, synchronizes all scans.
        """
        src_path = self._get_local_path(self.source)
        target_url = self.target["url"]

        # Get the list of scans to sync
        available_scans = self._list_local_scans(src_path)
        if source_scans is None:
            scans_to_sync = available_scans
        else:
            # Filter to only include scans that exist in source and are requested
            scans_to_sync = [scan for scan in source_scans if scan in available_scans]

        n_scans_to_sync = len(scans_to_sync)
        self.sync_progress = 0.
        for n, scan_id in enumerate(scans_to_sync):
            self.sync_progress = (n + 1) / float(n_scans_to_sync) * 100
            src_scan_path = src_path / scan_id

            # Create an archive of scan
            with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as tmp_file:
                archive_path = tmp_file.name

            try:
                self._create_scan_archive(src_scan_path, archive_path)
                # Upload via REST API
                response = upload_scan_archive(scan_id, archive_path, **config_from_url(target_url))
            finally:
                Path(archive_path).unlink(missing_ok=True)

    def _sync_http_to_local(self, source_scans=None):
        """Sync from HTTP REST API to a local database.

        Parameters
        ----------
        source_scans : list of str, optional
            List of scan IDs to synchronize. If None, synchronizes all scans.
        """
        src_url = self.source["url"]
        dst_path = self._get_local_path(self.target)

        # Create the destination directory if it doesn't exist
        dst_path.mkdir(parents=True, exist_ok=True)

        # Get the list of scans from REST API
        available_scans = self._list_http_scans(src_url)
        if source_scans is None:
            scans_to_sync = available_scans
        else:
            # Filter to only include scans that exist in source and are requested
            scans_to_sync = [scan for scan in source_scans if scan in available_scans]

        n_scans_to_sync = len(scans_to_sync)
        self.sync_progress = 0.
        for n, scan_id in enumerate(scans_to_sync):
            self.sync_progress = (n + 1) / float(n_scans_to_sync) * 100

            archive_path = Path(dst_path) / f"{scan_id}.zip"
            try:
                # Download archive
                response = download_scan_archive(scan_id, dst_path, **config_from_url(src_url))
                # Extract the downloaded archive
                self._extract_scan_archive(str(archive_path), dst_path / scan_id)
            finally:
                Path(archive_path).unlink(missing_ok=True)

    def _sync_local_to_ssh(self, source_scans=None):
        """Sync from local database to SSH server using SFTP.

        Parameters
        ----------
        source_scans : list of str, optional
            List of scan IDs to synchronize. If None, synchronizes all scans.
        """
        src_path = self._get_local_path(self.source)
        ssh = self._get_ssh_client(self.target["host"])
        sftp = ssh.open_sftp()

        try:
            # Ensure remote directory exists
            remote_path = str(self.target["path"])
            self._ensure_remote_directory(sftp, remote_path)

            # Get the list of scans to sync
            available_scans = self._list_local_scans(src_path)
            if source_scans is None:
                scans_to_sync = available_scans
            else:
                # Filter to only include scans that exist in source and are requested
                scans_to_sync = [scan for scan in source_scans if scan in available_scans]

            n_scans_to_sync = len(scans_to_sync)
            self.sync_progress = 0.
            for n, scan_id in enumerate(scans_to_sync):
                self.sync_progress = (n + 1) / float(n_scans_to_sync) * 100
                src_scan_path = src_path / scan_id
                remote_scan_path = f"{remote_path}/{scan_id}"

                # Upload scan directory
                self._upload_recursive(sftp, src_scan_path, remote_scan_path)
        finally:
            sftp.close()

    def _sync_ssh_to_local(self, source_scans=None):
        """Sync from SSH server to local database using SFTP.

        Parameters
        ----------
        source_scans : list of str, optional
            List of scan IDs to synchronize. If None, synchronizes all scans.
        """
        dst_path = self._get_local_path(self.target)
        ssh = self._get_ssh_client(self.source["host"])
        sftp = ssh.open_sftp()

        try:
            # Create the destination directory if it doesn't exist
            dst_path.mkdir(parents=True, exist_ok=True)

            # Get the list of scans from SSH server
            remote_path = str(self.source["path"])
            available_scans = self._list_ssh_scans(sftp, remote_path)
            if source_scans is None:
                scans_to_sync = available_scans
            else:
                # Filter to only include scans that exist in source and are requested
                scans_to_sync = [scan for scan in source_scans if scan in available_scans]

            n_scans_to_sync = len(scans_to_sync)
            self.sync_progress = 0.
            for n, scan_id in enumerate(scans_to_sync):
                self.sync_progress = (n + 1) / float(n_scans_to_sync) * 100
                remote_scan_path = f"{remote_path}/{scan_id}"
                dst_scan_path = dst_path / scan_id

                # Download scan directory
                self._download_recursive(sftp, remote_scan_path, dst_scan_path)
        finally:
            sftp.close()

    def _sync_ssh_to_ssh(self, source_scans=None):
        """Sync between two SSH servers via temporary local storage.

        Parameters
        ----------
        source_scans : list of str, optional
            List of scan IDs to synchronize. If None, synchronizes all scans.
        """
        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Download from source SSH to temp
            src_ssh = self._get_ssh_client(self.source["host"])
            src_sftp = src_ssh.open_sftp()

            try:
                remote_src_path = str(self.source["path"])
                available_scans = self._list_ssh_scans(src_sftp, remote_src_path)
                if source_scans is None:
                    scans_to_sync = available_scans
                else:
                    # Filter to only include scans that exist in source and are requested
                    scans_to_sync = [scan for scan in source_scans if scan in available_scans]

                n_scans_to_sync = len(scans_to_sync)
                self.sync_progress = 0.
                for n, scan_id in enumerate(scans_to_sync):
                    self.sync_progress = (n + 1) / float(n_scans_to_sync) * 100
                    remote_scan_path = f"{remote_src_path}/{scan_id}"
                    temp_scan_path = temp_path / scan_id

                    # Download to temp
                    self._download_recursive(src_sftp, remote_scan_path, temp_scan_path)
            finally:
                src_sftp.close()

            # Upload from temp to target SSH
            dst_ssh = self._get_ssh_client(self.target["host"])
            dst_sftp = dst_ssh.open_sftp()

            try:
                remote_dst_path = str(self.target["path"])
                self._ensure_remote_directory(dst_sftp, remote_dst_path)

                for scan_id in scans_to_sync:
                    temp_scan_path = temp_path / scan_id
                    remote_scan_path = f"{remote_dst_path}/{scan_id}"

                    if temp_scan_path.exists():
                        # Upload from temp
                        self._upload_recursive(dst_sftp, temp_scan_path, remote_scan_path)
            finally:
                dst_sftp.close()

    def _sync_http_to_http(self, source_scans=None):
        """Sync between two HTTP REST APIs via temporary local storage.

        Parameters
        ----------
        source_scans : list of str, optional
            List of scan IDs to synchronize. If None, synchronizes all scans.
        """
        src_url = self.source["url"]
        dst_url = self.target["url"]

        # Get the list of scans from source REST API
        available_scans = self._list_http_scans(src_url)
        if source_scans is None:
            scans_to_sync = available_scans
        else:
            # Filter to only include scans that exist in source and are requested
            scans_to_sync = [scan for scan in source_scans if scan in available_scans]

        n_scans_to_sync = len(scans_to_sync)
        self.sync_progress = 0.
        for n, scan_id in enumerate(scans_to_sync):
            self.sync_progress = (n + 1) / float(n_scans_to_sync) * 100
            dst_path = tempfile.gettempdir()
            archive_path = Path(dst_path) / f"{scan_id}.zip"
            try:
                # Download archive from source
                response = download_scan_archive(scan_id, dst_path, **config_from_url(src_url))
                # Upload archive to target
                response = upload_scan_archive(scan_id, archive_path, **config_from_url(dst_url))
            finally:
                Path(archive_path).unlink(missing_ok=True)

    def _sync_http_to_ssh(self, source_scans=None):
        """Sync from HTTP REST API to SSH server via temporary local storage.

        Parameters
        ----------
        source_scans : list of str, optional
            List of scan IDs to synchronize. If None, synchronizes all scans.
        """
        src_url = self.source["url"]
        ssh = self._get_ssh_client(self.target["host"])
        sftp = ssh.open_sftp()

        try:
            # Ensure remote directory exists
            remote_path = str(self.target["path"])
            self._ensure_remote_directory(sftp, remote_path)

            # Get the list of scans from REST API
            available_scans = self._list_http_scans(src_url)
            if source_scans is None:
                scans_to_sync = available_scans
            else:
                # Filter to only include scans that exist in source and are requested
                scans_to_sync = [scan for scan in source_scans if scan in available_scans]

            n_scans_to_sync = len(scans_to_sync)
            self.sync_progress = 0.
            for n, scan_id in enumerate(scans_to_sync):
                self.sync_progress = (n + 1) / float(n_scans_to_sync) * 100
                dst_path = Path(tempfile.gettempdir())
                archive_path = dst_path / f"{scan_id}.zip"
                # Download archive from source
                response = download_scan_archive(scan_id, dst_path, **config_from_url(src_url))

                # Extract to temp directory
                temp_scan_path = dst_path / scan_id
                self._extract_scan_archive(str(archive_path), temp_scan_path)

                # Upload to SSH server
                remote_scan_path = f"{remote_path}/{scan_id}"
                self._upload_recursive(sftp, temp_scan_path, remote_scan_path)
        finally:
            sftp.close()

    def _sync_ssh_to_http(self, source_scans=None):
        """Sync from SSH server to HTTP REST API via temporary local storage.

        Parameters
        ----------
        source_scans : list of str, optional
            List of scan IDs to synchronize. If None, synchronizes all scans.
        """
        dst_url = self.target["url"]
        ssh = self._get_ssh_client(self.source["host"])
        sftp = ssh.open_sftp()

        try:
            # Get the list of scans from the SSH server
            remote_path = str(self.source["path"])
            available_scans = self._list_ssh_scans(sftp, remote_path)
            if source_scans is None:
                scans_to_sync = available_scans
            else:
                # Filter to only include scans that exist in source and are requested
                scans_to_sync = [scan for scan in source_scans if scan in available_scans]

            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)

                n_scans_to_sync = len(scans_to_sync)
                self.sync_progress = 0.
                for n, scan_id in enumerate(scans_to_sync):
                    self.sync_progress = (n + 1) / float(n_scans_to_sync) * 100
                    # Download from SSH to temp
                    remote_scan_path = f"{remote_path}/{scan_id}"
                    temp_scan_path = temp_path / scan_id

                    self._download_recursive(sftp, remote_scan_path, temp_scan_path)

                    # Create archive
                    archive_path = temp_path / f"{scan_id}.zip"
                    self._create_scan_archive(temp_scan_path, str(archive_path))

                    # Upload archive via REST API
                    upload_scan_archive(scan_id, str(archive_path), **config_from_url(dst_url))
        finally:
            sftp.close()

    def _get_local_path(self, db):
        """Get the local path for a database."""
        if db["type"] == "fsdb":
            return db["fsdb"].path()
        else:
            return db["path"]

    def _list_local_scans(self, db_path):
        """List scan directories in a local database."""
        if db_path.exists():
            return [d.name for d in db_path.iterdir()
                    if d.is_dir() and not d.name.startswith('.')]
        return []

    def _list_http_scans(self, base_url):
        """List scans available via HTTP REST API."""
        try:
            response = requests.get(f"{base_url}/scans/")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error listing HTTP scans: {e}")
            return []

    def _list_ssh_scans(self, sftp, remote_path):
        """List scan directories on the SSH server."""
        try:
            entries = sftp.listdir_attr(remote_path)
            return [entry.filename for entry in entries
                    if stat.S_ISDIR(entry.st_mode) and not entry.filename.startswith('.')]
        except Exception as e:
            print(f"Error listing SSH scans: {e}")
            return []

    def _create_scan_archive(self, scan_path, archive_path):
        """Create a ZIP archive of a scan directory."""
        with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(scan_path):
                for file in files:
                    file_path = Path(root) / file
                    arc_path = file_path.relative_to(scan_path)
                    zipf.write(file_path, arc_path)

    def _extract_scan_archive(self, archive_path, extract_path):
        """Extract a ZIP archive to a directory."""
        extract_path.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(archive_path, 'r') as zipf:
            zipf.extractall(extract_path)

    def _upload_scan_archive(self, base_url, scan_id, archive_path):
        """Upload a scan archive via HTTP REST API."""
        try:
            with open(archive_path, 'rb') as f:
                files = {'archive': f}
                response = requests.post(f"{base_url}/scans/{scan_id}/upload", files=files)
                response.raise_for_status()
        except Exception as e:
            print(f"Error uploading scan archive: {e}")

    def _download_scan_archive(self, base_url, scan_id, archive_path):
        """Download a scan archive via HTTP REST API."""
        try:
            response = requests.get(f"{base_url}/scans/{scan_id}/download")
            response.raise_for_status()
            with open(archive_path, 'wb') as f:
                f.write(response.content)
        except Exception as e:
            print(f"Error downloading scan archive: {e}")

    def _ensure_remote_directory(self, sftp, remote_path):
        """Ensure a remote directory exists."""
        try:
            sftp.stat(remote_path)
        except FileNotFoundError:
            # Try to create directory recursively
            parent = str(Path(remote_path).parent)
            if parent != remote_path:
                self._ensure_remote_directory(sftp, parent)
            sftp.mkdir(remote_path)

    def _sync_directory_local(self, src_dir, dst_dir):
        """Synchronize a directory locally."""
        # Create the destination directory if it doesn't exist
        dst_dir.mkdir(parents=True, exist_ok=True)

        # Walk through the source directory
        for src_file in src_dir.rglob('*'):
            if src_file.is_file():
                rel_path = src_file.relative_to(src_dir)
                dst_file = dst_dir / rel_path

                # Ensure parent directory exists
                dst_file.parent.mkdir(parents=True, exist_ok=True)

                # Check if a file needs to be updated
                if self._should_update_file(src_file, dst_file):
                    shutil.copy2(src_file, dst_file)

    def _should_update_file(self, src_file, dst_file):
        """Check if a file needs to be updated based on mtime and size."""
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
        self._ensure_remote_directory(sftp, remote_dir)

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
        """Get or create an SSH client for a host."""
        if host not in self.ssh_clients:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            # Get credentials
            if host in self.ssh_credentials:
                creds = self.ssh_credentials[host]
                username = creds['username']
                password = creds['password']
            else:
                # Prompt for credentials
                username = input(f"Username for {host}: ")
                password = getpass.getpass(f"Password for {username}@{host}: ")

            client.connect(hostname=host, username=username, password=password)
            self.ssh_clients[host] = client
        return self.ssh_clients[host]

    def _close_ssh_connections(self):
        """Close all SSH connections."""
        for client in self.ssh_clients.values():
            client.close()
        self.ssh_clients.clear()


def config_from_url(url):
    """Parse URL into a configuration dictionary.

    Examples
    --------
    >>> from plantdb.commons.sync import config_from_url
    >>> config = config_from_url("http://localhost:5014/api/")
    >>> print(config)
    {'protocol': 'http', 'host': 'localhost', 'port': 5014, 'prefix': '/api/', 'ssl': False}
    """
    parsed_url = urllib3.util.parse_url(url)
    config = {}
    config["protocol"] = parsed_url.scheme.lower()
    config["host"] = parsed_url.host
    config["port"] = parsed_url.port
    config["prefix"] = parsed_url.path
    config["ssl"] = True if "https" in url else False
    return config


def _parse_database_spec(spec):
    """
    Parse and validate a database specification, determining the appropriate synchronization strategy.

    This function analyzes database specifications and returns a structured representation
    suitable for synchronization operations. It supports multiple database types including
    FSDB instances, local paths, HTTP(S) REST APIs, and SSH/SFTP connections.

    Parameters
    ----------
    spec : str, pathlib.Path, or FSDB
        Database specification in one of the following formats:
        - FSDB instance: A connected FSDB object
        - Local path: Absolute or relative filesystem paths
        - HTTP(S) URL: REST API endpoints (http://... or https://...)
        - SSH URL: SFTP connections (ssh://hostname:/path/to/db)

    Returns
    -------
    dict
        A comprehensive dictionary describing the database specification with keys:
        - "type": str, one of "fsdb", "local", "http", "ssh"
        - "path": pathlib.Path (for local/ssh types)
        - "fsdb": FSDB instance (for fsdb type)
        - "url": str (for http type)
        - "host": str (for ssh type)
        - "lock_path": pathlib.Path (for local/ssh types)
        - "marker_path": pathlib.Path (for local/ssh types)

    Raises
    ------
    OSError
        If the specification is invalid or does not meet requirements.
        - For local paths: Path is not a valid filesystem database
        - For SSH paths: Incorrect ssh://hostname:/path format
    ValueError
        If the specification type is not recognized or supported.

    Examples
    --------
    >>> from plantdb.commons.sync import _parse_database_spec
    >>> from plantdb.commons.fsdb import FSDB
    >>>
    >>> # FSDB instance
    >>> db = FSDB('/path/to/db')
    >>> result = _parse_database_spec(db)
    >>> result['type']
    'fsdb'
    >>>
    >>> # Local path
    >>> result = _parse_database_spec('/home/user/mydb')
    >>> result['type']
    'local'
    >>>
    >>> # HTTP REST API
    >>> result = _parse_database_spec('https://api.example.com/plantdb')
    >>> result['type']
    'http'
    >>>
    >>> # SSH/SFTP
    >>> result = _parse_database_spec('ssh://server.example.com:/data/mydb')
    >>> result['type']
    'ssh'
    >>> result['host']
    'server.example.com'

    Notes
    -----
    - FSDB instances are detected by checking for the FSDB class type
    - Local paths are validated using `_is_fsdb()` function
    - HTTP(S) URLs are identified by protocol prefixes
    - SSH URLs must follow the ssh://hostname:/path format
    - Lock and marker file paths are automatically generated for local and SSH types
    """
    # Check if it's an FSDB instance
    if hasattr(spec, '__class__') and spec.__class__.__name__ == 'FSDB':
        return {
            "type": "fsdb",
            "fsdb": spec,
            "path": spec.path(),
            "lock_path": spec.path() / LOCK_FILE_NAME,
            "marker_path": spec.path() / MARKER_FILE_NAME,
        }

    spec_str = str(spec)

    # Check for HTTP(S) URLs
    if spec_str.startswith(('http://', 'https://')):
        config = {
            "type": "http",
            "url": spec_str,
        }
        config.update(config_from_url(spec_str))
        return config

    # Check for SSH URLs
    if spec_str.startswith('ssh://'):
        # Parse ssh://hostname:/path/to/db format
        parsed = urlparse(spec_str)
        if not parsed.hostname or not parsed.path:
            raise OSError(f"Invalid SSH URL format: {spec_str}. Expected format: ssh://hostname:/path/to/db")

        host = parsed.hostname
        path = Path(parsed.path)

        return {
            "type": "ssh",
            "host": host,
            "path": path,
            "lock_path": path / LOCK_FILE_NAME,
            "marker_path": path / MARKER_FILE_NAME,
        }

    # Treat as local path
    path = Path(spec).resolve()
    if not _is_fsdb(path):
        raise OSError(f"Not a valid DB path: {path}")

    return {
        "type": "local",
        "host": '',
        "path": path,
        "lock_path": path / LOCK_FILE_NAME,
        "marker_path": path / MARKER_FILE_NAME,
    }
