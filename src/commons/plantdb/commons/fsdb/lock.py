#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
# LockManager

File-based locking for thread-safe resource management

The `LockManager` module provides a robust file-based locking mechanism to ensure thread-safe operations across multiple threads or processes.
It allows acquiring and releasing locks on resources identified by scan, fileset, or file IDs, with support for both shared (read) and exclusive (write) locks.

## Key Features

- Thread-safe lock acquisition and release using file-based locking mechanisms
- Support for both shared (multiple readers) and exclusive (single writer) locks
- Lock timeout to prevent indefinite waiting for lock acquisition
- Automatic cleanup of stale locks on initialization
- Monitoring and debugging capabilities with lock metadata storage
- Support for multiple lock levels: Scan, Fileset, and File

## Usage Examples

```python
from plantdb.commons.fsdb.lock import LockManager, LockType

# Initialize the lock manager with a base path
manager = LockManager('/path/to/database')

# Acquire an exclusive lock for a scan
with manager.acquire_lock('scan123', LockType.EXCLUSIVE, user='user1', level='scan'):
    # Perform critical section operations...

# Acquire a shared lock for a fileset
with manager.acquire_lock('scan123/fileset1', LockType.SHARED, user='user1', level='fileset'):
    # Perform critical section operations...

# Acquire an exclusive lock for a file
with manager.acquire_lock('scan123/fileset1/file1', LockType.EXCLUSIVE, user='user1', level='file'):
    # Perform critical section operations...
```
"""

import fcntl
import json
import os
import threading
import time
from contextlib import contextmanager
from enum import Enum
from typing import Dict
from typing import Optional

from plantdb.commons.log import get_logger


class LockLevel(Enum):
    """Enumeration of lock levels supported by the LockManager."""
    SCAN = "scan"          # Lock at scan level
    FILESET = "fileset"    # Lock at fileset level  
    FILE = "file"          # Lock at file level


class LockType(Enum):
    SHARED = "shared"      # Read operations
    EXCLUSIVE = "exclusive"  # Write operations


class LockError(Exception):
    """Raised when the lock acquisition fails."""

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)

    def __str__(self) -> str:
        return self.message


class LockTimeoutError(Exception):
    """Raised when the lock acquisition times out"""

    def __init__(self, message: str = "Error: Lock acquisition timed-out!"):
        self.message = message
        super().__init__(self.message)

    def __str__(self) -> str:
        return self.message


class LockManager:
    """Acquires and releases file-based locks for thread-safe resource management.

    This class provides functionality for acquiring and releasing file-based locks,
    ensuring thread-safe operations across multiple threads or processes. Locks are
    stored in a designated subdirectory within the specified base path, which is
    created if it does not exist. On initialization, stale lock files are cleaned up
    to maintain consistency.

    The class leverages thread locks to manage access to shared resources and uses
    file-based locking mechanisms to ensure exclusive access across processes.

    Attributes
    ----------
    base_path : str
        The base directory of the database.
    default_timeout : float
        The default timeout duration in seconds when attempting to acquire a lock.
        Default is 30.0 seconds.
    locks_dir: str
        Location of the directory where lock files are stored.
    _active_locks : Dict[str, Dict]
        Dictionary mapping lock names to dictionaries containing information about the lock including:

           - 'type': the type of the lock (shared or exclusive),
           - 'user': the user who acquired the lock (if available),
           - 'timestamp': the timestamp when the lock was acquired (if available),
           - 'count': int indicating how many locks are active for a given resource and lock type combination,
    _lock_files : Dict[str, int]
        The file descriptors (int) for each active lock in the dictionary mapping.
    _thread_lock: threading.RLock
        A thread-safe lock used to synchronize access to the active locks dictionary and file descriptors.

    Examples
    --------
    >>> from plantdb.commons.fsdb.lock import LockManager
    >>> manager = LockManager('/path/to/local/database')
    >>> lock = manager.acquire_lock('scan123', LockType.EXCLUSIVE, user='user1', level='scan')
    """

    def __init__(self, base_path: str, default_timeout: float = 30.0, **kwargs):
        """
        Lock manager constructor.

        Parameters
        ----------
        base_path : str or pathlib.Path
            The base directory of the database.
        default_timeout : float, optional
            The default timeout duration in seconds when attempting to acquire a lock.
            Default is 30.0 seconds.

        Other Parameters
        ----------------
        log_level : str, optional
            The log level (e.g., `DEBUG`, `INFO`). Default is `INFO`.
        """
        self.base_path = base_path
        self.default_timeout = default_timeout
        self.locks_dir = os.path.join(base_path, ".locks")
        self._active_locks: Dict[str, Dict] = {}  # Track active locks
        self._lock_files: Dict[str, int] = {}  # File descriptors for locks
        self._thread_lock = threading.RLock()  # Thread-safe operations

        # Store the last time we emitted a warning per resource
        self._warning_timestamps: Dict[str, float] = {}
        # How long we wait before emitting the same warning again (seconds)
        self._warning_debounce_interval: float = kwargs.get("warning_debounce_interval", 5.0)

        # Test write capability in base_path
        try:
            test_file = os.path.join(base_path, '.write_test.tmp')
            with open(test_file, 'w') as f:
                f.write('test')
            os.remove(test_file)
        except PermissionError as e:
            # Create a logger without the log_file as it will not be writable either
            logger = get_logger(__name__ + '.LockManager', log_level="DEBUG")
            logger.error(f"Cannot write to base_path `{base_path}`.")
            logger.debug(f"Current UID={os.getuid()}, GID={os.getgid()}")
            raise e

        # Ensure locks directory exists
        os.makedirs(self.locks_dir, exist_ok=True)
        # Initialize lock manager logger
        self.logger = get_logger(__name__ + '.LockManager',
                                 log_file=kwargs.get("log_file", os.path.join(base_path, '.locks', 'lock_manager.log')),
                                 log_level=kwargs.get("log_level", "INFO"))
        self.logger.debug(f"Initialized LockManager with base path: `{base_path}`")
        # Clean up stale locks on initialization
        self._cleanup_stale_locks()

    def _get_lock_file_path(self, resource_id: str, level: LockLevel) -> str:
        """
        Generates the file path for a lock file based on the provided resource ID and level.

        Parameters
        ----------
        resource_id : str
            The unique identifier for the resource (scan_id, scan_id/fileset_id, or scan_id/fileset_id/file_id).
        level : LockLevel
            The level of the lock (scan, fileset, or file).

        Returns
        -------
        str
            The full path to the lock file.
        """
        # For scan level, use scan_id.lock
        # For fileset level, use scan_id_fileset_id.lock  
        # For file level, use scan_id_fileset_id_file_id.lock
        
        if level == LockLevel.SCAN:
            return os.path.join(self.locks_dir, f"{resource_id}.lock")
        elif level == LockLevel.FILESET:
            # Split the resource_id to extract scan_id and fileset_id
            parts = resource_id.split('/')
            scan_id = parts[0]
            fileset_id = parts[1]
            return os.path.join(self.locks_dir, f"{scan_id}_{fileset_id}.lock")
        else:  # FILE level
            # Split the resource_id to extract scan_id, fileset_id, and file_id
            parts = resource_id.split('/')
            scan_id = parts[0]
            fileset_id = parts[1]
            file_id = parts[2]
            return os.path.join(self.locks_dir, f"{scan_id}_{fileset_id}_{file_id}.lock")

    def _get_lock_info_path(self, resource_id: str, level: LockLevel) -> str:
        """
        Get the path to the lock info file for a given resource ID and level.

        Parameters
        ----------
        resource_id : str
            The identifier of the resource.
        level : LockLevel
            The level of the lock (scan, fileset, or file).

        Returns
        -------
        str
            The absolute path to the lock info file.
        """
        # For scan level, use scan_id.info
        # For fileset level, use scan_id_fileset_id.info  
        # For file level, use scan_id_fileset_id_file_id.info
        
        if level == LockLevel.SCAN:
            return os.path.join(self.locks_dir, f"{resource_id}.info")
        elif level == LockLevel.FILESET:
            # Split the resource_id to extract scan_id and fileset_id
            parts = resource_id.split('/')
            scan_id = parts[0]
            fileset_id = parts[1]
            return os.path.join(self.locks_dir, f"{scan_id}_{fileset_id}.info")
        else:  # FILE level
            # Split the resource_id to extract scan_id, fileset_id, and file_id
            parts = resource_id.split('/')
            scan_id = parts[0]
            fileset_id = parts[1]
            file_id = parts[2]
            return os.path.join(self.locks_dir, f"{scan_id}_{fileset_id}_{file_id}.info")

    def _cleanup_stale_locks(self):
        """Remove stale lock files from previous sessions"""
        if not os.path.exists(self.locks_dir):
            return

        self.logger.info("Starting cleanup of stale locks...")
        cleaned_count = 0
        for filename in os.listdir(self.locks_dir):
            if filename.endswith('.lock'):
                lock_path = os.path.join(self.locks_dir, filename)
                try:
                    # Try to acquire lock briefly to check if it's stale
                    with open(lock_path, 'w') as f:
                        fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                        fcntl.flock(f.fileno(), fcntl.LOCK_UN)
                    # If successful, lock was stale - remove it
                    os.unlink(lock_path)
                    cleaned_count += 1
                    self.logger.debug(f"Removed stale lock file: {filename}")
                except (OSError, IOError):
                    self.logger.warning(f"Lock still active: {filename}")
                    # Lock is still active, keep it
                    pass
        self.logger.info(f"Cleaned up {cleaned_count} stale lock files")
        return

    def _write_lock_info(self, resource_id: str, lock_type: LockType, user: str, level: LockLevel):
        """Write lock metadata for monitoring and debugging"""
        info = {
            'resource_id': resource_id,
            'level': level.value,
            'lock_type': lock_type.value,
            'user': user,
            'timestamp': time.time(),
            'pid': os.getpid(),
            'thread_id': threading.get_ident()
        }

        info_path = self._get_lock_info_path(resource_id, level)
        with open(info_path, 'w') as f:
            json.dump(info, f, indent=2)

    def _remove_lock_info(self, resource_id: str, level: LockLevel):
        """Remove lock metadata file"""
        info_path = self._get_lock_info_path(resource_id, level)
        try:
            os.unlink(info_path)
        except OSError:
            pass

    @contextmanager
    def acquire_lock(self, resource_id: str, lock_type: LockType, user: str, level: LockLevel, timeout: Optional[float] = None):
        """
        Acquire a lock for a specific resource and lock type.

        This method attempts to acquire a lock (either shared or exclusive) for a given resource ID.
        If the lock is successfully acquired, it yields control to the calling code.
        Upon completion of the calling code, it ensures that the lock is released.

        Parameters
        ----------
        resource_id : str
            The unique identifier for the resource (scan_id, scan_id/fileset_id, or scan_id/fileset_id/file_id).
        lock_type : LockType
            The type of lock to acquire (shared or exclusive).
        user : str
            The user requesting the lock.
        level : LockLevel
            The level of the lock (scan, fileset, or file).
        timeout : Optional[float], optional
            The maximum time to wait for acquiring the lock, in seconds. If not provided,
            uses a default timeout value.

        Raises
        ------
        LockTimeoutError
            If the lock could not be acquired within the specified timeout period or if an exclusive
            lock is already held by another user.
        OSError
            If there is an error with file operations while trying to acquire the lock.
        IOError
            If there is an input/output error while trying to acquire the lock.

        Notes
        -----
        This method uses a context manager and should be used within a 'with' statement. It handles both shared
        and exclusive locks, allowing multiple acquisitions for shared locks but raising an exception if an exclusive
        lock is already held.
        """
        timeout = timeout or self.default_timeout
        # Create a unique lock key based on resource_id, level, and lock type
        lock_key = f"{resource_id}/{level.value}/{lock_type.value}"

        self.logger.debug(f"Attempting to acquire {lock_type.value} lock for {level.value} {resource_id} by user {user}")

        with self._thread_lock:
            # Check if we already have this lock
            if lock_key in self._active_locks:
                # For shared locks, allow multiple acquisitions
                if lock_type == LockType.SHARED:
                    self._active_locks[lock_key]['count'] += 1
                    self.logger.debug(f"Incrementing shared lock count for {level.value} {resource_id}")
                    try:
                        yield
                        return
                    finally:
                        self._active_locks[lock_key]['count'] -= 1
                        if self._active_locks[lock_key]['count'] <= 0:
                            self._release_lock(resource_id, lock_type, level)
                else:
                    # For exclusive locks, allow reentrant acquisition by the same user/thread
                    current_lock_user = self._active_locks[lock_key].get('user')
                    if current_lock_user == user:
                        self._active_locks[lock_key]['count'] += 1
                        self.logger.debug(f"Incrementing exclusive lock count for {level.value} {resource_id} by same user {user}")
                        try:
                            yield
                            return
                        finally:
                            self._active_locks[lock_key]['count'] -= 1
                            if self._active_locks[lock_key]['count'] <= 0:
                                self._release_lock(resource_id, lock_type, level)
                    else:
                        raise LockError(f"Exclusive lock already held for {level.value} {resource_id}")

        # Acquire new lock
        acquired = False
        start_time = time.time()

        # Helper to check for conflicting active locks of a different type
        def _has_active_lock(other_type: LockType) -> bool:
            other_key = f"{resource_id}/{level.value}/{other_type.value}"
            return other_key in self._active_locks

        try:
            lock_file_path = self._get_lock_file_path(resource_id, level)

            attempt = 0
            while time.time() - start_time < timeout:
                attempt += 1

                # Respect intra‑process lock conflicts before touching the file
                if lock_type == LockType.SHARED and _has_active_lock(LockType.EXCLUSIVE):
                    # An exclusive lock is active → treat as unavailable and retry
                    self.logger.debug(
                        f"Shared lock request for {level.value} {resource_id} blocked by active exclusive lock"
                    )
                    time.sleep(0.5)
                    continue

                if lock_type == LockType.EXCLUSIVE and _has_active_lock(LockType.SHARED):
                    # One or more shared locks are active → wait
                    self.logger.debug(
                        f"Exclusive lock request for {level.value} {resource_id} blocked by active shared lock(s)"
                    )
                    time.sleep(0.5)
                    continue

                try:
                    # Open lock file
                    lock_fd = os.open(lock_file_path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC)

                    # Determine fcntl flags based on lock type
                    if lock_type == LockType.EXCLUSIVE:
                        lock_flags = fcntl.LOCK_EX | fcntl.LOCK_NB
                    else:  # SHARED
                        lock_flags = fcntl.LOCK_SH | fcntl.LOCK_NB

                    # Try to acquire the lock (non-blocking)
                    fcntl.flock(lock_fd, lock_flags)
                    # SUCCESS - clear any stale warning timestamp for this resource
                    self._warning_timestamps.pop(resource_id, None)

                    # Lock acquired successfully
                    with self._thread_lock:
                        self._active_locks[lock_key] = {
                            'type': lock_type,
                            'user': user,
                            'timestamp': time.time(),
                            'count': 1
                        }
                        self._lock_files[lock_key] = lock_fd

                    self._write_lock_info(resource_id, lock_type, user, level)
                    acquired = True
                    self.logger.debug(
                        f"Successfully acquired {lock_type.value} lock for {level.value} {resource_id} "
                        f"(attempt {attempt})"
                    )
                    break

                except (OSError, IOError) as e:
                    # Lock not available, close fd and retry
                    try:
                        os.close(lock_fd)
                    except:
                        pass

                    now = time.time()
                    last_warn = self._warning_timestamps.get(resource_id, 0.0)
                    if now - last_warn >= self._warning_debounce_interval:
                        self.logger.warning(
                            f"Lock acquisition attempt failed for {level.value} {resource_id} (attempt {attempt}) - "
                            f"retrying... [pid={os.getpid()}, tid={threading.get_ident()}]"
                        )
                        self._warning_timestamps[resource_id] = now
                    else:
                        # We’re within the debounce window - log at DEBUG instead of spamming WARN
                        self.logger.debug(
                            f"Retrying lock for {level.value} {resource_id} (attempt {attempt}) - still waiting"
                        )
                    time.sleep(0.1)  # Brief pause before retry

            if not acquired:
                raise LockTimeoutError(
                    f"Could not acquire {lock_type.value} lock for {level.value} {resource_id} within {timeout} seconds")

            # Yield control to the calling code
            yield

        finally:
            # Always release the lock
            if acquired:
                self._release_lock(resource_id, lock_type, level)

    def _release_lock(self, resource_id: str, lock_type: LockType, level: LockLevel):
        """
        Release a lock for a given resource ID and lock type.

        This method releases an existing lock by unlocking the file descriptor, closing it,
        and removing the associated files and information from internal data structures.
        It ensures that the lock is completely removed and no longer active.

        Parameters
        ----------
        resource_id : str
            The unique identifier of the resource whose lock needs to be released.
        lock_type : LockType
            The type of lock to release.
        level : LockLevel
            The level of the lock (scan, fileset, or file).

        Notes
        -----
        This method should only be called after a successful acquisition of the same lock
        type for the given resource ID. It uses internal thread locks and file operations to ensure
        atomicity and consistency of lock management.

        See Also
        --------
        _acquire_lock : Method to acquire a lock.
        _remove_lock_info : Helper function to remove lock information.

        Warnings
        --------
        This method modifies internal data structures and should be used with caution.
        Improper use might lead to inconsistencies in the lock state or lost locks.
        """
        lock_key = f"{resource_id}/{level.value}/{lock_type.value}"
        self.logger.debug(f"Releasing {lock_type.value} lock for {level.value} {resource_id}")

        with self._thread_lock:
            if lock_key in self._active_locks:
                # Get file descriptor and close it
                if lock_key in self._lock_files:
                    try:
                        fd = self._lock_files[lock_key]
                        fcntl.flock(fd, fcntl.LOCK_UN)
                        os.close(fd)
                        self.logger.debug(f"Closed file descriptor for {resource_id}")
                    except Exception as e:
                        self.logger.error(f"Error closing lock file for {resource_id}: {str(e)}")
                    del self._lock_files[lock_key]

                # Remove from active locks
                del self._active_locks[lock_key]

                # Clean up lock file
                lock_file_path = self._get_lock_file_path(resource_id, level)
                try:
                    os.unlink(lock_file_path)
                    self.logger.debug(f"Removed lock file for {resource_id}")
                except OSError as e:
                    self.logger.error(f"Error removing lock file for {resource_id}: {str(e)}")

                # Remove lock info
                self._remove_lock_info(resource_id, level)
                self.logger.debug(f"Successfully released lock for {level.value} {resource_id}")

    def get_lock_status(self, resource_id: str, level: LockLevel) -> Dict:
        """
        Retrieve the current lock status for a given resource ID.

        This method checks all active locks and determines if there is an
        exclusive or shared lock associated with the specified resource. It returns
        a dictionary containing information about the exclusive lock (if any) and
        all shared locks associated with the resource.

        Parameters
        ----------
        resource_id : str
            The unique identifier of the resource for which to retrieve the lock status.
        level : LockLevel
            The level of the lock (scan, fileset, or file).

        Returns
        -------
        status : dict
            A dictionary with two keys:
                - 'exclusive': Information about the exclusive lock (if any), or None.
                  This is a nested dictionary containing the user and timestamp.
                - 'shared': A list of dictionaries, each representing a shared lock.
                  Each dictionary contains the user, timestamp, and count.

        Notes
        -----
        This method iterates through all active locks to find matches with the given resource ID.
        It distinguishes between exclusive and shared locks based on their type.

        Examples
        --------
        >>> from plantdb.commons.fsdb.lock import LockManager
        >>> manager = LockManager('/path/to/local/database')
        >>> lock = manager.acquire_lock('scan123', LockType.EXCLUSIVE, user='user1', level='scan')
        >>> status = manager.get_lock_status("scan123", LockLevel.SCAN)
        >>> print(status)
        {'exclusive': None, 'shared': [{'user': 'user1', 'timestamp': '2023-01-01T12:00:00', 'count': 1}]}
        """
        status = {'exclusive': None, 'shared': []}

        # Look for locks matching this resource and level
        for lock_key, lock_info in self._active_locks.items():
            # Extract resource_id and level from the lock key
            # Format: resource_id_level_locktype
            parts = lock_key.split('/', 2)
            if len(parts) != 3:
                continue
                
            lock_resource_id, lock_level, _ = parts
            # Check if this lock belongs to the requested resource and level
            if lock_resource_id == resource_id and lock_level == level.value:
                if lock_info['type'] == LockType.EXCLUSIVE:
                    status['exclusive'] = {
                        'user': lock_info['user'],
                        'timestamp': lock_info['timestamp']
                    }
                else:  # SHARED
                    status['shared'].append({
                        'user': lock_info['user'],
                        'timestamp': lock_info['timestamp'],
                        'count': lock_info['count']
                    })

        return status

    def cleanup_all_locks(self):
        """Emergency cleanup of all locks (use with caution)"""
        self.logger.warning("Cleaning up all active locks...")
        with self._thread_lock:
            for lock_key in list(self._active_locks.keys()):
                # Parse the lock key to extract resource_id and level
                parts = lock_key.split('/', 2)
                if len(parts) != 3:
                    continue
                    
                resource_id, level_str, lock_type_str = parts
                level = LockLevel(level_str)
                lock_type = LockType(lock_type_str)
                self._release_lock(resource_id, lock_type, level)
        return