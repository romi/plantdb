#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
# ScanLockManager

File-based locking for thread-safe resource management

The `ScanLockManager` module provides a robust file-based locking mechanism to ensure thread-safe operations across multiple threads or processes.
It allows acquiring and releasing locks on resources identified by scan IDs, with support for both shared (read) and exclusive (write) locks.

## Key Features

- Thread-safe lock acquisition and release using file-based locking mechanisms
- Support for both shared (multiple readers) and exclusive (single writer) locks
- Lock timeout to prevent indefinite waiting for lock acquisition
- Automatic cleanup of stale locks on initialization
- Monitoring and debugging capabilities with lock metadata storage

## Usage Examples

```python
from plantdb.commons.fsdb.lock import ScanLockManager

# Initialize the lock manager with a base path
manager = ScanLockManager('/path/to/database')

# Acquire an exclusive lock for 'scan123'
with manager.acquire_lock('scan123', LockType.EXCLUSIVE, user='user1'):
    # Perform critical section operations...

# Check the status of locks for 'scan123'
status = manager.get_lock_status('scan123')
print(status)
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


class LockType(Enum):
    SHARED = "shared"  # Read operations
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


class ScanLockManager :
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
        Dictionary mapping lock names (scan ID + lock type) to dictionaries containing information about the lock including:

           - 'type': the type of the lock (shared or exclusive),
           - 'user': the user who acquired the lock (if available),
           - 'timestamp': the timestamp when the lock was acquired (if available),
           - 'count': int indicating how many locks are active for a given scan and lock type combination,
    _lock_files : Dict[str, int]
        The file descriptors (int) for each active lock in the dictionary mapping.
    _thread_lock: threading.RLock
        A thread-safe lock used to synchronize access to the active locks dictionary and file descriptors.

    Examples
    --------
    >>> from plantdb.commons.fsdb.lock import ScanLockManager
    >>> manager = ScanLockManager('/path/to/local/database')
    >>> lock = manager.acquire_lock('scan123')
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

        # Store the last time we emitted a warning per scan_id
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
            logger = get_logger(__name__ + '.ScanLockManager', log_level="DEBUG")
            logger.error(f"Cannot write to base_path `{base_path}`.")
            logger.debug(f"Current UID={os.getuid()}, GID={os.getgid()}")
            raise e

        # Ensure locks directory exists
        os.makedirs(self.locks_dir, exist_ok=True)
        # Initialize lock manager logger
        self.logger = get_logger(__name__ + '.ScanLockManager',
                                 log_file=kwargs.get("log_file", os.path.join(base_path, '.locks', 'lock_manager.log')),
                                 log_level=kwargs.get("log_level", "INFO"))
        self.logger.debug(f"Initialized ScanLockManager with base path: `{base_path}`")
        # Clean up stale locks on initialization
        self._cleanup_stale_locks()

    def _get_lock_file_path(self, scan_id: str) -> str:
        """
        Generates the file path for a lock file based on the provided scan ID.

        Parameters
        ----------
        scan_id : str
            The unique identifier for the scan.

        Returns
        -------
        str
            The full path to the lock file.
        """
        # Use a single lock file per scan_id
        return os.path.join(self.locks_dir, f"{scan_id}.lock")

    def _get_lock_info_path(self, scan_id: str) -> str:
        """
        Get the path to the lock info file for a given scan ID.

        Parameters
        ----------
        scan_id : str
            The identifier of the scan.

        Returns
        -------
        str
            The absolute path to the lock info file.
        """
        return os.path.join(self.locks_dir, f"{scan_id}.info")

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

    def _write_lock_info(self, scan_id: str, lock_type: LockType, user: str):
        """Write lock metadata for monitoring and debugging"""
        info = {
            'scan_id': scan_id,
            'lock_type': lock_type.value,
            'user': user,
            'timestamp': time.time(),
            'pid': os.getpid(),
            'thread_id': threading.get_ident()
        }

        info_path = self._get_lock_info_path(scan_id)
        with open(info_path, 'w') as f:
            json.dump(info, f, indent=2)

    def _remove_lock_info(self, scan_id: str):
        """Remove lock metadata file"""
        info_path = self._get_lock_info_path(scan_id)
        try:
            os.unlink(info_path)
        except OSError:
            pass

    @contextmanager
    def acquire_lock(self, scan_id: str, lock_type: LockType, user: str, timeout: Optional[float] = None):
        """
        Acquire a lock for a specific scan and lock type.

        This method attempts to acquire a lock (either shared or exclusive) for a given scan ID.
        If the lock is successfully acquired, it yields control to the calling code.
        Upon completion of the calling code, it ensures that the lock is released.

        Parameters
        ----------
        scan_id : str
            The unique identifier for the scan.
        lock_type : LockType
            The type of lock to acquire (shared or exclusive).
        user : str
            The user requesting the lock.
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
        lock_key = f"{scan_id}_{lock_type.value}"

        self.logger.debug(f"Attempting to acquire {lock_type.value} lock for scan {scan_id} by user {user}")

        with self._thread_lock:
            # Check if we already have this lock
            if lock_key in self._active_locks:
                # For shared locks, allow multiple acquisitions
                if lock_type == LockType.SHARED:
                    self._active_locks[lock_key]['count'] += 1
                    self.logger.debug(f"Incrementing shared lock count for {scan_id}")
                    try:
                        yield
                        return
                    finally:
                        self._active_locks[lock_key]['count'] -= 1
                        if self._active_locks[lock_key]['count'] <= 0:
                            self._release_lock(scan_id, lock_type)
                else:
                    # For exclusive locks, allow reentrant acquisition by the same user/thread
                    current_lock_user = self._active_locks[lock_key].get('user')
                    if current_lock_user == user:
                        self._active_locks[lock_key]['count'] += 1
                        self.logger.debug(f"Incrementing exclusive lock count for {scan_id} by same user {user}")
                        try:
                            yield
                            return
                        finally:
                            self._active_locks[lock_key]['count'] -= 1
                            if self._active_locks[lock_key]['count'] <= 0:
                                self._release_lock(scan_id, lock_type)
                    else:
                        raise LockError(f"Exclusive lock already held for scan {scan_id}")

        # Acquire new lock
        acquired = False
        start_time = time.time()

        try:
            lock_file_path = self._get_lock_file_path(scan_id)

            attempt = 0
            while time.time() - start_time < timeout:
                attempt += 1
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
                    # SUCCESS - clear any stale warning timestamp for this scan_id
                    self._warning_timestamps.pop(scan_id, None)

                    # Lock acquired successfully
                    with self._thread_lock:
                        self._active_locks[lock_key] = {
                            'type': lock_type,
                            'user': user,
                            'timestamp': time.time(),
                            'count': 1
                        }
                        self._lock_files[lock_key] = lock_fd

                    self._write_lock_info(scan_id, lock_type, user)
                    acquired = True
                    self.logger.debug(
                        f"Successfully acquired {lock_type.value} lock for scan {scan_id} "
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
                    last_warn = self._warning_timestamps.get(scan_id, 0.0)
                    if now - last_warn >= self._warning_debounce_interval:
                        self.logger.warning(
                            f"Lock acquisition attempt failed for {scan_id} (attempt {attempt}) - "
                            f"retrying... [pid={os.getpid()}, tid={threading.get_ident()}]"
                        )
                        self._warning_timestamps[scan_id] = now
                    else:
                        # We’re within the debounce window - log at DEBUG instead of spamming WARN
                        self.logger.debug(
                            f"Retrying lock for {scan_id} (attempt {attempt}) - still waiting"
                        )
                    time.sleep(0.1)  # Brief pause before retry

            if not acquired:
                raise LockTimeoutError(
                    f"Could not acquire {lock_type.value} lock for scan {scan_id} within {timeout} seconds")

            # Yield control to the calling code
            yield

        finally:
            # Always release the lock
            if acquired:
                self._release_lock(scan_id, lock_type)

    def _release_lock(self, scan_id: str, lock_type: LockType):
        """
        Release a lock for a given scan ID and lock type.

        This method releases an existing lock by unlocking the file descriptor, closing it,
        and removing the associated files and information from internal data structures.
        It ensures that the lock is completely removed and no longer active.

        Parameters
        ----------
        scan_id : str
            The unique identifier of the scan whose lock needs to be released.
        lock_type : LockType
            The type of lock to release.

        Notes
        -----
        This method should only be called after a successful acquisition of the same lock
        type for the given scan ID. It uses internal thread locks and file operations to ensure
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
        lock_key = f"{scan_id}_{lock_type.value}"
        self.logger.debug(f"Releasing {lock_type.value} lock for scan {scan_id}")

        with self._thread_lock:
            if lock_key in self._active_locks:
                # Get file descriptor and close it
                if lock_key in self._lock_files:
                    try:
                        fd = self._lock_files[lock_key]
                        fcntl.flock(fd, fcntl.LOCK_UN)
                        os.close(fd)
                        self.logger.debug(f"Closed file descriptor for {scan_id}")
                    except Exception as e:
                        self.logger.error(f"Error closing lock file for {scan_id}: {str(e)}")
                    del self._lock_files[lock_key]

                # Remove from active locks
                del self._active_locks[lock_key]

                # Clean up lock file
                lock_file_path = self._get_lock_file_path(scan_id)
                try:
                    os.unlink(lock_file_path)
                    self.logger.debug(f"Removed lock file for {scan_id}")
                except OSError as e:
                    self.logger.error(f"Error removing lock file for {scan_id}: {str(e)}")

                # Remove lock info
                self._remove_lock_info(scan_id)
                self.logger.debug(f"Successfully released lock for scan {scan_id}")

    def get_lock_status(self, scan_id: str) -> Dict:
        """
        Retrieve the current lock status for a given scan ID.

        This method checks all active locks and determines if there is an
        exclusive or shared lock associated with the specified scan ID. It returns
        a dictionary containing information about the exclusive lock (if any) and
        all shared locks associated with the scan ID.

        Parameters
        ----------
        scan_id : str
            The unique identifier of the scan for which to retrieve the lock status.

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
        This method iterates through all active locks to find matches with the given scan ID.
        It distinguishes between exclusive and shared locks based on their type.

        Examples
        --------
        >>> from plantdb.commons.fsdb.lock import ScanLockManager
        >>> manager = ScanLockManager('/path/to/local/database')
        >>> lock = manager.acquire_lock('scan123')
        >>> status = manager.get_lock_status("scan123")
        >>> print(status)
        {'exclusive': None, 'shared': [{'user': 'user1', 'timestamp': '2023-01-01T12:00:00', 'count': 1}]}
        """
        status = {'exclusive': None, 'shared': []}

        for lock_key, lock_info in self._active_locks.items():
            if lock_key.startswith(scan_id):
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
                scan_id, lock_type_str = lock_key.split('_', 1)
                lock_type = LockType(lock_type_str)
                self._release_lock(scan_id, lock_type)
        return
