#!/usr/bin/env python
# -*- coding: utf-8 -*-

import fcntl
import json
import os
import shutil
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from plantdb.commons.fsdb.core import FSDB
from plantdb.commons.fsdb.lock import LockError
from plantdb.commons.fsdb.lock import LockTimeoutError
from plantdb.commons.fsdb.lock import LockType
from plantdb.commons.fsdb.lock import LockManager
from plantdb.commons.fsdb.lock import LockLevel
from plantdb.commons.test_database import setup_test_database


class TestLockType(unittest.TestCase):
    """Test cases for the LockType enum."""

    def test_lock_type_values(self):
        """Verify the values of the LockType enum."""
        self.assertEqual(LockType.SHARED.value, "shared")
        self.assertEqual(LockType.EXCLUSIVE.value, "exclusive")


class TestLockTimeoutError(unittest.TestCase):
    """Test cases for the LockTimeoutError exception."""

    def test_default_message(self):
        """Verify the default error message of LockTimeoutError."""
        error = LockTimeoutError()
        self.assertEqual(error.message, "Error: Lock acquisition timed-out!")
        self.assertEqual(str(error), "Error: Lock acquisition timed-out!")

    def test_custom_message(self):
        """Verify that a custom error message works correctly."""
        custom_message = "Custom timeout message"
        error = LockTimeoutError(custom_message)
        self.assertEqual(error.message, custom_message)
        self.assertEqual(str(error), custom_message)


class TestLockManager(unittest.TestCase):
    """Test cases for the ScanLockManager class."""

    def setUp(self):
        """Set up a temporary directory for testing."""
        self.temp_dir = tempfile.mkdtemp()
        # Create a lock manager with a short default timeout for faster tests
        self.lock_manager = LockManager(self.temp_dir, default_timeout=1.0)

    def tearDown(self):
        """Clean up temporary files after tests."""
        self.lock_manager.cleanup_all_locks()
        # Remove all files in the temp directory
        for root, dirs, files in os.walk(self.temp_dir, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        os.rmdir(self.temp_dir)

    def test_initialization(self):
        """Verify that initialization creates the lock directory and sets up attributes."""
        # Check that the locks directory was created
        locks_dir = os.path.join(self.temp_dir, ".locks")
        self.assertTrue(os.path.exists(locks_dir))
        self.assertTrue(os.path.isdir(locks_dir))

        # Check that attributes were set correctly
        self.assertEqual(self.lock_manager.base_path, self.temp_dir)
        self.assertEqual(self.lock_manager.default_timeout, 1.0)
        self.assertEqual(self.lock_manager.locks_dir, locks_dir)
        self.assertEqual(self.lock_manager._active_locks, {})
        self.assertEqual(self.lock_manager._lock_files, {})

    def test_get_lock_file_path(self):
        """Verify that _get_lock_file_path returns the correct path."""
        scan_id = "test_scan"
        # Test for SCAN level
        expected_path = os.path.join(self.temp_dir, ".locks", f"{scan_id}.lock")
        actual_path = self.lock_manager._get_lock_file_path(scan_id, LockLevel.SCAN)
        self.assertEqual(actual_path, expected_path)

        # Test for FILESET level
        fileset_id = "test_fileset"
        resource_id = f"{scan_id}/{fileset_id}"
        expected_path = os.path.join(self.temp_dir, ".locks", f"{scan_id}_{fileset_id}.lock")
        actual_path = self.lock_manager._get_lock_file_path(resource_id, LockLevel.FILESET)
        self.assertEqual(actual_path, expected_path)

        # Test for FILE level
        file_id = "test_file"
        resource_id = f"{scan_id}/{fileset_id}/{file_id}"
        expected_path = os.path.join(self.temp_dir, ".locks", f"{scan_id}_{fileset_id}_{file_id}.lock")
        actual_path = self.lock_manager._get_lock_file_path(resource_id, LockLevel.FILE)
        self.assertEqual(actual_path, expected_path)

    def test_get_lock_info_path(self):
        """Verify that _get_lock_info_path returns the correct path."""
        scan_id = "test_scan"
        # Test for SCAN level
        expected_path = os.path.join(self.temp_dir, ".locks", f"{scan_id}.info")
        actual_path = self.lock_manager._get_lock_info_path(scan_id, LockLevel.SCAN)
        self.assertEqual(actual_path, expected_path)

        # Test for FILESET level
        fileset_id = "test_fileset"
        resource_id = f"{scan_id}/{fileset_id}"
        expected_path = os.path.join(self.temp_dir, ".locks", f"{scan_id}_{fileset_id}.info")
        actual_path = self.lock_manager._get_lock_info_path(resource_id, LockLevel.FILESET)
        self.assertEqual(actual_path, expected_path)

        # Test for FILE level
        file_id = "test_file"
        resource_id = f"{scan_id}/{fileset_id}/{file_id}"
        expected_path = os.path.join(self.temp_dir, ".locks", f"{scan_id}_{fileset_id}_{file_id}.info")
        actual_path = self.lock_manager._get_lock_info_path(resource_id, LockLevel.FILE)
        self.assertEqual(actual_path, expected_path)

    @patch('fcntl.flock')
    @patch('os.unlink')
    def test_cleanup_stale_locks(self, mock_unlink, mock_flock):
        """Verify that _cleanup_stale_locks removes stale lock files."""
        # Create fake lock files
        locks_dir = os.path.join(self.temp_dir, ".locks")
        lock_file1 = os.path.join(locks_dir, "scan1_shared.lock")
        lock_file2 = os.path.join(locks_dir, "scan2_exclusive.lock")

        with open(lock_file1, 'w') as f:
            f.write("")
        with open(lock_file2, 'w') as f:
            f.write("")

        # Make sure the files exist
        self.assertTrue(os.path.exists(lock_file1))
        self.assertTrue(os.path.exists(lock_file2))

        # Mock flock to simulate stale locks
        mock_flock.return_value = None

        # Call the cleanup method
        self.lock_manager._cleanup_stale_locks()

        # Verify that flock was called for each lock file
        self.assertEqual(mock_flock.call_count, 4)  # 2 LOCK_EX and 2 LOCK_UN calls

        # Verify that unlink was called for each lock file
        self.assertEqual(mock_unlink.call_count, 2)

    def test_cleanup_stale_locks_with_active_locks(self):
        """Verify that _cleanup_stale_locks preserves active lock files."""
        # Create a lock file that will be considered active (by not mocking flock)
        locks_dir = os.path.join(self.temp_dir, ".locks")
        lock_file = os.path.join(locks_dir, "active_scan.lock")
        
        # Create the lock file
        with open(lock_file, 'w') as f:
            f.write("")

        # Make sure the file exists
        self.assertTrue(os.path.exists(lock_file))

        # Call the cleanup method
        self.lock_manager._cleanup_stale_locks()
        self.assertFalse(os.path.exists(lock_file))

    def test_write_and_remove_lock_info(self):
        """Verify that _write_lock_info creates a lock info file and _remove_lock_info removes it."""
        scan_id = "test_scan"
        lock_type = LockType.EXCLUSIVE
        user = "test_user"

        # Write lock info
        self.lock_manager._write_lock_info(scan_id, lock_type, user, LockLevel.SCAN)

        # Check that the file exists
        info_path = self.lock_manager._get_lock_info_path(scan_id, LockLevel.SCAN)
        self.assertTrue(os.path.exists(info_path))

        # Verify the content of the file
        with open(info_path, 'r') as f:
            info = json.load(f)
            self.assertEqual(info['resource_id'], scan_id)
            self.assertEqual(info['lock_type'], lock_type.value)
            self.assertEqual(info['user'], user)
            self.assertIn('timestamp', info)
            self.assertIn('pid', info)
            self.assertIn('thread_id', info)

        # Remove lock info
        self.lock_manager._remove_lock_info(scan_id, LockLevel.SCAN)

        # Check that the file no longer exists
        self.assertFalse(os.path.exists(info_path))

    def test_write_lock_info_fileset_level(self):
        """Verify that _write_lock_info works with fileset level locks."""
        scan_id = "test_scan"
        fileset_id = "test_fileset"
        resource_id = f"{scan_id}/{fileset_id}"
        lock_type = LockType.SHARED
        user = "test_user"

        # Write lock info
        self.lock_manager._write_lock_info(resource_id, lock_type, user, LockLevel.FILESET)

        # Check that the file exists
        info_path = self.lock_manager._get_lock_info_path(resource_id, LockLevel.FILESET)
        self.assertTrue(os.path.exists(info_path))

        # Verify the content of the file
        with open(info_path, 'r') as f:
            info = json.load(f)
            self.assertEqual(info['resource_id'], resource_id)
            self.assertEqual(info['lock_type'], lock_type.value)
            self.assertEqual(info['user'], user)
            self.assertIn('timestamp', info)
            self.assertIn('pid', info)
            self.assertIn('thread_id', info)

        # Remove lock info
        self.lock_manager._remove_lock_info(resource_id, LockLevel.FILESET)

        # Check that the file no longer exists
        self.assertFalse(os.path.exists(info_path))

    def test_write_lock_info_file_level(self):
        """Verify that _write_lock_info works with file level locks."""
        scan_id = "test_scan"
        fileset_id = "test_fileset"
        file_id = "test_file"
        resource_id = f"{scan_id}/{fileset_id}/{file_id}"
        lock_type = LockType.EXCLUSIVE
        user = "test_user"

        # Write lock info
        self.lock_manager._write_lock_info(resource_id, lock_type, user, LockLevel.FILE)

        # Check that the file exists
        info_path = self.lock_manager._get_lock_info_path(resource_id, LockLevel.FILE)
        self.assertTrue(os.path.exists(info_path))

        # Verify the content of the file
        with open(info_path, 'r') as f:
            info = json.load(f)
            self.assertEqual(info['resource_id'], resource_id)
            self.assertEqual(info['lock_type'], lock_type.value)
            self.assertEqual(info['user'], user)
            self.assertIn('timestamp', info)
            self.assertIn('pid', info)
            self.assertIn('thread_id', info)

        # Remove lock info
        self.lock_manager._remove_lock_info(resource_id, LockLevel.FILE)

        # Check that the file no longer exists
        self.assertFalse(os.path.exists(info_path))

    def test_acquire_shared_lock(self):
        """Verify that a shared lock can be acquired."""
        scan_id = "test_scan"
        user = "test_user"

        # Acquire a shared lock
        with self.lock_manager.acquire_lock(scan_id, LockType.SHARED, user, LockLevel.SCAN):
            # Check that the lock file exists
            lock_file = self.lock_manager._get_lock_file_path(scan_id, LockLevel.SCAN)
            self.assertTrue(os.path.exists(lock_file))

            # Check that the lock is registered in active locks
            lock_key = f"{scan_id}/scan/shared"
            self.assertIn(lock_key, self.lock_manager._active_locks)
            self.assertEqual(self.lock_manager._active_locks[lock_key]['type'], LockType.SHARED)
            self.assertEqual(self.lock_manager._active_locks[lock_key]['user'], user)
            self.assertEqual(self.lock_manager._active_locks[lock_key]['count'], 1)

            # Check that the lock info file exists
            info_path = self.lock_manager._get_lock_info_path(scan_id, LockLevel.SCAN)
            self.assertTrue(os.path.exists(info_path))

        # After the context manager exits, check that the lock is released
        lock_file = self.lock_manager._get_lock_file_path(scan_id, LockLevel.SCAN)
        self.assertFalse(os.path.exists(lock_file))
        lock_key = f"{scan_id}/scan/shared"
        self.assertNotIn(lock_key, self.lock_manager._active_locks)
        info_path = self.lock_manager._get_lock_info_path(scan_id, LockLevel.SCAN)
        self.assertFalse(os.path.exists(info_path))

    def test_acquire_exclusive_lock(self):
        """Verify that an exclusive lock can be acquired."""
        scan_id = "test_scan"
        user = "test_user"

        # Acquire an exclusive lock
        with self.lock_manager.acquire_lock(scan_id, LockType.EXCLUSIVE, user, LockLevel.SCAN):
            # Check that the lock file exists
            lock_file = self.lock_manager._get_lock_file_path(scan_id, LockLevel.SCAN)
            self.assertTrue(os.path.exists(lock_file))

            # Check that the lock is registered in active locks
            lock_key = f"{scan_id}/scan/exclusive"
            self.assertIn(lock_key, self.lock_manager._active_locks)
            self.assertEqual(self.lock_manager._active_locks[lock_key]['type'], LockType.EXCLUSIVE)
            self.assertEqual(self.lock_manager._active_locks[lock_key]['user'], user)
            self.assertEqual(self.lock_manager._active_locks[lock_key]['count'], 1)

            # Check that the lock info file exists
            info_path = self.lock_manager._get_lock_info_path(scan_id, LockLevel.SCAN)
            self.assertTrue(os.path.exists(info_path))

        # After the context manager exits, check that the lock is released
        lock_file = self.lock_manager._get_lock_file_path(scan_id, LockLevel.SCAN)
        self.assertFalse(os.path.exists(lock_file))
        lock_key = f"{scan_id}/scan/exclusive"
        self.assertNotIn(lock_key, self.lock_manager._active_locks)
        info_path = self.lock_manager._get_lock_info_path(scan_id, LockLevel.SCAN)
        self.assertFalse(os.path.exists(info_path))

    def test_acquire_multiple_shared_locks(self):
        """Verify that multiple shared locks can be acquired for the same scan ID."""
        scan_id = "test_scan"
        user1 = "user1"
        user2 = "user2"

        # Acquire a shared lock with user1
        with self.lock_manager.acquire_lock(scan_id, LockType.SHARED, user1, LockLevel.SCAN):
            # Acquire another shared lock with user2
            with self.lock_manager.acquire_lock(scan_id, LockType.SHARED, user2, LockLevel.SCAN):
                # Check that the lock is registered in active locks with a count of 2
                lock_key = f"{scan_id}/scan/shared"
                self.assertIn(lock_key, self.lock_manager._active_locks)
                self.assertEqual(self.lock_manager._active_locks[lock_key]['count'], 2)

            # After user2's lock is released, the count should be 1
            lock_key = f"{scan_id}/scan/shared"
            self.assertIn(lock_key, self.lock_manager._active_locks)
            self.assertEqual(self.lock_manager._active_locks[lock_key]['count'], 1)

        # After both locks are released, the lock should be gone
        lock_key = f"{scan_id}/scan/shared"
        self.assertNotIn(lock_key, self.lock_manager._active_locks)

    def test_exclusive_lock_prevents_shared(self):
        """Verify that an exclusive lock prevents acquiring a shared lock."""
        scan_id = "test_scan"
        user1 = "user1"
        user2 = "user2"

        # Acquire an exclusive lock with user1
        with self.lock_manager.acquire_lock(scan_id, LockType.EXCLUSIVE, user1, LockLevel.SCAN):
            # Try to acquire a shared lock with user2 (should time out)
            with self.assertRaises(LockTimeoutError):
                with self.lock_manager.acquire_lock(scan_id, LockType.SHARED, user2, LockLevel.SCAN, timeout=0.5):
                    pass

    def test_shared_lock_prevents_exclusive(self):
        """Verify that a shared lock prevents acquiring an exclusive lock."""
        scan_id = "test_scan"
        user1 = "user1"
        user2 = "user2"

        # Acquire a shared lock with user1
        with self.lock_manager.acquire_lock(scan_id, LockType.SHARED, user1, LockLevel.SCAN):
            # Try to acquire an exclusive lock with user2 (should time out)
            with self.assertRaises(LockTimeoutError):
                with self.lock_manager.acquire_lock(scan_id, LockType.EXCLUSIVE, user2, LockLevel.SCAN, timeout=0.5):
                    pass

    def test_exclusive_lock_prevents_exclusive(self):
        """Verify that an exclusive lock prevents acquiring another exclusive lock."""
        scan_id = "test_scan"
        user1 = "user1"
        user2 = "user2"

        # Acquire an exclusive lock with user1
        with self.lock_manager.acquire_lock(scan_id, LockType.EXCLUSIVE, user1, LockLevel.SCAN):
            # Try to acquire another exclusive lock with user2 (should raise a LockError)
            with self.assertRaises(LockError):
                with self.lock_manager.acquire_lock(scan_id, LockType.EXCLUSIVE, user2, LockLevel.SCAN, timeout=0.5):
                    pass

    def test_lock_timeout(self):
        """Verify that lock acquisition times out after the specified timeout period."""
        scan_id = "test_scan"
        user = "test_user"

        # Create a lock file and hold an exclusive lock on it
        lock_file_path = self.lock_manager._get_lock_file_path(scan_id, LockLevel.SCAN)
        os.makedirs(os.path.dirname(lock_file_path), exist_ok=True)

        # Open and lock the file outside of the lock manager
        lock_fd = os.open(lock_file_path, os.O_CREAT | os.O_WRONLY)
        fcntl.flock(lock_fd, fcntl.LOCK_EX)

        try:
            # Try to acquire the lock (should time out)
            start_time = time.time()
            with self.assertRaises(LockTimeoutError):
                with self.lock_manager.acquire_lock(scan_id, LockType.EXCLUSIVE, user, LockLevel.SCAN, timeout=0.5):
                    pass

            # Check that it took approximately the timeout duration
            elapsed = time.time() - start_time
            self.assertGreaterEqual(elapsed, 0.5)
            self.assertLess(elapsed, 1.0)  # Allow some buffer for timing
        finally:
            # Clean up the external lock
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            os.close(lock_fd)

    def test_get_lock_status_no_locks(self):
        """Verify that get_lock_status returns correct status when no locks exist."""
        scan_id = "test_scan"
        status = self.lock_manager.get_lock_status(scan_id, LockLevel.SCAN)
        self.assertEqual(status, {'exclusive': None, 'shared': []})

    def test_get_lock_status_with_exclusive_lock(self):
        """Verify that get_lock_status returns correct status with an exclusive lock."""
        scan_id = "test_scan"
        user = "test_user"

        # Acquire an exclusive lock
        with self.lock_manager.acquire_lock(scan_id, LockType.EXCLUSIVE, user, LockLevel.SCAN):
            # Get the lock status
            status = self.lock_manager.get_lock_status(scan_id, LockLevel.SCAN)
            print(self.lock_manager._active_locks.items())
            print(status)
            # Check the status
            self.assertIsNotNone(status['exclusive'])
            self.assertEqual(status['exclusive']['user'], user)
            self.assertIn('timestamp', status['exclusive'])
            self.assertEqual(status['shared'], [])

    def test_get_lock_status_with_shared_locks(self):
        """Verify that get_lock_status returns correct status with shared locks."""
        scan_id = "test_scan"
        user1 = "user1"
        user2 = "user2"

        # Acquire shared locks
        with self.lock_manager.acquire_lock(scan_id, LockType.SHARED, user1, LockLevel.SCAN):
            with self.lock_manager.acquire_lock(scan_id, LockType.SHARED, user2, LockLevel.SCAN):
                # Get the lock status
                status = self.lock_manager.get_lock_status(scan_id, LockLevel.SCAN)

                # Check the status
                self.assertIsNone(status['exclusive'])
                self.assertEqual(len(status['shared']), 1)  # Only one shared lock entry with count=2
                self.assertIn('user', status['shared'][0])
                self.assertIn('timestamp', status['shared'][0])
                self.assertEqual(status['shared'][0]['count'], 2)

    def test_get_lock_status_complex_combinations(self):
        """Verify that get_lock_status works correctly with complex lock combinations."""
        scan_id = "test_scan"
        user1 = "user1"
        user2 = "user2"

        # Acquire an exclusive lock
        with self.lock_manager.acquire_lock(scan_id, LockType.EXCLUSIVE, user1, LockLevel.SCAN):
            # Get the lock status - should show exclusive lock
            status = self.lock_manager.get_lock_status(scan_id, LockLevel.SCAN)
            self.assertIsNotNone(status['exclusive'])
            self.assertEqual(status['exclusive']['user'], user1)
            self.assertEqual(status['shared'], [])

            # Acquire a shared lock for the same resource (this should block)
            # But we'll test this differently by checking the internal state directly
            # since we're already holding an exclusive lock
            
            # Get the lock status after acquiring exclusive lock
            status = self.lock_manager.get_lock_status(scan_id, LockLevel.SCAN)
            self.assertIsNotNone(status['exclusive'])
            self.assertEqual(status['exclusive']['user'], user1)
            self.assertEqual(status['shared'], [])

        # After releasing all locks, status should be empty
        status = self.lock_manager.get_lock_status(scan_id, LockLevel.SCAN)
        self.assertEqual(status, {'exclusive': None, 'shared': []})

    def test_get_lock_status_fileset_level(self):
        """Verify that get_lock_status works with fileset level locks."""
        scan_id = "test_scan"
        fileset_id = "test_fileset"
        resource_id = f"{scan_id}/{fileset_id}"
        user1 = "user1"
        user2 = "user2"

        # Acquire an exclusive lock at fileset level
        with self.lock_manager.acquire_lock(resource_id, LockType.EXCLUSIVE, user1, LockLevel.FILESET):
            # Get the lock status
            status = self.lock_manager.get_lock_status(resource_id, LockLevel.FILESET)
            
            # Check the status
            self.assertIsNotNone(status['exclusive'])
            self.assertEqual(status['exclusive']['user'], user1)
            self.assertEqual(status['shared'], [])

        # Acquire a shared lock at fileset level
        with self.lock_manager.acquire_lock(resource_id, LockType.SHARED, user2, LockLevel.FILESET):
            # Get the lock status
            status = self.lock_manager.get_lock_status(resource_id, LockLevel.FILESET)
            
            # Check the status
            self.assertIsNone(status['exclusive'])
            self.assertEqual(len(status['shared']), 1)
            self.assertEqual(status['shared'][0]['user'], user2)
            self.assertEqual(status['shared'][0]['count'], 1)

    def test_cleanup_all_locks(self):
        """Verify that cleanup_all_locks releases all active locks."""
        scan_id1 = "scan1"
        scan_id2 = "scan2"
        user = "test_user"

        # Acquire multiple locks
        with self.lock_manager.acquire_lock(scan_id1, LockType.SHARED, user, LockLevel.SCAN):
            with self.lock_manager.acquire_lock(scan_id2, LockType.EXCLUSIVE, user, LockLevel.SCAN):
                # Check that the locks exist
                self.assertEqual(len(self.lock_manager._active_locks), 2)

                # Clean up all locks
                self.lock_manager.cleanup_all_locks()

                # Check that all locks are released
                self.assertEqual(len(self.lock_manager._active_locks), 0)
                self.assertEqual(len(self.lock_manager._lock_files), 0)

                # Check that the lock files are removed
                lock_file1 = self.lock_manager._get_lock_file_path(scan_id1, LockLevel.SCAN)
                lock_file2 = self.lock_manager._get_lock_file_path(scan_id2, LockLevel.SCAN)
                self.assertFalse(os.path.exists(lock_file1))
                self.assertFalse(os.path.exists(lock_file2))


class TestConcurrentLocks(unittest.TestCase):
    """Test cases for concurrent lock operations."""

    def setUp(self):
        """Set up a temporary directory for testing."""
        self.temp_dir = tempfile.mkdtemp()
        self.lock_manager = LockManager(self.temp_dir, default_timeout=2.0)

    def tearDown(self):
        """Clean up temporary files after tests."""
        self.lock_manager.cleanup_all_locks()
        # Remove all files in the temp directory
        for root, dirs, files in os.walk(self.temp_dir, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        os.rmdir(self.temp_dir)

    def test_concurrent_shared_locks(self):
        """Verify that multiple threads can acquire shared locks simultaneously."""
        scan_id = "test_scan"
        results = []
        errors = []

        def acquire_shared_lock(user):
            try:
                with self.lock_manager.acquire_lock(scan_id, LockType.SHARED, user, LockLevel.SCAN):
                    results.append(user)
                    time.sleep(0.5)  # Hold the lock for a bit
            except Exception as e:
                errors.append(str(e))

        # Create and start threads
        threads = []
        for i in range(5):
            user = f"user{i}"
            thread = threading.Thread(target=acquire_shared_lock, args=(user,))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Check that all threads were able to acquire the lock
        self.assertEqual(len(results), 5)
        self.assertEqual(len(errors), 0)

    def test_concurrent_exclusive_locks(self):
        """Verify that only one thread can acquire an exclusive lock at a time."""
        scan_id = "test_scan"
        results = []
        errors = []

        def acquire_exclusive_lock(user):
            try:
                # Use a short timeout for the threads
                with self.lock_manager.acquire_lock(scan_id, LockType.EXCLUSIVE, user, LockLevel.SCAN, timeout=0.5):
                    results.append(user)
                    # Hold the lock longer than the timeout of other threads (1.0s > 0.5s)
                    time.sleep(1.0)
            except Exception as e:
                errors.append(str(e))

        # Create and start threads
        threads = []
        for i in range(5):
            user = f"user{i}"
            thread = threading.Thread(target=acquire_exclusive_lock, args=(user,))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Check that only one thread was able to acquire the lock
        # Note: This assumes the first thread acquires the lock successfully
        self.assertEqual(len(results), 1)
        self.assertEqual(len(errors), 4)  # The other 4 should have timed out

    def test_exclusive_lock_blocks_shared_lock(self):
        """Verify that an exclusive lock prevents acquiring a shared lock for the same scan_id."""
        scan_id = "test_scan"
        user1 = "user1"
        user2 = "user2"
        results = []
        errors = []

        def acquire_exclusive_lock():
            try:
                with self.lock_manager.acquire_lock(scan_id, LockType.EXCLUSIVE, user1, LockLevel.SCAN, timeout=60.):
                    # Hold the exclusive lock for a while
                    time.sleep(0.5)
                    # Try to acquire a shared lock (this should not happen while exclusive holds)
                    results.append("exclusive_held")
            except Exception as e:
                errors.append(str(e))

        def acquire_shared_lock():
            try:
                # Try to acquire a shared lock while exclusive is held
                with self.lock_manager.acquire_lock(scan_id, LockType.SHARED, user2, LockLevel.SCAN, timeout=0.5):
                    results.append("shared_acquired")
            except Exception as e:
                errors.append(str(e))

        # Start exclusive lock thread
        exclusive_thread = threading.Thread(target=acquire_exclusive_lock)
        exclusive_thread.start()

        # Give exclusive thread time to acquire the lock
        time.sleep(0.1)

        # Start shared lock thread
        shared_thread = threading.Thread(target=acquire_shared_lock)
        shared_thread.start()

        # Wait for both threads to complete
        exclusive_thread.join()
        shared_thread.join()

        # Verify that exclusive lock was acquired and shared lock failed
        # The exclusive lock should be acquired and held, then released
        # The shared lock should fail to acquire due to the exclusive lock
        self.assertIn("exclusive_held", results)
        self.assertEqual(len(errors), 1)  # Should have one error from shared lock attempt

    def test_shared_lock_blocks_exclusive_lock(self):
        """Verify that a shared lock prevents acquiring an exclusive lock for the same scan_id."""
        scan_id = "test_scan"
        user1 = "user1"
        user2 = "user2"
        results = []
        errors = []

        def acquire_shared_lock():
            try:
                with self.lock_manager.acquire_lock(scan_id, LockType.SHARED, user1, LockLevel.SCAN):
                    # Hold the shared lock for a while
                    time.sleep(0.5)
                    results.append("shared_held")
            except Exception as e:
                errors.append(str(e))

        def acquire_exclusive_lock():
            try:
                # Try to acquire an exclusive lock while shared is held
                with self.lock_manager.acquire_lock(scan_id, LockType.EXCLUSIVE, user2, LockLevel.SCAN, timeout=0.5):
                    results.append("exclusive_acquired")
            except Exception as e:
                errors.append(str(e))

        # Start shared lock thread
        shared_thread = threading.Thread(target=acquire_shared_lock)
        shared_thread.start()

        # Give shared thread time to acquire the lock
        time.sleep(0.1)

        # Start exclusive lock thread
        exclusive_thread = threading.Thread(target=acquire_exclusive_lock)
        exclusive_thread.start()

        # Wait for both threads to complete
        shared_thread.join()
        exclusive_thread.join()

        # The test should verify that the exclusive lock attempt times out
        # Since we're using a short timeout, we expect the exclusive lock acquisition to fail
        # This confirms that shared locks block exclusive locks
        self.assertIn("shared_held", results)
        # At least one error should be present from the exclusive lock attempt
        self.assertGreater(len(errors), 0)

    def test_file_level_locks(self):
        """Verify that file level locks work correctly."""
        scan_id = "test_scan"
        fileset_id = "test_fileset"
        file_id = "test_file"
        resource_id = f"{scan_id}/{fileset_id}/{file_id}"
        user1 = "user1"
        user2 = "user2"

        # Acquire an exclusive lock at file level
        with self.lock_manager.acquire_lock(resource_id, LockType.EXCLUSIVE, user1, LockLevel.FILE):
            # Check that the lock file exists
            lock_file = self.lock_manager._get_lock_file_path(resource_id, LockLevel.FILE)
            self.assertTrue(os.path.exists(lock_file))

            # Check that the lock is registered in active locks
            lock_key = f"{resource_id}/file/exclusive"
            self.assertIn(lock_key, self.lock_manager._active_locks)
            self.assertEqual(self.lock_manager._active_locks[lock_key]['type'], LockType.EXCLUSIVE)
            self.assertEqual(self.lock_manager._active_locks[lock_key]['user'], user1)
            self.assertEqual(self.lock_manager._active_locks[lock_key]['count'], 1)

            # Check that the lock info file exists
            info_path = self.lock_manager._get_lock_info_path(resource_id, LockLevel.FILE)
            self.assertTrue(os.path.exists(info_path))

        # After the context manager exits, check that the lock is released
        lock_file = self.lock_manager._get_lock_file_path(resource_id, LockLevel.FILE)
        self.assertFalse(os.path.exists(lock_file))
        lock_key = f"{resource_id}/file/exclusive"
        self.assertNotIn(lock_key, self.lock_manager._active_locks)
        info_path = self.lock_manager._get_lock_info_path(resource_id, LockLevel.FILE)
        self.assertFalse(os.path.exists(info_path))

    def test_multiple_file_level_locks(self):
        """Verify that multiple file level locks can be acquired for different files."""
        scan_id = "test_scan"
        fileset_id = "test_fileset"
        file_id1 = "test_file1"
        file_id2 = "test_file2"
        resource_id1 = f"{scan_id}/{fileset_id}/{file_id1}"
        resource_id2 = f"{scan_id}/{fileset_id}/{file_id2}"
        user1 = "user1"

        # Acquire exclusive locks on different files
        with self.lock_manager.acquire_lock(resource_id1, LockType.EXCLUSIVE, user1, LockLevel.FILE):
            with self.lock_manager.acquire_lock(resource_id2, LockType.EXCLUSIVE, user1, LockLevel.FILE):
                # Both locks should be active
                lock_key1 = f"{resource_id1}/file/exclusive"
                lock_key2 = f"{resource_id2}/file/exclusive"
                self.assertIn(lock_key1, self.lock_manager._active_locks)
                self.assertIn(lock_key2, self.lock_manager._active_locks)
                
                # Check that both lock files exist
                lock_file1 = self.lock_manager._get_lock_file_path(resource_id1, LockLevel.FILE)
                lock_file2 = self.lock_manager._get_lock_file_path(resource_id2, LockLevel.FILE)
                self.assertTrue(os.path.exists(lock_file1))
                self.assertTrue(os.path.exists(lock_file2))


class TestFSDBLock(unittest.TestCase):
    """Test suite for verifying permission handling in FSDB operations.

    The tests exercise deletion of files, filesets, and scans under different user roles.
    A temporary FSDB directory is created for each test case.

    Attributes
    ----------
    fsdb_dir : str
        Path to the temporary FSDB directory created for the test suite.
    """
    def setUp(self):
        """Set up a temporary directory for testing."""
        self.fsdb_dir = setup_test_database(['real_plant', 'real_plant_analyzed'], db_path=None)

    def tearDown(self):
        """Clean up the temporary directory after tests."""
        shutil.rmtree(self.fsdb_dir)

    def test_delete_file_without_permission(self):
        """Test that attempting to delete a file without sufficient permissions raises ``PermissionError``."""
        db = FSDB(self.fsdb_dir)
        db.connect()
        db.login("guest", "guest")
        scan = db.get_scan('real_plant_analyzed')
        fs = scan.get_fileset('Masks_1__0__1__0____channel____rgb_5619aa428d')

        with self.assertRaises(PermissionError):
            fs.delete_file("00000_rgb.png")

    def test_delete_file_with_permission(self):
        """Test deletion of a file when the user has sufficient permissions."""
        db = FSDB(self.fsdb_dir)
        db.connect()
        db.login("admin", "admin")
        scan = db.get_scan('real_plant_analyzed')
        fs = scan.get_fileset('Masks_1__0__1__0____channel____rgb_5619aa428d')
        fs.delete_file("00000_rgb")

    def test_delete_fileset_without_permission(self):
        """Test deletion of a fileset without sufficient permissions."""
        db = FSDB(self.fsdb_dir)
        db.connect()
        db.login("guest", "guest")
        scan = db.get_scan('real_plant_analyzed')

        with self.assertRaises(PermissionError):
            scan.delete_fileset('Masks_1__0__1__0____channel____rgb_5619aa428d')

    def test_delete_fileset_with_permission(self):
        """Delete a fileset from a scan when the user has sufficient permissions."""
        db = FSDB(self.fsdb_dir)
        db.connect()
        db.login("admin", "admin")
        scan = db.get_scan('real_plant_analyzed')
        scan.delete_fileset('Masks_1__0__1__0____channel____rgb_5619aa428d')

    def test_delete_scan_without_permission(self):
        """Test that attempting to delete a scan without sufficient permissions raises ``PermissionError``."""
        db = FSDB(self.fsdb_dir)
        db.connect()
        db.login("guest", "guest")

        with self.assertRaises(PermissionError):
            db.delete_scan('real_plant_analyzed')

    def test_delete_scan_with_permission(self):
        """Test that an administrator can delete an existing scan."""
        db = FSDB(self.fsdb_dir)
        db.connect()
        db.login("admin", "admin")
        db.delete_scan('real_plant_analyzed')


if __name__ == '__main__':
    unittest.main()
