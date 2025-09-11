#!/usr/bin/env python
# -*- coding: utf-8 -*-

import fcntl
import json
import os
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from plantdb.commons.fsdb.lock import LockError
from plantdb.commons.fsdb.lock import LockTimeoutError
from plantdb.commons.fsdb.lock import LockType
from plantdb.commons.fsdb.lock import ScanLockManager


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


class TestScanLockManager(unittest.TestCase):
    """Test cases for the ScanLockManager class."""

    def setUp(self):
        """Set up a temporary directory for testing."""
        self.temp_dir = tempfile.mkdtemp()
        # Create a lock manager with a short default timeout for faster tests
        self.lock_manager = ScanLockManager(self.temp_dir, default_timeout=1.0)

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
        # Test for shared lock
        expected_path = os.path.join(self.temp_dir, ".locks", f"{scan_id}.lock")
        actual_path = self.lock_manager._get_lock_file_path(scan_id)
        self.assertEqual(actual_path, expected_path)

        # Test for exclusive lock
        expected_path = os.path.join(self.temp_dir, ".locks", f"{scan_id}.lock")
        actual_path = self.lock_manager._get_lock_file_path(scan_id)
        self.assertEqual(actual_path, expected_path)

    def test_get_lock_info_path(self):
        """Verify that _get_lock_info_path returns the correct path."""
        scan_id = "test_scan"
        expected_path = os.path.join(self.temp_dir, ".locks", f"{scan_id}.info")
        actual_path = self.lock_manager._get_lock_info_path(scan_id)
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

    def test_write_and_remove_lock_info(self):
        """Verify that _write_lock_info creates a lock info file and _remove_lock_info removes it."""
        scan_id = "test_scan"
        lock_type = LockType.EXCLUSIVE
        user = "test_user"

        # Write lock info
        self.lock_manager._write_lock_info(scan_id, lock_type, user)

        # Check that the file exists
        info_path = self.lock_manager._get_lock_info_path(scan_id)
        self.assertTrue(os.path.exists(info_path))

        # Verify the content of the file
        with open(info_path, 'r') as f:
            info = json.load(f)
            self.assertEqual(info['scan_id'], scan_id)
            self.assertEqual(info['lock_type'], lock_type.value)
            self.assertEqual(info['user'], user)
            self.assertIn('timestamp', info)
            self.assertIn('pid', info)
            self.assertIn('thread_id', info)

        # Remove lock info
        self.lock_manager._remove_lock_info(scan_id)

        # Check that the file no longer exists
        self.assertFalse(os.path.exists(info_path))

    def test_acquire_shared_lock(self):
        """Verify that a shared lock can be acquired."""
        scan_id = "test_scan"
        user = "test_user"

        # Acquire a shared lock
        with self.lock_manager.acquire_lock(scan_id, LockType.SHARED, user):
            # Check that the lock file exists
            lock_file = self.lock_manager._get_lock_file_path(scan_id)
            self.assertTrue(os.path.exists(lock_file))

            # Check that the lock is registered in active locks
            lock_key = f"{scan_id}_shared"
            self.assertIn(lock_key, self.lock_manager._active_locks)
            self.assertEqual(self.lock_manager._active_locks[lock_key]['type'], LockType.SHARED)
            self.assertEqual(self.lock_manager._active_locks[lock_key]['user'], user)
            self.assertEqual(self.lock_manager._active_locks[lock_key]['count'], 1)

            # Check that the lock info file exists
            info_path = self.lock_manager._get_lock_info_path(scan_id)
            self.assertTrue(os.path.exists(info_path))

        # After the context manager exits, check that the lock is released
        lock_file = self.lock_manager._get_lock_file_path(scan_id)
        self.assertFalse(os.path.exists(lock_file))
        lock_key = f"{scan_id}_shared"
        self.assertNotIn(lock_key, self.lock_manager._active_locks)
        info_path = self.lock_manager._get_lock_info_path(scan_id)
        self.assertFalse(os.path.exists(info_path))

    def test_acquire_exclusive_lock(self):
        """Verify that an exclusive lock can be acquired."""
        scan_id = "test_scan"
        user = "test_user"

        # Acquire an exclusive lock
        with self.lock_manager.acquire_lock(scan_id, LockType.EXCLUSIVE, user):
            # Check that the lock file exists
            lock_file = self.lock_manager._get_lock_file_path(scan_id)
            self.assertTrue(os.path.exists(lock_file))

            # Check that the lock is registered in active locks
            lock_key = f"{scan_id}_exclusive"
            self.assertIn(lock_key, self.lock_manager._active_locks)
            self.assertEqual(self.lock_manager._active_locks[lock_key]['type'], LockType.EXCLUSIVE)
            self.assertEqual(self.lock_manager._active_locks[lock_key]['user'], user)
            self.assertEqual(self.lock_manager._active_locks[lock_key]['count'], 1)

            # Check that the lock info file exists
            info_path = self.lock_manager._get_lock_info_path(scan_id)
            self.assertTrue(os.path.exists(info_path))

        # After the context manager exits, check that the lock is released
        lock_file = self.lock_manager._get_lock_file_path(scan_id)
        self.assertFalse(os.path.exists(lock_file))
        lock_key = f"{scan_id}_exclusive"
        self.assertNotIn(lock_key, self.lock_manager._active_locks)
        info_path = self.lock_manager._get_lock_info_path(scan_id)
        self.assertFalse(os.path.exists(info_path))

    def test_acquire_multiple_shared_locks(self):
        """Verify that multiple shared locks can be acquired for the same scan ID."""
        scan_id = "test_scan"
        user1 = "user1"
        user2 = "user2"

        # Acquire a shared lock with user1
        with self.lock_manager.acquire_lock(scan_id, LockType.SHARED, user1):
            # Acquire another shared lock with user2
            with self.lock_manager.acquire_lock(scan_id, LockType.SHARED, user2):
                # Check that the lock is registered in active locks with a count of 2
                lock_key = f"{scan_id}_shared"
                self.assertIn(lock_key, self.lock_manager._active_locks)
                self.assertEqual(self.lock_manager._active_locks[lock_key]['count'], 2)

            # After user2's lock is released, the count should be 1
            lock_key = f"{scan_id}_shared"
            self.assertIn(lock_key, self.lock_manager._active_locks)
            self.assertEqual(self.lock_manager._active_locks[lock_key]['count'], 1)

        # After both locks are released, the lock should be gone
        lock_key = f"{scan_id}_shared"
        self.assertNotIn(lock_key, self.lock_manager._active_locks)

    def test_exclusive_lock_prevents_shared(self):
        """Verify that an exclusive lock prevents acquiring a shared lock."""
        scan_id = "test_scan"
        user1 = "user1"
        user2 = "user2"

        # Acquire an exclusive lock with user1
        with self.lock_manager.acquire_lock(scan_id, LockType.EXCLUSIVE, user1):
            # Try to acquire a shared lock with user2 (should time out)
            with self.assertRaises(LockTimeoutError):
                with self.lock_manager.acquire_lock(scan_id, LockType.SHARED, user2, timeout=0.5):
                    pass

    def test_shared_lock_prevents_exclusive(self):
        """Verify that a shared lock prevents acquiring an exclusive lock."""
        scan_id = "test_scan"
        user1 = "user1"
        user2 = "user2"

        # Acquire a shared lock with user1
        with self.lock_manager.acquire_lock(scan_id, LockType.SHARED, user1):
            # Try to acquire an exclusive lock with user2 (should time out)
            with self.assertRaises(LockTimeoutError):
                with self.lock_manager.acquire_lock(scan_id, LockType.EXCLUSIVE, user2, timeout=0.5):
                    pass

    def test_exclusive_lock_prevents_exclusive(self):
        """Verify that an exclusive lock prevents acquiring another exclusive lock."""
        scan_id = "test_scan"
        user1 = "user1"
        user2 = "user2"

        # Acquire an exclusive lock with user1
        with self.lock_manager.acquire_lock(scan_id, LockType.EXCLUSIVE, user1):
            # Try to acquire another exclusive lock with user2 (should raise a LockError)
            with self.assertRaises(LockError):
                with self.lock_manager.acquire_lock(scan_id, LockType.EXCLUSIVE, user2, timeout=0.5):
                    pass

    def test_lock_timeout(self):
        """Verify that lock acquisition times out after the specified timeout period."""
        scan_id = "test_scan"
        user = "test_user"

        # Create a lock file and hold an exclusive lock on it
        lock_file_path = self.lock_manager._get_lock_file_path(scan_id)
        os.makedirs(os.path.dirname(lock_file_path), exist_ok=True)

        # Open and lock the file outside of the lock manager
        lock_fd = os.open(lock_file_path, os.O_CREAT | os.O_WRONLY)
        fcntl.flock(lock_fd, fcntl.LOCK_EX)

        try:
            # Try to acquire the lock (should time out)
            start_time = time.time()
            with self.assertRaises(LockTimeoutError):
                with self.lock_manager.acquire_lock(scan_id, LockType.EXCLUSIVE, user, timeout=0.5):
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
        status = self.lock_manager.get_lock_status(scan_id)
        self.assertEqual(status, {'exclusive': None, 'shared': []})

    def test_get_lock_status_with_exclusive_lock(self):
        """Verify that get_lock_status returns correct status with an exclusive lock."""
        scan_id = "test_scan"
        user = "test_user"

        # Acquire an exclusive lock
        with self.lock_manager.acquire_lock(scan_id, LockType.EXCLUSIVE, user):
            # Get the lock status
            status = self.lock_manager.get_lock_status(scan_id)

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
        with self.lock_manager.acquire_lock(scan_id, LockType.SHARED, user1):
            with self.lock_manager.acquire_lock(scan_id, LockType.SHARED, user2):
                # Get the lock status
                status = self.lock_manager.get_lock_status(scan_id)

                # Check the status
                self.assertIsNone(status['exclusive'])
                self.assertEqual(len(status['shared']), 1)  # Only one shared lock entry with count=2
                self.assertIn('user', status['shared'][0])
                self.assertIn('timestamp', status['shared'][0])
                self.assertEqual(status['shared'][0]['count'], 2)

    def test_cleanup_all_locks(self):
        """Verify that cleanup_all_locks releases all active locks."""
        scan_id1 = "scan1"
        scan_id2 = "scan2"
        user = "test_user"

        # Acquire multiple locks
        with self.lock_manager.acquire_lock(scan_id1, LockType.SHARED, user):
            with self.lock_manager.acquire_lock(scan_id2, LockType.EXCLUSIVE, user):
                # Check that the locks exist
                self.assertEqual(len(self.lock_manager._active_locks), 2)

                # Clean up all locks
                self.lock_manager.cleanup_all_locks()

                # Check that all locks are released
                self.assertEqual(len(self.lock_manager._active_locks), 0)
                self.assertEqual(len(self.lock_manager._lock_files), 0)

                # Check that the lock files are removed
                lock_file1 = self.lock_manager._get_lock_file_path(scan_id1)
                lock_file2 = self.lock_manager._get_lock_file_path(scan_id2)
                self.assertFalse(os.path.exists(lock_file1))
                self.assertFalse(os.path.exists(lock_file2))


class TestConcurrentLocks(unittest.TestCase):
    """Test cases for concurrent lock operations."""

    def setUp(self):
        """Set up a temporary directory for testing."""
        self.temp_dir = tempfile.mkdtemp()
        self.lock_manager = ScanLockManager(self.temp_dir, default_timeout=2.0)

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
                with self.lock_manager.acquire_lock(scan_id, LockType.SHARED, user):
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
                with self.lock_manager.acquire_lock(scan_id, LockType.EXCLUSIVE, user, timeout=1.0):
                    results.append(user)
                    time.sleep(0.5)  # Hold the lock for a bit
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


if __name__ == '__main__':
    unittest.main()
