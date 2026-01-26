#!/usr/bin/env python
# -*- coding: utf-8 -*-
import copy
import json
import logging
import os
import tempfile
import time
import unittest
from datetime import datetime
from datetime import timedelta
from pathlib import Path

from plantdb.commons.auth.manager import GroupManager
from plantdb.commons.auth.manager import UserManager
from plantdb.commons.auth.models import Group
from plantdb.commons.auth.models import Permission
from plantdb.commons.auth.models import Role
from plantdb.commons.auth.models import User
from plantdb.commons.auth.rbac import RBACManager
from plantdb.commons.auth.session import SessionManager


class TestPermission(unittest.TestCase):
    """Test cases for Permission class"""

    def test_permission_constants_are_defined(self):
        """Test that all permission constants are properly defined."""
        # Verify all expected permission constants exist
        self.assertTrue(hasattr(Permission, 'READ'))
        self.assertTrue(hasattr(Permission, 'WRITE'))
        self.assertTrue(hasattr(Permission, 'CREATE'))
        self.assertTrue(hasattr(Permission, 'DELETE'))
        self.assertTrue(hasattr(Permission, 'MANAGE_USERS'))
        self.assertTrue(hasattr(Permission, 'MANAGE_GROUPS'))

    def test_permission_values_are_strings(self):
        """Test that permission constants have expected string values."""
        # Verify permission values are what we expect
        self.assertIsInstance(Permission.READ.value, str)
        self.assertIsInstance(Permission.WRITE.value, str)
        self.assertIsInstance(Permission.CREATE.value, str)
        self.assertIsInstance(Permission.DELETE.value, str)
        self.assertIsInstance(Permission.MANAGE_USERS.value, str)
        self.assertIsInstance(Permission.MANAGE_GROUPS.value, str)


class TestRole(unittest.TestCase):
    """Test cases for Role class"""

    def test_role_constants_are_defined(self):
        """Test that all role constants are properly defined."""
        # Verify all expected role constants exist
        self.assertTrue(hasattr(Role, 'READER'))
        self.assertTrue(hasattr(Role, 'CONTRIBUTOR'))
        self.assertTrue(hasattr(Role, 'ADMIN'))

    def test_permissions_method_returns_list_for_valid_roles(self):
        """Test that permissions method returns appropriate permissions for each role."""
        # Test that permissions method works for all roles
        reader_perms = Role.READER.permissions
        contributor_perms = Role.CONTRIBUTOR.permissions
        admin_perms = Role.ADMIN.permissions

        # Verify return types are lists
        self.assertIsInstance(reader_perms, set)
        self.assertIsInstance(contributor_perms, set)
        self.assertIsInstance(admin_perms, set)


class TestUser(unittest.TestCase):
    """Test cases for User class"""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.sample_user_data = {
            'username': 'testuser',
            'fullname': 'Test User',
            'password_hash': 'hashed_password',
            'roles': {Role.READER},
            'created_at': datetime.now(),
            'is_active': True,
            'failed_attempts': 0,
            'last_failed_attempt': None,
            'locked_until': None
        }
        self.user = User(**self.sample_user_data)

    def test_user_to_dict_serializes_correctly(self):
        """Test that to_dict method properly serializes user data."""
        # Create user with sample data
        user_dict = self.user.to_dict()

        # Verify essential fields are present
        self.assertIn('username', user_dict)
        self.assertIn('fullname', user_dict)
        self.assertIn('roles', user_dict)
        self.assertIn('is_active', user_dict)
        self.assertEqual(user_dict['username'], 'testuser')

    def test_user_from_dict_deserializes_correctly(self):
        """Test that from_dict method properly creates User from dictionary."""
        # Create user from dictionary
        user = User.from_dict(self.sample_user_data)

        # Verify user properties
        self.assertEqual(user.username, self.sample_user_data['username'])
        self.assertEqual(user.fullname, self.sample_user_data['fullname'])
        self.assertEqual(user.roles, self.sample_user_data['roles'])
        self.assertTrue(user.is_active)

    def test_user_to_json_returns_valid_json_string(self):
        """Test that to_json method returns valid JSON string."""
        user = User(**self.sample_user_data)
        json_str = user.to_json()

        # Verify it's valid JSON
        self.assertIsInstance(json_str, str)
        parsed_data = json.loads(json_str)
        self.assertIsInstance(parsed_data, dict)
        self.assertEqual(parsed_data['username'], 'testuser')

    def test_user_from_json_creates_user_from_json_string(self):
        """Test that from_json method creates User from JSON string."""
        user = User(**self.sample_user_data)
        json_str = user.to_json()

        # Create new user from JSON
        new_user = User.from_json(json_str)

        # Verify users are equivalent
        self.assertEqual(user.username, new_user.username)
        self.assertEqual(user.fullname, new_user.fullname)
        self.assertEqual(user.roles, new_user.roles)

    def test_is_locked_out_returns_true_when_locked(self):
        """Test that _is_locked_out returns True when user is currently locked."""
        self.user.locked_until = datetime.now() + timedelta(minutes=1)
        self.assertTrue(self.user._is_locked_out())

    def test_is_locked_out_returns_false_when_lock_expired(self):
        """Test that _is_locked_out returns False when lock has expired."""
        # Set lock time in past
        self.user.locked_until = datetime.now() - timedelta(hours=1)
        self.assertFalse(self.user._is_locked_out())

    def test_record_failed_attempt_increments_counter(self):
        """Test that _record_failed_attempt properly increments failed attempts."""
        user = User(**self.sample_user_data)
        initial_attempts = copy.copy(user.failed_attempts)

        # Record failed attempt
        user._record_failed_attempt()

        # Verify counter incremented and timestamp updated
        self.assertEqual(user.failed_attempts, initial_attempts + 1)
        self.assertIsNotNone(user.last_failed_attempt)


class TestGroup(unittest.TestCase):
    """Test cases for Group class"""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.group = Group(
            name="testgroup",
            users=set(),
            created_by="admin",
            description="Test group",
            created_at=datetime.now(),
        )

    def test_add_user_adds_user_to_group(self):
        """Test that add_user method successfully adds user to group."""
        # Initially group should be empty
        self.assertEqual(len(self.group.users), 0)

        # Add user to group
        self.group.add_user("testuser")

        # Verify user was added
        self.assertEqual(len(self.group.users), 1)
        self.assertIn("testuser", self.group.users)

    def test_add_user_prevents_duplicate_users(self):
        """Test that add_user prevents adding the same user twice."""
        # Add user twice
        self.group.add_user("testuser")
        self.group.add_user("testuser")

        # User should only appear once
        self.assertEqual(len(self.group.users), 1)

    def test_remove_user_removes_user_from_group(self):
        """Test that remove_user method successfully removes user from group."""
        # Add user first
        self.group.add_user("testuser")
        self.assertIn("testuser", self.group.users)

        # Remove user
        self.group.remove_user("testuser")

        # Verify user was removed
        self.assertNotIn("testuser", self.group.users)

    def test_remove_user_handles_nonexistent_user(self):
        """Test that remove_user gracefully handles removing non-existent user."""
        # Try to remove user that doesn't exist
        initial_count = len(self.group.users)
        self.group.remove_user("nonexistent")

        # Group should remain unchanged
        self.assertEqual(len(self.group.users), initial_count)

    def test_has_user_returns_true_for_existing_user(self):
        """Test that has_user returns True when user exists in group."""
        # Add user to group
        self.group.add_user("testuser")

        # Check if user exists
        self.assertTrue(self.group.has_user("testuser"))

    def test_has_user_returns_false_for_nonexistent_user(self):
        """Test that has_user returns False when user doesn't exist in group."""
        # Check for non-existent user
        self.assertFalse(self.group.has_user("nonexistent"))


class TestUserManager(unittest.TestCase):
    """Test cases for UserManager class"""

    @staticmethod
    def _temp_user_file():
        return tempfile.NamedTemporaryFile(mode='w', delete=False, prefix='users_', suffix='.json').name

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create temporary file for testing
        self.temp_file = self._temp_user_file()

        self.max_login_attempts = 3
        self.lockout_duration = 1800  # 30 minutes
        self.user_manager = UserManager(self.temp_file,
                                        max_login_attempts=self.max_login_attempts,
                                        lockout_duration=self.lockout_duration)
        self.user_dict = dict(
            username="testuser",
            fullname="test user",
            password="testpassword",
            roles={Role.READER},
        )
        self.user_manager.create(**self.user_dict)

    def tearDown(self):
        """Clean up test fixtures after each test method."""
        # Remove temporary file
        if os.path.exists(self.temp_file):
            os.unlink(self.temp_file)

    def test_load_users_loads_users_from_file(self):
        """Test that _load_users properly loads users from JSON file."""
        # Create a new user manager (without testuser)
        user_manager = UserManager(self._temp_user_file(),
                                   max_login_attempts=self.max_login_attempts,
                                   lockout_duration=self.lockout_duration)
        # Override user_file with the one create a setUp that contains the 'testuser'
        user_manager.users_file = self.user_manager.users_file
        # Load users
        user_manager._load_users()
        self.assertIn("testuser", self.user_manager.users)

    def test_save_users_saves_users_to_file(self):
        """Test that _save_users properly saves users to JSON file."""
        # Add a user and save
        test_user = User(
            username="testuserB",
            fullname="test user",
            password_hash="hashed_password",
            roles={Role.READER},
            created_at=datetime.now(),
        )
        self.user_manager.users[test_user.username] = test_user
        self.user_manager._save_users()
        json_users = json.loads(Path(self.user_manager.users_file).read_text())
        self.assertTrue(any("testuserB" in user['username'] for user in json_users))

    def test_hash_password_returns_hashed_string(self):
        """Test that _hash_password returns a hashed version of password."""
        password = "testpassword"
        hashed = self.user_manager._hash_password(password)

        # Verify hash is different from original and is a string
        self.assertIsInstance(hashed, str)
        self.assertNotEqual(hashed, password)
        self.assertGreater(len(hashed), len(password))

    def test_exists_returns_true_for_existing_user(self):
        """Test that exists method returns True for existing users."""
        # Check if user exists
        self.assertTrue(self.user_manager.exists("testuser"))

    def test_exists_returns_false_for_nonexistent_user(self):
        """Test that exists method returns False for non-existing users."""
        self.assertFalse(self.user_manager.exists("nonexistent"))

    def test_create_creates_new_user_successfully(self):
        """Test that create method successfully creates new user."""
        # Create user
        self.user_manager.create(
            username="newuser",
            fullname="New User",
            password="password123",
            roles={Role.READER}
        )

        # Verify user was created
        self.assertIsNotNone(self.user_manager.users.get("newuser"))
        user = self.user_manager.get_user("newuser")
        self.assertEqual(user.username, "newuser")
        self.assertEqual(user.fullname, "New User")
        self.assertSetEqual(user.roles, {Role.READER})

    def test_get_user_returns_existing_user(self):
        """Test that get_user returns the correct user object."""
        retrieved_user = self.user_manager.get_user("testuser")
        self.assertIsInstance(retrieved_user, User)
        self.assertEqual(retrieved_user.username, "testuser")

    def test_get_user_returns_none_for_nonexistent_user(self):
        """Test that get_user returns None for non-existent users."""
        user = self.user_manager.get_user("nonexistent")
        self.assertIsNone(user)

    def test_is_locked_out_checks_user_lockout_status(self):
        """Test that is_locked_out properly checks user lockout status."""
        user = self.user_manager.get_user("testuser")
        user.locked_until = datetime.now() + timedelta(hours=1)
        # Check lockout status
        is_locked = self.user_manager.is_locked_out("testuser")
        self.assertTrue(is_locked)

    def test_is_active_checks_user_active_status(self):
        """Test that is_active properly checks if user is active."""
        # Check active status
        is_active = self.user_manager.is_active("testuser")
        self.assertTrue(is_active)

    def test_validate_user_password_success_case(self):
        """Test successful password validation scenario."""
        # Validate password
        is_valid = self.user_manager.validate_user_password("testuser", "testpassword")
        self.assertTrue(is_valid)

    def test_validate_user_password_failure_case(self):
        """Test failed password validation scenario."""
        # Validate with wrong password
        is_valid = self.user_manager.validate_user_password("testuser", "wrong_password")
        self.assertFalse(is_valid)


class TestGroupManager(unittest.TestCase):
    """Test cases for GroupManager class"""

    @staticmethod
    def _temp_group_file():
        return tempfile.NamedTemporaryFile(mode='w', delete=False, prefix='users_', suffix='.json').name

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create temporary file for testing
        self.temp_file = self._temp_group_file()

        self.group_manager = GroupManager(self.temp_file)
        self.group_dict = dict(
            name="test_group",
            users={"guest"},
            creator="guest"
        )
        self.group_manager.create_group(**self.group_dict)

    def tearDown(self):
        """Clean up test fixtures after each test method."""
        if os.path.exists(self.temp_file):
            os.unlink(self.temp_file)

    def test_load_groups_loads_from_file(self):
        """Test that _load_groups properly loads groups from JSON file."""
        # Create a new group manager (without test_group)
        group_manager = GroupManager(self._temp_group_file())
        # Override groups_file with the one create a setUp that contains the 'test_group'
        group_manager.groups_file = self.group_manager.groups_file
        # Load groups
        self.group_manager._load_groups()
        self.assertIn("test_group", self.group_manager.groups)

    def test_create_group_creates_new_group(self):
        """Test that create_group successfully creates a new group."""
        self.group_manager.create_group(
            name="newgroup",
            creator="admin",
            users={"guest"},
            description="New test group"
        )

        self.assertIsNotNone(self.group_manager.groups.get("newgroup"))
        group = self.group_manager.get_group("newgroup")
        self.assertEqual(group.name, "newgroup")
        self.assertEqual(group.created_by, 'admin')

    def test_get_group_returns_existing_group(self):
        """Test that get_group returns correct group object."""
        group = self.group_manager.get_group("test_group")
        self.assertIsInstance(group, Group)
        self.assertEqual(group.name, self.group_dict["name"])
        self.assertSetEqual(group.users, self.group_dict["users"])
        self.assertEqual(group.created_by, self.group_dict["creator"])

    def test_get_group_returns_none_for_nonexistent_group(self):
        """Test that get_group returns None for non-existent groups."""
        group = self.group_manager.get_group("nonexistent")
        self.assertIsNone(group)

    def test_delete_group_removes_group(self):
        """Test that delete_group successfully removes a group."""
        # Delete group
        result = self.group_manager.delete_group("test_group")
        self.assertTrue(result)

    def test_group_exists_returns_true_for_existing_group(self):
        """Test that group_exists returns True for existing groups."""
        exists = self.group_manager.group_exists("test_group")
        self.assertTrue(exists)

    def test_group_exists_returns_false_for_nonexistent_group(self):
        """Test that group_exists returns False for non-existent groups."""
        exists = self.group_manager.group_exists("nonexistent")
        self.assertFalse(exists)


class TestRBACManager(unittest.TestCase):
    """Test cases for RBACManager class"""

    @staticmethod
    def _temp_user_file():
        return tempfile.NamedTemporaryFile(mode='w', delete=False, prefix='users_', suffix='.json').name

    @staticmethod
    def _temp_group_file():
        return tempfile.NamedTemporaryFile(mode='w', delete=False, prefix='groups_', suffix='.json').name

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create temporary files for testing
        self.user_file = self._temp_user_file()
        self.group_file = self._temp_group_file()

        # Initialize real UserManager and GroupManager
        self.user_manager = UserManager(self.user_file)
        self.group_manager = GroupManager(self.group_file)

        # Create a test user with specific roles
        self.user_manager.create(
            username="testuser",
            fullname="Test User",
            password="password123",
            roles={Role.READER, Role.CONTRIBUTOR}
        )

        # Initialize the RBACManager with real dependencies
        self.rbac_manager = RBACManager()
        self.rbac_manager.users = self.user_manager
        self.rbac_manager.groups = self.group_manager

    def tearDown(self):
        """Clean up test fixtures after each test method."""
        # Remove temporary files
        if os.path.exists(self.user_file):
            os.unlink(self.user_file)
        if os.path.exists(self.group_file):
            os.unlink(self.group_file)

    def test_get_user_permissions_returns_permissions_for_user_roles(self):
        """Test that get_user_permissions returns correct permissions based on user roles."""
        # Get permissions for testuser which has READER and CONTRIBUTOR roles
        user = self.user_manager.get_user("testuser")
        permissions = self.rbac_manager.get_user_permissions(user)

        # Verify that permissions include those from both roles
        self.assertIn(Permission.READ, permissions)  # From READER role
        self.assertIn(Permission.WRITE, permissions)  # From CONTRIBUTOR role

    def test_has_permission_returns_true_for_authorized_user(self):
        """Test that has_permission returns True when user has required permission."""
        # Test if user has READ permission (which they should as a READER)
        user = self.user_manager.get_user("testuser")
        has_perm = self.rbac_manager.has_permission(user, Permission.READ)
        self.assertTrue(has_perm)

    def test_has_permission_returns_false_for_unauthorized_user(self):
        """Test that has_permission returns False when user lacks required permission."""
        # Test if user has DELETE permission (which they should not have)
        user = self.user_manager.get_user("testuser")
        has_perm = self.rbac_manager.has_permission(user, Permission.DELETE)
        self.assertFalse(has_perm)

    def test_is_guest_user_identifies_guest_correctly(self):
        """Test that is_guest_user correctly identifies guest users."""
        # User "guest" should be identified as a guest user
        user = self.user_manager.get_user(self.user_manager.GUEST_USERNAME)
        is_guest = self.rbac_manager.is_guest_user(user)
        self.assertTrue(is_guest)

        # Regular user should not be identified as a guest
        user = self.user_manager.get_user("testuser")
        is_guest = self.rbac_manager.is_guest_user(user)
        self.assertFalse(is_guest)

    def test_can_manage_groups_checks_manage_groups_permission(self):
        """Test that can_manage_groups checks for MANAGE_GROUPS permission."""
        # Create an admin user with MANAGE_GROUPS permission
        self.user_manager.create(
            username="adminuser",
            fullname="Admin User",
            password="adminpass",
            roles={Role.ADMIN}  # ADMIN role includes MANAGE_GROUPS permission
        )

        # Admin user should be able to manage groups
        adminuser = self.user_manager.get_user("admin")
        can_manage = self.rbac_manager.can_manage_groups(adminuser)
        self.assertTrue(can_manage)

        # Regular user should not be able to manage groups
        testuser = self.user_manager.get_user("testuser")
        can_manage = self.rbac_manager.can_manage_groups(testuser)
        self.assertFalse(can_manage)

    def test_create_group_with_permission(self):
        """Test that create_group delegates to GroupManager after permission check."""
        # Create a group using the admin user
        adminuser = self.user_manager.get_user("admin")
        group = self.rbac_manager.create_group(
            adminuser, "testgroup", {"admin"}, "Test group"
        )

        # Verify group was created
        self.assertIsNotNone(group)
        self.assertEqual(group.name, "testgroup")
        self.assertEqual(group.created_by, "admin")
        self.assertEqual(group.description, "Test group")

        # Verify group exists in group manager
        self.assertTrue(self.group_manager.group_exists("testgroup"))

    def test_create_group_without_permission(self):
        """Test that create_group raises exception when user lacks permission."""
        # Attempt to create a group with a regular user (who lacks MANAGE_GROUPS permission)
        user = self.user_manager.get_user("guest")
        group = self.rbac_manager.create_group(
            user, "testgroup", {"testuser"}, "Test group"
        )
        self.assertIsNone(group)


class TestSessionManager(unittest.TestCase):
    """Test cases for SessionManager class"""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.session_manager = SessionManager(session_timeout=3600)  # 1 hour timeout

    def test_init_creates_empty_sessions_dict(self):
        """Test that SessionManager initializes with empty sessions dictionary."""
        # Verify initial state is correct
        self.assertEqual(len(self.session_manager.sessions), 0)
        self.assertEqual(self.session_manager.session_timeout, 3600)
        self.assertIsNotNone(self.session_manager.logger)

    def test_user_has_session_returns_consistent_id(self):
        """Test that _user_has_session returns consistent session IDs for same user."""
        # Test session ID generation
        session_id1 = self.session_manager._user_has_session("testuser")
        session_id2 = self.session_manager._user_has_session("testuser")

        # Session IDs should be consistent for same user at same time
        self.assertEqual(session_id1, session_id2)

    def test_create_session_stores_session_data(self):
        """Test that create_session properly stores session with expiry time."""
        # Create session
        session_id = self.session_manager.create_session("testuser")

        # Verify session was created
        self.assertIn(session_id, self.session_manager.sessions)
        session_data = self.session_manager.sessions[session_id]
        self.assertEqual(session_data['username'], "testuser")

        # Should have created_at timestamp
        self.assertIn('created_at', session_data)
        self.assertIsInstance(session_data['created_at'], datetime)

    def test_validate_session_returns_true_for_valid_session(self):
        """Test that validate_session returns True for non-expired sessions."""
        # Create session first
        session_id = self.session_manager.create_session("testuser")

        # Validate immediately (should be valid)
        is_valid = self.session_manager.validate_session(session_id)
        self.assertTrue(is_valid)

    def test_validate_session_returns_false_for_expired_session(self):
        """Test that validate_session returns False for expired sessions."""
        # Create a session with a very short timeout
        temp_session_manager = SessionManager(session_timeout=0.1)  # 0.1 second timeout
        session_id = temp_session_manager.create_session("testuser")

        # Wait for session to expire
        time.sleep(0.2)

        # Validate after timeout period
        is_valid = temp_session_manager.validate_session(session_id)
        self.assertFalse(is_valid)

    def test_validate_session_returns_false_for_invalid_session_id(self):
        """Test that validate_session returns False for non-existent session IDs."""
        is_valid = self.session_manager.validate_session("invalid_session_id")
        self.assertFalse(is_valid)

    def test_invalidate_session_removes_session(self):
        """Test that invalidate_session removes session from storage."""
        # Create session first
        session_id = self.session_manager.create_session("testuser")
        self.assertIn(session_id, self.session_manager.sessions)

        # Invalidate session
        self.session_manager.invalidate_session(session_id)
        self.assertNotIn(session_id, self.session_manager.sessions)

    def test_cleanup_expired_sessions_removes_old_sessions(self):
        """Test that cleanup_expired_sessions removes only expired sessions."""
        # Create a session manager with a very short timeout for testing
        temp_session_manager = SessionManager(session_timeout=0.5)  # 0.5 second timeout

        # Create first session
        session1_id = temp_session_manager.create_session("user1")

        # Wait for a moment
        time.sleep(0.6)  # Wait for first session to expire

        # Create second session
        session2_id = temp_session_manager.create_session("user2")

        # Cleanup expired sessions
        temp_session_manager.cleanup_expired_sessions()

        # Only expired session should be removed
        self.assertNotIn(session1_id, temp_session_manager.sessions)
        self.assertIn(session2_id, temp_session_manager.sessions)

    def test_session_username_returns_correct_username(self):
        """Test that session_username returns the correct username for valid session."""
        # Create session
        session_id = self.session_manager.create_session("testuser")

        # Get username from session
        username = self.session_manager.session_username(session_id)
        self.assertEqual(username, "testuser")

    def test_session_username_returns_none_for_invalid_session(self):
        """Test that session_username returns None for invalid session ID."""
        username = self.session_manager.session_username("invalid_session")
        self.assertIsNone(username)


if __name__ == '__main__':
    # Configure logging to suppress logs during testing
    logging.getLogger().setLevel(logging.CRITICAL)

    # Run the tests
    unittest.main(verbosity=2)
