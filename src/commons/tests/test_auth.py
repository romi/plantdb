#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import logging
import os
import tempfile
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
        """Test that _hash_password returns a hashed version of the password."""
        password = "testpassword"
        hashed = self.user_manager._hash_password(password)

        # Verify hash is different from the original and is a string
        self.assertIsInstance(hashed, str)
        self.assertNotEqual(hashed, password)
        self.assertGreater(len(hashed), len(password))

    def test_exists_returns_true_for_existing_user(self):
        """Test that exists method returns True for existing users."""
        # Check if the user exists
        self.assertTrue(self.user_manager.exists("testuser"))

    def test_exists_returns_false_for_nonexistent_user(self):
        """Test that exists method returns False for non-existing users."""
        self.assertFalse(self.user_manager.exists("nonexistent"))

    def test_create_creates_new_user_successfully(self):
        """Test that create method successfully creates a new user."""
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
        """Test that is_locked_out properly checks the user lockout status."""
        user = self.user_manager.get_user("testuser")
        user.locked_until = datetime.now() + timedelta(hours=1)
        # Check lockout status
        is_locked = self.user_manager.is_locked_out("testuser")
        self.assertTrue(is_locked)

    def test_is_active_checks_user_active_status(self):
        """Test that is_active properly checks if the user is active."""
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
        # Validate with the wrong password
        is_valid = self.user_manager.validate_user_password("testuser", "wrong_password")
        self.assertFalse(is_valid)

    def test_update_password_success_case(self):
        """Test successful password update scenario."""
        # Update password
        self.user_manager.update_password("testuser", "testpassword", "newpassword")
        
        # Verify the password was updated by validating the new password
        is_valid = self.user_manager.validate_user_password("testuser", "newpassword")
        self.assertTrue(is_valid)
        
        # Verify old password is no longer valid
        is_valid_old = self.user_manager.validate_user_password("testuser", "testpassword")
        self.assertFalse(is_valid_old)

    def test_update_password_invalid_current_password(self):
        """Test password update fails when current password is incorrect."""
        # Try to update password with wrong current password
        self.user_manager.update_password("testuser", "wrongpassword", "newpassword")
        
        # Verify the password was NOT updated by validating the old password still works
        is_valid_old = self.user_manager.validate_user_password("testuser", "testpassword")
        self.assertTrue(is_valid_old)
        
        # Verify new password is not valid
        is_valid_new = self.user_manager.validate_user_password("testuser", "newpassword")
        self.assertFalse(is_valid_new)

    def test_update_password_nonexistent_user(self):
        """Test password update fails for nonexistent user."""
        # Try to update password for non-existent user
        self.user_manager.update_password("nonexistent", "testpassword", "newpassword")
        
        # Verify the password was NOT updated by validating the old password still works
        is_valid_old = self.user_manager.validate_user_password("testuser", "testpassword")
        self.assertTrue(is_valid_old)


class TestGroupManager(unittest.TestCase):
    """Test cases for GroupManager class"""

    @staticmethod
    def _temp_group_file():
        return tempfile.NamedTemporaryFile(mode='w', delete=False, prefix='users_', suffix='.json').name

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create a temporary file for testing
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
        """Test that _load_groups properly loads groups from a JSON file."""
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
        # Get permissions for the testuser which has READER and CONTRIBUTOR roles
        user = self.user_manager.get_user("testuser")
        permissions = self.rbac_manager.get_user_permissions(user)

        # Verify that permissions include those from both roles
        self.assertIn(Permission.READ, permissions)  # From READER role
        self.assertIn(Permission.WRITE, permissions)  # From CONTRIBUTOR role

    def test_has_permission_returns_true_for_authorized_user(self):
        """Test that has_permission returns True when the user has required permission."""
        # Test if the user has READ permission (which they should as a READER)
        user = self.user_manager.get_user("testuser")
        has_perm = self.rbac_manager.has_permission(user, Permission.READ)
        self.assertTrue(has_perm)

    def test_has_permission_returns_false_for_unauthorized_user(self):
        """Test that has_permission returns False when the user lacks required permission."""
        # Test if the user has DELETE permission (which they should not have)
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
        """Test that create_group delegates to GroupManager after a permission check."""
        # Create a group using the admin user
        adminuser = self.user_manager.get_user("admin")
        group = self.rbac_manager.create_group(
            adminuser, "testgroup", {"admin"}, "Test group"
        )

        # Verify that the group was created
        self.assertIsNotNone(group)
        self.assertEqual(group.name, "testgroup")
        self.assertEqual(group.created_by, "admin")
        self.assertEqual(group.description, "Test group")

        # Verify that the group exists in the group manager
        self.assertTrue(self.group_manager.group_exists("testgroup"))

    def test_create_group_without_permission(self):
        """Test that create_group raises an exception when the user lacks permission."""
        # Attempt to create a group with a regular user (who lacks MANAGE_GROUPS permission)
        user = self.user_manager.get_user("guest")
        group = self.rbac_manager.create_group(
            user, "testgroup", {"testuser"}, "Test group"
        )
        self.assertIsNone(group)


if __name__ == '__main__':
    # Configure logging to suppress logs during testing
    logging.getLogger().setLevel(logging.CRITICAL)

    # Run the tests
    unittest.main(verbosity=2)
