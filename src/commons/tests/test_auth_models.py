#!/usr/bin/env python
# -*- coding: utf-8 -*-

import copy
import json
import unittest
from datetime import datetime
from datetime import timedelta

from plantdb.commons.auth.models import Group
from plantdb.commons.auth.models import Permission
from plantdb.commons.auth.models import Role
from plantdb.commons.auth.models import User


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

    def test_role_rank_order(self):
        """Verify the hierarchical rank values are as expected."""
        self.assertEqual(Role.READER.rank, 1)
        self.assertEqual(Role.CONTRIBUTOR.rank, 2)
        self.assertEqual(Role.ADMIN.rank, 3)

        self.assertGreater(Role.ADMIN.rank, Role.CONTRIBUTOR.rank)
        self.assertGreater(Role.CONTRIBUTOR.rank, Role.READER.rank)

    def test_can_assign_logic(self):
        """Check that role assignment authority follows the rank hierarchy."""
        # ADMIN can assign anyone (including themselves)
        self.assertTrue(Role.ADMIN.can_assign(Role.ADMIN))
        self.assertTrue(Role.ADMIN.can_assign(Role.CONTRIBUTOR))
        self.assertTrue(Role.ADMIN.can_assign(Role.READER))

        # CONTRIBUTOR can assign themselves and lower roles
        self.assertTrue(Role.CONTRIBUTOR.can_assign(Role.CONTRIBUTOR))
        self.assertTrue(Role.CONTRIBUTOR.can_assign(Role.READER))
        self.assertFalse(Role.CONTRIBUTOR.can_assign(Role.ADMIN))

        # READER can only assign themselves
        self.assertTrue(Role.READER.can_assign(Role.READER))
        self.assertFalse(Role.READER.can_assign(Role.CONTRIBUTOR))
        self.assertFalse(Role.READER.can_assign(Role.ADMIN))

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
