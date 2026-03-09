#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
from datetime import datetime
from datetime import timezone

from plantdb.commons.auth.models import Permission
from plantdb.commons.auth.models import Role
from plantdb.commons.auth.models import TokenUser


class TestTokenUserPermissions(unittest.TestCase):
    """Test cases for TokenUser permission management features."""

    def setUp(self):
        """Set up common test fixtures."""
        self.now = datetime.now(timezone.utc)

    def test_get_permissions_for_dataset_exact_match(self):
        """Test getting permissions for exact dataset name match."""
        # Create TokenUser
        user = TokenUser(
            username="alice",
            fullname="Alice Smith",
            password_hash="hash",
            roles={Role.CONTRIBUTOR},
            created_at=self.now,
            dataset_permissions={'dataset_A': {Permission.READ, Permission.WRITE}}
        )

        # Get permissions for exact match
        perms = user.get_permissions_for_dataset('dataset_A')

        # Should return matching permissions
        self.assertEqual(len(perms), 2)
        self.assertIn(Permission.READ, perms)
        self.assertIn(Permission.WRITE, perms)

    def test_get_permissions_for_dataset_wildcard_match(self):
        """Test getting permissions with wildcard pattern matching."""
        # Create TokenUser with wildcard pattern
        user = TokenUser(
            username="alice",
            fullname="Alice Smith",
            password_hash="hash",
            roles={Role.CONTRIBUTOR},
            created_at=self.now,
            dataset_permissions={'dataset_*': {Permission.READ}}
        )

        # Get permissions for dataset matching pattern
        perms = user.get_permissions_for_dataset('dataset_test')

        # Should match wildcard pattern
        self.assertIn(Permission.READ, perms)

    def test_get_permissions_for_dataset_no_match(self):
        """Test getting permissions for non-matching dataset returns empty set."""
        # Create TokenUser
        user = TokenUser(
            username="alice",
            fullname="Alice Smith",
            password_hash="hash",
            roles={Role.READER},
            created_at=self.now,
            dataset_permissions={'dataset_A': {Permission.READ}}
        )

        # Get permissions for non-matching dataset
        perms = user.get_permissions_for_dataset('dataset_B')

        # Should return empty set
        self.assertEqual(len(perms), 0)

    def test_get_permissions_for_dataset_intersection_with_user_permissions(self):
        """Test that returned permissions are intersection of dataset and user permissions."""
        # Create TokenUser with limited user permissions
        user = TokenUser(
            username="alice",
            fullname="Alice Smith",
            password_hash="hash",
            roles={Role.READER},
            created_at=self.now,
            dataset_permissions={'dataset_A': {Permission.READ, Permission.WRITE}}  # Dataset allows more
        )

        # Get permissions for dataset
        perms = user.get_permissions_for_dataset('dataset_A')

        # Should only return intersection (READ)
        self.assertEqual(len(perms), 1)
        self.assertIn(Permission.READ, perms)
        self.assertNotIn(Permission.WRITE, perms)

    def test_get_permissions_for_dataset_multiple_patterns(self):
        """Test getting permissions when multiple patterns match."""
        # Create TokenUser with overlapping patterns
        user = TokenUser(
            username="alice",
            fullname="Alice Smith",
            password_hash="hash",
            roles={Role.CONTRIBUTOR},
            created_at=self.now,
            dataset_permissions={
                'dataset_*': {Permission.READ},  # This should match dataset_1
                'dataset_1': {Permission.WRITE}  # This should also match dataset_1
            }
        )

        # Get permissions for dataset_1
        perms = user.get_permissions_for_dataset('dataset_1')

        self.assertEqual(len(perms), 2)

    def test_get_permissions_for_dataset_complex_pattern(self):
        """Test getting permissions with complex pattern matching."""
        # Create TokenUser with complex pattern
        user = TokenUser(
            username="alice",
            fullname="Alice Smith",
            password_hash="hash",
            roles={Role.CONTRIBUTOR},
            created_at=self.now,
            dataset_permissions={'proj_*_scan': {Permission.READ, Permission.WRITE}}
        )

        # Get permissions for dataset matching pattern
        perms = user.get_permissions_for_dataset('proj_2023_scan')

        # Should match pattern
        self.assertIn(Permission.READ, perms)
        self.assertIn(Permission.WRITE, perms)

    def test_get_permissions_for_dataset_empty_permissions(self):
        """Test getting permissions when dataset has empty permission set."""
        # Create TokenUser with empty permissions
        user = TokenUser(
            username="alice",
            fullname="Alice Smith",
            password_hash="hash",
            roles={Role.CONTRIBUTOR},
            created_at=self.now,
            dataset_permissions={'dataset_A': set()}
        )

        # Get permissions for dataset
        perms = user.get_permissions_for_dataset('dataset_A')

        # Should return empty set
        self.assertEqual(len(perms), 0)

    def test_get_permissions_for_dataset_no_dataset_permissions(self):
        """Test getting permissions when user has no dataset permissions."""
        # Create TokenUser with empty dataset permissions
        with self.assertRaises(TypeError):
            user = TokenUser(
                username="alice",
                fullname="Alice Smith",
                password_hash="hash",
                roles={Role.READER},
                created_at=self.now,
                dataset_permissions={}
            )

    def test_get_permissions_for_dataset_with_manage_users_permission(self):
        """Test getting permissions for dataset with MANAGE_USERS permission."""
        # Create TokenUser with admin role which has MANAGE_USERS permission
        user = TokenUser(
            username="alice",
            fullname="Alice Smith",
            password_hash="hash",
            roles={Role.CONTRIBUTOR},
            created_at=self.now,
            dataset_permissions={'dataset_A': {Permission.READ, Permission.MANAGE_USERS}}
        )

        # Get permissions for dataset
        perms = user.get_permissions_for_dataset('dataset_A')

        # Should return intersection with user permissions 
        self.assertIn(Permission.READ, perms)
        # MANAGE_USERS should not be returned because user's permissions are intersection
        self.assertEqual(len(perms), 1)  # Only READ permission should be returned


if __name__ == '__main__':
    unittest.main()