#!/usr/bin/env python
# -*- coding: utf-8 -*-

import copy
import json
import unittest
from datetime import datetime
from datetime import timedelta
from datetime import timezone

from plantdb.commons.auth.models import Group
from plantdb.commons.auth.models import Permission
from plantdb.commons.auth.models import Role
from plantdb.commons.auth.models import TokenUser
from plantdb.commons.auth.models import User
from plantdb.commons.auth.models import dataset_perm_to_str
from plantdb.commons.auth.models import parse_dataset_perm


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

    def test_permission_values(self):
        """Test that all Permission enum members have correct values."""
        # Verify each permission has the expected string value
        self.assertEqual(Permission.READ.value, "read")
        self.assertEqual(Permission.WRITE.value, "write")
        self.assertEqual(Permission.CREATE.value, "create")
        self.assertEqual(Permission.DELETE.value, "delete")
        self.assertEqual(Permission.MANAGE_USERS.value, "manage_users")
        self.assertEqual(Permission.MANAGE_GROUPS.value, "manage_groups")

    def test_permission_equality_with_permission(self):
        """Test equality comparison between Permission instances."""
        # Two instances of the same permission should be equal
        perm1 = Permission.READ
        perm2 = Permission.READ
        self.assertEqual(perm1, perm2)

        # Different permissions should not be equal
        perm3 = Permission.WRITE
        self.assertNotEqual(perm1, perm3)

    def test_permission_equality_with_string(self):
        """Test equality comparison between Permission and string."""
        # Permission should equal its value string
        self.assertEqual(Permission.READ, "read")

        # Permission should equal its name string (converted)
        self.assertEqual(Permission.READ, "READ")

        # Permission should not equal unrelated string
        self.assertNotEqual(Permission.READ, "write")

        # Permission should not equal invalid string
        self.assertNotEqual(Permission.READ, "invalid")

    def test_permission_equality_with_other_types(self):
        """Test equality comparison with non-Permission, non-string objects."""
        # Should return False for incompatible types
        self.assertNotEqual(Permission.READ, 123)
        self.assertNotEqual(Permission.READ, None)
        self.assertNotEqual(Permission.READ, [])
        self.assertNotEqual(Permission.READ, {})

    def test_permission_hash(self):
        """Test that Permission instances can be hashed and used in sets."""
        # Permissions should be hashable
        perm_set = {Permission.READ, Permission.WRITE, Permission.READ}

        # Set should contain only unique permissions
        self.assertEqual(len(perm_set), 2)
        self.assertIn(Permission.READ, perm_set)
        self.assertIn(Permission.WRITE, perm_set)

    def test_from_string_with_value(self):
        """Test from_string() with permission value strings."""
        # Should correctly parse lowercase value
        perm = Permission.from_string("read")
        self.assertEqual(perm, Permission.READ)

        # Test all permission values
        self.assertEqual(Permission.from_string("write"), Permission.WRITE)
        self.assertEqual(Permission.from_string("create"), Permission.CREATE)
        self.assertEqual(Permission.from_string("delete"), Permission.DELETE)
        self.assertEqual(Permission.from_string("manage_users"), Permission.MANAGE_USERS)
        self.assertEqual(Permission.from_string("manage_groups"), Permission.MANAGE_GROUPS)

    def test_from_string_with_name(self):
        """Test from_string() with permission name strings."""
        # Should correctly parse uppercase name
        perm = Permission.from_string("READ")
        self.assertEqual(perm, Permission.READ)

        # Should handle mixed case by converting to uppercase
        perm2 = Permission.from_string("WrItE")
        self.assertEqual(perm2, Permission.WRITE)

    def test_from_string_with_qualified_name(self):
        """Test from_string() with fully qualified permission names."""
        # Should strip the "Permission." prefix
        perm = Permission.from_string("Permission.READ")
        self.assertEqual(perm, Permission.READ)

        # Should handle nested dots (takes last part)
        perm2 = Permission.from_string("plantdb.commons.Permission.WRITE")
        self.assertEqual(perm2, Permission.WRITE)

    def test_from_string_with_whitespace(self):
        """Test from_string() handles surrounding whitespace."""
        # Should strip leading/trailing whitespace
        perm = Permission.from_string("  READ  ")
        self.assertEqual(perm, Permission.READ)

        perm2 = Permission.from_string("\twrite\n")
        self.assertEqual(perm2, Permission.WRITE)

    def test_from_string_with_invalid_input(self):
        """Test from_string() raises ValueError for invalid input."""
        # Should raise ValueError for unknown permission
        with self.assertRaises(ValueError) as context:
            Permission.from_string("invalid_permission")
        self.assertIn("Unknown permission string", str(context.exception))

        # Should raise ValueError for empty string after processing
        with self.assertRaises(ValueError):
            Permission.from_string("UNKNOWN")


class TestRole(unittest.TestCase):
    """Test cases for Role class"""

    def test_role_constants_are_defined(self):
        """Test that all role constants are properly defined."""
        # Verify all expected role constants exist
        self.assertTrue(hasattr(Role, 'READER'))
        self.assertTrue(hasattr(Role, 'CONTRIBUTOR'))
        self.assertTrue(hasattr(Role, 'ADMIN'))

    def test_role_values(self):
        """Test that all Role enum members have correct values."""
        # Verify each role has the expected string value
        self.assertEqual(Role.READER.value, "reader")
        self.assertEqual(Role.CONTRIBUTOR.value, "contributor")
        self.assertEqual(Role.ADMIN.value, "admin")

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
        # Verify rank ordering: READER < CONTRIBUTOR < ADMIN
        self.assertEqual(Role.READER.rank, 1)
        self.assertEqual(Role.CONTRIBUTOR.rank, 2)
        self.assertEqual(Role.ADMIN.rank, 3)

        self.assertGreater(Role.ADMIN.rank, Role.CONTRIBUTOR.rank)
        self.assertGreater(Role.CONTRIBUTOR.rank, Role.READER.rank)

    def test_can_assign_same_role(self):
        """Test that a role can assign itself."""
        # Each role should be able to assign its own level
        self.assertTrue(Role.READER.can_assign(Role.READER))
        self.assertTrue(Role.CONTRIBUTOR.can_assign(Role.CONTRIBUTOR))
        self.assertTrue(Role.ADMIN.can_assign(Role.ADMIN))

    def test_can_assign_lower_role(self):
        """Test that higher roles can assign lower roles."""
        # CONTRIBUTOR can assign READER
        self.assertTrue(Role.CONTRIBUTOR.can_assign(Role.READER))

        # ADMIN can assign both lower roles
        self.assertTrue(Role.ADMIN.can_assign(Role.READER))
        self.assertTrue(Role.ADMIN.can_assign(Role.CONTRIBUTOR))

    def test_cannot_assign_higher_role(self):
        """Test that lower roles cannot assign higher roles."""
        # READER cannot assign higher roles
        self.assertFalse(Role.READER.can_assign(Role.CONTRIBUTOR))
        self.assertFalse(Role.READER.can_assign(Role.ADMIN))

        # CONTRIBUTOR cannot assign ADMIN
        self.assertFalse(Role.CONTRIBUTOR.can_assign(Role.ADMIN))

    def test_reader_permissions(self):
        """Test that READER role has only READ permission."""
        permissions = Role.READER.permissions

        # Should have exactly one permission
        self.assertEqual(len(permissions), 1)
        self.assertIn(Permission.READ, permissions)

    def test_contributor_permissions(self):
        """Test that CONTRIBUTOR role has READ, WRITE, and CREATE permissions."""
        permissions = Role.CONTRIBUTOR.permissions

        # Should have exactly three permissions
        self.assertEqual(len(permissions), 3)
        self.assertIn(Permission.READ, permissions)
        self.assertIn(Permission.WRITE, permissions)
        self.assertIn(Permission.CREATE, permissions)

    def test_admin_permissions(self):
        """Test that ADMIN role has all permissions."""
        permissions = Role.ADMIN.permissions

        # Should have all six permissions
        self.assertEqual(len(permissions), 6)
        self.assertIn(Permission.READ, permissions)
        self.assertIn(Permission.WRITE, permissions)
        self.assertIn(Permission.CREATE, permissions)
        self.assertIn(Permission.DELETE, permissions)
        self.assertIn(Permission.MANAGE_USERS, permissions)
        self.assertIn(Permission.MANAGE_GROUPS, permissions)


class TestDatasetPermFunctions(unittest.TestCase):
    """Test cases for dataset permission conversion functions."""

    def test_dataset_perm_to_str_empty(self):
        """Test converting empty dataset permissions to string."""
        # Empty dict should produce empty string
        result = dataset_perm_to_str({})
        self.assertEqual(result, "")

    def test_dataset_perm_to_str_single_permission(self):
        """Test converting single dataset with single permission."""
        # Single dataset with one permission
        ds_perm = {'dataset_A': [Permission.READ]}
        result = dataset_perm_to_str(ds_perm)
        self.assertEqual(result, "dataset_A/Permission.READ")

    def test_dataset_perm_to_str_multiple_permissions(self):
        """Test converting single dataset with multiple permissions."""
        # Single dataset with multiple permissions
        ds_perm = {'dataset_A': [Permission.READ, Permission.WRITE]}
        result = dataset_perm_to_str(ds_perm)

        # Result should contain both permissions separated by comma
        self.assertIn("dataset_A/", result)
        self.assertIn("Permission.READ", result)
        self.assertIn("Permission.WRITE", result)

    def test_dataset_perm_to_str_multiple_datasets(self):
        """Test converting multiple datasets with permissions."""
        # Multiple datasets with various permissions
        ds_perm = {
            'dataset_A': [Permission.READ],
            'dataset_B': [Permission.READ, Permission.CREATE]
        }
        result = dataset_perm_to_str(ds_perm)

        # Result should contain both datasets separated by semicolon
        self.assertIn("dataset_A/Permission.READ", result)
        self.assertIn("dataset_B/", result)
        self.assertIn(";", result)

    def test_parse_dataset_perm_empty(self):
        """Test parsing empty dataset permission string."""
        # Empty string should return empty dict
        result = parse_dataset_perm("")
        self.assertEqual(result, {})

    def test_parse_dataset_perm_single_permission(self):
        """Test parsing single dataset with single permission."""
        # Parse simple dataset with one permission
        input_str = "dataset_A/Permission.READ"
        result = parse_dataset_perm(input_str)

        # Should have one dataset with one permission
        self.assertEqual(len(result), 1)
        self.assertIn("dataset_A", result)
        self.assertIn(Permission.READ, result["dataset_A"])

    def test_parse_dataset_perm_multiple_permissions(self):
        """Test parsing single dataset with multiple permissions."""
        # Parse dataset with comma-separated permissions
        input_str = "dataset_A/Permission.READ,Permission.WRITE"
        result = parse_dataset_perm(input_str)

        # Should have one dataset with two permissions
        self.assertEqual(len(result), 1)
        self.assertEqual(len(result["dataset_A"]), 2)
        self.assertIn(Permission.READ, result["dataset_A"])
        self.assertIn(Permission.WRITE, result["dataset_A"])

    def test_parse_dataset_perm_multiple_datasets(self):
        """Test parsing multiple datasets with permissions."""
        # Parse semicolon-separated datasets
        input_str = "dataset_A/Permission.READ;dataset_B/Permission.WRITE,Permission.CREATE"
        result = parse_dataset_perm(input_str)

        # Should have two datasets
        self.assertEqual(len(result), 2)
        self.assertIn("dataset_A", result)
        self.assertIn("dataset_B", result)
        self.assertIn(Permission.READ, result["dataset_A"])
        self.assertIn(Permission.WRITE, result["dataset_B"])
        self.assertIn(Permission.CREATE, result["dataset_B"])

    def test_parse_dataset_perm_no_permissions(self):
        """Test parsing dataset with no permissions specified."""
        # Dataset name without permissions
        input_str = "dataset_A"
        result = parse_dataset_perm(input_str)

        # Should have dataset with empty permission set
        self.assertIn("dataset_A", result)
        self.assertEqual(len(result["dataset_A"]), 0)

    def test_round_trip_conversion(self):
        """Test that converting to string and back preserves data."""
        # Original dataset permissions
        original = {
            'dataset_A': [Permission.READ],
            'dataset_B': [Permission.READ, Permission.WRITE, Permission.CREATE]
        }

        # Convert to string and back
        str_repr = dataset_perm_to_str(original)
        parsed = parse_dataset_perm(str_repr)

        # Should have same datasets
        self.assertEqual(set(original.keys()), set(parsed.keys()))

        # Should have same permissions for each dataset
        for dataset in original:
            self.assertEqual(set(original[dataset]), parsed[dataset])

        # Should have a `Permission` instance
        self.assertIsInstance(list(parsed['dataset_A'])[0], Permission)

class TestUser(unittest.TestCase):
    """Test cases for the User class."""

    def setUp(self):
        """Set up common test fixtures."""
        # Create a basic user for testing
        self.now = datetime.now(timezone.utc)
        self.user = User(
            username="testuser",
            fullname="Test User",
            password_hash="hashed_password",
            roles={Role.CONTRIBUTOR},
            created_at=self.now,
        )

    def test_user_initialization_minimal(self):
        """Test creating a User with only required fields."""
        # Create user with minimal required fields
        user = User(
            username="alice",
            fullname="Alice Smith",
            password_hash="hash123",
            roles={Role.READER},
            created_at=self.now
        )

        # Verify required fields
        self.assertEqual(user.username, "alice")
        self.assertEqual(user.fullname, "Alice Smith")
        self.assertEqual(user.password_hash, "hash123")
        self.assertIn(Role.READER, user.roles)

        # Verify optional fields have correct defaults
        self.assertIsInstance(user.permissions, set)
        self.assertEqual(len(user.permissions), 1)
        self.assertIsNone(user.last_login)
        self.assertTrue(user.is_active)
        self.assertEqual(user.failed_attempts, 0)
        self.assertIsNone(user.last_failed_attempt)
        self.assertIsNone(user.locked_until)

    def test_user_initialization_full(self):
        """Test creating a User with all fields specified."""
        # Create user with all fields
        last_login = self.now - timedelta(days=1)
        last_failed = self.now - timedelta(hours=1)
        locked_until = self.now + timedelta(hours=1)
        pwd_change = self.now - timedelta(days=30)

        user = User(
            username="bob",
            fullname="Bob Jones",
            password_hash="hash456",
            roles={Role.ADMIN, Role.CONTRIBUTOR},
            created_at=self.now,
            last_login=last_login,
            is_active=False,
            failed_attempts=3,
            last_failed_attempt=last_failed,
            locked_until=locked_until,
            password_last_change=pwd_change
        )

        # Verify all fields
        self.assertEqual(user.username, "bob")
        self.assertEqual(user.fullname, "Bob Jones")
        self.assertEqual(len(user.roles), 2)
        self.assertEqual(len(user.permissions), 6)
        self.assertEqual(user.last_login, last_login)
        self.assertFalse(user.is_active)
        self.assertEqual(user.failed_attempts, 3)
        self.assertEqual(user.locked_until, locked_until)

    def test_user_equality_same(self):
        """Test equality comparison between identical users."""
        # Create two identical users
        user1 = User(
            username="alice",
            fullname="Alice Smith",
            password_hash="hash",
            roles={Role.READER},
            created_at=self.now
        )
        user2 = User(
            username="alice",
            fullname="Alice Smith",
            password_hash="hash",
            roles={Role.READER},
            created_at=self.now
        )

        # Should be equal
        self.assertEqual(user1, user2)

    def test_user_equality_different(self):
        """Test equality comparison between different users."""
        # Create two users with different usernames
        user1 = User(
            username="alice",
            fullname="Alice Smith",
            password_hash="hash",
            roles={Role.READER},
            created_at=self.now
        )
        user2 = User(
            username="bob",
            fullname="Bob Jones",
            password_hash="hash",
            roles={Role.READER},
            created_at=self.now
        )

        # Should not be equal
        self.assertNotEqual(user1, user2)

    def test_user_equality_non_user(self):
        """Test equality comparison with non-User objects."""
        # Should not equal non-User objects
        self.assertNotEqual(self.user, "not a user")
        self.assertNotEqual(self.user, 123)
        self.assertNotEqual(self.user, None)
        self.assertNotEqual(self.user, {})

    def test_to_dict_basic(self):
        """Test converting User to dictionary."""
        # Convert to dict
        user_dict = self.user.to_dict()

        # Verify dictionary structure
        self.assertIsInstance(user_dict, dict)
        self.assertEqual(user_dict['username'], "testuser")
        self.assertEqual(user_dict['fullname'], "Test User")
        self.assertEqual(user_dict['password_hash'], "hashed_password")
        self.assertIn('contributor', user_dict['roles'])
        self.assertTrue(user_dict['is_active'])
        self.assertEqual(user_dict['failed_attempts'], 0)

    def test_to_dict_with_datetimes(self):
        """Test that datetime fields are serialized to ISO format."""
        # Set various datetime fields
        self.user.last_login = self.now
        self.user.last_failed_attempt = self.now
        self.user.locked_until = self.now + timedelta(hours=1)

        # Convert to dict
        user_dict = self.user.to_dict()

        # Verify datetime fields are ISO strings
        self.assertIsInstance(user_dict['created_at'], str)
        self.assertIsInstance(user_dict['last_login'], str)
        self.assertIsInstance(user_dict['last_failed_attempt'], str)
        self.assertIsInstance(user_dict['locked_until'], str)

    def test_from_dict_basic(self):
        """Test creating User from dictionary."""
        # Create dict representation
        user_dict = {
            'username': 'alice',
            'fullname': 'Alice Smith',
            'password_hash': 'hash123',
            'created_at': self.now.isoformat(),
            'roles': ['reader'],
        }

        # Create user from dict
        user = User.from_dict(user_dict)

        # Verify user was created correctly
        self.assertEqual(user.username, 'alice')
        self.assertEqual(user.fullname, 'Alice Smith')
        self.assertIn(Role.READER, user.roles)

    def test_from_dict_with_permissions(self):
        """Test creating User from dict with permissions."""
        # Dict with permissions
        user_dict = {
            'username': 'bob',
            'fullname': 'Bob Jones',
            'password_hash': 'hash456',
            'roles': ['contributor'],
            'created_at': self.now.isoformat(),
        }

        # Create user from dict
        user = User.from_dict(user_dict)
        user.grant_permission('manage_users')

        # Verify permissions were parsed
        self.assertEqual(len(user.permissions), 4)
        self.assertIn(Permission.READ, user.permissions)
        self.assertIn(Permission.WRITE, user.permissions)
        self.assertIn(Permission.MANAGE_USERS, user.permissions)

    def test_from_dict_invalid_role_skipped(self):
        """Test that invalid roles in dict are skipped for backward compatibility."""
        # Dict with invalid role
        user_dict = {
            'username': 'alice',
            'fullname': 'Alice Smith',
            'password_hash': 'hash',
            'roles': ['reader', 'invalid_role'],
            'created_at': self.now.isoformat()
        }

        # Should create user, skipping invalid role
        user = User.from_dict(user_dict)
        self.assertEqual(len(user.roles), 1)
        self.assertIn(Role.READER, user.roles)

    def test_from_dict_missing_required_field(self):
        """Test that from_dict raises error when required field is missing."""
        # Dict missing required field
        user_dict = {
            'username': 'alice',
            # missing fullname
            'password_hash': 'hash',
            'roles': ['reader'],
            'created_at': self.now.isoformat()
        }

        # Should raise KeyError
        with self.assertRaises(KeyError):
            User.from_dict(user_dict)

    def test_to_json(self):
        """Test converting User to JSON string."""
        # Convert to JSON
        json_str = self.user.to_json()

        # Verify it's valid JSON
        self.assertIsInstance(json_str, str)
        parsed = json.loads(json_str)
        self.assertIsInstance(parsed, dict)
        self.assertEqual(parsed['username'], 'testuser')

    def test_from_json(self):
        """Test creating User from JSON string."""
        # Create JSON string
        json_str = json.dumps({
            'username': 'alice',
            'fullname': 'Alice Smith',
            'password_hash': 'hash',
            'roles': ['reader'],
            'created_at': self.now.isoformat()
        })

        # Create user from JSON
        user = User.from_json(json_str)

        # Verify user was created
        self.assertEqual(user.username, 'alice')
        self.assertIn(Role.READER, user.roles)

    def test_from_json_invalid(self):
        """Test that from_json raises error for invalid JSON."""
        # Invalid JSON string
        invalid_json = "{'username': 'alice'}"  # single quotes are invalid

        # Should raise JSONDecodeError
        with self.assertRaises(json.JSONDecodeError):
            User.from_json(invalid_json)

    def test_json_round_trip(self):
        """Test that converting to JSON and back preserves data."""
        # Convert to JSON and back
        json_str = self.user.to_json()
        restored_user = User.from_json(json_str)

        # Should be equal to original
        self.assertEqual(self.user, restored_user)

    def test_is_locked_out_not_locked(self):
        """Test _is_locked_out returns False when not locked."""
        # User not locked
        self.assertIsNone(self.user.locked_until)
        self.assertFalse(self.user._is_locked_out())

    def test_is_locked_out_currently_locked(self):
        """Test _is_locked_out returns True when currently locked."""
        # Lock user until future time
        self.user.locked_until = datetime.now() + timedelta(hours=1)
        self.assertTrue(self.user._is_locked_out())

    def test_is_locked_out_lock_expired(self):
        """Test _is_locked_out returns False and clears lock when expired."""
        # Lock user until past time
        self.user.locked_until = datetime.now() - timedelta(hours=1)

        # Should not be locked and lock should be cleared
        self.assertFalse(self.user._is_locked_out())
        self.assertIsNone(self.user.locked_until)

    def test_record_failed_attempt(self):
        """Test _record_failed_attempt increments counter and sets timestamp."""
        # Initial state
        self.assertEqual(self.user.failed_attempts, 0)
        self.assertIsNone(self.user.last_failed_attempt)

        # Record failed attempt
        self.user._record_failed_attempt()

        # Verify counter incremented and timestamp set
        self.assertEqual(self.user.failed_attempts, 1)
        self.assertIsNotNone(self.user.last_failed_attempt)

    def test_record_multiple_failed_attempts(self):
        """Test recording multiple failed attempts."""
        # Record multiple failures
        for i in range(1, 4):
            self.user._record_failed_attempt()
            self.assertEqual(self.user.failed_attempts, i)


class TestTokenUser(unittest.TestCase):
    """Test cases for the TokenUser class."""

    def setUp(self):
        """Set up common test fixtures."""
        self.now = datetime.now(timezone.utc)
        self.dataset_perms = {
            'dataset_A': {Permission.READ},
            'dataset_B': {Permission.READ, Permission.WRITE}
        }

    def test_tokenuser_initialization_with_dataset_permissions(self):
        """Test creating TokenUser with dataset permissions."""
        # Create TokenUser with dataset permissions
        user = TokenUser(
            username="alice",
            fullname="Alice Smith",
            password_hash="hash",
            roles={Role.CONTRIBUTOR},
            created_at=self.now,
            dataset_permissions=self.dataset_perms
        )

        # Verify dataset permissions
        self.assertEqual(user.dataset_permissions, self.dataset_perms)

    def test_tokenuser_initialization_without_dataset_permissions(self):
        """Test that TokenUser raises error without dataset_permissions."""
        # Should raise TypeError without dataset_permissions
        with self.assertRaises(TypeError):
            TokenUser(
                username="alice",
                fullname="Alice Smith",
                password_hash="hash",
                roles={Role.CONTRIBUTOR},
                created_at=self.now,
            )

    def test_tokenuser_to_dict(self):
        """Test converting TokenUser to dictionary includes dataset_permissions."""
        # Create TokenUser
        user = TokenUser(
            username="alice",
            fullname="Alice Smith",
            password_hash="hash",
            roles={Role.READER},
            created_at=self.now,
            dataset_permissions=self.dataset_perms
        )

        # Convert to dict
        user_dict = user.to_dict()

        # Verify dataset_permissions in dict
        self.assertIn('dataset_permissions', user_dict)
        self.assertEqual(user_dict['dataset_permissions'], self.dataset_perms)

    def test_tokenuser_from_dict(self):
        """Test creating TokenUser from dictionary."""
        # Create dict with dataset_permissions
        user_dict = {
            'username': 'bob',
            'fullname': 'Bob Jones',
            'password_hash': 'hash',
            'roles': ['contributor'],
            'created_at': self.now.isoformat(),
            'permissions': ['read', 'write'],
            'dataset_permissions': {
                'dataset_A': ['read'],
                'dataset_B': ['read', 'write']
            }
        }

        # Create TokenUser from dict
        user = TokenUser.from_dict(user_dict)

        # Verify dataset_permissions parsed correctly
        self.assertIn('dataset_A', user.dataset_permissions)
        self.assertIn(Permission.READ, user.dataset_permissions['dataset_A'])

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


class TestGroup(unittest.TestCase):
    """Test cases for the Group class."""

    def setUp(self):
        """Set up common test fixtures."""
        self.now = datetime.now(timezone.utc)
        self.group = Group(
            name="researchers",
            users={"alice", "bob"},
            created_at=self.now,
            created_by="alice",
            description="Research team"
        )

    def test_group_initialization_minimal(self):
        """Test creating Group with only required fields."""
        # Create group with minimal fields
        group = Group(
            name="team",
            users=set(),
            created_at=self.now,
            created_by="admin"
        )

        # Verify required fields
        self.assertEqual(group.name, "team")
        self.assertEqual(len(group.users), 0)
        self.assertEqual(group.created_by, "admin")
        self.assertIsNone(group.description)

    def test_group_initialization_full(self):
        """Test creating Group with all fields."""
        # Verify all fields
        self.assertEqual(self.group.name, "researchers")
        self.assertEqual(len(self.group.users), 2)
        self.assertIn("alice", self.group.users)
        self.assertIn("bob", self.group.users)
        self.assertEqual(self.group.description, "Research team")
        self.assertEqual(self.group.created_by, "alice")

    def test_add_user_new(self):
        """Test adding a new user to the group."""
        # Add new user
        result = self.group.add_user("charlie")

        # Should return True and user should be added
        self.assertTrue(result)
        self.assertIn("charlie", self.group.users)
        self.assertEqual(len(self.group.users), 3)

    def test_add_user_duplicate(self):
        """Test adding a user that already exists in the group."""
        # Try to add existing user
        result = self.group.add_user("alice")

        # Should return False and size should not change
        self.assertFalse(result)
        self.assertEqual(len(self.group.users), 2)

    def test_remove_user_existing(self):
        """Test removing an existing user from the group."""
        # Remove existing user
        result = self.group.remove_user("alice")

        # Should return True and user should be removed
        self.assertTrue(result)
        self.assertNotIn("alice", self.group.users)
        self.assertEqual(len(self.group.users), 1)

    def test_remove_user_non_existing(self):
        """Test removing a user that is not in the group."""
        # Try to remove non-existing user
        result = self.group.remove_user("charlie")

        # Should return False and size should not change
        self.assertFalse(result)
        self.assertEqual(len(self.group.users), 2)

    def test_has_user_existing(self):
        """Test checking if an existing user is in the group."""
        # Check for existing user
        self.assertTrue(self.group.has_user("alice"))
        self.assertTrue(self.group.has_user("bob"))

    def test_has_user_non_existing(self):
        """Test checking if a non-existing user is in the group."""
        # Check for non-existing user
        self.assertFalse(self.group.has_user("charlie"))

    def test_group_users_as_set(self):
        """Test that group users are stored as a set (no duplicates)."""
        # Create group with duplicate users in initialization
        group = Group(
            name="team",
            users={"alice", "bob", "alice"},  # Duplicate alice
            created_at=self.now,
            created_by="admin"
        )

        # Should only have 2 unique users
        self.assertEqual(len(group.users), 2)
