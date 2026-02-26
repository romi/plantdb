#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Role-Based Access Control (RBAC) Manager

Provides a comprehensive framework for handling user permissions, roles, and group
management within the PlantDB ecosystem. It supports fine‑grained access
control for scans and user accounts, allowing administrators to create, modify,
and revoke permissions with ease.

Key Features
------------
- Role and permission hierarchy with ADMIN, CONTRIBUTOR, and READER roles.
- Group‑based sharing of scans and dynamic membership handling.
- Decorator‑based permission checks (`requires_permission`) for method
  protection.
- User account lifecycle operations: activation, deactivation, unlocking.
- Full scan access control: ownership, group sharing, and global role
  fallback.

Usage Examples
--------------
>>> from plantdb.commons.auth.rbac import RBACManager, Permission, User
>>> rbac = RBACManager()
>>> # Create a new user and assign a role
>>> user = rbac.users.create_user('alice', roles={'CONTRIBUTOR'})
>>> # Create a group and add the user
>>> rbac.create_group(user, 'researchers')
>>> rbac.add_user_to_group(user, 'researchers', 'alice')
>>> # Check if the user can read a scan
>>> scan_meta = {'owner': 'bob', 'sharing': ['researchers']}
>>> can_read = rbac.can_access_scan(user, scan_meta, Permission.READ)
>>> print(can_read)
True
"""

from functools import wraps
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from typing import Union

from plantdb.commons.auth.manager import GroupManager
from plantdb.commons.auth.manager import UserManager
from plantdb.commons.auth.models import Group
from plantdb.commons.auth.models import Permission
from plantdb.commons.auth.models import Role
from plantdb.commons.auth.models import User
from plantdb.commons.log import get_logger


def requires_permission(required_permissions: Union[Permission, List[Permission]]):
    """Decorator to check if the specified user has the required permission(s).

    Parameters
    ----------
    required_permission:  Union[Permission, List[Permission]]
        A single permission string or list of permission that the user must have to access the decorated method.
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(self, requesting_user: User, *args, **kwargs) -> Any:
            # Ensure required_permissions is always a list
            perms_to_check = (
                required_permissions if isinstance(required_permissions, list)
                else [required_permissions]
            )

            # Get user's permissions (assuming you are calling from a class who as a `_get_user_permissions` method)
            user_permissions = self.get_user_permissions(requesting_user)

            # Check if the requesting user has all the required permissions
            has_permission = all(perm in user_permissions for perm in perms_to_check)

            if not has_permission:
                perm_names = [perm.name if hasattr(perm, 'name') else str(perm) for perm in perms_to_check]
                raise PermissionError(
                    f"User '{requesting_user.username}' does not have required permission(s): {', '.join(perm_names)}"
                )

            return func(self, requesting_user, *args, **kwargs)

        return wrapper

    return decorator


class RBACManager :
    """Manage Role-Based Access Control (RBAC) for users and permissions.

    This class provides methods to determine which permissions a user has,
    check if a user has a specific permission, and verify if a user can access
    or perform operations on scans based on their roles and permissions.

    Attributes
    ----------
    GUEST_USERNAME : str
        The username for the default guest user account.
    groups : plantdb.commons.auth.manager.GroupManager
        Manager for handling group operations.

    Examples
    --------
    >>> from plantdb.commons.auth.rbac import RBACManager
    >>> from plantdb.commons.test_database import test_database
    >>> db = test_database('all')
    >>> db.connect()
    >>> scan = db.get_scan('real_plant_analyzed')
    >>> rbac = RBACManager()
    >>> guest_user = rbac.create_guest_user('guest')
    >>> can_read = rbac.can_access_scan(guest_user, scan.metadata, Permission.READ)

    """

    def __init__(self, users_file: str = 'users.json', groups_file: str = "groups.json", max_login_attempts=3,
                 lockout_duration=900):
        """Initialize the RBACManager.

        Parameters
        ----------
        users_file : str, optional
            Path to the JSON file for storing users database. Default is 'users.json'.
        groups_file : str, optional
            Path to the JSON file for storing groups database. Defaults to "groups.json".
        max_login_attempts : int, optional
            Maximum number of failed login attempts before account lockout. Defaults to ``3``.
        lockout_duration : int or timedelta, optional
            Account lockout duration in seconds. Defaults to ``900`` (15 minutes).
        """
        self.logger = get_logger(__class__.__name__)
        self.users = UserManager(users_file, max_login_attempts, lockout_duration)
        self.groups = GroupManager(groups_file)

    def get_user_permissions(self, user: User) -> Set[Permission]:
        """Get the set of permissions a user has based on their assigned roles.

        This function returns a set containing all permissions directly assigned to
        the user as well as those inherited from any roles they are part of. The
        resulting set is a union of both direct and indirect permissions.

        Parameters
        ----------
        user : plantdb.commons.auth.models.User
            A `User` object representing the user whose permissions will be checked.
            This user must have attributes `permissions` and `roles`.

        Returns
        -------
        Set[Permission]
            A set containing all permissions that the specified user has access to,
            including those inherited from roles.

        Notes
        -----
        The result depends on the `role_permissions` attribute of the class instance.
        Ensure that this dictionary is properly initialized before calling this method.

        See Also
        --------
        plantdb.commons.auth.models.User : Represents a user with permissions and roles.
        plantdb.commons.auth.models.Permission : Represents a permission that can be assigned to users or roles.
        plantdb.commons.auth.models.Role : Represents a role with specific permissions.

        Examples
        --------
        >>> from plantdb.commons.auth.models import Permission
        >>> from plantdb.commons.auth.models import User
        >>> role_admin = Role('admin')
        >>> role_user = Role('user')
        >>> permission_a = Permission()  # Mocking a specific permission
        >>> permission_b = Permission()  # Mocking another specific permission
        >>> user = User(permissions={permission_a}, roles={role_user})
        >>> role_permissions = {Role('admin'): {permission_b}}
        >>> user.get_user_permissions(user)  # doctest: +SKIP
        {<__main__.Permission object at 0x...>}
        """
        permissions = set(user.permissions) if user.permissions else set()
        for role in user.roles:
            permissions.update(role.permissions)
        return permissions

    def has_permission(self, user: User, permission: Permission) -> bool:
        """Check if a user has a specific permission.

        This function determines whether the given user has the specified permission,
        including administrative privileges.

        Parameters
        ----------
        user : plantdb.commons.auth.models.User
            The User object to check for permissions.
        permission : plantdb.commons.auth.models.Permission
            The permission level or type to verify against the user's permissions.

        Returns
        -------
        bool
            ``True`` if the user has the given permission, ``False`` otherwise.

        Notes
        -----
        The function checks for the specific permission in the user's permission set.

        See Also
        --------
        get_user_permissions : Retrieve the list of permissions a user has.
        """
        user_permissions = self.get_user_permissions(user)
        return permission in user_permissions

    def has_role(self, user: User, role: Role) -> bool:
        """Check if a user has a specific role.

        Parameters
        ----------
        user : plantdb.commons.auth.models.User
            The user object to check for roles.
        role : plantdb.commons.auth.models.Role
            The role to verify against the user's roles.

        Returns
        -------
        bool
            ``True`` if the user has the given role, ``False`` otherwise.
        """
        return role in user.roles

    def is_guest_user(self, user: User) -> bool:
        """Check if the given user is the guest user.

        Parameters
        ----------
        user : plantdb.commons.auth.models.User
            The user object to check.

        Returns
        -------
        bool
            ``True`` if the user is the guest user, ``False`` otherwise.
        """
        return user.username == self.users.GUEST_USERNAME

    def get_guest_user(self) -> User:
        """Retrieves the guest user from the user repository.

        Returns
        -------
        User
            The User object corresponding to the guest username.
        """
        return self.users.get_user(self.users.GUEST_USERNAME)

    def can_create_user(self, user: User) -> bool:
        """Check if a user can create new users.

        Parameters
        ----------
        user : plantdb.commons.auth.models.User
            The user to check.

        Returns
        -------
        bool
            ``True`` if the user can create new users, ``False`` otherwise.
        """
        return self.has_permission(user, Permission.MANAGE_USERS)

    def can_manage_groups(self, user: User) -> bool:
        """Check if a user can manage groups (create/delete groups).

        Any user with a role that has a ``MANAGE_GROUPS`` permission can create groups.

        Parameters
        ----------
        user : plantdb.commons.auth.models.User
            The user to check.

        Returns
        -------
        bool
            ``True`` if the user can manage groups, ``False`` otherwise.
        """
        return self.has_permission(user, Permission.MANAGE_GROUPS)

    def can_add_to_group(self, user: User, group_name: str) -> bool:
        """Check if a user can add members to a specific group.

        Users can add members to a group if:
        1. They are an admin (MANAGE_GROUPS permission), OR
        2. They are a member of the group

        Parameters
        ----------
        user : plantdb.commons.auth.models.User
            The user requesting to add members.
        group_name : str
            The name of the group.

        Returns
        -------
        bool
            ``True`` if the user can add members to the group, ``False`` otherwise.
        """
        # Admins can manage any group
        if self.has_role(user, Role.ADMIN):
            return True

        # Group members can add users to their groups
        group = self.groups.get_group(group_name)
        return group is not None and group.has_user(user.username)

    def can_create_group(self, user: User) -> bool:
        """Check if a user can create groups.

        Any user with a role that has a ``MANAGE_GROUPS`` permission can create groups.

        Parameters
        ----------
        user : plantdb.commons.auth.models.User
            The user to check.

        Returns
        -------
        bool
            ``True`` if the user can create groups, ``False`` otherwise.
        """
        return self.has_permission(user, Permission.MANAGE_GROUPS)

    def can_delete_group(self, user: User) -> bool:
        """Check if a user can delete a group.

        Only users with the `MANAGE_GROUPS` permission can delete groups.

        Parameters
        ----------
        user : plantdb.commons.auth.models.User
            The user requesting to delete the group.

        Returns
        -------
        bool
            ``True`` if the user can delete the group, ``False`` otherwise.
        """
        # Admins can manage any group
        if self.has_role(user, Role.ADMIN):
            return True

        return self.has_permission(user, Permission.MANAGE_GROUPS)

    def create_group(self, user: User, group_name: str, users: Optional[Set[str]] = None,
                     description: Optional[str] = None) -> Optional[Group]:
        """Create a new group if the user has permission.

        Parameters
        ----------
        user : plantdb.commons.auth.models.User
            The user creating the group.
        group_name : str
            The unique name for the group.
        users : Optional[Set[str]]
            Initial set of users to add to the group.
        description : Optional[str]
            Optional description of the group.

        Returns
        -------
        Optional[Group]
            The created group object if successful, ``None`` if the permission was denied.

        Raises
        ------
        ValueError
            If a group with the same name already exists.
        """
        if not self.can_manage_groups(user):
            self.logger.error(f"Insufficient permission to create group '{group_name}' by user '{user.username}!")
            return None
        self.logger.info(f"Creating group '{group_name}' by user '{user.username}, with users '{users}'")
        return self.groups.create_group(group_name, user.username, users, description)

    def add_user_to_group(self, user: User, group_name: str, username_to_add: str) -> bool:
        """Add a user to a group if the requesting user has permission.

        Parameters
        ----------
        user : plantdb.commons.auth.models.User
            The user requesting to add a member.
        group_name : str
            The name of the group.
        username_to_add : str
            The username to add to the group.

        Returns
        -------
        bool
            ``True`` if the user was added successfully.
            ``False`` if the permission was denied or the operation failed.
        """
        if not self.can_add_to_group(user, group_name):
            self.logger.error(f"Insufficient permission to add user '{username_to_add}' to group '{group_name}' by user '{user.username}!")
            return False
        self.logger.info(f"Adding user '{username_to_add}' from group '{group_name}' by user '{user.username}'!")
        return self.groups.add_user_to_group(group_name, username_to_add)

    def remove_user_from_group(self, user: User, group_name: str, username_to_remove: str) -> bool:
        """Remove a user from a group if the requesting user has permission.

        Parameters
        ----------
        user : plantdb.commons.auth.models.User
            The user requesting to remove a member.
        group_name : str
            The name of the group.
        username_to_remove : str
            The username to remove from the group.

        Returns
        -------
        bool
            ``True`` if the user was removed successfully.
            ``False`` if the permission was denied or the operation failed.
        """
        if not self.can_add_to_group(user, group_name):  # Same permission as adding
            self.logger.error(f"Insufficient permission to remove user '{username_to_remove}' from group '{group_name}' by user '{user.username}!")
            return False
        self.logger.warning(f"Removing user '{username_to_remove}' from group '{group_name}' by user '{user.username}'!")
        return self.groups.remove_user_from_group(group_name, username_to_remove)

    def delete_group(self, user: User, group_name: str) -> bool:
        """Delete a group if the user has permission.

        Parameters
        ----------
        user : plantdb.commons.auth.models.User
            The user requesting to delete the group.
        group_name : str
            The name of the group to delete.

        Returns
        -------
        bool
            ``True`` if the group was deleted successfully.
            ``False`` if the permission was denied or the group is not found.
        """
        if not self.can_delete_group(user):
            self.logger.error(f"Insufficient permission to delete group '{group_name}' by user '{user.username}!")
            return False
        self.logger.warning(f"Deleting group '{group_name}' by user '{user.username}'!")
        return self.groups.delete_group(group_name)

    def get_user_groups(self, username: str) -> List[Group]:
        """Get all groups that a user belongs to.

        Parameters
        ----------
        username : str
            The username to search for.

        Returns
        -------
        List[Group]
            A list of Group objects that the user belongs to.
        """
        return self.groups.get_user_groups(username)

    def list_groups(self, user: User) -> Optional[List[Group]]:
        """List all groups if the user has permission.

        Parameters
        ----------
        user : plantdb.commons.auth.models.User
            The user requesting the group list.

        Returns
        -------
        Optional[List[Group]]
            A list of all groups if the user has permission, None otherwise.
        """
        # For now, any authenticated user can list groups
        # This can be restricted later if needed
        return self.groups.list_groups()

    def get_effective_role_for_scan(self, user: User, scan_metadata: dict) -> Role:
        """Get the effective role a user has for a specific scan dataset.

        This method determines the user's role based on:
        1. Dataset ownership (owner gets CONTRIBUTOR role)
        2. Group sharing (shared group members get CONTRIBUTOR role)
        3. User's global role (fallback)

        Parameters
        ----------
        user : plantdb.commons.auth.models.User
            The user to check.
        scan_metadata : dict
            The scan metadata containing 'owner' and optional 'sharing' fields.

        Returns
        -------
        plantdb.commons.models.Role
            The effective role for this specific scan dataset.
        """
        # Get the scan owner, default to guest if not specified
        scan_owner = scan_metadata.get('owner', self.users.GUEST_USERNAME)

        # If the user is the owner, it gets the CONTRIBUTOR role (unless admin)
        if user.username == scan_owner:
            # Admins keep their admin privileges
            if Role.ADMIN in user.roles:
                return Role.ADMIN
            # Owners get at least CONTRIBUTOR privileges
            current_roles = user.roles
            if Role.ADMIN in current_roles:
                return Role.ADMIN
            elif Role.CONTRIBUTOR in current_roles or Role.READER in current_roles:
                return Role.CONTRIBUTOR
            else:
                # Fallback to CONTRIBUTOR for owners
                return Role.CONTRIBUTOR

        # Check if the user belongs to any shared groups
        shared_groups = scan_metadata.get('sharing', [])
        if shared_groups:
            user_groups = self.groups.get_user_groups(user.username)
            user_group_names = {group.name for group in user_groups}

            # If the user belongs to any shared group, they get the CONTRIBUTOR role for this scan
            if any(group_name in user_group_names for group_name in shared_groups):
                # Admins keep their admin privileges
                if Role.ADMIN in user.roles:
                    return Role.ADMIN
                # Shared group members get CONTRIBUTOR privileges for this scan
                return Role.CONTRIBUTOR

        # Fall back to user's highest global role
        if Role.ADMIN in user.roles:
            return Role.ADMIN
        elif Role.CONTRIBUTOR in user.roles:
            return Role.CONTRIBUTOR
        else:
            return Role.READER

    def get_scan_permissions(self, user: User, scan_metadata: dict) -> Set[Permission]:
        """Get the set of permissions a user has for a specific scan dataset.

        This method considers the user's effective role for the specific scan,
        taking into account ownership and group sharing.

        Parameters
        ----------
        user : plantdb.commons.auth.models.User
            The user to check permissions for.
        scan_metadata : dict
            The scan metadata containing 'owner' and optional 'sharing' fields.

        Returns
        -------
        Set[Permission]
            The set of permissions the user has for this specific scan.
        """
        effective_role = self.get_effective_role_for_scan(user, scan_metadata)
        return effective_role.permissions

    def can_access_scan(self, user: User, scan_metadata: dict, operation: Permission) -> bool:
        """Check if a user can perform a specific operation on a scan dataset.

        This method implements the complete access control logic, including:
        - Owner-based access (owners get CONTRIBUTOR role for their scans)
        - Group-based access (shared group members get CONTRIBUTOR role)
        - Global role-based access (fallback to user's global role)
        - Admin override (admins can do everything)

        Parameters
        ----------
        user : plantdb.commons.auth.models.User
            The user requesting access.
        scan_metadata : dict
            The scan metadata containing 'owner' and optional 'sharing' fields.
        operation : plantdb.commons.auth.models.Permission
            The operation/permission being requested.

        Returns
        -------
        bool
            ``True`` if the user can perform the operation, ``False`` otherwise.
        """
        scan_permissions = self.get_scan_permissions(user, scan_metadata)
        return operation in scan_permissions

    def can_modify_scan_owner(self, user: User, scan_metadata: dict) -> bool:
        """Check if a user can modify the 'owner' field of a scan.

        Only users with permission to ``MANAGE_USERS`` can modify scan ownership.

        Parameters
        ----------
        user : plantdb.commons.auth.models.User
            The user requesting to modify ownership.
        scan_metadata : dict
            The scan metadata.

        Returns
        -------
        bool
            ``True`` if the user can modify the owner field, ``False`` otherwise.
        """
        return self.has_permission(user, Permission.MANAGE_USERS)

    def can_modify_scan_sharing(self, user: User, scan_metadata: dict) -> bool:
        """Check if a user can modify the 'sharing' field of a scan.

        Users can modify sharing if they have WRITE permission for the scan
        (_i.e._, they are the owner, in a shared group, or have a global CONTRIBUTOR+ role).

        Parameters
        ----------
        user : plantdb.commons.auth.models.User
            The user requesting to modify sharing.
        scan_metadata : dict
            The scan metadata.

        Returns
        -------
        bool
            ``True`` if the user can modify the sharing field, ``False`` otherwise.
        """
        return self.can_access_scan(user, scan_metadata, Permission.WRITE)

    def validate_scan_metadata_access(self, user: User, old_metadata: dict, new_metadata: dict) -> bool:
        """Validate that a user can make the proposed metadata changes.

        This method checks if the user has permission to modify specific fields
        like 'owner' and 'sharing' based on the RBAC rules.

        Parameters
        ----------
        user : plantdb.commons.auth.models.User
            The user attempting to modify metadata.
        old_metadata : dict
            The current scan metadata.
        new_metadata : dict
            The proposed new metadata.

        Returns
        -------
        bool
            ``True`` if all proposed changes are allowed, ``False`` otherwise.
        """
        # Check if the owner field is being modified
        old_owner = old_metadata.get('owner', self.users.GUEST_USERNAME)
        new_owner = new_metadata.get('owner', self.users.GUEST_USERNAME)

        if old_owner != new_owner:
            if not self.can_modify_scan_owner(user, old_metadata):
                return False

        # Check if the sharing field is being modified
        old_sharing = old_metadata.get('sharing', [])
        new_sharing = new_metadata.get('sharing', [])

        if old_sharing != new_sharing:
            if not self.can_modify_scan_sharing(user, old_metadata):
                return False

        return True

    def ensure_scan_owner(self, scan_metadata: dict) -> dict:
        """Ensure a scan has an owner field, defaulting to guest if missing.

        This method should be called when loading or creating scans to ensure
        the owner field is always present.

        Parameters
        ----------
        scan_metadata : dict
            The scan metadata to check and potentially modify.

        Returns
        -------
        dict
            The metadata updated with the 'owner' field.
        """
        if 'owner' not in scan_metadata:
            scan_metadata = scan_metadata.copy()
            scan_metadata['owner'] = self.users.GUEST_USERNAME
        return scan_metadata

    def validate_sharing_groups(self, sharing_groups: List[str]) -> bool:
        """Validate that all groups in the sharing list exist.

        Parameters
        ----------
        sharing_groups : List[str]
            List of group names to validate.

        Returns
        -------
        bool
            True if all groups exist, False otherwise.
        """
        for group_name in sharing_groups:
            if not self.groups.group_exists(group_name):
                return False
        return True

    def get_accessible_scans_for_user(self, user: User, all_scan_metadata: Dict[str, dict]) -> Dict[str, dict]:
        """Filter scans to only include those the user has READ access to.

        This method can be used to implement scan listing with proper access control.

        Parameters
        ----------
        user : plantdb.commons.auth.models.User
            The user requesting scan access.
        all_scan_metadata : Dict[str, dict]
            Dictionary mapping scan IDs to their metadata.

        Returns
        -------
        Dict[str, dict]
            Dictionary of scan IDs to metadata for scans the user can read.
        """
        accessible_scans = {}

        for scan_id, metadata in all_scan_metadata.items():
            # Ensure owner field is present
            metadata = self.ensure_scan_owner(metadata)

            # Check if the user has READ access to this scan
            if self.can_access_scan(user, metadata, Permission.READ):
                accessible_scans[scan_id] = metadata

        return accessible_scans

    def get_user_scan_role_summary(self, user: User, scan_metadata: dict) -> dict:
        """Get a summary of the user's access to a specific scan.

        This is useful for debugging and user interfaces to show access levels.

        Parameters
        ----------
        user : plantdb.commons.auth.models.User
            The user to analyze.
        scan_metadata : dict
            The scan metadata.

        Returns
        -------
        dict
            A dictionary containing access information including:

                - 'effective_role': the user's effective role for this scan
                - 'permissions': list of permissions the user has
                - 'access_reason': why the user has this level of access
                - 'is_owner': a boolean flag indicating if the user is the owner of the scan
                - 'shared_groups': a list of groups that share this scan, if any
        """
        metadata = self.ensure_scan_owner(scan_metadata)
        effective_role = self.get_effective_role_for_scan(user, metadata)
        permissions = list(self.get_scan_permissions(user, metadata))

        # Determine access reason
        scan_owner = metadata.get('owner', self.users.GUEST_USERNAME)
        shared_groups = metadata.get('sharing', [])
        user_groups = {group.name for group in self.groups.get_user_groups(user.username)}

        access_reason = []

        if Role.ADMIN in user.roles:
            access_reason.append("admin_role")

        if user.username == scan_owner:
            access_reason.append("owner")

        shared_group_matches = [g for g in shared_groups if g in user_groups]
        if shared_group_matches:
            access_reason.append(f"group_member: {', '.join(shared_group_matches)}")

        if not access_reason:
            access_reason.append("global_role")

        return {
            'effective_role': effective_role.value,
            'permissions': [p.value for p in permissions],
            'access_reason': access_reason,
            'is_owner': user.username == scan_owner,
            'shared_groups': shared_group_matches if shared_groups else []
        }

    @requires_permission(Permission.MANAGE_USERS)
    def unlock(self, requesting_user: User, username: str) -> bool:
        """Unlock a user account - requires MANAGE_USERS permission"""
        if not self.users.exists(username):
            self.logger.warning(f"Attempt to unlock non-existent user: {username}")
            return False

        user = self.users.get_user(username)
        if user.locked_until:
            self.users.unlock_user(user)
            self.logger.info(f"User {username} unlocked by {requesting_user.username}")
        else:
            self.logger.info(f"User {username} was not locked")
        return True

    @requires_permission(Permission.MANAGE_USERS)
    def activate(self, requesting_user: User, username: str) -> bool:
        """Activate a user account - requires MANAGE_USERS permission"""
        if not self.users.exists(username):
            self.logger.warning(f"Attempt to activate non-existent user: {username}")
            return False

        user = self.users.get_user(username)
        if user.is_active:
            self.users.activate(user)
            self.logger.info(f"User {username} activated by {requesting_user.username}")
            return True
        else:
            self.logger.info(f"User {username} was already active")
        return True

    @requires_permission(Permission.MANAGE_USERS)
    def deactivate(self, requesting_user: User, username: str) -> bool:
        """Deactivate a user account - requires MANAGE_USERS permission"""
        if not self.users.exists(username):
            self.logger.warning(f"Attempt to deactivate non-existent user: {username}")
            return False

        user = self.users.get_user(username)
        if user.is_active:
            self.users.deactivate(user)
            self.logger.info(f"User {username} deactivated by {requesting_user.username}")
            return True
        else:
            self.logger.info(f"User {username} was already inactive")
        return True
