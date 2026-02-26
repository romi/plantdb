#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
User and Group Management Module

This module provides two primary classes - `UserManager` and `GroupManager` - to handle
authentication, authorization, and role‑based access control in a lightweight
file‑based persistence system. User data, including hashed passwords, roles,
and lock‑out state, is stored in a JSON file, while group membership and
metadata are kept in a separate JSON file. The module is designed for small
projects or prototyping, offering straightforward APIs for creating users,
managing passwords, locking out accounts, and organizing users into
groups with associated permissions.

Key Features
------------
- **Password hashing** with Argon2 and secure verification.
- **Account lock‑out** after configurable failed login attempts.
- **User activation/deactivation** and role assignment.
- **Group management**: create, delete, add/remove users, and query memberships.
- **Atomic JSON persistence** to avoid data corruption.
- **Minimal dependencies** - relies only on the standard library plus `argon2`.

Usage Examples
--------------
>>> from pathlib import Path
>>> from plantdb.commons.auth.manager import UserManager, GroupManager, Role
>>> # Initialize managers (will create JSON files if absent)
>>> um = UserManager(users_file=Path('users.json'))
>>> gm = GroupManager(groups_file=Path('groups.json'))
>>> # Create a new user
>>> um.create('alice', 'Alice Smith', 'secure123', roles={Role.ADMIN})
>>> # Validate credentials
>>> assert um.validate('alice', 'secure123')
>>> # Create a group and add the user
>>> group = gm.create_group('admins', creator='alice', users={'alice'}, description='Admin group')
>>> # Check membership
>>> assert 'admins' in [g.name for g in gm.get_user_groups('alice')]
"""

import json
import os
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from typing import Union

from argon2 import PasswordHasher
from plantdb.commons.auth.models import Group
from plantdb.commons.auth.models import Role
from plantdb.commons.auth.models import TokenUser
from plantdb.commons.auth.models import User
from plantdb.commons.auth.models import parse_dataset_perm
from plantdb.commons.log import get_logger

ph = PasswordHasher()


class UserManager(object):
    """UserManager class for managing user data.

    The UserManager class provides methods to create, load,
    save, and manage users. It uses a JSON file to persist user data.
    It includes functionalities like password hashing and verification,
    user account lockout management, and handling of guest accounts.

    Attributes
    ----------
    users_file : str
        Path to the JSON file where user data is stored.
    users : Dict[str, plantdb.commons.auth.models.User]
        Dictionary containing all users, indexed by username.
    GUEST_USERNAME : str
        Default username for guest accounts.
    GUEST_PASSWORD : str
        Default password for guest accounts.

    Examples
    --------
    >>> from pathlib import Path
    >>> from tempfile import gettempdir
    >>> from plantdb.commons.auth.manager import UserManager
    >>> manager = UserManager(users_file=Path(gettempdir())/'users.json')
    >>> manager.validate_user_password(manager.GUEST_USERNAME, manager.GUEST_PASSWORD)
    True
    >>> manager.validate(manager.GUEST_USERNAME, manager.GUEST_PASSWORD)
    True
    >>> manager.validate('gest', manager.GUEST_PASSWORD)
    False
    >>> manager.deactivate(manager.get_user(manager.GUEST_USERNAME))
    >>> manager.validate(manager.GUEST_USERNAME, manager.GUEST_PASSWORD)
    WARNING  [UserManager] Account guest is not active.
    False
    """

    def __init__(self, users_file: str | Path = 'users.json', max_login_attempts=3, lockout_duration=900):
        """User manager constructor.

        Parameters
        ----------
        users_file : str or pathlib.Path, optional
            Path to the JSON file where user data is stored. Defaults to ``users.json``.
        max_login_attempts : int, optional
            Maximum number of failed login attempts before account lockout. Defaults to ``3``.
        lockout_duration : int or timedelta, optional
            Account lockout duration in seconds. Defaults to ``900`` (15 minutes).
        """
        self.ADMIN_USERNAME = "admin"
        self.GUEST_USERNAME = "guest"
        self.GUEST_PASSWORD = "guest"
        self.users_file = Path(users_file)
        self.users: Dict[str, User] = {}
        self.logger = get_logger(__class__.__name__)
        self._load_users()
        self._ensure_guest_user()
        self._ensure_admin_user()

        self.max_login_attempts = max_login_attempts
        self.lockout_duration = timedelta(seconds=lockout_duration) if isinstance(lockout_duration,
                                                                                  int) else lockout_duration

    def _load_users(self) -> None:
        """Loads the user database from a JSON file and populates the internal dictionary.

        This method checks if the users database file exists. If it does not exist, it initializes an empty dictionary,
        creates the file, and logs a warning message. If the file does exist, it loads the user data from the file,
        converts each user to a `User` object, and populates the internal dictionary with these objects.

        Notes
        -----
        The method assumes that the users database file is in JSON format and contains valid user data.
        """
        if not self.users_file.exists():
            self.logger.warning(f"No users database file found under '{self.users_file}'")
            self.users = {}
        elif self.users_file.stat().st_size == 0:
            self.logger.warning(f"Empty users database file found under '{self.users_file}'")
            self.users = {}
        else:
            # Load the users list from the database
            with open(self.users_file, "r") as f:
                users_list = json.load(f)
            # Convert to a username indexed dict of User objects
            self.users = {}
            for user in users_list:
                user = User.from_dict(user)
                self.users[user.username] = user
            self.logger.debug(f"Loaded {len(self.users)} users from '{self.users_file}'.")
        return

    def _save_users(self) -> None:
        """Save the user data to a JSON file.

        This method serializes the user objects into dictionaries, writes them to a temporary file,
        and then renames the temporary file to replace the original file.
        This ensures the atomicity of the operation.

        Raises
        ------
        Exception
            If any error occurs during the process, an exception is raised after cleaning up the temporary file.

        Notes
        -----
        This method ensures atomicity by writing to a temporary file and then renaming it.
        This is important to prevent data corruption in case of crashes or interruptions during the save process.
        """
        data = []
        for _, user in self.users.items():
            data.append(user.to_dict())

        temp_file = self.users_file.with_suffix('.tmp')
        # Create a temporary file in the same directory
        try:
            # Write to a temporary file first
            with open(temp_file, "w") as f:
                json.dump(data, f, indent=2)

            # Rename the temporary file to the final filename (atomic operation on most file systems)
            os.replace(temp_file, self.users_file)
            self.logger.debug(f"Saved {len(self.users)} users to '{self.users_file}'.")
        except Exception as e:
            # If anything goes wrong, clean up the temporary file
            if temp_file.exists():
                temp_file.unlink()
            self.logger.error(f"Failed to save users database: {str(e)}")
            raise Exception(f"Failed to save users to {self.users_file}: {e}")
        return

    @staticmethod
    def _hash_password(password: str) -> str:
        """Hash a plaintext password using argon2.

        Parameters
        ----------
        password : str
            The plaintext password to be hashed.

        Returns
        -------
        str
            The hashed password as a string.

        See Also
        --------
        argon2.PasswordHasher : Hash the plaintext password.
        """
        return ph.hash(password)

    def exists(self, username: str) -> bool:
        """Check if a user exists.

        Parameters
        ----------
        username : str
            The name of the user to check for existence.

        Returns
        -------
        bool
            `True` if the user exists, otherwise `False`.

        Notes
        -----
        This function is case-sensitive. For case-insensitive checks, convert both
        the username and the keys to the lower or upper case before comparison.
        """
        return username in self.users

    def create(self, username: str, fullname: str, password: str, roles: Union[Role, Set[Role]] = None) -> None:
        """Create a new user and store the user information in a file.

        Parameters
        ----------
        username : str
            The username of the user to create.
            This will be converted to lowercase.
        fullname : str
            The full name of the user to create.
        password : str
            The password of the user to create.
        roles : plantdb.commons.auth.models.Role or Set[plantdb.commons.auth.models.Role], optional
            The roles to associate with the user.
            If ``None`` (default), use the `Role.READER` role.

        Examples
        --------
        >>> from plantdb.commons.auth.manager import Role, UserManager
        >>> manager = UserManager()
        >>> manager.create('batman', "Bruce Wayne", "joker", Role.ADMIN)
        >>> print(manager.users)
        batman
        """
        username = username.lower()  # Convert the username to lowercase to maintain uniformity.
        timestamp = datetime.now()  # Get the current timestamp for tracking user creation time.

        # Verify if the username is available
        try:
            assert not self.exists(username)
        except AssertionError:
            self.logger.error(f"User '{username}' already exists!")
            return

        # Convert to a list:
        if isinstance(roles, Role):
            roles = {roles}

        # Add the new user's data to the `self.users` dictionary.
        self.users[username] = User(
            username=username,
            fullname=fullname,
            password_hash=self._hash_password(password),
            roles=roles or {Role.READER},
            created_at=timestamp,
            password_last_change=timestamp,
        )
        # Save all user data (including the newly created user) to the 'users.json' file.
        self._save_users()
        self.logger.debug(f"Created user '{username}' with fullname '{fullname}'.")
        if not username == self.GUEST_USERNAME:
            self.logger.info(f"Welcome {fullname}, please log in...'")
        return

    def _ensure_guest_user(self) -> None:
        """Ensure that a guest user exists in the system.

        If the guest user does not already exist, it creates one with a default
        username and password. This method is intended to be used internally by
        the class to ensure the presence of a guest account for various operational purposes.

        Notes
        -----
        This function modifies the internal state by potentially adding a new user
        to the system. It should be called only when it is necessary to ensure that
        the guest user exists.
        """
        if not self.exists(self.GUEST_USERNAME):
            self.logger.info(f"Creating guest user '{self.GUEST_USERNAME}'")
            # Create the guest user
            self.create(
                self.GUEST_USERNAME,
                fullname='PlantDB Guest',
                password=self.GUEST_PASSWORD,
                roles={Role.READER},
            )
        return

    def _ensure_admin_user(self) -> None:
        """Ensure that an admin user exists in the system.

        If the admin user does not already exist, it creates one with a default username.
        The password is a string of 25 hex digits, printed-out to the terminal.
        This method is intended to be used internally in the class to ensure the presence
        of an admin account for various operational purposes.

        Notes
        -----
        This function modifies the internal state by potentially adding a new user
        to the system. It should be called only when it is necessary to ensure that
        the admin user exists.
        """
        # admin_password = secrets.token_hex(25)
        admin_password = "admin"  # debug mode

        if not self.exists(self.ADMIN_USERNAME):
            self.logger.info(f"Creating admin user '{self.ADMIN_USERNAME}'")
            # Create the guest user
            self.create(
                self.ADMIN_USERNAME,
                fullname="PlantDB Admin",
                password=admin_password,
                roles={Role.ADMIN},
            )
            # Print identifiers to the terminal
            print(f"Created admin user '{self.ADMIN_USERNAME}' with password '{admin_password}'")
            print("Change it as soon as possible!")
        return

    def get_user(self, username: str) -> Union[User, None]:
        """Retrieve a User object based on the provided username.

        Parameters
        ----------
        username : str
            The unique identifier for the user to be retrieved.

        Returns
        -------
        Union[plantdb.commons.auth.models.User, None]
            An instance of the User class representing the requested user.
            If it does not exist, `None` is returned.
        """
        if not self.exists(username):
            self.logger.error(f"User '{username}' does not exist!")
            return None
        return self.users[username]

    def get_token_user(self, token_payload: dict) -> Union[TokenUser, None]:
        """Retrieve a User object based on the provided username.

        Parameters
        ----------
        token_payload : dict
            The token containing the identifier for the user to be retrieved and special permissions.
            Must be a token with the type `'api'`.

        Returns
        -------
        Union[TokenUser, None]
            An instance of the ``User`` class representing the requested user.
            If it does not exist, ``None`` is returned.

        Raises
        ------
        TypeError
            If the provided token payload does not correspond to an api token.
        """
        if token_payload["type"] != "api":
            self.logger.error("Provided token is not an api token!")
            raise TypeError("Provided token is not an api token!")
        username = token_payload.get('username')
        if username not in self.users:
            self.logger.error(f"User '{username}' does not exist!")
            return None
        user = self.users[username]
        return TokenUser(**user.to_dict(), dataset_permissions=token_payload.get("datasets"))

    def is_locked_out(self, username) -> bool:
        """Check if an account is locked.

        Parameters
        ----------
        username : str
            The username of the account to check.

        Returns
        -------
        bool
            ``True`` if the account is locked, ``False`` otherwise.
        """
        user = self.get_user(username)
        is_locked = user._is_locked_out()
        if is_locked:
            self.logger.info(f"Account {username} is locked, try logging in after {user.locked_until}.")
        return is_locked

    def is_active(self, username) -> bool:
        """Check whether a user account is active.

        Parameters
        ----------
        username : str
            The name of the user account to check for activity status.

        Returns
        -------
        bool
            ``True`` if the user account is active, ``False`` otherwise.
        """
        user = self.get_user(username)
        if not user.is_active:
            self.logger.warning(f"Account {username} is not active.")
        return user.is_active

    def validate_user_password(self, username: str, password: str) -> bool:
        """Validate a user's password.

        This function checks if the provided plaintext password matches the hashed password stored for the given username.

        Parameters
        ----------
        username : str
            The username of the user.
        password : str
            The plain-text password to validate.

        Returns
        -------
        bool
            ``True`` if the password is valid, ``False`` otherwise.
        """
        pw_hash = self.get_user(username).password_hash
        try:
            # Verify password, raises an exception if wrong.
            ph.verify(pw_hash, password)
        except Exception as e:
            self.logger.error(f"Failed to verify password for {username}: {e}")
            return False
        else:
            return True

    def _record_failed_attempt(self, username: str, max_failed_attempts: int, lockout_duration: timedelta) -> None:
        """Record a failed login attempt for a user and apply lockout if necessary.

        This method logs a failed login attempt for the specified user. If the number of
        failed attempts reaches or exceeds `max_failed_attempts`, the user is locked out
        for the duration specified by `lockout_duration`. The updated user data is then
        saved to a file, and a warning message is logged.

        Parameters
        ----------
        username : str
            The username of the account that attempted to log in.
        max_failed_attempts : int
            The maximum number of allowed failed login attempts before lockout.
        lockout_duration : timedelta
            The duration for which the user will be locked out after exceeding `max_failed_attempts`.
        """
        user = self.get_user(username)
        user._record_failed_attempt()
        if user.failed_attempts >= max_failed_attempts:
            self._lock_user(username, lockout_duration)
        # Save the updated user data to a file
        self._save_users()
        self.logger.warning(f"Failed login attempt (n={user.failed_attempts}) for user: {username}")
        return

    def _lock_user(self, username: str, lockout_duration: timedelta) -> None:
        """Locks a user account for a specified duration.

        This function sets the 'locked_until' attribute of the given user to the current time plus the
        lockout duration. It also logs an informational message about the action taken.

        Parameters
        ----------
        username : str
            The name or identifier of the user to be locked.
        lockout_duration : timedelta
            The duration for which the user should be locked out, specified as a time delta object.

        Notes
        -----
        This function assumes that the `user` object has a 'locked_until' attribute and a valid logger.
        Ensure proper handling of exceptions if these attributes are missing or if the user does not exist.
        """
        user = self.get_user(username)
        user.locked_until = datetime.now() + lockout_duration
        self.logger.info(f"Locking user '{username}' for {lockout_duration} seconds...")
        return

    # Admin methods

    def unlock_user(self, user: User) -> None:
        """Unlock a specified user.

        Parameters
        ----------
        user : plantdb.commons.auth.User
            The user to unlock.
        """
        user.locked_until = None
        self._save_users()
        return

    def activate(self, user: User) -> None:
        """Activates a user.

        Parameters
        ----------
        user : plantdb.commons.auth.User
            The user to activate.
        """
        # Verify if the User exists
        try:
            assert self.exists(user.username)
        except AssertionError:
            self.logger.error(f"User '{user.username}' does not exists!")
            return

        user.is_active = True
        self._save_users()
        return

    def deactivate(self, user: User) -> None:
        """Deactivates a user.

        Parameters
        ----------
        user : plantdb.commons.auth.User
            The user to deactivate.
        """
        # Verify if the User exists
        try:
            assert self.exists(user.username)
        except AssertionError:
            self.logger.error(f"User '{user.username}' does not exists!")
            return

        user.is_active = False
        self._save_users()
        return

    # API methods

    def validate(self, username: str, password: str) -> bool:
        """Validate the user credentials.

        Parameters
        ----------
        username : str
            The username provided by the user attempting to log in.
        password : str
            The password provided by the user attempting to log in.

        Returns
        -------
        bool
            ``True`` if the login attempt is successful, ``False`` otherwise.
        """
        if not self.exists(username):
            return False
        if not self.is_active(username):
            return False
        if self.is_locked_out(username):
            return False

        user = self.get_user(username)
        if self.validate_user_password(username, password):
            # Reset failed attempts on successful login
            user.failed_attempts = 0
            user.last_login = datetime.now()
            self._save_users()
            return True
        else:
            # Record a failed attemp
            self._record_failed_attempt(username, self.max_login_attempts, self.lockout_duration)
            self.logger.error(f"Invalid credentials for user '{username}'")
            return False

    def update_password(self, username: str, password: str, new_password: str) -> None:
        """Update the password of an existing user.

        Parameters
        ----------
        username : str
            The name of the user whose password is to be updated.
        password : str
            The current password of the user to verify their identity.
        new_password : str
            The new password to set for the user. If ``None``, no change will occur.
        """
        # Verify if the login exists
        try:
            assert self.exists(username)
        except AssertionError:
            self.logger.error(f"User '{username}' does not exists!")
            return

        if not self.validate(username, password):
            return

        user = self.get_user(username)
        timestamp = datetime.now()  # Get the current timestamp for tracking user creation time.
        if new_password:
            user.password = self._hash_password(new_password)
            user.password_last_change = timestamp

        self._save_users()
        self.logger.info(f"Password updated for user '{username}'...")
        return


class GroupManager:
    """Manages groups for the RBAC system.

    This class handles the creation, modification, and persistence of user groups.
    Groups are stored in a JSON file and loaded/saved as needed.

    Attributes
    ----------
    groups_file : Union[str, Path]
        Path to the JSON file where groups are stored.
    groups : Dict[str, plantdb.commons.auth.models.Group]
        Dictionary mapping group names to Group objects.
    """

    def __init__(self, groups_file: Union[str, Path] = "groups.json"):
        """Initialize the GroupManager.

        Parameters
        ----------
        groups_file : Union[str, Path]
            Path to the JSON file for storing groups. Defaults to "groups.json".
        """
        self.groups_file = Path(groups_file)
        self.groups: Dict[str, Group] = {}
        self.logger = get_logger(__class__.__name__)
        self._load_groups()

    def _load_groups(self) -> None:
        """Load groups from the JSON file."""
        if not self.groups_file.exists():
            self.logger.warning(f"No groups database file found under '{self.groups_file}'")
            self.groups = {}
        elif self.groups_file.stat().st_size == 0:
            self.logger.warning(f"Empty groups database file found under '{self.groups_file}'")
            self.groups = {}
        else:
            # Load the groups list from the database
            with open(self.groups_file, 'r') as f:
                data = json.load(f)
            # Convert to a group-name indexed dict of Group objects
            self.groups = {}
            for name, group_data in data.items():
                self.groups[name] = Group(
                    name=name,
                    users=set(group_data['users']),
                    description=group_data.get('description'),
                    created_at=datetime.fromisoformat(group_data['created_at']),
                    created_by=group_data['created_by']
                )
        return

    def _save_groups(self) -> None:
        """Save groups to the JSON file."""
        data = {}
        for name, group in self.groups.items():
            data[name] = {
                'users': list(group.users),
                'description': group.description,
                'created_at': group.created_at.isoformat(),
                'created_by': group.created_by
            }

        temp_file = self.groups_file.with_suffix('.tmp')
        try:
            # Write to a temporary file first
            with open(temp_file, "w") as f:
                json.dump(data, f, indent=2)

            # Rename the temporary file to the final filename (atomic operation on most file systems)
            os.replace(temp_file, self.groups_file)
        except Exception as e:
            # If anything goes wrong, clean up the temporary file
            if temp_file.exists():
                temp_file.unlink()
            self.logger.error(f"Failed to save groups database: {str(e)}")
            raise Exception(f"Failed to save groups to {self.groups_file}: {e}")

    def create_group(self, name: str, creator: str, users: Optional[Set[str]] = None,
                     description: Optional[str] = None) -> Group:
        """Create a new group.

        Parameters
        ----------
        name : str
            The unique name for the group.
        creator : str
            The username of the user creating the group.
        users : Optional[Set[str]]
            Initial set of users to add to the group. Creator is automatically added.
        description : Optional[str]
            Optional description of the group.

        Returns
        -------
        Group
            The created group object.

        Raises
        ------
        ValueError
            If a group with the same name already exists.
        """
        if name in self.groups:
            raise ValueError(f"Group '{name}' already exists")

        # Initialize users set and ensure the creator is included
        if users is None:
            users = set()
        users.add(creator)

        group = Group(
            name=name,
            users=users,
            description=description,
            created_at=datetime.now(),
            created_by=creator
        )

        self.groups[name] = group
        self._save_groups()
        return group

    def get_group(self, name: str) -> Optional[Group]:
        """Get a group by name.

        Parameters
        ----------
        name : str
            The name of the group to retrieve.

        Returns
        -------
        Optional[Group]
            The group object if it exists, ``None`` otherwise.
        """
        return self.groups.get(name)

    def delete_group(self, name: str) -> bool:
        """Delete a group.

        Parameters
        ----------
        name : str
            The name of the group to delete.

        Returns
        -------
        bool
            ``True`` if the group was deleted, ``False`` if it didn't exist.
        """
        if name not in self.groups:
            return False

        del self.groups[name]
        self._save_groups()
        return True

    def add_user_to_group(self, group_name: str, username: str) -> bool:
        """Add a user to a group.

        Parameters
        ----------
        group_name : str
            The name of the group.
        username : str
            The username to add to the group.

        Returns
        -------
        bool
            ``True`` if the user was added, ``False`` if the group doesn't exist or the user was already in the group.
        """
        group = self.get_group(group_name)
        if not group:
            return False

        result = group.add_user(username)
        if result:
            self._save_groups()
        return result

    def remove_user_from_group(self, group_name: str, username: str) -> bool:
        """Remove a user from a group.

        Parameters
        ----------
        group_name : str
            The name of the group.
        username : str
            The username to remove from the group.

        Returns
        -------
        bool
            ``True`` if the user was removed, ``False`` if the group doesn't exist or the user wasn't in the group.
        """
        group = self.get_group(group_name)
        if not group:
            return False

        result = group.remove_user(username)
        if result:
            self._save_groups()
        return result

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
        user_groups = []
        for group in self.groups.values():
            if group.has_user(username):
                user_groups.append(group)
        return user_groups

    def list_groups(self) -> List[Group]:
        """Get a list of all groups.

        Returns
        -------
        List[Group]
            A list of all Group objects.
        """
        return list(self.groups.values())

    def group_exists(self, name: str) -> bool:
        """Check if a group exists.

        Parameters
        ----------
        name : str
            The name of the group to check.

        Returns
        -------
        bool
            True if the group exists, False otherwise.
        """
        return name in self.groups
