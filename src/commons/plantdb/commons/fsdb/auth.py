#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import os
import secrets
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from enum import Enum
from functools import wraps
from pathlib import Path
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple
from typing import Union

import jwt
from argon2 import PasswordHasher

from plantdb.commons.log import get_logger

DATETIME_FORMAT = '%Y-%m-%d_%H:%M:%S'

ph = PasswordHasher()


class Permission(Enum):
    """A class representing the different permission levels for user actions.

    This enumeration defines various permissions that can be assigned to users,
    controlling their access to different functionalities within the system.

    Attributes
    ----------
    READ : str
        Permission to read scan data and metadata.
    WRITE : str
        Permission to write filesets, files, and associated metadata in existing scan datasets.
    CREATE : str
        Permission to create new scan datasets.
    DELETE : str
        Permission to delete scan datasets, filesets, and files.
    MANAGE_USERS : str
        Permission to add and remove users.
    MANAGE_GROUPS : str
        Permission to manage groups (create, delete, modify membership).

    Examples
    --------
    >>> from plantdb.commons.fsdb.auth import Permission
    Accessing a specific permission:
    >>> print(Permission.READ)
    Permission.READ
    Iterating through all permissions:
    >>> for perm in Permission:
    ...     print(f"{perm.name}: {perm.value}")
    READ: read
    WRITE: write
    CREATE: create
    DELETE: delete
    MANAGE_USERS: manage_users
    MANAGE_GROUPS: manage_groups
    """
    READ = "read"
    WRITE = "write"
    CREATE = "create"
    DELETE = "delete"
    MANAGE_USERS = "manage_users"
    MANAGE_GROUPS = "manage_groups"


class Role(Enum):
    """A class representing the role of a user in a system.

    The Role enum defines three types of roles that can be assigned to users:

        * READER: Has read-only access.
        * CONTRIBUTOR: Can modify content but not delete.
        * ADMIN: Full control over all aspects of the system.

    This enum helps in managing permissions and access levels efficiently within an application.

    Attributes
    ----------
    READER : str
        Represents a user with read-only access.
    CONTRIBUTOR : str
        Represents a user who can modify content but not delete it.
    ADMIN : str
        Represents a user with full control over the system.

    Examples
    --------
    >>> from plantdb.commons.fsdb.auth import Role
    >>> Role.READER
    <Role.READER: 'reader'>
    >>> Role.CONTRIBUTOR
    <Role.CONTRIBUTOR: 'contributor'>
    >>> Role.ADMIN
    <Role.ADMIN: 'admin'>
    >>> Role.ADMIN.permissions
    {<Permission.ADMIN_ALL: 'admin_all'>,
     <Permission.CREATE_SCAN: 'create_scan'>,
     <Permission.DELETE_SCAN: 'delete_scan'>,
     <Permission.MANAGE_USERS: 'manage_users'>,
     <Permission.READ_SCAN: 'read_scan'>,
     <Permission.WRITE_SCAN: 'write_scan'>}
    """
    READER = "reader"
    CONTRIBUTOR = "contributor"
    ADMIN = "admin"

    @property
    def permissions(self) -> Set[Permission]:
        """Get the set of permissions associated with this role.

        Returns
        -------
        Set[Permission]
            A set containing all permissions granted to this role.
        """
        role_permissions = {
            Role.READER: {
                Permission.READ,
            },
            Role.CONTRIBUTOR: {
                Permission.READ,
                Permission.WRITE,
                Permission.CREATE,
            },
            Role.ADMIN: {
                Permission.READ,
                Permission.WRITE,
                Permission.CREATE,
                Permission.DELETE,
                Permission.MANAGE_USERS,
                Permission.MANAGE_GROUPS,
            }
        }
        return role_permissions[self]


@dataclass
class User:
    """
    Summarize the purpose of the User class.

    The User class represents a user entity in an application. It contains attributes related to user authentication, roles,
    permissions, and activity timestamps. The class is designed to encapsulate user data and provide methods for user management.
    Users can have multiple roles and permissions, which are stored as sets. The class also tracks the creation time and last login
    time of a user.

    Attributes
    ----------
    username : str
        The unique username of the user.
    password_hash : str
        The hashed password of the user.
    roles : Set[Role]
        A set containing roles assigned to the user.
    created_at : datetime
        The timestamp when the user account was created.
    permissions : Set[Permission], optional
        A set containing specific permissions granted to the user.
    last_login : Optional[datetime], optional
        The timestamp of the last login. If not provided, defaults to None.
    is_active : bool, optional
        Indicates if the user account is active. Defaults to True.
    failed_attempts : int, optional
        Number of failed login attempts for the user. Defaults to 0.
    locked_until : Optional[datetime], optional
        Timestamp until which the user is locked out due to multiple failed attempts. Defaults to None.

    Notes
    -----
    Ensure that sensitive data like `password_hash` is handled securely and not exposed in logs or error messages.

    Examples
    --------
    >>> from datetime import datetime, timezone
    >>> from plantdb.commons.fsdb.auth import Permission, Role, User
    >>> user = User(
    ...     username="jdoe",
    ...     fullname="John Doe",
    ...     password_hash="hashed_password",
    ...     roles={Role.CONTRIBUTOR},
    ...     permissions={Permission.MANAGE_USERS},
    ...     created_at=datetime.now(timezone.utc),
    ... )
    >>> print(user.username)
    john_doe
    >>> print(user.roles)  # get roles
    {<Role.CONTRIBUTOR: 'contributor'>}
    >>> print(user.permissions)  # get directly assigned permissions
    {<Permission.MANAGE_USERS: 'manage_users'>}
    >>> user.last_login = datetime.now(timezone.utc)  # set the timestamp when the user account was last login
    """
    username: str
    fullname: str
    password_hash: str
    roles: Set[Role]
    created_at: datetime
    permissions: Optional[Set[Permission]] = None
    last_login: Optional[datetime] = None
    is_active: bool = True
    failed_attempts: int = 0
    last_failed_attempt: Optional[datetime] = None
    locked_until: Optional[datetime] = None
    password_last_change: Optional[datetime] = None

    def __eq__(self, other):
        """
        Compare two User objects for equality.

        Two User objects are considered equal if all their attributes have the same values.

        Parameters
        ----------
        other : Any
            The object to compare with.

        Returns
        -------
        bool
            ``True`` if the objects are equal, ``False`` otherwise.
        """
        if not isinstance(other, User):
            return False

        return all(self.__dict__[attr] == other.__dict__[attr] for attr in self.__dict__)

    def to_dict(self) -> dict:
        """
        Convert User object to dictionary for JSON serialization.

        Returns
        -------
        dict
            Dictionary representation of the user object.
        """
        return {
            'username': self.username,
            'fullname': self.fullname,
            'password_hash': self.password_hash,
            'roles': [role.value for role in self.roles],
            'created_at': self.created_at.isoformat(),
            'permissions': [perm.value for perm in self.permissions] if self.permissions else None,
            'last_login': self.last_login.isoformat() if self.last_login else None,
            'is_active': self.is_active,
            'failed_attempts': self.failed_attempts,
            'last_failed_attempt': self.last_failed_attempt.isoformat() if self.last_failed_attempt else None,
            'locked_until': self.locked_until.isoformat() if self.locked_until else None,
            'password_last_change': self.password_last_change.isoformat() if self.password_last_change else self.created_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'User':
        """
        Create User object from dictionary (JSON deserialization).

        Parameters
        ----------
        data : dict
            Dictionary containing user data.

        Returns
        -------
        User
            User object created from the dictionary data.

        Raises
        ------
        ValueError
            If required fields are missing or invalid.
        KeyError
            If required keys are missing from the dictionary.
        """
        try:
            # Parse roles
            roles = set()
            for role_value in data['roles']:
                try:
                    roles.add(Role(role_value))
                except ValueError:
                    # Skip invalid roles for backward compatibility
                    pass

            # Parse permissions
            permissions = set()
            if data.get('permissions'):
                for perm_value in data['permissions']:
                    try:
                        permissions.add(Permission(perm_value))
                    except ValueError:
                        # Skip invalid roles for backward compatibility
                        pass

            def _datetime_convert(data):
                if isinstance(data, datetime):
                    return data
                elif isinstance(data, str):
                    # Assume string is in iso format
                    return datetime.fromisoformat(data)
                else:
                    return None

            return cls(
                username=data['username'],
                fullname=data['fullname'],
                password_hash=data['password_hash'],
                roles=roles,
                created_at=_datetime_convert(data['created_at']),
                permissions=permissions,
                last_login=_datetime_convert(data.get('last_login')),
                is_active=data.get('is_active', True),
                failed_attempts=data.get('failed_attempts', 0),
                last_failed_attempt=_datetime_convert(data.get('last_failed_attempt')),
                locked_until=_datetime_convert(data.get('locked_until')),
                password_last_change=_datetime_convert(data.get('password_last_change')),
            )
        except KeyError as e:
            raise KeyError(f"Missing required field in user data: {e}")
        except ValueError as e:
            raise ValueError(f"Invalid data format in user data: {e}")

    def to_json(self) -> str:
        """
        Convert User object to JSON string.

        Returns
        -------
        str
            JSON string representation of the user object.


        """
        return json.dumps(self.to_dict(), indent=2)

    @classmethod
    def from_json(cls, json_str: str) -> 'User':
        """
        Create User object from JSON string.

        Parameters
        ----------
        json_str : str
            JSON string containing user data.

        Returns
        -------
        User
            User object created from the JSON data.

        Raises
        ------
        json.JSONDecodeError
            If the JSON string is invalid.
        ValueError
            If required fields are missing or invalid.

        Examples
        --------
        >>> import json
        >>> from plantdb.commons.fsdb.auth import User
        >>>
        """
        try:
            data = json.loads(json_str)
            return cls.from_dict(data)
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(f"Invalid JSON format: {e}", json_str, e.pos)

    def _is_locked_out(self) -> bool:
        """Verify if the account is locked.

        Parameters
        ----------
        username : str
            The username of the account to check for lock status.

        Returns
        -------
        bool
            ``True`` if the account is locked, otherwise ``False``.
        """
        if self.locked_until:
            if datetime.now() < self.locked_until:
                return True
            else:
                self.locked_until = None
        return False

    def _record_failed_attempt(self) -> None:
        """Record failed login attempt.

        Parameters
        ----------
        username : str
            The username for which the failed login attempt is being recorded.
        """
        self.failed_attempts += 1
        self.last_failed_attempt = datetime.now()
        return


@dataclass
class Group:
    """
    Represents a group of users for sharing scan datasets.

    Groups allow multiple users to collaborate on scan datasets. When a scan is shared
    with a group, all members of that group get CONTRIBUTOR role for that specific dataset.

    Attributes
    ----------
    name : str
        The unique name of the group.
    users : Set[str]
        A set of usernames that belong to this group.
    description : Optional[str], optional
        An optional description of the group's purpose.
    created_at : datetime
        The timestamp when the group was created.
    created_by : str
        The username of the user who created the group.

    Examples
    --------
    >>> from datetime import datetime, timezone
    >>> from plantdb.commons.fsdb.auth import Group
    >>> group = Group(
    ...     name="plant_researchers",
    ...     users={"alice", "bob"},
    ...     description="Plant research team",
    ...     created_at=datetime.now(timezone.utc),
    ...     created_by="alice"
    ... )
    >>> print(group.name)
    plant_researchers
    >>> "alice" in group.users
    True
    """
    name: str
    users: Set[str]
    created_at: datetime
    created_by: str
    description: Optional[str] = None

    def add_user(self, username: str) -> bool:
        """
        Add a user to the group.

        Parameters
        ----------
        username : str
            The username to add to the group.

        Returns
        -------
        bool
            True if the user was added (wasn't already in the group), False otherwise.
        """
        if username in self.users:
            return False
        self.users.add(username)
        return True

    def remove_user(self, username: str) -> bool:
        """
        Remove a user from the group.

        Parameters
        ----------
        username : str
            The username to remove from the group.

        Returns
        -------
        bool
            True if the user was removed (was in the group), False otherwise.
        """
        if username not in self.users:
            return False
        self.users.remove(username)
        return True

    def has_user(self, username: str) -> bool:
        """
        Check if a user is a member of the group.

        Parameters
        ----------
        username : str
            The username to check.

        Returns
        -------
        bool
            True if the user is a member of the group, False otherwise.
        """
        return username in self.users


class UserManager():
    """
    UserManager class for managing user data.

    The UserManager class provides methods to create, load,
    save, and manage users. It uses a JSON file to persist user data.
    It includes functionalities like password hashing and verification,
    user account lockout management, and handling of guest accounts.

    Attributes
    ----------
    users_file : str
        Path to the JSON file where user data is stored.
    users : Dict[str, User]
        Dictionary containing all users, indexed by username.
    GUEST_USERNAME : str
        Default username for guest accounts.
    GUEST_PASSWORD : str
        Default password for guest accounts.

    Examples
    --------
    >>> from pathlib import Path
    >>> from tempfile import gettempdir
    >>> from plantdb.commons.fsdb.auth import UserManager
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
        """
        Loads the user database from a JSON file and populates the internal dictionary.

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
        """
        Save the user data to a JSON file.

        This method serializes the user objects into dictionaries, writes them to a temporary file,
        and then renames the temporary file to replace the original file. This ensures atomicity of the operation.

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
        """
        Hash a plaintext password using argon2.

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
        """
        Check if a user exists.

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
        the username and the keys to lower or upper case before comparison.
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
        roles : Role or Set[Role], optional
            The roles to associate with the user.
            If ``None`` (default), use the `Role.READER` role.

        Examples
        --------
        >>> from plantdb.commons.fsdb.auth import Role, UserManager
        >>> manager = UserManager()
        >>> manager.create('batman', "Bruce Wayne", "joker", Role.ADMIN)
        >>> print(manager.users)
        batman
        """
        username = username.lower()  # Convert the username to lowercase to maintain uniformity.
        timestamp = datetime.now()  # Get the current timestamp for tracking user creation time.

        # Verify if the login is available
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
        # Save all user data (including the newly created user) to 'users.json' file.
        self._save_users()
        self.logger.debug(f"Created user '{username}' with fullname '{fullname}'.")
        if not username == self.GUEST_USERNAME:
            self.logger.info(f"Welcome {fullname}, please login...'")
        return

    def _ensure_guest_user(self) -> None:
        """
        Ensure that a guest user exists in the system.

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
        """
        Ensure that an admin user exists in the system.

        If the admin user does not already exist, it creates one with a default username.
        The password is a string of 25 hex digits, printed-out to the terminal.
        This method is intended to be used internally the class to ensure the presence of an admin account for various operational purposes.

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
        """
        Retrieve a User object based on the provided username.

        Parameters
        ----------
        username : str
            The unique identifier for the user to be retrieved.

        Returns
        -------
        Union[User, None]
            An instance of the User class representing the requested user.
            If it does not exist, `None` is returned.
        """
        if not self.exists(username):
            self.logger.error(f"User '{username}' does not exist!")
            return None
        return self.users[username]

    def is_locked_out(self, username) -> bool:
        """
        Check if an account is locked.

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
            self.logger.info(f"Account {user} is locked, try logging in after {user.locked_until}.")
        return is_locked

    def is_active(self, username) -> bool:
        """
        Check whether a user account is active.

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
        """
        Validate a user's password.

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
        hash = self.get_user(username).password_hash
        try:
            # Verify password, raises exception if wrong.
            ph.verify(hash, password)
        except Exception as e:
            self.logger.error(f"Failed to verify password for {username}: {e}")
            return False
        else:
            return True

    def _record_failed_attempt(self, username: str, max_failed_attempts: int, lockout_duration: timedelta) -> None:
        """
        Record a failed login attempt for a user and apply lockout if necessary.

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
            The duration for which the user will be locked out after exceeding
            `max_failed_attempts`.
        """
        user = self.get_user(username)
        user._record_failed_attempt()
        if user.failed_attempts >= max_failed_attempts:
            self._lock_user(username, lockout_duration)
        # Save the updated user data to file
        self._save_users()
        self.logger.warning(f"Failed login attempt (n={user.failed_attempts}) for user: {username}")
        return

    def _lock_user(self, username: str, lockout_duration: timedelta) -> None:
        """
        Locks a user account for a specified duration.

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
        """
        Unlock a specified user.

        Parameters
        ----------
        username : plantdb.commons.fsdb.auth.User
            The user to unlock.
        """
        user.locked_until = None
        self._save_users()
        return

    def activate(self, user: User) -> None:
        """
        Activates a user.

        Parameters
        ----------
        user : plantdb.commons.fsdb.auth.User
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
        """
        Deactivates a user.

        Parameters
        ----------
        user : plantdb.commons.fsdb.auth.User
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
        """
        Update the password of an existing user.

        Parameters
        ----------
        username : str
            The name of the user whose password is to be updated.
        password : str
            The current password of the user to verify their identity.
        new_password : str
            The new password to set for the user. If None, no change will occur.
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
    """
    Manages groups for the RBAC system.

    This class handles the creation, modification, and persistence of user groups.
    Groups are stored in a JSON file and loaded/saved as needed.

    Attributes
    ----------
    groups_file : str
        Path to the JSON file where groups are stored.
    groups : Dict[str, Group]
        Dictionary mapping group names to Group objects.
    """

    def __init__(self, groups_file: str = "groups.json"):
        """
        Initialize the GroupManager.

        Parameters
        ----------
        groups_file : str, optional
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
        """
        Create a new group.

        Parameters
        ----------
        name : str
            The unique name for the group.
        creator : str
            The username of the user creating the group.
        users : Optional[Set[str]], optional
            Initial set of users to add to the group. Creator is automatically added.
        description : Optional[str], optional
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

        # Initialize users set and ensure creator is included
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
        """
        Get a group by name.

        Parameters
        ----------
        name : str
            The name of the group to retrieve.

        Returns
        -------
        Optional[Group]
            The group object if it exists, None otherwise.
        """
        return self.groups.get(name)

    def delete_group(self, name: str) -> bool:
        """
        Delete a group.

        Parameters
        ----------
        name : str
            The name of the group to delete.

        Returns
        -------
        bool
            True if the group was deleted, False if it didn't exist.
        """
        if name not in self.groups:
            return False

        del self.groups[name]
        self._save_groups()
        return True

    def add_user_to_group(self, group_name: str, username: str) -> bool:
        """
        Add a user to a group.

        Parameters
        ----------
        group_name : str
            The name of the group.
        username : str
            The username to add to the group.

        Returns
        -------
        bool
            True if the user was added, False if the group doesn't exist or user was already in group.
        """
        group = self.get_group(group_name)
        if not group:
            return False

        result = group.add_user(username)
        if result:
            self._save_groups()
        return result

    def remove_user_from_group(self, group_name: str, username: str) -> bool:
        """
        Remove a user from a group.

        Parameters
        ----------
        group_name : str
            The name of the group.
        username : str
            The username to remove from the group.

        Returns
        -------
        bool
            True if the user was removed, False if the group doesn't exist or user wasn't in group.
        """
        group = self.get_group(group_name)
        if not group:
            return False

        result = group.remove_user(username)
        if result:
            self._save_groups()
        return result

    def get_user_groups(self, username: str) -> List[Group]:
        """
        Get all groups that a user belongs to.

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
        """
        Get a list of all groups.

        Returns
        -------
        List[Group]
            A list of all Group objects.
        """
        return list(self.groups.values())

    def group_exists(self, name: str) -> bool:
        """
        Check if a group exists.

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


def requires_permission(required_permissions: Union[Permission, List[Permission]]):
    """
    Decorator to check if the specified user has the required permission(s).

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


class RBACManager:
    """
    Manage Role-Based Access Control (RBAC) for users and permissions.

    This class provides methods to determine which permissions a user has,
    check if a user has a specific permission, and verify if a user can access
    or perform operations on scans based on their roles and permissions.

    Attributes
    ----------
    GUEST_USERNAME : str
        The username for the default guest user account.
    groups : GroupManager
        Manager for handling group operations.

    Examples
    --------
    >>> from plantdb.commons.fsdb.auth import RBACManager
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
        """
        Initialize the RBACManager.

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
        """
        Get the set of permissions a user has based on their assigned roles.

        This function returns a set containing all permissions directly assigned to
        the user as well as those inherited from any roles they are part of. The
        resulting set is a union of both direct and indirect permissions.

        Parameters
        ----------
        user : User
            A `User` object representing the user whose permissions will be checked.
            This user must have attributes `permissions` and `roles`.

        Returns
        -------
        Set[Permission]
            A set containing all permissions that the specified user has access to,
            including those inherited from roles.

        Examples
        --------
        >>> from plantdb.commons.fsdb.auth import Permission
        >>> from plantdb.commons.fsdb.auth import User
        >>> role_admin = Role('admin')
        >>> role_user = Role('user')
        >>> permission_a = Permission()  # Mocking a specific permission
        >>> permission_b = Permission()  # Mocking another specific permission
        >>> user = User(permissions={permission_a}, roles={role_user})
        >>> role_permissions = {Role('admin'): {permission_b}}
        >>> user.get_user_permissions(user)  # doctest: +SKIP
        {<__main__.Permission object at 0x...>}

        Notes
        -----
        The result depends on the `role_permissions` attribute of the class instance.
        Ensure that this dictionary is properly initialized before calling this method.

        See Also
        --------
        User : Represents a user with permissions and roles.
        Permission : Represents a permission that can be assigned to users or roles.
        Role : Represents a role with specific permissions.

        """
        permissions = set(user.permissions) if user.permissions else set()
        for role in user.roles:
            permissions.update(role.permissions)
        return permissions

    def has_permission(self, user: User, permission: Permission) -> bool:
        """
        Check if a user has a specific permission.

        This function determines whether the given user has the specified permission,
        including administrative privileges.

        Parameters
        ----------
        user : User
            The user object to check for permissions.
        permission : Permission
            The permission level or type to verify against the user's permissions.

        Returns
        -------
        bool
            `True` if the user has the given permission, `False` otherwise.

        Notes
        -----
        The function checks for the specific permission in the user's permission set.

        See Also
        --------
        get_user_permissions : Retrieve the list of permissions a user has.
        """
        user_permissions = self.get_user_permissions(user)
        return permission in user_permissions

    def is_guest_user(self, user: User) -> bool:
        """
        Check if the given user is the guest user.

        Parameters
        ----------
        user : User
            The user object to check.

        Returns
        -------
        bool
            True if the user is the guest user, False otherwise.
        """
        return user.username == self.users.GUEST_USERNAME

    def get_guest_user(self) -> User:
        return self.users.get_user(self.users.GUEST_USERNAME)

    def can_manage_groups(self, user: User) -> bool:
        """
        Check if a user can manage groups (create/delete groups).

        Parameters
        ----------
        user : User
            The user to check.

        Returns
        -------
        bool
            True if the user can manage groups, False otherwise.
        """
        return self.has_permission(user, Permission.MANAGE_GROUPS)

    def can_create_group(self, user: User) -> bool:
        """
        Check if a user can create groups.

        Any user with CONTRIBUTOR role or higher can create groups.

        Parameters
        ----------
        user : User
            The user to check.

        Returns
        -------
        bool
            True if the user can create groups, False otherwise.
        """
        return self.has_permission(user, Permission.CREATE)

    def can_add_to_group(self, user: User, group_name: str) -> bool:
        """
        Check if a user can add members to a specific group.

        Users can add members to a group if:
        1. They are an admin (MANAGE_GROUPS permission), OR
        2. They are a member of the group

        Parameters
        ----------
        user : User
            The user requesting to add members.
        group_name : str
            The name of the group.

        Returns
        -------
        bool
            True if the user can add members to the group, False otherwise.
        """
        # Admins can manage any group
        if self.has_permission(user, Permission.MANAGE_GROUPS):
            return True

        # Group members can add users to their groups
        group = self.groups.get_group(group_name)
        return group is not None and group.has_user(user.username)

    def can_delete_group(self, user: User) -> bool:
        """
        Check if a user can delete a group.

        Only users with the `MANAGE_GROUPS` permission can delete groups.

        Parameters
        ----------
        user : User
            The user requesting to delete the group.

        Returns
        -------
        bool
            ``True`` if the user can delete the group, ``False`` otherwise.
        """
        return self.has_permission(user, Permission.MANAGE_GROUPS)

    def create_group(self, user: User, name: str, users: Optional[Set[str]] = None,
                     description: Optional[str] = None) -> Optional[Group]:
        """
        Create a new group if the user has permission.

        Parameters
        ----------
        user : User
            The user creating the group.
        name : str
            The unique name for the group.
        users : Optional[Set[str]], optional
            Initial set of users to add to the group.
        description : Optional[str], optional
            Optional description of the group.

        Returns
        -------
        Optional[Group]
            The created group object if successful, None if permission denied.

        Raises
        ------
        ValueError
            If a group with the same name already exists.
        """
        if not self.can_create_group(user):
            return None

        return self.groups.create_group(name, user.username, users, description)

    def add_user_to_group(self, user: User, group_name: str, username_to_add: str) -> bool:
        """
        Add a user to a group if the requesting user has permission.

        Parameters
        ----------
        user : User
            The user requesting to add a member.
        group_name : str
            The name of the group.
        username_to_add : str
            The username to add to the group.

        Returns
        -------
        bool
            True if the user was added successfully, False if permission denied or operation failed.
        """
        if not self.can_add_to_group(user, group_name):
            return False

        return self.groups.add_user_to_group(group_name, username_to_add)

    def remove_user_from_group(self, user: User, group_name: str, username_to_remove: str) -> bool:
        """
        Remove a user from a group if the requesting user has permission.

        Parameters
        ----------
        user : User
            The user requesting to remove a member.
        group_name : str
            The name of the group.
        username_to_remove : str
            The username to remove from the group.

        Returns
        -------
        bool
            True if the user was removed successfully, False if permission denied or operation failed.
        """
        if not self.can_add_to_group(user, group_name):  # Same permission as adding
            return False

        return self.groups.remove_user_from_group(group_name, username_to_remove)

    def delete_group(self, user: User, group_name: str) -> bool:
        """
        Delete a group if the user has permission.

        Parameters
        ----------
        user : User
            The user requesting to delete the group.
        group_name : str
            The name of the group to delete.

        Returns
        -------
        bool
            True if the group was deleted successfully, False if permission denied or group not found.
        """
        if not self.can_delete_group(user):
            return False

        return self.groups.delete_group(group_name)

    def get_user_groups(self, username: str) -> List[Group]:
        """
        Get all groups that a user belongs to.

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
        """
        List all groups if the user has permission.

        Parameters
        ----------
        user : User
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
        """
        Get the effective role a user has for a specific scan dataset.

        This method determines the user's role based on:
        1. Dataset ownership (owner gets CONTRIBUTOR role)
        2. Group sharing (shared group members get CONTRIBUTOR role)
        3. User's global role (fallback)

        Parameters
        ----------
        user : User
            The user to check.
        scan_metadata : dict
            The scan metadata containing 'owner' and optional 'sharing' fields.

        Returns
        -------
        Role
            The effective role for this specific scan dataset.
        """
        # Get the scan owner, default to guest if not specified
        scan_owner = scan_metadata.get('owner', self.users.GUEST_USERNAME)

        # If user is the owner, they get CONTRIBUTOR role (unless they're admin)
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

        # Check if user belongs to any shared groups
        shared_groups = scan_metadata.get('sharing', [])
        if shared_groups:
            user_groups = self.groups.get_user_groups(user.username)
            user_group_names = {group.name for group in user_groups}

            # If user belongs to any shared group, they get CONTRIBUTOR role for this scan
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
        """
        Get the set of permissions a user has for a specific scan dataset.

        This method considers the user's effective role for the specific scan,
        taking into account ownership and group sharing.

        Parameters
        ----------
        user : User
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
        """
        Check if a user can perform a specific operation on a scan dataset.

        This method implements the complete access control logic including:
        - Owner-based access (owners get CONTRIBUTOR role for their scans)
        - Group-based access (shared group members get CONTRIBUTOR role)
        - Global role-based access (fallback to user's global role)
        - Admin override (admins can do everything)

        Parameters
        ----------
        user : User
            The user requesting access.
        scan_metadata : dict
            The scan metadata containing 'owner' and optional 'sharing' fields.
        operation : Permission
            The operation/permission being requested.

        Returns
        -------
        bool
            ``True`` if the user can perform the operation, ``False`` otherwise.
        """
        scan_permissions = self.get_scan_permissions(user, scan_metadata)
        return operation in scan_permissions

    def can_access_scan_by_owner(self, user: User, scan_owner: str, operation: Permission) -> bool:
        """
        Legacy method for checking scan access by owner name only.

        This method provides backward compatibility but doesn't support group sharing.
        For full RBAC support, use can_access_scan() with full metadata.

        Parameters
        ----------
        user : User
            The user requesting access.
        scan_owner : str
            The username of the scan owner.
        operation : Permission
            The operation/permission being requested.

        Returns
        -------
        bool
            True if the user can perform the operation, False otherwise.
        """
        # Create minimal metadata with just owner
        scan_metadata = {'owner': scan_owner}
        return self.can_access_scan(user, scan_metadata, operation)

    def can_modify_scan_owner(self, user: User, scan_metadata: dict) -> bool:
        """
        Check if a user can modify the 'owner' field of a scan.

        Only admins can modify scan ownership.

        Parameters
        ----------
        user : User
            The user requesting to modify ownership.
        scan_metadata : dict
            The scan metadata.

        Returns
        -------
        bool
            True if the user can modify the owner field, False otherwise.
        """
        return self.has_permission(user, Permission.MANAGE_USERS)

    def can_modify_scan_sharing(self, user: User, scan_metadata: dict) -> bool:
        """
        Check if a user can modify the 'sharing' field of a scan.

        Users can modify sharing if they have WRITE permission for the scan
        (i.e., they are the owner, in a shared group, or have global CONTRIBUTOR+ role).

        Parameters
        ----------
        user : User
            The user requesting to modify sharing.
        scan_metadata : dict
            The scan metadata.

        Returns
        -------
        bool
            True if the user can modify the sharing field, False otherwise.
        """
        return self.can_access_scan(user, scan_metadata, Permission.WRITE)

    def validate_scan_metadata_access(self, user: User, old_metadata: dict, new_metadata: dict) -> bool:
        """
        Validate that a user can make the proposed metadata changes.

        This method checks if the user has permission to modify specific fields
        like 'owner' and 'sharing' based on the RBAC rules.

        Parameters
        ----------
        user : User
            The user attempting to modify metadata.
        old_metadata : dict
            The current scan metadata.
        new_metadata : dict
            The proposed new metadata.

        Returns
        -------
        bool
            True if all proposed changes are allowed, False otherwise.
        """
        # Check if owner field is being modified
        old_owner = old_metadata.get('owner', self.users.GUEST_USERNAME)
        new_owner = new_metadata.get('owner', self.users.GUEST_USERNAME)

        if old_owner != new_owner:
            if not self.can_modify_scan_owner(user, old_metadata):
                return False

        # Check if sharing field is being modified
        old_sharing = old_metadata.get('sharing', [])
        new_sharing = new_metadata.get('sharing', [])

        if old_sharing != new_sharing:
            if not self.can_modify_scan_sharing(user, old_metadata):
                return False

        return True

    def ensure_scan_owner(self, scan_metadata: dict) -> dict:
        """
        Ensure a scan has an owner field, defaulting to guest if missing.

        This method should be called when loading or creating scans to ensure
        the owner field is always present.

        Parameters
        ----------
        scan_metadata : dict
            The scan metadata to check and potentially modify.

        Returns
        -------
        dict
            The metadata with owner field guaranteed to be present.
        """
        if 'owner' not in scan_metadata:
            scan_metadata = scan_metadata.copy()
            scan_metadata['owner'] = self.users.GUEST_USERNAME
        return scan_metadata

    def validate_sharing_groups(self, sharing_groups: List[str]) -> bool:
        """
        Validate that all groups in the sharing list exist.

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
        """
        Filter scans to only include those the user has READ access to.

        This method can be used to implement scan listing with proper access control.

        Parameters
        ----------
        user : User
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

            # Check if user has READ access to this scan
            if self.can_access_scan(user, metadata, Permission.READ):
                accessible_scans[scan_id] = metadata

        return accessible_scans

    def get_user_scan_role_summary(self, user: User, scan_metadata: dict) -> dict:
        """
        Get a summary of the user's access to a specific scan.

        This is useful for debugging and user interfaces to show access levels.

        Parameters
        ----------
        user : User
            The user to analyze.
        scan_metadata : dict
            The scan metadata.

        Returns
        -------
        dict
            A dictionary containing access information including:
            - effective_role: The user's effective role for this scan
            - permissions: List of permissions the user has
            - access_reason: Why the user has this level of access
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


class SessionManager:
    """
    Manages user sessions with expiration and validation.

    This class provides methods to create, validate, invalidate,
    and cleanup expired sessions. Each session is associated with a
    unique identifier (session_id) and has an expiry time based on the
    session timeout duration specified during initialization.

    Attributes
    ----------
    sessions : Dict[str, dict]
        A dictionary storing active sessions.
        Each key is a session ID, and each value is a dictionary containing:
        - 'username': str - The user associated with this session.
        - 'created_at': datetime - When the session was created.
        - 'last_accessed': datetime - Last time the session was accessed.
        - 'expires_at': datetime - Expiry time of the session.
    session_timeout : int
        Duration in seconds after which a session expires.
    max_concurrent_sessions : int
        The maximum number of concurrent sessions to allow.
    logger : logging.Logger
        The logger to use for this session manager.
    """

    def __init__(self, session_timeout: int = 3600, max_concurrent_sessions: int = 10):  # 1 hour default
        """
        Manage user sessions with timeout.

        Parameters
        ----------
        session_timeout : int, optional
            The duration for which the session should be valid in seconds.
            A session that exceeds this duration will be considered expired and removed.
            Defaults to ``3600`` seconds.
        max_concurrent_sessions : int, optional
            The maximum number of concurrent sessions to allow.
            Defaults to ``10``.
        """
        self.sessions: Dict[str, dict] = {}
        self.session_timeout = session_timeout
        self.max_concurrent_sessions = max_concurrent_sessions

        self.logger = get_logger(__class__.__name__)

    def _user_has_session(self, username) -> bool:
        """
        Check if a user has an active session.

        Parameters
        ----------
        username : str
            The unique identifier for the user whose active session status needs to be checked.

        Returns
        -------
        bool
            ``True`` if the user has an active session, ``False`` otherwise.
        """
        self.cleanup_expired_sessions()
        if username is not None:
            for _, session in self.sessions.items():
                if session['username'] == username:
                    return True
        return False

    def n_active_sessions(self) -> int:
        """
        Returns the number of active sessions.

        Cleans up expired sessions before counting and returns the number
        of remaining active sessions in the collection.

        Returns
        -------
        int
            The number of currently active sessions.

        See Also
        --------
        cleanup_expired_sessions : Cleans up the expired sessions in the collection.
        """
        self.cleanup_expired_sessions()
        return len(self.sessions)

    def _create_token(self, **kwargs) -> str:
        """
        Generate a secure random token.

        Returns
        -------
        str
            A securely generated (URL-safe) token.
        """
        return secrets.token_urlsafe(32)

    def create_session(self, username: str) -> Union[str, None]:
        """
        Create a new session for a user.

        If the user already has an active session, it returns the existing session ID.
        Otherwise, it creates a new session and returns its ID.

        Parameters
        ----------
        username : str
            The unique identifier of the user for whom to create a session.

        Returns
        -------
        Union[str, None]
            The ID of the created or existing session.

        Notes
        -----
        The session ID is a token generated using `secrets.token_urlsafe`.
        The session data includes the user ID, creation timestamp, last accessed timestamp, and expiration timestamp.
        """
        if self._user_has_session(username):
            self.logger.warning(f"User '{username}' already has an active session!")
            return None

        if self.n_active_sessions() >= self.max_concurrent_sessions:
            self.logger.warning(
                f"Reached max concurrent sessions limit ({self.max_concurrent_sessions})")
            return None

        now = datetime.now()
        exp_time = now + timedelta(seconds=self.session_timeout)
        # Create a session token
        session_token = secrets.token_urlsafe(32)

        self.sessions[session_token] = {
            'username': username,
            'created_at': now,
            'last_accessed': now,
            'expires_at': exp_time
        }
        return session_token

    def validate_session(self, session_id: str) -> Optional[dict]:
        """
        Validate a given session by checking its existence and expiration status.

        Parameters
        ----------
        session_id : str
            The unique identifier of the session to be validated.

        Returns
        -------
        dict or None
            A dictionary with user information if valid, ``None`` if invalid/expired.
            Returns dictionary with:
            - username: The authenticated user
            - created_at: When the session was created
            - last_accessed: When the session was last validated
            - expires_at: When the session expires

        Notes
        -----
        The `validate_session` method updates the session's last accessed time upon successful validation.
        """
        if session_id not in self.sessions:
            self.logger.warning(f"Provided session does not exist!")
            return None

        session = self.sessions[session_id]
        now = datetime.now()
        if now > session['expires_at']:
            username = session['username']
            self.logger.warning(f"The session for user '{username}' has expired. Please log back in!")
            success, username = self.invalidate_session(session_id)
            return None

        # Update last accessed time
        session['last_accessed'] = now
        return session

    def invalidate_session(self, session_id: str) -> Tuple[bool, str | None]:
        """
        Remove the given session identifier from the active sessions.

        Parameters
        ----------
        session_id : str
            The unique identifier of the session to be removed.

        Returns
        -------
        bool
            `True` if the specified session was found and removed, `False` otherwise.
        str
            The username corresponding to the invalidated session

        Notes
        -----
        The session ID is removed from the internal session dictionary.
        If the session does not exist, this method has no effect.
        """
        if session_id in self.sessions:
            username = self.sessions[session_id]['username']
            del self.sessions[session_id]
            return True, username

        return False, None

    def cleanup_expired_sessions(self) -> None:
        """
        Remove expired sessions from the session dictionary.

        This method iterates through all stored sessions and deletes any that have
        an expiration time earlier than the current time.

        Notes
        -----
        This function modifies the `self.sessions` dictionary in-place.
        """
        current_time = datetime.now()
        expired_sessions = [
            sid for sid, session in self.sessions.items()
            if current_time > session['expires_at']
        ]
        for sid in expired_sessions:
            del self.sessions[sid]
        return

    def session_username(self, session_id: str) -> Optional[str]:
        """Return the username associated with a session."""
        session_data = self.validate_session(session_id)
        return session_data['username'] if session_data else None

    def refresh_session(self, session_id: str) -> Optional[str]:
        """
        Refresh a session if it's still valid.

        Parameters
        ----------
        session_id : str
            Current session token

        Returns
        -------
        str or None
            New session token if refresh successful
        """
        session_data = self.validate_session(session_id)
        if not session_data:
            return None

        # Invalidate old session
        self.invalidate_session(session_id)

        # Create new session
        username = session_data['username']
        return self.create_session(username)


class SingleSessionManager(SessionManager):
    """
    Generate a single-session manager for handling database connections.

    The `SingleSessionManager` class is designed to manage a single active
    database session at any given time. It inherits from the base `SessionManager`
    class and overrides its initialization to ensure only one concurrent session
    is allowed, even if the base class allows more.

    Parameters
    ----------
    session_timeout : int, optional
        The timeout duration for each database session in seconds.
        If not specified, defaults to 3600 (1 hour).

    Attributes
    ----------
    sessions : Dict[str, dict]
        A dictionary storing active sessions.
        Each key is a session ID, and each value is a dictionary containing:
        - 'username': str - The user associated with this session.
        - 'created_at': datetime - When the session was created.
        - 'last_accessed': datetime - Last time the session was accessed.
        - 'expires_at': datetime - Expiry time of the session.
    session_timeout : int
        The configured timeout duration for sessions.
    max_concurrent_sessions : int
        Always set to 1 to ensure only one concurrent session is allowed.
    logger : logging.Logger
        The logger to use for this session manager.

    Examples
    --------
    >>> from plantdb.commons.fsdb.auth import SingleSessionManager
    >>> # Initialize the session manager
    >>> manager = SingleSessionManager()
    >>> # Create a new session with the username 'test'
    >>> session_token = manager.create_session('test')
    >>> # Attempt to create another session with the username 'test2'
    >>> _ = manager.create_session('test2')
    WARNING  [SessionManager] Reached max concurrent sessions limit (1)
    >>> # Validate the session and get its info
    >>> session = manager.validate_session(session_token)
    >>> print(session['expires_at'])  # Print the expiration date
    >>> # Refresh the session using the existing session token
    >>> new_session_token = manager.refresh_session(session_token)
    >>> # Validate the session and get its info
    >>> session = manager.validate_session(new_session_token)
    >>> print(session['expires_at'])  # Print the expiration date of the refreshed session

    Notes
    -----
    The `SingleSessionManager` enforces a single-session policy, which means any
    attempt to create more than one active session will result in an error or be
    handled according to the logic defined within this class.

    See Also
    --------
    session_manager.SessionManager : Base class for managing database sessions.
    """
    def __init__(self, session_timeout: int = 3600, **kwargs) -> None:
        super().__init__(session_timeout=session_timeout, max_concurrent_sessions=1)


class JWTSessionManager(SessionManager):
    """
    Manages user sessions with expiration and validation.

    This class provides methods to create, validate, invalidate,
    and cleanup expired sessions. Each session is associated with a
    unique identifier (session_id) and has an expiry time based on the
    session timeout duration specified during initialization.

    Attributes
    ----------
    sessions : Dict[str, dict]
        A dictionary storing active sessions.
        Each key is a session ID, and each value is a dictionary containing:
        - 'username': str - The user associated with this session.
        - 'created_at': datetime - When the session was created.
        - 'last_accessed': datetime - Last time the session was accessed.
        - 'expires_at': datetime - Expiry time of the session.
    session_timeout : int
        Duration in seconds after which a session expires.
    max_concurrent_sessions : int
        The maximum number of concurrent sessions to allow.
    secret_key : str
        The session manager secret key to use for authentication.
    logger : logging.Logger
        The logger to use for this session manager.
    """

    def __init__(self, session_timeout: int = 3600, max_concurrent_sessions: int = 10, secret_key: str = None):
        """
        Manage user sessions with timeout.

        Parameters
        ----------
        session_timeout : int, optional
            The duration for which the session should be valid in seconds.
            A session that exceeds this duration will be considered expired and removed.
            Defaults to ``3600`` seconds.
        max_concurrent_sessions : int, optional
            The maximum number of concurrent sessions to allow.
            Defaults to ``10``.
        secret_key : str, optional
            Secret key for JWT signing. If None, generates a random key.
        """
        super().__init__(session_timeout, max_concurrent_sessions)
        self.secret_key = secret_key or secrets.token_urlsafe(32)

    def _create_token(self, username, jti, exp_time, now):
        """
        Create a JSON Web Token (JWT) with registered claims.

        Generates and encodes a JWT using the provided username, unique identifier
        (jti), expiration time, and current time. The token includes standard
        registered claims as defined in RFC 7519.

        Parameters
        ----------
        username : str
            The subject of the JWT (user identifier).
        jti : str
            Unique identifier for the JWT.
        exp_time : datetime.datetime
            Expiration time of the JWT.
        now : datetime.datetime
            Current time when the JWT is issued.

        Returns
        -------
        str
            A string representation of the encoded JWT.

        See Also
        --------
        jwt.encode : Encode a payload into a JWT.
        """
        # Create a JWT payload with registered claims
        payload = {
            # Registered claims (RFC 7519)
            'iss': 'plantdb-api',  # issuer
            'sub': username,  # subject (user identifier)
            'aud': 'plantdb-client',  # audience
            'exp': int(exp_time.timestamp()),  # expiration time (Unix timestamp)
            'iat': int(now.timestamp()),  # issued at (Unix timestamp)
            'jti': jti  # JWT ID (unique identifier)
        }
        return jwt.encode(
            payload,
            self.secret_key,
            algorithm='HS512',
            headers={'typ': 'JWT', 'alg': 'HS512'}
        )

    def create_session(self, username: str) -> Union[str, None]:
        """
        Create a new session for a user.

        If the user already has an active session, it returns the existing session ID.
        Otherwise, it creates a new session and returns its ID.

        Parameters
        ----------
        username : str
            The unique identifier of the user for whom to create a session.

        Returns
        -------
        session_id : Union[str, None]
            The ID of the created or existing session.

        Notes
        -----
        Creates a JWT token following RFC 7519 standards with registered claims:
        - iss (issuer): Identifies the token issuer
        - sub (subject): The username of the authenticated user
        - aud (audience): Intended audience for the token
        - exp (expiration time): Token expiration timestamp
        - iat (issued at): Token creation timestamp
        - jti (JWT ID): Unique identifier for the token generated using `secrets.token_urlsafe`.
        """
        if self._user_has_session(username):
            self.logger.warning(f"User '{username}' already has an active session!")
            return None

        if self.n_active_sessions() >= self.max_concurrent_sessions:
            self.logger.warning(
                f"Too any users currently active, reached max concurrent sessions limit ({self.max_concurrent_sessions})")
            return None

        # Create a JWT payload with registered claims
        now = datetime.now()
        exp_time = now + timedelta(seconds=self.session_timeout)
        jti = secrets.token_urlsafe(16)  # unique token ID for tracking

        try:
            # Generate JWT token
            jwt_token = self._create_token(username, jti, exp_time, now)
        except Exception as e:
            self.logger.error(f"Failed to create JWT token for {username}: {e}")
            return None

        # Track session for concurrent limit enforcement
        self.sessions[jti] = {
            'username': username,
            'created_at': now,
            'last_accessed': now,
            'expires_at': exp_time
        }
        self.logger.debug(f"Created JWT token for '{username}'")
        return jwt_token

    def _payload_from_token(self, jwt_token: str) -> dict:
        """
        Decode the payload from a JWT token.

        This function decodes the JSON Web Token (JWT) using the specified secret key and
        verifies the token's audience and issuer. It returns the decoded payload as a dictionary.

        Parameters
        ----------
        jwt_token : str
            The JWT token to decode.

        Returns
        -------
        dict
            The decoded payload from the JWT token.

        Notes
        -----
        The JWT token must be correctly formatted and signed using the specified secret key.
        If the token is invalid or the signature does not match, a `jwt.ExpiredSignatureError`,
        `jwt.InvalidTokenError`, or `jwt.DecodeError` may be raised.

        See Also
        --------
        jwt.decode : Decodes the JSON Web Token.
        """
        return jwt.decode(
            jwt_token,
            self.secret_key,
            algorithms=['HS512'],
            audience='plantdb-client',  # Verify audience
            issuer='plantdb-api'  # Verify issuer
        )

    def validate_session(self, jwt_token: str) -> Optional[Dict[str, Any]]:
        """
        Validate a JWT token and return user information.

        Parameters
        ----------
        jwt_token : str
            The JWT token to validate.

        Returns
        -------
        dict or None
            User information if valid, ``None`` if invalid/expired.
            Returns dictionary with:
            - username: The authenticated user
            - issued_at: When the token was issued
            - expires_at: When the token expires
            - jti: Unique token identifier
            - issuer: Token issuer
            - audience: Token audience
        """
        try:
            # Decode and verify JWT token with proper validation
            payload = self._payload_from_token(jwt_token)

        except jwt.ExpiredSignatureError:
            self.logger.error("JWT token expired")
            return None
        except jwt.InvalidAudienceError:
            self.logger.error("JWT token has invalid audience")
            return None
        except jwt.InvalidIssuerError:
            self.logger.error("JWT token has invalid issuer")
            return None
        except jwt.InvalidTokenError as e:
            self.logger.error(f"Invalid JWT token: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error validating JWT token: {e}")
            return None

        # Update last accessed time in session tracking
        jti = payload.get('jti')
        if jti and jti in self.sessions:
            self.sessions[jti]['last_accessed'] = datetime.now()

        return {
            'username': payload['sub'],  # subject is the username
            'issued_at': payload['iat'],  # issued at timestamp
            'expires_at': payload['exp'],  # expiration timestamp
            'jti': jti,  # JWT ID
            'issuer': payload['iss'],  # issuer
            'audience': payload['aud']  # audience
        }

    def invalidate_session(self, jwt_token: str = None, jti: str = None) -> Tuple[bool, str | None]:
        """
        Invalidate a session by removing it from tracking.

        Parameters
        ----------
        jwt_token : str, optional
            JWT token to invalidate
        jti : str, optional
            Token ID to invalidate directly

        Returns
        -------
        bool
            `True` if the specified session was found and removed, `False` otherwise.
        str
            The username corresponding to the invalidated JWT token
        """
        if jwt_token:
            try:
                payload = self._payload_from_token(jwt_token)
                jti = payload.get('jti')
            except:
                return False, None

        if jti and jti in self.sessions:
            username = self.sessions[jti]['username']
            del self.sessions[jti]
            return True, username

        return False, None

    def cleanup_expired_sessions(self) -> None:
        """Remove expired sessions from tracking."""
        current_time = datetime.now()
        expired_sessions = [
            jti for jti, session in self.sessions.items()
            if current_time > session['expires_at']
        ]
        for jti in expired_sessions:
            del self.sessions[jti]
        return

    def session_username(self, jwt_token: str) -> Optional[str]:
        """
        Extract username from JWT token.

        Parameters
        ----------
        jwt_token : str
            JWT token

        Returns
        -------
        str or None
            Username if token is valid
        """
        session_data = self.validate_session(jwt_token)
        return session_data['username'] if session_data else None

    def refresh_session(self, jwt_token: str) -> Optional[str]:
        """
        Refresh a JWT token if it's still valid.

        Parameters
        ----------
        jwt_token : str
            Current JWT token

        Returns
        -------
        str or None
            New JWT token if refresh successful
        """
        session_data = self.validate_session(jwt_token)
        if not session_data:
            return None

        # Invalidate old session
        old_jti = session_data['jti']
        self.invalidate_session(jti=old_jti)

        # Create new session
        username = session_data['username']
        return self.create_session(username)
