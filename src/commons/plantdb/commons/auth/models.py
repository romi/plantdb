#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""PlantDB Commons Authentication Models
This module defines core authentication primitives: Permission, Role, User, and Group classes for a plant database system.
It provides fine‑grained permission management, role‑based access control, user serialization, and group membership utilities.

Key Features
------------
- Enumerations for granular permissions and role‑based access control.
- `User` dataclass with serialization (`to_dict`, `to_json`, etc.), authentication helpers and lockout logic.
- `Group` dataclass for collaborative access to scan datasets with user membership management.

Usage Examples
--------------
>>> from plantdb.commons.auth.models import Permission, Role, User, Group
>>> from datetime import datetime, timezone
>>> user = User(username="alice", fullname="Alice Smith", password_hash="hashed_pw", roles={Role.CONTRIBUTOR}, created_at=datetime.now(timezone.utc))
>>> print(user.roles)
{<Role.CONTRIBUTOR: 'contributor'>}
>>> group = Group(name="researchers", users={"alice"}, created_at=datetime.now(timezone.utc), created_by="alice")
>>> group.add_user("bob")
True
"""

import datetime
import json
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from enum import Enum
from fnmatch import fnmatchcase
from typing import Any
from typing import Dict
from typing import Optional
from typing import Set


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
    >>> from plantdb.commons.auth.models import Permission
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

    def __eq__(self, other: Any) -> bool:
        """Equality operator.
        
        Parameters
        ----------
        other : Any
            Object to compare with the permission instance.
            If a string is provided, it is interpreted as a permission name and converted to a
            ``Permission`` instance for comparison.

        Returns
        -------
        bool
            ``True`` if ``other`` represents the same permission as ``self``, otherwise ``False``.

        Notes
        -----
        The method safely handles objects that do not expose a ``value`` attribute
        by catching ``AttributeError`` and returning ``False``.  
        This ensures that comparisons with unrelated types do not raise unexpected exceptions.
        """
        if isinstance(other, str):
            try:
                return Permission.from_string(other) == self
            except ValueError:
                return False
        else:
            try:
                return other.value == self.value
            except AttributeError:
                return False

    def __hash__(self):
        return hash(self.value)

    @classmethod
    def from_string(cls, s: str) -> "Permission":
        """Convert a string to a `Permission` member.

        The function is tolerant of the following inputs:
        * the plain enum *value* (`"read"`);
        * the plain enum *name* (`"READ"`);
        * a fully‑qualified name (`"Permission.READ"`).

        Parameters
        ----------
        s: str
            The string representation supplied by the caller.

        Returns
        -------
        plantdb.commons.auth.models.Permission
            The matching enum member.

        Raises
        ------
        ValueError
            If ``s`` does not correspond to any defined permission.
        
        Examples
        --------
        >>> from plantdb.commons.auth.models import Permission
        >>> perm = Permission.from_string('READ')
        >>> print(perm)
        Permission.READ
        """
        # strip any surrounding whitespace.
        s = s.strip()

        # If the string contains a dot (e.g. "Permission.READ"), keep only the part after the last dot.
        if "." in s:
            s = s.split(".")[-1]

        # Try to resolve by *name* first (case‑insensitive)
        name_key = s.upper()
        if name_key in cls.__members__:  # ``cls.__members__`` holds a mapping of names → members
            return cls[name_key]

        # If that fails, try to resolve by *value*.
        try:
            return cls(s)
        except ValueError as exc:
            # ``cls(s)`` raises ValueError automatically if not found.
            raise ValueError(f"Unknown permission string: {s!r}") from exc


def dataset_perm_to_str(dataset_perm: Dict[str, list[Permission]]) -> str:
    """Convert a mapping of dataset names to permission lists into a compact string representation.

    Parameters
    ----------
    dataset_perm
        Mapping from dataset identifiers (strings) to a list of
        ``Permission`` objects that define the access rights for each dataset.

    Returns
    -------
    str
        A single string where each dataset entry is formatted as
        ``<dataset>/<perm1>,<perm2>,...`` and entries are separated by
        semicolons.  The string representation of each ``Permission`` object
        is obtained via ``str(permission)``.

    Notes
    -----
    The function does not perform validation of the individual ``Permission``
    objects beyond relying on their ``__str__`` method.  It is intended for
    serialising permission specifications for downstream processing or logging.

    Example
    -------
    >>> from plantdb.commons.auth.session import dataset_perm_to_str
    >>> from plantdb.commons.auth.models import Permission
    >>> ds = {'dataset_A': [Permission.READ], 'dataset_B': [Permission.READ, Permission.CREATE]}
    >>> dataset_perm_to_str(ds)
    'dataset_A/Permission.READ;dataset_B/Permission.READ,Permission.CREATE'
    """
    return ";".join([ds + '/' + ",".join(map(str, perms)) for ds, perms in dataset_perm.items()])


def parse_dataset_perm(datasets_str: str) -> Dict[str, set[Permission]]:
    """Convert a dataset permission string back into the original mapping.

    Parameters
    ----------
    datasets_str:
        The semicolon‑separated string.

    Returns
    -------
    Dict[str, set[Permission]]
        Mapping of dataset names to a set of Permission.
        Empty permission lists are represented as an empty list.

    Example
    -------
    >>> from plantdb.commons.auth.session import parse_dataset_perm
    >>> s = 'dataset_A/Permission.READ;dataset_B/Permission.READ,Permission.CREATE'
    >>> parse_dataset_perm(s)
    {'sales': ['read', 'write'], 'marketing': ['read']}
    """
    # Guard against an empty string.
    if not datasets_str:
        return {}

    result: Dict[str, set[Permission]] = {}

    # Split on the outer “;” separator - each piece corresponds to one dataset.
    for part in datasets_str.split(";"):
        # Each part is ``<dataset>/<perm1>,<perm2>,...``.
        # ``maxsplit=1`` protects us if a dataset name itself contains a '/'.
        if "/" not in part:
            # If there is no '/', treat the whole part as a dataset with no perms.
            ds, perms_section = part, ""
        else:
            ds, perms_section = part.split("/", 1)
        # An empty permissions section means the original list was empty.
        perms = set() if perms_section == "" else set(perms_section.split(","))
        result[ds] = {Permission.from_string(perm) for perm in perms}

    return result


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
    >>> from plantdb.commons.auth.models import Role
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
    def rank(self) -> int:
        """Get the hierarchical rank of the role. Higher is more powerful.

        Returns
        -------
        int
            The rank of the role.

        Examples
        --------
        >>> from plantdb.commons.auth.models import Role
        >>> guest = Role.READER
        >>> guest.rank
        1
        """
        ranks = {
            Role.READER: 1,
            Role.CONTRIBUTOR: 2,
            Role.ADMIN: 3,
        }
        return ranks[self]

    def can_assign(self, target_role: 'Role') -> bool:
        """Check if this role has the authority to assign the target_role.

        A user can only assign roles that are less than or equal to their own.

        Returns
        -------
        bool
            ``True`` if this role has the authority to assign the target_role; ``False`` otherwise.

        Examples
        --------
        >>> from plantdb.commons.auth.models import Role
        >>> guest = Role.READER
        >>> guest.can_assign(Role.CONTRIBUTOR)
        False
        >>> user = Role.CONTRIBUTOR
        >>> user.can_assign(Role.CONTRIBUTOR)
        True
        """
        return self.rank >= target_role.rank

    @property
    def permissions(self) -> Set[Permission]:
        """Get the set of permissions associated with this role.

        Returns
        -------
        Set[Permission]
            A set containing all permissions granted to this role.

        Examples
        --------
        >>> from plantdb.commons.auth.models import Role
        >>> guest = Role.READER
        >>> guest.permissions
        {<Permission.READ: 'read'>}
        >>> user = Role.CONTRIBUTOR
        >>> user.permissions
        {<Permission.CREATE: 'create'>,
         <Permission.READ: 'read'>,
         <Permission.WRITE: 'write'>}
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
    """Represents a user entity in the application.

    It contains attributes related to user authentication, roles, permissions, and activity timestamps.
    The class is designed to encapsulate user data and provide methods for user management.
    Users can have multiple roles and permissions, which are stored as sets.
    The class also tracks the creation time and last login time of a user.

    Attributes
    ----------
    username : str
        The unique username of the user.
    password_hash : str
        The hashed password of the user.
    roles : Set[plantdb.commons.auth.models.Role]
        A set containing roles assigned to the user.
    created_at : datetime
        The timestamp when the user account was created.
    permissions : Optional[Set[Permission]]
        A set containing specific permissions granted to the user.
    last_login : Optional[datetime]
        The timestamp of the last login. If not provided, defaults to ``None``.
    is_active : bool
        Indicates if the user account is active. Defaults to ``True``.
    failed_attempts : int
        Number of failed login attempts for the user. Defaults to ``0``.
    last_failed_attempt: Optional[datetime]
        The timestamp of the last failed attempts. Defaults to ``None``.
    locked_until : Optional[datetime]
        Timestamp until which the user is locked out due to multiple failed attempts. Defaults to ``None``.
    password_last_change : Optional[datetime]
        The timestamp of the last password change. Defaults to ``None``.

    Notes
    -----
    Ensure that sensitive data like ``password_hash`` is handled securely and not exposed in logs or error messages.

    Examples
    --------
    >>> from datetime import datetime, timezone
    >>> from plantdb.commons.auth.models import Permission, Role, User
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
        """Compare two ``User`` objects for equality.

        Two ``User`` objects are considered equal if all their attributes have the same values.

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
        """Convert a ``User`` object to a dictionary for JSON serialization.

        Returns
        -------
        dict
            Dictionary representation of the ``User`` object.
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
        """Create a ``User`` object from a dictionary (JSON deserialization).

        Parameters
        ----------
        data : dict
            Dictionary containing user data.

        Returns
        -------
        User
            The ``User`` object created from the dictionary data.

        Raises
        ------
        ValueError
            If required fields are missing or invalid.
        KeyError
            If required keys are missing from the dictionary.
        """
        args = cls._parse_user_dict(data)
        return cls(**args)

    @staticmethod
    def _parse_user_dict(data: dict) -> dict[str | Any, set[Any] | datetime | datetime | None | bool | int | Any]:
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

            args = {
                "username": data['username'],
                "fullname": data['fullname'],
                "password_hash": data['password_hash'],
                "roles": roles,
                "created_at": _datetime_convert(data['created_at']),
                "permissions": permissions,
                "last_login": _datetime_convert(data.get('last_login')),
                "is_active": data.get('is_active', True),
                "failed_attempts": data.get('failed_attempts', 0),
                "last_failed_attempt": _datetime_convert(data.get('last_failed_attempt')),
                "locked_until": _datetime_convert(data.get('locked_until')),
                "password_last_change": _datetime_convert(data.get('password_last_change')),
            }

        except KeyError as e:
            raise KeyError(f"Missing required field in user data: {e}")
        except ValueError as e:
            raise ValueError(f"Invalid data format in user data: {e}")
        return args

    def to_json(self) -> str:
        """Convert ``User`` object to JSON string.

        Returns
        -------
        str
            JSON string representation of the ``User`` object.
        """
        return json.dumps(self.to_dict(), indent=2)

    @classmethod
    def from_json(cls, json_str: str) -> 'User':
        """Create ``User`` object from JSON string.

        Parameters
        ----------
        json_str : str
            JSON string containing user data.

        Returns
        -------
        User
            The ``User`` object created from the JSON data.

        Raises
        ------
        json.JSONDecodeError
            If the JSON string is invalid.
        ValueError
            If required fields are missing or invalid.

        Examples
        --------
        >>> import json
        >>> from plantdb.commons.auth.models import User
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
class TokenUser(User):
    dataset_permissions: Optional[dict[str, set[Permission]]] = None

    def __post_init__(self):
        if not self.dataset_permissions:
            raise TypeError("`dataset_permissions` argument must be provided.`")

    def to_dict(self) -> dict:
        """Convert a ``TokenUser`` object to a dictionary for JSON serialization.

        Returns
        -------
        dict
            Dictionary representation of the ``TokenUser`` object.
        """
        d = super().to_dict()
        d["dataset_permissions"] = self.dataset_permissions
        return d

    @classmethod
    def from_dict(cls, data: dict) -> 'User':
        """Create a ``User`` object from a dictionary (JSON deserialization).

        Parameters
        ----------
        data : dict
            Dictionary containing user data.

        Returns
        -------
        TokenUser
            The ``TokenUser`` object created from the dictionary data.

        Raises
        ------
        ValueError
            If required fields are missing or invalid.
        KeyError
            If required keys are missing from the dictionary.
        """
        args = cls._parse_user_dict(data)
        args["dataset_permissions"] = {
            dname: {Permission(perm) for perm in permissions}
            for dname, permissions in args["dataset_permissions"].items()
        }
        return cls(**args)

    def get_permissions_for_dataset(self, dataset_name: str) -> set[Permission]:
        """
        Returns the set of permissions available for this TokenUser for the provided dataset name.

        Parameters
        ----------
        dataset_name: str
            Name of the dataset.

        Returns
        -------
        set[Permission]
            Set of available permissions for this TokenUser for the provided dataset name.

        """
        permissions = set()
        for dataset_pattern, _permissions in self.dataset_permissions.items():
            if fnmatchcase(dataset_name, dataset_pattern):
                permissions |= _permissions & self.permissions
        return permissions


@dataclass
class Group:
    """Represents a group of users for sharing scan datasets.

    Groups allow multiple users to collaborate on scan datasets. When a scan is shared
    with a group, all members of that group get the `` CONTRIBUTOR `` role for that specific dataset.

    Attributes
    ----------
    name : str
        The unique name of the group.
    users : Set[str]
        A set of usernames that belong to this group.
    description : Optional[str]
        An optional description of the group's purpose.
    created_at : datetime
        The timestamp when the group was created.
    created_by : str
        The username of the user who created the group.

    Examples
    --------
    >>> from datetime import datetime, timezone
    >>> from plantdb.commons.auth.models import Group
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
        """Add a user to the group.

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
        """Remove a user from the group.

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
        """Check if a user is a member of the group.

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
