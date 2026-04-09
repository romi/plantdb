#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Authentication and Authorization Utilities

Provides a unified set of tools for managing sessions, users, groups, and fine‑grained permissions
within the PlantDB ecosystem. By consolidating session handling, file‑based persistence, RBAC logic,
and core authentication models, it simplifies the integration of authentication flows across services.

Key Features
------------
- Flexible session management with optional JWT support and automatic expiration handling
- File‑based user and group persistence with Argon2 password hashing and atomic JSON updates
- Full role‑based access control (ADMIN, CONTRIBUTOR, READER) for scans and user accounts
- Serializable Permission, Role, User, and Group dataclasses with helper utilities

Usage Examples
--------------
>>> from pathlib import Path
>>> from plantdb.commons.auth import session, manager, rbac, models
>>>
>>> # Create a JWT session manager
>>> jwt_mgr = session.JWTSessionManager(secret_key="my_secret")
>>> token = jwt_mgr.create_session("alice")
>>> user_info = jwt_mgr.validate_session(token)
>>> print(user_info)
{'username': 'alice', 'issued_at': 1769011058, 'expires_at': 1769012858, 'jti': 'HVaAR4XHmIJgCKbZMDqmwg', ...}
>>>
>>> # Manage users and groups
>>> um = manager.UserManager(users_file=Path("users.json"))
>>> gm = manager.GroupManager(groups_file=Path("groups.json"))
>>> um.create("alice", "Alice Smith", "secure123", roles={manager.Role.ADMIN})
>>> gm.create_group("admins", creator="alice", users={"alice"}, description="Admin group")
>>>
>>> # RBAC operations
>>> rbac_mgr = rbac.RBACManager()
>>> user = rbac_mgr.create_user("alice", roles={rbac.Role.CONTRIBUTOR})
>>> rbac_mgr.create_group(user, "researchers")
>>> rbac_mgr.add_user_to_group(user, "researchers", "alice")
>>> scan_meta = {"owner": "bob", "sharing": ["researchers"]}
>>> can_read = rbac_mgr.can_access_scan(user, scan_meta, rbac.Permission.READ)
>>> print(can_read)
True
"""