# PlantDB Authentication & Authorization Overview

> This guide gives a developer‑oriented walkthrough of the authentication subsystem that lives under `plantdb.commons.auth`.  
> It covers the key modules and shows how they inter‑operate in a typical request cycle.

---

## 1. Core building blocks

| Module       | Responsibility                              | Key classes / functions                                       |
|--------------|---------------------------------------------|---------------------------------------------------------------|
| `models.py`  | Pure data definitions                       | `Permission`, `Role`, `User`, `Group`                         |
| `manager.py` | CRUD on persisted users/groups (JSON files) | `UserManager`, `GroupManager`                                 |
| `session.py` | Token / session lifecycle                   | `SessionManager`, `JWTSessionManager`, `SingleSessionManager` |
| `rbac.py`    | Permission resolution / access checks       | `RBACManager`                                                 |

> All modules share the same `plantdb.commons.log.get_logger` logger for
> consistent debugging output.

---

## 2. User & Group Persistence (`manager.py`)

### 2.1 `UserManager`

* **Storage** - `users.json` (JSON array). File is atomically updated via a temporary file + `os.replace`.
* **Password handling** - Argon2 (`argon2.PasswordHasher`) - never store plain text.
* **Lock‑out** - `max_login_attempts` and `lockout_duration` configure how many failed attempts trigger a temporary lock.
* **Guest & Admin** - On first run a `guest` user is created automatically;
  an `admin` account is also provisioned (debug‑mode password `"admin"` - replace in production).
* **Public API**

  ```python
  from plantdb.commons.auth.manager import UserManager
  from plantdb.commons.auth.models import Role
  um = UserManager(users_file="users.json")
  um.create("alice", "Alice Smith", "secret", roles={Role.ADMIN})
  assert um.validate("alice", "secret")          # login
  um.update_password("alice", "secret", "newpass")
  ```

### 2.2 `GroupManager`

* **Storage** - `groups.json` (JSON object keyed by group name).
* **Group model** - `Group(name, users, description, created_at, created_by)`.
* **Operations**

  ```python
  from plantdb.commons.auth.manager import GroupManager
  gm = GroupManager(groups_file="groups.json")
  gm.create_group("admins", creator="alice", users={"alice"})
  gm.add_user_to_group("admins", "bob")
  gm.get_user_groups("bob")          # returns list of Group
  ```

---

## 3. Session Management (`session.py`)

### 3.1 `SessionManager`

* Handles **plain token** sessions (random string, stored in memory).
* Enforces `max_concurrent_sessions`.
* Keeps a `sessions` dict mapping token → metadata (`username`, timestamps, `expires_at`).

### 3.2 `JWTSessionManager`

* Issues **JWTs** signed with HS512 (`secret_key`).
* Payload contains:
    * `iss`, `sub` (username), `aud`, `exp`, `iat`, `jti`.
* On `create_session` the JWT is issued **and** a session record is stored under `jti` for concurrent‑limit checks.
* `validate_session` decodes & verifies JWT, updates `last_accessed`.
* `refresh_session` invalidates old token and creates a new one.

> **Tip:** When integrating with a REST API, set the `Authorization: Bearer <jwt>` header and call
> `jwt_mgr.validate_session(token)` to obtain the user’s identity.

---

## 4. Role‑Based Access Control (`rbac.py`)

* `RBACManager` aggregates `UserManager` and `GroupManager` to resolve permissions.
* **Permission lookup** - combines user roles, group memberships, and per‑scan metadata.
* **Scan access** - methods like `can_access_scan`, `can_modify_scan_owner`
  consider owner, sharing groups, and the caller’s role.
* **Guest user** - automatically granted `Role.READER` and read‑only permissions.

```python
from plantdb.commons.auth.rbac import RBACManager
from plantdb.commons.auth.models import Permission

rbac_mgr = RBACManager()
user = rbac_mgr.users.get_user("alice")
can_read = rbac_mgr.can_access_scan(user, {}, Permission.READ)
```

---

## 5. Typical authentication flow

```python
from requests.exceptions import HTTPError
from plantdb.commons.auth.models import Permission
from plantdb.commons.auth.manager import UserManager
from plantdb.commons.auth.rbac import RBACManager
from plantdb.commons.auth.session import JWTSessionManager

# 1  User logs in
um = UserManager()
if um.validate("username", "password"):
  # 2  Issue a JWT
  jwt_mgr = JWTSessionManager(secret_key="super_secret")
  token = jwt_mgr.create_session("username")
  
  # 3  Protect an endpoint
  user_info = jwt_mgr.validate_session(token)   # returns dict with 'username', 'jti', etc.
  if not user_info:
      raise HTTPError(401)
  
  # 4  Authorization check
  rbac_mgr = RBACManager()
  if not rbac_mgr.can_access_scan(user_info['username'], {}, Permission.READ):
      raise HTTPError(403)

# 5  Proceed with the request
```

---

## 6. Key considerations for production

| Topic                 | Recommendation                                                                                                           |
|-----------------------|--------------------------------------------------------------------------------------------------------------------------|
| **Secret management** | Store `JWTSessionManager.secret_key` in an environment variable or a secret vault.                                       |
| **HTTPS**             | JWTs should be transmitted over TLS to prevent eavesdropping.                                                            |
| **Refresh strategy**  | Use short JWT lifetimes (e.g., 15 min) and `refresh_session` for seamless user experience.                               |
| **Audit log**         | Capture login attempts, lock‑out events, and failed permissions for compliance.                                          |
| **Group sync**        | If groups are dynamic (e.g., from LDAP), implement a sync job that updates `groups.json`.                                |
| **Password policy**   | Enforce complexity and rotation; Argon2 already provides strong hashing.                                                 |
| **Scalability**       | The current file‑based persistence is suitable for prototyping; switch to a database (PostgreSQL, MongoDB) when scaling. |

---

## 7. Extending the system

* **New roles** - Add a value to the `Role` enum and extend `Role.permissions` accordingly.
* **Custom permissions** - Create a new enum entry in `Permission` and grant it via a role or directly to a user.
* **External auth provider** - Replace
  `UserManager.validate` with a call to an external SSO service; keep the JWT logic unchanged.

---

## 8. Quick sanity check

```python
from plantdb.commons.auth.models import Permission
from plantdb.commons.auth.manager import UserManager
from plantdb.commons.auth.rbac import RBACManager
from plantdb.commons.auth.session import JWTSessionManager

# Verify that a fresh JWT is accepted
um = UserManager(users_file="users.json")
if not um.exists("alice"):
    um.create("alice", "Alice", "mypwd")

jwt_mgr = JWTSessionManager(secret_key="demo")
token = jwt_mgr.create_session("alice")
assert jwt_mgr.validate_session(token) is not None

# Verify RBAC
rbac = RBACManager()
alice = rbac.users.get_user("alice")
scan = {"owner": "bob", "sharing": ["admins"]}  # Example metadata
assert rbac.can_access_scan(alice, scan, Permission.READ)  # Depends on role/group
```

> Running this snippet should never raise an exception - it demonstrates that the
> authentication, session, and RBAC layers are wired together correctly.

---

## 9. Final words

The PlantDB `auth` stack is intentionally lightweight:

* file‑based persistence → no heavy DB dependencies,
* Argon2 hashing → strong security,
* JWT → stateless, scalable tokens.

Feel free to tweak any of the defaults (session timeout, lock‑out policy, password policy) to fit your deployment.
