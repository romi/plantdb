# ScanLockManager

> File‑Based Locking for Multi‑Process & Multi‑Thread Workloads

---

## 1. Overview

`ScanLockManager` implements a robust, **file‑based lock** system that works across:

| Level       | Scope                      | Example                                                  |
|-------------|----------------------------|----------------------------------------------------------|
| **Thread**  | In‑process synchronization | Multiple threads read the same scan                      |
| **Process** | Cross‑process protection   | A worker daemon and a web UI both write to the same scan |

Features:

- **Shared (read) & exclusive (write) locks** - multiple readers may coexist, but writers have exclusive access.
- **Timeouts** - configurable lock acquisition timeout to avoid deadlocks.
- **Automatic cleanup** - stale lock files (_e.g._ from a crash) are purged on manager start.
- **Metadata files** - optional `.info` files that record the lock holder, timestamp, PID, etc. Useful for debugging.
- **Thread‑safe public API** - the manager uses an internal `threading.RLock` so you can call it from anywhere in the same process.

> **Why file locks?**  
> They survive across process restarts and even across system reboots (as long as the underlying filesystem supports it). Unlike in‑memory mutexes, file locks are visible to any process that shares the same filesystem.

---

## 2. Core Concepts

| Concept         | Description                                                                                            |
|-----------------|--------------------------------------------------------------------------------------------------------|
| **Lock file**   | Each scan (`scan_id`) has a single lock file (`<scan_id>.lock`) in a dedicated `.locks` sub‑directory. |
| **Lock type**   | `LockType.SHARED` or `LockType.EXCLUSIVE`.                                                             |
| **Active lock** | A lock that has been acquired and not yet released.                                                    |
| **Lock info**   | Optional JSON file (`<scan_id>.info`) containing metadata about the current holder.                    |

---

## 3. Usage Example

```python
from plantdb.commons.fsdb.lock import ScanLockManager, LockType

# Initialise manager - the .locks directory will be created automatically
manager = ScanLockManager("/home/alice/scans")

# 1. Read‑only operation (shared lock)
with manager.acquire_lock("scan001", LockType.SHARED, user="reader1"):
    data = read_scan("scan001")  # user‑defined function
    process_data(data)

# 2. Write operation (exclusive lock)
try:
    with manager.acquire_lock("scan001", LockType.EXCLUSIVE, user="writer1", timeout=10):
        data = modify_scan("scan001")
        write_scan("scan001", data)
except LockTimeoutError:
    print("Could not obtain write lock - another process is writing")
```

---

## 4. Common Pitfalls & Debugging

| Symptom                                                     | Likely Cause                                           | Fix                                                                                        |
|-------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------------------------------------------|
| Lock never releases after process crash                     | Stale lock file remains                                | `ScanLockManager` cleans them on startup. For manual clean‑up: `rm /path/to/.locks/*.lock` |
| `LockError` raised unexpectedly                             | Another user holds an exclusive lock                   | Check `get_lock_status()` or inspect `<scan_id>.info`                                      |
| `LockTimeoutError` appears after a short wait               | File system has high latency or contention             | Increase `default_timeout`, or move locks to a faster disk                                 |
| `fcntl.flock` raises `OSError: [Errno 22] Invalid argument` | Trying to lock a non‑regular file (_e.g._ a directory) | Ensure lock files are regular (`.lock` files, not directories)                             |

> **Tip:** Enable debug logging on the `ScanLockManager` (`log_level="DEBUG"`) to see every lock attempt and release.

---

## 5. Extending the System

- **Custom lock file location** - override `_get_lock_file_path()` in a subclass.
- **Different locking backends** - replace `fcntl.flock` with e.g. `portalocker` for cross‑platform support.
- **Metadata hooks** - subclass `_write_lock_info()` to store additional context (e.g., user roles).

---

## 6. Testing Strategy

1. **Unit tests** - mock `fcntl` and file system interactions to test lock acquisition logic.
2. **Integration tests** - spawn multiple processes that compete for the same lock; verify that exclusive writers block readers/writers as expected.
3. **Crash simulation** - kill a process holding a lock and ensure the next startup cleans the stale file.
4. **Timeout checks** - configure a very short timeout and confirm that a `LockTimeoutError` is raised when contention is high.

---

## 7. Contributing

If you’d like to add features (_e.g._ Windows support, metrics collection, or a higher‑level API), feel free to open a PR.

Please include:

- Unit tests for the new code.
- Documentation updates (this file, if relevant).
- A clear description of the change and its motivation.
