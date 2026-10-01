#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
# Metadata database read/write helpers for the editor UI

Small FSDB helpers used by the metadata editor: load the flattened MIAPPE
metadata of every scan, and write updated metadata back to ``metadata.json``
with a ``.bak`` backup and schema validation.

A database root must be a formal PlantDB (FSDB, i.e. a directory containing a
``romidb`` marker file). A loose directory that is not a proper ROMI DB is
rejected.
"""
from __future__ import annotations

import atexit
import logging
import shutil
import threading
from pathlib import Path
from typing import Any
from typing import Callable

from plantdb.commons.fsdb.core import FSDB
from plantdb.commons.fsdb.metadata_schema import validate_biological_metadata
from plantdb.commons.cli.fsdb_migrate_metadata import migrate_metadata
from plantdb.commons.cli.fsdb_migrate_metadata import migrate_scan_metadata
from plantdb.commons.fsdb.path_helpers import _scan_metadata_path
from plantdb.commons.fsdb.metadata import _load_scan_metadata

from plantdb.client.metadata_app.field_spec import flatten

#: Top-level MIAPPE sections managed by the editor.
_BIOLOGICAL_SECTIONS = ("investigation", "study", "biologicalMaterial", "observedVariable")

#: Cache of live connected FSDB instances, keyed by resolved db path.
#: Connect is expensive (browses the DB tree and parses JSON), so each distinct database is connected once and reused until switched.
_DB_CACHE: dict[Path, FSDB] = {}
_DB_LOCK = threading.Lock()


def _connect(db_path: Path) -> FSDB:
    """Return a connected, cached FSDB for ``db_path``.

    The first call connects the database and stores it in ``_DB_CACHE``;
    subsequent calls reuse the cached instance for the same resolved path.

    Parameters
    ----------
    db_path : Path
        Path to the FSDB to connect.

    Returns
    -------
    FSDB
        A connected database instance.
    """
    db_path = Path(db_path).resolve()
    db = _DB_CACHE.get(db_path)
    if db is None:
        with _DB_LOCK:
            db = _DB_CACHE.get(db_path)
            if db is None:
                db = FSDB(db_path, no_auth=True)
                db.connect()
                _DB_CACHE[db_path] = db
    return db


def close_db(db_path: Path) -> None:
    """Disconnect and drop the cached FSDB for ``db_path``, if any.

    Used when the app switches to a different database so only one live
    connection stays in memory.

    Parameters
    ----------
    db_path : Path
        Path of the database whose cached connection to close.
    """
    db_path = Path(db_path).resolve()
    with _DB_LOCK:
        db = _DB_CACHE.pop(db_path, None)
    if db is not None:
        db.disconnect()


def _close_all() -> None:
    """Disconnect every still-cached FSDB at process exit."""
    for db in _DB_CACHE.values():
        db.disconnect()

atexit.register(_close_all)


def _scan_ids(db_path: Path) -> list[str]:
    """Return the scan ids of the FSDB at ``db_path``.

    Parameters
    ----------
    db_path : Path
        Path to the FSDB to inspect.

    Returns
    -------
    list of str
        The scan ids found in the database.

    Raises
    ------
    NotAnFSDBError
        If ``db_path`` is not a proper ROMI DB.
    """
    return _connect(db_path).list_scans(owner_only=False)


def get_scan_dir(db_path: Path, scan_id: str) -> Path:
    """Return the on-disk directory of ``scan_id`` in the FSDB at ``db_path``.

    Parameters
    ----------
    db_path : Path
        Path to the FSDB containing the scan.
    scan_id : str
        Identifier of the scan.

    Returns
    -------
    Path
        Directory of the scan.
    """
    return _connect(db_path).get_scan(scan_id, owner_only=False).path()


def all_scan_metadata(db_path: Path) -> dict[str, dict[str, Any]]:
    """Return flattened scan metadata for every scan in the database.

    Parameters
    ----------
    db_path : Path
        Path to the FSDB to load.

    Returns
    -------
    dict of dict
        A mapping of each scan id to its flattened MIAPPE metadata
        (see :func:`plantdb.client.metadata_app.field_spec.flatten`).
    """
    flattened: dict[str, dict[str, Any]] = {}
    for scan in _connect(db_path).get_scans():
        flattened[scan.id] = flatten(scan.get_metadata())
    return flattened


def write_scan_metadata(scan: "Scan", metadata: dict[str, Any], backup: bool = True) -> None:
    """Validate and write ``metadata`` to ``scan_dir/metadata/metadata.json``.

    A ``.bak`` copy of the previous file is written first when ``backup`` is ``True``.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.core.Scan
        Scan instance to use for metadata modification.
    metadata : dict of str to Any
        The metadata to persist.
    backup : bool, default True
        Write a ``.bak`` copy of the previous file before overwriting.

    Raises
    ------
    ValueError
        If the MIAPPE biological block of ``metadata`` is invalid.
    """
    validate_biological_metadata(metadata)
    md_path = _scan_metadata_path(scan)
    if backup and md_path.is_file():
        shutil.copy2(md_path, md_path.with_suffix(md_path.suffix + ".bak"))
    scan.set_metadata(metadata)


def update_biological(metadata: dict[str, Any], tree: dict[str, Any]) -> dict[str, Any]:
    """Return ``metadata`` with the biological sections replaced by ``tree``.

    Non-biological keys (owner, created, ...) are preserved.

    Parameters
    ----------
    metadata : dict of str to Any
        The full scan metadata to update.
    tree : dict of str to Any
        New MIAPPE biological tree to install.

    Returns
    -------
    dict of str to Any
        A copy of ``metadata`` with the biological sections replaced.
    """
    updated = dict(metadata)
    for section in _BIOLOGICAL_SECTIONS:
        updated.pop(section, None)
    updated.update(tree)
    return updated


def get_field(metadata: dict[str, Any], path: str) -> Any:
    """Return the value of the field at dot-``path`` in a nested dict.

    Parameters
    ----------
    metadata : dict of str to Any
        The nested metadata dict to read.
    path : str
        Dot-separated path to the field, e.g. ``study.title``.

    Returns
    -------
    Any
        The field value, or ``None`` if the path does not exist.
    """
    node = metadata
    for part in path.split("."):
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    return node


def set_field(metadata: dict[str, Any], path: str, value: Any) -> dict[str, Any]:
    """Return ``metadata`` with the field at dot-``path`` set to ``value``.

    Intermediate sections are created as needed.

    Parameters
    ----------
    metadata : dict of str to Any
        The nested metadata dict to modify (in place).
    path : str
        Dot-separated path to the field, e.g. ``study.title``.
    value : Any
        Value to set at the field.

    Returns
    -------
    dict of str to Any
        The same ``metadata`` dict, modified in place.
    """
    parts = path.split(".")
    node = metadata
    for part in parts[:-1]:
        if not isinstance(node.get(part), dict):
            node[part] = {}
        node = node[part]
    node[parts[-1]] = value
    return metadata


def apply_bulk(db_path: Path, scan_ids: list[str], path: str, value: Any,
               backup: bool = True) -> list[str]:
    """Set the field at ``path`` to ``value`` on every scan in ``scan_ids``.

    Parameters
    ----------
    db_path : Path
        Path to the FSDB containing the scans.
    scan_ids : list of str
        Scan ids to update.
    path : str
        Dot-separated field path to set.
    value : Any
        Value to set at the field.
    backup : bool, default True
        Back up each scan's metadata before overwriting.

    Returns
    -------
    list of str
        The scan ids that were actually modified (i.e. whose value changed).
        Writes are validated and backed up.
    """
    modified: list[str] = []
    db = _connect(db_path)
    for scan_id in scan_ids:
        scan = db.get_scan(scan_id)
        metadata = scan.get_metadata()
        if get_field(metadata, path) == value:
            continue
        set_field(metadata, path, value)
        write_scan_metadata(scan, metadata, backup=backup)
        modified.append(scan_id)
    return modified


def scan_needs_migration(scan: "Scan") -> bool:
    """Return ``True`` if a scan's metadata still holds a legacy ``object`` block.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.core.Scan
        Scan to inspect.

    Returns
    -------
    bool
        ``True`` if the scan requires migration to the MIAPPE schema.
    """
    return migrate_metadata(_load_scan_metadata(scan))[1]


def migratable_scans(db_path: Path) -> list[str]:
    """Return the ids of scans that still use the legacy (pre-MIAPPE) schema.

    Parameters
    ----------
    db_path : Path
        Path to the FSDB to inspect.

    Returns
    -------
    list of str
        Scan ids that require migration.
    """
    return [sid for sid in _scan_ids(db_path) if scan_needs_migration(get_scan_dir(db_path, sid))]


def migrate_scans_progress(db_path: Path, scan_ids: list[str], logger: logging.Logger,
                           on_progress: Callable[[int, int], None] | None = None) -> int:
    """Migrate ``scan_ids`` scan by scan, reporting progress.

    ``on_progress(done, total)`` is called after each scan is processed, so a
    caller can surface a live progress bar.

    Parameters
    ----------
    db_path : Path
        Path to the FSDB containing the scans.
    scan_ids : list of str
        Scan ids to migrate.
    logger : logging.Logger
        Logger used to report each migrated scan.
    on_progress : Callable[[int, int], None], optional
        Callback invoked with ``(done, total)`` after each scan.

    Returns
    -------
    int
        The number of scans that were actually migrated.
    """
    total = len(scan_ids)
    done = 0
    for sid in scan_ids:
        logger.info(f"Migrating {sid} ({done+1}/{total}): {get_scan_dir(db_path, sid)}")
        if migrate_scan_metadata(get_scan_dir(db_path, sid)):
            done += 1
        if on_progress is not None:
            on_progress(done, total)
    return done


__all__ = [
    "get_scan_dir", "close_db", "all_scan_metadata", "write_scan_metadata",
    "update_biological", "get_field", "set_field", "apply_bulk",
    "scan_needs_migration", "migratable_scans", "migrate_scans_progress",
]
