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

import json
import shutil
from pathlib import Path
from typing import Any

from plantdb.commons.fsdb.core import FSDB
from plantdb.commons.fsdb.metadata_schema import validate_biological_metadata
from plantdb.commons.cli.fsdb_migrate_metadata import migrate_metadata
from plantdb.commons.cli.fsdb_migrate_metadata import migrate_scan_metadata

from plantdb.client.metadata_app.field_spec import flatten

#: Top-level MIAPPE sections managed by the editor.
_BIOLOGICAL_SECTIONS = ("investigation", "study", "biologicalMaterial", "observedVariable")

#: Name of a scan's metadata file, relative to the scan directory.
_SCAN_METADATA_REL = Path("metadata") / "metadata.json"


def _scan_ids(db_path: Path) -> list[str]:
    """Return the scan ids of the FSDB at ``db_path``.

    Raises ``NotAnFSDBError`` if ``db_path`` is not a proper ROMI DB.
    """
    db = FSDB(db_path, no_auth=True)
    db.connect()
    try:
        return db.list_scans(owner_only=False)
    finally:
        db.disconnect()


def get_scan_dir(db_path: Path, scan_id: str) -> Path:
    """Return the on-disk directory of ``scan_id`` in the FSDB at ``db_path``."""
    db = FSDB(db_path, no_auth=True)
    db.connect()
    try:
        return db.get_scan(scan_id, owner_only=False).path()
    finally:
        db.disconnect()


def load_db(db_path: Path) -> tuple[list[str], dict[str, dict[str, Any]]]:
    """Return ``(scan_ids, flattened)`` for every scan in the database.

    ``flattened`` maps each scan id to its flattened MIAPPE metadata (see
    :func:`plantdb.client.metadata_app.field_spec.flatten`).
    """
    scan_ids = _scan_ids(db_path)
    flattened: dict[str, dict[str, Any]] = {}
    for scan_id in scan_ids:
        scan_dir = get_scan_dir(db_path, scan_id)
        flattened[scan_id] = flatten(read_scan_metadata(scan_dir))
    return scan_ids, flattened


def read_scan_metadata(scan_dir: Path) -> dict[str, Any]:
    """Read the full ``metadata.json`` of a scan directory."""
    md_path = scan_dir / _SCAN_METADATA_REL
    if not md_path.is_file():
        return {}
    with md_path.open() as f:
        return json.load(f)


def write_scan_metadata(scan_dir: Path, metadata: dict[str, Any],
                        backup: bool = True) -> None:
    """Validate and write ``metadata`` to ``scan_dir/metadata/metadata.json``.

    A ``.bak`` copy of the previous file is written first when ``backup`` is
    ``True``. Raises ``ValueError`` if the MIAPPE biological block is invalid.
    """
    validate_biological_metadata(metadata)
    md_path = scan_dir / _SCAN_METADATA_REL
    if backup and md_path.is_file():
        shutil.copy2(md_path, md_path.with_suffix(md_path.suffix + ".bak"))
    md_path.parent.mkdir(parents=True, exist_ok=True)
    with md_path.open("w") as f:
        json.dump(metadata, f, sort_keys=True, indent=4, separators=(',', ': '))


def update_biological(metadata: dict[str, Any], tree: dict[str, Any]) -> dict[str, Any]:
    """Return ``metadata`` with the biological sections replaced by ``tree``.

    Non-biological keys (owner, created, ...) are preserved.
    """
    updated = dict(metadata)
    for section in _BIOLOGICAL_SECTIONS:
        updated.pop(section, None)
    updated.update(tree)
    return updated


def get_field(metadata: dict[str, Any], path: str) -> Any:
    """Return the value of the field at dot-``path`` in a nested dict."""
    node = metadata
    for part in path.split("."):
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    return node


def set_field(metadata: dict[str, Any], path: str, value: Any) -> dict[str, Any]:
    """Return ``metadata`` with the field at dot-``path`` set to ``value``.

    Intermediate sections are created as needed.
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

    Returns the list of scan ids that were actually modified (i.e. whose value
    changed). Writes are validated and backed up.
    """
    modified: list[str] = []
    for scan_id in scan_ids:
        scan_dir = get_scan_dir(db_path, scan_id)
        metadata = read_scan_metadata(scan_dir)
        if get_field(metadata, path) == value:
            continue
        set_field(metadata, path, value)
        write_scan_metadata(scan_dir, metadata, backup=backup)
        modified.append(scan_id)
    return modified


def scan_needs_migration(scan_dir: Path) -> bool:
    """Return True if a scan's metadata still holds a legacy ``object`` block."""
    return migrate_metadata(read_scan_metadata(scan_dir))[1]


def migratable_scans(db_path: Path) -> list[str]:
    """Return the ids of scans that still use the legacy (pre-MIAPPE) schema."""
    return [sid for sid in _scan_ids(db_path)
            if scan_needs_migration(get_scan_dir(db_path, sid))]


def migrate_scans(db_path: Path, scan_ids: list[str]) -> int:
    """Migrate the given scans to the MIAPPE-aligned schema.

    Returns the number of scans that were actually migrated.
    """
    return sum(migrate_scan_metadata(get_scan_dir(db_path, sid)) for sid in scan_ids)


__all__ = [
    "get_scan_dir", "load_db", "read_scan_metadata", "write_scan_metadata",
    "update_biological", "get_field", "set_field", "apply_bulk",
    "scan_needs_migration", "migratable_scans", "migrate_scans",
]
