#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
# FSDB Metadata Migration CLI

One‑time migration of existing scan databases from the legacy flat
``Metadata.object`` biological metadata to the new MIAPPE-aligned tree
(``investigation`` / ``study`` / ``biologicalMaterial``).

This CLI reads every scan's ``metadata/metadata.json``, converts the old
``object`` block (v2 ``Metadata.object`` or v3 top‑level ``object``) into the
canonical MIAPPE tree and rewrites the file. It is idempotent: scans already
migrated (no legacy ``object`` block) are left untouched.

See ``docs/developers/miappe_metadata.md`` for the mapping and design.

## Usage Examples

```shell
fsdb_migrate_metadata /romi_db

# Migrate without keeping a '.bak' copy of each changed file:
fsdb_migrate_metadata /romi_db --no-backup
```
"""
from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

import click

from plantdb.commons.log import DEFAULT_LOG_LEVEL
from plantdb.commons.log import LOG_LEVELS
from plantdb.commons.log import get_logger

os.environ.setdefault('ROMI_APP_LOGGER', __name__.split('.')[-1])
logger = get_logger(os.getenv('ROMI_APP_LOGGER'), log_level=DEFAULT_LOG_LEVEL)


def build_miappe_tree(obj: dict) -> dict:
    """Convert a legacy ``object`` metadata dict into the MIAPPE-aligned tree."""
    if not isinstance(obj, dict):
        return {}

    def s(*keys, default=None):
        for k in keys:
            v = obj.get(k)
            if v not in (None, ""):
                return v
        return default

    species = s('species', default='') or ''
    parts = species.split(None, 1)
    genus = parts[0] if len(parts) > 1 else ""
    sp_name = species

    dag = s('DAG', default=None)
    try:
        age = int(dag) if dag is not None else None
    except (TypeError, ValueError):
        age = None

    return {
        "investigation": {
            "identifier": s('dataset_id'),
            "title": None,
            "description": None,
        },
        "study": {
            "identifier": s('experiment_id'),
            "title": None,
            "startDate": None,
            "endDate": None,
            "growthFacility": {"name": s('growth_environment', 'environment'), "country": None},
            "environment": {"photoperiod": s('growth_conditions')},
            "experimentalDesign": {"type": None},
            "experimentalFactors": {"treatment": s('treatment')},
        },
        "biologicalMaterial": {
            "biologicalMaterialId": s('plant_id', 'object_id'),
            "organism": {"genus": genus, "species": sp_name},
            "materialSource": {"id": None, "name": s('seed_stock')},
            "ageDays": age,
            "sample": s('sample'),
        },
    }


def _extract_object(metadata: dict) -> tuple[dict | None, bool]:
    """Return the legacy ``object`` block and whether it was nested under 'Metadata'."""
    md = metadata.get('Metadata')
    if isinstance(md, dict) and isinstance(md.get('object'), dict):
        return md['object'], True
    if isinstance(metadata.get('object'), dict):
        return metadata['object'], False
    return None, False


def migrate_metadata(metadata: dict) -> tuple[dict, bool]:
    """Return ``(metadata, migrated)`` with the legacy ``object`` block migrated to the MIAPPE tree.

    ``migrated`` is ``False`` (and ``metadata`` is returned unchanged) if no legacy
    ``object`` block is present, making the migration idempotent.
    """
    obj, nested = _extract_object(metadata)
    if obj is None:
        return metadata, False

    tree = build_miappe_tree(obj)
    # Drop the legacy block (nested under 'Metadata' or top-level)
    if nested:
        del metadata['Metadata']['object']
        if metadata['Metadata'] == {}:
            del metadata['Metadata']
    else:
        del metadata['object']

    # Merge the MIAPPE sections into the top level (without overriding existing)
    for key, value in tree.items():
        if value is not None and value != {}:
            metadata.setdefault(key, value)
    return metadata, True


def migrate_scan_metadata(scan_path: Path, backup: bool = True) -> bool:
    """Migrate a single scan's ``metadata/metadata.json`` in place.

    Parameters
    ----------
    scan_path : pathlib.Path
        Path to the scan directory.
    backup : bool, optional
        If ``True``, write a ``.bak`` copy of the file before overwriting it.

    Returns
    -------
    bool
        ``True`` if the scan was migrated, ``False`` if there was nothing to do.
    """
    md_path = scan_path / "metadata" / "metadata.json"
    if not md_path.is_file():
        return False
    with md_path.open() as f:
        metadata = json.load(f)
    migrated, did_migrate = migrate_metadata(metadata)
    if not did_migrate:
        return False
    if backup:
        shutil.copy2(md_path, md_path.with_suffix(md_path.suffix + ".bak"))
    with md_path.open("w") as f:
        json.dump(migrated, f, sort_keys=True, indent=4, separators=(',', ': '))
    return True


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.argument('db_path', type=click.Path(exists=True))
@click.option(
    "--no-backup",
    is_flag=True,
    help="Do not write a '.bak' copy of changed files.",
)
@click.option(
    "--log-level",
    type=click.Choice(LOG_LEVELS, case_sensitive=False),
    default=DEFAULT_LOG_LEVEL,
    show_default=True,
    help="Logging level.",
)
def main(db_path, no_backup, log_level):
    """Migrate all scans of a database to the MIAPPE-aligned biological metadata."""
    logger.setLevel(log_level)
    db_path = Path(db_path).resolve()
    migrated = unchanged = 0
    for scan_path in db_path.iterdir():
        if not scan_path.is_dir():
            continue
        if migrate_scan_metadata(scan_path, backup=not no_backup):
            migrated += 1
            logger.info(f"Migrated scan '{scan_path.name}'.")
        else:
            unchanged += 1
    logger.info(f"Done: {migrated} migrated, {unchanged} unchanged.")


if __name__ == '__main__':
    main()
