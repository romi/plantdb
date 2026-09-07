#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
# FSDB images.json Metadata Cleanup CLI

Removes the redundant biological / hardware metadata entries from every
scan's ``metadata/images.json`` in a database.

These entries are duplicated from ``metadata/metadata.json`` (which now holds
the canonical MIAPPE-aligned tree):

* older implementation (plant-imager): top-level ``object`` and ``hardware``;
* newer implementation (Plant-Imager3): a ``Metadata`` entry holding
  ``hardware`` and ``object``.

Everything else in ``images.json`` (``channels``, ``task_params``,
``workspace``, ``picamera``/``picamera2``, ``ScanPath``, ``created``, ...) is
left untouched.

## Usage Examples

```shell
# Dry-run first (shows what would be cleaned, writes nothing):
fsdb_clean_images_metadata /romi_db --dry-run

# Actually clean the files (a ``.bak`` copy is written for each changed file):
fsdb_clean_images_metadata /romi_db

# Clean without keeping backups:
fsdb_clean_images_metadata /romi_db --no-backup
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

# Keys that hold the redundant biological / hardware metadata in images.json
_LEGACY_TOP_LEVEL = ("object", "hardware")
_NEW_METADATA_ENTRY = "Metadata"


def _clean_images_metadata(images: dict) -> bool:
    """Remove redundant metadata entries from an ``images.json`` dict in place.

    Returns
    -------
    bool
        ``True`` if any entry was removed.
    """
    changed = False
    for key in _LEGACY_TOP_LEVEL:
        if key in images:
            del images[key]
            changed = True
    md = images.get(_NEW_METADATA_ENTRY)
    if isinstance(md, dict) and ("hardware" in md or "object" in md):
        del images[_NEW_METADATA_ENTRY]
        changed = True
    return changed


def clean_images_metadata(scan_path: Path, dry_run: bool = False,
                          backup: bool = True) -> bool:
    """Clean a single scan's ``metadata/images.json`` in place.

    Parameters
    ----------
    scan_path : pathlib.Path
        Path to the scan directory.
    dry_run : bool, optional
        If ``True``, report what would change without writing anything.
    backup : bool, optional
        If ``True`` and ``dry_run`` is ``False``, write a ``.bak`` copy of
        each changed file before overwriting it.

    Returns
    -------
    bool
        ``True`` if the file needed cleaning (and was cleaned unless
        ``dry_run`` was set), ``False`` if there was nothing to do.
    """
    img_path = scan_path / "metadata" / "images.json"
    if not img_path.is_file():
        return False
    with img_path.open() as f:
        images = json.load(f)
    if not isinstance(images, dict):
        logger.warning(f"Skipping non-dict images.json: {img_path}")
        return False
    if not _clean_images_metadata(images):
        return False
    if dry_run:
        logger.info(f"[dry-run] Would clean '{img_path}'.")
        return True
    if backup:
        bak_path = img_path.with_suffix(img_path.suffix + ".bak")
        shutil.copy2(img_path, bak_path)
    with img_path.open("w") as f:
        json.dump(images, f, sort_keys=True, indent=4, separators=(',', ': '))
    logger.info(f"Cleaned '{img_path}'.")
    return True


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.argument('db_path', type=click.Path(exists=True))
@click.option(
    "--dry-run",
    is_flag=True,
    help="Report what would be cleaned without modifying any file.",
)
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
def main(db_path, dry_run, no_backup, log_level):
    """Clean redundant metadata entries from every scan's images.json."""
    logger.setLevel(log_level)
    db_path = Path(db_path).resolve()
    cleaned = unchanged = 0
    for scan_path in db_path.iterdir():
        if not scan_path.is_dir():
            continue
        if clean_images_metadata(scan_path, dry_run=dry_run, backup=not no_backup):
            cleaned += 1
        else:
            unchanged += 1
    verb = "Would clean" if dry_run else "Cleaned"
    logger.info(f"Done: {verb} {cleaned} scan(s), {unchanged} unchanged.")


if __name__ == '__main__':
    main()
