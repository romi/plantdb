#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
# FSDB Folder Import CLI

A command‑line utility that imports a folder's contents as a ``Fileset`` in a known ``Scan`` dataset, optionally
attaching metadata and providing configurable logging.
It streamlines adding new data to a PlantDB database directly from the terminal.

## Key Features

- **Simple CLI interface** using `click` with positional arguments for the target scan and source folder.
- **Optional metadata**: load a JSON file and associate its contents with the imported fileset.
- **Configurable logging**: choose from predefined log levels to control output verbosity.
- **Automatic database handling**: connects to the FSDB, creates the necessary scan/fileset hierarchy, imports the files, applies metadata, and cleanly disconnects.
- **Login support**: authenticate with username/password or use no-auth for testing.

## Usage Examples

### Basic import of a folder into an existing scan

```shell
fsdb_import_folder /romi_db/scan001 /path/to/folder_with_files
```

### Import with metadata and verbose logging

```shell
fsdb_import_folder /romi_db/scan001 /path/to/folder_with_files \
    --metadata /path/to/folder_meta.json \
    --log-level INFO
```

"""

import json
import os
from pathlib import Path

import click
from click_option_group import OptionGroup
from click_option_group import optgroup

from plantdb.commons.fsdb.core import FSDB
from plantdb.commons.log import DEFAULT_LOG_LEVEL
from plantdb.commons.log import LOG_LEVELS
from plantdb.commons.log import get_logger

# Create a logger and set the environment variable
os.environ.setdefault('ROMI_APP_LOGGER', __file__.split('.')[0])
logger = get_logger(os.getenv('ROMI_APP_LOGGER'), log_level=DEFAULT_LOG_LEVEL)


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.argument('scan', type=click.Path(exists=True))
@click.argument('folder', type=click.Path(exists=True))
@click.option(
    '--metadata',
    type=click.Path(exists=True),
    default=None,
    help='Path to a JSON file with fileset related metadata.'
)
@optgroup.group("Login", cls=OptionGroup)
@optgroup.option('-u', '--user', type=str, default=None,
                 help="Username for FSDB login.")
@optgroup.option('-p', '--password', type=str, default=None,
                 help="Password for FSDB login.")
@optgroup.option('--no-auth', is_flag=True, default=False,
                 help="Use a database with automatic 'admin' user log in, for testing purposes only.")
@optgroup.group("Logging", cls=OptionGroup)
@optgroup.option(
    "--log-level",
    type=click.Choice(LOG_LEVELS, case_sensitive=False),
    default=DEFAULT_LOG_LEVEL,
    show_default=True,
    help="Logging level.",
)
def main(scan, folder, metadata, user, password, no_auth, log_level):
    """FSDB Folder Import CLI

    A command‑line utility that imports a folder's contents as a ``Fileset`` in a known ``Scan`` dataset, optionally
    attaching metadata and providing configurable logging.

    SCAN is the path to the scan that will receive the imported fileset.
    FOLDER is the path to the source folder containing files to import.
    """
    # Get the logger and change the level if needed:
    logger = get_logger(os.environ.get('ROMI_APP_LOGGER', __name__))
    logger.setLevel(log_level)

    if not (no_auth or (user and password)):
        raise click.UsageError("Requires using either the --no-auth flag or using both --user and --password")

    # Load metadata if a path is provided
    if metadata is not None:
        with open(metadata, "r", encoding="utf-8") as f:
            metadata = json.load(f)

    scan_path = Path(scan)
    scan_id = scan_path.name
    db_path = scan_path.parent

    # Initialize the database
    db = FSDB(db_path, no_auth=no_auth)
    db.connect()

    # Authenticate unless explicitly disabled
    if not no_auth and (user and password):
        db.login(user, password)

    folder_path = Path(folder).resolve()
    fileset_id = folder_path.name

    fileset = scan.create_fileset(fileset_id)
    try:
        for f in os.listdir(folder_path):
            if os.path.isfile(os.path.join(folder_path, f)):
                fi = fileset.create_file(os.path.splitext(f)[0])
                fi.import_file(os.path.join(folder_path, f))
    except Exception as e:
        scan.delete_fileset(fileset_id)
        raise e

    if metadata is not None:
        fileset.set_metadata(metadata)

    db.disconnect()


if __name__ == '__main__':
    main()
