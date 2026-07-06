#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
# FSDB File Import CLI

A command‑line utility that imports a single file into a specified PlantDB fileset, optionally attaching metadata and providing configurable logging. It streamlines adding new data to a PlantDB database directly from the terminal.

## Key Features

- **Simple CLI interface** using `click` with positional arguments for the target fileset and source file.
- **Optional metadata**: load a JSON file and associate its contents with the imported file.
- **Configurable logging**: choose from predefined log levels to control output verbosity.
- **Automatic database handling**: connects to the FSDB, creates the necessary scan/fileset/file hierarchy, imports the file, applies metadata, and cleanly disconnects.

## Usage Examples

### Basic import of a file into an existing fileset

```shell
fsdb_import_file /romi_db/scan001/filesetA /path/to/image_01.png
```

### Import with metadata and verbose logging

```shell
fsdb_import_file /romi_db/scan001/filesetA /path/to/image_01.png \
    --metadata /path/to/image_01_meta.json \
    --log-level INFO
```

"""

import json
import os
from pathlib import Path

import click
from click_option_group import OptionGroup
from click_option_group import optgroup

from plantdb.commons.log import DEFAULT_LOG_LEVEL
from plantdb.commons.log import LOG_LEVELS
from plantdb.commons.log import get_logger

# Create a logger and set the environment variable
os.environ.setdefault('ROMI_APP_LOGGER', __file__.split('.')[0])
logger = get_logger(os.getenv('ROMI_APP_LOGGER'), log_level=DEFAULT_LOG_LEVEL)

from plantdb.commons.fsdb.core import FSDB


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.argument('fileset_path', type=click.Path(exists=True))
@click.argument('file_path', type=click.Path(exists=True))
@click.option(
    '--metadata',
    type=click.Path(exists=True),
    default=None,
    help='Path to a JSON file with file related metadata.'
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
def main(fileset_path, file_path, metadata, user, password, no_auth, log_level):
    """FSDB File Import CLI

    A command‑line utility that imports a single file into a specified PlantDB fileset, optionally attaching
    metadata and providing configurable logging.

    FILESET is the path to the fileset that will receive the imported file.
    FILE is the path to the source file you want to import.
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

    fileset_path = Path(fileset_path)  # Directory representing the target fileset
    fileset_id = fileset_path.name  # Identifier derived from fileset folder name
    file_id = Path(file_path).stem  # Identifier derived from source filename (without extension)
    scan_id = fileset_path.parent  # Parent directory used as scan identifier
    db_path = fileset_path.parent.parent  # Grandparent directory points to the database location

    # Initialize the database
    db = FSDB(db_path, no_auth=no_auth)
    db.connect()

    # Authenticate unless explicitly disabled
    if not no_auth and (user and password):
        db.login(user, password)

    scan = db.create_scan(scan_id)
    fileset_obj = scan.create_fileset(fileset_id)
    file_obj = fileset_obj.create_file(file_id)
    file_obj.import_file(file_path)

    if metadata is not None:
        file_obj.set_metadata(metadata)

    db.disconnect()


if __name__ == '__main__':
    main()
