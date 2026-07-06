#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
# FSDB Images Import CLI

A command‑line utility that imports the content of a folder as an 'images' ``Fileset`` to a new ``Scan`` dataset
 optionally attaching metadata and providing configurable logging.
 It streamlines adding new image data to a PlantDB database directly from the terminal.

## Key Features

- **Simple CLI interface** using `click` with positional arguments for the target database and source folder.
- **Automatic scan creation**: Creates a new scan dataset with the folder name or a custom name.
- **Image filtering**: Automatically filters for common image file extensions (.png, .jpg, .jpeg).
- **Optional metadata**: load a JSON file and associate its contents with the imported fileset.
- **Configurable logging**: choose from predefined log levels to control output verbosity.
- **Automatic database handling**: connects to the FSDB, creates the necessary scan/fileset hierarchy, imports the files, applies metadata, and cleanly disconnects.
- **Login support**: authenticate with username/password or use no-auth for testing.

## Usage Examples

### Basic import of images from a folder to a new scan

```shell
fsdb_import_images /romi_db /path/to/folder_with_images
```

### Import with custom scan name and verbose logging

```shell
fsdb_import_images /romi_db /path/to/folder_with_images \
    --name my_custom_scan \
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

IMG_EXT = [".png", ".jpg", ".jpeg"]


def list_image_files(images_path):
    """List image files in the path.

    Parameters
    ----------
    images_path : pathlib.Path
        The path to the folder containing the dataset.

    Returns
    -------
    list
        The list of image files, selected by their extensions.
    """
    return [f for f in images_path.iterdir() if f.suffix in IMG_EXT]


def load_metadata(md_path) -> dict:
    with open(md_path) as json_file:
        metadata = json.load(json_file)
    return metadata


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.argument('db_path', type=click.Path(exists=True))
@click.argument('folder', type=click.Path(exists=True))
@click.option('--name', type=str, default="",
              help="Name of the scan dataset where to import the fileset to. "
                   "Defaults to the folder name")
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
def main(db_path, folder, name, metadata, user, password, no_auth, log_level):
    """FSDB Images Import CLI

    A command‑line utility that imports the content of a folder as an 'images' ``Fileset`` to a new ``Scan`` dataset,
    optionally attaching metadata and providing configurable logging.

    DATABASE is the path to the local database where to create the scan dataset.
    FOLDER is the path to the folder containing the images to import.
    """
    # Get the logger and change the level if needed:
    logger = get_logger(os.environ.get('ROMI_APP_LOGGER', __name__))
    logger.setLevel(log_level)

    if not (no_auth or (user and password)):
        raise click.UsageError("Requires using either the --no-auth flag or using both --user and --password")

    # - Configure a logger from this application:
    folder_path = Path(folder).resolve()
    if not folder_path.exists():
        raise FileNotFoundError(f"{folder_path} do not exist!")
    if not folder_path.is_dir():
        raise NotADirectoryError(f"{folder_path} is not a directory!")

    # - Check there are some image files in the provided folder:
    img_files = list_image_files(folder_path)
    try:
        assert len(img_files) != 0
    except AssertionError:
        logger.error(f"No image found in folder '{folder}'!")
    else:
        logger.info(f"Found {len(img_files)} image files in folder '{folder}'.")

    # - Connect to the database:
    db = FSDB(db_path, no_auth=no_auth)
    db.connect()

    # - Authenticate unless explicitly disabled
    if not no_auth and (user and password):
        db.login(user, password)

    # - Defines the scan dataset name to create
    default_scan_name = folder_path.name
    if name != "":
        ds_name = Path(name).stem
    else:
        ds_name = default_scan_name

    # - Create the scan dataset:
    scan = db.create_scan(ds_name)

    # - Try to load metadata:
    if metadata is None:
        md_path = folder_path / "metadata.json"
        if md_path.exists():
            metadata = load_metadata(md_path)
    else:
        metadata = load_metadata(Path(metadata))

    # - Create & populate the 'images' fileset:
    fileset = scan.create_fileset('images')
    try:
        for f in img_files:
            fi = fileset.create_file(f.stem)
            fi.import_file(f.absolute())
    except Exception as e:
        scan.delete_fileset(fileset.id)
        raise e

    # - Add the metadata to the 'images' fileset:
    if metadata is not None:
        fileset.set_metadata(metadata)

    db.disconnect()


if __name__ == '__main__':
    main()
