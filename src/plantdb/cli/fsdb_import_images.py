#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Import the content of a folder as an 'images' ``Fileset`` to a new ``Scan`` dataset.
"""
import argparse
import json
from pathlib import Path

from plantdb.fsdb import FSDB
from plantdb.log import LOGLEV
from plantdb.log import configure_logger

DESC = """
Import the content of a folder as an 'images' ``Fileset`` to a new ``Scan`` dataset.
"""
IMG_EXT = [".png", ".jpg", ".jpeg"]


def parsing():
    parser = argparse.ArgumentParser(description=DESC)
    parser.add_argument("database", type=str,
                        help="Local database where to create the scan dataset.")
    parser.add_argument("folder", type=str,
                        help="Folder containing the images to import.")

    db_opt = parser.add_argument_group("Database options")
    db_opt.add_argument("--name", type=str, default="",
                        help="Name of the scan dataset where to import the fileset to. "
                             "Defaults to the folder name")
    db_opt.add_argument("--metadata", type=str, default=None,
                        help="JSON or TOML file with metadata. "
                             "Defaults to `metadata.json`")

    log_opt = parser.add_argument_group("Logging options")
    log_opt.add_argument("--log-level", dest="log_level", type=str, default="INFO", choices=LOGLEV,
                         help="Level of message logging, defaults to 'INFO'.")

    return parser


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


def main():
    # - Parse the input arguments to variables:
    parser = parsing()
    args = parser.parse_args()

    # - Configure a logger from this application:
    global logger
    logger = configure_logger('fsdb_import_folder', log_level=args.log_level)
    folder_path = Path(args.folder).resolve()
    if not folder_path.exists():
        raise FileNotFoundError(f"{folder_path} do not exist!")
    if not folder_path.is_dir():
        raise NotADirectoryError(f"{folder_path} is not a directory!")

    # - Check there are some image files in the provided folder:
    img_files = list_image_files(folder_path)
    try:
        assert len(img_files) != 0
    except AssertionError:
        logger.error(f"No image found in folder '{args.folder}'!")
    else:
        logger.info(f"Found {len(img_files)} image files in folder '{args.folder}'.")

    # - Connect to the database:
    db = FSDB(args.database)
    db.connect()

    # - Defines the scan dataset name to create
    default_scan_name = folder_path.name
    if args.name != "":
        ds_name = Path(args.name)
    else:
        ds_name = default_scan_name

    # - Create the scan dataset:
    scan = db.create_scan(ds_name)

    # - Try to load metadata:
    metadata = None
    if args.metadata is None:
        md_path = folder_path / "metadata.json"
        if md_path.exists():
            metadata = load_metadata(md_path)
    else:
        metadata = load_metadata(Path(args.metadata))

    # - Create & populate the 'images' fileset:
    fileset = scan.create_fileset('images')
    try:
        for f in img_files:
            fi = fileset.create_file(f.stem)
            fi.import_file(f.absolute())
    except:
        scan.delete_fileset(fileset.id)

    # - Add the metadata to the 'images' fileset:
    if metadata is not None:
        fileset.set_metadata(metadata)

    db.disconnect()

if __name__ == '__main__':
    main()
