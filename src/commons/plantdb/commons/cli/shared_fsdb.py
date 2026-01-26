#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Create a local plantdb database (FSDB) using shared datasets.

This module provides a command-line interface for setting up a local plantdb database by cloning shared datasets.
It is useful for users who need to work with specific test datasets without downloading them individually.

Key Features
------------

- Supports cloning of multiple datasets, including all available ones or a specified subset.
- Optional flags to clone configuration files and trained CNN model files.
- Force download option to refresh the archive regardless of existing data.
- Configurable logging levels for tracking setup progress and debugging.

Usage
-----

To create a local database with the default 'real_plant' dataset at `/path/to/db`:

.. code-block:: bash
   $ shared_fsdb /path/to/db

To clone all available datasets along with configuration files to `/path/to/db`:

.. code-block:: bash
   $ shared_fsdb --dataset all --config /path/to/db
"""

import argparse

from plantdb.commons.log import DEFAULT_LOG_LEVEL
from plantdb.commons.log import LOG_LEVELS
from plantdb.commons.log import get_logger
from plantdb.commons.test_database import DATASET
from plantdb.commons.test_database import setup_test_database


def parsing():
    # Create argument parser for command-line arguments
    parser = argparse.ArgumentParser(description='Create a local plantdb database (FSDB) using shared datasets.')

    # Define required 'path' argument - path to test database
    parser.add_argument('path', type=str,
                        help='Path to the test database to set up.')

    # Define optional flags for configuration, models, and force download
    parser.add_argument('-d', '--dataset', type=str, nargs="+",
                        default=['real_plant'],
                        help="Test dataset to clone, use 'all' to get all of them. " + \
                             "You can list several dataset names to clone. " + \
                             "Available dataset names are: " + \
                             ", ".join([f"'{ds}'" for ds in DATASET]) + ". " + \
                             "By default we clone the 'real_plant' dataset.")

    # Define optional flags for configuration, models, and force download
    parser.add_argument('--config', action='store_true',
                        help='Use this to also clone the configuration files.')
    parser.add_argument('--models', action='store_true',
                        help='Use this to also clone the trained CNN model files.')
    parser.add_argument('--force', action='store_true',
                        help='Use this to force download of archive.')

    log_opt = parser.add_argument_group("Logging options")
    log_opt.add_argument("--log-level", dest="log_level", type=str, default=DEFAULT_LOG_LEVEL, choices=LOG_LEVELS,
                         help="Level of message logging, defaults to 'INFO'.")
    return parser


def main():
    # Initialize argument parser and parse arguments
    parser = parsing()
    args = parser.parse_args()

    # Set up logger with the specified log level from arguments
    logger = get_logger(__name__, log_level=args.log_level)

    # If "all" is in dataset, replace it with all available datasets
    if args.dataset[0] == "all":
        args.dataset = DATASET

    # Validate and filter requested datasets to ensure they exist
    args.dataset = list(set(args.dataset) & set(DATASET))

    # Log critical error and raise ValueError if no valid dataset names are provided
    if len(args.dataset) == 0:
        logger.critical(f"No valid dataset name defined, select among {' ,'.join(DATASET)}.")
        raise ValueError("No valid dataset name defined!")

    # Setup test database with the validated datasets and specified options
    out_path = setup_test_database(args.dataset,
                                   db_path=args.path,
                                   with_configs=args.config,
                                   with_models=args.models,
                                   force=args.force)

    # Log success message indicating completion of dataset cloning
    logger.info(f"Done cloning dataset{'s' if len(args.dataset) > 1 else ''}: {', '.join(args.dataset)}.")


if __name__ == "__main__":
    main()
