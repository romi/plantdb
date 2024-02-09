#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Create a local plantdb database (FSDB) using shared datasets."""

import argparse

from plantdb.log import configure_logger
from plantdb.test_database import DATASET
from plantdb.test_database import setup_test_database

logger = configure_logger(__name__)


def parsing():
    parser = argparse.ArgumentParser(description='Create a local plantdb database (FSDB) using shared datasets.')

    parser.add_argument('path', type=str,
                        help='Path to the test database to set up.')
    parser.add_argument('-d', '--dataset', type=str, nargs="+",
                        default=['real_plant'],
                        help="Test dataset to clone, use 'all' to get all of them. " +\
                             "You can list several dataset names to clone. " +\
                             "Available dataset names are: " +\
                             ", ".join([f"'{ds}'" for ds in DATASET]) + ". " +\
                             "By default we clone the 'real_plant' dataset.")
    parser.add_argument('--config', action='store_true',
                        help='Use this to also clone the configuration files.')
    parser.add_argument('--models', action='store_true',
                        help='Use this to also clone the trained CNN model files.')
    parser.add_argument('--force', action='store_true',
                        help='Use this to force download of archive.')
    return parser


def main():
    parser = parsing()
    args = parser.parse_args()

    if args.dataset[0] == "all":
        args.dataset = DATASET

    args.dataset = list(set(args.dataset) & set(DATASET))
    if len(args.dataset) == 0:
        logger.critical(f"No valid dataset name defined, select among {' ,'.join(DATASET)}.")
        raise ValueError("No valid dataset name defined!")

    out_path = setup_test_database(args.dataset,
                                   out_path=args.path,
                                   with_configs=args.config,
                                   with_models=args.models,
                                   force=args.force,
                                   )
    logger.info(f"Done cloning dataset{'s' if len(args.dataset) > 1 else ''}: {', '.join(args.dataset)}.")


if __name__ == "__main__":
    main()
