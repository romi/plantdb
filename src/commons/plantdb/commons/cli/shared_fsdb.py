#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
# Shared FSDB Setup CLI

A command‑line utility for setting up a local plantdb database (FSDB) by cloning shared datasets. It is useful for users who need to work with specific test datasets without downloading them individually.

## Key Features

- **Simple CLI interface** using `click` with positional arguments for the target path.
- **Dataset selection**: Supports cloning of multiple datasets, including all available ones or a specified subset.
- **Optional data cloning**: Clone configuration files and trained CNN model files.
- **Force download option**: Refresh the archive regardless of existing data.
- **Configurable logging**: choose from predefined log levels to control output verbosity.

## Usage Examples

To create a local database with the default 'real_plant' dataset at `/path/to/db`:

```shell
shared_fsdb /path/to/db
```

To clone all available datasets along with configuration files to `/path/to/db`:

```shell
shared_fsdb --dataset all --config /path/to/db
```

"""

import os
from pathlib import Path

import click
from click_option_group import OptionGroup
from click_option_group import optgroup

from plantdb.commons.log import DEFAULT_LOG_LEVEL
from plantdb.commons.log import LOG_LEVELS
from plantdb.commons.log import get_logger
from plantdb.commons.test_database import DATASET
from plantdb.commons.test_database import setup_test_database

# Create a logger and set the environment variable
os.environ.setdefault('ROMI_APP_LOGGER', __name__.split('.')[-1])
logger = get_logger(os.getenv('ROMI_APP_LOGGER'), log_level=DEFAULT_LOG_LEVEL)


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.argument('path', type=click.Path())
@click.option('-d', '--dataset', type=str, nargs="+",
              default=['real_plant'],
              help="Test dataset to clone, use 'all' to get all of them. " +
                   "You can list several dataset names to clone. " +
                   "Available dataset names are: " +
                   ", ".join([f"'{ds}'" for ds in DATASET]) + ". " +
                   "By default we clone the 'real_plant' dataset.")
@click.option('--config', is_flag=True,
              help='Use this to also clone the configuration files.')
@click.option('--models', is_flag=True,
              help='Use this to also clone the trained CNN model files.')
@click.option('--force', is_flag=True,
              help='Use this to force download of archive.')
@optgroup.group("Logging", cls=OptionGroup)
@optgroup.option(
    "--log-level",
    type=click.Choice(LOG_LEVELS, case_sensitive=False),
    default=DEFAULT_LOG_LEVEL,
    show_default=True,
    help="Logging level.",
)
def main(path, dataset, config, models, force, log_level):
    """Shared FSDB Setup CLI

    A command‑line utility for setting up a local plantdb database (FSDB) by cloning shared datasets.

    PATH is the path to the test database to set up.
    """
    # Get the logger and change the level if needed:
    logger = get_logger(os.environ.get('ROMI_APP_LOGGER', __name__))
    logger.setLevel(log_level)

    # If "all" is in dataset, replace it with all available datasets
    if dataset[0] == "all":
        dataset = DATASET

    # Validate and filter requested datasets to ensure they exist
    dataset = list(set(dataset) & set(DATASET))

    # Log critical error and raise ValueError if no valid dataset names are provided
    if len(dataset) == 0:
        logger.critical(f"No valid dataset name defined, select among {' ,'.join(DATASET)}.")
        raise ValueError("No valid dataset name defined!")

    # Set up test database with the validated datasets and specified options
    out_path = setup_test_database(dataset,
                                   db_path=path,
                                   with_configs=config,
                                   with_models=models,
                                   force=force)

    # Log success message indicating completion of dataset cloning
    logger.info(f"Done cloning dataset{'s' if len(dataset) > 1 else ''}: {', '.join(dataset)}.")


if __name__ == "__main__":
    main()
