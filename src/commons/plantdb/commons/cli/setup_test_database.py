#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
# Test Database Setup CLI

A command-line utility to download and set up a ROMI test database from ZENODO.
It wraps the ``plantdb.commons.test_database.setup_test_database`` function to
download the requested test dataset(s) and, optionally, the pipeline
configuration files and trained CNN models.

## Usage Examples

### Download the default 'real_plant' dataset to the per-user cache directory

```shell
setup_test_database
```

### Download several datasets to a specific database directory

```shell
setup_test_database real_plant virtual_plant --db-path /tmp/ROMI_DB
```

### Download all datasets, with configs and models

```shell
setup_test_database all --with-configs --with-models
```

### Force re-download of the dataset

```shell
setup_test_database real_plant --force
```

"""

import os

import click
from click_option_group import OptionGroup
from click_option_group import optgroup

from plantdb.commons.log import DEFAULT_LOG_LEVEL
from plantdb.commons.log import LOG_LEVELS
from plantdb.commons.log import get_logger
from plantdb.commons.test_database import setup_test_database

# Create a logger and set the environment variable
os.environ.setdefault('ROMI_APP_LOGGER', __name__.split('.')[-1])
logger = get_logger(os.getenv('ROMI_APP_LOGGER'), log_level=DEFAULT_LOG_LEVEL)


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.argument('dataset', nargs=-1, required=False)
@click.option('--db-path', type=click.Path(), default=None,
              help="Path to the directory where to set up the database. Defaults to the per-user cache directory '~/.cache/plantdb'."
              )
@click.option('--with-configs', is_flag=True, default=False,
              help="Also download the pipeline configuration files."
              )
@click.option('--with-models', is_flag=True, default=False,
              help="Also download the trained CNN model files."
              )
@click.option('--force', is_flag=True, default=False,
              help="Force re-download of the archive(s), even if they already exist locally."
              )
@click.option('--keep-tmp', is_flag=True, default=False,
              help="Keep the temporary downloaded archive files."
              )
@optgroup.group("Logging", cls=OptionGroup)
@optgroup.option("--log-level", type=click.Choice(LOG_LEVELS, case_sensitive=False), default=DEFAULT_LOG_LEVEL,
                 show_default=True, help="Logging level.",
                 )
def main(dataset, db_path, with_configs, with_models, force, keep_tmp, log_level):
    """Test Database Setup CLI

    Download and set up a ROMI test database from ZENODO.

    DATASET is the name of the dataset(s) to download, or 'all' to download every dataset.
    If omitted, defaults to 'real_plant'.
    """
    # Get the logger and change the level if needed:
    logger = get_logger(os.environ.get('ROMI_APP_LOGGER', __name__))
    logger.setLevel(log_level)

    # Normalize the dataset argument:
    if not dataset:
        dataset = 'real_plant'
    elif len(dataset) == 1:
        dataset = dataset[0]
    else:
        dataset = list(dataset)

    # Download and set up the test database:
    db_path = setup_test_database(
        dataset,
        db_path=db_path,
        keep_tmp=keep_tmp,
        with_configs=with_configs,
        with_models=with_models,
        force=force,
    )
    logger.info(f"The test database is set up under '{db_path}'.")


if __name__ == '__main__':
    main()
