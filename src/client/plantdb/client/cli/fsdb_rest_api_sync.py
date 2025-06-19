#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
FSDB Synchronization via REST API

This module provides functionality to synchronize scan archives between two FSDB
(Plant Database) instances using REST API. It ensures data consistency and aids in
migrating or backing up scan data efficiently between different database instances.

Key Features
------------
- **Command-line Interface (CLI)**: Includes an argument parser for easy configuration of source and target databases.
- **URL Validation**: Verifies the format and accessibility of the provided database URLs.
- **Scan Synchronization**: Retrieves scan archives from the source database and uploads them to the target database.
- **REST API Integration**: Utilizes REST API calls for secure and efficient data transfer.
- **Progress Tracking**: Displays a progress bar with `tqdm` to show transfer status for each scan.
- **Logging**: Offers informational and error logging to provide feedback during the synchronization process.
- **Error Handling**: Handles common errors such as invalid URLs, inaccessible ports, and connectivity issues.

Usage Examples
--------------
To synchronize two FSDB databases, run the script from the command line with the
`origin` and `target` arguments in the format `host:port`.
Temporary files are stored in the `/tmp` directory during the transfer process.

```bash
python fsdb_rest_api_sync.py 192.168.1.1:5000 192.168.1.2:5000
```

This command connects the origin database at `192.168.1.1:5000` to the target database
at `192.168.1.2:5000` and transfers all scan archives.

To filter the transferred scans based on a regular expression, use the `--filter` optional argument.
For example, to transfer only scans with names starting with 'virtual_':
```shell
python fsdb_rest_api_sync.py 192.168.1.1:5000 192.168.1.2:5000 --filter 'virtual_*'
```

Testing
-------
To test this script, you can set up two test FSDB REST API instances locally, with each instance running on a different port.
One instance should start with sample data, while the other should be empty.
You can then use this script to transfer data from the first instance to the second.

For example:

1. In the first terminal, run the following command:
    ```shell
    fsdb_rest_api --port 5001 --test
    ```
    This will create a database with sample data that listens on port 5001.
2. In the second terminal, run:
    ```shell
    fsdb_rest_api --port 5002 --test --empty
    ```
    This will create an empty database that listens on port 5002.
3. Finally, in the third terminal, run the sync script:
    ```shell
    fsdb_rest_api_sync 'http://127.0.0.1:5001' 'http://127.0.0.1:5002'
    ```
    This command transfers the data from the instance running on port 5001 to the instance running on port 5002.

"""

import argparse
import re
from pathlib import Path
from requests.exceptions import HTTPError
from urllib.parse import urlparse

from tqdm import tqdm

from plantdb.commons.log import DEFAULT_LOG_LEVEL
from plantdb.commons.log import LOG_LEVELS
from plantdb.commons.log import get_logger
from plantdb.client.rest_api import download_scan_archive
from plantdb.client.rest_api import list_scan_names
from plantdb.client.rest_api import refresh
from plantdb.client.rest_api import test_availability
from plantdb.client.rest_api import upload_scan_archive


def parsing():
    """Parses command line arguments for database synchronization.

    Returns
    -------
    argparse.ArgumentParser
        An argument parser configured for FSDB synchronization.
    """
    parser = argparse.ArgumentParser(description='Transfer dataset from an origin FSDB database towards a target using REST API.')
    parser.add_argument('origin', type=str,
                        help='source database URL, as "host:port".')
    parser.add_argument('target', type=str,
                        help='target database URL, as "host:port".')

    parser.add_argument('--filter', type=str, default=None,
                        help='optional regular expression to filter scan names.')

    log_opt = parser.add_argument_group("Logging options")
    log_opt.add_argument("--log-level", dest="log_level", type=str, default=DEFAULT_LOG_LEVEL, choices=LOG_LEVELS,
                         help="Level of message logging, defaults to 'INFO'.")

    return parser


def filter_scan(scan_list, filter_pattern, logger):
    """Filters a list of scan identifiers based on a regular expression pattern.

    Parameters
    ----------
    scan_list : list of str
        A list of scan identifiers to be filtered.
    filter_pattern : str
        A regular expression pattern used to filter the scan identifiers.
    logger : logging.Logger
        A logger instance for logging messages.

    Returns
    -------
    list of str or None
        A list containing only the scan identifiers that match the
        regular expression pattern, or None if the pattern is invalid.
    """
    try:
        regex = re.compile(filter_pattern)
        scan_list = [scan_id for scan_id in scan_list if regex.search(scan_id)]
        return scan_list
    except re.error as e:
        logger.error(f"Invalid regular expression '{filter_pattern}': {e}")
    return


def sync_scan_archives(origin_url, target_url, filter_pattern=None, log_level=DEFAULT_LOG_LEVEL):
    """Synchronizes scan archives between an origin and a target database.

    This function retrieves a list of scan archives from an origin database and transfers each
    scan to a target database.
    It relies on REST API calls to download the scan archives from the origin and upload them to the target.
    Optionally filters the list of scans using a regular expression.

    Parameters
    ----------
    origin_url : str
        A correctly formatted URL pointing to the origin plantDB database with a REST API enabled.
    target_url : str
        A correctly formatted URL pointing to the target plantDB database with a REST API enabled.
    filter_pattern : str, optional
        A regular expression to filter scan names.
        Defaults to ``None``, which means no filtering is applied.
    log_level : str, optional
        The log level for the logger.
        Defaults to ``DEFAULT_LOG_LEVEL``, which is 'INFO'.

    Raises
    ------
    SystemExit
        If there are errors parsing command-line arguments.

    Notes
    -----
    - The method assumes that both databases are accessible and have a REST API enabled.
    - The method refreshes the scan in the target database after each transfer.
    - Temporary files are stored in the `/tmp` directory during the transfer process and deleted afterward.
    """
    logger = get_logger('fsdb_rest_api_sync', log_level=log_level)

    parsed_origin = urlparse(origin_url)
    origin_host, origin_port = parsed_origin.hostname, parsed_origin.port
    parsed_target = urlparse(target_url)
    target_host, target_port = parsed_target.hostname, parsed_target.port
    logger.info(f"Origin URL is '{origin_host}' on port '{origin_port}'.")
    logger.info(f"Target URL is '{target_host}' on port '{target_port}'.")

    origin_scan_list = list_scan_names(host=origin_host, port=origin_port)
    target_scan_list = list_scan_names(host=target_host, port=target_port)
    if filter_pattern:
        origin_scan_list = filter_scan(origin_scan_list, filter_pattern, logger)
        logger.info(f"{len(origin_scan_list)} scans match the filter pattern '{filter_pattern}'.")
    else:
        logger.info(f"Found {len(origin_scan_list)} scans in origin database.")

    for scan_id in tqdm(origin_scan_list, desc="Transfer", unit="scan"):
        if scan_id in target_scan_list:
            logger.debug(f"Scan '{scan_id}' already exists in target database. Skipping...")
            continue
        logger.debug(f"Transferring scan '{scan_id}'...")
        # Download from origin DB via REST API
        f_path, msg = download_scan_archive(scan_id, out_dir='/tmp', host=origin_host, port=origin_port)
        logger.debug(msg)
        # Upload to target DB via REST API
        msg = upload_scan_archive(scan_id, f_path, host=target_host, port=target_port)
        logger.debug(msg)
        # Delete the temporary file
        Path(f_path).unlink()
        # Refresh the scan in the target to load its infos:
        try:
            msg = refresh(scan_id, host=target_host, port=target_port)
        except HTTPError as e:
            logger.error(f"Error refreshing target database for scan '{scan_id}': {e}")
            continue
        logger.debug(msg)

    return


def main():
    """Main execution function.

    This function orchestrates the parsing of command-line arguments, validation of the
    provided database URLs, and the synchronization of scan archives between the origin
    and target databases.
    """
    # Parse command-line arguments using the parsing function
    parser = parsing()
    # Extract the parsed arguments
    args = parser.parse_args()

    # Validate the availability of origin and target database URLs
    test_availability(args.origin)
    test_availability(args.target)

    # Synchronize scan archives between the origin and target databases
    sync_scan_archives(args.origin, args.target, args.filter, args.log_level)  # Perform synchronization


if __name__ == '__main__':
    main()
