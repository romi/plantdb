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
    fsdb_rest_api_sync 127.0.0.1:5001 127.0.0.1:5002
    ```
    This command transfers the data from the instance running on port 5001 to the instance running on port 5002.

"""

import argparse
import re
import socket
from pathlib import Path

from tqdm import tqdm

from plantdb.log import DEFAULT_LOG_LEVEL
from plantdb.log import LOG_LEVELS
from plantdb.log import get_logger
from plantdb.rest_api_client import download_scan_archive
from plantdb.rest_api_client import list_scan_names
from plantdb.rest_api_client import upload_scan_archive


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


def check_url(url):
    """Verifies the connectivity to a given host and port from a URL-like string.

    This function parses a URL string into host and port components, attempts to establish a
    socket connection to check its availability, and raises appropriate exceptions on failure.

    Parameters
    ----------
    url : str
        A string specifying the host and port in the format 'host:port'.

    Raises
    ------
    ValueError
        If the input URL is not in the correct format 'host:port'.
    ConnectionError
        If the specified host and port cannot be connected, indicating that the port might
        be closed or unavailable.
    RuntimeError
        If an unexpected error occurs during the verification process.
    """
    try:
        host, port = url.split(':')
        port = int(port)  # Ensure port is an integer
        socket.setdefaulttimeout(2)  # Set a timeout for the connection check
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex((host, port)) != 0:
                raise ConnectionError(f"Cannot connect to {host}:{port}. Port might be closed or unavailable.")
    except ValueError:
        raise ValueError(f"Database URL should be 'host:port', got '{url}' instead.")
    except Exception as e:
        raise RuntimeError(f"Error verifying the URL '{url}': {e}")


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
        A 'host:port' URL pointing to the origin plantDB database with a REST API enabled.
    target_url : str
        A 'host:port' URL pointing to the target plantDB database with a REST API enabled.
    filter_pattern : str, optional
        A regular expression to filter scan names.

    Raises
    ------
    SystemExit
        If there are errors parsing command-line arguments.

    Notes
    -----
    - The script assumes that connection details for the origin and target databases are
      provided as command-line arguments in the format `host:port`.
    - Temporary files are stored in the `/tmp` directory during the transfer process.
    """
    logger = get_logger('fsdb_rest_api_sync', log_level=log_level)

    origin_host, origin_port = origin_url.split(':')
    target_host, target_port = target_url.split(':')
    logger.info(f"Origin URL is '{origin_host}' on port '{origin_port}'.")
    logger.info(f"Target URL is '{target_host}' on port '{target_port}'.")

    scan_list = list_scan_names(host=origin_host, port=origin_port)
    if filter_pattern:
        scan_list = filter_scan(scan_list, filter_pattern, logger)
        logger.info(f"{len(scan_list)} scans match the filter pattern '{filter_pattern}'.")
    else:
        logger.info(f"Found {len(scan_list)} scans in origin database.")

    for scan_id in tqdm(scan_list, desc="Transfer:", unit="scan"):
        logger.debug(f"Transferring scan '{scan_id}'...")
        # Download from origin DB via REST API
        f_path, msg = download_scan_archive(scan_id, out_dir='/tmp', host=origin_host, port=origin_port)
        logger.debug(msg)
        # Upload to target DB via REST API
        msg = upload_scan_archive(scan_id, f_path, host=target_host, port=target_port)
        logger.debug(msg)
        # Delete the temporary file
        Path(f_path).unlink()

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

    # Validate the origin and target database URLs
    check_url(args.origin)  # Ensure the `origin` URL is formatted correctly
    check_url(args.target)  # Ensure the `target` URL is formatted correctly

    # Synchronize scan archives between the origin and target databases
    sync_scan_archives(args.origin, args.target, args.filter, args.log_level)  # Perform synchronization


if __name__ == '__main__':
    main()
