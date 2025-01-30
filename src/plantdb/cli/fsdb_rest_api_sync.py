#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Synchronize two ``FSDB`` databases using REST API.
"""
import argparse
import logging
import socket

from tqdm import tqdm

from plantdb.rest_api_client import download_scan_archive
from plantdb.rest_api_client import list_scan_names
from plantdb.rest_api_client import upload_scan_archive

logger = logging.getLogger("API_FSDBSync")
logging.basicConfig(level=logging.INFO)


def parsing():
    """Parses command line arguments for database synchronization.

    Returns
    -------
    argparse.ArgumentParser
        An argument parser configured for FSDB synchronization.
    """
    parser = argparse.ArgumentParser(description='Synchronize two FSDB databases.')
    parser.add_argument('origin', type=str,
                        help='source database URL, as "host:port".')
    parser.add_argument('target', type=str,
                        help='target database URL, as "host:port".')
    return parser


def check_url(url):
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


def sync_scan_archives(origin_url, target_url):
    """Synchronizes scan archives between an origin and a target database.

    This function retrieves a list of scan archives from an origin database and transfers each
    scan to a target database. It relies on REST API calls to download the scan archives from
    the origin and upload them to the target. Logging messages are generated to provide
    feedback about the status of each operation, including details about the origin and 
    target databases, as well as the transfer status of each scan.

    Parameters
    ----------
    origin_url : str
        A 'host:port' URL pointing to the origin plantDB database with a REST API enabled.
    target_url : str
        A 'host:port' URL pointing to the target plantDB database with a REST API enabled.

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
    origin_host, origin_port = origin_url.split(':')
    target_host, target_port = target_url.split(':')
    logger.info(f"Origin URL is '{origin_host}' on port '{origin_port}'.")
    logger.info(f"Target URL is '{target_host}' on port '{target_port}'.")

    scan_list = list_scan_names(host=origin_host, port=origin_port)
    logger.info(f"Found {len(scan_list)} scans in origin database.")

    for scan_id in tqdm(scan_list, desc="Transfer:", unit="scan"):
        logger.info(f"Transferring scan '{scan_id}'...")
        # Download from origin DB via REST API
        f_path, msg = download_scan_archive(scan_id, out_dir='/tmp', host=origin_host, port=origin_port)
        logger.info(msg)
        # Upload to target DB via REST API
        msg = upload_scan_archive(scan_id, f_path, host=target_host, port=target_port)
        logger.info(msg)
        # Delete the temporary file
        f_path.unlink()

    return


def main():
    """Main execution function.

    This function orchestrates the parsing of command-line arguments, validation of the
    provided database URLs, and the synchronization of scan archives between the origin
    and target databases.

    It ensures the input parameters are formatted correctly before invoking the
    synchronization process.
    """
    # Parse command-line arguments using the parsing function
    parser = parsing()
    # Extract the parsed arguments
    args = parser.parse_args()

    # Validate the origin and target database URLs
    check_url(args.origin)  # Ensure the `origin` URL is formatted correctly
    check_url(args.target)  # Ensure the `target` URL is formatted correctly

    # Synchronize scan archives between the origin and target databases
    sync_scan_archives(args.origin, args.target)  # Perform synchronization


if __name__ == '__main__':
    main()
