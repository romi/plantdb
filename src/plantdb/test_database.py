#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# plantdb - Data handling tools for the ROMI project
#
# Copyright (C) 2018-2019 Sony Computer Science Laboratories
# Authors: D. Colliaux, T. Wintz, P. Hanappe
#
# This file is part of plantdb.
#
# plantdb is free software: you can redistribute it
# and/or modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation, either
# version 3 of the License, or (at your option) any later version.
#
# plantdb is distributed in the hope that it will be
# useful, but WITHOUT ANY WARRANTY; without even the implied
# warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with plantdb.  If not, see
# <https://www.gnu.org/licenses/>.
# ------------------------------------------------------------------------------

"""
This module regroup tools to download test datasets, pipeline configuration files and trained CNN models from ZENODO repository.
It aims at simplifying the creation of a test database for demonstration or CI purposes.

Examples
--------
>>> from plantdb.test_database import setup_test_database
>>> # EXAMPLE 1 - Download and extract the 'real_plant' test database to `plantdb/tests/testdata` module directory:
>>> db_path = setup_test_database('real_plant')
INFO     [plantdb.test_database] File 'real_plant.zip' exists locally. Skipping download.
INFO     [plantdb.test_database] Verifying 'real_plant.zip' MD5 hash value...
INFO     [plantdb.test_database] The test database is set up under '/home/jonathan/Projects/plantdb/tests/testdata'.
>>> print(db_path)
PosixPath('/home/jonathan/Projects/plantdb/tests/testdata')
>>> # EXAMPLE 2 - Download and extract the 'real_plant' and 'virtual_plant' test dataset and configuration pipelines to a temporary folder called 'ROMI_DB':
>>> db_path = setup_test_database(['real_plant', 'virtual_plant'], '/tmp/ROMI_DB', with_configs=True)
INFO     [plantdb.test_database] File 'real_plant.zip' exists locally. Skipping download.
INFO     [plantdb.test_database] Verifying 'real_plant.zip' MD5 hash value...
INFO     [plantdb.test_database] File 'virtual_plant.zip' exists locally. Skipping download.
INFO     [plantdb.test_database] Verifying 'virtual_plant.zip' MD5 hash value...
INFO     [plantdb.test_database] File 'configs.zip' exists locally. Skipping download.
INFO     [plantdb.test_database] Verifying 'configs.zip' MD5 hash value...
INFO     [plantdb.test_database] The test database is set up under '/tmp/ROMI_DB'.
>>> print(db_path)
PosixPath('/tmp/ROMI_DB')

The list of valid dataset names are:
  * ``'real_plant'``: 60 images of a Col-0 _Arabidopsis thaliana_ plant acquired with the _Plant Imager_;
  * ``'virtual_plant'``: 18 snapshots of a virtual _Arabidopsis thaliana_ plant generated with the _Virtual Plant Imager_;
  * ``'real_plant_analyzed'``: the ``real_plant`` dataset reconstructed using the ``AnglesAndInternodes`` task with the ``config/geom_pipe_real.toml`` configuration file;
  * ``'virtual_plant_analyzed'``: the ``virtual_plant`` dataset reconstructed using the ``AnglesAndInternodes`` task with the ``config/geom_pipe_virtual.toml`` configuration file;
  * ``'arabidopsis000'``: 72 snapshots of a virtual _Arabidopsis thaliana_ plant generated with the _Virtual Plant Imager_;

Archive ``'configs.zip'`` contains the configuration files used with the ``romi_run_task`` CLI to reconstruct the datasets.

Archive ``'models.zip'`` contains a preconfigured directory structure with the trained CNN weight file Resnet_896_896_epoch50.pt.
"""
import hashlib
from pathlib import Path
from tempfile import gettempdir
from zipfile import ZipFile

import requests
from tqdm import tqdm

from plantdb.log import configure_logger

DATASET = ["real_plant", "real_plant_analyzed",
           "virtual_plant", "virtual_plant_analyzed",
           "arabidopsis000",
           ]
BASE_URL = "https://zenodo.org/records/10379172/files/"
#: The ZENODO URL corresponding to the pipeline configurations, trained CNN models and test datasets archives:
ZIP_URLS = {
    **{"configs": BASE_URL + "configs.zip", "models": BASE_URL + "models.zip"},
    **{ds: BASE_URL + f"{ds}.zip" for ds in DATASET}
}
#: The ZENODO MD5 hashes corresponding to the pipeline configurations, trained CNN models and test datasets archives:
ZIP_MD5S = {
    "configs": "41e3739a3f9311ee50d328ff1c3087c8",
    "models": "abe92f58e799f28cc991416b3b31fcac",
    "real_plant": "982e75804aa9ef4078a97aac54bd892e",
    "real_plant_analyzed": "cc312c7b304797f2e0e4ad1abefcde32",
    "virtual_plant": "bbee0bcc53e7a099454534bdbe1f9123",
    "virtual_plant_analyzed": "ad94725f173bcce684e19e8d8c833f05",
    "arabidopsis000": "10e0c1fefcc8cf3b629db5b42fd64e36",
}

#: Path to `plantdb` module root directory:
ROOT = Path(__file__).absolute().parent.parent.parent
#: Path to `plantdb` module "tests/testdata" directory:
TEST_DIR = ROOT / "tests" / "testdata"
#: Path to the temporary test database directory.
TMP_TEST_DIR = Path(gettempdir()) / 'ROMI_DB'

logger = configure_logger(__name__)


def _tmp_fpath_from_url(url) -> Path:
    temp_dir = Path(gettempdir())  # get the location of the temporary directory
    tmp_fname = Path(url).name  # get the name of the file to download from the URL
    return temp_dir / tmp_fname


def _save_file_from_url(url):
    """Save URL to a temporary file.

    Parameters
    ----------
    url : str
        A valid URL pointing toward a file.

    Returns
    -------
    pathlib.Path
        The path to the temporary file containing the downloaded file.

    Examples
    --------
    >>> from plantdb.test_database import ZIP_URLS
    >>> from plantdb.test_database import _save_file_from_url
    >>> zip_fname = _save_file_from_url(ZIP_URLS['real_plant'])
    >>> zip_fname
    PosixPath('/tmp/real_plant.zip')
    >>> zip_fname = _save_file_from_url(ZIP_URLS['real_plant_analyzed'])
    >>> zip_fname
    PosixPath('/tmp/real_plant_analyzed.zip')
    """
    tmp_fname = _tmp_fpath_from_url(url)

    logger.info(f"Downloading {url} to {tmp_fname}...")
    r = requests.get(url, stream=True)
    total_size = int(r.headers.get("content-length", 0))  # total size in bytes
    block_size = 32 * 1024  # block-size reads
    progress = 0  # progress tracker
    pbar = tqdm(total=total_size, unit="B", unit_scale=True, unit_divisor=1024)
    with open(tmp_fname, "wb") as f:
        for chunk in r.iter_content(block_size):
            f.write(chunk)
            progress = progress + len(chunk)
            pbar.update(block_size)
    pbar.close()
    if total_size != 0 and progress != total_size:
        raise IOError(f"Error downloading file {tmp_fname.name}!")

    # Close HTTP(S) connection:
    r.close()

    return tmp_fname


def _test_hash(tmp_fname, hash_value, hash_method="md5"):
    """Test the hash value of downloaded file against known hash from ZENODO.

    Parameters
    ----------
    tmp_fname : pathlib.Path
        The path to the downloaded file.
    hash_value : str
        The reference hash value, from ZENODO.
    hash_method : str, optional
        The hash method to use, by default "md5".

    Examples
    --------
    >>> from plantdb.test_database import ZIP_URLS, ZIP_MD5S
    >>> from plantdb.test_database import _test_hash
    >>> from plantdb.test_database import _save_file_from_url
    >>> zip_fname = _save_file_from_url(ZIP_URLS['real_plant'])
    >>> _test_hash(zip_fname, ZIP_MD5S['real_plant'], "md5")
    """
    # Instantiate hash method:
    h = hashlib.new(hash_method.lower())
    # Compute hash based on file content:
    with open(tmp_fname, "rb") as f:
        h.update(f.read())
    # Compare known and computed hash values, raise `ValueError` if not a match:
    logger.info(f"Verifying '{tmp_fname.name}' {hash_method.upper()} hash value...")
    try:
        assert hash_value == h.hexdigest()
    except AssertionError:
        logger.critical(f"{hash_method.upper()} hash verification failed for file {tmp_fname}!")
        logger.critical(f"Expected '{hash_value}', got '{h.hexdigest()}'.")
        tmp_fname.unlink()  # Delete the file if hash comparison fail!
        logger.warning(f"Deleted {tmp_fname}.")
        raise ValueError("Wrong hash value.")
    return


def _get_extract_archive(archive, out_path=TEST_DIR, keep_tmp=False, force=False):
    """Download and extract an archive from ZENODO.

    Parameters
    ----------
    archive : {'configs', 'models', 'real_plant', 'virtual_plant', 'real_plant_analyzed', 'virtual_plant_analyzed', 'arabidopsis000'}
        The base name (without zip extension) of the archive to download.
    out_path : str or pathlib.Path, optional
        The path where to extract the downloaded archive. Defaults to ``TEST_DIR``.
    keep_tmp : bool, optional
        Whether to keep the temporary files. Defaults to ``False``.
    force : bool, optional
        Whether to force redownload of archive. Defaults to ``False``.

    Notes
    -----
    If the file already exists, it will not be downloaded except if ``force`` is ``True``.
    However, it will still be checked against the corresponding known hash value.

    Returns
    -------
    pathlib.Path
        The path to the downloaded archive.
    """
    if isinstance(out_path, str):
        out_path = Path(out_path)

    url = ZIP_URLS[archive]
    tmp_fname = _tmp_fpath_from_url(url)
    if tmp_fname.exists() and not force:
        logger.info(f"File '{tmp_fname.name}' exists locally. Skipping download.")
    else:
        tmp_fname = _save_file_from_url(url)
    # Test the downloaded file hash against known value:
    _test_hash(tmp_fname, ZIP_MD5S[archive], "md5")
    # Extract to given destination if no error was raised:
    ZipFile(tmp_fname).extractall(path=out_path)
    # Remove the temporary file if not explicitly requested to keep it:
    if not keep_tmp:
        tmp_fname.unlink()
        logger.warning(f"Deleted {tmp_fname}.")
    return out_path / archive


def get_test_dataset(dataset, out_path=TEST_DIR, keep_tmp=False, force=False):
    """Download and extract a test dataset from ZENODO.

    Parameters
    ----------
    dataset : {'real_plant', 'virtual_plant', 'real_plant_analyzed', 'virtual_plant_analyzed', 'arabidopsis000'}
        The name of the dataset to download.
    out_path : str or pathlib.Path, optional
        The path where to extract the test dataset archive. Defaults to ``TEST_DIR``.
    keep_tmp : bool, optional
        Whether to keep the temporary files. Defaults to ``False``.
    force : bool, optional
        Whether to force redownload of archive. Defaults to ``False``.

    Returns
    -------
    pathlib.Path
        The path to the downloaded test dataset.

    Examples
    --------
    >>> from plantdb.test_database import get_test_dataset
    >>> get_test_dataset()  # download and extract the test dataset to `plantdb/tests/testdata` directory
    """
    ds_path = out_path / dataset
    if ds_path.exists() and not force:
        out_path = ds_path
    else:
        out_path = _get_extract_archive(dataset, out_path=out_path, keep_tmp=keep_tmp, force=force)
    return out_path


def get_models_dataset(out_path=TEST_DIR, keep_tmp=False, force=False):
    """Download and extract the trained CNN model from ZENODO.

    Parameters
    ----------
    out_path : str or pathlib.Path, optional
        The path where to download the trained CNN model. Defaults to ``TEST_DIR``.
    keep_tmp : bool, optional
        Whether to keep the temporary files. Defaults to ``False``.
    force : bool, optional
        Whether to force redownload of archive. Defaults to ``False``.

    Returns
    -------
    pathlib.Path
        The path to the downloaded trained CNN model.

    Examples
    --------
    >>> from plantdb.test_database import get_models_dataset
    >>> get_models_dataset()  # download and extract the trained CNN models to `plantdb/tests/testdata` directory
    """
    ds_path = out_path / "models"
    if ds_path.exists() and not force:
        out_path = ds_path
    else:
        out_path = _get_extract_archive("models", out_path=out_path, keep_tmp=keep_tmp, force=force)
    return out_path


def get_configs(out_path=TEST_DIR, keep_tmp=False, force=False):
    """Download and extract the pipeline configurations from ZENODO.

    Parameters
    ----------
    out_path : str or pathlib.Path, optional
        The path where to download the pipeline configurations. Defaults to ``TEST_DIR``.
    keep_tmp : bool, optional
        Whether to keep the temporary files. Defaults to ``False``.
    force : bool, optional
        Whether to force redownload of archive. Defaults to ``False``.

    Returns
    -------
    pathlib.Path
        The path to the downloaded configs.

    Examples
    --------
    >>> from plantdb.test_database import get_configs
    >>> get_configs()  # download and extract the pipeline configurations to `plantdb/tests/testdata` directory
    """
    ds_path = out_path / "configs"
    if ds_path.exists() and not force:
        out_path = ds_path
    else:
        out_path = _get_extract_archive("configs", out_path=out_path, keep_tmp=keep_tmp, force=force)
    return out_path


def setup_test_database(dataset, out_path=TEST_DIR, keep_tmp=True, with_configs=False, with_models=False, force=False):
    """Download and extract the test database from ZENODO.

    Parameters
    ----------
    dataset : "all" or str or list
        The dataset name or a list of dataset names to download to the test database.
        Using "all" allows to download all defined datasets.
        See notes below for a list of dataset names and their meanings.
    out_path : str or pathlib.Path, optional
        The path where to set up the database. Defaults to ``TEST_DIR``.
    keep_tmp : bool, optional
        Whether to keep the temporary files. Defaults to ``False``.
    with_configs : bool, optional
        Whether to download the config files. Defaults to ``False``.
    with_models : bool, optional
        Whether to download the trained CNN model files. Defaults to ``False``.
    force : bool, optional
        Whether to force redownload of archive. Defaults to ``False``.

    Returns
    -------
    pathlib.Path
        The path to the database.

    Notes
    -----
    The list of valid dataset names are:
      * ``'real_plant'``: 60 images of a Col-0 _Arabidopsis thaliana_ plant acquired with the _Plant Imager_;
      * ``'virtual_plant'``: 18 snapshots of a virtual _Arabidopsis thaliana_ plant generated with the _Virtual Plant Imager_;
      * ``'real_plant_analyzed'``: the ``real_plant`` dataset reconstructed using the ``AnglesAndInternodes`` task with the ``testcfg/geom_pipe_real.toml`` configuration file;
      * ``'virtual_plant_analyzed'``: the ``virtual_plant`` dataset reconstructed using the ``AnglesAndInternodes`` task with the ``config/geom_pipe_virtual.toml`` configuration file;
      * ``'arabidopsis000'``: 72 snapshots of a virtual _Arabidopsis thaliana_ plant generated with the _Virtual Plant Imager_;

    Examples
    --------
    >>> from plantdb.test_database import setup_test_database, TMP_TEST_DIR
    >>> # EXAMPLE 1 - Download and extract the 'real_plant' test database to `plantdb/tests/testdata` module directory:
    >>> setup_test_database('real_plant')
    PosixPath('/home/jonathan/Projects/plantdb/tests/testdata')
    >>> # EXAMPLE 2 - Download and extract the 'real_plant' and 'virtual_plant' test dataset and configuration pipelines to a temporary folder called 'ROMI_DB':
    >>> setup_test_database(['real_plant', 'virtual_plant'], TMP_TEST_DIR, with_configs=True)
    PosixPath('/tmp/ROMI_DB')
    """
    from plantdb.fsdb import MARKER_FILE_NAME
    from plantdb.fsdb import LOCK_FILE_NAME
    if isinstance(out_path, str):
        out_path = Path(out_path)
    # Make sure the path to the database exists:
    out_path.mkdir(parents=True, exist_ok=True)
    # Make sure the marker file exists:
    marker_path = out_path / MARKER_FILE_NAME
    marker_path.touch(exist_ok=True)
    # Make sure the locking file do NOT exist:
    lock_path = out_path / LOCK_FILE_NAME
    lock_path.unlink(missing_ok=True)
    # Get the list of all test dataset if required:
    if isinstance(dataset, str) and dataset.lower() == "all":
        dataset = DATASET
    # Create a dict of keyword arguments to use for download:
    kwargs = {'out_path': out_path, 'keep_tmp': keep_tmp, 'force': force}
    # Download the test datasets:
    if isinstance(dataset, list):
        [get_test_dataset(ds, **kwargs) for ds in dataset]
    else:
        _ = get_test_dataset(dataset, **kwargs)
    # Download configs archive if requested:
    if with_configs:
        _ = get_configs(**kwargs)
    if with_models:
        _ = get_models_dataset(**kwargs)

    logger.info(f"The test database is set up under '{out_path}'.")
    return out_path


def test_database(dataset='real_plant_analyzed', out_path=TMP_TEST_DIR, **kwargs):
    """Create and return an FSDB test database.

    Parameters
    ----------
    dataset : str or list of str, optional
        The (list of) test dataset to use, by default 'real_plant_analyzed'.
    out_path : str or pathlib.Path, optional
        The path where to set up the database.
        Defaults to the temporary directory under 'ROMI_DB', as defined by ``TMP_TEST_DIR``.

    Other Parameters
    ----------------
    keep_tmp : bool
        Whether to keep the temporary files. Defaults to ``False``.
    with_configs : bool
        Whether to download the config files. Defaults to ``False``.
    with_models : bool
        Whether to download the trained CNN model files. Defaults to ``False``.
    force : bool
        Whether to force redownload of archive. Defaults to ``False``.

    Returns
    -------
    plantdb.fsdb.FSDB
        The FSDB test database.

    Examples
    --------
    >>> from plantdb.test_database import test_database
    >>> db = test_database()
    >>> db.connect()
    >>> db.list_scans()
    ['real_plant_analyzed']
    >>> db.path()
    PosixPath('/tmp/ROMI_DB')
    >>> db.disconnect()
    """
    from plantdb.fsdb import FSDB
    return FSDB(setup_test_database(dataset, out_path=out_path, **kwargs))
