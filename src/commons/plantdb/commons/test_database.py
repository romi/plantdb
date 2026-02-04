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
This module regroups tools to download test datasets, pipeline configuration files and trained CNN models from ZENODO repository.
It aims at simplifying the creation of a test database for demonstration or CI purposes.

Examples
--------
>>> from plantdb.commons.test_database import setup_test_database
>>> # EXAMPLE 1 - Download and extract the 'real_plant' test database to `plantdb/tests/testdata` module directory:
>>> db_path = setup_test_database('real_plant')
INFO     [test_database] File 'real_plant.zip' exists locally. Skipping download.
INFO     [test_database] Verifying 'real_plant.zip' MD5 hash value...
INFO     [test_database] The test database is set up under '/home/jonathan/Projects/plantdb/tests/testdata'.
>>> print(db_path)
PosixPath('/home/jonathan/Projects/plantdb/tests/testdata')
>>> # EXAMPLE 2 - Download and extract the 'real_plant' and 'virtual_plant' test dataset and configuration pipelines to a temporary folder called 'ROMI_DB':
>>> db_path = setup_test_database(['real_plant', 'virtual_plant'], '/tmp/ROMI_DB', with_configs=True)
INFO     [test_database] File 'real_plant.zip' exists locally. Skipping download.
INFO     [test_database] Verifying 'real_plant.zip' MD5 hash value...
INFO     [test_database] File 'virtual_plant.zip' exists locally. Skipping download.
INFO     [test_database] Verifying 'virtual_plant.zip' MD5 hash value...
INFO     [test_database] File 'configs.zip' exists locally. Skipping download.
INFO     [test_database] Verifying 'configs.zip' MD5 hash value...
INFO     [test_database] The test database is set up under '/tmp/ROMI_DB'.
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
from tempfile import mkdtemp
from zipfile import ZipFile

import requests
from plantdb.commons.auth.session import SingleSessionManager
from plantdb.commons.log import get_logger
from tqdm import tqdm

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

logger = get_logger(__name__)


def _tmp_fpath_from_url(url) -> Path:
    """Generate a temporary file path for a file from a given URL.

    Parameters
    ----------
    url : str
        The URL of the file to download.

    Returns
    -------
    pathlib.Path
        The path to the temporary file generated from the URL.
    """
    temp_dir = Path(gettempdir())  # get the location of the temporary directory
    tmp_fname = Path(url).name  # get the name of the file to download from the URL
    return temp_dir / tmp_fname


def _mkdtemp_romidb() -> Path:
    """Creates a temporary directory with a specific prefix.

    The created directory has the prefix `'ROMI_DB'` to make it identifiable.

    Returns
    -------
    pathlib.Path
        The path object pointing to the newly created temporary directory.

    Examples
    --------
    >>> from plantdb.commons.test_database import _mkdtemp_romidb
    >>> _mkdtemp_romidb()
    PosixPath('/tmp/ROMI_DB_********')
    """
    return Path(mkdtemp(prefix='ROMI_DB_'))


def _save_file_from_url(url) -> Path:
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
    >>> from plantdb.commons.test_database import ZIP_URLS
    >>> from plantdb.commons.test_database import _save_file_from_url
    >>> zip_fname = _save_file_from_url(ZIP_URLS['real_plant'])
    >>> zip_fname
    PosixPath('/tmp/real_plant.zip')
    >>> zip_fname = _save_file_from_url(ZIP_URLS['real_plant_analyzed'])
    >>> zip_fname
    PosixPath('/tmp/real_plant_analyzed.zip')
    """
    tmp_fname = _tmp_fpath_from_url(url)
    logger.info(f"Downloading {url} to {tmp_fname}...")

    # Ensure we hit the raw file endpoint
    if "zenodo.org/records/" in url and "files/" in url and "?download=" not in url:
        url = url + "?download=1"
    headers = {"User-Agent": "Mozilla/5.0 (compatible; MyDownloader/1.0)"}

    with requests.get(url, stream=True, headers=headers, timeout=5) as r:
        r.raise_for_status()  # raise an exception for bad status
        total_size = int(r.headers.get("content-length", 0))
        block_size = 32 * 1024

        progress = 0
        pbar = tqdm(total=total_size, unit="B", unit_scale=True, unit_divisor=1024)

        with open(tmp_fname, "wb") as f:
            for chunk in r.iter_content(block_size):
                if not chunk:  # guard against keep‑alive chunks
                    continue
                f.write(chunk)
                progress += len(chunk)
                pbar.update(len(chunk))

        pbar.close()

        if total_size and progress != total_size:
            raise IOError(f"Error downloading file {tmp_fname.name}!")

    return tmp_fname


def _test_hash(tmp_fname, hash_value, hash_method="md5"):
    """Test the hash value of a downloaded file against a known hash from ZENODO.

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
    >>> from plantdb.commons.test_database import ZIP_URLS, ZIP_MD5S
    >>> from plantdb.commons.test_database import _test_hash
    >>> from plantdb.commons.test_database import _save_file_from_url
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
        tmp_fname.unlink()  # Delete the file if hash comparison fails!
        logger.warning(f"Deleted {tmp_fname}.")
        raise ValueError("Wrong hash value.")
    return


def _get_archive(archive, force=False) :
    """Download and verify an archive file from a given URL.

    This function retrieves an archive file from a specified URL.
    If the file already exists locally and `force` is not set to ``True``, it skips downloading again,
    otherwise it downloads the file afresh.
    The downloaded file's hash is then tested against a known value for verification.

    Parameters
    ----------
    archive : str
        The name of the archive to download.
    force : bool, optional
        If ``True``, forces re-downloading even if the file already exists locally. Defaults to ``False``.

    Returns
    -------
    Path
        The path to the downloaded and verified archive file.
    """
    url = ZIP_URLS[archive]
    tmp_fname = _tmp_fpath_from_url(url)
    if tmp_fname.exists() and not force:
        logger.info(f"File '{tmp_fname.name}' exists locally. Skipping download.")
    else:
        tmp_fname = _save_file_from_url(url)
    # Test the downloaded file hash against a known value:
    _test_hash(tmp_fname, ZIP_MD5S[archive], "md5")
    return tmp_fname


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
        Whether to force redownloading of the archive. Defaults to ``False``.

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

    tmp_fname = _get_archive(archive, force=force)
    # Extract to the given destination if no error was raised:
    ZipFile(tmp_fname).extractall(path=out_path)
    # Remove the temporary file if not explicitly requested to keep it:
    if not keep_tmp:
        tmp_fname.unlink()
        logger.warning(f"Deleted {tmp_fname}.")
    return out_path / archive


def get_test_dataset(dataset, db_path=TEST_DIR, keep_tmp=False, force=False):
    """Download and extract a test dataset from ZENODO.

    Parameters
    ----------
    dataset : {'real_plant', 'virtual_plant', 'real_plant_analyzed', 'virtual_plant_analyzed', 'arabidopsis000'}
        The name of the dataset to download.
    db_path : str or pathlib.Path, optional
        The path where to extract the test dataset archive. Defaults to ``TEST_DIR``.
    keep_tmp : bool, optional
        Whether to keep the temporary files. Defaults to ``False``.
    force : bool, optional
        Whether to force redownloading of the archive. Defaults to ``False``.

    Returns
    -------
    pathlib.Path
        The path to the downloaded test dataset.

    Examples
    --------
    >>> from plantdb.commons.test_database import get_test_dataset
    >>> get_test_dataset()  # download and extract the test dataset to `plantdb/tests/testdata` directory
    """
    ds_path = db_path / dataset
    if ds_path.exists() and not force:
        db_path = ds_path
    else:
        db_path = _get_extract_archive(dataset, out_path=db_path, keep_tmp=keep_tmp, force=force)
    return db_path


def get_models_dataset(db_path=TEST_DIR, keep_tmp=False, force=False):
    """Download and extract the trained CNN model from ZENODO.

    Parameters
    ----------
    db_path : str or pathlib.Path, optional
        The path where to download the trained CNN model. Defaults to ``TEST_DIR``.
    keep_tmp : bool, optional
        Whether to keep the temporary files. Defaults to ``False``.
    force : bool, optional
        Whether to force redownloading of the archive. Defaults to ``False``.

    Returns
    -------
    pathlib.Path
        The path to the downloaded trained CNN model.

    Examples
    --------
    >>> from plantdb.commons.test_database import get_models_dataset
    >>> get_models_dataset()  # download and extract the trained CNN models to `plantdb/tests/testdata` directory
    """
    ds_path = db_path / "models"
    if ds_path.exists() and not force:
        db_path = ds_path
    else:
        db_path = _get_extract_archive("models", out_path=db_path, keep_tmp=keep_tmp, force=force)
    return db_path


def get_configs(db_path=TEST_DIR, keep_tmp=False, force=False):
    """Download and extract the pipeline configurations from ZENODO.

    Parameters
    ----------
    db_path : str or pathlib.Path, optional
        The path where to download the pipeline configurations. Defaults to ``TEST_DIR``.
    keep_tmp : bool, optional
        Whether to keep the temporary files. Defaults to ``False``.
    force : bool, optional
        Whether to force redownloading of archive. Defaults to ``False``.

    Returns
    -------
    pathlib.Path
        The path to the downloaded configs.

    Examples
    --------
    >>> from plantdb.commons.test_database import get_configs
    >>> get_configs()  # download and extract the pipeline configurations to `plantdb/tests/testdata` directory
    """
    ds_path = db_path / "configs"
    if ds_path.exists() and not force:
        db_path = ds_path
    else:
        db_path = _get_extract_archive("configs", out_path=db_path, keep_tmp=keep_tmp, force=force)
    return db_path


def setup_empty_database(db_path=None):
    """Sets up an empty ROMI database.

    Sets up necessary marker file and ensures the absence of a lock file.

    Parameters
    ----------
    db_path : str or Path, optional
        The directory path where the database should be set up.
        Defaults to ``None``.

    Returns
    -------
    pathlib.Path
        The directory path where the database was set up.

    Examples
    --------
    >>> from plantdb.commons.test_database import setup_empty_database
    >>> path = setup_empty_database()
    >>> print(path)  # initialize a `ROMI_DB` directory in the temporary directory by default
    /tmp/ROMI_DB_********
    >>> print([path.name for path in path.iterdir()])  # only the 'marker' file is created
    ['romidb']
    """
    from plantdb.commons.fsdb.core import MARKER_FILE_NAME

    if isinstance(db_path, str):
        db_path = Path(db_path)
    elif db_path is None:
        db_path = _mkdtemp_romidb()
    else:
        try:
            assert isinstance(db_path, Path)
        except AssertionError:
            logger.critical(f"Invalid path to set up the database: '{db_path}'.")
            logger.critical(
                "Please provide a valid path to set up the database or leave it to None to use a temporary directory.")
            raise TypeError(f"Invalid type for 'out_path': {type(db_path)}.")

    # Make sure the path to the database exists:
    db_path.mkdir(parents=True, exist_ok=True)
    # Make sure the marker file exists:
    marker_path = db_path / MARKER_FILE_NAME
    marker_path.touch(exist_ok=True)

    return db_path


def setup_test_database(dataset, db_path=TEST_DIR, keep_tmp=True, with_configs=False, with_models=False, force=False):
    """Download and extract the test database from ZENODO.

    Parameters
    ----------
    dataset : "all" or str or list
        The dataset name or a list of dataset names to download to the test database.
        Using "all" allows downloading all defined datasets.
        See the notes below for a list of dataset names and their meanings.
    db_path : str or pathlib.Path, optional
        The path where to set up the database. Defaults to ``TEST_DIR``.
    keep_tmp : bool, optional
        Whether to keep the temporary files. Defaults to ``False``.
    with_configs : bool, optional
        Whether to download the config files. Defaults to ``False``.
    with_models : bool, optional
        Whether to download the trained CNN model files. Defaults to ``False``.
    force : bool, optional
        Whether to force download of archive. Defaults to ``False``.

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
    >>> from plantdb.commons.test_database import setup_test_database
    >>> # EXAMPLE 1 - Download and extract the 'real_plant' test database to `plantdb/tests/testdata` module directory:
    >>> setup_test_database('real_plant')
    PosixPath('/home/jonathan/Projects/plantdb/tests/testdata')
    >>> # EXAMPLE 2 - Download and extract the 'real_plant' and 'virtual_plant' test dataset and configuration pipelines to a temporary folder called 'ROMI_DB':
    >>> setup_test_database(['real_plant', 'virtual_plant'], None, with_configs=True)
    PosixPath('/tmp/ROMI_DB_********')
    """
    # Initialize an empty ROMI database
    db_path = setup_empty_database(db_path)

    # Get the list of all test dataset if required:
    if isinstance(dataset, str) and dataset.lower() == "all":
        dataset = DATASET
    # Create a dict of keyword arguments to use for download:
    kwargs = {'db_path': db_path, 'keep_tmp': keep_tmp, 'force': force}
    # Download the test datasets:
    if isinstance(dataset, list):
        [get_test_dataset(ds, **kwargs) for ds in dataset]
    else:
        _ = get_test_dataset(dataset, **kwargs)
    # Download the configuration files archive if requested:
    if with_configs:
        _ = get_configs(**kwargs)
    # Download the trained CNN files archive if requested:
    if with_models:
        _ = get_models_dataset(**kwargs)

    # --- Backward compatibility ---
    # If the database was set up under a previous version, the Scan may not have an 'owner' entry in their metadata.
    # -----------------------------
    from plantdb.commons.fsdb.core import FSDB
    # Connect to the database and iterate over scans to get the owner of each scan:
    db = FSDB(db_path)
    db.connect()
    db.list_scans(owner_only=False)
    for scan_name, scan in db.scans.items():
        _ = scan.owner  # get the owner of the scan, if unknown, it will be set to the anonymous user
    db.disconnect()

    logger.info(f"The test database is set up under '{db_path}'.")
    return db_path


def test_database(dataset='real_plant_analyzed', db_path=None, **kwargs):
    """Create and return an FSDB test database.

    Parameters
    ----------
    dataset : str or list[str] or None, optional
        The (list of) test dataset to use, by default 'real_plant_analyzed'.
        Using "all" allows downloading all defined datasets.
        If ``None``, only set up an empty database.
    db_path : str or pathlib.Path, optional
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
        Whether to force redownloading of archive. Defaults to ``False``.

    Returns
    -------
    plantdb.commons.fsdb.FSDB
        The FSDB test database.

    Examples
    --------
    >>> from plantdb.commons.test_database import test_database
    >>> db = test_database()
    >>> db.connect()
    >>> db.list_scans()
    ['real_plant_analyzed']
    >>> db.path()
    PosixPath('/tmp/ROMI_DB_********')
    >>> db.disconnect()
    """
    from plantdb.commons.fsdb.core import FSDB
    session_manager = kwargs.pop('session_manager', None)
    if dataset is None:
        return FSDB(setup_empty_database(db_path=db_path), session_manager=session_manager)
    else:
        return FSDB(setup_test_database(dataset, db_path=db_path, **kwargs), session_manager=session_manager)


def dummy_db(with_scan=False, with_fileset=False, with_file=False):
    """Create a dummy temporary database.

    Parameters
    ----------
    with_scan : bool, optional
        If ``True`` (default to ``False``), add a ``Scan``, named ``"myscan_001"``, to the database.
    with_fileset : bool, optional
        If ``True`` (default to ``False``), add a ``Fileset``, named ``"fileset_001"``, to the scan ``"myscan_001"``.
    with_file : bool, optional
        If ``True`` (default to ``False``), add three ``File``, to the fileset ``"fileset_001"``:

        - a dummy PNG array, named ``"dummy_image"``;
        - a dummy RGB image, named ``"test_image"``;
        - a dummy JSON file, named ``"test_json"``;

    Returns
    -------
    plantdb.commons.fsdb.FSDB
        The dummy database.

    Notes
    -----
    - Returns a 'connected' database, no need to call the ``connect()`` method.
    - Uses the 'admin' user to login.
    - Calling the ``disconnect()`` method will clean up the associated temporary directory.

    Examples
    --------
    >>> from plantdb.commons.test_database import dummy_db
    >>> db = dummy_db(with_file=True)
    >>> db.connect()
    INFO     [FSDB] Connected to database successfully
    >>> from plantdb.commons.fsdb.core import get_logged_username
    >>> get_logged_username(db)  # 'admin' is logged by default
    'admin'
    >>> print(db.path())  # the database directory
    /tmp/romidb_********
    >>> print(db.list_scans())
    ['myscan_001']
    >>> scan = db.get_scan("myscan_001")  # get the existing scan
    >>> print(scan.list_filesets())
    ['fileset_001']
    >>> fs = scan.get_fileset("fileset_001")
    >>> print(list(fs.list_files()))
    ['dummy_image', 'test_image', 'test_json']
    >>> f = fs.get_file("test_image")
    >>> print(f.path())
    /tmp/romidb_********/myscan_001/fileset_001/test_image.png
    >>> db.disconnect()  # clean up (delete) the temporary dummy database
    >>> print(db.path().exists())
    False
    """
    from plantdb.commons import io
    from plantdb.commons.fsdb.core import FSDB
    from plantdb.commons.fsdb.core import MARKER_FILE_NAME

    # Create a temporary folder to host the dummy database
    db_path = _mkdtemp_romidb()
    # Add the necessary marker file
    marker_file = db_path / MARKER_FILE_NAME
    marker_file.open(mode='w').close()
    # Create the FSDB instance and connect
    db = FSDB(db_path, required_filesets=[], session_manager=SingleSessionManager())
    # Flag this instance as a dummy DB so that disconnect will clean up the temp folder
    db._is_dummy = True
    db.connect()
    # Login as adin to get all the rights (to create and edit)
    _ = db.login('admin', 'admin')

    if with_file:
        # To create a `File`, existing `Scan` & `Fileset` are required
        with_scan, with_fileset = True, True
    if with_fileset:
        # To create a `Fileset`, an existing `Scan` is required
        with_scan = True

    # Create a `Scan` object if required:
    if with_scan:
        scan = db.create_scan("myscan_001")
        scan.set_metadata("test", 1)

    # Create a `Fileset` object if required:
    if with_fileset:
        fs = scan.create_fileset("fileset_001")
        fs.set_metadata("test_fileset", 1)

    # Create a `Fileset` object if required:
    if with_file:
        import numpy as np
        # -- Create a fixed dummy image:
        f = fs.create_file("dummy_image")
        img = np.array([[255, 0], [0, 255]]).astype('uint8')
        io.write_image(f, img, "png")
        f.set_metadata("dummy image", True)
        # -- Create a random RGB image:
        f = fs.create_file("test_image")
        rng = np.random.default_rng()
        img = np.array(255 * rng.random((50, 50, 3)), dtype='uint8')
        io.write_image(f, img, "png")
        f.set_metadata("random image", True)
        # -- Create a dummy JSON
        f = fs.create_file("test_json")
        md = {"Who you gonna call?": "Ghostbuster"}
        io.write_json(f, md, "json")
        f.set_metadata("random json", True)

    return db
