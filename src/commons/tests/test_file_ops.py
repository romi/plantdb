#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Unit tests for the file_ops module.
"""

import json

import pytest

from plantdb.commons.fsdb.file_ops import _delete_file
from plantdb.commons.fsdb.file_ops import _delete_timelapse
from plantdb.commons.fsdb.file_ops import _load_file
from plantdb.commons.fsdb.file_ops import _load_fileset
from plantdb.commons.fsdb.file_ops import _load_fileset_files
from plantdb.commons.fsdb.file_ops import _load_measures
from plantdb.commons.fsdb.file_ops import _load_scan
from plantdb.commons.fsdb.file_ops import _load_scan_at
from plantdb.commons.fsdb.file_ops import _load_scan_filesets
from plantdb.commons.fsdb.file_ops import _load_scan_measures
from plantdb.commons.fsdb.file_ops import _load_scans
from plantdb.commons.fsdb.file_ops import _make_fileset
from plantdb.commons.fsdb.file_ops import _make_scan
from plantdb.commons.test_database import dummy_db


@pytest.fixture
def db_with_fileset():
    """Provide a dummy database with a fileset."""
    db = dummy_db(with_fileset=True)
    db.connect()
    yield db
    db.disconnect()


@pytest.fixture
def db_with_file():
    """Provide a dummy database with a file."""
    db = dummy_db(with_file=True)
    db.connect()
    yield db
    db.disconnect()


@pytest.fixture
def db_with_fileset_and_file():
    """Provide a dummy database with a fileset and file."""
    db = dummy_db(with_fileset=True, with_file=True)
    db.connect()
    yield db
    db.disconnect()


def test_load_scans_empty_db():
    """Test loading scans from an empty database."""
    db = dummy_db()
    db.connect()
    scans = _load_scans(db)
    assert isinstance(scans, dict)
    assert len(scans) == 0
    db.disconnect()


def test_load_scans_with_scans():
    """Test loading scans when scans exist in the database."""
    db = dummy_db()
    db.connect()
    scan = db.create_scan("test_scan")
    scans = _load_scans(db)
    assert len(scans) == 1
    assert "test_scan" in scans
    assert scans["test_scan"].id == "test_scan"
    db.disconnect()


def test_load_scan_success():
    """Test successful loading of a scan."""
    db = dummy_db(with_fileset=True)
    db.connect()
    scan = db.create_scan("test_scan")
    scan_obj = _load_scan(db, "test_scan")
    assert scan_obj is not None
    assert scan_obj.id == "test_scan"
    db.disconnect()


def test_load_scan_nonexistent():
    """Test loading a non-existent scan returns None."""
    db = dummy_db()
    db.connect()
    scan_obj = _load_scan(db, "nonexistent")
    assert scan_obj is None
    db.disconnect()


def test_load_scan_filesets_success():
    """Test loading scan filesets from a valid files.json."""
    db = dummy_db(with_file=True)
    db.connect()
    scan = db.get_scan("myscan_001")
    filesets, needs_update = _load_scan_filesets(scan)
    assert isinstance(filesets, dict) or filesets is None
    db.disconnect()


def test_load_scan_filesets_invalid_json():
    """Test loading scan filesets when files.json is malformed."""
    db = dummy_db(with_file=True)
    db.connect()
    scan = db.get_scan("myscan_001")

    # Mock files.json to have invalid structure
    files_json = scan.path() / "files.json"
    with open(files_json, "w") as f:
        json.dump({"filesets": "not_a_list"}, f)

    filesets, needs_update = _load_scan_filesets(scan)
    assert filesets is None
    assert needs_update is True
    db.disconnect()


def test_load_fileset_success():
    """Test loading a fileset from valid info."""
    db = dummy_db(with_file=True)
    db.connect()
    scan = db.get_scan("myscan_001")

    # Get the fileset info from the files.json
    files_json = scan.path() / "files.json"
    with open(files_json, "r") as f:
        structure = json.load(f)

    filesets_info = structure["filesets"]
    fileset_info = filesets_info[0] if filesets_info else {}

    # Load the fileset using the actual function with a real scan
    # We can't easily mock the scan, so we'll test the function directly
    # by creating a scan instance and using it properly
    from plantdb.commons.fsdb.core import Scan
    scan_instance = Scan(db, "myscan_001")
    fileset, needs_update = _load_fileset(scan_instance, fileset_info)
    assert fileset is not None
    assert isinstance(needs_update, bool)
    db.disconnect()


def test_load_fileset_files_success():
    """Test loading fileset files from a valid list."""
    db = dummy_db(with_file=True)
    db.connect()
    scan = db.get_scan("myscan_001")

    # Get the fileset info from the files.json
    files_json = scan.path() / "files.json"
    with open(files_json, "r") as f:
        structure = json.load(f)

    filesets_info = structure["filesets"]
    fileset_info = filesets_info[0] if filesets_info else {}

    # Load the fileset files using the actual function with a real scan
    from plantdb.commons.fsdb.core import Scan, Fileset
    scan_instance = Scan(db, "myscan_001")
    fileset = Fileset(scan_instance, fileset_info['id'])
    files, needs_update = _load_fileset_files(fileset, fileset_info)
    assert isinstance(files, dict)
    assert isinstance(needs_update, bool)
    db.disconnect()


def test_load_fileset_files_invalid_files():
    """Test loading fileset files when files is not a list."""
    db = dummy_db(with_file=True)
    db.connect()
    scan = db.get_scan("myscan_001")

    # Mock fileset info with invalid files
    fileset_info = {"id": "test", "files": "not_a_list"}

    # Load the fileset files using the actual function with a real scan
    from plantdb.commons.fsdb.core import Scan, Fileset
    scan_instance = Scan(db, "myscan_001")
    fileset = Fileset(scan_instance, "test")

    with pytest.raises(IOError):
        _load_fileset_files(fileset, fileset_info)
    db.disconnect()


def test_load_file_success():
    """Test loading a file from valid info."""
    db = dummy_db(with_file=True)
    db.connect()
    scan = db.get_scan("myscan_001")

    # Get the fileset info from the files.json
    files_json = scan.path() / "files.json"
    with open(files_json, "r") as f:
        structure = json.load(f)

    filesets_info = structure["filesets"]
    fileset_info = filesets_info[0] if filesets_info else {}
    files_info = fileset_info.get("files", [])
    file_info = files_info[0] if files_info else {}

    # Load the file using the actual function with a real scan
    from plantdb.commons.fsdb.core import Scan, Fileset
    scan_instance = Scan(db, "myscan_001")
    fileset = Fileset(scan_instance, fileset_info['id'])

    file = _load_file(fileset, file_info)
    assert file is not None
    assert hasattr(file, 'id')
    db.disconnect()


def test_load_measures_success():
    """Test loading measures from a valid JSON file."""
    db = dummy_db()
    db.connect()
    scan = db.create_scan("test_scan")
    # Create a measures.json file
    measures_file = scan.path() / "measures.json"
    with open(measures_file, "w") as f:
        json.dump({"test": "data"}, f)

    measures = _load_measures(measures_file)
    assert isinstance(measures, dict)
    assert measures["test"] == "data"
    db.disconnect()


def test_load_measures_invalid_data():
    """Test loading measures when JSON does not contain a dict."""
    db = dummy_db()
    db.connect()
    scan = db.create_scan("test_scan")
    # Create a measures.json file with non-dict data
    measures_file = scan.path() / "measures.json"
    with open(measures_file, "w") as f:
        json.dump(["not", "a", "dict"], f)

    with pytest.raises(IOError):
        _load_measures(measures_file)
    db.disconnect()


def test_load_scan_measures():
    """Test loading scan measures."""
    db = dummy_db()
    db.connect()
    scan = db.create_scan("test_scan")
    # Create a measures.json file
    measures_file = scan.path() / "measures.json"
    with open(measures_file, "w") as f:
        json.dump({"test": "data"}, f)

    measures = _load_scan_measures(scan)
    assert isinstance(measures, dict)
    assert measures["test"] == "data"
    db.disconnect()


def test_delete_file_no_filename():
    """Test deleting a file with no filename attribute."""
    db = dummy_db(with_file=True)
    db.connect()
    scan = db.get_scan("myscan_001")
    fileset = scan.get_fileset("fileset_001")
    file = fileset.get_file("dummy_image")
    file.filename = None

    # Should not raise an exception
    _delete_file(file)
    db.disconnect()


def test_make_fileset():
    """Test making a fileset directory."""
    db = dummy_db()
    db.connect()
    scan = db.create_scan("test_scan")
    fileset = scan.create_fileset("test_fileset")

    path = _make_fileset(fileset)
    assert path.exists()
    assert path.is_dir()
    db.disconnect()


def test_make_scan():
    """Test making a scan directory."""
    db = dummy_db()
    db.connect()
    scan = db.create_scan("test_scan")

    path = _make_scan(scan)
    assert path.exists()
    assert path.is_dir()
    db.disconnect()


def test_make_scan_nested():
    """Test making a member scan directory under a timelapse container."""
    from plantdb.commons.fsdb.core import Scan
    db = dummy_db()
    db.connect()
    scan = Scan(db, "tl_scan_0")
    scan.metadata = {"timelapse": {"id": "tl_001", "index": 0}}

    path = _make_scan(scan)
    assert path.exists()
    assert path.is_dir()
    assert path == (db.path() / "tl_001" / "tl_scan_0").resolve()
    assert (db.path() / "tl_001" / "timelapse.json").is_file()
    db.disconnect()


def test_dual_read_scans():
    """Test loading scans from a DB containing both flat and timelapse scans."""
    from plantdb.commons.fsdb.core import Scan
    db = dummy_db()
    db.connect()

    # Create a flat scan
    flat_scan = db.create_scan("flat_scan_01")
    _ = flat_scan.create_fileset("fs1")

    # Create a nested member scan in a timelapse
    tl_scan = Scan(db, "tl_member_01")
    tl_scan.metadata = {"timelapse": {"id": "tl_exp", "index": 0}}
    _make_scan(tl_scan)
    tl_fs = tl_scan.create_fileset("fs_tl")
    (tl_scan.path() / "metadata").mkdir(exist_ok=True)
    (tl_scan.path() / "metadata" / "metadata.json").write_text(json.dumps(tl_scan.metadata))

    loaded_scans = _load_scans(db)
    assert "flat_scan_01" in loaded_scans
    assert "tl_member_01" in loaded_scans
    assert loaded_scans["tl_member_01"].metadata["timelapse"]["id"] == "tl_exp"

    # Test _load_scan single lookup
    single_scan = _load_scan(db, "tl_member_01")
    assert single_scan is not None
    assert single_scan.id == "tl_member_01"
    assert single_scan.metadata["timelapse"]["id"] == "tl_exp"

    db.disconnect()


def test_delete_timelapse():
    """Test deleting a timelapse container (non-recursive guard and recursive deletion)."""
    from plantdb.commons.fsdb.core import Scan
    db = dummy_db()
    db.connect()

    # Create timelapse and member scan
    tl_scan = Scan(db, "tl_scan_to_del")
    tl_scan.metadata = {"timelapse": {"id": "tl_del_test", "index": 0}}
    _make_scan(tl_scan)
    (tl_scan.path() / "metadata").mkdir(exist_ok=True)
    (tl_scan.path() / "metadata" / "metadata.json").write_text(json.dumps(tl_scan.metadata))
    tl_fs = tl_scan.create_fileset("fs")

    # Deleting non-empty timelapse with recursive=False should raise ValueError
    with pytest.raises(ValueError):
        _delete_timelapse(db, "tl_del_test", recursive=False)

    # Deleting with recursive=True should succeed
    _delete_timelapse(db, "tl_del_test", recursive=True)
    assert not (db.path() / "tl_del_test").exists()

    db.disconnect()
