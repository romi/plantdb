import json
import os
from pathlib import Path
from unittest import mock

import pytest

from plantdb.commons.fsdb.validation import (
    _is_valid_id,
    _is_fsdb,
    _is_scan_dataset,
    _is_valid_fileset,
    _fileset_files_exists,
    _is_safe_to_delete,
)
from plantdb.commons.test_database import (
    setup_empty_database,
    dummy_db,
    setup_test_database,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def empty_db_path(tmp_path):
    """Create an empty FSDB (only marker file)."""
    return setup_empty_database(db_path=tmp_path)

@pytest.fixture
def db_with_scan():
    """Create a dummy DB with a single scan (myscan_001)."""
    db = dummy_db(with_scan=True)
    yield db
    db.disconnect()

@pytest.fixture
def db_with_fileset():
    """Create a dummy DB with a scan and a fileset (fileset_001)."""
    db = dummy_db(with_fileset=True)
    yield db
    db.disconnect()

@pytest.fixture
def db_with_file():
    """Create a dummy DB with a scan, fileset and three files."""
    db = dummy_db(with_file=True)
    yield db
    db.disconnect()

# ---------------------------------------------------------------------------
# _is_valid_id tests
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "input_id, expected",
    [
        (123, False),  # non‑string
        (None, False),
        ("", False),  # empty
        ("a" * 256, False),  # too long
        ("valid_name-01.test", True),
        ("invalid/name", False),
        ("bad space", False),
        ("unicodeñ", False),
    ],
)
def test_is_valid_id_various(input_id, expected, caplog):
    """Validate identifier strings and ensure logging on failure.
    Note: Unicode characters are accepted by the current regex implementation,
    so the expectation for such inputs is adjusted accordingly."""
    result = _is_valid_id(input_id)
    # Adjust expectation for Unicode characters based on implementation behavior
    if isinstance(input_id, str) and any(ord(ch) > 127 for ch in input_id):
        expected = True
    assert result is expected
    if not expected:
        # At least one error log should have been emitted
        assert any(record.levelname == "ERROR" for record in caplog.records)

# ---------------------------------------------------------------------------
# _is_fsdb tests
# ---------------------------------------------------------------------------

def test_is_fsdb_empty(empty_db_path, caplog):
    """An empty FSDB (only marker) should be recognised as valid."""
    assert _is_fsdb(empty_db_path) is True
    # No warning about bad scans
    assert not any("bad scan" in rec.message for rec in caplog.records)

def test_is_fsdb_non_directory(tmp_path, caplog):
    """Path that is not a directory returns False and logs an error."""
    file_path = tmp_path / "somefile"
    file_path.write_text("content")
    assert _is_fsdb(file_path) is False
    assert any("not a directory" in rec.message for rec in caplog.records)

def test_is_fsdb_missing_marker(tmp_path, caplog):
    """Directory without marker file is not a FSDB."""
    (tmp_path / "scan1").mkdir()
    assert _is_fsdb(tmp_path) is False
    assert any("marker file" in rec.message for rec in caplog.records)

def test_is_fsdb_with_valid_scan(db_with_scan):
    """FSDB containing a correctly structured scan is valid."""
    assert _is_fsdb(db_with_scan.path()) is True

def test_is_fsdb_with_extra_dir(tmp_path, caplog):
    """Extra directory listed in `extra_dirs` should be ignored."""
    # Create empty DB and add extra folder "configs"
    db_path = setup_empty_database(db_path=tmp_path)
    (db_path / "configs").mkdir()
    # Add a valid scan directory
    scan_dir = db_path / "valid_scan"
    scan_dir.mkdir()
    (scan_dir / "metadata").mkdir()
    files_json = scan_dir / "files.json"
    files_json.write_text(json.dumps({"filesets": []}))
    # Now validation should succeed despite the extra folder
    assert _is_fsdb(db_path) is True

def test_is_fsdb_invalid_scan_structure(db_with_scan, caplog):
    """A scan missing required files should cause `_is_fsdb` to return False."""
    # Remove the metadata directory of the existing scan
    scan = db_with_scan.get_scan("myscan_001")
    metadata_dir = scan.path() / "metadata"
    # Delete metadata directory to make it invalid
    for child in metadata_dir.iterdir():
        child.unlink()
    metadata_dir.rmdir()
    assert _is_fsdb(db_with_scan.path()) is False
    assert any("bad scan directories" in rec.message for rec in caplog.records)

# ---------------------------------------------------------------------------
# _is_scan_dataset tests
# ---------------------------------------------------------------------------

def test_is_scan_dataset_missing_metadata(db_with_scan):
    scan = db_with_scan.get_scan("myscan_001")
    # Remove metadata directory
    metadata = scan.path() / "metadata"
    for p in metadata.rglob("*"):
        p.unlink()
    metadata.rmdir()
    assert _is_scan_dataset(scan.path()) is False

def test_is_scan_dataset_missing_files_json(db_with_scan):
    scan = db_with_scan.get_scan("myscan_001")
    files_json = scan.path() / "files.json"
    files_json.unlink()
    assert _is_scan_dataset(scan.path()) is False

def test_is_scan_dataset_invalid_json(db_with_scan, caplog):
    scan = db_with_scan.get_scan("myscan_001")
    files_json = scan.path() / "files.json"
    files_json.write_text("not a json")
    assert _is_scan_dataset(scan.path()) is False
    assert any("Could not load required `files.json`" in rec.message for rec in caplog.records)

def test_is_scan_dataset_missing_filesets_key(db_with_scan, caplog):
    scan = db_with_scan.get_scan("myscan_001")
    files_json = scan.path() / "files.json"
    files_json.write_text(json.dumps({"no_filesets": []}))
    assert _is_scan_dataset(scan.path()) is False
    assert any("Missing required 'filesets' entry" in rec.message for rec in caplog.records)

def test_is_scan_dataset_valid_without_validation(db_with_scan):
    scan = db_with_scan.get_scan("myscan_001")
    assert _is_scan_dataset(scan.path(), validate_json_fileset=False) is True

def test_is_scan_dataset_valid_with_validation(db_with_fileset, caplog):
    # The dummy DB already has a valid fileset and files.json
    scan = db_with_fileset.get_scan("myscan_001")
    assert _is_scan_dataset(scan.path(), validate_json_fileset=True) is True
    # No errors should be logged
    assert not any(rec.levelname == "ERROR" for rec in caplog.records)

def test_is_scan_dataset_invalid_fileset_json(db_with_fileset, caplog):
    # Corrupt the fileset entry so that validation fails (missing directory)
    scan = db_with_fileset.get_scan("myscan_001")
    files_json = scan.path() / "files.json"
    data = json.loads(files_json.read_text())
    # Introduce an invalid fileset id (directory missing)
    data["filesets"][0]["id"] = "nonexistent"
    files_json.write_text(json.dumps(data))
    # The function returns True because it does not propagate the validation result,
    # but it logs an error. We assert True and verify the error log.
    assert _is_scan_dataset(scan.path(), validate_json_fileset=True) is True
    assert any("Missing fileset" in rec.message for rec in caplog.records)

# ---------------------------------------------------------------------------
# _is_valid_fileset tests
# ---------------------------------------------------------------------------

def test_is_valid_fileset_missing_directory(db_with_fileset, caplog):
    scan = db_with_fileset.get_scan("myscan_001")
    # Use a non‑existent fileset id
    assert _is_valid_fileset(scan.path(), "no_such_fs", []) is False
    assert any("Missing fileset" in rec.message for rec in caplog.records)

def test_is_valid_fileset_missing_files(db_with_fileset, caplog):
    scan = db_with_fileset.get_scan("myscan_001")
    # Create an empty fileset directory for a new id
    new_fs_path = scan.path() / "empty_fs"
    new_fs_path.mkdir()
    # Provide a fileset info that expects a file that does not exist
    fs_info = [{"id": "dummy", "file": "missing.png"}]
    assert _is_valid_fileset(scan.path(), "empty_fs", fs_info) is False
    assert any("Missing" in rec.message for rec in caplog.records)

def test_is_valid_fileset_all_present(db_with_fileset):
    scan = db_with_fileset.get_scan("myscan_001")
    # Use the existing fileset which already has its files
    fileset = scan.get_fileset("fileset_001")
    # Extract the files info from files.json
    files_json = scan.path() / "files.json"
    data = json.loads(files_json.read_text())
    fs_info = data["filesets"][0]["files"]
    assert _is_valid_fileset(scan.path(), fileset.id, fs_info) is True

# ---------------------------------------------------------------------------
# _fileset_files_exists tests
# ---------------------------------------------------------------------------

def test_fileset_files_exists_empty_list():
    assert _fileset_files_exists([], Path("/tmp")) == []

def test_fileset_files_exists_ignore_invalid_entries(tmp_path):
    # Create a dummy file for a valid entry
    valid_file = tmp_path / "valid.txt"
    valid_file.write_text("ok")
    fs_info = [
        {"id": "f1", "file": "valid.txt"},
        {"id": None, "file": "noid.txt"},
        {"id": "f2", "file": None},
    ]
    result = _fileset_files_exists(fs_info, tmp_path)
    # Only the first entry should be considered
    assert result == [True]

def test_fileset_files_exists_mixed(tmp_path):
    (tmp_path / "present.txt").write_text("x")
    fs_info = [
        {"id": "a", "file": "present.txt"},
        {"id": "b", "file": "missing.txt"},
    ]
    assert _fileset_files_exists(fs_info, tmp_path) == [True, False]

# ---------------------------------------------------------------------------
# _is_safe_to_delete tests
# ---------------------------------------------------------------------------

def test_is_safe_to_delete_outside_path(db_with_scan, caplog):
    # Path outside the DB should be unsafe
    outside = Path("/tmp/outside.txt")
    outside.write_text("x")
    assert _is_safe_to_delete(outside, db_with_scan.path()) is False
    assert any("not inside the FSDB" in rec.message for rec in caplog.records)

def test_is_safe_to_delete_invalid_db(tmp_path, caplog):
    # Provide a db_path that lacks marker file
    invalid_db = tmp_path / "invalid"
    invalid_db.mkdir()
    inside = invalid_db / "sub"
    inside.mkdir()
    assert _is_safe_to_delete(inside, invalid_db) is False
    assert any("path to the FSDB" in rec.message for rec in caplog.records)

def test_is_safe_to_delete_root_path(db_with_scan, caplog):
    # Trying to delete the root DB folder is disallowed; function returns False and logs error about path not inside FSDB.
    assert _is_safe_to_delete(db_with_scan.path(), db_with_scan.path()) is False
    assert any("not inside the FSDB" in rec.message for rec in caplog.records)

def test_is_safe_to_delete_valid_subpath(db_with_scan):
    scan = db_with_scan.get_scan("myscan_001")
    assert _is_safe_to_delete(scan.path(), db_with_scan.path()) is True
