#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import unittest

from plantdb.commons.fsdb.core import FSDB
from plantdb.commons.fsdb.core import File
from plantdb.commons.fsdb.core import Fileset
from plantdb.commons.fsdb.core import Scan
from plantdb.commons.fsdb.core import _filter_query
from plantdb.commons.fsdb.exceptions import FileNotFoundError
from plantdb.commons.fsdb.exceptions import FilesetExistsError
from plantdb.commons.fsdb.exceptions import FilesetNotFoundError
from plantdb.commons.fsdb.exceptions import ScanExistsError
from plantdb.commons.fsdb.exceptions import ScanNotFoundError
from plantdb.commons.testing import DummyDBTestCase


class TestFSDBDummy(DummyDBTestCase):

    def test_connect(self):
        db = self.get_test_db()
        self.assertIsInstance(db, FSDB)
        self.assertTrue(db.path().is_dir())
        self.assertTrue(db.is_connected)

    def test_get_scan(self):
        db = self.get_test_db()
        scan = db.get_scan("myscan_001")  # exists
        self.assertIsInstance(scan, Scan)
        with self.assertRaises(ScanNotFoundError):
            db.get_scan("myscan_002")  # does not exist

    def test_create_scan(self):
        db = self.get_test_db()
        scan_id = "testscan_2"
        scan = db.create_scan(scan_id)
        self.assertTrue(scan.id == scan_id)
        self.assertTrue(scan.path().is_dir())

    def test_set_scan_metadata(self):
        scan = self.get_test_scan()
        md = {"test": "value"}
        scan.set_metadata(md)
        db = scan.get_db()
        scan_id = scan.id
        db._is_dummy = False  # to avoid cleanup by `disconnect` method
        db.disconnect()
        db.connect()
        scan = db.get_scan(scan_id)
        self.assertTrue(scan.get_metadata('test') == md['test'])

    def test_list_scan(self):
        db = self.get_test_db()
        self.assertListEqual(db.list_scans(), ['myscan_001'])

    def test_delete_scan(self):
        db = self.get_test_db()
        scan_path = db.get_scan("myscan_001").path()
        db.delete_scan("myscan_001")

        self.assertTrue(db.path().is_dir())
        with self.assertRaises(ScanNotFoundError):
            db.get_scan("myscan_001")
        self.assertFalse(scan_path.is_dir())

    def test_get_fileset(self):
        scan = self.get_test_scan()
        fileset = scan.get_fileset("fileset_001")  # exists
        self.assertIsInstance(fileset, Fileset)
        with self.assertRaises(FilesetNotFoundError):
            scan.get_fileset("fileset_002")  # does not exist

    def test_create_fileset(self):
        scan = self.get_test_scan()
        fs_id = "testfileset_2"
        fileset = scan.create_fileset(fs_id)
        self.assertTrue(fileset.id == fs_id)
        self.assertTrue(fileset.path().is_dir())

    def test_set_fileset_metadata(self):
        fileset = self.get_test_fileset()
        md = {"test": "value"}
        fileset.set_metadata(md)
        db = fileset.get_db()
        scan_id = fileset.get_scan().id
        fs_id = fileset.id
        db._is_dummy = False  # to avoid cleanup by `disconnect` method
        db.disconnect()
        db.connect()
        fileset = db.get_scan(scan_id).get_fileset(fs_id)
        self.assertTrue(fileset.get_metadata('test') == md['test'])

    def test_list_fileset(self):
        scan = self.get_test_scan()
        self.assertListEqual(scan.list_filesets(), ['fileset_001'])

    def test_delete_fileset(self):
        scan = self.get_test_scan()
        fs_path = scan.get_fileset("fileset_001").path()
        scan.delete_fileset("fileset_001")

        self.assertTrue(scan.path().is_dir())
        with self.assertRaises(FilesetNotFoundError):
            scan.get_fileset("fileset_001")
        self.assertFalse(fs_path.is_dir())

    def test_get_file(self):
        fileset = self.get_test_fileset()
        file = fileset.get_file("test_image")  # exists
        self.assertIsInstance(file, File)

    def test_create_file(self):
        from plantdb.commons.io import write_json
        fileset = self.get_test_fileset()
        f_id = "test_file_md"
        file = fileset.create_file(f_id)
        self.assertTrue(file.id == f_id)
        self.assertIsNone(file.filename)  # Do not exist on drive before `io.write_*`
        write_json(file, {"test": "value"})
        self.assertTrue(file.path().is_file())

    def test_set_file_metadata(self):
        file = self.get_test_image_file()
        md = {"test": "value"}
        file.set_metadata(md)
        db = file.get_db()
        scan_id = file.get_scan().id
        fs_id = file.get_fileset().id
        f_id = file.id
        db._is_dummy = False  # to avoid cleanup by `disconnect` method
        db.disconnect()
        db.connect()
        file = db.get_scan(scan_id).get_fileset(fs_id).get_file(f_id)
        self.assertTrue(file.get_metadata('test') == md['test'])

    def test_list_file(self):
        fs = self.get_test_fileset()
        self.assertListEqual(fs.list_files(), ['dummy_image', 'test_image', 'test_json'])

    def test_write_raw_file(self):
        fs = self.get_test_fileset()
        new_f = fs.create_file('file_007')
        md = {"Name": "Bond, James Bond"}  # Create an example dictionary to save as JSON
        import json
        data = json.dumps(md).encode()
        new_f.write_raw(data, 'json')
        self.assertTrue(new_f.path().is_file())

    def test_read_raw_file(self):
        fs = self.get_test_fileset()
        new_f = fs.create_file('file_007')
        md = {"Name": "Bond, James Bond"}  # Create an example dictionary to save as JSON
        import json
        data = json.dumps(md).encode()
        new_f.write_raw(data, 'json')
        file = fs.get_file('file_007')
        self.assertTrue(file.read_raw() == data)

    def test_write_file(self):
        fs = self.get_test_fileset()
        new_f = fs.create_file('file_007')
        md = {"Name": "Bond, James Bond"}  # Create an example dictionary to save as JSON
        import json
        data = json.dumps(md)
        new_f.write(data, 'json')
        self.assertTrue(new_f.path().is_file())

    def test_read_file(self):
        fs = self.get_test_fileset()
        new_f = fs.create_file('file_007')
        md = {"Name": "Bond, James Bond"}  # Create an example dictionary to save as JSON
        import json
        data = json.dumps(md)
        new_f.write(data, 'json')
        file = fs.get_file('file_007')
        self.assertTrue(file.read() == data)

    def test_delete_file(self):
        fs = self.get_test_fileset()
        f_path = fs.get_file("test_image").path()
        fs.delete_file("test_image")

        self.assertTrue(fs.path().is_dir())
        with self.assertRaises(FileNotFoundError):
            fs.get_file("test_image")
        self.assertFalse(f_path.is_file())

    def test_import_file(self):
        fs = self.get_test_fileset()
        file = fs.get_file('test_image')
        new_file = fs.create_file('test_image2')
        new_file.import_file(file.path())

        self.assertTrue(file.path().is_file())
        self.assertTrue(new_file.path().is_file())
        self.assertFalse(file.path() == new_file.path())
        self.assertFalse(file.path().stat == new_file.path().stat)

    def test_fsdb_path_returns_copy(self):
        """FSDB.path() returns a deep copy so mutating it does not affect basedir."""
        db = self.get_test_db()
        p1 = db.path()
        p2 = db.path()
        # Two calls must yield equal but distinct objects
        self.assertEqual(p1, p2)
        self.assertIsNot(p1, p2)

    def test_fsdb_disconnect_sets_is_connected_false(self):
        """FSDB.disconnect() sets is_connected to False."""
        db = self.get_test_db()
        self.assertTrue(db.is_connected)
        db._is_dummy = False  # prevent directory removal
        db.disconnect()
        self.assertFalse(db.is_connected)

    def test_fsdb_disconnect_clears_scans(self):
        """FSDB.disconnect() empties the internal scans dictionary."""
        db = self.get_test_db()
        db._is_dummy = False  # prevent directory removal
        db.disconnect()
        self.assertEqual(db.scans, {})

    def test_fsdb_scan_exists_true(self):
        """FSDB.scan_exists() returns True for a scan that exists in the database."""
        db = self.get_test_db()
        # 'myscan_001' is created by DummyDBTestCase.setUp
        self.assertTrue(db.scan_exists("myscan_001"))

    def test_fsdb_scan_exists_false(self):
        """FSDB.scan_exists() returns False for a scan that does not exist."""
        db = self.get_test_db()
        self.assertFalse(db.scan_exists("nonexistent_scan"))

    def test_fsdb_get_scans_returns_list(self):
        """FSDB.get_scans() returns a non-empty list of Scan instances."""
        db = self.get_test_db()
        scans = db.get_scans()
        self.assertIsInstance(scans, list)
        self.assertGreater(len(scans), 0)
        # Every element must be a Scan
        for s in scans:
            self.assertIsInstance(s, Scan)

    def test_fsdb_create_scan_invalid_id(self):
        """FSDB.create_scan() raises ValueError when the scan ID contains invalid characters."""
        db = self.get_test_db()
        # '/' is not allowed in a scan identifier
        with self.assertRaises(ValueError):
            db.create_scan("invalid/scan")

    def test_fsdb_create_scan_duplicate_raises(self):
        """FSDB.create_scan() raises ScanExistsError when the scan ID already exists."""
        db = self.get_test_db()
        # 'myscan_001' already exists after setUp
        with self.assertRaises(ScanExistsError):
            db.create_scan("myscan_001")

    def test_fsdb_create_scan_with_metadata(self):
        """FSDB.create_scan() stores the supplied metadata on the new scan."""
        db = self.get_test_db()
        scan = db.create_scan("scan_with_md", metadata={"project": "test_project"})
        # Metadata should be immediately accessible on the returned object
        self.assertEqual(scan.get_metadata("project"), "test_project")

    def test_fsdb_reload_single_scan(self):
        """FSDB.reload() with a scan ID re-loads only that scan without error."""
        db = self.get_test_db()
        # Should complete without raising an exception
        db.reload("myscan_001")
        # The scan must still be accessible after reload
        scan = db.get_scan("myscan_001")
        self.assertIsInstance(scan, Scan)


# ---------------------------------------------------------------------------
# Scan tests
# ---------------------------------------------------------------------------

class TestScan(DummyDBTestCase):

    def test_scan_path_is_under_db_path(self):
        """Scan.path() is a sub-directory of the parent FSDB.path()."""
        scan = self.get_test_scan()
        db_path = scan.get_db().path()
        # The scan path must start with the db path
        self.assertTrue(str(scan.path()).startswith(str(db_path)))

    def test_scan_fileset_exists_true(self):
        """Scan.fileset_exists() returns True for a fileset that exists."""
        scan = self.get_test_scan()
        self.assertTrue(scan.fileset_exists("fileset_001"))

    def test_scan_fileset_exists_false(self):
        """Scan.fileset_exists() returns False for a fileset that does not exist."""
        scan = self.get_test_scan()
        self.assertFalse(scan.fileset_exists("nonexistent_fileset"))

    def test_scan_get_filesets_returns_list(self):
        """Scan.get_filesets() returns a list containing Fileset instances."""
        scan = self.get_test_scan()
        filesets = scan.get_filesets()
        self.assertIsInstance(filesets, list)
        self.assertGreater(len(filesets), 0)
        for fs in filesets:
            self.assertIsInstance(fs, Fileset)

    def test_scan_create_fileset_invalid_id(self):
        """Scan.create_fileset() raises ValueError when the fileset ID is invalid."""
        scan = self.get_test_scan()
        with self.assertRaises(ValueError):
            scan.create_fileset("invalid/fileset")

    def test_scan_create_fileset_duplicate_raises(self):
        """Scan.create_fileset() raises FilesetExistsError when the fileset ID already exists."""
        scan = self.get_test_scan()
        with self.assertRaises(FilesetExistsError):
            scan.create_fileset("fileset_001")

    def test_scan_list_filesets_no_query(self):
        """Scan.list_filesets() without a query returns all fileset IDs as strings."""
        scan = self.get_test_scan()
        fs_ids = scan.list_filesets()
        self.assertIsInstance(fs_ids, list)
        # All entries must be strings
        for fs_id in fs_ids:
            self.assertIsInstance(fs_id, str)

    def test_scan_get_metadata_key(self):
        """Scan.get_metadata(key) returns the value for an existing metadata key."""
        scan = self.get_test_scan()
        scan.set_metadata({"colour": "green"})
        # Verify the value is retrievable by key
        self.assertEqual(scan.get_metadata("colour"), "green")

    def test_scan_get_metadata_default(self):
        """Scan.get_metadata(key) returns the supplied default for a missing key."""
        scan = self.get_test_scan()
        result = scan.get_metadata("nonexistent_key", default="fallback")
        self.assertEqual(result, "fallback")

    def test_scan_set_metadata_string_key(self):
        """Scan.set_metadata(key, value) correctly stores a string-keyed metadata entry."""
        scan = self.get_test_scan()
        scan.set_metadata("species", "arabidopsis")
        self.assertEqual(scan.get_metadata("species"), "arabidopsis")

    def test_scan_get_db(self):
        """Scan.get_db() returns the parent FSDB instance."""
        scan = self.get_test_scan()
        db = scan.get_db()
        self.assertIsInstance(db, FSDB)


# ---------------------------------------------------------------------------
# Fileset tests
# ---------------------------------------------------------------------------

class TestFileset(DummyDBTestCase):

    def test_fileset_path_is_under_scan_path(self):
        """Fileset.path() is a sub-directory of the parent Scan.path()."""
        fileset = self.get_test_fileset()
        scan_path = fileset.get_scan().path()
        self.assertTrue(str(fileset.path()).startswith(str(scan_path)))

    def test_fileset_file_exists_true(self):
        """Fileset.file_exists() returns True for a file that exists."""
        fileset = self.get_test_fileset()
        # 'test_image' is created by DummyDBTestCase.setUp
        self.assertTrue(fileset.file_exists("test_image"))

    def test_fileset_file_exists_false(self):
        """Fileset.file_exists() returns False for a file that does not exist."""
        fileset = self.get_test_fileset()
        self.assertFalse(fileset.file_exists("nonexistent_file"))

    def test_fileset_get_files_returns_list(self):
        """Fileset.get_files() returns a list of File instances."""
        fileset = self.get_test_fileset()
        files = fileset.get_files()
        self.assertIsInstance(files, list)
        self.assertGreater(len(files), 0)
        for f in files:
            self.assertIsInstance(f, File)

    def test_fileset_create_file_invalid_id(self):
        """Fileset.create_file() raises ValueError when the file ID contains invalid characters."""
        fileset = self.get_test_fileset()
        with self.assertRaises(ValueError):
            fileset.create_file("invalid/file")

    def test_fileset_get_metadata_key(self):
        """Fileset.get_metadata(key) returns the value for an existing metadata key."""
        fileset = self.get_test_fileset()
        fileset.set_metadata({"instrument": "camera"})
        self.assertEqual(fileset.get_metadata("instrument"), "camera")

    def test_fileset_get_metadata_default(self):
        """Fileset.get_metadata(key) returns the default value for a missing key."""
        fileset = self.get_test_fileset()
        result = fileset.get_metadata("missing_key", default=42)
        self.assertEqual(result, 42)

    def test_fileset_set_metadata_dict(self):
        """Fileset.set_metadata(dict) updates the metadata from a full dictionary."""
        fileset = self.get_test_fileset()
        fileset.set_metadata({"wavelength": "650nm", "lens": "macro"})
        self.assertEqual(fileset.get_metadata("wavelength"), "650nm")
        self.assertEqual(fileset.get_metadata("lens"), "macro")

    def test_fileset_get_scan(self):
        """Fileset.get_scan() returns the parent Scan instance."""
        fileset = self.get_test_fileset()
        scan = fileset.get_scan()
        self.assertIsInstance(scan, Scan)
        self.assertEqual(scan.id, "myscan_001")

    def test_fileset_get_db(self):
        """Fileset.get_db() returns the parent FSDB instance."""
        fileset = self.get_test_fileset()
        db = fileset.get_db()
        self.assertIsInstance(db, FSDB)


# ---------------------------------------------------------------------------
# File tests
# ---------------------------------------------------------------------------

class TestFile(DummyDBTestCase):

    def test_file_path_is_file_after_write_raw(self):
        """File.path() points to an actual file on disk after write_raw()."""
        fileset = self.get_test_fileset()
        new_file = fileset.create_file("bond_raw")
        data = json.dumps({"agent": "007"}).encode()
        new_file.write_raw(data, "json")
        # The file must exist on disk
        self.assertTrue(new_file.path().is_file())

    def test_file_get_metadata_key(self):
        """File.get_metadata(key) returns the correct value for a present key."""
        file = self.get_test_image_file()
        # Set a known metadata entry, then retrieve it
        file.set_metadata("resolution", "1080p")
        self.assertEqual(file.get_metadata("resolution"), "1080p")

    def test_file_get_metadata_default(self):
        """File.get_metadata(key) returns the default for a key that is absent."""
        file = self.get_test_image_file()
        result = file.get_metadata("absent_key", default="none")
        self.assertEqual(result, "none")

    def test_file_set_metadata_dict(self):
        """File.set_metadata(dict) merges a dictionary into the file's metadata."""
        file = self.get_test_image_file()
        file.set_metadata({"camera": "FLIR", "exposure": "1/200"})
        self.assertEqual(file.get_metadata("camera"), "FLIR")
        self.assertEqual(file.get_metadata("exposure"), "1/200")

    def test_file_get_fileset(self):
        """File.get_fileset() returns the parent Fileset instance."""
        file = self.get_test_image_file()
        fileset = file.get_fileset()
        self.assertIsInstance(fileset, Fileset)
        self.assertEqual(fileset.id, "fileset_001")

    def test_file_get_scan(self):
        """File.get_scan() returns the grandparent Scan instance."""
        file = self.get_test_image_file()
        scan = file.get_scan()
        self.assertIsInstance(scan, Scan)
        self.assertEqual(scan.id, "myscan_001")

    def test_file_get_db(self):
        """File.get_db() returns the root FSDB instance."""
        file = self.get_test_image_file()
        db = file.get_db()
        self.assertIsInstance(db, FSDB)

    def test_file_read_raw_roundtrip(self):
        """Writing bytes with write_raw() and reading them back with read_raw() is lossless."""
        fileset = self.get_test_fileset()
        new_file = fileset.create_file("roundtrip_raw")
        original = json.dumps({"key": "value"}).encode()
        new_file.write_raw(original, "json")
        # read_raw must return exactly the same bytes
        self.assertEqual(new_file.read_raw(), original)

    def test_file_read_roundtrip(self):
        """Writing a string with write() and reading it back with read() is lossless."""
        fileset = self.get_test_fileset()
        new_file = fileset.create_file("roundtrip_str")
        original = json.dumps({"key": "value"})
        new_file.write(original, "json")
        # read must return the identical string
        self.assertEqual(new_file.read(), original)

    def test_file_import_file_invalid_path(self):
        """File.import_file() raises ValueError when the source path does not exist."""
        fileset = self.get_test_fileset()
        new_file = fileset.create_file("import_fail")
        with self.assertRaises(ValueError):
            new_file.import_file("/tmp/this_path_does_not_exist_at_all.png")


# ---------------------------------------------------------------------------
# _filter_query tests
# ---------------------------------------------------------------------------

class TestFilterQuery(DummyDBTestCase):

    def _get_files(self):
        """Helper: return the list of File objects from the default fileset."""
        return self.get_test_fileset().get_files()

    def test_filter_query_no_query_returns_all(self):
        """_filter_query with query=None returns the full unfiltered list."""
        files = self._get_files()
        result = _filter_query(files, query=None)
        # All files must be present
        self.assertEqual(len(result), len(files))

    def test_filter_query_empty_dict_returns_all(self):
        """_filter_query with an empty dict query returns the full unfiltered list."""
        files = self._get_files()
        result = _filter_query(files, query={})
        self.assertEqual(len(result), len(files))

    def test_filter_query_with_match(self):
        """_filter_query returns only the objects whose metadata matches the query."""
        files = self._get_files()
        # 'test_image' has metadata {'random image': True}
        result = _filter_query(files, query={"random image": True})
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].id, "test_image")

    def test_filter_query_no_match_returns_empty(self):
        """_filter_query returns an empty list when no object matches the query."""
        files = self._get_files()
        result = _filter_query(files, query={"nonexistent_key": "nonexistent_value"})
        self.assertEqual(len(result), 0)

    def test_filter_query_fuzzy(self):
        """_filter_query with fuzzy=True supports regular expression matching."""
        scan = self.get_test_scan()
        # Set a metadata value that can be matched with a regex
        scan.set_metadata({"biome": "tropical_forest"})
        scans = self.get_test_db().get_scans()
        # Regex 'tropic.*' should match 'tropical_forest'
        result = _filter_query(scans, query={"biome": "tropic.*"}, fuzzy=True)
        self.assertGreater(len(result), 0)
        self.assertEqual(result[0].id, "myscan_001")


if __name__ == "__main__":
    unittest.main()
