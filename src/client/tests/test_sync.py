#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import tempfile
import unittest
from pathlib import Path

from plantdb.commons.fsdb.path_helpers import MARKER_FILE_NAME
from plantdb.commons.fsdb.path_helpers import TIMELAPSE_MARKER_FILE_NAME
from plantdb.commons.testing import DummyDBTestCase

from plantdb.client.sync import FSDBSync


class TestSyncDummy(DummyDBTestCase):
    def test_sync_local_local(self):
        db = self.get_test_db()
        db._is_dummy = False
        db.disconnect()
        with tempfile.TemporaryDirectory() as tmpdir:
            marker_path = Path(tmpdir) / MARKER_FILE_NAME
            with marker_path.open(mode="x") as _:
                x = FSDBSync(db.path(), tmpdir)
                x.sync()

    def test_sync_timelapse_local(self):
        db = self.get_test_db()
        db.create_timelapse("tl_sync_exp")
        tl_md = {"timelapse": {"id": "tl_sync_exp", "scheduled": "2026-09-03T10:00:00Z", "index": 0}}
        scan = db.create_scan("tl_sync_scan_0", metadata=tl_md)
        _ = scan.create_fileset("images")

        with tempfile.TemporaryDirectory() as tmpdir:
            marker_path = Path(tmpdir) / MARKER_FILE_NAME
            marker_path.touch()
            sync_runner = FSDBSync(db.path(), tmpdir)
            sync_runner.sync()

            target_db = Path(tmpdir)
            # Verify target directory structure contains nested timelapse scan
            self.assertTrue((target_db / "tl_sync_exp" / TIMELAPSE_MARKER_FILE_NAME).is_file())
            self.assertTrue((target_db / "tl_sync_exp" / "tl_sync_scan_0" / "files.json").is_file())


if __name__ == "__main__":
    unittest.main()
