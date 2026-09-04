#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import tempfile
import unittest
from pathlib import Path

from plantdb.commons.fsdb.core import MARKER_FILE_NAME
from plantdb.client.sync import FSDBSync
from plantdb.commons.testing import DummyDBTestCase


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
        scan = db.create_scan("tl_sync_scan_0", metadata={"timelapse": {"id": "tl_sync_exp", "scheduled": "2026-09-03T10:00:00Z", "index": 0}})
        _ = scan.create_fileset("images")

        with tempfile.TemporaryDirectory() as tmpdir:
            marker_path = Path(tmpdir) / MARKER_FILE_NAME
            marker_path.touch()
            sync_runner = FSDBSync(db.path(), tmpdir)
            sync_runner.sync()

            target_db = Path(tmpdir)
            # Verify target directory structure contains nested timelapse scan
            self.assertTrue((target_db / "tl_sync_exp" / "timelapse.json").is_file())
            self.assertTrue((target_db / "tl_sync_exp" / "tl_sync_scan_0" / "files.json").is_file())


if __name__ == "__main__":
    unittest.main()
