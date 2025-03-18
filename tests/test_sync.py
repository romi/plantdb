#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import tempfile
import unittest
from pathlib import Path

from plantdb.server.fsdb import MARKER_FILE_NAME
from plantdb.server.sync import FSDBSync
from plantdb.server.testing import DummyDBTestCase


class TestSyncDummy(DummyDBTestCase):
    def test_sync_local_local(self):
        db = self.get_test_db()
        db.dummy = False
        db.disconnect()
        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = Path(tmpdir) / MARKER_FILE_NAME
            with lock_path.open(mode="x") as _:
                x = FSDBSync(db.path(), tmpdir)
                x.sync()
    # How to test remote sync?


if __name__ == "__main__":
    unittest.main()
