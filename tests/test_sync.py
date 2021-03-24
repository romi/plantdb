import os
import tempfile
import unittest

from plantdb.fsdb import MARKER_FILE_NAME
from plantdb.sync import FSDBSync
from plantdb.testing import DBTestCase


class TestSync(DBTestCase):
    def test_sync_local_local(self):
        db = self.get_test_db()
        db.disconnect()
        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = os.path.abspath(os.path.join(tmpdir, MARKER_FILE_NAME))
            with open(lock_path, "x") as _:
                x = FSDBSync(db.basedir, tmpdir)
                x.sync()
    # How to test remote sync?


if __name__ == "__main__":
    unittest.main()
