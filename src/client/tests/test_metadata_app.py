#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import tempfile
import unittest
import json
from pathlib import Path

from plantdb.commons.fsdb.core import FSDB
from plantdb.commons.fsdb.core import MARKER_FILE_NAME
from plantdb.commons.fsdb.core import NoAuthSessionManager
from plantdb.commons.fsdb.metadata_schema import SCAN_BIOLOGICAL_SCHEMA

from plantdb.client.metadata_app import db_ops
from plantdb.client.metadata_app.field_spec import FIELD_BY_PATH
from plantdb.client.metadata_app.field_spec import FIELD_SPECS
from plantdb.client.metadata_app.field_spec import coerce
from plantdb.client.metadata_app.field_spec import collect_suggestions
from plantdb.client.metadata_app.field_spec import flatten
from plantdb.client.metadata_app.field_spec import unflatten


def _mk_db(scans=None):
    """Create a temporary FSDB with the given ``{id: metadata}`` scans."""
    tmp = Path(tempfile.mkdtemp())
    (tmp / MARKER_FILE_NAME).touch()
    db = FSDB(tmp, session_manager=NoAuthSessionManager())
    db.connect()
    for sid, metadata in (scans or {}).items():
        db.create_scan(sid, metadata=metadata)
    db.disconnect()
    return tmp


class TestFieldSpec(unittest.TestCase):
    def test_specs_cover_schema(self):
        """Every leaf of the validator schema must have a field spec (no drift)."""
        def leaves(schema, prefix=""):
            for key, rule in schema.items():
                path = f"{prefix}.{key}" if prefix else key
                if isinstance(rule, dict):
                    yield from leaves(rule, path)
                else:
                    yield path
        schema_leaves = set(leaves(SCAN_BIOLOGICAL_SCHEMA))
        spec_paths = set(FIELD_BY_PATH)
        self.assertEqual(schema_leaves, spec_paths)

    def test_flatten_unflatten_roundtrip(self):
        metadata = {
            "investigation": {"identifier": "DS1", "title": None, "description": "x"},
            "study": {"growthFacility": {"name": "Lyon", "country": None}},
            "biologicalMaterial": {"organism": {"genus": "Arabidopsis",
                                                "species": "Arabidopsis thaliana"},
                                   "ageDays": 42},
            "unrelated": "kept",
        }
        flat = flatten(metadata)
        self.assertEqual(flat["biologicalMaterial.organism.species"], "Arabidopsis thaliana")
        self.assertNotIn("unrelated", flat)
        tree = unflatten(flat)
        self.assertEqual(tree["biologicalMaterial"]["organism"]["species"], "Arabidopsis thaliana")
        self.assertEqual(tree["biologicalMaterial"]["ageDays"], 42)

    def test_coerce(self):
        self.assertEqual(coerce("biologicalMaterial.ageDays", "42"), 42)
        self.assertEqual(coerce("study.growthFacility.name", "Lyon"), "Lyon")
        self.assertIsNone(coerce("study.growthFacility.name", ""))

    def test_tooltips_present(self):
        for spec in FIELD_SPECS:
            self.assertTrue(spec["tooltip"]["definition"], spec["path"])

    def test_collect_suggestions(self):
        scans = [
            flatten({"biologicalMaterial": {"organism": {"species": "a"}}}),
            flatten({"biologicalMaterial": {"organism": {"species": "b"}}}),
            flatten({"biologicalMaterial": {"organism": {"species": None}}}),
        ]
        self.assertEqual(collect_suggestions(scans, "biologicalMaterial.organism.species"), ["a", "b"])


class TestDbOps(unittest.TestCase):
    def test_bulk_apply_and_backup(self):
        tmp = _mk_db({
            "scan_a": {"biologicalMaterial": {"organism": {"species": "Arabidopsis thaliana"}, "ageDays": 40}},
            "scan_b": {"biologicalMaterial": {"organism": {"species": "Arabidopsis thaliana"}, "ageDays": 40}},
        })
        modified = db_ops.apply_bulk(tmp, ["scan_a", "scan_b"],
                                     "biologicalMaterial.organism.species", "Solanum lycopersicum")
        self.assertEqual(modified, ["scan_a", "scan_b"])
        _, flat = db_ops.load_db(tmp)
        self.assertEqual(flat["scan_a"]["biologicalMaterial.organism.species"], "Solanum lycopersicum")
        # backup written
        self.assertTrue((tmp / "scan_a" / "metadata" / "metadata.json.bak").is_file())
        # idempotent: nothing modified on second run
        self.assertEqual(db_ops.apply_bulk(tmp, ["scan_a", "scan_b"],
                                           "biologicalMaterial.organism.species",
                                           "Solanum lycopersicum"), [])

    def test_bulk_only_touches_matching(self):
        tmp = _mk_db({
            "scan_a": {"biologicalMaterial": {"biologicalMaterialId": "A"}},
            "scan_b": {"biologicalMaterial": {"biologicalMaterialId": "B"}},
        })
        modified = db_ops.apply_bulk(tmp, ["scan_a"], "biologicalMaterial.organism.species", "X")
        self.assertEqual(modified, ["scan_a"])
        _, flat = db_ops.load_db(tmp)
        self.assertNotIn("biologicalMaterial.organism.species", flat["scan_b"])

    def test_per_scan_update_preserves_non_biological(self):
        tmp = _mk_db({
            "scan_a": {"owner": "admin",
                       "biologicalMaterial": {"biologicalMaterialId": "old"}},
        })
        scan_dir = db_ops.get_scan_dir(tmp, "scan_a")
        metadata = db_ops.read_scan_metadata(scan_dir)
        metadata = db_ops.update_biological(
            metadata, unflatten({"biologicalMaterial.biologicalMaterialId": "new"}))
        db_ops.write_scan_metadata(scan_dir, metadata)
        saved = db_ops.read_scan_metadata(scan_dir)
        self.assertEqual(saved["biologicalMaterial"]["biologicalMaterialId"], "new")
        self.assertEqual(saved["owner"], "admin")

    def test_write_validates(self):
        tmp = _mk_db({"scan_a": {}})
        scan_dir = db_ops.get_scan_dir(tmp, "scan_a")
        with self.assertRaises(ValueError):
            db_ops.write_scan_metadata(scan_dir, {"biologicalMaterial": {"ageDays": "not-a-number"}})

    def test_loose_directory_without_marker_rejected(self):
        """A directory without the romidb marker is not a proper ROMI DB."""
        tmp = Path(tempfile.mkdtemp())
        md_dir = tmp / "scan_1" / "metadata"
        md_dir.mkdir(parents=True)
        (md_dir / "metadata.json").write_text(json.dumps(
            {"biologicalMaterial": {"organism": {"species": "Arabidopsis thaliana"}}}))
        from plantdb.commons.fsdb.exceptions import NotAnFSDBError
        with self.assertRaises(NotAnFSDBError):
            db_ops.load_db(tmp)


if __name__ == "__main__":
    unittest.main()
