#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Tests for the MIAPPE-aligned scan metadata schema and the legacy migration."""
import unittest

from plantdb.commons.cli.fsdb_migrate_metadata import build_miappe_tree
from plantdb.commons.cli.fsdb_migrate_metadata import migrate_metadata
from plantdb.commons.cli.fsdb_clean_images_metadata import _clean_images_metadata
from plantdb.commons.fsdb.metadata_schema import validate_biological_metadata


VALID_METADATA = {
    "investigation": {"identifier": "romi_demo_1", "title": None, "description": None},
    "study": {
        "identifier": "romi_demo_2020",
        "growthFacility": {"name": "Lyon-Indoor", "country": None},
        "environment": {"photoperiod": "LD+SD"},
        "experimentalFactors": {"treatment": "None"},
    },
    "biologicalMaterial": {
        "biologicalMaterialId": "romi_demo_1",
        "organism": {"genus": "Arabidopsis", "species": "Arabidopsis thaliana"},
        "materialSource": {"name": "Col-0"},
        "ageDays": 40,
        "sample": "main stem",
    },
}


class TestValidateBiologicalMetadata(unittest.TestCase):
    def test_valid_metadata_passes(self):
        validate_biological_metadata(VALID_METADATA)

    def test_absent_biological_block_is_valid(self):
        validate_biological_metadata({"owner": "admin"})

    def test_empty_metadata_is_valid(self):
        validate_biological_metadata({})

    def test_wrong_scalar_type_fails(self):
        md = {"biologicalMaterial": {"ageDays": "forty"}}
        with self.assertRaises(ValueError):
            validate_biological_metadata(md)

    def test_non_object_section_fails(self):
        with self.assertRaises(ValueError):
            validate_biological_metadata({"study": ["not", "an", "object"]})

    def test_nested_object_wrong_type_fails(self):
        md = {"biologicalMaterial": {"organism": {"species": 123}}}
        with self.assertRaises(ValueError):
            validate_biological_metadata(md)

    def test_none_values_allowed(self):
        validate_biological_metadata({"biologicalMaterial": {"ageDays": None}})


class TestMetadataMigration(unittest.TestCase):
    def test_build_miappe_tree_mapping(self):
        obj = {
            "dataset_id": "ds1",
            "experiment_id": "exp1",
            "growth_environment": "Lyon-Indoor",
            "growth_conditions": "LD+SD",
            "treatment": "None",
            "species": "Arabidopsis thaliana",
            "seed_stock": "Col-0",
            "plant_id": "plantA",
            "DAG": "42",
            "sample": "main stem",
        }
        tree = build_miappe_tree(obj)
        self.assertEqual(tree["investigation"]["identifier"], "ds1")
        self.assertEqual(tree["study"]["identifier"], "exp1")
        self.assertEqual(tree["study"]["growthFacility"]["name"], "Lyon-Indoor")
        self.assertEqual(tree["study"]["environment"]["photoperiod"], "LD+SD")
        self.assertEqual(tree["study"]["experimentalFactors"]["treatment"], "None")
        self.assertEqual(tree["biologicalMaterial"]["biologicalMaterialId"], "plantA")
        self.assertEqual(tree["biologicalMaterial"]["organism"]["species"], "Arabidopsis thaliana")
        self.assertEqual(tree["biologicalMaterial"]["organism"]["genus"], "Arabidopsis")
        self.assertEqual(tree["biologicalMaterial"]["materialSource"]["name"], "Col-0")
        self.assertEqual(tree["biologicalMaterial"]["ageDays"], 42)
        self.assertEqual(tree["biologicalMaterial"]["sample"], "main stem")

    def test_build_miappe_tree_reads_v2_environment(self):
        tree = build_miappe_tree({"environment": "Lyon indoor"})
        self.assertEqual(tree["study"]["growthFacility"]["name"], "Lyon indoor")

    def test_migrate_nested_metadata_block(self):
        old = {"Metadata": {"object": {"species": "Arabidopsis thaliana"}}, "owner": "admin"}
        new, migrated = migrate_metadata(old)
        self.assertTrue(migrated)
        self.assertNotIn("Metadata", new)
        self.assertEqual(new["biologicalMaterial"]["organism"]["species"], "Arabidopsis thaliana")

    def test_migrate_flat_object_block(self):
        old = {"object": {"species": "Arabidopsis thaliana"}}
        new, migrated = migrate_metadata(old)
        self.assertTrue(migrated)
        self.assertNotIn("object", new)
        self.assertEqual(new["biologicalMaterial"]["organism"]["species"], "Arabidopsis thaliana")

    def test_migrate_idempotent(self):
        old = {"object": {"species": "Arabidopsis thaliana"}}
        new, migrated = migrate_metadata(old)
        self.assertTrue(migrated)
        new2, migrated2 = migrate_metadata(new)
        self.assertFalse(migrated2)
        self.assertIs(new2, new)

    def test_migrate_does_not_touch_migrated_metadata(self):
        new, migrated = migrate_metadata(dict(VALID_METADATA))
        self.assertFalse(migrated)
        self.assertEqual(new["biologicalMaterial"]["organism"]["species"], "Arabidopsis thaliana")


class TestCleanImagesMetadata(unittest.TestCase):
    def test_removes_legacy_top_level_object_and_hardware(self):
        images = {"channels": ["rgb"], "object": {"species": "x"}, "hardware": {"camera": "pic"}, "task_params": {}}
        self.assertTrue(_clean_images_metadata(images))
        self.assertEqual(list(images.keys()), ["channels", "task_params"])

    def test_removes_new_metadata_entry_wrapping_hardware_object(self):
        images = {"Metadata": {"hardware": {"cam": "x"}, "object": {"species": "y"}}, "picamera": {"res_x": 2000}}
        self.assertTrue(_clean_images_metadata(images))
        self.assertEqual(list(images.keys()), ["picamera"])

    def test_keeps_unrelated_metadata_entry(self):
        images = {"Metadata": {"other": 1}, "task_params": {}}
        self.assertFalse(_clean_images_metadata(images))
        self.assertIn("Metadata", images)

    def test_nothing_to_clean(self):
        images = {"channels": ["rgb"], "task_params": {}}
        self.assertFalse(_clean_images_metadata(images))
        self.assertEqual(list(images.keys()), ["channels", "task_params"])


if __name__ == '__main__':
    unittest.main()
