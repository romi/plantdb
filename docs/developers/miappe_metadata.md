# MIAPPE-aligned scan metadata

This document describes the **implemented** metadata management for biological data in `fsdb.Scan` and the MIAPPE details behind it.

## Overview

Biological metadata lives in a scan's `metadata/metadata.json` under four top-level MIAPPE sections:

- `investigation`, 
- `study`, 
- `biologicalMaterial`
- `observedVariable`.

Plantdb-managed keys stay unchanged at the top level:
- `owner`
- `created`
- `created_by`
- `last_modified`
- `timelapse`

The acquisition configuration (hardware, camera, scan path) is ROMI-specific and does not map onto a MIAPPE section, so it is out of scope of the biological schema.

The biological block is **optional**: a scan without it is valid.
But if a section is present, its known fields are type-checked at the write boundary.

### ISA & MIAPPE

The tree follows the **ISA** (Investigation / Study / Assay) framework and its plant-phenotyping specialization **MIAPPE** (Minimum Information about a Plant Phenotyping Experiment).

Each top-level block corresponds to a MIAPPE section:

- **Investigation** — the overall project/dataset.
- **Study** — the experiment (facility, environment, experimental design/factors).
- **Biological material** — the scanned plant, MIAPPE's *observation unit*.
- **Observed variable** — the trait measured and the method used.

## Canonical tree

```jsonc
{
  "owner": "admin", "created": "…", "created_by": "…", "last_modified": "…",
  "timelapse": {"id": "…"},

  "investigation": {
    "identifier": "romi_demo_1",
    "title": null,
    "description": null
  },

  "study": {
    "identifier": "romi_demo_2020",
    "title": null,
    "startDate": null, "endDate": null,
    "growthFacility": {"name": "Lyon-Indoor", "country": null},
    "environment": {"photoperiod": "LD+SD"},
    "experimentalDesign": {"type": null},
    "experimentalFactors": {"treatment": "None"}
  },

  "biologicalMaterial": {
    "biologicalMaterialId": "romi_demo_1",
    "organism": {"genus": "Arabidopsis", "species": "Arabidopsis thaliana"},
    "materialSource": {"id": null, "name": "Col-0"},
    "ageDays": 40,
    "sample": "main stem"
  },

  "observedVariable": {
    "trait": "plant_3d_structure",
    "method": "photometric_scanning",
    "scale": null
  }
}
```

### Field details

| Section / field                           | Type         | MIAPPE codename               | Notes                                     |
|-------------------------------------------|--------------|-------------------------------|-------------------------------------------|
| `investigation.identifier`                | str          | investigationId               | Unique identifier of the investigation.   |
| `investigation.title`                     | str          | investigationTitle            | Human-readable title.                     |
| `investigation.description`               | str          | investigationDescription      | Longer description.                       |
| `study.identifier`                        | str          | studyId                       | Unique identifier of the study.           |
| `study.title`                             | str          | studyTitle                    | Human-readable title.                     |
| `study.startDate` / `study.endDate`       | str          | studyStartDate / studyEndDate | ISO 8601.                                 |
| `study.growthFacility.name`               | str          | siteName                      | Facility where the experiment took place. |
| `study.growthFacility.country`            | str          | siteName                      | Extension: ISO country code.              |
| `study.environment.photoperiod`           | str          | envParam                      | Environment parameter.                    |
| `study.experimentalDesign.type`           | str          | expeDesignType                | Experimental design.                      |
| `study.experimentalFactors.treatment`     | str          | expeFactorType                | Experimental factor.                      |
| `biologicalMaterial.biologicalMaterialId` | str          | biologicalMaterialId          | The scanned plant.                        |
| `biologicalMaterial.organism.genus`       | str          | genus                         | Genus name.                               |
| `biologicalMaterial.organism.species`     | str          | species                       | Specific epithet.                         |
| `biologicalMaterial.materialSource.id`    | str          | materialSourceId              | Source identifier.                        |
| `biologicalMaterial.materialSource.name`  | str          | materialSourceAccName         | e.g. seed stock accession.                |
| `biologicalMaterial.ageDays`              | int \| float | —                             | Age in days.                              |
| `biologicalMaterial.sample`               | str          | sampleId                      | Anatomical entity measured.               |
| `observedVariable.trait`                  | str          | traitName                     | Trait under observation.                  |
| `observedVariable.method`                 | str          | methodName                    | Observation method.                       |
| `observedVariable.scale`                  | str          | scaleName                     | Scale of the variable.                    |

`None` values are always allowed for every field.

## Code layout

### Schema & validation

The `metadata_schema.py` file defines the canonical structure (`SCAN_BIOLOGICAL_SCHEMA` and per-section schemas) and a lightweight validator `validate_biological_metadata(metadata)` with **no external dependency**.
It type-checks each present section against its schema and raises `ValueError` on a field of the wrong type.
A scan without any biological block passes validation.

```python
from plantdb.commons.fsdb.metadata_schema import validate_biological_metadata
validate_biological_metadata(metadata)
```

### Write boundary

Validation is wired into both write paths so invalid biological metadata is
rejected once, in the shared code:

- `FSDB.create_scan` (`core.py`) calls `validate_biological_metadata(metadata)` before creating the scan directory.
- `MetadataManager._update_metadata` (in `metadata.py`) validates for `Scan` objects, so `Scan.set_metadata` rejects bad biological blocks at the write boundary.
  `_scrub_forbidden_keys` still protects `owner`, `sharing` and the immutable `timelapse.id`.

### Consumers — `plantdb.server.services.scan.get_scan_info`

`get_scan_info` reads the MIAPPE paths directly from the scan metadata:

- `biologicalMaterial.organism.species` → `metadata.species`
- `biologicalMaterial.biologicalMaterialId` → `metadata.plant`
- `study.growthFacility.name` → `metadata.environment`

### Editing UI — `plantdb.client.metadata_app`

The Dash web app (`metadata_app`) views, edits and bulk-fills the MIAPPE tree.

- `field_spec.py` declares every editable leaf field (`FIELD_SPECS`), its type, and a tooltip sourced from the MIAPPE checklist data model.
  It flattens a nested dict to `{dot.path: value}` and rebuilds it (`flatten` / `unflatten`), with type coercion (`coerce`).
- `db_ops.py` reads/writes `metadata/metadata.json` with a `.bak` backup and schema validation (`write_scan_metadata`), and provides bulk editing   (`apply_bulk`), plus scan detection and migration helpers (`scan_needs_migration`, `migratable_scans`, `migrate_scans_progress`).

## Migration from the legacy schema

Scans created before this schema stored biological metadata as a flat string dict, either as a top-level `object` block (v3) or nested as `Metadata.object` (v2), and duplicated it into `metadata/images.json`.

The one-time migration CLI `fsdb_migrate_metadata` converts each scan's `metadata/metadata.json` `object` block into the canonical MIAPPE tree and rewrites the file.
It is idempotent: scans already migrated (no legacy `object` block) are left untouched.

```shell
fsdb_migrate_metadata /romi_db
```

The companion `fsdb_clean_images_metadata` CLI removes the now-redundant biological / hardware entries from each scan's `metadata/images.json` (top-level `object`/`hardware` for older producers, a `Metadata` entry for Plant-Imager3).
It runs dry by default (`--dry-run`) and writes `.bak` copies.

```shell
fsdb_clean_images_metadata /romi_db --dry-run
fsdb_clean_images_metadata /romi_db
```

The metadata editor also flags legacy scans and offers in-UI migration.

### Mapping table (old → new)

| Old                                                   | New                                           | MIAPPE ref               |
|-------------------------------------------------------|-----------------------------------------------|--------------------------|
| `object.dataset_id`                                   | `investigation.identifier`                    | Investigation            |
| `object.experiment_id`                                | `study.identifier`                            | Study ID                 |
| `object.growth_environment` (or `object.environment`) | `study.growthFacility.name`                   | Study site               |
| `object.growth_conditions`                            | `study.environment.photoperiod`               | Environment param        |
| `object.treatment`                                    | `study.experimentalFactors.treatment`         | Experimental factor      |
| `object.species`                                      | `biologicalMaterial.organism.{genus,species}` | Organism/Species         |
| `object.seed_stock`                                   | `biologicalMaterial.materialSource.name`      | Material source          |
| `object.plant_id` (or `object_id`)                    | `biologicalMaterial.biologicalMaterialId`     | Biological material ID   |
| `object.DAG`                                          | `biologicalMaterial.ageDays` (int)            | development stage/event  |
| `object.sample`                                       | `biologicalMaterial.sample`                   | Sample/anatomical entity |

## Tests

- `test_metadata_schema.py` covers valid trees, absent/empty blocks, wrong scalar and nested types, `None` values, the `build_miappe_tree` mapping, and migration idempotence (`migrate_metadata`, `_clean_images_metadata`).
- `test_scan_api.py` creates, reads and updates scans with MIAPPE-aligned metadata through the API and asserts the biological block round-trips.
- `test_fsdb.py` exercises `Scan.set_metadata` against the new tree.

## Notes / caveats

- The MIAPPE repo ships only TSV/XLSX checklists; the referenced `w3id.org/miappe/*_schema.json` schemas do not exist.
- Plantdb defines its own schema and references MIAPPE by *concept/section*, not by a live schema URL.
- `investigation` is identical across scans of a study/timelapse; it could later be hoisted to the `TimeLapse` level.
- Ontology / units are deliberately deferred; the tree is designed so `organism.species` → `{taxonID, name}` later.
- Values are plain for now.
