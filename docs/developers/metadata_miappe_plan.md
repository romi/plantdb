# Plan: MIAPPE-aligned biological metadata for `fsdb.Scan`

Status: proposed · Date: 2026-09-04

## Decisions (locked in)

- **Approach:** Full MIAPPE/ISA tree restructure of `fsdb.Scan` metadata.
- **Semantic authority:** MIAPPE (plant-phenotyping specific).
- **Values:** Plain values for now — no ontology annotations, no units yet.
- **Compatibility:** **Hard break** — new canonical schema, one-time migration of
  existing databases, update producer + consumers, drop old-key aliasing.

## 1. Current problem

Biological metadata is a flat, string-only dict `Metadata.object` with no role
separation, no units, no hierarchy, no schema/validation. It is duplicated into
`metadata/images.json`. Several fields are redundant
(`dataset_id`, `plant_id`, `experiment_id` all ≈ `romi_demo_*`).

Current producer sources:

- `plant-imager/src/plantimager/webui/assets/hardware_scan_rx0.toml`
  (`[Scan.metadata.object]`, `[Scan.metadata.hardware]`, `[Scan.metadata.workspace]`)
- `Plant-Imager3/src/webui/plantimager/webui/assets/config_scan.toml`

Current consumer in plantdb:

- `get_scan_info` — `src/server/plantdb/server/services/scan.py:124-135`
  reads `Metadata.object → species / growth_environment / plant_id`.
- `_load_scan_metadata` — `src/commons/plantdb/commons/fsdb/metadata.py:152-159`
  has a 2026 migration FIXME that pulls `object / hardware / acquisition_date`
  from `images.json` for pre-2026 scans.

## 2. Proposed canonical tree (new `metadata/metadata.json`)

MIAPPE sections become top-level blocks. plantdb-managed keys
(`owner`, `created`, `created_by`, `last_modified`, `timelapse`) stay unchanged.
ROMI acquisition config lives in its own `acquisition` block (MIAPPE has no
assay level, so hardware/camera stay at scan level).

```jsonc
{
  "owner": "admin", "created": "…", "created_by": "…", "last_modified": "…",
  "timelapse": {"id": "…"},

  "investigation": {                      // was dataset_id
    "identifier": "romi_demo_1",
    "title": null,
    "description": null
  },

  "study": {                              // was experiment_id
    "identifier": "romi_demo_2020",
    "title": null,
    "startDate": null, "endDate": null,
    "growthFacility": {"name": "Lyon-Indoor", "country": null},   // was growth_environment
    "environment": {"photoperiod": "LD+SD"},                       // was growth_conditions
    "experimentalDesign": {"type": null},
    "experimentalFactors": {"treatment": "None"}                   // was treatment
  },

  "biologicalMaterial": {                 // this scanned plant = MIAPPE Observation Unit
    "biologicalMaterialId": "romi_demo_1",                          // was plant_id
    "organism": {"genus": "Arabidopsis", "species": "Arabidopsis thaliana"},  // was species
    "materialSource": {"id": null, "name": "Col-0"},                // was seed_stock
    "ageDays": 40,                                                  // was DAG
    "sample": "main stem"                                           // was sample
  },

  "observedVariable": {"trait": "plant_3d_structure", "method": "photometric_scanning", "scale": null},

  "acquisition": {                        // ROMI-specific, non-biological
    "scanPath": {"className": "Circle", "parameters": {"centerX":375,"centerY":375,"radius":350,"nPoints":36,"tilt":0,"z":0}},   // was ScanPath
    "hardware": {"frame":"30profile v3","xMotor":{"type":"NEMA23","model":"X-Carve NEMA23"},"yMotor":{…},"zMotor":null,"panMotor":{…},"tiltMotor":null,"sensors":[{"id":"sensor_1","type":"PiCamera","model":"PiCamera HQ","lens":"Official 6mm","resolution":{"x":2000,"y":1500},"encoding":"jpeg"}]},  // was Metadata.hardware
    "camera": [ {"id":"picamera","stageOffset":{…},"encoding":"jpeg","resX":2000,"resY":1500}, {"id":"picamera2",…} ]   // was picamera, picamera2
  }
}
```

### Mapping table (old → new)

| Old | New | MIAPPE ref |
|---|---|---|
| `object.dataset_id` | `investigation.identifier` | Investigation |
| `object.experiment_id` | `study.identifier` | Study ID |
| `object.growth_environment` | `study.growthFacility.name` | Study site |
| `object.growth_conditions` | `study.environment.photoperiod` | Environment param |
| `object.treatment` | `study.experimentalFactors.treatment` | Experimental factor |
| `object.species` | `biologicalMaterial.organism.{genus,species}` | Organism/Species |
| `object.seed_stock` | `biologicalMaterial.materialSource.name` | Material source |
| `object.plant_id` | `biologicalMaterial.biologicalMaterialId` | Biological material ID |
| `object.DAG` | `biologicalMaterial.ageDays` (int) | development stage/event |
| `object.sample` | `biologicalMaterial.sample` | Sample/anatomical entity |
| `Metadata.hardware.*` | `acquisition.hardware.*` | (instrumentation) |
| `ScanPath` | `acquisition.scanPath` | — |
| `picamera`, `picamera2` | `acquisition.camera[]` | — |

## 3. Code changes — plantdb (`/home/jonathan/Projects/plantdb`)

1. **Schema** — add a JSON-Schema (or lightweight validator) for the biological
   block, e.g. `src/commons/plantdb/commons/fsdb/metadata_schema.py`
   (or a `.json` asset). `required` = `investigation`, `study`, `biologicalMaterial`.
2. **Validation hook** — validate `metadata` in `FSDB.create_scan`
   (core.py:997) and `Scan.set_metadata` (core.py:3162) via
   `MetadataManager._update_metadata` / `_prepare_new_metadata`, so invalid
   biological metadata is rejected at the write boundary, once, in the shared
   path.
3. **Migration** — one-off script/CLI (e.g.
   `src/commons/plantdb/commons/cli/fsdb_migrate_metadata.py`, or extend
   `fsdb_import`) that reads old `Metadata.{object,hardware}` + `ScanPath` /
   `picamera*`, writes the new tree into both `metadata/metadata.json` **and**
   `metadata/images.json`, and records `schemaVersion`.
4. **Consumer update** — `get_scan_info`
   (`src/server/plantdb/server/services/scan.py:124-135`) reads the new paths:
   `study.growthFacility.name`, `biologicalMaterial.organism.species`,
   `biologicalMaterial.biologicalMaterialId`. Remove the old `Metadata.object`
   branch.
5. **Legacy read** — update the `_load_scan_metadata` FIXME
   (`metadata.py:152-159`) to no longer special-case `object / hardware /
   acquisition_date` from `images.json` (post-migration these live in the scan
   tree).

## 4. Code changes — producer (align with hard break)

- `plant-imager/src/plantimager/webui/assets/hardware_scan_rx0.toml` and
  `Plant-Imager3/src/webui/plantimager/webui/assets/config_scan.toml`: rewrite
  `[Scan.metadata.object]` / `[Scan.metadata.hardware]` (and camera blocks) to
  emit the new `investigation / study / biologicalMaterial / acquisition`
  structure.
- Confirm the code that serializes `Scan.metadata.*` into the scan metadata JSON
  writes the new nested structure (check
  `plant-imager/src/plantimager/tasks/scan.py`).

## 5. Tests

- New `test_metadata_schema.py`: valid tree passes; missing
  `biologicalMaterial` / wrong types fail.
- Update `test_scan_api.py` (`test_scan_create_metadata`,
  `test_scan_metadata_post` / `test_scan_metadata_get`) and `test_fsdb.py`
  (`test_set_scan_metadata`) to the new tree.
- Migration round-trip test: old `Metadata.object` → new tree → identical field
  values.
- Update `test_scan_services` / `get_scan_info` fixtures to new paths.

## 6. Notes / caveats

- The existing `MIAPPE-aligned redesign of the JSON metadata.md` in
  Plant-Imager3 is a good starting reference, but its **referenced MIAPPE
  JSON-schemas do not exist** (`w3id.org/miappe/*_schema.json`; the MIAPPE repo
  ships only TSV/XLSX checklists). We define our own schema and reference MIAPPE
  by *concept/section*, not by a live schema URL. Optionally later emit real
  ISA-JSON / MIAPPE export.
- `investigation` is identical across scans of a study/timelapse — it could
  later be hoisted to the `TimeLapse` level; out of scope now.
- Ontology / units deliberately deferred; the tree is designed so
  `organism.species` → `{taxonID, name}` later.
