# Changelog

All notable changes to this project are documented in this file.

## Version 0.16.2 - 2026-10-01

### Fixed
- Fix Docker image build by removing inline comments from the metadata-action tags block, which were being merged into the tag value and producing an invalid reference format.
- Fix conda package build by normalizing dependency names from PyPI underscores to conda hyphens (e.g. `click_option_group` → `click-option-group`), which were unresolvable on conda-forge.

## Version 0.16.1 - 2026-10-01

### Fixed
- Fix conda package build by invoking `conda-build` directly, as conda 26+ no longer exposes the `conda build` subcommand.
- Fix Docker image tagging to use the release version and `latest` instead of the git SHA, and remove the inline comment that broke the tag reference format.
- Add the missing `__init__.py` to the `plantdb.client.metadata_app` package so the documentation build can import it.

### Changed
- Document how to create a GitHub release (CLI and web page) to trigger the publishing workflows.

## Version 0.16.0 - 2026-10-01

### Added
- Add timelapse support across the stack: `TimeLapse` API in `commons`, CRUD over the REST API, and sync in the client SDK and filesystem sync.
- Add MIAPPE-aligned biological metadata schema with write-time validation, plus legacy metadata migration and `images.json` cleanup CLIs.
- Add a MIAPPE metadata editor web UI with unified field editing, migration modal, and Help/About modals.
- Add `fsdb_healthcheck` CLI (Click-based) and enforce FSDB validation on connect.
- Add support for Plant Imager v3 metadata format in `scan.py`.
- Add support for a deployment prefix threaded through scan info/data services and client URL builders.
- Add backup and `--no-backup` option to `fsdb_migrate_metadata`.
- Add `diskcache` dependency to cache FSDB connections and stream migration progress.

### Changed
- Centralize REST API endpoint helpers and rename `PLANTDB_API_PREFIX` to `API_PREFIX`.
- Refactor the REST API client package with request helpers and standardize error payloads to use an `error` key.
- Make `Fileset.connect` return `None` instead of `True`; drop deprecated `Series` alias and `get_series` method.
- Switch sync app UI to a Click CLI with a configurable logger.
- Serialize file writes with a fileset-level lock to prevent concurrent-write corruption.
- Make test database downloads resilient to slow networks.

### Fixed
- Fix scan loading to resolve nested timelapse scans by loading metadata before filesets.
- Fix timelapse metadata set via dict assignment.
- Fix metadata loading to use the JSON path.
- Fix CLI logger name and app logger initialization.

### Removed
- Remove deprecated `Series` alias and `get_series` method.
- Remove `required_filesets` handling and enforce FSDB validation on connect.
