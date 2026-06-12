# REST API

Welcome to the PlantDB REST API!

This interface provides programmatic access to plant scan datasets, images, point clouds, meshes, and related metadata.
Use the API to list scans, retrieve files, and manage data directly from your applications.

- **Quick start**: Run `python fsdb_rest_api.py --test` to launch a temporary server.
- **Core endpoints**: `/scans`, `/scan/<id>`, `/image/...`, `/pointcloud/...`, `/mesh/...`.
- **Management**:
    - Register users via `/register`
    - Obtain JWT tokens via `/login`
    - Create new scans via `/api/scan`

## Table of Contents

- [How to Run the REST API](rest_api_usage.md): for detailed usage instructions
- [REST API Endpoints](rest_api_endpoints.md): for a full endpoint reference
