# REST API

Welcome to the PlantDB REST API!

This interface provides programmatic access to plant scan datasets, images, point clouds, meshes, and related metadata.
Use the API to list scans, retrieve files, and manage data directly from your applications.

- **Quick start**: Run `python fsdb_rest_api.py --test` to launch a temporary server.
- **Core endpoints**: `/scans`, `/scans/<id>`, `/assets/image/...`, `/assets/pointcloud/...`, `/assets/mesh/...`.
- **Management**:
    - Register users via `/auth/register`
    - Obtain JWT tokens via `/auth/login`
    - Create new scans via `/scans/<id>`

## Table of Contents

- [How to Run the REST API](rest_api_usage.md): for detailed usage instructions
- [REST API Endpoints](rest_api_endpoints.md): for a full endpoint reference
