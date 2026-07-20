# REST API Endpoints

This document describes the REST API endpoints for the PlantDB (Plant Database) system, which provides access to plant scan datasets, images, point clouds, meshes, and related data through RESTful endpoints.

## Overview

The PlantDB REST API is implemented using Flask and Flask-RESTful, providing a comprehensive interface for managing and accessing plant-related datasets. The Python implementation is located in the
`plantdb.server.rest_api` module, and the CLI server is started using the `fsdb_rest_api` CLI.

**Base URL**: `http://127.0.0.1:5000` (default, configurable)
**API URL prefix**: `/api/v1` (default, configurable)

## Table of Contents

1. [Getting Started](#getting-started)
2. [Health and Status](#health-and-status)
3. [Database Management](#database-management)
4. [Authentication](#authentication)
5. [Scans](#scans)
6. [Assets](#assets)

---

## Getting Started

### Starting the Server

To run the server with a temporary test database:
```bash
fsdb_rest_api --test
```

For more explanations, have a look at [how to run the REST API](rest_api_usage.md).

### Authentication

Some endpoints require authentication.
Use the `/auth/register`, `/auth/login` endpoints to obtain authentication tokens.

---

## Health and Status

### GET `/health`

**REST API status**

- **Description**: Check API server health and database connectivity
- **Response**: JSON with health status information
- **Example**: `GET /health`

---

## Database Management

### GET `/refresh/`

**Reload the whole database**

- **Description**: Refresh the list of scans in the database
- **Response**: HTTP 200 on completion
- **Example**: `GET /refresh/`

### GET `/refresh/{scan_id}`

**Reload the scan**

- **Description**: Refresh a specific scan in the database
- **Path Parameters**:
    - `scan_id`: Unique identifier of the scan
- **Response**: HTTP 200 on completion
- **Example**: `GET /refresh/real_plant_analyzed`

---

## Authentication

### POST `/auth/login`

**User login**

- **Description**: Authenticate user and return authentication token
- **Request Body**: JSON with username and password
- **Response**: Authentication token

### POST `/auth/logout`

**User logout**

- **Description**: Invalidate user authentication token
- **Response**: Success message

### POST `/auth/register`

**New user registration**

- **Description**: Register a new user account
- **Request Body**: JSON with user details
- **Response**: User registration confirmation

### GET `/auth/tokens/refresh`

**Retrieve a specific scan**

- **Description**: Refresh authentication token
- **Response**: New authentication token

### GET `/auth/tokens/validation`

**Retrieve a specific scan**

- **Description**: Validate authentication token
- **Response**: Token validation status

### POST `/auth/tokens/create-api-token`

**Create API token**

- **Description**: Create a new API token for programmatic access
- **Request Body**: JSON with token details
- **Response**: Created API token

---

## Scans

### GET `/scans/`

**List scans**

- **Description**: Retrieve a list of all available scan datasets
- **Query Parameters**:
    - `filterQuery` (optional): Filter scans based on metadata content
- **Response**: JSON array of scan summaries

### GET `/scans/info`

**List scans**

- **Description**: Get comprehensive scan information in tabular format
- **Query Parameters**:
    - `filterQuery` (optional): Filter scans
- **Response**: JSON object with detailed scan metadata, file counts, and task status

### GET `/scans/{scan_id}/`

**Retrieve a specific scan**

- **Description**: Get detailed information about a specific scan dataset
- **Path Parameters**:
    - `scan_id`: Unique identifier of the scan
- **Response**: JSON object with comprehensive scan details including camera parameters, poses, and file URIs

### POST `/scans/{scan_id}/`

**Create a new scan**

- **Description**: Create a new scan dataset
- **Path Parameters**:
    - `scan_id`: Unique identifier of the scan
- **Request Body**: JSON with scan metadata
- **Response**: Scan creation confirmation

### GET `/scans/{scan_id}/metadata/`

**Get scan metadata**

- **Description**: Retrieve metadata for a specific scan
- **Path Parameters**:
    - `scan_id`: Unique identifier of the scan
- **Response**: JSON with scan metadata

### POST `/scans/{scan_id}/metadata/`

**Update scan metadata**

- **Description**: Update metadata for a specific scan
- **Path Parameters**:
    - `scan_id`: Unique identifier of the scan
- **Request Body**: JSON with updated metadata
- **Response**: Metadata update confirmation

### GET `/scans/{scan_id}/filesets`

**List filesets for scan**

- **Description**: List all filesets within a scan
- **Path Parameters**:
    - `scan_id`: Unique identifier of the scan
- **Response**: JSON array of fileset identifiers

---

## Filesets

### POST `/filesets/{scan_id}/{fileset_id}/`

**Create a new fileset**

- **Description**: Create a new fileset within a scan
- **Path Parameters**:
    - `scan_id`: Unique identifier of the scan
    - `fileset_id`: Unique identifier of the fileset
- **Request Body**: JSON with fileset information
- **Response**: Fileset creation confirmation

### GET `/filesets/{scan_id}/{fileset_id}/metadata/`

**Get fileset metadata**

- **Description**: Retrieve metadata for a specific fileset
- **Path Parameters**:
    - `scan_id`: Unique identifier of the scan
    - `fileset_id`: Unique identifier of the fileset
- **Response**: JSON with fileset metadata

### POST `/filesets/{scan_id}/{fileset_id}/metadata/`

**Update fileset metadata**

- **Description**: Update metadata for a specific fileset
- **Path Parameters**:
    - `scan_id`: Unique identifier of the scan
    - `fileset_id`: Unique identifier of the fileset
- **Request Body**: JSON with updated metadata
- **Response**: Metadata update confirmation

### GET `/filesets/{scan_id}/{fileset_id}/files`

**List files**

- **Description**: List all files within a fileset
- **Path Parameters**:
    - `scan_id`: Unique identifier of the scan
    - `fileset_id`: Unique identifier of the fileset
- **Response**: JSON array of file identifiers

---

## Files

### GET `/files/{scan_id}/{fileset_id}/{file_id}`

**Retrieve file**

- **Description**: Get details about a specific file
- **Path Parameters**:
    - `scan_id`: Unique identifier of the scan
    - `fileset_id`: Unique identifier of the fileset
    - `file_id`: Unique identifier of the file
- **Response**: JSON with file details

### POST `/files/{scan_id}/{fileset_id}/{file_id}`

**Create a new file**

- **Description**: Add a new file to a fileset
- **Path Parameters**:
    - `scan_id`: Unique identifier of the scan
    - `fileset_id`: Unique identifier of the fileset
    - `file_id`: Unique identifier of the file
- **Request Body**: File data and metadata
- **Response**: File creation confirmation

### GET `/files/{scan_id}/{fileset_id}/{file_id}/metadata/`

**Get file metadata**

- **Description**: Retrieve metadata for a specific file
- **Path Parameters**:
    - `scan_id`: Unique identifier of the scan
    - `fileset_id`: Unique identifier of the fileset
    - `file_id`: Unique identifier of the file
- **Response**: JSON with file metadata

### POST `/files/{scan_id}/{fileset_id}/{file_id}/metadata/`

**Update file metadata**

- **Description**: Update metadata for a specific file
- **Path Parameters**:
    - `scan_id`: Unique identifier of the scan
    - `fileset_id`: Unique identifier of the fileset
    - `file_id`: Unique identifier of the file
- **Request Body**: JSON with updated metadata
- **Response**: Metadata update confirmation

---

## Assets

### GET `/assets/files/{file_path}`

**Retrieve a specific scan**

- **Description**: Retrieve any file from the database using its path
- **Path Parameters**:
    - `file_path`: Full path to the file within the database
- **Response**: File content (binary or text)

### GET `/assets/archive/{scan_id}`

**Retrieve scan archive**

- **Description**: Download a complete dataset as a ZIP file
- **Path Parameters**:
    - `scan_id`: Scan identifier to archive
- **Response**: ZIP file containing the entire dataset

### POST `/assets/archive/{scan_id}`

**Create a new scan by uploading a scan archive**

- **Description**: Upload a ZIP file to create a new scan dataset
- **Path Parameters**:
    - `scan_id`: Target scan identifier
- **Form Data**:
    - `file_upload`: The ZIP file to upload
- **Response**: Scan creation confirmation

### GET `/assets/image/{scan_id}/{fileset_id}/{file_id}`

**Get image**

- **Description**: Retrieve an image file
- **Path Parameters**:
    - `scan_id`: Scan identifier
    - `fileset_id`: Fileset identifier (usually 'images')
    - `file_id`: Image file identifier (without extension)
- **Response**: Image file (JPEG/PNG)

### POST `/assets/image/{scan_id}/{fileset_id}/{file_id}`

**Create a new image**

- **Description**: Upload an image file to a specific scan dataset
- **Path Parameters**:
    - `scan_id`: Scan identifier
    - `fileset_id`: Fileset identifier (usually 'images')
    - `file_id`: Image file identifier (without extension)
- **Form Data**:
    - `file_upload`: The image file to upload
- **Response**: Image creation confirmation

### GET `/assets/pointcloud/{scan_id}`

**Retrieve scan pointcloud**

- **Description**: Retrieve point cloud files for a scan
- **Path Parameters**:
    - `scan_id`: Scan identifier
- **Response**: Point cloud file (PLY format)

### GET `/assets/mesh/{scan_id}`

**Retrieve scan triangular mesh**

- **Description**: Retrieve 3D mesh files for a scan
- **Path Parameters**:
    - `scan_id`: Scan identifier
- **Response**: Mesh file (PLY format)

### GET `/assets/sequence/{scan_id}`

**Retrieve scan sequence**

- **Description**: Retrieve sequence-related information for a scan
- **Path Parameters**:
    - `scan_id`: Scan identifier
- **Response**: JSON with sequence data

### GET `/assets/skeleton/{scan_id}`

**Retrieve scan skeleton**

- **Description**: Retrieve curve skeleton information for a scan
- **Path Parameters**:
    - `scan_id`: Scan identifier
- **Response**: JSON with curve skeleton data