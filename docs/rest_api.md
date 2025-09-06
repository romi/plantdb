# PlantDB REST API Documentation

This document describes the REST API endpoints for the PlantDB (Plant Database) system, which provides access to plant scan datasets, images, point clouds, meshes, and related data through RESTful endpoints.

## Overview

The PlantDB REST API is implemented using Flask and Flask-RESTful, providing a comprehensive interface for managing and accessing plant-related datasets. The Python implementation is located in the `plantdb.server.rest_api` module, and the CLI server is started using `fsdb_rest_api.py`.

**Base URL**: `http://127.0.0.1:5000` (default, configurable)

## Table of Contents

1. [Getting Started](#getting-started)
2. [Core Dataset Endpoints](#core-dataset-endpoints)
3. [File Access Endpoints](#file-access-endpoints)
4. [Specialized Data Endpoints](#specialized-data-endpoints)
5. [Database Management](#database-management)
6. [API Management Endpoints](#api-management-endpoints)
7. [Health and Status](#health-and-status)
8. [Data Structures](#data-structures)
9. [Examples](#examples)

---

## Getting Started

### Starting the Server

```bash
# Start with default settings
python fsdb_rest_api.py --db_location /path/to/database

# Start with custom host and port
python fsdb_rest_api.py --db_location /path/to/database --host 0.0.0.0 --port 8080

# Start in debug mode
python fsdb_rest_api.py --db_location /path/to/database --debug

# Start with test database
python fsdb_rest_api.py --test --debug
```

### Authentication

Some endpoints require authentication. Use the `/register` and `/login` endpoints to obtain authentication tokens.

---

## Core Dataset Endpoints

### GET `/`
**Home endpoint**
- **Description**: Returns basic API information
- **Response**: Welcome message and API status

### GET `/scans`
**List all scans**
- **Description**: Retrieve a list of all available scan datasets
- **Query Parameters**: 
  - `filterQuery` (optional): Filter scans based on metadata content
- **Response**: JSON array of scan summaries
- **Examples**: 
  - All scans: `GET /scans`
  - Filtered: `GET /scans?filterQuery=arabidopsis`

### GET `/scans_info`
**Scan information table**
- **Description**: Get comprehensive scan information in tabular format
- **Query Parameters**: 
  - `filterQuery` (optional): Filter scans
- **Response**: JSON object with detailed scan metadata, file counts, and task status
- **Example**: `GET /scans_info`

### GET `/scans/<scan_id>`
**Individual scan details**
- **Description**: Get detailed information about a specific scan dataset
- **Path Parameters**: 
  - `scan_id`: Unique identifier of the scan
- **Response**: JSON object with comprehensive scan details including camera parameters, poses, and file URIs
- **Examples**:
  - `GET /scans/real_plant_analyzed`
  - `GET /scans/virtual_plant_analyzed`

!!! warning "Dependency"
    This endpoint requires the `Colmap` task to be completed. Returns HTTP 500 if missing.

---

## File Access Endpoints

### GET `/files/<path>`
**Direct file access**
- **Description**: Retrieve any file from the database using its path
- **Path Parameters**: 
  - `path`: Full path to the file within the database
- **Response**: File content (binary or text)
- **Examples**:
  - Image: `GET /files/real_plant_analyzed/images/00000_rgb.jpg`
  - Metadata: `GET /files/real_plant_analyzed/metadata/metadata.json`

### POST `/files/<scan_id>`
**Upload file to dataset**
- **Description**: Upload a new file to a specific scan dataset
- **Path Parameters**: 
  - `scan_id`: Target scan identifier
- **Form Data**: 
  - `file_upload`: The file to upload
- **Response**: Upload confirmation
- **Example**:
  ```bash
  curl -X POST http://127.0.0.1:5000/files/my_scan -F "file_upload=@example_file.txt"
  ```

### GET `/archive/<scan_id>`
**Download dataset archive**
- **Description**: Download a complete dataset as a ZIP file
- **Path Parameters**: 
  - `scan_id`: Scan identifier to archive
- **Response**: ZIP file containing the entire dataset
- **Examples**:
  - `GET /archive/real_plant_analyzed`
  - `GET /archive/virtual_plant_analyzed`

---

## Specialized Data Endpoints

### GET `/image/<scan_id>/<fileset_id>/<file_id>`
**Access images with resizing**
- **Description**: Retrieve images with optional resizing
- **Path Parameters**: 
  - `scan_id`: Scan identifier
  - `fileset_id`: Fileset identifier (usually 'images')
  - `file_id`: Image file identifier (without extension)
- **Query Parameters**: 
  - `size`: Image size - `orig` (original), `large`, `thumb` (thumbnail)
- **Response**: Image file (JPEG/PNG)
- **Examples**:
  - Thumbnail: `GET /image/real_plant_analyzed/images/00000_rgb`
  - Original: `GET /image/real_plant_analyzed/images/00000_rgb?size=orig`
  - Large: `GET /image/real_plant_analyzed/images/00000_rgb?size=large`

### GET `/pointcloud/<scan_id>/<fileset_id>/<file_id>`
**Access point clouds**
- **Description**: Retrieve point cloud files with optional voxel size control
- **Path Parameters**: 
  - `scan_id`: Scan identifier
  - `fileset_id`: Point cloud fileset identifier
  - `file_id`: Point cloud file identifier
- **Query Parameters**: 
  - `size`: `orig` (original), `preview`, or a float value for voxel size
- **Response**: Point cloud file (PLY format)
- **Examples**:
  - Preview: `GET /pointcloud/real_plant_analyzed/PointCloud_1_0_1_0_10_0_7ee836e5a9/PointCloud`
  - Original: `GET /pointcloud/real_plant_analyzed/PointCloud_1_0_1_0_10_0_7ee836e5a9/PointCloud?size=orig`
  - Custom voxel: `GET /pointcloud/real_plant_analyzed/PointCloud_1_0_1_0_10_0_7ee836e5a9/PointCloud?size=2.3`

### GET `/pcGroundTruth/<scan_id>/<fileset_id>/<file_id>`
**Access ground truth point clouds**
- **Description**: Retrieve ground truth point cloud files
- **Parameters**: Same as `/pointcloud` endpoint
- **Response**: Ground truth point cloud file (PLY format)

### GET `/mesh/<scan_id>/<fileset_id>/<file_id>`
**Access 3D meshes**
- **Description**: Retrieve 3D mesh files
- **Path Parameters**: 
  - `scan_id`: Scan identifier
  - `fileset_id`: Mesh fileset identifier
  - `file_id`: Mesh file identifier
- **Query Parameters**: 
  - `size`: Currently only `orig` (original) supported
- **Response**: Mesh file (PLY format)
- **Example**: `GET /mesh/real_plant_analyzed/TriangleMesh_9_most_connected_t_open3d_00e095c359/TriangleMesh`

### GET `/skeleton/<scan_id>`
**Access curve skeleton data**
- **Description**: Retrieve curve skeleton information for a scan
- **Path Parameters**: 
  - `scan_id`: Scan identifier
- **Response**: JSON with curve skeleton data
- **Example**: `GET /skeleton/real_plant_analyzed`

### GET `/sequence/<scan_id>`
**Access sequence information**
- **Description**: Retrieve sequence-related information for a scan
- **Path Parameters**: 
  - `scan_id`: Scan identifier
- **Response**: JSON with sequence data
- **Example**: `GET /sequence/real_plant_analyzed`

---

## Database Management

### GET `/refresh`
**Refresh scan database**
- **Description**: Refresh the list of scans in the database
- **Response**: HTTP 200 on completion
- **Example**: `GET /refresh`

---

## API Management Endpoints

The following endpoints provide programmatic access to create and manage database content:

### POST `/api/scan`
**Create new scan**
- **Description**: Create a new scan dataset
- **Request Body**: JSON with scan metadata
- **Response**: Scan creation confirmation

### GET/POST `/api/scan/<scan_id>/metadata`
**Manage scan metadata**
- **Description**: Get or update scan metadata
- **Path Parameters**: 
  - `scan_id`: Scan identifier

### GET `/api/scan/<scan_id>/filesets`
**List scan filesets**
- **Description**: List all filesets within a scan
- **Path Parameters**: 
  - `scan_id`: Scan identifier

### POST `/api/fileset`
**Create new fileset**
- **Description**: Create a new fileset within a scan
- **Request Body**: JSON with fileset information

### GET/POST `/api/fileset/<scan_id>/<fileset_id>/metadata`
**Manage fileset metadata**
- **Description**: Get or update fileset metadata

### GET `/api/fileset/<scan_id>/<fileset_id>/files`
**List fileset files**
- **Description**: List all files within a fileset

### POST `/api/file`
**Create new file**
- **Description**: Add a new file to a fileset
- **Request Body**: File data and metadata

### GET/POST `/api/file/<scan_id>/<fileset_id>/<file_id>/metadata`
**Manage file metadata**
- **Description**: Get or update file metadata

---

## Health and Status

### GET `/health`
**Health check**
- **Description**: Check API server health and database connectivity
- **Response**: JSON with health status information
- **Example**: `GET /health`

---

## Data Structures

### Scan Summary Structure

Information returned by `/scans` endpoint:

```json
{
  "id": "scan_id",
  "metadata": {
    "date": "2019-02-01 13:35:42",
    "species": "Arabidopsis",
    "plant": "plant_001",
    "environment": "controlled",
    "nbPhotos": 150,
    "files": {
      "metadata": "/files/scan_id/metadata/metadata.json",
      "archive": "/archive/scan_id"
    }
  },
  "thumbnailUri": "/files/scan_id/images/thumbnail.jpg",
  "hasPointCloud": true,
  "hasMesh": true,
  "hasSkeleton": false,
  "hasTreeGraph": false,
  "hasAngleData": false,
  "hasAutomatedMeasures": false,
  "hasManualMeasures": true,
  "hasSegmentation2D": true,
  "hasPcdGroundTruth": false,
  "hasPointCloudEvaluation": false,
  "hasSegmentedPointCloud": false,
  "error": false
}
```

### Detailed Scan Structure

Information returned by `/scans/<scan_id>` includes all summary fields plus:

#### Camera Model
```json
{
  "camera": {
    "model": {
      "id": 1,
      "model": "OPENCV",
      "width": 1440,
      "height": 1080,
      "params": [1166.95, 1166.95, 720, 540, -0.0014, -0.0014, 0, 0]
    }
  }
}
```

#### Camera Poses
```json
{
  "camera": {
    "poses": [
      {
        "id": 1,
        "tvec": [369.43, 120.36, -62.07],
        "rotmat": [
          [0.0648, -0.9972, 0.0382],
          [-0.3390, -0.0580, -0.9390],
          [0.9385, 0.0479, -0.3418]
        ],
        "photoUri": "/files/scan_id/images/00000_rgb.jpg",
        "thumbnailUri": "/files/scan_id/images/00000_rgb?size=thumb",
        "isMatched": true
      }
    ]
  }
}
```

#### Reconstruction Workspace
```json
{
  "workspace": {
    "x": [340, 440],
    "y": [330, 410], 
    "z": [-180, 105]
  }
}
```

### Status Indicators

The `has*` fields indicate data availability:

- **hasPointCloud**: Point cloud reconstruction available
- **hasMesh**: 3D mesh reconstruction available
- **hasSkeleton**: Curve skeleton analysis available
- **hasTreeGraph**: Tree structure analysis available
- **hasAngleData**: Angle and internode measurements available
- **hasAutomatedMeasures**: Automated measurements available
- **hasManualMeasures**: Manual measurements available (`measures.json`)
- **hasSegmentation2D**: 2D segmentation masks available
- **hasPcdGroundTruth**: Ground truth point cloud available
- **hasPointCloudEvaluation**: Point cloud evaluation results available
- **hasSegmentedPointCloud**: Segmented point cloud available

---

## Examples

### Basic Usage

```bash
# Get all scans
curl http://127.0.0.1:5000/scans

# Get specific scan details  
curl http://127.0.0.1:5000/scans/real_plant_analyzed

# Get an image thumbnail
curl http://127.0.0.1:5000/image/real_plant_analyzed/images/00000_rgb

# Download a point cloud
curl http://127.0.0.1:5000/pointcloud/real_plant_analyzed/PointCloud_xyz/PointCloud?size=orig -o pointcloud.ply

# Download complete dataset
curl http://127.0.0.1:5000/archive/real_plant_analyzed -o dataset.zip
```

### Filtering Scans

```bash
# Search for Arabidopsis plants
curl "http://127.0.0.1:5000/scans?filterQuery=arabidopsis"

# Search in scan info table
curl "http://127.0.0.1:5000/scans_info?filterQuery=controlled_environment"
```

### Working with Files

```bash
# Access metadata file
curl http://127.0.0.1:5000/files/my_scan/metadata/metadata.json

# Upload a new file
curl -X POST http://127.0.0.1:5000/files/my_scan \
     -F "file_upload=@measurements.json"

# Get different image sizes
curl http://127.0.0.1:5000/image/my_scan/images/photo_001        # Default
curl http://127.0.0.1:5000/image/my_scan/images/photo_001?size=thumb  # Thumbnail
curl http://127.0.0.1:5000/image/my_scan/images/photo_001?size=orig   # Original
```

---

## Error Handling

The API returns standard HTTP status codes:

- **200**: Success
- **400**: Bad Request (invalid parameters)
- **404**: Not Found (scan/file doesn't exist)
- **500**: Internal Server Error (missing dependencies, processing errors)

Error responses include JSON with error details:

```json
{
  "error": "Scan not found",
  "message": "The requested scan 'invalid_scan' does not exist"
}
```

---

## References

- Implementation: `plantdb.server.rest_api` module
- Server CLI: `fsdb_rest_api.py`
- Flask Documentation: [[5]](https://flask.palletsprojects.com/en/stable/api/)
- Flask-RESTful: [[8]](https://flask-restful.readthedocs.io/)
