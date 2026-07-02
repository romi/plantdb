"""
# REST API Client Module

Module containing functions for interacting with the PlantDB REST API, including making requests, parsing responses, and constructing URLs. This module provides a complete client-side interface for accessing PlantDB data and performing operations like authentication, data retrieval, and file management.

## Key Features

- **Authentication**: Login, logout, token validation, and refresh functionality
- **Data Retrieval**: Fetch scan information, task data, images, and configuration files
- **URL Construction**: Generate API endpoint URLs with proper formatting and parameters
- **Request Handling**: Make HTTP requests with various methods and options
- **Response Parsing**: Parse and convert API responses into Python objects (JSON, images, point clouds, meshes, etc.)
- **File Management**: Upload and download scan archives and individual files
- **Configuration Access**: Retrieve scan and reconstruction configuration files in TOML format

## Usage Examples

Hereafter is a minimal working example that:

1. Starts a test PlantDB REST API server
2. Logs in to the server
3. Retrieves scan information
4. Gets image data from a scan

```python
>>> # Start a test PlantDB REST API server first, in a terminal:
>>> # $ fsdb_rest_api --test
>>> from plantdb.client.rest_api.requests import request_login
>>> from plantdb.client.rest_api.parsers import parse_scans_info, parse_task_images
>>> # Login to the test server
>>> login_data = request_login('localhost', 'admin', 'admin', port=5000)
>>> # Get scan information
>>> scan_dict = parse_scans_info('localhost', port=5000)
>>> print(sorted(scan_dict.keys()))
['arabidopsis000', 'real_plant', 'real_plant_analyzed', 'virtual_plant', 'virtual_plant_analyzed']
>>> # Get images from a scan
>>> images = parse_task_images('localhost', 'real_plant', port=5000)
>>> print(len(images))
60
```
"""