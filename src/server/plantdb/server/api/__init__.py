#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
REST API for PlantDB

This module implements a collection of Flask Resource classes that expose endpoints for handling scans,
datasets, authentication, and related resources.
It centralizes request handling, JWT validation, rate‑limiting, and file‑URI resolution, providing a
robust backend for 3‑D scan data services.

Key Features
------------
- **Authentication** - JWT‑based login, logout, token validation and refresh.
- **Health monitoring** - Simple health‑check endpoint exposing database status.
- **Scan lifecycle** - Create, retrieve, update, and list scans and their associated filesets.
- **File handling** - Endpoints for uploading, retrieving, and managing individual files, datasets,
  point clouds, meshes, and related assets.
- **Metadata services** - Accessors for scan metadata, fileset metadata, and file‑specific metadata.
- **Utility helpers** - Functions for URI generation, name sanitization, rate limiting, and archive validation.
"""
