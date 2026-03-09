#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# plantdb - Data handling tools for the ROMI project
#
# Copyright (C) 2018-2019 Sony Computer Science Laboratories
# Authors: D. Colliaux, T. Wintz, P. Hanappe
#
# This file is part of plantdb.
#
# plantdb is free software: you can redistribute it
# and/or modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation, either
# version 3 of the License, or (at your option) any later version.
#
# plantdb is distributed in the hope that it will be
# useful, but WITHOUT ANY WARRANTY; without even the implied
# warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with plantdb.  If not, see
# <https://www.gnu.org/licenses/>.
# ------------------------------------------------------------------------------

import hashlib
import logging
import os
from pathlib import Path
from tempfile import mkstemp
from typing import Any
from typing import List
from zipfile import BadZipFile
from zipfile import ZipFile

from flask import request

import plantdb.commons.fsdb.core


# --------------------------------------------------------------------------- #
# Custom Errors
# --------------------------------------------------------------------------- #

class ArchiveError(RuntimeError):
    """Base class for archive‑related errors."""


class ValidationError(ArchiveError):
    """Raised when an uploaded archive fails validation."""


class ExtractionError(ArchiveError):
    """Raised when an error occurs while extracting an archive."""


class FileUploadError(RuntimeError):
    """Raised when something goes wrong while persisting an uploaded file."""


# --------------------------------------------------------------------------- #
#  Validation helpers (POST - file upload)
# --------------------------------------------------------------------------- #

_REQUIRED_HEADERS = {
    "Content-Disposition",
    "Content-Length",
    "X-File-Path",
}


def _missing_headers(headers: dict) -> List[str]:
    """Return a list of required header names that are absent."""
    return [h for h in _REQUIRED_HEADERS if h not in headers]


def validate_upload_headers(headers: dict) -> dict:
    """
    Verify that the request contains all mandatory headers and return a
    dictionary with normalized values.

    Parameters
    ----------
    headers :
        ``request.headers`` mapping.

    Returns
    -------
    dict
        ``{
            "rel_filename": str,
            "content_length": int,
            "chunk_size": int,
        }``

    Raises
    ------
    FileUploadError
        If a required header is missing or malformed.
    """
    missing = _missing_headers(headers)
    if missing:
        raise FileUploadError(f"Missing required header(s): {', '.join(missing)}")

    rel_filename = headers.get("X-File-Path", "").strip()
    if not rel_filename:
        raise FileUploadError("Header 'X-File-Path' must contain a non‑empty value")

    try:
        content_length = int(headers.get("Content-Length", "0"))
    except ValueError as exc:
        raise FileUploadError("Header 'Content-Length' must be an integer") from exc
    if content_length <= 0:
        raise FileUploadError("Header 'Content-Length' must be > 0")

    try:
        chunk_size = int(headers.get("X-Chunk-Size", "0"))
    except ValueError as exc:
        raise FileUploadError("Header 'X-Chunk-Size' must be an integer") from exc
    if chunk_size < 0:
        raise FileUploadError("Header 'X-Chunk-Size' cannot be negative")

    return {
        "rel_filename": rel_filename,
        "content_length": content_length,
        "chunk_size": chunk_size,
    }


# --------------------------------------------------------------------------- #
#  Scan‑related helpers
# --------------------------------------------------------------------------- #

def get_scan_path(db: plantdb.commons.fsdb.core.FSDB, scan_id: str, **kwargs) -> Path:
    """Resolve the absolute path of a scan.

    Parameters
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        Database object exposing ``get_scan``.
    scan_id : str
        Identifier of the scan.

    Returns
    -------
    pathlib.Path
        Absolute directory of the scan.
    """
    scan = db.get_scan(scan_id, **kwargs)
    return Path(scan.path())


# --------------------------------------------------------------------------- #
#  File‑writing helpers (atomic, testable)
# --------------------------------------------------------------------------- #

def write_file(file_path: Path, data: bytes) -> int:
    """Write ``data`` to ``file_path`` in a single operation.

    Returns
    -------
    int
        Number of bytes written.
    """
    with file_path.open("wb") as fp:
        written = fp.write(data)
    return written


def write_streamed_file(file_path: Path, content_length: int, chunk_size: int) -> int:
    """
    Persist a streamed upload.  The Flask ``request`` object is accessed
    globally - this function only deals with the low‑level I/O.

    Parameters
    ----------
    file_path : pathlib.Path
        Destination file.
    content_length : int
        Expected total size (from the ``Content‑Length`` header).
    chunk_size : int
        Size of each chunk read from ``request.stream``.  ``0`` means “no
        streaming”, but the caller should have already handled that case.

    Returns
    -------
    int
        Number of bytes actually written.
    """
    bytes_received = 0
    with file_path.open("wb") as fp:
        while bytes_received < content_length:
            to_read = min(chunk_size, content_length - bytes_received)
            chunk = request.stream.read(to_read)
            if not chunk:  # pragma: no cover - defensive
                break
            fp.write(chunk)
            bytes_received += len(chunk)
    return bytes_received


# ------------------------------------------------------------------- #
#   Validation helpers (POST - archive handling)
# ------------------------------------------------------------------- #

def _ensure_file_present(uploaded_file: Any) -> None:
    if uploaded_file is None:
        raise ValidationError("No ZIP file provided!")


def _validate_mime_type(uploaded_file: Any) -> None:
    if uploaded_file.mimetype != "application/zip":
        raise ValidationError(
            f"Invalid MIME type '{uploaded_file.mimetype}'. Expected 'application/zip'."
        )


def _validate_extension(uploaded_file: Any) -> None:
    if not uploaded_file.filename.lower().endswith(".zip"):
        raise ValidationError(
            f"Invalid file extension for '{uploaded_file.filename}'. Expected '.zip'."
        )


def _save_to_temp(uploaded_file: Any, logger: logging.Logger) -> Path:
    """Save the uploaded file to a temporary location and return its path."""
    try:
        _, tmp_path_str = mkstemp(prefix="uploaded_", suffix=".zip")
        tmp_path = Path(tmp_path_str)
        logger.debug("Saving uploaded ZIP to temporary file %s", tmp_path)
        uploaded_file.save(tmp_path)
        return tmp_path
    except Exception as exc:
        logger.error("Failed to write uploaded file to disk: %s", exc)
        raise ValidationError("Failed to save uploaded file.") from exc


def _test_zip_integrity(zip_path: Path, logger: logging.Logger) -> None:
    """Raise `ValidationError` if the ZIP archive is corrupt."""
    try:
        with ZipFile(zip_path, "r") as zip_f:
            zip_f.testzip()
    except BadZipFile as exc:
        logger.error("Uploaded file is not a valid ZIP archive: %s", exc)
        raise ValidationError(f"Invalid ZIP archive: {exc}") from exc


def is_valid_archive(archive_path):
    """Validate if a given archive meets specific directory and file requirements.

    This function checks if the provided archive contains certain required directories and files,
    and verifies that the directory structure does not exceed a specified depth.

    Parameters
    ----------
    archive_path : str or pathlib.Path
        The path to the archive file (zip) to be validated.

    Returns
    -------
    bool
        Returns ``True`` if the archive meets all requirements, otherwise ``False``.

    Notes
    -----
    - The function currently assumes that the required directories and files are all at or above a specified depth in the archive.
    - Make sure that the provided `archive_path` points to a valid zip file.

    See Also
    --------
    zipfile.ZipFile : Python's built-in module for reading and writing ZIP files.

    Examples
    --------
    >>> import os
    >>> from zipfile import ZipFile
    >>> from plantdb.server.services.assets import is_valid_archive
    >>> # Creating a valid archive for demonstration purposes
    >>> with ZipFile('test_archive.zip', 'w') as zip_ref:
    ...    zip_ref.writestr('images/', '')
    ...    zip_ref.writestr('images/test1.jpg', '')
    ...    zip_ref.writestr('metadata/', '')
    ...    zip_ref.writestr('metadata/data.json', '')
    ...    zip_ref.writestr('files.json', '')
    >>> # Validate the archive
    >>> is_valid_archive('test_archive.zip')
    True
    >>> is_valid_archive('/tmp/real_plant.zip')
    True
    >>> is_valid_archive('/tmp/real_plant_analyzed.zip')
    True
    """
    req_dirs = ['images/', 'metadata/']
    req_files = ['files.json']

    top_dir = ''
    max_dir_dept = 2
    with ZipFile(archive_path, 'r') as zip_ref:
        zip_files = zip_ref.namelist()

    # List all top-level members in the zip file
    top_level_dirs = {name for name in zip_files if name.count('/') == 1 and name.endswith('/')}
    top_level_dirs |= {name.split('/')[0] + '/' for name in zip_files if name.count('/') == 1 and name.split('/')[0]}

    # If a lone directory is found at the top, move one step down
    if len(top_level_dirs) == 1:
        top_dir = next(iter(top_level_dirs))
        top_level_dirs = {name.replace(top_dir, '') for name in zip_files if
                          name.count('/') == 2 and name.endswith('/')}
        top_level_dirs |= {name.split('/')[1] + '/' for name in zip_files if
                           name.count('/') == 2 and '/'.join(name.split('/')[:-1])}

    # Check if the required file and directories are among them
    has_req_dirs = [rd in top_level_dirs for rd in req_dirs]
    has_req_files = [f'{top_dir}{rf}' in zip_files for rf in req_files]
    req_dir_depth = all(
        name.count('/') <= max_dir_dept + 1 if 'metadata' in name else name.count('/') <= max_dir_dept for name in
        zip_files)

    if all(has_req_dirs) and all(has_req_files) and req_dir_depth:
        return True
    else:
        # if not all(has_req_dirs):
        #    print(f"Missing required directories: {list(zip(req_dirs, [not r for r in has_req_dirs]))}")
        # if not all(has_req_files):
        #    print(f"Missing required files: {list(zip(req_files, [not r for r in has_req_files]))}")
        return False


def _ensure_valid_structure(zip_path: Path) -> None:
    """
    Perform any domain‑specific validation on the archive structure.
    Placeholder for ``is_valid_archive`` logic.
    """
    if not is_valid_archive(zip_path):
        raise ValidationError("Invalid scan dataset archive structure.")


# ------------------------------------------------------------------- #
#   ARCHIVES - ZIP creation (GET) & Extraction (POST)
# ------------------------------------------------------------------- #

def create_zip_for_scan(scan_path: Path, logger: logging.Logger) -> Path:
    """Create a temporary ZIP file containing all files under ``scan_path`` (except any ``webcache`` directories).

    Parameters
    ----------
    scan_path:
        The root directory of the scan to archive.
    logger:
        Logger used for diagnostic messages.

    Returns
    -------
    pathlib.Path
        The path to the created temporary ZIP file.

    Raises
    ------
    plantdb.server.services.assets_service.ArchiveError
        If the ZIP file cannot be created.
    """
    fd, zip_path_str = mkstemp(suffix=".zip")
    os.close(fd)  # we only need the path, not the open file descriptor
    zip_path = Path(zip_path_str)

    logger.info("Creating archive for scan at %s", scan_path)

    try:
        with ZipFile(zip_path, "w") as zip_f:
            for root, _dirs, files in os.walk(scan_path):
                # Skip any ``webcache`` sub‑directories
                if "webcache" in Path(root).parts:
                    continue
                for file in files:
                    full_path = Path(root, file)
                    relative_path = full_path.relative_to(scan_path)
                    zip_f.write(full_path, arcname=str(relative_path))
    except Exception as exc:
        logger.error("Failed to create archive: %s", exc)
        zip_path.unlink(missing_ok=True)
        raise ArchiveError(f"Could not create archive: {exc}") from exc

    return zip_path


def extract_zip_to_scan(zip_path: Path, destination: Path, logger: logging.Logger) -> List[str]:
    """ Extract ``zip_path`` into ``destination`` while performing safety checks.

    Parameters
    ----------
    zip_path:
        Path to the validated temporary ZIP file.
    destination:
        Directory where the archive contents should be placed.
    logger:
        Logger for diagnostic output.

    Returns
    -------
    list[str]
        List of extracted absolute file paths.

    Raises
    ------
    plantdb.server.services.assets_service.ExtractionError
        If any step of the extraction fails.

    Notes
    -----
    Safety checks include: path traversal, duplicate files, and hash verification
    """
    logger.debug(f"Preparing to extract {zip_path} into {destination}")

    # Detect a single top‑level directory that should be stripped
    with ZipFile(zip_path, "r") as zip_f:
        top_level_dirs = [
            name
            for name in zip_f.namelist()
            if name.endswith("/") and name.count("/") == 1
        ]
    strip_top_dir = len(top_level_dirs) == 1

    extracted: List[str] = []

    try:
        with ZipFile(zip_path, "r") as zip_f:
            for member in zip_f.namelist():
                # Skip directory entries
                if member.endswith("/"):
                    continue

                # Ensure the filename is UTF‑8 decodable
                try:
                    member = member.encode("utf-8").decode("utf-8")
                except UnicodeDecodeError as exc:
                    raise ExtractionError(
                        f"Filename encoding error in archive entry '{member}'."
                    ) from exc

                # Remove the top‑level folder if present
                target_rel = Path(member)
                if strip_top_dir:
                    target_rel = Path(*target_rel.parts[1:])

                target_path = (destination / target_rel).resolve()

                # Safety: the target must stay inside ``destination``
                if not str(target_path).startswith(str(destination.resolve())):
                    raise ExtractionError(
                        f"Path traversal attempt detected: {member}"
                    )

                # Create parent directories
                target_path.parent.mkdir(parents=True, exist_ok=True)

                # Extract with hash verification
                with zip_f.open(member) as src, target_path.open("wb") as dst:
                    src_hash = hashlib.sha256()
                    while chunk := src.read(8192):
                        src_hash.update(chunk)
                        dst.write(chunk)
                    extracted_hash = src_hash.hexdigest()

                # Verify the file on disk matches the hash we just computed
                with target_path.open("rb") as f:
                    dst_hash = hashlib.sha256()
                    while chunk := f.read(8192):
                        dst_hash.update(chunk)

                if extracted_hash != dst_hash.hexdigest():
                    logger.error("Hash mismatch after extracting %s", target_path)

                extracted.append(str(target_path))

    except Exception as exc:
        logger.error("Extraction failed: %s", exc)
        raise ExtractionError(str(exc)) from exc

    return extracted
