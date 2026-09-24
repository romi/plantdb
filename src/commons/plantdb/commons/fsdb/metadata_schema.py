#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
# Scan Metadata Schema Module

Defines the canonical MIAPPE-aligned structure for the biological metadata of a
``Scan`` and a lightweight validator that checks it (no external dependency).

The biological metadata lives in the scan's ``metadata/metadata.json`` under the
top-level MIAPPE sections: ``investigation``, ``study``, ``biologicalMaterial``
and ``observedVariable``. The validator only inspects these keys **when present**:
a scan without biological metadata is valid (the block is optional), but if a
section is provided its known fields are type-checked.

See ``docs/developers/miappe_metadata.md`` for the full mapping and design.
"""
from __future__ import annotations

from typing import Any

# ----------------------------------------------------------------------
# Canonical MIAPPE-aligned structure for a scan's biological metadata.
# Each value is either a type (or tuple of types) for a scalar field, or a
# nested ``dict`` describing a sub-object. ``None`` values are always allowed.
# ----------------------------------------------------------------------
INVESTIGATION_SCHEMA: dict[str, Any] = {
    "identifier": str,
    "title": str,
    "description": str,
}

STUDY_SCHEMA: dict[str, Any] = {
    "identifier": str,
    "title": str,
    "startDate": str,
    "endDate": str,
    "growthFacility": {"name": str, "country": str},
    "environment": {"photoperiod": str},
    "experimentalDesign": {"type": str},
    "experimentalFactors": {"treatment": str},
}

BIOLOGICAL_MATERIAL_SCHEMA: dict[str, Any] = {
    "biologicalMaterialId": str,
    "organism": {"genus": str, "species": str},
    "materialSource": {"id": str, "name": str},
    "ageDays": (int, float),
    "sample": str,
}

OBSERVED_VARIABLE_SCHEMA: dict[str, Any] = {
    "trait": str,
    "method": str,
    "scale": str,
}

#: Maps a top-level MIAPPE section name to the schema describing its fields.
SCAN_BIOLOGICAL_SCHEMA: dict[str, Any] = {
    "investigation": INVESTIGATION_SCHEMA,
    "study": STUDY_SCHEMA,
    "biologicalMaterial": BIOLOGICAL_MATERIAL_SCHEMA,
    "observedVariable": OBSERVED_VARIABLE_SCHEMA,
}


def _check(block: Any, schema: dict, path: str, errors: list[str]) -> None:
    """Recursively type-check ``block`` against ``schema``, collecting errors."""
    if block is None:
        return
    if not isinstance(block, dict):
        errors.append(f"'{path}' must be an object")
        return
    for key, rule in schema.items():
        if key not in block:
            continue
        value = block[key]
        if isinstance(rule, dict):
            _check(value, rule, f"{path}.{key}", errors)
        elif value is not None and not isinstance(value, rule):
            allowed = " or ".join(t.__name__ for t in rule) if isinstance(rule, tuple) else rule.__name__
            errors.append(f"'{path}.{key}' must be {allowed}, got {type(value).__name__}")


def validate_biological_metadata(metadata: dict[str, Any]) -> None:
    """Validate the MIAPPE biological block of ``metadata``.

    Parameters
    ----------
    metadata : dict
        The scan metadata dictionary.

    Raises
    ------
    ValueError
        If a present MIAPPE section contains a field of the wrong type.
    """
    if not isinstance(metadata, dict):
        raise ValueError("Scan metadata must be a dictionary")
    errors: list[str] = []
    for section, schema in SCAN_BIOLOGICAL_SCHEMA.items():
        if section in metadata:
            _check(metadata[section], schema, section, errors)
    if errors:
        raise ValueError("Invalid biological metadata: " + "; ".join(errors))
