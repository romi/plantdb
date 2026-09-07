#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
# MIAPPE field specification for the metadata editor UI

Defines every editable leaf field of the canonical MIAPPE-aligned scan
metadata tree (see ``plantdb.commons.fsdb.metadata_schema``) plus the detailed
explanations of each field (sourced from the MIAPPE checklist data model TSV).

It also provides helpers to flatten a nested metadata dict to a flat
``{dot.path: value}`` mapping and to rebuild the tree, which the UI uses to
render editable forms and to write values back.
"""
from __future__ import annotations

from typing import Any

from plantdb.commons.fsdb.metadata_schema import SCAN_BIOLOGICAL_SCHEMA

#: A field description. ``type`` is one of ('str', 'int', 'float'); ``suggest``
#: controls whether the UI offers a dropdown of existing values; ``tooltip``
#: holds the MIAPPE definition (``definition``), ``example``, ``format`` and the
#: ``codename`` it maps to in the MIAPPE checklist.
FieldSpec = dict[str, Any]

#: Curated tooltips keyed by the MIAPPE checklist codename they map to.
_MIAPPE_TOOLTIPS: dict[str, dict[str, str]] = {
    "investigationId": {
        "definition": "Identifier comprising the unique name of the institution/database hosting the submission of the investigation data, and the accession number of the investigation in that institution.",
        "example": "EBI:12345678", "format": "Unique identifier", "codename": "investigationId"},
    "investigationTitle": {
        "definition": "Human-readable string summarising the investigation.",
        "example": "Adaptation of Maize to Temperate Climates", "format": "Free text (short)", "codename": "investigationTitle"},
    "investigationDescription": {
        "definition": "Human-readable text describing the investigation in more detail.",
        "format": "Free text", "codename": "investigationDescription"},
    "studyId": {
        "definition": "Unique identifier comprising the name or identifier for the institution/database hosting the submission of the study data, and the identifier of the study in that institution.",
        "example": "INRA:2023-03", "format": "Unique identifier", "codename": "studyId"},
    "studyTitle": {
        "definition": "Name, human-readable text summarising the study.",
        "format": "Free text (short)", "codename": "studyTitle"},
    "studyStartDate": {
        "definition": "Date and, if relevant, time when the experiment started.",
        "format": "Date/Time (ISO 8601, optional time zone)", "codename": "studyStartDate"},
    "studyEndDate": {
        "definition": "Date and, if relevant, time when the experiment ended.",
        "format": "Date/Time (ISO 8601, optional time zone)", "codename": "studyEndDate"},
    "siteName": {
        "definition": "The name of the natural site, experimental field, greenhouse, phenotyping facility, etc. where the experiment took place.",
        "example": "Lyon Indoor", "format": "Free text (short)", "codename": "siteName"},
    "growthFacilityType": {
        "definition": "Type of growth facility in which the study was carried out (e.g. greenhouse, growth chamber).",
        "example": "greenhouse", "format": "Free text (short)", "codename": "growthFacilityType"},
    "envParam": {
        "definition": "Name of the environment parameter constant within the experiment (e.g. photoperiod, temperature).",
        "example": "LD+SD", "format": "Free text", "codename": "envParam"},
    "expeDesignType": {
        "definition": "Type of experimental design of the study (e.g. randomized complete block, split-plot).",
        "example": "randomized complete block", "format": "Free text", "codename": "expeDesignType"},
    "expeFactorType": {
        "definition": "Name/Acronym of the experimental factor (e.g. treatment, irrigation regime).",
        "example": "treatment", "format": "Free text", "codename": "expeFactorType"},
    "biologicalMaterialId": {
        "definition": "Code used to identify the biological material in the data file. Can correspond to experimental plant ID, seed lot ID, etc. Should be unique within the investigation.",
        "example": "P1", "format": "Unique identifier", "codename": "biologicalMaterialId"},
    "organism": {
        "definition": "An identifier for the organism at the species level. Use of the NCBI taxon ID is recommended.",
        "example": "Arabidopsis thaliana", "format": "Unique identifier", "codename": "organism"},
    "genus": {
        "definition": "Genus name for the organism under study, according to standard scientific nomenclature.",
        "example": "Arabidopsis", "format": "Genus name", "codename": "genus"},
    "species": {
        "definition": "Species name (formally: specific epithet) for the organism under study, according to standard scientific nomenclature.",
        "example": "thaliana", "format": "Species name", "codename": "species"},
    "materialSourceId": {
        "definition": "An identifier for the source of the biological material (repository plus accession of the donor).",
        "format": "Unique identifier", "codename": "materialSourceId"},
    "materialSourceAccName": {
        "definition": "Genebank accession registered name or other designation given to the material, other than the donor's accession number or collecting number; or variety name.",
        "example": "Col-0", "format": "Free text (short)", "codename": "materialSourceAccName"},
    "sampleId": {
        "definition": "Unique identifier for the sample (e.g. the organ/part that was measured).",
        "example": "main stem", "format": "Unique identifier", "codename": "sampleId"},
    "traitName": {
        "definition": "Name of the (plant or environmental) trait under observation.",
        "example": "internode length", "format": "Free text", "codename": "traitName"},
    "methodName": {
        "definition": "Name of the method of observation.",
        "example": "segmentation", "format": "Free text", "codename": "methodName"},
    "scaleName": {
        "definition": "Name of the scale associated with the variable.",
        "example": "cm", "format": "Unique identifier", "codename": "scaleName"},
}


def _spec(path: str, label: str, ftype: str = "str", codename: str | None = None,
          suggest: bool = False, note: str | None = None) -> FieldSpec:
    """Build a ``FieldSpec`` with a tooltip from the MIAPPE table or a note.

    Parameters
    ----------
    path : str
        Dot-separated field path in the MIAPPE tree.
    label : str
        Human-readable label shown in the UI.
    ftype : str, default 'str'
        Field value type, one of ``'str'``, ``'int'``, ``'float'``.
    codename : str or None, optional
        MIAPPE checklist codename used to look up the tooltip.
    suggest : bool, default False
        Whether the UI offers a dropdown of existing values.
    note : str or None, optional
        Extra text appended to the definition, or used as the definition
        when no MIAPPE tooltip exists.

    Returns
    -------
    FieldSpec
        The field specification dict.
    """
    if codename and codename in _MIAPPE_TOOLTIPS:
        tooltip = dict(_MIAPPE_TOOLTIPS[codename])
        if note:
            tooltip["definition"] = f"{tooltip['definition']} ({note})"
    else:
        tooltip = {"definition": note or label, "format": "Free text", "codename": codename}
    return {"path": path, "label": label, "type": ftype, "suggest": suggest, "tooltip": tooltip}


#: Ordered list of every editable leaf field of the MIAPPE biological tree.
FIELD_SPECS: list[FieldSpec] = [
    # investigation
    _spec("investigation.identifier", "Identifier", codename="investigationId"),
    _spec("investigation.title", "Title", codename="investigationTitle"),
    _spec("investigation.description", "Description", codename="investigationDescription"),
    # study
    _spec("study.identifier", "Identifier", codename="studyId"),
    _spec("study.title", "Title", codename="studyTitle"),
    _spec("study.startDate", "Start date", codename="studyStartDate"),
    _spec("study.endDate", "End date", codename="studyEndDate"),
    _spec("study.growthFacility.name", "Growth facility name", suggest=True, codename="siteName"),
    _spec("study.growthFacility.country", "Growth facility country", codename="siteName",
          note="extension: ISO country code of the growth facility"),
    _spec("study.environment.photoperiod", "Photoperiod", suggest=True, codename="envParam"),
    _spec("study.experimentalDesign.type", "Experimental design", suggest=True, codename="expeDesignType"),
    _spec("study.experimentalFactors.treatment", "Treatment", suggest=True, codename="expeFactorType"),
    # biological material
    _spec("biologicalMaterial.biologicalMaterialId", "Biological material ID", codename="biologicalMaterialId"),
    _spec("biologicalMaterial.organism.species", "Species", suggest=True, codename="species"),
    _spec("biologicalMaterial.organism.genus", "Genus", suggest=True, codename="genus"),
    _spec("biologicalMaterial.materialSource.id", "Material source ID", codename="materialSourceId"),
    _spec("biologicalMaterial.materialSource.name", "Material source name", suggest=True, codename="materialSourceAccName"),
    _spec("biologicalMaterial.ageDays", "Age (days)", ftype="int"),
    _spec("biologicalMaterial.sample", "Sample", suggest=True, codename="sampleId"),
    # observed variable
    _spec("observedVariable.trait", "Trait", codename="traitName"),
    _spec("observedVariable.method", "Method", codename="methodName"),
    _spec("observedVariable.scale", "Scale", codename="scaleName"),
]

#: Map dot-path -> FieldSpec for fast lookup.
FIELD_BY_PATH: dict[str, FieldSpec] = {spec["path"]: spec for spec in FIELD_SPECS}

#: Fields allowed as integer/float values.
_NUMERIC_FIELDS: set[str] = {p for p, s in FIELD_BY_PATH.items() if s["type"] in ("int", "float")}


def flatten(metadata: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    """Flatten a nested metadata dict into a flat ``{dot.path: value}`` mapping.

    Only leaf fields declared in ``FIELD_SPECS`` are kept.

    Parameters
    ----------
    metadata : dict of str to Any
        The nested metadata dict to flatten.
    prefix : str, default ''
        Dot-separated path prefix to prepend (used in recursion).

    Returns
    -------
    dict of str to Any
        The flattened mapping.
    """
    flat: dict[str, Any] = {}
    for key, value in metadata.items():
        path = f"{prefix}.{key}" if prefix else key
        if path in FIELD_BY_PATH:
            flat[path] = value
        elif isinstance(value, dict):
            flat.update(flatten(value, path))
    return flat


def unflatten(flat: dict[str, Any]) -> dict[str, Any]:
    """Rebuild a nested metadata dict from a flat ``{dot.path: value}`` mapping.

    Intermediate sections are created as needed. Values are coerced to the
    declared field type.

    Parameters
    ----------
    flat : dict of str to Any
        The flattened mapping to rebuild.

    Returns
    -------
    dict of str to Any
        The nested metadata tree.
    """
    tree: dict[str, Any] = {}
    for path, value in flat.items():
        parts = path.split(".")
        target = tree
        for part in parts[:-1]:
            target = target.setdefault(part, {})
        target[parts[-1]] = coerce(path, value)
    return tree


def coerce(path: str, value: Any) -> Any:
    """Coerce ``value`` to the declared type of the field at ``path``.

    Parameters
    ----------
    path : str
        Dot-separated field path whose declared type is applied.
    value : Any
        Value to coerce.

    Returns
    -------
    Any
        The coerced value, the original value if it cannot be coerced, or
        ``None`` for empty values.
    """
    if value is None or value == "":
        return None
    spec = FIELD_BY_PATH[path]
    ftype = spec["type"]
    try:
        if ftype == "int":
            return int(float(value))
        if ftype == "float":
            return float(value)
    except (TypeError, ValueError):
        return value
    return str(value)


def collect_suggestions(scans: list[dict[str, Any]], path: str) -> list[str]:
    """Return the sorted unique non-empty values of ``path`` across ``scans``.

    Parameters
    ----------
    scans : list of dict of str to Any
        A list of flattened scan dicts (see :func:`flatten`).
    path : str
        Dot-separated field path to collect values for.

    Returns
    -------
    list of str
        Sorted unique values.
    """
    values: set[str] = set()
    for flat in scans:
        v = flat.get(path)
        if v is not None and str(v) not in (None, "", "None"):
            values.add(str(v))
    return sorted(values)


def sections() -> list[str]:
    """Return the ordered top-level MIAPPE section names.

    Returns
    -------
    list of str
        The top-level keys of the biological schema.
    """
    return list(SCAN_BIOLOGICAL_SCHEMA.keys())


def specs_for_section(section: str) -> list[FieldSpec]:
    """Return the field specs whose dot-path starts with ``section``.

    Parameters
    ----------
    section : str
        Top-level MIAPPE section name, e.g. ``study``.

    Returns
    -------
    list of FieldSpec
        The field specs belonging to the section.
    """
    return [s for s in FIELD_SPECS if s["path"].startswith(section + ".")]


__all__ = [
    "FIELD_SPECS", "FIELD_BY_PATH", "_NUMERIC_FIELDS",
    "flatten", "unflatten", "coerce", "collect_suggestions", "sections", "specs_for_section",
]
