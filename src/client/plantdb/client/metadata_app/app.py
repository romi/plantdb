#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
# MIAPPE Metadata Editor - Dash Web UI

A web-based UI to view, edit and bulk-fill the MIAPPE-aligned biological
metadata (``investigation`` / ``study`` / ``biologicalMaterial`` /
``observedVariable``) of the scans of a **local** PlantDB (FSDB).

It offers two editing modes:

* a **bulk editor** to set a single field's value on a chosen subset of scans
  (all / matching a scan-ID regexp / matching a metadata field value / an
  explicit checklist), and
* a **per-scan editor** to inspect and edit the full tree of one scan.

Each field shows the MIAPPE definition as a tooltip on hover, and free-text
fields suggest values already used across the database.

## Usage example

```shell
metadata_app --db-path /data/ROMI/test_PI3_juillet_Alexis
# then open http://localhost:8050
```
"""
from __future__ import annotations

import os
import re
from pathlib import Path

import click
import dash_bootstrap_components as dbc
from dash import Dash
from dash import Input
from dash import Output
from dash import State
from dash import callback
from dash import dcc
from dash import html

os.environ.setdefault('ROMI_APP_LOGGER', 'metadata_gui')
from plantdb.commons.log import DEFAULT_LOG_LEVEL
from plantdb.commons.log import get_logger

from plantdb.commons.fsdb.exceptions import NotAnFSDBError

from plantdb.client.metadata_app import db_ops
from plantdb.client.metadata_app.field_spec import FIELD_SPECS
from plantdb.client.metadata_app.field_spec import FIELD_BY_PATH
from plantdb.client.metadata_app.field_spec import coerce
from plantdb.client.metadata_app.field_spec import collect_suggestions
from plantdb.client.metadata_app.field_spec import sections
from plantdb.client.metadata_app.field_spec import specs_for_section
from plantdb.client.metadata_app.field_spec import unflatten

app = Dash(name="plantdb-metadata",
           title="PlantDB Metadata Editor",
           external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.BOOTSTRAP],
           assets_folder=str(Path(__file__).parent / "assets"))


def _field_input_id(path: str) -> str:
    """Dash-safe component id for a field path."""
    return "per-" + path.replace(".", "__")


def _tooltip(spec) -> str:
    """Build a compact tooltip string for a field spec."""
    tt = spec["tooltip"]
    lines = [tt["definition"]]
    if tt.get("format"):
        lines.append(f"Format: {tt['format']}")
    if tt.get("example"):
        lines.append(f"Example: {tt['example']}")
    if tt.get("codename"):
        lines.append(f"MIAPPE: {tt['codename']}")
    return " | ".join(lines)


def scan_checklist(scan_ids):
    """Build the scope checklist options for the given scan ids."""
    return [{"label": s, "value": s} for s in scan_ids]


FIELD_OPTIONS = [{"label": spec["path"], "value": spec["path"]} for spec in FIELD_SPECS]
FIELD_STATES = [State(_field_input_id(spec["path"]), "value") for spec in FIELD_SPECS]

#: DB path store.
#: ``main()`` pre-fills the store from ``--db-path`` so the database is loaded automatically on startup.
DB_PATH_STORE = dcc.Store(id="db-path-store", data=None)

def _filtered_scans(scan_ids, scans, regexp, fpath, fvalue):
    """Return scan ids matching the regexp AND the metadata filter."""
    rx = None
    if regexp:
        try:
            rx = re.compile(regexp)
        except re.error:
            rx = None
    out = []
    for sid in scan_ids:
        if rx is not None and not rx.search(sid):
            continue
        if fpath and fvalue and fvalue.lower() not in str(scans[sid].get(fpath, "")).lower():
            continue
        out.append(sid)
    return out


# ----------------------------------------------------------------------
# Layout
# ----------------------------------------------------------------------
app.layout = dbc.Container([
    dbc.Navbar([
        dbc.Row([
            dbc.Col(
                html.H1([
                    html.I(className="bi bi-pencil-square me-2"),
                    "PlantDB Metadata Editor"
                ], style={'textAlign': 'center', 'width': '100%', 'color': "#F3F3F3"}),
                width=12, className="text-center"
            )
        ], className="w-100"),
    ], color="#00a960", class_name="mb-3"),

    dbc.Card([
        dbc.CardBody([
            dbc.Row([
                dbc.Col([
                    dbc.InputGroup([
                        dbc.InputGroupText(html.I(className="bi bi-folder2-open")),
                        dbc.Input(id="db-path", type="text", placeholder="/path/to/database"),
                        dbc.Button([html.I(className="bi bi-arrow-repeat me-1"), "Load"],
                                   id="load-btn", color="success", n_clicks=0),
                    ])
                ], width=12)
            ]),
            html.Div(id="db-status", className="mt-2"),
            html.Div(id="migration-status", className="mt-2"),
        ])
    ], className="mb-4"),

    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H4([
                    html.I(className="bi bi-bullseye me-2"), "1. Choose scans"], className="mb-0")),
                dbc.CardBody([
                    dbc.InputGroup([
                        dbc.InputGroupText(html.I(className="bi bi-funnel")),
                        dbc.Input(id="scan-regexp", type="text",
                                  placeholder="Scan ID regexp (e.g. ^test_.*2026)"),
                    ], className="mb-2"),
                    dbc.Row([
                        dbc.Col(dbc.Select(id="meta-filter-path", placeholder="Metadata field..."), width=7),
                        dbc.Col(dbc.Input(id="meta-filter-value", type="text", placeholder="field value"), width=5),
                    ], className="mb-2"),
                    html.Div(id="scope-info", className="mb-2 text-muted small"),
                    dbc.Checklist(id="scan-checklist", options=[], value=[], switch=True),
                ])
            ], className="mb-4"),

            dbc.Card([
                dbc.CardHeader(html.H4([
                    html.I(className="bi bi-diagram3 me-2"), "2. Bulk edit field"], className="mb-0")),
                dbc.CardBody([
                    dbc.Label("Field to set:"),
                    dbc.Select(id="bulk-field", options=FIELD_OPTIONS),
                    html.Div(id="bulk-tooltip", className="mt-2 mb-2 small text-muted"),
                    dbc.Label("Suggested existing values:"),
                    dbc.Select(id="bulk-suggest", options=[], placeholder="(pick or type below)"),
                    dbc.Label("Value:", className="mt-2"),
                    dbc.Input(id="bulk-value", type="text"),
                    html.Div(id="bulk-preview", className="mt-2 text-muted small"),
                    dbc.Button([html.I(className="bi bi-check2-square me-1"), "Apply to selected"],
                               id="bulk-apply-btn", color="primary", className="mt-3"),
                    html.Div(id="bulk-status", className="mt-2"),
                ])
            ]),
        ], width=6),

        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H4([
                    html.I(className="bi bi-pencil-square me-2"), "Edit a single scan"], className="mb-0")),
                dbc.CardBody([
                    dbc.Label("Scan:"),
                    dbc.Select(id="scan-select", options=[], placeholder="Select a scan..."),
                    html.Div(id="per-scan-form", className="mt-3"),
                    dbc.Button([html.I(className="bi bi-save me-1"), "Save scan"],
                               id="per-scan-save-btn", color="success", className="mt-3"),
                    html.Div(id="per-scan-status", className="mt-2"),
                ])
            ]),
        ], width=6),
    ]),

    dcc.Store(id="scans-store", data={}, storage_type="session"),
    DB_PATH_STORE,

], id="metadata-app", fluid=True)


# ----------------------------------------------------------------------
# Callbacks
# ----------------------------------------------------------------------
@callback(
    Output("db-path-store", "data"),
    Input("load-btn", "n_clicks"),
    State("db-path", "value"),
    prevent_initial_call=True
)
def set_db_path_from_button(n_clicks, db_path):
    """Write the entered path into the store, which triggers loading."""
    if n_clicks and db_path:
        return db_path
    return None

@callback(
    Output("db-path", "value"),
    Input("db-path-store", "data"),
    prevent_initial_call=False
)
def set_db_path_value(db_path):
    """Update the text input value from the store."""
    return db_path or ""

@callback(
    [Output("scans-store", "data"),
     Output("scan-checklist", "options"),
     Output("scan-checklist", "value"),
     Output("scan-select", "options"),
     Output("meta-filter-path", "options"),
     Output("db-status", "children"),
     Output("migration-status", "children")],
    Input("db-path-store", "data"),
    prevent_initial_call=False
)
def load_database(db_path):
    if not db_path:
        return {}, [], [], [], [], "", ""
    db_path = Path(db_path).expanduser().resolve()
    if not db_path.is_dir():
        return {}, [], [], [], [], dbc.Alert(f"Path does not exist: `{db_path}`", color="danger"), ""
    try:
        scan_ids, scans = db_ops.load_db(db_path)
        data = {"db_path": str(db_path), "scan_ids": scan_ids, "scans": scans}
        opts = scan_checklist(scan_ids)
        n = len(scan_ids)
        ok_alert = dbc.Alert([html.I(className="bi bi-check-circle-fill me-2"),
                              f"Loaded {n} scan(s) from `{db_path}`"], color="success")
        migratable = db_ops.migratable_scans(db_path)
        if migratable:
            mig_alert = dbc.Alert([
                html.I(className="bi bi-exclamation-triangle-fill me-2"),
                html.Strong(f"{len(migratable)} scan(s) use the legacy (pre-MIAPPE) schema and need migration:"),
                html.Ul([html.Li(s) for s in migratable[:20]] +
                        ([html.Li(f"... and {len(migratable) - 20} more")] if len(migratable) > 20 else [])),
                html.Small("Edit is disabled for legacy scans until migrated."),
                html.Div(dbc.Button([html.I(className="bi bi-arrow-repeat me-1"), "Migrate now"],
                                    id="migrate-btn", color="warning", n_clicks=0), className="mt-2"),
            ], color="warning")
        else:
            mig_alert = ""
        return (data, opts, scan_ids,
                [{"label": s, "value": s} for s in scan_ids],
                FIELD_OPTIONS, ok_alert, mig_alert)
    except NotAnFSDBError as e:
        return ({}, [], [], [], [],
                dbc.Alert([html.I(className="bi bi-exclamation-triangle-fill me-2"), str(e)],
                          color="danger"), "")
    except Exception as e:
        return ({}, [], [], [], [], "",
                dbc.Alert([html.I(className="bi bi-x-octagon-fill me-2"), f"Load failed: {e}"],
                          color="danger"))


@callback(
    [Output("migration-status", "children", allow_duplicate=True),
     Output("scans-store", "data", allow_duplicate=True),
     Output("scan-checklist", "options", allow_duplicate=True),
     Output("scan-checklist", "value", allow_duplicate=True),
     Output("scan-select", "options", allow_duplicate=True)],
    Input("migrate-btn", "n_clicks"),
    State("scans-store", "data"),
    prevent_initial_call=True
)
def do_migrate(n_clicks, data):
    if not n_clicks or not data:
        return "", data, [], [], []
    db_path = Path(data["db_path"])
    try:
        migratable = db_ops.migratable_scans(db_path)
        done = db_ops.migrate_scans(db_path, migratable)
        scan_ids, scans = db_ops.load_db(db_path)
        new_data = {"db_path": str(db_path), "scan_ids": scan_ids, "scans": scans}
        msg = dbc.Alert([html.I(className="bi bi-check-circle-fill me-2"),
                         f"Migrated {done} scan(s) to the MIAPPE schema."], color="success")
        return msg, new_data, scan_checklist(scan_ids), scan_ids, [{"label": s, "value": s} for s in scan_ids]
    except Exception as e:
        return (dbc.Alert([html.I(className="bi bi-x-octagon-fill me-2"), f"Migration failed: {e}"],
                          color="danger"), data, [], [], [])


@callback(
    [Output("scan-checklist", "options", allow_duplicate=True),
     Output("scan-checklist", "value", allow_duplicate=True),
     Output("scope-info", "children")],
    [Input("scan-regexp", "value"),
     Input("meta-filter-path", "value"),
     Input("meta-filter-value", "value")],
    State("scans-store", "data"),
    prevent_initial_call=True
)
def update_scope(regexp, fpath, fvalue, data):
    if not data or not data.get("scan_ids"):
        return [], [], ""
    matched = _filtered_scans(data["scan_ids"], data["scans"], regexp, fpath, fvalue)
    return (scan_checklist(matched), matched,
            f"{len(matched)} of {len(data['scan_ids'])} scan(s) match the filters.")


@callback(
    [Output("bulk-tooltip", "children"),
     Output("bulk-suggest", "options"),
     Output("bulk-value", "value", allow_duplicate=True)],
    Input("bulk-field", "value"),
    State("scans-store", "data"),
    prevent_initial_call=True
)
def update_bulk_field(field, data):
    if not field:
        return "", [], ""
    spec = FIELD_BY_PATH[field]
    tooltip = html.Div([
        html.I(className="bi bi-info-circle me-1"),
        html.Span(f"{spec['label']}: ", style={"fontWeight": "bold"}),
        html.Span(spec["tooltip"]["definition"]),
        html.Div(spec["tooltip"].get("format", ""), className="small text-muted"),
    ])
    suggestions = []
    if spec["suggest"] and data and data.get("scans"):
        suggestions = [{"label": v, "value": v}
                       for v in collect_suggestions(list(data["scans"].values()), field)]
    return tooltip, suggestions, ""


@callback(
    Output("bulk-value", "value"),
    Input("bulk-suggest", "value"),
    prevent_initial_call=True
)
def set_bulk_value_from_suggestion(value):
    return value


@callback(
    [Output("bulk-status", "children"),
     Output("scans-store", "data", allow_duplicate=True),
     Output("scan-checklist", "options", allow_duplicate=True),
     Output("scan-checklist", "value", allow_duplicate=True)],
    Input("bulk-apply-btn", "n_clicks"),
    [State("scans-store", "data"),
     State("scan-checklist", "value"),
     State("bulk-field", "value"),
     State("bulk-value", "value")],
    prevent_initial_call=True
)
def apply_bulk(n_clicks, data, selected, field, value):
    if not n_clicks:
        return "", data, [], []
    if not data or not field or value is None or value == "":
        return (dbc.Alert("Select a field, a value and at least one scan.", color="warning"),
                data, [], [])
    db_path = Path(data["db_path"])
    selected = selected or []
    try:
        modified = db_ops.apply_bulk(db_path, selected, field, coerce(field, value))
        scan_ids, scans = db_ops.load_db(db_path)
        new_data = {"db_path": str(db_path), "scan_ids": scan_ids, "scans": scans}
        msg = dbc.Alert([html.I(className="bi bi-check-circle-fill me-2"),
                         f"Updated {len(modified)} scan(s)."], color="success")
        return msg, new_data, scan_checklist(scan_ids), scan_ids
    except Exception as e:
        return (dbc.Alert([html.I(className="bi bi-x-octagon-fill me-2"), f"Apply failed: {e}"],
                          color="danger"), data, [], [])


@callback(
    Output("per-scan-form", "children"),
    Input("scan-select", "value"),
    State("scans-store", "data"),
    prevent_initial_call=True
)
def render_scan_form(scan_id, data):
    if not scan_id or not data or not data.get("scans"):
        return ""
    flat = data["scans"].get(scan_id, {})
    cards = []
    for section in sections():
        specs = specs_for_section(section)
        if not specs:
            continue
        rows = []
        for spec in specs:
            path = spec["path"]
            value = flat.get(path)
            kind = {"type": "number"} if spec["type"] in ("int", "float") else {"type": "text"}
            rows.append(dbc.Row([
                dbc.Col(dbc.Label(spec["label"], title=_tooltip(spec)), width=4, className="align-self-center"),
                dbc.Col(dbc.Input(id=_field_input_id(path),
                                  value="" if value is None else str(value), **kind), width=8),
            ], className="mb-2"))
        cards.append(dbc.Card([
            dbc.CardHeader(html.H5([html.I(className="bi bi-folder me-2"), section], className="mb-0")),
            dbc.CardBody(rows),
        ], className="mb-2"))
    return cards


@callback(
    [Output("per-scan-status", "children"),
     Output("scans-store", "data", allow_duplicate=True),
     Output("scan-select", "options", allow_duplicate=True)],
    Input("per-scan-save-btn", "n_clicks"),
    [State("scans-store", "data"),
     State("scan-select", "value"),
     *FIELD_STATES],
    prevent_initial_call=True
)
def save_scan(n_clicks, data, scan_id, *field_values):
    if not n_clicks:
        return "", data, []
    if not data or not scan_id:
        return dbc.Alert("Select a scan first.", color="warning"), data, []
    try:
        flat = {spec["path"]: val for spec, val in zip(FIELD_SPECS, field_values)}
        tree = unflatten({k: v for k, v in flat.items()})
        db_path = Path(data["db_path"])
        scan_dir = db_ops.get_scan_dir(db_path, scan_id)
        metadata = db_ops.read_scan_metadata(scan_dir)
        metadata = db_ops.update_biological(metadata, tree)
        db_ops.write_scan_metadata(scan_dir, metadata)
        scan_ids, scans = db_ops.load_db(db_path)
        new_data = {"db_path": str(db_path), "scan_ids": scan_ids, "scans": scans}
        msg = dbc.Alert([html.I(className="bi bi-check-circle-fill me-2"), f"Saved scan '{scan_id}'."],
                        color="success")
        return msg, new_data, [{"label": s, "value": s} for s in scan_ids]
    except Exception as e:
        return (dbc.Alert([html.I(className="bi bi-x-octagon-fill me-2"), f"Save failed: {e}"],
                          color="danger"), data, [])


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--db-path", type=click.Path(exists=True, file_okay=False, dir_okay=True),
              help="Path to the local PlantDB (FSDB) to edit.")
@click.option("--port", type=int, default=8050, help="Port to run the web application on.")
@click.option("--debug", is_flag=True, help="Enable debug mode.")
def main(db_path, port, debug):
    """MIAPPE metadata editor - Dash UI for editing scan metadata of a local PlantDB."""
    logger = get_logger(os.environ.get('ROMI_APP_LOGGER', __name__))
    logger.setLevel(DEFAULT_LOG_LEVEL)

    if db_path:
        DB_PATH_STORE.data = db_path

    app.run(host="0.0.0.0", port=port, debug=debug)


if __name__ == "__main__":
    main()
