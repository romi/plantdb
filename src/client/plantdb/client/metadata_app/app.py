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
import time
from pathlib import Path

import click
import dash_bootstrap_components as dbc
import diskcache
from dash import Dash
from dash import DiskcacheManager
from dash import Input
from dash import Output
from dash import State
from dash import callback
from dash import clientside_callback
from dash import dcc
from dash import html

os.environ.setdefault('ROMI_APP_LOGGER', 'metadata_gui')
from plantdb.commons.log import DEFAULT_LOG_LEVEL
from plantdb.commons.log import get_logger

from plantdb.commons.fsdb.exceptions import NotAnFSDBError

from plantdb.client.metadata_app import db_ops
from plantdb.client.metadata_app.field_spec import FIELD_SPECS
from plantdb.client.metadata_app.field_spec import coerce
from plantdb.client.metadata_app.field_spec import sections
from plantdb.client.metadata_app.field_spec import specs_for_section
from plantdb.client.metadata_app.field_spec import unflatten

logger = get_logger(os.environ.get('ROMI_APP_LOGGER', __name__))
logger.setLevel(DEFAULT_LOG_LEVEL)

#: Background-callback backend; migration runs in a subprocess so its progress can stream.
_CACHE = diskcache.Cache(str(Path(__file__).parent / ".dashcache"))
background_callback_manager = DiskcacheManager(_CACHE)

app = Dash(name="plantdb-metadata",
           title="PlantDB Metadata Editor",
           external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.BOOTSTRAP],
           assets_folder=str(Path(__file__).parent / "assets"),
           background_callback_manager=background_callback_manager)


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

    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(
                    html.H4([html.I(className="bi bi-database me-2"), "FSDB location"], className="mb-0")
                ),
                dbc.CardBody([
                    dbc.InputGroup([
                        dbc.InputGroupText(html.I(className="bi bi-folder2-open")),
                        dbc.Input(id="db-path", type="text", placeholder="/path/to/database"),
                        dbc.Button([html.I(className="bi bi-arrow-repeat me-1"), "Load"],
                                   id="load-btn", color="success", n_clicks=0),
                    ]),
                    html.Div(id="db-status", className="mt-2"),
                    html.Div(id="migration-status", className="mt-2"),
                ])
            ]),
        ]),

        dbc.Col([
            dbc.Card([
                dbc.CardHeader(
                    html.H4([html.I(className="bi bi-info-circle-fill me-2"), "About"], className="mb-0")
                ),
                dbc.CardBody([
                    html.P([
                        "This tool allows you to edit the metadata of a PlantDB database. "
                        "It is based on the MIAPPE schema, a standardized way to store metadata in PlantDB, but can also edit legacy scans."
                    ]),
                    html.P([
                        "Hover over the field labels to see the MIAPPE codenames and definitions. "
                    ])
                ])
            ]),
        ])
    ], className="mb-4"),

    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(
                    html.H4([html.I(className="bi bi-search me-2"), "Scan selection"], className="mb-0")
                ),
                dbc.CardBody([
                    dbc.Accordion([
                        dbc.AccordionItem([
                            dbc.InputGroup([
                                dbc.InputGroupText(html.I(className="bi bi-funnel")),
                                dbc.Input(id="scan-regexp", type="text",
                                          placeholder="Scan ID regexp (e.g. ^test_.*2026)"),
                            ], className="mb-2"),
                            dbc.Row([
                                dbc.Col(dbc.Select(id="meta-filter-path", placeholder="Metadata field..."), width=7),
                                dbc.Col(dbc.Input(id="meta-filter-value", type="text", placeholder="field value"),
                                        width=5),
                            ], className="mb-2"),
                            html.Div(id="scope-info", className="mb-2 text-muted small"),
                            dbc.Checklist(id="scan-checklist", options=[], value=[], switch=True),
                        ], title=[html.I(className="bi bi-pencil-square me-2"), "Bulk edit"], item_id="bulk"),
                        dbc.AccordionItem([
                            dbc.Label("Scan:"),
                            dbc.Select(id="scan-select", options=[], placeholder="Select a scan..."),
                        ], title=[html.I(className="bi bi-folder me-2"), "Single scan edit"], item_id="single", ),
                    ], id="edit-accordion", start_collapsed=False, flush=True),
                ])
            ]),
        ], width=6),

        dbc.Col([
            dbc.Card([
                dbc.CardHeader(
                    html.H4([html.I(className="bi bi-pencil-square me-2"), "Field edit"], className="mb-0")
                ),
                dbc.CardBody([
                    html.Div(id="per-scan-form"),
                    dbc.Button([html.I(className="bi bi-save me-1"), "Apply"],
                               id="field-apply-btn", color="success", className="mt-3"),
                    html.Div(id="field-status", className="mt-2"),
                ])
            ]),
        ], width=6),
    ], className="mb-4"),

    dcc.Store(id="scans-store", data={}, storage_type="session"),
    dcc.Store(id="migratable-store", data={}, storage_type="session"),
    dcc.Store(id="migration-done", data=False, storage_type="session"),
    DB_PATH_STORE,

    dbc.Modal([
        dbc.ModalHeader(dbc.ModalTitle("MIAPPE migration required")),
        dbc.ModalBody(id="migration-modal-body"),
    ], id="migration-modal", is_open=False, centered=True),

], id="metadata-app", fluid=True)


def _migration_modal_body(migratable):
    """Build the migration-warning modal body for the given scan ids."""
    return dbc.ModalBody([
        dbc.Alert([html.I(className="bi bi-exclamation-triangle-fill me-2"),
                   f"{len(migratable)} scan(s) use the legacy (pre-MIAPPE) schema and require migration "
                   f"before they can be edited."],
                  color="warning", className="mb-2"),
        dbc.Row([
            dbc.Col(dbc.Button([html.I(className="bi bi-arrow-repeat me-1"), "Migrate"],
                               id="migrate-btn", color="warning", n_clicks=0, size="lg"), width=6),
            dbc.Col(dbc.Button("Abort",
                               id="migrate-abort-btn", color="danger", n_clicks=0, size="lg"), width=6),
        ], className="mb-2"),
        dbc.Progress(id="migration-progress", value=0, max=1, label="",
                     striped=True, animated=True, className="mb-2"),
        html.Div([
            html.Strong("Scans requiring migration:"),
            html.Ul([html.Li(s) for s in sorted(migratable)], className="mb-0"),
        ]),
    ])


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
     Output("migratable-store", "data"),
     Output("scan-checklist", "options"),
     Output("scan-checklist", "value"),
     Output("scan-select", "options"),
     Output("meta-filter-path", "options"),
     Output("db-status", "children"), ],
    Input("db-path-store", "data"),
    State("scans-store", "data"),
    prevent_initial_call=False
)
def load_database(db_path, prev_data):
    if not db_path:
        return {}, {}, [], [], [], [], ""
    db_path = Path(db_path).expanduser().resolve()
    if not db_path.is_dir():
        return {}, {}, [], [], [], [], dbc.Alert(f"Path does not exist: `{db_path}`", color="danger"), "", False, ""
    try:
        scan_ids, scans = db_ops.load_db(db_path)
        if prev_data and prev_data.get("db_path") and Path(prev_data["db_path"]).resolve() != db_path:
            db_ops.close_db(Path(prev_data["db_path"]))
        data = {"db_path": str(db_path), "scan_ids": scan_ids, "scans": scans}
        opts = scan_checklist(scan_ids)
        n = len(scan_ids)
        ok_alert = dbc.Alert([html.I(className="bi bi-check-circle-fill me-2"),
                              f"Loaded {n} scan(s) from `{db_path}`"], color="success")
        migratable = db_ops.migratable_scans(db_path)
        return (data, migratable, opts, scan_ids,
                [{"label": s, "value": s} for s in scan_ids],
                FIELD_OPTIONS, ok_alert)
    except NotAnFSDBError as e:
        return ({}, {}, [], [], [], [],
                dbc.Alert([html.I(className="bi bi-exclamation-triangle-fill me-2"), str(e)],
                          color="danger"))
    except Exception as e:
        return ({}, {}, [], [], [], [],
                dbc.Alert([html.I(className="bi bi-x-octagon-fill me-2"), f"Load failed: {e}"],
                          color="danger"))


@callback(
    [
        Output("migration-modal", "is_open"),
        Output("migration-status", "children"),
        Output("migration-modal-body", "children")
    ],
    Input("migratable-store", "data"),
    prevent_initial_call=False
)
def set_migration_modal(migratable):
    if not migratable:
        return False, "", ""

    mig_note = dbc.Alert([html.I(className="bi bi-exclamation-triangle-fill me-2"),
                          f"{len(migratable)} scan(s) require MIAPPE migration."],
                         color="warning")

    return True, mig_note, _migration_modal_body(migratable)


@callback(
    [Output("migration-status", "children", allow_duplicate=True),
     Output("migration-modal", "is_open", allow_duplicate=True),
     Output("migration-done", "data", allow_duplicate=True)],
    Input("migrate-btn", "n_clicks"),
    State("migratable-store", "data"),
    State("db-path-store", "data"),
    State("migration-modal", "is_open"),
    background=True,
    progress=[Output("migration-progress", "value"),
              Output("migration-progress", "max"),
              Output("migration-progress", "label")],
    running=[(Output("migrate-btn", "disabled"), True, False)],
    prevent_initial_call=True
)
def do_migration(set_progress, n_clicks, migratable, db_path, modal_open):
    if not n_clicks:
        return "", modal_open, False
    if not migratable:
        return dbc.Alert("No scans to migrate.", color="warning"), False, False
    total = len(migratable)

    def _report(done, total):
        set_progress((done, total, f"{done} / {total}"))

    try:
        db_ops.migrate_scans_progress(Path(db_path), list(migratable), logger, _report)
        msg = dbc.Alert([html.I(className="bi bi-check-circle-fill me-2"),
                         f"Migrated {total} scan(s) to the MIAPPE schema."], color="success")
        return msg, True, time.time()
    except Exception as e:
        msg = dbc.Alert([html.I(className="bi bi-x-octagon-fill me-2"),
                         f"Migration failed: {e}"], color="danger")
        return msg, True, False


clientside_callback(
    """
    function(done) {
        if (done) {
            return new Promise((resolve) => setTimeout(() => resolve(false), 3000));
        }
        return dash_clientside.no_update;
    }
    """,
    Output("migration-modal", "is_open", allow_duplicate=True),
    Input("migration-done", "data"),
    prevent_initial_call=True
)


@callback(
    [Output("migration-status", "children", allow_duplicate=True),
     Output("migration-modal", "is_open", allow_duplicate=True)],
    Input("migrate-abort-btn", "n_clicks"),
    State("migration-modal", "is_open"),
    State("migration-status", "children"),
    prevent_initial_call=True
)
def abort_migration(n_clicks, modal_open, modal_status):
    if n_clicks:
        msg = dbc.Alert([html.I(className="bi bi-x-octagon-fill me-2"),
                         f"Migration aborted!"], color="danger")
        return msg, False
    else:
        return modal_status, modal_open


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
    Output("per-scan-form", "children"),
    [Input("edit-accordion", "active_item"),
     Input("scan-select", "value")],
    State("scans-store", "data"),
    prevent_initial_call=False
)
def render_edit_form(mode, scan_id, data):
    """Render the unified field form: values filled for a single scan, placeholders for bulk."""
    flat = {}
    if mode == "single" and scan_id and data and data.get("scans"):
        flat = data["scans"].get(scan_id, {})
    section_accordion = dbc.Accordion([], id="field-accordion", flush=True)
    for section in sections():
        specs = specs_for_section(section)
        if not specs:
            continue
        rows = []
        for spec in specs:
            path = spec["path"]
            value = flat.get(path)
            kind = {"type": "number"} if spec["type"] in ("int", "float") else {"type": "text"}
            rows.append(dbc.InputGroup([
                dbc.InputGroupText(html.Label(spec["label"]), className="align-self-center"),
                dbc.Input(id=_field_input_id(path), value="" if value is None else str(value), **kind),
                dbc.InputGroupText(html.I(className="bi bi-question-circle", title=_tooltip(spec)),
                                   className="align-self-center"),
            ], className="mb-1 field-group"))
        section_accordion.children.append(dbc.AccordionItem(rows,
                                                             title=[html.I(className=f"{_SECTION_ICONS.get(section, 'bi-folder')} me-2"), section],
                                                             className="mb-2"))
    return section_accordion


_SECTION_ICONS = {
    "investigation": "bi-clipboard-data",
    "study": "bi-calendar3",
    "biologicalMaterial": "bi-flower1",
    "observedVariable": "bi-rulers",
}

@callback(
    [Output("field-status", "children"),
     Output("scans-store", "data", allow_duplicate=True),
     Output("scan-checklist", "options", allow_duplicate=True),
     Output("scan-checklist", "value", allow_duplicate=True)],
    Input("field-apply-btn", "n_clicks"),
    [State("edit-accordion", "active_item"),
     State("scans-store", "data"),
     State("scan-select", "value"),
     State("scan-checklist", "value"),
     *FIELD_STATES],
    prevent_initial_call=True
)
def apply_edit(n_clicks, mode, data, scan_id, selected, *field_values):
    if not n_clicks:
        return "", data, [], []
    if not data:
        return dbc.Alert("Load a database first.", color="warning"), data, [], []
    flat = {spec["path"]: val for spec, val in zip(FIELD_SPECS, field_values)}
    db_path = Path(data["db_path"])
    try:
        if mode == "single":
            if not scan_id:
                return dbc.Alert("Select a single scan first.", color="warning"), data, [], []
            tree = unflatten(dict(flat))
            scan_dir = db_ops.get_scan_dir(db_path, scan_id)
            metadata = db_ops.read_scan_metadata(scan_dir)
            metadata = db_ops.update_biological(metadata, tree)
            db_ops.write_scan_metadata(scan_dir, metadata)
            msg = f"Saved scan '{scan_id}'."
        else:
            selected = selected or []
            if not selected:
                return dbc.Alert("Select at least one scan for bulk edit.", color="warning"), data, [], []
            edited = {p: v for p, v in flat.items() if v not in (None, "")}
            if not edited:
                return dbc.Alert("Enter at least one value to apply.", color="warning"), data, [], []
            modified = set()
            for path, value in edited.items():
                modified.update(db_ops.apply_bulk(db_path, selected, path, coerce(path, value)))
            msg = f"Applied to {len(modified)} scan(s)."
        scan_ids, scans = db_ops.load_db(db_path)
        new_data = {"db_path": str(db_path), "scan_ids": scan_ids, "scans": scans}
        return (dbc.Alert([html.I(className="bi bi-check-circle-fill me-2"), msg], color="success"),
                new_data, scan_checklist(scan_ids), scan_ids)
    except Exception as e:
        return (dbc.Alert([html.I(className="bi bi-x-octagon-fill me-2"), f"Edit failed: {e}"],
                          color="danger"), data, [], [])


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--db-path", type=click.Path(exists=True, file_okay=False, dir_okay=True),
              help="Path to the local PlantDB (FSDB) to edit.")
@click.option("--port", type=int, default=8050, help="Port to run the web application on.")
@click.option("--debug", is_flag=True, help="Enable debug mode.")
def main(db_path, port, debug):
    """MIAPPE metadata editor - Dash UI for editing scan metadata of a local PlantDB."""

    if db_path:
        DB_PATH_STORE.data = db_path

    app.run(host="0.0.0.0", port=port, debug=debug)


if __name__ == "__main__":
    main()
