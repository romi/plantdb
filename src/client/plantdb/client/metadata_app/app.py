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
from typing import Any

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
from plantdb.client.metadata_app.db_ops import _connect
from plantdb.client.metadata_app.field_spec import FIELD_SPECS
from plantdb.client.metadata_app.field_spec import FieldSpec
from plantdb.client.metadata_app.field_spec import coerce
from plantdb.client.metadata_app.field_spec import sections
from plantdb.client.metadata_app.field_spec import specs_for_section
from plantdb.client.metadata_app.field_spec import unflatten

logger = get_logger(os.environ.get('ROMI_APP_LOGGER', __name__))
logger.setLevel(DEFAULT_LOG_LEVEL)

# Long-running work (migration) runs as a Dash background callback in a subprocess,
# so its progress can stream back to the UI; diskcache persists the callback state.
#: Background-callback backend; migration runs in a subprocess so its progress can stream.
_CACHE = diskcache.Cache(str(Path(__file__).parent / ".dashcache"))
background_callback_manager = DiskcacheManager(_CACHE)

app = Dash(name="plantdb-metadata",
           title="PlantDB Metadata Editor",
           external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.BOOTSTRAP],
           assets_folder=str(Path(__file__).parent / "assets"),
           background_callback_manager=background_callback_manager)


def _field_input_id(path: str) -> str:
    """Return a Dash-safe component id for a field path."""
    return "per-" + path.replace(".", "__")


def _tooltip(spec: FieldSpec) -> str:
    """Build a compact tooltip string for a field spec.

    Parameters
    ----------
    spec : FieldSpec
        A MIAPPE field definition.

    Returns
    -------
    str
        The definition, plus any format, example and MIAPPE codename,
        joined by ``|``.
    """
    tt = spec["tooltip"]
    lines = [tt["definition"]]
    if tt.get("format"):
        lines.append(f"Format: {tt['format']}")
    if tt.get("example"):
        lines.append(f"Example: {tt['example']}")
    if tt.get("codename"):
        lines.append(f"MIAPPE: {tt['codename']}")
    return " | ".join(lines)


def scan_checklist(scan_ids: list[str]) -> list[dict[str, str]]:
    """Build the scope checklist options for the given scan ids.

    Parameters
    ----------
    scan_ids : list[str]
        The scan ids to turn into checklist entries.

    Returns
    -------
    list[dict[str, str]]
        ``{"label": id, "value": id}`` entries for a Dash checklist.
    """
    return [{"label": s, "value": s} for s in scan_ids]


FIELD_OPTIONS = [{"label": spec["path"], "value": spec["path"]} for spec in FIELD_SPECS]
FIELD_STATES = [State(_field_input_id(spec["path"]), "value") for spec in FIELD_SPECS]

# Indirection store: main() pre-fills it from --db-path so the database loads on
# startup, while the Load button writes to it to trigger the same loading flow.
#: Save the location of the current database.
DB_PATH_STORE = dcc.Store(id="db-path-store", data=None)


def _filtered_scans(scan_ids: list[str], scans: dict[str, dict[str, Any]],
                    regexp: str | None, fpath: str | None, fvalue: str | None) -> list[str]:
    """Return scan ids matching the ID regexp AND the metadata value filter.

    Parameters
    ----------
    scan_ids : list[str]
        All scan ids of the loaded database.
    scans : dict[str, dict[str, Any]]
        Flattened ``{scan_id: {dot.path: value}}`` metadata per scan.
    regexp : str | None
        Optional regexp matched against the scan id; ``None`` keeps all.
    fpath : str | None
        Optional metadata field path to filter on; ignored unless both
        ``fpath`` and ``fvalue`` are provided.
    fvalue : str | None
        Case-insensitive substring the field value must contain.

    Returns
    -------
    list[str]
        Scan ids passing both filters.
    """
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

UI_HELP = """
## How to use this UI

### 1. Load a database.
Enter the path to your PlantDB (FSDB) in the "FSDB location" box and click Load.
The number of loaded scans is shown below it.

### 2. Migrate legacy scans.
If some scans still use the old pre-MIAPPE schema, a migration dialog appears.
Click Migrate to convert them before editing (required).

### 3. Choose what to edit.
Under "Scan selection", pick either "Bulk edit" to edit several scans at once, or "Single scan edit" to inspect and edit one scan.
In bulk mode, narrow the list with the scan-ID regex and/or metadata-value filters, then tick the scans to target.

### 4. Fill in the fields.
The fields are grouped by MIAPPE section (investigation, study, biological material, observed variable).
Hover the help icon next to a field to see its MIAPPE codename and definition.

### 5. Apply your changes.
Click Apply.
In single mode the whole scan is saved.
In bulk mode only the fields you filled in are written to each selected scan.
"""

UI_ABOUT = """
This tool allows you to edit the metadata of a PlantDB database.

### ISA & MIAPPE

The biological metadata follows the **ISA** (Investigation / Study / Assay)
framework and its plant-phenotyping specialization **MIAPPE** (Minimum
Information about a Plant Phenotyping Experiment). Each top-level metadata
block corresponds to a MIAPPE section:

- **Investigation** — the overall project/dataset.
- **Study** — the experiment (facility, environment, experimental design/factors).
- **Biological material** — the scanned plant, MIAPPE's *observation unit*.
- **Observed variable** — the trait measured and the method used.

For the detailed mapping from legacy fields to the MIAPPE tree, see the
[developer documentation](docs/developers/miappe_metadata.md).

Scans created before this schema can also be edited here; they are flagged and
migrated to the MIAPPE structure on load.
"""

# ----------------------------------------------------------------------
# Layout
# ----------------------------------------------------------------------
app.layout = dbc.Container([
    # App header bar
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
        # Help and About nav entries opening their respective modals
        dbc.Nav(
            [
                dbc.NavItem(
                    dbc.NavLink([html.H4("Help")],
                                id="help-btn", href="#", active=False)
                ),
                dbc.NavItem(
                    dbc.NavLink([html.H4("About")],
                                id="about-btn", href="#", active=False)
                ),
            ],
            className="ms-auto",
        ),
    ], color="#00a960", class_name="mb-3"),

    # Main row: scan selection (left) + field edit form (right)
    dbc.Row([
        dbc.Col([
            # FSDB location: path input, Load button, and status areas
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
            # Scan selection: filter + checklist for bulk, or a single scan picker
            dbc.Card([
                dbc.CardHeader(
                    html.H4([html.I(className="bi bi-search me-2"), "Scan selection"], className="mb-0")
                ),
                dbc.CardBody([
                    # Accordion switching between the two edit scopes
                    dbc.Accordion([
                        # Bulk edit: filter scans by ID regexp and/or metadata value
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
                        # Single scan edit: pick one scan to inspect
                        dbc.AccordionItem([
                            dbc.Label("Scan:"),
                            dbc.Select(id="scan-select", options=[], placeholder="Select a scan..."),
                        ], title=[html.I(className="bi bi-folder me-2"), "Single scan edit"], item_id="single", ),
                    ], id="edit-accordion", start_collapsed=False, flush=True),
                ])
            ]),
        ], width=6),

        # Field edit: the shared per-scan/bulk form plus Apply button and status
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(
                    html.H4([html.I(className="bi bi-pencil-square me-2"), "Metadata field edition"], className="mb-0")
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

    # Session stores: loaded scans, migratable scans, and migration-done flag
    dcc.Store(id="scans-store", data={}, storage_type="session"),
    dcc.Store(id="migratable-store", data={}, storage_type="session"),
    dcc.Store(id="migration-done", data=False, storage_type="session"),
    DB_PATH_STORE,

    # Blocking modal shown when the loaded database still has legacy (pre-MIAPPE) scans.
    dbc.Modal([
        dbc.ModalHeader(dbc.ModalTitle("MIAPPE migration required")),
        dbc.ModalBody(id="migration-modal-body"),
    ], id="migration-modal", is_open=False, centered=True),

    # Help modal opened from the "Help" nav entry
    dbc.Modal([
        dbc.ModalHeader(dbc.ModalTitle([html.I(className="bi bi-question-circle me-2"), "Help"])),
        dbc.ModalBody([
            dcc.Markdown(UI_HELP)
        ]),
    ], id="help-modal", is_open=False, centered=True),

    # About modal opened from the "About" nav entry
    dbc.Modal([
        dbc.ModalHeader(dbc.ModalTitle([html.I(className="bi bi-info-circle me-2"), "About"])),
        dbc.ModalBody([
            dcc.Markdown(UI_ABOUT)
        ]),
    ], id="about-modal", is_open=False, centered=True),

], id="metadata-app", fluid=True)


def _migration_modal_body(migratable: list[str]) -> dbc.ModalBody:
    """Build the migration-warning modal body for the given scan ids.

    Parameters
    ----------
    migratable : list[str]
        Scan ids that still use the legacy (pre-MIAPPE) schema.

    Returns
    -------
    dbc.ModalBody
        The modal content: warning, Migrate/Abort buttons, a progress bar
        and the list of scans requiring migration.
    """
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
        return {}, {}, [], [], [], [], dbc.Alert(f"Path does not exist: `{db_path}`", color="danger")
    try:
        scans_md = db_ops.all_scan_metadata(db_path)
        scan_ids = list(scans_md.keys())
        # Switching databases: drop the cached connection for the previous one to
        # avoid holding several live FSDB instances at once.
        if prev_data and prev_data.get("db_path") and Path(prev_data["db_path"]).resolve() != db_path:
            db_ops.close_db(Path(prev_data["db_path"]))
        data = {"db_path": str(db_path), "scan_ids": scan_ids, "scans": scans_md}
        opts = scan_checklist(scan_ids)
        n = len(scan_ids)
        ok_alert = dbc.Alert([html.I(className="bi bi-check-circle-fill me-2"),
                              f"Loaded {n} scan(s) from `{db_path}`"], color="success")
        # Pre-compute which scans still need migration; this drives the warning modal.
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


# Toggle the About modal when its nav entry is clicked.
@callback(
    Output("about-modal", "is_open"),
    Input("about-btn", "n_clicks"),
    State("about-modal", "is_open"),
    prevent_initial_call=True,
)
def toggle_about(n_clicks, is_open):
    return not is_open


# Toggle the Help modal when its nav entry is clicked.
@callback(
    Output("help-modal", "is_open"),
    Input("help-btn", "n_clicks"),
    State("help-modal", "is_open"),
    prevent_initial_call=True,
)
def toggle_help(n_clicks, is_open):
    return not is_open


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

    def _report(done: int, total: int) -> None:
        set_progress((done, total, f"{done} / {total}"))

    # Runs in a background subprocess; set_progress streams counts to the progress bar.
    try:
        db_ops.migrate_scans_progress(Path(db_path), list(migratable), logger, _report)
        msg = dbc.Alert([html.I(className="bi bi-check-circle-fill me-2"),
                         f"Migrated {total} scan(s) to the MIAPPE schema."], color="success")
        return msg, True, time.time()
    except Exception as e:
        msg = dbc.Alert([html.I(className="bi bi-x-octagon-fill me-2"),
                         f"Migration failed: {e}"], color="danger")
        return msg, True, False


# On success, close the modal a few seconds after it is shown so the user can see
# the confirmation; otherwise leave it untouched.
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
    # One shared form is used for both edit modes: in "single" mode it is prefilled
    # with the scan's values, in "bulk" mode the fields stay empty as templates.
    section_accordion = dbc.Accordion([], id="field-accordion", always_open=True)
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

        sec_acc = dbc.AccordionItem(rows,
                                    title=[html.I(className=f"{_SECTION_ICONS.get(section, 'bi-folder')} me-2"),
                                           section],
                                    className="mb-2")
        section_accordion.children.append(sec_acc)
    return section_accordion


#: Dictionary of icons associated with the metadata sections
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
            # Rebuild the nested MIAPPE tree and merge it into the scan's metadata.
            tree = unflatten(dict(flat))
            scan = _connect(db_path).get_scan(scan_id)
            metadata = scan.get_metadata()
            metadata = db_ops.update_biological(metadata, tree)
            db_ops.write_scan_metadata(scan, metadata)
            msg = f"Saved scan '{scan_id}'."
        else:
            selected = selected or []
            if not selected:
                return dbc.Alert("Select at least one scan for bulk edit.", color="warning"), data, [], []
            # Only non-empty fields are applied, each to every selected scan.
            edited = {p: v for p, v in flat.items() if v not in (None, "")}
            if not edited:
                return dbc.Alert("Enter at least one value to apply.", color="warning"), data, [], []
            modified = set()
            for path, value in edited.items():
                modified.update(db_ops.apply_bulk(db_path, selected, path, coerce(path, value)))
            msg = f"Applied to {len(modified)} scan(s)."
        # Reload so the refreshed scan list/checklist reflect the edits.
        scans_md = db_ops.all_scan_metadata(db_path)
        scan_ids = list(scans_md.keys())
        new_data = {"db_path": str(db_path), "scan_ids": scan_ids, "scans": scans_md}
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
def main(db_path: str | None, port: int, debug: bool) -> None:
    """Launch the MIAPPE metadata editor web UI."""
    # Pre-seed the store so the DB loads automatically without clicking Load.
    if db_path:
        DB_PATH_STORE.data = db_path

    app.run(host="0.0.0.0", port=port, debug=debug)


if __name__ == "__main__":
    main()
