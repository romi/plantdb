# !/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Dash UI for FSDBSync - PlantDB Database Synchronization Tool

This module provides a web-based user interface for the FSDBSync class,
allowing users to easily configure and manage database synchronization
between different database types (local, SSH, HTTP).

## Usage example

### Local Path to Local Path
```python
>>> from plantdb.commons.test_database import test_database
>>> # Create a test source database
>>> db_source = test_database()
>>> # Create a test target database
>>> db_target = test_database(dataset=None)
>>> # ---------------------------------------------------------------------
>>> # Synchronize the database using the WebUI
>>> # Use the path printed out during the test FSDB initialization
>>> # ---------------------------------------------------------------------
>>> db_source.disconnect()  # Remove the test database
>>> db_target.disconnect()  # Remove the test database
```

### Local Path to HTTP REST API
```python
>>> from plantdb.server.test_rest_api import TestRestApiServer
>>> from plantdb.commons.test_database import test_database
>>> # Create a test source database
>>> db_source = test_database()
>>> # Create a test target database
>>> db_target = TestRestApiServer(test=True, empty=True)
>>> db_target.start()
>>> # ---------------------------------------------------------------------
>>> # Synchronize the database using the WebUI
>>> # Use the path and URL printed out during the test FSDB initialization
>>> # ---------------------------------------------------------------------
>>> db_target.stop()
```

"""
import argparse
import time
import traceback
from pathlib import Path

import dash_bootstrap_components as dbc
import paramiko
import requests
from dash import Dash
from dash import Input
from dash import Output
from dash import State
from dash import callback
from dash import ctx
from dash import dcc
from dash import html

from plantdb.client.rest_api import list_scan_names
from plantdb.commons.fsdb import FSDB
from plantdb.client.sync import FSDBSync
from plantdb.client.sync import config_from_url

#: URL for the ROMI project logo used in the navigation bar
ROMI_LOGO: str = "https://romi-project.eu/assets/logo.svg"

# Initialize the Dash app with Bootstrap theme
app = Dash(name="plantdb-sync",
           title="PlantDB Synchronization Tool",
           external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.BOOTSTRAP],
           assets_folder=str(Path(__file__).parent / "assets"))

# Global variables to store sync instance and scan lists
sync_instance = None
source_scans = []
target_scans = []


def create_database_config_card(title, db_side):
    """Create a card for database configuration"""
    return dbc.Card([
        dbc.CardHeader(html.H4([html.I(className="bi bi-database me-2"), title], className="mb-0")),
        dbc.CardBody([
            # Database type selection
            dbc.Row([
                dbc.Col([
                    dbc.Label([html.I(className="bi bi-hdd-network me-1"), "Database Type:"]),
                    dbc.Select(
                        id=f"{db_side}-type",
                        options=[
                            {"label": "Local Path", "value": "local"},
                            {"label": "SSH/SFTP", "value": "ssh"},
                            {"label": "HTTP REST API", "value": "http"}
                        ],
                        value="local"
                    )
                ], width=12)
            ], className="mb-3"),

            # Pre-register all possible input IDs as hidden components to avoid callback errors
            html.Div([
                # Source inputs
                create_local_inputs(db_side),
                create_ssh_inputs(db_side),
                create_http_inputs(db_side),
            ]),

            # Test connection button and status
            dbc.Row([
                dbc.Col([
                    dbc.Button([
                        html.I(className="bi bi-plug me-1"),
                        "Test Connection"
                    ],
                        id=f"{db_side}-test-btn",
                        color="primary",
                        size="md",
                        disabled=True
                    ),
                    html.Div(id=f"{db_side}-status", className="mt-2")
                ], width=12)
            ])
        ])
    ], className="mb-4")


def create_local_inputs(db_side):  # db_side is "source" or "target"
    """Create inputs for local database configuration"""
    return dbc.Row([
        dbc.Col([
            dbc.InputGroup([
                dbc.InputGroupText(html.I(className="bi bi-folder2-open")),
                dbc.Input(
                    id=f"{db_side}-path",
                    placeholder="/path/to/database",
                    type="text",
                    value=""
                )
            ])
        ], width=12)
    ], id=f"{db_side}-local-inputs", className="mb-3", style={"display": "none"})


def create_ssh_inputs(db_side):  # db_side is "source" or "target"
    """Create inputs for SSH database configuration"""
    return html.Div([
        dbc.Row([
            dbc.Col([
                dbc.InputGroup([
                    dbc.InputGroupText(html.I(className="bi bi-globe")),
                    dbc.Input(
                        id=f"{db_side}-hostname",
                        placeholder="server.example.com",
                        type="text",
                        value=""
                    )
                ])
            ], width=6),
            dbc.Col([
                dbc.InputGroup([
                    dbc.InputGroupText(html.I(className="bi bi-folder2")),
                    dbc.Input(
                        id=f"{db_side}-ssh-path",
                        placeholder="/path/to/database",
                        type="text",
                        value=""
                    )
                ])
            ], width=6)
        ], className="mb-3"),
        dbc.Row([
            dbc.Col([
                dbc.InputGroup([
                    dbc.InputGroupText(html.I(className="bi bi-person")),
                    dbc.Input(
                        id=f"{db_side}-username",
                        placeholder="username",
                        type="text",
                        value=""
                    )
                ])
            ], width=6),
            dbc.Col([
                dbc.InputGroup([
                    dbc.InputGroupText(html.I(className="bi bi-key")),
                    dbc.Input(
                        id=f"{db_side}-password",
                        placeholder="password",
                        type="password",
                        value=""
                    )
                ])
            ], width=6)
        ], className="mb-3")
    ], id=f"{db_side}-ssh-inputs", style={"display": "none"})


def create_http_inputs(db_side):  # db_side is "source" or "target"
    """Create inputs for HTTP database configuration"""
    return dbc.Row([
        dbc.Col([
            dbc.InputGroup([
                dbc.InputGroupText(html.I(className="bi bi-link-45deg")),
                dbc.Input(
                    id=f"{db_side}-url",
                    placeholder="http://api.example.com:5000",
                    type="text",
                    value=""
                )
            ])
        ], width=12)
    ], id=f"{db_side}-http-inputs", className="mb-3", style={"display": "none"})


def create_scan_list_card(title, list_id, search_id=None):
    """Create a card for displaying scan lists"""
    search_component = []
    if search_id:
        search_component = [
            dbc.InputGroup([
                dbc.InputGroupText(html.I(className="bi bi-search")),
                dbc.Input(
                    id=search_id,
                    placeholder="Search scans...",
                    type="text"
                ),
                dbc.Button([
                    html.I(className="bi bi-x-circle"),
                    " Clear"
                ],
                    id=f"{search_id}-clear",
                    color="secondary",
                    outline=True
                )
            ], className="mb-3")
        ]

    return dbc.Card([
        dbc.CardHeader(html.H4([html.I(className="bi bi-card-list me-2"), title], className="mb-0")),
        dbc.CardBody([
            *search_component,
            dbc.Checklist(
                id=list_id,
                options=[],
                value=[],
                inline=False,
                switch=True
            ),
            html.Div([
                html.I(className="bi bi-info-circle me-1"),
                html.Span(id=f"{list_id}-info")
            ], className="mt-2 text-muted small")
        ])
    ])


# App layout
app.layout = dbc.Container([
    dbc.Navbar([
        # Header
        dbc.Row([
            dbc.Col(
                html.H1([
                    html.I(className="bi bi-arrow-repeat me-2"),
                    "PlantDB Synchronization Tool"
                ], style={'textAlign': 'center', 'width': '100%', 'color': "#F3F3F3"}),
                width=12,  # Span all columns to center
                className="text-center"
            )
        ], className="w-100"),
    ], color="#00a960", class_name="mb-3"),

    # Database Configuration Section
    dbc.Row([
        dbc.Col([
            create_database_config_card("Source Database", "source")
        ], width=6),
        dbc.Col([
            create_database_config_card("Target Database", "target")
        ], width=6)
    ]),

    # Load Controls
    dbc.Card([
        dbc.CardBody([
            dbc.Button([
                html.I(className="bi bi-arrow-repeat me-1"),
                "Load Scan Lists"
            ],
                id="load-scans-btn",
                color="success",
                size="lg",
                className="me-2",
                disabled=True
            ),
        ], className="justify-content-center d-flex"),
    ], className="mb-4"),

    # Scan Lists Section
    dbc.Row([
        dbc.Col([
            create_scan_list_card("Source Scans", "source-scans", "source-search")
        ], width=6),
        dbc.Col([
            create_scan_list_card("Target Scans (Read-only)", "target-scans")
        ], width=6)
    ], className="mb-4"),

    # Sync Controls
    dbc.Card([
        dbc.CardBody([
            dbc.Button([
                html.I(className="bi bi-cloud-arrow-up-fill me-1"),
                "Start Synchronization"
            ],
                id="sync-btn",
                color="success",
                size="lg",
                disabled=True
            ),
        ], className="justify-content-center d-flex"),
    ], className="mb-4"),

    # Progress and Logs
    dbc.Card([
        dbc.CardHeader(html.H4([
            html.I(className="bi bi-activity me-2"),
            "Synchronization Progress"
        ], className="mb-0")),
        dbc.CardBody([
            dbc.Progress(id="sync-progress", value=0),
        ])
    ], className="mb-4"),

    dbc.Accordion([
        dbc.AccordionItem([
            html.Div([
                "Logs:"
            ], className="mb-2"),
            html.Div(id="sync-logs",
                     style={"height": "200px", "overflowY": "auto", "border": "1px solid #dee2e6",
                            "padding": "10px"})
        ], title=html.H4([html.I(className="bi bi-journal-text me-2 fs-5"), "Synchronization Logs"])),
    ], start_collapsed=True, className="mb-4"),

    # Store components for maintaining state
    dcc.Store(id="source-config-store", data={}, storage_type="session"),
    dcc.Store(id="target-config-store", data={}, storage_type="session"),
    dcc.Store(id="scan-data-store", data={}, storage_type="session"),
    dcc.Interval(id="interval-component", interval=1000, n_intervals=0, disabled=True),

], id="sync-app", fluid=True)


# Callbacks for dynamic configuration inputs
@callback(
    [Output("source-local-inputs", "style"),
     Output("source-ssh-inputs", "style"),
     Output("source-http-inputs", "style"),
     Output("source-test-btn", "disabled")],
    [Input("source-type", "value")]
)
def update_source_config_inputs(connection_type):  # This is the database connection type (local/ssh/http)
    if not connection_type:
        return {'display': 'none'}, {'display': 'none'}, {'display': 'none'}, True

    if connection_type == "local":
        return {'display': True}, {'display': 'none'}, {'display': 'none'}, False
    elif connection_type == "ssh":
        return {'display': 'none'}, {'display': True}, {'display': 'none'}, False
    elif connection_type == "http":
        return {'display': 'none'}, {'display': 'none'}, {'display': True}, False

    return {'display': 'none'}, {'display': 'none'}, {'display': 'none'}, True


@callback(
    [Output("target-local-inputs", "style"),
     Output("target-ssh-inputs", "style"),
     Output("target-http-inputs", "style"),
     Output("target-test-btn", "disabled")],
    [Input("target-type", "value")]
)
def update_target_config_inputs(connection_type):  # This is the database connection type (local/ssh/http)
    if not connection_type:
        return {'display': 'none'}, {'display': 'none'}, {'display': 'none'}, True

    if connection_type == "local":
        return {'display': True}, {'display': 'none'}, {'display': 'none'}, False
    elif connection_type == "ssh":
        return {'display': 'none'}, {'display': True}, {'display': 'none'}, False
    elif connection_type == "http":
        return {'display': 'none'}, {'display': 'none'}, {'display': True}, False

    return {'display': 'none'}, {'display': 'none'}, {'display': 'none'}, True


# Connection test callbacks
@callback(
    [Output("source-status", "children"),
     Output("source-config-store", "data")],
    [Input("source-test-btn", "n_clicks")],
    [State("source-type", "value"),
     State("source-path", "value"),
     State("source-hostname", "value"),
     State("source-ssh-path", "value"),
     State("source-username", "value"),
     State("source-password", "value"),
     State("source-url", "value")],
    prevent_initial_call=True
)
def test_source_connection(n_clicks, connection_type, path, hostname, ssh_path, username, password, url):
    if not n_clicks:
        return "", {}

    try:
        config = {"type": connection_type}

        if connection_type == "local":
            if not path:
                return dbc.Alert(
                    [html.I(className="bi bi-exclamation-triangle-fill me-2"), "Please enter a database path"],
                    color="warning"), {}
            config["spec"] = path
            # Test if path exists and is valid
            test_path = Path(path)
            if not test_path.exists():
                return dbc.Alert([html.I(className="bi bi-x-octagon-fill me-2"), "Path does not exist"],
                                 color="danger"), {}

        elif connection_type == "ssh":
            if not all([hostname, ssh_path, username]):
                return dbc.Alert(
                    [html.I(className="bi bi-exclamation-triangle-fill me-2"), "Please fill in all SSH fields"],
                    color="warning"), {}
            config["spec"] = f"ssh://{hostname}:{ssh_path}"
            config["hostname"] = hostname
            config["username"] = username
            config["password"] = password

            # Test SSH connection
            try:
                client = paramiko.SSHClient()
                client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                client.connect(hostname=hostname, username=username, password=password, timeout=5)
                client.close()
            except Exception as e:
                return dbc.Alert([html.I(className="bi bi-x-octagon-fill me-2"), f"SSH connection failed: {str(e)}"],
                                 color="danger"), {}

        elif connection_type == "http":
            if not url:
                return dbc.Alert([html.I(className="bi bi-exclamation-triangle-fill me-2"), "Please enter an API URL"],
                                 color="warning"), {}
            config["spec"] = url
            config.update(config_from_url(url))

            # Test HTTP connection
            try:
                response = requests.get(f"{url}/health/", timeout=5)
                response.raise_for_status()
            except Exception as e:
                return dbc.Alert([html.I(className="bi bi-x-octagon-fill me-2"), f"HTTP connection failed: {str(e)}"],
                                 color="danger"), {}

        return dbc.Alert([html.I(className="bi bi-check-circle-fill me-2"), "Connection successful!"],
                         color="success"), config

    except Exception as e:
        return dbc.Alert([html.I(className="bi bi-x-octagon-fill me-2"), f"Connection test failed: {str(e)}"],
                         color="danger"), {}


@callback(
    [Output("target-status", "children"),
     Output("target-config-store", "data")],
    [Input("target-test-btn", "n_clicks")],
    [State("target-type", "value"),
     State("target-path", "value"),
     State("target-hostname", "value"),
     State("target-ssh-path", "value"),
     State("target-username", "value"),
     State("target-password", "value"),
     State("target-url", "value")],
    prevent_initial_call=True
)
def test_target_connection(n_clicks, connection_type, path, hostname, ssh_path, username, password, url):
    if not n_clicks:
        return "", {}

    try:
        config = {"type": connection_type}

        if connection_type == "local":
            if not path:
                return dbc.Alert(
                    [html.I(className="bi bi-exclamation-triangle-fill me-2"), "Please enter a database path"],
                    color="warning"), {}
            config["spec"] = path
            # Test if path exists, create if it doesn't
            test_path = Path(path)
            test_path.mkdir(parents=True, exist_ok=True)

        elif connection_type == "ssh":
            if not all([hostname, ssh_path, username]):
                return dbc.Alert(
                    [html.I(className="bi bi-exclamation-triangle-fill me-2"), "Please fill in all SSH fields"],
                    color="warning"), {}
            config["spec"] = f"ssh://{hostname}:{ssh_path}"
            config["hostname"] = hostname
            config["username"] = username
            config["password"] = password

            # Test SSH connection
            try:
                client = paramiko.SSHClient()
                client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                client.connect(hostname=hostname, username=username, password=password, timeout=5)
                client.close()
            except Exception as e:
                return dbc.Alert([html.I(className="bi bi-x-octagon-fill me-2"), f"SSH connection failed: {str(e)}"],
                                 color="danger"), {}

        elif connection_type == "http":
            if not url:
                return dbc.Alert([html.I(className="bi bi-exclamation-triangle-fill me-2"), "Please enter an API URL"],
                                 color="warning"), {}
            config["spec"] = url
            config.update(config_from_url(url))

            # Test HTTP connection
            try:
                response = requests.get(f"{url}/health", timeout=5)
                response.raise_for_status()
            except Exception as e:
                return dbc.Alert([html.I(className="bi bi-x-octagon-fill me-2"), f"HTTP connection failed: {str(e)}"],
                                 color="danger"), {}

        return dbc.Alert([html.I(className="bi bi-check-circle-fill me-2"), "Connection successful!"],
                         color="success"), config

    except Exception as e:
        return dbc.Alert([html.I(className="bi bi-x-octagon-fill me-2"), f"Connection test failed: {str(e)}"],
                         color="danger"), {}


# Enable load scans button when both connections are tested
@callback(
    Output("load-scans-btn", "disabled"),
    Input("source-config-store", "data"),
    Input("target-config-store", "data")
)
def enable_load_scans(source_config, target_config):
    return not (source_config and target_config)


def get_scans_for_db(config):
    """Get list of scans for a database configuration"""

    def _try_refresh(url):
        try:
            response = make_api_request(url)
        except Exception as e:
            refreshed = False
        else:
            refreshed = response.status_code == 200
        return refreshed

    try:
        if config["type"] == "local":
            path = Path(config["spec"])
            db = FSDB(path)
            db.connect(unsafe=True)
            return db.list_scans(owner_only=False)
        elif config["type"] == "http":
            from plantdb.client.rest_api import refresh_url
            from plantdb.client.rest_api import make_api_request
            url = refresh_url(**config)
            refreshed = _try_refresh(url)
            n_attempts = 0
            while not refreshed:
                if n_attempts >= 3:
                    break
                n_attempts += 1
                time.sleep(5)
                refreshed = _try_refresh(url)
            return list_scan_names(**config)
        elif config["type"] == "ssh":
            # For SSH, we'll need to use the sync instance's SSH methods
            # This is a simplified version - in practice you'd use the FSDBSync methods
            return []
        return []
    except Exception:
        return []


# Load scan lists and handle search functionality in one callback
@callback(
    [Output("source-scans", "options"),
     Output("target-scans", "options"),
     Output("scan-data-store", "data"),
     Output("source-scans-info", "children"),
     Output("target-scans-info", "children")],
    [Input("load-scans-btn", "n_clicks"),
     Input("source-search", "value"),
     Input("source-search-clear", "n_clicks")],
    [State("source-config-store", "data"),
     State("target-config-store", "data"),
     State("scan-data-store", "data")],
    prevent_initial_call=True
)
def update_scan_lists(load_clicks, search_value, clear_clicks, source_config, target_config, scan_data):
    ctx_id = ctx.triggered_id

    # Handle clear search button
    if ctx_id == "source-search-clear":
        search_value = ""

    # If load button was clicked, reload the scan data
    if ctx_id == "load-scans-btn":
        if not load_clicks or not source_config or not target_config:
            return [], [], {}, "", ""

        try:
            # Create FSDBSync instance
            global sync_instance
            sync_instance = FSDBSync(source_config["spec"], target_config["spec"])

            # Set SSH credentials if needed
            if source_config.get("hostname"):
                sync_instance.set_ssh_credentials(
                    source_config["hostname"],
                    source_config["username"],
                    source_config.get("password")
                )
            if target_config.get("hostname"):
                sync_instance.set_ssh_credentials(
                    target_config["hostname"],
                    target_config["username"],
                    target_config.get("password")
                )

            # Get scan lists based on database types
            source_scans = get_scans_for_db(source_config)
            target_scans = get_scans_for_db(target_config)

            scan_data = {
                "source_scans": source_scans,
                "target_scans": target_scans
            }

            # Create options for source scans (no filtering yet)
            source_options = []
            for scan in source_scans:
                is_disabled = scan in target_scans
                source_options.append({
                    "label": html.Span(
                        scan,
                        style={"color": "gray" if is_disabled else "black"}
                    ),
                    "value": scan,
                    "disabled": is_disabled
                })

            # Create options for target scans (read-only)
            target_options = [
                {"label": scan, "value": scan, "disabled": True}
                for scan in target_scans
            ]

            source_info = f"Found {len(source_scans)} scans ({len([s for s in source_scans if s not in target_scans])} available to sync)"
            target_info = f"Found {len(target_scans)} scans"

            return source_options, target_options, scan_data, source_info, target_info

        except Exception as e:
            error_msg = f"Error loading scans: {str(e)}"
            return [], [], {}, error_msg, error_msg

    # Handle search filtering (when search_value changes or clear is clicked)
    elif ctx_id in ["source-search", "source-search-clear"]:
        if not scan_data:
            return [], [], {}, "", ""

        source_scans = scan_data.get("source_scans", [])
        target_scans = scan_data.get("target_scans", [])

        # Filter scans based on search value
        if search_value:
            filtered_scans = [scan for scan in source_scans if search_value.lower() in scan.lower()]
        else:
            filtered_scans = source_scans

        # Create filtered options for source scans
        source_options = []
        for scan in filtered_scans:
            is_disabled = scan in target_scans
            source_options.append({
                "label": html.Span(
                    scan,
                    style={"color": "gray" if is_disabled else "black"}
                ),
                "value": scan,
                "disabled": is_disabled
            })

        # Target options remain the same
        target_options = [
            {"label": scan, "value": scan, "disabled": True}
            for scan in target_scans
        ]

        source_info = f"Showing {len(filtered_scans)} of {len(source_scans)} scans"
        target_info = f"Found {len(target_scans)} scans"

        return source_options, target_options, scan_data, source_info, target_info

    # Default return (shouldn't reach here normally)
    return [], [], {}, "", ""


# Separate callback for clearing search input
@callback(
    Output("source-search", "value"),
    Input("source-search-clear", "n_clicks"),
    prevent_initial_call=True
)
def clear_search_input(n_clicks):
    return ""


# Enable sync button when scans are selected
@callback(
    Output("sync-btn", "disabled"),
    Input("source-scans", "value")
)
def enable_sync_button(selected_scans):
    return not selected_scans


# Auto-restore source configuration from store
@callback(
    [Output("source-type", "value"),
     Output("source-path", "value"),
     Output("source-hostname", "value"),
     Output("source-ssh-path", "value"),
     Output("source-username", "value"),
     Output("source-password", "value"),
     Output("source-url", "value")],
    Input("source-config-store", "data"),
    prevent_initial_call=False
)
def restore_source_config(config_data):
    if not config_data:
        return "local", "", "", "", "", "", ""

    db_type = config_data.get("type", "local")

    # Initialize all fields with empty values
    path = hostname = ssh_path = username = password = url = ""

    if db_type == "local":
        path = config_data.get("spec", "")
    elif db_type == "ssh":
        hostname = config_data.get("hostname", "")
        ssh_path = config_data.get("spec", "").replace(f"ssh://{hostname}:", "") if hostname else ""
        username = config_data.get("username", "")
        password = config_data.get("password", "")
    elif db_type == "http":
        url = config_data.get("spec", "")

    return db_type, path, hostname, ssh_path, username, password, url


# Auto-restore target configuration from store
@callback(
    [Output("target-type", "value"),
     Output("target-path", "value"),
     Output("target-hostname", "value"),
     Output("target-ssh-path", "value"),
     Output("target-username", "value"),
     Output("target-password", "value"),
     Output("target-url", "value")],
    Input("target-config-store", "data"),
    prevent_initial_call=False
)
def restore_target_config(config_data):
    if not config_data:
        return "local", "", "", "", "", "", ""

    db_type = config_data.get("type", "local")

    # Initialize all fields with empty values
    path = hostname = ssh_path = username = password = url = ""

    if db_type == "local":
        path = config_data.get("spec", "")
    elif db_type == "ssh":
        hostname = config_data.get("hostname", "")
        ssh_path = config_data.get("spec", "").replace(f"ssh://{hostname}:", "") if hostname else ""
        username = config_data.get("username", "")
        password = config_data.get("password", "")
    elif db_type == "http":
        url = config_data.get("spec", "")

    return db_type, path, hostname, ssh_path, username, password, url


# Synchronization process
@callback(
    Output("sync-progress", "value"),
    Output("sync-logs", "children"),
    Output("interval-component", "disabled"),
    Input("sync-btn", "n_clicks"),
    Input("interval-component", "n_intervals"),
    State("source-scans", "value"),
    prevent_initial_call=True
)
def start_synchronization(sync_clicks, n_intervals, selected_scans):
    global sync_instance

    if sync_instance is None:
        return 0., "", True

    if ctx.triggered_id == "sync-btn":
        if not sync_instance or not selected_scans:
            logs = [html.P(
                [html.I(className="bi bi-exclamation-triangle-fill me-2"), "No scans selected for synchronization"])]
            return 0., logs, True

        try:
            # Start a synchronization process
            logs = [
                html.P([html.I(className="bi bi-arrow-repeat me-1"), f"Synchronizing {len(selected_scans)} scans..."])]

            # Start the sync operation
            sync_instance.sync(thread=True, source_scans=selected_scans)
            return sync_instance.sync_progress, logs, False

        except Exception as e:
            error_msg = f"Sync failed!\n{str(e)}\n{traceback.format_exc()}"
            return 0., [html.Pre(error_msg)], True

    elif ctx.triggered_id == "interval-component":
        # Update progress during sync
        progress = sync_instance.sync_progress

        # Disable 'interval-component' when finished (prevents infinite polling)
        if progress >= 100.:
            logs = [
                html.P([html.I(className="bi bi-check2-all me-1"), "All selected scans synchronized successfully."])]
            return 100., logs, True

        logs = [html.P([html.I(className="bi bi-percent me-1"), f"Progress: {progress}%"])]
        return progress, logs, False

    return 0., "", True


def parsing():
    parser = argparse.ArgumentParser()

    parser.add_argument("--port", type=int, default=8050)
    parser.add_argument("--debug", action="store_true")
    return parser


def main():
    parser = parsing()
    args = parser.parse_args()
    app.run(host="0.0.0.0", port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
