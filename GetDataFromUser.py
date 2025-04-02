# ---------------------------
# Updated UI and Callbacks
# ---------------------------
import sys
sys.path.append("../bfabric-web-apps")

import bfabric_web_apps
from bfabric_web_apps.utils.redis_queue import q

from dash import Input, Output, State, html, dcc, dash_table, callback, no_update
import dash.exceptions
import dash_bootstrap_components as dbc

from generic.callbacks import app
from generic.components import no_auth
from pathlib import Path

import os
import csv
import pandas as pd
from io import StringIO
from ExecuteRunMainJob import (
    read_file_as_bytes,
    create_resource_paths
)
from GetDataFromBfabric import (
    load_samplesheet_data_when_loading_app,
    parse_samplesheet_data_only,
    update_csv_bfore_runing_main_job
)

# ---------------------------
# Sidebar with Lane Dropdown
# ---------------------------
sidebar = [
    html.P("Select Lane:"),  # Label for lane selection
    dcc.Dropdown(
        id="lane-dropdown",
        options=[],   # Options will be updated dynamically based on created CSV files
        value=None,      # Default to the first lane
    ),
    html.Br(),
    html.P(id="sidebar_text_3", children="Submit job to which queue?"),
    dcc.Dropdown(
        options=[
            {'label': 'light', 'value': 'light'},
            {'label': 'heavy', 'value': 'heavy'}
        ],
        value='light',
        id='queue'
    ),
    html.Br(),
    dbc.Button('Submit', id='example-button'),
]

# ---------------------------
# Main Layout Definition
# ---------------------------
# ---------------------------
# Main Layout Definition
# ---------------------------
app_specific_layout = dbc.Row(
    id="page-content-main",
    children=[
        dcc.Loading(
            html.Div(
                id="alerts-container",
                children=[
                    dbc.Alert(
                        "Success: Pipeline started successfully!",
                        color="success",
                        id="alert-fade-success",
                        dismissable=True,
                        is_open=False
                    ),
                    dbc.Alert(
                        "Error: Pipeline start failed!",
                        color="danger",
                        id="alert-fade-fail",
                        dismissable=True,
                        is_open=False
                    ),
                ]
            )
        ),
        html.Div([
            dbc.Modal([
                dbc.ModalHeader(dbc.ModalTitle("Ready to Prepare Create Workunits?")),
                dbc.ModalBody("Are you sure you're ready to create workunits?"),
                dbc.ModalFooter(
                    dbc.Button("Yes!", id="Submit", className="ms-auto", n_clicks=0)
                ),
            ], id="modal-confirmation", is_open=False),
        ]),
        dbc.Col(
            html.Div(
                id="sidebar",
                children=sidebar,
                style={
                    "border-right": "2px solid #d4d7d9",
                    "height": "100%",
                    "padding": "20px",
                    "font-size": "20px"
                }
            ),
            width=3,
        ),
        dbc.Col(
            html.Div(
                id="page-content",
                children=[
                    # This is where the unauthenticated message or user UI is inserted:
                    html.Div(id="auth-div"),

                    # IMPORTANT: Add a hidden placeholder DataTable so it always exists
                    dash_table.DataTable(
                        id='samplesheet-table',
                        data=[],
                        columns=[],
                        style_table={'display': 'none'},  # Hide initially
                    )
                ],
            ),
            width=9,
        ),
        dcc.Store(id='csv_list_store', data=[]),
        dcc.Store(id='previous-lane-store', data=0),
    ],
    style={"margin-top": "0px", "min-height": "40vh"},
)


documentation_content = [
    html.H2("Welcome to Bfabric App Template"),
    html.P("This app serves as the user-interface for Bfabric App Template, "
           "a versatile tool designed to help build and customize new applications."),
    html.Br(),
    html.P("It is a simple application which allows you to bulk-create resources, "
           "workunits, and demonstrates how to use the bfabric-web-apps library."),
    html.Br(),
    html.P(["Please check out the official documentation of ",
            html.A("Bfabric Web Apps", href="https://bfabric-docs.gwc-solutions.ch/index.html"),
            "."])
]

app_title = "Run Demultiplex UI"

app.layout = bfabric_web_apps.get_static_layout(
    app_title,
    app_specific_layout,
    documentation_content,
    layout_config={"workunits": True, "queue": True, "bug": True}
)


# ---------------------------
# Callback: Toggle the Modal
# ---------------------------
@app.callback(
    Output("modal-confirmation", "is_open"),
    [Input("example-button", "n_clicks"), Input("Submit", "n_clicks")],
    [State("modal-confirmation", "is_open")],
)
def toggle_modal(n1, n2, is_open):
    if n1 or n2:
        return not is_open
    return is_open

# ---------------------------
# Callback: Update UI Based on Authentication
# ---------------------------
@app.callback(
    [
        Output('example-button', 'disabled'),
        Output('submit-bug-report', 'disabled'),
        Output('Submit', 'disabled'),
        Output('auth-div', 'children'),
    ],
    [Input('token_data', 'data')],
    [State('entity', 'data'),]
)
def update_ui(token_data, entity_data):
    if token_data is None:
        sidebar_state = (True, True, True)
    elif not bfabric_web_apps.DEV:
        sidebar_state = (False, False, False)
    else:
        sidebar_state = (True, True, True)

    if not entity_data or not token_data:
        auth_div_content = html.Div(
            children=no_auth,
            style={"margin-top": "20vh", "margin-left": "2vw", "font-size": "20px"}
        )

    else:
        try:
            samplesheet_table = dash_table.DataTable(
                id='samplesheet-table',
                data=[],        
                columns=[],     
                editable=True,
                sort_action="native",
                sort_mode="multi",
                column_selectable="single",
                row_selectable="multi",
                row_deletable=False,
                selected_columns=[],
                selected_rows=[],
                page_action="native",
                page_current=0,
                page_size=15,
                style_table={'overflowX': 'auto', 'maxWidth': '90%'},
                style_cell={'minWidth': '60px', 'width': '100px', 'maxWidth': '180px', 'whiteSpace': 'normal'},
            )
            auth_div_content = html.Div(
                children=[
                    html.H4(
                        id="samplesheet-title",
                        children="",
                        style={
                            "fontSize": "18px",       # Smaller text size
                            "marginTop": "50px",      # A bit lower
                            "textAlign": "left"     # Center aligned
                        }
                    ),
                    samplesheet_table
                ]
                ,
                style={"margin-top": "1vw", "margin-left": "2vw", "margin-bottom": "2vw"}
            )
        except Exception as e:
            return (*sidebar_state, html.P(f"Error Logging into B-Fabric: {str(e)}"))

    return (*sidebar_state, auth_div_content)

# ---------------------------
# Callback: Update Lane Dropdown Options Based on Created CSV Files
# ---------------------------
@app.callback(
    Output("lane-dropdown", "options"),
    Input("csv_list_store", "data"),
    prevent_initial_call=True
)
def update_lane_dropdown_options(csv_list):
    # csv_list is expected to be a list of CSV filenames.
    if not csv_list or not isinstance(csv_list, list):
        dropdown_options = [{"label": "Lane 1", "value": 0}]
    else:
        dropdown_options = [{"label": f"Lane {i+1}", "value": i} for i in range(len(csv_list))]

        return dropdown_options

# Callback: Load Samplesheet Data Based on Token, Lane Selection, and Created CSV Files
# ---------------------------
@app.callback(
    Output("samplesheet-table", "data"),
    Output("samplesheet-table", "columns"),
    Output("samplesheet-table", "selected_rows"),
    State("token_data", "data"),
    Input("lane-dropdown", "value"),
    Input("csv_list_store", "data"),  # Use the dynamically created CSV list,
)
def load_samplesheet_data(token_data, lane_value, csv_list):
    if not csv_list or not isinstance(csv_list, list):
        raise dash.exceptions.PreventUpdate("No CSV list available.")
    return load_samplesheet_data_when_loading_app(token_data, lane_value, csv_list)

# ---------------------------
# Callback: Update Samplesheet Title Based on Lane Selection
# ---------------------------
@app.callback(
    Output("samplesheet-title", "children"),
    Input("lane-dropdown", "value"),
    prevent_initial_call=True
)
def update_samplesheet_title(lane_value):
    try:
        lane_num = int(lane_value) + 1
    except (ValueError, TypeError):
        lane_num = 1
    return f"Samples Lane {lane_num}"

# ---------------------------
# Callback: Highlight Selected Columns in the DataTable
# ---------------------------
@app.callback(
    Output('samplesheet-table', 'style_data_conditional'),
    Input('samplesheet-table', 'selected_columns'),
    prevent_initial_call=True
)

def highlight_selected_columns(selected_columns):
    return [{'if': {'column_id': col}, 'background_color': '#D2F3FF'} for col in selected_columns]




@app.callback(
    Output("previous-lane-store", "data"),
    Input("lane-dropdown", "value"),
    State("previous-lane-store", "data"),
    State("samplesheet-table", "data"),
    State("samplesheet-table", "selected_rows"),
    State("csv_list_store", "data"),
    prevent_initial_call=True
)
def save_on_lane_change(new_lane, prev_lane, table_data, selected_rows, csv_list):
    if prev_lane is not None and csv_list:
        # Get the CSV path for the lane that is about to be switched out.
        csv_path = csv_list[prev_lane]
        update_csv_bfore_runing_main_job(table_data, selected_rows, csv_path)
    # Return the new lane as the "previous" lane for the next change.
    return new_lane