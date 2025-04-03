import sys
sys.path.append("../bfabric-web-apps")

import bfabric_web_apps
from bfabric_web_apps.utils.redis_queue import q

from dash import Input, Output, State, html, dcc, dash_table, callback, no_update
import dash.exceptions
import dash_bootstrap_components as dbc
import pandas as pd

from generic.callbacks import app
from generic.components import no_auth

import os
from ExecuteRunMainJob import (
    read_file_as_bytes,
    create_resource_paths
)
from GetDataFromBfabric import (
    load_samplesheet_data_when_loading_app
)

# ------------------------------------------------------------------------------
# Sidebar Components: Lane Dropdown, Queue Selection Dropdown, and Submit Button (Run Main Job)
# ------------------------------------------------------------------------------

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
    html.H2("Welcome to the Demultiplex App"),
    html.P(
        "This demultiplex app is built on the redis_index.py template from the "
        "bfabric_web_app_templates repository. It streamlines the process of managing "
        "and executing Nextflow pipelines for demultiplexing tasks."
    ),
    html.Br(),
    html.H4("1. Bfabric Integration"),
    html.P(
        "The GetDataFromBfabric.py module is responsible for all API interactions with Bfabric. "
        "It retrieves necessary metadata and sample information, and creates the samplesheets required "
        "to run the Nextflow pipeline."
    ),
    html.Br(),
    html.H4("2. User Interface"),
    html.P(
        "The GetDataFromUser.py module provides a user-friendly interface that allows users to view, edit, "
        "and manage the samplesheets. Users can alter sample details, change entries, or delete them, ensuring "
        "that the samplesheet data is accurate before pipeline execution."
    ),
    html.Br(),
    html.H4("3. Execution of the Main Job"),
    html.P(
        "The ExecuteRunMainJob.py module contains the helper functions and logic to run the main job pipeline. "
        "It manages bash command execution, resource path creation, and job queuing via Redis, facilitating "
        "asynchronous processing of the demultiplexing workflow."
    ),
    html.Br(),
    html.P(
        "Together, these three parts—Bfabric integration, a comprehensive user interface, and robust job execution—"
        "form a cohesive system designed to support efficient demultiplexing workflows."
    )
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
    """
    Toggle the visibility of the modal confirmation dialog.

    Args:
        n1 (int): Number of clicks on the example button.
        n2 (int): Number of clicks on the Submit button inside the modal.
        is_open (bool): Current state of the modal (open or closed).

    Returns:
        bool: The new state of the modal (True for open, False for closed).
    """
    if n1 or n2:
        return not is_open
    return is_open


# ---------------------------
# Callback: Update UI
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
    """
    Update the user interface based on the authentication status.

    Args:
        - token_data (dict): Token data for the current session.
        - entity_data (dict): Entity data representing the authenticated user.

    Returns:
        - (bool): Whether the example button is disabled.
        - (bool): Whether the bug report submit button is disabled.
        - (bool): Whether the Submit button is disabled.
        - (dash_html_components.Div): Content for the auth-div (either authentication message or DataTable).
    """
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
    """
    Update the lane dropdown options based on the list of available CSV files.

    Args:
        csv_list (list): A list of CSV filenames representing lanes.

    Returns:
        list: A list of dictionaries for dropdown options (e.g., [{"label": "Lane 1", "value": 0}, ...]).
    """
    # csv_list is expected to be a list of CSV filenames.
    if not csv_list or not isinstance(csv_list, list):
        dropdown_options = [{"label": "Lane 1", "value": 0}]
    else:
        dropdown_options = [{"label": f"Lane {i+1}", "value": i} for i in range(len(csv_list))]

        return dropdown_options


# ---------------------------
# Callback: Update Samplesheet Title Based on Lane Selection
# ---------------------------
@app.callback(
    Output("samplesheet-title", "children"),
    Input("lane-dropdown", "value"),
    prevent_initial_call=True
)
def update_samplesheet_title(lane_value):
    """
    Update the title of the samplesheet based on the selected lane.

    Args:
        lane_value (int or None): The index of the selected lane.

    Returns:
        str: A string title for the samplesheet (e.g., "Samples Lane 1").
    """
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
    """
    Highlight the selected columns in the samplesheet table.

    Args:
        selected_columns (list): List of column IDs that are selected.

    Returns:
        list: A list of style dictionaries to apply background color to selected columns.
    """
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
    """
    Save the current samplesheet data to the appropriate CSV file when the lane selection changes.

    Args:
        new_lane (int): The newly selected lane index.
        prev_lane (int): The previously selected lane index.
        table_data (list): List of dictionaries representing the samplesheet data.
        selected_rows (list): List of indices for rows that are selected.
        csv_list (list): List of CSV file paths corresponding to lanes.

    Returns:
        int: The new lane value, stored for future reference.
    """
    if prev_lane is not None and csv_list:
        # Get the CSV path for the lane that is about to be switched out.
        csv_path = csv_list[prev_lane]
        update_csv_based_on_ui(table_data, selected_rows, csv_path)
    # Return the new lane as the "previous" lane for the next change.
    return new_lane


# ------------------------------------------------------------------------------
# Function: Update CSV Based on UI Data
# ------------------------------------------------------------------------------
def update_csv_based_on_ui(table_data, selected_rows, csv_path):
    """
    Update the CSV file based on the user-edited table data from the UI.

    This function performs the following steps:
      1. Converts the table data (provided as a list of dictionaries) into a Pandas DataFrame.
      2. Filters the DataFrame based on the selected row indices; if no rows are selected,
         the DataFrame is set to empty.
      3. Reads the entire CSV file from the specified path.
      4. Locates the "[Data]" section within the CSV file.
      5. Preserves the lines up to and including the header of the data section.
      6. Reconstructs the data rows using the updated DataFrame values while ensuring that
         the data aligns with the original CSV header columns.
      7. Writes the reassembled CSV content back to the file.

    Args:
        table_data (list): List of dictionaries representing the current state of the samplesheet table.
        selected_rows (list): List of indices indicating which rows are selected for updating.
        csv_path (str): Path to the CSV file that needs to be updated.

    Returns:
        str: An error message if the "[Data]" section or its header is not found in the CSV.
             If successful, the CSV file is updated in-place and the function returns None.
    """
    # Convert the table data (edited values from the UI) to a DataFrame. 
    updated_df = pd.DataFrame(table_data)
    if selected_rows is None or len(selected_rows) == 0:
        updated_df = updated_df.iloc[0:0]
    else:
        updated_df = updated_df.iloc[selected_rows]

    # Read the full original CSV file as a list of lines using the provided path.
    with open(csv_path, 'r', encoding='utf-8') as f:
        all_lines = f.readlines()

    # Locate the line where the "[Data]" marker is.
    data_marker_index = None
    for i, line in enumerate(all_lines):
        if line.strip().startswith("[Data]"):
            data_marker_index = i
            break
    if data_marker_index is None:
        return "Error: [Data] section not found in CSV."

    # Ensure there is a header line after the [Data] marker.
    if len(all_lines) <= data_marker_index + 1:
        return "Error: Data header line missing in CSV."

    # Preserve all lines up to and including the original data header line.
    preserved_lines = all_lines[:data_marker_index + 2]

    # Extract the original header columns.
    orig_header_line = all_lines[data_marker_index + 1]
    orig_header_cols = orig_header_line.strip().split(",")

    # Build new data rows aligned with the original header.
    new_data_rows = []
    for _, row in updated_df.iterrows():
        new_row = []
        for col in orig_header_cols:
            if col in updated_df.columns:
                new_row.append(str(row[col]))
            else:
                new_row.append("")
        new_data_rows.append(",".join(new_row) + "\n")

    new_data_csv = "".join(new_data_rows)
    new_file_content = "".join(preserved_lines) + new_data_csv

    # Write the reassembled content back to the CSV file.
    with open(csv_path, 'w', encoding='utf-8', newline='') as f:
        f.write(new_file_content)