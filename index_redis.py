# Ensure version compatibility between bfabric_web_apps and bfabric_web_app_template.
# Both must be the same version to avoid compatibility issues.
# Example: If bfabric_web_apps is version 0.1.3, bfabric_web_app_template must also be 0.1.3.
# Verify and update versions accordingly before running the application.

import sys
sys.path.append("../bfabric-web-apps")

from dash import Input, Output, State, html, dcc, dash_table, callback, no_update
import dash.exceptions
import dash_bootstrap_components as dbc
import bfabric_web_apps
from generic.callbacks import app
from generic.components import no_auth
from pathlib import Path
from bfabric_web_apps import run_main_job
from bfabric_web_apps.utils.redis_queue import q
import os
import csv
import pandas as pd
from io import StringIO
from demultiplex_functions import (
    parse_samplesheet_data_only,
    read_file_as_bytes,
    load_samplesheet_data_when_loading_app,
    update_csv_bfore_runing_main_job,
    create_resource_paths
)

bfabric_web_apps.CONFIG_FILE_PATH = "~/.bfabricpy.yml"
bfabric_web_apps.DEVELOPER_EMAIL_ADDRESS = "griffin@gwcustom.com"
bfabric_web_apps.BUG_REPORT_EMAIL_ADDRESS = "gwtools@fgcz.system"

# Here we define the sidebar of the UI, including the clickable components like dropdown and slider. 
sidebar = [
    html.P(id="sidebar_text_3", children="Submit job to which queue?"),  # Text for the queue selection.
    dcc.Dropdown(
        options=[
            {'label': 'light', 'value': 'light'},
            {'label': 'heavy', 'value': 'heavy'}
        ],
        value='light',
        id='queue'
    ),
    html.Br(),
    dbc.Button('Submit', id='example-button'),  # Button for submission.
]

# here we define the modal that will pop up when the user clicks the submit button.
modal = html.Div([
    dbc.Modal([
        dbc.ModalHeader(dbc.ModalTitle("Ready to Prepare Create Workunits?")),
        dbc.ModalBody("Are you sure you're ready to create workunits?"),
        dbc.ModalFooter(dbc.Button("Yes!", id="Submit", className="ms-auto", n_clicks=0)),],
    id="modal-confirmation",
    is_open=False,),
])

# Here are the alerts which will pop up when the user creates workunits 
alerts = html.Div(
    [
        dbc.Alert("Success: Workunit created!", color="success", id="alert-fade-success", dismissable=True, is_open=False),
        dbc.Alert("Error: Workunit creation failed!", color="danger", id="alert-fade-fail", dismissable=True, is_open=False),
    ], style={"margin": "20px"}
)

# Here we define a Dash layout, which includes the sidebar, and the main content of the app. 
app_specific_layout = dbc.Row(
    id="page-content-main",
    children=[
        dcc.Loading(alerts),
        modal,
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
                    html.Div(id="auth-div"),
                ],
            ),
            width=9,
        ),
            dcc.Store(id='sample_data', data=[])
    ],
    style={"margin-top": "0px", "min-height": "40vh"},
)

# Here we define the documentation content for the app.
documentation_content = [
    html.H2("Welcome to Bfabric App Template"),
    html.P(
        [
            "This app serves as the user-interface for Bfabric App Template, "
            "a versatile tool designed to help build and customize new applications."
        ]
    ),
    html.Br(),
    html.P(
        [
            "It is a simple application which allows you to bulk-create resources, "
            "workunits, and demonstrates how to use the bfabric-web-apps library."
        ]
    ),
    html.Br(),
    html.P(
        [
            "Please check out the official documentation of ",
            html.A("Bfabric Web Apps", href="https://bfabric-docs.gwc-solutions.ch/index.html"),
            "."
        ]
    )
]

app_title = "Bfabric App Template"

# here we use the get_static_layout function from bfabric_web_apps to set up the app layout.
app.layout = bfabric_web_apps.get_static_layout(         # The function from bfabric_web_apps that sets up the app layout.
    app_title,                          # The app title we defined previously
    app_specific_layout,     # The main content for the app defined in components.py
    documentation_content,    # Documentation content for the app defined in components.py
    layout_config={"workunits": True, "queue": True, "bug": True}  # Configuration for the layout
)

# This callback is necessary for the modal to pop up when the user clicks the submit button.
@app.callback(
    Output("modal-confirmation", "is_open"),
    [Input("example-button", "n_clicks"), Input("Submit", "n_clicks")],
    [State("modal-confirmation", "is_open")],
)
def toggle_modal(n1, n2, is_open):
    if n1 or n2:
        return not is_open
    return is_open

# This callback updates the UI based on the user's authentication status and the entity data.
@app.callback(
    [
        Output('example-button', 'disabled'),
        Output('submit-bug-report', 'disabled'),
        Output('Submit', 'disabled'),
        Output('auth-div', 'children'),
    ],
    [
        Input('token_data', 'data'),
    ],
    [State('entity', 'data')]
)
def update_ui(token_data, entity_data):

    # Determine sidebar and input states based on token_data and development mode.
    if token_data is None:
        sidebar_state = (True, True, True)
    elif not bfabric_web_apps.DEV:
        sidebar_state = (False, False, False)
    else:
        sidebar_state = (True, True, True)

    # Generate content for the auth-div based on authentication and entity data.
    if not entity_data or not token_data:
        auth_div_content = html.Div(
            children=no_auth,
            style={
                "margin-top": "20vh",
                "margin-left": "2vw",
                "font-size": "20px"
            }
        )
    else:
        try:
            samplesheet_table = dash_table.DataTable(
                id='samplesheet-table',
                data=[],        
                columns=[],     
                # Interactivity
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

                # Styling: remove `margin: '0 auto'` so it's left-aligned
                style_table={
                    'overflowX': 'auto',
                    'maxWidth': '90%'  # or '100%' if you prefer
                },
                style_cell={
                    'minWidth': '60px',
                    'width': '100px',
                    'maxWidth': '180px',
                    'whiteSpace': 'normal',
                },
            )

            auth_div_content = html.Div(
                children=[
                    html.H4("Samples"),
                    samplesheet_table
                ],
                style={
                    "margin-top": "1vw",
                    "margin-left": "2vw",
                    "margin-bottom": "2vw",
                }
            )

        except Exception as e:
            return (*sidebar_state, html.P(f"Error Logging into B-Fabric: {str}"))

    return (*sidebar_state, auth_div_content)



# Samplesheet and UI table

# This function creates the samplesheets when loading the application
@app.callback(
    Output("samplesheet-table", "data"),
    Output("samplesheet-table", "columns"),
    Output("samplesheet-table", "selected_rows"),  # new output
    Input("token_data", "data"),
)
def load_samplesheet_data(token_data):
    return load_samplesheet_data_when_loading_app(token_data)



# This function highlights the selected column
@app.callback(
    Output('samplesheet-table', 'style_data_conditional'),
    Input('samplesheet-table', 'selected_columns')
)
def highlight_selected_columns(selected_columns):
    return [
        {
            'if': {'column_id': col},
            'background_color': '#D2F3FF'
        }
        for col in selected_columns
    ]


#Run Pipeline Funciton

# This callback creates workunits and resources based on the user's input, and displays the corresponding alert, based on success or failure.
@app.callback(
    [
        Output("alert-fade-success", "is_open"), 
        Output("alert-fade-fail", "is_open"), 
        Output("alert-fade-fail", "children"),
        Output("refresh-workunits", "children")
    ],
    [Input("Submit", "n_clicks")],
    [
        State('token', 'data'),
        State("token_data", "data"),
        State("queue", "value"),
        State('samplesheet-table', 'data'),
        State('samplesheet-table', 'selected_rows'),
        State('sample_data', 'data')
    ],
    prevent_initial_call=True
)
def run_main_job(n_clicks, token, token_data, queue, table_data, selected_rows, sample_dict):

    update_csv_bfore_runing_main_job(n_clicks, table_data, selected_rows)

    # file_as_bytes = read_file_as_bytes("C:/Users/marc_/Desktop/Test_Pipeline_Run/From/test.csv")
    samplesheet_as_bytes = read_file_as_bytes("./Samplesheet.csv")
    pipeline_samplesheet_as_bytes = read_file_as_bytes("./pipeline_samplesheet.csv")
    NFC_DMC_config_as_bytes = read_file_as_bytes("./NFC_DMX.config")

    # Define file paths (Remote -> Local)
    files_as_byte_strings = {
        "./Samplesheet.csv": samplesheet_as_bytes,
        "./pipeline_samplesheet.csv": pipeline_samplesheet_as_bytes,
        "./NFC_DMX.config": NFC_DMC_config_as_bytes
    }

    bash_commands = [
    """\
    /home/nfc/.local/bin/nextflow run nf-core/demultiplex \
    -profile docker \
    --input /APPLICATION/200611_A00789R_0071_BHHVCCDRXX/pipeline_samplesheet.csv \
    --outdir /STORAGE/OUTPUT_TEST \
    --demultiplexer bcl2fastq \
    --skip_tools samshee,checkqc \
    -c /APPLICATION/200611_A00789R_0071_BHHVCCDRXX/NFC_DMX.config \
    -r 1.5.4 > /STORAGE/nextflow.log 2>&1 &
    """
    ]
    
    resource_paths = create_resource_paths(sample_dict)
    print("resource_paths", resource_paths)
    #resource_paths = {"./test.txt": 2220,"./test1.txt": 2220} # The recource path to file or folder as key and the container_id as value.

    attachment_paths = {"/STORAGE/OUTPUT_TEST/multiqc/multiqc_report.html": "multiqc_report.html"}

    if queue == "heavy":
        q('heavy').enqueue(run_main_job, kwargs={"files_as_byte_strings": files_as_byte_strings, "bash_commands": bash_commands, "resource_paths": resource_paths, "attachment_paths": attachment_paths, "token": token})
    else:
        q('light').enqueue(run_main_job, kwargs={"files_as_byte_strings": files_as_byte_strings, "bash_commands": bash_commands, "resource_paths": resource_paths, "attachment_paths": attachment_paths, "token": token})


# Here we run the app on the specified host and port.
if __name__ == "__main__":
    app.run_server(debug=True, port=bfabric_web_apps.PORT, host=bfabric_web_apps.HOST)