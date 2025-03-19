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
import pandas as pd
from io import StringIO

bfabric_web_apps.CONFIG_FILE_PATH = "~/.bfabricpy.yml"
bfabric_web_apps.DEVELOPER_EMAIL_ADDRESS = "griffin@gwcustom.com"
bfabric_web_apps.BUG_REPORT_EMAIL_ADDRESS = "gwtools@fgcz.system"

dropdown_options = ['Genomics (project 2220)', 'Proteomics (project 3000)', 'Metabolomics (project 31230)']
dropdown_values = ['2220', '3000', '31230']

# Here we define the sidebar of the UI, including the clickable components like dropdown and slider. 
sidebar = [
    html.P(id="sidebar_text", children="How Many Resources to Create?"),  # Sidebar header text.
    dcc.Slider(0, 10, 1, value=4, id='example-slider'),  # Slider for selecting a numeric value.
    html.Br(),
    html.P(id="sidebar_text_2", children="For Which Internal Unit?"),
    dcc.Dropdown(
        options=[{'label': option, 'value': value} for option, value in zip(dropdown_options, dropdown_values)],
        value=dropdown_options[0],
        id='example-dropdown'  # Dropdown ID for callback integration.
    ),
    html.Br(),
    html.P(id="sidebar_text_3", children="Submit job to which queue?"),  # Text for the input field.
    dcc.Dropdown(
        options=[
            {'label': 'light', 'value': 'light'},
            {'label': 'heavy', 'value': 'heavy'}
        ],
        value='light',
        id='queue'
    ),
    html.Br(),
    dbc.Input(value='Content of Resources', id='example-input'),  # Text input field.
    html.Br(),
    dbc.Button('Submit', id='example-button'),  # Button for user submission.
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
    ],
    style={"margin-top": "0px", "min-height": "40vh"}
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
        Output('sidebar_text', 'hidden'),
        Output('example-slider', 'disabled'),
        Output('example-dropdown', 'disabled'),
        Output('example-input', 'disabled'),
        Output('example-button', 'disabled'),
        Output('submit-bug-report', 'disabled'),
        Output('Submit', 'disabled'),
        Output('auth-div', 'children'),
    ],
    [
        Input('example-slider', 'value'),
        Input('example-dropdown', 'value'),
        Input('example-input', 'value'),
        Input('token_data', 'data'),
    ],
    [State('entity', 'data')]
)
def update_ui(slider_val, dropdown_val, input_val, token_data, entity_data):

    # Determine sidebar and input states based on token_data and development mode.
    if token_data is None:
        sidebar_state = (True, True, True, True, True, True, True)
    elif not bfabric_web_apps.DEV:
        sidebar_state = (False, False, False, False, False, False, False)
    else:
        sidebar_state = (True, True, True, True, True, True, True)

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
                    "margin-top": "5vh",
                    "margin-left": "2vw",
                    "font-size": "15px"
                }
            )

        except Exception as e:
            return (*sidebar_state, html.P(f"Error Logging into B-Fabric: {str}"))

    return (*sidebar_state, auth_div_content)


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
        State("example-slider", "value"),
        State("example-dropdown", "value"),
        State("example-input", "value"),
        State("token_data", "data"),
        State("queue", "value")
    ],
    prevent_initial_call=True
)
def run_main_job(token, n_clicks, slider_val, dropdown_val, input_val, token_data, queue):

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

    resource_paths = {"./test.txt": 2220,"./test1.txt": 2220} # The recource path to file or folder as key and the container_id as value.

    attachment_paths = {"./test_report.txt": "test_report.txt", "./another_test_report.txt": "another_test_report.txt"} # The recource path to file or folder as key and the container_id as value.

    q('light').enqueue(run_main_job, kwargs={"files_as_byte_strings": files_as_byte_strings, "bash_commands": bash_commands, "resource_paths": resource_paths, "attachment_paths": attachment_paths, "token": token})


def read_file_as_bytes(file_path, max_size_mb=400):
    """Reads any file type and stores it as a byte string in a dictionary."""
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)  # Convert bytes to MB
    if file_size_mb > max_size_mb:
        raise ValueError(f"File {file_path} exceeds {max_size_mb}MB limit ({file_size_mb:.2f}MB).")

    with open(file_path, "rb") as f:  # Read as bytes
        file_as_bytes = f.read()

    return file_as_bytes


@app.callback(
    Output("samplesheet-table", "data"),
    Output("samplesheet-table", "columns"),
    Output("samplesheet-table", "selected_rows"),  # new output
    Input("token_data", "data"),
)
def load_samplesheet_data(token_data):
    if not token_data:
        raise dash.exceptions.PreventUpdate
    
    csv_path = "Samplesheet.csv"
    if not os.path.isfile(csv_path):
        raise dash.exceptions.PreventUpdate("Samplesheet.csv doesn't exist yet.")
    
    df = parse_samplesheet_data_only(csv_path)
    if df.empty:
        return [], [], []
    
    # Build the DataTable columns such that only these four columns are editable
    editable_cols = {"I7_Index_ID", "index", "I5_Index_ID", "index2"}
    columns = []
    for col in df.columns:
        # If the column name is in editable_cols, set editable to True, otherwise False
        columns.append({
            "name": col,
            "id": col,
            "editable": (col in editable_cols)
        })
    
    data = df.to_dict("records")
    
    # By default, all rows are selected
    all_indices = list(range(len(df)))
    
    return data, columns, all_indices




def parse_samplesheet_data_only(filepath: str) -> pd.DataFrame:
    with open(filepath, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # Locate the [Data] line.
    data_start_idx = None
    for i, line in enumerate(lines):
        if line.strip().startswith("[Data]"):
            data_start_idx = i
            break
    
    # If [Data] not found, return empty DataFrame to avoid crashing.
    if data_start_idx is None:
        return pd.DataFrame()
    
    # The row right after "[Data]" is the actual CSV header line (Sample_ID, Sample_Name, etc)
    # Then the data rows follow after that.
    csv_lines = lines[data_start_idx + 1:]  # <-- use +1 here, NOT +2
    
    csv_string = "".join(csv_lines)
    df = pd.read_csv(StringIO(csv_string))
    
    # Drop columns that are completely empty (optional)
    df.dropna(axis=1, how='all', inplace=True)
    
    return df


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
@app.callback(
    Input('Submit', 'n_clicks'),
    State('samplesheet-table', 'data'),
    State('samplesheet-table', 'selected_rows')
)
def update_csv(n_clicks, table_data, selected_rows):
    if not n_clicks:
        raise dash.exceptions.PreventUpdate

    # Convert the table data (edited values from the UI) to a DataFrame.
    updated_df = pd.DataFrame(table_data)
    if selected_rows is None or len(selected_rows) == 0:
        # If no rows are selected, use an empty DataFrame.
        updated_df = updated_df.iloc[0:0]
    else:
        # Filter the DataFrame to include only selected rows.
        updated_df = updated_df.iloc[selected_rows]

    # Read the full original CSV file as a list of lines.
    with open('Samplesheet.csv', 'r', encoding='utf-8') as f:
        all_lines = f.readlines()

    # Locate the line where the "[Data]" marker is.
    data_marker_index = None
    for i, line in enumerate(all_lines):
        if line.strip().startswith("[Data]"):
            data_marker_index = i
            break
    if data_marker_index is None:
        return "Error: [Data] section not found in CSV."

    # Preserve all lines up to (and including) the [Data] marker.
    preserved_lines = all_lines[:data_marker_index+1]

    # Create a new header line from the UI.
    # This header comes from the DataTable columns (i.e. the DataFrame's column names).
    new_header_line = ",".join(updated_df.columns) + "\n"

    # Convert the updated (selected/edited) data to CSV format (without header and index).
    new_data_csv = updated_df.to_csv(index=False, header=False)

    # Reassemble the file: preserved header sections + new header + new data.
    new_file_content = "".join(preserved_lines) + new_header_line + new_data_csv

    # Write the reassembled content back to Samplesheet.csv.
    with open('Samplesheet.csv', 'w', encoding='utf-8') as f:
        f.write(new_file_content)



# Here we run the app on the specified host and port.
if __name__ == "__main__":
    app.run_server(debug=True, port=bfabric_web_apps.PORT, host=bfabric_web_apps.HOST)

