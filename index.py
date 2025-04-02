"""
This module integrates bfabric_web_apps with bfabric_web_app_template,
ensuring that both modules are of the same version (e.g., 0.1.6) to avoid compatibility issues.
It sets up the necessary configurations, defines a Dash callback for running the main job pipeline,
and starts the web server.

Before running the application, ensure that bfabric_web_apps and bfabric_web_app_template are compatible!.
"""

import sys
import os
from dash import Input, Output, State

sys.path.append("../bfabric-web-apps")

import bfabric_web_apps
from bfabric_web_apps import run_main_job
from bfabric_web_apps.utils.redis_queue import q

import GetDataFromUser

from ExecuteRunMainJob import (
    read_file_as_bytes,
    create_resource_paths
)

import GetDataFromBfabric
from GetDataFromBfabric import (
    update_csv_bfore_runing_main_job,
)

from generic.callbacks import app

# Set configuration parameters for bfabric_web_apps.
bfabric_web_apps.CONFIG_FILE_PATH = "~/.bfabricpy.yml"
bfabric_web_apps.DEVELOPER_EMAIL_ADDRESS = "griffin@gwcustom.com"
bfabric_web_apps.BUG_REPORT_EMAIL_ADDRESS = "gwtools@fgcz.system"


# ---------------------------
# Run Pipeline Callback
# ---------------------------
@ app.callback(
    [
        Output("alert-fade-success", "is_open"), 
        Output("alert-fade-fail", "is_open"), 
        Output("alert-fade-fail", "children"),
        Output("refresh-workunits", "children")
    ],
    [Input("Submit", "n_clicks")],
    [
        State('url', 'search'),
        State("token_data", "data"),
        State("queue", "value"),
        State('samplesheet-table', "data"),
        State('samplesheet-table', "selected_rows"),
        State("lane-dropdown", "value"),
        State("csv_list_store", "data")
    ],
    prevent_initial_call=True
)
def run_main_job_callback(n_clicks, url_params, token_data, queue, table_data, selected_rows, lane_val, csv_list):
    """
    Dash callback for running the main job pipeline.

    This callback is triggered when the "Submit" button is clicked. It performs the following:
      1. Updates the selected lane CSV file with user edits (if a lane is selected).
      2. Prepares a dictionary of files (as byte strings) to be processed, including:
            - Lane sample sheets.
            - The pipeline sample sheet.
            - The NFC_DMX configuration file.
      3. Constructs bash commands to run the nf-core demultiplex pipeline using Nextflow.
      4. Creates resource paths to map file paths to container IDs.
      5. Enqueues the main job for asynchronous processing via a Redis queue.

    Parameters:
        n_clicks (int): The number of times the submit button has been clicked.
        url_params (str): URL search parameters (often used for tokens or authentication).
        token_data (dict): Token data required for generating resource paths.
        queue (str): The name of the Redis queue.
        table_data (list): Data from the samplesheet table reflecting user edits.
        selected_rows (list): List of selected row indices from the samplesheet table.
        lane_val (str/int): The selected lane identifier.
        csv_list (dict): Mapping of lane identifiers to their corresponding CSV file paths.

    Returns:
        tuple: Contains:
            - Boolean indicating if the success alert should be open.
            - Boolean indicating if the failure alert should be open.
            - A string with the failure message (if any).
            - A string message to refresh workunits.
    """
    try:
        # 1. Update the selected lane CSV with the user edits.
        if lane_val:
            csv_path = csv_list[lane_val]
            update_csv_bfore_runing_main_job(table_data, selected_rows, csv_path)

        # 2. Prepare the final dictionary of files as byte strings.
        files_as_byte_strings = {}

        # Loop through all lane sample sheets and add them to the dictionary.
        for sheet_path in csv_list:
            # Key format: "./<filename>" (e.g., "./Samplesheet_lane_1.csv")
            key = f"./{os.path.basename(sheet_path)}"
            files_as_byte_strings[key] = read_file_as_bytes(sheet_path)

        # 3. Add the pipeline sample sheet and NFC_DMX configuration file.
        files_as_byte_strings["./pipeline_samplesheet.csv"] = read_file_as_bytes("./pipeline_samplesheet.csv")
        files_as_byte_strings["./NFC_DMX.config"] = read_file_as_bytes("./NFC_DMX.config")

        # Define the output directory for the pipeline.
        base_dir = "/STORAGE/OUTPUT_TEST"

        # Construct the bash command to run the nf-core demultiplex pipeline.
        bash_commands = [
            f"""/home/nfc/.local/bin/nextflow run nf-core/demultiplex \
            -profile docker \
            --input /APPLICATION/200611_A00789R_0071_BHHVCCDRXX/pipeline_samplesheet.csv \
            --outdir {base_dir} \
            --demultiplexer bcl2fastq \
            --skip_tools samshee,checkqc \
            -c /APPLICATION/200611_A00789R_0071_BHHVCCDRXX/NFC_DMX.config \
            -r 1.5.4 > {base_dir}/nextflow.log 2>&1 &"""
        ]

        # 4. Create resource paths mapping file or folder to container IDs.
        resource_paths = create_resource_paths(token_data, base_dir)
        print("resource_paths", resource_paths)

        # Set attachment paths (e.g., for reports) to be monitored for output.
        attachment_paths = {"/STORAGE/OUTPUT_TEST/multiqc/multiqc_report.html": "multiqc_report.html"}

        # 5. Enqueue the main job into the Redis queue for asynchronous execution.
        q(queue).enqueue(run_main_job, kwargs={
            "files_as_byte_strings": files_as_byte_strings,
            "bash_commands": bash_commands,
            "resource_paths": resource_paths,
            "attachment_paths": attachment_paths,
            "token": url_params
        })

        # Return success alert open, failure alert closed, no error message, and a success message.
        return True, False, "", "Job submitted successfully"

    except Exception as e:
        # If an error occurs, return failure alert open with the error message.
        return False, True, f"Job submission failed: {str(e)}", "Job submission failed"


# ---------------------------
# Main Application Runner
# ---------------------------
if __name__ == "__main__":
    # Start the Dash web server.
    # The 'app' instance is provided by GetDataFromUser.py.
    GetDataFromUser.app.run_server(
        debug=True,
        port=bfabric_web_apps.PORT,
        host=bfabric_web_apps.HOST
    )
