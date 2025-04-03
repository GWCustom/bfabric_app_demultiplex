# This module integrates bfabric_web_apps with bfabric_web_app_template,
# ensuring that both modules are of the same version (e.g., 0.1.6) to avoid compatibility issues.
# It sets up the necessary configurations, defines a Dash callback for running the main job pipeline,
# and starts the web server.

# Before running the application, ensure that bfabric_web_apps and bfabric_web_app_template are compatible!.

import sys
sys.path.insert(0, "../bfabric-web-apps")

import os
from dash import Input, Output, State
import bfabric_web_apps
from bfabric_web_apps import run_main_job, get_logger, read_file_as_bytes
from bfabric_web_apps.utils.redis_queue import q
import GetDataFromUser
from GetDataFromUser import update_csv_based_on_ui
from ExecuteRunMainJob import create_resource_paths
import GetDataFromBfabric
from generic.callbacks import app

# Set configuration parameters for bfabric_web_apps.
bfabric_web_apps.CONFIG_FILE_PATH = "~/.bfabricpy.yml"
bfabric_web_apps.DEVELOPER_EMAIL_ADDRESS = "griffin@gwcustom.com"
bfabric_web_apps.BUG_REPORT_EMAIL_ADDRESS = "gwtools@fgcz.system"
bfabric_web_apps.debug = True


# ---------------------------
# Run Main Job Callback
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
    Callback to run the main job pipeline asynchronously when the "Submit" button is clicked.
    
    The callback executes the following steps:
      1. **Update CSV File:**  
         If a lane is selected (indicated by `lane_val`), the corresponding CSV file in `csv_list`
         is updated with any user edits from `table_data` and the rows selected in `selected_rows`.
      
      2. **Prepare Files Dictionary:**  
         Constructs a dictionary named `files_as_byte_strings` that maps file paths (as keys) to the
         file contents read as byte strings (as values). The structure is as follows:
            {
                "./filename": <file_as_bytes>,
            }
         - For each lane sample sheet, the key is formatted as "./<filename>" using the basename of the file.
         - In addition, the pipeline sample sheet and the NFC_DMX configuration file are also included.
      
      3. **Construct Bash Commands:**  
         Creates a list of bash command strings that are used to run the nf-core demultiplex pipeline 
         via Nextflow.
      
      4. **Create Resource Paths:**  
         Uses `create_resource_paths` to map file paths or directories to container IDs based on the
         provided token data and the designated output directory.
      
      5. **Enqueue the Job:**  
         The main job is enqueued for asynchronous processing via a Redis queue. The job is submitted 
         using the provided queue (either "light" or "heavy") along with all prepared parameters:
            - The dictionary of files as byte strings.
            - The list of bash commands.
            - The resource paths mapping.
            - Attachment paths for monitoring outputs (e.g., a MultiQC report).
            - The token extracted from URL parameters.
    
    Parameters:
        n_clicks (int): Number of times the submit button has been clicked.
        url_params (str): URL parameters (includes token information for authentication).
        token_data (dict): Authentication token data required for resource path generation.
        queue (str): Name of the Redis queue to use ("light" or "heavy").
        table_data (list): List of dictionaries representing the current state of the samplesheet table,
                           including any user edits.
        selected_rows (list): List of indices indicating which rows in the samplesheet table are selected.
        lane_val (int or str): Identifier for the selected lane (used to pick the correct CSV file from csv_list).
        csv_list (list): List mapping lane identifiers to their corresponding CSV file paths.
    
    Returns:
            - (bool) Success alert state: True if the job was submitted successfully.
            - (bool) Failure alert state: True if the job submission failed.
            - (str) Failure message: An error message if the submission failed; otherwise, an empty string.
            - (str) Refresh workunits message: A status message indicating the outcome of the job submission.
    """
    L = get_logger(token_data)
    try:
        # Log that the user has initiated the main job pipeline.
        L.log_operation("Pipeline Info", "Job is started: User initiated main job pipeline.")
        # 1. Update the selected lane CSV with the user edits.
        if lane_val:
            csv_path = csv_list[lane_val]
            update_csv_based_on_ui(table_data, selected_rows, csv_path)

        # 2. Prepare the final dictionary of files as byte strings.
        files_as_byte_strings = {}

        # Loop through all lane sample sheets and add them to the dictionary.
        for sheet_path in csv_list:
            # Key format: "./<filename>" (e.g., "./Samplesheet_lane_1.csv")
            key = f"./{os.path.basename(sheet_path)}"
            files_as_byte_strings[key] = read_file_as_bytes(sheet_path)
            L.log_operation("Pipeline Info", f"File loaded: {key} loaded from {sheet_path}.")

        # 3. Add the pipeline sample sheet and NFC_DMX configuration file.
        files_as_byte_strings["./pipeline_samplesheet.csv"] = read_file_as_bytes("./pipeline_samplesheet.csv")
        L.log_operation("Pipeline Info", "Pipeline samplesheet loaded from ./pipeline_samplesheet.csv.")
        files_as_byte_strings["./NFC_DMX.config"] = read_file_as_bytes("./NFC_DMX.config")
        L.log_operation("Pipeline Info", "NFC_DMX configuration loaded from ./NFC_DMX.config.")

        # Define the output directory for the pipeline.
        base_dir = "/STORAGE/OUTPUT_TEST"

        # Construct the bash command to run the nf-core demultiplex pipeline.
        bash_commands = [

            "rm -rf /APPLICATION/200611_A00789R_0071_BHHVCCDRXX/work"
            ,
            f"""/home/nfc/.local/bin/nextflow run nf-core/demultiplex \
            -profile docker \
            --input /APPLICATION/200611_A00789R_0071_BHHVCCDRXX/pipeline_samplesheet.csv \
            --outdir {base_dir} \
            --demultiplexer bcl2fastq \
            --skip_tools samshee,checkqc \
            -c /APPLICATION/200611_A00789R_0071_BHHVCCDRXX/NFC_DMX.config \
            -r 1.5.4 > {base_dir}/nextflow.log 2>&1"""
        ]

        # 4. Create resource paths mapping file or folder to container IDs.
        resource_paths = create_resource_paths(token_data, base_dir)
        L.log_operation("Pipeline Info", f"Resource paths created: {resource_paths}")
        print("resource_paths", resource_paths)

        # Set attachment paths (e.g., for reports)
        attachment_paths = {"/STORAGE/OUTPUT_TEST/multiqc/multiqc_report.html": "multiqc_report.html"}
        L.log_operation("Pipeline Info", "Attachment paths created: {attachment_paths}")

        # 5. Enqueue the main job into the Redis queue for asynchronous execution.
        q(queue).enqueue(run_main_job, kwargs={
            "files_as_byte_strings": files_as_byte_strings,
            "bash_commands": bash_commands,
            "resource_paths": resource_paths,
            "attachment_paths": attachment_paths,
            "token": url_params
        })
      
        # Log that the job was submitted successfully.
        L.log_operation("Pipeline Info", f"Job submitted successfully to {queue} Redis queue.")
        # Return success alert open, failure alert closed, no error message, and a success message.
        return True, False, "", "Job submitted successfully"

    except Exception as e:
        # Log that the job submission failed.
        L.log_operation("Pipeline Info", f"Job submission failed: {str(e)}")
        # If an error occurs, return failure alert open with the error message.
        return False, True, f"Job submission failed: {str(e)}", "Job submission failed"


# ---------------------------
# Main Application Runner
# ---------------------------
if __name__ == "__main__":
    # Start the Dash web server.
    # The 'app' instance is provided by GetDataFromUser.py.
    GetDataFromUser.app.run_server(
        debug=bfabric_web_apps.debug,
        port=bfabric_web_apps.PORT,
        host=bfabric_web_apps.HOST
    )
