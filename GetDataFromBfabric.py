import sys
sys.path.append("../bfabric-web-apps")

import bfabric_web_apps
from bfabric_web_apps.objects.BfabricInterface import bfabric_interface
from sample_sheet import SampleSheet, Sample
from datetime import datetime
import pandas as pd
from io import StringIO
import os
import csv
from dash import Input, Output, State, html, dcc, dash_table, callback, no_update
import dash.exceptions
import dash_bootstrap_components as dbc
from generic.callbacks import app

#-----------------------
# Callback for creating samplesheets when loading the app
#-----------------------
@app.callback(
    Output('csv_list_store', 'data'),
    [Input("token_data", "data")],
    [State("app_data", "data")]
)
def create_samplesheets_when_loading_app(token_data, app_data):
    """
    Generates the required samplesheet CSV files and returns as output.

    Args:
        token_data (dict): Authentication token data.
        app_data (dict): Application metadata.

    Returns:
        list: List of created CSV filenames (excluding the pipeline_samplesheet).
    """
    if token_data:
        csv_list = create_samplesheets(
            token_data,
            app_data,
            output_file_pipeline_samplesheet="pipeline_samplesheet.csv"
        )
        return csv_list


#-----------------------
# Function for creating the samplesheets based on API calls to Bfabric
#-----------------------

def create_samplesheets(token_data, app_data, output_file_pipeline_samplesheet="pipeline_samplesheet.csv"):
    """
    Create lane-specific sample sheets and a pipeline_samplesheet.csv.
    
    Steps:
      1. Query metadata for run, rununit, and instrument.
      2. For each lane in the rununit, fetch lane-specific sample IDs and details.
      3. Create a SampleSheet for each lane and write it to a file (e.g. Samplesheet_lane_1.csv).
      4. Create a companion pipeline_samplesheet.csv that maps lanes to samplesheet paths.
    
    Parameters:
        token_data: Authentication and metadata token, must include "entity_id_data".
        app_data: Application metadata, expected to contain the key "name".
        output_file_pipeline_samplesheet: Filename for the pipeline samplesheet CSV.
    
    Returns:
        A list of filenames for lane-specific CSV samplesheets (excluding pipeline_samplesheet.csv).
    """
    L = bfabric_web_apps.get_logger(token_data)
    wrapper = bfabric_interface.get_wrapper()

    # Query run and rununit metadata using token_data "entity_id_data"
    run = L.logthis(
        api_call=wrapper.read,
        endpoint="run",
        obj={"id": token_data["entity_id_data"]},
        flush_logs=False
    )
    rununit = L.logthis(
        api_call=wrapper.read,
        endpoint="rununit",
        obj={"runid": token_data["entity_id_data"]},
        flush_logs=False
    )

    # Retrieve instrument data
    instrument_id = rununit[0]["instrument"]["id"]
    instrument_data = L.logthis(
        api_call=wrapper.read,
        endpoint="instrument",
        obj={"id": instrument_id},
        flush_logs=False
    )

    rununit_data = rununit[0]
    instrument_data = instrument_data[0]

    # Extract lane IDs from rununit_data's "rununitlane" field
    lane_ids = [str(lane["id"]) for lane in rununit_data.get("rununitlane", [])]
    if not lane_ids:
        print("No lanes found in rununit data.")
        return []

    # Retrieve lane objects in a single call
    lane_data_list = L.logthis(
        api_call=wrapper.read,
        endpoint="rununitlane",
        obj={"id": lane_ids},
        flush_logs=False
    )

    lane_samplesheet_files = {}  # Mapping from lane number to samplesheet filename

    # Process each lane and create its respective samplesheet
    for idx, lane in enumerate(lane_data_list):
        lane_number = idx + 1
        lane_sample_ids = [str(s["id"]) for s in lane.get("sample", [])]
        if not lane_sample_ids:
            print("Lane {} does not have any assigned samples.".format(lane_number))
            continue

        # Fetch full sample details; if more than 100 samples, fetch in batches
        lane_samples = []
        if len(lane_sample_ids) < 100:
            lane_samples = L.logthis(
                api_call=wrapper.read,
                endpoint="sample",
                obj={"id": lane_sample_ids},
                flush_logs=False
            )
        else:
            for i in range(0, len(lane_sample_ids), 100):
                lane_samples += L.logthis(
                    api_call=wrapper.read,
                    endpoint="sample",
                    obj={"id": lane_sample_ids[i:i+100]},
                    flush_logs=False
                )

        # Create a new SampleSheet object for the current lane
        ss = SampleSheet()
        ss.Header["IEMFileVersion"] = 5
        ss.Header["Experiment Name"] = "{} - Lane {}".format(rununit_data.get("name"), lane_number)
        ss.Header["Date"] = manipulate_date_format(rununit_data.get("created"))
        ss.Header["Workflow"] = "GenerateFASTQ"
        ss.Header["Application"] = app_data.get("name")
        ss.Header["Instrument Type"] = instrument_data.get("name")
        ss.Reads = [76, 76]
        ss.Settings["Adapter"] = "CTGTCTCTTATACACATCT"

        # Add each sample record to the samplesheet
        for record in lane_samples:
            sample_dict = {
                "Sample_ID": record["id"],
                "Sample_Name": record["name"],
                "Sample_Plate": "",
                "Sample_Well": "",
                "Index_Plate": "",
                "Index_Plate_Well": "",
                "I7_Index_ID": record["multiplexiddmx"],
                "index": record["multiplexiddmx"],
                "I5_Index_ID": record["multiplexid2dmx"],
                "index2": record["multiplexid2dmx"],
                "Sample_Project": record["container"]["id"],
                "Description": ""
            }
            ss.add_sample(Sample(sample_dict))

        # Write the lane-specific samplesheet to a CSV file
        lane_sheet_filename = "Samplesheet_lane_{}.csv".format(lane_number)
        with open(lane_sheet_filename, "w+", newline="") as handle:
            ss.write(handle)
        print("Samplesheet for lane {} written to {}".format(lane_number, lane_sheet_filename))
        lane_samplesheet_files[lane_number] = lane_sheet_filename

    # Generate the pipeline_samplesheet.csv (not included in the returned list)
    create_pipeline_samplesheet_csv(run[0], rununit_data, lane_samplesheet_files, output_file_pipeline_samplesheet)
    return list(lane_samplesheet_files.values())


#-----------------------
# Helper function: Create Pipeline Samplesheet CSV
#-----------------------

def create_pipeline_samplesheet_csv(run, rununit_data, lane_samplesheet_files, output_file):
    """
    Creates the pipeline_samplesheet.csv for Nextflow usage.

    Args:
        run (dict): Run metadata that includes the datafolder path.
        rununit_data (dict): Rununit metadata.
        lane_samplesheet_files (dict): Mapping of lane numbers to their samplesheet filenames.
        output_file (str): The output filename for the pipeline samplesheet CSV.

    Returns:
        None
    """
    run_id = os.path.basename(run.get("datafolder"))
    rows = []
    for lane_number, sheet_file in sorted(lane_samplesheet_files.items()):
        full_sheet_path = os.path.join(run.get("datafolder"), sheet_file)
        rows.append([run_id, full_sheet_path, str(lane_number), run.get("datafolder")])

    with open(output_file, mode="w+", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["id", "samplesheet", "lane", "flowcell"])
        writer.writerows(rows)
    print("pipeline_samplesheet.csv written to: {}".format(output_file))


#-----------------------
# Helper function: Manipulate Date Format
#-----------------------
def manipulate_date_format(original_str):
    """
    Convert a datetime string from 'YYYY-mm-dd HH:MM:SS' to 'M/D/YYYY',
    removing any leading zeros from the month and day.
    
    Parameters:
        original_str: A date string in the format 'YYYY-mm-dd HH:MM:SS'.
    
    Returns:
        A reformatted date string in the format 'M/D/YYYY'.
    """
    dt_obj = datetime.strptime(original_str, "%Y-%m-%d %H:%M:%S")
    return "{}/{}/{}".format(dt_obj.month, dt_obj.day, dt_obj.year)


#-----------------------
# Callback: Load Samplesheet Data for UI
#-----------------------
@app.callback(
    Output("samplesheet-table", "data"),
    Output("samplesheet-table", "columns"),
    Output("samplesheet-table", "selected_rows"),
    State("token_data", "data"),
    Input("lane-dropdown", "value"),
    Input("csv_list_store", "data"),  # Use the dynamically created CSV list,
)
def load_samplesheet_data(token_data, lane_value, csv_list):
    """
    Load and return samplesheet data based on the token, selected lane, and available CSV files.

    Args:
        token_data (dict): Authentication token data.
        lane_value (int or None): The index of the selected lane.
        csv_list (list): List of CSV file paths.

    Returns:
        tuple: A tuple containing:
            - (list): Table data (list of dictionaries).
            - (list): Table columns (list of dictionaries).
            - (list): Indices of selected rows.
    """
    if not csv_list or not isinstance(csv_list, list):
        raise dash.exceptions.PreventUpdate("No CSV list available.")
    return load_samplesheet_data_when_loading_app(token_data, lane_value, csv_list)

#-----------------------
# Helper function: Load Samplesheet Data When Loading App
#-----------------------

def load_samplesheet_data_when_loading_app(token_data, lane_value, csv_list):
    """
    Loads samplesheet data for a specific lane from a CSV file.

    Args:
        token_data (dict): Authentication token data.
        lane_value (int or None): The index of the selected lane.
        csv_list (list): List of CSV file paths.

    Returns:
        tuple: A tuple containing:
            - list: Table data (list of dictionaries).
            - list: Table columns (list of dictionaries).
            - list: Indices of selected rows.
    """
    if not token_data:
        raise dash.exceptions.PreventUpdate
    
    if lane_value == None:
         return [], [], []

    try:
        lane_index = int(lane_value)
    except (ValueError, TypeError):
        lane_index = 0

    if lane_index >= len(csv_list):
        raise dash.exceptions.PreventUpdate(f"Lane {lane_value} does not exist.")

    csv_path = csv_list[lane_index]
    if not os.path.isfile(csv_path):
        raise dash.exceptions.PreventUpdate(f"{csv_path} doesn't exist yet.")

    df = parse_samplesheet_data_only(csv_path)
    if df.empty:
        return [], [], []

    editable_cols = {"index", "index2"}
    columns = [{"name": col, "id": col, "editable": (col in editable_cols)} for col in df.columns]
    data = df.to_dict("records")
    all_indices = list(range(len(df)))
    return data, columns, all_indices



#-----------------------
# Helper function: Parse Samplesheet Data Only
#-----------------------

def parse_samplesheet_data_only(filepath):
    """
    Parses the samplesheet CSV file to extract the data section.

    This function locates the "[Data]" marker and reads the CSV data starting from the header row
    following the marker. Only specific columns are retained.

    Args:
        filepath (str): Path to the samplesheet CSV file.

    Returns:
        pd.DataFrame: A DataFrame containing the samplesheet data.
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    # Locate the [Data] line.
    data_start_idx = None
    for i, line in enumerate(lines):
        if line.strip().startswith("[Data]"):
            data_start_idx = i
            break

    if data_start_idx is None:
        return pd.DataFrame()

    # Read data starting right after [Data]
    csv_lines = lines[data_start_idx + 1:]  # +1 to include header row

    csv_string = "".join(csv_lines)
    df = pd.read_csv(StringIO(csv_string))

    # Select only the specific columns
    columns_to_keep = ["Sample_ID", "Sample_Name", "index", "index2", "Sample_Project"]
    df = df[columns_to_keep]

    return df