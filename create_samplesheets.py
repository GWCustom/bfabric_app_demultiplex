"""
Module: create_samplesheets
Description:
    This module provides functionality for creating lane-specific sample sheets for mRNA ligation prep,
    as well as generating a companion pipeline_samplesheet.csv for Nextflow usage.
    
    The main functions include:
      - create_samplesheets: Queries necessary metadata and creates individual samplesheets for each lane.
      - create_pipeline_samplesheet_csv: Generates a CSV that maps lanes to their samplesheet file paths.
      - manipulate_date_format: Reformats a datetime string to a simplified date format.
"""

import sys
import csv
import os
from datetime import datetime

# Append relative path to locate bfabric-web-apps package
sys.path.append("../bfabric-web-apps")

import bfabric_web_apps
from bfabric_web_apps.objects.BfabricInterface import bfabric_interface
from sample_sheet import SampleSheet, Sample


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


def create_pipeline_samplesheet_csv(run, rununit_data, lane_samplesheet_files, output_file):
    """
    Create the pipeline_samplesheet.csv for Nextflow usage.
    
    The generated CSV contains four columns: id, samplesheet, lane, flowcell.
    
    Parameters:
        run: Run metadata that includes the datafolder path.
        rununit_data: Rununit metadata.
        lane_samplesheet_files: Mapping of lane numbers to their samplesheet filenames.
        output_file: The output filename for the pipeline samplesheet CSV.
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