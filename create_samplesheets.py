# ---------------------------
# Modified create_samplesheets.py
# ---------------------------
import sys
import csv
import os
from datetime import datetime

sys.path.append("../bfabric-web-apps")

import bfabric_web_apps
from bfabric_web_apps.objects.BfabricInterface import bfabric_interface
from sample_sheet import SampleSheet, Sample

def create_samplesheets(
    token_data,
    app_data,
    output_file_pipeline_samplesheet="pipeline_samplesheet.csv"
):
    """
    Create lane-specific sample sheets (mRNA ligation prep) and a pipeline_samplesheet.csv.
    
    Steps:
      1. Query metadata for run, rununit, and instrument.
      2. For each lane (from rununitlane), fetch the lane-specific sample IDs and details.
      3. Create a SampleSheet for each lane and write it to a file (e.g. Samplesheet_lane_1.csv).
      4. Create a companion pipeline_samplesheet.csv.
    
    Returns:
      tuple: (last_lane_samples, created_csv_files)
             - last_lane_samples: list of sample details from the last processed lane (as before)
             - created_csv_files: list of lane-specific CSV filenames (excluding pipeline_samplesheet.csv)
    """
    L = bfabric_web_apps.get_logger(token_data)
    wrapper = bfabric_interface.get_wrapper()

    # Get run and rununit metadata using token_data "entity_id_data"
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

    # Get instrument data
    instrument_id = rununit[0]["instrument"]["id"]
    instrument_data = L.logthis(
        api_call=wrapper.read,
        endpoint="instrument",
        obj={"id": instrument_id},
        flush_logs=False
    )

    rununit_data = rununit[0]
    instrument_data = instrument_data[0]

    # Get all lane IDs from rununitlane field of rununit_data
    lane_ids = [str(lane["id"]) for lane in rununit_data.get("rununitlane", [])]
    if not lane_ids:
        print("No lanes found in rununit data.")
        return None, []
    
    # Get lane objects in one call
    lane_data_list = L.logthis(
        api_call=wrapper.read,
        endpoint="rununitlane",
        obj={"id": lane_ids},
        flush_logs=False
    )

    # Dictionary to store lane number to samplesheet file mapping
    lane_samplesheet_files = {}
    last_lane_samples = []  # This will hold the samples from the last processed lane

    # Process each lane and create its samplesheet
    for idx, lane in enumerate(lane_data_list):
        lane_number = idx + 1
        lane_sample_ids = [str(s["id"]) for s in lane.get("sample", [])]
        if not lane_sample_ids:
            print(f"Lane {lane_number} does not have any assigned samples.")
            continue

        # Fetch full sample details (batch if necessary)
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

        # Create a new SampleSheet object for this lane
        ss = SampleSheet()
        ss.Header["IEMFileVersion"] = 5
        ss.Header["Experiment Name"] = f"{rununit_data.get('name')} - Lane {lane_number}"
        ss.Header["Date"] = manipulate_date_format(rununit_data.get("created"))
        ss.Header["Workflow"] = "GenerateFASTQ"
        ss.Header["Application"] = app_data.get("name")
        ss.Header["Instrument Type"] = instrument_data.get("name")
        ss.Reads = [76, 76]
        ss.Settings["Adapter"] = "CTGTCTCTTATACACATCT"

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

        # Write lane-specific samplesheet
        lane_sheet_filename = f"Samplesheet_lane_{lane_number}.csv"
        with open(lane_sheet_filename, "w", newline="") as handle:
            ss.write(handle)
        print(f"Samplesheet for lane {lane_number} written to {lane_sheet_filename}")
        lane_samplesheet_files[lane_number] = lane_sheet_filename

        # Store samples from this lane (current behavior)
        last_lane_samples = lane_samples

    # Create the pipeline_samplesheet.csv (this file is not included in the returned list)
    create_pipeline_samplesheet_csv(run[0], rununit_data, lane_samplesheet_files, output_file_pipeline_samplesheet)
    
    return list(lane_samplesheet_files.values())

def create_pipeline_samplesheet_csv(
    run,
    rununit_data,
    lane_samplesheet_files,
    output_file
):
    """
    Create the pipeline_samplesheet.csv for Nextflow usage.
    This CSV has 4 columns: id, samplesheet, lane, flowcell.
    """
    run_id = os.path.basename(run.get("datafolder"))
    rows = []
    for lane_number, sheet_file in sorted(lane_samplesheet_files.items()):
        full_sheet_path = os.path.join(run.get("datafolder"), sheet_file)
        rows.append([run_id, full_sheet_path, str(lane_number), run.get("datafolder")])

    with open(output_file, mode="w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["id", "samplesheet", "lane", "flowcell"])
        writer.writerows(rows)
    print(f"pipeline_samplesheet.csv written to: {output_file}")

def manipulate_date_format(original_str):
    """
    Convert date strings from 'YYYY-mm-dd HH:MM:SS' to 'M/D/YYYY'
    (dropping leading zeros from the month/day).
    """
    dt_obj = datetime.strptime(original_str, "%Y-%m-%d %H:%M:%S")
    return f"{dt_obj.month}/{dt_obj.day}/{dt_obj.year}"
