
import os
import csv
import pandas as pd
# run_main_job


def read_file_as_bytes(file_path, max_size_mb=400):
    """Reads any file type and stores it as a byte string in a dictionary."""
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)  # Convert bytes to MB
    if file_size_mb > max_size_mb:
        raise ValueError(f"File {file_path} exceeds {max_size_mb}MB limit ({file_size_mb:.2f}MB).")

    with open(file_path, "rb") as f:  # Read as bytes
        file_as_bytes = f.read()

    return file_as_bytes


def parse_samples_csv(file_path):
    """
    Parses a sample CSV file that includes a [Data] section.
    
    The function skips header sections until it reaches the "[Data]" marker.
    The row immediately following "[Data]" is assumed to be the header for the sample data.
    
    Returns:
        list: A list of dictionaries, each representing a sample.
    """
    samples = []
    with open(file_path, newline='') as f:
        lines = f.readlines()

    # Find the index of the [Data] section.
    data_index = None
    for i, line in enumerate(lines):
        if line.strip().startswith("[Data]"):
            data_index = i
            break

    if data_index is None:
        raise ValueError(f"No [Data] section found in file: {file_path}")

    # The header row is the next line after the [Data] marker.
    header = lines[data_index + 1].strip().split(',')
    # Data rows start after the header.
    for line in lines[data_index + 2:]:
        if not line.strip():
            continue
        row = line.strip().split(',')
        if len(row) < len(header):
            continue
        sample_data = dict(zip(header, row))
        samples.append(sample_data)
    return samples

def create_resource_paths(token_data, base_dir):
    """
    Constructs a dictionary of resource paths (keys) and their corresponding container IDs (values)
    using data from pipeline_samplesheet.csv and sample CSV files.
    
    Process:
      - Reads pipeline_samplesheet.csv to obtain pipeline rows.
      - For each pipeline row, it:
          • Determines the formatted lane string (e.g. L001, L002, etc.).
          • Extracts the samplesheet's basename from the pipeline row.
          • Checks if the samplesheet is among the provided csv_list.
          • Parses the samples CSV file to obtain sample metadata.
          • Enumerates the samples to assign an order (S1, S2, …).
          • Uses the Sample_Project field as the container ID.
          • Constructs file paths for both R1 and R2 reads:
                <Sample_Name>_Sx_<lane_str>_R{read}_001.fastq.gz
      - The full resource path is built as:
            <base_dir>/<pipeline_id>/<lane_str>/<container_id>/<sample_id>/<file_name>
    
    Returns:
        dict: Keys are resource paths and values are the corresponding container IDs.
    """
    resource_paths = {}

    # Read pipeline_samplesheet.csv to get pipeline rows.
    pipeline_path = "pipeline_samplesheet.csv"
    pipeline_rows = []
    with open(pipeline_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            pipeline_rows.append(row)

    # Process each pipeline row.
    for p_row in pipeline_rows:
        pipeline_id = p_row["id"]
        lane = p_row["lane"]
        lane_str = f"L{int(lane):03d}"  # Format lane as L001, L002, etc.
        
        # Get the basename of the samplesheet from the pipeline row.
        samplesheet_path = p_row["samplesheet"]
        samplesheet_basename = os.path.basename(samplesheet_path)

        # Parse the samples CSV file to obtain sample metadata.
        samples = parse_samples_csv(samplesheet_basename)

        # Enumerate samples to assign sample order (S1, S2, ...).
        for idx, sample in enumerate(samples, start=1):
            sample_id = sample["Sample_ID"]
            sample_name = sample["Sample_Name"]
            # Use the Sample_Project field as container_id.
            container_id = sample["Sample_Project"]

            sample_identifier = f"S{idx}"
            
            # Create file paths for both R1 and R2.
            for read in ["R1", "R2"]:
                file_name = f"{sample_name}_{sample_identifier}_{lane_str}_{read}_001.fastq.gz"
                full_path = f"{base_dir}/{pipeline_id}/{lane_str}/{container_id}/{sample_id}/{file_name}"
                resource_paths[full_path] = container_id

    return resource_paths



