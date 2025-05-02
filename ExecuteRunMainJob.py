import os
import csv
import pandas as pd

# ---------------------------
# Resource Path Construction
# ---------------------------
def create_resource_paths_and_dataset(token_data, base_dir):
    """
    Constructs a dictionary mapping resource file paths to container IDs using pipeline and sample CSV data.
    Additionally, creates the dataset dictionary for the resulting dataset object. 

    Process:
      - Reads pipeline_samplesheet.csv to obtain pipeline rows.
      - For each pipeline row:
          • Formats the lane number as a string (e.g., L001, L002, etc.).
          • Extracts the samplesheet's basename from the pipeline row.
          • Parses the corresponding samples CSV file to obtain sample metadata.
          • Enumerates the samples to assign an order (e.g., S1, S2, ...).
          • Uses the Sample_Project field as the container ID.
          • Constructs file paths for both R1 and R2 reads in the format:
                <Sample_Name>_Sx_<lane_str>_R{read}_001.fastq.gz
      - The full resource path is built as:
            <base_dir>/<pipeline_id>/<lane_str>/<container_id>/<sample_id>/<file_name>

    Args:
        token_data (dict): Token data for authentication (currently not used in this function).
        base_dir (str): Base directory where the resource files will be stored.

    Returns:
        A Tuple containing:
            dict: A dictionary where keys are resource file paths and values are the corresponding container IDs.
              For example:
              {
                  "/STORAGE/OUTPUT/12345/L001/ContainerA/1001/SampleName_S1_L001_R1_001.fastq.gz": "ContainerID",
              }
            dict: A dataset dictionary where keys are container IDs and values are dataset dictionaries.
                For example:
                {
                    "ContainerID": {
                        "Sample": ["SampleName_S1_L001_R1_001.fastq.gz", ...],
                        ...
                    },
                }
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

        # Initialize entries for the dataset dictionary.
        sample_names = []
        r1 = []
        r2 = []
        strandenesses = []
        container_ids = []

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

                if read == "R1":
                    r1.append(full_path)
                else:
                    r2.append(full_path)
                    sample_names.append(sample_name)
                    strandenesses.append("auto") 
                    container_ids.append(container_id)
    
    dataset_dict = {}

    for container_id in set(container_ids):
        # Create a dataset dictionary for each container_id.
        dataset_dict[str(container_id)] = {
            "Sample": [],
            "FASTQ Read 1": [],
            "FASTQ Read 2": [],
            "Strandedness": []
        }

    for i, container_id in enumerate(container_ids):
        dataset_dict[str(container_id)]['Sample'].append(sample_names[i])
        dataset_dict[str(container_id)]['FASTQ Read 1'].append(r1[i])
        dataset_dict[str(container_id)]['FASTQ Read 2'].append(r2[i])
        dataset_dict[str(container_id)]['Strandedness'].append(strandenesses[i])

    return resource_paths, dataset_dict


# ---------------------------
# Parse Samples CSV | Helper function for the Resource Path Construction
# ---------------------------
def parse_samples_csv(file_path):
    """
    Parses a sample CSV file that includes a "[Data]" section.

    The function skips header sections until it reaches the "[Data]" marker.
    The row immediately following "[Data]" is assumed to be the header for the sample data.
    Each subsequent row is converted into a dictionary using the header.

    Args:
        file_path (str): Path to the sample CSV file.

    Returns:
        list: A list of dictionaries, each representing a sample.

    Raises:
        ValueError: If no "[Data]" section is found in the file.
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