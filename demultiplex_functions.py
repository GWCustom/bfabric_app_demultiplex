import pandas as pd
from io import StringIO
import os
import csv

# Samplesheets

def parse_samplesheet_data_only(filepath: str) -> pd.DataFrame:
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


def update_csv_bfore_runing_main_job(n_clicks, table_data, selected_rows):

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

    # Ensure there is a header line after the [Data] marker.
    if len(all_lines) <= data_marker_index + 1:
        return "Error: Data header line missing in CSV."

    # Preserve all lines up to and including the original data header line.
    # This keeps the [Data] marker and the header (with all the commas) intact.
    preserved_lines = all_lines[:data_marker_index + 2]

    # Extract the original header columns from the preserved header line.
    orig_header_line = all_lines[data_marker_index + 1]
    orig_header_cols = orig_header_line.strip().split(",")

    # Build new data rows aligned with the original header.
    new_data_rows = []
    for _, row in updated_df.iterrows():
        new_row = []
        for col in orig_header_cols:
            # If the column exists in the updated data, use its value;
            # otherwise, leave the field empty.
            if col in updated_df.columns:
                new_row.append(str(row[col]))
            else:
                new_row.append("")
        new_data_rows.append(",".join(new_row) + "\n")

    new_data_csv = "".join(new_data_rows)

    # Reassemble the file: preserved header sections + new data rows.
    new_file_content = "".join(preserved_lines) + new_data_csv

    # Write the reassembled content back to Samplesheet.csv.
    with open('Samplesheet.csv', 'w', encoding='utf-8', newline='') as f:
        f.write(new_file_content)




def load_samplesheet_data_when_loading_app(token_data):
    
    if not token_data:
        raise dash.exceptions.PreventUpdate
    
    csv_path = "Samplesheet.csv"
    if not os.path.isfile(csv_path):
        raise dash.exceptions.PreventUpdate("Samplesheet.csv doesn't exist yet.")
    
    df = parse_samplesheet_data_only(csv_path)
    if df.empty:
        return [], [], []
    
    # Build the DataTable columns such that only these four columns are editable
    editable_cols = {"index", "index2"}
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

#---------------------------------------------------------------------------------------------------#

# run_main_job


def read_file_as_bytes(file_path, max_size_mb=400):
    """Reads any file type and stores it as a byte string in a dictionary."""
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)  # Convert bytes to MB
    if file_size_mb > max_size_mb:
        raise ValueError(f"File {file_path} exceeds {max_size_mb}MB limit ({file_size_mb:.2f}MB).")

    with open(file_path, "rb") as f:  # Read as bytes
        file_as_bytes = f.read()

    return file_as_bytes



def create_resource_paths(sample_dict):
    """
    Constructs a dictionary of resource paths (keys) and their corresponding container IDs (values).

    Parameters:
        sample_dict (list): A list of dictionaries representing samples.

    The resource path is built using:
      - A hardcoded base: /STORAGE/OUTPUT_TEST/
      - The pipeline ID from pipeline_samplesheet.csv.
      - The lane (formatted as L001, L002, etc.) from pipeline_samplesheet.csv.
      - The container ID (from the sample dictionary).
      - The sample ID (from the sample dictionary).
      - A file name built as:
            <Sample_Name>_Sx_<lane_str>_R{read}_001.fastq.gz 
        for both R1 and R2, where Sx depends on the sample order in sample_dict.
    
    Returns:
        dict: A dictionary where keys are the resource paths and values are the corresponding container IDs.
    """
    base_dir = "/STORAGE/OUTPUT_TEST"
    resource_paths = {}

    # Read pipeline_samplesheet.csv to get pipeline rows.
    pipeline_path = "pipeline_samplesheet.csv"
    pipeline_rows = []
    with open(pipeline_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            pipeline_rows.append(row)
    
    # Use the provided sample_dict directly.
    samples = sample_dict

    # Loop through each pipeline row and each sample to build file paths.
    for p_row in pipeline_rows:
        pipeline_id = p_row["id"]
        lane = p_row["lane"]
        # Format lane as L001, L002, etc.
        lane_str = f"L{int(lane):03d}"
        
        # Use enumerate to assign sample order starting at 1.
        for idx, sample in enumerate(samples, start=1):
            sample_id = sample["id"]
            sample_name = sample["name"]
            container_id = sample["container"]["id"]
            
            # Construct sample identifier based on order (S1, S2, ...)
            sample_identifier = f"S{idx}"
            
            # Create file paths for both R1 and R2.
            for read in ["R1", "R2"]:
                file_name = f"{sample_name}_{sample_identifier}_{lane_str}_{read}_001.fastq.gz"
                full_path = f"{base_dir}/{pipeline_id}/{lane_str}/{container_id}/{sample_id}/{file_name}"
                resource_paths[full_path] = container_id
                
    return resource_paths
